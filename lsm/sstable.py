"""Immutable sorted JSON SSTable writer, reader, and validator.

On-disk JSON schema (PRD Section 8)::

    {
      "file_id": 1,
      "min_key": "a",
      "max_key": "z",
      "entry_count": 2,
      "entries": [
        ["a", "1"],
        ["z", null]
      ]
    }

* ``entries`` is sorted strictly ascending by key.
* A ``null`` value represents a tombstone (key deleted).
* Files are immutable once created: written via temp file -> fsync ->
  ``os.replace`` -> ``fsync_dir``.
"""
from __future__ import annotations

from bisect import bisect_left
import json
import os
from typing import Dict, List, Optional, Tuple, Union

from ._io import atomic_write_bytes
from .errors import CorruptFileError, StorageIOError

SSTABLE_PREFIX = "sst_"
SSTABLE_SUFFIX = ".json"
_REQUIRED_FIELDS = frozenset(
    {"file_id", "min_key", "max_key", "entries", "entry_count"}
)

# Sentinel distinguishing "key absent in this SSTable" from None (tombstone).
SST_MISSING = object()


def sstable_filename(file_id: int) -> str:
    """Return canonical zero-padded filename for ``file_id`` (e.g. ``sst_000001.json``)."""
    if type(file_id) is not int or file_id < 1:
        raise ValueError("file_id must be a positive integer")
    return f"{SSTABLE_PREFIX}{file_id:06d}{SSTABLE_SUFFIX}"


class SSTable:
    """In-memory handle for a validated, immutable on-disk SSTable."""

    def __init__(
        self,
        file_id: int,
        path: str,
        min_key: str,
        max_key: str,
        entries: List[Tuple[str, Optional[str]]],
    ) -> None:
        self.file_id = file_id
        self.path = path
        self.filename = os.path.basename(path)
        self.min_key = min_key
        self.max_key = max_key
        self._entries = entries
        self._keys = [k for k, _ in entries]

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @property
    def entries(self) -> List[Tuple[str, Optional[str]]]:
        return list(self._entries)

    def get(self, key: str) -> object:
        """Look up ``key`` in this SSTable.

        Returns:
        * ``str`` value if present and live,
        * ``None`` if present as a tombstone (caller MUST stop searching older files),
        * :data:`SST_MISSING` if ``key`` is not in this file (caller continues).
        """
        if key < self.min_key or key > self.max_key:
            return SST_MISSING
        idx = bisect_left(self._keys, key)
        if idx < len(self._keys) and self._keys[idx] == key:
            return self._entries[idx][1]
        return SST_MISSING

    @classmethod
    def write(
        cls,
        dirpath: Union[str, "os.PathLike[str]"],
        file_id: int,
        items: Dict[str, Optional[str]],
        sync: bool = True,
    ) -> "SSTable":
        """Atomically write ``items`` as a sorted SSTable and return it.

        ``items`` must be non-empty (flushing an empty memtable is a no-op
        handled by :class:`LSMStore`).
        """
        if type(file_id) is not int or file_id < 1:
            raise ValueError("file_id must be a positive integer")
        if not items:
            raise ValueError("cannot write an empty SSTable")

        sorted_entries: List[Tuple[str, Optional[str]]] = sorted(
            items.items(), key=lambda kv: kv[0]
        )
        min_key = sorted_entries[0][0]
        max_key = sorted_entries[-1][0]
        entry_count = len(sorted_entries)

        payload = {
            "file_id": file_id,
            "min_key": min_key,
            "max_key": max_key,
            "entry_count": entry_count,
            "entries": [[k, v] for k, v in sorted_entries],
        }
        raw = json.dumps(payload, ensure_ascii=True, indent=2).encode("ascii") + b"\n"

        directory = os.fspath(dirpath)
        target_path = os.path.join(directory, sstable_filename(file_id))
        atomic_write_bytes(target_path, raw, sync=sync)
        return cls(file_id, target_path, min_key, max_key, sorted_entries)

    @classmethod
    def load(
        cls,
        path: Union[str, "os.PathLike[str]"],
        expected_file_id: Optional[int] = None,
    ) -> "SSTable":
        """Load and strictly validate an SSTable from ``path``.

        Raises :class:`CorruptFileError` if the file is missing, unreadable as
        JSON, or fails schema, sort-order, key-bounds, or ``entry_count`` checks.
        """
        filepath = os.fspath(path)
        filename = os.path.basename(filepath)

        def bad(reason: str) -> CorruptFileError:
            return CorruptFileError(filename, reason)

        try:
            with open(filepath, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            raise bad("SSTable file is missing") from None
        except OSError as exc:
            raise StorageIOError(f"cannot read SSTable {filename!r}: {exc}") from exc

        try:
            obj = json.loads(raw.decode("utf-8"))
        except ValueError:
            raise bad("not valid UTF-8 JSON") from None

        if not isinstance(obj, dict) or set(obj) != _REQUIRED_FIELDS:
            raise bad(
                "SSTable must be a JSON object with fields "
                "file_id, min_key, max_key, entries, entry_count"
            )

        file_id = obj["file_id"]
        min_key = obj["min_key"]
        max_key = obj["max_key"]
        raw_entries = obj["entries"]
        entry_count = obj["entry_count"]

        if type(file_id) is not int or file_id < 1:
            raise bad("file_id must be a positive integer")
        if expected_file_id is not None and file_id != expected_file_id:
            raise bad(f"expected file_id {expected_file_id}, got {file_id}")
        if not isinstance(min_key, str) or min_key == "":
            raise bad("min_key must be a non-empty string")
        if not isinstance(max_key, str) or max_key == "":
            raise bad("max_key must be a non-empty string")
        if type(entry_count) is not int or entry_count < 1:
            raise bad("entry_count must be a positive integer")
        if not isinstance(raw_entries, list) or len(raw_entries) != entry_count:
            raise bad(
                f"entry_count ({entry_count}) does not match entries length "
                f"({len(raw_entries) if isinstance(raw_entries, list) else 'non-list'})"
            )

        entries: List[Tuple[str, Optional[str]]] = []
        prev_key: Optional[str] = None
        for idx, pair in enumerate(raw_entries):
            if not isinstance(pair, list) or len(pair) != 2:
                raise bad(f"entry {idx} must be a 2-element [key, value] list")
            k, v = pair[0], pair[1]
            if not isinstance(k, str) or k == "":
                raise bad(f"entry {idx} has invalid key")
            if v is not None and not isinstance(v, str):
                raise bad(f"entry {idx} value must be a string or null")
            if prev_key is not None and k <= prev_key:
                raise bad(
                    f"entries not strictly sorted at index {idx}: {k!r} <= {prev_key!r}"
                )
            prev_key = k
            entries.append((k, v))

        if entries[0][0] != min_key:
            raise bad(
                f"min_key {min_key!r} does not match first entry {entries[0][0]!r}"
            )
        if entries[-1][0] != max_key:
            raise bad(
                f"max_key {max_key!r} does not match last entry {entries[-1][0]!r}"
            )

        return cls(file_id, filepath, min_key, max_key, entries)
