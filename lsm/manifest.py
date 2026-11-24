"""Atomic manifest persistence for live SSTables and flush watermark.

On-disk JSON schema (PRD Section 8)::

    {
      "live_files": ["sst_000001.json", "sst_000002.json"],
      "next_file_id": 3,
      "last_flushed_seq": 2000
    }

* ``live_files`` lists SSTable filenames in creation order (oldest to newest).
* ``next_file_id`` is the next SSTable integer ID to assign (>= 1).
* ``last_flushed_seq`` is the highest WAL sequence number already persisted
  in an SSTable (>= 0). On startup, WAL records with ``seq <= last_flushed_seq``
  are skipped during replay.
"""
from __future__ import annotations

import json
import os
from typing import List, Union

from ._io import atomic_write_bytes
from .errors import CorruptFileError, StorageIOError

MANIFEST_FILENAME = "manifest.json"
_REQUIRED_FIELDS = frozenset({"live_files", "next_file_id", "last_flushed_seq"})


class Manifest:
    """In-memory representation of ``manifest.json``."""

    def __init__(
        self,
        path: str,
        live_files: OptionalList = None,
        next_file_id: int = 1,
        last_flushed_seq: int = 0,
    ) -> None:
        self.path = path
        self.filename = os.path.basename(path)
        self.live_files: List[str] = list(live_files) if live_files else []
        self.next_file_id: int = next_file_id
        self.last_flushed_seq: int = last_flushed_seq

    @classmethod
    def load_or_create(
        cls, dirpath: Union[str, "os.PathLike[str]"]
    ) -> "Manifest":
        """Load ``manifest.json`` if present, else return a fresh default state.

        A missing manifest means a brand-new database (no flushes yet); we do
        not write ``manifest.json`` until the first flush occurs so opening an
        empty directory stays lightweight.
        """
        directory = os.fspath(dirpath)
        path = os.path.join(directory, MANIFEST_FILENAME)
        filename = os.path.basename(path)

        def bad(reason: str) -> CorruptFileError:
            return CorruptFileError(filename, reason)

        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            return cls(path, live_files=[], next_file_id=1, last_flushed_seq=0)
        except OSError as exc:
            raise StorageIOError(f"cannot read manifest {filename!r}: {exc}") from exc

        try:
            obj = json.loads(raw.decode("utf-8"))
        except ValueError:
            raise bad("not valid UTF-8 JSON") from None

        if not isinstance(obj, dict) or set(obj) != _REQUIRED_FIELDS:
            raise bad(
                "manifest must be a JSON object with fields "
                "live_files, next_file_id, last_flushed_seq"
            )

        live_files = obj["live_files"]
        next_file_id = obj["next_file_id"]
        last_flushed_seq = obj["last_flushed_seq"]

        if not isinstance(live_files, list) or not all(
            isinstance(name, str) and name != "" and os.path.basename(name) == name
            for name in live_files
        ):
            raise bad("live_files must be a list of non-empty base filenames")
        if len(set(live_files)) != len(live_files):
            raise bad("live_files contains duplicate entries")
        if type(next_file_id) is not int or next_file_id < 1:
            raise bad("next_file_id must be a positive integer")
        if type(last_flushed_seq) is not int or last_flushed_seq < 0:
            raise bad("last_flushed_seq must be a non-negative integer")

        return cls(
            path=path,
            live_files=live_files,
            next_file_id=next_file_id,
            last_flushed_seq=last_flushed_seq,
        )

    def save(self, sync: bool = True) -> None:
        """Atomically persist the manifest state to disk."""
        payload = {
            "live_files": list(self.live_files),
            "next_file_id": self.next_file_id,
            "last_flushed_seq": self.last_flushed_seq,
        }
        raw = json.dumps(payload, ensure_ascii=True, indent=2).encode("ascii") + b"\n"
        atomic_write_bytes(self.path, raw, sync=sync)


OptionalList = List[str]
