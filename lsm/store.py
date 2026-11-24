"""LSMStore - Step A: memtable + WAL (no SSTables yet).

Write path (rule 1): validate -> append to WAL (flush, fsync if sync=True)
-> THEN update the memtable. If the WAL append raises, the memtable is never
touched and the sequence number is not consumed.
"""
from __future__ import annotations

import os
from typing import Dict, Optional, Union

from .errors import InvalidKeyError, StorageIOError
from .wal import OP_DELETE, OP_PUT, WAL_FILENAME, WriteAheadLog

#: Maximum key size, measured in UTF-8 bytes (not characters).
MAX_KEY_BYTES = 1024

# Distinguishes "key not in memtable" from "key is a tombstone (None)".
# Step A's get() maps both to None, but Step B's read path must keep
# them apart: a tombstone STOPS the search, a miss CONTINUES to SSTables.
_MISSING = object()


def validate_key(key: object) -> None:
    """Raise InvalidKeyError unless key is a non-empty str <= MAX_KEY_BYTES."""
    if not isinstance(key, str):
        raise InvalidKeyError(f"key must be a str, got {type(key).__name__}")
    if key == "":
        raise InvalidKeyError("key must not be empty")
    try:
        size = len(key.encode("utf-8"))
    except UnicodeEncodeError:
        raise InvalidKeyError("key is not valid Unicode text") from None
    if size > MAX_KEY_BYTES:
        raise InvalidKeyError(f"key is {size} bytes; limit is {MAX_KEY_BYTES}")


def validate_value(value: object) -> None:
    """Values passed to put() must be str. None is reserved for delete()."""
    if not isinstance(value, str):
        raise TypeError(
            f"value must be a str, got {type(value).__name__} "
            "(use delete() to remove a key)"
        )


class LSMStore:
    """Teaching-scale LSM-Tree key-value store (first half, Step A)."""

    def __init__(self, path: Union[str, "os.PathLike[str]"],
                 memtable_limit: int = 1000, sync: bool = True) -> None:
        if type(memtable_limit) is not int or memtable_limit < 1:
            raise ValueError("memtable_limit must be an int >= 1")
        if not isinstance(sync, bool):
            raise ValueError("sync must be a bool")
        self._dir = os.fspath(path)
        self._memtable_limit = memtable_limit  # used by flush in Step B
        self._sync = sync
        self._closed = False
        # Step B loads this from the manifest. Records at or below it are
        # already inside an SSTable and must NOT be replayed.
        self._last_flushed_seq = 0
        self._memtable: Dict[str, Optional[str]] = {}

        try:
            os.makedirs(self._dir, exist_ok=True)
        except OSError as exc:
            raise StorageIOError(f"cannot create {self._dir!r}: {exc}") from exc

        # Recovery: read the WAL (raises CorruptFileError on mid-file damage,
        # repairs a partial final line) and rebuild the memtable from it.
        self._wal, records = WriteAheadLog.open_and_replay(
            os.path.join(self._dir, WAL_FILENAME), sync=sync)
        max_seq = 0
        for rec in records:
            max_seq = max(max_seq, rec.seq)
            if rec.seq > self._last_flushed_seq:
                # Last record for a key wins; DELETE stores a tombstone (None).
                self._memtable[rec.key] = rec.value
        # Sequence numbers must never be reused, even across restarts.
        self._next_seq = max(self._last_flushed_seq, max_seq) + 1

    # ------------------------------------------------------------------ API
    def put(self, key: str, value: str) -> None:
        """Insert or overwrite ``key``."""
        self._ensure_open()
        validate_key(key)
        validate_value(value)
        self._write(OP_PUT, key, value)

    def get(self, key: str) -> Optional[str]:
        """Return the value for ``key``, or None if missing or deleted."""
        self._ensure_open()
        validate_key(key)
        found = self._memtable_lookup(key)
        return None if found is _MISSING else found  # type: ignore[return-value]

    def delete(self, key: str) -> None:
        """Delete ``key`` by writing a tombstone. Succeeds if key is absent."""
        self._ensure_open()
        validate_key(key)
        self._write(OP_DELETE, key, None)

    def flush(self) -> None:
        """Step B: write the memtable to an SSTable."""
        raise NotImplementedError("flush() is built in Step B")

    def compact(self) -> None:
        """Second half of the project: merge SSTables. API placeholder."""
        raise NotImplementedError("compact() is planned for the second half")

    def close(self) -> None:
        """Close the store. Idempotent. Unflushed data stays safe in the WAL."""
        if self._closed:
            return
        self._closed = True
        self._wal.close()

    def __enter__(self) -> "LSMStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------ internals
    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("operation on a closed LSMStore")

    def _memtable_lookup(self, key: str) -> object:
        """Return the value, None (tombstone) or the _MISSING sentinel."""
        return self._memtable.get(key, _MISSING)

    def _write(self, op: str, key: str, value: Optional[str]) -> None:
        seq = self._next_seq
        # DURABILITY ORDER: WAL first. If this raises StorageIOError we exit
        # here: memtable unchanged, seq not consumed, WAL rolled back.
        self._wal.append(seq, op, key, value)
        self._next_seq = seq + 1
        # Only now is the write acknowledged, so only now is it visible.
        self._memtable[key] = value
        # Step B: if len(self._memtable) >= self._memtable_limit: self.flush()
