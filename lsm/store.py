"""LSMStore - Step B: memtable + WAL + SSTable flush + manifest + multi-file reads.

Write path (rule 1): validate -> append to WAL (flush, fsync if sync=True)
-> update memtable -> auto-flush if memtable reaches ``memtable_limit``.

Flush path (rule 2): write sorted SSTable (temp + fsync + replace) ->
update manifest (temp + fsync + replace) -> truncate WAL -> clear memtable.

Read path (rule 3): check memtable first, then SSTables from newest to
oldest (skipping files via ``min_key``/``max_key``). First match wins; a
tombstone (``None``) stops the search immediately.
"""
from __future__ import annotations

import os
from typing import BinaryIO, Dict, List, Optional, Union

from ._io import cleanup_temp_files
from .errors import DatabaseLockedError, InvalidKeyError, StorageIOError
from .manifest import Manifest
from .sstable import SST_MISSING, SSTable, sstable_filename
from .wal import OP_DELETE, OP_PUT, WAL_FILENAME, WriteAheadLog

if os.name == "nt":
    import msvcrt
else:
    import fcntl

#: Maximum key size, measured in UTF-8 bytes (not characters).
MAX_KEY_BYTES = 1024

#: Lock file name inside the database directory.
LOCK_FILENAME = "LOCK"

# Distinguishes "key not in layer" from "key is a tombstone (None)".
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


def _acquire_dir_lock(dirpath: str) -> BinaryIO:
    """Acquire an exclusive non-blocking lock on ``<dirpath>/LOCK``."""
    lock_path = os.path.join(dirpath, LOCK_FILENAME)
    try:
        fh = open(lock_path, "a+b")
    except OSError as exc:
        raise StorageIOError(f"cannot open lock file {lock_path!r}: {exc}") from exc

    try:
        if os.name == "nt":
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise DatabaseLockedError(
            f"database directory {dirpath!r} is locked by another process"
        ) from None
    return fh


def _release_dir_lock(fh: Optional[BinaryIO]) -> None:
    """Release and close the directory lock handle."""
    if fh is None:
        return
    try:
        if os.name == "nt":
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    finally:
        try:
            fh.close()
        except OSError:
            pass


class LSMStore:
    """Teaching-scale LSM-Tree key-value store (Step B)."""

    def __init__(
        self,
        path: Union[str, "os.PathLike[str]"],
        memtable_limit: int = 1000,
        sync: bool = True,
    ) -> None:
        if type(memtable_limit) is not int or memtable_limit < 1:
            raise ValueError("memtable_limit must be an int >= 1")
        if not isinstance(sync, bool):
            raise ValueError("sync must be a bool")
        self._dir = os.fspath(path)
        self._memtable_limit = memtable_limit
        self._sync = sync
        self._closed = False
        self._lock_fh: Optional[BinaryIO] = None
        self._wal: Optional[WriteAheadLog] = None
        self._memtable: Dict[str, Optional[str]] = {}
        self._memtable_max_seq: int = 0
        self._sstables: List[SSTable] = []  # oldest -> newest

        try:
            os.makedirs(self._dir, exist_ok=True)
        except OSError as exc:
            raise StorageIOError(f"cannot create {self._dir!r}: {exc}") from exc

        self._lock_fh = _acquire_dir_lock(self._dir)
        try:
            # 1. Remove any stray .tmp files left behind by a mid-flush crash.
            cleanup_temp_files(self._dir)

            # 2. Load manifest and validate every live SSTable on startup.
            self._manifest = Manifest.load_or_create(self._dir)
            for fname in self._manifest.live_files:
                sst = SSTable.load(os.path.join(self._dir, fname))
                self._sstables.append(sst)

            # 3. Replay WAL records newer than manifest.last_flushed_seq.
            self._wal, records = WriteAheadLog.open_and_replay(
                os.path.join(self._dir, WAL_FILENAME), sync=sync
            )
            max_seq = self._manifest.last_flushed_seq
            for rec in records:
                if rec.seq > max_seq:
                    max_seq = rec.seq
                if rec.seq > self._manifest.last_flushed_seq:
                    self._memtable[rec.key] = rec.value
                    self._memtable_max_seq = max(self._memtable_max_seq, rec.seq)

            # Sequence numbers must never be reused, even across restarts.
            self._next_seq = max_seq + 1
        except Exception:
            if self._wal is not None:
                try:
                    self._wal.close()
                except Exception:
                    pass
            _release_dir_lock(self._lock_fh)
            self._lock_fh = None
            raise

    # ------------------------------------------------------------------ API
    def put(self, key: str, value: str) -> None:
        """Insert or overwrite ``key``."""
        self._ensure_open()
        validate_key(key)
        validate_value(value)
        self._write(OP_PUT, key, value)

    def get(self, key: str) -> Optional[str]:
        """Return the newest value for ``key``, or None if missing or deleted."""
        self._ensure_open()
        validate_key(key)

        # 1. Check memtable first.
        found = self._memtable_lookup(key)
        if found is not _MISSING:
            return None if found is None else found  # type: ignore[return-value]

        # 2. Search SSTables from newest to oldest.
        for sst in reversed(self._sstables):
            res = sst.get(key)
            if res is not SST_MISSING:
                return None if res is None else res  # type: ignore[return-value]

        return None

    def delete(self, key: str) -> None:
        """Delete ``key`` by writing a tombstone. Succeeds if key is absent."""
        self._ensure_open()
        validate_key(key)
        self._write(OP_DELETE, key, None)

    def flush(self) -> None:
        """Flush the memtable to a new immutable SSTable.

        No-op if the memtable is currently empty.
        Durability order (Rule 2):
        1. Write sorted SSTable via temp file + fsync + atomic rename.
        2. Atomically update manifest with the new SSTable and watermark.
        3. Truncate the WAL only AFTER the manifest is durable.
        4. Clear the in-memory memtable.
        """
        self._ensure_open()
        if not self._memtable:
            return
        assert self._wal is not None

        file_id = self._manifest.next_file_id
        sst = SSTable.write(
            self._dir, file_id=file_id, items=self._memtable, sync=self._sync
        )

        self._manifest.live_files.append(sstable_filename(file_id))
        self._manifest.next_file_id = file_id + 1
        self._manifest.last_flushed_seq = max(
            self._manifest.last_flushed_seq, self._memtable_max_seq
        )
        self._manifest.save(sync=self._sync)

        # Safe to truncate WAL now that SSTable and manifest are on disk.
        self._wal.truncate()

        self._sstables.append(sst)
        self._memtable.clear()
        self._memtable_max_seq = 0

    def compact(self) -> None:
        """Second half of the project: merge SSTables. API placeholder."""
        raise NotImplementedError("compact() is planned for the second half")

    def close(self) -> None:
        """Close the WAL and release the directory lock. Idempotent."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._wal is not None:
                self._wal.close()
        finally:
            _release_dir_lock(self._lock_fh)
            self._lock_fh = None

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
        assert self._wal is not None
        seq = self._next_seq
        # DURABILITY ORDER: WAL first. If this raises StorageIOError we exit
        # here: memtable unchanged, seq not consumed, WAL rolled back.
        self._wal.append(seq, op, key, value)
        self._next_seq = seq + 1
        # Only now is the write acknowledged, so only now is it visible.
        self._memtable[key] = value
        self._memtable_max_seq = seq

        if len(self._memtable) >= self._memtable_limit:
            self.flush()
