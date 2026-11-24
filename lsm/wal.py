"""Write-ahead log (WAL).

File format: one JSON object per line (UTF-8, in practice pure ASCII because
we encode with ``ensure_ascii=True``)::

    {"seq":1,"op":"PUT","key":"a","value":"1"}
    {"seq":2,"op":"DELETE","key":"a","value":null}

Why the WAL exists: the memtable lives only in RAM. Before the store
acknowledges a write it appends the record here and (if ``sync=True``)
fsyncs, so after a crash the memtable can be rebuilt by replaying the log.

Recovery rules implemented in :func:`read_wal`:

* A final line with **no trailing newline** is a *partial* record (the
  process died mid-write). It was never acknowledged, so it is ignored.
* Any other malformed line is corruption -> ``CorruptFileError``.
"""
from __future__ import annotations

import errno
import json
import os
from typing import BinaryIO, List, NamedTuple, Optional, Tuple

from .errors import CorruptFileError, StorageIOError

WAL_FILENAME = "wal.log"
OP_PUT = "PUT"
OP_DELETE = "DELETE"
_VALID_OPS = (OP_PUT, OP_DELETE)
_FIELDS = frozenset({"seq", "op", "key", "value"})


class WalRecord(NamedTuple):
    """One decoded WAL entry. ``value`` is None for DELETE (a tombstone)."""

    seq: int
    op: str
    key: str
    value: Optional[str]


def fsync_dir(dirpath: str) -> None:
    """fsync a directory so a newly created/renamed entry survives a crash.

    Creating a file only modifies the *directory*; without this, a power
    loss can leave the file's data durable but its name missing. POSIX only:
    Windows cannot open directories this way, so we skip it there.
    (Step B will reuse this for SSTable and manifest renames.)
    """
    if os.name == "nt":
        return
    try:
        fd = os.open(dirpath, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise StorageIOError(f"cannot fsync directory {dirpath!r}: {exc}") from exc


def encode_record(seq: int, op: str, key: str, value: Optional[str]) -> bytes:
    """Serialize one record to a single newline-terminated line of bytes.

    ``ensure_ascii=True`` escapes every non-ASCII character (and any raw
    newline inside a key/value), so a record can never contain a literal
    ``\\n`` except the terminator. That property is what makes
    "split on newline" a safe way to find record boundaries.
    """
    text = json.dumps(
        {"seq": seq, "op": op, "key": key, "value": value},
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return text.encode("ascii") + b"\n"


def _parse_record(raw: bytes, filename: str, lineno: int, prev_seq: int) -> WalRecord:
    """Decode and strictly validate one complete WAL line."""

    def bad(reason: str) -> CorruptFileError:
        return CorruptFileError(filename, f"line {lineno}: {reason}")

    try:
        obj = json.loads(raw.decode("utf-8"))
    except ValueError:  # covers UnicodeDecodeError and JSONDecodeError
        raise bad("not valid UTF-8 JSON") from None
    if not isinstance(obj, dict) or set(obj) != _FIELDS:
        raise bad("record must be an object with exactly seq/op/key/value")

    seq, op, key, value = obj["seq"], obj["op"], obj["key"], obj["value"]
    # bool is a subclass of int in Python; reject it explicitly.
    if type(seq) is not int or seq < 1:
        raise bad("seq must be a positive integer")
    if seq <= prev_seq:
        raise bad(f"seq {seq} is not greater than previous seq {prev_seq}")
    if op not in _VALID_OPS:
        raise bad(f"unknown op {op!r}")
    if not isinstance(key, str) or key == "":
        raise bad("key must be a non-empty string")
    if op == OP_PUT and not isinstance(value, str):
        raise bad("PUT value must be a string")
    if op == OP_DELETE and value is not None:
        raise bad("DELETE value must be null")
    return WalRecord(seq, op, key, value)


def read_wal(path: str) -> Tuple[List[WalRecord], int]:
    """Read every complete record from the WAL at ``path``.

    Returns ``(records, valid_length)`` where ``valid_length`` is the byte
    offset just past the last complete record. If the file ends in a partial
    line, ``valid_length`` is smaller than the file size; the caller should
    truncate to ``valid_length`` before appending (see
    :meth:`WriteAheadLog.open_and_replay`). A missing file is an empty log.

    Raises CorruptFileError for corruption anywhere except a partial final
    line. Reads the whole file into memory: fine at teaching scale, and a
    streaming reader is an easy later upgrade.
    """
    filename = os.path.basename(path)
    try:
        with open(path, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return [], 0
    except OSError as exc:
        raise StorageIOError(f"cannot read WAL {filename!r}: {exc}") from exc

    segments = data.split(b"\n")
    # After split, the last element is whatever followed the final newline:
    # b"" for a cleanly terminated file, or a partial record otherwise.
    # We deliberately drop it WITHOUT parsing: even if it happens to be valid
    # JSON, a missing newline proves the append never completed, so the write
    # was never acknowledged.
    segments.pop()

    records: List[WalRecord] = []
    valid_length = 0
    prev_seq = 0
    for lineno, raw in enumerate(segments, start=1):
        rec = _parse_record(raw, filename, lineno, prev_seq)
        records.append(rec)
        prev_seq = rec.seq
        valid_length += len(raw) + 1  # +1 for the newline we split on
    return records, valid_length


class WriteAheadLog:
    """Append-only WAL writer. Create it with :meth:`open_and_replay`."""

    def __init__(self, path: str, f: BinaryIO, size: int, sync: bool) -> None:
        self._path = path
        self._f = f
        self._size = size  # bytes of valid, complete records in the file
        self._sync = sync
        self._broken = False
        self._closed = False

    @classmethod
    def open_and_replay(
        cls, path: str, sync: bool = True
    ) -> Tuple["WriteAheadLog", List[WalRecord]]:
        """Read the existing log, repair a partial tail, open for appending."""
        records, valid_length = read_wal(path)
        dirpath = os.path.dirname(os.path.abspath(path))
        try:
            existed = os.path.exists(path)
            if existed and os.path.getsize(path) > valid_length:
                # Chop off the partial final line. If we did not, the next
                # append would be glued onto the garbage and turn a harmless
                # partial tail into mid-file corruption on the NEXT recovery.
                with open(path, "r+b") as fh:
                    fh.truncate(valid_length)
                    fh.flush()
                    os.fsync(fh.fileno())
            # buffering=0 -> raw FileIO: no Python-level buffer that could
            # keep stale bytes after a failed write. "ab" = O_APPEND, so every
            # write lands at the current end of file.
            f = open(path, "ab", buffering=0)
        except OSError as exc:
            raise StorageIOError(f"cannot open WAL {path!r}: {exc}") from exc
        if not existed and sync:
            try:
                fsync_dir(dirpath)  # make the new file's name durable
            except StorageIOError:
                f.close()
                raise
        return cls(path, f, valid_length, sync), records

    @property
    def size(self) -> int:
        return self._size

    def append(self, seq: int, op: str, key: str, value: Optional[str]) -> None:
        """Durably append one record, or raise StorageIOError.

        On failure the file is rolled back to its previous length so no
        half-written record is left behind. If even the rollback fails the
        WAL is marked broken and refuses all further appends, because
        appending after garbage would corrupt the log.
        """
        if self._closed:
            raise ValueError("WAL is closed")
        if self._broken:
            raise StorageIOError("WAL is in a failed state; reopen the store")
        data = encode_record(seq, op, key, value)
        try:
            view = memoryview(data)
            while view:  # raw writes may be short; loop until all bytes are out
                n = self._f.write(view)
                if not n:
                    raise OSError(errno.EIO, "WAL write made no progress")
                view = view[n:]
            self._f.flush()  # no-op for raw files; kept to mirror the rule
            if self._sync:
                # DURABILITY: fsync forces the bytes to stable storage. Only
                # after this returns may the caller acknowledge the write.
                os.fsync(self._f.fileno())
        except OSError as exc:
            self._rollback()
            raise StorageIOError(f"WAL append failed: {exc}") from exc
        self._size += len(data)

    def _rollback(self) -> None:
        try:
            os.ftruncate(self._f.fileno(), self._size)
        except OSError:
            self._broken = True

    def close(self) -> None:
        """Close the file handle. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        try:
            self._f.close()
        except OSError as exc:
            raise StorageIOError(f"cannot close WAL: {exc}") from exc
