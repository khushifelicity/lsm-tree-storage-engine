"""Low-level crash-safe file and directory I/O helpers."""
from __future__ import annotations

import os
import uuid
from typing import Union

from .errors import StorageIOError

TMP_SUFFIX = ".tmp"


def fsync_dir(dirpath: str) -> None:
    """fsync a directory so a newly created/renamed entry survives a crash.

    Creating or renaming a file modifies the *directory*; without this, a power
    loss can leave the file's data durable but its name missing. POSIX only:
    Windows cannot open directories this way, so we skip it there.
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


def atomic_write_bytes(
    target_path: Union[str, "os.PathLike[str]"],
    data: bytes,
    sync: bool = True,
) -> None:
    """Atomically write ``data`` to ``target_path`` via a temporary file.

    Sequence:
    1. Write all bytes to ``<target>.<pid>.<uuid>.tmp`` in the same directory.
    2. Flush and (if ``sync=True``) ``os.fsync`` the temp file.
    3. Atomically replace ``target_path`` via ``os.replace``.
    4. If ``sync=True``, ``fsync_dir`` on the parent directory.

    If any step fails, the temporary file is removed on a best-effort basis
    and :class:`StorageIOError` is raised without touching ``target_path``.
    """
    target = os.fspath(target_path)
    dirpath = os.path.dirname(os.path.abspath(target))
    base = os.path.basename(target)
    tmp_path = os.path.join(
        dirpath, f".{base}.{os.getpid()}.{uuid.uuid4().hex}{TMP_SUFFIX}"
    )

    try:
        with open(tmp_path, "wb") as fh:
            view = memoryview(data)
            while view:
                n = fh.write(view)
                if not n:
                    raise OSError("short write made no progress")
                view = view[n:]
            fh.flush()
            if sync:
                os.fsync(fh.fileno())
        os.replace(tmp_path, target)
        if sync:
            fsync_dir(dirpath)
    except OSError as exc:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        raise StorageIOError(f"atomic write to {base!r} failed: {exc}") from exc


def cleanup_temp_files(dirpath: Union[str, "os.PathLike[str]"]) -> int:
    """Remove any stray ``*.tmp`` files left behind by a prior crash.

    Returns the count of deleted temporary files.
    """
    directory = os.fspath(dirpath)
    removed = 0
    try:
        entries = os.listdir(directory)
    except OSError as exc:
        raise StorageIOError(f"cannot list directory {directory!r}: {exc}") from exc

    for name in entries:
        if name.endswith(TMP_SUFFIX):
            full = os.path.join(directory, name)
            try:
                if os.path.isfile(full):
                    os.unlink(full)
                    removed += 1
            except OSError as exc:
                raise StorageIOError(
                    f"cannot remove temp file {name!r}: {exc}"
                ) from exc
    return removed
