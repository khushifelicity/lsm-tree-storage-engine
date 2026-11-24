"""Exception types for the LSM store.

All domain exceptions inherit from :class:`LSMError` so callers can either
catch specific errors or handle any storage-engine failure with one clause.
"""


class LSMError(Exception):
    """Base class for all LSM-Tree storage engine errors."""


class InvalidKeyError(LSMError):
    """A key is not a non-empty string within the documented size limit."""


class CorruptFileError(LSMError):
    """A WAL, SSTable or manifest file is damaged.

    ``filename`` is the base name of the offending file so callers (and
    tests) can tell *which* file is bad without parsing the message.
    """

    def __init__(self, filename: str, reason: str) -> None:
        self.filename = filename
        self.reason = reason
        super().__init__(f"{filename}: {reason}")


class DatabaseLockedError(LSMError):
    """Another LSMStore already has this directory open."""


class StorageIOError(LSMError):
    """An OS-level I/O operation failed (write, fsync, rename, ...)."""


class WorkloadFormatError(LSMError):
    """A workload file is malformed (defined now, used in the second half)."""
