"""Exception types for the LSM store.

Each exception derives directly from ``Exception`` to keep Step A minimal.
(A shared ``LSMError`` base class would let callers write one ``except``
clause for everything; that is a possible addition if you want it.)
"""


class InvalidKeyError(Exception):
    """A key is not a non-empty string within the documented size limit."""


class CorruptFileError(Exception):
    """A WAL, SSTable or manifest file is damaged.

    ``filename`` is the base name of the offending file so callers (and
    tests) can tell *which* file is bad without parsing the message.
    """

    def __init__(self, filename: str, reason: str) -> None:
        self.filename = filename
        self.reason = reason
        super().__init__(f"{filename}: {reason}")


class DatabaseLockedError(Exception):
    """Another LSMStore already has this directory open (used from Step B)."""


class StorageIOError(Exception):
    """An OS-level I/O operation failed (write, fsync, rename, ...)."""


class WorkloadFormatError(Exception):
    """A workload file is malformed (defined now, used in the second half)."""
