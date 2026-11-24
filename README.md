# lsm: a teaching-scale LSM-Tree key-value store

A from-scratch LSM-Tree key-value storage engine in pure Python 3 (standard library + pytest). It favors **clarity and correctness over speed** and is meant for learning how database storage engines work.

## Status

| Component | State |
|---|---|
| Memtable (put / get / delete, tombstones) | Done |
| Write-ahead log with crash replay | Done |
| SSTable flush, manifest, multi-file reads | Planned (Step B) |
| Startup cleanup, lock file | Planned (Step B) |
| Compaction, range scan, workload runner, analysis | Planned (second half) |

`flush()` and `compact()` currently raise `NotImplementedError`.

## How it works

```
 put/delete ──► WAL (append, flush, fsync) ──► memtable (dict)
                                                   │ (Step B) when full
                                                   ▼
                                       immutable sorted SSTable files
 get ──► memtable ──► SSTables, newest to oldest (Step B)
```

- **Memtable:** an in-memory dict. A deleted key is stored as a tombstone (`None`).
- **WAL (`wal.log`):** append-only, one JSON record per line:
  `{"seq": 1, "op": "PUT", "key": "a", "value": "1"}`. A write is acknowledged only after the record is appended (and fsynced when `sync=True`). The memtable is updated afterwards.
- **Recovery on open:** the WAL is replayed into the memtable. A partial final line (a crash mid-write) is ignored and trimmed. Corruption anywhere else raises `CorruptFileError`. Sequence numbers continue after the highest one seen.

## Usage

```python
from lsm import LSMStore

with LSMStore("./mydb", memtable_limit=1000, sync=True) as db:
    db.put("name", "ada")
    print(db.get("name"))    # "ada"
    db.delete("name")
    print(db.get("name"))    # None
```

| Method | Behaviour |
|---|---|
| `put(key, value)` | Insert or overwrite. `value` must be a `str`. |
| `get(key)` | Returns the value, or `None` if missing or deleted. |
| `delete(key)` | Writes a tombstone. Succeeds even if the key doesn't exist. |
| `flush()` | Memtable to SSTable (Step B). |
| `compact()` | Stub for the second half. |
| `close()` | Idempotent. Also called by the context manager. |

**Keys** must be non-empty strings of at most 1024 UTF-8 bytes, otherwise `InvalidKeyError` is raised.

**Errors** (`lsm/errors.py`): `InvalidKeyError`, `CorruptFileError` (carries `.filename`), `DatabaseLockedError`, `StorageIOError`, `WorkloadFormatError`.

## Durability notes

- `sync=True` fsyncs every write, which is safe but slow. `sync=False` is fast but can lose recent writes on power loss.
- If a WAL append fails, the file is rolled back and the memtable is left untouched. If the rollback itself fails, the WAL refuses further writes.
- If an fsync fails, the write's outcome is unknown. It may or may not survive a later crash.

## Project layout

```
lsm/
  __init__.py   public exports
  errors.py     exception types
  wal.py        write-ahead log
  store.py      LSMStore (memtable + WAL)
tests/
  test_memtable.py
  test_wal.py
requirements.txt
```

## Running the tests

```bash
pip install -r requirements.txt
python -m pytest -v
```

Use `python -m pytest` (not bare `pytest`) so the repo root is on `sys.path`.

## Roadmap

1. **Step B:** SSTables, manifest, flush, tombstone-aware reads across files, startup cleanup, lock file, randomized model test.
2. **Second half:** compaction, range scan, workload runner, pandas/matplotlib analysis, benchmarks.

## License

MIT. See `LICENSE`.
