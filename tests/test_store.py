"""Tests for LSMStore Step B: flush, multi-file reads, lock, recovery, model."""
from __future__ import annotations

import json
import os
import random
import pytest

from lsm import (
    CorruptFileError,
    DatabaseLockedError,
    LSMError,
    LSMStore,
)


def test_explicit_flush_persists_across_restart_and_truncates_wal(tmp_path):
    with LSMStore(tmp_path, memtable_limit=100, sync=False) as db:
        db.put("k1", "v1")
        db.put("k2", "v2")
        db.flush()
        # WAL should be truncated after flush
        assert os.path.getsize(tmp_path / "wal.log") == 0
        assert (tmp_path / "sst_000001.json").exists()
        assert (tmp_path / "manifest.json").exists()

    with LSMStore(tmp_path, memtable_limit=100, sync=False) as db:
        assert db.get("k1") == "v1"
        assert db.get("k2") == "v2"
        assert db.get("missing") is None


def test_auto_flush_and_multi_file_read_precedence_with_tombstones(tmp_path):
    with LSMStore(tmp_path, memtable_limit=2, sync=False) as db:
        # Flush 1 -> sst_000001.json
        db.put("a", "1")
        db.put("b", "1")
        # Flush 2 -> sst_000002.json (overwrites 'a', deletes 'b')
        db.put("a", "2")
        db.delete("b")
        # Unflushed in memtable -> overwrites 'a' again
        db.put("a", "3")

        assert db.get("a") == "3"
        assert db.get("b") is None

    # Reopen and verify across memtable (replayed from WAL) + 2 SSTables
    with LSMStore(tmp_path, memtable_limit=2, sync=False) as db:
        assert db.get("a") == "3"
        assert db.get("b") is None

        # Flush memtable -> sst_000003.json, then delete 'a' and flush -> sst_000004.json
        db.flush()
        db.delete("a")
        db.put("c", "final")

    with LSMStore(tmp_path, memtable_limit=2, sync=False) as db:
        assert db.get("a") is None
        assert db.get("b") is None
        assert db.get("c") == "final"


def test_startup_cleans_up_stray_tmp_files(tmp_path):
    stray = tmp_path / ".sst_000001.json.999.deadbeef.tmp"
    stray.write_text("partial crash garbage", encoding="utf-8")

    with LSMStore(tmp_path, sync=False) as db:
        assert not stray.exists()
        db.put("ok", "yes")
        assert db.get("ok") == "yes"


def test_directory_lock_prevents_concurrent_open(tmp_path):
    db1 = LSMStore(tmp_path, sync=False)
    try:
        with pytest.raises(DatabaseLockedError) as exc_info:
            LSMStore(tmp_path, sync=False)
        assert isinstance(exc_info.value, LSMError)
    finally:
        db1.close()

    # Reopening after close succeeds
    with LSMStore(tmp_path, sync=False) as db2:
        db2.put("after_unlock", "1")
        assert db2.get("after_unlock") == "1"


def test_corrupt_manifest_raises_corrupt_file_error(tmp_path):
    with LSMStore(tmp_path, memtable_limit=1, sync=False) as db:
        db.put("k", "v")

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"bad": "schema"}), encoding="utf-8")

    with pytest.raises(CorruptFileError) as exc_info:
        LSMStore(tmp_path, sync=False)
    assert exc_info.value.filename == "manifest.json"


def test_randomized_model_against_plain_dict(tmp_path):
    """Randomized sequence of put/get/delete/flush/reopen matches a plain dict."""
    rng = random.Random(42)
    keys = [f"user:{i}" for i in range(25)]
    model: dict[str, str] = {}

    db = LSMStore(tmp_path, memtable_limit=5, sync=False)
    try:
        for step in range(400):
            op = rng.choices(
                ["put", "delete", "get", "flush", "reopen"],
                weights=[40, 20, 25, 10, 5],
            )[0]
            k = rng.choice(keys)

            if op == "put":
                val = f"val_{step}_{rng.randint(0, 999)}"
                db.put(k, val)
                model[k] = val
            elif op == "delete":
                db.delete(k)
                model.pop(k, None)
            elif op == "get":
                assert db.get(k) == model.get(k)
            elif op == "flush":
                db.flush()
            elif op == "reopen":
                db.close()
                db = LSMStore(tmp_path, memtable_limit=5, sync=False)

            # Spot-check a random key on every step
            probe = rng.choice(keys)
            assert db.get(probe) == model.get(probe)
    finally:
        db.close()
