"""Memtable behaviour through the public API (put/get/delete/validation)."""
import pytest

from lsm import InvalidKeyError, LSMStore
from lsm.store import MAX_KEY_BYTES
from lsm.wal import WAL_FILENAME, read_wal


@pytest.fixture
def db(tmp_path):
    # sync=False keeps the suite fast; durability is covered in test_wal.py.
    with LSMStore(tmp_path / "db", sync=False) as store:
        yield store


def test_get_on_empty_db_returns_none(db):
    assert db.get("anything") is None


def test_put_then_get(db):
    db.put("name", "ada")
    assert db.get("name") == "ada"


def test_overwrite_returns_latest_value(db):
    db.put("k", "v1")
    db.put("k", "v2")
    assert db.get("k") == "v2"


def test_delete_existing_key(db):
    db.put("k", "v")
    db.delete("k")
    assert db.get("k") is None


def test_delete_missing_key_succeeds_and_writes_tombstone(db):
    db.delete("ghost")  # must not raise
    assert db.get("ghost") is None
    assert db._memtable["ghost"] is None  # tombstone is stored, not skipped


def test_put_after_delete_resurrects_key(db):
    db.put("k", "v1")
    db.delete("k")
    db.put("k", "v2")
    assert db.get("k") == "v2"


def test_empty_string_value_is_a_real_value(db):
    db.put("k", "")
    assert db.get("k") == ""


def test_unicode_keys_and_values(db):
    db.put("clé-🔑", "välue-✓")
    assert db.get("clé-🔑") == "välue-✓"


def test_key_at_size_limit_is_accepted(db):
    db.put("x" * MAX_KEY_BYTES, "ok")
    db.put("é" * (MAX_KEY_BYTES // 2), "ok")  # 2 bytes per char = exactly 1024
    assert db.get("x" * MAX_KEY_BYTES) == "ok"


@pytest.mark.parametrize("bad_key", [
    "", 123, None, b"bytes", ["k"],
    "x" * (MAX_KEY_BYTES + 1),
    "é" * (MAX_KEY_BYTES // 2 + 1),  # 1026 bytes though only 513 characters
    "\ud800",                         # lone surrogate cannot be encoded
])
def test_invalid_keys_rejected_everywhere(db, bad_key):
    with pytest.raises(InvalidKeyError):
        db.put(bad_key, "v")
    with pytest.raises(InvalidKeyError):
        db.get(bad_key)
    with pytest.raises(InvalidKeyError):
        db.delete(bad_key)


@pytest.mark.parametrize("bad_value", [None, 5, b"bytes", ["v"]])
def test_put_rejects_non_string_values(db, bad_value):
    with pytest.raises(TypeError):
        db.put("k", bad_value)
    assert db.get("k") is None


def test_rejected_writes_leave_no_wal_record(tmp_path):
    with LSMStore(tmp_path / "db", sync=False) as db:
        with pytest.raises(InvalidKeyError):
            db.put("", "v")
        with pytest.raises(TypeError):
            db.put("k", None)
    records, _ = read_wal(str(tmp_path / "db" / WAL_FILENAME))
    assert records == []


def test_operations_after_close_raise(tmp_path):
    db = LSMStore(tmp_path / "db", sync=False)
    db.close()
    db.close()  # idempotent
    for call in (lambda: db.put("k", "v"), lambda: db.get("k"),
                 lambda: db.delete("k")):
        with pytest.raises(ValueError):
            call()


def test_context_manager_closes_store(tmp_path):
    with LSMStore(tmp_path / "db", sync=False) as db:
        db.put("k", "v")
    with pytest.raises(ValueError):
        db.get("k")


@pytest.mark.parametrize("bad_limit", [0, -1, 1.5, "10", True])
def test_bad_memtable_limit_rejected(tmp_path, bad_limit):
    with pytest.raises(ValueError):
        LSMStore(tmp_path / "db", memtable_limit=bad_limit)


def test_compact_is_a_stub(db):
    with pytest.raises(NotImplementedError):
        db.compact()
