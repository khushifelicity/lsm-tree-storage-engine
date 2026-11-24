"""WAL file format, recovery rules, durability ordering and I/O failures."""
import errno
import json
import os

import pytest

from lsm import CorruptFileError, LSMStore, StorageIOError
from lsm.wal import WAL_FILENAME, WriteAheadLog, read_wal


def line(seq, op="PUT", key="k", value="v"):
    if op == "DELETE":
        value = None
    return json.dumps({"seq": seq, "op": op, "key": key, "value": value}) + "\n"


def wal_path(db_dir):
    return str(db_dir / WAL_FILENAME)


# ---------------------------------------------------------------- WAL unit
def test_append_and_read_roundtrip(tmp_path):
    path = str(tmp_path / WAL_FILENAME)
    wal, records = WriteAheadLog.open_and_replay(path, sync=False)
    assert records == []
    wal.append(1, "PUT", "a", "1")
    wal.append(2, "DELETE", "a", None)
    wal.append(3, "PUT", "né\nwline", "väl\n✓")  # newline/unicode stay on one line
    wal.close()
    records, valid = read_wal(path)
    assert [(r.seq, r.op, r.key, r.value) for r in records] == [
        (1, "PUT", "a", "1"), (2, "DELETE", "a", None),
        (3, "PUT", "né\nwline", "väl\n✓")]
    assert valid == os.path.getsize(path)


def test_missing_file_is_empty_log(tmp_path):
    assert read_wal(str(tmp_path / "nope.log")) == ([], 0)


def test_partial_final_line_is_ignored(tmp_path):
    path = tmp_path / WAL_FILENAME
    good = line(1) + line(2)
    path.write_bytes(good.encode() + b'{"seq":3,"op":"PU')
    records, valid = read_wal(str(path))
    assert [r.seq for r in records] == [1, 2]
    assert valid == len(good)


def test_final_line_without_newline_is_ignored_even_if_valid_json(tmp_path):
    path = tmp_path / WAL_FILENAME
    path.write_bytes((line(1) + line(2)).encode().rstrip(b"\n"))
    records, _ = read_wal(str(path))
    assert [r.seq for r in records] == [1]  # missing newline => never acknowledged


def test_partial_tail_is_truncated_so_later_appends_stay_clean(tmp_path):
    path = tmp_path / WAL_FILENAME
    path.write_bytes(line(1).encode() + b'{"seq":2,"op":"PU')
    wal, records = WriteAheadLog.open_and_replay(str(path), sync=False)
    assert [r.seq for r in records] == [1]
    wal.append(2, "PUT", "x", "y")
    wal.close()
    records, _ = read_wal(str(path))  # would raise if garbage were glued in
    assert [(r.seq, r.key) for r in records] == [(1, "k"), (2, "x")]


BAD_LINES = {
    "garbage": "this is not json\n",
    "blank_line": "\n",
    "json_array": "[1, 2, 3]\n",
    "missing_field": json.dumps({"seq": 2, "op": "PUT", "key": "k"}) + "\n",
    "extra_field": json.dumps(
        {"seq": 2, "op": "PUT", "key": "k", "value": "v", "x": 1}) + "\n",
    "unknown_op": line(2, op="UPSERT"),
    "put_with_null_value": line(2, value=None),
    "delete_with_value": json.dumps(
        {"seq": 2, "op": "DELETE", "key": "k", "value": "v"}) + "\n",
    "empty_key": line(2, key=""),
    "string_seq": line("2"),
    "float_seq": '{"seq":2.0,"op":"PUT","key":"k","value":"v"}\n',
    "bool_seq": '{"seq":true,"op":"PUT","key":"k","value":"v"}\n',
    "duplicate_seq": line(1),
}


@pytest.mark.parametrize("bad", list(BAD_LINES))
def test_mid_file_corruption_raises(tmp_path, bad):
    path = tmp_path / WAL_FILENAME
    path.write_bytes((line(1) + BAD_LINES[bad] + line(3)).encode())
    with pytest.raises(CorruptFileError) as excinfo:
        read_wal(str(path))
    assert excinfo.value.filename == WAL_FILENAME
    assert "line 2" in str(excinfo.value)


def test_corrupt_last_line_with_newline_is_not_a_partial_line(tmp_path):
    path = tmp_path / WAL_FILENAME
    path.write_bytes((line(1) + "garbage\n").encode())  # newline-terminated
    with pytest.raises(CorruptFileError):
        read_wal(str(path))


def test_invalid_utf8_raises(tmp_path):
    path = tmp_path / WAL_FILENAME
    path.write_bytes(line(1).encode() + b"\xff\xfe\n" + line(3).encode())
    with pytest.raises(CorruptFileError):
        read_wal(str(path))


# ------------------------------------------------------------ store replay
def test_write_close_reopen_data_present(tmp_path):
    with LSMStore(tmp_path / "db") as db:
        db.put("a", "1")
        db.put("b", "2")
        db.put("a", "3")
        db.delete("b")
    with LSMStore(tmp_path / "db") as db:
        assert db.get("a") == "3"
        assert db.get("b") is None
        assert db._memtable["b"] is None  # tombstone replayed, not dropped


def test_seq_numbers_continue_after_reopen(tmp_path):
    with LSMStore(tmp_path / "db", sync=False) as db:
        db.put("a", "1")
        db.put("b", "2")
    with LSMStore(tmp_path / "db", sync=False) as db:
        db.put("c", "3")
    records, _ = read_wal(wal_path(tmp_path / "db"))
    assert [r.seq for r in records] == [1, 2, 3]


def test_store_ignores_partial_final_line_and_keeps_working(tmp_path):
    with LSMStore(tmp_path / "db", sync=False) as db:
        db.put("a", "1")
        db.put("b", "2")
    with open(wal_path(tmp_path / "db"), "ab") as f:
        f.write(b'{"seq":3,"op":"PUT","key":"c","va')  # simulated crash
    with LSMStore(tmp_path / "db", sync=False) as db:
        assert db.get("a") == "1" and db.get("b") == "2"
        assert db.get("c") is None
        db.put("d", "4")
    with LSMStore(tmp_path / "db", sync=False) as db:  # second recovery is clean
        assert db.get("d") == "4"
        assert db.get("c") is None


def test_store_open_raises_on_mid_file_corruption(tmp_path):
    with LSMStore(tmp_path / "db", sync=False) as db:
        db.put("a", "1")
        db.put("b", "2")
        db.put("c", "3")
    raw = open(wal_path(tmp_path / "db"), "rb").read().split(b"\n")
    raw[1] = b'{"seq":2,"op":"PUT","key":"b","valu'  # damage the MIDDLE record
    with open(wal_path(tmp_path / "db"), "wb") as f:
        f.write(b"\n".join(raw))
    with pytest.raises(CorruptFileError) as excinfo:
        LSMStore(tmp_path / "db", sync=False)
    assert excinfo.value.filename == WAL_FILENAME


# ------------------------------------------------- durability & I/O errors
def test_sync_true_fsyncs_each_write_and_sync_false_does_not(tmp_path, monkeypatch):
    calls = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), real_fsync(fd)))
    with LSMStore(tmp_path / "synced", sync=True) as db:
        calls.clear()  # ignore fsyncs done while opening
        db.put("a", "1")
        db.delete("a")
        assert len(calls) == 2
    with LSMStore(tmp_path / "unsynced", sync=False) as db:
        calls.clear()
        db.put("a", "1")
        assert calls == []


def test_failed_wal_append_leaves_memtable_and_wal_consistent(tmp_path, monkeypatch):
    db = LSMStore(tmp_path / "db", sync=True)
    db.put("a", "1")

    def failing_fsync(fd):
        raise OSError(errno.EIO, "injected fsync failure")

    monkeypatch.setattr(os, "fsync", failing_fsync)
    with pytest.raises(StorageIOError):
        db.put("b", "2")
    monkeypatch.undo()

    assert db.get("b") is None          # memtable NOT updated
    assert db.get("a") == "1"
    records, _ = read_wal(wal_path(tmp_path / "db"))
    assert [r.key for r in records] == ["a"]   # WAL rolled back

    db.put("c", "3")                    # store is still usable
    db.close()
    records, _ = read_wal(wal_path(tmp_path / "db"))
    assert [(r.seq, r.key) for r in records] == [(1, "a"), (2, "c")]  # seq reused

    with LSMStore(tmp_path / "db") as db:
        assert (db.get("a"), db.get("b"), db.get("c")) == ("1", None, "3")


def test_wal_that_cannot_roll_back_refuses_further_writes(tmp_path, monkeypatch):
    db = LSMStore(tmp_path / "db", sync=True)
    db.put("a", "1")

    def boom(*args):
        raise OSError(errno.EIO, "injected")

    monkeypatch.setattr(os, "fsync", boom)
    monkeypatch.setattr(os, "ftruncate", boom)
    with pytest.raises(StorageIOError):
        db.put("b", "2")
    monkeypatch.undo()

    with pytest.raises(StorageIOError):   # poisoned: would append after garbage
        db.put("c", "3")
    assert db.get("b") is None and db.get("c") is None
    db.close()

    with LSMStore(tmp_path / "db") as db:  # recovery still works
        assert db.get("a") == "1"
        # "b" was fully written but its fsync failed, so its outcome is
        # genuinely unknown: either answer is legal, corruption is not.
        assert db.get("b") in (None, "2")
        assert db.get("c") is None
