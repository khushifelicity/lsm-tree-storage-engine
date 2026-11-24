"""Tests for immutable JSON SSTable serialization, lookup, and validation."""
from __future__ import annotations

import json
import pytest

from lsm.errors import CorruptFileError
from lsm.sstable import SST_MISSING, SSTable, sstable_filename


def test_write_and_load_sorted_sstable(tmp_path):
    items = {"zebra": "last", "alpha": "first", "middle": None}
    sst = SSTable.write(tmp_path, file_id=1, items=items, sync=False)

    assert sst.filename == sstable_filename(1)
    assert sst.min_key == "alpha"
    assert sst.max_key == "zebra"
    assert sst.entry_count == 3
    assert sst.entries == [("alpha", "first"), ("middle", None), ("zebra", "last")]

    reloaded = SSTable.load(sst.path, expected_file_id=1)
    assert reloaded.get("alpha") == "first"
    assert reloaded.get("middle") is None  # tombstone
    assert reloaded.get("zebra") == "last"
    assert reloaded.get("aaa") is SST_MISSING  # below min_key
    assert reloaded.get("zzz") is SST_MISSING  # above max_key
    assert reloaded.get("beta") is SST_MISSING  # inside range, absent


@pytest.mark.parametrize(
    "mutator",
    [
        lambda d: d.pop("entry_count"),
        lambda d: d.update({"extra": 123}),
        lambda d: d.update({"entry_count": 99}),
        lambda d: d.update({"min_key": "wrong"}),
        lambda d: d.update({"max_key": "wrong"}),
        lambda d: d.update({"entries": [["b", "2"], ["a", "1"]], "min_key": "b", "max_key": "a"}),
        lambda d: d.update({"entries": [["a", "1"], ["a", "2"]], "min_key": "a", "max_key": "a"}),
        lambda d: d.update({"entries": [["a", 123], ["b", "2"]]}),
    ],
)
def test_corrupt_sstable_raises_corrupt_file_error(tmp_path, mutator):
    sst = SSTable.write(tmp_path, file_id=2, items={"a": "1", "b": "2"}, sync=False)
    with open(sst.path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    mutator(payload)
    with open(sst.path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)

    with pytest.raises(CorruptFileError) as exc_info:
        SSTable.load(sst.path)
    assert exc_info.value.filename == sst.filename


def test_invalid_json_sstable_raises(tmp_path):
    bad_path = tmp_path / "sst_000001.json"
    bad_path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(CorruptFileError) as exc_info:
        SSTable.load(bad_path)
    assert exc_info.value.filename == "sst_000001.json"
