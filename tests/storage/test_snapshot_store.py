from __future__ import annotations

import os

import pytest

from ledgernet.common.types import Snapshot
from ledgernet.storage.snapshot_store import HEADER_LEN, FileSnapshotStore, snapshot_file_name


def test_empty_store_has_no_snapshot(tmp_path):
    assert FileSnapshotStore(str(tmp_path)).load_latest() is None


def test_save_and_load_latest(tmp_path):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(10, 2, b"ten"))
    store.save(Snapshot(20, 3, b"twenty"))
    assert store.load_latest() == Snapshot(20, 3, b"twenty")
    assert FileSnapshotStore(str(tmp_path)).load_latest() == Snapshot(20, 3, b"twenty")


def test_keeps_only_newest(tmp_path):
    store = FileSnapshotStore(str(tmp_path), keep=2)
    for i in (5, 10, 15, 20):
        store.save(Snapshot(i, 1, b"x" * i))
    assert sorted(os.listdir(tmp_path)) == [snapshot_file_name(15), snapshot_file_name(20)]


def test_corrupt_newest_falls_back(tmp_path):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(10, 1, b"older"))
    store.save(Snapshot(20, 1, b"newer"))
    path = tmp_path / snapshot_file_name(20)
    blob = bytearray(path.read_bytes())
    blob[HEADER_LEN] ^= 0xFF  # corrupt the data: checksum fails
    path.write_bytes(bytes(blob))
    assert store.load_latest() == Snapshot(10, 1, b"older")


@pytest.mark.parametrize("cut", [0, 5, HEADER_LEN - 1, HEADER_LEN + 2])
def test_truncated_newest_falls_back(tmp_path, cut):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(10, 1, b"older"))
    store.save(Snapshot(20, 1, b"newer-data"))
    path = tmp_path / snapshot_file_name(20)
    path.write_bytes(path.read_bytes()[:cut])
    assert store.load_latest() == Snapshot(10, 1, b"older")


def test_leftover_tmp_is_ignored(tmp_path):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(10, 1, b"good"))
    (tmp_path / (snapshot_file_name(30) + ".tmp")).write_bytes(b"crashed mid-write")
    assert store.load_latest() == Snapshot(10, 1, b"good")
    store.save(Snapshot(11, 1, b"next"))  # the next save cleans the leftover up
    assert not any(n.endswith(".tmp") for n in os.listdir(tmp_path))


def test_header_index_must_match_file_name(tmp_path):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(10, 1, b"ten"))
    store.save(Snapshot(20, 1, b"twenty"))
    os.replace(tmp_path / snapshot_file_name(10), tmp_path / snapshot_file_name(30))
    assert store.load_latest() == Snapshot(20, 1, b"twenty")


def test_empty_data_round_trip(tmp_path):
    store = FileSnapshotStore(str(tmp_path))
    store.save(Snapshot(1, 1, b""))
    assert store.load_latest() == Snapshot(1, 1, b"")
