"""The offline auditor passes clean directories and catches deliberate corruption (task A6.6)."""

from __future__ import annotations

import os
import sqlite3

import pytest

from ledgernet.common.types import Snapshot
from ledgernet.storage.file_log import WAL_NAME, encode_entry_record, scan_wal
from ledgernet.storage.snapshot_store import FileSnapshotStore
from tools.audit import audit_dirs, main
from tools.fake_cluster import build_fake_cluster

# 2300 entries with a snapshot every 1000: each node has a snapshot at 2000 and 300 entries in its log.
ENTRIES, INTERVAL = 2300, 1000


@pytest.fixture
def cluster(tmp_path):
    return build_fake_cluster(
        str(tmp_path / "cluster"), n_nodes=5, n_entries=ENTRIES, snapshot_interval=INTERVAL, lag={4: 120}
    )


def test_clean_cluster_passes(cluster):
    report = audit_dirs(cluster.node_dirs, cluster.genesis_path)
    assert report.ok, report.render()
    n1, n5 = report.nodes[0], report.nodes[4]
    assert n1.snapshot_index == 2000 and n1.last_applied == ENTRIES and n1.replayed == ENTRIES - 2000
    assert n5.last_applied == ENTRIES - 120
    assert len({n.state_hash for n in report.nodes[:4]}) == 1
    assert report.common_indexes > 100
    assert "RESULT: PASS" in report.render()


def test_full_replay_without_snapshots(tmp_path):
    fc = build_fake_cluster(str(tmp_path / "c"), n_nodes=3, n_entries=600, snapshot_interval=0)
    report = audit_dirs(fc.node_dirs, fc.genesis)
    assert report.ok, report.render()
    assert all(n.replayed == 600 and n.snapshot_index == 0 for n in report.nodes)


def test_cli_exit_codes(cluster, capsys):
    assert main(["--genesis", cluster.genesis_path, *cluster.node_dirs]) == 0
    assert "RESULT: PASS" in capsys.readouterr().out
    wal = os.path.join(cluster.node_dirs[1], "wal", WAL_NAME)
    _flip_byte(wal, os.path.getsize(wal) // 2)
    assert main(["--genesis", cluster.genesis_path, *cluster.node_dirs]) == 1
    assert "RESULT: FAIL" in capsys.readouterr().out


def _flip_byte(path, offset):
    with open(path, "r+b") as f:
        f.seek(offset)
        b = f.read(1)
        f.seek(offset)
        f.write(bytes([b[0] ^ 0xFF]))


def test_detects_corrupted_committed_record(cluster):
    wal = os.path.join(cluster.node_dirs[1], "wal", WAL_NAME)
    _flip_byte(wal, os.path.getsize(wal) // 2)
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    assert not report.ok
    assert any("beyond the end of the log" in e for e in report.nodes[1].errors)


def test_detects_rewritten_history_on_one_node(cluster):
    """A record re-written with a valid CRC: the node is self-consistent, the cross-check is not."""
    wal = os.path.join(cluster.node_dirs[2], "wal", WAL_NAME)
    with open(wal, "rb") as f:
        records, _ = scan_wal(f.read())
    entries = [value for kind, value in records]
    victim = entries[50]
    other = next(e for e in entries if e.command != victim.command)
    entries[50] = type(victim)(victim.index, victim.term, other.command)
    with open(wal, "wb") as f:
        f.write(b"".join(encode_entry_record(e) for e in entries))
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    assert report.nodes[2].ok
    assert not report.ok
    assert any(f"at index {victim.index}" in e for e in report.errors)


def _tamper_snapshot(node_dir, tmp_path, sql):
    store = FileSnapshotStore(os.path.join(node_dir, "snapshots"))
    snap = store.load_latest()
    db = str(tmp_path / "tamper.db")
    with open(db, "wb") as f:
        f.write(snap.data)
    conn = sqlite3.connect(db)
    conn.execute(sql)
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    with open(db, "rb") as f:
        store.save(Snapshot(snap.last_included_index, snap.last_included_term, f.read()))


def test_detects_broken_conservation(cluster, tmp_path):
    _tamper_snapshot(
        cluster.node_dirs[0], tmp_path, "UPDATE accounts SET balance = balance + 100 WHERE rowid = 1"
    )
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    assert not report.ok
    assert any("conservation broken" in e for e in report.nodes[0].errors)


def test_detects_corrupted_chain_head(cluster, tmp_path):
    _tamper_snapshot(
        cluster.node_dirs[3], tmp_path, "UPDATE meta SET v = zeroblob(32) WHERE k = 'chain_head'"
    )
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    assert not report.ok
    assert any("disagree at index 2000" in e for e in report.errors)


def test_corrupt_newest_snapshot_is_reported(cluster):
    snap_dir = os.path.join(cluster.node_dirs[0], "snapshots")
    newest = max(os.listdir(snap_dir))
    _flip_byte(os.path.join(snap_dir, newest), 100)
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    n1 = report.nodes[0]
    assert any("corrupt" in w for w in n1.warnings)
    assert any("no valid snapshot covers it" in e for e in n1.errors)


def test_torn_tail_beyond_commit_is_only_a_warning(cluster):
    wal = os.path.join(cluster.node_dirs[0], "wal", WAL_NAME)
    with open(wal, "ab") as f:
        f.write(b"\x00\x00\x00\x99partial")
    report = audit_dirs(cluster.node_dirs, cluster.genesis)
    assert report.ok, report.render()
    assert any("torn tail" in w for w in report.nodes[0].warnings)
    assert os.path.getsize(wal) > 0  # read-only: the auditor never repairs files


def test_missing_wal_directory(tmp_path, cluster):
    report = audit_dirs([str(tmp_path / "nope")], cluster.genesis)
    assert not report.ok
