"""The LogStore contract (implementation.md 3.7), run against every implementation.

Parametrised over ``InMemoryLogStore`` (the fake P develops against) and ``FileLogStore`` (the real
WAL), so both are guaranteed to behave identically from Raft's point of view. ``reopen`` simulates a
process restart: the in-memory store keeps the same object (its "disk"), the file store is rebuilt
from its directory.
"""

from __future__ import annotations

import pytest

from ledgernet.common.types import LogEntry
from ledgernet.storage.file_log import FileLogStore
from ledgernet.testing.fakes import InMemoryLogStore


class _Memory:
    persistent = False

    def __init__(self, tmp_path):
        self.store = InMemoryLogStore()

    def reopen(self):
        return self.store


class _File:
    persistent = True

    def __init__(self, tmp_path):
        self.dir = str(tmp_path / "wal")
        self.store = FileLogStore(self.dir)

    def reopen(self):
        self.store.close()
        self.store = FileLogStore(self.dir)
        return self.store


@pytest.fixture(params=[_Memory, _File], ids=["memory", "file"])
def impl(request, tmp_path):
    holder = request.param(tmp_path)
    yield holder
    if isinstance(holder, _File):
        holder.store.close()


def _entries(start, end, term=1):
    return [LogEntry(i, term, f"cmd-{i}".encode()) for i in range(start, end)]


def test_empty_store(impl):
    s = impl.store
    assert s.load_hard_state() == (0, None)
    assert s.load_commit_index() == 0
    assert s.snapshot_index() == 0 and s.snapshot_term() == 0
    assert s.last_index() == 0
    assert s.term_at(0) == 0
    assert s.term_at(1) is None
    assert s.entries(1, 10) == []


def test_hard_state_round_trip(impl):
    impl.store.save_hard_state(3, "n2")
    assert impl.store.load_hard_state() == (3, "n2")
    impl.store.save_hard_state(4, None)
    assert impl.reopen().load_hard_state() == (4, None)


def test_commit_index_round_trip(impl):
    impl.store.save_commit_index(7)
    impl.store.save_commit_index(9)
    assert impl.store.load_commit_index() == 9
    assert impl.reopen().load_commit_index() == 9


def test_append_and_read(impl):
    s = impl.store
    s.append(_entries(1, 6, term=1))
    s.append(_entries(6, 8, term=2))
    assert s.last_index() == 7
    assert [s.term_at(i) for i in range(9)] == [0, 1, 1, 1, 1, 1, 2, 2, None]
    assert [e.index for e in s.entries(2, 5)] == [2, 3, 4]
    assert [e.index for e in s.entries(6, 100)] == [6, 7]  # clipped to last_index
    assert s.entries(8, 10) == []
    assert s.entries(3, 4)[0] == LogEntry(3, 1, b"cmd-3")


def test_append_empty_is_noop(impl):
    impl.store.append([])
    assert impl.store.last_index() == 0


def test_non_contiguous_append_rejected(impl):
    impl.store.append(_entries(1, 3))
    with pytest.raises(ValueError):
        impl.store.append([LogEntry(5, 1, b"gap")])
    with pytest.raises(ValueError):
        impl.store.append([LogEntry(2, 1, b"overlap")])


def test_truncate(impl):
    s = impl.store
    s.append(_entries(1, 6))
    s.truncate_from(4)
    assert s.last_index() == 3 and s.term_at(4) is None
    s.truncate_from(10)  # beyond the end: nothing happens
    assert s.last_index() == 3
    s.append(_entries(4, 6, term=2))
    assert [(e.index, e.term) for e in s.entries(1, 10)] == [(1, 1), (2, 1), (3, 1), (4, 2), (5, 2)]
    r = impl.reopen()
    assert [(e.index, e.term) for e in r.entries(1, 10)] == [(1, 1), (2, 1), (3, 1), (4, 2), (5, 2)]


def test_compact_keeps_suffix(impl):
    s = impl.store
    s.append(_entries(1, 6, term=1) + _entries(6, 11, term=2))
    s.compact_to(7, 2)
    assert s.snapshot_index() == 7 and s.snapshot_term() == 2
    assert s.last_index() == 10
    assert s.term_at(7) == 2 and s.term_at(6) is None and s.term_at(8) == 2
    assert [e.index for e in s.entries(8, 20)] == [8, 9, 10]
    with pytest.raises(IndexError):
        s.entries(7, 9)
    with pytest.raises(IndexError):
        s.entries(1, 9)
    r = impl.reopen()
    assert r.snapshot_index() == 7 and r.snapshot_term() == 2 and r.last_index() == 10
    assert [e.index for e in r.entries(8, 20)] == [8, 9, 10]


def test_compact_past_end_empties_log(impl):
    s = impl.store
    s.append(_entries(1, 4))
    s.compact_to(10, 5)  # InstallSnapshot beyond our log
    assert s.snapshot_index() == 10 and s.last_index() == 10 and s.term_at(10) == 5
    assert s.entries(11, 20) == []
    s.append([LogEntry(11, 5, b"next")])
    assert s.last_index() == 11
    r = impl.reopen()
    assert r.snapshot_index() == 10 and r.last_index() == 11 and r.term_at(11) == 5


def test_compact_is_noop_at_or_below_snapshot(impl):
    s = impl.store
    s.append(_entries(1, 6))
    s.compact_to(3, 1)
    s.compact_to(2, 1)
    s.compact_to(3, 1)
    assert s.snapshot_index() == 3 and s.last_index() == 5


def test_truncate_into_snapshot_rejected(impl):
    s = impl.store
    s.append(_entries(1, 6))
    s.compact_to(3, 1)
    with pytest.raises(ValueError):
        s.truncate_from(3)
    s.truncate_from(4)
    assert s.last_index() == 3


def test_append_after_compact_and_truncate_survives_reopen(impl):
    s = impl.store
    s.append(_entries(1, 21))
    s.compact_to(10, 1)
    s.truncate_from(15)
    s.append(_entries(15, 18, term=3))
    s.save_hard_state(3, "n1")
    s.save_commit_index(16)
    r = impl.reopen()
    assert r.snapshot_index() == 10 and r.last_index() == 17
    assert [(e.index, e.term) for e in r.entries(11, 100)] == [(i, 1) for i in range(11, 15)] + [
        (15, 3),
        (16, 3),
        (17, 3),
    ]
    assert r.load_hard_state() == (3, "n1") and r.load_commit_index() == 16


def test_raft_install_snapshot_sequence(impl):
    """The exact call sequence RaftNode uses when InstallSnapshot conflicts with the local log."""
    s = impl.store
    s.append(_entries(1, 9, term=1))
    s.compact_to(4, 1)
    # Leader's snapshot at (12, 3) does not match our term at 12 (we have no entry 12).
    assert s.term_at(12) != 3
    s.truncate_from(s.snapshot_index() + 1)
    s.compact_to(12, 3)
    assert s.snapshot_index() == 12 and s.last_index() == 12
    r = impl.reopen()
    assert r.snapshot_index() == 12 and r.last_index() == 12 and r.term_at(12) == 3
