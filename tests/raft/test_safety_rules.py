"""Direct tests of the Raft RPC handlers: each safety rule checked on a single node, no cluster needed."""

import pytest

from ledgernet.common.rpc_types import (
    AppendEntriesRequest,
    InstallSnapshotRequest,
    RequestVoteRequest,
)
from ledgernet.common.types import LogEntry, RaftConfig
from ledgernet.raft.node import RaftNode
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import AppliedHistory, SimStateMachine
from ledgernet.raft.sim_network import SimNetwork, SimTransport
from ledgernet.testing.fakes import InMemoryLogStore, InMemorySnapshotStore, RecordingStateMachine

PEERS = ["n1", "n2", "n3"]


def make_node(log=None, hard_state=(0, None), commit=0, snapshots=None, node_id="n1"):
    store = InMemoryLogStore()
    store.save_hard_state(*hard_state)
    if log:
        store.append(log)
    store.save_commit_index(commit)
    snapshots = snapshots or InMemorySnapshotStore()
    node = RaftNode(
        node_id,
        PEERS,
        SimTransport(node_id, SimNetwork(0)),
        store,
        snapshots,
        SimStateMachine(node_id, AppliedHistory()),
        RaftConfig(),
    )
    node._recover()
    return node, store, snapshots


def vote(term, candidate, last_index, last_term, pre_vote=False):
    return RequestVoteRequest(term, candidate, last_index, last_term, pre_vote)


LOG = [LogEntry(1, 1, b"a"), LogEntry(2, 2, b"b"), LogEntry(3, 2, b"c")]  # last = (index 3, term 2)


@pytest.mark.parametrize(
    "last_index, last_term, granted",
    [
        (9, 1, False),  # longer log but older last term: loses
        (2, 2, False),  # same last term, shorter log: loses
        (3, 2, True),  # identical log: wins
        (1, 3, True),  # newer last term beats a longer log
    ],
)
def test_vote_only_for_candidates_with_an_up_to_date_log(last_index, last_term, granted):
    async def scenario():
        node, _, _ = make_node(LOG, hard_state=(2, None))
        response = await node.on_request_vote(vote(3, "n2", last_index, last_term))
        assert response.vote_granted is granted

    run_simulated(scenario)


def test_vote_is_saved_to_disk_before_replying_and_survives_a_restart():
    async def scenario():
        node, store, snapshots = make_node(LOG, hard_state=(2, None))
        assert (await node.on_request_vote(vote(3, "n2", 3, 2))).vote_granted
        assert store.load_hard_state() == (3, "n2")  # on disk already
        # Crash and restart on the same disk: a different candidate in the same term must be refused.
        reborn = RaftNode(
            "n1",
            PEERS,
            SimTransport("n1", SimNetwork(0)),
            store,
            snapshots,
            RecordingStateMachine(),
            RaftConfig(),
        )
        reborn._recover()
        assert not (await reborn.on_request_vote(vote(3, "n3", 3, 2))).vote_granted
        assert (
            await reborn.on_request_vote(vote(3, "n2", 3, 2))
        ).vote_granted  # same candidate again is fine

    run_simulated(scenario)


def test_stale_term_votes_are_refused_and_higher_terms_are_adopted_durably():
    async def scenario():
        node, store, _ = make_node(LOG, hard_state=(5, "n3"))
        assert not (await node.on_request_vote(vote(4, "n2", 3, 2))).vote_granted
        response = await node.on_request_vote(vote(6, "n2", 3, 2))
        assert response.vote_granted and response.term == 6
        assert store.load_hard_state() == (6, "n2")

    run_simulated(scenario)


def test_prevote_never_changes_term_vote_or_role():
    async def scenario():
        node, store, _ = make_node(LOG, hard_state=(2, "n3"))
        response = await node.on_request_vote(vote(3, "n2", 3, 2, pre_vote=True))
        assert response.vote_granted
        assert store.load_hard_state() == (2, "n3") and node.status().term == 2

    run_simulated(scenario)


def test_prevote_is_refused_while_a_leader_is_still_heard():
    async def scenario():
        node, _, _ = make_node(LOG, hard_state=(2, None))
        await node.on_append_entries(AppendEntriesRequest(2, "n3", 3, 2, [], 0))  # heartbeat from the leader
        assert not (await node.on_request_vote(vote(3, "n2", 3, 2, pre_vote=True))).vote_granted

    run_simulated(scenario)


def test_append_entries_from_an_old_term_is_rejected():
    async def scenario():
        node, store, _ = make_node(LOG, hard_state=(5, None))
        response = await node.on_append_entries(
            AppendEntriesRequest(4, "n2", 3, 2, [LogEntry(4, 4, b"x")], 0)
        )
        assert not response.success and response.term == 5
        assert store.last_index() == 3

    run_simulated(scenario)


def test_append_entries_mismatch_reports_conflict_hint_for_fast_backup():
    async def scenario():
        node, _, _ = make_node(LOG, hard_state=(2, None))
        too_far = await node.on_append_entries(AppendEntriesRequest(3, "n2", 7, 3, [], 0))
        assert (too_far.success, too_far.conflict_index, too_far.conflict_term) == (False, 4, 0)
        wrong_term = await node.on_append_entries(AppendEntriesRequest(3, "n2", 3, 3, [], 0))
        # Our entries 2 and 3 are both term 2, so the hint points at the first of them.
        assert (wrong_term.success, wrong_term.conflict_index, wrong_term.conflict_term) == (False, 2, 2)

    run_simulated(scenario)


def test_conflicting_uncommitted_suffix_is_replaced_but_matching_entries_are_kept():
    async def scenario():
        node, store, _ = make_node(LOG, hard_state=(2, None))
        replacement = [LogEntry(3, 3, b"new3"), LogEntry(4, 3, b"new4")]
        response = await node.on_append_entries(AppendEntriesRequest(3, "n2", 2, 2, replacement, 0))
        assert response.success and response.match_index == 4
        assert [(e.index, e.term) for e in store.entries(1, 5)] == [(1, 1), (2, 2), (3, 3), (4, 3)]

    run_simulated(scenario)


def test_stale_duplicate_append_entries_never_deletes_newer_entries():
    async def scenario():
        node, store, _ = make_node(hard_state=(1, None))
        entries = [LogEntry(i, 1, f"e{i}".encode()) for i in range(1, 6)]
        await node.on_append_entries(AppendEntriesRequest(1, "n2", 0, 0, entries, 0))
        # A delayed copy of an earlier, shorter message arrives afterwards.
        response = await node.on_append_entries(AppendEntriesRequest(1, "n2", 0, 0, entries[:2], 0))
        assert response.success and store.last_index() == 5

    run_simulated(scenario)


def test_commit_index_follows_the_leader_but_never_beyond_what_was_verified():
    async def scenario():
        node, _, _ = make_node(hard_state=(1, None))
        entries = [LogEntry(i, 1, f"e{i}".encode()) for i in range(1, 4)]
        await node.on_append_entries(AppendEntriesRequest(1, "n2", 0, 0, entries, leader_commit=99))
        assert node.status().commit_index == 3  # capped at the last entry this message vouched for

    run_simulated(scenario)


def test_a_leader_can_never_make_us_overwrite_committed_entries():
    async def scenario():
        node, _, _ = make_node(LOG, hard_state=(2, None), commit=3)
        with pytest.raises(RuntimeError, match="committed"):
            await node.on_append_entries(AppendEntriesRequest(3, "n2", 2, 2, [LogEntry(3, 3, b"evil")], 0))

    run_simulated(scenario)


def test_install_snapshot_older_than_what_we_applied_is_ignored():
    async def scenario():
        node, _, snapshots = make_node(LOG, hard_state=(2, None), commit=3)
        response = await node.on_install_snapshot(InstallSnapshotRequest(2, "n2", 2, 2, b""))
        assert response.term == 2 and snapshots.load_latest() is None

    run_simulated(scenario)
