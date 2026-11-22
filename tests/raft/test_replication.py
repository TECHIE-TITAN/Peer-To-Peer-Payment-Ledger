"""Replication, commit rule, divergent logs, and results delivered to waiting clients."""

import asyncio

import pytest
from conftest import seeds

from ledgernet.common.errors import LeadershipLostError, NotLeaderError
from ledgernet.common.tx import NOOP_COMMAND
from ledgernet.common.types import LogEntry, RaftConfig
from ledgernet.raft.node import LEADER, RaftNode
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster, SimStateMachine
from ledgernet.raft.sim_network import SimNetwork, SimTransport
from ledgernet.testing.fakes import InMemoryLogStore, InMemorySnapshotStore


async def _thousand_proposals(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    await cluster.wait_for_leader()
    commands = [f"cmd-{i}".encode() for i in range(1000)]
    for command in commands:
        await cluster.propose_and_wait(command)
    await cluster.wait_for_convergence()
    applied = [
        [command for _, _, command in node._sm.applied if command != NOOP_COMMAND] for node in cluster.live()
    ]
    # Every node applied the same 1000 commands (plus any no-ops) in the same order.
    reference = applied[0]
    assert all(commands == [c for c in a if c in set(commands)] for a in applied)
    assert all(a == reference for a in applied)
    assert len({node._sm.state_hash() for node in cluster.live()}) == 1
    cluster.check_invariants(full=True)
    await cluster.stop()


@pytest.mark.parametrize("seed", range(5))
def test_thousand_proposals_apply_identically_on_all_nodes(seed):
    run_simulated(_thousand_proposals, seed)


def test_propose_on_a_follower_raises_not_leader_with_hint():
    async def scenario():
        cluster = SimCluster(3, seed=7)
        await cluster.start()
        leader = await cluster.wait_for_leader()
        await cluster.propose_and_wait(b"warm-up")
        await cluster.wait_for_convergence()
        follower = next(n for n in cluster.live() if n is not leader)
        with pytest.raises(NotLeaderError) as excinfo:
            await follower.propose(b"nope")
        assert excinfo.value.leader_hint == leader.node_id
        await cluster.stop()

    run_simulated(scenario)


def test_wait_for_apply_returns_result_and_answers_late_callers_immediately():
    async def scenario():
        cluster = SimCluster(3, seed=8)
        await cluster.start()
        leader = await cluster.wait_for_leader()
        index, term = await leader.propose(b"hello")
        result = await leader.wait_for_apply(index, term, timeout=5.0)
        assert (result.index, result.term) == (index, term)
        again = await leader.wait_for_apply(index, term, timeout=0.0)  # already applied: no waiting
        assert again == result
        with pytest.raises(LeadershipLostError):  # same index, wrong term: that entry was overwritten
            await leader.wait_for_apply(index, term + 1, timeout=1.0)
        await cluster.stop()

    run_simulated(scenario)


def test_wait_for_apply_times_out_when_nothing_can_commit():
    async def scenario():
        cluster = SimCluster(3, seed=9)
        await cluster.start()
        leader = await cluster.wait_for_leader()
        await cluster.propose_and_wait(b"warm-up")
        for node_id in cluster.node_ids:
            if node_id != leader.node_id:
                await cluster.crash(node_id)
        index, term = await leader.propose(b"stuck")
        with pytest.raises(asyncio.TimeoutError):
            await leader.wait_for_apply(index, term, timeout=0.1)
        await cluster.stop()

    run_simulated(scenario)


async def _divergent_suffix(seed: int) -> None:
    """A cut-off leader accepts entries that can never commit; healing must overwrite them."""
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    old_leader = await cluster.wait_for_leader()
    await cluster.propose_and_wait(b"before")
    cluster.network.isolate(old_leader.node_id)
    lost = [await old_leader.propose(f"doomed-{i}".encode()) for i in range(10)]
    new_leader = await cluster.wait_for_leader(among=[n for n in cluster.node_ids if n != old_leader.node_id])
    for i in range(20):
        await cluster.propose_and_wait(f"kept-{i}".encode())
    cluster.network.heal()
    await cluster.wait_for_convergence()
    stale_index, stale_term = lost[0]
    assert cluster.log_stores[old_leader.node_id].term_at(stale_index) != stale_term
    assert not any(c.startswith(b"doomed") for _, _, c in new_leader._sm.applied)
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_divergent_uncommitted_suffix_is_overwritten(seed):
    run_simulated(_divergent_suffix, seed)


async def _lost_leadership_future(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    old_leader = await cluster.wait_for_leader()
    await cluster.propose_and_wait(b"before")
    cluster.network.isolate(old_leader.node_id)
    index, term = await old_leader.propose(b"doomed")
    waiter = asyncio.ensure_future(old_leader.wait_for_apply(index, term, timeout=20.0))
    await cluster.wait_for_leader(among=[n for n in cluster.node_ids if n != old_leader.node_id])
    for i in range(5):
        await cluster.propose_and_wait(f"kept-{i}".encode())
    cluster.network.heal()
    with pytest.raises(LeadershipLostError):  # the slot was filled by another term's entry
        await waiter
    await cluster.stop()


@seeds
def test_waiting_client_learns_its_entry_was_overwritten(seed):
    run_simulated(_lost_leadership_future, seed)


def _bare_leader(entries, term):
    """A RaftNode in the leader role with a hand-built log, for exercising the commit rule directly."""
    store = InMemoryLogStore()
    store.append(entries)
    node = RaftNode(
        "n1",
        ["n1", "n2", "n3", "n4", "n5"],
        SimTransport("n1", SimNetwork(0)),
        store,
        InMemorySnapshotStore(),
        SimStateMachine("n1", __import__("ledgernet.raft.sim_cluster", fromlist=["x"]).AppliedHistory()),
        RaftConfig(),
    )
    node._term, node._role = term, LEADER
    for peer in node._peers:
        node._match_index[peer] = 0
    return node, store


def test_figure_8_old_term_entry_is_not_committed_by_counting_replicas():
    """Raft paper Figure 8: an entry from an older term stored on a majority is still not committed."""
    node, store = _bare_leader([LogEntry(1, 1, b"a"), LogEntry(2, 2, b"b")], term=4)
    node._match_index.update({"n2": 2, "n3": 2, "n4": 0, "n5": 0})  # entry 2 (term 2) is on n1, n2, n3
    node._advance_commit()
    assert node._commit_index == 0, "must not commit a previous-term entry by replica count"

    store.append([LogEntry(3, 4, b"current-term")])  # leader appends an entry of its own term ...
    node._match_index.update({"n2": 3, "n3": 3})  # ... and a majority stores it
    node._advance_commit()
    assert node._commit_index == 3, "committing a current-term entry commits everything before it"
