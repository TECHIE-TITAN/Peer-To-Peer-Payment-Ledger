"""Leader election: one leader, re-election, no leader in a minority, PreVote."""

import asyncio

from conftest import seeds

from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster


async def _single_leader(size: int, seed: int) -> None:
    cluster = SimCluster(size, seed)
    await cluster.start()
    cluster.start_monitor()
    leader = await cluster.wait_for_leader()
    await asyncio.sleep(3.0)  # stays stable: still exactly one leader, same term
    status = leader.status()
    assert cluster.leaders_in_term(status.term) == [leader.node_id]
    assert cluster.leader() is leader
    assert all(n.status().leader_id == leader.node_id for n in cluster.live())
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_three_nodes_elect_one_stable_leader(seed):
    run_simulated(_single_leader, 3, seed)


@seeds
def test_five_nodes_elect_one_stable_leader(seed):
    run_simulated(_single_leader, 5, seed)


def test_single_node_cluster_elects_itself_and_commits():
    async def scenario():
        cluster = SimCluster(1, seed=1)
        await cluster.start()
        await cluster.wait_for_leader()
        result = await cluster.propose_and_wait(b"solo")
        assert result.index >= 2
        await cluster.stop()

    run_simulated(scenario)


async def _reelection(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    first = await cluster.wait_for_leader()
    old_term = first.status().term
    await cluster.crash(first.node_id)
    second = await cluster.wait_for_leader(timeout_s=10.0)
    assert second.node_id != first.node_id and second.status().term > old_term
    # The old leader comes back and must follow, not fight.
    await cluster.restart(first.node_id)
    await asyncio.sleep(3.0)
    assert cluster.leader() is second
    assert cluster.nodes[first.node_id].status().role == "follower"
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_new_leader_after_leader_crash(seed):
    run_simulated(_reelection, seed)


def test_no_leader_in_a_minority_of_two():
    async def scenario():
        cluster = SimCluster(5, seed=3)
        await cluster.start()
        cluster.start_monitor()
        leader = await cluster.wait_for_leader()
        survivors = [n for n in cluster.node_ids if n != leader.node_id][:1] + [leader.node_id]
        for node_id in cluster.node_ids:
            if node_id not in survivors:
                await cluster.crash(node_id)
        await asyncio.sleep(10.0)  # many election timeouts
        assert cluster.leader() is None, cluster.describe()
        cluster.check_invariants()
        await cluster.stop()

    run_simulated(scenario)


def test_prevote_keeps_an_isolated_node_from_inflating_its_term():
    async def scenario():
        cluster = SimCluster(5, seed=4)
        await cluster.start()
        cluster.start_monitor()
        leader = await cluster.wait_for_leader()
        term = leader.status().term
        lonely = next(n for n in cluster.node_ids if n != leader.node_id)
        cluster.network.isolate(lonely)
        await asyncio.sleep(10.0)  # it times out over and over, but never starts a real election
        assert cluster.nodes[lonely].status().term == term
        cluster.network.heal()
        await asyncio.sleep(3.0)
        assert cluster.leader() is leader and leader.status().term == term  # nobody was disrupted
        await cluster.propose_and_wait(b"after-heal")
        cluster.check_invariants(full=True)
        await cluster.stop()

    run_simulated(scenario)
