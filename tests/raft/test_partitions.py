"""Network partitions: minority leader steps down, majority keeps committing, everyone converges."""

import asyncio

import pytest
from conftest import seeds

from ledgernet.common.errors import NotLeaderError
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster


async def _leader_in_the_minority(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    old_leader = await cluster.wait_for_leader()
    await cluster.propose_and_wait(b"before")
    buddy = next(n for n in cluster.node_ids if n != old_leader.node_id)
    minority = {old_leader.node_id, buddy}
    majority = set(cluster.node_ids) - minority
    cluster.network.partition([minority, majority])

    # CheckQuorum: the minority leader gives up within about one election timeout and refuses writes.
    await cluster.wait_until(
        lambda: old_leader.status().role != "leader", 5.0, "minority leader to step down"
    )
    with pytest.raises(NotLeaderError):
        await old_leader.propose(b"rejected")

    # The majority keeps committing.
    new_leader = await cluster.wait_for_leader(among=list(majority))
    committed_before = new_leader.status().commit_index
    for i in range(20):
        await cluster.propose_and_wait(f"majority-{i}".encode())
    assert new_leader.status().commit_index > committed_before
    assert old_leader.status().commit_index < new_leader.status().commit_index  # minority committed nothing

    cluster.network.heal()
    await cluster.wait_for_convergence()
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_partition_with_leader_in_minority(seed):
    run_simulated(_leader_in_the_minority, seed)


async def _repeated_partitions(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    await cluster.wait_for_leader()
    rng = cluster.network.rng
    for round_number in range(8):
        ids = cluster.node_ids[:]
        rng.shuffle(ids)
        cluster.network.partition([set(ids[:2]), set(ids[2:])])
        await asyncio.sleep(rng.uniform(0.5, 3.0))
        cluster.network.heal()
        await cluster.propose_and_wait(f"round-{round_number}".encode())
    await cluster.wait_for_convergence()
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_repeated_random_partitions_converge(seed):
    run_simulated(_repeated_partitions, seed)


def test_no_side_of_a_two_way_split_without_a_majority_can_commit():
    async def scenario():
        cluster = SimCluster(5, seed=21)
        await cluster.start()
        cluster.start_monitor()
        await cluster.wait_for_leader()
        cluster.network.partition([{"n1", "n2"}, {"n3", "n4"}])  # n5 is cut off from everyone
        await asyncio.sleep(10.0)
        assert cluster.leader() is None, cluster.describe()
        cluster.network.heal()
        await cluster.wait_for_leader()
        await cluster.propose_and_wait(b"recovered")
        cluster.check_invariants(full=True)
        await cluster.stop()

    run_simulated(scenario)
