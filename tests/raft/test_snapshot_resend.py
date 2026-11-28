"""A follower that is down while the log is compacted past it must not cost the leader a snapshot reload per try.

Regression for the recovery experiment: with snapshots on, committing while a follower was down was several
times slower, because every replication attempt re-read and re-hashed the whole snapshot file and pushed it at
a node that was not there.
"""

import asyncio
from collections import Counter

from ledgernet.common.types import RaftConfig
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster
from ledgernet.testing.fakes import InMemorySnapshotStore


class CountingSnapshotStore(InMemorySnapshotStore):
    """Stands in for FileSnapshotStore, whose load_latest reads the file and checks its SHA-256 every call."""

    def __init__(self) -> None:
        super().__init__()
        self.loads = 0

    def load_latest(self):
        self.loads += 1
        return super().load_latest()


async def _commit_with_a_follower_down(seed: int):
    cluster = SimCluster(3, seed, RaftConfig(snapshot_interval=20))
    for node_id in cluster.node_ids:
        cluster.snapshot_stores[node_id] = CountingSnapshotStore()
    await cluster.start()
    cluster.start_monitor()
    leader = await cluster.wait_for_leader()
    victim = next(n for n in cluster.node_ids if n != leader.node_id)
    await cluster.propose_and_wait(b"warm-up")
    await cluster.wait_for_convergence()
    await cluster.crash(victim)

    rpcs = Counter()
    original_call = cluster.network.call

    async def counting_call(src, dst, method, request):
        if dst == victim:
            rpcs[method] += 1
        return await original_call(src, dst, method, request)

    cluster.network.call = counting_call
    loads_before = cluster.snapshot_stores[leader.node_id].loads

    async def client(number: int) -> None:
        for i in range(15):  # 8 clients, each waiting for its own commit, like the load generator
            await cluster.propose_and_wait(f"c{number}-{i}".encode())

    await asyncio.gather(*(client(n) for n in range(8)))
    loads_while_down = cluster.snapshot_stores[leader.node_id].loads - loads_before
    return cluster, victim, rpcs, loads_while_down


def test_the_leader_does_not_reload_the_snapshot_for_every_attempt():
    async def scenario():
        cluster, _, _, loads_while_down = await _commit_with_a_follower_down(seed=1)
        assert loads_while_down <= 1, f"snapshot reloaded {loads_while_down} times while a follower was down"
        await cluster.stop()

    run_simulated(scenario)


def test_a_down_follower_is_probed_cheaply_instead_of_being_sent_the_snapshot():
    async def scenario():
        cluster, victim, rpcs, _ = await _commit_with_a_follower_down(seed=2)
        assert rpcs["on_append_entries"] > 5  # the leader keeps checking whether it is back
        assert rpcs["on_install_snapshot"] <= 2, dict(rpcs)  # but does not push the snapshot at a dead node
        await cluster.restart(victim)
        await cluster.wait_for_convergence()
        assert cluster.nodes[victim].stats["snapshots_installed"] >= 1  # and it still catches up promptly
        cluster.check_invariants(full=True)
        await cluster.stop()

    run_simulated(scenario)


def test_a_follower_that_returns_gets_its_snapshot_without_waiting_for_a_backoff():
    async def scenario():
        cluster, victim, _, _ = await _commit_with_a_follower_down(seed=3)
        loop = asyncio.get_running_loop()
        await cluster.restart(victim)
        restarted_at = loop.time()
        await cluster.wait_for_convergence(timeout_s=5.0)
        assert loop.time() - restarted_at < 1.0  # about one heartbeat, not seconds
        await cluster.stop()

    run_simulated(scenario)
