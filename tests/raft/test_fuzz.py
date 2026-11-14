"""Randomised chaos: crashes, partitions, 20% loss, delay 0-50 ms, 10% duplication, invariants after every step."""

from conftest import fuzz_seeds, requires_fuzz, seeds

from ledgernet.common.types import RaftConfig
from ledgernet.raft.sim_chaos import run_chaos, settle
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster


async def _chaos(seed: int, snapshot_interval: int) -> None:
    cluster = SimCluster(5, seed, RaftConfig(snapshot_interval=snapshot_interval))
    await cluster.start()
    cluster.start_monitor()
    await run_chaos(cluster, duration_s=20.0, seed=seed)
    await settle(cluster)
    assert cluster.acked, f"seed {seed}: chaos run committed nothing at all"
    cluster.check_invariants(full=True)
    await cluster.stop()
    assert not cluster.violations, cluster.violations


@seeds
def test_chaos_with_snapshots(seed):
    run_simulated(_chaos, seed, 40)


@seeds
def test_chaos_without_snapshots(seed):
    run_simulated(_chaos, seed, 0)


@requires_fuzz
@fuzz_seeds
def test_long_fuzz_200_seeds(seed):
    run_simulated(_chaos, seed, 40 if seed % 2 else 0)
