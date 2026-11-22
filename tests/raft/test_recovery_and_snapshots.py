"""Crash and restart, snapshot + replay, and InstallSnapshot for far-behind followers."""

from conftest import seeds

from ledgernet.common.tx import NOOP_COMMAND
from ledgernet.common.types import RaftConfig, Snapshot
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_cluster import SimCluster


def _commands(node):
    return [c for _, _, c in node._sm.applied if c != NOOP_COMMAND]


async def _restart_everything(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    await cluster.wait_for_leader()
    for i in range(50):
        await cluster.propose_and_wait(f"before-{i}".encode())
    await cluster.wait_for_convergence()
    digest_before = cluster.live()[0]._sm.state_hash()
    await cluster.crash_all()
    await cluster.restart_all()
    await cluster.wait_for_leader()
    for i in range(10):
        await cluster.propose_and_wait(f"after-{i}".encode())
    await cluster.wait_for_convergence()
    for node in cluster.live():  # nothing acknowledged was lost
        applied = set(_commands(node)) | {c for _, _, c in cluster.acked if c in set(_commands(node))}
        assert {f"before-{i}".encode() for i in range(50)} <= set(_commands(node)) | applied
    assert digest_before != cluster.live()[0]._sm.state_hash()  # and it kept moving forward
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_restart_of_all_five_nodes_loses_nothing(seed):
    run_simulated(_restart_everything, seed)


async def _repeated_crashes(seed: int) -> None:
    cluster = SimCluster(5, seed)
    await cluster.start()
    cluster.start_monitor()
    await cluster.wait_for_leader()
    for round_number in range(6):
        victims = cluster.network.rng.sample(cluster.node_ids, 2)
        for victim in victims:
            await cluster.crash(victim)
        for i in range(10):
            await cluster.propose_and_wait(f"r{round_number}-{i}".encode())
        for victim in victims:
            await cluster.restart(victim)
        await cluster.wait_for_leader()
    await cluster.wait_for_convergence()
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_two_nodes_crash_and_restart_repeatedly_while_proposing(seed):
    run_simulated(_repeated_crashes, seed)


async def _snapshot_then_restart(seed: int) -> None:
    config = RaftConfig(snapshot_interval=25)
    cluster = SimCluster(3, seed, config)
    await cluster.start()
    cluster.start_monitor()
    await cluster.wait_for_leader()
    for i in range(120):
        await cluster.propose_and_wait(f"x{i}".encode())
    await cluster.wait_for_convergence()
    assert all(n.stats["snapshots_taken"] >= 1 for n in cluster.live())
    assert all(s.snapshot_index() > 0 for s in cluster.log_stores.values())  # the log really was trimmed
    await cluster.crash_all()
    await cluster.restart_all()  # restore snapshot, replay the rest of the local log
    await cluster.wait_for_leader()
    await cluster.propose_and_wait(b"post-restart")
    await cluster.wait_for_convergence()
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_restart_recovers_from_snapshot_plus_log_replay(seed):
    run_simulated(_snapshot_then_restart, seed)


async def _lagging_follower_gets_a_snapshot(seed: int) -> None:
    config = RaftConfig(snapshot_interval=50)
    cluster = SimCluster(5, seed, config)
    await cluster.start()
    cluster.start_monitor()
    leader = await cluster.wait_for_leader()
    laggard = next(n for n in cluster.node_ids if n != leader.node_id)
    await cluster.crash(laggard)
    for i in range(300):  # far more than snapshot_interval while it is away
        await cluster.propose_and_wait(f"y{i}".encode())
    await cluster.restart(laggard)
    await cluster.wait_for_convergence()
    node = cluster.nodes[laggard]
    assert (
        node.stats["snapshots_installed"] >= 1
    ), "the leader had trimmed what it needed: expected InstallSnapshot"
    cluster.check_invariants(full=True)
    await cluster.stop()


@seeds
def test_follower_far_behind_recovers_through_install_snapshot(seed):
    run_simulated(_lagging_follower_gets_a_snapshot, seed)


def test_crash_between_saving_snapshot_and_trimming_log_is_repaired_on_restart():
    async def scenario():
        cluster = SimCluster(3, seed=11, config=RaftConfig(snapshot_interval=0))
        await cluster.start()
        await cluster.wait_for_leader()
        for i in range(30):
            await cluster.propose_and_wait(f"z{i}".encode())
        await cluster.wait_for_convergence()
        victim = cluster.live()[0].node_id
        snap = cluster.nodes[victim]._sm.take_snapshot()
        await cluster.crash(victim)
        cluster.snapshot_stores[victim].save(
            Snapshot(snap.last_included_index, snap.last_included_term, snap.data)
        )
        assert cluster.log_stores[victim].snapshot_index() == 0  # snapshot saved, log not yet trimmed
        await cluster.restart(victim)
        assert cluster.log_stores[victim].snapshot_index() == snap.last_included_index
        await cluster.wait_for_convergence()
        await cluster.stop()

    run_simulated(scenario)


def test_restart_replays_the_local_log_before_any_leader_contact():
    async def scenario():
        cluster = SimCluster(3, seed=13)
        await cluster.start()
        await cluster.wait_for_leader()
        for i in range(20):
            await cluster.propose_and_wait(f"local-{i}".encode())
        await cluster.wait_for_convergence()
        victim = cluster.live_ids()[0]
        committed = cluster.nodes[victim].status().commit_index
        await cluster.crash(victim)
        cluster.network.isolate(victim)  # it cannot hear from any leader
        await cluster.restart(victim)
        status = cluster.nodes[victim].status()
        assert status.commit_index == committed > 0
        assert status.last_applied == committed  # rebuilt from its own disk alone
        await cluster.stop()

    run_simulated(scenario)
