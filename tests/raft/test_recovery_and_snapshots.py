"""Crash and restart, snapshot + replay, and InstallSnapshot for far-behind followers."""

from conftest import seeds

from ledgernet.common.tx import NOOP_COMMAND
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
