"""Randomised fault injection for Raft tests (owned by P): crashes, partitions, loss, delay, duplication."""

from __future__ import annotations

import asyncio
import random

from ledgernet.raft.sim_cluster import SimCluster


async def run_chaos(
    cluster: SimCluster,
    duration_s: float,
    seed: int,
    max_down: int = 2,
    clients: int = 3,
) -> int:
    """Propose commands from several clients while randomly breaking the cluster.

    Returns how many commands were acknowledged. Afterwards the caller heals the cluster and checks
    convergence; safety invariants are checked throughout by the cluster's monitor.
    """
    rng = random.Random(seed)
    loop = asyncio.get_running_loop()
    end = loop.time() + duration_s
    acknowledged = 0

    async def client(client_id: int) -> None:
        nonlocal acknowledged
        counter = 0
        while loop.time() < end:
            counter += 1
            command = f"c{client_id}-{counter}".encode()
            try:
                await cluster.propose_and_wait(command, timeout_s=5.0)
                acknowledged += 1
            except AssertionError:
                pass  # no leader for a while during chaos is expected
            await asyncio.sleep(rng.uniform(0.0, 0.05))

    async def saboteur() -> None:
        while loop.time() < end:
            await asyncio.sleep(rng.uniform(0.2, 1.5))
            action = rng.choice(["crash", "restart", "partition", "heal", "lossy", "clear", "isolate"])
            down = [n for n in cluster.node_ids if cluster.nodes[n] is None]
            if action == "crash" and len(down) < max_down:
                await cluster.crash(rng.choice([n for n in cluster.node_ids if n not in down]))
            elif action == "restart" and down:
                await cluster.restart(rng.choice(down))
            elif action == "partition":
                ids = cluster.node_ids[:]
                rng.shuffle(ids)
                cut = rng.randint(1, len(ids) - 1)
                cluster.network.partition([set(ids[:cut]), set(ids[cut:])])
            elif action == "isolate":
                cluster.network.isolate(rng.choice(cluster.node_ids))
            elif action == "heal":
                cluster.network.heal()
            elif action == "lossy":
                cluster.network.set_drop(0.2)
                cluster.network.set_delay(0, 50)
                cluster.network.set_duplicate(0.1)
            elif action == "clear":
                cluster.network.clear_faults()

    workers = [asyncio.ensure_future(client(i)) for i in range(clients)]
    workers.append(asyncio.ensure_future(saboteur()))
    await asyncio.gather(*workers)
    return acknowledged


async def settle(cluster: SimCluster, timeout_s: float = 60.0) -> None:
    """Heal everything, restart every node, and wait until the cluster is committing and converged."""
    cluster.network.clear_faults()
    for node_id in cluster.node_ids:
        if cluster.nodes[node_id] is None:
            await cluster.restart(node_id)
    await cluster.wait_for_leader(timeout_s)
    await cluster.propose_and_wait(b"settle-marker", timeout_s)
    await cluster.wait_for_convergence(timeout_s)
