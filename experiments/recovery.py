"""Recovery-time experiment (implementation.md V7.9 and Section 11).

    python -m experiments.recovery --lengths 10000 50000 100000 --intervals 0 1000 --runs 3

For each (log length L, snapshot interval): a fresh cluster, L committed transfers, then `--runs` trials of
"SIGKILL a follower, commit `--lag` more entries, restart it". Per trial:

* restore      = `STATE_RESTORED.ms` from the node's events (0 when there is no snapshot to restore)
* local replay = `LOCAL_REPLAY_DONE.ms`
* catch-up     = from `LOCAL_REPLAY_DONE` until the node's `last_applied` reaches the leader's `commit_index`
                 as sampled just before the restart (polled every 10 ms)
* total        = the three added up; process start-up (interpreter and imports) is not included

Writes `results/recovery/<timestamp>/recovery.csv`; plot with:

    python analysis/recovery.py results/recovery/<timestamp>/recovery.csv --out results/figures

Building 100 000 entries takes minutes per configuration: try `--lengths 2000 --intervals 0 500` first.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import tempfile
import time
from pathlib import Path

import grpc

from cluster.launcher import Cluster, ClusterError
from experiments.common import (
    Bench,
    Filler,
    add_cluster_args,
    bench_cluster,
    log,
    new_channel,
    raft_settings,
    results_dir,
    write_json,
)
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc

COLUMNS = [
    "run",
    "log_length",
    "snapshot_interval",
    "restore_ms",
    "local_replay_ms",
    "catchup_ms",
    "total_ms",
    "platform",
]


async def wait_applied(address: str, target: int, timeout: float) -> float:
    """Poll GetStatus every 10 ms until last_applied >= target; returns the time it was first seen."""
    deadline = time.monotonic() + timeout
    async with new_channel(address) as channel:
        stub = pb_grpc.LedgerStub(channel)
        while time.monotonic() < deadline:
            try:
                status = await stub.GetStatus(pb.GetStatusRequest(), timeout=1)
                if status.last_applied >= target:
                    return time.time()
            except grpc.aio.AioRpcError:
                pass
            await asyncio.sleep(0.01)
    raise ClusterError(f"{address} did not reach last_applied {target} within {timeout}s")


def phases(cluster: Cluster, victim: str, since: float) -> tuple[float, float, float]:
    """(restore_ms, local_replay_ms, local replay finished at) from the victim's events after `since`."""
    events = [e for e in cluster.collect_events() if e["node"] == victim and e["ts"] >= since]
    restore = next((e["ms"] for e in events if e["event"] == "STATE_RESTORED"), 0.0)
    replay = next((e for e in events if e["event"] == "LOCAL_REPLAY_DONE"), None)
    if replay is None:
        raise ClusterError(f"{victim} logged no LOCAL_REPLAY_DONE after restart")
    return float(restore), float(replay["ms"]), float(replay["ts"])


async def trial(bench: Bench, filler: Filler, lag: int, timeout: float) -> tuple[float, float, float]:
    cluster = bench.cluster
    leader = await cluster.wait_for_leader()
    victim = next(n for n in cluster.node_ids if n != leader)
    await cluster.wait_converged(timeout=timeout)
    cluster.kill(victim)
    await filler.run(lag)  # the cluster moves on without it
    target = (await cluster.statuses())[leader].commit_index
    restarted = time.time()
    await cluster.start(victim, wait=False)
    done = await wait_applied(cluster.address(victim), target, timeout)
    restore_ms, replay_ms, replay_ts = phases(cluster, victim, restarted)
    catchup_ms = max(0.0, (done - replay_ts) * 1000)
    return restore_ms, replay_ms, catchup_ms


def append_row(path: Path, row: dict) -> None:
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if new:
            writer.writeheader()
        writer.writerow(row)


async def amain(args: argparse.Namespace) -> None:
    out = results_dir(args, "recovery")
    write_json(out / "config.json", vars(args))
    csv_path = out / "recovery.csv"
    log(f"writing to {csv_path}")
    for length in args.lengths:
        for interval in args.intervals:
            log(f"L={length} snapshot_interval={interval}: building the log")
            raft = raft_settings(args, snapshot_interval=interval)
            with tempfile.TemporaryDirectory(prefix="ledgernet-recovery-") as work:
                async with bench_cluster(
                    work, n_nodes=args.nodes, accounts=args.accounts, raft=raft
                ) as bench:
                    filler = Filler(bench, args.concurrency, args.seed)
                    await filler.run(length)
                    for run_no in range(1, args.runs + 1):
                        restore, replay, catchup = await trial(bench, filler, args.lag, args.timeout)
                        row = {
                            "run": run_no,
                            "log_length": length,
                            "snapshot_interval": interval,
                            "restore_ms": f"{restore:.1f}",
                            "local_replay_ms": f"{replay:.1f}",
                            "catchup_ms": f"{catchup:.1f}",
                            "total_ms": f"{restore + replay + catchup:.1f}",
                            "platform": args.platform,
                        }
                        append_row(csv_path, row)
                        log(
                            f"  run {run_no}: restore {restore:.0f} + replay {replay:.0f} + "
                            f"catch-up {catchup:.0f} = {restore + replay + catchup:.0f} ms"
                        )
    log(f"\nanalyse with:  python analysis/recovery.py {csv_path} --out results/figures")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Recovery time of a returning follower")
    add_cluster_args(p)
    p.add_argument("--lengths", type=int, nargs="+", default=[10_000, 50_000, 100_000])
    p.add_argument(
        "--intervals", type=int, nargs="+", default=[0, 1000], help="snapshot_interval values (0=off)"
    )
    p.add_argument("--runs", type=int, default=3, help="kill/restart trials per configuration")
    p.add_argument("--lag", type=int, default=1000, help="entries committed while the follower is down")
    p.add_argument("--concurrency", type=int, default=32, help="parallel senders while building the log")
    p.add_argument("--timeout", type=float, default=300.0)
    asyncio.run(amain(p.parse_args(argv)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
