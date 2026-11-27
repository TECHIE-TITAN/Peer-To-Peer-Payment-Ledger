"""Leader-failover experiment (implementation.md V7.9 and Section 11).

    python -m experiments.failover --runs 10

Each run: a fresh 5-node cluster, steady closed-loop load, SIGKILL the leader `--kill-at` seconds after the
load starts, poll GetStatus every 10 ms until another node reports `role=leader` in a higher term. Writes
`requests.csv`, `timeline.csv`, `meta.json` per run under `results/failover/<timestamp>/run_NN/`, the layout
`analysis/failover.py` reads:

    python analysis/failover.py results/failover/<timestamp>

timeline.csv has the polled `LEADER_ELECTED` (what the analysis uses). The nodes' own `LEADER_ELECTED` events
are copied in as `LEADER_ELECTED_NODE` for cross-checking; they are earlier by up to one poll.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import tempfile
import time
from pathlib import Path

from cluster.launcher import Cluster, ClusterError
from experiments.common import (
    add_cluster_args,
    bench_cluster,
    log,
    raft_settings,
    results_dir,
    run_load,
    update_meta,
    write_json,
)

POLL_S = 0.01
TIMELINE_COLUMNS = ["ts", "event", "node", "term", "detail"]


async def poll_new_leader(
    cluster: Cluster, dead: str, old_term: int, timeout: float
) -> tuple[float, str, int]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = await cluster.statuses()
        now = time.time()
        for node, status in statuses.items():
            if node != dead and status.role == "leader" and status.term > old_term:
                return now, node, status.term
        await asyncio.sleep(POLL_S)
    raise ClusterError(f"no new leader within {timeout}s of killing {dead}")


def write_timeline(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TIMELINE_COLUMNS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r["ts"]))


async def one_run(args: argparse.Namespace, run_dir: Path, run_no: int) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ledgernet-failover-") as work:
        async with bench_cluster(
            work, n_nodes=args.nodes, accounts=args.accounts, raft=raft_settings(args)
        ) as bench:
            cluster = bench.cluster
            load = asyncio.create_task(
                run_load(
                    bench,
                    run_dir,
                    mode="closed",
                    clients=args.clients,
                    duration_s=args.duration,
                    warmup_s=args.warmup,
                    seed=args.seed + run_no,
                    platform=args.platform,
                    backoff_initial_s=args.backoff_initial_ms / 1000,
                    backoff_max_s=args.backoff_max_ms / 1000,
                )
            )
            load_started = time.time()
            await asyncio.sleep(args.kill_at)
            victim = await cluster.wait_for_leader()
            old_term = (await cluster.statuses())[victim].term
            kill_ts = time.time()
            cluster.kill(victim)
            elected_ts, new_leader, new_term = await poll_new_leader(cluster, victim, old_term, timeout=30)
            summary = await load
            node_events = [
                e for e in cluster.collect_events() if e["event"] == "LEADER_ELECTED" and e["ts"] > kill_ts
            ]

    rows = [
        {"ts": f"{load_started:.6f}", "event": "LOAD_START", "node": "", "term": "", "detail": ""},
        {"ts": f"{kill_ts:.6f}", "event": "LEADER_KILLED", "node": victim, "term": old_term, "detail": ""},
        {
            "ts": f"{elected_ts:.6f}",
            "event": "LEADER_ELECTED",
            "node": new_leader,
            "term": new_term,
            "detail": "polled",
        },
    ]
    rows += [
        {
            "ts": f"{e['ts']:.6f}",
            "event": "LEADER_ELECTED_NODE",
            "node": e["node"],
            "term": e.get("term", ""),
            "detail": "node event",
        }
        for e in node_events
    ]
    write_timeline(run_dir / "timeline.csv", rows)
    update_meta(
        run_dir,
        experiment="failover",
        election_timeout_ms=f"{args.election_min_ms}-{args.election_max_ms}",
        heartbeat_interval_ms=args.heartbeat_ms,
        n_nodes=args.nodes,
        killed=victim,
        new_leader=new_leader,
        kill_at_s=args.kill_at,
        client_backoff_ms=f"{args.backoff_initial_ms:g}-{args.backoff_max_ms:g}",
    )
    return {
        "election_ms": (elected_ts - kill_ts) * 1000,
        "victim": victim,
        "new_leader": new_leader,
        **summary,
    }


async def amain(args: argparse.Namespace) -> None:
    out = results_dir(args, "failover")
    write_json(out / "config.json", vars(args))
    log(f"writing to {out}")
    for run_no in range(1, args.runs + 1):
        result = await one_run(args, out / f"run_{run_no:02d}", run_no)
        log(
            f"run {run_no}/{args.runs}: killed {result['victim']}, {result['new_leader']} elected after "
            f"{result['election_ms']:.0f} ms; {result['committed']} commits, p99 {result['p99_ms']:.0f} ms"
        )
    log(f"\nanalyse with:  python analysis/failover.py {out} --out results/figures")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Leader failover under load")
    add_cluster_args(p)
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--warmup", type=float, default=2.0)
    p.add_argument("--duration", type=float, default=18.0, help="measured seconds after the warm-up")
    p.add_argument(
        "--backoff-initial-ms",
        type=float,
        default=10.0,
        help="client retry backoff, first step (equal to the ceiling = a constant retry interval)",
    )
    p.add_argument("--backoff-max-ms", type=float, default=10.0, help="client retry backoff ceiling")
    p.add_argument("--kill-at", type=float, default=10.0, help="seconds after the load starts")
    asyncio.run(amain(p.parse_args(argv)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
