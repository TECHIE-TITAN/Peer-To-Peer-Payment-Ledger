"""Synthetic data in the Section 11 schemas, so the plot scripts can be built before real runs exist.

    python analysis/make_synthetic.py --out results/synthetic

Writes:
    <out>/latency/<platform>/rate_<R>/requests.csv + meta.json
    <out>/failover/run_<k>/requests.csv + timeline.csv + meta.json
    <out>/recovery.csv

The numbers are plausible, not real: latency grows with load, the leader dies at t = 10 s and a new one is
elected after roughly one election timeout, recovery time grows with log length and snapshots cut it.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.common import RECOVERY_COLUMNS, REQUESTS_COLUMNS, TIMELINE_COLUMNS

T0 = 1_790_000_000.0  # a fixed Unix time, so the files are reproducible
NODES = ["n1", "n2", "n3", "n4", "n5"]


def _write_csv(path: str, columns: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)


def _row(send_ts, latency_ms, outcome="COMMITTED", status="APPLIED", node="n1", nonce=1, index=1, attempts=1):
    return {
        "send_ts": f"{send_ts:.6f}",
        "recv_ts": f"{send_ts + latency_ms / 1000:.6f}",
        "latency_ms": f"{latency_ms:.3f}",
        "outcome": outcome,
        "tx_status": status if outcome == "COMMITTED" else "",
        "attempts": attempts,
        "node_first": node,
        "node_final": node,
        "sender": f"{nonce % 97:016x}",
        "nonce": nonce,
        "index": index if outcome == "COMMITTED" else "",
    }


def make_latency(out: str, rng: random.Random, duration_s: float = 20.0) -> None:
    for platform, base_ms, capacity in (("laptop", 4.0, 900.0), ("rce", 2.5, 1400.0)):
        for rate in (50, 100, 200, 400, 800):
            rho = min(rate / capacity, 0.95)
            rows, t, index = [], T0, 0
            while t < T0 + duration_s:
                t += rng.expovariate(rate)
                index += 1
                latency = base_ms + rng.lognormvariate(0.0, 0.35) * base_ms * (1 + rho / (1 - rho))
                if rng.random() < 0.002:
                    latency += rng.uniform(50, 300)  # an occasional retry or slow fsync
                rows.append(_row(t, latency, node=NODES[0], nonce=index, index=index))
            run = os.path.join(out, "latency", platform, f"rate_{rate}")
            _write_csv(os.path.join(run, "requests.csv"), REQUESTS_COLUMNS, rows)
            meta = {"mode": "open", "offered_rate": rate, "duration_s": duration_s, "platform": platform}
            with open(os.path.join(run, "meta.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)


def make_failover(out: str, rng: random.Random, runs: int = 10, rate: float = 200.0) -> None:
    for k in range(runs):
        kill_at = T0 + 10.0
        election_s = rng.uniform(0.30, 0.65)  # election timeout 300-600 ms plus a vote round
        resume_s = election_s + rng.uniform(0.02, 0.15)
        rows, t, index = [], T0, 0
        while t < T0 + 30.0:
            t += rng.expovariate(rate)
            index += 1
            if kill_at - 0.005 <= t < kill_at + resume_s:
                # Stuck until the new leader commits, then answered (after a redirect).
                latency = (kill_at + resume_s - t) * 1000 + rng.uniform(3, 8)
                rows.append(_row(t, latency, node="n2", nonce=index, index=index, attempts=3))
            else:
                node = "n1" if t < kill_at else "n2"
                rows.append(_row(t, 4 + rng.lognormvariate(0, 0.3) * 3, node=node, nonce=index, index=index))
        timeline = [
            {"ts": f"{T0 + 0.5:.6f}", "event": "LEADER_ELECTED", "node": "n1", "term": 1, "detail": ""},
            {"ts": f"{kill_at:.6f}", "event": "LEADER_KILLED", "node": "n1", "term": 1, "detail": "SIGKILL"},
            {
                "ts": f"{kill_at + election_s:.6f}",
                "event": "LEADER_ELECTED",
                "node": "n2",
                "term": 2,
                "detail": "",
            },
            {"ts": f"{T0 + 20.0:.6f}", "event": "NODE_RESTARTED", "node": "n1", "term": 2, "detail": ""},
        ]
        run = os.path.join(out, "failover", f"run_{k:02d}")
        _write_csv(os.path.join(run, "requests.csv"), REQUESTS_COLUMNS, rows)
        _write_csv(os.path.join(run, "timeline.csv"), TIMELINE_COLUMNS, timeline)
        with open(os.path.join(run, "meta.json"), "w", encoding="utf-8") as f:
            json.dump({"offered_rate": rate, "platform": "laptop", "election_timeout_ms": [300, 600]}, f)


def make_recovery(out: str, rng: random.Random, runs: int = 5) -> None:
    rows = []
    for platform, speed in (("laptop", 1.0), ("rce", 0.7)):
        for log_length in (10_000, 50_000, 100_000):
            for interval in (0, 1000):
                for r in range(runs):
                    if interval == 0:
                        restore, replay = 0.0, log_length * 0.045 * speed
                    else:
                        restore = (40 + log_length * 0.0006) * speed
                        replay = rng.uniform(0, interval) * 0.045 * speed
                    catchup = rng.uniform(80, 250) * speed
                    jitter = rng.uniform(0.9, 1.1)
                    restore, replay = restore * jitter, replay * jitter
                    rows.append(
                        {
                            "run": r,
                            "log_length": log_length,
                            "snapshot_interval": interval,
                            "restore_ms": f"{restore:.1f}",
                            "local_replay_ms": f"{replay:.1f}",
                            "catchup_ms": f"{catchup:.1f}",
                            "total_ms": f"{restore + replay + catchup:.1f}",
                            "platform": platform,
                        }
                    )
    _write_csv(os.path.join(out, "recovery.csv"), RECOVERY_COLUMNS, rows)


def make_all(out: str, seed: int = 1) -> None:
    rng = random.Random(seed)
    make_latency(out, rng)
    make_failover(out, rng)
    make_recovery(out, rng)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Write synthetic requests/timeline/recovery CSVs")
    p.add_argument("--out", default="results/synthetic")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)
    make_all(args.out, args.seed)
    print(f"synthetic data written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
