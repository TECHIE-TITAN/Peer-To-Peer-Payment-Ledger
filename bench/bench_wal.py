"""Write-ahead log micro-benchmark: raw fsync latency and FileLogStore append throughput (A6.5).

    python -m bench.bench_wal --dir /tmp/ledgernet-bench --entries 2000 --csv results/bench_wal.csv

Run it on the disk the nodes will use (on RCE: local disk, not the NFS home directory). Each append call
costs one fsync (group commit), so throughput grows with the batch size.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import statistics
import tempfile
import time

from ledgernet.common.tx import TRANSFER_LEN
from ledgernet.common.types import LogEntry
from ledgernet.storage.file_log import FileLogStore


def _percentile(sorted_values: list[float], p: float) -> float:
    return sorted_values[min(len(sorted_values) - 1, round(p / 100 * (len(sorted_values) - 1)))]


def bench_fsync(dir_path: str, n: int) -> dict:
    path = os.path.join(dir_path, "fsync.bin")
    samples = []
    with open(path, "ab") as f:
        for _ in range(n):
            f.write(b"x" * (TRANSFER_LEN + 25))
            f.flush()
            t0 = time.perf_counter()
            os.fsync(f.fileno())
            samples.append((time.perf_counter() - t0) * 1000.0)
    os.remove(path)
    samples.sort()
    return {
        "test": "fsync",
        "batch": 1,
        "calls": n,
        "p50_ms": _percentile(samples, 50),
        "p99_ms": _percentile(samples, 99),
        "mean_ms": statistics.fmean(samples),
        "entries_per_s": "",
    }


def bench_append(dir_path: str, entries: int, batch: int) -> dict:
    wal_dir = os.path.join(dir_path, f"wal-b{batch}")
    store = FileLogStore(wal_dir)
    command = b"\x01" + bytes(TRANSFER_LEN - 1)
    samples = []
    index = 1
    t_start = time.perf_counter()
    while index <= entries:
        chunk = [LogEntry(i, 1, command) for i in range(index, min(entries, index + batch - 1) + 1)]
        t0 = time.perf_counter()
        store.append(chunk)
        samples.append((time.perf_counter() - t0) * 1000.0)
        index += len(chunk)
    elapsed = time.perf_counter() - t_start
    store.close()
    shutil.rmtree(wal_dir, ignore_errors=True)
    samples.sort()
    return {
        "test": "append",
        "batch": batch,
        "calls": len(samples),
        "p50_ms": _percentile(samples, 50),
        "p99_ms": _percentile(samples, 99),
        "mean_ms": statistics.fmean(samples),
        "entries_per_s": entries / elapsed,
    }


def run(dir_path: str, entries: int, batches: list[int], fsync_calls: int) -> list[dict]:
    os.makedirs(dir_path, exist_ok=True)
    rows = [bench_fsync(dir_path, fsync_calls)]
    rows += [bench_append(dir_path, entries, b) for b in batches]
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="FileLogStore / fsync benchmark")
    p.add_argument("--dir", default=None, help="directory on the disk to test (default: a temp dir)")
    p.add_argument("--entries", type=int, default=2000)
    p.add_argument("--batches", default="1,16,256")
    p.add_argument("--fsync-calls", type=int, default=200)
    p.add_argument("--csv", default=None, help="also write the table to this CSV file")
    args = p.parse_args(argv)
    work = args.dir or tempfile.mkdtemp(prefix="ledgernet-bench-")
    rows = run(work, args.entries, [int(b) for b in args.batches.split(",")], args.fsync_calls)
    print(f"{'test':<8}{'batch':>7}{'calls':>8}{'p50 ms':>10}{'p99 ms':>10}{'mean ms':>10}{'entries/s':>12}")
    for r in rows:
        eps = f"{r['entries_per_s']:.0f}" if r["entries_per_s"] != "" else "-"
        print(
            f"{r['test']:<8}{r['batch']:>7}{r['calls']:>8}{r['p50_ms']:>10.3f}{r['p99_ms']:>10.3f}"
            f"{r['mean_ms']:>10.3f}{eps:>12}"
        )
    if args.csv:
        os.makedirs(os.path.dirname(os.path.abspath(args.csv)), exist_ok=True)
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
    if args.dir is None:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
