"""Commit-latency experiment (implementation.md V7.9 and Section 11).

    python -m experiments.latency --rates 50 100 200 400

For each offered load (open loop, Poisson arrivals): a fresh cluster, A's load generator, and
`requests.csv` + `meta.json` under `results/latency/<timestamp>/rate_<R>/`. Then:

    python analysis/latency.py results/latency/<timestamp> --out results/figures
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile

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


async def amain(args: argparse.Namespace) -> None:
    out = results_dir(args, "latency")
    write_json(out / "config.json", vars(args))
    log(f"writing to {out}")
    for rate in args.rates:
        run_dir = out / f"rate_{rate:g}"
        with tempfile.TemporaryDirectory(prefix="ledgernet-latency-") as work:
            async with bench_cluster(
                work, n_nodes=args.nodes, accounts=args.accounts, raft=raft_settings(args)
            ) as bench:
                summary = await run_load(
                    bench,
                    run_dir,
                    mode="open",
                    rate=float(rate),
                    duration_s=args.duration,
                    warmup_s=args.warmup,
                    seed=args.seed,
                    platform=args.platform,
                )
        update_meta(run_dir, experiment="latency", n_nodes=args.nodes)
        log(
            f"{rate:g} tx/s offered: {summary['throughput_tps']:.0f} tx/s committed, "
            f"p50 {summary['p50_ms']:.1f} ms, p95 {summary['p95_ms']:.1f} ms, p99 {summary['p99_ms']:.1f} ms"
        )
    log(f"\nanalyse with:  python analysis/latency.py {out} --out results/figures")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Commit latency against offered load")
    add_cluster_args(p)
    p.set_defaults(accounts=500)  # an open loop needs many idle senders
    p.add_argument("--rates", type=float, nargs="+", default=[50, 100, 200, 400], help="offered tx/s")
    p.add_argument("--warmup", type=float, default=5.0)
    p.add_argument("--duration", type=float, default=30.0, help="measured seconds after the warm-up")
    asyncio.run(amain(p.parse_args(argv)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
