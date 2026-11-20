"""Commit latency: CDFs and p50/p95/p99 against offered load, laptop vs RCE (implementation.md 11).

    python analysis/latency.py results/latency --out results/figures

Each run is a directory with ``requests.csv`` and (optionally) ``meta.json``. The offered load comes from
``meta.offered_rate`` (open loop); otherwise the achieved throughput is used and the axis says so.
Latency is measured on COMMITTED requests (any ledger verdict), client side, including redirects and
retries.
"""

from __future__ import annotations

import argparse
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.common import (
    BLUE_RAMP,
    INK_SECONDARY,
    PLATFORM_COLOR,
    apply_style,
    committed,
    find_runs,
    load_meta,
    load_requests,
    percentile,
    save_figure,
)


def summarize_runs(run_dirs: list[str]) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    """One row per run, plus each run's sorted latencies for the CDFs."""
    rows, latencies = [], {}
    for d in run_dirs:
        meta = load_meta(d)
        df = load_requests(d)
        ok = committed(df)
        lat = np.sort(ok["latency_ms"].to_numpy(dtype=float))
        span = ok["recv_ts"].max() - ok["send_ts"].min() if len(ok) else 0.0
        throughput = len(ok) / span if span > 0 else 0.0
        offered = meta.get("offered_rate")
        rows.append(
            {
                "run": d,
                "platform": meta.get("platform", "laptop"),
                "offered_rate": float(offered) if offered else float("nan"),
                "throughput_tps": throughput,
                "load": float(offered) if offered else throughput,
                "requests": len(df),
                "committed": len(ok),
                "p50_ms": percentile(ok["latency_ms"], 50),
                "p95_ms": percentile(ok["latency_ms"], 95),
                "p99_ms": percentile(ok["latency_ms"], 99),
            }
        )
        latencies[d] = lat
    table = pd.DataFrame(rows).sort_values(["platform", "load"]).reset_index(drop=True)
    return table, latencies


def plot_cdf(table: pd.DataFrame, latencies: dict[str, np.ndarray], out: str) -> list[str]:
    platforms = list(dict.fromkeys(table["platform"]))
    fig, axes = plt.subplots(
        1, len(platforms), figsize=(5.2 * len(platforms), 3.6), sharey=True, squeeze=False
    )
    for ax, platform in zip(axes[0], platforms):
        sub = table[table["platform"] == platform].sort_values("load")
        # Magnitude -> one blue ramp, light (low load) to dark (high load).
        steps = np.linspace(0, len(BLUE_RAMP) - 1, num=max(len(sub), 1)).round().astype(int)
        for (_, row), step in zip(sub.iterrows(), steps):
            lat = latencies[row["run"]]
            if len(lat) == 0:
                continue
            y = np.arange(1, len(lat) + 1) / len(lat)
            ax.plot(lat, y, color=BLUE_RAMP[step], label=f"{row['load']:.0f} tx/s")
        ax.set_xscale("log")
        ax.set_title(f"Commit latency CDF ({platform})")
        ax.set_xlabel("latency (ms, log scale)")
        ax.legend(title="offered load", loc="lower right", title_fontsize=8)
    axes[0][0].set_ylabel("fraction of committed requests")
    return save_figure(fig, out, "latency_cdf")


def plot_percentiles(table: pd.DataFrame, out: str) -> list[str]:
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6), sharey=True)
    xlabel = (
        "offered load (tx/s)"
        if table["offered_rate"].notna().all()
        else "load (tx/s; achieved if not offered)"
    )
    for ax, col, name in zip(axes, ["p50_ms", "p95_ms", "p99_ms"], ["p50", "p95", "p99"]):
        for platform, sub in table.groupby("platform", sort=False):
            sub = sub.sort_values("load")
            color = PLATFORM_COLOR.get(platform, INK_SECONDARY)
            ax.plot(sub["load"], sub[col], color=color, marker="o", markersize=5, label=platform)
            last = sub.iloc[-1]
            ax.annotate(
                f"{last[col]:.1f}", (last["load"], last[col]), textcoords="offset points", xytext=(4, 0),
                fontsize=7, color=INK_SECONDARY, va="center",
            )  # fmt: skip
        ax.set_title(f"{name} commit latency")
        ax.set_xlabel(xlabel)
        ax.legend(loc="upper left")
    axes[0].set_ylabel("latency (ms)")
    return save_figure(fig, out, "latency_percentiles")


def run(paths: list[str], out: str) -> pd.DataFrame:
    apply_style()
    run_dirs = find_runs(paths, "requests.csv")
    if not run_dirs:
        raise SystemExit(f"no requests.csv found under {paths}")
    table, latencies = summarize_runs(run_dirs)
    os.makedirs(out, exist_ok=True)
    table.to_csv(os.path.join(out, "latency_summary.csv"), index=False, float_format="%.3f")
    plot_cdf(table, latencies, out)
    plot_percentiles(table, out)
    return table


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Latency plots from requests.csv runs")
    p.add_argument("paths", nargs="+", help="run directories or roots to search")
    p.add_argument("--out", default="results/figures")
    args = p.parse_args(argv)
    table = run(args.paths, args.out)
    print(table[["platform", "load", "committed", "p50_ms", "p95_ms", "p99_ms"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
