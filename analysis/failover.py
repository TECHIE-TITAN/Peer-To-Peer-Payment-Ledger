"""Leader failover: throughput over time and election / unavailability times (implementation.md 11).

    python analysis/failover.py results/failover --out results/figures

Each run directory holds ``requests.csv`` and ``timeline.csv``. Per run:

* election time   = first ``LEADER_ELECTED`` after ``LEADER_KILLED`` - ``LEADER_KILLED``
* unavailability  = first commit of a request *sent* after ``LEADER_KILLED`` - ``LEADER_KILLED``
  (responses to requests already in flight to the old leader can arrive just after the kill; counting
  them would report an outage of ~0 ms)

Election time is bounded below by the election timeout, so report the configured timeout next to it
(``meta.election_timeout_ms`` is printed when present).
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
    CATEGORICAL,
    INK_SECONDARY,
    MUTED,
    apply_style,
    committed,
    find_runs,
    load_meta,
    load_requests,
    save_figure,
)

BIN_S = 0.1


def analyse_run(run_dir: str) -> dict:
    timeline = pd.read_csv(os.path.join(run_dir, "timeline.csv"))
    kills = timeline[timeline["event"] == "LEADER_KILLED"]["ts"]
    if kills.empty:
        raise ValueError(f"{run_dir}: no LEADER_KILLED event")
    kill = float(kills.min())
    elected = timeline[(timeline["event"] == "LEADER_ELECTED") & (timeline["ts"] > kill)]["ts"]
    ok = committed(load_requests(run_dir))
    after = ok[ok["send_ts"] >= kill]["recv_ts"]
    return {
        "run": run_dir,
        "kill_ts": kill,
        "election_ms": (float(elected.min()) - kill) * 1000 if not elected.empty else float("nan"),
        "unavailability_ms": (float(after.min()) - kill) * 1000 if not after.empty else float("nan"),
    }


def throughput_series(run_dir: str, kill: float) -> tuple[np.ndarray, np.ndarray]:
    """Committed responses per second in 100 ms bins, time relative to the kill."""
    t = committed(load_requests(run_dir))["recv_ts"].to_numpy(dtype=float) - kill
    if len(t) == 0:
        return np.array([]), np.array([])
    edges = np.arange(np.floor(t.min() / BIN_S) * BIN_S, t.max() + BIN_S, BIN_S)
    counts, edges = np.histogram(t, bins=edges)
    return edges[:-1], counts / BIN_S


def plot_throughput(results: pd.DataFrame, out: str, highlight: int = 0) -> list[str]:
    fig, ax = plt.subplots(figsize=(9, 3.6))
    for i, row in results.iterrows():
        x, y = throughput_series(row["run"], row["kill_ts"])
        if i != highlight:
            ax.plot(x, y, color=MUTED, linewidth=1, alpha=0.6)
    hi = results.iloc[highlight]
    x, y = throughput_series(hi["run"], hi["kill_ts"])
    ax.plot(x, y, color=CATEGORICAL[0], label=f"{os.path.basename(hi['run'])} (other runs in gray)")
    ymax = ax.get_ylim()[1]
    # The two lines are usually < 1 s apart: label the kill on its left and the election on its right.
    marks = ((0.0, "leader killed ", "right"), (hi["election_ms"] / 1000, " new leader elected", "left"))
    for xpos, label, align in marks:
        if np.isfinite(xpos):
            ax.axvline(xpos, color=INK_SECONDARY, linewidth=1, linestyle="--")
            ax.text(xpos, ymax * 0.97, label, fontsize=7, color=INK_SECONDARY, va="top", ha=align)
    ax.set_title("Throughput around a leader failure (100 ms bins)")
    ax.set_xlabel("time since the leader was killed (s)")
    ax.set_ylabel("committed tx/s")
    ax.legend(loc="lower right")
    return save_figure(fig, out, "failover_throughput")


def plot_distribution(results: pd.DataFrame, out: str) -> list[str]:
    fig, ax = plt.subplots(figsize=(6, 3.2))
    cols = [("election_ms", "election time"), ("unavailability_ms", "unavailability")]
    for pos, (col, _label) in enumerate(cols):
        vals = results[col].dropna().to_numpy()
        jitter = np.linspace(-0.12, 0.12, num=len(vals)) if len(vals) > 1 else np.zeros(len(vals))
        ax.scatter(
            np.full(len(vals), pos) + jitter, vals, s=36, color=CATEGORICAL[pos], edgecolor="white", lw=1
        )
        if len(vals):
            med = float(np.median(vals))
            ax.hlines(med, pos - 0.25, pos + 0.25, color=INK_SECONDARY, linewidth=2)
            ax.text(pos + 0.28, med, f"median {med:.0f} ms", fontsize=7, color=INK_SECONDARY, va="center")
    ax.set_xticks(range(len(cols)), [label for _, label in cols])
    ax.set_xlim(-0.5, len(cols) - 0.2)
    ax.set_ylabel("ms after the leader was killed")
    ax.set_title(f"Failover across {len(results)} runs")
    return save_figure(fig, out, "failover_times")


def summary_table(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in ("election_ms", "unavailability_ms"):
        v = results[col].dropna()
        rows.append(
            {
                "metric": col,
                "runs": len(v),
                "mean_ms": v.mean(),
                "median_ms": v.median(),
                "p95_ms": v.quantile(0.95) if len(v) else float("nan"),
                "min_ms": v.min(),
                "max_ms": v.max(),
            }
        )
    return pd.DataFrame(rows)


def run(paths: list[str], out: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    apply_style()
    run_dirs = find_runs(paths, "timeline.csv")
    if not run_dirs:
        raise SystemExit(f"no timeline.csv found under {paths}")
    results = pd.DataFrame([analyse_run(d) for d in run_dirs])
    table = summary_table(results)
    os.makedirs(out, exist_ok=True)
    results.to_csv(os.path.join(out, "failover_runs.csv"), index=False, float_format="%.3f")
    table.to_csv(os.path.join(out, "failover_table.csv"), index=False, float_format="%.1f")
    plot_throughput(results, out)
    plot_distribution(results, out)
    return results, table


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Failover plots and table")
    p.add_argument("paths", nargs="+", help="run directories or roots to search")
    p.add_argument("--out", default="results/figures")
    args = p.parse_args(argv)
    results, table = run(args.paths, args.out)
    timeout = load_meta(results.iloc[0]["run"]).get("election_timeout_ms")
    if timeout:
        print(f"configured election timeout: {timeout} ms")
    print(table.to_string(index=False, float_format=lambda v: f"{v:.1f}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
