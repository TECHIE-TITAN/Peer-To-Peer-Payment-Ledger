"""Recovery time of a returning node vs log length: full replay vs snapshot + replay (implementation.md 11).

    python analysis/recovery.py results/recovery/recovery.csv --out results/figures

``recovery.csv`` columns: run, log_length, snapshot_interval, restore_ms, local_replay_ms, catchup_ms,
total_ms, platform. ``snapshot_interval = 0`` means full replay.
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

from analysis.common import CATEGORICAL, INK_SECONDARY, RECOVERY_COLUMNS, SURFACE, apply_style, save_figure

PHASES = [("restore_ms", "restore snapshot"), ("local_replay_ms", "local replay"), ("catchup_ms", "catch-up")]


def _mode(interval: int) -> str:
    return "full replay" if interval == 0 else f"snapshot every {interval}"


def load_recovery(paths: list[str]) -> pd.DataFrame:
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    missing = set(RECOVERY_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"recovery.csv is missing columns {sorted(missing)}")
    return df


def summary_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c, _ in PHASES] + ["total_ms"]
    g = df.groupby(["platform", "snapshot_interval", "log_length"])
    table = g[cols].mean().add_suffix("_mean").join(g["total_ms"].median().rename("total_ms_median"))
    table = table.join(g.size().rename("runs"))
    return table.reset_index()


def plot_total(table: pd.DataFrame, out: str) -> list[str]:
    platforms = list(dict.fromkeys(table["platform"]))
    fig, axes = plt.subplots(
        1, len(platforms), figsize=(5.2 * len(platforms), 3.6), sharey=True, squeeze=False
    )
    intervals = sorted(table["snapshot_interval"].unique())
    for ax, platform in zip(axes[0], platforms):
        for slot, interval in enumerate(intervals):  # fixed order: full replay first
            sub = table[(table["platform"] == platform) & (table["snapshot_interval"] == interval)]
            sub = sub.sort_values("log_length")
            ax.plot(sub["log_length"], sub["total_ms_mean"], color=CATEGORICAL[slot], marker="o", markersize=5,
                    label=_mode(interval))  # fmt: skip
            last = sub.iloc[-1]
            ax.annotate(f"{last['total_ms_mean']:.0f} ms", (last["log_length"], last["total_ms_mean"]),
                        textcoords="offset points", xytext=(4, 0), fontsize=7, color=INK_SECONDARY,
                        va="center")  # fmt: skip
        ax.set_title(f"Recovery time vs log length ({platform})")
        ax.set_xlabel("log length (entries)")
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
        ax.legend(loc="upper left")
    axes[0][0].set_ylabel("mean total recovery (ms)")
    return save_figure(fig, out, "recovery_total")


def plot_phases(table: pd.DataFrame, out: str) -> list[str]:
    platforms = list(dict.fromkeys(table["platform"]))
    fig, axes = plt.subplots(1, len(platforms), figsize=(6 * len(platforms), 3.8), sharey=True, squeeze=False)
    for ax, platform in zip(axes[0], platforms):
        sub = table[table["platform"] == platform].sort_values(["log_length", "snapshot_interval"])
        labels = [
            f"{n // 1000}k\n{'full' if i == 0 else 'snap'}"
            for n, i in zip(sub["log_length"], sub["snapshot_interval"])
        ]
        x = np.arange(len(sub))
        bottom = np.zeros(len(sub))
        for slot, (col, name) in enumerate(PHASES):
            vals = sub[f"{col}_mean"].to_numpy()
            # 2px surface-colored edge = the gap between stacked segments.
            ax.bar(x, vals, bottom=bottom, width=0.7, color=CATEGORICAL[slot], edgecolor=SURFACE, linewidth=1.5,
                   label=name)  # fmt: skip
            bottom += vals
        for xi, total in zip(x, bottom):
            ax.text(xi, total, f"{total:.0f}", ha="center", va="bottom", fontsize=7, color=INK_SECONDARY)
        ax.set_xticks(x, labels)
        ax.set_title(f"Recovery phases ({platform})")
        ax.set_xlabel("log length / mode (full = full replay, snap = snapshot + replay)")
        ax.legend(loc="upper left")
    axes[0][0].set_ylabel("mean time (ms)")
    return save_figure(fig, out, "recovery_phases")


def run(paths: list[str], out: str) -> pd.DataFrame:
    apply_style()
    table = summary_table(load_recovery(paths))
    os.makedirs(out, exist_ok=True)
    table.to_csv(os.path.join(out, "recovery_summary.csv"), index=False, float_format="%.1f")
    plot_total(table, out)
    plot_phases(table, out)
    return table


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Recovery-time plots")
    p.add_argument("paths", nargs="+", help="recovery.csv file(s)")
    p.add_argument("--out", default="results/figures")
    args = p.parse_args(argv)
    table = run(args.paths, args.out)
    print(table.to_string(index=False, float_format=lambda v: f"{v:.1f}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
