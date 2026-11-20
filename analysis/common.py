"""Shared helpers for the analysis scripts: run discovery, CSV loading and one consistent figure style.

Colors follow a validated categorical order (blue, orange, aqua, ...), assigned in fixed order and never
cycled; magnitudes (offered load) use a single blue ramp, light to dark. Every figure also gets a CSV
table next to it, so no number is readable from color alone.
"""

from __future__ import annotations

import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
MUTED = "#b5b3ad"
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE_RAMP = [
    "#86b6ef",
    "#6da7ec",
    "#5598e7",
    "#3987e5",
    "#2a78d6",
    "#256abf",
    "#1c5cab",
    "#184f95",
    "#104281",
]
PLATFORM_COLOR = {"laptop": CATEGORICAL[0], "rce": CATEGORICAL[1]}

REQUESTS_COLUMNS = [
    "send_ts", "recv_ts", "latency_ms", "outcome", "tx_status", "attempts",
    "node_first", "node_final", "sender", "nonce", "index",
]  # fmt: skip
TIMELINE_COLUMNS = ["ts", "event", "node", "term", "detail"]
RECOVERY_COLUMNS = [
    "run", "log_length", "snapshot_interval", "restore_ms", "local_replay_ms", "catchup_ms", "total_ms", "platform",
]  # fmt: skip


def apply_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "axes.edgecolor": MUTED,
            "axes.labelcolor": INK_SECONDARY,
            "axes.titlecolor": INK,
            "axes.titlesize": 11,
            "axes.labelsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": INK_SECONDARY,
            "ytick.color": INK_SECONDARY,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.frameon": False,
            "legend.fontsize": 8,
            "lines.linewidth": 2,
            "font.size": 9,
        }
    )


def save_figure(fig, out_dir: str, name: str) -> list[str]:
    """Save as PDF and PNG; returns the paths written."""
    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for ext in ("pdf", "png"):
        path = os.path.join(out_dir, f"{name}.{ext}")
        fig.savefig(path, dpi=200, bbox_inches="tight")
        paths.append(path)
    plt.close(fig)
    return paths


def find_runs(roots: list[str], marker: str) -> list[str]:
    """Every directory under ``roots`` (inclusive) that contains a file named ``marker``."""
    found = []
    for root in roots:
        for dirpath, _, files in os.walk(root):
            if marker in files:
                found.append(dirpath)
    return sorted(found)


def load_meta(run_dir: str) -> dict:
    path = os.path.join(run_dir, "meta.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_requests(run_dir: str) -> pd.DataFrame:
    df = pd.read_csv(os.path.join(run_dir, "requests.csv"))
    missing = set(REQUESTS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{run_dir}/requests.csv is missing columns {sorted(missing)}")
    return df


def committed(df: pd.DataFrame) -> pd.DataFrame:
    """Requests the system finished (any ledger verdict), the population latency is measured on."""
    return df[df["outcome"] == "COMMITTED"]


def percentile(series: pd.Series, p: float) -> float:
    return float(series.quantile(p / 100.0)) if len(series) else float("nan")
