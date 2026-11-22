from __future__ import annotations

import csv

from bench.bench_wal import main


def test_bench_wal_runs_and_writes_csv(tmp_path):
    out = tmp_path / "bench.csv"
    assert main(["--dir", str(tmp_path / "w"), "--entries", "40", "--batches", "1,8", "--fsync-calls", "5",
                 "--csv", str(out)]) == 0  # fmt: skip
    rows = list(csv.DictReader(out.open()))
    assert [r["test"] for r in rows] == ["fsync", "append", "append"]
    assert float(rows[2]["entries_per_s"]) > 0
