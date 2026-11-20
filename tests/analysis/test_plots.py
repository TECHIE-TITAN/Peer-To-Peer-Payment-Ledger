"""The plot scripts run end to end on synthetic data (task A6.8 acceptance)."""

from __future__ import annotations

import os

from analysis import failover, latency, make_synthetic, recovery


def test_all_plots_on_synthetic_data(tmp_path):
    data, figs = str(tmp_path / "synthetic"), str(tmp_path / "figures")
    make_synthetic.make_all(data, seed=3)

    lat = latency.run([os.path.join(data, "latency")], figs)
    assert set(lat["platform"]) == {"laptop", "rce"} and len(lat) == 10
    assert (lat["p50_ms"] <= lat["p95_ms"]).all() and (lat["p95_ms"] <= lat["p99_ms"]).all()

    runs, table = failover.run([os.path.join(data, "failover")], figs)
    assert len(runs) == 10
    election = table.set_index("metric").loc["election_ms"]
    assert 300 <= election["median_ms"] <= 650
    assert (runs["unavailability_ms"] >= runs["election_ms"]).all()

    rec = recovery.run([os.path.join(data, "recovery.csv")], figs)
    full = rec[(rec["snapshot_interval"] == 0) & (rec["platform"] == "laptop")].set_index("log_length")
    snap = rec[(rec["snapshot_interval"] == 1000) & (rec["platform"] == "laptop")].set_index("log_length")
    assert (snap["total_ms_mean"] < full["total_ms_mean"]).all()

    expected = [
        "latency_cdf", "latency_percentiles", "failover_throughput", "failover_times",
        "recovery_total", "recovery_phases",
    ]  # fmt: skip
    for name in expected:
        for ext in ("pdf", "png"):
            path = os.path.join(figs, f"{name}.{ext}")
            assert os.path.getsize(path) > 1000, path
    for table_name in ("latency_summary.csv", "failover_table.csv", "recovery_summary.csv"):
        assert os.path.exists(os.path.join(figs, table_name))
