"""Shared helpers for the Raft test suite."""

import os

import pytest

QUICK_SEEDS = int(os.environ.get("LEDGERNET_SEEDS", "20"))
FUZZ_ENABLED = os.environ.get("LEDGERNET_FUZZ") == "1"
FUZZ_SEEDS = int(os.environ.get("LEDGERNET_FUZZ_SEEDS", "200"))

seeds = pytest.mark.parametrize("seed", range(QUICK_SEEDS))
fuzz_seeds = pytest.mark.parametrize("seed", range(FUZZ_SEEDS))
requires_fuzz = pytest.mark.skipif(
    not FUZZ_ENABLED, reason="long fuzz run: set LEDGERNET_FUZZ=1 (make fuzz-raft)"
)
