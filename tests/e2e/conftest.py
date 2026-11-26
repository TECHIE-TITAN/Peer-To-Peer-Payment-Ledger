"""Fixtures for the end-to-end scenarios: a factory that builds a real multi-process cluster."""

from __future__ import annotations

import pytest
from e2e_support import FAST_RAFT, ROOT, Wallet, write_genesis

from cluster.launcher import Cluster, write_local_config


@pytest.fixture
async def make_cluster(tmp_path):
    """`await make_cluster(balances, n=5)` -> a started Cluster.

    `balances` maps Wallet -> paise. Extra keyword arguments go to Cluster (e.g. unsafe_debug=True);
    `raft` overrides settings from FAST_RAFT; `start=False` leaves the nodes stopped.
    """
    made: list[Cluster] = []

    async def build(
        balances: dict[Wallet, int], n: int = 5, raft: dict | None = None, start: bool = True, **kwargs
    ) -> Cluster:
        genesis = write_genesis(tmp_path / "genesis.json", balances)
        config = write_local_config(str(tmp_path), genesis, n_nodes=n, raft={**FAST_RAFT, **(raft or {})})
        cluster = Cluster(config, cwd=ROOT, **kwargs)
        made.append(cluster)
        if start:
            await cluster.start_all()
        return cluster

    yield build
    for cluster in made:
        await cluster.close()
