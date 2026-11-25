"""Shared helpers for the end-to-end scenarios (V7.8): wallets, genesis, and the common finish line.

Every scenario ends with `finish()`: wait for convergence, check that all nodes agree and conserve money, stop
the nodes gracefully, and run A's offline auditor over their data directories.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cluster.launcher import Cluster
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer
from ledgernet.ledger.genesis import dump_genesis
from tools.audit import audit_dirs

ROOT = str(Path(__file__).resolve().parents[2])
RS = 100  # paise per rupee
FAST_RAFT = {
    "election_timeout_min_ms": 150,
    "election_timeout_max_ms": 300,
    "heartbeat_interval_ms": 30,
    "rpc_timeout_ms": 150,
    "snapshot_interval": 0,
}


@dataclass(eq=False)  # compare and hash by identity, so wallets can be dict keys
class Wallet:
    priv: Ed25519PrivateKey

    @classmethod
    def new(cls) -> Wallet:
        return cls(Ed25519PrivateKey.generate())

    @property
    def pub(self) -> bytes:
        return public_bytes(self.priv)

    def transfer(self, to: Wallet, amount: int, nonce: int) -> bytes:
        return encode_transfer(sign_transfer(self.priv, to.pub, amount, nonce))


def write_genesis(path: Path, balances: dict[Wallet, int]) -> str:
    path.write_text(dump_genesis({w.pub: paise for w, paise in balances.items()}))
    return str(path)


async def finish(cluster: Cluster, expected_supply: int, converge_timeout: float = 60.0) -> dict:
    """The common ending of every scenario. Returns the converged statuses."""
    statuses = await cluster.wait_converged(timeout=converge_timeout)
    values = list(statuses.values())
    assert len({s.state_hash for s in values}) == 1, "nodes disagree on state_hash"
    assert len({s.chain_head for s in values}) == 1, "nodes disagree on chain_head"
    assert all(s.total_balance == s.total_supply == expected_supply for s in values), "money not conserved"

    for node_id in cluster.running():
        await cluster.stop(node_id)  # graceful: closes the log stores so the auditor sees clean files
    data_dirs = [cluster.data_dir(n) for n in cluster.node_ids]
    report = await asyncio.to_thread(audit_dirs, data_dirs, cluster.genesis_path)
    assert report.ok, report.render()
    return statuses
