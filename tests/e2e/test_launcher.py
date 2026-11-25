"""Cluster launcher (V7.7) on real node processes: crash, restart, partition, heal, lost disk.

Each test starts three `python -m ledgernet.node` processes (file storage, SQLite ledger, real Raft over
gRPC), so this is also the first test of the whole stack running as separate processes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cluster.launcher import Cluster, ClusterError, write_local_config
from ledgernet.client import COMMITTED
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import dump_genesis

ROOT = str(Path(__file__).resolve().parents[2])
RS_500 = 50_000  # paise
FAST_RAFT = {
    "election_timeout_min_ms": 150,
    "election_timeout_max_ms": 300,
    "heartbeat_interval_ms": 30,
    "rpc_timeout_ms": 150,
    "snapshot_interval": 0,
}


@dataclass
class Wallet:
    priv: Ed25519PrivateKey

    @property
    def pub(self) -> bytes:
        return public_bytes(self.priv)

    def transfer(self, to: Wallet, amount: int, nonce: int) -> bytes:
        return encode_transfer(sign_transfer(self.priv, to.pub, amount, nonce))


@pytest.fixture
def wallets():
    return {name: Wallet(Ed25519PrivateKey.generate()) for name in ("alice", "bob")}


@pytest.fixture
async def unstarted(tmp_path, wallets):
    genesis = tmp_path / "genesis.json"
    genesis.write_text(dump_genesis({wallets["alice"].pub: RS_500, wallets["bob"].pub: 0}))
    config = write_local_config(str(tmp_path), str(genesis), n_nodes=3, raft=FAST_RAFT)
    async with Cluster(config, cwd=ROOT) as c:
        yield c


@pytest.fixture
async def cluster(unstarted):
    await unstarted.start_all()
    return unstarted


async def pay(cluster, wallets, nonce, amount=1_000):
    result = await cluster.client.submit(wallets["alice"].transfer(wallets["bob"], amount, nonce))
    assert result.outcome == COMMITTED and result.tx_status == TxStatus.APPLIED, result
    return result


def assert_conserved(statuses):
    assert all(s.total_balance == s.total_supply == RS_500 for s in statuses.values())


async def test_cluster_elects_a_leader_commits_and_converges(cluster, wallets):
    leader = await cluster.wait_for_leader()
    for nonce in (1, 2, 3):
        await pay(cluster, wallets, nonce)
    statuses = await cluster.wait_converged()
    assert set(statuses) == set(cluster.node_ids)
    assert len({s.state_hash for s in statuses.values()}) == 1
    assert_conserved(statuses)
    assert await cluster.client.get_account(wallets["bob"].pub) == (3_000, 0)

    names = {e["event"] for e in cluster.collect_events()}
    assert {"NODE_START", "SERVING", "LEADER_ELECTED"} <= names
    assert leader in cluster.node_ids


async def test_killing_the_leader_elects_another_and_the_old_one_rejoins(cluster, wallets):
    old_leader = await cluster.wait_for_leader()
    await pay(cluster, wallets, 1)
    await cluster.wait_converged()

    cluster.kill(old_leader)  # SIGKILL: nothing flushed, nothing announced
    assert not cluster.is_running(old_leader)
    new_leader = await cluster.wait_for_leader(exclude=[old_leader])
    assert new_leader != old_leader
    await pay(cluster, wallets, 2)  # two of three nodes are a majority

    await cluster.start(old_leader)  # recovers from its own wal and sqlite state
    statuses = await cluster.wait_converged()
    assert set(statuses) == set(cluster.node_ids)
    assert_conserved(statuses)
    assert await cluster.client.get_account(wallets["bob"].pub) == (2_000, 0)


async def test_partitioned_follower_lags_then_catches_up_after_heal(cluster, wallets):
    leader = await cluster.wait_for_leader()
    isolated = next(n for n in cluster.node_ids if n != leader)
    await pay(cluster, wallets, 1)
    await cluster.wait_converged()

    await cluster.partition([[isolated]])  # everyone else forms the other group
    await pay(cluster, wallets, 2)
    await pay(cluster, wallets, 3)
    statuses = await cluster.statuses()
    assert statuses[isolated].last_applied < statuses[leader].last_applied  # it missed the new entries

    await cluster.heal()
    converged = await cluster.wait_converged()
    assert len({s.state_hash for s in converged.values()}) == 1
    assert_conserved(converged)


async def test_minority_side_of_a_partition_cannot_commit(cluster, wallets):
    leader = await cluster.wait_for_leader()
    others = [n for n in cluster.node_ids if n != leader]
    await pay(cluster, wallets, 1)
    await cluster.wait_converged()

    await cluster.partition([[leader], others])  # the old leader is now alone
    new_leader = await cluster.wait_for_leader(exclude=[leader])
    assert new_leader in others
    await pay(cluster, wallets, 2)  # committed by the majority side

    await cluster.heal()
    converged = await cluster.wait_converged()
    assert_conserved(converged)
    assert await cluster.client.get_account(wallets["bob"].pub) == (2_000, 0)  # nothing lost or doubled


async def test_a_late_joining_follower_catches_up_from_an_empty_log(unstarted, wallets):
    """Two of three nodes run and commit; the third starts later with nothing and is brought up to date."""
    cluster = unstarted
    await cluster.start("n1", wait=False)
    await cluster.start("n2", wait=False)
    await cluster.wait_ready("n1")
    await cluster.wait_ready("n2")
    await cluster.wait_for_leader()
    for nonce in (1, 2, 3, 4):
        await pay(cluster, wallets, nonce)

    await cluster.start("n3")
    statuses = await cluster.wait_converged()
    assert set(statuses) == {"n1", "n2", "n3"}
    assert_conserved(statuses)
    assert await cluster.client.get_account(wallets["bob"].pub, "n3") == (4_000, 0)  # n3 serves it too


async def test_wipe_removes_a_stopped_nodes_data(cluster):
    victim = cluster.node_ids[-1]
    await cluster.stop(victim)
    assert os.path.exists(cluster.data_dir(victim))
    cluster.wipe(victim)
    assert not os.path.exists(cluster.data_dir(victim))


async def test_restarting_every_node_keeps_the_committed_state(cluster, wallets):
    await cluster.wait_for_leader()
    for nonce in (1, 2):
        await pay(cluster, wallets, nonce)
    await cluster.wait_converged()

    for node_id in cluster.node_ids:
        cluster.kill(node_id)
    for node_id in cluster.node_ids:
        await cluster.start(node_id, wait=False)
    for node_id in cluster.node_ids:
        await cluster.wait_ready(node_id)
    await cluster.wait_for_leader()
    after = await cluster.wait_converged()

    assert_conserved(after)
    assert await cluster.client.get_account(wallets["bob"].pub) == (2_000, 0)


async def test_launcher_guards(cluster):
    node = cluster.node_ids[0]
    with pytest.raises(ClusterError, match="already running"):
        await cluster.start(node)
    with pytest.raises(ClusterError, match="before wiping"):
        cluster.wipe(node)
    with pytest.raises(ValueError):
        await cluster.partition([["n1"], ["n1", "n2"]])
    with pytest.raises(ValueError):
        await cluster.partition([["n9"]])


def test_ssh_mode_is_not_available_yet(tmp_path):
    with pytest.raises(NotImplementedError):
        Cluster(str(tmp_path / "x.yaml"), mode="ssh")
