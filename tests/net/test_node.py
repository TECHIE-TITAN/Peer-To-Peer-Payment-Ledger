"""Cluster config loader and the node process wiring (V7.5).

The cluster tests run several `Node`s in one process over real localhost gRPC. They are the first place where
RaftNode, GrpcTransport, the Ledger service and (in the second test) A's file storage and SQLite ledger run
together.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
from dataclasses import dataclass

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.client import COMMITTED, LedgerClient
from ledgernet.common import events
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import dump_genesis
from ledgernet.node import Node, parse_args
from ledgernet.node_config import load_cluster_config, parse_cluster_config

RS_500 = 50_000  # paise


# ---- config loader


def _valid_doc():
    return {
        "genesis": "config/genesis.json",
        "raft": {"snapshot_interval": 10},
        "submit_timeout_s": 3,
        "nodes": [
            {"id": "n1", "host": "127.0.0.1", "port": 50051, "data_dir": "./data/n1"},
            {"id": "n2", "host": "127.0.0.1", "port": 50052, "data_dir": "./data/n2"},
        ],
    }


def test_valid_config():
    config = parse_cluster_config(_valid_doc())
    assert config.genesis_path == "config/genesis.json"
    assert config.raft.snapshot_interval == 10
    assert config.raft.election_timeout_min_ms == 300  # default kept
    assert config.submit_timeout_s == 3.0
    assert config.node_ids() == ["n1", "n2"]
    assert config.addresses() == {"n1": "127.0.0.1:50051", "n2": "127.0.0.1:50052"}
    assert config.node("n2").data_dir == "./data/n2"
    with pytest.raises(KeyError):
        config.node("n9")


def test_defaults_when_optional_sections_are_missing():
    doc = _valid_doc()
    del doc["raft"], doc["submit_timeout_s"]
    config = parse_cluster_config(doc)
    assert config.raft.heartbeat_interval_ms == 75
    assert config.submit_timeout_s == 5.0


def test_the_shipped_local_config_loads():
    config = load_cluster_config("cluster/cluster.local.yaml")
    assert config.node_ids() == ["n1", "n2", "n3", "n4", "n5"]
    assert config.raft.pre_vote is True


def _mutate(path, value):
    def apply(doc):
        target = doc
        for key in path[:-1]:
            target = target[key]
        if value is DELETE:
            del target[path[-1]]
        else:
            target[path[-1]] = value
        return doc

    return apply


DELETE = object()


@pytest.mark.parametrize(
    "mutation,message",
    [
        (_mutate(["genesis"], DELETE), "genesis"),
        (_mutate(["nodes"], []), "nodes"),
        (_mutate(["nodes"], DELETE), "nodes"),
        (_mutate(["nodes", 1, "id"], "n1"), "unique"),
        (_mutate(["nodes", 1, "port"], 50051), "same host:port"),
        (_mutate(["nodes", 0, "port"], 70000), "port"),
        (_mutate(["nodes", 0, "port"], "50051"), "port"),
        (_mutate(["nodes", 0, "data_dir"], DELETE), "data_dir"),
        (_mutate(["nodes", 0, "colour"], "red"), "unknown key"),
        (_mutate(["raft", "typo_ms"], 5), "unknown raft"),
        (_mutate(["raft", "pre_vote"], "yes"), "pre_vote"),
        (_mutate(["raft", "rpc_timeout_ms"], 1.5), "rpc_timeout_ms"),
        (_mutate(["raft", "election_timeout_min_ms"], 900), "must not exceed"),
        (_mutate(["raft", "heartbeat_interval_ms"], 400), "heartbeat"),
        (_mutate(["raft", "snapshot_interval"], -1), "snapshot_interval"),
        (_mutate(["submit_timeout_s"], 0), "positive"),
        (_mutate(["extra"], 1), "unknown top-level"),
    ],
)
def test_invalid_configs_are_rejected(mutation, message):
    with pytest.raises(ValueError, match=message):
        parse_cluster_config(mutation(_valid_doc()))


def test_non_mapping_config_is_rejected():
    with pytest.raises(ValueError):
        parse_cluster_config(["not", "a", "mapping"])


def test_cli_flags_and_defaults():
    args = parse_args(["--config", "c.yaml", "--id", "n3"])
    assert (args.consensus, args.storage, args.ledger, args.unsafe_debug) == ("raft", "file", "sqlite", False)
    argv = ["--config", "c.yaml", "--id", "n3", "--consensus", "single", "--storage", "memory"]
    args = parse_args([*argv, "--ledger", "reference", "--unsafe-debug"])
    assert (args.consensus, args.storage, args.ledger) == ("single", "memory", "reference")
    assert args.unsafe_debug is True


def test_bad_choice_is_rejected(tmp_path):
    config = parse_cluster_config(_valid_doc())
    with pytest.raises(ValueError, match="consensus"):
        Node(config, "n1", consensus="paxos")
    with pytest.raises(KeyError):
        Node(config, "n9")


# ---- running nodes


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


def _free_ports(n: int) -> list[int]:
    sockets = [socket.socket() for _ in range(n)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def _write_config(tmp_path, wallets, n_nodes):
    genesis_path = tmp_path / "genesis.json"
    genesis_path.write_text(dump_genesis({wallets["alice"].pub: RS_500, wallets["bob"].pub: 0}))
    ports = _free_ports(n_nodes)
    doc = {
        "genesis": str(genesis_path),
        "raft": {
            "election_timeout_min_ms": 150,
            "election_timeout_max_ms": 300,
            "heartbeat_interval_ms": 30,
            "rpc_timeout_ms": 150,
            "snapshot_interval": 0,
        },
        "submit_timeout_s": 5,
        "nodes": [
            {"id": f"n{i + 1}", "host": "127.0.0.1", "port": port, "data_dir": str(tmp_path / f"n{i + 1}")}
            for i, port in enumerate(ports)
        ],
    }
    path = tmp_path / "cluster.yaml"
    path.write_text(yaml.safe_dump(doc))
    return load_cluster_config(str(path))


async def _wait_until(predicate, timeout=20.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.05)
    pytest.fail(f"timed out waiting for {what}")


class Cluster:
    def __init__(self, config, **node_kwargs):
        self.config = config
        self.node_kwargs = node_kwargs
        self.nodes: dict[str, Node] = {}
        self.client = LedgerClient(config.addresses(), rpc_timeout_s=3.0, backoff_initial_s=0.02)

    async def start(self, node_id):
        node = Node(self.config, node_id, configure_events=False, **self.node_kwargs)
        await node.start()
        self.nodes[node_id] = node

    async def stop(self, node_id):
        await self.nodes.pop(node_id).stop()

    async def start_all(self):
        for node_id in self.config.node_ids():
            await self.start(node_id)

    async def stop_all(self):
        await self.client.close()
        for node_id in list(self.nodes):
            await self.stop(node_id)

    async def statuses(self):
        live = {self.config.node(n).address for n in self.nodes}
        answers = await self.client.status_all()
        return {a: s for a, s in answers.items() if a in live and s is not None}

    async def wait_for_leader(self):
        async def find():
            statuses = await self.statuses()
            leaders = [s for s in statuses.values() if s.role == "leader"]
            return leaders[0] if len(statuses) == len(self.nodes) and len(leaders) == 1 else None

        return await _wait_until(find, what="a single leader")

    async def wait_converged(self, min_applied=1):
        async def converged():
            statuses = list((await self.statuses()).values())
            if len(statuses) != len(self.nodes):
                return None
            same = {(s.last_applied, s.state_hash, s.chain_head) for s in statuses}
            return statuses if len(same) == 1 and statuses[0].last_applied >= min_applied else None

        return await _wait_until(converged, what="all live nodes to converge")


async def _run_transfers(cluster, wallets):
    alice, bob = wallets["alice"], wallets["bob"]
    for nonce in (1, 2, 3):
        result = await cluster.client.submit(alice.transfer(bob, 1_000, nonce))
        assert result.outcome == COMMITTED
        assert result.tx_status == TxStatus.APPLIED


async def test_single_mode_node_serves_and_logs_events(tmp_path, wallets, monkeypatch):
    monkeypatch.setattr(events, "_path", None)  # events.py is process-global; restore it afterwards
    config = _write_config(tmp_path, wallets, 1)
    node = Node(config, "n1", consensus="single", storage="memory", ledger="reference")
    await node.start()
    try:
        async with LedgerClient([config.node("n1").address]) as client:
            result = await client.submit(wallets["alice"].transfer(wallets["bob"], 20_000, 1))
            assert result.outcome == COMMITTED and result.tx_status == TxStatus.APPLIED
            assert await client.get_account(wallets["bob"].pub) == (20_000, 0)
            status = await client.status(config.node("n1").address)
            assert status.role == "leader" and status.total_balance == status.total_supply == RS_500
    finally:
        await node.stop()
    lines = (tmp_path / "n1" / "events.jsonl").read_text().splitlines()
    names = [json.loads(line)["event"] for line in lines]
    assert names == ["NODE_START", "SERVING"]


async def test_stop_is_idempotent_and_start_twice_is_rejected(tmp_path, wallets):
    config = _write_config(tmp_path, wallets, 1)
    fakes = {"consensus": "single", "storage": "memory", "ledger": "reference"}
    node = Node(config, "n1", configure_events=False, **fakes)
    await node.start()
    with pytest.raises(RuntimeError):
        await node.start()
    await node.stop()
    await node.stop()


async def test_three_node_raft_cluster_on_the_fakes(tmp_path, wallets):
    """Real Raft over real gRPC, with in-memory storage and the reference ledger."""
    config = _write_config(tmp_path, wallets, 3)
    cluster = Cluster(config, storage="memory", ledger="reference")
    await cluster.start_all()
    try:
        await cluster.wait_for_leader()
        await _run_transfers(cluster, wallets)
        statuses = await cluster.wait_converged(min_applied=3)
        assert all(s.total_balance == s.total_supply == RS_500 for s in statuses)
        assert await cluster.client.get_account(wallets["bob"].pub) == (3_000, 0)
        assert all(n.fatal_error() is None for n in cluster.nodes.values())
    finally:
        await cluster.stop_all()


async def test_three_node_cluster_with_file_storage_and_sqlite_survives_a_follower_restart(tmp_path, wallets):
    config = _write_config(tmp_path, wallets, 3)
    cluster = Cluster(config, storage="file", ledger="sqlite")
    await cluster.start_all()
    try:
        leader = await cluster.wait_for_leader()
        await _run_transfers(cluster, wallets)
        await cluster.wait_converged(min_applied=3)

        follower = next(n for n in config.node_ids() if n != leader.node_id)
        await cluster.stop(follower)
        for nonce in (4, 5):  # still a majority of two: commits continue
            result = await cluster.client.submit(wallets["alice"].transfer(wallets["bob"], 1_000, nonce))
            assert result.outcome == COMMITTED

        await cluster.start(follower)  # same data_dir: it recovers from its own disk, then catches up
        statuses = await cluster.wait_converged(min_applied=5)
        assert len({s.state_hash for s in statuses}) == 1
        assert all(s.total_balance == s.total_supply == RS_500 for s in statuses)
        for node_id in config.node_ids():
            data_dir = tmp_path / node_id
            assert (data_dir / "wal").is_dir() and (data_dir / "state.db").exists()
        assert all(n.fatal_error() is None for n in cluster.nodes.values())
    finally:
        await cluster.stop_all()
