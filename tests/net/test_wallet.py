"""Wallet CLI (V7.6): money parsing, key files, genesis, and send/balance/status against a real server."""

from __future__ import annotations

import os
import socket
import stat

import grpc
import pytest

from ledgernet.client.wallet import (
    WalletError,
    amain,
    build_genesis,
    create_key,
    format_rupees,
    parse_rupees,
    resolve_public_key,
)
from ledgernet.ledger.genesis import load_genesis
from ledgernet.net.services import LedgerServicer, register_services
from ledgernet.testing.fakes import ReferenceLedger, SingleNodeConsensus

# ---- money


@pytest.mark.parametrize(
    "text,paise",
    [("0", 0), ("1", 100), ("25.50", 2550), ("25.5", 2550), ("0.01", 1), ("1000000", 10**8), ("7.00", 700)],
)
def test_parse_rupees(text, paise):
    assert parse_rupees(text) == paise


@pytest.mark.parametrize("text", ["", "abc", "-1", "1.005", "1e3", "nan", "inf", "1,000"])
def test_parse_rupees_rejects_bad_input(text):
    with pytest.raises(WalletError):
        parse_rupees(text)


def test_format_rupees():
    assert format_rupees(0) == "0.00"
    assert format_rupees(5) == "0.05"
    assert format_rupees(2550) == "25.50"
    assert format_rupees(100_000_000) == "1000000.00"


# ---- keys and genesis


def test_keygen_writes_matching_key_files(tmp_path):
    keys = str(tmp_path / "keys")
    pub = create_key("alice", keys)
    seed = (tmp_path / "keys" / "alice.key").read_text().strip()
    assert len(seed) == 64 and (tmp_path / "keys" / "alice.pub").read_text().strip() == pub.hex()
    assert resolve_public_key("alice", keys) == pub
    if os.name == "posix":
        assert stat.S_IMODE(os.stat(tmp_path / "keys" / "alice.key").st_mode) == 0o600


def test_keygen_refuses_to_overwrite_unless_forced(tmp_path):
    keys = str(tmp_path)
    first = create_key("alice", keys)
    with pytest.raises(WalletError, match="already exists"):
        create_key("alice", keys)
    assert create_key("alice", keys, overwrite=True) != first


@pytest.mark.parametrize("name", ["", "a/b", ".hidden"])
def test_bad_key_names(tmp_path, name):
    with pytest.raises(WalletError):
        create_key(name, str(tmp_path))


def test_resolve_accepts_a_hex_key_and_rejects_unknown_names(tmp_path):
    assert resolve_public_key("ab" * 32, str(tmp_path)) == bytes.fromhex("ab" * 32)
    with pytest.raises(WalletError, match="unknown account"):
        resolve_public_key("nobody", str(tmp_path))


async def test_genesis_command_writes_a_loadable_file(tmp_path, capsys):
    keys, out = str(tmp_path / "keys"), str(tmp_path / "cfg" / "genesis.json")
    alice, bob = create_key("alice", keys), create_key("bob", keys)
    code = await amain(["--keys-dir", keys, "genesis", "--out", out, "alice=500", "bob=250.50"])
    assert code == 0
    assert load_genesis(out) == {alice: 50_000, bob: 25_050}
    assert "total supply Rs 750.50" in capsys.readouterr().out


def test_build_genesis_errors(tmp_path):
    keys = str(tmp_path)
    create_key("alice", keys)
    with pytest.raises(WalletError, match="NAME=RUPEES"):
        build_genesis(["alice"], keys)
    with pytest.raises(WalletError, match="twice"):
        build_genesis(["alice=1", "alice=2"], keys)
    with pytest.raises(WalletError, match="unknown account"):
        build_genesis(["carol=1"], keys)
    with pytest.raises(WalletError, match="at least one"):
        build_genesis([], keys)


# ---- against a running node


def _free_address() -> str:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{s.getsockname()[1]}"


@pytest.fixture
async def ledger_node(tmp_path):
    keys = str(tmp_path / "keys")
    alice, bob = create_key("alice", keys), create_key("bob", keys)
    genesis_path = str(tmp_path / "genesis.json")
    assert await amain(["--keys-dir", keys, "genesis", "--out", genesis_path, "alice=500", "bob=0"]) == 0

    ledger = ReferenceLedger(load_genesis(genesis_path))
    consensus = SingleNodeConsensus("n1", ledger)
    server = grpc.aio.server()
    register_services(server, LedgerServicer(consensus, ledger, {}, 2.0))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    node = {"keys": keys, "nodes": f"127.0.0.1:{port}", "alice": alice, "bob": bob, "ledger": ledger}
    yield node
    await server.stop(0)


def _args(node, *rest):
    return ["--keys-dir", node["keys"], "--nodes", node["nodes"], "--timeout", "3", *rest]


async def test_balance_command(ledger_node, capsys):
    assert await amain(_args(ledger_node, "balance", "alice")) == 0
    assert "alice: Rs 500.00  (last nonce 0)" in capsys.readouterr().out


async def test_send_applies_and_moves_money(ledger_node, capsys):
    assert await amain(_args(ledger_node, "send", "alice", "bob", "25.50")) == 0
    out = capsys.readouterr().out
    assert "APPLIED: alice -> bob  Rs 25.50  nonce 1" in out and "tx_id" in out

    assert await amain(_args(ledger_node, "balance", "bob")) == 0
    assert "bob: Rs 25.50" in capsys.readouterr().out
    assert ledger_node["ledger"].get_account(ledger_node["alice"]) == (47_450, 1)


async def test_consecutive_sends_pick_the_next_nonce_automatically(ledger_node, capsys):
    for _ in range(3):
        assert await amain(_args(ledger_node, "send", "alice", "bob", "1")) == 0
    assert ledger_node["ledger"].get_account(ledger_node["alice"]) == (49_700, 3)
    assert "nonce 3" in capsys.readouterr().out


async def test_send_to_a_raw_hex_key(ledger_node):
    assert await amain(_args(ledger_node, "send", "alice", ledger_node["bob"].hex(), "1")) == 0
    assert ledger_node["ledger"].get_account(ledger_node["bob"]) == (100, 0)


async def test_insufficient_funds_is_reported_and_exits_nonzero(ledger_node, capsys):
    assert await amain(_args(ledger_node, "send", "alice", "bob", "501")) == 1
    assert "REJECTED_INSUFFICIENT_FUNDS" in capsys.readouterr().out


async def test_explicit_wrong_nonce_is_rejected_by_the_ledger(ledger_node, capsys):
    assert await amain(_args(ledger_node, "send", "alice", "bob", "1", "--nonce", "9")) == 1
    assert "REJECTED_BAD_NONCE" in capsys.readouterr().out


async def test_zero_amount_is_refused_before_contacting_a_node(ledger_node, capsys):
    assert await amain(_args(ledger_node, "send", "alice", "bob", "0")) == 2
    assert "greater than zero" in capsys.readouterr().err


async def test_unknown_sender_key(ledger_node, capsys):
    assert await amain(_args(ledger_node, "send", "mallory", "bob", "1")) == 2
    assert "no private key" in capsys.readouterr().err


async def test_status_table_lists_every_node_and_marks_dead_ones(ledger_node, capsys):
    dead = _free_address()
    args = ["--keys-dir", ledger_node["keys"], "--nodes", f"{ledger_node['nodes']},{dead}", "status"]
    assert await amain(args) == 0
    out = capsys.readouterr().out
    assert "leader" in out and "n1" in out
    assert dead in out and "unreachable" in out


async def test_unreachable_cluster_exits_with_an_error(tmp_path, capsys):
    keys = str(tmp_path)
    create_key("alice", keys)
    code = await amain(["--keys-dir", keys, "--nodes", _free_address(), "--timeout", "1", "balance", "alice"])
    assert code == 3
    assert "unreachable" in capsys.readouterr().err


async def test_missing_config_gives_a_helpful_message(tmp_path, capsys):
    keys = str(tmp_path)
    create_key("alice", keys)
    code = await amain(["--keys-dir", keys, "--config", str(tmp_path / "nope.yaml"), "balance", "alice"])
    assert code == 2
    assert "--nodes" in capsys.readouterr().err
