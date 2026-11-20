"""Load generator end to end against the in-process stub server (task A6.7)."""

from __future__ import annotations

import csv
import json

import grpc
import pytest

from bench.accounts import bench_key, load_keys, make_bench_accounts
from bench.loadgen import CSV_COLUMNS, LoadConfig, main, run_loadgen
from bench.raw_client import COMMITTED, PRECHECK_FAILED, RawLedgerClient
from bench.stub_server import StubLedgerServicer, start_stub_server
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import TxStatus
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.testing.fakes import ReferenceLedger, SingleNodeConsensus

N_ACCOUNTS = 12


@pytest.fixture
def accounts(tmp_path):
    genesis_path = str(tmp_path / "genesis.json")
    keys_dir = str(tmp_path / "keys")
    genesis = make_bench_accounts(N_ACCOUNTS, 1_000_000, keys_dir, genesis_path)
    return genesis, list(load_keys(keys_dir).values()), genesis_path, keys_dir


def _read_rows(out_dir):
    with open(out_dir / "requests.csv", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == CSV_COLUMNS
        return list(reader)


@pytest.mark.parametrize("mode", ["closed", "open"])
async def test_loadgen_against_stub_server(tmp_path, accounts, mode):
    genesis, keys, _, _ = accounts
    server, port, ledger = await start_stub_server(
        genesis, ledger_kind="sqlite", db_path=str(tmp_path / "s.db")
    )
    try:
        out = tmp_path / f"run-{mode}"
        cfg = LoadConfig(
            nodes={"n1": f"127.0.0.1:{port}"},
            genesis=genesis,
            keys=keys,
            out_dir=str(out),
            mode=mode,
            clients=4,
            rate=80.0,
            duration_s=1.0,
            warmup_s=0.3,
            seed=7,
        )
        summary = await run_loadgen(cfg)
    finally:
        await server.stop(None)

    rows = _read_rows(out)
    assert rows and summary["committed"] > 0
    committed = [r for r in rows if r["outcome"] == COMMITTED]
    assert all(r["tx_status"] == "APPLIED" for r in committed)
    assert all(float(r["latency_ms"]) >= 0 and float(r["recv_ts"]) >= float(r["send_ts"]) for r in committed)
    assert all(r["node_first"] == "n1" and r["node_final"] == "n1" for r in committed)

    acked = json.loads((out / "acked.json").read_text())
    assert len(acked) >= len(committed)  # warm-up requests are acked too, but not measured
    for txid in acked:
        assert ledger.get_receipt(bytes.fromhex(txid)).status == TxStatus.APPLIED
    assert ledger.total_balance() == ledger.total_supply()
    meta = json.loads((out / "meta.json").read_text())
    assert meta["mode"] == mode and meta["summary"]["committed"] == summary["committed"]
    if mode == "open":
        assert meta["offered_rate"] == 80.0


async def test_client_follows_not_leader_hint(accounts):
    genesis, keys, _, _ = accounts
    leader_server, leader_port, ledger = await start_stub_server(genesis)
    follower = grpc.aio.server()
    leader_addr = f"127.0.0.1:{leader_port}"
    pb_grpc.add_LedgerServicer_to_server(
        StubLedgerServicer(
            SingleNodeConsensus("n2", ReferenceLedger(genesis), leader=False, leader_hint="n1"),
            ReferenceLedger(genesis),
            addresses={"n1": leader_addr},
        ),
        follower,
    )
    follower_port = follower.add_insecure_port("127.0.0.1:0")
    await follower.start()
    client = RawLedgerClient({"n2": f"127.0.0.1:{follower_port}", "n1": leader_addr})
    try:
        cmd = encode_transfer(sign_transfer(keys[0], public_bytes(keys[1]), 10, 1))
        res = await client.submit(cmd, node="n2")
        assert res.outcome == COMMITTED and res.tx_status == TxStatus.APPLIED
        assert (res.node_first, res.node_final, res.attempts) == ("n2", "n1", 2)
        assert ledger.get_receipt(tx_id(cmd)) is not None

        # A replay of the same bytes is committed as BAD_NONCE (replay protection end to end).
        again = await client.submit(cmd)
        assert again.outcome == COMMITTED and again.tx_status == TxStatus.REJECTED_BAD_NONCE

        forged = bytearray(cmd)
        forged[100] ^= 1
        bad = await client.submit(bytes(forged))
        assert bad.outcome == PRECHECK_FAILED and bad.tx_status == TxStatus.REJECTED_BAD_SIGNATURE
    finally:
        await client.close()
        await follower.stop(None)
        await leader_server.stop(None)


async def test_unreachable_cluster_gives_up(accounts):
    _, keys, _, _ = accounts
    client = RawLedgerClient({"n1": "127.0.0.1:1"}, rpc_timeout_s=0.2, backoff_initial_s=0.01)
    try:
        cmd = encode_transfer(sign_transfer(keys[0], public_bytes(keys[1]), 10, 1))
        res = await client.submit(cmd, max_attempts=3)
        assert res.outcome == "FAILED" and res.attempts == 3
    finally:
        await client.close()


def test_bench_keys_are_deterministic(accounts):
    genesis, keys, _, _ = accounts
    assert public_bytes(bench_key(0)) in genesis
    assert {public_bytes(k) for k in keys} == set(genesis)


def test_cli_rejects_too_many_clients(tmp_path, accounts):
    _, _, genesis_path, keys_dir = accounts
    with pytest.raises(ValueError):
        main(
            [
                "--nodes", "n1=127.0.0.1:1", "--genesis", genesis_path, "--keys-dir", keys_dir,
                "--clients", str(N_ACCOUNTS + 1), "--out", str(tmp_path / "x"),
            ]
        )  # fmt: skip
