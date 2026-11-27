"""Shared plumbing for the experiment scripts (V7.9): a benchmark cluster, load, result directories.

Every experiment builds a fresh 5-node cluster of real processes in a temporary directory, funds the
benchmark accounts (A's `bench.accounts`), and writes its results under `results/<experiment>/<timestamp>/`
together with the configuration it ran with.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import grpc

from bench.accounts import load_keys, make_bench_accounts
from bench.loadgen import LoadConfig, run_loadgen
from cluster.launcher import Cluster, write_local_config
from ledgernet.client import COMMITTED
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import load_genesis

ROOT = str(Path(__file__).resolve().parents[1])
BALANCE_PAISE = 100_000_000  # Rs 10 lakh per benchmark account: never runs dry in an experiment


def add_cluster_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--nodes", type=int, default=5, help="cluster size")
    p.add_argument("--accounts", type=int, default=100, help="funded benchmark accounts")
    p.add_argument("--election-min-ms", type=int, default=150)
    p.add_argument("--election-max-ms", type=int, default=300)
    p.add_argument("--heartbeat-ms", type=int, default=30)
    p.add_argument("--rpc-timeout-ms", type=int, default=150)
    p.add_argument("--snapshot-interval", type=int, default=1000)
    p.add_argument("--platform", choices=["laptop", "rce"], default="laptop")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out-root", default="results", help="results go to <out-root>/<experiment>/<timestamp>")


def raft_settings(args: argparse.Namespace, **override) -> dict:
    settings = {
        "election_timeout_min_ms": args.election_min_ms,
        "election_timeout_max_ms": args.election_max_ms,
        "heartbeat_interval_ms": args.heartbeat_ms,
        "rpc_timeout_ms": args.rpc_timeout_ms,
        "snapshot_interval": args.snapshot_interval,
    }
    return {**settings, **override}


def results_dir(args: argparse.Namespace, experiment: str) -> Path:
    path = Path(args.out_root) / experiment / datetime.now().strftime("%Y%m%d-%H%M%S")
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str), encoding="utf-8")


def update_meta(run_dir: Path, **extra) -> None:
    """Add fields to the `meta.json` the load generator wrote (the analysis scripts read it)."""
    path = run_dir / "meta.json"
    meta = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    write_json(path, {**meta, **extra})


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


@dataclass
class Bench:
    cluster: Cluster
    keys_dir: str
    genesis_path: str


@asynccontextmanager
async def bench_cluster(workdir: str, *, n_nodes: int, accounts: int, raft: dict) -> AsyncIterator[Bench]:
    """A started cluster with funded benchmark accounts, stopped gracefully on exit."""
    keys_dir = str(Path(workdir) / "keys")
    genesis_path = str(Path(workdir) / "genesis.json")
    make_bench_accounts(accounts, BALANCE_PAISE, keys_dir, genesis_path)
    config = write_local_config(workdir, genesis_path, n_nodes=n_nodes, raft=raft)
    cluster = Cluster(config, cwd=ROOT)
    try:
        await cluster.start_all()
        await cluster.wait_for_leader()
        yield Bench(cluster, keys_dir, genesis_path)
    finally:
        await cluster.close()


async def run_load(bench: Bench, out_dir: Path, **fields) -> dict:
    """A's load generator against this cluster; writes requests.csv, acked.json and meta.json to out_dir."""
    genesis = load_genesis(bench.genesis_path)
    keys = [
        k for k in sorted(load_keys(bench.keys_dir).values(), key=public_bytes) if public_bytes(k) in genesis
    ]
    cfg = LoadConfig(
        nodes=bench.cluster.config.addresses(), genesis=genesis, keys=keys, out_dir=str(out_dir), **fields
    )
    return await run_loadgen(cfg)


class Filler:
    """Commits a given number of small transfers as fast as it can, to build up a log of a known length."""

    def __init__(self, bench: Bench, concurrency: int = 32, seed: int = 1) -> None:
        keys = sorted(load_keys(bench.keys_dir).values(), key=public_bytes)
        if len(keys) < concurrency * 2:
            raise ValueError(f"need at least {concurrency * 2} accounts for {concurrency} workers")
        self._client = bench.cluster.client
        self._senders = [[k, public_bytes(k), 0] for k in keys]  # key, pubkey, last nonce
        self._concurrency = concurrency
        self._rng = random.Random(seed)

    async def run(self, count: int) -> None:
        remaining = count
        started = time.monotonic()
        next_report = 0.1

        async def worker(group: list[list]) -> None:
            nonlocal remaining, next_report
            i = 0
            while remaining > 0:
                remaining -= 1
                sender = group[i % len(group)]
                i += 1
                recipient = self._senders[0][1] if sender is not self._senders[0] else self._senders[1][1]
                command = encode_transfer(sign_transfer(sender[0], recipient, 1, sender[2] + 1))
                result = await self._client.submit(command)
                if result.outcome == COMMITTED and result.tx_status == TxStatus.APPLIED:
                    sender[2] += 1
                else:  # not applied: learn the real nonce and let another transfer take its place
                    _, sender[2] = await self._client.get_account(sender[1])
                    remaining += 1
                done = count - remaining
                if done >= next_report * count:
                    log(f"  filled {done}/{count} ({time.monotonic() - started:.0f}s)")
                    next_report += 0.1

        groups = [self._senders[w :: self._concurrency] for w in range(self._concurrency)]
        await asyncio.gather(*(worker(g) for g in groups))


def new_channel(address: str) -> grpc.aio.Channel:
    """A channel that reconnects quickly: for polling a node that is restarting."""
    options = [("grpc.initial_reconnect_backoff_ms", 50), ("grpc.max_reconnect_backoff_ms", 200)]
    return grpc.aio.insecure_channel(address, options=options)
