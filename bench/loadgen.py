"""Load generator (implementation.md A6.7).

    python -m bench.accounts --n 100                       # test keys + config/genesis.bench.json
    python -m bench.stub_server --genesis config/genesis.bench.json --port 50061 &
    python -m bench.loadgen --nodes n1=127.0.0.1:50061 --genesis config/genesis.bench.json \
        --keys-dir keys/bench --mode closed --clients 8 --duration 20 --warmup 5 --out results/latency/run1

Accounts: K funded genesis accounts whose keys are in ``--keys-dir``. Every sender belongs to exactly one
worker (closed loop) or is checked out by exactly one in-flight request (open loop), and nonces are
tracked locally, so concurrent requests never collide by accident.

Modes:
* ``closed``: C workers, each sends its next transfer as soon as the previous one finishes.
* ``open``: Poisson arrivals at R tx/s. An arrival with no idle sender is recorded as ``NO_FREE_SENDER``
  (the offered load exceeded what K accounts can carry; raise ``--accounts``).

Outputs in ``--out``: ``requests.csv`` (Section 11 schema; only requests sent after the warm-up),
``acked.json`` (tx_ids acknowledged as APPLIED, for the durability check) and ``meta.json`` (the run
configuration and a summary, read by the analysis scripts).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import statistics
import time
from dataclasses import dataclass, field

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from bench.accounts import load_keys
from bench.raw_client import parse_addresses
from ledgernet.client import COMMITTED, NOT_APPLIED, PRECHECK_FAILED, LedgerClient
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import load_genesis

CSV_COLUMNS = [
    "send_ts", "recv_ts", "latency_ms", "outcome", "tx_status", "attempts",
    "node_first", "node_final", "sender", "nonce", "index",
]  # fmt: skip
NO_FREE_SENDER = "NO_FREE_SENDER"

log = logging.getLogger(__name__)


@dataclass
class LoadConfig:
    nodes: dict[str, str]
    genesis: dict[bytes, int]
    keys: list[Ed25519PrivateKey]
    out_dir: str
    mode: str = "closed"
    clients: int = 8
    rate: float = 100.0
    duration_s: float = 10.0
    warmup_s: float = 2.0
    seed: int = 1
    amount_max_paise: int = 100
    max_attempts: int = 10
    platform: str = "laptop"
    drain_timeout_s: float = 10.0


@dataclass
class _Sender:
    key: Ed25519PrivateKey
    pub: bytes
    nonce: int = 0


@dataclass
class _Run:
    cfg: LoadConfig
    client: LedgerClient
    recipients: list[bytes]
    rng: random.Random
    measure_from: float = 0.0  # time.time() after which rows are kept
    rows: list[dict] = field(default_factory=list)
    acked: list[str] = field(default_factory=list)


async def _send_one(run: _Run, sender: _Sender) -> None:
    rng = run.rng
    recipient = rng.choice(run.recipients)
    while recipient == sender.pub:
        recipient = rng.choice(run.recipients)
    amount = rng.randint(1, run.cfg.amount_max_paise)
    nonce = sender.nonce + 1
    cmd = encode_transfer(sign_transfer(sender.key, recipient, amount, nonce))

    send_ts = time.time()
    # perf_counter, not monotonic: on Windows monotonic ticks every ~15.6 ms, which would quantize latencies.
    t0 = time.perf_counter()
    res = await run.client.submit(cmd, max_attempts=run.cfg.max_attempts)
    latency_ms = (time.perf_counter() - t0) * 1000.0
    recv_ts = send_ts + latency_ms / 1000.0

    if res.outcome == COMMITTED and res.tx_status in (TxStatus.APPLIED, TxStatus.REJECTED_INSUFFICIENT_FUNDS):
        sender.nonce = nonce
        if res.tx_status == TxStatus.APPLIED:
            run.acked.append(tx_id(cmd).hex())
    elif res.outcome != PRECHECK_FAILED:  # BAD_NONCE, NOT_APPLIED or FAILED: ask the cluster
        try:
            _, sender.nonce = await run.client.get_account(sender.pub)
        except grpc.aio.AioRpcError as exc:  # keep the local guess; the next failure resyncs again
            log.warning("nonce resync for %s failed: %s", sender.pub.hex()[:16], exc.code())

    if send_ts >= run.measure_from:
        run.rows.append(
            {
                "send_ts": f"{send_ts:.6f}",
                "recv_ts": f"{recv_ts:.6f}",
                "latency_ms": f"{latency_ms:.3f}",
                "outcome": res.outcome,
                "tx_status": TxStatus(res.tx_status).name if res.tx_status is not None else "",
                "attempts": res.attempts,
                "node_first": res.node_first,
                "node_final": res.node_final,
                "sender": sender.pub.hex()[:16],
                "nonce": nonce,
                "index": res.index if res.outcome == COMMITTED else "",
            }
        )


async def _closed_loop(run: _Run, senders: list[_Sender], deadline: float) -> None:
    cfg = run.cfg
    groups = [senders[w :: cfg.clients] for w in range(cfg.clients)]

    async def worker(group: list[_Sender]) -> None:
        i = 0
        while time.monotonic() < deadline:
            await _send_one(run, group[i % len(group)])
            i += 1

    await asyncio.gather(*(worker(g) for g in groups))


async def _open_loop(run: _Run, senders: list[_Sender], deadline: float) -> None:
    cfg = run.cfg
    idle: asyncio.Queue[_Sender] = asyncio.Queue()
    for s in senders:
        idle.put_nowait(s)
    in_flight: set[asyncio.Task] = set()

    async def one(sender: _Sender) -> None:
        try:
            await _send_one(run, sender)
        finally:
            idle.put_nowait(sender)

    next_at = time.monotonic()
    while True:
        next_at += run.rng.expovariate(cfg.rate)
        if next_at >= deadline:
            break
        await asyncio.sleep(max(0.0, next_at - time.monotonic()))
        if idle.empty():
            now = time.time()
            if now >= run.measure_from:
                run.rows.append(
                    {**dict.fromkeys(CSV_COLUMNS, ""), "send_ts": f"{now:.6f}", "outcome": NO_FREE_SENDER}
                )
            continue
        task = asyncio.ensure_future(one(idle.get_nowait()))
        in_flight.add(task)
        task.add_done_callback(in_flight.discard)
    if in_flight:
        await asyncio.wait(in_flight, timeout=cfg.drain_timeout_s)


def _pick_accounts(genesis: dict[bytes, int], keys: list[Ed25519PrivateKey], limit: int) -> list[_Sender]:
    funded = [k for k in keys if public_bytes(k) in genesis]
    funded.sort(key=public_bytes)
    return [_Sender(k, public_bytes(k)) for k in funded[:limit]]


async def run_loadgen(cfg: LoadConfig) -> dict:
    """Run one load-generation session and write its outputs. Returns the summary dict."""
    if cfg.mode not in ("closed", "open"):
        raise ValueError("mode must be 'closed' or 'open'")
    senders = _pick_accounts(cfg.genesis, cfg.keys, len(cfg.keys))
    if not senders:
        raise ValueError("none of the given keys is funded in genesis")
    if cfg.mode == "closed" and cfg.clients > len(senders):
        raise ValueError(f"{cfg.clients} clients need at least as many accounts (have {len(senders)})")
    client = LedgerClient(cfg.nodes)
    run = _Run(cfg, client, sorted(cfg.genesis), random.Random(cfg.seed))
    try:
        nonces = await asyncio.gather(*(client.get_account(s.pub) for s in senders))
        for s, (_, nonce) in zip(senders, nonces):
            s.nonce = nonce
        start_ts, start = time.time(), time.monotonic()
        run.measure_from = start_ts + cfg.warmup_s
        deadline = start + cfg.warmup_s + cfg.duration_s
        if cfg.mode == "closed":
            await _closed_loop(run, senders, deadline)
        else:
            await _open_loop(run, senders, deadline)
        end_ts = time.time()
    finally:
        await client.close()

    summary = summarize(run.rows, cfg.duration_s)
    meta = {
        "mode": cfg.mode,
        "clients": cfg.clients if cfg.mode == "closed" else None,
        "offered_rate": cfg.rate if cfg.mode == "open" else None,
        "duration_s": cfg.duration_s,
        "warmup_s": cfg.warmup_s,
        "seed": cfg.seed,
        "accounts": len(senders),
        "nodes": cfg.nodes,
        "platform": cfg.platform,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "summary": summary,
    }
    write_outputs(cfg.out_dir, run.rows, run.acked, meta)
    return summary


def summarize(rows: list[dict], duration_s: float) -> dict:
    ok = [float(r["latency_ms"]) for r in rows if r["outcome"] == COMMITTED]
    ok.sort()

    def pct(p: float) -> float | None:
        if not ok:
            return None
        return ok[min(len(ok) - 1, round(p / 100 * (len(ok) - 1)))]

    counts: dict[str, int] = {}
    for r in rows:
        key = r["outcome"] if r["outcome"] != COMMITTED else f"COMMITTED/{r['tx_status']}"
        counts[key] = counts.get(key, 0) + 1
    return {
        "requests": len(rows),
        "committed": len(ok),
        "throughput_tps": len(ok) / duration_s if duration_s > 0 else 0.0,
        "p50_ms": pct(50),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
        "mean_ms": statistics.fmean(ok) if ok else None,
        "outcomes": counts,
        "not_applied": counts.get(NOT_APPLIED, 0),
    }


def write_outputs(out_dir: str, rows: list[dict], acked: list[str], meta: dict) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "requests.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: r["send_ts"]))
    with open(os.path.join(out_dir, "acked.json"), "w", encoding="utf-8") as f:
        json.dump(sorted(acked), f, indent=0)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="LedgerNet load generator")
    p.add_argument(
        "--nodes", action="append", required=True, help="n1=host:port[,n2=host:port...] (repeatable)"
    )
    p.add_argument("--genesis", required=True)
    p.add_argument("--keys-dir", required=True, help="directory of <name>.key files for funded accounts")
    p.add_argument("--accounts", type=int, default=100, help="use at most this many funded accounts")
    p.add_argument("--mode", choices=["closed", "open"], default="closed")
    p.add_argument("--clients", type=int, default=8, help="closed loop: concurrent clients")
    p.add_argument("--rate", type=float, default=100.0, help="open loop: offered load in tx/s")
    p.add_argument("--duration", type=float, default=10.0, help="measured seconds (after the warm-up)")
    p.add_argument("--warmup", type=float, default=2.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--amount-max-paise", type=int, default=100)
    p.add_argument("--platform", choices=["laptop", "rce"], default="laptop")
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)

    keys = list(load_keys(args.keys_dir).values())
    genesis = load_genesis(args.genesis)
    keys = [k for k in sorted(keys, key=public_bytes) if public_bytes(k) in genesis][: args.accounts]
    cfg = LoadConfig(
        nodes=parse_addresses(args.nodes),
        genesis=genesis,
        keys=keys,
        out_dir=args.out,
        mode=args.mode,
        clients=args.clients,
        rate=args.rate,
        duration_s=args.duration,
        warmup_s=args.warmup,
        seed=args.seed,
        amount_max_paise=args.amount_max_paise,
        platform=args.platform,
    )
    summary = asyncio.run(run_loadgen(cfg))
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
