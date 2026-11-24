"""Wallet CLI (V7.6): keys, genesis, balances and transfers from the command line.

    python -m ledgernet.client.wallet keygen alice bob
    python -m ledgernet.client.wallet genesis --out config/genesis.json alice=500 bob=500
    python -m ledgernet.client.wallet balance alice
    python -m ledgernet.client.wallet send alice bob 25.50
    python -m ledgernet.client.wallet status

Money is typed in rupees and stored in paise. Key files (docs 3.12): `keys/<name>.key` is the 32-byte Ed25519
seed as 64 hex characters, `keys/<name>.pub` the public key as 64 hex characters. These are test keys.
Nodes come from `--nodes host:port,host:port` or, by default, from `--config cluster/cluster.local.yaml`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from decimal import Decimal, InvalidOperation

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.client.client import COMMITTED, NOT_APPLIED, PRECHECK_FAILED, LedgerClient
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import dump_genesis
from ledgernet.node_config import load_cluster_config

DEFAULT_KEYS_DIR = "keys"
DEFAULT_CONFIG = "cluster/cluster.local.yaml"
PAISE_PER_RUPEE = 100


class WalletError(Exception):
    """A user mistake (unknown key, bad amount, ...); the CLI prints it without a traceback."""


# ---- money


def parse_rupees(text: str) -> int:
    """'25.50' -> 2550 paise. At most two decimals, no negatives, no exponents."""
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise WalletError(f"not an amount: {text!r}") from None
    if not value.is_finite() or value < 0 or "e" in text.lower():
        raise WalletError(f"not a valid amount: {text!r}")
    paise = value * PAISE_PER_RUPEE
    if paise != paise.to_integral_value():
        raise WalletError(f"{text!r} has more than two decimal places")
    return int(paise)


def format_rupees(paise: int) -> str:
    return f"{paise // PAISE_PER_RUPEE}.{paise % PAISE_PER_RUPEE:02d}"


# ---- keys


def create_key(name: str, keys_dir: str = DEFAULT_KEYS_DIR, overwrite: bool = False) -> bytes:
    """Generate `<keys_dir>/<name>.key` and `.pub`; returns the public key."""
    if not name or "/" in name or name.startswith("."):
        raise WalletError(f"bad key name: {name!r}")
    key_path = os.path.join(keys_dir, f"{name}.key")
    if os.path.exists(key_path) and not overwrite:
        raise WalletError(f"{key_path} already exists (refusing to overwrite a key)")
    os.makedirs(keys_dir, exist_ok=True)
    priv = Ed25519PrivateKey.generate()
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="ascii") as f:
        f.write(priv.private_bytes_raw().hex() + "\n")
    with open(os.path.join(keys_dir, f"{name}.pub"), "w", encoding="ascii") as f:
        f.write(public_bytes(priv).hex() + "\n")
    return public_bytes(priv)


def load_private_key(name: str, keys_dir: str = DEFAULT_KEYS_DIR) -> Ed25519PrivateKey:
    path = os.path.join(keys_dir, f"{name}.key")
    try:
        with open(path, encoding="ascii") as f:
            return Ed25519PrivateKey.from_private_bytes(bytes.fromhex(f.read().strip()))
    except FileNotFoundError:
        raise WalletError(f"no private key for {name!r} ({path} not found)") from None
    except ValueError:
        raise WalletError(f"{path} is not a 64-hex-character Ed25519 seed") from None


def resolve_public_key(who: str, keys_dir: str = DEFAULT_KEYS_DIR) -> bytes:
    """A key name (reads `<name>.pub`) or a 64-character hex public key."""
    if len(who) == 64:
        try:
            return bytes.fromhex(who)
        except ValueError:
            pass
    path = os.path.join(keys_dir, f"{who}.pub")
    try:
        with open(path, encoding="ascii") as f:
            pub = bytes.fromhex(f.read().strip())
    except FileNotFoundError:
        raise WalletError(f"unknown account {who!r}: {path} not found, and not a 64-hex key") from None
    except ValueError:
        raise WalletError(f"{path} is not a 64-hex-character public key") from None
    if len(pub) != 32:
        raise WalletError(f"{path} is not a 32-byte public key")
    return pub


def build_genesis(entries: list[str], keys_dir: str = DEFAULT_KEYS_DIR) -> dict[bytes, int]:
    """['alice=500', 'bob=250.50'] -> {pubkey: paise}."""
    genesis: dict[bytes, int] = {}
    for entry in entries:
        name, sep, amount = entry.partition("=")
        if not sep or not name:
            raise WalletError(f"expected NAME=RUPEES, got {entry!r}")
        pub = resolve_public_key(name, keys_dir)
        if pub in genesis:
            raise WalletError(f"{name!r} appears twice")
        genesis[pub] = parse_rupees(amount)
    if not genesis:
        raise WalletError("give at least one NAME=RUPEES")
    return genesis


# ---- commands


def _node_addresses(args: argparse.Namespace) -> list[str]:
    if args.nodes:
        return [a.strip() for a in args.nodes.split(",") if a.strip()]
    try:
        return list(load_cluster_config(args.config).addresses().values())
    except (OSError, ValueError) as exc:
        raise WalletError(f"cannot read nodes from {args.config}: {exc} (use --nodes host:port)") from None


def _cmd_keygen(args: argparse.Namespace) -> int:
    for name in args.names:
        pub = create_key(name, args.keys_dir, args.force)
        print(f"{name}: {pub.hex()}")
    return 0


def _cmd_genesis(args: argparse.Namespace) -> int:
    genesis = build_genesis(args.accounts, args.keys_dir)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(dump_genesis(genesis))
    total = sum(genesis.values())
    print(f"wrote {args.out}: {len(genesis)} account(s), total supply Rs {format_rupees(total)}")
    return 0


async def _cmd_balance(args: argparse.Namespace) -> int:
    pub = resolve_public_key(args.who, args.keys_dir)
    async with LedgerClient(_node_addresses(args), rpc_timeout_s=args.timeout) as client:
        balance, nonce = await client.get_account(pub, args.node)
    print(f"{args.who}: Rs {format_rupees(balance)}  (last nonce {nonce})")
    return 0


async def _cmd_send(args: argparse.Namespace) -> int:
    priv = load_private_key(args.sender, args.keys_dir)
    recipient = resolve_public_key(args.recipient, args.keys_dir)
    amount = parse_rupees(args.rupees)
    if amount == 0:
        raise WalletError("amount must be greater than zero")
    async with LedgerClient(_node_addresses(args), rpc_timeout_s=args.timeout) as client:
        nonce = args.nonce
        if nonce is None:
            _, last = await client.get_account(public_bytes(priv), args.node)
            nonce = last + 1
        command = encode_transfer(sign_transfer(priv, recipient, amount, nonce))
        result = await client.submit(command, node=args.node)

    label = f"{args.sender} -> {args.recipient}  Rs {format_rupees(amount)}  nonce {nonce}"
    if result.outcome == COMMITTED:
        verdict = TxStatus(result.tx_status).name
        print(f"{verdict}: {label}")
        print(f"  tx_id {tx_id(command).hex()}")
        print(f"  log index {result.index}, term {result.term}, block {result.block_hash[:16]}")
        return 0 if result.tx_status == TxStatus.APPLIED else 1
    if result.outcome == PRECHECK_FAILED:
        print(f"REJECTED before the log ({TxStatus(result.tx_status).name}): {label}")
    elif result.outcome == NOT_APPLIED:
        print(f"NOT APPLIED: nonce {nonce} was already used by another transfer: {label}")
    else:
        print(f"UNKNOWN: gave up after {result.attempts} attempt(s); check the receipt later: {label}")
        print(f"  tx_id {tx_id(command).hex()}")
    return 1


async def _cmd_status(args: argparse.Namespace) -> int:
    async with LedgerClient(_node_addresses(args), rpc_timeout_s=args.timeout) as client:
        statuses = await client.status_all()
    header = f"{'address':<22} {'node':<5} {'role':<10} {'term':>5} {'commit':>7} {'applied':>8}  state"
    print(header)
    for address, s in statuses.items():
        if s is None:
            print(f"{address:<22} {'-':<5} {'unreachable':<10}")
            continue
        print(
            f"{address:<22} {s.node_id:<5} {s.role:<10} {s.term:>5} {s.commit_index:>7} "
            f"{s.last_applied:>8}  {s.state_hash[:12]}"
        )
    return 0 if any(s is not None for s in statuses.values()) else 1


# ---- argument parsing and entry point


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m ledgernet.client.wallet", description=__doc__.split("\n")[0])
    p.add_argument("--keys-dir", default=DEFAULT_KEYS_DIR)
    p.add_argument("--config", default=DEFAULT_CONFIG, help="cluster YAML to read node addresses from")
    p.add_argument("--nodes", help="comma-separated host:port list (overrides --config)")
    p.add_argument("--node", help="ask this one node (host:port) instead of the leader / first node")
    p.add_argument("--timeout", type=float, default=6.0, help="per-RPC timeout in seconds")
    sub = p.add_subparsers(dest="command", required=True)

    k = sub.add_parser("keygen", help="create key pairs")
    k.add_argument("names", nargs="+")
    k.add_argument("--force", action="store_true", help="overwrite existing keys")

    g = sub.add_parser("genesis", help="write genesis.json from NAME=RUPEES pairs")
    g.add_argument("accounts", nargs="+", metavar="NAME=RUPEES")
    g.add_argument("--out", default="config/genesis.json")

    b = sub.add_parser("balance", help="show an account's balance")
    b.add_argument("who", help="key name or 64-hex public key")

    s = sub.add_parser("send", help="sign and submit a transfer")
    s.add_argument("sender", help="key name (needs keys/<name>.key)")
    s.add_argument("recipient", help="key name or 64-hex public key")
    s.add_argument("rupees")
    s.add_argument("--nonce", type=int, help="default: the sender's current nonce + 1")

    sub.add_parser("status", help="table of every node's role, term and state hash")
    return p


async def amain(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "keygen": _cmd_keygen,
        "genesis": _cmd_genesis,
        "balance": _cmd_balance,
        "send": _cmd_send,
        "status": _cmd_status,
    }
    try:
        result = handlers[args.command](args)
        return await result if asyncio.iscoroutine(result) else result
    except WalletError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except grpc.aio.AioRpcError as exc:
        print(f"error: node unreachable or failed ({exc.code().name}): {exc.details()}", file=sys.stderr)
        return 3


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    sys.exit(main())
