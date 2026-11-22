"""Benchmark accounts: deterministic test keys plus a matching genesis file.

    python -m bench.accounts --n 100 --keys-dir keys/bench --genesis config/genesis.bench.json

Key files follow implementation.md 3.12: ``<name>.key`` holds the 32-byte Ed25519 seed as 64 hex
characters, ``<name>.pub`` the public key as 64 hex characters. These are test keys only.
"""

from __future__ import annotations

import argparse
import hashlib
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.common.tx import public_bytes
from ledgernet.ledger.genesis import dump_genesis


def bench_key(i: int, label: str = "bench") -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(hashlib.sha256(f"ledgernet/{label}/{i}".encode()).digest())


def write_keys(keys_dir: str, keys: dict[str, Ed25519PrivateKey]) -> None:
    os.makedirs(keys_dir, exist_ok=True)
    for name, key in keys.items():
        seed = key.private_bytes_raw()
        with open(os.path.join(keys_dir, f"{name}.key"), "w", encoding="ascii") as f:
            f.write(seed.hex() + "\n")
        with open(os.path.join(keys_dir, f"{name}.pub"), "w", encoding="ascii") as f:
            f.write(public_bytes(key).hex() + "\n")


def load_keys(keys_dir: str) -> dict[str, Ed25519PrivateKey]:
    """All ``*.key`` files in a directory, by name."""
    out = {}
    for name in sorted(os.listdir(keys_dir)):
        if name.endswith(".key"):
            with open(os.path.join(keys_dir, name), encoding="ascii") as f:
                seed = bytes.fromhex(f.read().strip())
            out[name[: -len(".key")]] = Ed25519PrivateKey.from_private_bytes(seed)
    return out


def make_bench_accounts(n: int, balance: int, keys_dir: str, genesis_path: str) -> dict[bytes, int]:
    keys = {f"bench-{i:04d}": bench_key(i) for i in range(n)}
    write_keys(keys_dir, keys)
    genesis = {public_bytes(k): balance for k in keys.values()}
    os.makedirs(os.path.dirname(os.path.abspath(genesis_path)), exist_ok=True)
    with open(genesis_path, "w", encoding="utf-8") as f:
        f.write(dump_genesis(genesis))
    return genesis


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Create benchmark keys and genesis")
    p.add_argument("--n", type=int, default=100)
    p.add_argument("--balance-paise", type=int, default=100_000_000, help="per account (default Rs 10 lakh)")
    p.add_argument("--keys-dir", default="keys/bench")
    p.add_argument("--genesis", default="config/genesis.bench.json")
    args = p.parse_args(argv)
    make_bench_accounts(args.n, args.balance_paise, args.keys_dir, args.genesis)
    print(f"wrote {args.n} keys to {args.keys_dir} and genesis to {args.genesis}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
