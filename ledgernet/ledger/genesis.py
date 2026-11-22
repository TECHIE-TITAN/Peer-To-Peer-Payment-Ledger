"""Load and write ``config/genesis.json`` (implementation.md 3.12): ``{"accounts": {"<64-hex pubkey>": paise}}``."""

from __future__ import annotations

import json


def parse_genesis(doc: dict) -> dict[bytes, int]:
    accounts = doc.get("accounts")
    if not isinstance(accounts, dict):
        # A malformed file is bad input, so ValueError (like every other check here), not TypeError.
        raise ValueError('genesis must be {"accounts": {"<hex>": paise}}')  # noqa: TRY004
    out: dict[bytes, int] = {}
    for hex_pk, balance in accounts.items():
        pk = bytes.fromhex(hex_pk)
        if len(pk) != 32:
            raise ValueError(f"genesis key {hex_pk!r} is not 32 bytes")
        if not isinstance(balance, int) or isinstance(balance, bool) or balance < 0:
            raise ValueError(f"genesis balance for {hex_pk} must be a non-negative integer (paise)")
        out[pk] = balance
    if sum(out.values()) >= 2**63:
        raise ValueError("total supply too large")
    return out


def load_genesis(path: str) -> dict[bytes, int]:
    with open(path, encoding="utf-8") as f:
        return parse_genesis(json.load(f))


def dump_genesis(genesis: dict[bytes, int]) -> str:
    return json.dumps({"accounts": {pk.hex(): bal for pk, bal in sorted(genesis.items())}}, indent=2) + "\n"
