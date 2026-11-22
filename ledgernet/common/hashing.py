from __future__ import annotations

import hashlib
import struct
from typing import Dict, Tuple

GENESIS_DOMAIN = b"LEDGERNET/v1/genesis"
BLOCK_DOMAIN = b"LEDGERNET/v1/block"
STATE_DOMAIN = b"LEDGERNET/v1/state"


def genesis_hash(balances: Dict[bytes, int]) -> bytes:
    h = hashlib.sha256(GENESIS_DOMAIN)
    for pk in sorted(balances):
        h.update(pk + struct.pack(">Q", balances[pk]))
    return h.digest()


def block_hash(prev: bytes, index: int, term: int, command: bytes) -> bytes:
    """Every log entry (transfer, rejected transfer or NOOP) is one block."""
    return hashlib.sha256(BLOCK_DOMAIN + prev + struct.pack(">QQ", index, term) + command).digest()


def state_hash(accounts: Dict[bytes, Tuple[int, int]], last_applied: int, chain_head: bytes) -> str:
    """accounts: pubkey -> (balance, nonce). Every existing account is included, sorted by pubkey."""
    h = hashlib.sha256(STATE_DOMAIN + struct.pack(">Q", last_applied) + chain_head)
    for pk in sorted(accounts):
        bal, nonce = accounts[pk]
        h.update(pk + struct.pack(">QQ", bal, nonce))
    return h.hexdigest()
