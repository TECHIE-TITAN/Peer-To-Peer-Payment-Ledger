"""Hypothesis strategies for random ledger command streams (shared by the ledger tests).

A stream is built from a small cast of deterministic keys. Some are funded at genesis, some are
unknown senders. Operations cover everything the ledger rules distinguish: valid transfers, same-nonce
conflicts, consecutive overdrafts, replays of earlier commands, forged signatures, wrong keys, random
bytes, truncated commands, NOOPs, zero amounts and self-transfers.
"""

from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from hypothesis import strategies as st

from ledgernet.common.tx import NOOP_COMMAND, encode_transfer, public_bytes, sign_transfer
from ledgernet.common.types import LogEntry

N_KEYS = 6
KEYS = [Ed25519PrivateKey.from_private_bytes(bytes([i + 1]) * 32) for i in range(N_KEYS)]
PUBS = [public_bytes(k) for k in KEYS]


@st.composite
def genesis_strategy(draw) -> dict[bytes, int]:
    funded = draw(st.lists(st.sampled_from(range(N_KEYS)), min_size=1, max_size=N_KEYS - 1, unique=True))
    return {PUBS[i]: draw(st.integers(min_value=0, max_value=1_000)) for i in funded}


_op = st.one_of(
    st.tuples(
        st.just("valid"),
        st.integers(0, N_KEYS - 1),
        st.integers(0, N_KEYS - 1),
        st.integers(0, 600),
        st.sampled_from([0, 0, 0, 0, -1, 1, 2]),
    ),
    st.tuples(st.just("replay"), st.integers(0, 10_000)),
    st.tuples(st.just("forge"), st.integers(0, N_KEYS - 1), st.integers(0, N_KEYS - 1), st.integers(0, 144)),
    st.tuples(st.just("wrong_key"), st.integers(0, N_KEYS - 1), st.integers(0, N_KEYS - 1)),
    st.tuples(st.just("random"), st.binary(min_size=0, max_size=160)),
    st.tuples(st.just("random145"), st.binary(min_size=144, max_size=144)),
    st.tuples(st.just("truncated"), st.integers(1, 144)),
    st.tuples(st.just("noop")),
    st.tuples(st.just("self"), st.integers(0, N_KEYS - 1)),
)


@st.composite
def stream_strategy(draw, max_len: int = 40) -> tuple[dict[bytes, int], list[LogEntry]]:
    """Returns ``(genesis, entries)``; entries have contiguous indexes and non-decreasing terms."""
    genesis = draw(genesis_strategy())
    ops = draw(st.lists(_op, min_size=1, max_size=max_len))
    nonces = [0] * N_KEYS  # the stream's own guess of each nonce; deliberately simple, not the ledger's view
    cmds: list[bytes] = []
    for op in ops:
        kind = op[0]
        if kind == "valid":
            _, s, r, amount, skew = op
            nonce = max(0, nonces[s] + 1 + skew)
            cmd = encode_transfer(sign_transfer(KEYS[s], PUBS[r], amount, nonce))
            if skew == 0:
                nonces[s] += 1
        elif kind == "replay":
            cmd = cmds[op[1] % len(cmds)] if cmds else NOOP_COMMAND
        elif kind == "forge":
            _, s, r, pos = op
            raw = bytearray(encode_transfer(sign_transfer(KEYS[s], PUBS[r], 10, nonces[s] + 1)))
            raw[pos] ^= 0x40
            cmd = bytes(raw)
        elif kind == "wrong_key":
            _, s, r = op
            honest = sign_transfer(KEYS[s], PUBS[r], 10, nonces[s] + 1)
            other = sign_transfer(KEYS[(s + 1) % N_KEYS], PUBS[r], 10, nonces[s] + 1)
            cmd = encode_transfer(
                honest.__class__(honest.sender, honest.recipient, 10, honest.nonce, other.signature)
            )
        elif kind == "random":
            cmd = op[1]
        elif kind == "random145":
            cmd = b"\x01" + op[1]
        elif kind == "truncated":
            cmd = encode_transfer(sign_transfer(KEYS[0], PUBS[1], 1, 1))[: op[1]]
        elif kind == "noop":
            cmd = NOOP_COMMAND
        else:  # self-transfer
            s = op[1]
            cmd = encode_transfer(sign_transfer(KEYS[s], PUBS[s], 5, nonces[s] + 1))
        cmds.append(cmd)
    term_bumps = draw(st.lists(st.booleans(), min_size=len(cmds), max_size=len(cmds)))
    entries, term = [], 1
    for i, (cmd, bump) in enumerate(zip(cmds, term_bumps), start=1):
        term += 1 if bump and i % 7 == 0 else 0
        entries.append(LogEntry(i, term, cmd))
    return genesis, entries
