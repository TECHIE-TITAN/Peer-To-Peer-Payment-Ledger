"""Transfer encoding, signatures and hash rules (implementation.md 3.4, 3.5; task A6.1).

The fixed vectors pin the wire format: any accidental change to the encoding, the signed message,
the domain strings or the hash constructions breaks these tests.
"""

from __future__ import annotations

import os

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from hypothesis import given, settings
from hypothesis import strategies as st

from ledgernet.common.hashing import block_hash, genesis_hash, state_hash
from ledgernet.common.tx import (
    NOOP_COMMAND,
    TRANSFER_LEN,
    DecodeError,
    Transfer,
    decode_command,
    encode_transfer,
    public_bytes,
    sign_transfer,
    tx_id,
)

EXAMPLES = 10_000 if os.environ.get("LEDGERNET_FUZZ") else 300

# ---------------------------------------------------------------- fixed vectors

ALICE_SEED = bytes(32)
BOB_SEED = bytes([1]) * 32
ALICE_PUB = "3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29"  # RFC 8032 test 1
BOB_PUB = "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"
VECTOR_SIGNATURE = (
    "5aa0b8f84e0c52c3272d083f70705e7f601f84bcbe9a9106f75993cea92dd7dd"
    "b79111f0b92525c98bb4775f992c0e75e84e5980e55704dd8e4d09dd45e23509"
)
VECTOR_COMMAND = "01" + ALICE_PUB + BOB_PUB + "000000000000c350" + "0000000000000001" + VECTOR_SIGNATURE
VECTOR_TX_ID = "78f496f05eb30edcedf2f45ce442463c80f42c867e64eee937efb62cb6ea652a"
VECTOR_GENESIS_HASH = "1cf2fe5250a8e5cdac7477bcbbeeef84e0728a2aa704f526f80f701b7a45d2f4"
VECTOR_BLOCK_1 = "df36ff0a93274d22b9afc227eeeeb05510ddf6318a0b0d7e7fde83ea2fb55164"
VECTOR_STATE_HASH = "3b809dfe3390262458146144eb476a3b098a43726dcbba1824062e5345f2278c"


def _key(seed: bytes) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(seed)


ALICE, BOB = _key(ALICE_SEED), _key(BOB_SEED)


def _vector_transfer() -> Transfer:
    return sign_transfer(ALICE, public_bytes(BOB), 50_000, 1)  # Rs 500 = 50 000 paise


def test_public_keys():
    assert public_bytes(ALICE).hex() == ALICE_PUB
    assert public_bytes(BOB).hex() == BOB_PUB


def test_signature_and_command_vector():
    t = _vector_transfer()
    assert t.signature.hex() == VECTOR_SIGNATURE
    cmd = encode_transfer(t)
    assert cmd.hex() == VECTOR_COMMAND
    assert len(cmd) == TRANSFER_LEN == 145
    assert tx_id(cmd).hex() == VECTOR_TX_ID


def test_signed_message_layout():
    t = _vector_transfer()
    expected = (
        b"LEDGERNET/v1/transfer"
        + t.sender
        + t.recipient
        + (50_000).to_bytes(8, "big")
        + (1).to_bytes(8, "big")
    )
    assert t.signing_bytes() == expected


def test_hash_vectors():
    alice, bob = bytes.fromhex(ALICE_PUB), bytes.fromhex(BOB_PUB)
    g = genesis_hash({alice: 5_000_000, bob: 5_000_000})
    assert g.hex() == VECTOR_GENESIS_HASH
    h1 = block_hash(g, 1, 1, bytes.fromhex(VECTOR_COMMAND))
    assert h1.hex() == VECTOR_BLOCK_1
    assert state_hash({alice: (4_950_000, 1), bob: (5_050_000, 0)}, 1, h1) == VECTOR_STATE_HASH


def test_hashes_do_not_depend_on_dict_order():
    alice, bob = bytes.fromhex(ALICE_PUB), bytes.fromhex(BOB_PUB)
    assert genesis_hash({alice: 1, bob: 2}) == genesis_hash({bob: 2, alice: 1})
    assert state_hash({alice: (1, 0), bob: (2, 0)}, 0, b"h") == state_hash(
        {bob: (2, 0), alice: (1, 0)}, 0, b"h"
    )


# ---------------------------------------------------------------- round trip and decoding


def test_round_trip():
    t = _vector_transfer()
    decoded = decode_command(encode_transfer(t))
    assert decoded == t and decoded.verify()


def test_noop():
    assert NOOP_COMMAND == b"\x00"
    assert decode_command(NOOP_COMMAND) is None


@pytest.mark.parametrize(
    "cmd",
    [
        b"",
        b"\x00\x00",  # NOOP is exactly one byte
        b"\x02",
        bytes.fromhex(VECTOR_COMMAND)[:-1],  # one byte short
        bytes.fromhex(VECTOR_COMMAND) + b"\x00",  # one byte long
        b"\x02" + bytes.fromhex(VECTOR_COMMAND)[1:],  # unknown command type
        b"\x00" + bytes.fromhex(VECTOR_COMMAND)[1:],  # NOOP type with a body
    ],
)
def test_malformed_commands(cmd):
    with pytest.raises(DecodeError):
        decode_command(cmd)


@pytest.mark.parametrize(
    "field, value",
    [("sender", b"\x01" * 31), ("recipient", b"\x01" * 33), ("signature", b"\x01" * 63)],
)
def test_wrong_lengths_rejected(field, value):
    t = _vector_transfer()
    kwargs = {
        "sender": t.sender,
        "recipient": t.recipient,
        "amount": t.amount,
        "nonce": t.nonce,
        "signature": t.signature,
    }
    kwargs[field] = value
    with pytest.raises(ValueError):
        encode_transfer(Transfer(**kwargs))


@pytest.mark.parametrize("amount, nonce", [(-1, 1), (2**64, 1), (1, -1), (1, 2**64)])
def test_out_of_range_rejected(amount, nonce):
    t = _vector_transfer()
    with pytest.raises(ValueError):
        encode_transfer(Transfer(t.sender, t.recipient, amount, nonce, t.signature))


def test_extreme_values_round_trip():
    t = sign_transfer(ALICE, public_bytes(BOB), 2**64 - 1, 2**64 - 1)
    decoded = decode_command(encode_transfer(t))
    assert decoded == t and decoded.verify()


# ---------------------------------------------------------------- tampering


@pytest.mark.parametrize("field", ["sender", "recipient", "amount", "nonce", "signature"])
def test_tampering_with_any_field_fails(field):
    t = _vector_transfer()
    values = {
        "sender": t.sender,
        "recipient": t.recipient,
        "amount": t.amount,
        "nonce": t.nonce,
        "signature": t.signature,
    }
    if field in ("amount", "nonce"):
        values[field] += 1
    else:
        raw = bytearray(values[field])
        raw[0] ^= 0x01
        values[field] = bytes(raw)
    assert not Transfer(**values).verify()


def test_signature_from_another_key_fails():
    t = _vector_transfer()
    forged = sign_transfer(BOB, t.recipient, t.amount, t.nonce)
    assert not Transfer(t.sender, t.recipient, t.amount, t.nonce, forged.signature).verify()


def test_invalid_public_key_does_not_raise():
    t = _vector_transfer()
    assert Transfer(b"\xff" * 32, t.recipient, t.amount, t.nonce, t.signature).verify() is False


@settings(max_examples=EXAMPLES, deadline=None)
@given(bit=st.integers(min_value=0, max_value=TRANSFER_LEN * 8 - 1))
def test_any_single_bit_flip_breaks_command(bit):
    cmd = bytearray.fromhex(VECTOR_COMMAND)
    cmd[bit // 8] ^= 1 << (bit % 8)
    try:
        decoded = decode_command(bytes(cmd))
    except DecodeError:
        return
    assert decoded is not None and not decoded.verify()


@settings(max_examples=EXAMPLES, deadline=None)
@given(
    seed=st.binary(min_size=32, max_size=32),
    recipient=st.binary(min_size=32, max_size=32),
    amount=st.integers(min_value=0, max_value=2**64 - 1),
    nonce=st.integers(min_value=0, max_value=2**64 - 1),
)
def test_round_trip_property(seed, recipient, amount, nonce):
    t = sign_transfer(_key(seed), recipient, amount, nonce)
    decoded = decode_command(encode_transfer(t))
    assert decoded == t and decoded.verify()
