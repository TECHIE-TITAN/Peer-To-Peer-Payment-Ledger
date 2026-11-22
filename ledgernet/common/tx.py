from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

DOMAIN_TRANSFER = b"LEDGERNET/v1/transfer"
CMD_NOOP = 0x00
CMD_TRANSFER = 0x01
_BODY_FMT = ">32s32sQQ"
_TRANSFER_FMT = ">32s32sQQ64s"
TRANSFER_LEN = 1 + struct.calcsize(_TRANSFER_FMT)  # 145
NOOP_COMMAND = bytes([CMD_NOOP])


class DecodeError(ValueError):
    pass


@dataclass(frozen=True)
class Transfer:
    sender: bytes       # 32-byte raw Ed25519 public key
    recipient: bytes    # 32-byte raw Ed25519 public key
    amount: int         # paise
    nonce: int          # must equal sender's stored nonce + 1
    signature: bytes = b""

    def signing_bytes(self) -> bytes:
        return DOMAIN_TRANSFER + struct.pack(
            _BODY_FMT, self.sender, self.recipient, self.amount, self.nonce
        )

    def verify(self) -> bool:
        try:
            Ed25519PublicKey.from_public_bytes(self.sender).verify(
                self.signature, self.signing_bytes()
            )
            return True
        except (InvalidSignature, ValueError):
            return False


def public_bytes(priv: Ed25519PrivateKey) -> bytes:
    return priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


def sign_transfer(priv: Ed25519PrivateKey, recipient: bytes, amount: int, nonce: int) -> Transfer:
    unsigned = Transfer(public_bytes(priv), recipient, amount, nonce)
    return Transfer(unsigned.sender, recipient, amount, nonce, priv.sign(unsigned.signing_bytes()))


def encode_transfer(t: Transfer) -> bytes:
    if len(t.sender) != 32 or len(t.recipient) != 32 or len(t.signature) != 64:
        raise ValueError("bad field length")
    if not (0 <= t.amount < 2**64 and 0 <= t.nonce < 2**64):
        raise ValueError("amount/nonce out of range")
    return bytes([CMD_TRANSFER]) + struct.pack(
        _TRANSFER_FMT, t.sender, t.recipient, t.amount, t.nonce, t.signature
    )


def decode_command(cmd: bytes) -> Optional[Transfer]:
    """Returns None for NOOP, a Transfer for TRANSFER; raises DecodeError otherwise."""
    if cmd == NOOP_COMMAND:
        return None
    if len(cmd) != TRANSFER_LEN or cmd[0] != CMD_TRANSFER:
        raise DecodeError(f"bad command (len={len(cmd)})")
    s, r, a, n, sig = struct.unpack(_TRANSFER_FMT, cmd[1:])
    return Transfer(s, r, a, n, sig)


def tx_id(cmd: bytes) -> bytes:
    """Receipt key for a command."""
    return hashlib.sha256(cmd).digest()
