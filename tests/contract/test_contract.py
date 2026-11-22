"""Shared contract tests: guard the frozen contracts (implementation.md, Section 3) against accidental edits."""

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.common.tx import (
    NOOP_COMMAND,
    TRANSFER_LEN,
    decode_command,
    encode_transfer,
    public_bytes,
    sign_transfer,
    tx_id,
)
from ledgernet.common.types import LogEntry, Snapshot, TxStatus
from ledgernet.testing.fakes import (
    InMemoryLogStore,
    InMemorySnapshotStore,
    RecordingStateMachine,
    ReferenceLedger,
    SingleNodeConsensus,
)


def _key(seed_byte: int) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(bytes([seed_byte]) * 32)


def test_sign_encode_decode_verify_round_trip():
    alice, bob = _key(1), _key(2)
    tx = sign_transfer(alice, public_bytes(bob), 50_000, 1)
    cmd = encode_transfer(tx)
    assert len(cmd) == TRANSFER_LEN == 145
    decoded = decode_command(cmd)
    assert decoded == tx and decoded.verify()
    assert decode_command(NOOP_COMMAND) is None


def test_tampered_byte_fails_verification():
    cmd = bytearray(encode_transfer(sign_transfer(_key(1), public_bytes(_key(2)), 10, 1)))
    for position in (1, 40, 70, 80, 100, 144):
        mutated = bytearray(cmd)
        mutated[position] ^= 0x01
        decoded = decode_command(bytes(mutated))
        assert not decoded.verify()


def _ledger_with_alice():
    alice, bob, carol = _key(1), _key(2), _key(3)
    genesis = {public_bytes(alice): 50_000}
    return ReferenceLedger(genesis), alice, public_bytes(bob), public_bytes(carol)


def test_reference_ledger_double_spend_same_nonce():
    ledger, alice, bob, carol = _ledger_with_alice()
    first = encode_transfer(sign_transfer(alice, bob, 50_000, 1))
    second = encode_transfer(sign_transfer(alice, carol, 50_000, 1))
    assert ledger.apply(LogEntry(1, 1, first)).status == TxStatus.APPLIED
    assert ledger.apply(LogEntry(2, 1, second)).status == TxStatus.REJECTED_BAD_NONCE
    assert ledger.total_balance() == ledger.total_supply()


def test_reference_ledger_double_spend_consecutive_nonces():
    ledger, alice, bob, carol = _ledger_with_alice()
    first = encode_transfer(sign_transfer(alice, bob, 50_000, 1))
    second = encode_transfer(sign_transfer(alice, carol, 50_000, 2))
    assert ledger.apply(LogEntry(1, 1, first)).status == TxStatus.APPLIED
    assert ledger.apply(LogEntry(2, 1, second)).status == TxStatus.REJECTED_INSUFFICIENT_FUNDS
    assert ledger.get_receipt(tx_id(second)) is not None


def test_in_memory_log_store_append_truncate_compact():
    store = InMemoryLogStore()
    store.append([LogEntry(i, 1, b"x") for i in range(1, 6)])
    assert store.last_index() == 5 and store.term_at(3) == 1
    store.truncate_from(4)
    assert store.last_index() == 3
    store.compact_to(2, 1)
    assert store.snapshot_index() == 2 and store.term_at(2) == 1 and store.term_at(1) is None
    assert [e.index for e in store.entries(3, 10)] == [3]
    with pytest.raises(IndexError):
        store.entries(2, 4)
    with pytest.raises(ValueError):
        store.append([LogEntry(9, 1, b"gap")])


def test_in_memory_snapshot_store_round_trip():
    store = InMemorySnapshotStore()
    assert store.load_latest() is None
    snap = Snapshot(5, 2, b"data")
    store.save(snap)
    assert store.load_latest() == snap


def test_recording_state_machine_snapshot_restore():
    sm = RecordingStateMachine()
    for i in range(1, 4):
        sm.apply(LogEntry(i, 1, bytes([i])))
    snap = sm.take_snapshot()
    other = RecordingStateMachine()
    other.restore(snap)
    assert other.last_applied() == 3 and other.state_hash() == sm.state_hash()


async def test_single_node_consensus_propose_then_wait():
    ledger, alice, bob, _ = _ledger_with_alice()
    consensus = SingleNodeConsensus("n1", ledger)
    cmd = encode_transfer(sign_transfer(alice, bob, 100, 1))
    index, term = await consensus.propose(cmd)
    result = await consensus.wait_for_apply(index, term, timeout=1.0)
    assert result.status == TxStatus.APPLIED
