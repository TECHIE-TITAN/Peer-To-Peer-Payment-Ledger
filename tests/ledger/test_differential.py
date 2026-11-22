"""SqliteLedger must match ReferenceLedger (the executable spec of Section 3.6) exactly. Task A6.2/A6.3.

Quick suite: 300 examples per property. Acceptance run (>= 10 000 examples):

    LEDGERNET_FUZZ=1 pytest tests/ledger/test_differential.py
"""

from __future__ import annotations

import os
import tempfile

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledgernet.common.tx import encode_transfer, sign_transfer, tx_id
from ledgernet.common.types import LogEntry, TxStatus
from ledgernet.ledger.sqlite_ledger import SqliteLedger
from ledgernet.testing.fakes import ReferenceLedger
from tests.ledger.streams import KEYS, PUBS, stream_strategy

EXAMPLES = 10_000 if os.environ.get("LEDGERNET_FUZZ") else 300
_SETTINGS = settings(max_examples=EXAMPLES, deadline=None, suppress_health_check=[HealthCheck.too_slow])


def _assert_same(ref: ReferenceLedger, sql: SqliteLedger) -> None:
    assert sql.last_applied() == ref.last_applied()
    assert sql.chain_head() == ref.chain_head()
    assert sql.state_hash() == ref.state_hash()
    assert sql.total_balance() == sql.total_supply() == ref.total_supply()


@_SETTINGS
@given(stream=stream_strategy())
def test_sqlite_matches_reference(stream):
    genesis, entries = stream
    with tempfile.TemporaryDirectory() as d:
        ref = ReferenceLedger(genesis)
        sql = SqliteLedger(os.path.join(d, "state.db"), genesis)
        try:
            _assert_same(ref, sql)
            for e in entries:
                expected = ref.apply(e)
                assert sql.apply(e) == expected, f"entry {e.index}"
                _assert_same(ref, sql)
                assert sql.get_receipt(tx_id(e.command)) == ref.get_receipt(tx_id(e.command))
            for pk in PUBS:
                assert sql.get_account(pk) == ref.get_account(pk)
            assert sql.receipt_count() == len(ref.receipts)
        finally:
            sql.close()


@_SETTINGS
@given(stream=stream_strategy(), cut_fraction=st.floats(min_value=0, max_value=1))
def test_snapshot_restore_then_continue(stream, cut_fraction):
    """Snapshot at a random point, restore into a fresh instance, keep applying: hashes match."""
    genesis, entries = stream
    cut = int(len(entries) * cut_fraction)
    with tempfile.TemporaryDirectory() as d:
        whole = SqliteLedger(os.path.join(d, "whole.db"), genesis)
        part = SqliteLedger(os.path.join(d, "part.db"), genesis)
        fresh = SqliteLedger(os.path.join(d, "fresh.db"), genesis)
        try:
            for e in entries[:cut]:
                whole.apply(e)
                part.apply(e)
            snap = part.take_snapshot()
            assert snap.last_included_index == cut
            fresh.restore(snap)
            assert fresh.state_hash() == part.state_hash()
            for e in entries[cut:]:
                assert fresh.apply(e) == whole.apply(e)
            assert fresh.state_hash() == whole.state_hash()
            assert fresh.chain_head() == whole.chain_head()
            for e in entries:
                assert fresh.get_receipt(tx_id(e.command)) == whole.get_receipt(tx_id(e.command))
        finally:
            for x in (whole, part, fresh):
                x.close()


# ---------------------------------------------------------------- targeted cases


@pytest.fixture
def ledger(tmp_path):
    led = SqliteLedger(str(tmp_path / "state.db"), {PUBS[0]: 50_000})
    yield led
    led.close()


def _cmd(s, r, amount, nonce):
    return encode_transfer(sign_transfer(KEYS[s], PUBS[r], amount, nonce))


def test_double_spend_same_nonce(ledger):
    assert ledger.apply(LogEntry(1, 1, _cmd(0, 1, 50_000, 1))).status == TxStatus.APPLIED
    assert ledger.apply(LogEntry(2, 1, _cmd(0, 2, 50_000, 1))).status == TxStatus.REJECTED_BAD_NONCE
    assert ledger.get_account(PUBS[2]) == (0, 0)


def test_double_spend_consecutive_nonces_consumes_nonce(ledger):
    second = _cmd(0, 2, 50_000, 2)
    assert ledger.apply(LogEntry(1, 1, _cmd(0, 1, 50_000, 1))).status == TxStatus.APPLIED
    r = ledger.apply(LogEntry(2, 1, second))
    assert r.status == TxStatus.REJECTED_INSUFFICIENT_FUNDS
    assert ledger.get_account(PUBS[0]) == (0, 2)
    assert ledger.get_receipt(tx_id(second)) == r
    # Funds arrive later; the consumed transfer can never be replayed.
    ledger.apply(LogEntry(3, 1, _cmd(1, 0, 50_000, 1)))
    assert ledger.apply(LogEntry(4, 1, second)).status == TxStatus.REJECTED_BAD_NONCE


def test_unknown_sender_overdraft_creates_account(ledger):
    assert ledger.apply(LogEntry(1, 1, _cmd(3, 0, 1, 1))).status == TxStatus.REJECTED_INSUFFICIENT_FUNDS
    assert ledger.get_account(PUBS[3]) == (0, 1)


def test_out_of_order_apply_rejected(ledger):
    with pytest.raises(ValueError):
        ledger.apply(LogEntry(2, 1, b"\x00"))


def test_huge_amount_is_insufficient_funds(ledger):
    assert (
        ledger.apply(LogEntry(1, 1, _cmd(0, 1, 2**64 - 1, 1))).status == TxStatus.REJECTED_INSUFFICIENT_FUNDS
    )


def test_constructor_starts_from_genesis(tmp_path):
    path = str(tmp_path / "state.db")
    a = SqliteLedger(path, {PUBS[0]: 10})
    a.apply(LogEntry(1, 1, _cmd(0, 1, 5, 1)))
    a.close()
    b = SqliteLedger(path, {PUBS[0]: 10})
    assert b.last_applied() == 0 and b.get_account(PUBS[0]) == (10, 0)
    b.close()


def test_total_supply_limit(tmp_path):
    with pytest.raises(ValueError):
        SqliteLedger(str(tmp_path / "x.db"), {PUBS[0]: 2**62, PUBS[1]: 2**62})


def test_restore_rejects_garbage_and_keeps_state(ledger):
    from ledgernet.common.types import Snapshot

    ledger.apply(LogEntry(1, 1, _cmd(0, 1, 5, 1)))
    before = ledger.state_hash()
    with pytest.raises(ValueError):
        ledger.restore(Snapshot(1, 1, b"not a database"))
    good = ledger.take_snapshot()
    with pytest.raises(ValueError):
        ledger.restore(Snapshot(7, 1, good.data))  # index does not match meta.last_applied
    assert ledger.state_hash() == before
    ledger.apply(LogEntry(2, 1, b"\x00"))


def test_snapshot_is_independent_of_later_applies(ledger):
    snap = ledger.take_snapshot()
    ledger.apply(LogEntry(1, 1, _cmd(0, 1, 5, 1)))
    ledger.restore(snap)
    assert ledger.last_applied() == 0 and ledger.get_account(PUBS[0]) == (50_000, 0)
