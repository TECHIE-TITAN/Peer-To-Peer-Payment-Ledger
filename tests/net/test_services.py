"""Ledger and Admin services (V7.4), tested over a real grpc.aio server.

The node runs on SingleNodeConsensus + ReferenceLedger.

Nothing here needs Raft, SQLite or a real network: the same servicers run unchanged after integration.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.common.errors import LeadershipLostError
from ledgernet.common.interfaces import Consensus
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import ApplyResult, RaftStatus, TxStatus
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.net.services import AdminServicer, LedgerServicer, register_services
from ledgernet.testing.fakes import ReferenceLedger, SingleNodeConsensus

Outcome = pb.SubmitTransferResponse.Outcome
RS_500 = 50_000  # paise
ADDRESSES = {"n1": "127.0.0.1:50051", "n2": "127.0.0.1:50052"}


@dataclass
class Wallet:
    priv: Ed25519PrivateKey

    @property
    def pub(self) -> bytes:
        return public_bytes(self.priv)

    def transfer(self, to: Wallet, amount: int, nonce: int) -> bytes:
        return encode_transfer(sign_transfer(self.priv, to.pub, amount, nonce))


@pytest.fixture
def wallets():
    return {name: Wallet(Ed25519PrivateKey.generate()) for name in ("alice", "bob", "carol")}


class FakeInjector:
    def __init__(self) -> None:
        self.rules: list = []
        self.cleared = 0

    def set_rules(self, rules) -> None:
        self.rules = list(rules)

    def clear(self) -> None:
        self.rules = []
        self.cleared += 1


class ScriptedConsensus(Consensus):
    """Consensus whose wait_for_apply misbehaves on demand (timeout / lost leadership)."""

    def __init__(self, wait_error: BaseException) -> None:
        self.node_id = "n1"
        self._wait_error = wait_error

    async def start(self): ...
    async def stop(self): ...

    async def propose(self, command):
        return 7, 3

    async def wait_for_apply(self, index, term, timeout):
        raise self._wait_error

    def status(self):
        return RaftStatus("n1", "leader", 3, "n1", 0, 0, 7, 0)


class Node:
    """A running grpc.aio server plus a client stub, for one test."""

    def __init__(self, consensus, ledger, injector, unsafe_debug):
        self.consensus, self.ledger, self.injector = consensus, ledger, injector
        self.unsafe_debug = unsafe_debug
        self.server = None
        self.channel = None
        self.ledger_stub = None
        self.admin_stub = None

    async def start(self) -> None:
        self.server = grpc.aio.server()
        register_services(
            self.server,
            LedgerServicer(self.consensus, self.ledger, ADDRESSES, 2.0, self.unsafe_debug),
            AdminServicer(self.injector),
        )
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.ledger_stub = pb_grpc.LedgerStub(self.channel)
        self.admin_stub = pb_grpc.AdminStub(self.channel)

    async def stop(self) -> None:
        await self.channel.close()
        await self.server.stop(0)

    async def submit(self, command: bytes, skip_precheck: bool = False) -> pb.SubmitTransferResponse:
        return await self.ledger_stub.SubmitTransfer(
            pb.SubmitTransferRequest(command=command, skip_precheck=skip_precheck)
        )

    async def account(self, wallet: Wallet) -> tuple[int, int]:
        resp = await self.ledger_stub.GetAccount(pb.GetAccountRequest(pubkey=wallet.pub))
        return resp.balance, resp.nonce


async def _start_node(wallets, consensus_factory=None, leader=True, hint=None, unsafe_debug=False):
    genesis = {wallets["alice"].pub: RS_500, wallets["bob"].pub: 0, wallets["carol"].pub: 0}
    ledger = ReferenceLedger(genesis)
    consensus = (
        consensus_factory(ledger) if consensus_factory else SingleNodeConsensus("n1", ledger, leader, hint)
    )
    node = Node(consensus, ledger, FakeInjector(), unsafe_debug)
    await node.start()
    return node


@pytest.fixture
async def node(wallets):
    n = await _start_node(wallets)
    yield n
    await n.stop()


@pytest.fixture
async def debug_node(wallets):
    n = await _start_node(wallets, unsafe_debug=True)
    yield n
    await n.stop()


# ---- SubmitTransfer: happy path


async def test_committed_transfer(node, wallets):
    alice, bob = wallets["alice"], wallets["bob"]
    resp = await node.submit(alice.transfer(bob, 20_000, nonce=1))

    assert resp.outcome == Outcome.COMMITTED
    assert resp.tx_status == TxStatus.APPLIED
    assert resp.index == 1
    assert resp.term == 1
    assert resp.block_hash == node.ledger.chain_head()
    assert await node.account(alice) == (30_000, 1)
    assert await node.account(bob) == (20_000, 0)
    assert node.ledger.total_balance() == node.ledger.total_supply()


# ---- precheck


async def test_bad_signature_is_rejected_before_the_log(node, wallets):
    command = bytearray(wallets["alice"].transfer(wallets["bob"], 100, 1))
    command[-1] ^= 0x01  # flip one signature bit
    resp = await node.submit(bytes(command))

    assert resp.outcome == Outcome.PRECHECK_FAILED
    assert resp.tx_status == TxStatus.REJECTED_BAD_SIGNATURE
    assert node.ledger.last_applied() == 0  # never entered the log


@pytest.mark.parametrize("command", [b"", b"\x01" + b"\x00" * 10, b"\x02" + b"\x00" * 144, b"\x01" * 146])
async def test_bad_format_is_rejected_before_the_log(node, command):
    resp = await node.submit(command)
    assert resp.outcome == Outcome.PRECHECK_FAILED
    assert resp.tx_status == TxStatus.REJECTED_BAD_FORMAT
    assert node.ledger.last_applied() == 0


async def test_clients_cannot_submit_a_noop(node):
    resp = await node.submit(b"\x00")
    assert resp.outcome == Outcome.PRECHECK_FAILED
    assert resp.tx_status == TxStatus.REJECTED_BAD_FORMAT
    assert node.ledger.last_applied() == 0


async def test_skip_precheck_is_ignored_without_unsafe_debug(node, wallets):
    command = bytearray(wallets["alice"].transfer(wallets["bob"], 100, 1))
    command[-1] ^= 0x01
    resp = await node.submit(bytes(command), skip_precheck=True)
    assert resp.outcome == Outcome.PRECHECK_FAILED
    assert node.ledger.last_applied() == 0


async def test_forged_transfer_through_the_log_is_rejected_by_the_ledger(debug_node, wallets):
    """S5: with skip_precheck on an --unsafe-debug node the forgery reaches the log and is rejected there."""
    command = bytearray(wallets["alice"].transfer(wallets["bob"], 100, 1))
    command[-1] ^= 0x01
    resp = await debug_node.submit(bytes(command), skip_precheck=True)

    assert resp.outcome == Outcome.COMMITTED
    assert resp.tx_status == TxStatus.REJECTED_BAD_SIGNATURE
    assert resp.index == 1  # it did enter the log
    assert await debug_node.account(wallets["alice"]) == (RS_500, 0)  # nothing moved


async def test_unsafe_debug_node_still_prechecks_by_default(debug_node, wallets):
    command = bytearray(wallets["alice"].transfer(wallets["bob"], 100, 1))
    command[-1] ^= 0x01
    resp = await debug_node.submit(bytes(command))  # skip_precheck not requested
    assert resp.outcome == Outcome.PRECHECK_FAILED


# ---- redirects, retries, timeouts


async def test_not_leader_redirects_with_address(wallets):
    node = await _start_node(wallets, leader=False, hint="n2")
    try:
        resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert resp.outcome == Outcome.NOT_LEADER
        assert resp.leader_hint == "n2"
        assert resp.leader_address == ADDRESSES["n2"]
        assert node.ledger.last_applied() == 0
    finally:
        await node.stop()


async def test_not_leader_without_a_known_leader(wallets):
    node = await _start_node(wallets, leader=False, hint=None)
    try:
        resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert resp.outcome == Outcome.NOT_LEADER
        assert resp.leader_hint == ""
        assert resp.leader_address == ""
    finally:
        await node.stop()


async def test_not_leader_with_unknown_hint_has_no_address(wallets):
    node = await _start_node(wallets, leader=False, hint="n9")
    try:
        resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert resp.leader_hint == "n9"
        assert resp.leader_address == ""
    finally:
        await node.stop()


async def test_lost_leadership_answers_retry(wallets):
    node = await _start_node(
        wallets, consensus_factory=lambda _l: ScriptedConsensus(LeadershipLostError("x"))
    )
    try:
        resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert resp.outcome == Outcome.RETRY
        assert (resp.index, resp.term) == (7, 3)
    finally:
        await node.stop()


async def test_wait_timeout_answers_timeout(wallets):
    node = await _start_node(wallets, consensus_factory=lambda _l: ScriptedConsensus(asyncio.TimeoutError()))
    try:
        resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert resp.outcome == Outcome.TIMEOUT
        assert (resp.index, resp.term) == (7, 3)
    finally:
        await node.stop()


# ---- double spend and replay (S1, S4)


async def test_double_spend_same_nonce(node, wallets):
    """S1a: both transfers sent at once; exactly one applies, the other gets BAD_NONCE."""
    alice, bob, carol = wallets["alice"], wallets["bob"], wallets["carol"]
    t_bob = alice.transfer(bob, RS_500, nonce=1)
    t_carol = alice.transfer(carol, RS_500, nonce=1)

    r1, r2 = await asyncio.gather(node.submit(t_bob), node.submit(t_carol))

    statuses = sorted([r1.tx_status, r2.tx_status])
    assert statuses == [TxStatus.APPLIED, TxStatus.REJECTED_BAD_NONCE]
    assert await node.account(alice) == (0, 1)
    bob_balance, _ = await node.account(bob)
    carol_balance, _ = await node.account(carol)
    assert bob_balance + carol_balance == RS_500
    assert node.ledger.total_balance() == node.ledger.total_supply()


async def test_double_spend_consecutive_nonces(node, wallets):
    """S1b: nonces 1 and 2; the second overdraws. Its nonce is still consumed."""
    alice, bob, carol = wallets["alice"], wallets["bob"], wallets["carol"]
    first = await node.submit(alice.transfer(bob, RS_500, nonce=1))
    second = await node.submit(alice.transfer(carol, RS_500, nonce=2))

    assert first.tx_status == TxStatus.APPLIED
    assert second.tx_status == TxStatus.REJECTED_INSUFFICIENT_FUNDS
    assert await node.account(alice) == (0, 2)  # nonce consumed
    assert await node.account(carol) == (0, 0)


async def test_replay_is_rejected(node, wallets):
    """S4: resubmitting an applied command gives BAD_NONCE and changes nothing."""
    command = wallets["alice"].transfer(wallets["bob"], 10_000, nonce=1)
    first = await node.submit(command)
    replay = await node.submit(command)

    assert first.tx_status == TxStatus.APPLIED
    assert replay.outcome == Outcome.COMMITTED
    assert replay.tx_status == TxStatus.REJECTED_BAD_NONCE
    assert await node.account(wallets["alice"]) == (RS_500 - 10_000, 1)
    assert await node.account(wallets["bob"]) == (10_000, 0)


async def test_wrong_nonce_is_rejected(node, wallets):
    resp = await node.submit(wallets["alice"].transfer(wallets["bob"], 100, nonce=5))
    assert resp.tx_status == TxStatus.REJECTED_BAD_NONCE


# ---- reads


async def test_receipt_for_applied_and_overdraft_but_not_for_bad_nonce(node, wallets):
    alice, bob = wallets["alice"], wallets["bob"]
    applied = alice.transfer(bob, RS_500, nonce=1)
    overdraft = alice.transfer(bob, 1, nonce=2)
    bad_nonce = alice.transfer(bob, 1, nonce=9)
    for command in (applied, overdraft, bad_nonce):
        await node.submit(command)

    async def receipt(command: bytes) -> pb.GetReceiptResponse:
        return await node.ledger_stub.GetReceipt(pb.GetReceiptRequest(tx_id=tx_id(command)))

    got = await receipt(applied)
    assert got.found and got.tx_status == TxStatus.APPLIED and got.index == 1
    got = await receipt(overdraft)
    assert got.found and got.tx_status == TxStatus.REJECTED_INSUFFICIENT_FUNDS and got.index == 2
    assert not (await receipt(bad_nonce)).found


async def test_get_account_unknown_and_as_of_index(node, wallets):
    stranger = Wallet(Ed25519PrivateKey.generate())
    resp = await node.ledger_stub.GetAccount(pb.GetAccountRequest(pubkey=stranger.pub))
    assert (resp.balance, resp.nonce, resp.as_of_index) == (0, 0, 0)

    await node.submit(wallets["alice"].transfer(wallets["bob"], 1, nonce=1))
    resp = await node.ledger_stub.GetAccount(pb.GetAccountRequest(pubkey=wallets["bob"].pub))
    assert (resp.balance, resp.as_of_index) == (1, 1)


@pytest.mark.parametrize("bad", [b"", b"\x01" * 31, b"\x01" * 33])
async def test_get_account_rejects_bad_pubkey_length(node, bad):
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await node.ledger_stub.GetAccount(pb.GetAccountRequest(pubkey=bad))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_get_receipt_rejects_bad_tx_id_length(node):
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await node.ledger_stub.GetReceipt(pb.GetReceiptRequest(tx_id=b"short"))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_get_status(node, wallets):
    await node.submit(wallets["alice"].transfer(wallets["bob"], 5, nonce=1))
    status = await node.ledger_stub.GetStatus(pb.GetStatusRequest())

    assert status.node_id == "n1"
    assert status.role == "leader"
    assert status.leader_id == "n1"
    assert status.last_applied == 1
    assert status.chain_head == node.ledger.chain_head()
    assert status.state_hash == node.ledger.state_hash()
    assert status.total_balance == status.total_supply == RS_500


async def test_get_status_on_a_follower_without_leader(wallets):
    node = await _start_node(wallets, leader=False, hint=None)
    try:
        status = await node.ledger_stub.GetStatus(pb.GetStatusRequest())
        assert status.role == "follower"
        assert status.leader_id == ""
    finally:
        await node.stop()


async def test_two_nodes_with_same_log_report_same_hashes(wallets):
    """The comparison the demos end with: equal state_hash and chain_head after the same transfers."""
    a = await _start_node(wallets)
    b = await _start_node(wallets)
    try:
        commands = [
            wallets["alice"].transfer(wallets["bob"], 100, 1),
            wallets["alice"].transfer(wallets["carol"], 200, 2),
            wallets["alice"].transfer(wallets["bob"], 10**9, 3),
        ]
        for command in commands:
            await a.submit(command)
            await b.submit(command)
        sa = await a.ledger_stub.GetStatus(pb.GetStatusRequest())
        sb = await b.ledger_stub.GetStatus(pb.GetStatusRequest())
        assert sa.state_hash == sb.state_hash and sa.chain_head == sb.chain_head
    finally:
        await a.stop()
        await b.stop()


# ---- Admin


async def test_set_and_clear_faults(node):
    rule = pb.FaultRule(target="n2", drop_prob=0.2, delay_min_ms=0, delay_max_ms=100, duplicate_prob=0.05)
    partition_all = pb.FaultRule(target="*", partitioned=True)
    await node.admin_stub.SetFaults(pb.SetFaultsRequest(rules=[rule, partition_all]))
    assert [r.target for r in node.injector.rules] == ["n2", "*"]
    assert node.injector.rules[0].drop_prob == pytest.approx(0.2)
    assert node.injector.rules[1].partitioned

    await node.admin_stub.ClearFaults(pb.Empty())
    assert node.injector.rules == []
    assert node.injector.cleared == 1


@pytest.mark.parametrize(
    "rule",
    [
        pb.FaultRule(target="", drop_prob=0.1),
        pb.FaultRule(target="n2", drop_prob=1.5),
        pb.FaultRule(target="n2", duplicate_prob=-0.1),
        pb.FaultRule(target="n2", delay_min_ms=50, delay_max_ms=10),
    ],
)
async def test_invalid_fault_rules_are_rejected(node, rule):
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await node.admin_stub.SetFaults(pb.SetFaultsRequest(rules=[rule]))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert node.injector.rules == []  # nothing half-applied


def test_apply_result_type_is_the_contract():
    """Guard: the servicer relies on ApplyResult carrying these exact fields."""
    assert {"index", "term", "status", "block_hash"} <= set(ApplyResult.__dataclass_fields__)
