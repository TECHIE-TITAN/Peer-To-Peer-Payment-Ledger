"""LedgerClient (V7.6) against real grpc.aio servers running the V7.4 services on the single-node fakes.

Leader redirects, dead nodes, RETRY and the two ways a TIMEOUT is resolved are all exercised end to end.
"""

from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.client import COMMITTED, FAILED, NOT_APPLIED, PRECHECK_FAILED, LedgerClient
from ledgernet.common.errors import LeadershipLostError
from ledgernet.common.interfaces import Consensus
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import TxStatus
from ledgernet.net.services import LedgerServicer, register_services
from ledgernet.testing.fakes import ReferenceLedger, SingleNodeConsensus

RS_500 = 50_000  # paise


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
    return {name: Wallet(Ed25519PrivateKey.generate()) for name in ("alice", "bob")}


class Scripted(Consensus):
    """Wraps a real SingleNodeConsensus; `script` says how each wait_for_apply call behaves, in order:
    "ok", "lost" (LeadershipLostError), "timeout" (TimeoutError before applying anything is reported) or
    "timeout_after_apply" (the entry is applied, but the reply is lost). Afterwards: "ok"."""

    def __init__(self, inner: SingleNodeConsensus, script: list[str]) -> None:
        self.node_id = inner.node_id
        self._inner = inner
        self._script = list(script)

    async def start(self):
        await self._inner.start()

    async def stop(self):
        await self._inner.stop()

    async def propose(self, command):
        return await self._inner.propose(command)

    async def wait_for_apply(self, index, term, timeout):
        step = self._script.pop(0) if self._script else "ok"
        if step == "lost":
            raise LeadershipLostError("scripted")
        if step == "timeout":
            raise asyncio.TimeoutError("scripted")
        result = await self._inner.wait_for_apply(index, term, timeout)
        if step == "timeout_after_apply":
            raise asyncio.TimeoutError("scripted")
        return result

    def status(self):
        return self._inner.status()


class Server:
    def __init__(self, server: grpc.aio.Server, address: str, ledger: ReferenceLedger) -> None:
        self.server, self.address, self.ledger = server, address, ledger

    async def stop(self) -> None:
        await self.server.stop(0)


async def _serve(wallets, node_id="n1", leader=True, hint=None, hint_address="", script=None) -> Server:
    ledger = ReferenceLedger({wallets["alice"].pub: RS_500, wallets["bob"].pub: 0})
    consensus: Consensus = SingleNodeConsensus(node_id, ledger, leader, hint)
    if script is not None:
        consensus = Scripted(consensus, script)
    server = grpc.aio.server()
    addresses = {hint: hint_address} if hint else {}
    register_services(server, LedgerServicer(consensus, ledger, addresses, 2.0))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return Server(server, f"127.0.0.1:{port}", ledger)


def _free_address() -> str:
    """An address nothing listens on."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{s.getsockname()[1]}"


def _client(*addresses: str) -> LedgerClient:
    return LedgerClient(list(addresses), rpc_timeout_s=2.0, backoff_initial_s=0.01, backoff_max_s=0.05)


@pytest.fixture
async def servers():
    started: list[Server] = []
    yield started
    for s in started:
        await s.stop()


# ---- basics


async def test_submit_commits_on_the_leader(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    alice, bob = wallets["alice"], wallets["bob"]
    async with _client(leader.address) as client:
        result = await client.submit(alice.transfer(bob, 20_000, 1))

        assert result.outcome == COMMITTED
        assert result.tx_status == TxStatus.APPLIED
        assert (result.index, result.attempts) == (1, 1)
        assert result.block_hash == leader.ledger.chain_head()
        assert await client.get_account(alice.pub) == (30_000, 1)
        assert await client.get_account(bob.pub) == (20_000, 0)
        unknown = await client.get_receipt(b"\x00" * 32)
        assert unknown.found is False


async def test_receipt_is_found_for_the_exact_bytes(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    command = wallets["alice"].transfer(wallets["bob"], 100, 1)
    async with _client(leader.address) as client:
        await client.submit(command)
        receipt = await client.get_receipt(tx_id(command))
        assert receipt.found and receipt.index == 1 and receipt.tx_status == TxStatus.APPLIED


async def test_precheck_failure_is_final_and_not_retried(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    command = bytearray(wallets["alice"].transfer(wallets["bob"], 100, 1))
    command[-1] ^= 0x01
    async with _client(leader.address) as client:
        result = await client.submit(bytes(command))
    assert result.outcome == PRECHECK_FAILED
    assert result.tx_status == TxStatus.REJECTED_BAD_SIGNATURE
    assert result.attempts == 1


async def test_ledger_rejection_is_a_committed_outcome(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    command = wallets["alice"].transfer(wallets["bob"], RS_500 + 1, 1)
    async with _client(leader.address) as client:
        result = await client.submit(command)
    assert result.outcome == COMMITTED
    assert result.tx_status == TxStatus.REJECTED_INSUFFICIENT_FUNDS


# ---- finding the leader


async def test_follower_redirects_the_client_to_the_leader(servers, wallets):
    leader = await _serve(wallets, "n1")
    follower = await _serve(wallets, "n2", leader=False, hint="n1", hint_address=leader.address)
    servers.extend([leader, follower])
    alice, bob = wallets["alice"], wallets["bob"]
    async with _client(follower.address, leader.address) as client:
        first = await client.submit(alice.transfer(bob, 100, 1))
        assert first.outcome == COMMITTED
        assert first.attempts == 2  # follower said NOT_LEADER, then the leader answered
        assert (first.node_first, first.node_final) == (follower.address, leader.address)

        second = await client.submit(alice.transfer(bob, 100, 2))
        assert second.attempts == 1  # the leader is cached now
        assert second.node_first == leader.address


async def test_follower_without_a_hint_makes_the_client_try_the_next_node(servers, wallets):
    follower = await _serve(wallets, "n2", leader=False)
    leader = await _serve(wallets, "n1")
    servers.extend([follower, leader])
    async with _client(follower.address, leader.address) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
    assert result.outcome == COMMITTED
    assert result.node_final == leader.address


async def test_hint_naming_an_unconfigured_leader_is_followed(servers, wallets):
    leader = await _serve(wallets, "n1")
    follower = await _serve(wallets, "n2", leader=False, hint="n1", hint_address=leader.address)
    servers.extend([leader, follower])
    async with _client(follower.address) as client:  # the client was never told about the leader
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
        assert result.outcome == COMMITTED and result.node_final == leader.address
        assert leader.address in client.addresses


async def test_dead_node_is_skipped(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    async with _client(_free_address(), leader.address) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
    assert result.outcome == COMMITTED
    assert result.attempts == 2
    assert result.node_final == leader.address


async def test_all_nodes_down_gives_up_with_failed(wallets):
    async with _client(_free_address(), _free_address()) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1), max_attempts=3)
    assert result.outcome == FAILED
    assert result.attempts == 3


async def test_pinning_a_node(servers, wallets):
    a = await _serve(wallets, "n1")
    b = await _serve(wallets, "n2")
    servers.extend([a, b])
    async with _client(a.address, b.address) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1), node=b.address)
    assert result.node_first == b.address and result.node_final == b.address
    assert b.ledger.last_applied() == 1 and a.ledger.last_applied() == 0


# ---- RETRY and TIMEOUT


async def test_retry_resubmits_the_same_bytes(servers, wallets):
    node = await _serve(wallets, script=["lost"])
    servers.append(node)
    alice, bob = wallets["alice"], wallets["bob"]
    async with _client(node.address) as client:
        result = await client.submit(alice.transfer(bob, 20_000, 1))
        assert result.outcome == COMMITTED
        assert result.attempts == 2
        # The first copy was applied locally in this fake, so the second hit the nonce check: money moved once.
        assert await client.get_account(alice.pub) == (30_000, 1)
        assert await client.get_account(bob.pub) == (20_000, 0)
    assert node.ledger.total_balance() == node.ledger.total_supply()


async def test_timeout_after_apply_is_resolved_by_the_receipt(servers, wallets):
    node = await _serve(wallets, script=["timeout_after_apply"])
    servers.append(node)
    alice, bob = wallets["alice"], wallets["bob"]
    async with _client(node.address) as client:
        result = await client.submit(alice.transfer(bob, 20_000, 1))
        assert result.outcome == COMMITTED
        assert result.tx_status == TxStatus.APPLIED
        assert result.attempts == 1  # resolved through GetReceipt, no resubmission
        assert result.index == 1
        assert await client.get_account(alice.pub) == (30_000, 1)  # applied exactly once


async def test_timeout_with_a_spent_nonce_is_not_applied(servers, wallets):
    """Nonce 1 is consumed by one transfer; a different transfer with nonce 1 times out and can never apply."""
    node = await _serve(wallets, script=["ok", "timeout"])
    servers.append(node)
    alice, bob = wallets["alice"], wallets["bob"]
    async with _client(node.address) as client:
        assert (await client.submit(alice.transfer(bob, 100, 1))).outcome == COMMITTED
        conflicting = alice.transfer(bob, 999, 1)  # same sender and nonce, different amount
        result = await client.submit(conflicting)
        assert result.outcome == NOT_APPLIED
        assert result.attempts == 1
        assert await client.get_account(alice.pub) == (RS_500 - 100, 1)


async def test_unresolved_timeouts_end_in_failed_after_resubmitting(servers, wallets):
    """A transfer with a future nonce is rejected without a receipt and without spending the nonce, so a
    timeout on it stays undecided: the client resubmits each time, then gives up with FAILED."""
    node = await _serve(wallets, script=["timeout"] * 3)
    servers.append(node)
    async with _client(node.address) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 5), max_attempts=3)
    assert result.outcome == FAILED
    assert result.attempts == 3


# ---- status


async def test_status_all_reports_unreachable_nodes_as_none(servers, wallets):
    leader = await _serve(wallets, "n1")
    servers.append(leader)
    dead = _free_address()
    async with _client(leader.address, dead) as client:
        statuses = await client.status_all()
        assert set(statuses) == {leader.address, dead}
        assert statuses[dead] is None
        assert statuses[leader.address].node_id == "n1"
        assert statuses[leader.address].role == "leader"
        assert (await client.status(leader.address)).total_balance == RS_500


def test_no_addresses_is_an_error():
    with pytest.raises(ValueError):
        LedgerClient([])


def test_a_name_to_address_mapping_is_accepted():
    client = LedgerClient({"n1": "127.0.0.1:1", "n2": "127.0.0.1:2"})
    assert client.addresses == ["127.0.0.1:1", "127.0.0.1:2"]


# ---- named nodes (a {name: address} mapping)


async def test_named_nodes_are_accepted_and_reported_by_name(servers, wallets):
    leader = await _serve(wallets, "n1")
    follower = await _serve(wallets, "n2", leader=False, hint="n1", hint_address=leader.address)
    servers.extend([leader, follower])
    names = {"n2": follower.address, "n1": leader.address}
    async with LedgerClient(names, rpc_timeout_s=2.0, backoff_initial_s=0.01) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1), node="n2")
        assert result.outcome == COMMITTED
        assert (result.node_first, result.node_final, result.attempts) == ("n2", "n1", 2)
        assert await client.get_account(wallets["bob"].pub, "n1") == (100, 0)
        assert (await client.status("n2")).role == "follower"


async def test_unnamed_nodes_are_reported_by_address(servers, wallets):
    leader = await _serve(wallets)
    servers.append(leader)
    async with _client(leader.address) as client:
        result = await client.submit(wallets["alice"].transfer(wallets["bob"], 100, 1))
    assert result.node_first == result.node_final == leader.address
