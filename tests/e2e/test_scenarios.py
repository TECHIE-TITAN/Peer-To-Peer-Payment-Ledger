"""End-to-end scenarios S1a, S1b, S2, S4, S5 and S8 (docs/implementation.md Section 10) on 5 real processes.

Each ends with `finish()`: convergence, equal hashes, conservation, then A's auditor on every data directory.
"""

from __future__ import annotations

import asyncio

import grpc
import pytest
from e2e_support import RS, Wallet, finish

from ledgernet.client import COMMITTED, NOT_APPLIED, PRECHECK_FAILED
from ledgernet.common.tx import tx_id
from ledgernet.common.types import TxStatus
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc

RS_500 = 500 * RS


@pytest.fixture
def people():
    return {name: Wallet.new() for name in ("alice", "bob", "carol")}


async def start(make_cluster, people, **kwargs):
    cluster = await make_cluster({people["alice"]: RS_500, people["bob"]: 0, people["carol"]: 0}, **kwargs)
    await cluster.wait_for_leader()
    return cluster


async def balance(cluster, wallet, node=None):
    return (await cluster.client.get_account(wallet.pub, node))[0]


# ---- S1a / S1b: conflicting transfers


async def test_s1a_same_nonce_double_spend_is_stopped(make_cluster, people):
    alice, bob, carol = people["alice"], people["bob"], people["carol"]
    cluster = await start(make_cluster, people)
    to_bob = alice.transfer(bob, RS_500, nonce=1)
    to_carol = alice.transfer(carol, RS_500, nonce=1)

    first, second = await asyncio.gather(
        cluster.client.submit(to_bob, node="n1"), cluster.client.submit(to_carol, node="n4")
    )

    assert first.outcome == second.outcome == COMMITTED
    assert sorted([first.tx_status, second.tx_status]) == [TxStatus.APPLIED, TxStatus.REJECTED_BAD_NONCE]
    await cluster.wait_converged()
    received = [await balance(cluster, bob), await balance(cluster, carol)]
    assert sorted(received) == [0, RS_500]  # exactly one of them got paid
    assert await balance(cluster, alice) == 0
    for node_id in cluster.node_ids:  # and every node agrees
        assert await balance(cluster, alice, node_id) == 0
        assert [await balance(cluster, bob, node_id), await balance(cluster, carol, node_id)] == received
    await finish(cluster, RS_500)


async def test_s1b_consecutive_nonces_cannot_spend_the_same_money_twice(make_cluster, people):
    alice, bob, carol = people["alice"], people["bob"], people["carol"]
    cluster = await start(make_cluster, people)
    first_tx = alice.transfer(bob, RS_500, nonce=1)
    second_tx = alice.transfer(carol, RS_500, nonce=2)

    results = await asyncio.gather(
        cluster.client.submit(first_tx, node="n1"), cluster.client.submit(second_tx, node="n4")
    )

    assert all(r.outcome == COMMITTED for r in results)
    statuses = [r.tx_status for r in results]
    assert statuses.count(TxStatus.APPLIED) == 1  # never two, whichever the leader orders first
    others = [s for s in statuses if s != TxStatus.APPLIED]
    assert others[0] in (TxStatus.REJECTED_INSUFFICIENT_FUNDS, TxStatus.REJECTED_BAD_NONCE)
    await cluster.wait_converged()
    assert await balance(cluster, alice) == 0
    assert await balance(cluster, bob) + await balance(cluster, carol) == RS_500
    await finish(cluster, RS_500)


# ---- S4: replay


async def test_s4_replayed_transfer_is_rejected_on_every_node(make_cluster, people):
    alice, bob = people["alice"], people["bob"]
    cluster = await start(make_cluster, people)
    command = alice.transfer(bob, 100 * RS, nonce=1)
    assert (await cluster.client.submit(command)).tx_status == TxStatus.APPLIED
    await cluster.wait_converged()

    for node_id in ("n1", "n3"):  # the same node and a different one
        again = await cluster.client.submit(command, node=node_id)
        assert again.outcome == COMMITTED
        assert again.tx_status == TxStatus.REJECTED_BAD_NONCE
    await cluster.wait_converged()
    for node_id in cluster.node_ids:
        assert await balance(cluster, bob, node_id) == 100 * RS
        assert await balance(cluster, alice, node_id) == RS_500 - 100 * RS
    await finish(cluster, RS_500)


# ---- S5: forged signature


async def test_s5_forged_signature_never_changes_state(make_cluster, people):
    alice, bob = people["alice"], people["bob"]
    cluster = await start(make_cluster, people, unsafe_debug=True)
    forged = bytearray(alice.transfer(bob, 100 * RS, nonce=1))
    forged[-1] ^= 0x01  # flip one signature bit

    rejected = await cluster.client.submit(bytes(forged))
    assert rejected.outcome == PRECHECK_FAILED
    assert rejected.tx_status == TxStatus.REJECTED_BAD_SIGNATURE

    leader = await cluster.wait_for_leader()
    async with grpc.aio.insecure_channel(cluster.address(leader)) as channel:  # bypass the precheck
        response = await pb_grpc.LedgerStub(channel).SubmitTransfer(
            pb.SubmitTransferRequest(command=bytes(forged), skip_precheck=True), timeout=10
        )
    assert response.outcome == pb.SubmitTransferResponse.COMMITTED  # it did enter the log
    assert response.tx_status == TxStatus.REJECTED_BAD_SIGNATURE  # and the ledger rejected it

    await cluster.wait_converged()
    for node_id in cluster.node_ids:  # identical verdict everywhere, nothing moved, nonce not consumed
        assert await cluster.client.get_account(alice.pub, node_id) == (RS_500, 0)
    assert (await cluster.client.submit(alice.transfer(bob, RS, nonce=1))).tx_status == TxStatus.APPLIED
    await finish(cluster, RS_500)


# ---- S8: whole-cluster restart


async def test_s8_full_cluster_sigkill_and_restart_recovers_every_committed_transfer(make_cluster, people):
    alice, bob = people["alice"], people["bob"]
    cluster = await start(make_cluster, people)
    for nonce in range(1, 21):
        result = await cluster.client.submit(alice.transfer(bob, RS, nonce))
        assert result.tx_status == TxStatus.APPLIED
    await cluster.wait_converged()

    for node_id in cluster.node_ids:
        cluster.kill(node_id)  # SIGKILL everything: only what reached disk survives
    for node_id in cluster.node_ids:
        await cluster.start(node_id, wait=False)
    for node_id in cluster.node_ids:
        await cluster.wait_ready(node_id)
    await cluster.wait_for_leader()
    await cluster.wait_converged()

    assert await cluster.client.get_account(bob.pub) == (20 * RS, 0)
    assert await cluster.client.get_account(alice.pub) == (RS_500 - 20 * RS, 20)
    assert (await cluster.client.submit(alice.transfer(bob, RS, nonce=21))).tx_status == TxStatus.APPLIED
    await finish(cluster, RS_500)


# ---- S2: leader failure under load


async def _sender_loop(cluster, wallet, sink, stop, acked):
    """One account sending 1-paise transfers in sequence; records the tx_id of every APPLIED one."""
    nonce = 1
    while not stop.is_set():
        await asyncio.sleep(0.02)  # about 50 transfers per second per account
        command = wallet.transfer(sink, 1, nonce)
        result = await cluster.client.submit(command, max_attempts=6)
        if result.outcome == COMMITTED:
            if result.tx_status == TxStatus.APPLIED:
                acked.append(tx_id(command))
            nonce += 1
            continue
        if result.outcome == NOT_APPLIED:
            nonce += 1  # that nonce was taken by an earlier copy of ours
            continue
        await asyncio.sleep(0.1)  # PRECHECK_FAILED cannot happen here; FAILED: try the same bytes again


async def _missing_receipts(cluster, node_id, tx_ids, chunk=100):
    """The tx_ids this node has no receipt for. Looked up in chunks: thousands of simultaneous RPCs on one
    channel get cancelled, which would be a test artifact, not a lost transfer."""
    missing = []
    for start in range(0, len(tx_ids), chunk):
        batch = tx_ids[start : start + chunk]
        receipts = await asyncio.gather(*(cluster.client.get_receipt(t, node_id) for t in batch))
        missing += [t for t, r in zip(batch, receipts) if not r.found]
    return missing


async def test_s2_leader_sigkill_under_load_loses_no_acknowledged_transfer(make_cluster):
    senders = [Wallet.new() for _ in range(6)]
    sink = Wallet.new()
    balances = {w: 1_000 * RS for w in senders}
    balances[sink] = 0
    cluster = await make_cluster(balances)
    await cluster.wait_for_leader()

    stop, acked = asyncio.Event(), []
    loops = [asyncio.create_task(_sender_loop(cluster, w, sink, stop, acked)) for w in senders]
    try:
        await asyncio.sleep(3)
        before_kill = len(acked)
        assert before_kill > 0, "no load got through before the crash"

        victim = await cluster.wait_for_leader()
        cluster.kill(victim)
        survivor_leader = await cluster.wait_for_leader(exclude=[victim])
        assert survivor_leader != victim
        await asyncio.sleep(3)
        after_failover = len(acked)
        assert after_failover > before_kill, "commits did not resume after the leader died"

        await cluster.start(victim)  # same data_dir: it recovers and catches up
        await asyncio.sleep(3)
    finally:
        stop.set()
        await asyncio.gather(*loops, return_exceptions=True)

    await cluster.wait_converged(timeout=60)
    assert len(acked) > after_failover
    # Every acknowledged transfer has a receipt on every node, so none was lost in the failover.
    for node_id in cluster.node_ids:
        missing = await _missing_receipts(cluster, node_id, acked)
        assert not missing, f"{node_id} lacks receipts for {len(missing)} of {len(acked)} acknowledged"
    supply = sum(balances.values())
    await finish(cluster, supply)
