"""Ledger and Admin gRPC services (V7.4).

`LedgerServicer` is the client-facing front door of a node. It never decides what a transfer means (that is
the ledger's job, applied identically on every node after Raft commits it); it only:

  1. rejects obvious garbage cheaply (bad format, bad signature) before it can enter the log,
  2. hands the command to Raft (`propose`),
  3. waits until the entry is applied locally and reports the ledger's verdict.

The servicers depend only on the shared interfaces (`Consensus`, `LedgerStateMachine`), so they run unchanged
on `SingleNodeConsensus` + `ReferenceLedger` today and on `RaftNode` + `SqliteLedger` after integration.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Protocol

import grpc

from ledgernet.common.errors import LeadershipLostError, NotLeaderError
from ledgernet.common.interfaces import Consensus, LedgerStateMachine
from ledgernet.common.tx import DecodeError, decode_command
from ledgernet.common.types import NodeId, TxStatus
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.net.convert import optional_id_to_proto, raft_status_to_proto

logger = logging.getLogger(__name__)

PUBKEY_LEN = 32
TX_ID_LEN = 32

Outcome = pb.SubmitTransferResponse.Outcome


class LedgerServicer(pb_grpc.LedgerServicer):
    def __init__(
        self,
        consensus: Consensus,
        ledger: LedgerStateMachine,
        addresses: dict[NodeId, str],
        submit_timeout_s: float,
        unsafe_debug: bool = False,
    ) -> None:
        self._consensus = consensus
        self._ledger = ledger
        self._addresses = addresses
        self._submit_timeout_s = submit_timeout_s
        self._unsafe_debug = unsafe_debug

    # ---- SubmitTransfer

    async def SubmitTransfer(self, request: pb.SubmitTransferRequest, context) -> pb.SubmitTransferResponse:
        command = bytes(request.command)

        # 1. Cheap precheck. `skip_precheck` counts only on an --unsafe-debug node (forged-transfer test).
        if not (request.skip_precheck and self._unsafe_debug):
            rejected = self._precheck(command)
            if rejected is not None:
                return pb.SubmitTransferResponse(outcome=Outcome.PRECHECK_FAILED, tx_status=int(rejected))

        # 2. Ask Raft to order it. Only the leader accepts.
        try:
            index, term = await self._consensus.propose(command)
        except NotLeaderError as exc:
            hint = exc.leader_hint
            return pb.SubmitTransferResponse(
                outcome=Outcome.NOT_LEADER,
                leader_hint=optional_id_to_proto(hint),
                leader_address=self._addresses.get(hint, "") if hint else "",
            )

        # 3. Wait until it is applied locally and report the ledger's verdict.
        try:
            result = await self._consensus.wait_for_apply(index, term, self._submit_timeout_s)
        except LeadershipLostError:
            # Our entry was overwritten by another leader's log: it will never commit. Resubmitting the same
            # bytes is safe because the nonce makes a duplicate harmless.
            return pb.SubmitTransferResponse(outcome=Outcome.RETRY, index=index, term=term)
        except asyncio.TimeoutError:
            # Outcome unknown: it may still commit. The client resolves it via GetReceipt or the nonce.
            return pb.SubmitTransferResponse(outcome=Outcome.TIMEOUT, index=index, term=term)

        return pb.SubmitTransferResponse(
            outcome=Outcome.COMMITTED,
            tx_status=int(result.status),
            index=result.index,
            term=result.term,
            block_hash=result.block_hash,
        )

    @staticmethod
    def _precheck(command: bytes) -> TxStatus | None:
        """None if the command may enter the log, otherwise the TxStatus explaining the rejection."""
        try:
            transfer = decode_command(command)
        except DecodeError:
            return TxStatus.REJECTED_BAD_FORMAT
        if transfer is None:  # a NOOP is the leader's own entry; clients may only submit transfers
            return TxStatus.REJECTED_BAD_FORMAT
        if not transfer.verify():
            return TxStatus.REJECTED_BAD_SIGNATURE
        return None

    # ---- reads

    async def GetAccount(self, request: pb.GetAccountRequest, context) -> pb.GetAccountResponse:
        pubkey = bytes(request.pubkey)
        if len(pubkey) != PUBKEY_LEN:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"pubkey must be {PUBKEY_LEN} bytes")
        balance, nonce = self._ledger.get_account(pubkey)
        return pb.GetAccountResponse(balance=balance, nonce=nonce, as_of_index=self._ledger.last_applied())

    async def GetReceipt(self, request: pb.GetReceiptRequest, context) -> pb.GetReceiptResponse:
        tx_id = bytes(request.tx_id)
        if len(tx_id) != TX_ID_LEN:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"tx_id must be {TX_ID_LEN} bytes")
        receipt = self._ledger.get_receipt(tx_id)
        if receipt is None:
            return pb.GetReceiptResponse(found=False)
        return pb.GetReceiptResponse(
            found=True,
            index=receipt.index,
            term=receipt.term,
            tx_status=int(receipt.status),
            block_hash=receipt.block_hash,
        )

    async def GetStatus(self, request: pb.GetStatusRequest, context) -> pb.GetStatusResponse:
        return raft_status_to_proto(
            self._consensus.status(),
            chain_head=self._ledger.chain_head(),
            state_hash=self._ledger.state_hash(),
            total_balance=self._ledger.total_balance(),
            total_supply=self._ledger.total_supply(),
        )


# ---- Admin


class FaultInjectorLike(Protocol):
    """What AdminServicer needs from the fault injector (implemented by FaultInjector in V7.2)."""

    def set_rules(self, rules: Sequence[pb.FaultRule]) -> None: ...

    def clear(self) -> None: ...


class AdminServicer(pb_grpc.AdminServicer):
    def __init__(self, injector: FaultInjectorLike) -> None:
        self._injector = injector

    async def SetFaults(self, request: pb.SetFaultsRequest, context) -> pb.Empty:
        rules = list(request.rules)
        for rule in rules:
            problem = _rule_problem(rule)
            if problem is not None:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, problem)
        self._injector.set_rules(rules)
        logger.info("fault rules set: %d rule(s)", len(rules))
        return pb.Empty()

    async def ClearFaults(self, request: pb.Empty, context) -> pb.Empty:
        self._injector.clear()
        logger.info("fault rules cleared")
        return pb.Empty()


def _rule_problem(rule: pb.FaultRule) -> str | None:
    if not rule.target:
        return "target must be a node id or '*'"
    if not 0.0 <= rule.drop_prob <= 1.0:
        return "drop_prob must be between 0 and 1"
    if not 0.0 <= rule.duplicate_prob <= 1.0:
        return "duplicate_prob must be between 0 and 1"
    if rule.delay_min_ms > rule.delay_max_ms:
        return "delay_min_ms must not exceed delay_max_ms"
    return None


def register_services(
    server: grpc.aio.Server, ledger: LedgerServicer | None = None, admin: AdminServicer | None = None
) -> None:
    """Attach the client-facing services to a grpc.aio server (the Raft service is added by the transport)."""
    if ledger is not None:
        pb_grpc.add_LedgerServicer_to_server(ledger, server)
    if admin is not None:
        pb_grpc.add_AdminServicer_to_server(admin, server)
