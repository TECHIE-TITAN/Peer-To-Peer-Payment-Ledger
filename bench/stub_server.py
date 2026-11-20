"""A one-node gRPC ``Ledger`` server on the stand-ins, for building the load generator before
V's node program exists (implementation.md A6.7). Not part of the real system.

    make proto
    python -m bench.stub_server --genesis config/genesis.bench.json --port 50061 [--ledger sqlite]

It follows the SubmitTransfer contract of V7.4: precheck (decode + signature) -> propose ->
wait_for_apply -> COMMITTED with the ledger verdict.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import tempfile

import grpc

from ledgernet.common.errors import LeadershipLostError, NotLeaderError
from ledgernet.common.interfaces import Consensus, LedgerStateMachine
from ledgernet.common.tx import DecodeError, decode_command
from ledgernet.common.types import TxStatus
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.ledger.genesis import load_genesis
from ledgernet.ledger.sqlite_ledger import SqliteLedger
from ledgernet.testing.fakes import ReferenceLedger, SingleNodeConsensus

Resp = pb.SubmitTransferResponse


class StubLedgerServicer(pb_grpc.LedgerServicer):
    def __init__(
        self,
        consensus: Consensus,
        ledger: LedgerStateMachine,
        submit_timeout_s: float = 5.0,
        addresses: dict[str, str] | None = None,
    ) -> None:
        self._consensus = consensus
        self._ledger = ledger
        self._timeout_s = submit_timeout_s
        self._addresses = addresses or {}

    async def SubmitTransfer(self, request, context):
        try:
            tx = decode_command(request.command)
        except DecodeError:
            return Resp(outcome=Resp.PRECHECK_FAILED, tx_status=int(TxStatus.REJECTED_BAD_FORMAT))
        if tx is None:
            return Resp(outcome=Resp.PRECHECK_FAILED, tx_status=int(TxStatus.REJECTED_BAD_FORMAT))
        if not tx.verify():
            return Resp(outcome=Resp.PRECHECK_FAILED, tx_status=int(TxStatus.REJECTED_BAD_SIGNATURE))
        try:
            index, term = await self._consensus.propose(request.command)
        except NotLeaderError as exc:
            hint = exc.leader_hint or ""
            return Resp(
                outcome=Resp.NOT_LEADER, leader_hint=hint, leader_address=self._addresses.get(hint, "")
            )
        try:
            result = await asyncio.wait_for(
                self._consensus.wait_for_apply(index, term, self._timeout_s), self._timeout_s
            )
        except LeadershipLostError:
            return Resp(outcome=Resp.RETRY)
        except asyncio.TimeoutError:
            return Resp(outcome=Resp.TIMEOUT)
        return Resp(
            outcome=Resp.COMMITTED,
            tx_status=int(result.status),
            index=result.index,
            term=result.term,
            block_hash=result.block_hash,
        )

    async def GetAccount(self, request, context):
        balance, nonce = self._ledger.get_account(request.pubkey)
        return pb.GetAccountResponse(balance=balance, nonce=nonce, as_of_index=self._ledger.last_applied())

    async def GetReceipt(self, request, context):
        r = self._ledger.get_receipt(request.tx_id)
        if r is None:
            return pb.GetReceiptResponse(found=False)
        return pb.GetReceiptResponse(
            found=True, index=r.index, term=r.term, tx_status=int(r.status), block_hash=r.block_hash
        )

    async def GetStatus(self, request, context):
        s = self._consensus.status()
        return pb.GetStatusResponse(
            node_id=s.node_id,
            role=s.role,
            term=s.term,
            leader_id=s.leader_id or "",
            commit_index=s.commit_index,
            last_applied=s.last_applied,
            last_log_index=s.last_log_index,
            snapshot_index=s.snapshot_index,
            chain_head=self._ledger.chain_head(),
            state_hash=self._ledger.state_hash(),
            total_balance=self._ledger.total_balance(),
            total_supply=self._ledger.total_supply(),
        )


async def start_stub_server(
    genesis: dict[bytes, int],
    host: str = "127.0.0.1",
    port: int = 0,
    ledger_kind: str = "reference",
    db_path: str | None = None,
) -> tuple[grpc.aio.Server, int, LedgerStateMachine]:
    """Start the server on the running event loop. Returns ``(server, bound_port, ledger)``."""
    if ledger_kind == "sqlite":
        ledger: LedgerStateMachine = SqliteLedger(
            db_path or os.path.join(tempfile.mkdtemp(prefix="ledgernet-stub-"), "state.db"), genesis
        )
    else:
        ledger = ReferenceLedger(genesis)
    consensus = SingleNodeConsensus("n1", ledger)
    server = grpc.aio.server()
    pb_grpc.add_LedgerServicer_to_server(StubLedgerServicer(consensus, ledger), server)
    bound = server.add_insecure_port(f"{host}:{port}")
    await server.start()
    return server, bound, ledger


async def _serve(args) -> None:
    server, port, _ = await start_stub_server(load_genesis(args.genesis), args.host, args.port, args.ledger)
    print(f"stub ledger server on {args.host}:{port} ({args.ledger})", flush=True)
    await server.wait_for_termination()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="One-node stand-in Ledger gRPC server")
    p.add_argument("--genesis", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=50061)
    p.add_argument("--ledger", choices=["reference", "sqlite"], default="reference")
    asyncio.run(_serve(p.parse_args(argv)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
