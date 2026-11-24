"""LedgerClient (V7.6): the client library used by the wallet CLI, the load generator and the e2e tests.

It hides the three things a caller should not have to think about:

  * finding the leader: try the cached leader, follow `leader_address` hints from followers, otherwise
    round-robin over the known nodes;
  * transient failure: back off exponentially on timeouts, dead nodes and elections, and resubmit the
    identical bytes after `RETRY` (safe: the nonce makes a duplicate harmless);
  * ambiguous timeouts: when the server says "outcome unknown" the client looks the transfer up instead of
    guessing (see `_resolve_timeout`).

Nodes are identified by their "host:port" address.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import grpc

from ledgernet.common.tx import DecodeError, decode_command, tx_id
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc

Resp = pb.SubmitTransferResponse

# Final outcomes of `submit`.
COMMITTED = "COMMITTED"  # in the log and applied; `tx_status` is the ledger's verdict (APPLIED, REJECTED_*)
PRECHECK_FAILED = "PRECHECK_FAILED"  # bad format or signature; never entered the log
NOT_APPLIED = "NOT_APPLIED"  # after a timeout: the nonce was consumed by a different transfer
FAILED = "FAILED"  # gave up after `max_attempts`; the outcome is unknown


@dataclass
class SubmitResult:
    outcome: str
    tx_status: int | None = None
    index: int = 0
    term: int = 0
    block_hash: str = ""
    attempts: int = 0
    node_first: str = ""  # address of the first node tried
    node_final: str = ""  # address of the node that gave the final answer


class LedgerClient:
    def __init__(
        self,
        addresses: Sequence[str] | Mapping[str, str],
        rpc_timeout_s: float = 6.0,
        backoff_initial_s: float = 0.05,
        backoff_max_s: float = 1.0,
    ) -> None:
        # A mapping {name: "host:port"} is accepted for convenience; only the addresses matter.
        addrs = list(addresses.values()) if isinstance(addresses, Mapping) else list(addresses)
        if not addrs:
            raise ValueError("no node addresses given")
        self._nodes: list[str] = list(dict.fromkeys(addrs))
        self._stubs: dict[str, pb_grpc.LedgerStub] = {}
        self._channels: list[grpc.aio.Channel] = []
        self._leader: str | None = None
        self._rr = 0
        self._rpc_timeout_s = rpc_timeout_s
        self._backoff_initial_s = backoff_initial_s
        self._backoff_max_s = backoff_max_s

    @property
    def addresses(self) -> list[str]:
        return list(self._nodes)

    async def close(self) -> None:
        for channel in self._channels:
            await channel.close()
        self._channels.clear()
        self._stubs.clear()

    async def __aenter__(self) -> LedgerClient:  # noqa: PYI034 (typing.Self needs Python 3.11)
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ---- plumbing

    def _stub(self, address: str) -> pb_grpc.LedgerStub:
        stub = self._stubs.get(address)
        if stub is None:
            channel = grpc.aio.insecure_channel(address)
            self._channels.append(channel)
            stub = self._stubs[address] = pb_grpc.LedgerStub(channel)
        return stub

    def _next_node(self) -> str:
        address = self._nodes[self._rr % len(self._nodes)]
        self._rr += 1
        return address

    def _learn(self, address: str) -> str:
        """A hint may name a node we were not configured with: remember it."""
        if address not in self._nodes:
            self._nodes.append(address)
        return address

    # ---- submit

    async def submit(self, command: bytes, node: str | None = None, max_attempts: int = 10) -> SubmitResult:
        """Send a signed transfer command and return what happened to it.

        `node` pins the first node to try (default: the cached leader, else the next one round-robin).
        """
        target = node or self._leader or self._next_node()
        result = SubmitResult(outcome=FAILED, node_first=target)
        backoff = self._backoff_initial_s

        async def back_off() -> None:
            nonlocal backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max_s)

        while result.attempts < max_attempts:
            result.attempts += 1
            result.node_final = target
            try:
                resp = await self._stub(target).SubmitTransfer(
                    pb.SubmitTransferRequest(command=command), timeout=self._rpc_timeout_s
                )
            except grpc.aio.AioRpcError:
                # Node down, partitioned, or the RPC itself timed out: try another one.
                if self._leader == target:
                    self._leader = None
                target = self._next_node()
                await back_off()
                continue

            if resp.outcome == Resp.COMMITTED:
                self._leader = target
                result.outcome, result.tx_status = COMMITTED, resp.tx_status
                result.index, result.term, result.block_hash = resp.index, resp.term, resp.block_hash
                return result

            if resp.outcome == Resp.PRECHECK_FAILED:
                result.outcome, result.tx_status = PRECHECK_FAILED, resp.tx_status
                return result

            if resp.outcome == Resp.NOT_LEADER:
                if resp.leader_address:
                    target = self._leader = self._learn(resp.leader_address)
                else:  # election in progress: give it a moment, then ask someone else
                    self._leader = None
                    target = self._next_node()
                    await back_off()
                continue

            if resp.outcome == Resp.RETRY:  # our entry was overwritten; the same bytes are safe to resubmit
                self._leader = None
                target = self._next_node()
                continue

            # TIMEOUT: the outcome is unknown until we look.
            resolved = await self._resolve_timeout(command, target)
            if resolved is not None:
                resolved.attempts, resolved.node_first = result.attempts, result.node_first
                return resolved
            await back_off()
        return result

    async def _resolve_timeout(self, command: bytes, node: str) -> SubmitResult | None:
        """Decide what happened to a transfer whose submit timed out, or None if still undecided.

        The nonce is read BEFORE the receipt on purpose. If the account nonce already reached the transfer's
        nonce and no receipt exists afterwards, the transfer cannot have been applied (a receipt would have
        been written when it was) and now never can be, because that nonce is spent. Reading them the other
        way round could miss a transfer applied in between and wrongly report NOT_APPLIED.
        """
        try:
            transfer = decode_command(command)
        except DecodeError:
            return None
        if transfer is None:
            return None
        try:
            _, nonce = await self.get_account(transfer.sender, node)
            receipt = await self.get_receipt(tx_id(command), node)
        except grpc.aio.AioRpcError:
            return None
        if receipt.found:
            return SubmitResult(
                COMMITTED,
                receipt.tx_status,
                receipt.index,
                receipt.term,
                receipt.block_hash,
                node_final=node,
            )
        if nonce >= transfer.nonce:
            return SubmitResult(NOT_APPLIED, node_final=node)
        return None  # may still commit, or never will: resubmitting the same bytes is safe either way

    # ---- reads (served by one node's local state; pass `node` to ask a specific one)

    async def get_account(self, pubkey: bytes, node: str | None = None) -> tuple[int, int]:
        """(balance in paise, last used nonce)."""
        resp = await self._stub(node or self._leader or self._nodes[0]).GetAccount(
            pb.GetAccountRequest(pubkey=pubkey), timeout=self._rpc_timeout_s
        )
        return resp.balance, resp.nonce

    async def get_receipt(self, txid: bytes, node: str | None = None) -> pb.GetReceiptResponse:
        return await self._stub(node or self._leader or self._nodes[0]).GetReceipt(
            pb.GetReceiptRequest(tx_id=txid), timeout=self._rpc_timeout_s
        )

    async def status(self, node: str) -> pb.GetStatusResponse:
        return await self._stub(node).GetStatus(pb.GetStatusRequest(), timeout=self._rpc_timeout_s)

    async def status_all(self) -> dict[str, pb.GetStatusResponse | None]:
        """GetStatus from every known node concurrently; None for a node that did not answer."""

        async def one(address: str) -> pb.GetStatusResponse | None:
            try:
                return await self.status(address)
            except grpc.aio.AioRpcError:
                return None

        addresses = list(self._nodes)
        answers = await asyncio.gather(*(one(a) for a in addresses))
        return dict(zip(addresses, answers))
