"""Minimal client on the raw generated gRPC stubs, used by the load generator until V's
``ledgernet.client.LedgerClient`` lands (implementation.md A6.7 / V7.6). It follows the same rules:

* try the cached leader, follow ``leader_address`` hints, otherwise round-robin;
* back off exponentially on ``TIMEOUT`` and connection errors; resubmit identical bytes on ``RETRY``
  (safe because of the nonce);
* resolve an ambiguous ``TIMEOUT`` with ``GetReceipt``; if there is no receipt but the account nonce has
  reached the transfer's nonce, a different transfer consumed it, so this one definitively did not apply.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import grpc

from ledgernet.common.tx import decode_command, tx_id
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc

Resp = pb.SubmitTransferResponse

# Final outcomes (strings, as written to requests.csv).
COMMITTED = "COMMITTED"
PRECHECK_FAILED = "PRECHECK_FAILED"
NOT_APPLIED = "NOT_APPLIED"  # resolved after a timeout: the nonce was consumed by another transfer
FAILED = "FAILED"  # gave up after max_attempts


@dataclass
class SubmitResult:
    outcome: str
    tx_status: int | None = None
    index: int = 0
    term: int = 0
    block_hash: str = ""
    attempts: int = 0
    node_first: str = ""
    node_final: str = ""


def parse_addresses(items: list[str]) -> dict[str, str]:
    """``["n1=127.0.0.1:50051", "127.0.0.1:50052"]`` -> ``{name: "host:port"}`` (unnamed: name = address)."""
    out: dict[str, str] = {}
    for item in items:
        for part in item.split(","):
            part = part.strip()
            if not part:
                continue
            name, sep, addr = part.partition("=")
            out[name if sep else part] = addr if sep else part
    if not out:
        raise ValueError("no node addresses given")
    return out


class RawLedgerClient:
    def __init__(
        self,
        addresses: dict[str, str],
        rpc_timeout_s: float = 6.0,
        backoff_initial_s: float = 0.05,
        backoff_max_s: float = 1.0,
    ) -> None:
        self._names = list(addresses)
        self._addr = dict(addresses)
        self._by_addr = {a: n for n, a in addresses.items()}
        self._stubs: dict[str, pb_grpc.LedgerStub] = {}
        self._channels: list[grpc.aio.Channel] = []
        self._leader: str | None = None
        self._rr = 0
        self._rpc_timeout_s = rpc_timeout_s
        self._backoff_initial_s = backoff_initial_s
        self._backoff_max_s = backoff_max_s

    def _stub(self, name: str) -> pb_grpc.LedgerStub:
        stub = self._stubs.get(name)
        if stub is None:
            channel = grpc.aio.insecure_channel(self._addr[name])
            self._channels.append(channel)
            stub = self._stubs[name] = pb_grpc.LedgerStub(channel)
        return stub

    def _next_node(self) -> str:
        name = self._names[self._rr % len(self._names)]
        self._rr += 1
        return name

    def _node_for_address(self, address: str) -> str:
        name = self._by_addr.get(address)
        if name is None:  # a leader we were not told about: remember it
            name = address
            self._addr[name] = address
            self._by_addr[address] = name
            self._names.append(name)
        return name

    async def close(self) -> None:
        for ch in self._channels:
            await ch.close()

    async def submit(self, command: bytes, node: str | None = None, max_attempts: int = 10) -> SubmitResult:
        target = node or self._leader or self._next_node()
        result = SubmitResult(outcome=FAILED, node_first=target)
        backoff = self._backoff_initial_s
        while result.attempts < max_attempts:
            result.attempts += 1
            result.node_final = target
            try:
                resp = await self._stub(target).SubmitTransfer(
                    pb.SubmitTransferRequest(command=command), timeout=self._rpc_timeout_s
                )
            except grpc.aio.AioRpcError:
                if self._leader == target:
                    self._leader = None
                target = self._next_node()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._backoff_max_s)
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
                    target = self._node_for_address(resp.leader_address)
                    self._leader = target
                else:  # election in progress: wait a little, then try someone else
                    self._leader = None
                    target = self._next_node()
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self._backoff_max_s)
                continue
            if resp.outcome == Resp.RETRY:
                self._leader = None
                target = self._next_node()
                continue
            # TIMEOUT: the outcome is unknown until we look.
            resolved = await self._resolve_timeout(command, target)
            if resolved is not None:
                resolved.attempts, resolved.node_first = result.attempts, result.node_first
                return resolved
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._backoff_max_s)
        return result

    async def _resolve_timeout(self, command: bytes, node: str) -> SubmitResult | None:
        try:
            receipt = await self.get_receipt(tx_id(command), node)
            if receipt.found:
                return SubmitResult(
                    COMMITTED,
                    receipt.tx_status,
                    receipt.index,
                    receipt.term,
                    receipt.block_hash,
                    node_final=node,
                )
            tx = decode_command(command)
            _, nonce = await self.get_account(tx.sender, node)
            if nonce >= tx.nonce:
                return SubmitResult(NOT_APPLIED, node_final=node)
        except grpc.aio.AioRpcError:
            pass
        return None

    async def get_account(self, pubkey: bytes, node: str | None = None) -> tuple[int, int]:
        resp = await self._stub(node or self._leader or self._names[0]).GetAccount(
            pb.GetAccountRequest(pubkey=pubkey), timeout=self._rpc_timeout_s
        )
        return resp.balance, resp.nonce

    async def get_receipt(self, txid: bytes, node: str | None = None) -> pb.GetReceiptResponse:
        return await self._stub(node or self._leader or self._names[0]).GetReceipt(
            pb.GetReceiptRequest(tx_id=txid), timeout=self._rpc_timeout_s
        )

    async def status(self, node: str) -> pb.GetStatusResponse:
        return await self._stub(node).GetStatus(pb.GetStatusRequest(), timeout=self._rpc_timeout_s)
