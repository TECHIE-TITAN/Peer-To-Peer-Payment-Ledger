"""Simulated network between in-process Raft nodes (owned by P; test infrastructure only).

SimNetwork delivers Raft RPCs by awaiting the target's handler directly, and can drop, delay,
duplicate, reorder and partition messages on command. All randomness comes from one seeded
random.Random so a failing run is reproducible.
"""

from __future__ import annotations

import asyncio
import random

from ledgernet.common.errors import TransportError
from ledgernet.common.interfaces import RaftRpcHandler, Transport
from ledgernet.common.rpc_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from ledgernet.common.types import NodeId


class SimNetwork:
    def __init__(self, seed: int, rpc_timeout_s: float = 0.2) -> None:
        self.rng = random.Random(seed)
        self.rpc_timeout_s = rpc_timeout_s
        self._handlers: dict[NodeId, RaftRpcHandler] = {}
        self._groups: list[set[NodeId]] | None = None
        self._isolated: set[NodeId] = set()
        self.drop_prob = 0.0
        self.delay_min_s = 0.0
        self.delay_max_s = 0.0
        self.duplicate_prob = 0.0
        self._in_flight: set[asyncio.Task] = set()
        self.stats = {"sent": 0, "dropped": 0, "duplicated": 0}

    # ---- node registry (used by SimTransport.start / stop)

    def register(self, node_id: NodeId, handler: RaftRpcHandler) -> None:
        self._handlers[node_id] = handler

    def unregister(self, node_id: NodeId) -> None:
        self._handlers.pop(node_id, None)

    # ---- fault controls

    def partition(self, groups: list[set[NodeId]]) -> None:
        """Nodes can only talk to nodes in their own group. Nodes in no group are fully cut off."""
        self._groups = [set(group) for group in groups]

    def isolate(self, node_id: NodeId) -> None:
        self._isolated.add(node_id)

    def heal(self) -> None:
        """Remove every partition and isolation (loss, delay and duplication settings stay)."""
        self._groups = None
        self._isolated.clear()

    def set_drop(self, probability: float) -> None:
        self.drop_prob = probability

    def set_delay(self, min_ms: float, max_ms: float) -> None:
        self.delay_min_s, self.delay_max_s = min_ms / 1000, max_ms / 1000

    def set_duplicate(self, probability: float) -> None:
        self.duplicate_prob = probability

    def clear_faults(self) -> None:
        self.heal()
        self.drop_prob = self.duplicate_prob = 0.0
        self.delay_min_s = self.delay_max_s = 0.0

    def can_reach(self, src: NodeId, dst: NodeId) -> bool:
        if src in self._isolated or dst in self._isolated:
            return False
        if self._groups is None:
            return True
        return any(src in group and dst in group for group in self._groups)

    # ---- delivery

    async def call(self, src: NodeId, dst: NodeId, method: str, request):
        """Deliver one RPC. The delivery keeps running even if the caller gives up (a late message)."""
        self.stats["sent"] += 1
        task = asyncio.ensure_future(self._deliver(src, dst, method, request))
        self._in_flight.add(task)
        task.add_done_callback(self._finished)
        return await asyncio.shield(task)

    def _finished(self, task: asyncio.Task) -> None:
        self._in_flight.discard(task)
        if not task.cancelled():
            task.exception()  # mark retrieved; the caller may already have given up on it

    async def _lose(self) -> None:
        self.stats["dropped"] += 1
        await asyncio.sleep(self.rpc_timeout_s)
        raise TransportError("simulated timeout")

    async def _delay(self) -> None:
        if self.delay_max_s > 0:
            await asyncio.sleep(self.rng.uniform(self.delay_min_s, self.delay_max_s))

    async def _deliver(self, src: NodeId, dst: NodeId, method: str, request):
        if dst not in self._handlers:
            raise TransportError("connection refused (node is down)")
        if not self.can_reach(src, dst) or self.rng.random() < self.drop_prob:
            await self._lose()
        await self._delay()
        if self.rng.random() < self.duplicate_prob:
            self.stats["duplicated"] += 1
            duplicate = asyncio.ensure_future(self._deliver_duplicate(src, dst, method, request))
            self._in_flight.add(duplicate)
            duplicate.add_done_callback(self._finished)
        handler = self._handlers.get(dst)
        if handler is None or not self.can_reach(src, dst):
            await self._lose()
        response = await getattr(handler, method)(request)
        if not self.can_reach(dst, src) or self.rng.random() < self.drop_prob:
            await self._lose()
        await self._delay()
        return response

    async def _deliver_duplicate(self, src: NodeId, dst: NodeId, method: str, request) -> None:
        await self._delay()
        handler = self._handlers.get(dst)
        if handler is not None and self.can_reach(src, dst):
            await getattr(handler, method)(request)  # the response is discarded

    async def shutdown(self) -> None:
        for task in list(self._in_flight):
            task.cancel()
        await asyncio.gather(*list(self._in_flight), return_exceptions=True)


class SimTransport(Transport):
    """The Transport plug (3.7) backed by a SimNetwork. Used only by tests."""

    def __init__(self, node_id: NodeId, network: SimNetwork) -> None:
        self._node_id = node_id
        self._network = network

    async def start(self, handler: RaftRpcHandler) -> None:
        self._network.register(self._node_id, handler)

    async def stop(self) -> None:
        self._network.unregister(self._node_id)

    async def request_vote(self, target: NodeId, req: RequestVoteRequest) -> RequestVoteResponse:
        return await self._network.call(self._node_id, target, "on_request_vote", req)

    async def append_entries(self, target: NodeId, req: AppendEntriesRequest) -> AppendEntriesResponse:
        return await self._network.call(self._node_id, target, "on_append_entries", req)

    async def install_snapshot(self, target: NodeId, req: InstallSnapshotRequest) -> InstallSnapshotResponse:
        return await self._network.call(self._node_id, target, "on_install_snapshot", req)
