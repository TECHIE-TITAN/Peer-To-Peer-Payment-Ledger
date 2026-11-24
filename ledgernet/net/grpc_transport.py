"""GrpcTransport (V7.3): Raft's network, over gRPC.

Client side: one lazily created, reused `grpc.aio` channel per peer. Every call goes through the fault
injector and has a deadline; any gRPC or timeout failure becomes `TransportError`, which is the only error
Raft knows how to handle.

Server side: `start(handler)` builds the node's single gRPC server. The Raft service is added here because it
needs the handler (the RaftNode). The Ledger and Admin services share the same port: node.py passes
`extra_services`, a function that registers them on the server before it starts (a grpc.aio server cannot
gain services once running).

`RaftNode` owns `start()`/`stop()` of the transport. Nobody else may call them.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import grpc

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
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.net.convert import from_proto, to_proto
from ledgernet.net.faults import FaultInjector

logger = logging.getLogger(__name__)

MAX_MESSAGE_BYTES = 64 * 1024 * 1024  # snapshots travel in one message
GRPC_OPTIONS = [
    ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
    ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
]


class GrpcTransport(Transport):
    def __init__(
        self,
        node_id: NodeId,
        peer_addresses: dict[NodeId, str],
        rpc_timeout_ms: int,
        injector: FaultInjector | None = None,
        *,
        listen_address: str | None = None,
        extra_services: Callable[[grpc.aio.Server], None] | None = None,
    ) -> None:
        """`peer_addresses` maps node id -> "host:port" (it may include this node itself).

        `listen_address` defaults to every interface on this node's own port from `peer_addresses`.
        `extra_services` registers the Ledger/Admin services on the server before it starts.
        """
        self._node_id = node_id
        self._peers = {n: a for n, a in peer_addresses.items() if n != node_id}
        self._rpc_timeout_s = rpc_timeout_ms / 1000.0
        self._injector = injector
        if listen_address is None:
            own = peer_addresses.get(node_id)
            if own is None:
                raise ValueError(f"no listen_address given and {node_id} is not in peer_addresses")
            listen_address = f"0.0.0.0:{own.rsplit(':', 1)[1]}"
        self._listen_address = listen_address
        self._extra_services = extra_services
        self._server: grpc.aio.Server | None = None
        self._channels: dict[NodeId, grpc.aio.Channel] = {}
        self._stubs: dict[NodeId, pb_grpc.RaftStub] = {}
        self.port: int | None = None  # the port actually bound (useful when listening on port 0)

    # ---- lifecycle (called by RaftNode only)

    async def start(self, handler: RaftRpcHandler) -> None:
        if self._server is not None:
            raise RuntimeError("transport already started")
        server = grpc.aio.server(options=GRPC_OPTIONS)
        pb_grpc.add_RaftServicer_to_server(_RaftServicer(handler), server)
        if self._extra_services is not None:
            self._extra_services(server)
        self.port = server.add_insecure_port(self._listen_address)
        if self.port == 0:
            raise TransportError(f"cannot listen on {self._listen_address}")
        await server.start()
        self._server = server
        logger.info("%s listening on %s", self._node_id, self._listen_address)

    async def stop(self) -> None:
        server, self._server = self._server, None
        channels, self._channels, self._stubs = list(self._channels.values()), {}, {}
        for channel in channels:
            await channel.close()
        if server is not None:
            await server.stop(0)

    # ---- client side

    def _stub(self, target: NodeId) -> pb_grpc.RaftStub:
        stub = self._stubs.get(target)
        if stub is None:
            address = self._peers.get(target)
            if address is None:
                raise TransportError(f"unknown peer {target}")
            channel = grpc.aio.insecure_channel(address, options=GRPC_OPTIONS)
            self._channels[target] = channel
            stub = self._stubs[target] = pb_grpc.RaftStub(channel)
        return stub

    async def _invoke(self, target: NodeId, method: str, request) -> Any:
        stub = self._stub(target)  # raises TransportError for an unknown peer

        async def send():
            try:
                message = await getattr(stub, method)(to_proto(request), timeout=self._rpc_timeout_s)
            except grpc.aio.AioRpcError as exc:
                raise TransportError(f"{method} to {target}: {exc.code().name}") from exc
            except asyncio.TimeoutError as exc:
                raise TransportError(f"{method} to {target}: timeout") from exc
            return from_proto(message)

        if self._injector is None:
            return await send()
        return await self._injector.around(target, send)

    async def request_vote(self, target: NodeId, req: RequestVoteRequest) -> RequestVoteResponse:
        return await self._invoke(target, "RequestVote", req)

    async def append_entries(self, target: NodeId, req: AppendEntriesRequest) -> AppendEntriesResponse:
        return await self._invoke(target, "AppendEntries", req)

    async def install_snapshot(self, target: NodeId, req: InstallSnapshotRequest) -> InstallSnapshotResponse:
        return await self._invoke(target, "InstallSnapshot", req)


class _RaftServicer(pb_grpc.RaftServicer):
    """Server side: proto -> dataclass -> RaftNode handler -> dataclass -> proto."""

    def __init__(self, handler: RaftRpcHandler) -> None:
        self._handler = handler

    async def RequestVote(self, request: pb.RequestVoteRequest, context) -> pb.RequestVoteResponse:
        return to_proto(await self._handler.on_request_vote(from_proto(request)))

    async def AppendEntries(self, request: pb.AppendEntriesRequest, context) -> pb.AppendEntriesResponse:
        return to_proto(await self._handler.on_append_entries(from_proto(request)))

    async def InstallSnapshot(
        self, request: pb.InstallSnapshotRequest, context
    ) -> pb.InstallSnapshotResponse:
        return to_proto(await self._handler.on_install_snapshot(from_proto(request)))


__all__ = ["MAX_MESSAGE_BYTES", "GrpcTransport"]
