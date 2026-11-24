"""Node process (V7.5): wires one LedgerNet node together and runs it.

    python -m ledgernet.node --config cluster/cluster.local.yaml --id n3
        [--consensus raft|single] [--storage file|memory] [--ledger sqlite|reference] [--unsafe-debug]

The three swap flags let integration replace one fake at a time (docs/implementation.md Section 8):

    --consensus single   SingleNodeConsensus (every node is its own leader; for harness debugging)
    --consensus raft     RaftNode over GrpcTransport
    --storage memory     InMemoryLogStore + InMemorySnapshotStore   |  file: FileLogStore + FileSnapshotStore
    --ledger reference   ReferenceLedger                            |  sqlite: SqliteLedger

One gRPC port per node serves the Raft, Ledger and Admin services. With Raft, the transport builds the server
(it needs the RaftNode as handler) and `extra_services` adds the other two before it starts.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

import grpc

from ledgernet.common import events
from ledgernet.common.interfaces import Consensus, LedgerStateMachine, LogStore, SnapshotStore
from ledgernet.ledger.genesis import load_genesis
from ledgernet.net.faults import FaultInjector
from ledgernet.net.grpc_transport import GrpcTransport
from ledgernet.net.services import AdminServicer, LedgerServicer, register_services
from ledgernet.node_config import ClusterConfig, load_cluster_config
from ledgernet.raft.node import RaftNode
from ledgernet.testing.fakes import (
    InMemoryLogStore,
    InMemorySnapshotStore,
    ReferenceLedger,
    SingleNodeConsensus,
)

logger = logging.getLogger("ledgernet.node")

CONSENSUS_CHOICES = ("raft", "single")
STORAGE_CHOICES = ("file", "memory")
LEDGER_CHOICES = ("sqlite", "reference")


class Node:
    """One running node. `start()` builds and starts everything, `stop()` shuts it down in order."""

    def __init__(
        self,
        config: ClusterConfig,
        node_id: str,
        *,
        consensus: str = "raft",
        storage: str = "file",
        ledger: str = "sqlite",
        unsafe_debug: bool = False,
        configure_events: bool = True,
    ) -> None:
        for value, choices, what in (
            (consensus, CONSENSUS_CHOICES, "consensus"),
            (storage, STORAGE_CHOICES, "storage"),
            (ledger, LEDGER_CHOICES, "ledger"),
        ):
            if value not in choices:
                raise ValueError(f"{what} must be one of {choices}, not {value!r}")
        self.config = config
        self.spec = config.node(node_id)
        self.node_id = node_id
        self._consensus_kind, self._storage_kind, self._ledger_kind = consensus, storage, ledger
        self._unsafe_debug = unsafe_debug
        # events.py is process-global; tests that run several nodes in one process turn this off.
        self._configure_events = configure_events

        self.injector = FaultInjector()
        self.consensus: Consensus | None = None
        self.ledger: LedgerStateMachine | None = None
        self.log_store: LogStore | None = None
        self.snapshot_store: SnapshotStore | None = None
        self.transport: GrpcTransport | None = None
        self._server: grpc.aio.Server | None = None
        self.port: int | None = None
        self._started = False

    # ---- lifecycle

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("node already started")
        data_dir = self.spec.data_dir
        os.makedirs(data_dir, exist_ok=True)
        if self._configure_events:
            events.configure(self.node_id, os.path.join(data_dir, "events.jsonl"))
        events.emit("NODE_START")

        genesis = load_genesis(self.config.genesis_path)
        self.ledger = self._build_ledger(genesis)
        if self._consensus_kind == "raft":
            self._build_raft()
        else:
            self.consensus = SingleNodeConsensus(self.node_id, self.ledger)

        try:
            if self._consensus_kind == "raft":
                await self.consensus.start()  # RaftNode starts the transport, hence the gRPC server
                self.port = self.transport.port
            else:
                await self.consensus.start()
                await self._start_standalone_server()
        except BaseException:
            await self.stop()
            raise
        self._started = True
        events.emit("SERVING", port=self.port)
        logger.info(
            "%s serving on port %s (consensus=%s storage=%s ledger=%s)",
            self.node_id,
            self.port,
            self._consensus_kind,
            self._storage_kind,
            self._ledger_kind,
        )

    async def stop(self) -> None:
        """Stop serving, then close the durable pieces (FileLogStore.close() flushes the commit index)."""
        consensus, self.consensus = self.consensus, None
        server, self._server = self._server, None
        self._started = False
        if consensus is not None:
            await consensus.stop()  # for Raft this also stops the transport and its server
        if server is not None:
            await server.stop(0)
        for closable in (self.log_store, self.ledger):
            close = getattr(closable, "close", None)
            if close is not None:
                close()
        self.log_store = self.ledger = None

    # ---- construction

    def _build_ledger(self, genesis: dict[bytes, int]) -> LedgerStateMachine:
        if self._ledger_kind == "reference":
            return ReferenceLedger(genesis)
        from ledgernet.ledger.sqlite_ledger import SqliteLedger

        return SqliteLedger(os.path.join(self.spec.data_dir, "state.db"), genesis)

    def _build_stores(self) -> tuple[LogStore, SnapshotStore]:
        if self._storage_kind == "memory":
            return InMemoryLogStore(), InMemorySnapshotStore()
        from ledgernet.storage.file_log import FileLogStore
        from ledgernet.storage.snapshot_store import FileSnapshotStore

        return (
            FileLogStore(os.path.join(self.spec.data_dir, "wal")),
            FileSnapshotStore(os.path.join(self.spec.data_dir, "snapshots")),
        )

    def _register_client_services(self, server: grpc.aio.Server) -> None:
        register_services(
            server,
            ledger=LedgerServicer(
                self.consensus,
                self.ledger,
                self.config.addresses(),
                self.config.submit_timeout_s,
                self._unsafe_debug,
            ),
            admin=AdminServicer(self.injector),
        )

    def _build_raft(self) -> None:
        self.log_store, self.snapshot_store = self._build_stores()
        self.transport = GrpcTransport(
            self.node_id,
            self.config.addresses(),
            self.config.raft.rpc_timeout_ms,
            self.injector,
            listen_address=f"0.0.0.0:{self.spec.port}",
            extra_services=self._register_client_services,
        )
        self.consensus = RaftNode(
            self.node_id,
            self.config.node_ids(),
            self.transport,
            self.log_store,
            self.snapshot_store,
            self.ledger,
            self.config.raft,
        )

    async def _start_standalone_server(self) -> None:
        server = grpc.aio.server()
        self._register_client_services(server)
        self.port = server.add_insecure_port(f"0.0.0.0:{self.spec.port}")
        if self.port == 0:
            raise RuntimeError(f"cannot listen on port {self.spec.port}")
        await server.start()
        self._server = server

    # ---- introspection (tests, launcher)

    def fatal_error(self) -> BaseException | None:
        return getattr(self.consensus, "fatal_error", None)


# ---- process entry point


def _configure_logging(data_dir: str, level: str) -> None:
    os.makedirs(data_dir, exist_ok=True)
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.FileHandler(os.path.join(data_dir, "node.log")), logging.StreamHandler()],
    )


async def run(args: argparse.Namespace) -> int:
    config = load_cluster_config(args.config)
    node = Node(
        config,
        args.id,
        consensus=args.consensus,
        storage=args.storage,
        ledger=args.ledger,
        unsafe_debug=args.unsafe_debug,
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_requested.set)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop_requested.set))

    await node.start()
    try:
        await stop_requested.wait()
        logger.info("%s shutting down", node.node_id)
    finally:
        await node.stop()
    return 1 if node.fatal_error() is not None else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m ledgernet.node", description="Run one LedgerNet node.")
    p.add_argument("--config", required=True, help="cluster YAML, e.g. cluster/cluster.local.yaml")
    p.add_argument("--id", required=True, help="this node's id from the config, e.g. n3")
    p.add_argument("--consensus", choices=CONSENSUS_CHOICES, default="raft")
    p.add_argument("--storage", choices=STORAGE_CHOICES, default="file")
    p.add_argument("--ledger", choices=LEDGER_CHOICES, default="sqlite")
    p.add_argument("--unsafe-debug", action="store_true", help="honour skip_precheck (forged-tx test only)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_cluster_config(args.config)
        data_dir = config.node(args.id).data_dir
    except (OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    _configure_logging(data_dir, args.log_level)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
