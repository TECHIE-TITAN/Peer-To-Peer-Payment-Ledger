from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from ledgernet.common.rpc_types import (
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
)
from ledgernet.common.types import ApplyResult, LogEntry, NodeId, RaftStatus, Snapshot

# All methods below are called from the asyncio event-loop thread only.


class LogStore(ABC):
    """Durable Raft log + hard state. Implemented by A (FileLogStore); fake: InMemoryLogStore."""

    @abstractmethod
    def load_hard_state(self) -> Tuple[int, Optional[NodeId]]: ...          # (current_term, voted_for)
    @abstractmethod
    def save_hard_state(self, term: int, voted_for: Optional[NodeId]) -> None: ...  # durable before return
    @abstractmethod
    def load_commit_index(self) -> int: ...
    @abstractmethod
    def save_commit_index(self, index: int) -> None: ...
    # May be lazily durable (e.g. fsync at most every 100 ms), but must never persist
    # a value larger than one it was given. A stale value is safe (it is a lower bound).

    @abstractmethod
    def snapshot_index(self) -> int: ...   # last index covered by the snapshot; 0 if none
    @abstractmethod
    def snapshot_term(self) -> int: ...
    @abstractmethod
    def last_index(self) -> int: ...       # == snapshot_index() when the log is empty
    @abstractmethod
    def term_at(self, index: int) -> Optional[int]: ...
    # index == 0 -> 0; index == snapshot_index -> snapshot_term;
    # compacted or beyond last_index -> None
    @abstractmethod
    def entries(self, start: int, end: int) -> List[LogEntry]: ...
    # [start, end), clipped to last_index; IndexError if start <= snapshot_index
    @abstractmethod
    def append(self, entries: List[LogEntry]) -> None: ...
    # entries must be contiguous starting at last_index()+1; durable (fsync) before return
    @abstractmethod
    def truncate_from(self, index: int) -> None: ...
    # delete all entries >= index (index > snapshot_index); durable before return
    @abstractmethod
    def compact_to(self, index: int, term: int) -> None: ...
    # discard entries <= index and record (index, term) as the snapshot point.
    # If index >= last_index the log becomes empty. No-op if index <= snapshot_index.


class SnapshotStore(ABC):
    """Implemented by A (FileSnapshotStore); fake: InMemorySnapshotStore."""

    @abstractmethod
    def save(self, snap: Snapshot) -> None: ...      # atomic and durable before return
    @abstractmethod
    def load_latest(self) -> Optional[Snapshot]: ...


class StateMachine(ABC):
    @abstractmethod
    def last_applied(self) -> int: ...
    @abstractmethod
    def apply(self, entry: LogEntry) -> ApplyResult: ...
    # deterministic; requires entry.index == last_applied() + 1, else ValueError
    @abstractmethod
    def take_snapshot(self) -> Snapshot: ...         # covers last_applied()
    @abstractmethod
    def restore(self, snap: Snapshot) -> None: ...   # replaces all state
    @abstractmethod
    def state_hash(self) -> str: ...


class LedgerStateMachine(StateMachine):
    """Implemented by A (SqliteLedger); fake/spec: ReferenceLedger."""

    @abstractmethod
    def get_account(self, pubkey: bytes) -> Tuple[int, int]: ...   # (balance, nonce); (0,0) if absent
    @abstractmethod
    def get_receipt(self, txid: bytes) -> Optional[ApplyResult]: ...
    @abstractmethod
    def chain_head(self) -> str: ...
    @abstractmethod
    def total_balance(self) -> int: ...
    @abstractmethod
    def total_supply(self) -> int: ...


class RaftRpcHandler(ABC):
    """Server side of Raft RPCs. RaftNode (P) implements this; the transport (V) calls it."""

    @abstractmethod
    async def on_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse: ...
    @abstractmethod
    async def on_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse: ...
    @abstractmethod
    async def on_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse: ...


class Transport(ABC):
    """Client side of Raft RPCs. GrpcTransport (V); SimTransport (P, tests only).
    Every call raises TransportError on timeout/drop/partition/connection failure."""

    @abstractmethod
    async def start(self, handler: RaftRpcHandler) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def request_vote(self, target: NodeId, req: RequestVoteRequest) -> RequestVoteResponse: ...
    @abstractmethod
    async def append_entries(self, target: NodeId, req: AppendEntriesRequest) -> AppendEntriesResponse: ...
    @abstractmethod
    async def install_snapshot(self, target: NodeId, req: InstallSnapshotRequest) -> InstallSnapshotResponse: ...


class Consensus(ABC):
    """What the Ledger service (V) needs from Raft (P). Fake: SingleNodeConsensus."""

    node_id: NodeId

    @abstractmethod
    async def start(self) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def propose(self, command: bytes) -> Tuple[int, int]: ...
    # Returns (index, term) once appended to the leader's durable log.
    # Raises NotLeaderError(leader_hint) if this node is not the leader.
    @abstractmethod
    async def wait_for_apply(self, index: int, term: int, timeout: float) -> ApplyResult: ...
    # Resolves when the entry at `index` is applied locally.
    # Raises LeadershipLostError if the applied entry's term != term,
    # asyncio.TimeoutError after `timeout` seconds.
    @abstractmethod
    def status(self) -> RaftStatus: ...
