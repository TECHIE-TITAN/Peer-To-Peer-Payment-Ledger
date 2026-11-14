from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

NodeId = str  # "n1" .. "n5"


@dataclass(frozen=True)
class LogEntry:
    index: int        # 1-based, contiguous
    term: int
    command: bytes    # opaque to Raft; see tx.py for the encoding


@dataclass(frozen=True)
class Snapshot:
    last_included_index: int
    last_included_term: int
    data: bytes       # opaque to Raft; produced/consumed by the StateMachine


class TxStatus(IntEnum):
    APPLIED = 0
    NOOP = 1
    REJECTED_BAD_FORMAT = 2
    REJECTED_BAD_SIGNATURE = 3
    REJECTED_BAD_NONCE = 4
    REJECTED_INSUFFICIENT_FUNDS = 5   # nonce IS consumed (see 3.6)
    REJECTED_INVALID_AMOUNT = 6       # amount == 0 or sender == recipient


@dataclass(frozen=True)
class ApplyResult:
    index: int
    term: int
    status: TxStatus
    block_hash: str   # hex SHA-256 chain head after applying this entry


@dataclass(frozen=True)
class RaftConfig:
    election_timeout_min_ms: int = 300
    election_timeout_max_ms: int = 600
    heartbeat_interval_ms: int = 75
    rpc_timeout_ms: int = 200
    max_entries_per_append: int = 256
    snapshot_interval: int = 1000      # 0 disables snapshots (full-replay mode)
    pre_vote: bool = True
    check_quorum: bool = True


@dataclass(frozen=True)
class RaftStatus:
    node_id: NodeId
    role: str                 # "leader" | "follower" | "candidate"
    term: int
    leader_id: Optional[NodeId]
    commit_index: int
    last_applied: int
    last_log_index: int
    snapshot_index: int
