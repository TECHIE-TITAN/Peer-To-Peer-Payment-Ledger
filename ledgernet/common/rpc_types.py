from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from ledgernet.common.types import LogEntry, NodeId


@dataclass(frozen=True)
class RequestVoteRequest:
    term: int
    candidate_id: NodeId
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False


@dataclass(frozen=True)
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntriesRequest:
    term: int
    leader_id: NodeId
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass(frozen=True)
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = 0       # on success: index of last entry known to match
    conflict_index: int = 0    # on failure: hint for fast nextIndex backup
    conflict_term: int = 0     # on failure: term at conflict_index (0 = none)


@dataclass(frozen=True)
class InstallSnapshotRequest:
    term: int
    leader_id: NodeId
    last_included_index: int
    last_included_term: int
    data: bytes


@dataclass(frozen=True)
class InstallSnapshotResponse:
    term: int
