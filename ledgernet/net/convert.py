"""Conversion between the plain Raft dataclasses (ledgernet.common) and protobuf messages (V7.1).

Raft (P) only ever sees the dataclasses. The transport and the gRPC servicers (V) use these functions at the
network boundary, so no protobuf type ever reaches the Raft code.

    to_proto(dataclass_instance)  -> protobuf message
    from_proto(protobuf_message)  -> dataclass instance

Both directions cover LogEntry and the six Raft RPC messages. The generated code in `ledgernet/gen` comes
from `make proto` and is never edited or committed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ledgernet.common.rpc_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from ledgernet.common.types import LogEntry, RaftStatus
from ledgernet.gen import ledgernet_pb2 as pb

# ---- dataclass -> proto


def _log_entry_to_proto(e: LogEntry) -> pb.LogEntry:
    return pb.LogEntry(index=e.index, term=e.term, command=e.command)


def _request_vote_request_to_proto(r: RequestVoteRequest) -> pb.RequestVoteRequest:
    return pb.RequestVoteRequest(
        term=r.term,
        candidate_id=r.candidate_id,
        last_log_index=r.last_log_index,
        last_log_term=r.last_log_term,
        pre_vote=r.pre_vote,
    )


def _request_vote_response_to_proto(r: RequestVoteResponse) -> pb.RequestVoteResponse:
    return pb.RequestVoteResponse(term=r.term, vote_granted=r.vote_granted)


def _append_entries_request_to_proto(r: AppendEntriesRequest) -> pb.AppendEntriesRequest:
    return pb.AppendEntriesRequest(
        term=r.term,
        leader_id=r.leader_id,
        prev_log_index=r.prev_log_index,
        prev_log_term=r.prev_log_term,
        entries=[_log_entry_to_proto(e) for e in r.entries],
        leader_commit=r.leader_commit,
    )


def _append_entries_response_to_proto(r: AppendEntriesResponse) -> pb.AppendEntriesResponse:
    return pb.AppendEntriesResponse(
        term=r.term,
        success=r.success,
        match_index=r.match_index,
        conflict_index=r.conflict_index,
        conflict_term=r.conflict_term,
    )


def _install_snapshot_request_to_proto(r: InstallSnapshotRequest) -> pb.InstallSnapshotRequest:
    return pb.InstallSnapshotRequest(
        term=r.term,
        leader_id=r.leader_id,
        last_included_index=r.last_included_index,
        last_included_term=r.last_included_term,
        data=r.data,
    )


def _install_snapshot_response_to_proto(r: InstallSnapshotResponse) -> pb.InstallSnapshotResponse:
    return pb.InstallSnapshotResponse(term=r.term)


_TO_PROTO: dict[type, Callable[[Any], Any]] = {
    LogEntry: _log_entry_to_proto,
    RequestVoteRequest: _request_vote_request_to_proto,
    RequestVoteResponse: _request_vote_response_to_proto,
    AppendEntriesRequest: _append_entries_request_to_proto,
    AppendEntriesResponse: _append_entries_response_to_proto,
    InstallSnapshotRequest: _install_snapshot_request_to_proto,
    InstallSnapshotResponse: _install_snapshot_response_to_proto,
}

# ---- proto -> dataclass


def _log_entry_from_proto(m: pb.LogEntry) -> LogEntry:
    return LogEntry(index=m.index, term=m.term, command=bytes(m.command))


def _request_vote_request_from_proto(m: pb.RequestVoteRequest) -> RequestVoteRequest:
    return RequestVoteRequest(
        term=m.term,
        candidate_id=m.candidate_id,
        last_log_index=m.last_log_index,
        last_log_term=m.last_log_term,
        pre_vote=m.pre_vote,
    )


def _request_vote_response_from_proto(m: pb.RequestVoteResponse) -> RequestVoteResponse:
    return RequestVoteResponse(term=m.term, vote_granted=m.vote_granted)


def _append_entries_request_from_proto(m: pb.AppendEntriesRequest) -> AppendEntriesRequest:
    return AppendEntriesRequest(
        term=m.term,
        leader_id=m.leader_id,
        prev_log_index=m.prev_log_index,
        prev_log_term=m.prev_log_term,
        entries=[_log_entry_from_proto(e) for e in m.entries],
        leader_commit=m.leader_commit,
    )


def _append_entries_response_from_proto(m: pb.AppendEntriesResponse) -> AppendEntriesResponse:
    return AppendEntriesResponse(
        term=m.term,
        success=m.success,
        match_index=m.match_index,
        conflict_index=m.conflict_index,
        conflict_term=m.conflict_term,
    )


def _install_snapshot_request_from_proto(m: pb.InstallSnapshotRequest) -> InstallSnapshotRequest:
    return InstallSnapshotRequest(
        term=m.term,
        leader_id=m.leader_id,
        last_included_index=m.last_included_index,
        last_included_term=m.last_included_term,
        data=bytes(m.data),
    )


def _install_snapshot_response_from_proto(m: pb.InstallSnapshotResponse) -> InstallSnapshotResponse:
    return InstallSnapshotResponse(term=m.term)


_FROM_PROTO: dict[type, Callable[[Any], Any]] = {
    pb.LogEntry: _log_entry_from_proto,
    pb.RequestVoteRequest: _request_vote_request_from_proto,
    pb.RequestVoteResponse: _request_vote_response_from_proto,
    pb.AppendEntriesRequest: _append_entries_request_from_proto,
    pb.AppendEntriesResponse: _append_entries_response_from_proto,
    pb.InstallSnapshotRequest: _install_snapshot_request_from_proto,
    pb.InstallSnapshotResponse: _install_snapshot_response_from_proto,
}

# ---- public API


def to_proto(obj: Any) -> Any:
    """Convert a Raft dataclass (LogEntry or an RPC request/response) to its protobuf message."""
    try:
        convert = _TO_PROTO[type(obj)]
    except KeyError:
        raise TypeError(f"no protobuf conversion for {type(obj).__name__}") from None
    return convert(obj)


def from_proto(msg: Any) -> Any:
    """Convert a protobuf message back to the matching Raft dataclass."""
    try:
        convert = _FROM_PROTO[type(msg)]
    except KeyError:
        raise TypeError(f"no dataclass conversion for {type(msg).__name__}") from None
    return convert(msg)


# ---- optional node ids
# Protobuf has no null: "no leader" travels as an empty string. The Ledger service uses these for
# leader hints and for RaftStatus.leader_id.


def optional_id_to_proto(value: str | None) -> str:
    return value or ""


def optional_id_from_proto(value: str) -> str | None:
    return value or None


def raft_status_to_proto(
    status: RaftStatus,
    *,
    chain_head: str = "",
    state_hash: str = "",
    total_balance: int = 0,
    total_supply: int = 0,
) -> pb.GetStatusResponse:
    """RaftStatus plus the ledger's view (from the state machine) -> GetStatusResponse."""
    return pb.GetStatusResponse(
        node_id=status.node_id,
        role=status.role,
        term=status.term,
        leader_id=optional_id_to_proto(status.leader_id),
        commit_index=status.commit_index,
        last_applied=status.last_applied,
        last_log_index=status.last_log_index,
        snapshot_index=status.snapshot_index,
        chain_head=chain_head,
        state_hash=state_hash,
        total_balance=total_balance,
        total_supply=total_supply,
    )


def raft_status_from_proto(m: pb.GetStatusResponse) -> RaftStatus:
    """The Raft part of a GetStatusResponse (chain_head, state_hash and totals stay on the message)."""
    return RaftStatus(
        node_id=m.node_id,
        role=m.role,
        term=m.term,
        leader_id=optional_id_from_proto(m.leader_id),
        commit_index=m.commit_index,
        last_applied=m.last_applied,
        last_log_index=m.last_log_index,
        snapshot_index=m.snapshot_index,
    )
