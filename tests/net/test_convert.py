"""Round-trip tests for ledgernet.net.convert (V7.1): every Raft message survives dataclass -> protobuf ->
bytes on the wire -> protobuf -> dataclass unchanged."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

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
from ledgernet.net.convert import (
    from_proto,
    optional_id_from_proto,
    optional_id_to_proto,
    raft_status_from_proto,
    raft_status_to_proto,
    to_proto,
)

u64 = st.integers(min_value=0, max_value=2**64 - 1)
node_id = st.sampled_from(["n1", "n2", "n3", "n4", "n5"])
blob = st.binary(max_size=300)

log_entries = st.builds(LogEntry, index=u64, term=u64, command=blob)
request_votes = st.builds(
    RequestVoteRequest,
    term=u64,
    candidate_id=node_id,
    last_log_index=u64,
    last_log_term=u64,
    pre_vote=st.booleans(),
)
vote_responses = st.builds(RequestVoteResponse, term=u64, vote_granted=st.booleans())
append_requests = st.builds(
    AppendEntriesRequest,
    term=u64,
    leader_id=node_id,
    prev_log_index=u64,
    prev_log_term=u64,
    entries=st.lists(log_entries, max_size=8),
    leader_commit=u64,
)
append_responses = st.builds(
    AppendEntriesResponse,
    term=u64,
    success=st.booleans(),
    match_index=u64,
    conflict_index=u64,
    conflict_term=u64,
)
snapshot_requests = st.builds(
    InstallSnapshotRequest,
    term=u64,
    leader_id=node_id,
    last_included_index=u64,
    last_included_term=u64,
    data=st.binary(max_size=4096),
)
snapshot_responses = st.builds(InstallSnapshotResponse, term=u64)

CASES = [
    (log_entries, pb.LogEntry),
    (request_votes, pb.RequestVoteRequest),
    (vote_responses, pb.RequestVoteResponse),
    (append_requests, pb.AppendEntriesRequest),
    (append_responses, pb.AppendEntriesResponse),
    (snapshot_requests, pb.InstallSnapshotRequest),
    (snapshot_responses, pb.InstallSnapshotResponse),
]


def _round_trip_over_the_wire(obj, proto_type):
    """dataclass -> proto -> bytes -> proto -> dataclass, exactly what two nodes do over gRPC."""
    message = to_proto(obj)
    assert isinstance(message, proto_type)
    wire = message.SerializeToString()
    received = proto_type()
    received.ParseFromString(wire)
    return from_proto(received)


@pytest.mark.parametrize("strategy,proto_type", CASES, ids=[t.__name__ for _, t in CASES])
def test_round_trip(strategy, proto_type):
    @settings(max_examples=100, deadline=None)
    @given(strategy)
    def check(obj):
        assert _round_trip_over_the_wire(obj, proto_type) == obj

    check()


def test_pre_vote_flag_is_carried():
    for flag in (True, False):
        req = RequestVoteRequest(7, "n2", 10, 6, pre_vote=flag)
        assert _round_trip_over_the_wire(req, pb.RequestVoteRequest).pre_vote is flag


def test_heartbeat_has_no_entries():
    heartbeat = AppendEntriesRequest(3, "n1", 9, 2, [], 8)
    back = _round_trip_over_the_wire(heartbeat, pb.AppendEntriesRequest)
    assert back.entries == []
    assert back == heartbeat


def test_entries_keep_order_and_content():
    entries = [LogEntry(i, 4, bytes([i]) * i) for i in range(1, 6)]
    req = AppendEntriesRequest(4, "n3", 0, 0, entries, 0)
    back = _round_trip_over_the_wire(req, pb.AppendEntriesRequest)
    assert [e.index for e in back.entries] == [1, 2, 3, 4, 5]
    assert back.entries == entries


def test_command_bytes_are_not_altered():
    transfer_like = bytes(range(145))  # 145 bytes, like a TRANSFER command, with every low byte value
    noop = b"\x00"
    for command in (transfer_like, noop, b""):
        entry = LogEntry(1, 1, command)
        back = _round_trip_over_the_wire(entry, pb.LogEntry)
        assert type(back.command) is bytes
        assert back.command == command


def test_large_snapshot_payload():
    data = bytes(range(256)) * 4096  # 1 MiB
    req = InstallSnapshotRequest(2, "n1", 5000, 2, data)
    back = _round_trip_over_the_wire(req, pb.InstallSnapshotRequest)
    assert back == req


def test_max_uint64_values():
    big = 2**64 - 1
    resp = AppendEntriesResponse(big, True, big, big, big)
    assert _round_trip_over_the_wire(resp, pb.AppendEntriesResponse) == resp


def test_unknown_types_are_rejected():
    with pytest.raises(TypeError):
        to_proto("not a message")
    with pytest.raises(TypeError):
        from_proto(pb.GetAccountRequest())


# ---- optional ids and status


def test_optional_id_helpers():
    assert optional_id_to_proto(None) == ""
    assert optional_id_to_proto("n2") == "n2"
    assert optional_id_from_proto("") is None
    assert optional_id_from_proto("n2") == "n2"


@given(
    st.builds(
        RaftStatus,
        node_id=node_id,
        role=st.sampled_from(["leader", "follower", "candidate"]),
        term=u64,
        leader_id=st.one_of(st.none(), node_id),
        commit_index=u64,
        last_applied=u64,
        last_log_index=u64,
        snapshot_index=u64,
    )
)
@settings(max_examples=100, deadline=None)
def test_raft_status_round_trip(status):
    message = raft_status_to_proto(
        status, chain_head="ab" * 32, state_hash="cd" * 32, total_balance=7, total_supply=9
    )
    assert message.chain_head == "ab" * 32
    assert message.total_supply == 9
    assert raft_status_from_proto(message) == status


def test_status_without_leader_maps_to_none():
    status = RaftStatus("n4", "candidate", 3, None, 0, 0, 0, 0)
    message = raft_status_to_proto(status)
    assert message.leader_id == ""
    assert raft_status_from_proto(message).leader_id is None
