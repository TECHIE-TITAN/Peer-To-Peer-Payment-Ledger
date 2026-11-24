"""GrpcTransport (V7.3): two transports on localhost talking through a trivial echo RaftRpcHandler."""

from __future__ import annotations

import asyncio
import socket
import time

import grpc
import pytest

from ledgernet.common.errors import TransportError
from ledgernet.common.interfaces import RaftRpcHandler
from ledgernet.common.rpc_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from ledgernet.common.types import LogEntry
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.net.faults import FaultInjector, Rule
from ledgernet.net.grpc_transport import GrpcTransport
from ledgernet.net.services import AdminServicer, register_services


class EchoHandler(RaftRpcHandler):
    """Records what arrived and answers with values derived from the request."""

    def __init__(self, delay_s: float = 0.0) -> None:
        self.votes: list[RequestVoteRequest] = []
        self.appends: list[AppendEntriesRequest] = []
        self.snapshots: list[InstallSnapshotRequest] = []
        self.delay_s = delay_s

    async def on_request_vote(self, req):
        self.votes.append(req)
        await asyncio.sleep(self.delay_s)
        return RequestVoteResponse(term=req.term + 1, vote_granted=not req.pre_vote)

    async def on_append_entries(self, req):
        self.appends.append(req)
        await asyncio.sleep(self.delay_s)
        last = req.entries[-1].index if req.entries else req.prev_log_index
        return AppendEntriesResponse(req.term, True, last, 0, 0)

    async def on_install_snapshot(self, req):
        self.snapshots.append(req)
        await asyncio.sleep(self.delay_s)
        return InstallSnapshotResponse(term=req.term + len(req.data))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Pair:
    """Transports n1 and n2, each with its own EchoHandler and injector."""

    def __init__(self, rpc_timeout_ms=2000, extra_services=None):
        ports = {"n1": _free_port(), "n2": _free_port()}
        self.addresses = {n: f"127.0.0.1:{p}" for n, p in ports.items()}
        self.injectors = {n: FaultInjector(seed=1) for n in ports}
        self.handlers = {n: EchoHandler() for n in ports}
        self.transports = {
            n: GrpcTransport(
                n,
                self.addresses,
                rpc_timeout_ms,
                self.injectors[n],
                listen_address=f"127.0.0.1:{ports[n]}",
                extra_services=extra_services if n == "n2" else None,
            )
            for n in ports
        }

    async def start(self):
        for n, t in self.transports.items():
            await t.start(self.handlers[n])

    async def stop(self):
        for t in self.transports.values():
            await t.stop()


@pytest.fixture
async def pair():
    p = Pair()
    await p.start()
    yield p
    await p.stop()


# ---- the three RPCs, end to end


async def test_request_vote_round_trip(pair):
    req = RequestVoteRequest(term=4, candidate_id="n1", last_log_index=9, last_log_term=3, pre_vote=False)
    resp = await pair.transports["n1"].request_vote("n2", req)
    assert pair.handlers["n2"].votes == [req]  # arrived as the same dataclass
    assert resp == RequestVoteResponse(term=5, vote_granted=True)


async def test_pre_vote_flag_survives_the_wire(pair):
    req = RequestVoteRequest(term=4, candidate_id="n1", last_log_index=0, last_log_term=0, pre_vote=True)
    resp = await pair.transports["n1"].request_vote("n2", req)
    assert pair.handlers["n2"].votes[0].pre_vote is True
    assert resp.vote_granted is False


async def test_append_entries_with_entries(pair):
    entries = [LogEntry(i, 2, bytes([i]) * 145) for i in range(5, 9)]
    req = AppendEntriesRequest(2, "n1", 4, 2, entries, 6)
    resp = await pair.transports["n1"].append_entries("n2", req)
    assert pair.handlers["n2"].appends == [req]
    assert resp.success and resp.match_index == 8


async def test_heartbeat_has_no_entries(pair):
    req = AppendEntriesRequest(2, "n1", 4, 2, [], 4)
    resp = await pair.transports["n1"].append_entries("n2", req)
    assert pair.handlers["n2"].appends[0].entries == []
    assert resp.match_index == 4


async def test_both_directions_work(pair):
    req = RequestVoteRequest(1, "n2", 0, 0)
    await pair.transports["n2"].request_vote("n1", req)
    assert pair.handlers["n1"].votes == [req] and pair.handlers["n2"].votes == []


async def test_snapshot_larger_than_the_default_grpc_limit(pair):
    data = bytes(range(256)) * (20 * 1024)  # 5 MiB, above gRPC's default 4 MiB cap
    req = InstallSnapshotRequest(3, "n1", 100, 3, data)
    resp = await pair.transports["n1"].install_snapshot("n2", req)
    assert pair.handlers["n2"].snapshots[0].data == data
    assert resp.term == 3 + len(data)


async def test_concurrent_calls_share_one_channel(pair):
    t = pair.transports["n1"]
    calls = (t.request_vote("n2", RequestVoteRequest(i, "n1", 0, 0)) for i in range(1, 30))
    results = await asyncio.gather(*calls)
    assert [r.term for r in results] == list(range(2, 31))
    assert len(t._channels) == 1


# ---- failures become TransportError


async def test_unknown_peer(pair):
    with pytest.raises(TransportError):
        await pair.transports["n1"].request_vote("n9", RequestVoteRequest(1, "n1", 0, 0))


async def test_peer_not_running(pair):
    await pair.transports["n2"].stop()
    with pytest.raises(TransportError):
        await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))


async def test_peer_comes_back_on_the_same_channel(pair):
    t1, t2 = pair.transports["n1"], pair.transports["n2"]
    await t1.request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    await t2.stop()
    with pytest.raises(TransportError):
        await t1.request_vote("n2", RequestVoteRequest(2, "n1", 0, 0))
    await t2.start(pair.handlers["n2"])
    for _ in range(50):  # the channel reconnects with backoff
        try:
            await t1.request_vote("n2", RequestVoteRequest(3, "n1", 0, 0))
            break
        except TransportError:
            await asyncio.sleep(0.1)
    else:
        pytest.fail("never reconnected")


async def test_slow_handler_hits_the_rpc_deadline():
    p = Pair(rpc_timeout_ms=100)
    p.handlers["n2"].delay_s = 1.0
    await p.start()
    try:
        started = time.monotonic()
        with pytest.raises(TransportError, match="DEADLINE_EXCEEDED|timeout"):
            await p.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
        assert time.monotonic() - started < 0.8
    finally:
        await p.stop()


async def test_handler_exception_is_a_transport_error(pair):
    async def boom(req):
        raise RuntimeError("handler bug")

    pair.handlers["n2"].on_request_vote = boom
    with pytest.raises(TransportError):
        await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))


async def test_stop_is_idempotent_and_start_twice_is_rejected(pair):
    with pytest.raises(RuntimeError):
        await pair.transports["n1"].start(pair.handlers["n1"])
    await pair.transports["n1"].stop()
    await pair.transports["n1"].stop()


# ---- fault injection through the real transport


async def test_injected_partition_blocks_the_call_without_reaching_the_peer(pair):
    pair.injectors["n1"].set_rules([Rule("n2", partitioned=True)])
    with pytest.raises(TransportError, match="partition"):
        await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    assert pair.handlers["n2"].votes == []
    # The other direction is untouched: a partition is per injector.
    await pair.transports["n2"].request_vote("n1", RequestVoteRequest(1, "n2", 0, 0))

    pair.injectors["n1"].clear()
    await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    assert len(pair.handlers["n2"].votes) == 1


async def test_injected_delay_is_observed(pair):
    pair.injectors["n1"].set_rules([Rule("n2", delay_min_ms=150, delay_max_ms=150)])
    started = time.monotonic()
    await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    assert time.monotonic() - started >= 0.14


async def test_injected_drop_loses_requests_and_responses(pair):
    pair.injectors["n1"].set_rules([Rule("n2", drop_prob=1.0)])
    for term in range(1, 41):
        with pytest.raises(TransportError):
            await pair.transports["n1"].request_vote("n2", RequestVoteRequest(term, "n1", 0, 0))
    seen = len(pair.handlers["n2"].votes)
    assert 0 < seen < 40  # some lost before sending, some after the peer already processed them


async def test_injected_duplicate_reaches_the_peer_twice(pair):
    pair.injectors["n1"].set_rules([Rule("n2", duplicate_prob=1.0)])
    await pair.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    assert len(pair.handlers["n2"].votes) == 2


async def test_transport_without_an_injector_works():
    ports = {"n1": _free_port(), "n2": _free_port()}
    addresses = {n: f"127.0.0.1:{p}" for n, p in ports.items()}
    ts = {n: GrpcTransport(n, addresses, 1000) for n in ports}  # default listen address from peer_addresses
    for t in ts.values():
        await t.start(EchoHandler())
    try:
        resp = await ts["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
        assert resp.term == 2
    finally:
        for t in ts.values():
            await t.stop()


def test_listen_address_is_required_if_the_node_is_not_in_peer_addresses():
    with pytest.raises(ValueError):
        GrpcTransport("n1", {"n2": "127.0.0.1:1"}, 1000)


# ---- one server, three services


async def test_extra_services_share_the_raft_port():
    """Admin runs on the same port as Raft; setting a rule over gRPC changes how n2's transport behaves."""
    holder: dict[str, FaultInjector] = {}

    def register(server):
        holder["injector"] = FaultInjector(seed=1)
        register_services(server, admin=AdminServicer(holder["injector"]))

    p = Pair(extra_services=register)
    await p.start()
    try:
        async with grpc.aio.insecure_channel(p.addresses["n2"]) as channel:
            admin = pb_grpc.AdminStub(channel)
            await admin.SetFaults(pb.SetFaultsRequest(rules=[pb.FaultRule(target="n1", partitioned=True)]))
            assert [r.target for r in holder["injector"].rules()] == ["n1"]
            await admin.ClearFaults(pb.Empty())
            assert holder["injector"].rules() == []
        # and Raft still works on that port
        await p.transports["n1"].request_vote("n2", RequestVoteRequest(1, "n1", 0, 0))
    finally:
        await p.stop()
