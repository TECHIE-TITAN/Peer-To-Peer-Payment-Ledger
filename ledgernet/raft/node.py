"""RaftNode: leader election with heartbeats (owned by P).

Everything runs on one asyncio event loop and never awaits while a critical section is half done:
store and state-machine calls are synchronous, and the only awaits are transport calls and sleeps.
That is why no locks are needed (implementation.md, Section 12).

Design notes (persistence ordering, how each safety rule is guaranteed) live in
ledgernet/raft/README.md. Terms used here follow the glossary: log, entry, index, term, commit, apply.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

from ledgernet.common import events
from ledgernet.common.errors import TransportError
from ledgernet.common.interfaces import (
    Consensus,
    LogStore,
    RaftRpcHandler,
    SnapshotStore,
    StateMachine,
    Transport,
)
from ledgernet.common.rpc_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from ledgernet.common.tx import NOOP_COMMAND
from ledgernet.common.types import (
    ApplyResult,
    LogEntry,
    NodeId,
    RaftConfig,
    RaftStatus,
)

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


logger = logging.getLogger("ledgernet.raft")


class RaftNode(Consensus, RaftRpcHandler):
    """One Raft participant.

    `peers` may contain this node's own id (it is ignored), so callers can pass the full node list.
    The node owns the transport lifecycle: start() calls transport.start(self), stop() calls
    transport.stop().
    """

    def __init__(
        self,
        node_id: NodeId,
        peers: list[NodeId],
        transport: Transport,
        log_store: LogStore,
        snapshot_store: SnapshotStore,
        state_machine: StateMachine,
        config: RaftConfig,
    ) -> None:
        self.node_id = node_id
        self._peers: list[NodeId] = [p for p in peers if p != node_id]
        self._majority = (len(self._peers) + 1) // 2 + 1
        self._transport = transport
        self._store = log_store
        self._snapshots = snapshot_store
        self._sm = state_machine
        self._config = config

        # Test hooks (set after construction, before start()); production code leaves the defaults.
        self._now: Callable[[], float] = time.monotonic
        self._rng = random.Random()
        self.on_leader_elected: Callable[[RaftNode], None] | None = None

        # Counters and failure flag that tests (and V's node.py) can inspect.
        self.stats: dict[str, int] = {
            "elections_started": 0,
        }
        self.fatal_error: BaseException | None = None

        # Persistent state (mirrored from the LogStore).
        self._term = 0
        self._voted_for: NodeId | None = None

        # Volatile state.
        self._role = FOLLOWER
        self._leader_id: NodeId | None = None
        self._commit_index = 0
        self._next_index: dict[NodeId, int] = {}
        self._match_index: dict[NodeId, int] = {}
        self._ack_time: dict[NodeId, float] = {}
        self._election_deadline = 0.0

        # Background machinery.
        self._stopped = True
        self._tasks: set = set()
        self._election_task: asyncio.Task | None = None
        self._replication_tasks: dict[NodeId, asyncio.Task] = {}
        self._replication_kick: dict[NodeId, asyncio.Event] = {}

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._recover()
        self._stopped = False
        self._reset_election_deadline()
        await self._transport.start(self)
        self._spawn(self._ticker_loop(), "ticker")
        await asyncio.sleep(0)  # let the background tasks start, so an immediate stop() cancels them cleanly

    async def stop(self) -> None:
        self._stopped = True
        tasks = list(self._tasks) + list(self._replication_tasks.values())
        if self._election_task is not None:
            tasks.append(self._election_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._replication_tasks.clear()
        self._election_task = None
        await self._transport.stop()

    # ------------------------------------------------------------------ Consensus API

    async def propose(self, command: bytes) -> tuple[int, int]:
        raise NotImplementedError("log replication is added in the next step")

    async def wait_for_apply(self, index: int, term: int, timeout: float) -> ApplyResult:
        raise NotImplementedError("the apply loop is added in the next step")

    def status(self) -> RaftStatus:
        return RaftStatus(
            node_id=self.node_id,
            role=self._role,
            term=self._term,
            leader_id=self._leader_id,
            commit_index=self._commit_index,
            last_applied=self._sm.last_applied(),
            last_log_index=self._store.last_index(),
            snapshot_index=self._store.snapshot_index(),
        )

    # ------------------------------------------------------------------ startup and recovery

    def _recover(self) -> None:
        """Load the persistent term and vote. (Replaying the local log arrives with crash recovery.)"""
        self._term, self._voted_for = self._store.load_hard_state()

    # ------------------------------------------------------------------ small helpers

    def _spawn(self, coro: Awaitable, name: str) -> asyncio.Task:
        task = asyncio.ensure_future(self._guard(coro, name))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _guard(self, coro: Awaitable, name: str) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # a bug: record it loudly instead of dying silently
            self.fatal_error = exc
            logger.exception("[%s t%d] background task %s crashed", self.node_id, self._term, name)

    def _info(self, msg: str, *args) -> None:
        logger.info("[%s t%d] " + msg, self.node_id, self._term, *args)

    def _debug(self, msg: str, *args) -> None:
        logger.debug("[%s t%d] " + msg, self.node_id, self._term, *args)

    def _warn(self, msg: str, *args) -> None:
        logger.warning("[%s t%d] " + msg, self.node_id, self._term, *args)

    def _reset_election_deadline(self) -> None:
        low = self._config.election_timeout_min_ms / 1000
        high = self._config.election_timeout_max_ms / 1000
        self._election_deadline = self._now() + self._rng.uniform(low, high)

    def _set_term(self, term: int, voted_for: NodeId | None) -> None:
        """Persist first, then update memory, so a failed write never leaves us ahead of our disk."""
        self._store.save_hard_state(term, voted_for)
        self._term, self._voted_for = term, voted_for

    def _set_role(self, role: str) -> None:
        if role != self._role:
            self._role = role
            self._info("role -> %s", role)
            events.emit("ROLE_CHANGE", role=role, term=self._term)

    def _last_log(self) -> tuple[int, int]:
        """(last_log_index, last_log_term)."""
        index = self._store.last_index()
        return index, self._store.term_at(index) or 0

    def _log_is_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Raft section 5.4.1: is the candidate's log at least as up to date as ours?"""
        my_index, my_term = self._last_log()
        return (last_term, last_index) >= (my_term, my_index)

    async def _rpc(self, awaitable: Awaitable, timeout_s: float):
        try:
            return await asyncio.wait_for(awaitable, timeout_s)
        except asyncio.TimeoutError:
            raise TransportError("rpc timed out") from None

    # ------------------------------------------------------------------ role transitions

    def _become_follower(self, term: int, leader_id: NodeId | None = None) -> None:
        if term > self._term:
            self._set_term(term, None)
        self._set_role(FOLLOWER)
        self._leader_id = leader_id
        self._stop_replication()
        self._reset_election_deadline()

    def _note_leader_contact(self, leader_id: NodeId) -> None:
        self._leader_id = leader_id
        self._reset_election_deadline()

    def _become_leader(self) -> None:
        self._set_role(LEADER)
        self._leader_id = self.node_id
        now = self._now()
        last_index = self._store.last_index()
        for peer in self._peers:
            self._next_index[peer] = last_index + 1
            self._match_index[peer] = 0
            self._ack_time[peer] = now
        # A NOOP of our own term lets entries from earlier terms commit (Raft section 5.4.2).
        self._store.append([LogEntry(last_index + 1, self._term, NOOP_COMMAND)])
        self._info("elected leader")
        events.emit("LEADER_ELECTED", term=self._term)
        if self.on_leader_elected is not None:
            self.on_leader_elected(self)
        self._start_replication()

    # ------------------------------------------------------------------ ticker and elections

    async def _ticker_loop(self) -> None:
        tick_s = max(0.001, min(0.010, self._config.heartbeat_interval_ms / 3000))
        while True:
            await asyncio.sleep(tick_s)
            now = self._now()
            if (
                self._role != LEADER
                and now >= self._election_deadline
                and (self._election_task is None or self._election_task.done())
            ):
                self._election_task = asyncio.ensure_future(self._guard(self._run_election(), "election"))

    async def _run_election(self) -> None:
        self.stats["elections_started"] += 1
        self._reset_election_deadline()
        if self._role == LEADER or self._stopped:
            return
        self._set_term(self._term + 1, self.node_id)
        term = self._term
        self._set_role(CANDIDATE)
        self._leader_id = None
        self._reset_election_deadline()
        last_index, last_term = self._last_log()
        request = RequestVoteRequest(term, self.node_id, last_index, last_term)
        self._info("starting election")
        won = await self._collect_votes(request, lambda: self._role == CANDIDATE and self._term == term)
        if won and self._role == CANDIDATE and self._term == term and not self._stopped:
            self._become_leader()

    async def _collect_votes(self, request: RequestVoteRequest, still_relevant: Callable[[], bool]) -> bool:
        """Ask every peer concurrently; True as soon as a majority (counting ourselves) grants."""
        granted = 1
        pending = {asyncio.ensure_future(self._ask_vote(peer, request)) for peer in self._peers}
        try:
            while pending and granted < self._majority:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if not still_relevant():
                    return False
                for task in done:
                    response = task.result()
                    if response is None:
                        continue
                    if response.term > self._term:
                        self._become_follower(response.term)
                        return False
                    if response.vote_granted:
                        granted += 1
        finally:
            for task in pending:
                task.cancel()
        return granted >= self._majority and still_relevant()

    async def _ask_vote(self, peer: NodeId, request: RequestVoteRequest) -> RequestVoteResponse | None:
        try:
            return await self._rpc(
                self._transport.request_vote(peer, request), self._config.rpc_timeout_ms / 1000
            )
        except TransportError:
            return None

    # ------------------------------------------------------------------ RequestVote handler

    async def on_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        if req.term > self._term:
            self._become_follower(req.term)
        if req.term < self._term:
            return RequestVoteResponse(self._term, False)
        if self._voted_for not in (None, req.candidate_id):
            return RequestVoteResponse(self._term, False)
        if not self._log_is_up_to_date(req.last_log_index, req.last_log_term):
            return RequestVoteResponse(self._term, False)
        if self._voted_for != req.candidate_id:
            self._set_term(self._term, req.candidate_id)  # durable before we reply
        self._reset_election_deadline()
        self._debug("granted vote to %s", req.candidate_id)
        return RequestVoteResponse(self._term, True)

    # ------------------------------------------------------------------ AppendEntries handler

    async def on_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """Heartbeat handling only: accept the leader and reset the election timer."""
        if req.term < self._term:
            return AppendEntriesResponse(self._term, False)
        self._become_follower(req.term, req.leader_id)
        self._note_leader_contact(req.leader_id)
        return AppendEntriesResponse(self._term, True)

    # ------------------------------------------------------------------ InstallSnapshot handler

    async def on_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse:
        raise NotImplementedError("snapshots are added in a later step")

    # ------------------------------------------------------------------ leader: replication

    def _start_replication(self) -> None:
        term = self._term
        for peer in self._peers:
            self._replication_kick[peer] = asyncio.Event()
            self._replication_tasks[peer] = asyncio.ensure_future(
                self._guard(self._replicate_loop(peer, term), f"replicate-{peer}")
            )

    def _stop_replication(self) -> None:
        for task in self._replication_tasks.values():
            task.cancel()
        self._replication_tasks.clear()
        self._replication_kick.clear()

    async def _replicate_loop(self, peer: NodeId, term: int) -> None:
        kick = self._replication_kick[peer]
        heartbeat_s = self._config.heartbeat_interval_ms / 1000
        while self._role == LEADER and self._term == term:
            if await self._replicate_once(peer, term):
                continue  # more to send right now
            try:
                await asyncio.wait_for(kick.wait(), heartbeat_s)
            except asyncio.TimeoutError:
                pass
            kick.clear()

    async def _replicate_once(self, peer: NodeId, term: int) -> bool:
        """Send one heartbeat (an empty AppendEntries). Log replication arrives in the next step."""
        request = AppendEntriesRequest(term, self.node_id, 0, 0, [], 0)
        try:
            response = await self._rpc(
                self._transport.append_entries(peer, request), self._config.rpc_timeout_ms / 1000
            )
        except TransportError:
            return False
        if self._role != LEADER or self._term != term:
            return False
        if response.term > self._term:
            self._become_follower(response.term)
            return False
        self._ack_time[peer] = self._now()
        return False


def _elapsed_ms(started: float) -> float:
    return (time.monotonic() - started) * 1000.0
