"""RaftNode: election, replication, commit, apply and local-log recovery (owned by P).

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
from collections import OrderedDict
from collections.abc import Awaitable, Callable

from ledgernet.common import events
from ledgernet.common.errors import LeadershipLostError, NotLeaderError, TransportError
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

APPLY_BATCH_SIZE = 100  # entries applied between yields to the event loop
RESULT_CACHE_SIZE = 10_000  # recent ApplyResults kept for wait_for_apply

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
        self._apply_wakeup: asyncio.Event | None = None

        # Client futures for wait_for_apply.
        self._waiters: dict[int, list[tuple[int, asyncio.Future]]] = {}
        self._results: OrderedDict[int, ApplyResult] = OrderedDict()

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._recover()
        self._apply_wakeup = asyncio.Event()
        self._stopped = False
        self._reset_election_deadline()
        await self._transport.start(self)
        self._spawn(self._ticker_loop(), "ticker")
        self._spawn(self._apply_loop(), "applier")
        if self._commit_index > self._sm.last_applied():
            self._apply_wakeup.set()
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
        for futures in self._waiters.values():
            for _, future in futures:
                if not future.done():
                    future.set_exception(LeadershipLostError("node stopped"))
        self._waiters.clear()
        await self._transport.stop()

    # ------------------------------------------------------------------ Consensus API

    async def propose(self, command: bytes) -> tuple[int, int]:
        if self._role != LEADER or self._stopped:
            raise NotLeaderError(self._leader_id)
        index = self._store.last_index() + 1
        term = self._term
        self._store.append([LogEntry(index, term, command)])  # durable before we count ourselves
        self._kick_replication()
        self._advance_commit()  # matters for a single-node cluster
        return index, term

    async def wait_for_apply(self, index: int, term: int, timeout: float) -> ApplyResult:
        cached = self._results.get(index)
        if cached is not None:
            return self._check_result_term(cached, term)
        if self._sm.last_applied() >= index:
            # Applied long ago (evicted from the cache) or skipped by a snapshot: the result is gone.
            raise LeadershipLostError(f"result for index {index} is no longer available")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(index, []).append((term, future))
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            remaining = [w for w in self._waiters.get(index, []) if w[1] is not future]
            if remaining:
                self._waiters[index] = remaining
            else:
                self._waiters.pop(index, None)

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
        """Replay the local log up to the persisted commit index, so a restart need not wait for a leader."""
        self._term, self._voted_for = self._store.load_hard_state()

        started = time.monotonic()
        target = min(self._store.load_commit_index(), self._store.last_index())
        replayed = 0
        while self._sm.last_applied() < target:
            first = self._sm.last_applied() + 1
            entries = self._store.entries(first, min(target, first + APPLY_BATCH_SIZE - 1) + 1)
            if not entries:
                raise RuntimeError(f"local log has no entry at index {first} during replay")
            for entry in entries:
                self._record_result(self._sm.apply(entry))
                replayed += 1
        self._commit_index = max(target, self._sm.last_applied())
        events.emit("LOCAL_REPLAY_DONE", applied=replayed, ms=_elapsed_ms(started))
        self._info("recovered: term=%d commit=%d replayed=%d", self._term, self._commit_index, replayed)

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

    def _check_result_term(self, result: ApplyResult, term: int) -> ApplyResult:
        if result.term != term:
            raise LeadershipLostError(f"entry {result.index} was committed in term {result.term}, not {term}")
        return result

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
        self._advance_commit()

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
        if req.term < self._term:
            return AppendEntriesResponse(self._term, False)
        self._become_follower(req.term, req.leader_id)
        self._note_leader_contact(req.leader_id)

        prev_index, entries = req.prev_log_index, list(req.entries)
        snapshot_index = self._store.snapshot_index()
        if prev_index < snapshot_index:
            # The part before our snapshot is committed and therefore identical to the leader's.
            entries = [e for e in entries if e.index > snapshot_index]
            prev_index = snapshot_index
        else:
            last_index = self._store.last_index()
            if prev_index > last_index:
                return AppendEntriesResponse(
                    self._term, False, conflict_index=last_index + 1, conflict_term=0
                )
            local_term = self._store.term_at(prev_index)
            if local_term != req.prev_log_term:
                return self._conflict_response(prev_index, local_term or 0)

        self._merge_entries(prev_index, entries)
        last_new = req.prev_log_index + len(req.entries)
        if req.leader_commit > self._commit_index:
            self._set_commit(max(self._commit_index, min(req.leader_commit, last_new)))
        return AppendEntriesResponse(self._term, True, match_index=last_new)

    def _conflict_response(self, prev_index: int, conflict_term: int) -> AppendEntriesResponse:
        """Tell the leader where our log first disagrees, so it can skip a whole term at once."""
        first = prev_index
        floor = self._store.snapshot_index() + 1
        while first > floor and self._store.term_at(first - 1) == conflict_term:
            first -= 1
        return AppendEntriesResponse(self._term, False, conflict_index=first, conflict_term=conflict_term)

    def _merge_entries(self, prev_index: int, entries: list[LogEntry]) -> None:
        """Append what is new; truncate only where a stored entry has a different term.

        Entries whose term already matches are skipped, never truncated: a delayed or duplicated
        AppendEntries must not delete newer entries.
        """
        for position, entry in enumerate(entries):
            existing_term = self._store.term_at(entry.index)
            if existing_term == entry.term:
                continue
            if existing_term is not None:
                if entry.index <= self._commit_index:
                    raise RuntimeError(f"leader asks to overwrite committed entry {entry.index}")
                self._store.truncate_from(entry.index)
            self._store.append(entries[position:])  # durable before we answer success
            return

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

    def _kick_replication(self) -> None:
        for event in self._replication_kick.values():
            event.set()

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
        """Send one AppendEntries (or InstallSnapshot). True means: go again immediately."""
        next_index = min(self._next_index[peer], self._store.last_index() + 1)
        prev_index = next_index - 1
        prev_term = self._store.term_at(prev_index) or 0
        last_index = self._store.last_index()
        entries: list[LogEntry] = []
        if next_index <= last_index:
            stop = min(last_index + 1, next_index + self._config.max_entries_per_append)
            entries = self._store.entries(next_index, stop)
        request = AppendEntriesRequest(term, self.node_id, prev_index, prev_term, entries, self._commit_index)
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
        if response.success:
            self._match_index[peer] = max(self._match_index[peer], response.match_index)
            self._next_index[peer] = self._match_index[peer] + 1
            self._advance_commit()
            return self._next_index[peer] <= self._store.last_index()

        self._next_index[peer] = self._backed_up_next_index(peer, next_index, response)
        return True

    def _backed_up_next_index(self, peer: NodeId, tried: int, response: AppendEntriesResponse) -> int:
        """Fast backup: jump past our last entry of the follower's conflicting term, else to its hint."""
        if response.conflict_term:
            last_of_term = self._last_index_of_term(response.conflict_term)
            candidate = last_of_term + 1 if last_of_term else response.conflict_index
        else:
            candidate = response.conflict_index
        if candidate <= 0 or candidate >= tried:
            candidate = tried - 1  # always make progress
        return max(candidate, self._match_index[peer] + 1, 1)

    def _last_index_of_term(self, term: int) -> int:
        """Last index in our log whose entry has `term`, or 0 if our log has none."""
        index = self._store.last_index()
        floor = self._store.snapshot_index()
        while index > floor:
            entry_term = self._store.term_at(index)
            if entry_term is not None and entry_term <= term:
                return index if entry_term == term else 0
            index -= 1
        return floor if floor and self._store.snapshot_term() == term else 0

    def _advance_commit(self) -> None:
        """Commit the highest N stored on a majority, but only if entry N is from the current term."""
        if self._role != LEADER:
            return
        stored = sorted(
            [self._store.last_index()] + [self._match_index[p] for p in self._peers], reverse=True
        )
        candidate = stored[self._majority - 1]
        if candidate > self._commit_index and self._store.term_at(candidate) == self._term:
            self._set_commit(candidate)

    def _set_commit(self, index: int) -> None:
        if index <= self._commit_index:
            return
        self._commit_index = index
        self._store.save_commit_index(index)
        if self._apply_wakeup is not None:
            self._apply_wakeup.set()

    # ------------------------------------------------------------------ apply loop and snapshots

    async def _apply_loop(self) -> None:
        assert self._apply_wakeup is not None
        while True:
            await self._apply_wakeup.wait()
            self._apply_wakeup.clear()
            while self._sm.last_applied() < self._commit_index:
                self._apply_batch()
                await asyncio.sleep(0)  # let heartbeats and RPCs run between batches

    def _apply_batch(self) -> None:
        """Apply up to APPLY_BATCH_SIZE committed entries in index order. Runs without awaiting."""
        first = self._sm.last_applied() + 1
        last = min(self._commit_index, first + APPLY_BATCH_SIZE - 1)
        entries = self._store.entries(first, last + 1)
        if not entries:
            raise RuntimeError(f"committed entry {first} is missing from the log")
        for entry in entries:
            self._record_result(self._sm.apply(entry))

    def _record_result(self, result: ApplyResult) -> None:
        self._results[result.index] = result
        while len(self._results) > RESULT_CACHE_SIZE:
            self._results.popitem(last=False)
        for term, future in self._waiters.pop(result.index, []):
            if future.done():
                continue
            if result.term == term:
                future.set_result(result)
            else:
                future.set_exception(
                    LeadershipLostError(
                        f"entry {result.index} was committed in term {result.term}, not {term}"
                    )
                )


def _elapsed_ms(started: float) -> float:
    return (time.monotonic() - started) * 1000.0
