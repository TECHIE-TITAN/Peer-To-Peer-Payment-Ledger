"""SimCluster: a whole Raft cluster in one process, plus the safety-invariant checker (owned by P).

Crash simulation matches a real process: `crash` throws the RaftNode and its state machine away, while
`restart` builds a new RaftNode on the same log and snapshot stores, like a process that lost its memory
but kept its disk.
"""

from __future__ import annotations

import asyncio
import random

from ledgernet.common.errors import LeadershipLostError, NotLeaderError
from ledgernet.common.types import ApplyResult, LogEntry, NodeId, RaftConfig
from ledgernet.raft.node import LEADER, RaftNode
from ledgernet.raft.sim_network import SimNetwork, SimTransport
from ledgernet.testing.fakes import InMemoryLogStore, InMemorySnapshotStore, RecordingStateMachine

LOG_MATCHING_WINDOW = 64  # entries compared per node pair in the periodic (non-full) check


class AppliedHistory:
    """What every node applied at every index. Two nodes disagreeing at an index is a safety violation."""

    def __init__(self) -> None:
        self.at_index: dict[int, tuple[int, bytes, str]] = {}
        self.violations: list[str] = []

    def record(self, node_id: NodeId, entry: LogEntry, result: ApplyResult) -> None:
        seen = (entry.term, entry.command, result.block_hash)
        known = self.at_index.setdefault(entry.index, seen)
        if known != seen:
            self.violations.append(
                f"state machine safety: {node_id} applied {seen} at index {entry.index}, others applied {known}"
            )


class SimStateMachine(RecordingStateMachine):
    def __init__(self, node_id: NodeId, history: AppliedHistory) -> None:
        super().__init__()
        self._node_id = node_id
        self._history = history

    def apply(self, entry: LogEntry) -> ApplyResult:
        result = super().apply(entry)
        self._history.record(self._node_id, entry, result)
        return result


class SimCluster:
    def __init__(self, size: int = 5, seed: int = 0, config: RaftConfig | None = None) -> None:
        self.seed = seed
        self.config = config or RaftConfig()
        self.node_ids: list[NodeId] = [f"n{i}" for i in range(1, size + 1)]
        self.network = SimNetwork(seed, self.config.rpc_timeout_ms / 1000)
        self.log_stores = {n: InMemoryLogStore() for n in self.node_ids}
        self.snapshot_stores = {n: InMemorySnapshotStore() for n in self.node_ids}
        self.nodes: dict[NodeId, RaftNode | None] = {n: None for n in self.node_ids}
        self.history = AppliedHistory()
        self.violations: list[str] = self.history.violations
        self.leader_of_term: dict[int, NodeId] = {}
        self.acked: list[tuple[int, int, bytes]] = []  # (index, term, command) confirmed by wait_for_apply
        self._incarnations = {n: 0 for n in self.node_ids}
        self._last_commit: dict[NodeId, int] = {}
        self._monitor: asyncio.Task | None = None

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        for node_id in self.node_ids:
            await self._build_and_start(node_id)

    async def stop(self) -> None:
        if self._monitor is not None:
            self._monitor.cancel()
            await asyncio.gather(self._monitor, return_exceptions=True)
        for node in self.nodes.values():
            if node is not None:
                await node.stop()
        await self.network.shutdown()

    async def _build_and_start(self, node_id: NodeId) -> RaftNode:
        self._incarnations[node_id] += 1
        node_seed = self.seed * 100_003 + int(node_id[1:]) * 1_009 + self._incarnations[node_id]
        node = RaftNode(
            node_id,
            self.node_ids,
            SimTransport(node_id, self.network),
            self.log_stores[node_id],
            self.snapshot_stores[node_id],
            SimStateMachine(node_id, self.history),
            self.config,
        )
        node._now = asyncio.get_running_loop().time
        node._rng = random.Random(node_seed)
        node.on_leader_elected = self._on_leader_elected
        self.nodes[node_id] = node
        self._last_commit[node_id] = 0
        await node.start()
        return node

    async def crash(self, node_id: NodeId) -> None:
        node = self.nodes[node_id]
        if node is not None:
            await node.stop()
            self.nodes[node_id] = None

    async def restart(self, node_id: NodeId) -> None:
        await self.crash(node_id)
        await self._build_and_start(node_id)

    async def crash_all(self) -> None:
        for node_id in self.node_ids:
            await self.crash(node_id)

    async def restart_all(self) -> None:
        for node_id in self.node_ids:
            await self.restart(node_id)

    def live(self) -> list[RaftNode]:
        return [node for node in self.nodes.values() if node is not None]

    def live_ids(self) -> list[NodeId]:
        return [node.node_id for node in self.live()]

    # ---------------------------------------------------------------- leaders and proposing

    def leader(self) -> RaftNode | None:
        """The live leader with the highest term (older leaders may linger until they hear the new term)."""
        leaders = [n for n in self.live() if n.status().role == LEADER]
        return max(leaders, key=lambda n: n.status().term) if leaders else None

    def leaders_in_term(self, term: int) -> list[NodeId]:
        return [n.node_id for n in self.live() if n.status().role == LEADER and n.status().term == term]

    async def wait_for_leader(self, timeout_s: float = 15.0, among: list[NodeId] | None = None) -> RaftNode:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            leader = self.leader()
            if leader is not None and (among is None or leader.node_id in among):
                return leader
            await asyncio.sleep(0.01)
        raise AssertionError(f"no leader within {timeout_s}s (seed {self.seed}): {self.describe()}")

    async def propose_and_wait(self, command: bytes, timeout_s: float = 30.0) -> ApplyResult:
        """Retry until some leader commits and applies the command; remember it as acknowledged."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            leader = self.leader()
            if leader is None:
                await asyncio.sleep(0.02)
                continue
            try:
                index, term = await leader.propose(command)
                result = await leader.wait_for_apply(index, term, timeout=3.0)
            except (NotLeaderError, LeadershipLostError, asyncio.TimeoutError):
                await asyncio.sleep(0.02)
                continue
            self.acked.append((result.index, result.term, command))
            return result
        raise AssertionError(
            f"command not committed within {timeout_s}s (seed {self.seed}): {self.describe()}"
        )

    async def wait_until(self, condition, timeout_s: float = 30.0, what: str = "condition") -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while loop.time() < deadline:
            if condition():
                return
            await asyncio.sleep(0.01)
        raise AssertionError(f"timed out waiting for {what} (seed {self.seed}): {self.describe()}")

    async def wait_for_convergence(self, timeout_s: float = 30.0) -> None:
        """Every live node has applied the same prefix and holds the same digest."""

        def converged() -> bool:
            nodes = self.live()
            if not nodes:
                return False
            applied = {n.status().last_applied for n in nodes}
            commits = {n.status().commit_index for n in nodes}
            digests = {n._sm.state_hash() for n in nodes}
            return len(applied) == len(commits) == len(digests) == 1 and applied == commits

        await self.wait_until(converged, timeout_s, "convergence")

    def describe(self) -> str:
        parts = []
        for node_id in self.node_ids:
            node = self.nodes[node_id]
            if node is None:
                parts.append(f"{node_id}:down")
            else:
                s = node.status()
                parts.append(
                    f"{node_id}:{s.role[0]}t{s.term} c{s.commit_index} a{s.last_applied} l{s.last_log_index}"
                )
        return " ".join(parts)

    # ---------------------------------------------------------------- invariants

    def _on_leader_elected(self, node: RaftNode) -> None:
        term = node.status().term
        known = self.leader_of_term.setdefault(term, node.node_id)
        if known != node.node_id:
            self.violations.append(f"election safety: {known} and {node.node_id} both lead term {term}")
        # Leader Completeness: a new leader's log holds every entry acknowledged in an earlier term.
        store = self.log_stores[node.node_id]
        for index, entry_term, _ in self.acked:
            if entry_term < term and store.term_at(index) != entry_term and index > store.snapshot_index():
                self.violations.append(
                    f"leader completeness: {node.node_id} (term {term}) lacks acknowledged entry {index}@{entry_term}"
                )

    def check_invariants(self, full: bool = False) -> None:
        """Raise AssertionError if any safety property is broken. `full` compares whole committed logs."""
        for node in self.live():
            if node.fatal_error is not None:
                raise AssertionError(f"{node.node_id} background task crashed: {node.fatal_error!r}")
            commit = node.status().commit_index
            if commit < self._last_commit[node.node_id]:
                self.violations.append(
                    f"{node.node_id} commit index went backwards {self._last_commit[node.node_id]} -> {commit}"
                )
            self._last_commit[node.node_id] = commit
        self._check_log_matching(full)
        if self.violations:
            raise AssertionError(f"seed {self.seed}: " + "; ".join(self.violations[:5]))

    def _committed(self, node_id: NodeId) -> int:
        node = self.nodes[node_id]
        return (
            node.status().commit_index if node is not None else self.log_stores[node_id].load_commit_index()
        )

    def _check_log_matching(self, full: bool) -> None:
        """Any two nodes hold identical entries at every index both have committed (Log Matching)."""
        for position, a in enumerate(self.node_ids):
            for b in self.node_ids[position + 1 :]:
                store_a, store_b = self.log_stores[a], self.log_stores[b]
                limit = min(
                    self._committed(a), self._committed(b), store_a.last_index(), store_b.last_index()
                )
                start = max(store_a.snapshot_index(), store_b.snapshot_index()) + 1
                if not full:
                    start = max(start, limit - LOG_MATCHING_WINDOW)
                if start > limit:
                    continue
                for x, y in zip(store_a.entries(start, limit + 1), store_b.entries(start, limit + 1)):
                    if x != y:
                        self.violations.append(
                            f"log matching: {a} and {b} differ at committed index {x.index}"
                        )
                        return

    def start_monitor(self, interval_s: float = 0.02) -> None:
        """Check invariants continuously in the background; the first failure is kept in `violations`."""

        async def watch() -> None:
            while True:
                await asyncio.sleep(interval_s)
                try:
                    self.check_invariants()
                except AssertionError as exc:
                    if str(exc) not in self.violations:
                        self.violations.append(str(exc))
                    return

        self._monitor = asyncio.ensure_future(watch())
