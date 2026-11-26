"""Cluster launcher (V7.7): starts, kills and heals real node processes for the e2e tests and experiments.

    async with Cluster("cluster/cluster.local.yaml") as cluster:
        await cluster.start_all()
        leader = await cluster.wait_for_leader()
        cluster.kill(leader)                       # SIGKILL, the crash the tests care about
        await cluster.wait_for_leader()            # a new one is elected
        await cluster.start(leader)                # same data_dir: recovers from its own disk
        await cluster.wait_converged()

Local mode runs one `python -m ledgernet.node` subprocess per node. Faults go through the Admin service, so
partitions need no root. SSH mode (RCE) arrives with week 3; asking for it now fails loudly.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterable, Sequence
from typing import Any

import grpc
import yaml

from ledgernet.client import LedgerClient
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.node_config import ClusterConfig, load_cluster_config

POLL_S = 0.05
DEFAULT_TIMEOUT_S = 30.0


class ClusterError(RuntimeError):
    pass


class Cluster:
    def __init__(
        self,
        config_path: str,
        mode: str = "local",
        *,
        consensus: str = "raft",
        storage: str = "file",
        ledger: str = "sqlite",
        unsafe_debug: bool = False,
        cwd: str | None = None,
        python: str | None = None,
    ) -> None:
        if mode != "local":
            raise NotImplementedError(f"mode {mode!r}: only 'local' exists so far (ssh arrives in week 3)")
        self.config_path = os.path.abspath(config_path)
        self.config: ClusterConfig = load_cluster_config(config_path)
        self.genesis_path = self.config.genesis_path
        self._flags = ["--consensus", consensus, "--storage", storage, "--ledger", ledger]
        if unsafe_debug:
            self._flags.append("--unsafe-debug")
        # LEDGERNET_LOG_LEVEL=DEBUG makes every node log Raft's decisions (to <data_dir>/node.log).
        self._flags += ["--log-level", os.environ.get("LEDGERNET_LOG_LEVEL", "INFO")]
        self._cwd = cwd or os.getcwd()
        self._python = python or sys.executable
        self._procs: dict[str, subprocess.Popen] = {}
        self._logs: dict[str, Any] = {}
        self._client = LedgerClient(self.config.addresses(), rpc_timeout_s=2.0)
        self._admin_channels: dict[str, grpc.aio.Channel] = {}

    async def __aenter__(self) -> Cluster:  # noqa: PYI034 (typing.Self needs Python 3.11)
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    @property
    def node_ids(self) -> list[str]:
        return self.config.node_ids()

    @property
    def client(self) -> LedgerClient:
        return self._client

    def address(self, node_id: str) -> str:
        return self.config.node(node_id).address

    def data_dir(self, node_id: str) -> str:
        return self.config.node(node_id).data_dir

    # ---- process control

    def is_running(self, node_id: str) -> bool:
        proc = self._procs.get(node_id)
        return proc is not None and proc.poll() is None

    def running(self) -> list[str]:
        return [n for n in self.node_ids if self.is_running(n)]

    async def start(self, node_id: str, wait: bool = True) -> None:
        """Start one node (an error if it is already running) and, by default, wait until it answers."""
        self.config.node(node_id)  # unknown ids raise KeyError here
        if self.is_running(node_id):
            raise ClusterError(f"{node_id} is already running")
        self._spawn(node_id)
        if wait:
            await self.wait_ready(node_id)

    def _spawn(self, node_id: str) -> None:
        """Launch the node process (a quick, synchronous fork/exec) with its output going to stdout.log."""
        data_dir = self.data_dir(node_id)
        os.makedirs(data_dir, exist_ok=True)
        self._close_log(node_id)
        log = open(os.path.join(data_dir, "stdout.log"), "ab")  # noqa: SIM115 (closed in _close_log)
        self._logs[node_id] = log
        command = [self._python, "-m", "ledgernet.node", "--config", self.config_path]
        command += ["--id", node_id, *self._flags]
        self._procs[node_id] = subprocess.Popen(command, cwd=self._cwd, stdout=log, stderr=subprocess.STDOUT)

    async def start_all(self) -> None:
        for node_id in self.node_ids:
            await self.start(node_id, wait=False)
        for node_id in self.node_ids:
            await self.wait_ready(node_id)

    async def wait_ready(self, node_id: str, timeout: float = DEFAULT_TIMEOUT_S) -> None:
        """Block until the node answers GetStatus; fail fast if its process died."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            proc = self._procs.get(node_id)
            if proc is None or proc.poll() is not None:
                code = proc.returncode if proc else None
                raise ClusterError(f"{node_id} exited with code {code}\n{self.tail(node_id)}")
            try:
                await self._client.status(self.address(node_id))
                return
            except grpc.aio.AioRpcError:
                await asyncio.sleep(POLL_S)
        raise ClusterError(f"{node_id} did not become ready in {timeout}s\n{self.tail(node_id)}")

    def kill(self, node_id: str) -> None:
        """SIGKILL: no shutdown code runs. This is the crash the recovery tests need."""
        proc = self._procs.get(node_id)
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGKILL if hasattr(signal, "SIGKILL") else signal.SIGTERM)
            proc.wait()
        self._close_log(node_id)

    async def stop(self, node_id: str, timeout: float = 10.0) -> None:
        """SIGTERM: a graceful shutdown (closes the log store); SIGKILL if it takes longer than `timeout`."""
        proc = self._procs.get(node_id)
        if proc is None or proc.poll() is not None:
            self._close_log(node_id)
            return
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)
        self._close_log(node_id)

    def wipe(self, node_id: str) -> None:
        """Delete a stopped node's data directory.

        Use it between runs or before a node's first start. Do NOT restart a node that has already
        acknowledged entries after wiping it: Raft assumes acknowledged log entries survive, so the leader
        never retries below that node's old match_index and it stays stuck (and it could vote twice in a
        term). Bringing back a node with a lost disk needs a membership change, which this project omits.
        """
        if self.is_running(node_id):
            raise ClusterError(f"stop {node_id} before wiping it")
        path = self.data_dir(node_id)
        # On Windows a file of a just-exited process (or one an antivirus scanner opened) can stay locked for a
        # moment, and rmtree then leaves it behind. Retry briefly, and fail loudly instead of half-wiping.
        for attempt in range(50):
            shutil.rmtree(path, ignore_errors=True)
            if not os.path.exists(path):
                return
            time.sleep(0.05 * min(attempt + 1, 4))
        shutil.rmtree(path)  # raises with the real reason

    async def close(self) -> None:
        """Stop every node and release connections. Safe to call twice."""
        for node_id in list(self._procs):
            await self.stop(node_id)
        await self._client.close()
        for channel in self._admin_channels.values():
            await channel.close()
        self._admin_channels.clear()

    def _close_log(self, node_id: str) -> None:
        log = self._logs.pop(node_id, None)
        if log is not None:
            log.close()

    def tail(self, node_id: str, lines: int = 30) -> str:
        """The end of a node's stdout/stderr and node.log, for error messages."""
        out = []
        for name in ("stdout.log", "node.log"):
            path = os.path.join(self.data_dir(node_id), name)
            try:
                with open(path, errors="replace") as f:
                    out.append(f"--- {path}\n" + "".join(f.readlines()[-lines:]))
            except OSError:
                pass
        return "\n".join(out)

    # ---- observing the cluster

    async def statuses(self) -> dict[str, pb.GetStatusResponse]:
        """GetStatus of every running node that answers, keyed by node id."""
        live = {self.address(n): n for n in self.running()}
        answers = await self._client.status_all()
        return {live[a]: s for a, s in answers.items() if s is not None and a in live}

    async def leader(self) -> str | None:
        """The id of the live node claiming leadership in the highest term, or None."""
        leaders = [(s.term, n) for n, s in (await self.statuses()).items() if s.role == "leader"]
        return max(leaders)[1] if leaders else None

    async def wait_for_leader(self, timeout: float = DEFAULT_TIMEOUT_S, exclude: Iterable[str] = ()) -> str:
        excluded = set(exclude)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leader = await self.leader()
            if leader is not None and leader not in excluded:
                return leader
            await asyncio.sleep(POLL_S)
        raise ClusterError(f"no leader within {timeout}s; statuses: {await self.statuses()}")

    async def wait_converged(
        self, timeout: float = DEFAULT_TIMEOUT_S, nodes: Sequence[str] | None = None
    ) -> dict:
        """Wait until the given (default: all running) nodes agree on last_applied, state_hash and chain_head,
        and have applied everything the highest commit index covers. Returns their statuses."""
        wanted = set(nodes if nodes is not None else self.running())
        deadline = time.monotonic() + timeout
        last: dict = {}
        while time.monotonic() < deadline:
            last = await self.statuses()
            picked = {n: s for n, s in last.items() if n in wanted}
            if len(picked) == len(wanted) and picked:
                values = list(picked.values())
                same = {(s.last_applied, s.state_hash, s.chain_head) for s in values}
                if len(same) == 1 and values[0].last_applied >= max(s.commit_index for s in values):
                    return picked
            await asyncio.sleep(POLL_S)
        summary = {n: (s.role, s.term, s.commit_index, s.last_applied) for n, s in last.items()}
        raise ClusterError(f"not converged after {timeout}s: {summary}")

    # ---- faults (through the Admin service)

    def _admin(self, node_id: str) -> pb_grpc.AdminStub:
        channel = self._admin_channels.get(node_id)
        if channel is None:
            channel = self._admin_channels[node_id] = grpc.aio.insecure_channel(self.address(node_id))
        return pb_grpc.AdminStub(channel)

    async def set_faults(self, node_id: str, rules: Sequence[pb.FaultRule]) -> None:
        await self._admin(node_id).SetFaults(pb.SetFaultsRequest(rules=list(rules)), timeout=5)

    async def clear_faults(self, node_id: str) -> None:
        await self._admin(node_id).ClearFaults(pb.Empty(), timeout=5)

    async def partition(self, groups: Sequence[Sequence[str]]) -> None:
        """Split the running nodes into groups that cannot talk to each other. Nodes not named in any group
        form one more group together. Every node drops all RPCs to every node outside its own group."""
        named = [set(g) for g in groups]
        flat = [n for g in named for n in g]
        unknown = set(flat) - set(self.node_ids)
        if unknown or len(flat) != len(set(flat)):
            bad = sorted(unknown) or "a node listed twice"
            raise ValueError(f"groups must name known nodes, each once (problem: {bad})")
        rest = set(self.node_ids) - set(flat)
        if rest:
            named.append(rest)
        for group in named:
            outside = [n for n in self.node_ids if n not in group]
            rules = [pb.FaultRule(target=t, partitioned=True) for t in outside]
            for node_id in group:
                if self.is_running(node_id):
                    await self.set_faults(node_id, rules)

    async def heal(self) -> None:
        """Remove every injected fault from every running node."""
        for node_id in self.running():
            await self.clear_faults(node_id)

    # ---- events

    def collect_events(self) -> list[dict]:
        """Every node's events.jsonl merged and sorted by timestamp (for the experiments)."""
        merged: list[dict] = []
        for node_id in self.node_ids:
            path = os.path.join(self.data_dir(node_id), "events.jsonl")
            try:
                with open(path, encoding="utf-8") as f:
                    merged.extend(json.loads(line) for line in f if line.strip())
            except FileNotFoundError:
                continue
        return sorted(merged, key=lambda e: e["ts"])


def write_local_config(
    directory: str,
    genesis_path: str,
    n_nodes: int = 3,
    raft: dict | None = None,
    ports: Sequence[int] | None = None,
    submit_timeout_s: float = 5.0,
) -> str:
    """Write a `cluster.yaml` for n nodes on 127.0.0.1 into `directory` and return its path.

    Ports default to free ones picked now; data directories live under `directory`. Used by tests that need
    their own cluster; real runs use `cluster/cluster.local.yaml`.
    """
    import socket

    if ports is None:
        sockets = [socket.socket() for _ in range(n_nodes)]
        try:
            for s in sockets:
                s.bind(("127.0.0.1", 0))
            ports = [s.getsockname()[1] for s in sockets]
        finally:
            for s in sockets:
                s.close()
    doc = {
        "genesis": genesis_path,
        "raft": raft or {},
        "submit_timeout_s": submit_timeout_s,
        "nodes": [
            {"id": f"n{i}", "host": "127.0.0.1", "port": p, "data_dir": os.path.join(directory, f"n{i}")}
            for i, p in enumerate(ports, start=1)
        ],
    }
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "cluster.yaml")
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f)
    return path
