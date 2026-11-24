# ruff: noqa: TRY004  (a malformed config file is bad input, so every check raises ValueError, as in genesis.py)
"""Cluster configuration loader (V7.5): reads and validates `cluster/*.yaml` (implementation.md 3.12).

Paths in the file (`genesis`, `data_dir`) are used as written, so relative paths mean "relative to the
directory the node is started from" (the repo root, for the launcher).
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any

import yaml

from ledgernet.common.types import NodeId, RaftConfig

DEFAULT_SUBMIT_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class NodeSpec:
    id: NodeId
    host: str
    port: int
    data_dir: str

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class ClusterConfig:
    genesis_path: str
    raft: RaftConfig
    submit_timeout_s: float
    nodes: tuple[NodeSpec, ...]

    def node(self, node_id: NodeId) -> NodeSpec:
        for spec in self.nodes:
            if spec.id == node_id:
                return spec
        raise KeyError(f"unknown node id {node_id!r} (known: {', '.join(self.node_ids())})")

    def node_ids(self) -> list[NodeId]:
        return [n.id for n in self.nodes]

    def addresses(self) -> dict[NodeId, str]:
        return {n.id: n.address for n in self.nodes}


def load_cluster_config(path: str) -> ClusterConfig:
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    try:
        return parse_cluster_config(doc)
    except ValueError as exc:
        raise ValueError(f"{path}: {exc}") from exc


def parse_cluster_config(doc: Any) -> ClusterConfig:
    if not isinstance(doc, dict):
        raise ValueError("config must be a mapping")
    unknown = set(doc) - {"genesis", "raft", "submit_timeout_s", "nodes"}
    if unknown:
        raise ValueError(f"unknown top-level key(s): {', '.join(sorted(unknown))}")

    genesis = doc.get("genesis")
    if not isinstance(genesis, str) or not genesis:
        raise ValueError("'genesis' must be a path to genesis.json")

    submit_timeout_s = doc.get("submit_timeout_s", DEFAULT_SUBMIT_TIMEOUT_S)
    if isinstance(submit_timeout_s, bool) or not isinstance(submit_timeout_s, (int, float)):
        raise ValueError("'submit_timeout_s' must be a number")
    if submit_timeout_s <= 0:
        raise ValueError("'submit_timeout_s' must be positive")

    return ClusterConfig(
        genesis_path=genesis,
        raft=_parse_raft(doc.get("raft") or {}),
        submit_timeout_s=float(submit_timeout_s),
        nodes=_parse_nodes(doc.get("nodes")),
    )


def _parse_raft(raw: Any) -> RaftConfig:
    if not isinstance(raw, dict):
        raise ValueError("'raft' must be a mapping")
    fields = {f.name: f for f in dataclasses.fields(RaftConfig)}
    unknown = set(raw) - set(fields)
    if unknown:
        raise ValueError(f"unknown raft setting(s): {', '.join(sorted(unknown))}")
    defaults = RaftConfig()
    for name, value in raw.items():
        expected = type(getattr(defaults, name))
        if expected is bool:
            if not isinstance(value, bool):
                raise ValueError(f"raft.{name} must be true or false")
        elif isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"raft.{name} must be an integer")
    config = RaftConfig(**raw)

    if config.election_timeout_min_ms <= 0 or config.rpc_timeout_ms <= 0 or config.heartbeat_interval_ms <= 0:
        raise ValueError("raft timeouts must be positive")
    if config.election_timeout_min_ms > config.election_timeout_max_ms:
        raise ValueError("raft.election_timeout_min_ms must not exceed election_timeout_max_ms")
    if config.heartbeat_interval_ms >= config.election_timeout_min_ms:
        raise ValueError("raft.heartbeat_interval_ms must be below election_timeout_min_ms")
    if config.max_entries_per_append < 1:
        raise ValueError("raft.max_entries_per_append must be at least 1")
    if config.snapshot_interval < 0:
        raise ValueError("raft.snapshot_interval must be 0 (off) or positive")
    return config


def _parse_nodes(raw: Any) -> tuple[NodeSpec, ...]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("'nodes' must be a non-empty list")
    specs: list[NodeSpec] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"nodes[{i}] must be a mapping")
        missing = {"id", "host", "port", "data_dir"} - set(item)
        if missing:
            raise ValueError(f"nodes[{i}] is missing: {', '.join(sorted(missing))}")
        extra = set(item) - {"id", "host", "port", "data_dir"}
        if extra:
            raise ValueError(f"nodes[{i}] has unknown key(s): {', '.join(sorted(extra))}")
        node_id, host, port, data_dir = item["id"], item["host"], item["port"], item["data_dir"]
        if not isinstance(node_id, str) or not node_id:
            raise ValueError(f"nodes[{i}].id must be a non-empty string")
        if not isinstance(host, str) or not host:
            raise ValueError(f"nodes[{i}].host must be a non-empty string")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError(f"nodes[{i}].port must be an integer from 1 to 65535")
        if not isinstance(data_dir, str) or not data_dir:
            raise ValueError(f"nodes[{i}].data_dir must be a non-empty string")
        specs.append(NodeSpec(node_id, host, port, data_dir))

    ids = [s.id for s in specs]
    if len(set(ids)) != len(ids):
        raise ValueError("node ids must be unique")
    endpoints = [(s.host, s.port) for s in specs]
    if len(set(endpoints)) != len(endpoints):
        raise ValueError("two nodes share the same host:port")
    return tuple(specs)
