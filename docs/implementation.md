# LedgerNet — Implementation Plan (Team 14, Project 24)

**Team:** P = Pranjal Garg · A = Anushka Sinha · V = Vansh Goyal

This document is the single source of truth for building LedgerNet. It does three things:

1. **Freezes the contracts** (Section 3): the exact Python interfaces, data formats, wire protocol, hash rules and file layouts that connect the three parts. They are written out in full, including working fakes, so nobody waits for anybody.
2. **Splits the work** into three nearly independent work packages (Sections 5–7), each with tasks in order, acceptance criteria and a "Day 1" list.
3. **Plans integration** (Section 8) as a sequence of component swaps, so bugs are easy to localise.

> **Rule of thumb:** if you need something from a teammate's area, code against the interface in Section 3 and use the fake from `ledgernet/testing/fakes.py`. Never import a teammate's in-progress module before integration week.

---

## Table of contents

1. Ownership at a glance
2. Architecture and request flow
3. Frozen contracts (copy these into the repo first)
4. Step 0: repository bootstrap (≈30 minutes, anyone)
5. P — Consensus (Raft)
6. A — Ledger, cryptography, storage, audit, load generation and analysis
7. V — Networking, node runtime, wallet, cluster tooling, end-to-end tests, experiments
8. Integration plan and timeline
9. Dependency matrix
10. Test and demo scenarios (mapped to the proposal)
11. Metrics: exact definitions
12. Engineering rules (determinism, git, contract changes)
13. Glossary

---

## 1. Ownership at a glance

| Area | Owner | Directories |
|---|---|---|
| Raft: election, replication, commit, apply loop, persistence protocol, snapshots/compaction, InstallSnapshot, PreVote, CheckQuorum, simulated network for tests | **P** | `ledgernet/raft/`, `tests/raft/` |
| Transfer format and signatures, hash chain, ledger state machine on SQLite, write-ahead log (`FileLogStore`), snapshot store, offline auditor, load generator, analysis and plots | **A** | `ledgernet/ledger/`, `ledgernet/storage/`, `tools/`, `bench/`, `analysis/`, `tests/ledger/`, `tests/storage/` |
| Protobuf and gRPC, transport with fault injection, Ledger/Admin services, node process wiring and config, wallet CLI and client library, local and RCE cluster launcher, end-to-end scenarios, experiment orchestration | **V** | `proto/`, `ledgernet/net/`, `ledgernet/node.py`, `ledgernet/client/`, `cluster/`, `experiments/`, `tests/net/`, `tests/e2e/` |
| Contracts (Section 3) | **All three** (changes need everyone's approval) | `ledgernet/common/`, `ledgernet/testing/`, `proto/ledgernet.proto` |

Effort is roughly balanced: P has the hardest single component; A and V each have more, smaller components.

---

## 2. Architecture and request flow

```
                 ┌──────────────── one node process (×5) ──────────────────┐
 wallet / ──gRPC─▶ Ledger service (V) ──propose(cmd)──▶ RaftNode (P)       │
 loadgen        │      ▲  GetAccount/GetStatus/GetReceipt  │   │           │
                │      │                                    │   │ append    │
                │      └──── LedgerStateMachine (A) ◀─apply─┘   ▼ (fsync)   │
                │              SQLite state.db                FileLogStore  │
                │              FileSnapshotStore (A) ◀─snapshot   (A)       │
                │                                                           │
                │   GrpcTransport + FaultInjector (V) ◀──RPCs──▶ other nodes│
                │   Admin service (V): SetFaults / ClearFaults              │
                └───────────────────────────────────────────────────────────┘
```

**Life of a transfer**

1. The wallet builds a `Transfer` (sender pubkey, recipient pubkey, amount in paise, nonce), signs it with Ed25519 and encodes it as a 145-byte command (Section 3.4).
2. The wallet calls `Ledger.SubmitTransfer` on any node. A follower answers `NOT_LEADER` with a leader hint; the wallet retries at the leader.
3. The leader prechecks format and signature (cheap rejection of garbage), then calls `raft.propose(cmd)`, which appends the entry to its log (fsync) and replicates it.
4. Once 3 of 5 nodes have stored the entry, it is committed. Every node applies committed entries in log order through `LedgerStateMachine.apply`, using the same deterministic rules (Section 3.6). Every entry, accepted or rejected, extends the SHA-256 hash chain.
5. The leader's `wait_for_apply(index, term)` resolves and the service returns the `TxStatus` to the client.

---

## 3. Frozen contracts

Everything in this section goes into the repo **verbatim** in Step 0. The code here is complete and working, not pseudocode. The fakes in 3.9 are what make parallel work possible.

### 3.1 `ledgernet/common/types.py`

```python
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
```

### 3.2 `ledgernet/common/rpc_types.py` (Raft messages as plain dataclasses)

Raft (P) only ever sees these. V converts them to and from protobuf.

```python
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
```

### 3.3 `ledgernet/common/errors.py`

```python
from typing import Optional


class NotLeaderError(Exception):
    def __init__(self, leader_hint: Optional[str] = None):
        super().__init__(f"not leader (hint={leader_hint})")
        self.leader_hint = leader_hint


class LeadershipLostError(Exception):
    """The entry proposed at (index, term) was overwritten by a different term: it will never commit."""


class TransportError(Exception):
    """RPC failed: timeout, connection refused, injected drop or partition."""
```

### 3.4 `ledgernet/common/tx.py` (transfer encoding and signatures)

**Command encoding** (what goes into a log entry):

| Command | Bytes |
|---|---|
| NOOP | `0x00` (exactly 1 byte) |
| TRANSFER | `0x01` ‖ sender(32) ‖ recipient(32) ‖ amount(u64 BE) ‖ nonce(u64 BE) ‖ signature(64) = **145 bytes** |

**Signed message:** `b"LEDGERNET/v1/transfer"` ‖ sender ‖ recipient ‖ amount(u64 BE) ‖ nonce(u64 BE).
Amounts are integers in **paise** (Rs 500 = 50 000). No floats anywhere.

```python
from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

DOMAIN_TRANSFER = b"LEDGERNET/v1/transfer"
CMD_NOOP = 0x00
CMD_TRANSFER = 0x01
_BODY_FMT = ">32s32sQQ"
_TRANSFER_FMT = ">32s32sQQ64s"
TRANSFER_LEN = 1 + struct.calcsize(_TRANSFER_FMT)  # 145
NOOP_COMMAND = bytes([CMD_NOOP])


class DecodeError(ValueError):
    pass


@dataclass(frozen=True)
class Transfer:
    sender: bytes       # 32-byte raw Ed25519 public key
    recipient: bytes    # 32-byte raw Ed25519 public key
    amount: int         # paise
    nonce: int          # must equal sender's stored nonce + 1
    signature: bytes = b""

    def signing_bytes(self) -> bytes:
        return DOMAIN_TRANSFER + struct.pack(
            _BODY_FMT, self.sender, self.recipient, self.amount, self.nonce
        )

    def verify(self) -> bool:
        try:
            Ed25519PublicKey.from_public_bytes(self.sender).verify(
                self.signature, self.signing_bytes()
            )
            return True
        except (InvalidSignature, ValueError):
            return False


def public_bytes(priv: Ed25519PrivateKey) -> bytes:
    return priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


def sign_transfer(priv: Ed25519PrivateKey, recipient: bytes, amount: int, nonce: int) -> Transfer:
    unsigned = Transfer(public_bytes(priv), recipient, amount, nonce)
    return Transfer(unsigned.sender, recipient, amount, nonce, priv.sign(unsigned.signing_bytes()))


def encode_transfer(t: Transfer) -> bytes:
    if len(t.sender) != 32 or len(t.recipient) != 32 or len(t.signature) != 64:
        raise ValueError("bad field length")
    if not (0 <= t.amount < 2**64 and 0 <= t.nonce < 2**64):
        raise ValueError("amount/nonce out of range")
    return bytes([CMD_TRANSFER]) + struct.pack(
        _TRANSFER_FMT, t.sender, t.recipient, t.amount, t.nonce, t.signature
    )


def decode_command(cmd: bytes) -> Optional[Transfer]:
    """Returns None for NOOP, a Transfer for TRANSFER; raises DecodeError otherwise."""
    if cmd == NOOP_COMMAND:
        return None
    if len(cmd) != TRANSFER_LEN or cmd[0] != CMD_TRANSFER:
        raise DecodeError(f"bad command (len={len(cmd)})")
    s, r, a, n, sig = struct.unpack(_TRANSFER_FMT, cmd[1:])
    return Transfer(s, r, a, n, sig)


def tx_id(cmd: bytes) -> bytes:
    """Receipt key for a command."""
    return hashlib.sha256(cmd).digest()
```

### 3.5 `ledgernet/common/hashing.py` (hash chain and state hash)

```python
from __future__ import annotations

import hashlib
import struct
from typing import Dict, Tuple

GENESIS_DOMAIN = b"LEDGERNET/v1/genesis"
BLOCK_DOMAIN = b"LEDGERNET/v1/block"
STATE_DOMAIN = b"LEDGERNET/v1/state"


def genesis_hash(balances: Dict[bytes, int]) -> bytes:
    h = hashlib.sha256(GENESIS_DOMAIN)
    for pk in sorted(balances):
        h.update(pk + struct.pack(">Q", balances[pk]))
    return h.digest()


def block_hash(prev: bytes, index: int, term: int, command: bytes) -> bytes:
    """Every log entry (transfer, rejected transfer or NOOP) is one block."""
    return hashlib.sha256(BLOCK_DOMAIN + prev + struct.pack(">QQ", index, term) + command).digest()


def state_hash(accounts: Dict[bytes, Tuple[int, int]], last_applied: int, chain_head: bytes) -> str:
    """accounts: pubkey -> (balance, nonce). Every existing account is included, sorted by pubkey."""
    h = hashlib.sha256(STATE_DOMAIN + struct.pack(">Q", last_applied) + chain_head)
    for pk in sorted(accounts):
        bal, nonce = accounts[pk]
        h.update(pk + struct.pack(">QQ", bal, nonce))
    return h.hexdigest()
```

### 3.6 Ledger rules (normative; implemented exactly by `ReferenceLedger` in 3.9)

For each committed entry, in log order, a node does:

1. `decode_command` fails → `REJECTED_BAD_FORMAT`.
2. NOOP → `NOOP`.
3. `amount == 0` or `sender == recipient` → `REJECTED_INVALID_AMOUNT`.
4. Signature invalid → `REJECTED_BAD_SIGNATURE`.
5. Let `(bal, nonce)` be the sender's account, or `(0, 0)` if it does not exist. If `tx.nonce != nonce + 1` → `REJECTED_BAD_NONCE`.
6. If `bal < amount` → `REJECTED_INSUFFICIENT_FUNDS`, **and the sender's nonce becomes `nonce + 1`** (the signed transfer is consumed, so it can never be replayed later when funds arrive). The account is created if missing.
7. Otherwise: sender becomes `(bal - amount, nonce + 1)`; recipient (created at `(0, 0)` if missing) gains `amount` → `APPLIED`.

Then, for **every** entry regardless of outcome: `chain_head = block_hash(chain_head, index, term, command)` and `last_applied = index`.

**Receipts:** for `APPLIED` and `REJECTED_INSUFFICIENT_FUNDS` (the nonce-consuming outcomes), store `tx_id(command) → ApplyResult`. Other outcomes leave no receipt.

**Genesis:** a fixed map `pubkey → balance`; all nonces 0; `chain_head_0 = genesis_hash(...)`; `total_supply = sum(genesis balances)`, which must be `< 2**63`. There is no minting after genesis, so **conservation** means `sum(balances) == total_supply` at every applied index.

**Why exactly one of two Rs 500 payments succeeds:** both transfers carry Alice's signature and a nonce. With the same nonce, the second one in log order gets `BAD_NONCE`. With consecutive nonces, the second gets `INSUFFICIENT_FUNDS`. Every node applies the same log with the same rules, so every node rejects the same one.

### 3.7 `ledgernet/common/interfaces.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from ledgernet.common.rpc_types import (
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
)
from ledgernet.common.types import ApplyResult, LogEntry, NodeId, RaftStatus, Snapshot

# All methods below are called from the asyncio event-loop thread only.


class LogStore(ABC):
    """Durable Raft log + hard state. Implemented by A (FileLogStore); fake: InMemoryLogStore."""

    @abstractmethod
    def load_hard_state(self) -> Tuple[int, Optional[NodeId]]: ...          # (current_term, voted_for)
    @abstractmethod
    def save_hard_state(self, term: int, voted_for: Optional[NodeId]) -> None: ...  # durable before return
    @abstractmethod
    def load_commit_index(self) -> int: ...
    @abstractmethod
    def save_commit_index(self, index: int) -> None: ...
    # May be lazily durable (e.g. fsync at most every 100 ms), but must never persist
    # a value larger than one it was given. A stale value is safe (it is a lower bound).

    @abstractmethod
    def snapshot_index(self) -> int: ...   # last index covered by the snapshot; 0 if none
    @abstractmethod
    def snapshot_term(self) -> int: ...
    @abstractmethod
    def last_index(self) -> int: ...       # == snapshot_index() when the log is empty
    @abstractmethod
    def term_at(self, index: int) -> Optional[int]: ...
    # index == 0 -> 0; index == snapshot_index -> snapshot_term;
    # compacted or beyond last_index -> None
    @abstractmethod
    def entries(self, start: int, end: int) -> List[LogEntry]: ...
    # [start, end), clipped to last_index; IndexError if start <= snapshot_index
    @abstractmethod
    def append(self, entries: List[LogEntry]) -> None: ...
    # entries must be contiguous starting at last_index()+1; durable (fsync) before return
    @abstractmethod
    def truncate_from(self, index: int) -> None: ...
    # delete all entries >= index (index > snapshot_index); durable before return
    @abstractmethod
    def compact_to(self, index: int, term: int) -> None: ...
    # discard entries <= index and record (index, term) as the snapshot point.
    # If index >= last_index the log becomes empty. No-op if index <= snapshot_index.


class SnapshotStore(ABC):
    """Implemented by A (FileSnapshotStore); fake: InMemorySnapshotStore."""

    @abstractmethod
    def save(self, snap: Snapshot) -> None: ...      # atomic and durable before return
    @abstractmethod
    def load_latest(self) -> Optional[Snapshot]: ...


class StateMachine(ABC):
    @abstractmethod
    def last_applied(self) -> int: ...
    @abstractmethod
    def apply(self, entry: LogEntry) -> ApplyResult: ...
    # deterministic; requires entry.index == last_applied() + 1, else ValueError
    @abstractmethod
    def take_snapshot(self) -> Snapshot: ...         # covers last_applied()
    @abstractmethod
    def restore(self, snap: Snapshot) -> None: ...   # replaces all state
    @abstractmethod
    def state_hash(self) -> str: ...


class LedgerStateMachine(StateMachine):
    """Implemented by A (SqliteLedger); fake/spec: ReferenceLedger."""

    @abstractmethod
    def get_account(self, pubkey: bytes) -> Tuple[int, int]: ...   # (balance, nonce); (0,0) if absent
    @abstractmethod
    def get_receipt(self, txid: bytes) -> Optional[ApplyResult]: ...
    @abstractmethod
    def chain_head(self) -> str: ...
    @abstractmethod
    def total_balance(self) -> int: ...
    @abstractmethod
    def total_supply(self) -> int: ...


class RaftRpcHandler(ABC):
    """Server side of Raft RPCs. RaftNode (P) implements this; the transport (V) calls it."""

    @abstractmethod
    async def on_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse: ...
    @abstractmethod
    async def on_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse: ...
    @abstractmethod
    async def on_install_snapshot(self, req: InstallSnapshotRequest) -> InstallSnapshotResponse: ...


class Transport(ABC):
    """Client side of Raft RPCs. GrpcTransport (V); SimTransport (P, tests only).
    Every call raises TransportError on timeout/drop/partition/connection failure."""

    @abstractmethod
    async def start(self, handler: RaftRpcHandler) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def request_vote(self, target: NodeId, req: RequestVoteRequest) -> RequestVoteResponse: ...
    @abstractmethod
    async def append_entries(self, target: NodeId, req: AppendEntriesRequest) -> AppendEntriesResponse: ...
    @abstractmethod
    async def install_snapshot(self, target: NodeId, req: InstallSnapshotRequest) -> InstallSnapshotResponse: ...


class Consensus(ABC):
    """What the Ledger service (V) needs from Raft (P). Fake: SingleNodeConsensus."""

    node_id: NodeId

    @abstractmethod
    async def start(self) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def propose(self, command: bytes) -> Tuple[int, int]: ...
    # Returns (index, term) once appended to the leader's durable log.
    # Raises NotLeaderError(leader_hint) if this node is not the leader.
    @abstractmethod
    async def wait_for_apply(self, index: int, term: int, timeout: float) -> ApplyResult: ...
    # Resolves when the entry at `index` is applied locally.
    # Raises LeadershipLostError if the applied entry's term != term,
    # asyncio.TimeoutError after `timeout` seconds.
    @abstractmethod
    def status(self) -> RaftStatus: ...
```

### 3.8 Constructor signatures (fixed so `node.py` can be written early)

| Class | Module | Owner | Signature |
|---|---|---|---|
| `RaftNode(Consensus, RaftRpcHandler)` | `ledgernet/raft/node.py` | P | `RaftNode(node_id: NodeId, peers: list[NodeId], transport: Transport, log_store: LogStore, snapshot_store: SnapshotStore, state_machine: StateMachine, config: RaftConfig)` |
| `FileLogStore(LogStore)` | `ledgernet/storage/file_log.py` | A | `FileLogStore(dir_path: str)` |
| `FileSnapshotStore(SnapshotStore)` | `ledgernet/storage/snapshot_store.py` | A | `FileSnapshotStore(dir_path: str, keep: int = 2)` |
| `SqliteLedger(LedgerStateMachine)` | `ledgernet/ledger/sqlite_ledger.py` | A | `SqliteLedger(db_path: str, genesis: dict[bytes, int])` — always starts at genesis (deletes any old `db_path`); Raft calls `restore()` if a snapshot exists |
| `GrpcTransport(Transport)` | `ledgernet/net/grpc_transport.py` | V | `GrpcTransport(node_id: NodeId, peer_addresses: dict[NodeId, str], rpc_timeout_ms: int, injector: FaultInjector)` |

Step 0 creates each of these as a stub whose `__init__` raises `NotImplementedError`, so imports work from day one.

### 3.9 `ledgernet/testing/fakes.py` (shared fakes; `ReferenceLedger` is the executable spec)

```python
from __future__ import annotations

import hashlib
import json
import pickle
from typing import Dict, List, Optional, Tuple

from ledgernet.common.errors import NotLeaderError
from ledgernet.common.hashing import block_hash, genesis_hash, state_hash
from ledgernet.common.interfaces import (
    Consensus, LedgerStateMachine, LogStore, SnapshotStore, StateMachine,
)
from ledgernet.common.tx import DecodeError, decode_command, tx_id
from ledgernet.common.types import (
    ApplyResult, LogEntry, NodeId, RaftStatus, Snapshot, TxStatus,
)


class InMemoryLogStore(LogStore):
    """Survives a simulated crash if you keep the object and build a new RaftNode on it."""

    def __init__(self) -> None:
        self._term, self._voted_for = 0, None
        self._commit = 0
        self._snap_index, self._snap_term = 0, 0
        self._log: List[LogEntry] = []   # _log[i].index == _snap_index + 1 + i

    def load_hard_state(self): return self._term, self._voted_for
    def save_hard_state(self, term, voted_for): self._term, self._voted_for = term, voted_for
    def load_commit_index(self): return self._commit
    def save_commit_index(self, index): self._commit = index
    def snapshot_index(self): return self._snap_index
    def snapshot_term(self): return self._snap_term
    def last_index(self): return self._snap_index + len(self._log)

    def term_at(self, index):
        if index == 0:
            return 0
        if index == self._snap_index:
            return self._snap_term
        if index < self._snap_index or index > self.last_index():
            return None
        return self._log[index - self._snap_index - 1].term

    def entries(self, start, end):
        if start <= self._snap_index:
            raise IndexError(f"index {start} is compacted (snapshot at {self._snap_index})")
        end = min(end, self.last_index() + 1)
        return list(self._log[start - self._snap_index - 1: end - self._snap_index - 1])

    def append(self, entries):
        for e in entries:
            if e.index != self.last_index() + 1:
                raise ValueError(f"non-contiguous append {e.index}, last={self.last_index()}")
            self._log.append(e)

    def truncate_from(self, index):
        if index <= self._snap_index:
            raise ValueError("cannot truncate into the snapshot")
        del self._log[index - self._snap_index - 1:]

    def compact_to(self, index, term):
        if index <= self._snap_index:
            return
        if index >= self.last_index():
            self._log = []
        else:
            self._log = self._log[index - self._snap_index:]
        self._snap_index, self._snap_term = index, term


class InMemorySnapshotStore(SnapshotStore):
    def __init__(self) -> None:
        self._snap: Optional[Snapshot] = None
    def save(self, snap): self._snap = snap
    def load_latest(self): return self._snap


class RecordingStateMachine(StateMachine):
    """For Raft tests: records applied commands and keeps a running digest."""

    def __init__(self) -> None:
        self.applied: List[Tuple[int, int, bytes]] = []
        self._last_applied, self._last_term = 0, 0
        self._digest = b"\x00" * 32

    def last_applied(self): return self._last_applied

    def apply(self, entry):
        if entry.index != self._last_applied + 1:
            raise ValueError(f"out-of-order apply {entry.index}, expected {self._last_applied + 1}")
        self.applied.append((entry.index, entry.term, entry.command))
        self._digest = hashlib.sha256(self._digest + entry.command).digest()
        self._last_applied, self._last_term = entry.index, entry.term
        return ApplyResult(entry.index, entry.term, TxStatus.APPLIED, self._digest.hex())

    def take_snapshot(self):
        data = pickle.dumps((self._digest, self._last_applied, self._last_term))
        return Snapshot(self._last_applied, self._last_term, data)

    def restore(self, snap):
        self._digest, self._last_applied, self._last_term = pickle.loads(snap.data)
        self.applied = []

    def state_hash(self): return self._digest.hex()


class ReferenceLedger(LedgerStateMachine):
    """Executable specification of Section 3.6. SqliteLedger must match it exactly."""

    def __init__(self, genesis: Dict[bytes, int]) -> None:
        if sum(genesis.values()) >= 2**63:
            raise ValueError("total supply too large")
        self._genesis = dict(genesis)
        self.accounts: Dict[bytes, Tuple[int, int]] = {pk: (b, 0) for pk, b in genesis.items()}
        self.receipts: Dict[bytes, ApplyResult] = {}
        self._last_applied, self._last_term = 0, 0
        self._head = genesis_hash(self._genesis)

    def last_applied(self): return self._last_applied

    def apply(self, entry):
        if entry.index != self._last_applied + 1:
            raise ValueError(f"out-of-order apply {entry.index}, expected {self._last_applied + 1}")
        status = self._execute(entry.command)
        self._head = block_hash(self._head, entry.index, entry.term, entry.command)
        self._last_applied, self._last_term = entry.index, entry.term
        result = ApplyResult(entry.index, entry.term, status, self._head.hex())
        if status in (TxStatus.APPLIED, TxStatus.REJECTED_INSUFFICIENT_FUNDS):
            self.receipts[tx_id(entry.command)] = result
        return result

    def _execute(self, cmd: bytes) -> TxStatus:
        try:
            tx = decode_command(cmd)
        except DecodeError:
            return TxStatus.REJECTED_BAD_FORMAT
        if tx is None:
            return TxStatus.NOOP
        if tx.amount == 0 or tx.sender == tx.recipient:
            return TxStatus.REJECTED_INVALID_AMOUNT
        if not tx.verify():
            return TxStatus.REJECTED_BAD_SIGNATURE
        bal, nonce = self.accounts.get(tx.sender, (0, 0))
        if tx.nonce != nonce + 1:
            return TxStatus.REJECTED_BAD_NONCE
        if bal < tx.amount:
            self.accounts[tx.sender] = (bal, nonce + 1)
            return TxStatus.REJECTED_INSUFFICIENT_FUNDS
        self.accounts[tx.sender] = (bal - tx.amount, nonce + 1)
        rb, rn = self.accounts.get(tx.recipient, (0, 0))
        self.accounts[tx.recipient] = (rb + tx.amount, rn)
        return TxStatus.APPLIED

    def take_snapshot(self):
        data = json.dumps({
            "accounts": {pk.hex(): list(v) for pk, v in self.accounts.items()},
            "receipts": {k.hex(): [r.index, r.term, int(r.status), r.block_hash]
                         for k, r in self.receipts.items()},
            "last_applied": self._last_applied, "last_term": self._last_term,
            "head": self._head.hex(),
        }, sort_keys=True).encode()
        return Snapshot(self._last_applied, self._last_term, data)

    def restore(self, snap):
        d = json.loads(snap.data)
        self.accounts = {bytes.fromhex(k): (v[0], v[1]) for k, v in d["accounts"].items()}
        self.receipts = {bytes.fromhex(k): ApplyResult(v[0], v[1], TxStatus(v[2]), v[3])
                         for k, v in d["receipts"].items()}
        self._last_applied, self._last_term = d["last_applied"], d["last_term"]
        self._head = bytes.fromhex(d["head"])

    def state_hash(self): return state_hash(self.accounts, self._last_applied, self._head)
    def get_account(self, pubkey): return self.accounts.get(pubkey, (0, 0))
    def get_receipt(self, txid): return self.receipts.get(txid)
    def chain_head(self): return self._head.hex()
    def total_balance(self): return sum(b for b, _ in self.accounts.values())
    def total_supply(self): return sum(self._genesis.values())


class SingleNodeConsensus(Consensus):
    """For V: commits every proposal instantly. Set leader=False to exercise redirects."""

    def __init__(self, node_id: NodeId, sm: StateMachine, leader: bool = True,
                 leader_hint: Optional[NodeId] = None) -> None:
        self.node_id, self._sm = node_id, sm
        self.leader, self.leader_hint = leader, leader_hint
        self._term = 1
        self._results: Dict[int, ApplyResult] = {}

    async def start(self): pass
    async def stop(self): pass

    async def propose(self, command):
        if not self.leader:
            raise NotLeaderError(self.leader_hint)
        index = self._sm.last_applied() + 1
        self._results[index] = self._sm.apply(LogEntry(index, self._term, command))
        return index, self._term

    async def wait_for_apply(self, index, term, timeout):
        return self._results[index]

    def status(self):
        n = self._sm.last_applied()
        return RaftStatus(self.node_id, "leader" if self.leader else "follower", self._term,
                          self.node_id if self.leader else self.leader_hint, n, n, n, 0)
```

### 3.10 `ledgernet/common/events.py` (structured event log used for metrics)

```python
import json
import time
from typing import Optional

_path: Optional[str] = None
_node: Optional[str] = None


def configure(node_id: str, path: str) -> None:
    global _path, _node
    _path, _node = path, node_id


def emit(event: str, **fields) -> None:
    if _path is None:
        return
    rec = {"ts": time.time(), "node": _node, "event": event, **fields}
    with open(_path, "a") as f:
        f.write(json.dumps(rec) + "\n")
```

Required events:

| Event | Fields | Emitted by |
|---|---|---|
| `NODE_START` | — | V (`node.py`) |
| `STATE_RESTORED` | `snapshot_index`, `ms` | P (after loading the snapshot at startup) |
| `LOCAL_REPLAY_DONE` | `applied`, `ms` | P (after replaying the local log up to the persisted commit index) |
| `ROLE_CHANGE` | `role`, `term` | P |
| `LEADER_ELECTED` | `term` | P (on becoming leader) |
| `SNAPSHOT_TAKEN` | `index`, `ms`, `bytes` | P |
| `SNAPSHOT_INSTALLED` | `index`, `ms` | P |
| `SERVING` | `port` | V (gRPC server ready) |

### 3.11 `proto/ledgernet.proto`

```proto
syntax = "proto3";
package ledgernet;

// ---------------- Raft (node-to-node) ----------------
message LogEntry { uint64 index = 1; uint64 term = 2; bytes command = 3; }

message RequestVoteRequest {
  uint64 term = 1; string candidate_id = 2;
  uint64 last_log_index = 3; uint64 last_log_term = 4; bool pre_vote = 5;
}
message RequestVoteResponse { uint64 term = 1; bool vote_granted = 2; }

message AppendEntriesRequest {
  uint64 term = 1; string leader_id = 2;
  uint64 prev_log_index = 3; uint64 prev_log_term = 4;
  repeated LogEntry entries = 5; uint64 leader_commit = 6;
}
message AppendEntriesResponse {
  uint64 term = 1; bool success = 2; uint64 match_index = 3;
  uint64 conflict_index = 4; uint64 conflict_term = 5;
}

message InstallSnapshotRequest {
  uint64 term = 1; string leader_id = 2;
  uint64 last_included_index = 3; uint64 last_included_term = 4; bytes data = 5;
}
message InstallSnapshotResponse { uint64 term = 1; }

service Raft {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
}

// ---------------- Client-facing ----------------
message SubmitTransferRequest {
  bytes command = 1;          // 145-byte TRANSFER command (tx.py)
  bool skip_precheck = 2;     // honoured only if node runs with --unsafe-debug (forged-tx test)
}
message SubmitTransferResponse {
  enum Outcome {
    COMMITTED = 0;            // see tx_status for the ledger verdict
    NOT_LEADER = 1;           // retry at leader_address (may be empty)
    TIMEOUT = 2;              // outcome unknown: check GetReceipt / nonce
    RETRY = 3;                // entry was overwritten; safe to resubmit the same bytes
    PRECHECK_FAILED = 4;      // bad format or signature; never entered the log
  }
  Outcome outcome = 1;
  string leader_hint = 2;
  string leader_address = 3;  // "host:port"
  uint32 tx_status = 4;       // TxStatus value (types.py)
  uint64 index = 5;
  uint64 term = 6;
  string block_hash = 7;
}

message GetAccountRequest { bytes pubkey = 1; }
message GetAccountResponse { uint64 balance = 1; uint64 nonce = 2; uint64 as_of_index = 3; }

message GetReceiptRequest { bytes tx_id = 1; }
message GetReceiptResponse {
  bool found = 1; uint64 index = 2; uint64 term = 3; uint32 tx_status = 4; string block_hash = 5;
}

message GetStatusRequest {}
message GetStatusResponse {
  string node_id = 1; string role = 2; uint64 term = 3; string leader_id = 4;
  uint64 commit_index = 5; uint64 last_applied = 6; uint64 last_log_index = 7;
  uint64 snapshot_index = 8; string chain_head = 9; string state_hash = 10;
  uint64 total_balance = 11; uint64 total_supply = 12;
}

service Ledger {
  rpc SubmitTransfer(SubmitTransferRequest) returns (SubmitTransferResponse);
  rpc GetAccount(GetAccountRequest) returns (GetAccountResponse);
  rpc GetReceipt(GetReceiptRequest) returns (GetReceiptResponse);
  rpc GetStatus(GetStatusRequest) returns (GetStatusResponse);
}

// ---------------- Fault injection ----------------
message FaultRule {
  string target = 1;          // peer node id, or "*" for all peers
  double drop_prob = 2;       // probability an outbound Raft RPC (request or response) is lost
  uint32 delay_min_ms = 3;
  uint32 delay_max_ms = 4;
  bool partitioned = 5;       // all RPCs to target fail
  double duplicate_prob = 6;  // request sent twice
}
message SetFaultsRequest { repeated FaultRule rules = 1; }
message Empty {}

service Admin {
  rpc SetFaults(SetFaultsRequest) returns (Empty);
  rpc ClearFaults(Empty) returns (Empty);
}
```

### 3.12 Files on disk

**`config/genesis.json`**
```json
{ "accounts": { "<64-hex pubkey>": 5000000, "<64-hex pubkey>": 5000000 } }
```

**`cluster/cluster.local.yaml`** (V owns the loader; P and A never read it)
```yaml
genesis: config/genesis.json
raft:
  election_timeout_min_ms: 300
  election_timeout_max_ms: 600
  heartbeat_interval_ms: 75
  rpc_timeout_ms: 200
  max_entries_per_append: 256
  snapshot_interval: 1000
  pre_vote: true
  check_quorum: true
submit_timeout_s: 5
nodes:
  - { id: n1, host: 127.0.0.1, port: 50051, data_dir: ./data/n1 }
  - { id: n2, host: 127.0.0.1, port: 50052, data_dir: ./data/n2 }
  - { id: n3, host: 127.0.0.1, port: 50053, data_dir: ./data/n3 }
  - { id: n4, host: 127.0.0.1, port: 50054, data_dir: ./data/n4 }
  - { id: n5, host: 127.0.0.1, port: 50055, data_dir: ./data/n5 }
```
One gRPC port per node serves the `Raft`, `Ledger` and `Admin` services.

**Node data directory:**
```
<data_dir>/
  wal/          FileLogStore (A)
  snapshots/    FileSnapshotStore (A)
  state.db      SqliteLedger (A) — rebuilt from snapshot + log at every start
  events.jsonl  event log (3.10)
  node.log      human-readable logging
```

**Key files:** `keys/<name>.key` holds the 32-byte Ed25519 private seed as 64 hex characters; `keys/<name>.pub` holds the public key as 64 hex characters.

---

## 4. Step 0: repository bootstrap (≈30 minutes, whoever is first)

Every file below is copy-paste from Section 3, so all three can do this locally at the same time; whoever pushes first wins and the others just pull.

```
ledgernet/                      (repo root)
├── pyproject.toml
├── Makefile
├── .github/workflows/ci.yml    (pytest on push)
├── proto/ledgernet.proto       [3.11]
├── config/genesis.json         (any test keys; V generates real ones later)
├── ledgernet/
│   ├── __init__.py
│   ├── common/  __init__.py types.py rpc_types.py errors.py tx.py hashing.py interfaces.py events.py
│   ├── testing/ __init__.py fakes.py
│   ├── gen/     __init__.py    (generated code goes here, gitignored)
│   ├── raft/    __init__.py node.py (stub)                       ← P
│   ├── ledger/  __init__.py sqlite_ledger.py (stub)              ← A
│   ├── storage/ __init__.py file_log.py snapshot_store.py (stubs) ← A
│   ├── net/     __init__.py grpc_transport.py (stub)             ← V
│   ├── client/  __init__.py                                      ← V
│   └── node.py  (stub)                                           ← V
├── tools/ bench/ analysis/                                       ← A
├── cluster/ experiments/                                         ← V
└── tests/ contract/ raft/ ledger/ storage/ net/ e2e/
```

**`pyproject.toml` dependencies:** `grpcio`, `grpcio-tools`, `protobuf`, `cryptography`, `pyyaml`; dev: `pytest`, `pytest-asyncio`, `hypothesis`, `pandas`, `matplotlib`. Python ≥ 3.10. Pin exact versions (determinism, Section 12).

**`Makefile`**
```make
proto:
	python -m grpc_tools.protoc -Iledgernet/gen=proto --python_out=. --grpc_python_out=. --pyi_out=. proto/ledgernet.proto
test:
	pytest -q
```
The `-Iledgernet/gen=proto` mapping makes the generated files land in `ledgernet/gen/` with package-correct imports (`from ledgernet.gen import ledgernet_pb2`).

**`tests/contract/test_contract.py`** (write together in Step 0, a few lines each): sign → encode → decode → verify round trip; a tampered byte fails verification; `ReferenceLedger` double spend (same-nonce and consecutive-nonce cases); `InMemoryLogStore` append/truncate/compact; `SingleNodeConsensus` propose → wait. These guard the contract against accidental edits.

---

## 5. P — Consensus (Raft)

**Goal:** `ledgernet/raft/node.py: RaftNode`, implementing `Consensus` and `RaftRpcHandler`, correct under crashes, partitions, loss, delay, duplication and reordering. Tested entirely in-process against the fakes plus P's own simulated network. **P needs nothing from A or V until integration.**

### Day 1 (start immediately)
- Write `ledgernet/raft/sim.py`: `SimNetwork` and `SimTransport(Transport)` (P5.1).
- Write the `RaftNode` skeleton: state fields, role enum, ticker loop, `status()`.
- Get leader election passing for 3 and 5 nodes on `SimNetwork` with `InMemoryLogStore` + `RecordingStateMachine`.

### Tasks

**P5.1 Simulated network (test infrastructure, `ledgernet/raft/sim.py`)**
- `SimNetwork` holds a registry `node_id → RaftRpcHandler`. `SimTransport(node_id, net)` delivers calls by `await`ing the target handler directly.
- Per-link controls: `partition(groups: list[set[NodeId]])`, `heal()`, `set_drop(p)`, `set_delay(min_ms, max_ms)`, `set_duplicate(p)`, `isolate(node)`. Use a seeded `random.Random` so failures are reproducible.
- Delivery must allow reordering (random delay per message) and duplication (deliver twice, return one response).
- Crash simulation: `crash(node)` stops its `RaftNode` and unregisters it; `restart(node)` builds a new `RaftNode` on the **same** `InMemoryLogStore` / `InMemorySnapshotStore` objects and a fresh `RecordingStateMachine`, exactly like a real process that loses memory but keeps its disk.
- Helper `SimCluster(n, seed)` for tests: `leader()`, `wait_for_leader()`, `propose_and_wait(cmd)`, `check_invariants()`.

**P5.2 Core state and timers**
- Persistent (via `LogStore`): `current_term`, `voted_for`, log. Volatile: `role`, `leader_id`, `commit_index`, `last_applied` (read from the state machine), and the leader's `next_index[]` and `match_index[]`.
- A single ticker coroutine (≈10 ms tick) handles the election timeout (randomised in `[min, max]`; reset on a valid AppendEntries/InstallSnapshot from the current leader and on granting a vote) and heartbeats.
- Make the clock injectable (`self._now = time.monotonic`) so tests can speed up time if needed.
- **Any** RPC request or response with `term > current_term`: set the term, clear the vote, `save_hard_state`, become follower. Ignore responses with a stale term.

**P5.3 Leader election**
- `RequestVote` handler: grant iff `req.term >= current_term`, not already voted for someone else in this term, and the candidate's log is at least as up to date (compare `(last_log_term, last_log_index)`). **Call `save_hard_state` before replying.**
- Candidate: increment term, vote for self (persist), send RequestVote to all peers concurrently, become leader on a majority (3 of 5).
- On becoming leader: emit `LEADER_ELECTED`, set `next_index = last_index + 1` and `match_index = 0` for each peer, and **append a NOOP entry** (`NOOP_COMMAND`) so entries from earlier terms can be committed.

**P5.4 Log replication and commit**
- Leader runs one replication task per follower: AppendEntries with up to `max_entries_per_append` entries from `next_index`; an empty AppendEntries is the heartbeat. Trigger immediately on new proposals rather than waiting for the next heartbeat.
- Follower `AppendEntries` handler:
  - Reject if `req.term < current_term`.
  - Consistency check on `prev_log_index`/`prev_log_term`. On failure, fill `conflict_index`/`conflict_term` (missing entry: `conflict_index = last_index + 1`, `conflict_term = 0`; otherwise the first index of the conflicting term).
  - For each incoming entry: if an existing entry at that index has a **different term**, `truncate_from(index)` and append the rest; if the terms match, skip it. **Never truncate when terms match**: a delayed or duplicated AppendEntries must not delete newer entries.
  - Skip entries at or below `snapshot_index` (already in the snapshot).
  - `append` must be durable before replying success with `match_index = prev_log_index + len(entries)`.
  - `commit_index = max(commit_index, min(leader_commit, match_index))`.
- Leader on response: on success, `match_index[f] = max(match_index[f], resp.match_index)` and `next_index[f] = match_index[f] + 1`. On failure, back up fast: if the leader has entries of `conflict_term`, jump to one past its last entry of that term, else to `conflict_index`.
- Commit rule: the highest `N` such that a majority has `match_index >= N` (the leader counts its own `last_index`) **and** `term_at(N) == current_term`.
- `propose(command)`: if not leader, raise `NotLeaderError(leader_id)`; append `LogEntry(last_index + 1, current_term, command)` durably; kick replication; return `(index, term)`.

**P5.5 Apply loop and client futures**
- Applier task: while `last_applied < commit_index`, fetch entries in batches and call `state_machine.apply(entry)`, yielding to the event loop every ~100 entries.
- `wait_for_apply(index, term, timeout)`: register a future keyed by `index`. When that index is applied, resolve with the `ApplyResult` if the applied entry's term equals `term`, otherwise raise `LeadershipLostError`. If the index was already applied when called, answer immediately (keep an LRU of recent results, e.g. the last 10 000). Use `asyncio.wait_for` for the timeout.
- Do not fail pending futures when stepping down: the entry may still commit under the next leader; the timeout covers the rest.
- Call `log_store.save_commit_index(commit_index)` whenever it advances (A's store throttles the fsync).

**P5.6 Startup and recovery (the proposal's "snapshot + replay")**
1. `(term, voted_for) = load_hard_state()`.
2. `snap = snapshot_store.load_latest()`. If present: `state_machine.restore(snap)`, emit `STATE_RESTORED`. If `snap.last_included_index > log_store.snapshot_index()` (a crash between saving the snapshot and compacting), call `log_store.compact_to(...)` to reconcile.
3. Replay locally: apply entries from `last_applied + 1` up to `min(load_commit_index(), last_index)`; emit `LOCAL_REPLAY_DONE`; set `commit_index` to that value.
4. Start as follower; the leader's AppendEntries supply the rest ("catch up from the leader").

**P5.7 Snapshots and log compaction**
- After applying, if `snapshot_interval > 0` and `last_applied - snapshot_index >= snapshot_interval`: `snap = state_machine.take_snapshot()`, `snapshot_store.save(snap)` (durable), **then** `log_store.compact_to(snap.last_included_index, snap.last_included_term)`. Emit `SNAPSHOT_TAKEN`. Never compact before the snapshot is durable.
- Leader: if `next_index[f] <= snapshot_index`, send `InstallSnapshot` with `snapshot_store.load_latest()`; on success set `match_index[f] = last_included_index`.
- Follower `InstallSnapshot` handler: reject a stale term; if `last_included_index <= last_applied`, ignore and reply. Otherwise save the snapshot durably; if `term_at(idx) == term`, call `compact_to(idx, term)` (keeping the suffix); otherwise `truncate_from(snapshot_index + 1)` and then `compact_to(idx, term)`. Then `state_machine.restore(snap)` and `commit_index = max(commit_index, idx)`. Emit `SNAPSHOT_INSTALLED`.

**P5.8 Partition behaviour (needed for the demo)**
- **CheckQuorum:** a leader that has not had successful responses from a majority within one election timeout steps down to follower. This makes the minority side "refuse writes" quickly (it answers `NotLeaderError`) instead of accepting proposals that only time out.
- **PreVote:** before incrementing its term, a would-be candidate sends `RequestVote(pre_vote=True, term=current_term + 1)`. Peers grant it only if they have not heard from a leader within the minimum election timeout and the log check passes, and they do **not** change their own term. A real election starts only after a pre-vote majority. This stops an isolated minority node from forcing a disruptive election when the partition heals.

**P5.9 Tests (`tests/raft/`)** — all on `SimCluster`, each run over many seeds (e.g. 200):
- Election: exactly one leader emerges; re-election after the leader crashes; no leader in a minority of 2.
- Replication: 1 000 proposals commit and apply identically on all 5 nodes.
- The Figure 8 scenario from the Raft paper (a leader must not commit an old-term entry by counting replicas).
- A follower with a long divergent uncommitted suffix has it overwritten.
- Partition 3|2 with the leader in the minority: the minority leader steps down (CheckQuorum), the majority commits, and after healing all converge.
- Crash/restart of up to 2 nodes repeatedly while proposing; restart of all 5.
- Snapshot: a follower down for more than `snapshot_interval` entries recovers via InstallSnapshot.
- Fuzz with drop 20%, delay 0–50 ms, duplicate 10%, random partitions and crashes.
- **Invariant checker run after every step:** at most one leader per term; any two nodes agree on every index ≤ the smaller commit index (Log Matching); applied prefixes produce identical digests (State Machine Safety); `commit_index` never decreases on a live node; every entry acknowledged through `wait_for_apply` appears in every later leader's log (Leader Completeness / durability).

**P5.10 Stretch:** ReadIndex linearizable reads (`async def read_index() -> int`, added through a contract PR), leadership transfer, pipelined AppendEntries.

### P acceptance criteria (end of Week 2)
- All P5.9 tests pass on 200 seeds in under 5 minutes.
- `RaftNode` uses only the Section 3 interfaces (no imports from `ledgernet.storage`, `ledgernet.ledger` or `ledgernet.net`).
- A short design note in `ledgernet/raft/README.md` covering persistence ordering and how each invariant is ensured.

---

## 6. A — Ledger, cryptography, storage, audit, load generator, analysis

**Goal:** the real durable components behind `LogStore`, `SnapshotStore` and `LedgerStateMachine`, plus tooling that proves correctness and produces the evaluation plots. **A needs nothing from P or V for A6.1–A6.6 and A6.8; A6.7 uses V's client library (end of Week 1) and has a stand-in until then.**

### Day 1 (start immediately)
- Own `ledgernet/common/tx.py` and `hashing.py`: write `tests/ledger/test_tx.py` (fixed vectors, round trip, tampering with every field, wrong lengths, out-of-range amounts, malformed commands).
- Create the SQLite schema and start `SqliteLedger.apply` with a differential test against `ReferenceLedger`.

### Tasks

**A6.1 Transfer and hash tests**
- Fixed test vectors: a deterministic key from seed `bytes(32)` and a known transfer; commit the expected command hex and signature hex to the repo so any accidental encoding change breaks CI.
- Property tests (`hypothesis`): any single-bit flip in a signed command makes `verify()` false or decoding fail.

**A6.2 `SqliteLedger` (`ledgernet/ledger/sqlite_ledger.py`)**
- Schema:
  ```sql
  CREATE TABLE accounts (pubkey BLOB PRIMARY KEY, balance INTEGER NOT NULL, nonce INTEGER NOT NULL);
  CREATE TABLE receipts (tx_id BLOB PRIMARY KEY, idx INTEGER, term INTEGER, status INTEGER, block_hash TEXT);
  CREATE TABLE meta (k TEXT PRIMARY KEY, v BLOB);   -- last_applied, last_term, chain_head, total_supply
  ```
- The constructor deletes any existing `db_path`, creates the schema and inserts genesis. Durability comes from the Raft log and snapshots, so use `PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF` for speed.
- `apply`: one SQLite transaction per entry (an `apply_batch` helper can come later), implementing Section 3.6 exactly; verify the signature before opening the transaction.
- `state_hash()`: stream `SELECT pubkey, balance, nonce FROM accounts ORDER BY pubkey` into the same construction as `hashing.state_hash` (refactor it to accept an iterator if needed, keeping output identical).
- `total_balance()`: `SELECT SUM(balance)`.
- **Differential test (the key test):** `hypothesis` generates command streams containing valid transfers, same-nonce conflicts, consecutive overdrafts, replays of earlier commands, forged signatures, random bytes, NOOPs and unknown senders. Apply each stream to both `ReferenceLedger` and `SqliteLedger`; after every entry assert equal `ApplyResult`, `state_hash()` and `chain_head()`, and `total_balance() == total_supply()`.

**A6.3 Ledger snapshots**
- `take_snapshot()`: on Python 3.11+, back up into an in-memory DB with `Connection.backup()` and `serialize()`; on 3.10, back up into a temp file and read its bytes. Return `Snapshot(last_applied, last_term, bytes)`.
- `restore(snap)`: write the bytes to `db_path + ".restore"`, close the live connection, atomically replace, reopen, and check `meta.last_applied == snap.last_included_index`.
- Test: snapshot at random points, restore into a fresh instance, continue applying the same stream; hashes must match an uninterrupted instance.

**A6.4 `FileSnapshotStore` (`ledgernet/storage/snapshot_store.py`)**
- File `snap-<index:012d>.bin` = header (`magic b"LNSNAP1"`, index u64, term u64, data length u64, SHA-256 of data) + data.
- `save`: write `*.tmp`, `fsync`, `os.replace`, `fsync` the directory; delete all but the newest `keep` snapshots.
- `load_latest`: the newest file whose header and SHA-256 verify; skip corrupt ones.
- Tests: a corrupted newest file falls back to the previous one; a leftover `.tmp` from a crash mid-write is ignored.

**A6.5 `FileLogStore`, the write-ahead log (`ledgernet/storage/file_log.py`)**
- Files in `dir_path`: `log.wal` (records), `hardstate.json` (term, voted_for), `commit` (u64), `logmeta.json` (snapshot_index, snapshot_term).
- Record format: `u32 length ‖ u32 crc32(payload) ‖ payload`, with payload = `u8 type ‖ ...`:
  - `ENTRY`: index u64, term u64, command bytes.
  - `TRUNCATE`: from_index u64 (logical truncation, replayed on load).
- On open, rebuild the in-memory index (a list of `LogEntry`, as in `InMemoryLogStore`) by scanning `log.wal`. A short or CRC-failing record at the tail is a torn write: truncate the file at that offset and continue.
- `append(entries)`: write all records, then **one `fsync` per call** (group commit), then update memory.
- `truncate_from`: append a `TRUNCATE` record and `fsync`, then update memory.
- `compact_to(index, term)`: write the remaining entries to `log.wal.tmp`, `fsync`, write `logmeta.json` atomically, `os.replace` the log, `fsync` the directory. On open, if `logmeta` says the snapshot is past entries still in the file, drop the covered entries (handles a crash between the two renames).
- `save_hard_state`: atomic tmp + `fsync` + rename + directory `fsync`, on every call.
- `save_commit_index`: write in place, `fsync` at most every 100 ms; on read, a corrupt or short file means 0.
- It must pass **the same test suite as `InMemoryLogStore`**: write `tests/storage/test_logstore_contract.py` parametrised over both implementations (P can also run it against the fake).
- Crash tests: write N entries, cut `log.wal` at every byte offset inside the last record, reopen, and check that exactly the fully written prefix survives. Measure `fsync` latency and append throughput in `bench/bench_wal.py`.

**A6.6 Offline auditor (`tools/audit.py`)**
- `python tools/audit.py --genesis config/genesis.json data/n1 data/n2 ...`
- For each data directory: load the latest snapshot (restored into `SqliteLedger`) or genesis, open `FileLogStore` read-only, and replay up to the persisted commit index, checking chain continuity, per-account nonce monotonicity, conservation at every step and no negative balances.
- Across directories: committed prefixes agree on every common index (compare block hashes), and state hashes match at equal `last_applied`.
- Prints a PASS/FAIL report and exits non-zero on failure. V's end-to-end tests call `tools.audit.audit_dirs(...)` after every scenario.
- Develop it on logs produced by driving `SqliteLedger` + `FileLogStore` directly from a script; no Raft needed.

**A6.7 Load generator (`bench/loadgen.py`)** — starts in Week 2
- Uses V's `ledgernet.client.LedgerClient` (due end of Week 1). **Until then**, write `bench/stub_server.py`: a ~40-line gRPC `Ledger` server on `SingleNodeConsensus` + `ReferenceLedger`, using the generated stubs from `make proto`.
- Setup: K funded genesis accounts (e.g. 100); each worker owns a disjoint set of senders and tracks their nonces locally, so concurrent workers never collide unless a conflict test asks for it.
- Modes: closed loop (C concurrent clients) and open loop (target rate R tx/s with Poisson arrivals). Duration, warm-up and seed as flags.
- Output `requests.csv` (schema in Section 11), and `acked.json` listing the tx_ids acknowledged as `APPLIED` (for the durability check).

**A6.8 Analysis and plots (`analysis/`)** — can start Day 1 on synthetic data
- `analysis/make_synthetic.py` produces fake `requests.csv`, `timeline.csv` and `recovery.csv` in the Section 11 schemas.
- `analysis/latency.py`: CDF and p50/p95/p99 vs offered load; laptop vs RCE overlay.
- `analysis/failover.py`: throughput over time (100 ms bins) with vertical lines at `LEADER_KILLED` and `LEADER_ELECTED`; a table of election time and time to first commit across ≥ 10 runs (mean, median, p95).
- `analysis/recovery.py`: recovery time vs log length (10k / 50k / 100k entries), full replay vs snapshot + replay, split into restore / local replay / catch-up phases.
- All figures saved as PDF and PNG to `results/figures/`.

### A acceptance criteria (end of Week 2)
- The differential test passes on ≥ 10 000 hypothesis examples.
- Both `LogStore` implementations pass the shared contract suite; torn-write tests pass.
- The auditor detects a deliberately corrupted block and a deliberately broken conservation.
- The plot scripts run end to end on synthetic data.

---

## 7. V — Networking, node runtime, wallet, cluster tooling, end-to-end tests, experiments

**Goal:** everything that turns the components into a running 5-node system you can use, break and measure. **V needs nothing from P or A until integration**: the whole client path is built and tested on `SingleNodeConsensus` + `ReferenceLedger`.

### Day 1 (start immediately)
- `make proto`; write `ledgernet/net/convert.py` (dataclass ↔ protobuf) with round-trip tests for every message in 3.2.
- Write the `Ledger` gRPC service on `SingleNodeConsensus` + `ReferenceLedger` and submit a signed transfer from a Python test.

### Tasks

**V7.1 Conversion layer (`ledgernet/net/convert.py`)**
- `to_proto(x)` / `from_proto(msg)` for every Raft message and `LogEntry`. Empty `leader_id` strings map to `None`.
- Round-trip property tests with `hypothesis`.

**V7.2 Fault injector (`ledgernet/net/faults.py`)**
- `FaultInjector` holds rules (by `target` or `"*"`) and a seeded RNG, and exposes `async def around(target, call)`:
  - `partitioned`: sleep 1–5 ms, then raise `TransportError`.
  - `drop_prob`: with probability `p/2` drop before sending and with `p/2` drop after receiving (lost response), raising `TransportError`.
  - Delay uniformly in `[delay_min_ms, delay_max_ms]` before sending.
  - `duplicate_prob`: send twice concurrently, return one response.
- Unit tests with a fake call function.

**V7.3 `GrpcTransport` (`ledgernet/net/grpc_transport.py`)**
- One `grpc.aio` channel per peer, created lazily and reused, with `max_send/receive_message_length = 64 MB` (for snapshots).
- Each outbound call goes through `injector.around(target, ...)` with a gRPC deadline of `rpc_timeout_ms`; map any `grpc.aio.AioRpcError` or `asyncio.TimeoutError` to `TransportError`.
- `start(handler)`: registers the `Raft` servicer, which converts proto → dataclass, awaits `handler.on_...`, and converts back.
- Tests: two `GrpcTransport`s on localhost with a trivial echo `RaftRpcHandler`; an injected partition produces `TransportError`; injected delay is observed.

**V7.4 Ledger and Admin services (`ledgernet/net/services.py`)**
- `LedgerServicer(consensus: Consensus, ledger: LedgerStateMachine, addresses: dict[NodeId, str], submit_timeout_s, unsafe_debug: bool)`.
- `SubmitTransfer`:
  1. Unless `skip_precheck and unsafe_debug`: `decode_command` + `verify()`; failure → `PRECHECK_FAILED` with `tx_status`.
  2. `propose`; on `NotLeaderError` return `NOT_LEADER` with `leader_hint` and `leader_address`.
  3. `wait_for_apply` → `COMMITTED` with `tx_status`, `index`, `term`, `block_hash`; `LeadershipLostError` → `RETRY`; timeout → `TIMEOUT`.
- `GetAccount`, `GetReceipt` and `GetStatus` read the local state machine and `consensus.status()`; `GetStatus` includes `state_hash`, `chain_head`, `total_balance` and `total_supply`.
- `AdminServicer` sets and clears injector rules.
- Tests on `SingleNodeConsensus` + `ReferenceLedger`: a committed transfer, precheck rejection, `NOT_LEADER` redirect (`leader=False`), double spend with same and consecutive nonces, replay → `BAD_NONCE`, receipt lookup.

**V7.5 Node process (`ledgernet/node.py`)**
- `python -m ledgernet.node --config cluster/cluster.local.yaml --id n3 [--storage memory|file] [--ledger reference|sqlite] [--consensus single|raft] [--unsafe-debug]`
- The flags let integration swap one component at a time (Section 8). Defaults after integration: `file`, `sqlite`, `raft`.
- Startup: configure `events` and emit `NODE_START`; build the stores, ledger, injector, transport and `RaftNode`; start one gRPC server with all three services; emit `SERVING`; handle SIGTERM gracefully (crash tests use SIGKILL).
- Config loader (`ledgernet/node_config.py`) validates the YAML and builds `RaftConfig`.

**V7.6 Client library and wallet**
- `ledgernet/client/client.py: LedgerClient(addresses: list[str])` — **deliver by end of Week 1 (A's load generator uses it).**
  - `async submit(cmd: bytes, max_attempts=10) -> SubmitResult`: try the cached leader, follow `leader_address` hints, otherwise round-robin; exponential backoff on `TIMEOUT` and connection errors; resubmit identical bytes on `RETRY` (safe because of the nonce).
  - Resolving an ambiguous `TIMEOUT`: `GetReceipt(tx_id)`; if found, that is the final answer. If not found and the account nonce is ≥ the transfer's nonce, the nonce was consumed by a different transfer, so this one definitively did not apply. Otherwise resubmit.
  - `get_account`, `get_receipt`, `status(addr)`, `status_all()`.
- `ledgernet/client/wallet.py` CLI:
  - `keygen <name>` → `keys/<name>.key` and `.pub`
  - `genesis --out config/genesis.json alice=50000 bob=50000 ...` (rupees in, paise stored)
  - `balance <name>`; `send <from> <to> <rupees> [--nonce N] [--node n2]` (fetches the nonce if not given); `status` (table of all nodes: role, term, commit, applied, state-hash prefix).

**V7.7 Cluster launcher (`cluster/launcher.py`)**
- Python API used by tests and experiments: `Cluster(config_path, mode="local"|"ssh")` with `start_all()`, `start(id)`, `kill(id)` (SIGKILL), `stop(id)` (SIGTERM), `wipe(id)`, `leader()`, `wait_for_leader(timeout)`, `wait_converged(timeout)` (all live nodes report equal `last_applied`, `state_hash` and `chain_head`), `partition(groups)`, `heal()`, `set_faults(id, rules)`, `collect_events()`.
- Local mode: one `subprocess.Popen` per node, logging to `data/<id>/node.log`.
- SSH mode (RCE): `ssh host 'cd repo && .venv/bin/python -m ledgernet.node ...'`, with PID files for kill and restart, and an example `cluster/cluster.rce.yaml`.
  - **Put `data_dir` on local disk** (e.g. `/tmp/ledgernet-$USER/n1`), not an NFS home directory: fsync on NFS is slow and distorts latency.
  - Choose ports from a configurable high range to avoid clashes with other RCE users.
  - Check clock sync (`chronyc`/`ntpq`), since timestamps from different machines are compared.
- `cluster/setup_rce.sh`: clone or pull, create the venv, install, `make proto` on each host.

**V7.8 End-to-end scenarios (`tests/e2e/`)** — listed in Section 10; each ends with `cluster.wait_converged()` and `tools.audit.audit_dirs(...)`. Before integration, debug the harness logic with `--consensus single` on one node.

**V7.9 Experiment orchestration (`experiments/`)**
- `latency.py`: for each load level, start the cluster, run A's load generator, save `requests.csv`.
- `failover.py`: steady load; at t = 10 s kill the leader and log `LEADER_KILLED` to `timeline.csv`; poll `GetStatus` every 10 ms to record `LEADER_ELECTED` (the first node reporting `role=leader` in a higher term); repeat ≥ 10 times.
- `recovery.py`: build a log of L entries; kill a follower; keep committing for a while; restart it; record the phases from its `events.jsonl` plus the time until its `last_applied` reaches the leader's `commit_index` sampled at restart; run with both `snapshot_interval=0` and `1000`; write `recovery.csv`.
- All results go under `results/<experiment>/<timestamp>/`, together with the config used.

### V acceptance criteria (end of Week 2)
- One node with `--consensus single --ledger reference --storage memory` serves the wallet end to end.
- Transport and fault-injection tests pass; the launcher starts and kills 5 local processes.
- `LedgerClient` merged and documented by end of Week 1.

---

## 8. Integration plan and timeline

This assumes about 5 weeks; stretch or compress evenly to fit the real deadline.

| Week | P | A | V |
|---|---|---|---|
| **Day 1** | Step 0 together (30 min) | Step 0 together | Step 0 together |
| **1** | SimNetwork, election, replication, commit | tx/hash tests, SqliteLedger + differential test, start FileLogStore, synthetic-data plots | proto/convert, fault injector, transport, Ledger service on fakes, **LedgerClient** |
| **2** | Apply loop, futures, recovery, snapshots, InstallSnapshot, PreVote/CheckQuorum, fuzz tests | FileLogStore crash tests, ledger snapshots, FileSnapshotStore, auditor, load generator | node.py with swap flags, wallet CLI, local launcher, e2e harness on the single-node fake |
| **3 — integration** | Pair with V on swaps 1–2; fix Raft bugs | Pair with V on swaps 2–3; fix storage/ledger bugs | Drive swaps 1–4 |
| **4** | Partition/failover hardening; ReadIndex (stretch) | Auditor in all e2e runs; load generator on the real cluster; first real plots | All e2e scenarios; RCE deployment; experiment scripts |
| **5** | Report: consensus and correctness | Report: evaluation and figures | Report: system and testing; demo script and rehearsal |

**Integration swaps (Week 3), one at a time; each must leave all e2e tests green:**

1. `--consensus raft --storage memory --ledger reference`: real Raft over real gRPC on 5 local processes. Isolates Raft and transport bugs. (P + V)
2. `--storage file`: real WAL and snapshot store; crash/restart scenarios become meaningful. (P + A + V)
3. `--ledger sqlite`: real state machine; the auditor must pass. (A + V)
4. The same on RCE machines with `cluster.rce.yaml`. (V, with help)

If something breaks after a swap, the swapped component (or its interaction with the previous one) is the suspect.

---

## 9. Dependency matrix

| Consumer | Needs | From | Available | Stand-in until then |
|---|---|---|---|---|
| P | LogStore, SnapshotStore | A | Week 2 | `InMemoryLogStore`, `InMemorySnapshotStore` (same semantics, shared contract tests) |
| P | StateMachine | A | Week 2 | `RecordingStateMachine` |
| P | Transport | V | Week 1 | P's own `SimTransport` (better for tests anyway) |
| A | Command format, hash rules | Contract | Day 1 | — |
| A | Client library for the load generator | V | End of Week 1 | `bench/stub_server.py` + raw generated stubs |
| A | Real CSVs for plots | V's experiments + A's load generator | Week 4 | `make_synthetic.py` |
| V | Consensus | P | Week 3 | `SingleNodeConsensus` |
| V | LedgerStateMachine | A | Week 2 | `ReferenceLedger` |
| V | Stores | A | Week 2 | in-memory fakes via `--storage memory` |
| V | Auditor | A | Week 2 | `GetStatus`-based convergence check |

The only real ordering constraint is `LedgerClient` (V → A, end of Week 1), and even that has a stand-in.

---

## 10. Test and demo scenarios (mapped to the proposal)

Every scenario ends the same way: wait for convergence; all five `GetStatus` responses show equal `chain_head` and `state_hash`; `total_balance == total_supply` on every node; `tools/audit.py` reports PASS.

| # | Scenario | Steps | Expected | Invariant shown |
|---|---|---|---|---|
| S1a | Conflicting transfers, same nonce | Alice has Rs 500. Sign two Rs 500 transfers (to Bob and to Carol) with the same nonce; send to n1 and n4 simultaneously (`asyncio.gather`) | Exactly one `APPLIED`, the other `REJECTED_BAD_NONCE`, identical on all nodes | No double spend, agreement |
| S1b | Conflicting transfers, consecutive nonces | As above with nonces k and k+1 | One `APPLIED`, one `REJECTED_INSUFFICIENT_FUNDS` | No double spend |
| S2 | Leader failure under load | Steady load; SIGKILL the leader at t = 10 s; restart it at t = 30 s | New leader elected; commits resume; the restarted node catches up; every acked tx has a receipt on every node | Durability, agreement |
| S3 | Network partition | Partition {leader, x} \| {other 3} via Admin; keep sending to all nodes for 20 s; heal | The minority answers `NOT_LEADER` (after CheckQuorum) and commits nothing; the majority keeps committing; after healing all converge | Safety under partition |
| S4 | Replay | Resubmit an already applied command to the same node and to a different node | `REJECTED_BAD_NONCE` on both; balances unchanged | Replay protection |
| S5 | Forged signature | Flip one signature byte; submit normally, then with `skip_precheck` on an `--unsafe-debug` cluster | `PRECHECK_FAILED`; through the log, `REJECTED_BAD_SIGNATURE` identically on all nodes | Deterministic signature checks |
| S6 | Message loss, delay, duplication | Admin on all nodes: drop 20%, delay 0–100 ms, duplicate 5%; run load for 60 s; clear | Progress continues (slower); convergence | Agreement on an unreliable network |
| S7 | Minority crash and durability | Kill 2 followers, then the leader (2 of 5 alive); restart 2 nodes | No commits while fewer than 3 are alive; after restart every previously acked tx is still present | Durability, f = 2 |
| S8 | Full-cluster restart | Load; SIGKILL all 5; restart all | Identical committed state recovered from disk | Durability |
| S9 | Lagging node via snapshot | Kill a follower; commit more than 2 000 entries; restart it | It receives InstallSnapshot (`SNAPSHOT_INSTALLED` event) and converges | Recovery |

S1, S2, S3 and S9 make up the live demo, each ending with the `wallet status` table showing identical hashes on all nodes.

---

## 11. Metrics: exact definitions and CSV schemas

**Commit latency:** measured at the client, from sending `SubmitTransfer` to receiving `COMMITTED`, including redirects and retries. Report p50/p95/p99 and the CDF at several offered loads, on laptops and on RCE.

**Leader-failover time:** two numbers per run. *Election time* = first `LEADER_ELECTED` in a new term − `LEADER_KILLED`. *Unavailability* = first successful commit after the kill − `LEADER_KILLED`. Election time is bounded below by the election timeout, so report the configured timeout next to the results.

**Recovery time:** for a returning node, split into *restore* (`STATE_RESTORED.ms`), *local replay* (`LOCAL_REPLAY_DONE.ms`) and *catch-up* (time until `last_applied` reaches the leader's `commit_index` sampled at restart). Compare `snapshot_interval=0` (full replay) with `1000` (snapshot + replay) at log lengths of 10k, 50k and 100k entries.

**`requests.csv`** (load generator)
```
send_ts,recv_ts,latency_ms,outcome,tx_status,attempts,node_first,node_final,sender,nonce,index
```
**`timeline.csv`** (experiments)
```
ts,event,node,term,detail
```
**`recovery.csv`** (experiments)
```
run,log_length,snapshot_interval,restore_ms,local_replay_ms,catchup_ms,total_ms,platform
```
`platform` is `laptop` or `rce`. Timestamps are Unix seconds as floats.

---

## 12. Engineering rules

**Determinism (anything reachable from `apply`)**
- Integers only: no floats, no `time`, no `random`, no environment reads.
- Never rely on dict or set iteration order for hashing; always sort.
- The same Python and `cryptography` versions on every node (pinned in `pyproject.toml`).
- Every entry, including rejected transfers and NOOPs, extends the chain and advances `last_applied`.

**Durability ordering (P and A)**
- Persist term and vote before answering RequestVote; persist entries before answering AppendEntries success and before the leader counts itself toward a commit.
- The snapshot must be durable before compaction. Atomic file replacement = write tmp + fsync + rename + directory fsync.

**Async hygiene**
- Raft, the services and the state machine all run on one event loop per node; no locks are needed as long as no `await` sits inside a critical section. `fsync` blocks briefly, which is acceptable at this scale; if commit latency suffers, P may move `append` to `asyncio.to_thread` as long as store calls stay serialised.

**Git workflow**
- `main` is always green (CI runs `make proto && pytest`). Branches `p/...`, `a/...`, `v/...`; small PRs, each reviewed by one other member.
- Edit only your own directories. Contract files (`ledgernet/common/`, `ledgernet/testing/`, `proto/`) change only through a PR titled `CONTRACT: ...` approved by **both** other members, with the fakes and contract tests updated in the same PR.

**Logging**
- Python `logging` with node id and term on every line; DEBUG for per-RPC detail (off by default); INFO for role changes, commit rate and snapshots.

**Weekly 30-minute sync:** each person demos what passes, raises any contract change requests, and the team confirms the next week's swap.

---

## 13. Glossary

| Term | Meaning |
|---|---|
| NOOP entry | Appended by every new leader so earlier-term entries can be committed; extends the chain, changes no balances |
| Receipt | `tx_id → ApplyResult`, stored only for nonce-consuming outcomes (`APPLIED`, `REJECTED_INSUFFICIENT_FUNDS`) |
| Chain head | Running SHA-256 over all applied entries since genesis; equal chain heads at equal indexes mean identical histories |
| State hash | SHA-256 over all accounts, `last_applied` and the chain head; equal state hashes mean identical ledgers |
| Snapshot + replay | Startup path: restore the latest snapshot, replay the local log up to the persisted commit index, then catch up from the leader |
| CheckQuorum | A leader without majority contact for one election timeout steps down, so the minority side refuses writes |
| PreVote | A probe round before a real election, so isolated nodes cannot inflate terms and disrupt the cluster on rejoining |
