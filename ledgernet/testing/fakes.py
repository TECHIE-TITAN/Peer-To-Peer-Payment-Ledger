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
