"""Offline auditor: independently re-checks node data directories after a run (implementation.md A6.6).

    python tools/audit.py --genesis config/genesis.json data/n1 data/n2 data/n3 data/n4 data/n5

Per directory it restores the latest snapshot into a fresh ``SqliteLedger`` (or starts from genesis),
opens the write-ahead log read-only and replays it up to the persisted commit index, checking at every
entry:

* chain continuity: each block hash equals ``block_hash(previous head, index, term, command)``;
* log sanity: contiguous indexes and non-decreasing terms;
* nonces: only the sender's nonce moves, by exactly one for nonce-consuming outcomes, else not at all;
* conservation (``sum(balances) == total supply``) and no negative balance;
* (by default) every ``ApplyResult`` agrees with ``ReferenceLedger``, the executable specification.

Across directories it checks that the committed prefixes agree on every common index (block hashes)
and that nodes with equal ``last_applied`` have equal state hashes.

It prints a PASS/FAIL report and exits non-zero on failure. V's end-to-end tests call
``tools.audit.audit_dirs(...)`` after every scenario.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field

if __package__ in (None, ""):  # run as a script: make the repository root importable
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ledgernet.common.hashing import block_hash
from ledgernet.common.tx import DecodeError, decode_command
from ledgernet.common.types import TxStatus
from ledgernet.ledger.genesis import load_genesis
from ledgernet.ledger.sqlite_ledger import SqliteLedger
from ledgernet.storage.file_log import FileLogStore, WalCorruptError
from ledgernet.storage.snapshot_store import FileSnapshotStore
from ledgernet.testing.fakes import ReferenceLedger

_NONCE_CONSUMING = (TxStatus.APPLIED, TxStatus.REJECTED_INSUFFICIENT_FUNDS)
_REPLAY_BATCH = 1000


@dataclass
class NodeAudit:
    data_dir: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    snapshot_index: int = 0
    commit_index: int = 0
    last_log_index: int = 0
    last_applied: int = 0
    replayed: int = 0
    chain_head: str = ""
    state_hash: str = ""
    total_balance: int = 0
    total_supply: int = 0
    block_hashes: dict[int, str] = field(default_factory=dict, repr=False)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class AuditReport:
    nodes: list[NodeAudit]
    errors: list[str] = field(default_factory=list)  # cross-node
    warnings: list[str] = field(default_factory=list)
    common_indexes: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors and all(n.ok for n in self.nodes)

    def render(self) -> str:
        lines = []
        for n in self.nodes:
            lines.append(
                f"AUDIT {n.data_dir}: {'PASS' if n.ok else 'FAIL'}  snapshot={n.snapshot_index} "
                f"commit={n.commit_index} log={n.last_log_index} applied={n.last_applied} "
                f"replayed={n.replayed} head={n.chain_head[:12]} state={n.state_hash[:12]} "
                f"balance={n.total_balance}/{n.total_supply}"
            )
            lines += [f"  error: {e}" for e in n.errors]
            lines += [f"  warning: {w}" for w in n.warnings]
        if len(self.nodes) > 1:
            lines.append(
                f"CROSS-CHECK: {'PASS' if not self.errors else 'FAIL'}  "
                f"({len(self.nodes)} nodes, {self.common_indexes} indexes compared)"
            )
            lines += [f"  error: {e}" for e in self.errors]
            lines += [f"  warning: {w}" for w in self.warnings]
        lines.append(f"RESULT: {'PASS' if self.ok else 'FAIL'}")
        return "\n".join(lines)


class _Stop(Exception):
    """Raised inside a node audit once its state can no longer be trusted."""


def audit_dir(data_dir: str, genesis: dict[bytes, int], check_reference: bool = True) -> NodeAudit:
    """Audit one node data directory (``<data_dir>/wal`` and ``<data_dir>/snapshots``)."""
    result = NodeAudit(data_dir=data_dir, total_supply=sum(genesis.values()))
    scratch = tempfile.mkdtemp(prefix="ledgernet-audit-")
    ledger = None
    try:
        ledger = SqliteLedger(os.path.join(scratch, "state.db"), genesis)
        _audit_into(result, data_dir, genesis, ledger, check_reference)
    except _Stop:
        pass
    finally:
        if ledger is not None:
            result.last_applied = ledger.last_applied()
            result.chain_head = ledger.chain_head()
            result.state_hash = ledger.state_hash()
            result.total_balance = ledger.total_balance()
            ledger.close()
        shutil.rmtree(scratch, ignore_errors=True)
    return result


def _fail(result: NodeAudit, msg: str) -> None:
    result.errors.append(msg)
    raise _Stop


def _audit_into(
    result: NodeAudit, data_dir: str, genesis: dict[bytes, int], ledger: SqliteLedger, check_reference: bool
) -> None:
    wal_dir = os.path.join(data_dir, "wal")
    snap_dir = os.path.join(data_dir, "snapshots")
    if not os.path.isdir(wal_dir):
        _fail(result, f"no write-ahead log directory at {wal_dir}")

    # 1. Starting state: the latest valid snapshot, or genesis.
    result.block_hashes[0] = ledger.chain_head()
    if os.path.isdir(snap_dir):
        store = FileSnapshotStore(snap_dir)
        on_disk = store.indexes()
        snap = store.load_latest()
        if on_disk and (snap is None or snap.last_included_index != on_disk[0]):
            result.warnings.append(
                f"newest snapshot file (index {on_disk[0]}) is corrupt; "
                f"using {snap.last_included_index if snap else 'genesis'}"
            )
        if snap is not None:
            try:
                ledger.restore(snap)
            except ValueError as exc:
                _fail(result, f"snapshot {snap.last_included_index} cannot be restored: {exc}")
            result.snapshot_index = snap.last_included_index
            if ledger.total_supply() != result.total_supply:
                _fail(
                    result,
                    f"snapshot {snap.last_included_index} has total supply {ledger.total_supply()}, "
                    f"genesis says {result.total_supply}",
                )
            _check_whole_state(result, ledger, f"snapshot {snap.last_included_index}")
            result.block_hashes = {snap.last_included_index: ledger.chain_head()}

    reference = None
    if check_reference:
        reference = ReferenceLedger(genesis)
        if ledger.last_applied() > 0:
            reference.restore(ledger.to_reference_snapshot())

    # 2. The log, read-only.
    try:
        log = FileLogStore(wal_dir, read_only=True)
    except WalCorruptError as exc:
        _fail(result, f"write-ahead log is corrupt: {exc}")
    result.commit_index = log.load_commit_index()
    result.last_log_index = log.last_index()
    if log.torn_bytes:
        result.warnings.append(f"write-ahead log has a torn tail of {log.torn_bytes} bytes (ignored)")
    if result.commit_index > log.last_index():
        result.errors.append(
            f"persisted commit index {result.commit_index} is beyond the end of the log "
            f"({log.last_index()}): committed entries are missing or corrupt"
        )
    if log.snapshot_index() > ledger.last_applied():
        _fail(
            result,
            f"log is compacted to {log.snapshot_index()} but no valid snapshot covers it "
            f"(state is at {ledger.last_applied()})",
        )
    target = min(result.commit_index, log.last_index())

    # 3. Replay with checks after every entry.
    prev_term = ledger.last_term()
    while ledger.last_applied() < target:
        first = ledger.last_applied() + 1
        batch = log.entries(first, min(target, first + _REPLAY_BATCH - 1) + 1)
        if not batch:
            _fail(result, f"log has no entry at index {first}")
        for entry in batch:
            if entry.index != ledger.last_applied() + 1:
                _fail(result, f"log index jumps from {ledger.last_applied()} to {entry.index}")
            if entry.term < prev_term:
                _fail(result, f"term goes backwards at index {entry.index}: {prev_term} -> {entry.term}")
            prev_term = entry.term
            expected_head = block_hash(
                bytes.fromhex(ledger.chain_head()), entry.index, entry.term, entry.command
            )
            touched = _touched_accounts(entry.command)
            before = {pk: ledger.get_account(pk) for pk in touched}

            applied = ledger.apply(entry)
            result.replayed += 1
            result.block_hashes[entry.index] = applied.block_hash

            if applied.block_hash != expected_head.hex() or ledger.chain_head() != expected_head.hex():
                _fail(result, f"hash chain broken at index {entry.index}")
            if reference is not None:
                expected = reference.apply(entry)
                if expected != applied:
                    _fail(
                        result,
                        f"index {entry.index}: ledger gave {applied.status.name}, "
                        f"specification gives {expected.status.name}",
                    )
            _check_step(result, ledger, entry.index, touched, before, applied.status)

    _check_whole_state(result, ledger, f"index {ledger.last_applied()}")
    if reference is not None and reference.state_hash() != ledger.state_hash():
        _fail(result, "final state hash differs from the reference ledger")


def _touched_accounts(command: bytes) -> list[bytes]:
    try:
        tx = decode_command(command)
    except DecodeError:
        return []
    if tx is None or tx.sender == tx.recipient:
        return []
    return [tx.sender, tx.recipient]


def _check_step(
    result: NodeAudit,
    ledger: SqliteLedger,
    index: int,
    touched: list[bytes],
    before: dict[bytes, tuple[int, int]],
    status: TxStatus,
) -> None:
    if ledger.total_balance() != result.total_supply:
        _fail(
            result, f"conservation broken at index {index}: {ledger.total_balance()} != {result.total_supply}"
        )
    for pos, pk in enumerate(touched):
        old_nonce = before[pk][1]
        bal, nonce = ledger.get_account(pk)
        if bal < 0:
            _fail(result, f"negative balance for {pk.hex()[:16]} at index {index}")
        is_sender = pos == 0
        step = 1 if (is_sender and status in _NONCE_CONSUMING) else 0
        if nonce != old_nonce + step:
            _fail(
                result,
                f"nonce of {pk.hex()[:16]} moved {old_nonce} -> {nonce} at index {index} ({status.name})",
            )


def _check_whole_state(result: NodeAudit, ledger: SqliteLedger, where: str) -> None:
    if ledger.total_balance() != result.total_supply:
        _fail(result, f"conservation broken at {where}: {ledger.total_balance()} != {result.total_supply}")
    for pk, bal, nonce in ledger.iter_accounts():
        if bal < 0 or nonce < 0:
            _fail(result, f"negative balance or nonce for {pk.hex()[:16]} at {where}")


def cross_check(nodes: list[NodeAudit], report: AuditReport) -> None:
    compared: set[int] = set()
    for i, a in enumerate(nodes):
        for b in nodes[i + 1 :]:
            common = sorted(set(a.block_hashes) & set(b.block_hashes))
            compared.update(common)
            for idx in common:
                if a.block_hashes[idx] != b.block_hashes[idx]:
                    report.errors.append(
                        f"{a.data_dir} and {b.data_dir} disagree at index {idx}: "
                        f"{a.block_hashes[idx][:12]} vs {b.block_hashes[idx][:12]}"
                    )
                    break
            if a.ok and b.ok and a.last_applied == b.last_applied and a.state_hash != b.state_hash:
                report.errors.append(
                    f"{a.data_dir} and {b.data_dir} have different state hashes at index {a.last_applied}"
                )
            if len(common) <= 1 and a.ok and b.ok:
                report.warnings.append(f"{a.data_dir} and {b.data_dir} share no replayed index to compare")
    report.common_indexes = len(compared)


def audit_dirs(
    data_dirs: list[str], genesis: dict[bytes, int] | str, check_reference: bool = True
) -> AuditReport:
    """Audit every directory and cross-check them. ``genesis`` is a mapping or a path to genesis.json."""
    if isinstance(genesis, str):
        genesis = load_genesis(genesis)
    nodes = [audit_dir(d, genesis, check_reference) for d in data_dirs]
    report = AuditReport(nodes)
    cross_check(nodes, report)
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Offline LedgerNet auditor")
    p.add_argument("--genesis", required=True, help="path to genesis.json")
    p.add_argument("--no-reference", action="store_true", help="skip the cross-check against ReferenceLedger")
    p.add_argument("data_dirs", nargs="+", help="node data directories (each with wal/ and snapshots/)")
    args = p.parse_args(argv)
    report = audit_dirs(args.data_dirs, args.genesis, check_reference=not args.no_reference)
    print(report.render())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
