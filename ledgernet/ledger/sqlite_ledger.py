"""SqliteLedger: the real ledger state machine (implementation.md 3.6, tasks A6.2 and A6.3).

It implements the ledger rules of Section 3.6 exactly; ``ReferenceLedger`` in ``ledgernet.testing.fakes``
is the executable specification, and ``tests/ledger/test_differential.py`` checks that both give identical
results, chain heads and state hashes on random command streams.

Determinism: nothing reachable from ``apply`` reads the clock, randomness or the environment, money is
integer paise, and hashes iterate accounts in ``ORDER BY pubkey`` (SQLite compares BLOBs with memcmp,
the same order as sorting Python ``bytes``).

Durability comes from the Raft log and snapshots, not from this database: the constructor always starts
from genesis (deleting any old file) and Raft calls ``restore`` if a snapshot exists. That is why the
database runs with ``synchronous=OFF``.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import struct
import tempfile
from collections.abc import Iterable, Iterator

from ledgernet.common.hashing import STATE_DOMAIN, block_hash, genesis_hash
from ledgernet.common.interfaces import LedgerStateMachine
from ledgernet.common.tx import DecodeError, decode_command, tx_id
from ledgernet.common.types import ApplyResult, LogEntry, Snapshot, TxStatus

SCHEMA = """
CREATE TABLE accounts (pubkey BLOB PRIMARY KEY, balance INTEGER NOT NULL, nonce INTEGER NOT NULL);
CREATE TABLE receipts (tx_id BLOB PRIMARY KEY, idx INTEGER, term INTEGER, status INTEGER, block_hash TEXT);
CREATE TABLE meta (k TEXT PRIMARY KEY, v BLOB);
"""

_RECEIPT_STATUSES = (TxStatus.APPLIED, TxStatus.REJECTED_INSUFFICIENT_FUNDS)


def streaming_state_hash(rows: Iterable[tuple[bytes, int, int]], last_applied: int, chain_head: bytes) -> str:
    """Same construction as ``hashing.state_hash`` but over ``(pubkey, balance, nonce)`` rows that are
    already sorted by pubkey, so the whole table never has to be loaded into a dict."""
    h = hashlib.sha256(STATE_DOMAIN + struct.pack(">Q", last_applied) + chain_head)
    for pk, bal, nonce in rows:
        h.update(bytes(pk) + struct.pack(">QQ", bal, nonce))
    return h.hexdigest()


def _remove_db_files(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass


class SqliteLedger(LedgerStateMachine):
    def __init__(self, db_path: str, genesis: dict[bytes, int]) -> None:
        total = sum(genesis.values())
        if total >= 2**63:
            raise ValueError("total supply too large")
        for pk, bal in genesis.items():
            if len(pk) != 32 or bal < 0:
                raise ValueError("genesis needs 32-byte public keys and non-negative balances")
        self._db_path = db_path
        self._total_supply = total
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        _remove_db_files(db_path)
        _remove_db_files(db_path + ".restore")

        self._conn = self._connect(db_path)
        head = genesis_hash(genesis)
        with self._conn:
            self._conn.executescript(SCHEMA)
            self._conn.executemany(
                "INSERT INTO accounts (pubkey, balance, nonce) VALUES (?, ?, 0)",
                sorted(genesis.items()),
            )
            self._conn.executemany(
                "INSERT INTO meta (k, v) VALUES (?, ?)",
                [("last_applied", 0), ("last_term", 0), ("chain_head", head), ("total_supply", total)],
            )
        self._last_applied, self._last_term, self._head = 0, 0, head

    # ------------------------------------------------------------------ connection handling

    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, isolation_level=None)  # we issue BEGIN/COMMIT ourselves
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        return conn

    def _load_meta(self) -> None:
        meta = dict(self._conn.execute("SELECT k, v FROM meta"))
        self._last_applied = int(meta["last_applied"])
        self._last_term = int(meta["last_term"])
        self._head = bytes(meta["chain_head"])
        self._total_supply = int(meta["total_supply"])

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ StateMachine

    def last_applied(self) -> int:
        return self._last_applied

    def apply(self, entry: LogEntry) -> ApplyResult:
        if entry.index != self._last_applied + 1:
            raise ValueError(f"out-of-order apply {entry.index}, expected {self._last_applied + 1}")
        # Everything that needs no database (decoding, the signature check) happens before BEGIN.
        tx = None
        status: TxStatus | None = None
        try:
            tx = decode_command(entry.command)
        except DecodeError:
            status = TxStatus.REJECTED_BAD_FORMAT
        if status is None:
            if tx is None:
                status = TxStatus.NOOP
            elif tx.amount == 0 or tx.sender == tx.recipient:
                status = TxStatus.REJECTED_INVALID_AMOUNT
            elif not tx.verify():
                status = TxStatus.REJECTED_BAD_SIGNATURE

        head = block_hash(self._head, entry.index, entry.term, entry.command)
        c = self._conn
        c.execute("BEGIN")
        try:
            if status is None:
                assert tx is not None
                row = c.execute(
                    "SELECT balance, nonce FROM accounts WHERE pubkey = ?", (tx.sender,)
                ).fetchone()
                bal, nonce = row if row is not None else (0, 0)
                if tx.nonce != nonce + 1:
                    status = TxStatus.REJECTED_BAD_NONCE
                elif bal < tx.amount:
                    status = TxStatus.REJECTED_INSUFFICIENT_FUNDS  # the nonce is still consumed
                    self._put_account(tx.sender, bal, nonce + 1)
                else:
                    status = TxStatus.APPLIED
                    self._put_account(tx.sender, bal - tx.amount, nonce + 1)
                    c.execute(
                        "INSERT INTO accounts (pubkey, balance, nonce) VALUES (?, ?, 0) "
                        "ON CONFLICT(pubkey) DO UPDATE SET balance = balance + excluded.balance",
                        (tx.recipient, tx.amount),
                    )
            result = ApplyResult(entry.index, entry.term, status, head.hex())
            if status in _RECEIPT_STATUSES:
                c.execute(
                    "INSERT OR REPLACE INTO receipts (tx_id, idx, term, status, block_hash) VALUES (?, ?, ?, ?, ?)",
                    (tx_id(entry.command), entry.index, entry.term, int(status), result.block_hash),
                )
            c.executemany(
                "UPDATE meta SET v = ? WHERE k = ?",
                [(entry.index, "last_applied"), (entry.term, "last_term"), (head, "chain_head")],
            )
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise
        self._last_applied, self._last_term, self._head = entry.index, entry.term, head
        return result

    def _put_account(self, pubkey: bytes, balance: int, nonce: int) -> None:
        self._conn.execute(
            "INSERT INTO accounts (pubkey, balance, nonce) VALUES (?, ?, ?) "
            "ON CONFLICT(pubkey) DO UPDATE SET balance = excluded.balance, nonce = excluded.nonce",
            (pubkey, balance, nonce),
        )

    def take_snapshot(self) -> Snapshot:
        """A consistent copy of the whole database as bytes, covering ``last_applied()``."""
        if hasattr(sqlite3.Connection, "serialize"):  # Python 3.11+
            mem = sqlite3.connect(":memory:")
            try:
                self._conn.backup(mem)
                data = bytes(mem.serialize())
            finally:
                mem.close()
        else:  # Python 3.10: back up into a temporary file and read it back
            fd, tmp = tempfile.mkstemp(suffix=".db", dir=os.path.dirname(os.path.abspath(self._db_path)))
            os.close(fd)
            try:
                dest = sqlite3.connect(tmp)
                try:
                    self._conn.backup(dest)
                    dest.execute("PRAGMA journal_mode=DELETE")
                finally:
                    dest.close()
                with open(tmp, "rb") as f:
                    data = f.read()
            finally:
                os.remove(tmp)
        return Snapshot(self._last_applied, self._last_term, data)

    def restore(self, snap: Snapshot) -> None:
        """Replace all state with the snapshot: write it beside the live DB, close, swap, reopen."""
        staging = self._db_path + ".restore"
        _remove_db_files(staging)
        with open(staging, "wb") as f:
            f.write(snap.data)
        # Validate before touching the live database, so a bad snapshot leaves the ledger as it was.
        try:
            probe = sqlite3.connect(staging)
            try:
                meta = dict(probe.execute("SELECT k, v FROM meta"))
            finally:
                probe.close()
        except sqlite3.DatabaseError as exc:
            _remove_db_files(staging)
            raise ValueError(f"snapshot is not a ledger database: {exc}") from exc
        if int(meta.get("last_applied", -1)) != snap.last_included_index:
            _remove_db_files(staging)
            raise ValueError(
                f"snapshot meta.last_applied={meta.get('last_applied')} but "
                f"last_included_index={snap.last_included_index}"
            )
        _remove_db_files(staging + "-wal")
        _remove_db_files(staging + "-shm")
        self._conn.close()
        for suffix in ("-wal", "-shm"):  # a stale WAL must never be replayed onto the new file
            try:
                os.remove(self._db_path + suffix)
            except FileNotFoundError:
                pass
        os.replace(staging, self._db_path)
        self._conn = self._connect(self._db_path)
        self._load_meta()

    def state_hash(self) -> str:
        rows = self._conn.execute("SELECT pubkey, balance, nonce FROM accounts ORDER BY pubkey")
        return streaming_state_hash(rows, self._last_applied, self._head)

    # ------------------------------------------------------------------ LedgerStateMachine

    def get_account(self, pubkey: bytes) -> tuple[int, int]:
        row = self._conn.execute("SELECT balance, nonce FROM accounts WHERE pubkey = ?", (pubkey,)).fetchone()
        return (row[0], row[1]) if row is not None else (0, 0)

    def get_receipt(self, txid: bytes) -> ApplyResult | None:
        row = self._conn.execute(
            "SELECT idx, term, status, block_hash FROM receipts WHERE tx_id = ?", (txid,)
        ).fetchone()
        if row is None:
            return None
        return ApplyResult(row[0], row[1], TxStatus(row[2]), row[3])

    def chain_head(self) -> str:
        return self._head.hex()

    def total_balance(self) -> int:
        return int(self._conn.execute("SELECT COALESCE(SUM(balance), 0) FROM accounts").fetchone()[0])

    def total_supply(self) -> int:
        return self._total_supply

    # ------------------------------------------------------------------ extras (auditor, tooling)

    def last_term(self) -> int:
        return self._last_term

    def iter_accounts(self) -> Iterator[tuple[bytes, int, int]]:
        """All accounts as ``(pubkey, balance, nonce)``, sorted by pubkey."""
        for pk, bal, nonce in self._conn.execute(
            "SELECT pubkey, balance, nonce FROM accounts ORDER BY pubkey"
        ):
            yield bytes(pk), bal, nonce

    def receipt_count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM receipts").fetchone()[0])
