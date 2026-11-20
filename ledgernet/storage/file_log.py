"""FileLogStore: the durable Raft log and hard state (implementation.md A6.5).

Files in ``dir_path``:

* ``log.wal``       records ``u32 length | u32 crc32(payload) | payload`` (big-endian), where payload is
                    ``u8 type | ...``: ENTRY = index u64, term u64, command bytes; TRUNCATE = from_index u64.
* ``hardstate.json`` ``{"term": ..., "voted_for": ...}``, atomically replaced on every save.
* ``commit``        ``u64 commit_index | u32 crc32``, written in place, fsynced at most every 100 ms.
* ``logmeta.json``  ``{"snapshot_index": ..., "snapshot_term": ...}``, atomically replaced on compaction.

On open the in-memory index (a list of ``LogEntry``, exactly as in ``InMemoryLogStore``) is rebuilt by
scanning ``log.wal``. A short or CRC-failing record is a torn write from a crash: the file is cut at that
offset (unless the store is opened read-only) and everything before it is kept.

Durability: ``append`` writes all records and then calls ``fsync`` once (group commit) before it returns;
``truncate_from`` appends a TRUNCATE record and fsyncs; ``compact_to`` rewrites the surviving suffix to
``log.wal.tmp``, fsyncs it, atomically replaces ``logmeta.json`` and only then renames the new log into
place. If a crash lands between those two renames, the open path drops entries already covered by the
snapshot recorded in ``logmeta.json``.
"""

from __future__ import annotations

import json
import logging
import os
import struct
import time
import zlib
from typing import BinaryIO

from ledgernet.common.interfaces import LogStore
from ledgernet.common.types import LogEntry, NodeId
from ledgernet.storage._fsutil import atomic_write, fsync_dir

log = logging.getLogger(__name__)

WAL_NAME = "log.wal"
HARDSTATE_NAME = "hardstate.json"
COMMIT_NAME = "commit"
LOGMETA_NAME = "logmeta.json"

REC_ENTRY = 1
REC_TRUNCATE = 2

_REC_HEADER = struct.Struct(">II")  # length, crc32(payload)
_ENTRY_HEAD = struct.Struct(">BQQ")  # type, index, term
_TRUNCATE = struct.Struct(">BQ")  # type, from_index
_COMMIT = struct.Struct(">QI")  # commit index, crc32 of the first 8 bytes

COMMIT_FSYNC_INTERVAL_S = 0.1


class WalCorruptError(RuntimeError):
    """A record passed its CRC but makes no sense (a bug, not a torn write)."""


def encode_entry_record(entry: LogEntry) -> bytes:
    payload = _ENTRY_HEAD.pack(REC_ENTRY, entry.index, entry.term) + entry.command
    return _REC_HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def encode_truncate_record(from_index: int) -> bytes:
    payload = _TRUNCATE.pack(REC_TRUNCATE, from_index)
    return _REC_HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def scan_wal(blob: bytes) -> tuple[list[tuple[int, object]], int]:
    """Parse WAL bytes into records.

    Returns ``(records, valid_length)``: records are ``(REC_ENTRY, LogEntry)`` or ``(REC_TRUNCATE, int)``,
    and ``valid_length`` is the offset of the first torn or corrupt record (``len(blob)`` if none).
    """
    records: list[tuple[int, object]] = []
    offset = 0
    end = len(blob)
    while offset < end:
        if end - offset < _REC_HEADER.size:
            break
        length, crc = _REC_HEADER.unpack_from(blob, offset)
        start = offset + _REC_HEADER.size
        if length == 0 or end - start < length:
            break
        payload = blob[start : start + length]
        if zlib.crc32(payload) != crc:
            break
        rtype = payload[0]
        if rtype == REC_ENTRY and length >= _ENTRY_HEAD.size:
            _, index, term = _ENTRY_HEAD.unpack_from(payload, 0)
            records.append((REC_ENTRY, LogEntry(index, term, bytes(payload[_ENTRY_HEAD.size :]))))
        elif rtype == REC_TRUNCATE and length == _TRUNCATE.size:
            records.append((REC_TRUNCATE, _TRUNCATE.unpack_from(payload, 0)[1]))
        else:
            raise WalCorruptError(f"unknown WAL record type {rtype} (length {length}) at offset {offset}")
        offset = start + length
    return records, offset


class FileLogStore(LogStore):
    """Durable ``LogStore``. Pass ``read_only=True`` (the auditor does) to inspect a directory without
    repairing a torn tail or writing anything."""

    def __init__(self, dir_path: str, read_only: bool = False) -> None:
        self._dir = dir_path
        self._read_only = read_only
        if read_only:
            if not os.path.isdir(dir_path):
                raise FileNotFoundError(dir_path)
        else:
            os.makedirs(dir_path, exist_ok=True)
        self._wal_path = os.path.join(dir_path, WAL_NAME)
        self._commit_path = os.path.join(dir_path, COMMIT_NAME)

        self._term, self._voted_for = self._read_hard_state()
        self._snap_index, self._snap_term = self._read_logmeta()
        self._commit = self._read_commit()
        self._log: list[LogEntry] = []  # _log[i].index == _snap_index + 1 + i
        self.torn_bytes = 0  # bytes cut (or, read-only, ignored) at the tail on open
        self._load_wal()

        self._wal: BinaryIO | None = None
        self._commit_file: BinaryIO | None = None
        self._last_commit_fsync = 0.0
        self._commit_dirty = False
        if not read_only:
            stale_tmp = self._wal_path + ".tmp"
            if os.path.exists(stale_tmp):  # crash before the compaction rename: the old log is still valid
                os.remove(stale_tmp)
            self._wal = open(self._wal_path, "ab")  # noqa: SIM115 (long-lived handle)

    # ------------------------------------------------------------------ open / recovery

    def _read_hard_state(self) -> tuple[int, NodeId | None]:
        path = os.path.join(self._dir, HARDSTATE_NAME)
        if not os.path.exists(path):
            return 0, None
        with open(path, "rb") as f:
            d = json.loads(f.read())
        return int(d["term"]), d["voted_for"]

    def _read_logmeta(self) -> tuple[int, int]:
        path = os.path.join(self._dir, LOGMETA_NAME)
        if not os.path.exists(path):
            return 0, 0
        with open(path, "rb") as f:
            d = json.loads(f.read())
        return int(d["snapshot_index"]), int(d["snapshot_term"])

    def _read_commit(self) -> int:
        try:
            with open(self._commit_path, "rb") as f:
                raw = f.read(_COMMIT.size)
        except FileNotFoundError:
            return 0
        if len(raw) != _COMMIT.size:
            return 0
        value, crc = _COMMIT.unpack(raw)
        return value if zlib.crc32(raw[:8]) == crc else 0

    def _load_wal(self) -> None:
        try:
            with open(self._wal_path, "rb") as f:
                blob = f.read()
        except FileNotFoundError:
            return
        records, valid = scan_wal(blob)
        for rtype, value in records:
            if rtype == REC_ENTRY:
                entry = value  # type: ignore[assignment]
                if entry.index <= self._snap_index:  # covered by the snapshot (crash mid-compaction)
                    continue
                if entry.index != self.last_index() + 1:
                    raise WalCorruptError(
                        f"non-contiguous entry {entry.index} in {self._wal_path}, last={self.last_index()}"
                    )
                self._log.append(entry)
            else:
                from_index = max(int(value), self._snap_index + 1)  # type: ignore[arg-type]
                del self._log[from_index - self._snap_index - 1 :]
        if valid < len(blob):
            self.torn_bytes = len(blob) - valid
            log.warning("%s: torn tail of %d bytes at offset %d", self._wal_path, self.torn_bytes, valid)
            if not self._read_only:
                with open(self._wal_path, "r+b") as f:
                    f.truncate(valid)
                    f.flush()
                    os.fsync(f.fileno())

    # ------------------------------------------------------------------ hard state and commit index

    def load_hard_state(self) -> tuple[int, NodeId | None]:
        return self._term, self._voted_for

    def save_hard_state(self, term: int, voted_for: NodeId | None) -> None:
        self._check_writable()
        data = json.dumps({"term": term, "voted_for": voted_for}, sort_keys=True).encode()
        atomic_write(os.path.join(self._dir, HARDSTATE_NAME), data)
        self._term, self._voted_for = term, voted_for

    def load_commit_index(self) -> int:
        return self._commit

    def save_commit_index(self, index: int) -> None:
        self._check_writable()
        if self._commit_file is None:
            mode = "r+b" if os.path.exists(self._commit_path) else "w+b"
            self._commit_file = open(self._commit_path, mode, buffering=0)  # noqa: SIM115
        raw8 = struct.pack(">Q", index)
        self._commit_file.seek(0)
        self._commit_file.write(raw8 + struct.pack(">I", zlib.crc32(raw8)))
        self._commit = index
        self._commit_dirty = True
        now = time.monotonic()
        if now - self._last_commit_fsync >= COMMIT_FSYNC_INTERVAL_S:
            self._sync_commit(now)

    def _sync_commit(self, now: float) -> None:
        if self._commit_file is not None and self._commit_dirty:
            os.fsync(self._commit_file.fileno())
            self._commit_dirty = False
        self._last_commit_fsync = now

    # ------------------------------------------------------------------ log reads

    def snapshot_index(self) -> int:
        return self._snap_index

    def snapshot_term(self) -> int:
        return self._snap_term

    def last_index(self) -> int:
        return self._snap_index + len(self._log)

    def term_at(self, index: int) -> int | None:
        if index == 0:
            return 0
        if index == self._snap_index:
            return self._snap_term
        if index < self._snap_index or index > self.last_index():
            return None
        return self._log[index - self._snap_index - 1].term

    def entries(self, start: int, end: int) -> list[LogEntry]:
        if start <= self._snap_index:
            raise IndexError(f"index {start} is compacted (snapshot at {self._snap_index})")
        end = min(end, self.last_index() + 1)
        return list(self._log[start - self._snap_index - 1 : end - self._snap_index - 1])

    # ------------------------------------------------------------------ log writes

    def append(self, entries: list[LogEntry]) -> None:
        self._check_writable()
        if not entries:
            return
        expected = self.last_index() + 1
        for e in entries:
            if e.index != expected:
                raise ValueError(f"non-contiguous append {e.index}, last={expected - 1}")
            expected += 1
        assert self._wal is not None
        self._wal.write(b"".join(encode_entry_record(e) for e in entries))
        self._wal.flush()
        os.fsync(self._wal.fileno())  # one fsync per call (group commit)
        self._log.extend(entries)

    def truncate_from(self, index: int) -> None:
        self._check_writable()
        if index <= self._snap_index:
            raise ValueError("cannot truncate into the snapshot")
        if index > self.last_index():
            return
        assert self._wal is not None
        self._wal.write(encode_truncate_record(index))
        self._wal.flush()
        os.fsync(self._wal.fileno())
        del self._log[index - self._snap_index - 1 :]

    def compact_to(self, index: int, term: int) -> None:
        self._check_writable()
        if index <= self._snap_index:
            return
        if index >= self.last_index():
            remaining: list[LogEntry] = []
        else:
            remaining = self._log[index - self._snap_index :]

        tmp = self._wal_path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(b"".join(encode_entry_record(e) for e in remaining))
            f.flush()
            os.fsync(f.fileno())
        meta = json.dumps({"snapshot_index": index, "snapshot_term": term}, sort_keys=True).encode()
        atomic_write(os.path.join(self._dir, LOGMETA_NAME), meta)
        # From here on a crash is safe: on open, logmeta makes us skip entries <= index in either file.
        assert self._wal is not None
        self._wal.close()  # Windows cannot replace a file that is open
        os.replace(tmp, self._wal_path)
        fsync_dir(self._dir)
        self._wal = open(self._wal_path, "ab")  # noqa: SIM115 (long-lived handle)
        self._log = remaining
        self._snap_index, self._snap_term = index, term

    # ------------------------------------------------------------------ lifecycle

    def close(self) -> None:
        """Flush the lazily synced commit index and release file handles. Safe to call twice."""
        if self._commit_file is not None:
            self._sync_commit(time.monotonic())
            self._commit_file.close()
            self._commit_file = None
        if self._wal is not None:
            self._wal.close()
            self._wal = None

    def _check_writable(self) -> None:
        if self._read_only:
            raise PermissionError("FileLogStore opened read-only")
        if self._wal is None:
            raise ValueError("FileLogStore is closed")
