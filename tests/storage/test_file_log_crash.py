"""FileLogStore crash behaviour: torn writes and crashes in the middle of compaction."""

from __future__ import annotations

import json
import os

import pytest

from ledgernet.common.types import LogEntry
from ledgernet.storage.file_log import (
    LOGMETA_NAME,
    WAL_NAME,
    FileLogStore,
    encode_entry_record,
    encode_truncate_record,
)


def _entry(i, term=1, size=20):
    return LogEntry(i, term, bytes([i % 256]) * size)


def _write_log(dir_path, n):
    s = FileLogStore(dir_path)
    for i in range(1, n + 1):
        s.append([_entry(i)])
    s.close()


def test_cut_at_every_byte_of_last_record(tmp_path):
    """Cutting log.wal anywhere inside the last record keeps exactly the fully written prefix."""
    n = 5
    src = tmp_path / "src"
    _write_log(str(src), n)
    full = (src / WAL_NAME).read_bytes()
    last_len = len(encode_entry_record(_entry(n)))
    start_of_last = len(full) - last_len

    for cut in range(start_of_last, len(full) + 1):
        d = tmp_path / f"cut{cut}"
        d.mkdir()
        (d / WAL_NAME).write_bytes(full[:cut])
        s = FileLogStore(str(d))
        expected = n if cut == len(full) else n - 1
        assert s.last_index() == expected, f"cut at {cut}"
        assert s.entries(1, 100) == [_entry(i) for i in range(1, expected + 1)]
        assert s.torn_bytes == (0 if cut == len(full) else cut - start_of_last)
        # The torn tail was cut off on disk, so the store can carry on appending.
        s.append([_entry(expected + 1, term=2)])
        s.close()
        r = FileLogStore(str(d))
        assert r.last_index() == expected + 1 and r.term_at(expected + 1) == 2
        r.close()


def test_cut_inside_truncate_record(tmp_path):
    d = tmp_path / "wal"
    s = FileLogStore(str(d))
    s.append([_entry(i) for i in range(1, 6)])
    s.truncate_from(3)
    s.close()
    full = (d / WAL_NAME).read_bytes()
    rec = len(encode_truncate_record(3))
    for cut in range(len(full) - rec, len(full)):
        (d / WAL_NAME).write_bytes(full[:cut])
        r = FileLogStore(str(d))
        assert r.last_index() == 5  # the truncate never became durable
        r.close()


def test_corrupt_crc_in_tail_is_cut(tmp_path):
    d = tmp_path / "wal"
    _write_log(str(d), 4)
    blob = bytearray((d / WAL_NAME).read_bytes())
    blob[-1] ^= 0xFF  # flip a byte inside the last command
    (d / WAL_NAME).write_bytes(bytes(blob))
    s = FileLogStore(str(d))
    assert s.last_index() == 3
    s.close()


def test_read_only_does_not_repair(tmp_path):
    d = tmp_path / "wal"
    _write_log(str(d), 3)
    blob = (d / WAL_NAME).read_bytes()
    (d / WAL_NAME).write_bytes(blob[:-4])
    r = FileLogStore(str(d), read_only=True)
    assert r.last_index() == 2 and r.torn_bytes > 0
    assert (d / WAL_NAME).read_bytes() == blob[:-4]  # untouched
    with pytest.raises(PermissionError):
        r.append([_entry(3)])


def test_crash_between_logmeta_and_log_rename(tmp_path):
    """logmeta.json says the snapshot is at 6, but log.wal still holds entries 1..10."""
    d = tmp_path / "wal"
    _write_log(str(d), 10)
    (d / LOGMETA_NAME).write_text(json.dumps({"snapshot_index": 6, "snapshot_term": 1}))
    (d / (WAL_NAME + ".tmp")).write_bytes(b"".join(encode_entry_record(_entry(i)) for i in range(7, 11)))
    s = FileLogStore(str(d))
    assert s.snapshot_index() == 6 and s.last_index() == 10
    assert [e.index for e in s.entries(7, 20)] == [7, 8, 9, 10]
    assert not os.path.exists(d / (WAL_NAME + ".tmp"))
    s.close()


def test_crash_before_logmeta_ignores_tmp(tmp_path):
    d = tmp_path / "wal"
    _write_log(str(d), 5)
    (d / (WAL_NAME + ".tmp")).write_bytes(b"half written garbage")
    s = FileLogStore(str(d))
    assert s.snapshot_index() == 0 and s.last_index() == 5
    s.close()


def test_corrupt_commit_file_reads_as_zero(tmp_path):
    d = tmp_path / "wal"
    s = FileLogStore(str(d))
    s.save_commit_index(42)
    s.close()
    assert FileLogStore(str(d)).load_commit_index() == 42
    raw = bytearray((d / "commit").read_bytes())
    raw[0] ^= 0x01
    (d / "commit").write_bytes(bytes(raw))
    assert FileLogStore(str(d)).load_commit_index() == 0
    (d / "commit").write_bytes(b"\x00\x01")
    assert FileLogStore(str(d)).load_commit_index() == 0
