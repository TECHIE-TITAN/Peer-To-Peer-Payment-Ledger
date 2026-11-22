"""FileSnapshotStore: durable, checksummed snapshot files (implementation.md A6.4).

Each snapshot is one file ``snap-<index:012d>.bin``:

    header = magic b"LNSNAP1" | index u64 | term u64 | data length u64 | SHA-256(data)   (big-endian)
    body   = data

``save`` writes ``<name>.tmp``, fsyncs it, renames it into place and fsyncs the directory, then deletes
all but the newest ``keep`` snapshots. ``load_latest`` returns the newest file whose header and
checksum verify, so a corrupted newest snapshot falls back to the previous one and a ``.tmp`` left by a
crash mid-write is ignored.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import struct

from ledgernet.common.interfaces import SnapshotStore
from ledgernet.common.types import Snapshot
from ledgernet.storage._fsutil import fsync_dir

log = logging.getLogger(__name__)

MAGIC = b"LNSNAP1"
_HEADER_FMT = ">7sQQQ32s"
HEADER_LEN = struct.calcsize(_HEADER_FMT)  # 63
_NAME_RE = re.compile(r"^snap-(\d{12})\.bin$")


def snapshot_file_name(index: int) -> str:
    return f"snap-{index:012d}.bin"


def encode_snapshot_file(snap: Snapshot) -> bytes:
    digest = hashlib.sha256(snap.data).digest()
    header = struct.pack(
        _HEADER_FMT, MAGIC, snap.last_included_index, snap.last_included_term, len(snap.data), digest
    )
    return header + snap.data


def decode_snapshot_file(blob: bytes) -> Snapshot:
    """Parse and verify one snapshot file; raises ValueError if it is corrupt."""
    if len(blob) < HEADER_LEN:
        raise ValueError("short snapshot header")
    magic, index, term, length, digest = struct.unpack(_HEADER_FMT, blob[:HEADER_LEN])
    if magic != MAGIC:
        raise ValueError("bad snapshot magic")
    data = blob[HEADER_LEN:]
    if len(data) != length:
        raise ValueError(f"snapshot length mismatch: header {length}, file {len(data)}")
    if hashlib.sha256(data).digest() != digest:
        raise ValueError("snapshot checksum mismatch")
    return Snapshot(index, term, data)


class FileSnapshotStore(SnapshotStore):
    def __init__(self, dir_path: str, keep: int = 2) -> None:
        if keep < 1:
            raise ValueError("keep must be >= 1")
        self._dir = dir_path
        self._keep = keep
        os.makedirs(dir_path, exist_ok=True)

    @property
    def dir_path(self) -> str:
        return self._dir

    def _listing(self) -> list[tuple[int, str]]:
        """(index, file name) of every finished snapshot file, newest first."""
        found = []
        for name in os.listdir(self._dir):
            m = _NAME_RE.match(name)
            if m:
                found.append((int(m.group(1)), name))
        found.sort(reverse=True)
        return found

    def indexes(self) -> list[int]:
        """Indexes of all snapshot files on disk (valid or not), newest first. Used by the auditor."""
        return [index for index, _ in self._listing()]

    def save(self, snap: Snapshot) -> None:
        name = snapshot_file_name(snap.last_included_index)
        final = os.path.join(self._dir, name)
        tmp = final + ".tmp"
        with open(tmp, "wb") as f:
            f.write(encode_snapshot_file(snap))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, final)
        fsync_dir(self._dir)
        self._prune()

    def _prune(self) -> None:
        for _, name in self._listing()[self._keep :]:
            os.remove(os.path.join(self._dir, name))
        for name in os.listdir(self._dir):
            if name.endswith(".tmp"):  # left by a crash in the middle of an earlier save
                os.remove(os.path.join(self._dir, name))

    def load_latest(self) -> Snapshot | None:
        for index, name in self._listing():
            path = os.path.join(self._dir, name)
            try:
                with open(path, "rb") as f:
                    snap = decode_snapshot_file(f.read())
            except (OSError, ValueError) as exc:
                log.warning("skipping corrupt snapshot %s: %s", path, exc)
                continue
            if snap.last_included_index != index:
                log.warning("skipping snapshot %s: header index %d", path, snap.last_included_index)
                continue
            return snap
        return None
