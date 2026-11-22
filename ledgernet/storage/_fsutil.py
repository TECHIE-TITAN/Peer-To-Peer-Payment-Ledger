"""Small durable-file helpers shared by the storage classes (owned by A)."""

from __future__ import annotations

import errno
import os


def fsync_dir(dir_path: str) -> None:
    """Make a rename or file creation inside `dir_path` durable.

    POSIX needs an fsync on the directory itself. Windows cannot open a directory as a file and
    NTFS journals the rename, so there it is a no-op. Filesystems that do not support directory
    fsync (EINVAL) are tolerated; any other error is raised.
    """
    if os.name == "nt":
        return
    fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    except OSError as exc:
        if exc.errno != errno.EINVAL:
            raise
    finally:
        os.close(fd)


def atomic_write(path: str, data: bytes) -> None:
    """Replace `path` with `data` atomically and durably: tmp + fsync + rename + directory fsync."""
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(os.path.abspath(path)))
