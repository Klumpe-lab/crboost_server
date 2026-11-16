"""The modes project files and folders get.

`tempfile.mkstemp` creates its file 0600 whatever the umask, and renaming it into place keeps that
mode. On a filesystem with group ACLs (CBE's Isilon) nobody notices; on a plain POSIX filesystem the
project's other users can no longer read what this process saved.
"""

from __future__ import annotations

import os
import stat

# The process umask, read once at import: os.umask can only be read by setting it.
_UMASK = os.umask(0)
os.umask(_UMASK)

SHARED_FILE_MODE = 0o666 & ~_UMASK


def make_shared(fd: int) -> None:
    """Give a file opened with `mkstemp` the mode a plain `open()` would have given it."""
    os.fchmod(fd, SHARED_FILE_MODE)


def make_group_writable_dir(path) -> None:
    """A project folder the group works in: group-writable, and setgid so what anyone makes inside it keeps the
    folder's group whatever their primary group. Not ours to change (made by someone else): left as it is."""
    try:
        mode = os.stat(path).st_mode
        os.chmod(path, mode | stat.S_IRWXG | stat.S_ISGID)
    except PermissionError:
        # Someone else's folder: only its owner may change its mode.
        pass
