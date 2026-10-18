"""Deleting a whole project directory.

`delete_project_dir` first renames the directory to `.<name>.deleting-<hex>` beside it: one metadata
operation, however many terabytes the project holds. From then on no project scan lists it
(`is_being_deleted`), a link to the project finds nothing, and nothing that writes into the project by
its path can put files back into a directory that is being taken apart. The directory is then deleted
one job directory at a time, so a caller can show progress, with project_params.json last: a deletion
that cannot finish puts what is left back under the project's name, where the scan lists it again, and
names what it could not delete.

`delete_path` is the single deletion step, shared with clearing a project's Trash/.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import stat
import threading
import uuid
from pathlib import Path
from typing import Any

from services.background_tasks import ProgressCallback
from services.result import err, ok

logger = logging.getLogger(__name__)

PARAMS_FILE = "project_params.json"
_DELETING_RE = re.compile(r"\..+\.deleting-[0-9a-f]{8}")


def is_being_deleted(name: str) -> bool:
    """Whether a directory name is the one `delete_project_dir` gives a project while it deletes it."""
    return _DELETING_RE.fullmatch(name) is not None


def delete_path(path: Path) -> list[str]:
    """Delete a file, a symlink (never followed) or a whole tree, carrying on past what cannot be deleted;
    returns those paths with the reason. A path that is already gone is nothing to do."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        # Deleted already, e.g. from another server.
        return []
    problems: list[str] = []
    if stat.S_ISDIR(st.st_mode):
        shutil.rmtree(path, onerror=lambda _fn, failed, exc_info: problems.append(f"{failed}: {exc_info[1]}"))
        return problems
    try:
        os.unlink(path)
    except OSError as e:
        problems.append(f"{path}: {e}")
    return problems


def _units(root: Path) -> list[Path]:
    """What to delete, in order: every directory one level down (the job directories, which hold the
    bytes), then each top-level entry, and project_params.json last."""
    with os.scandir(root) as it:
        tops = sorted(it, key=lambda e: e.name)
    units: list[Path] = []
    for top in tops:
        if top.is_dir(follow_symlinks=False):
            with os.scandir(top.path) as it:
                units += sorted(Path(c.path) for c in it if c.is_dir(follow_symlinks=False))
    units += [Path(t.path) for t in tops if t.name != PARAMS_FILE]
    units.append(root / PARAMS_FILE)
    return units


def _take_apart(root: Path, progress_cb: ProgressCallback | None, stop: threading.Event | None) -> str | None:
    """Delete `root` unit by unit. Returns what stopped it, or None once `root` is gone."""
    try:
        units = _units(root)
    except OSError as e:
        # A directory this account cannot list: nothing is deleted yet, the caller reports it.
        return str(e)
    for i, unit in enumerate(units):
        if stop is not None and stop.is_set():
            return "stopped"
        if progress_cb is not None:
            progress_cb(i, len(units), unit.relative_to(root).as_posix())
        problems = delete_path(unit)
        if problems:
            more = f" (and {len(problems) - 1} more)" if len(problems) > 1 else ""
            return f"{problems[0]}{more}"
    try:
        os.rmdir(root)
    except OSError as e:
        # Not empty after all: something wrote into it meanwhile. The caller reports it.
        return str(e)
    return None


def delete_project_dir(
    project_dir: Path, progress_cb: ProgressCallback | None = None, stop: threading.Event | None = None
) -> dict[str, Any]:
    """Permanently delete a project directory. `progress_cb` hears of each unit before it goes; a set
    `stop` ends the deletion before the next one, as a failure would. Blocks: callers run it off the
    event loop."""
    if project_dir.is_symlink():
        # Whether the link or the project it points to should go is not this function's guess.
        return err(f"{project_dir} is a symlink: nothing deleted")
    doomed = project_dir.with_name(f".{project_dir.name}.deleting-{uuid.uuid4().hex[:8]}")
    try:
        os.rename(project_dir, doomed)
    except FileNotFoundError:
        # Deleted already, e.g. from another server.
        return ok()
    except OSError as e:
        logger.exception("Could not start deleting %s", project_dir)
        return err(f"Could not delete {project_dir}: {e}")
    problem = _take_apart(doomed, progress_cb, stop)
    if problem is None:
        return ok()
    try:
        os.rename(doomed, project_dir)
        left = project_dir
    except OSError:
        # The name was taken meanwhile (a new project): what is left keeps the deletion name.
        left = doomed
    return err(f"Could not delete all of {project_dir.name}: {problem}. What is left is in {left}")
