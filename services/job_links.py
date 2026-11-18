"""Readable names for job folders: a symlink beside each `jobNNN` folder, named after what ran there.

    External/005_alignment_aretomo -> job005
    External/023_tm_ribosome       -> job023
    Import/001_import              -> job001

The real folders keep their `jobNNN` names; every path written into a star file, and every reader that looks
for `jobNNN`, is unchanged. The links are derived from ProjectState and can be rebuilt at any time: a job in
`state.jobs` whose folder exists gets one; a folder no job owns (deleted, or a failed attempt that started
over in a new folder) gets none. Only links this module made are touched: a symlink named `NNN_<slug>` whose
target is a bare `jobNNN`. Real folders and links a user made by hand are left alone.

`wanted_links(state)` reads memory only and runs on the loop; `apply_links(project_dir, wanted)` is the disk
side and blocks. `sync_job_links(project_dir, state)` does both, the second in a thread. Design:
docs/roadmaps/roadmap_57-readable-job-folders.md.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from services.jobs.spec import JOB_SPEC_BY_TYPE

if TYPE_CHECKING:
    from services.project_state import ProjectState

logger = logging.getLogger(__name__)

LINK_DIRS = ("External", "Import")
MAX_NAME = 48

_JOB_DIR = re.compile(r"^(External|Import)/(job(\d+))/?$")
_JOB_TARGET = re.compile(r"^job\d+$")
_OUR_LINK = re.compile(r"^\d{3,}_[a-z0-9_-]+$")
_UNSAFE = re.compile(r"[^a-z0-9_-]+")


def _slug(text: str) -> str:
    return _UNSAFE.sub("_", text.lower()).strip("_")


def link_name(job_model, state: ProjectState | None = None) -> tuple[str, str, str] | None:
    """(category dir, link name, target) for a job that owns a folder, e.g.
    ("External", "005_alignment_aretomo", "job005"); None for a job without one. `state` lets a Denoise
    Predict be named by the method of the train job whose model it runs, which its driver uses over its own
    `denoise_method`."""
    m = _JOB_DIR.match(job_model.relion_job_name or "")
    if m is None:
        return None
    category, target, number = m.group(1), m.group(2), m.group(3)
    spec = JOB_SPEC_BY_TYPE.get(job_model.job_type)
    # A type without a slug (none today) still gets a readable name from its type id.
    kind = spec.dir_slug if spec is not None and spec.dir_slug else job_model.job_type.value
    qualifier = getattr(job_model, "species_id", None)
    if not qualifier and spec is not None and spec.dir_qualifier:
        value = getattr(job_model, spec.dir_qualifier, None)
        if state is not None and hasattr(job_model, "inherited_from_train"):
            value = job_model.inherited_from_train(state)[0] or value
        qualifier = value.value if isinstance(value, Enum) else value
    parts = [number, _slug(kind)]
    if qualifier:
        parts.append(_slug(str(qualifier)))
    name = "_".join(p for p in parts if p)[:MAX_NAME].rstrip("_")
    return category, name, target


def wanted_links(state: ProjectState) -> dict[str, dict[str, str]]:
    """{category dir: {link name: target}} for every job in `state.jobs` that owns a folder. Reads memory
    only. Two jobs claiming one folder (should not happen) give two links to it."""
    wanted: dict[str, dict[str, str]] = {c: {} for c in LINK_DIRS}
    for job_model in list(state.jobs.values()):
        hit = link_name(job_model, state)
        if hit is not None:
            category, name, target = hit
            wanted[category][name] = target
    return wanted


def _is_ours(entry: os.DirEntry) -> bool:
    if not entry.is_symlink() or not _OUR_LINK.match(entry.name):
        return False
    try:
        return bool(_JOB_TARGET.match(os.readlink(entry.path)))
    except OSError:
        return False


def apply_links(project_dir: Path, wanted: dict[str, dict[str, str]]) -> dict[str, int]:
    """Make the links in the project's category dirs match `wanted`. Blocks: call off the loop. A link that
    cannot be written (a folder without write permission, a read-only mount) is logged and skipped: nothing
    reads the links, so a missing one breaks nothing. Returns {"made": n, "removed": n, "failed": n}."""
    counts = {"made": 0, "removed": 0, "failed": 0}
    for category in LINK_DIRS:
        base = project_dir / category
        if not base.is_dir():
            continue
        want = {name: target for name, target in wanted.get(category, {}).items() if (base / target).is_dir()}
        with os.scandir(base) as it:
            present = {e.name: os.readlink(e.path) for e in it if _is_ours(e)}
        for name, target in present.items():
            if want.get(name) == target:
                continue
            try:
                os.unlink(base / name)
                counts["removed"] += 1
            except FileNotFoundError:
                pass  # removed meanwhile (another server syncing the same project)
            except OSError as e:
                counts["failed"] += 1
                logger.warning("%s · could not remove the job link %s/%s: %s", project_dir.name, category, name, e)
        for name, target in want.items():
            if present.get(name) == target:
                continue
            path = base / name
            if os.path.lexists(path) and not path.is_symlink():
                # A real file or folder with this name: never ours to replace.
                continue
            tmp = base / f".{name}.tmp"
            try:
                if os.path.lexists(tmp):
                    os.unlink(tmp)
                os.symlink(target, tmp)
                os.replace(tmp, path)
                counts["made"] += 1
            except OSError as e:
                counts["failed"] += 1
                logger.warning("%s · could not write the job link %s/%s: %s", project_dir.name, category, name, e)
                try:
                    os.unlink(tmp)
                except FileNotFoundError:
                    pass  # never made
                except OSError:
                    logger.warning("%s · left %s behind", project_dir.name, tmp)
    return counts


async def sync_job_links(project_dir: Path, state: ProjectState) -> dict[str, int]:
    """Bring the project's job links in line with `state`: what is wanted is read on the loop, the disk is
    changed in a thread."""
    wanted = wanted_links(state)
    counts = await asyncio.to_thread(apply_links, Path(project_dir), wanted)
    if counts["made"] or counts["removed"]:
        logger.info("%s · job links: %s", Path(project_dir).name, counts)
    return counts
