"""Restore: jobs back from a project's Trash/ onto its roster, as they were when they were removed.

The folder goes back where it was (`External/jobNNN`, a rename), and the job's settings back into the project
state from its record or an earlier copy of the project file (`project_trash.find_job_settings`). A job keeps its
instance id unless another job has taken it since; then it gets the next free one, and its folder number stays.
What its inputs named (overrides, the inputs it was submitted with, facets of this project) is named again by the
instance ids those jobs hold now, through the folders they had: a job that went to Trash with it and comes back
with it is found, one that is gone is dropped from the overrides and reported.

Restored in the UI's Trash dialog (`ui/projects_overview.py`) and through the API (`restore_from_trash`).
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
from pathlib import Path
from typing import Any

from services.job_links import sync_job_links
from services.job_models import jobtype_paramclass
from services.models_base import InstanceId, JobType
from services.path_resolution_service import override_instance_id
from services.pipeline_edit import next_instance_id
from services.project_state import ProjectState
from services.project_trash import TRASH_DIR, JobSettings, find_job_settings, job_dir_of, move_back
from services.result import err, ok

logger = logging.getLogger(__name__)


def _trash_entries(project_dir: Path) -> list[str]:
    try:
        return sorted(e.name for e in os.scandir(project_dir / TRASH_DIR) if e.is_dir(follow_symlinks=False))
    except FileNotFoundError:
        # No job was ever removed from this project.
        return []


def _job_number(entry: str) -> int:
    digits = "".join(c for c in job_dir_of(entry).rsplit("/", 1)[-1] if c.isdigit())
    return int(digits) if digits else 0


def _renamed_key(key: Any, rename: dict[str, str | None], dropped: list[str]) -> Any:
    """An override key naming an instance, named again: the same key when it names none or the job keeps its id;
    None when the job it named is gone."""
    if not isinstance(key, str):
        return key
    iid = override_instance_id(key)
    if iid is None or iid not in rename:
        return key
    new = rename[iid]
    if new is None:
        dropped.append(key)
        return None
    return key[: len(key) - len(iid)] + new


def _renamed_facets(facets: Any, project: str, rename: dict[str, str | None], dropped: list[str]) -> Any:
    """Facet choices (`<project path>|<instance id>`): this project's named again, other projects' as they are."""
    if not isinstance(facets, dict):
        return facets
    out = {}
    for where, choice in facets.items():
        path, sep, iid = choice.partition("|") if isinstance(choice, str) else ("", "", "")
        if sep and path.rstrip("/") == project and iid in rename:
            if rename[iid] is None:
                dropped.append(choice)
                continue
            choice = f"{path}|{rename[iid]}"
        out[where] = choice
    return out


def _rename_references(job: dict[str, Any], project: str, rename: dict[str, str | None]) -> list[str]:
    """Name the job's inputs by today's instance ids, in place. Returns what named a job that is gone."""
    dropped: list[str] = []
    overrides = job.get("source_overrides") or {}
    for slot, key in list(overrides.items()):
        new = _renamed_key(key, rename, dropped)
        if new is None:
            del overrides[slot]
        else:
            overrides[slot] = new
    for submitted in (job.get("submitted_inputs") or {}).values():
        if isinstance(submitted, dict):
            # What it ran with is a record: a key naming a job that is gone stays as it was.
            new = _renamed_key(submitted.get("source_key"), rename, [])
            if new is not None:
                submitted["source_key"] = new
            submitted["facets"] = _renamed_facets(submitted.get("facets"), project, rename, [])
    for slot, facets in list((job.get("facet_choices") or {}).items()):
        job["facet_choices"][slot] = _renamed_facets(facets, project, rename, dropped)
    return dropped


async def restore_from_trash(backend, project_path: Path, names: list[str] | None = None) -> dict[str, Any]:
    """Put the named Trash/ entries (all of them when None) back on the project's roster. `ok(restored, refused)`:
    per job its entry, instance id (and the one it had, when that was taken), folder, where its settings came from
    and the inputs that named a job now gone; per entry left in Trash/ why. Saves the project."""
    project_dir = Path(project_path)
    state: ProjectState = backend.state_service.state_for(project_dir)
    if names is None:
        names = await asyncio.to_thread(_trash_entries, project_dir)
    if not names:
        return err(f"Trash/ of {project_dir.name} holds no job folder.")
    found = await asyncio.to_thread(find_job_settings, project_dir, list(names))

    refused: list[dict[str, str]] = []
    live = {jm.relion_job_name.rstrip("/"): iid for iid, jm in state.jobs.items() if jm.relion_job_name}
    settings: dict[str, JobSettings] = {}
    for name in sorted(found, key=_job_number):
        s = found[name]
        if isinstance(s, str):
            refused.append({"entry": name, "why": s})
        elif job_dir_of(name) in live:
            refused.append({"entry": name, "why": f"{job_dir_of(name)} is on the roster as {live[job_dir_of(name)]}"})
        else:
            settings[name] = s

    # Instance ids: a job keeps its own while nothing else holds it, the rest take the next free ones.
    taken = set(state.jobs) | set(state.pipeline_order)
    new_iid: dict[str, str] = {}
    for name, s in settings.items():
        if s.instance_id not in taken:
            new_iid[name] = s.instance_id
            taken.add(s.instance_id)
    param_classes = jobtype_paramclass()
    types: dict[str, JobType] = {}
    for name, s in list(settings.items()):
        try:
            job_type = JobType.from_string(s.job.get("job_type") or InstanceId.split(s.instance_id)[0])
        except ValueError:
            refused.append({"entry": name, "why": f"its job type {s.job.get('job_type')!r} is not one this code knows"})
            del settings[name]
            new_iid.pop(name, None)
            continue
        if job_type not in param_classes:
            refused.append({"entry": name, "why": f"no parameter model for {job_type.value}"})
            del settings[name]
            new_iid.pop(name, None)
            continue
        types[name] = job_type
        if name not in new_iid:
            new_iid[name] = next_instance_id(job_type, sorted(taken), [])
            taken.add(new_iid[name])

    # The folders' jobs today: the roster's and the ones coming back.
    by_dir = dict(live) | {job_dir_of(n): new_iid[n] for n in settings}
    project = str(project_dir).rstrip("/")
    restored: list[dict[str, Any]] = []
    for name, s in settings.items():
        rename = {old: by_dir.get(folder) for old, folder in s.instances.items()}
        job = copy.deepcopy(s.job)
        dropped = _rename_references(job, project, rename)
        try:
            params, repairs = ProjectState._build_job_params(param_classes[types[name]], job)
        except Exception as e:
            logger.exception("Restore: %s's settings do not load", name)
            refused.append({"entry": name, "why": f"its settings do not load: {e}"})
            continue
        try:
            await asyncio.to_thread(move_back, project_dir, name)
        except OSError as e:
            logger.exception("Restore: could not move Trash/%s back", name)
            refused.append({"entry": name, "why": f"its folder could not be moved back: {e}"})
            continue
        iid = new_iid[name]
        params._project_state = state
        state.jobs[iid] = params
        if s.path_mapping:
            state.job_path_mapping[iid] = s.path_mapping
        if iid not in state.pipeline_order:
            state.pipeline_order.append(iid)
        restored.append(
            {
                "entry": name,
                "instance_id": iid,
                **({"was": s.instance_id} if iid != s.instance_id else {}),
                "job_dir": job_dir_of(name),
                "from": s.source,
                **({"inputs_dropped": dropped} if dropped else {}),
                **({"repairs": repairs} if repairs else {}),
            }
        )
    if restored:
        state.mark_dirty()
        await backend.state_service.save_project(project_path=project_dir, force=True)
        await sync_job_links(project_dir, state)
    if not restored:
        return err("Nothing restored: " + "; ".join(f"{r['entry']}: {r['why']}" for r in refused), refused=refused)
    return ok(restored=restored, refused=refused)
