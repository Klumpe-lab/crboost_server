"""Species administration (roadmap 10 S3) — the delete cascade, headless.

Moved out of `TemplateWorkbench._do_delete_species` so the Species page's Overview
tab can delete a species without a mounted workbench. Order matters: bound pipeline
jobs first (`backend.delete_job` reads the job model for its directory, so the state
must still describe them), then template / mask files (+ `.meta.json` sidecars), then
the empty-only `templates/<sid>` rmdir, then `ProjectState.remove_species` (registry +
pick lists + authoritative choices + resolver overrides; marks dirty + bumps the rev)
and a forced save. Failures are collected into the result, never fatal — a
half-deleted species is still better than one whose registry entry survives while
its jobs are gone.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from services.models_base import InstanceId
from services.project_state import get_project_state_for
from services.result import err, ok

logger = logging.getLogger(__name__)


def delete_file_with_sidecar(file_path: str) -> str | None:
    """Remove a registered template / mask file and its `.meta.json` sidecar. A missing
    file is not an error. Returns the human-readable error text when something is left
    behind, else None."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except OSError as e:
        logger.warning("Could not remove %s: %s", file_path, e)
        return f"Could not remove {os.path.basename(file_path)}: {e}"
    sidecar = Path(file_path).with_name(Path(file_path).name + ".meta.json")
    try:
        if sidecar.exists():
            sidecar.unlink()
    except OSError as e:
        logger.warning("Could not remove sidecar %s: %s", sidecar, e)
        return f"Could not remove sidecar {sidecar.name}: {e}"
    return None


async def delete_species(backend, project_path: Path, species_id: str) -> dict:
    """The cascade. `ok(deleted_jobs=[iid, ...], deleted_files=n, errors=[text, ...])`
    (errors non-empty = partial success, the registry entry IS gone); `err(...)` only
    when the species is unknown."""
    project_path = Path(project_path)
    state = get_project_state_for(project_path)
    sp = state.get_species(species_id)
    if sp is None:
        return err(f"Unknown species '{species_id}'")

    errors: list[str] = []
    deleted_jobs: list[str] = []
    for iid in state.species_references(species_id)["jobs"]:
        job_name = InstanceId.split(iid)[0]
        result = await backend.delete_job(job_name, project_path, instance_id=iid)
        if result.get("success"):
            deleted_jobs.append(iid)
        else:
            logger.warning("Deleting job %s with species %s failed: %s", iid, species_id, result.get("error"))
            errors.append(f"job {iid}: {result.get('error')}")

    deleted_files = 0
    for path in [t.template_path for t in sp.templates] + [m.mask_path for m in sp.masks]:
        problem = delete_file_with_sidecar(path)
        if problem is None:
            deleted_files += 1
        else:
            errors.append(problem)

    # rmdir only succeeds when empty — leftover files (manually dropped MRCs, RELION
    # run logs) keep the folder around on purpose; the user can rm -rf later.
    folder = project_path / "templates" / species_id
    try:
        os.rmdir(folder)
    except FileNotFoundError:
        pass  # a de-novo species that never mounted the workbench has no folder
    except OSError:
        logger.info("Species folder %s not empty after cascade; left in place", folder)

    state.remove_species(species_id)
    # Awaited, not fire-and-forget: a create_task here could be GC'd before it runs and
    # leave the deleted species back on disk after a reload.
    await backend.save_project(project_path, force=True)
    return ok(deleted_jobs=deleted_jobs, deleted_files=deleted_files, errors=errors)
