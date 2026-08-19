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
from datetime import datetime
from pathlib import Path

from services.models_base import InstanceId
from services.project_state import ParticleTemplate, TemplateMask, get_project_state_for, sidecar_ensure
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


# ══════════════════════════════════════════════════════════════════════════════
# Template / mask registration (picking-UI roadmap 01 S1)
#
# Lifted verbatim from `TemplateWorkbench._append_template` / `._append_mask` /
# `._select_template` / `._select_mask` so registering a template no longer requires
# a mounted 2111-line workbench — the species creation dialog binds a template and a
# mask up front through exactly these calls. None of them save: they mutate through
# `ProjectState.mutate_species` (marks dirty + bumps the registry rev) and return, so
# the caller keeps owning persistence (the workbench saves + refreshes per register,
# the creation dialog force-saves once at the end).
# ══════════════════════════════════════════════════════════════════════════════


def register_template(
    state,
    species_id: str,
    template_path: str,
    *,
    polarity: str,
    source: str,
    lowpass: float | None = None,
    imported_from: str | None = None,
    notes: str = "",
) -> dict:
    """Append a `ParticleTemplate` to the species, or replace in place when a registered
    entry already sits at this path (its id is preserved so dropdowns stay stable).
    Auto-selects only when it is the first entry. `ok(template_id=...)`."""
    new_id = sidecar_ensure(template_path, "template")
    tpl = ParticleTemplate(
        id=new_id,
        template_path=template_path,
        polarity=polarity if polarity in ("white", "black") else "black",
        lowpass_resolution_ang=lowpass,
        source=source,
        imported_from=imported_from,
        created_at=datetime.now(),
        notes=notes,
    )

    def _apply(sp) -> None:
        existing_idx = next((i for i, t in enumerate(sp.templates) if t.template_path == template_path), None)
        if existing_idx is not None:
            # Preserve the existing id so dropdowns elsewhere stay stable.
            tpl.id = sp.templates[existing_idx].id
            sp.templates[existing_idx] = tpl
        else:
            sp.templates.append(tpl)
        if not sp.selected_template_id:
            sp.selected_template_id = tpl.id

    if not state.mutate_species(species_id, _apply):
        return err(f"Unknown species '{species_id}'")
    return ok(template_id=tpl.id)


def register_mask(state, species_id: str, mask: TemplateMask) -> dict:
    """Append a `TemplateMask`, or replace in place at the same path. Auto-selects only
    when it is the first entry. `ok(mask_id=...)`."""
    mid = sidecar_ensure(mask.mask_path, "mask")
    mask = mask.model_copy(update={"id": mid, "created_at": datetime.now()})

    def _apply(sp) -> None:
        existing_idx = next((i for i, m in enumerate(sp.masks) if m.mask_path == mask.mask_path), None)
        if existing_idx is not None:
            mask.id = sp.masks[existing_idx].id
            sp.masks[existing_idx] = mask
        else:
            sp.masks.append(mask)
        if not sp.selected_mask_id:
            sp.selected_mask_id = mask.id

    if not state.mutate_species(species_id, _apply):
        return err(f"Unknown species '{species_id}'")
    return ok(mask_id=mask.id)


def select_template(state, species_id: str, template_id: str) -> dict:
    """Make `template_id` the species' current template. Unknown ids are a no-op on the
    model (as before) and reported here."""
    found = [False]

    def _apply(sp) -> None:
        if any(t.id == template_id for t in sp.templates):
            sp.selected_template_id = template_id
            found[0] = True

    if not state.mutate_species(species_id, _apply):
        return err(f"Unknown species '{species_id}'")
    if not found[0]:
        return err(f"No template '{template_id}' registered to '{species_id}'")
    return ok(template_id=template_id)


def select_mask(state, species_id: str, mask_id: str) -> dict:
    """Make `mask_id` the species' current mask."""
    found = [False]

    def _apply(sp) -> None:
        if any(m.id == mask_id for m in sp.masks):
            sp.selected_mask_id = mask_id
            found[0] = True

    if not state.mutate_species(species_id, _apply):
        return err(f"Unknown species '{species_id}'")
    if not found[0]:
        return err(f"No mask '{mask_id}' registered to '{species_id}'")
    return ok(mask_id=mask_id)
