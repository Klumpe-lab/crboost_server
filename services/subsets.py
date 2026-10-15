"""The Subset job outside the resolver: what a Subset is ready to cut, checked before a Run
allocates anything, and the star it writes at submit.

A Subset's star is the global block of the star it cuts, restricted to the chosen tilt series.
The per-tilt-series stars it names stay the cut job's own files, by absolute path, so the jobs
after it read that job's results, and Warp export still finds the tsCtf job behind a tomogram.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from services.configs.starfile_service import StarfileService
from services.io_slots import JobFileType
from services.jobs.spec import JOB_SPECS, display_name
from services.models_base import JobStatus, JobType
from services.path_resolution_service import cut_output, override_instance_id, subset_cut

if TYPE_CHECKING:
    from services.path_resolution_service import PathResolutionService
    from services.project_state import ProjectState

logger = logging.getLogger(__name__)

GLOBAL_BLOCK = "global"
NAME_COL = "rlnTomoName"


def is_path_column(column: str) -> bool:
    """A column naming a file or folder: RELION's `...File` / `...FileName` / `...Dir` columns and the volumes."""
    return column.endswith(("File", "FileName", "Dir")) or column.startswith("rlnTomoReconstructedTomogram")


def _global_block(data: dict[str, Any], star: Path) -> pd.DataFrame:
    block = data.get(GLOBAL_BLOCK)
    if isinstance(block, dict):  # one row, written without loop_
        block = pd.DataFrame([block])
    if not isinstance(block, pd.DataFrame) or NAME_COL not in block.columns:
        raise ValueError(f"{star} has no '{GLOBAL_BLOCK}' block with {NAME_COL}")
    return block


def cut_names(star: Path) -> list[str]:
    """The tilt series a per-tilt-series star lists, in its row order."""
    return [str(n) for n in _global_block(StarfileService().read(star), star)[NAME_COL]]


def jobs_after(cut_type: JobFileType) -> list[JobType]:
    """The job types that can follow a Subset of `cut_type`: those whose tilt series come from an
    input accepting it (`JobSpec.items_from`), in pipeline order."""
    out: list[JobType] = []
    for spec in JOB_SPECS:
        if spec.items_from is None:
            continue
        slots = getattr(spec.param_class, "INPUT_SCHEMA", None) or []
        slot = next((s for s in slots if s.key == spec.items_from), None)
        if slot is not None and cut_type in slot.accepts:
            out.append(spec.job_type)
    return out


def add_refusal(state: ProjectState, job_type: JobType, after: str | None, clone_of: str | None) -> str | None:
    """Why a job of `job_type` cannot be added after `after`, or None when it can. A Subset needs
    the afterok orchestrator and is either cloned (`clone_of`) or made after a finished job that
    has an output to cut; a job after a Subset must take its tilt series from what that cuts."""
    if job_type == JobType.SUBSET:
        if not state.use_afterok_orchestrator:
            return "A Subset runs under the afterok orchestrator only (use_afterok_orchestrator in conf.yaml)."
        if clone_of is not None:
            return None
        source = state.jobs.get(after) if after else None
        if source is None:
            return "A Subset is made with the Subset button in the tab of the finished job whose output it cuts."
        if source.job_type == JobType.SUBSET:
            return f"{after} is a Subset already: clone it and untick the tilt series to leave out."
        if cut_output(source.job_type) is None:
            return f"{display_name(source.job_type)} has no output a Subset can cut."
        if source.execution_status != JobStatus.SUCCEEDED:
            return f"{after} has not finished; a Subset cuts a finished output."
        return None
    if after is None:
        return None
    subset = state.jobs.get(after)
    cut = subset_cut(state, subset) if subset is not None and subset.job_type == JobType.SUBSET else None
    if cut is None:
        return f"{after} is not a Subset with an output to cut."
    if job_type not in jobs_after(cut[1].produces):
        return f"{display_name(job_type)} does not take its tilt series from what {after} cuts."
    return None


def subset_problem(state: ProjectState, resolver: PathResolutionService, instance_id: str) -> str | None:
    """Why the Subset `instance_id` cannot be written now, or None: no tilt series chosen, nothing
    to cut, a cut that has not finished or whose star is missing, or chosen tilt series that star
    does not list."""
    job_model = state.jobs[instance_id]
    if not job_model.ts_names:
        return f"{instance_id}: no tilt series chosen"
    cut = subset_cut(state, job_model)
    if cut is None:
        key = (job_model.source_overrides or {}).get("input_star")
        named = override_instance_id(key) if key else None
        if named is None:
            return f"{instance_id} cuts nothing (a Subset is made with the Subset button of a finished job)"
        if named not in state.jobs:
            return f"{instance_id}: {named}, the job it cuts, is no longer in the project"
        return f"{instance_id}: {named} has no output a Subset can cut"
    source_id = cut[0]
    status = state.jobs[source_id].execution_status
    if status != JobStatus.SUCCEEDED:
        return f"{instance_id}: {source_id}, the job it cuts, has not finished ({status.value.lower()})"
    check = resolver.validate_input_slot(
        JobType.SUBSET, job_model, "input_star", check_filesystem=False, consumer_instance_id=instance_id
    )
    if not check.resolved_path:
        return f"{instance_id}: {check.error_message or 'the star it cuts does not resolve'}"
    star = Path(check.resolved_path)
    if not star.is_file():
        return f"{instance_id}: the star it cuts is missing: {star}"
    try:
        listed = set(cut_names(star))
    except (OSError, ValueError) as e:
        logger.exception("Subset %s: cannot read %s", instance_id, star)
        return f"{instance_id}: cannot read the star it cuts ({star}): {e}"
    missing = [n for n in job_model.ts_names if n not in listed]
    if missing:
        shown = ", ".join(missing[:5]) + (f" (+{len(missing) - 5} more)" if len(missing) > 5 else "")
        return f"{instance_id}: {len(missing)} chosen tilt series are not in {source_id}'s {star.name}: {shown}"
    return None


def _absolute(value: Any, star_dir: Path, project_root: Path, column: str) -> Any:
    """`value` as an absolute path: as it is when absolute, else against the star's folder, else
    against the project root, whichever holds it. An empty cell stays as it is."""
    if pd.isna(value):
        return value
    text = str(value)
    if not text or text == "None" or Path(text).is_absolute():
        return value
    for base in (star_dir, project_root):
        if (base / text).exists():
            return str((base / text).resolve())
    raise FileNotFoundError(f"{column} {text}: neither in {star_dir} nor under the project root {project_root}")


def write_subset_star(src: Path, dst: Path, names: list[str], project_root: Path) -> int:
    """Write `dst`: `src` with its global block restricted to the rows of `names` (in `src`'s order)
    and every path column made absolute, its other blocks as they are. Raises when `src` lacks one
    of `names` or a relative path is found neither next to `src` nor under the project root.
    Returns the number of rows written."""
    data = StarfileService().read(src)
    block = _global_block(data, src)
    rows = block[block[NAME_COL].astype(str).isin(set(names))].copy()
    absent = set(names) - set(rows[NAME_COL].astype(str))
    if absent:
        raise ValueError(f"{src} has no row for {sorted(absent)}")
    for column in [c for c in rows.columns if is_path_column(c)]:
        rows[column] = [_absolute(v, src.parent, project_root, column) for v in rows[column]]
    dst.parent.mkdir(parents=True, exist_ok=True)
    StarfileService().write({**data, GLOBAL_BLOCK: rows.reset_index(drop=True)}, dst)
    return len(rows)
