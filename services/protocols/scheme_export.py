"""Derive a vanilla-RELION `Schemes/<name>/` from a protocol (FEATURE_recipes.md §6,
fidelity tier 1; roadmap 14 S1).

The scheme is the same artifact crboost's own schemer path materializes at deploy:
`scheme.star` (linear edges, WAIT/EXIT operators) plus one RELION-compatible `job.star` per
stage whose `fn_exe` launches the crboost driver for that instance. It is therefore
runnable with `relion_schemer` in a project the protocol was APPLIED to — the drivers read
their parameters from that project's `project_params.json` by instance id — and it is
site-flavoured by construction (absolute server dir; container wrapping happens inside the
drivers). Pure-native RELION job.stars for the reconstruct/class3d stages and the v1
wrapper-executable form are deferred. One-way: there is no scheme import.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from services.configs.starfile_service import StarfileService
from services.jobs.spec import JOB_SPEC_BY_TYPE, driver_invocation
from services.models_base import JobCategory, JobType
from services.path_resolution_service import PathResolutionError, PathResolutionService, get_context_paths
from services.project_state import ProjectState
from services.protocols.schema import Protocol
from services.result import err, ok
from services.scheduling_and_orchestration.pipeline_orchestrator_service import write_scheme_star

logger = logging.getLogger(__name__)

SERVER_DIR = Path(__file__).resolve().parent.parent.parent


def export_relion_scheme(
    protocol: Protocol, *, project_dir: Path | str, scheme_name: str | None = None
) -> dict[str, Any]:
    """Write `<project_dir>/Schemes/<scheme_name>/` for the protocol's stages as they exist
    in that project. Works on a DETACHED copy of the project state: nothing is registered,
    nothing is saved — the scheme is the only output. Job directories are predicted the way
    the schemer path predicts them (existing `relion_job_name` kept, fresh stages numbered
    after the highest known job)."""
    project_dir = Path(project_dir).expanduser().resolve()
    params_file = project_dir / "project_params.json"
    if not params_file.exists():
        return err(f"{project_dir} has no project_params.json — apply the protocol to it first")
    state = ProjectState.load(params_file)
    state.project_path = project_dir

    ids = protocol.stage_ids()
    missing = [iid for iid in ids if iid not in state.jobs]
    if missing:
        return err(f"project lacks protocol stage(s) {missing} — apply the protocol to this project first")

    scheme_name = scheme_name or protocol.name
    scheme_dir = project_dir / "Schemes" / scheme_name
    scheme_dir.mkdir(parents=True, exist_ok=True)
    star = StarfileService()
    resolver = PathResolutionService(state, active_instance_ids=set(ids))
    known = [jm.relion_job_number for jm in state.jobs.values() if jm.relion_job_number]
    next_num = max([state.job_dir_counter, *known, 0]) + (0 if state.job_dir_counter > max(known, default=0) else 1)
    next_num = max(next_num, 1)

    for iid in ids:
        jm = state.jobs[iid]
        jt = jm.job_type
        category = JobCategory.IMPORT if jt == JobType.IMPORT_MOVIES else JobCategory.EXTERNAL
        rel = (jm.relion_job_name or "").rstrip("/")
        if not rel:
            rel = f"{category.value}/job{next_num:03d}"
            next_num += 1
        job_dir = project_dir / rel
        try:
            io_paths = resolver.resolve_all_paths(jt, jm, job_dir, instance_id=iid)
        except PathResolutionError as e:
            return err(f"{iid}: {e}")
        jm.paths = {k: str(v) for k, v in {**get_context_paths(jt, jm, job_dir), **io_paths}.items() if v is not None}
        state.job_path_mapping[iid] = rel
        resolver.invalidate_cache()
        spec = JOB_SPEC_BY_TYPE[jt]
        fn_exe = (
            driver_invocation(
                server_dir=SERVER_DIR,
                driver_script=SERVER_DIR / "drivers" / spec.driver,
                instance_id=iid,
                project_path=project_dir,
            )
            if spec.driver
            else "true  # crboost import job: relion.importtomo runs RELION's native importer"
        )
        jm.generate_job_star(job_dir=scheme_dir / iid, fn_exe=fn_exe, star_handler=star)

    write_scheme_star(star, scheme_dir, scheme_name, ids)
    logger.info("Exported RELION scheme %s (%d jobs)", scheme_dir, len(ids))
    return ok(
        scheme_dir=str(scheme_dir),
        jobs=ids,
        command=f"cd {project_dir} && relion_schemer --scheme {scheme_name} --run --verb 2",
    )
