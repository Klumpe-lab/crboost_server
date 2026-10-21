"""Which job a job's inputs come from, read from the paths it was given.

Pure functions of the project state, for the UI (which reconstruction a pick layer or a
segmentation sits on) and for the drivers (which reconstruction a job's output traces to).
"""

from pathlib import Path

from services.models_base import JobType


def job_dirs(state) -> dict[Path, str]:
    """{job dir: instance} for every job with a RELION job dir, by plain path (no resolve, so no
    stat per job on NFS); the job paths it is matched against are written the same way."""
    project = Path(state.project_path)
    return {project / jm.relion_job_name.rstrip("/"): iid for iid, jm in state.jobs.items() if jm.relion_job_name}


def recon_of(state, iid: str, dirs: dict[Path, str] | None = None) -> str | None:
    """The reconstruct instance whose tomograms `iid` works on, followed back through the job
    dirs its input paths point into (candidate extraction <- template matching <- reconstruct;
    denoise <- reconstruct); `iid` itself when it is one. None when no input leads to one."""
    dirs = job_dirs(state) if dirs is None else dirs
    queue, seen = [iid], set()
    while queue:
        cur = queue.pop(0)
        if cur in seen or cur not in state.jobs:
            continue
        seen.add(cur)
        jm = state.jobs[cur]
        if jm.job_type == JobType.TS_RECONSTRUCT:
            return cur
        for value in (jm.paths or {}).values():
            if not isinstance(value, str) or not value.startswith("/"):
                continue
            path = Path(value)
            parent = next((dirs[p] for p in (path, *path.parents) if p in dirs), None)
            if parent is not None:
                queue.append(parent)
    return None
