# ui/components/recon_compare.py
"""Every reconstruction of one tomogram across the project's TS Reconstruct jobs.

A project can hold several reconstruct jobs of the same tilt-series (e.g. one from
aligntiltsWarp and one from a missAlign refinement). Different alignments put the
specimen at different heights in the volume, so each job's own central slice (the
WarpTools PNG) cuts a different plane. The comparison instead shows every version's
X/Y slab on the first version's plane, found by registering the volumes
(`services/visualization/recon_register.py`). The lineage string names the alignment
each job traces back to (reconstruct <- tsCtf <- alignment/refinement), read from the
paths the jobs actually ran with.
"""

import json
import re
from pathlib import Path

from services.models_base import JobType
from services.visualization.preview_render import is_output_stale
from services.visualization.recon_register import render_matched_slabs
from services.workers import WorkKind, run_in_worker
from ui.background_task import BackgroundTask

# Failed plane-matching renders, keyed by reference dir + TS + the version set. What is
# in flight is whatever the task registry still runs under the same key.
_MATCH_ERRORS: dict[str, str] = {}


def _instance_for_dir(state, job_dir: Path) -> str | None:
    """Instance whose RELION job dir is `job_dir`."""
    project = Path(state.project_path)
    for iid, jm in state.jobs.items():
        rel = (jm.relion_job_name or "").rstrip("/")
        if rel and (project / rel).resolve() == job_dir.resolve():
            return iid
    return None


def lineage(state, recon_iid: str) -> str:
    """'tsCtf__2 (External/job008) ← missAlign (External/job007)' for a reconstruct instance."""
    parts: list[str] = []
    iid = recon_iid
    for _ in range(2):  # reconstruct -> ctf -> alignment/refinement
        src = (state.jobs[iid].paths or {}).get("input_processing")
        if not src:
            break
        parent = _instance_for_dir(state, Path(src).parent)
        if parent is None:
            parts.append(str(Path(src).parent))
            break
        parts.append(f"{parent} ({(state.jobs[parent].relion_job_name or '').rstrip('/')})")
        iid = parent
    return " ← ".join(parts)


def _alignment_of(state, recon_iid: str) -> str:
    """The alignment or refinement instance a reconstruct job traces back to (reconstruct <- ctf <-
    alignment), or the reconstruct instance itself when its inputs do not lead to known jobs."""
    iid = recon_iid
    for _ in range(2):
        src = (state.jobs[iid].paths or {}).get("input_processing")
        parent = _instance_for_dir(state, Path(src).parent) if src else None
        if parent is None:
            break
        iid = parent
    return iid


def recon_version_labels(state) -> dict[str, str]:
    """{reconstruct instance: short name} for every reconstruct job in the project: the alignment it
    traces back to (aligntiltsWarp, missAlign), with the reconstruct instance added when two jobs
    trace back to the same one. Project-wide, so a label means the same job on every tomogram."""
    recon = [iid for iid, jm in state.jobs.items() if jm.job_type == JobType.TS_RECONSTRUCT and jm.relion_job_name]
    base = {iid: _alignment_of(state, iid) for iid in recon}
    counts: dict[str, int] = {}
    for name in base.values():
        counts[name] = counts.get(name, 0) + 1
    return {iid: name if counts[name] == 1 or name == iid else f"{name} ({iid})" for iid, name in base.items()}


def recon_versions_for_ts(state, ts_name: str) -> list[dict]:
    """[{iid, job, dir, lineage, label, mrc}] for every reconstruct job with a volume of `ts_name`,
    ordered by job dir."""
    project = Path(state.project_path)
    name = re.compile(rf"{re.escape(ts_name)}_[\d.]+Apx\.mrc")
    labels = recon_version_labels(state)
    out = []
    for iid, jm in state.jobs.items():
        if jm.job_type != JobType.TS_RECONSTRUCT or not jm.relion_job_name:
            continue
        job_dir = project / jm.relion_job_name.rstrip("/")
        rec_dir = job_dir / "warp_tiltseries" / "reconstruction"
        mrcs = sorted(p for p in rec_dir.glob(f"{ts_name}_*Apx.mrc") if name.fullmatch(p.name))
        if mrcs:
            job = jm.relion_job_name.rstrip("/")
            out.append(
                {
                    "iid": iid,
                    "job": job,
                    "dir": job_dir,
                    "lineage": lineage(state, iid),
                    "label": labels.get(iid, iid),
                    "mrc": mrcs[0],
                }
            )
    out.sort(key=lambda v: v["job"])
    return out


def matched_slabs(versions: list[dict], ts_name: str, project_path: Path, refresh) -> tuple[str, str | None]:
    """Give each version its X/Y slab on the first version's plane (`slab` PNG and, once
    rendered, `plane` = the JSON the renderer wrote), rendering the set in the background
    when any is missing or older than its volumes. Returns the pick viewer's slab state
    (``fresh`` / ``failed`` / ``in-flight`` / ``kicked``; spin only on the last two) and,
    when failed, why."""
    ref = versions[0]
    tag = ref["job"].replace("/", "_")
    for v in versions:
        stem = f"{ts_name}__ref" if v is ref else f"{ts_name}__on_{tag}"
        base = v["dir"] / "vis" / "compare"
        v["slab"], v["plane_json"] = base / f"{stem}.png", base / f"{stem}.json"

    if all(not is_output_stale(p, [ref["mrc"], v["mrc"]]) for v in versions for p in (v["slab"], v["plane_json"])):
        for v in versions:
            v["plane"] = json.loads(v["plane_json"].read_text())
        return "fresh", None

    key = _match_key(versions, ts_name)
    if key in _MATCH_ERRORS:
        return "failed", _MATCH_ERRORS[key]
    dedup = f"recon-match:{key}"
    if BackgroundTask.existing(dedup) is not None:
        return "in-flight", None

    async def _run(progress_cb):
        progress_cb(0, 0, "registering reconstructions…")
        await run_in_worker(
            WorkKind.RENDER,
            render_matched_slabs,
            (ref["mrc"], ref["slab"], ref["plane_json"]),
            [(v["mrc"], v["slab"], v["plane_json"]) for v in versions[1:]],
        )
        return "matched slabs rendered"

    def _done(task) -> None:
        if task.status != "succeeded":
            _MATCH_ERRORS[key] = str(task.error or task.status)
        refresh()

    BackgroundTask(
        title=f"Match recon planes · {ts_name}",
        subtitle=" vs ".join(v["job"] for v in versions),
        project_path=str(project_path),
        dedup_key=dedup,
    ).submit(_run, on_complete=_done, show_start_toast=False)
    return "kicked", None


def _match_key(versions: list[dict], ts_name: str) -> str:
    return f"{versions[0]['dir']}:{ts_name}:" + ",".join(v["job"] for v in versions)


def retry_match(versions: list[dict], ts_name: str) -> None:
    """Forget a failed render so the next build kicks it again."""
    _MATCH_ERRORS.pop(_match_key(versions, ts_name), None)
