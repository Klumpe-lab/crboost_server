"""
Journey — per-tilt-series dashboard.

The dialog is anchored on a tilt-series. The sidebar lists every TS in the
project (union across all array-job manifests) with a 6-pill journey strip
showing pipeline-stage status. Selecting a TS loads a stack of section cards
in the main pane — one per pipeline stage that has data for the selected TS.

Pill states per stage:
  ok        — stage produced expected output for this TS
  fail      — stage errored on this TS
  running   — stage is in flight (job RUNNING/QUEUED)
  zero      — stage processed this TS but produced no output (e.g. PyTOM
              returned zero picks above cutoff). Distinguishable from
              "pending" so users can tell "ran but yielded nothing" from
              "never ran".
  pending   — stage hasn't reached this TS yet

The surface used to be called "Tomogram Dashboard"; it's been renamed
"Journey" because it carries per-TS analytics across the whole pipeline,
not just the candidate-extract preview pair.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from nicegui import ui

from services.models_base import JobStatus, JobType
from services.project_state import get_project_state
from services.templating.template_metadata import (
    get_effective_template_path,
    read_template_header,
)
from services.tilt_series.build import _infer_position
from services.visualization.imod_vis import generate_candidate_vis
from services.visualization.preview_orchestrator import (
    _find_warp_tomo_preview,
    generate_candidate_previews,
    read_preview_manifest,
)
from ui.components.task_utils import read_manifest, resolve_job_dir, scan_statuses

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _candidate_extract_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.TEMPLATE_EXTRACT_PYTOM:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def _subtomo_extract_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.SUBTOMO_EXTRACTION:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def _job_dir_for(instance_id: str, job_model, project_path: Path) -> Optional[Path]:
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        d = project_path / rjn.rstrip("/")
        if d.is_dir():
            return d
    state = get_project_state()
    mapped = (state.job_path_mapping or {}).get(instance_id)
    if mapped:
        d = project_path / mapped.rstrip("/")
        if d.is_dir():
            return d
    return None


def _read_tomograms_table(tomograms_star: Path) -> Optional[pd.DataFrame]:
    if not tomograms_star.exists():
        return None
    try:
        import starfile

        data = starfile.read(tomograms_star, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                return v
    except Exception as e:
        logger.warning("Could not read %s: %s", tomograms_star, e)
    return None


def _resolve_volume_for_3dmod(tomo_row: pd.Series, project_path: Path) -> Optional[Path]:
    if "rlnTomoReconstructedTomogram" not in tomo_row.index:
        return None
    p = Path(str(tomo_row["rlnTomoReconstructedTomogram"]))
    if not p.is_absolute():
        p = project_path / p
    f32 = p.with_name(p.stem + "_f32.mrc")
    if f32.exists():
        return f32
    if p.exists():
        return p
    return None


def _vis_asset_url(asset_path: str) -> str:
    # mtime-keyed cache-buster — see ROADMAP §4.7. When the atlas/manifest
    # regenerates, the URL changes, so the browser doesn't keep serving a
    # stale copy from disk cache against an unchanged path.
    try:
        v = int(Path(asset_path).stat().st_mtime)
    except OSError:
        v = 0
    return f"/api/vis-asset?path={urllib.parse.quote(asset_path, safe='')}&v={v}"


def _position_label(tomo_name: str) -> tuple[str, tuple[int, int]]:
    stage, beam = _infer_position(tomo_name)
    if stage == 0:
        return tomo_name.rsplit("_", 1)[-1], (stage, beam)
    return f"Pos {stage} · Beam {beam}", (stage, beam)


def has_any_extract_jobs() -> bool:
    state = get_project_state()
    return any(_candidate_extract_instances(state))


def has_any_previews_rendered() -> bool:
    state = get_project_state()
    if state.project_path is None:
        return False
    for instance_id, job_model in _candidate_extract_instances(state):
        job_dir = _job_dir_for(instance_id, job_model, state.project_path)
        if not job_dir:
            continue
        if (job_dir / "vis" / "preview" / "manifest.json").exists():
            return True
    return False


def has_any_dashboard_data() -> bool:
    """True when at least one array job has emitted a task manifest, i.e.
    the dashboard has any TS data to populate the sidebar with."""
    state = get_project_state()
    if state.project_path is None:
        return False
    project_path = Path(state.project_path)
    for jt in (JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT, JobType.TS_CTF, JobType.TS_RECONSTRUCT):
        for iid, jm in state.jobs.items():
            if getattr(jm, "job_type", None) != jt and iid.split("__")[0] != jt.value:
                continue
            jd = resolve_job_dir(jm, project_path)
            if jd and (jd / ".task_manifest.json").exists():
                return True
    return False


# ---------------------------------------------------------------------------
# Per-TS journey collector — feeds the 6-pill sidebar strip
# ---------------------------------------------------------------------------


# (key, label, JobType for array stages, or None for synthetic stages handled below).
_PILL_STAGES: list[tuple[str, str, Optional[JobType]]] = [
    ("fs_ctf", "FS/CTF", JobType.FS_MOTION_CTF),
    ("align", "Align", JobType.TS_ALIGNMENT),
    ("ctf", "CTF", JobType.TS_CTF),
    ("recon", "Recon", JobType.TS_RECONSTRUCT),
    ("pick", "Pick", None),
    ("subtomo", "Subtomo", None),
]


# Legacy-job fallback: when `.task_manifest.json` is absent, derive the TS
# list from the stage's primary output star (which lists every TS the job
# touched) and apply a coarse job-level status to all of them. Lets pre-
# array-tracker projects show real "ok" pills instead of being stuck on
# "pending" for stages that actually finished.
_ARRAY_STAGE_OUTPUT_STAR: dict[JobType, str] = {
    JobType.FS_MOTION_CTF: "fs_motion_and_ctf.star",
    JobType.TS_ALIGNMENT: "aligned_tilt_series.star",
    JobType.TS_CTF: "ts_ctf_tilt_series.star",
    JobType.TS_RECONSTRUCT: "tomograms.star",
}


def _ts_names_from_star(p: Path) -> list[str]:
    """Return the rlnTomoName column from the first DataFrame in a star file."""
    if not p.exists():
        return []
    try:
        import starfile

        data = starfile.read(p, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                return [str(x) for x in v["rlnTomoName"].tolist()]
    except Exception as e:
        logger.warning("Could not read TS list from %s: %s", p, e)
    return []


def _coarse_job_status(jm) -> str:
    es = getattr(jm, "execution_status", None)
    if es == JobStatus.SUCCEEDED:
        return "ok"
    if es == JobStatus.FAILED:
        return "fail"
    if es in (JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.SCHEDULED):
        return "running"
    return "pending"


def _array_stage_status(project_path: Path, jm) -> tuple[list[str], dict[str, str]]:
    """For an array job, return (ordered TS items, {ts: status_string}).

    Prefers the per-TS array-task tracker (.task_manifest.json + .task_status/)
    when present. Falls back to the stage's output star + job-level execution
    status for legacy jobs that ran before the tracker was wired up.
    """
    job_dir = resolve_job_dir(jm, project_path)
    if job_dir is None:
        return [], {}
    manifest = read_manifest(job_dir)
    if manifest is not None:
        items = manifest.get("items") or []
        if items:
            return list(items), scan_statuses(job_dir, items)

    jt = getattr(jm, "job_type", None)
    primary = _ARRAY_STAGE_OUTPUT_STAR.get(jt)
    candidates: list[Path] = []
    if primary:
        candidates.append(job_dir / primary)
    candidates.append(job_dir / "tomograms.star")
    items: list[str] = []
    for p in candidates:
        items = _ts_names_from_star(p)
        if items:
            break
    if not items:
        return [], {}
    coarse = _coarse_job_status(jm)
    return items, {ts: coarse for ts in items}


def _job_running_or_failed(jm) -> Optional[str]:
    """For non-array jobs, derive a coarse status from execution_status. Returns
    'running' / 'fail' / None (None means "fall back to per-TS data check")."""
    es = getattr(jm, "execution_status", None)
    if es == JobStatus.RUNNING or es == JobStatus.QUEUED or es == JobStatus.SCHEDULED:
        return "running"
    if es == JobStatus.FAILED:
        return "fail"
    return None


def _zero_pick_tomos_from_tmresults(job_dir: Path) -> set[str]:
    """Walk `<job_dir>/tmResults/*_particles.star` and return the set of
    tomograms whose per-TS particles file exists but contains zero data
    rows. This is the on-disk signal that PyTOM ran on that TS and produced
    no candidates above cutoff — the supervisor's `pd.concat` merge silently
    drops these, so they vanish from `candidates.star` and the preview
    manifest. We surface them here so the journey pill can read "zero"
    instead of the misleading "pending".

    Fast: each file is header-only (~600 bytes); a 24-TS project takes a
    few ms. Returns an empty set if `tmResults/` doesn't exist (older
    project layouts).
    """
    tm_dir = job_dir / "tmResults"
    if not tm_dir.is_dir():
        return set()
    out: set[str] = set()
    for p in tm_dir.glob("*_particles.star"):
        try:
            import starfile

            data = starfile.read(p, always_dict=True)
        except Exception:
            continue
        # Find the particles dataframe (first DataFrame in the file).
        df = None
        for v in data.values():
            if isinstance(v, pd.DataFrame):
                df = v
                break
        if df is None or len(df) > 0:
            continue
        # Strip the "_particles" suffix to recover the tomo name.
        stem = p.stem
        if stem.endswith("_particles"):
            tomo_name = stem[: -len("_particles")]
            out.add(tomo_name)
    return out


def _candidate_extract_status_per_ts(job_dir: Path, jm) -> dict[str, str]:
    """Read the candidate-extract job's preview manifest to bucket TS statuses.

    Buckets:
      - "ok": manifest entry has picks_json
      - "fail": tomo listed in summary.errored
      - "zero": tomo was processed but produced 0 picks above cutoff. Fast
        path reads summary.zero_picks if present (manifest v10+); otherwise
        falls back to scanning `tmResults/*_particles.star` for header-only
        files.
      - "running" / "pending": defaults based on job state, applied for any
        TS that's expected (per the staged tomograms.star) but not yet
        covered by any of the buckets above.
    """
    manifest = read_preview_manifest(job_dir) or {}
    entries = manifest.get("tomograms") or {}
    summary = manifest.get("summary") or {}
    errored = {e.get("tomo") for e in (summary.get("errored") or []) if e.get("tomo")}

    # Fast path: orchestrator-recorded zero_picks (v10+). Fallback: scan
    # tmResults for legacy manifests. The scan is cheap (header-only files)
    # so we run it unconditionally on miss to recover from old projects.
    zero_picks: set[str] = set(summary.get("zero_picks") or [])
    if not zero_picks:
        zero_picks = _zero_pick_tomos_from_tmresults(job_dir)

    coarse = _job_running_or_failed(jm)
    out: dict[str, str] = {}

    for tomo_name, entry in entries.items():
        if tomo_name in errored:
            out[tomo_name] = "fail"
        elif entry.get("picks_json"):
            out[tomo_name] = "ok"
        elif coarse == "running":
            out[tomo_name] = "running"
        else:
            out[tomo_name] = "pending"

    # Promote zero-pick tomos. These don't appear in `entries` (the
    # orchestrator only emitted entries for tomos with at least one pick),
    # so they're additive to the dict.
    for tomo_name in zero_picks:
        if tomo_name not in out:
            out[tomo_name] = "zero"

    return out


def _read_subtomo_extracted_ts(job_dir: Path) -> set[str]:
    """Read job_dir/particles.star and return the set of TS that had at
    least one row of extracted particles. Tolerant of missing files /
    parse errors."""
    particles_star = job_dir / "particles.star"
    if not particles_star.exists():
        return set()
    try:
        import starfile

        data = starfile.read(particles_star, always_dict=True)
        df = data.get("particles")
        if df is None:
            for v in data.values():
                if isinstance(v, pd.DataFrame) and "rlnTomoName" in v.columns:
                    df = v
                    break
        if df is None or "rlnTomoName" not in df.columns:
            return set()
        return {str(t) for t in df["rlnTomoName"].astype(str).unique()}
    except Exception as e:
        logger.warning("Could not parse subtomo particles.star %s: %s", particles_star, e)
        return set()


def _subtomo_extract_status_per_ts(
    job_dir: Path, jm, expected_ts: Optional[set[str]] = None
) -> dict[str, str]:
    """Bucket per-TS status for the subtomo-extraction job.

    Two layouts are supported, in priority order:

      1. **Array layout** (post-conversion): `.task_manifest.json` exists.
         Per-TS pass/fail from `.task_status/<ts>.{ok,fail}` is the source
         of truth. An "ok" task that didn't write any row to particles.star
         is demoted to "zero" (extraction ran but produced 0 particles for
         that TS — e.g. all picks filtered by max_dose / min_frames).

      2. **Legacy one-shot layout** (no manifest): we don't have per-TS
         markers. Fall back to particles.star membership crossed with
         `expected_ts` (typically the union of "ok" picks across upstream
         candidate-extract instances). A TS in `expected_ts` but absent
         from particles.star is "zero" iff the job has SUCCEEDED, else
         "running" / "pending" depending on job state.
    """
    from ui.components.task_utils import read_manifest as read_array_manifest
    from ui.components.task_utils import scan_statuses

    extracted = _read_subtomo_extracted_ts(job_dir)
    out: dict[str, str] = {}

    # ── Layout 1: array layout ─────────────────────────────────────────
    array_manifest = read_array_manifest(job_dir)
    if array_manifest is not None:
        items = array_manifest.get("items") or []
        if items:
            statuses = scan_statuses(job_dir, items)
            for ts in items:
                st = statuses.get(ts, "pending")
                if st == "ok" and ts not in extracted:
                    # task completed but the TS isn't in particles.star —
                    # relion_tomo_subtomo ran and produced nothing (all
                    # candidates filtered out at this stage).
                    out[ts] = "zero"
                else:
                    out[ts] = st
            return out

    # ── Layout 2: legacy one-shot ──────────────────────────────────────
    for ts in extracted:
        out[ts] = "ok"

    if expected_ts:
        es = getattr(jm, "execution_status", None)
        job_running = es in (JobStatus.RUNNING, JobStatus.QUEUED, JobStatus.SCHEDULED)
        job_succeeded = es == JobStatus.SUCCEEDED
        for ts in expected_ts:
            if ts in extracted:
                continue
            if job_succeeded:
                out[ts] = "zero"
            elif job_running:
                out[ts] = "running"
            # Else: leave unset; caller defaults to "pending".

    return out


def _collect_dashboard_journey(project_state, project_path: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Collect per-TS status across the 6 dashboard stages.

    Returns:
        journey: {ts_name: {stage_key: status_string}} where status is one of
                 "ok" / "fail" / "running" / "pending".
        ts_names: ordered list of all tilt series in the project (union across
                  array-job manifests). Order follows the first stage that
                  declares a given TS.
    """
    journey: dict[str, dict[str, str]] = {}
    ts_order: list[str] = []
    seen: set[str] = set()

    # Walk the 4 array stages first so ts_order reflects pipeline order.
    for key, _label, jt in _PILL_STAGES:
        if jt is None:
            continue
        for iid, jm in (project_state.jobs or {}).items():
            if getattr(jm, "job_type", None) != jt and iid.split("__")[0] != jt.value:
                continue
            items, statuses = _array_stage_status(project_path, jm)
            if not items:
                continue
            for ts_name in items:
                if ts_name not in seen:
                    ts_order.append(ts_name)
                    seen.add(ts_name)
                journey.setdefault(ts_name, {})[key] = statuses.get(ts_name, "pending")
            break  # one job per array stage

    # Pick stage: combine across all candidate-extract instances. Promotion
    # order keeps "ok" winning over "zero" (multi-species: if one species
    # produced picks here and another didn't, the row is genuinely "ok").
    pick_combined: dict[str, str] = {}
    pick_order = {"ok": 5, "running": 4, "zero": 3, "fail": 2, "pending": 1}
    for iid, jm in _candidate_extract_instances(project_state):
        jd = _job_dir_for(iid, jm, project_path)
        if jd is None:
            continue
        statuses = _candidate_extract_status_per_ts(jd, jm)
        for ts_name, st in statuses.items():
            cur = pick_combined.get(ts_name)
            if cur is None or pick_order.get(st, 0) > pick_order.get(cur, 0):
                pick_combined[ts_name] = st
            if ts_name not in seen:
                ts_order.append(ts_name)
                seen.add(ts_name)
    for ts_name, st in pick_combined.items():
        journey.setdefault(ts_name, {})["pick"] = st

    # Subtomo stage: combine across all subtomo-extract instances. Pass the
    # "ok" pick set as `expected_ts` so the predicate can infer zero-state
    # for TS that should have been extracted but didn't make it into
    # particles.star (e.g. filtered out by max_dose / min_frames).
    picked_ok: set[str] = {ts for ts, st in pick_combined.items() if st == "ok"}
    subtomo_combined: dict[str, str] = {}
    subtomo_order = {"ok": 5, "running": 4, "zero": 3, "fail": 2, "pending": 1}
    for iid, jm in _subtomo_extract_instances(project_state):
        jd = _job_dir_for(iid, jm, project_path)
        if jd is None:
            continue
        statuses = _subtomo_extract_status_per_ts(jd, jm, expected_ts=picked_ok)
        for ts_name, st in statuses.items():
            cur = subtomo_combined.get(ts_name)
            if cur is None or subtomo_order.get(st, 0) > subtomo_order.get(cur, 0):
                subtomo_combined[ts_name] = st
            if ts_name not in seen:
                ts_order.append(ts_name)
                seen.add(ts_name)
    for ts_name, st in subtomo_combined.items():
        journey.setdefault(ts_name, {})["subtomo"] = st

    # Fill missing pills with "pending" so renderers don't have to defend.
    for ts_name in ts_order:
        row = journey.setdefault(ts_name, {})
        for key, _label, _jt in _PILL_STAGES:
            row.setdefault(key, "pending")

    return journey, ts_order


# ---------------------------------------------------------------------------
# Plotly figure builders — used by the picks-only scatter fallback when no
# subtomo cutout atlas exists. ui.plotly() accepts a JSON dict directly, so we
# build dicts rather than depending on the plotly Python package (ROADMAP §4.2).
# ---------------------------------------------------------------------------


def _empty_fig(message: str) -> dict:
    return {
        "data": [],
        "layout": {
            "annotations": [
                {
                    "text": message,
                    "showarrow": False,
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "font": {"color": "#9ca3af", "size": 12},
                }
            ],
            "margin": {"t": 5, "b": 5, "l": 5, "r": 5},
            "paper_bgcolor": "#f8fafc",
            "plot_bgcolor": "#f8fafc",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
        },
        "config": {"displaylogo": False, "responsive": True},
    }


def _build_xy_scatter_fig(picks: list, tomo_dims_xyz: tuple, score_field: Optional[str]) -> dict:
    x_dim, y_dim, _z_dim = tomo_dims_xyz
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    ys = [p["y"] for p in picks]
    custom = [[p["i"], p.get("z", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 6, "line": {"width": 0}, "opacity": 0.85}
    if has_scores:
        marker["color"] = [p.get("score") for p in picks]
        marker["colorscale"] = "Viridis"
        marker["showscale"] = True
        marker["colorbar"] = {
            "title": {"text": score_field or "score", "font": {"size": 9}},
            "thickness": 8,
            "len": 0.7,
            "tickfont": {"size": 9},
            "outlinewidth": 0,
        }
    else:
        marker["color"] = "#fbbf24"

    trace = {
        "type": "scattergl",
        "x": xs,
        "y": ys,
        "mode": "markers",
        "marker": marker,
        "customdata": custom,
        "hovertemplate": (
            "pick #%{customdata[0]}<br>"
            "x=%{x}, y=%{y}, z=%{customdata[1]}"
            + ("<br>score=%{customdata[2]:.4f}" if has_scores else "")
            + "<extra></extra>"
        ),
        "name": "picks",
    }
    layout: dict = {
        "xaxis": {
            "title": {"text": "X (px)", "font": {"size": 10}},
            "range": [0, x_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Y (px)", "font": {"size": 10}},
            "range": [0, y_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#f8fafc",
        "showlegend": False,
        "shapes": [
            {
                "type": "rect",
                "xref": "x",
                "yref": "y",
                "x0": 0,
                "y0": 0,
                "x1": x_dim,
                "y1": y_dim,
                "line": {"color": "#cbd5e1", "width": 0.8, "dash": "dash"},
                "layer": "above",
            }
        ],
    }
    return {"data": [trace], "layout": layout, "config": {"displaylogo": False, "responsive": True}}


def _build_xz_scatter_fig(
    picks: list, tomo_dims_xyz: tuple, score_field: Optional[str], xz_preview_url: Optional[str] = None
) -> dict:
    x_dim, _y_dim, z_dim = tomo_dims_xyz
    has_scores = picks and "score" in picks[0]
    xs = [p["x"] for p in picks]
    zs = [p["z"] for p in picks]
    custom = [[p["i"], p.get("y", 0), p.get("score")] for p in picks]
    marker: dict = {"size": 5, "line": {"width": 0}, "opacity": 0.85}
    if has_scores:
        marker["color"] = [p.get("score") for p in picks]
        marker["colorscale"] = "Viridis"
        marker["showscale"] = False
    else:
        marker["color"] = "#fbbf24"

    trace = {
        "type": "scattergl",
        "x": xs,
        "y": zs,
        "mode": "markers",
        "marker": marker,
        "customdata": custom,
        "hovertemplate": (
            "pick #%{customdata[0]}<br>"
            "x=%{x}, z=%{y}, y=%{customdata[1]}"
            + ("<br>score=%{customdata[2]:.4f}" if has_scores else "")
            + "<extra></extra>"
        ),
        "name": "picks",
    }
    layout: dict = {
        "xaxis": {
            "title": {"text": "X (px)", "font": {"size": 10}},
            "range": [0, x_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "yaxis": {
            "title": {"text": "Z (px)", "font": {"size": 10}},
            "range": [0, z_dim],
            "showgrid": False,
            "zeroline": False,
            "tickfont": {"size": 9},
        },
        "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#0f172a" if xz_preview_url else "white",
        "showlegend": False,
        "shapes": [
            {
                "type": "rect",
                "xref": "x",
                "yref": "y",
                "x0": 0,
                "y0": 0,
                "x1": x_dim,
                "y1": z_dim,
                "line": {"color": "#cbd5e1", "width": 0.8, "dash": "dash"},
            }
        ],
    }
    if xz_preview_url:
        layout["images"] = [
            {
                "source": xz_preview_url,
                "xref": "x",
                "yref": "y",
                "x": 0,
                "y": z_dim,
                "sizex": x_dim,
                "sizey": z_dim,
                "sizing": "stretch",
                "opacity": 0.85,
                "layer": "below",
            }
        ]
    return {"data": [trace], "layout": layout, "config": {"displaylogo": False, "responsive": True}}


def _build_score_hist_fig(picks: list, score_field: Optional[str]) -> dict:
    scores = [p.get("score") for p in picks if p.get("score") is not None]
    if not scores:
        return _empty_fig("no score column in candidates.star")
    mean_v = sum(scores) / len(scores)
    return {
        "data": [
            {
                "type": "histogram",
                "x": scores,
                "nbinsx": 30,
                "marker": {"color": "#4338ca"},
                "hovertemplate": "%{x}<br>%{y} picks<extra></extra>",
            }
        ],
        "layout": {
            "xaxis": {"title": {"text": score_field or "score", "font": {"size": 10}}, "tickfont": {"size": 9}},
            "yaxis": {"title": {"text": "count", "font": {"size": 10}}, "tickfont": {"size": 9}},
            "margin": {"t": 8, "b": 38, "l": 50, "r": 8},
            "bargap": 0.05,
            "paper_bgcolor": "white",
            "plot_bgcolor": "white",
            "shapes": [
                {
                    "type": "line",
                    "xref": "x",
                    "yref": "paper",
                    "x0": mean_v,
                    "x1": mean_v,
                    "y0": 0,
                    "y1": 1,
                    "line": {"color": "#9ca3af", "width": 1.2, "dash": "dash"},
                }
            ],
            "annotations": [
                {
                    "text": f"mean {mean_v:.4f}",
                    "xref": "x",
                    "yref": "paper",
                    "x": mean_v,
                    "y": 0.96,
                    "showarrow": False,
                    "yanchor": "top",
                    "xanchor": "left",
                    "xshift": 4,
                    "font": {"size": 9, "color": "#6b7280"},
                    "bgcolor": "rgba(255,255,255,0.85)",
                }
            ],
        },
        "config": {"displaylogo": False, "responsive": True},
    }


def _read_picks_json(path: Path) -> dict:
    if not path or not Path(path).exists():
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}
    try:
        return json.loads(Path(path).read_text())
    except Exception as e:
        logger.warning("Failed to load picks.json %s: %s", path, e)
        return {"picks": [], "tomo_dims_xyz_px": [0, 0, 0], "score_field": None, "n": 0}




# ---------------------------------------------------------------------------
# Per-tilt star helpers (for FS Motion/CTF, TS Align, TS CTF, Tilt Filter)
# ---------------------------------------------------------------------------


def _read_per_tilt_df(per_tilt_star_path: Path) -> Optional[pd.DataFrame]:
    """Load the per-TS tilt block from a per-tilt star file. Each per-tilt
    star has one data block named after the TS, with one row per tilt."""
    if not per_tilt_star_path.exists():
        return None
    try:
        import starfile

        data = starfile.read(per_tilt_star_path, always_dict=True)
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "rlnTomoNominalStageTiltAngle" in v.columns:
                return v
    except Exception as e:
        logger.warning("Could not read per-tilt star %s: %s", per_tilt_star_path, e)
    return None


def _per_tilt_star_path(job_dir: Path, ts_name: str) -> Path:
    """Convention used by FS Motion/CTF, TS Align, TS CTF — per-tilt star
    sits at ``<job_dir>/tilt_series/<ts_name>.star``."""
    return job_dir / "tilt_series" / f"{ts_name}.star"


def _safe_floats(series) -> list[float]:
    """Coerce a pandas Series to a list of Python floats; non-finite stays as
    None so Plotly draws gaps instead of dropping to the floor."""
    import math

    out: list[float] = []
    for v in series:
        try:
            f = float(v)
        except (TypeError, ValueError):
            out.append(None)
            continue
        if not math.isfinite(f):
            out.append(None)
        else:
            out.append(f)
    return out


def _is_meaningful_series(values: list[float], *, threshold: float = 1e-3) -> bool:
    """True when the column carries real signal — at least one finite value
    AND max-abs above `threshold`. Filters out WarpTools placeholder columns
    (`1e-6` for AccumMotion / CtfMaxResolution; `None` for CtfFigureOfMerit)
    so we don't pollute the dashboard with flat-line plots. See memory
    `project_warp_relion_star_placeholders.md`."""
    finite = [v for v in values if v is not None]
    if not finite:
        return False
    return max(abs(v) for v in finite) >= threshold


def _stats(values: list[float]) -> dict:
    """Median / IQR / count over the non-None entries. Returns a dict with
    keys median, q1, q3, min, max, n."""
    import statistics

    finite = [v for v in values if v is not None]
    if not finite:
        return {"median": None, "q1": None, "q3": None, "min": None, "max": None, "n": 0}
    finite_sorted = sorted(finite)
    n = len(finite_sorted)
    median = statistics.median(finite_sorted)
    if n >= 4:
        q1 = statistics.median(finite_sorted[: n // 2])
        q3 = statistics.median(finite_sorted[(n + 1) // 2 :])
    else:
        q1 = q3 = median
    return {"median": median, "q1": q1, "q3": q3, "min": finite_sorted[0], "max": finite_sorted[-1], "n": n}


def _build_per_tilt_chart(
    x_tilts: list[float],
    series: list[dict],
    *,
    x_label: str = "tilt (°)",
    y_label: str = "",
    h_lines: Optional[list[dict]] = None,
    customdata: Optional[list[list]] = None,
    y_unit: str = "",
    y_range: Optional[tuple[float, float]] = None,
) -> dict:
    """Compact chart: x = tilt angle, y = one or more per-tilt metrics.

    `series`: list of {name, y, color, dash?, mode?} entries. Default mode is
    `markers` — discrete per-tilt estimates connect badly with lines (zigzag
    or tangled) so caller must opt-in via `mode='lines+markers'` when the
    metric varies continuously across tilts (e.g. shifts, refined angles).
    `customdata`: parallel list of [tilt_index, frame_basename, ...] pairs;
    surfaced in hover so users can identify which tilt a point belongs to.
    `y_unit`: short suffix appended to the y value in the hover string
    (e.g. " µm", " Å").
    `y_range`: fixed y-axis [min, max] — locks the axis across tilt-series
    so the same metric is visually comparable. Auto-extends if observed
    data exceeds the bounds (so we never clip outliers).
    """
    traces = []
    for s in series:
        marker_size = s.get("marker_size", 6)
        trace: dict = {
            "type": "scatter",
            "mode": s.get("mode", "markers"),
            "x": x_tilts,
            "y": s["y"],
            "line": {"color": s.get("color", "#4338ca"), "width": s.get("width", 1.4), "dash": s.get("dash", "solid")},
            "marker": {
                "size": marker_size,
                "color": s.get("color", "#4338ca"),
                "line": {"color": "#ffffff", "width": 0.6},
            },
            "name": s["name"],
        }
        if customdata is not None:
            trace["customdata"] = customdata
            trace["hovertemplate"] = (
                f"<b>{s['name']}</b>: %{{y:.3g}}{y_unit}"
                "<br>Tilt #%{customdata[0]} · %{x:.2f}°"
                "<br><span style='font-size:9px;color:#94a3b8'>%{customdata[1]}</span>"
                "<extra></extra>"
            )
        else:
            trace["hovertemplate"] = (
                f"<b>{s['name']}</b>: %{{y:.3g}}{y_unit}<br>Stage angle: %{{x:.2f}}°<extra></extra>"
            )
        traces.append(trace)
    layout: dict = {
        "xaxis": {"title": {"text": x_label, "font": {"size": 9}}, "tickfont": {"size": 8}, "zeroline": False},
        "yaxis": {"title": {"text": y_label, "font": {"size": 9}}, "tickfont": {"size": 8}, "zeroline": False},
        "margin": {"t": 6, "b": 32, "l": 50, "r": 12},
        "paper_bgcolor": "white",
        "plot_bgcolor": "#fafafa",
        "showlegend": len(series) > 1,
        "legend": {"orientation": "h", "x": 0, "y": 1.14, "font": {"size": 9}},
        "hovermode": "x unified",
    }
    if y_range:
        # Auto-extend the fixed range if observed data exceeds it — never
        # clip outliers; lock the axis only when data fits.
        ymin, ymax = float(y_range[0]), float(y_range[1])
        for s in series:
            for v in s.get("y") or []:
                if v is None:
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if fv < ymin:
                    ymin = fv
                if fv > ymax:
                    ymax = fv
        layout["yaxis"]["range"] = [ymin, ymax]
        layout["yaxis"]["autorange"] = False
    if h_lines:
        shapes = []
        annotations = []
        for h in h_lines:
            shapes.append(
                {
                    "type": "line",
                    "xref": "paper",
                    "yref": "y",
                    "x0": 0,
                    "x1": 1,
                    "y0": h["y"],
                    "y1": h["y"],
                    "line": {"color": h.get("color", "#9ca3af"), "width": 1.0, "dash": "dash"},
                }
            )
            if h.get("label"):
                annotations.append(
                    {
                        "text": h["label"],
                        "xref": "paper",
                        "yref": "y",
                        "x": 1,
                        "xanchor": "right",
                        "y": h["y"],
                        "yanchor": "bottom",
                        "showarrow": False,
                        "font": {"size": 8, "color": h.get("color", "#9ca3af")},
                        "bgcolor": "rgba(255,255,255,0.85)",
                    }
                )
        if shapes:
            layout["shapes"] = shapes
        if annotations:
            layout["annotations"] = annotations
    return {
        "data": traces,
        "layout": layout,
        "config": {"displaylogo": False, "responsive": True, "displayModeBar": False},
    }


def _read_atlas_index(index_path: Path) -> Optional[dict]:
    if not index_path or not Path(index_path).exists():
        return None
    try:
        meta = json.loads(Path(index_path).read_text())
        if not meta.get("index"):
            return None
        return meta
    except Exception as e:
        logger.warning("Could not parse cutout index %s: %s", index_path, e)
        return None


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------


_CB_CSS = """
.cb-sidebar {
    width: 300px; min-width: 300px; flex-shrink: 0;
    display: flex; flex-direction: column;
}
.cb-sidebar-header {
    padding: 8px 10px 6px;
    border-bottom: 1px solid #e5e7eb;
    background: #f8fafc;
    flex-shrink: 0;
    font-size: 11px;
    color: #475569;
}
.cb-sidebar-rows { overflow-y: auto; flex: 1; min-height: 0; }
.cb-ts-row {
    display: flex; flex-direction: column; gap: 2px;
    padding: 6px 10px; border-bottom: 1px solid #f1f1f1;
    cursor: pointer; font-size: 12px;
}
.cb-ts-row:hover { background: #f8fafc; }
.cb-ts-row.selected { background: #eef2ff; border-left: 3px solid #6366f1; padding-left: 7px; }
.cb-ts-row .cb-ts-pos { font-weight: 600; color: #1f2937; font-size: 11px; }
.cb-ts-row .cb-ts-name {
    font-family: ui-monospace, monospace; font-size: 9.5px; color: #6b7280;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-ts-row .cb-ts-meta { font-size: 10px; color: #6b7280; font-family: ui-monospace, monospace; }
.cb-pill-strip { display: flex; gap: 2px; margin-top: 2px; }
.cb-pill { height: 4px; flex: 1; border-radius: 2px; background: #e5e7eb; }
.cb-pill.ok { background: #10b981; }
.cb-pill.fail { background: #dc2626; }
.cb-pill.running { background: #f59e0b; }
.cb-pill.pending { background: #d1d5db; }
/* zero = stage processed this TS but produced no output (e.g. 0 picks
   above cutoff). Dimmed amber-into-grey so it reads as "ran, yielded
   nothing" — distinct from both "ok" green and "pending" grey. */
.cb-pill.zero {
    background: repeating-linear-gradient(
        45deg, #9ca3af, #9ca3af 2px, #d1d5db 2px, #d1d5db 4px
    );
}
/* skip = supervisor deliberately did not dispatch a task for this TS
   (upstream produced nothing actionable). Soft hatched grey reads as
   "intentionally blank", distinct from "pending" flat grey. */
.cb-pill.skip {
    background: repeating-linear-gradient(
        45deg, #cbd5e1, #cbd5e1 2px, #e5e7eb 2px, #e5e7eb 4px
    );
}
.cb-main { padding: 12px; }
/* (height/flex/overflow set inline at construction time so the dialog viewport
   chain is self-contained; this rule only carries the padding chrome.) */
.cb-empty {
    flex: 1; display: flex; align-items: center; justify-content: center;
    color: #9ca3af; font-size: 13px; padding: 40px; flex-direction: column; gap: 8px;
}
.cb-section-title {
    font-size: 10px; text-transform: uppercase; font-weight: 700;
    color: #475569; letter-spacing: 0.4px;
}
.cb-section-card {
    background: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px;
    padding: 10px 12px; margin-bottom: 8px;
}
.cb-section-card-header { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
.cb-aspect { width: 100%; }
.cb-picks-right { border-left: 1px solid #eef2f7; padding-left: 14px; }
@media (max-width: 900px) {
    .cb-picks-right {
        border-left: none; padding-left: 0;
        border-top: 1px solid #eef2f7; padding-top: 10px;
    }
}
.cb-hover-card {
    background: #f8fafc; border: 1px solid #e5e7eb; border-radius: 4px;
    padding: 8px 10px; font-family: ui-monospace, monospace;
    font-size: 11px; color: #374151;
    display: grid; grid-template-columns: max-content 1fr;
    gap: 4px 12px; align-items: baseline;
}
.cb-hover-card .cb-hover-key {
    color: #6b7280; text-transform: uppercase; font-size: 9px;
    font-weight: 700; letter-spacing: 0.4px;
}
.cb-hover-card .cb-hover-val { color: #1f2937; }
.cb-hover-card.cb-hover-empty { color: #9ca3af; font-style: italic; }
.cb-gallery-grid {
    display: grid; grid-template-columns: repeat(auto-fill, 96px);
    gap: 4px; padding: 6px 2px 6px 2px; justify-content: start;
}
.cb-gallery-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    border-radius: 3px; cursor: pointer; overflow: hidden;
    border: 2px solid transparent; transition: transform 0.06s ease;
}
.cb-gallery-tile:hover { transform: scale(1.04); border-color: #c7d2fe; }
.cb-gallery-tile.selected {
    border-color: #4338ca;
    box-shadow: 0 0 0 1px #4338ca, 0 4px 10px rgba(67,56,202,0.25);
}
.cb-gallery-tile .cb-tile-score {
    position: absolute; bottom: 0; right: 0;
    padding: 1px 4px; background: rgba(15,23,42,0.72);
    font-family: ui-monospace, monospace; font-size: 9px; color: #f8fafc;
    border-top-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-z {
    position: absolute; bottom: 0; left: 0;
    padding: 1px 4px; background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace; font-size: 9px; color: #cbd5e1;
    border-top-right-radius: 3px;
}
.cb-gallery-tile .cb-tile-idx {
    position: absolute; top: 0; right: 0;
    padding: 0 4px; background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace; font-size: 9px; color: #cbd5e1;
    border-bottom-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-rank {
    position: absolute; top: 0; left: 0;
    padding: 0 4px; background: rgba(67,56,202,0.85);
    font-family: ui-monospace, monospace; font-size: 9px; color: white;
    border-bottom-right-radius: 3px;
}
.cb-gallery-empty {
    padding: 18px; text-align: center; font-size: 11px; color: #6b7280;
    background: #f8fafc; border-radius: 4px; border: 1px dashed #cbd5e1;
}
.cb-tomo-preview {
    width: 100%; background: #0f172a; border-radius: 4px;
    overflow: hidden; position: relative;
}
.cb-tomo-preview img { width: 100%; height: 100%; object-fit: cover; display: block; }
.cb-preview-stack { display: flex; flex-direction: column; gap: 6px; width: 100%; }
.cb-pick-marker {
    position: absolute; width: 14px; height: 14px;
    border-radius: 50%; border: 2px solid #fff;
    background: rgba(244, 114, 182, 0.95);
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.55), 0 0 8px rgba(244, 114, 182, 0.55);
    transform: translate(-50%, -50%);
    pointer-events: none; opacity: 0;
    transition: opacity 0.08s ease, left 0.05s linear, top 0.05s linear;
    z-index: 6; left: 0; top: 0;
}
.cb-pick-ghost {
    position: absolute; width: 5px; height: 5px;
    border-radius: 50%; background: rgba(67, 56, 202, 0.55);
    box-shadow: 0 0 0 0.5px rgba(255, 255, 255, 0.25);
    transform: translate(-50%, -50%);
    pointer-events: none; z-index: 4;
}
.cb-overlay-hide .cb-pick-ghost { display: none; }
.cb-preview-toolbar {
    display: flex; align-items: center; gap: 10px;
    font-size: 10px; color: #475569;
    padding: 2px 0 4px 0;
}
.cb-gallery-scroll { overflow-y: auto; max-height: 75vh; padding-right: 4px; }
.cb-failures-list {
    font-size: 10px; color: #6b7280;
    font-family: ui-monospace, monospace;
    max-height: 90px; overflow-y: auto;
    background: #f8fafc; border: 1px solid #e5e7eb;
    border-radius: 3px; padding: 6px 8px;
}
.cb-failures-list .cb-failure-row {
    display: flex; gap: 8px; padding: 1px 0;
    border-bottom: 1px dashed #e5e7eb;
}
.cb-failures-list .cb-failure-row:last-child { border-bottom: none; }
.cb-failures-list .cb-failure-i { color: #ef4444; min-width: 32px; }
.cb-instance-toolbar {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: #475569;
    padding: 2px 0 4px 0; flex-wrap: wrap;
}
.cb-datadump-grid {
    display: grid; grid-template-columns: max-content 1fr;
    gap: 2px 14px; font-family: ui-monospace, monospace;
    font-size: 11px; padding: 2px 0;
}
.cb-datadump-key {
    color: #6b7280; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
    align-self: baseline;
}
.cb-datadump-val { color: #1f2937; align-self: baseline; word-break: break-all; }
.cb-metric-strip {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569;
}
.cb-section-placeholder {
    font-size: 10px; color: #9ca3af; font-style: italic;
    padding: 4px 0 2px 0;
}
.cb-plot-row {
    display: flex; gap: 8px; flex-wrap: wrap; margin: 4px 0 4px 0;
}
.cb-plot-cell {
    flex: 1 1 320px; min-width: 260px;
    background: #ffffff; border: 1px solid #f1f5f9; border-radius: 4px;
    padding: 2px 4px;
}
.cb-plot-cell-wide { flex: 1 1 100%; min-width: 280px; }
.cb-plot-label {
    font-size: 9px; color: #64748b; font-weight: 600;
    padding: 1px 4px 0; text-transform: uppercase; letter-spacing: 0.3px;
}
.cb-stat-strip {
    display: flex; gap: 14px; flex-wrap: wrap;
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569; padding: 2px 0 4px 0;
}
.cb-stat-strip .cb-stat-key { color: #94a3b8; margin-right: 3px; }
.cb-stat-strip .cb-stat-val { color: #1e293b; font-weight: 600; }
.cb-drop-list {
    font-family: ui-monospace, monospace; font-size: 10px; color: #6b7280;
    background: #fef3c7; border: 1px solid #fde68a; border-radius: 3px;
    padding: 6px 8px; margin: 4px 0; max-height: 120px; overflow-y: auto;
}
.cb-drop-list .cb-drop-row {
    display: flex; gap: 8px; padding: 1px 0; border-bottom: 1px dashed #fde68a;
}
.cb-drop-list .cb-drop-row:last-child { border-bottom: none; }
.cb-drop-list .cb-drop-tilt { color: #b45309; min-width: 60px; }
.cb-datadump-grid-2col {
    display: grid; grid-template-columns: max-content 1fr max-content 1fr;
    gap: 2px 12px; font-family: ui-monospace, monospace;
    font-size: 11px; padding: 2px 0;
}
.cb-pixel-section-title {
    display: flex; align-items: center; gap: 5px; padding: 8px 0 2px 0;
    font-size: 11px; color: #475569; font-weight: 600;
    border-top: 1px solid #f1f5f9; margin-top: 6px;
}
/* Wrapper allows horizontal scroll on narrow viewports without breaking
 * column alignment. The table itself is one CSS Grid so universal +
 * per-species rows share column widths automatically. */
.cb-pixel-table-wrapper {
    overflow-x: auto;
    padding-bottom: 4px;
}
.cb-pixel-table {
    display: grid;
    grid-template-columns:
        minmax(170px, max-content)
        minmax(60px, max-content)
        minmax(120px, max-content)
        minmax(160px, max-content)
        minmax(150px, max-content)
        minmax(120px, max-content)
        minmax(90px, max-content)
        minmax(180px, 1fr);
    column-gap: 28px;
    font-family: ui-monospace, monospace;
    font-size: 11px;
    padding: 2px 0;
    min-width: max-content;  /* lets the grid grow past the wrapper for x-scroll */
}
.cb-pixel-cell {
    display: flex; align-items: center; gap: 5px;
    padding: 4px 2px;
    color: #1f2937;
    border-bottom: 1px dashed #f1f5f9;
    white-space: nowrap;
}
.cb-pixel-cell.cb-pixel-header {
    color: #6b7280; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
    border-bottom: 1px solid #e2e8f0;
}
.cb-pixel-cell.cb-pixel-warn-error { background: #fef2f2; color: #b91c1c; }
.cb-pixel-cell.cb-pixel-warn-warn  { background: #fff7ed; color: #b45309; }
.cb-pixel-cell.cb-pixel-warn-info  { color: #475569; }
.cb-pixel-cell.cb-pixel-notes { white-space: normal; color: #475569; font-size: 10px; }
.cb-pixel-stripe {
    display: inline-block; width: 3px; height: 12px;
    border-radius: 1px; background: #cbd5e1; flex-shrink: 0;
}
.cb-pixel-stage-label    { color: #1e293b; font-weight: 600; }
.cb-pixel-instance-label { color: #94a3b8; font-size: 9px; }
.cb-pixel-warn-icon      { cursor: help; }
/* Species marker = a single full-width row inside the same grid; keeps
 * column alignment perfect. */
.cb-pixel-species-row {
    grid-column: 1 / -1;
    display: flex; align-items: center; gap: 8px;
    padding: 8px 2px 4px 2px; margin-top: 6px;
    border-top: 1px dashed #cbd5e1;
    font-family: ui-monospace, monospace;
    background: #f8fafc;
}
.cb-pixel-species-row .cb-pixel-stripe { width: 4px; height: 14px; }
.cb-pixel-species-name {
    color: #334155; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; font-size: 11px;
}
.cb-pixel-species-id {
    color: #94a3b8; font-size: 9px; font-weight: 400;
}
/* Inline status chips used in section-card headers and chip strips. */
.cb-chip-strip {
    display: flex; gap: 6px; flex-wrap: wrap;
    padding: 4px 0 6px 0;
}
.cb-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 2px 8px; border-radius: 999px;
    font-family: ui-monospace, monospace; font-size: 10px;
    border: 1px solid #e5e7eb; background: #f8fafc;
    color: #475569; cursor: help; line-height: 1.4;
    white-space: nowrap;
}
.cb-chip .cb-chip-label {
    color: #94a3b8; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
}
.cb-chip .cb-chip-value { color: #1e293b; font-weight: 600; }
.cb-chip-ok      { background: #ecfdf5; border-color: #a7f3d0; }
.cb-chip-ok      .cb-chip-value { color: #047857; }
.cb-chip-warn    { background: #fffbeb; border-color: #fcd34d; }
.cb-chip-warn    .cb-chip-value { color: #b45309; }
.cb-chip-error   { background: #fef2f2; border-color: #fecaca; }
.cb-chip-error   .cb-chip-value { color: #b91c1c; }
.cb-chip-info    { background: #eff6ff; border-color: #bfdbfe; }
.cb-chip-info    .cb-chip-value { color: #1d4ed8; }
.cb-chip-neutral { background: #f1f5f9; }
.cb-chip-icon    { font-size: 11px !important; }
/* Recon-section large canvas: viewport-filling WarpTools PNG preview. */
.cb-recon-preview {
    width: 100%;
    background: #0f172a;
    border-radius: 6px;
    overflow: hidden;
    margin: 8px 0 4px 0;
    display: flex; justify-content: center; align-items: center;
}
.cb-recon-preview img {
    width: 100%;
    max-height: 75vh;
    object-fit: contain;
    display: block;
}
.cb-recon-preview-caption {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #6b7280; padding: 2px 4px 6px 4px;
}
"""


def _ensure_assets_loaded() -> None:
    ui.add_head_html(f"<style>{_CB_CSS}</style>")


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def open_tomo_dashboard(ts_name: Optional[str] = None, focus_section: Optional[str] = None) -> None:
    """Open the unified per-TS dashboard.

    Args:
        ts_name: optional TS to select on open. If absent or unknown, opens at
                 the first TS in the project's union list.
        focus_section: optional section key (e.g. "candidate_extract") to
                       scroll into view once the main pane renders.
    """
    state = get_project_state()
    if state.project_path is None:
        ui.notify("No project loaded.", type="warning")
        return

    project_path = Path(state.project_path)
    journey, ts_names = _collect_dashboard_journey(state, project_path)

    initial_ts = ts_name if (ts_name and ts_name in ts_names) else (ts_names[0] if ts_names else None)
    selected = {"ts": initial_ts}

    # Per-mount auto-kick dedup. Cleared on each fresh dashboard open so a
    # reload after the user fixed a stuck job re-triggers generation.
    reset_auto_kick_state()
    _ensure_assets_loaded()

    with (
        ui.dialog().props("maximized") as dlg,
        # Explicit viewport-locked geometry. Quasar's maximized dialog provides
        # a full-viewport wrapper, but `h-full` on the card was resolving to
        # the *content* height (so the page grew tall enough to need browser-
        # zoom-out to see). Pin to 100vh × 100vw and let the inner flex chain
        # carve out a scrollable main pane.
        ui.card()
        .classes("bg-gray-50 p-0")
        .style(
            "width: 100vw; height: 100vh; max-width: 100vw; max-height: 100vh; "
            "overflow: hidden; display: flex; flex-direction: column; border-radius: 0;"
        ),
    ):
        # Floating close button — same convention as the previous dialog.
        (
            ui.button(icon="close", on_click=dlg.close)
            .props("flat dense round size=sm")
            .classes("text-gray-500 absolute z-10")
            .style("top: 6px; right: 8px;")
        )

        # Inner flex-row wrapper. Plain <div> instead of ui.row() to avoid
        # Quasar's `.row` flex-wrap rules clashing with our height chain.
        with ui.element("div").style(
            "flex: 1 1 0; min-height: 0; display: flex; flex-direction: row; width: 100%; overflow: hidden;"
        ):
            sidebar = ui.element("div").classes("cb-sidebar bg-white border-r border-gray-200").style("height: 100%;")
            main_area = (
                ui.element("div")
                .classes("cb-main")
                .style("height: 100%; min-height: 0; flex: 1 1 0; min-width: 0; overflow-y: auto; overflow-x: hidden;")
            )

            def render_main() -> None:
                main_area.clear()
                with main_area:
                    if selected["ts"] is None:
                        _render_no_data_empty_state()
                    else:
                        _render_main_pane_for_ts(selected["ts"], state, project_path, refresh_all)
                if focus_section and selected["ts"] is not None:
                    _scroll_section_into_view(focus_section)

            def render_sidebar() -> None:
                # Re-collect journey on rebuild — pick/subtomo statuses change as
                # the regen handlers update preview manifests, and array-job
                # pills tick over as SLURM tasks complete.
                fresh_journey, fresh_ts_names = _collect_dashboard_journey(state, project_path)
                sidebar.clear()
                with sidebar:
                    with ui.element("div").classes("cb-sidebar-header"):
                        ui.label(f"{len(fresh_ts_names)} tilt series").classes("font-mono")
                        # Compact stage legend so users can decode the pill strip
                        # without hovering each pill. Pills also carry tooltips.
                        with (
                            ui.row()
                            .classes("items-center gap-1")
                            .style("margin-top: 4px; flex-wrap: nowrap; font-size: 9px; color: #94a3b8;")
                        ):
                            for _key, label, _jt in _PILL_STAGES:
                                ui.label(label).classes("font-mono").style("flex: 1; text-align: center;")
                    rows_container = ui.element("div").classes("cb-sidebar-rows")
                    with rows_container:
                        for ts in fresh_ts_names:
                            _render_ts_row(ts, fresh_journey.get(ts, {}), selected, refresh_all)

            def refresh_all() -> None:
                render_sidebar()
                render_main()

            refresh_all()

            # Live refresh while the dialog is open: re-render every 4 s if
            # any background task is in flight for this project. Keeps the
            # journey strip / section cards in sync with manifests being
            # written by an async preview-render or IMOD-gen task. Skip
            # the rebuild when nothing's running to avoid burning the
            # event loop on idle dashboards.
            from services.background_tasks import get_background_task_registry

            _last_signature = {"sig": None}

            def _maybe_refresh() -> None:
                try:
                    registry = get_background_task_registry()
                    active = [
                        t for t in registry.for_project(str(project_path)) if t.is_running
                    ]
                    # Signature picks up "task started", "task finished", and
                    # per-tick progress so we re-render any time meaningful
                    # state changed. Cheap to compute; cheap to compare.
                    sig = tuple(
                        (t.id, t.progress_current, t.progress_total) for t in active
                    ) + (tuple(
                        # Include very-recently-finished tasks so the dashboard
                        # picks up the final manifest write (which happens at
                        # the end of the render) within one refresh window.
                        (t.id, t.status) for t in registry.for_project(str(project_path))
                        if not t.is_running
                        and t.finished_at
                        and (t.finished_at - t.started_at).total_seconds() < 86400
                    ),)
                    if sig != _last_signature["sig"]:
                        _last_signature["sig"] = sig
                        refresh_all()
                except RuntimeError:
                    # Client gone — timer will clean up shortly.
                    pass

            live_timer = ui.timer(4.0, _maybe_refresh)
            dlg.on("hide", lambda _e=None: live_timer.cancel())
            dlg.on("before-hide", lambda _e=None: live_timer.cancel())

    dlg.open()


def _render_no_data_empty_state() -> None:
    """Shown in the main pane when the project has no array-job manifests
    (so the sidebar is empty too). The dashboard still opens — this gives
    the user a stable place to land when the project hasn't been run yet."""
    with ui.element("div").classes("cb-empty"):
        ui.icon("hourglass_empty", size="48px").classes("text-gray-400")
        ui.label("No tilt-series data yet.").classes("text-sm text-gray-500")
        ui.label(
            "Run any of the array jobs (FS Motion/CTF, Alignment, CTF, Reconstruct)"
            " to populate the sidebar with per-TS rows."
        ).classes("text-[11px] italic text-gray-400 text-center").style("max-width: 460px;")


def _scroll_section_into_view(section_key: str) -> None:
    """Scroll the first section card whose data-section matches `section_key`
    into view inside the main pane. Runs after the DOM settles."""
    ui.run_javascript(
        "setTimeout(function(){"
        f"  const el = document.querySelector('.cb-section-card[data-section={section_key!r}]');"
        "  if (el) el.scrollIntoView({behavior: 'smooth', block: 'start'});"
        "}, 120);"
    )


def _render_ts_row(ts_name: str, journey_row: dict, selected: dict, refresh) -> None:
    """One sidebar row with position label, mono name, journey pill strip, and
    optional metric line."""
    cls = "cb-ts-row"
    if selected.get("ts") == ts_name:
        cls += " selected"

    label, _ = _position_label(ts_name)

    def on_click() -> None:
        if selected.get("ts") == ts_name:
            return
        selected["ts"] = ts_name
        refresh()

    with ui.element("div").classes(cls).on("click", on_click):
        ui.label(label).classes("cb-ts-pos")
        ui.label(ts_name).classes("cb-ts-name")
        with ui.element("div").classes("cb-pill-strip"):
            for key, stage_label, _jt in _PILL_STAGES:
                status = journey_row.get(key, "pending")
                ui.element("div").classes(f"cb-pill {status}").tooltip(_pill_tooltip(stage_label, status))


_PILL_TOOLTIP_LABEL = {
    "ok": "done",
    "fail": "failed",
    "running": "running",
    "zero": "ran, produced 0 results above cutoff",
    "pending": "not started",
}


def _pill_tooltip(stage_label: str, status: str) -> str:
    return f"{stage_label}: {_PILL_TOOLTIP_LABEL.get(status, status)}"


# ---------------------------------------------------------------------------
# Main pane orchestrator: per-TS section stack
# ---------------------------------------------------------------------------


def _render_main_pane_for_ts(ts_name: str, project_state, project_path: Path, refresh) -> None:
    """Render the main-pane section stack for the selected TS. Sections emit
    in pipeline order; each is a no-op if the corresponding job isn't in the
    pipeline (per ROADMAP §2.1 contract)."""
    rendered_any = False

    # Project-wide / per-TS analytics — primitive datadumps for now (Slice C).
    for emit in (
        _render_dataset_section,
        _render_fs_motion_ctf_section,
        _render_tilt_filter_section,
        _render_ts_alignment_section,
        _render_ts_ctf_section,
        _render_reconstruct_section,
    ):
        if emit(ts_name, project_state, project_path, refresh):
            rendered_any = True

    # Template Match — one card per TM instance for this TS. Surfaces
    # declared-vs-applied symmetry parity + template/mask intrinsic-shape
    # chips. Lives between Reconstruct and Candidate Extract in pipeline order.
    for instance_id, job_model in _template_match_instances(project_state):
        if _render_template_match_section(ts_name, instance_id, job_model, project_path, refresh):
            rendered_any = True

    # Candidate Extract — flagship card carries the preview pair, gallery, and
    # scatter fallback. Stacked per matching candidate-extract instance.
    for instance_id, job_model in _candidate_extract_instances(project_state):
        if _render_candidate_extract_section(ts_name, instance_id, job_model, project_path, refresh):
            rendered_any = True

    if not rendered_any:
        with ui.element("div").classes("cb-empty"):
            ui.icon("hourglass_empty", size="36px")
            ui.label(f"No section data yet for {ts_name}.").classes("text-xs")
            ui.label("Section cards appear once the matching pipeline jobs have run.").classes(
                "text-[11px] italic text-center"
            ).style("max-width: 420px;")


# ---------------------------------------------------------------------------
# Slice C analytics sections — primitive key/value datadumps (no new manifests
# needed; read directly from job_model.* + the stage's output star)
# ---------------------------------------------------------------------------


def _find_job_by_type(project_state, jt: JobType) -> Optional[tuple[str, object]]:
    """Return (instance_id, job_model) for the first job matching this type,
    or None. The match accepts either `job_model.job_type == jt` or an
    `instance_id` whose base prefix matches `jt.value` (covers `__species`
    instances)."""
    for iid, jm in (project_state.jobs or {}).items():
        if getattr(jm, "job_type", None) == jt or iid.split("__")[0] == jt.value:
            return iid, jm
    return None


def _render_datadump_card(
    section_key: str,
    icon: str,
    title: str,
    metric_strip: str,
    instance_id: Optional[str],
    job_status_label: Optional[str],
    rows: list[tuple[str, str]],
    note: Optional[str] = None,
) -> None:
    """Slice-C primitive section card: header + 1-line metric strip + key/value
    grid. Reused by every analytics emitter."""
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = section_key
        if instance_id:
            card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon(icon, size="14px").classes("text-indigo-600")
            ui.label(title).classes("cb-section-title")
            if instance_id:
                ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_strip:
                ui.label(metric_strip).classes("cb-metric-strip")
            if job_status_label and job_status_label.lower() != "succeeded":
                ui.label(job_status_label).classes("text-[10px] text-amber-600 font-mono")
        if rows:
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label("—" if v is None or v == "" else str(v)).classes("cb-datadump-val")
        if note:
            ui.label(note).classes("cb-section-placeholder")


# ---------------------------------------------------------------------------
# Pixel / binning sanity panel  (ROADMAP §11)
#
# Shows, in one dense monospace table, how pixel size + tomogram dimensions
# + per-instance box / padding / particle-diameter propagate through the
# pipeline. Sanity rules flag violations inline (per-cell icon + tooltip):
#  - box (Å) vs particle diameter (Å) outside 1.5–3×
#  - subtomo crop > box (impossible padding)
#  - particle diameter inconsistency across candidate-extract instances
#  - template volume px ≠ reconstruction px (picks would be garbage)
# ---------------------------------------------------------------------------


def _split_species_id(instance_id: str) -> Optional[str]:
    """`templatematching__ribosome` → `ribosome`; bare instance_id → None."""
    parts = instance_id.split("__", 1)
    return parts[1] if len(parts) > 1 else None


def _resolve_species(state, job_model, instance_id: str):
    """Find the ParticleSpecies a per-particle job is attached to. Tries:
    1. `instance_id` suffix (`templatematching__ribosome` → `ribosome`).
    2. `job_model.species_id` field (set even when instance_id is bare).
    3. Single-species fallback: if exactly one species exists in the
       project, attribute the job to it.
    Returns (species or None, species_id or None)."""
    sid = _split_species_id(instance_id)
    if sid:
        sp = state.get_species(sid)
        if sp:
            return sp, sid
    sid2 = getattr(job_model, "species_id", None)
    if sid2:
        sp = state.get_species(sid2)
        if sp:
            return sp, sid2
        return None, sid2
    if len(state.species_registry) == 1:
        sp = state.species_registry[0]
        return sp, sp.id
    return None, None


# Template-header reads are cached centrally in
# services.templating.template_metadata (mtime-keyed); use the shared
# helper here as a thin tuple shim so existing callsites don't change.


def _read_template_apix_box(template_path: str) -> tuple[Optional[float], Optional[int]]:
    info = read_template_header(template_path)
    return info.apix_ang, info.box_px


def _template_match_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for iid, jm in state.jobs.items():
        if getattr(jm, "job_type", None) == JobType.TEMPLATE_MATCH_PYTOM:
            out.append((iid, jm))
    return sorted(out, key=lambda kv: kv[0])


# ---------------------------------------------------------------------------
# tmResults *_job.json reader — surfaces what PyTOM actually applied per TS
# (vs. what the user declared in project_params.json)
# ---------------------------------------------------------------------------


def _read_tm_job_json(job_dir: Path, ts_name: str) -> Optional[dict]:
    """PyTOM writes `tmResults/{tomo_name}_job.json` per TS. Returns the
    parsed dict or None if missing/unreadable."""
    p = job_dir / "tmResults" / f"{ts_name}_job.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning("Could not read TM job json %s: %s", p, e)
        return None


def _parse_tomo_dimensions(s: str) -> Optional[tuple[int, int, int]]:
    """`'4096x4096x2048'` → `(4096, 4096, 2048)`. Returns None on parse failure.
    Native-pixel-size dimensions as written into TsAlignmentParams.tomo_dimensions."""
    if not s:
        return None
    try:
        parts = s.lower().split("x")
        if len(parts) != 3:
            return None
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, AttributeError):
        return None


def _scale_tomo_dims(native_dims: tuple[int, int, int], native_px: float, target_px: float) -> tuple[int, int, int]:
    if target_px <= 0 or native_px <= 0:
        return native_dims
    f = native_px / target_px
    return (int(round(native_dims[0] * f)), int(round(native_dims[1] * f)), int(round(native_dims[2] * f)))


def _compute_pixel_chain(project_state) -> list[dict]:
    """Walk pipeline stages and return rows for the pixel-sanity table.
    One row per pipeline stage; multi-instance fan-out for TM / Pick / Subtomo
    (one row per species). Each row has the columns the table renders, plus
    an empty `warnings` dict that `_apply_sanity_rules` later populates."""
    ms = project_state.microscope
    acq = project_state.acquisition
    native_px = float(ms.pixel_size_angstrom or 0.0)

    rows: list[dict] = []

    def make_row(stage_key: str, stage_label: str, **kw) -> dict:
        return {
            "stage_key": stage_key,
            "stage_label": stage_label,
            "px_size_ang": kw.get("px_size_ang"),
            "tomo_px": kw.get("tomo_px"),
            "box_px": kw.get("box_px"),
            "box_ang": kw.get("box_ang"),
            "particle_diameter_ang": kw.get("particle_diameter_ang"),
            "notes": kw.get("notes") or [],
            "warnings": {},
            "instance_id": kw.get("instance_id"),
            "species_color": kw.get("species_color"),
            "species_id": kw.get("species_id"),
            "species_name": kw.get("species_name"),
            "_template_workbench_px": kw.get("_template_workbench_px"),
            "_crop_px": kw.get("_crop_px"),
        }

    # ---- Camera (always) ----
    rows.append(
        make_row(
            "camera",
            "Camera",
            px_size_ang=native_px or None,
            tomo_px=(acq.detector_dimensions[0], acq.detector_dimensions[1], None),
            notes=[f"detector frame · {int(ms.acceleration_voltage_kv)} kV"],
        )
    )

    # ---- FS Motion / CTF ----
    fs = _find_job_by_type(project_state, JobType.FS_MOTION_CTF)
    if fs:
        rows.append(
            make_row(
                "fs_ctf",
                "FS Motion / CTF",
                px_size_ang=native_px or None,
                instance_id=fs[0],
                notes=["per-frame; no rescale"],
            )
        )

    # ---- Tilt Filter (pipeline job OR standalone) ----
    tf_in_pipeline = _find_job_by_type(project_state, JobType.TILT_FILTER)
    tf_standalone = (
        project_state.project_path is not None and (Path(project_state.project_path) / "TiltFilter").exists()
    )
    if tf_in_pipeline or tf_standalone:
        rows.append(
            make_row(
                "tilt_filter",
                "Tilt Filter",
                px_size_ang=native_px or None,
                instance_id=tf_in_pipeline[0] if tf_in_pipeline else None,
                notes=["row drop only; no rescale"],
            )
        )

    # ---- Alignment (rescale + native-px tomo dims) ----
    ali = _find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    aligned_px: Optional[float] = None
    aligned_dims_native: Optional[tuple[int, int, int]] = None
    aligned_dims_at_align_px: Optional[tuple[int, int, int]] = None
    if ali:
        ali_iid, ali_jm = ali
        v = float(getattr(ali_jm, "rescale_angpixs", 0.0) or 0.0)
        aligned_px = v if v > 0 else None
        aligned_dims_native = _parse_tomo_dimensions(getattr(ali_jm, "tomo_dimensions", "") or "")
        if aligned_dims_native and aligned_px and native_px > 0:
            aligned_dims_at_align_px = _scale_tomo_dims(aligned_dims_native, native_px, aligned_px)
        notes = []
        if aligned_px and native_px > 0:
            notes.append(f"rescale ÷{aligned_px / native_px:.1f}")
        am = getattr(ali_jm, "alignment_method", None)
        if am is not None:
            notes.append(f"method={getattr(am, 'value', am)}")
        rows.append(
            make_row(
                "align",
                "Align",
                px_size_ang=aligned_px,
                tomo_px=aligned_dims_at_align_px,
                instance_id=ali_iid,
                notes=notes,
            )
        )

    # ---- TS CTF (post-alignment refit; inherits aligned px / dims) ----
    ctf = _find_job_by_type(project_state, JobType.TS_CTF)
    if ctf:
        rows.append(
            make_row(
                "ts_ctf",
                "TS CTF",
                px_size_ang=aligned_px,
                tomo_px=aligned_dims_at_align_px,
                instance_id=ctf[0],
                notes=["inherits align scale"],
            )
        )

    # ---- Reconstruct (rescale to recon_px) ----
    rec = _find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    recon_px: Optional[float] = None
    recon_dims: Optional[tuple[int, int, int]] = None
    if rec:
        rec_iid, rec_jm = rec
        v = float(getattr(rec_jm, "rescale_angpixs", 0.0) or 0.0)
        recon_px = v if v > 0 else None
        if aligned_dims_native and recon_px and native_px > 0:
            recon_dims = _scale_tomo_dims(aligned_dims_native, native_px, recon_px)
        notes = []
        if recon_px and native_px > 0:
            notes.append(f"rescale ÷{recon_px / native_px:.1f}")
        if getattr(rec_jm, "deconv", 0):
            notes.append("deconv")
        rows.append(
            make_row("recon", "Recon", px_size_ang=recon_px, tomo_px=recon_dims, instance_id=rec_iid, notes=notes)
        )

    # ---- Template Match (one row per species) ----
    for tm_iid, tm_jm in _template_match_instances(project_state):
        species, species_id = _resolve_species(project_state, tm_jm, tm_iid)
        # Template path: per-job override (v1) wins when set, otherwise the
        # species's v2 template (or v1 fallback). MRC header is the
        # authoritative source for apix and box.
        tmpl_path = getattr(tm_jm, "template_path", "") or (
            get_effective_template_path(species) if species else ""
        )
        tmpl_px = 0.0
        tmpl_box = 0
        if tmpl_path:
            mrc_apix, mrc_box = _read_template_apix_box(tmpl_path)
            tmpl_px = float(mrc_apix or 0.0)
            tmpl_box = int(mrc_box or 0)
        op_px = recon_px or aligned_px
        tmpl_box_ang = (tmpl_box * tmpl_px) if (tmpl_box and tmpl_px) else None
        notes = []
        ang_search = getattr(tm_jm, "angular_search", None)
        if ang_search:
            notes.append(f"θ={ang_search}°")
        # Symmetry: prefer species (v2 source of truth); fall back to job (v1).
        sym = (getattr(species, "symmetry", None) if species else None) or getattr(tm_jm, "symmetry", None)
        if sym:
            notes.append(f"sym={sym}")
        if tmpl_px:
            notes.append(f"tmpl px={tmpl_px:g}")
        if tmpl_path and not (tmpl_px and tmpl_box):
            notes.append("tmpl header unreadable")
        rows.append(
            make_row(
                "tm",
                "TM",
                px_size_ang=op_px,
                tomo_px=recon_dims,
                box_px=tmpl_box if tmpl_box else None,
                box_ang=tmpl_box_ang,
                instance_id=tm_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                _template_workbench_px=tmpl_px or None,
                notes=notes,
            )
        )

    # ---- Candidate Extract (one row per species) ----
    candidate_diameter_by_species: dict[Optional[str], list[tuple[str, float]]] = {}
    for ce_iid, ce_jm in _candidate_extract_instances(project_state):
        species, species_id = _resolve_species(project_state, ce_jm, ce_iid)
        # Particle diameter: prefer species.diameter_ang (v2 source of truth);
        # fall back to the per-Pick-job value (v1) so projects pre-migration
        # still surface a number.
        species_diameter = float(getattr(species, "diameter_ang", 0.0) or 0.0) if species else 0.0
        diameter = species_diameter or float(getattr(ce_jm, "particle_diameter_ang", 0.0) or 0.0)
        if diameter:
            candidate_diameter_by_species.setdefault(species_id, []).append((ce_iid, diameter))
        notes = []
        method = getattr(ce_jm, "cutoff_method", None)
        cv = getattr(ce_jm, "cutoff_value", None)
        if method is not None and cv is not None:
            mv = getattr(method, "value", str(method))
            notes.append(f"{mv}={cv:g}")
        max_n = getattr(ce_jm, "max_num_particles", None)
        if max_n:
            notes.append(f"max N={max_n}")
        score_apix = getattr(ce_jm, "apix_score_map", "auto") or "auto"
        if score_apix and score_apix != "auto":
            notes.append(f"score apix={score_apix}")
        # Particle diameter in voxels at recon px (handy mental check)
        if diameter and recon_px:
            notes.append(f"Ø ≈ {diameter / recon_px:.0f} px @ {recon_px:g} Å/px")
        rows.append(
            make_row(
                "pick",
                "Pick",
                px_size_ang=recon_px,
                tomo_px=recon_dims,
                particle_diameter_ang=diameter or None,
                instance_id=ce_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                notes=notes,
            )
        )

    # ---- Subtomo Extract (one row per species) ----
    for se_iid, se_jm in _subtomo_extract_instances(project_state):
        species, species_id = _resolve_species(project_state, se_jm, se_iid)
        binning = float(getattr(se_jm, "binning", 1.0) or 1.0)
        eff_px = (native_px * binning) if native_px > 0 else None
        bx = int(getattr(se_jm, "box_size", 0) or 0)
        cx = int(getattr(se_jm, "crop_size", -1) or -1)
        box_px = bx if bx > 0 else None
        box_ang = (box_px * eff_px) if (box_px and eff_px) else None
        notes = []
        if binning != 1.0:
            notes.append(f"bin={binning:g}")
        # Surface candidate diameter cross-link for sanity rule
        diameter_for_species: Optional[float] = None
        items = candidate_diameter_by_species.get(species_id) or []
        if items:
            diameter_for_species = items[0][1]
        rows.append(
            make_row(
                "subtomo",
                "Subtomo",
                px_size_ang=eff_px,
                box_px=box_px,
                box_ang=box_ang,
                particle_diameter_ang=diameter_for_species,
                instance_id=se_iid,
                species_color=getattr(species, "color", None),
                species_id=species_id,
                species_name=getattr(species, "name", None) or species_id,
                _crop_px=cx if cx > 0 else None,
                notes=notes,
            )
        )

    return rows


def _apply_sanity_rules(rows: list[dict]) -> None:
    """Mutate `rows`: populate per-cell `warnings` for sanity-rule violations.

    Each warning is `(level, message)` where level ∈ {"error", "warn", "info"}
    and the dict key matches a column id from `_PIXEL_COLUMNS` (so the icon
    attaches to the offending cell).
    """
    recon_px: Optional[float] = None
    for r in rows:
        if r["stage_key"] == "recon":
            recon_px = r["px_size_ang"]
            break

    # Particle diameter consistency across candidate-extract instances of
    # the same species
    by_species: dict[Optional[str], list[dict]] = {}
    for r in rows:
        if r["stage_key"] == "pick" and r.get("particle_diameter_ang"):
            by_species.setdefault(r.get("species_id"), []).append(r)
    for sid, items in by_species.items():
        if len(items) <= 1:
            continue
        diam_values = [r["particle_diameter_ang"] for r in items]
        if max(diam_values) - min(diam_values) > 1e-3 * max(diam_values):
            msg = (
                f"Particle diameter differs across candidate-extract instances for "
                f"species '{sid or '—'}' ({min(diam_values):g}–{max(diam_values):g} Å) — "
                f"likely a binning-arithmetic mistake."
            )
            for r in items:
                r["warnings"]["particle"] = ("warn", msg)

    # Box vs particle diameter  (TM and Subtomo)
    # Box vs particle Ø — tighter zones than the older 1.5–3.0× window
    # (per JOURNEY_CANDIDATE_METRICS.md §"Box and crop sizing rationality"):
    #   red  < 1.5×  (particle won't fit; tight Refine3D shifts will clip)
    #   amber 1.5–2.0× (acceptable but no margin for refinement)
    #   green 2.0–3.0×
    #   amber > 3.0× (wasted compute)
    for r in rows:
        if r["stage_key"] not in ("tm", "subtomo"):
            continue
        b = r.get("box_ang")
        d = r.get("particle_diameter_ang")
        if not b or not d:
            continue
        ratio = b / d
        if ratio < 1.5:
            r["warnings"]["box"] = (
                "error",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — particle won't fit. "
                f"Aim for ≥ 2.0× (≥ 1.5× absolute floor).",
            )
        elif ratio < 2.0:
            r["warnings"]["box"] = (
                "warn",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — tight; no margin for "
                f"Refine3D shifts. Aim for ≥ 2.0×.",
            )
        elif ratio > 3.0:
            r["warnings"]["box"] = (
                "warn",
                f"Box {b:g} Å is {ratio:.2f}× particle diameter {d:g} Å — wasted compute. Aim for 2.0–3.0×.",
            )

    # Template volume px vs recon px (silent mismatch ⇒ garbage picks)
    if recon_px:
        for r in rows:
            if r["stage_key"] != "tm":
                continue
            wb_px = r.get("_template_workbench_px")
            if wb_px and abs(wb_px - recon_px) / recon_px > 0.05:
                r["warnings"]["px"] = (
                    "error",
                    f"Template prepared at {wb_px:g} Å/px but reconstruction is at "
                    f"{recon_px:g} Å/px. Picks will be unreliable from this mismatch. "
                    f"Re-render the template at {recon_px:g} Å/px (simpler than re-running "
                    f"the reconstruction; templates are cheap to regenerate).",
                )

    # Subtomo crop sanity
    #
    # Crop ratio thresholds (per JOURNEY_CANDIDATE_METRICS.md):
    #   red    crop < diameter  (particle clipped — absolute floor)
    #   red    crop > box       (invalid; crop must fit inside box)
    #   amber  crop / diameter  < 1.2× (tight; no margin for shifts)
    #   green  crop / diameter ≥ 1.5×
    for r in rows:
        if r["stage_key"] != "subtomo":
            continue
        crop = r.get("_crop_px")
        box = r.get("box_px")
        eff_px = r.get("px_size_ang")
        diameter = r.get("particle_diameter_ang")
        if box and crop is not None and crop > box:
            r["warnings"]["crop"] = (
                "error",
                f"crop ({crop} px) > box ({box} px) — invalid; crop must fit within the box.",
            )
            continue
        # Cropped volume must contain the particle (≥ 1× diameter is the
        # absolute floor; below that, the particle doesn't fit in the cropped
        # output cube and gets clipped). Between 1.0–1.2× is the "tight,
        # no margin" warn zone; ≥ 1.5× is the comfortable target.
        if crop and eff_px and diameter:
            crop_ang = crop * eff_px
            ratio = crop_ang / diameter
            if ratio < 1.0:
                r["warnings"]["crop"] = (
                    "error",
                    f"crop {crop_ang:g} Å ({crop} px) < particle diameter {diameter:g} Å — "
                    f"particle won't fit in the cropped subtomogram. Increase crop_size.",
                )
            elif ratio < 1.2:
                r["warnings"]["crop"] = (
                    "warn",
                    f"crop {crop_ang:g} Å is {ratio:.2f}× particle diameter {diameter:g} Å — "
                    f"tight; only {(ratio - 1) * 50:.0f}% margin per side around the particle. "
                    f"Refine3D shifts may clip. Aim for ≥ 1.5×.",
                )


# --- Sanity-table renderers --------------------------------------------------


def _fmt_px(v: Optional[float]) -> str:
    return "—" if not v else f"{v:g}"


def _fmt_dims_px(d: Optional[tuple]) -> str:
    if d is None:
        return "—"
    parts = [str(x) for x in d if x is not None]
    return " × ".join(parts) if parts else "—"


def _fmt_dims_ang(d: Optional[tuple], px: Optional[float]) -> str:
    if d is None or not px:
        return "—"
    vals = [int(round(x * px)) for x in d if x is not None]
    return " × ".join(f"{v:,}" for v in vals) if vals else "—"


def _fmt_box_combined(r: dict) -> str:
    """`128 px (794 Å)` or `—`. Single column, both units inline."""
    bp = r.get("box_px")
    ba = r.get("box_ang")
    if bp is None and ba is None:
        return "—"
    if bp is None:
        return f"{ba:g} Å"
    if ba is None:
        return f"{bp} px"
    return f"{bp} px ({ba:g} Å)"


def _fmt_crop_combined(r: dict) -> str:
    cp = r.get("_crop_px")
    eff_px = r.get("px_size_ang")
    if not cp:
        return "—"
    if eff_px:
        return f"{cp} px ({cp * eff_px:g} Å)"
    return f"{cp} px"


def _fmt_particle(r: dict) -> str:
    d = r.get("particle_diameter_ang")
    if not d:
        return "—"
    return f"{d:g} Å"


def _pixel_cell(text: str, warning: Optional[tuple[str, str]] = None, *, notes: bool = False) -> None:
    cls = "cb-pixel-cell"
    if notes:
        cls += " cb-pixel-notes"
    if warning:
        cls += f" cb-pixel-warn-{warning[0]}"
    with ui.element("div").classes(cls):
        ui.label(text)
        if warning:
            icon = "error" if warning[0] == "error" else "warning_amber"
            ui.icon(icon, size="12px").classes("cb-pixel-warn-icon").tooltip(warning[1])


_PIXEL_COLUMNS: list[tuple[str, str, str]] = [
    ("stage", "stage", ""),
    ("px", "Å/px", "Pixel size at this stage."),
    ("dim_px", "tomo (px)", "Tomogram (or detector) dimensions in voxels at this stage's pixel size."),
    (
        "dim_ang",
        "tomo (Å)",
        "Physical size of the imaged volume in Å. Approximately invariant across stages — "
        "only the px sampling changes with binning.",
    ),
    (
        "box",
        "box",
        "Template-volume box (TM) or particle subtomo box (Subtomo). Shown as 'N px (M Å)'. "
        "Flagged when the Å dimension is outside 1.5–3× of particle diameter.",
    ),
    (
        "crop",
        "crop",
        "Cropped subtomogram size — Subtomo Extract only. crop_size = -1 in config means 'no cropping'. "
        "Shown as 'N px (M Å)'.",
    ),
    (
        "particle",
        "particle",
        "Particle diameter (Å) — set on Candidate Extract. Cross-applies to TM and Subtomo rows for "
        "the box-vs-particle sanity check (since the box must contain the particle plus margin).",
    ),
    ("notes", "notes", ""),
]

_UNIVERSAL_STAGE_KEYS = {"camera", "fs_ctf", "tilt_filter", "align", "ts_ctf", "recon"}


def _render_pixel_row_cells(r: dict) -> None:
    """Emit the row cells for one row inside the surrounding `cb-pixel-table`.
    Column count must match `_PIXEL_COLUMNS`."""
    stripe = r.get("species_color")
    for col_key, _label, _hint in _PIXEL_COLUMNS:
        warning = r["warnings"].get(col_key)
        if col_key == "stage":
            cls = "cb-pixel-cell"
            if warning:
                cls += f" cb-pixel-warn-{warning[0]}"
            with ui.element("div").classes(cls):
                ui.element("span").classes("cb-pixel-stripe").style(f"background:{stripe};" if stripe else "")
                ui.label(r["stage_label"]).classes("cb-pixel-stage-label")
                if r.get("instance_id"):
                    ui.label(r["instance_id"]).classes("cb-pixel-instance-label")
        elif col_key == "px":
            _pixel_cell(_fmt_px(r["px_size_ang"]), warning)
        elif col_key == "dim_px":
            _pixel_cell(_fmt_dims_px(r["tomo_px"]), warning)
        elif col_key == "dim_ang":
            _pixel_cell(_fmt_dims_ang(r["tomo_px"], r["px_size_ang"]), warning)
        elif col_key == "box":
            _pixel_cell(_fmt_box_combined(r), warning)
        elif col_key == "crop":
            _pixel_cell(_fmt_crop_combined(r), warning)
        elif col_key == "particle":
            _pixel_cell(_fmt_particle(r), warning)
        elif col_key == "notes":
            _pixel_cell(" · ".join(r["notes"]) if r["notes"] else "—", warning, notes=True)


def _render_pixel_header_cells() -> None:
    for _, label, hint in _PIXEL_COLUMNS:
        with ui.element("div").classes("cb-pixel-cell cb-pixel-header"):
            ui.label(label)
            if hint:
                ui.icon("info_outline", size="11px").classes("cb-pixel-warn-icon").tooltip(hint)


def _group_rows_by_species(rows: list[dict]) -> tuple[list[dict], list[tuple[Optional[str], list[dict]]]]:
    """Split into (universal_rows, [(species_id, species_rows), ...]).
    Universal stages share one table; per-species stages each get their own
    sub-table so multi-species projects stay readable."""
    universal: list[dict] = []
    by_species: dict[Optional[str], list[dict]] = {}
    species_order: list[Optional[str]] = []
    for r in rows:
        if r["stage_key"] in _UNIVERSAL_STAGE_KEYS:
            universal.append(r)
            continue
        sid = r.get("species_id")
        if sid not in by_species:
            species_order.append(sid)
            by_species[sid] = []
        by_species[sid].append(r)
    return universal, [(sid, by_species[sid]) for sid in species_order]


def _render_pixel_sanity_table(rows: list[dict]) -> None:
    """Dense monospace table. Universal stages (Camera → Recon) at the top,
    per-species stages (TM, Pick, Subtomo) underneath, separated by a
    full-width species marker row. All rows share one CSS Grid so columns
    line up across species. Wrapper has `overflow-x: auto` for narrow
    viewports. Sanity-rule violations surface as per-cell icons with
    tooltips."""
    if not rows:
        return

    with ui.element("div").classes("cb-pixel-section-title"):
        ui.icon("rule", size="13px").classes("text-indigo-600")
        ui.label("Pixel / binning sanity")
        ui.icon("info_outline", size="11px").classes("cb-pixel-warn-icon").tooltip(
            "Per-stage pixel size, tomogram dimensions, and template/extract/subtomo "
            "box + padding. Inline warnings flag binning-arithmetic mistakes "
            "(particle won't fit in box, template px ≠ recon px, etc.). "
            "Per-particle stages (TM, Pick, Subtomo) appear under a species marker; "
            "the table scrolls horizontally on narrow viewports."
        )

    universal, species_groups = _group_rows_by_species(rows)

    with ui.element("div").classes("cb-pixel-table-wrapper"):
        with ui.element("div").classes("cb-pixel-table"):
            _render_pixel_header_cells()
            for r in universal:
                _render_pixel_row_cells(r)
            for sid, group_rows in species_groups:
                first = group_rows[0]
                species_name = first.get("species_name") or sid or "unspecified"
                species_color = first.get("species_color") or "#94a3b8"
                with ui.element("div").classes("cb-pixel-species-row"):
                    ui.element("span").classes("cb-pixel-stripe").style(f"background:{species_color};")
                    ui.label(str(species_name)).classes("cb-pixel-species-name")
                    if sid and species_name != sid:
                        ui.label(f"({sid})").classes("cb-pixel-species-id")
                for r in group_rows:
                    _render_pixel_row_cells(r)


# ---------------------------------------------------------------------------
# Generic chip renderer + stage-0 chips
# ---------------------------------------------------------------------------


def _render_chip(
    label: str,
    value: str,
    *,
    status: str = "neutral",
    tooltip: Optional[str] = None,
    icon: Optional[str] = None,
) -> None:
    """One status chip. `status` ∈ {ok, warn, error, info, neutral}."""
    cls = f"cb-chip cb-chip-{status}"
    with ui.element("span").classes(cls) as chip:
        if icon:
            ui.icon(icon, size="11px").classes("cb-chip-icon")
        ui.label(label).classes("cb-chip-label")
        ui.label(value).classes("cb-chip-value")
        if tooltip:
            chip.tooltip(tooltip)


def _read_tomohand_from_import_star(star_path: Path) -> Optional[int]:
    """Return `_rlnTomoHand` from an Import-job tilt_series.star, sampling the
    first data table that carries it. Returns ±1 or None on absence."""
    if not star_path.exists():
        return None
    try:
        import starfile

        data = starfile.read(star_path, always_dict=True)
    except Exception as e:
        logger.warning("Could not read TomoHand from %s: %s", star_path, e)
        return None
    for v in data.values():
        if isinstance(v, pd.DataFrame) and "rlnTomoHand" in v.columns:
            vals = pd.to_numeric(v["rlnTomoHand"], errors="coerce").dropna().unique().tolist()
            if not vals:
                continue
            # Mixed values across TS are unusual but possible — surface +1/-1
            # as a magnitude (sign of the first) when uniform, else 0 sentinel.
            uniq = sorted({int(round(x)) for x in vals})
            if len(uniq) == 1:
                return int(uniq[0])
            return 0  # mixed
    return None


def _find_import_job(project_state) -> Optional[tuple[str, object]]:
    """Locate the Import (relion.importtomo) job. The dataset chip needs it
    to cross-check the in-memory `invert_defocus_hand` against the actual
    `_rlnTomoHand` Import wrote into `tilt_series.star`."""
    return _find_job_by_type(project_state, JobType.IMPORT_MOVIES)


def _render_stage0_chips(project_state, project_path: Path) -> None:
    """Per-project chips that summarize import-time/microscope choices that
    silently change downstream science. Currently: TomoHand (chirality /
    depth-dependent defocus sign). Empty container if nothing to show."""
    from services.models_base import AcquisitionParams

    acq = project_state.acquisition
    # In-memory intention. invert_defocus_hand=True → flip_tiltseries_hand=Yes
    # → TomoHand=-1; False → TomoHand=+1. Source: ImportMoviesParams._get_job_specific_options.
    config_hand = -1 if bool(acq.invert_defocus_hand) else 1

    # On-disk: read tilt_series.star from the Import job, if it exists.
    disk_hand: Optional[int] = None
    imp = _find_import_job(project_state)
    if imp:
        imp_dir = _job_dir_for(imp[0], imp[1], project_path)
        if imp_dir:
            disk_hand = _read_tomohand_from_import_star(imp_dir / "tilt_series.star")

    # Stale-default: compare to the AcquisitionParams field default. Catches
    # projects created before the 2026-05-16 invert_defocus_hand=True flip.
    default_hand = -1 if bool(AcquisitionParams.model_fields["invert_defocus_hand"].default) else 1

    # Status logic:
    #   error  — disk_hand exists and disagrees with config_hand (Import was run
    #            with one setting, the project then edited it; downstream jobs
    #            will use the disk value but the user thinks otherwise)
    #   warn   — config differs from the current code default (drift candidate)
    #   warn   — disk_hand == 0 (mixed across tilt-series; rare, usually
    #            indicates a double-imported project)
    #   ok     — everything aligned
    if disk_hand == 0:
        status = "warn"
        value = "mixed"
        tooltip = (
            "Different `_rlnTomoHand` values across tilt-series in this project's "
            "Import output. Usually means the project was double-imported with "
            "different invert_defocus_hand settings. Verify by re-importing."
        )
    elif disk_hand is not None and disk_hand != config_hand:
        status = "error"
        value = f"{disk_hand:+d}"
        tooltip = (
            f"Import wrote _rlnTomoHand={disk_hand:+d} into tilt_series.star "
            f"(this is what downstream jobs will use), but project_params.json now "
            f"declares invert_defocus_hand={acq.invert_defocus_hand} → expected "
            f"{config_hand:+d}. Re-run Import to align the two, or revert the config."
        )
    elif config_hand != default_hand:
        status = "warn"
        value = f"{config_hand:+d}"
        tooltip = (
            f"_rlnTomoHand={config_hand:+d} (from invert_defocus_hand="
            f"{acq.invert_defocus_hand}). Current code default would give "
            f"{default_hand:+d} — verify this project's value is intentional. "
            f"_rlnTomoHand is the sign convention for depth-dependent defocus in "
            f"RELION's CTF correction (see HANDOFF_412_DEBUG.md)."
        )
    else:
        status = "ok"
        value = f"{config_hand:+d}"
        src = "from Import output" if disk_hand is not None else "from acquisition config"
        tooltip = (
            f"_rlnTomoHand={config_hand:+d} ({src}). Sign convention for "
            f"depth-dependent defocus in CTF correction. The Klumpe-lab Titan "
            f"convention is -1 (invert_defocus_hand=True)."
        )

    with ui.element("div").classes("cb-chip-strip"):
        _render_chip("TomoHand", value, status=status, tooltip=tooltip, icon="compare_arrows")


def _render_dataset_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Project-wide acquisition + microscope settings + pixel/binning sanity table.
    Two-column key/val grid above; below it, a dense per-stage table showing
    pixel size, tomo dims, and template/extract/subtomo box + padding with
    inline sanity-rule warnings (ROADMAP §11). A chip strip at the top
    surfaces import-time choices that silently change downstream science
    (TomoHand, etc.)."""
    ms = project_state.microscope
    acq = project_state.acquisition

    metric_parts = [f"{ms.pixel_size_angstrom:g} Å/px", f"{int(ms.acceleration_voltage_kv)} kV"]
    if acq.dose_per_tilt:
        metric_parts.append(f"{acq.dose_per_tilt:g} e⁻/Å²/tilt")
    if acq.tilt_axis_degrees is not None:
        metric_parts.append(f"axis {acq.tilt_axis_degrees:g}°")
    if project_state.import_total_tilt_series:
        metric_parts.append(f"{project_state.import_selected_tilt_series}/{project_state.import_total_tilt_series} TS")

    rows: list[tuple[str, str]] = [
        ("microscope", getattr(ms.microscope_type, "value", str(ms.microscope_type))),
        ("voltage", f"{ms.acceleration_voltage_kv:g} kV"),
        ("Cs", f"{ms.spherical_aberration_mm:g} mm"),
        ("ampl. contrast", f"{ms.amplitude_contrast:g}"),
        ("pixel size", f"{ms.pixel_size_angstrom:g} Å"),
        ("dose / tilt", f"{acq.dose_per_tilt:g} e⁻/Å²" if acq.dose_per_tilt else "—"),
        ("tilt axis", f"{acq.tilt_axis_degrees:g}°"),
        ("detector", f"{acq.detector_dimensions[0]}×{acq.detector_dimensions[1]} px"),
        ("acq. software", acq.acquisition_software or "—"),
        ("sample thickness", f"{acq.sample_thickness_nm:g} nm" if acq.sample_thickness_nm else "—"),
    ]
    if acq.eer_fractions_per_frame:
        rows.append(("EER fractions/frame", str(acq.eer_fractions_per_frame)))
    if acq.invert_tilt_angles:
        rows.append(("invert tilts", "yes"))
    if acq.invert_defocus_hand:
        rows.append(("invert defocus hand", "yes"))

    pixel_rows = _compute_pixel_chain(project_state)
    _apply_sanity_rules(pixel_rows)

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "dataset"
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("memory", size="14px").classes("text-indigo-600")
            ui.label("Dataset").classes("cb-section-title")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
        _render_stage0_chips(project_state, project_path)
        # 2-col grid: pairs of (key, val, key, val) per visual row.
        with ui.element("div").classes("cb-datadump-grid-2col"):
            for k, v in rows:
                ui.label(k).classes("cb-datadump-key")
                ui.label("—" if v is None or v == "" else str(v)).classes("cb-datadump-val")
        _render_pixel_sanity_table(pixel_rows)

    return True


# --- Plot helpers that run inside an existing card body ----------------------


_DEFOCUS_U_COLOR = "#4338ca"  # indigo-700
_DEFOCUS_V_COLOR = "#a855f7"  # purple-500
_CTF_RES_COLOR = "#059669"  # emerald-600
_CTF_FOM_COLOR = "#0ea5e9"  # sky-500
_MOTION_TOTAL_COLOR = "#d97706"  # amber-600
_MOTION_EARLY_COLOR = "#fb923c"  # orange-400
_MOTION_LATE_COLOR = "#ea580c"  # orange-600
_X_SHIFT_COLOR = "#0891b2"  # cyan-600
_Y_SHIFT_COLOR = "#7c3aed"  # violet-600


def _stat_strip(rows: list[tuple[str, str]]) -> None:
    """Tiny inline stats line: `key val · key val ...`"""
    if not rows:
        return
    with ui.element("div").classes("cb-stat-strip"):
        for k, v in rows:
            with ui.element("span"):
                ui.html(f"<span class='cb-stat-key'>{k}</span><span class='cb-stat-val'>{v}</span>", sanitize=False)


def _plot_cell(label: str, fig: dict, *, height_px: int = 220, wide: bool = False, hint: Optional[str] = None) -> None:
    """One plot tile inside a `.cb-plot-row` parent. `hint` is a short tooltip
    explainer attached to the title (helps newcomers parse the metric).

    Default height bumped to 220px (from 150) to give the markers vertical
    breathing room — at 150 the dots crowd together and small spreads vanish.
    """
    cls = "cb-plot-cell cb-plot-cell-wide" if wide else "cb-plot-cell"
    with ui.element("div").classes(cls):
        with ui.row().classes("items-center gap-1").style("padding: 1px 4px 0;"):
            lbl = ui.label(label).classes("cb-plot-label").style("padding: 0;")
            if hint:
                lbl.tooltip(hint)
                ui.icon("info_outline", size="11px").classes("text-gray-400 cursor-help").tooltip(hint)
        ui.plotly(fig).style(f"width: 100%; height: {height_px}px;")


def _per_tilt_customdata(df: pd.DataFrame) -> list[list]:
    """Build [[tilt_index, frame_basename], ...] customdata so plot hovers
    can name the specific tilt instead of just its angle."""
    n = int(len(df))
    if "rlnMicrographMovieName" in df.columns:
        bases = [Path(str(v)).name for v in df["rlnMicrographMovieName"].tolist()]
    else:
        bases = [""] * n
    return [[i + 1, bases[i]] for i in range(n)]


# One-line explainers attached to plot titles. Defocus / astig / shifts are
# domain-jargon; quick tooltips let newcomers parse the dashboard without
# leaving the page.
_HINT_DEFOCUS = (
    "Per-tilt astigmatic CTF defocus. Defocus U = long-axis (more underfocus), "
    "V = short-axis. Mean ≈ (U+V)/2 is the conventionally reported defocus; "
    "spread between U and V is astigmatism."
)
_HINT_ASTIG = (
    "Magnitude of CTF astigmatism (|U − V|, Å). Large astig widens CTF zeros and reduces achievable resolution."
)
_HINT_CTF_RES = "Best resolution (Å) at which CTF zeros could be fit. Lower = better fit / more usable signal."
_HINT_CTF_FOM = "CTF fit figure-of-merit, dimensionless 0..1. Higher = more confident fit."
_HINT_MOTION = (
    "Accumulated beam-induced motion (Å) summed over the tilt's movie frames. Early = first half, late = second half."
)
_HINT_SHIFT = (
    "Per-tilt translation in Å applied during alignment to register each tilt to a common reference. "
    "A spike means a tilt is hard to align (often: contamination, charging, or ice motion)."
)
_HINT_ALIGN_ANGLES = (
    "Refined per-tilt rotational corrections. X tilt − nominal = how far the refit moved the stage tilt; "
    "Y tilt and Z rot are the secondary tilt-axis and in-plane rotation."
)


def _render_ctf_motion_plots(df: pd.DataFrame, *, show_motion: bool = True) -> None:
    """Defocus + astigmatism (always plotted as scatter, since each tilt is an
    independent estimate). CTF max-resolution / FOM / motion are gated on
    `_is_meaningful_series` because WarpTools-exported RELION stars often
    write `1e-6` placeholders for those columns — see
    `project_warp_relion_star_placeholders.md`."""
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    cd = _per_tilt_customdata(df)

    has_def = "rlnDefocusU" in df.columns and "rlnDefocusV" in df.columns
    has_astig = "rlnCtfAstigmatism" in df.columns
    has_res = "rlnCtfMaxResolution" in df.columns
    has_fom = "rlnCtfFigureOfMerit" in df.columns
    has_motion_total = "rlnAccumMotionTotal" in df.columns

    skipped: list[str] = []

    with ui.element("div").classes("cb-plot-row"):
        if has_def:
            du = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusU"])]
            dv = [None if v is None else v / 1.0e4 for v in _safe_floats(df["rlnDefocusV"])]
            fig = _build_per_tilt_chart(
                tilts,
                [
                    {"name": "Defocus U", "y": du, "color": _DEFOCUS_U_COLOR, "marker_size": 7},
                    {"name": "Defocus V", "y": dv, "color": _DEFOCUS_V_COLOR, "marker_size": 7},
                ],
                y_label="defocus (µm)",
                customdata=cd,
                y_unit=" µm",
                y_range=(0.0, 10.0),
            )
            _plot_cell("Defocus U / V per tilt", fig, hint=_HINT_DEFOCUS)
        if has_astig:
            astig = _safe_floats(df["rlnCtfAstigmatism"])
            if _is_meaningful_series(astig):
                fig = _build_per_tilt_chart(
                    tilts,
                    [{"name": "astig", "y": astig, "color": "#ec4899", "marker_size": 7}],
                    y_label="astigmatism (Å)",
                    customdata=cd,
                    y_unit=" Å",
                    y_range=(0.0, 1500.0),
                )
                _plot_cell("Astigmatism per tilt", fig, hint=_HINT_ASTIG)

    # CTF fit-quality plots: skip when WarpTools wrote placeholders.
    if has_res:
        res = _safe_floats(df["rlnCtfMaxResolution"])
        if _is_meaningful_series(res):
            with ui.element("div").classes("cb-plot-row"):
                fig = _build_per_tilt_chart(
                    tilts,
                    [{"name": "CTF max res", "y": res, "color": _CTF_RES_COLOR, "marker_size": 6}],
                    y_label="resolution (Å)",
                    customdata=cd,
                    y_unit=" Å",
                    y_range=(0.0, 30.0),
                )
                _plot_cell("CTF fit resolution per tilt", fig, hint=_HINT_CTF_RES)
        else:
            skipped.append("CTF max-res")
    if has_fom:
        fom = _safe_floats(df["rlnCtfFigureOfMerit"])
        if _is_meaningful_series(fom, threshold=1e-4):
            with ui.element("div").classes("cb-plot-row"):
                fig = _build_per_tilt_chart(
                    tilts,
                    [{"name": "FOM", "y": fom, "color": _CTF_FOM_COLOR, "marker_size": 6}],
                    y_label="FOM",
                    customdata=cd,
                    y_range=(0.0, 1.0),
                )
                _plot_cell("CTF figure of merit per tilt", fig, hint=_HINT_CTF_FOM)
        else:
            skipped.append("CTF FOM")

    # Motion: cumulative across frames within a tilt — *is* a meaningful curve
    # vs tilt order, so use lines+markers when it's not placeholder.
    if show_motion and has_motion_total:
        mt = _safe_floats(df["rlnAccumMotionTotal"])
        if _is_meaningful_series(mt, threshold=0.05):
            with ui.element("div").classes("cb-plot-row"):
                series = [{"name": "total", "y": mt, "color": _MOTION_TOTAL_COLOR, "mode": "lines+markers"}]
                if "rlnAccumMotionEarly" in df.columns:
                    series.append(
                        {
                            "name": "early",
                            "y": _safe_floats(df["rlnAccumMotionEarly"]),
                            "color": _MOTION_EARLY_COLOR,
                            "dash": "dot",
                            "mode": "lines+markers",
                        }
                    )
                if "rlnAccumMotionLate" in df.columns:
                    series.append(
                        {
                            "name": "late",
                            "y": _safe_floats(df["rlnAccumMotionLate"]),
                            "color": _MOTION_LATE_COLOR,
                            "dash": "dash",
                            "mode": "lines+markers",
                        }
                    )
                fig = _build_per_tilt_chart(tilts, series, y_label="accum. motion (Å)", customdata=cd, y_unit=" Å")
                _plot_cell("Beam-induced motion per tilt", fig, wide=True, hint=_HINT_MOTION)
        else:
            skipped.append("motion")

    if skipped:
        ui.label(
            f"Skipped: {', '.join(skipped)} — WarpTools writes placeholder values for these columns "
            "in its RELION star export (real numbers live in WarpTools' own metadata)."
        ).classes("cb-section-placeholder")


def _render_alignment_plots(df: pd.DataFrame) -> None:
    """Per-tilt shift magnitude, X/Y/Z angle deltas relative to nominal.

    Markers only — even for smoothly-varying metrics, connecting per-tilt
    estimates with lines turns outliers into zigzag and obscures the actual
    distribution (see `feedback_dashboard_plot_principles`).
    """
    tilts = _safe_floats(df["rlnTomoNominalStageTiltAngle"])
    cd = _per_tilt_customdata(df)
    has_shift = "rlnTomoXShiftAngst" in df.columns and "rlnTomoYShiftAngst" in df.columns
    has_xtilt = "rlnTomoXTilt" in df.columns
    has_ytilt = "rlnTomoYTilt" in df.columns
    has_zrot = "rlnTomoZRot" in df.columns

    with ui.element("div").classes("cb-plot-row"):
        if has_shift:
            xs = _safe_floats(df["rlnTomoXShiftAngst"])
            ys = _safe_floats(df["rlnTomoYShiftAngst"])
            mag = [(x * x + y * y) ** 0.5 if x is not None and y is not None else None for x, y in zip(xs, ys)]
            if _is_meaningful_series(mag):
                fig = _build_per_tilt_chart(
                    tilts,
                    [
                        {"name": "|shift|", "y": mag, "color": "#1d4ed8"},
                        {"name": "X shift", "y": xs, "color": _X_SHIFT_COLOR},
                        {"name": "Y shift", "y": ys, "color": _Y_SHIFT_COLOR},
                    ],
                    y_label="shift (Å)",
                    customdata=cd,
                    y_unit=" Å",
                )
                _plot_cell("Refined shift per tilt", fig, hint=_HINT_SHIFT)
        if has_xtilt or has_ytilt or has_zrot:
            series = []
            if has_xtilt:
                xt = _safe_floats(df["rlnTomoXTilt"])
                resid = [val - nt if nt is not None and val is not None else None for nt, val in zip(tilts, xt)]
                if _is_meaningful_series(resid):
                    series.append({"name": "X tilt − nom", "y": resid, "color": "#dc2626"})
            if has_ytilt:
                yt = _safe_floats(df["rlnTomoYTilt"])
                if _is_meaningful_series(yt):
                    series.append({"name": "Y tilt", "y": yt, "color": "#f97316"})
            if has_zrot:
                zr = _safe_floats(df["rlnTomoZRot"])
                if _is_meaningful_series(zr):
                    series.append({"name": "Z rot", "y": zr, "color": "#0ea5e9"})
            if series:
                fig = _build_per_tilt_chart(tilts, series, y_label="angle (°)", customdata=cd, y_unit="°")
                _plot_cell("Refined alignment angles", fig, hint=_HINT_ALIGN_ANGLES)


def _render_fs_motion_ctf_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = _find_job_by_type(project_state, JobType.FS_MOTION_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    metric_parts = [f"motion {jm.m_grid}", f"bfac {jm.m_bfac}", f"ctf {jm.c_range_min_max} Å", f"win {jm.c_window}"]
    param_rows = [
        ("motion range", jm.m_range_min_max),
        ("motion grid", jm.m_grid),
        ("motion bfac", str(jm.m_bfac)),
        ("ctf range", jm.c_range_min_max),
        ("ctf grid", jm.c_grid),
        ("ctf window", str(jm.c_window)),
        ("defocus search", f"{jm.c_defocus_min_max} µm"),
        ("phase shift", "yes" if jm.do_phase else "no"),
        ("avg halves", "yes" if jm.out_average_halves else "no"),
        ("skip first / last", f"{jm.out_skip_first} / {jm.out_skip_last}"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "fs_motion_ctf",
            "speed",
            "FS Motion / CTF",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "fs_motion_ctf"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("speed", size="14px").classes("text-indigo-600")
            ui.label("FS Motion / CTF").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        ctf_res = [v for v in _safe_floats(df.get("rlnCtfMaxResolution", [])) if v is not None]
        motion_total = [v for v in _safe_floats(df.get("rlnAccumMotionTotal", [])) if v is not None]
        d_stats = _stats(defocus_um)
        r_stats = _stats(ctf_res)
        m_stats = _stats(motion_total)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (Q1 {d_stats['q1']:.2f} · Q3 {d_stats['q3']:.2f})")
            )
        if r_stats["n"]:
            strip_rows.append(("CTF res", f"{r_stats['median']:.1f} Å (worst {r_stats['max']:.1f})"))
        if m_stats["n"]:
            strip_rows.append(("motion (max)", f"{m_stats['max']:.1f} Å"))
        _stat_strip(strip_rows)

        _render_ctf_motion_plots(df, show_motion=True)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


def _render_ts_alignment_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = _find_job_by_type(project_state, JobType.TS_ALIGNMENT)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    method = getattr(jm.alignment_method, "value", str(jm.alignment_method))
    metric_parts = [
        f"{method}",
        f"{jm.rescale_angpixs:g} Å/px",
        jm.tomo_dimensions,
        f"thick {jm.sample_thickness_nm:g} nm",
    ]
    param_rows = [
        ("method", method),
        ("rescale", f"{jm.rescale_angpixs:g} Å/px"),
        ("tomo dims", jm.tomo_dimensions),
        ("sample thickness", f"{jm.sample_thickness_nm:g} nm"),
        ("patch X / Y", f"{jm.patch_x} / {jm.patch_y}"),
        ("axis iter / batch", f"{jm.axis_iter} / {jm.axis_batch}"),
        ("imod patch / overlap", f"{jm.imod_patch_size} / {jm.imod_overlap}"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "ts_alignment",
            "straighten",
            "TS Alignment",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "ts_alignment"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("straighten", size="14px").classes("text-indigo-600")
            ui.label("TS Alignment").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        # Stat strip: max shift magnitude + tilt-axis residual range
        x_shift = _safe_floats(df.get("rlnTomoXShiftAngst", [])) if "rlnTomoXShiftAngst" in df.columns else []
        y_shift = _safe_floats(df.get("rlnTomoYShiftAngst", [])) if "rlnTomoYShiftAngst" in df.columns else []
        mag = [(x * x + y * y) ** 0.5 for x, y in zip(x_shift, y_shift) if x is not None and y is not None]
        m_stats = _stats(mag)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
        if m_stats["n"]:
            strip_rows.append(("|shift| max / median", f"{m_stats['max']:.1f} / {m_stats['median']:.1f} Å"))
        if "rlnTomoYTilt" in df.columns:
            yt = [v for v in _safe_floats(df["rlnTomoYTilt"]) if v is not None]
            if yt:
                yt_stats = _stats(yt)
                strip_rows.append(("Y tilt range", f"{yt_stats['min']:.2f}° → {yt_stats['max']:.2f}°"))
        _stat_strip(strip_rows)

        _render_alignment_plots(df)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


def _render_ts_ctf_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    found = _find_job_by_type(project_state, JobType.TS_CTF)
    if not found:
        return False
    instance_id, jm = found
    job_dir = _job_dir_for(instance_id, jm, project_path)
    status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
    metric_parts = [
        f"defocus {jm.defocus_min_max} µm",
        f"range {jm.range_min_max} Å",
        f"win {jm.window}",
        f"hand {jm.defocus_hand}",
    ]
    param_rows = [
        ("range", f"{jm.range_min_max} Å"),
        ("defocus search", f"{jm.defocus_min_max} µm"),
        ("defocus hand", jm.defocus_hand),
        ("window", str(jm.window)),
        ("phase shift", "yes" if jm.do_phase else "no"),
        ("perdevice", str(jm.perdevice)),
    ]

    if job_dir is None:
        _render_datadump_card(
            "ts_ctf",
            "blur_on",
            "TS CTF (post-alignment)",
            " · ".join(metric_parts),
            instance_id,
            status_label,
            param_rows,
            note="Job hasn't started — outputs not on disk yet.",
        )
        return True

    df = _read_per_tilt_df(_per_tilt_star_path(job_dir, ts_name))
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "ts_ctf"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("blur_on", size="14px").classes("text-indigo-600")
            ui.label("TS CTF (post-alignment)").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if df is None:
            ui.label(f"No per-tilt star at tilt_series/{ts_name}.star yet.").classes("cb-section-placeholder")
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
            return True

        defocus_um = [v / 1.0e4 for v in _safe_floats(df.get("rlnDefocusU", [])) if v is not None]
        ctf_res = [v for v in _safe_floats(df.get("rlnCtfMaxResolution", [])) if v is not None]
        d_stats = _stats(defocus_um)
        r_stats = _stats(ctf_res)
        strip_rows: list[tuple[str, str]] = [("tilts", str(int(len(df))))]
        if d_stats["n"]:
            strip_rows.append(
                ("defocus", f"{d_stats['median']:.2f} µm (range {d_stats['min']:.2f}–{d_stats['max']:.2f})")
            )
        if r_stats["n"]:
            strip_rows.append(("CTF res", f"{r_stats['median']:.1f} Å (worst {r_stats['max']:.1f})"))
        _stat_strip(strip_rows)

        # Skip the motion plot here — TS CTF doesn't change per-tilt motion;
        # that's already shown in the FS Motion/CTF section above.
        _render_ctf_motion_plots(df, show_motion=False)

        with ui.expansion("Job parameters").classes("w-full text-[10px]").props("dense"):
            with ui.element("div").classes("cb-datadump-grid"):
                for k, v in param_rows:
                    ui.label(k).classes("cb-datadump-key")
                    ui.label(str(v)).classes("cb-datadump-val")
    return True


# --- Tilt Filter --------------------------------------------------------------


def _resolve_tilt_filter_dir(project_state, project_path: Path) -> Optional[Path]:
    """Return the directory containing tiltseries_filtered.star and
    tiltseries_labeled.star — supports both pipeline-job tilt filtering
    (TILT_FILTER) and the standalone TiltFilter tool that writes to
    `<project_root>/TiltFilter/`."""
    found = _find_job_by_type(project_state, JobType.TILT_FILTER)
    if found:
        instance_id, jm = found
        jd = _job_dir_for(instance_id, jm, project_path)
        if jd is not None:
            cand = jd / "filtered"
            if (cand / "tiltseries_labeled.star").exists() or (cand / "tiltseries_filtered.star").exists():
                return cand
    standalone = project_path / "TiltFilter"
    if (standalone / "tiltseries_labeled.star").exists() or (standalone / "tiltseries_filtered.star").exists():
        return standalone
    return None


def _read_per_tilt_frame_names(per_tilt_star: Path) -> list[str]:
    df = _read_per_tilt_df(per_tilt_star)
    if df is None or "rlnMicrographMovieName" not in df.columns:
        return []
    return [str(v) for v in df["rlnMicrographMovieName"].tolist()]


def _read_per_tilt_kept_dropped(filter_dir: Path, ts_name: str) -> Optional[dict]:
    """Diff labeled vs filtered per-tilt star to compute kept and dropped rows.

    Returns a dict with keys: n_labeled, n_kept, dropped (list of dicts with
    `index`, `tilt_angle`, `frame`). None if the labeled file is missing."""
    labeled_p = filter_dir / "tilt_series_labeled" / f"{ts_name}.star"
    filtered_p = filter_dir / "tilt_series_filtered" / f"{ts_name}.star"
    labeled_df = _read_per_tilt_df(labeled_p)
    if labeled_df is None:
        return None
    kept_frames: set[str] = set()
    if filtered_p.exists():
        kept_frames = set(_read_per_tilt_frame_names(filtered_p))
    n_labeled = int(len(labeled_df))
    dropped: list[dict] = []
    if "rlnMicrographMovieName" in labeled_df.columns and kept_frames:
        for i, row in labeled_df.iterrows():
            frame = str(row["rlnMicrographMovieName"])
            if frame in kept_frames:
                continue
            tilt_angle = None
            if "rlnTomoNominalStageTiltAngle" in labeled_df.columns:
                try:
                    tilt_angle = float(row["rlnTomoNominalStageTiltAngle"])
                except (TypeError, ValueError):
                    tilt_angle = None
            dropped.append({"index": int(i), "tilt_angle": tilt_angle, "frame": Path(frame).name})
    n_kept = n_labeled - len(dropped) if kept_frames else n_labeled
    return {"n_labeled": n_labeled, "n_kept": n_kept, "dropped": dropped, "labeled_df": labeled_df}


def _render_tilt_filter_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Per-TS tilt-filter diagnostics. Renders for either:
      - a TILT_FILTER pipeline job (uses its `filtered/` subdir), or
      - the standalone TiltFilter tool output at `<project_root>/TiltFilter/`.
    Skips silently when neither is present."""
    filter_dir = _resolve_tilt_filter_dir(project_state, project_path)
    if filter_dir is None:
        return False

    job_found = _find_job_by_type(project_state, JobType.TILT_FILTER)
    instance_id = job_found[0] if job_found else "TiltFilter (standalone)"
    jm = job_found[1] if job_found else None

    metric_parts = []
    if jm is not None:
        metric_parts = [f"model {jm.model_name}", f"thresh {jm.prob_threshold:g}", f"action {jm.prob_action}"]

    info = _read_per_tilt_kept_dropped(filter_dir, ts_name)
    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "tilt_filter"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("tune", size="14px").classes("text-indigo-600")
            ui.label("Tilt Filter").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_parts:
                ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            if jm is not None:
                status_label = getattr(jm.execution_status, "value", str(jm.execution_status))
                if status_label.lower() != "succeeded":
                    ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if info is None:
            ui.label(f"No labeled tilt list for {ts_name} in {filter_dir}").classes("cb-section-placeholder")
            return True

        n_labeled = info["n_labeled"]
        n_kept = info["n_kept"]
        n_dropped = len(info["dropped"])
        kept_pct = (100.0 * n_kept / n_labeled) if n_labeled else 0.0
        strip_rows = [("kept", f"{n_kept}/{n_labeled}  ({kept_pct:.0f}%)"), ("dropped", str(n_dropped))]
        if jm is not None:
            strip_rows.append(("manual labels", str(len(jm.tilt_labels))))
        _stat_strip(strip_rows)

        if info["dropped"]:
            with ui.expansion(f"{n_dropped} dropped tilt(s)", value=True).classes("w-full text-[10px]").props("dense"):
                with ui.element("div").classes("cb-drop-list"):
                    for d in info["dropped"]:
                        with ui.element("div").classes("cb-drop-row"):
                            tilt_str = f"{d['tilt_angle']:+.2f}°" if d["tilt_angle"] is not None else "?°"
                            ui.label(tilt_str).classes("cb-drop-tilt")
                            ui.label(d["frame"])

        if jm is not None:
            param_rows = [
                ("model", jm.model_name),
                ("image size", str(jm.image_size)),
                ("dl batch size", str(jm.dl_batch_size)),
                ("prob threshold", f"{jm.prob_threshold:g}"),
                ("prob action", jm.prob_action),
            ]
            with ui.expansion("Filter parameters").classes("w-full text-[10px]").props("dense"):
                with ui.element("div").classes("cb-datadump-grid"):
                    for k, v in param_rows:
                        ui.label(k).classes("cb-datadump-key")
                        ui.label(str(v)).classes("cb-datadump-val")

    return True


# ---------------------------------------------------------------------------
# Reconstruct section: surfaces per-TS reconstructed-tomogram polarity.
#
# The template/mask polarity (BLACK vs WHITE) must match the tomogram
# polarity. If the WarpTools `TomoFullReconstructInvert` setting changes
# between runs or projects, the polarity chip flags the mismatch before
# TM produces meaningless CC scores. Reads a center 1024×1024 Z slice
# only — full-volume reads are forbidden per ROADMAP §4.1.
# ---------------------------------------------------------------------------


_TOMO_POLARITY_CACHE: dict[tuple[str, int], dict] = {}


def _compute_tomogram_polarity(mrc_path: Path) -> Optional[dict]:
    """Sample a center 1024×1024 Z slice from a reconstructed tomogram,
    compute %bright / %dark voxel fractions, classify polarity. Cached by
    (path, mtime). Returns None on read failure or non-3D volumes."""
    try:
        st = mrc_path.stat()
    except OSError:
        return None
    key = (str(mrc_path), int(st.st_mtime))
    cached = _TOMO_POLARITY_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        import mrcfile
        import numpy as np

        with mrcfile.mmap(str(mrc_path), mode="r") as m:
            data = m.data
            if data.ndim != 3:
                return None
            nz, ny, nx = data.shape
            cz, cy, cx = nz // 2, ny // 2, nx // 2
            half = 512
            y0, y1 = max(0, cy - half), min(ny, cy + half)
            x0, x1 = max(0, cx - half), min(nx, cx + half)
            # Materialize a copy so the array survives the mmap close
            # (ROADMAP §4.5 mmap view trap).
            slab = np.array(data[cz, y0:y1, x0:x1], dtype=np.float32, copy=True)
    except Exception as e:
        logger.warning("Could not read tomogram %s for polarity: %s", mrc_path, e)
        return None

    if slab.size == 0:
        return None
    mean = float(slab.mean())
    std = float(slab.std()) or 1.0
    upper = mean + 1.5 * std
    lower = mean - 1.5 * std
    n = float(slab.size)
    pct_bright = 100.0 * float((slab > upper).sum()) / n
    pct_dark = 100.0 * float((slab < lower).sum()) / n

    # Classify. Margins picked from the doc example: GT had 6.6%/6.8% =
    # essentially symmetric. >1.5x ratio + >8% absolute = clear skew.
    if pct_bright > 1.5 * pct_dark and pct_bright > 8.0:
        polarity = "bright"
    elif pct_dark > 1.5 * pct_bright and pct_dark > 8.0:
        polarity = "dark"
    else:
        polarity = "symmetric"

    result = {
        "pct_bright": pct_bright,
        "pct_dark": pct_dark,
        "polarity": polarity,
        "slab_shape": list(slab.shape),
    }
    _TOMO_POLARITY_CACHE[key] = result
    return result


def _expected_polarity_from_templates(project_state) -> Optional[str]:
    """Return the consensus selected-template polarity across species
    ("white" or "black") if every species agrees, else None."""
    seen: set[str] = set()
    for sp in project_state.species_registry or []:
        tpl = sp.get_selected_template() if hasattr(sp, "get_selected_template") else None
        pol = getattr(tpl, "polarity", None) if tpl else None
        if pol:
            seen.add(str(pol).lower())
    if len(seen) == 1:
        return next(iter(seen))
    return None


def _tomo_polarity_chip_status(polarity: str, expected: Optional[str]) -> tuple[str, str]:
    """Return (status, hint) for the polarity chip."""
    if expected is None:
        if polarity == "symmetric":
            return "neutral", "Roughly symmetric distribution — typical reconstruction."
        return (
            "info",
            f"{polarity.capitalize()}-skewed reconstruction. Make sure your template polarity "
            f"({polarity}) matches this tomogram.",
        )
    if expected == "white" and polarity == "dark":
        return "error", (
            "Template polarity is WHITE (expects bright particles) but the reconstructed "
            "tomogram is DARK-skewed. PyTOM is matching inverted contrast — invert the "
            "template, the tomogram, or both, and re-run TM."
        )
    if expected == "black" and polarity == "bright":
        return "error", (
            "Template polarity is BLACK (expects dark particles) but the reconstructed "
            "tomogram is BRIGHT-skewed. PyTOM is matching inverted contrast — invert the "
            "template, the tomogram, or both, and re-run TM."
        )
    return "ok", f"Tomogram polarity ({polarity}) matches template polarity ({expected})."


def _render_reconstruct_section(ts_name: str, project_state, project_path: Path, refresh) -> bool:
    """Per-TS Reconstruct card. Currently surfaces tomogram polarity; the
    WarpTools PNG + X/Z slab + 3dmod copy command are still hosted in the
    Candidate Extract section (Slice B refactor pending)."""
    rec = _find_job_by_type(project_state, JobType.TS_RECONSTRUCT)
    if not rec:
        return False
    rec_iid, rec_jm = rec
    job_dir = _job_dir_for(rec_iid, rec_jm, project_path)
    if not job_dir:
        return False
    tomo_df = _read_tomograms_table(job_dir / "tomograms.star")
    if tomo_df is None or "rlnTomoName" not in tomo_df.columns:
        return False
    row = tomo_df[tomo_df["rlnTomoName"].astype(str) == ts_name]
    if row.empty:
        return False
    tomo_row = row.iloc[0]
    mrc_path = _resolve_volume_for_3dmod(tomo_row, project_path)

    metric_parts: list[str] = []
    rescale = float(getattr(rec_jm, "rescale_angpixs", 0.0) or 0.0)
    if rescale:
        metric_parts.append(f"{rescale:g} Å/px")
    if "rlnTomoTomogramBinning" in tomo_row.index:
        try:
            metric_parts.append(f"bin {float(tomo_row['rlnTomoTomogramBinning']):g}")
        except (TypeError, ValueError):
            pass

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "reconstruct"
        card._props["data-instance"] = rec_iid
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("view_in_ar", size="14px").classes("text-indigo-600")
            ui.label("Reconstruct").classes("cb-section-title")
            ui.label(rec_iid).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_parts:
                ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            status_label = getattr(rec_jm.execution_status, "value", str(rec_jm.execution_status))
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        if mrc_path is None or not Path(mrc_path).exists():
            with ui.element("div").classes("cb-chip-strip"):
                _render_chip(
                    "polarity",
                    "no MRC",
                    status="neutral",
                    tooltip="Reconstructed tomogram MRC not on disk for this TS — can't sample for polarity.",
                    icon="brightness_medium",
                )
            _render_recon_big_preview(ts_name, project_path, None)
            return True

        polarity = _compute_tomogram_polarity(Path(mrc_path))
        with ui.element("div").classes("cb-chip-strip"):
            if polarity is None:
                _render_chip(
                    "polarity",
                    "read error",
                    status="warn",
                    tooltip=f"Could not sample center Z slice of {mrc_path} (see server logs).",
                    icon="brightness_medium",
                )
            else:
                expected = _expected_polarity_from_templates(project_state)
                status, hint = _tomo_polarity_chip_status(polarity["polarity"], expected)
                _render_chip(
                    "polarity",
                    polarity["polarity"],
                    status=status,
                    tooltip=(
                        f"Reconstructed tomogram polarity: {polarity['polarity'].upper()} "
                        f"({polarity['pct_bright']:.1f}% bright vs {polarity['pct_dark']:.1f}% "
                        f"dark voxels in a center {polarity['slab_shape'][0]}×{polarity['slab_shape'][1]} "
                        f"Z slice). {hint}"
                    ),
                    icon="brightness_medium",
                )
                _render_chip(
                    "bright %",
                    f"{polarity['pct_bright']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels above mean + 1.5σ in the sampled Z slice.",
                )
                _render_chip(
                    "dark %",
                    f"{polarity['pct_dark']:.1f}",
                    status="neutral",
                    tooltip="Fraction of voxels below mean − 1.5σ in the sampled Z slice.",
                )
        _render_recon_big_preview(ts_name, project_path, Path(mrc_path))
    return True


def _render_recon_big_preview(ts_name: str, project_path: Path, mrc_path: Optional[Path]) -> None:
    """Viewport-filling WarpTools tomogram PNG. The PNG sits next to the
    .mrc as `<tomo>_<res>Apx.png`, written by ts_reconstruct as a side
    effect. Rendered with `object-fit: contain` so the natural aspect
    ratio (typically wide XY top-down) survives a tall viewport."""
    png_path = _find_warp_tomo_preview(project_path, ts_name, mrc_path)
    if not png_path or not png_path.exists():
        ui.label("No WarpTools preview PNG on disk for this tomogram.").classes("cb-section-placeholder")
        return
    url = _vis_asset_url(str(png_path))
    with ui.element("div").classes("cb-recon-preview"):
        ui.html(
            f"<img src='{url}' alt='{ts_name} WarpTools tomogram preview' />",
            sanitize=False,
        )
    ui.label(f"WarpTools preview · {png_path.name}").classes("cb-recon-preview-caption")


# ---------------------------------------------------------------------------
# Template Match section: declared-vs-applied symmetry parity (and, when
# combined with Slice 5, template + mask intrinsic-shape chips). One card
# per (TS, TM instance) so multi-species projects get a card per species
# under each TS — matching the candidate-extract layout.
# ---------------------------------------------------------------------------


def _fmt_dims_combined(dims: Optional[tuple], px: Optional[float]) -> Optional[str]:
    """`(1024, 1024, 512), 6.2` → "1024×1024×512 px (6350×6350×3174 Å)"."""
    if not dims:
        return None
    parts_px = "×".join(str(d) for d in dims if d is not None)
    if not parts_px:
        return None
    if px and px > 0:
        parts_ang = "×".join(f"{int(round(d * px)):,}" for d in dims if d is not None)
        return f"{parts_px} px ({parts_ang} Å)"
    return f"{parts_px} px"


def _render_template_match_section(ts_name: str, instance_id: str, job_model, project_path: Path, refresh) -> bool:
    """Per-TS Template Match card. One card per (TS, TM instance); multi-
    species projects get one card per species under each TS. Surfaces the
    full size context (tomo, template, mask, particle, extraction) plus
    mask geometry diagnostics so the user can reason about picks at a
    glance."""
    job_dir = _job_dir_for(instance_id, job_model, project_path)
    if not job_dir:
        return False
    state = get_project_state()
    species, species_id = _resolve_species(state, job_model, instance_id)
    applied_job_json = _read_tm_job_json(job_dir, ts_name)

    # Walk the pixel chain once and pull the rows this card cares about
    # (tomo dims at TM apix, particle diameter, subtomo box/crop).
    pixel_rows = _compute_pixel_chain(state)
    recon_row = next((r for r in pixel_rows if r["stage_key"] == "recon"), None)
    pick_row = next(
        (r for r in pixel_rows if r["stage_key"] == "pick" and r.get("species_id") == species_id), None
    )
    subtomo_row = next(
        (r for r in pixel_rows if r["stage_key"] == "subtomo" and r.get("species_id") == species_id), None
    )

    metric_parts: list[str] = []
    if species:
        metric_parts.append(species.name or species_id or "species")
    if getattr(job_model, "angular_search", None):
        metric_parts.append(f"θ={job_model.angular_search}°")
    sym = getattr(species, "symmetry", None) if species else None
    if not sym:
        sym = getattr(job_model, "symmetry", None)
    if sym:
        metric_parts.append(f"sym {sym}")

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "template_match"
        card._props["data-instance"] = instance_id
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("auto_graph", size="14px").classes("text-indigo-600")
            ui.label("Template match").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            if metric_parts:
                ui.label(" · ".join(metric_parts)).classes("cb-metric-strip")
            status_label = getattr(job_model.execution_status, "value", str(job_model.execution_status))
            if status_label.lower() != "succeeded":
                ui.label(status_label).classes("text-[10px] text-amber-600 font-mono")

        with ui.element("div").classes("cb-chip-strip"):
            _render_tm_size_chips(
                job_model=job_model,
                species=species,
                applied_job_json=applied_job_json,
                recon_row=recon_row,
                pick_row=pick_row,
                subtomo_row=subtomo_row,
            )

        if applied_job_json is None:
            ui.label(f"No tmResults/{ts_name}_job.json yet — TM hasn't produced output for this TS.").classes(
                "cb-section-placeholder"
            )

    return True


def _render_tm_size_chips(
    *,
    job_model,
    species,
    applied_job_json: Optional[dict],
    recon_row: Optional[dict],
    pick_row: Optional[dict],
    subtomo_row: Optional[dict],
) -> None:
    """All the sizes (tomo / template / mask / particle / extraction) plus
    mask geometry diagnostics, in one chip strip. Each chip skips silently
    when its source isn't available so the strip degrades gracefully on
    partially-configured projects.
    """
    from services.templating.mrc_inspection import inspect_mask_intrinsics

    # Reference apix — PyTOM's applied voxel_size wins if the json is on
    # disk; otherwise fall back to the recon row's px size.
    tm_apix: Optional[float] = None
    if applied_job_json and isinstance(applied_job_json.get("voxel_size"), (int, float)):
        v = float(applied_job_json["voxel_size"])
        tm_apix = v if v > 0 else None
    if tm_apix is None and recon_row:
        tm_apix = recon_row.get("px_size_ang")

    # ── Tomo ─────────────────────────────────────────────────────────────
    if recon_row:
        tomo_text = _fmt_dims_combined(recon_row.get("tomo_px"), recon_row.get("px_size_ang"))
        if tomo_text:
            _render_chip(
                "tomo",
                tomo_text,
                status="neutral",
                tooltip=(
                    f"Reconstructed tomogram dimensions at "
                    f"{(recon_row.get('px_size_ang') or 0):g} Å/voxel. From TsAlignmentParams.tomo_dimensions × "
                    f"rescale ratio (TsReconstructParams.rescale_angpixs)."
                ),
                icon="view_in_ar",
            )

    # ── Template ─────────────────────────────────────────────────────────
    tpl_path = getattr(job_model, "template_path", "") or (
        get_effective_template_path(species) if species else ""
    )
    tpl_header = read_template_header(tpl_path) if tpl_path else None
    if tpl_header and tpl_header.box_px:
        if tpl_header.apix_ang:
            tpl_text = (
                f"{tpl_header.box_px}px ({tpl_header.box_px * tpl_header.apix_ang:g} Å)"
                f" @ {tpl_header.apix_ang:g} Å/px"
            )
        else:
            tpl_text = f"{tpl_header.box_px}px"
        # Apix mismatch with TM = error; otherwise neutral.
        status = "neutral"
        warn = ""
        if tm_apix and tpl_header.apix_ang and abs(tm_apix - tpl_header.apix_ang) / tm_apix > 0.05:
            status = "error"
            warn = f" — DIFFERENT from TM voxel_size {tm_apix:g} Å. Re-render template at {tm_apix:g} Å/px."
        _render_chip(
            "tmpl",
            tpl_text,
            status=status,
            tooltip=(
                f"Template volume: {tpl_header.nx}×{tpl_header.ny}×{tpl_header.nz} px "
                f"@ {tpl_header.apix_ang:g} Å/voxel.{warn}"
            ),
            icon="hexagon",
        )

    # ── Mask ─────────────────────────────────────────────────────────────
    mask_path = getattr(job_model, "mask_path", "") or ""
    if not mask_path and species:
        sel = species.get_selected_mask() if hasattr(species, "get_selected_mask") else None
        mask_path = (getattr(sel, "mask_path", "") or "") if sel else ""

    mask_intrinsics = inspect_mask_intrinsics(mask_path) if mask_path else None
    if mask_intrinsics:
        # Mask box (size text follows the template format).
        mask_box = max(mask_intrinsics.nx, mask_intrinsics.ny, mask_intrinsics.nz)
        if mask_intrinsics.apix_ang:
            mask_text = (
                f"{mask_box}px ({mask_box * mask_intrinsics.apix_ang:g} Å)"
                f" @ {mask_intrinsics.apix_ang:g} Å/px"
            )
        else:
            mask_text = f"{mask_box}px"
        mask_status = "neutral"
        mask_warn = ""
        if tm_apix and mask_intrinsics.apix_ang and abs(tm_apix - mask_intrinsics.apix_ang) / tm_apix > 0.05:
            mask_status = "error"
            mask_warn = f" — DIFFERENT from TM voxel_size {tm_apix:g} Å."
        _render_chip(
            "mask",
            mask_text,
            status=mask_status,
            tooltip=(
                f"Mask volume: {mask_intrinsics.nx}×{mask_intrinsics.ny}×{mask_intrinsics.nz} px "
                f"@ {mask_intrinsics.apix_ang:g} Å/voxel.{mask_warn}"
            ),
            icon="filter_tilt_shift",
        )

        if mask_intrinsics.diameter_ang_at_half_max:
            diam_text = f"{mask_intrinsics.diameter_ang_at_half_max:.0f} Å"
            if mask_intrinsics.apix_ang:
                diam_text += f" ({mask_intrinsics.diameter_ang_at_half_max / mask_intrinsics.apix_ang:.0f} px)"
            _render_chip(
                "mask Ø",
                diam_text,
                status="neutral",
                tooltip=(
                    "Equivalent-sphere diameter from voxels > 0.5*max in the mask volume — "
                    "what PyTOM treats as the effective mask radius, independent of soft edges "
                    "or filename labels."
                ),
                icon="circle",
            )

        if mask_intrinsics.isotropy_ratio is not None:
            if mask_intrinsics.looks_spherical:
                iso_status, iso_extra = "ok", "≥0.95 → spherical (mask_is_spherical fast path appropriate)."
            elif mask_intrinsics.isotropy_ratio < 0.85:
                iso_status, iso_extra = (
                    "warn",
                    "<0.85 → elongated. mask_is_spherical=True would apply incorrect shortcuts.",
                )
            else:
                iso_status, iso_extra = "info", "0.85–0.95 → mildly anisotropic."
            _render_chip(
                "iso",
                f"{mask_intrinsics.isotropy_ratio:.2f}",
                status=iso_status,
                tooltip=(
                    f"Isotropy = min(σx,σy,σz)/max(σx,σy,σz) = {mask_intrinsics.isotropy_ratio:.3f}. "
                    + iso_extra
                ),
                icon="all_inclusive",
            )

        if mask_intrinsics.com_offset_magnitude_vox > 0.5:
            _render_chip(
                "COM off",
                f"{mask_intrinsics.com_offset_magnitude_vox:.2f} vox",
                status="warn",
                tooltip=(
                    f"Mask center-of-mass is {mask_intrinsics.com_offset_magnitude_vox:.2f} voxels "
                    f"from box center (Δx={mask_intrinsics.com_offset_x_vox:+.2f}, "
                    f"Δy={mask_intrinsics.com_offset_y_vox:+.2f}, "
                    f"Δz={mask_intrinsics.com_offset_z_vox:+.2f}). Picks land relative to box "
                    f"center — re-center the mask or accept a constant offset."
                ),
                icon="adjust",
            )

    # ── Particle ─────────────────────────────────────────────────────────
    diameter = (pick_row or {}).get("particle_diameter_ang")
    if diameter:
        diam_text = f"{diameter:g} Å"
        if tm_apix:
            diam_text += f" (~{diameter / tm_apix:.0f} px @ {tm_apix:g})"
        _render_chip(
            "particle Ø",
            diam_text,
            status="info",
            tooltip=(
                "Particle diameter declared on the candidate-extract job (or species). "
                "Drives box/crop sanity checks downstream."
            ),
            icon="adjust",
        )

    # ── Extraction (subtomo) ─────────────────────────────────────────────
    if subtomo_row:
        box_px = subtomo_row.get("box_px")
        eff_px = subtomo_row.get("px_size_ang")
        if box_px:
            box_ang = box_px * eff_px if eff_px else None
            box_text = f"{box_px}px ({box_ang:g} Å)" if box_ang else f"{box_px}px"
            _render_chip(
                "extract box",
                box_text,
                status="neutral",
                tooltip=(
                    f"Subtomogram extraction box at {eff_px:g} Å/voxel. Should be 2–3× particle "
                    f"diameter; see the pixel-sanity table for the rule check."
                ),
                icon="crop_square",
            )
        crop_px = subtomo_row.get("_crop_px")
        if crop_px:
            crop_text = f"{crop_px}px ({crop_px * eff_px:g} Å)" if eff_px else f"{crop_px}px"
            _render_chip(
                "extract crop",
                crop_text,
                status="neutral",
                tooltip="Cropped subtomogram output size (crop_size). Must fit inside the box and ≥ particle Ø.",
                icon="crop",
            )


# ---------------------------------------------------------------------------
# Candidate Extract section: carries forward the entire flagship preview +
# gallery + scatter fallback content from the old per-job dialog, re-keyed on
# (ts_name, instance_id).
# ---------------------------------------------------------------------------


def _render_candidate_extract_section(ts_name: str, instance_id: str, job_model, project_path: Path, refresh) -> bool:
    """One Candidate Extract card for this TS within this candidate-extract
    instance. Returns True if a card was rendered."""
    job_dir = _job_dir_for(instance_id, job_model, project_path)
    if not job_dir:
        return False

    rows = _collect_tomo_rows_for_instance(job_dir, project_path)
    row = next((r for r in rows if r["tomo_name"] == ts_name), None)
    if row is None:
        return False

    # Lazy auto-generate previews + IMOD overlays on first view of this job
    # in the current dashboard mount. Both helpers are idempotent (per-mount
    # set + BackgroundTask dedup_key) and skip when nothing's to do.
    _auto_kick_preview_generation(instance_id, job_model, job_dir, project_path, refresh)
    _auto_kick_imod_generation(instance_id, job_model, job_dir, project_path, refresh)

    manifest = read_preview_manifest(job_dir) or {}

    with ui.element("div").classes("cb-section-card w-full") as card:
        card._props["data-section"] = "candidate_extract"
        card._props["data-instance"] = instance_id

        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("auto_awesome", size="14px").classes("text-indigo-600")
            ui.label("Candidate extract").classes("cb-section-title")
            ui.label(instance_id).classes("text-[10px] font-mono text-gray-500")
            ui.space()
            diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
            if diameter:
                ui.label(f"diameter {diameter:.0f} Å").classes("text-[10px] text-gray-500 font-mono")
            job_succeeded = job_model.execution_status == JobStatus.SUCCEEDED
            if not job_succeeded:
                ui.label(str(job_model.execution_status)).classes("text-[10px] text-amber-600 font-mono")

        # Per-card regen toolbar — operates at job scope (not per-TS yet; that's
        # Slice E). Buttons are scoped to this card so multi-species projects
        # get their own controls per species.
        has_imod_models = (job_dir / "vis" / "imodPartRad").exists() and any(
            (job_dir / "vis" / "imodPartRad").glob("*.mod")
        )
        with ui.row().classes("cb-instance-toolbar"):
            gen_missing_btn = (
                ui.button(
                    "Render missing",
                    icon="auto_fix_high",
                    on_click=lambda: _handle_generate_for_instance(
                        instance_id, job_model, job_dir, project_path, False, gen_missing_btn, refresh
                    ),
                )
                .props("dense no-caps unelevated size=sm")
                .classes("bg-purple-50 text-purple-700 border border-purple-200")
                .style("padding: 0 8px; min-height: 22px;")
                .tooltip(
                    "Generate previews only for tomograms whose manifest entry is missing or stale. "
                    "Cached entries are skipped. Runs in the background — track in the tray (bottom-right)."
                )
            )
            regen_btn = (
                ui.button(
                    "Re-render all",
                    icon="refresh",
                    on_click=lambda: _handle_generate_for_instance(
                        instance_id, job_model, job_dir, project_path, True, regen_btn, refresh
                    ),
                )
                .props("dense no-caps flat size=sm")
                .classes("text-gray-500")
                .style("padding: 0 6px; min-height: 22px;")
                .tooltip(
                    "Bypass cache and regenerate every tomogram's preview from scratch. "
                    "Use after renderer changes. Runs in the background — track in the tray."
                )
            )
            imod_btn = (
                ui.button(
                    "IMOD",
                    icon="scatter_plot",
                    on_click=lambda: _handle_generate_imod_for_instance(
                        instance_id, job_model, job_dir, project_path, imod_btn, refresh
                    ),
                )
                .props("dense no-caps unelevated size=sm")
                .classes("bg-blue-50 text-blue-700 border border-blue-200")
                .style("padding: 0 8px; min-height: 22px;")
                .tooltip("Regenerate IMOD .mod overlays" if has_imod_models else "Generate IMOD .mod overlays")
            )

        if row["status"] == "errored":
            with ui.element("div").classes("cb-empty"):
                ui.icon("error_outline", size="28px").classes("text-red-500")
                ui.label("Render error: " + (row.get("error") or "unknown")).classes("text-xs text-red-600")
            return True
        if row["status"] not in ("ok", "missing-volume"):
            with ui.element("div").classes("cb-empty"):
                ui.spinner(size="28px", color="indigo-500")
                ui.label("Generating preview for this tilt-series…").classes("text-xs")
                ui.label("Auto-kicked in the background — the page refreshes when the manifest lands.").classes(
                    "text-[11px] italic text-gray-500"
                )
            return True

        entry = (manifest.get("tomograms") or {}).get(row["tomo_name"]) or {}
        has_atlas = bool(entry.get("cutout_atlas") and entry.get("cutout_index"))

        _render_tomo_header_section(row, manifest)
        _render_3dmod_section(row)
        _render_preview_and_gallery_section(row, entry, manifest)
        if not has_atlas:
            _render_picks_scatter_section(row, entry, manifest)

    return True


def _render_tomo_header_section(row: dict, manifest: dict) -> None:
    entry = (manifest.get("tomograms") or {}).get(row["tomo_name"]) or {}
    score_field = manifest.get("score_field")
    with ui.row().classes("w-full items-baseline gap-3 flex-wrap"):
        ui.label(row["position_label"]).classes("text-sm font-bold text-gray-800")
        ui.label(row["tomo_name"]).classes("text-[10px] font-mono text-gray-500")
        ui.space()
        if row["score_range"]:
            ui.label(f"score {row['score_range'][0]:.3f}–{row['score_range'][1]:.3f}").classes(
                "text-[11px] text-gray-500 font-mono"
            )
        if entry.get("score_mean") is not None:
            ui.label(f"mean {entry['score_mean']:.3f}").classes("text-[11px] text-gray-500 font-mono")
        ui.label(f"N={row['n_picks']}").classes("text-[11px] font-mono")
        if score_field:
            ui.label(f"colored by {score_field}").classes("text-[10px] text-gray-500 italic")
        if entry.get("warp_tomo_preview"):
            ui.label("· Warp tomogram backdrop").classes("text-[10px] text-emerald-600 italic")
    if row["status"] == "missing-volume":
        ui.label("No reconstructed tomogram on disk for 3dmod — picks plot still works.").classes(
            "text-[11px] text-amber-700 italic"
        )


def _render_3dmod_section(row: dict) -> None:
    """Render the 3dmod copy-command. Always shows the full `3dmod <vol>
    <mod>` form so the user can copy a ready-to-paste invocation; status
    indicator next to the field reports whether the .mod overlay is
    on disk yet (green) or still being generated (amber)."""
    if not row.get("vol_path"):
        return
    mod = row.get("mod_path")
    mod_exists = bool(row.get("mod_exists"))
    if mod:
        cmd = f"3dmod {row['vol_path']} {mod}"
        if mod_exists:
            status_icon, status_color, status_tip = "check_circle", "text-emerald-600", "IMOD overlay ready."
        else:
            status_icon, status_color = "hourglass_top", "text-amber-600"
            status_tip = (
                "IMOD overlay being generated in the background. The command above is "
                "copyable already — by the time you paste, the .mod file should be in place. "
                "Picks will appear as circles over the tomogram."
            )
    else:
        cmd = f"3dmod {row['vol_path']}"
        status_icon, status_color, status_tip = "info", "text-gray-400", "Open volume only."
    with ui.row().classes("w-full items-center gap-1"):
        ui.label("3dmod").classes("text-[9px] uppercase font-bold text-gray-400 w-12")
        ui.input(value=cmd).props("dense outlined readonly hide-bottom-space").classes(
            "text-xs font-mono flex-1"
        ).style("min-width: 0;")
        ui.icon(status_icon, size="14px").classes(status_color).tooltip(status_tip)
        ui.button(
            icon="content_copy",
            on_click=lambda c=cmd: (ui.clipboard.write(c), ui.notify("Copied", type="positive", timeout=800)),
        ).props("flat dense round size=sm").classes("text-gray-500 hover:text-gray-800").tooltip("Copy 3dmod command")


def _render_preview_and_gallery_section(row: dict, entry: dict, manifest: dict) -> None:
    """Flagship: tomogram preview (left) + subtomo cutout gallery (right).
    Slice B will hoist the preview half into a separate Reconstruction
    section card; for now both halves live inside the Candidate Extract
    section card per Slice A's "carry forward all current sections" rule."""
    warp_path = entry.get("warp_tomo_preview")
    warp_url = _vis_asset_url(warp_path) if warp_path else None
    xz_path = entry.get("xz_preview")
    xz_url = _vis_asset_url(xz_path) if xz_path else None
    index_path = entry.get("cutout_index")
    atlas_meta = _read_atlas_index(Path(index_path)) if index_path else None

    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [1, 1, 1]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)

    # Per-render unique ids — let the gallery JS find "its" preview markers
    # without colliding with sibling cards in a multi-species view.
    nonce = uuid.uuid4().hex[:8]
    xy_host_id = f"cb-tomo-xy-{nonce}"
    xz_host_id = f"cb-tomo-xz-{nonce}"
    gallery_id = f"cb-gallery-{nonce}"

    xy_aspect = max(0.3, min(3.0, x_dim / y_dim))
    xz_aspect = max(1.5, min(8.0, x_dim / z_dim))

    pick_xy_frac: list[tuple[int, float, float]] = []
    pick_xz_frac: list[tuple[int, float, float]] = []
    for p in picks:
        try:
            i = int(p.get("i"))
        except (TypeError, ValueError):
            continue
        fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
        fy = max(0.0, min(1.0, float(p.get("y", 0)) / y_dim))
        fz = max(0.0, min(1.0, float(p.get("z", 0)) / z_dim))
        # IMOD-up Y/Z convention (ROADMAP §4.8): invert vertical for DOM.
        pick_xy_frac.append((i, fx, 1.0 - fy))
        pick_xz_frac.append((i, fx, 1.0 - fz))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("photo_library", size="14px").classes("text-indigo-600")
            ui.label("Tomogram & picks").classes("cb-section-title")
            ui.space()
            n_ok = entry.get("cutout_n_ok") or 0
            n_pick = row.get("n_picks") or 0
            n_fail = len(entry.get("cutout_failures") or [])
            if atlas_meta is not None:
                badge = f"{n_ok}/{n_pick} cutouts"
                if n_fail:
                    badge += f" · {n_fail} failures"
                ui.label(badge).classes("text-[10px] text-gray-500 font-mono")

        preview_target_total_h = 540
        col_max_w_calc = int(preview_target_total_h / max(0.05, (1.0 / xy_aspect + 1.0 / xz_aspect)))
        col_max_w = max(360, min(720, col_max_w_calc))

        with ui.row().classes("w-full gap-3 items-start flex-wrap"):
            left_col = (
                ui.column().classes("gap-1").style(f"flex: 1 1 360px; min-width: 320px; max-width: {col_max_w}px;")
            )
            with left_col:
                with ui.row().classes("cb-preview-toolbar"):
                    ui.label("Tomogram slices").classes("cb-section-title")
                    ui.space()
                    if pick_xy_frac:
                        show_all = ui.checkbox("Show all picks", value=True).props("dense").classes("text-[10px]")

                        def _toggle_overlay(e):
                            cls = "cb-overlay-hide"
                            if e.value:
                                left_col.classes(remove=cls)
                            else:
                                left_col.classes(add=cls)

                        show_all.on_value_change(_toggle_overlay)

                with ui.element("div").classes("cb-preview-stack"):
                    xy_host = ui.element("div").classes("cb-tomo-preview")
                    xy_host._props["id"] = xy_host_id
                    xy_host.style(f"aspect-ratio: {x_dim}/{y_dim};")
                    with xy_host:
                        if warp_url:
                            ui.image(warp_url)
                            for i, fx, fy in pick_xy_frac:
                                g = ui.element("div").classes("cb-pick-ghost")
                                g._props["data-pick-idx"] = str(i)
                                g.style(f"left: {fx * 100:.3f}%; top: {fy * 100:.3f}%;")
                            ui.element("div").classes("cb-pick-marker")
                        else:
                            with ui.column().classes("absolute inset-0 items-center justify-center text-center"):
                                ui.icon("photo", size="36px").classes("text-gray-500")
                                ui.label("No WarpTools preview yet").classes("text-[11px] text-gray-400")
                                ui.label("(run ts_reconstruct first)").classes("text-[10px] text-gray-500 italic")

                    xz_host = ui.element("div").classes("cb-tomo-preview")
                    xz_host._props["id"] = xz_host_id
                    xz_host.style(f"aspect-ratio: {x_dim}/{z_dim};")
                    with xz_host:
                        if xz_url:
                            ui.image(xz_url)
                            for i, fx, fz_top in pick_xz_frac:
                                g = ui.element("div").classes("cb-pick-ghost")
                                g._props["data-pick-idx"] = str(i)
                                g.style(f"left: {fx * 100:.3f}%; top: {fz_top * 100:.3f}%;")
                            ui.element("div").classes("cb-pick-marker")
                        else:
                            with ui.column().classes("absolute inset-0 items-center justify-center text-center"):
                                ui.icon("layers", size="28px").classes("text-gray-500")
                                ui.label("No X/Z slab preview").classes("text-[10px] text-gray-500 italic")

            with ui.column().classes("gap-2 cb-picks-right").style("flex: 1 1 420px; min-width: 320px;"):
                if atlas_meta is None:
                    _render_gallery_placeholder()
                else:
                    _render_gallery_body(
                        row,
                        entry,
                        manifest,
                        atlas_meta,
                        xy_host_id,
                        xz_host_id,
                        gallery_id,
                        bool(warp_url),
                        bool(xz_url),
                    )


def _render_gallery_placeholder() -> None:
    ui.label("Subtomo gallery").classes("cb-section-title")
    with ui.element("div").classes("cb-gallery-empty"):
        ui.icon("hourglass_empty", size="20px").classes("text-amber-500 block mx-auto mb-1")
        ui.html(
            "Subtomo cutout atlas not built — needs a <code>SUBTOMO_EXTRACTION</code> "
            "job whose particles match these picks by Å coordinates.",
            sanitize=False,
        )


def _render_gallery_body(
    row: dict,
    entry: dict,
    manifest: dict,
    atlas_meta: dict,
    xy_host_id: str,
    xz_host_id: str,
    gallery_id: str,
    has_xy: bool,
    has_xz: bool,
) -> None:
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1]
    pixel_size_ang = entry.get("pixel_size_ang")

    atlas_url = _vis_asset_url(entry["cutout_atlas"])
    cols = int(atlas_meta.get("cols", 8))
    rows = int(atlas_meta.get("rows", 1))
    cutout_index = atlas_meta.get("index", {})
    failures = entry.get("cutout_failures") or atlas_meta.get("failures") or []

    x_dim = max(int(tomo_dims[0]), 1)
    y_dim = max(int(tomo_dims[1]), 1)
    z_dim = max(int(tomo_dims[2]), 1)
    pick_xy_frac: dict[int, list[float]] = {}
    pick_xz_frac: dict[int, list[float]] = {}
    for k in cutout_index:
        try:
            i = int(k)
        except (TypeError, ValueError):
            continue
        if 0 <= i < len(picks):
            p = picks[i]
            fx = max(0.0, min(1.0, float(p.get("x", 0)) / x_dim))
            fy_top = max(0.0, min(1.0, 1.0 - float(p.get("y", 0)) / y_dim))
            fz_top = max(0.0, min(1.0, 1.0 - float(p.get("z", 0)) / z_dim))
            pick_xy_frac[i] = [fx, fy_top]
            pick_xz_frac[i] = [fx, fz_top]

    state = {"selected_idx": None, "sort_mode": "best"}

    with ui.row().classes("w-full items-center gap-2"):
        ui.label("Subtomo gallery").classes("cb-section-title")
        ui.space()
        sort_select = (
            ui.select(
                options={"best": "Best score", "worst": "Worst score", "z": "By Z (deep → shallow)"}, value="best"
            )
            .props("dense outlined")
            .classes("text-xs")
            .style("min-width: 180px;")
        )

    grid_container = ui.element("div").classes("cb-gallery-scroll w-full")
    grid_container._props["id"] = gallery_id

    ui.label("Selected pick").classes("cb-section-title mt-2")
    hover_labels = _render_hover_card_skeleton()

    if failures:
        with ui.expansion(f"Why {len(failures)} tile(s) failed").classes("w-full text-[10px]"):
            with ui.element("div").classes("cb-failures-list"):
                for f in failures:
                    with ui.element("div").classes("cb-failure-row"):
                        ui.label(f"#{f.get('i', '?')}").classes("cb-failure-i")
                        ui.label(str(f.get("reason", "unknown")))

    def _sorted_pick_indices() -> list:
        mode = state["sort_mode"]
        available = [int(k) for k in cutout_index.keys()]
        if mode == "best":
            return sorted(available)
        if mode == "worst":
            return sorted(available, reverse=True)
        if mode == "z":

            def _z(i):
                return picks[i].get("z", 0) if 0 <= i < len(picks) else 0

            return sorted(available, key=_z, reverse=True)
        return sorted(available)

    def _on_tile_click(pick_idx: int):
        state["selected_idx"] = pick_idx
        _update_hover_card(
            type("E", (), {"args": {"points": [{"customdata": [pick_idx]}]}})(), picks, pixel_size_ang, hover_labels
        )
        _refresh_grid()

    DISPLAY_TILE_PX = 96
    bg_w = cols * DISPLAY_TILE_PX
    bg_h = rows * DISPLAY_TILE_PX
    bg_size_css = f"{bg_w}px {bg_h}px"

    def _refresh_grid():
        grid_container.clear()
        with grid_container, ui.element("div").classes("cb-gallery-grid"):
            for rank, pick_idx in enumerate(_sorted_pick_indices()):
                pos = cutout_index.get(str(pick_idx))
                if not pos:
                    continue
                r, c = pos
                bg_x = -c * DISPLAY_TILE_PX
                bg_y = -r * DISPLAY_TILE_PX
                pick = picks[pick_idx] if 0 <= pick_idx < len(picks) else None
                cls = "cb-gallery-tile"
                if state["selected_idx"] == pick_idx:
                    cls += " selected"
                style = (
                    f"background-image: url({atlas_url}); "
                    f"background-size: {bg_size_css}; "
                    f"background-position: {bg_x}px {bg_y}px;"
                )
                tile = ui.element("div").classes(cls).style(style)
                tile._props["data-pick-idx"] = str(pick_idx)
                if pick is not None:
                    parts = [f"#{pick_idx}"]
                    if pick.get("score") is not None:
                        parts.append(f"score={pick['score']:.4f}")
                    parts.append(f"x={int(pick.get('x', 0))}")
                    parts.append(f"y={int(pick.get('y', 0))}")
                    parts.append(f"z={int(pick.get('z', 0))}")
                    tile._props["title"] = "  ".join(parts)
                tile.on("click", lambda _e, i=pick_idx: _on_tile_click(i))
                with tile:
                    ui.html(f"#{rank + 1}", sanitize=False).classes("cb-tile-rank")
                    ui.html(f"{pick_idx}", sanitize=False).classes("cb-tile-idx")
                    if pick is not None and pick.get("z") is not None:
                        ui.html(f"z{int(pick['z'])}", sanitize=False).classes("cb-tile-z")
                    if pick and pick.get("score") is not None:
                        ui.html(f"{pick['score']:.3f}", sanitize=False).classes("cb-tile-score")

    sort_select.on_value_change(lambda e: (state.update(sort_mode=e.value or "best"), _refresh_grid()))
    _refresh_grid()

    if (has_xy and pick_xy_frac) or (has_xz and pick_xz_frac):
        slices = []
        if has_xy and pick_xy_frac:
            slices.append({"id": xy_host_id, "picks": pick_xy_frac})
        if has_xz and pick_xz_frac:
            slices.append({"id": xz_host_id, "picks": pick_xz_frac})
        ui.run_javascript(
            "setTimeout(function(){"
            f"  const grid = document.getElementById({gallery_id!r});"
            "  if (!grid) return;"
            f"  const slices = {json.dumps(slices)};"
            "  const wired = slices.map(function(s){"
            "    const host = document.getElementById(s.id);"
            "    if (!host) return null;"
            "    return {marker: host.querySelector('.cb-pick-marker'), picks: s.picks};"
            "  }).filter(function(x){ return x && x.marker; });"
            "  if (!wired.length) return;"
            "  function place(idx) {"
            "    wired.forEach(function(w){"
            "      const xy = w.picks[idx];"
            "      if (!xy) { w.marker.style.opacity = '0'; return; }"
            "      w.marker.style.left = (xy[0] * 100).toFixed(3) + '%';"
            "      w.marker.style.top = (xy[1] * 100).toFixed(3) + '%';"
            "      w.marker.style.opacity = '1';"
            "    });"
            "  }"
            "  function hideAll() { wired.forEach(function(w){ w.marker.style.opacity = '0'; }); }"
            "  grid.addEventListener('mouseover', function(e){"
            "    const t = e.target.closest && e.target.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!t || !grid.contains(t)) return;"
            "    place(t.getAttribute('data-pick-idx'));"
            "  });"
            "  grid.addEventListener('mouseout', function(e){"
            "    const t = e.target.closest && e.target.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!t) return;"
            "    const next = e.relatedTarget && e.relatedTarget.closest "
            "      && e.relatedTarget.closest('.cb-gallery-tile[data-pick-idx]');"
            "    if (!next) hideAll();"
            "  });"
            "}, 80);"
        )


def _render_picks_scatter_section(row: dict, entry: dict, manifest: dict) -> None:
    """Scatter-only fallback: X/Y + X/Z + score histogram. Renders only when
    the gallery isn't available."""
    picks_json_path = entry.get("picks_json")
    picks_data = (
        _read_picks_json(Path(picks_json_path)) if picks_json_path else {"picks": [], "tomo_dims_xyz_px": [0, 0, 0]}
    )
    picks = picks_data.get("picks", [])
    tomo_dims = tuple(picks_data.get("tomo_dims_xyz_px") or entry.get("tomo_dims_xyz_px") or [1, 1, 1])
    score_field = manifest.get("score_field")
    pixel_size_ang = entry.get("pixel_size_ang")
    xz_url = _vis_asset_url(entry["xz_preview"]) if entry.get("xz_preview") else None

    x_dim, y_dim, z_dim = (max(int(d), 1) for d in tomo_dims)
    xy_aspect = min(2.5, max(0.5, x_dim / y_dim))
    xz_aspect = min(5.0, max(1.5, x_dim / z_dim))
    xy_target_h = 380
    xy_max_w = min(640, max(280, int(xy_target_h * xy_aspect)))
    xz_target_h = 220
    xz_max_w = min(720, max(280, int(xz_target_h * xz_aspect)))

    with ui.element("div").classes("cb-section-card w-full"):
        with ui.element("div").classes("cb-section-card-header"):
            ui.icon("scatter_plot", size="14px").classes("text-indigo-600")
            ui.label("Pick distribution").classes("cb-section-title")
            ui.label(f"  ({row['n_picks']} picks)").classes("text-[10px] text-gray-400")
            ui.space()
            ui.label("· no subtomo extraction yet — gallery view unavailable").classes(
                "text-[10px] text-amber-700 italic"
            )

        with ui.row().classes("w-full gap-3 items-stretch flex-wrap"):
            with ui.column().classes("gap-1").style(f"flex: 1 1 320px; min-width: 280px; max-width: {xy_max_w}px;"):
                ui.label("X / Y top-down").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xy_aspect};"):
                    xy_plot = ui.plotly(_build_xy_scatter_fig(picks, tomo_dims, score_field)).style(
                        "width: 100%; height: 100%;"
                    )
            with (
                ui.column()
                .classes("gap-2 cb-picks-right")
                .style(f"flex: 1 1 320px; min-width: 280px; max-width: {xz_max_w}px;")
            ):
                ui.label("X / Z side").classes("cb-section-title")
                with ui.element("div").classes("cb-aspect").style(f"aspect-ratio: {xz_aspect};"):
                    xz_plot = ui.plotly(_build_xz_scatter_fig(picks, tomo_dims, score_field, xz_url)).style(
                        "width: 100%; height: 100%;"
                    )
                ui.label("Score distribution").classes("cb-section-title")
                ui.plotly(_build_score_hist_fig(picks, score_field)).style("width: 100%; height: 160px;")
                ui.label("Hovered pick").classes("cb-section-title")
                hover_labels = _render_hover_card_skeleton()

                def on_hover(e, _picks=picks, _ps=pixel_size_ang, _labels=hover_labels):
                    _update_hover_card(e, _picks, _ps, _labels)

                xy_plot.on("plotly_hover", on_hover, throttle=0.08)
                xz_plot.on("plotly_hover", on_hover, throttle=0.08)


def _render_hover_card_skeleton() -> dict:
    labels: dict = {}
    with ui.element("div").classes("cb-hover-card cb-hover-empty") as card:
        ui.label("idle").classes("cb-hover-key")
        ui.label("hover any pick to see details").classes("cb-hover-val")
    labels["__card"] = card
    return labels


def _update_hover_card(e, picks: list, pixel_size_ang, labels: dict) -> None:
    args = getattr(e, "args", None) or {}
    points = args.get("points") or []
    if not points:
        return
    p = points[0] or {}
    cd = p.get("customdata") or []
    if not cd:
        return
    try:
        idx = int(cd[0])
    except (TypeError, ValueError):
        return
    if idx < 0 or idx >= len(picks):
        return
    pick = picks[idx]

    card = labels.get("__card")
    if card is None:
        return
    if not labels.get("__populated"):
        card.clear()
        card.classes(remove="cb-hover-empty")
        with card:
            for key in ("idx", "px", "ang", "score", "z%-tile", "nn"):
                ui.label(key).classes("cb-hover-key")
                val_label = ui.label("—").classes("cb-hover-val")
                labels[key] = val_label
        labels["__populated"] = True

    labels["idx"].set_text(f"#{pick['i']}")
    labels["px"].set_text(f"{pick['x']}, {pick['y']}, {pick['z']}")
    if pixel_size_ang:
        ax = pick["x"] * pixel_size_ang
        ay = pick["y"] * pixel_size_ang
        az = pick["z"] * pixel_size_ang
        labels["ang"].set_text(f"{ax:.0f}, {ay:.0f}, {az:.0f}")
    else:
        labels["ang"].set_text("(no pixel size)")
    if pick.get("score") is not None:
        labels["score"].set_text(f"{pick['score']:.4f}")
    else:
        labels["score"].set_text("—")
    if pick.get("z_pct") is not None:
        labels["z%-tile"].set_text(f"{pick['z_pct']:.0f}")
    else:
        labels["z%-tile"].set_text("—")
    if pick.get("nn_px") is not None:
        nn_px = pick["nn_px"]
        if pixel_size_ang:
            labels["nn"].set_text(f"{nn_px:.1f} px ({nn_px * pixel_size_ang:.0f} Å)")
        else:
            labels["nn"].set_text(f"{nn_px:.1f} px")
    else:
        labels["nn"].set_text("—")


# ---------------------------------------------------------------------------
# Per-instance row collection: builds row dicts for every tomogram in a
# candidate-extract job, merging tomograms.star and the preview manifest.
# ---------------------------------------------------------------------------


def _collect_tomo_rows_for_instance(job_dir: Path, project_path: Path) -> list[dict]:
    tomograms_star = job_dir / "tomograms.star"
    tomo_df = _read_tomograms_table(tomograms_star)
    manifest = read_preview_manifest(job_dir) or {}
    tomo_entries = manifest.get("tomograms") or {}
    summary = manifest.get("summary") or {}
    missing_volume = set(summary.get("missing_volume") or [])
    errored_map = {e["tomo"]: e.get("error", "") for e in (summary.get("errored") or [])}

    rows: list[dict] = []
    if tomo_df is None:
        for tomo_name, entry in tomo_entries.items():
            label, (stage, beam) = _position_label(tomo_name)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": entry.get("tomo_mrc"),
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": "ok" if entry.get("picks_json") else "no-preview",
                    "error": None,
                }
            )
    else:
        for _, tomo_row in tomo_df.iterrows():
            tomo_name = str(tomo_row["rlnTomoName"])
            label, (stage, beam) = _position_label(tomo_name)
            entry = tomo_entries.get(tomo_name) or {}
            vol_path = _resolve_volume_for_3dmod(tomo_row, project_path)
            mod_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
            if tomo_name in missing_volume and not entry.get("picks_json"):
                status = "missing-volume"
            elif tomo_name in errored_map:
                status = "errored"
            elif entry.get("picks_json"):
                status = "ok"
            else:
                status = "no-preview"
            rows.append(
                {
                    "tomo_name": tomo_name,
                    "position_label": label,
                    "stage": stage,
                    "beam": beam,
                    "vol_path": str(vol_path) if vol_path else None,
                    "mod_path": str(mod_path),
                    "mod_exists": mod_path.exists(),
                    "n_picks": entry.get("n_picks"),
                    "score_range": entry.get("score_range"),
                    "status": status,
                    "error": errored_map.get(tomo_name),
                }
            )
    rows.sort(key=lambda r: (r["stage"], r["beam"], r["tomo_name"]))
    return rows


# ---------------------------------------------------------------------------
# Generation handlers (regen previews / IMOD models from per-card buttons)
# ---------------------------------------------------------------------------


def _make_imod_command_runner():
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)])
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _generate_imod_sync(
    candidates_star: Path, tomograms_star: Path, diameter: float, job_dir: Path, project_path: Path
) -> None:
    generate_candidate_vis(
        candidates_star=candidates_star,
        tomograms_star=tomograms_star,
        particle_diameter_ang=diameter,
        output_dir=job_dir,
        command_runner=_make_imod_command_runner(),
        project_root=project_path,
    )


# Per-mount dedup so the auto-kick helpers don't pile up completion timers
# when the user clicks between tilt-series in the sidebar. BackgroundTask's
# own dedup_key prevents redundant *work*, but we still want to avoid
# installing multiple ui.timer pollers per (job_dir).
_AUTO_KICKED_PREVIEWS: set[str] = set()
_AUTO_KICKED_IMOD: set[str] = set()


def reset_auto_kick_state() -> None:
    """Clear the auto-kick dedup sets — used by `open_tomo_dashboard` so each
    fresh dashboard mount can re-trigger generation if the page is reloaded."""
    _AUTO_KICKED_PREVIEWS.clear()
    _AUTO_KICKED_IMOD.clear()


def _auto_kick_preview_generation(
    instance_id: str, job_model, job_dir: Path, project_path: Path, refresh
) -> bool:
    """If the candidate-extract job has succeeded but some tomograms are
    missing from the preview manifest, kick off a background 'Render
    missing' with completion handler that refreshes the page when done.
    Returns True iff a kickoff was submitted (or one was already in
    flight). Safe to call on every render — both module-level set and
    BackgroundTask dedup_key prevent re-submission."""
    key = str(job_dir)
    if key in _AUTO_KICKED_PREVIEWS:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    _AUTO_KICKED_PREVIEWS.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = get_project_state()

    async def _run(progress_cb):
        import asyncio as _asyncio

        return await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star, tomograms_star, diameter, job_dir,
            project_path, progress_cb, False, state,
        )

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-render previews · {instance_id}",
        subtitle="Filling missing tomogram entries",
        project_path=str(project_path),
        dedup_key=f"render-previews:{job_dir}:no-force",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


def _auto_kick_imod_generation(
    instance_id: str, job_model, job_dir: Path, project_path: Path, refresh
) -> bool:
    """If candidates.star exists for a succeeded extract but the IMOD .mod
    overlays aren't on disk, kick off background generation that auto-
    refreshes the 3dmod command lines on completion. Same dedup semantics
    as the preview kickoff above."""
    key = str(job_dir)
    if key in _AUTO_KICKED_IMOD:
        return False
    if getattr(job_model, "execution_status", None) != JobStatus.SUCCEEDED:
        return False
    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        return False
    imod_dir = job_dir / "vis" / "imodPartRad"
    if imod_dir.exists() and any(imod_dir.glob("*.mod")):
        return False  # already have overlays
    _AUTO_KICKED_IMOD.add(key)

    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb):
        import asyncio as _asyncio

        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(
            _generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path
        )
        return "IMOD overlays ready; 3dmod commands now include them"

    from ui.background_task import BackgroundTask

    BackgroundTask(
        title=f"Auto-generate IMOD overlays · {instance_id}",
        subtitle="Per-tomogram .mod files for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run, on_complete=lambda _t: refresh(), show_start_toast=False)
    return True


async def _handle_generate_imod_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, btn, refresh
) -> None:
    """Submit IMOD model generation to the background task registry. Same
    pattern as the preview-render handler: fire-and-forget, user tracks
    via the workspace tray."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot generate IMOD models",
            type="negative", timeout=4000,
        )
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))

    async def _run(progress_cb) -> str:
        # IMOD generation has no per-tomogram progress hook today; treat as
        # indeterminate (total=0).
        progress_cb(0, 0, "generating IMOD .mod overlays…")
        await _asyncio.to_thread(
            _generate_imod_sync, candidates_star, tomograms_star, diameter, job_dir, project_path
        )
        return "IMOD overlays ready; 3dmod commands now include them"

    BackgroundTask(
        title=f"IMOD models · {instance_id}",
        subtitle="Generate .mod overlays for 3dmod",
        project_path=str(project_path),
        dedup_key=f"imod-models:{job_dir}",
    ).submit(_run)
    refresh()


async def _handle_generate_for_instance(
    instance_id: str, job_model, job_dir: Path, project_path: Path, force: bool, btn, refresh
) -> None:
    """Submit a preview-render to the BackgroundTaskRegistry. Returns
    immediately; user tracks progress via the workspace tray. Multiple
    clicks on the same (job_dir, force) combo dedupe to a single task to
    keep concurrent writes to manifest.json from racing each other."""
    import asyncio as _asyncio

    from ui.background_task import BackgroundTask

    candidates_star = job_dir / "candidates.star"
    tomograms_star = job_dir / "tomograms.star"
    if not candidates_star.exists() or not tomograms_star.exists():
        ui.notify(
            "candidates.star or tomograms.star missing — cannot render previews",
            type="negative", timeout=4000,
        )
        return
    diameter = float(getattr(job_model, "particle_diameter_ang", 0.0))
    state = get_project_state()

    action = "Re-render all" if force else "Render missing"
    subtitle = (
        "Bypass cache; regenerate every tomogram"
        if force
        else "Skip tomograms with a fresh manifest entry"
    )

    async def _run(progress_cb) -> str:
        # The orchestrator is sync; off-thread it so the event loop stays
        # responsive. progress_cb signature matches:
        #   preview_orchestrator.py: progress_cb(i, total, tomo_name)
        # which lines up with our registry's (current, total, message).
        summary = await _asyncio.to_thread(
            generate_candidate_previews,
            candidates_star, tomograms_star, diameter, job_dir,
            project_path, progress_cb, force, state,
        )
        n_new = len(summary["ok"])
        n_cached = len(summary["skipped_cached"])
        n_missing = len(summary["missing_volume"])
        n_err = len(summary["errored"])
        # Pull zero-pick count from the manifest summary if the orchestrator
        # recorded it (manifest v10+).
        try:
            from services.visualization.preview_orchestrator import read_preview_manifest

            manifest = read_preview_manifest(job_dir) or {}
            n_zero = len((manifest.get("summary") or {}).get("zero_picks") or [])
        except Exception:
            n_zero = 0

        parts = [f"{n_new} rendered", f"{n_cached} cached"]
        if n_zero:
            parts.append(f"{n_zero} zero-picks")
        if n_missing:
            parts.append(f"{n_missing} missing volume")
        if n_err:
            parts.append(f"{n_err} errored")
        return ", ".join(parts)

    BackgroundTask(
        title=f"Journey previews · {instance_id}",
        subtitle=f"{action} — {subtitle}",
        project_path=str(project_path),
        # Dedup keyed by job_dir + force flag so concurrent clicks coalesce
        # rather than racing on writes to the same manifest.json.
        dedup_key=f"preview-render:{job_dir}:force={force}",
    ).submit(_run)
    refresh()
