"""Journey dashboard — data & lookup layer.

Pure status/metadata collectors that feed the dashboard renderers, plus the
small shared job/species lookup helpers and overlay constants the renderers and
the per-TS strip all read from. No NiceGUI, no client context — callers pass
``ProjectState`` explicitly.
"""

from __future__ import annotations

import logging
import urllib.parse
from pathlib import Path

import pandas as pd

from services.array_tasks import read_manifest, resolve_job_dir, scan_statuses
from services.jobs.spec import JOB_SPEC_BY_TYPE
from services.models_base import InstanceId, JobStatus, JobType, PickListType
from services.models_base import resolve_species as resolve_species
from services.models_base import split_species_id as split_species_id
from services.tilt_series.build import _infer_position
from services.visualization.preview_orchestrator import read_preview_manifest

logger = logging.getLogger(__name__)


# Per-species overlay colors for the shared tomogram canvas. Indexed by the
# candidate-extract instance's sorted position so a species keeps its color
# across re-renders (and matches its checkbox). Maximally-saturated hues that
# are absent from a greyscale tomogram (no mid-grays) so the dots pop; the
# .cb-pick-ghost dual halo (dark + light ring) keeps them legible on both the
# bright and dark ends of the backdrop.
SPECIES_OVERLAY_COLORS = [
    "#ff1744",  # vivid red
    "#00e5ff",  # vivid cyan
    "#ffea00",  # vivid yellow
    "#d500f9",  # vivid magenta-purple
    "#76ff03",  # neon green
    "#2979ff",  # vivid blue
    "#ff9100",  # vivid orange
    "#f50057",  # vivid pink
]

# Overlay glyph per pick-list type. Color (per list) is the primary
# distinguisher; the glyph is secondary reinforcement so several lists over one
# tomogram read apart at a glance. Machine picks = circle, ArtiaX/curation
# products = diamond/triangle, external = square. The CSS lives next to
# `.cb-pick-ghost` (the `.cb-shape-*` rules). Easy to retune — it's one dict.
_PICK_LIST_GLYPH = {
    PickListType.AUTO: "circle",
    PickListType.FILTERED: "circle",
    PickListType.MANUAL: "diamond",
    PickListType.IMPORTED: "square",
    PickListType.MERGED: "triangle",
}


def find_job_by_type(project_state, jt: JobType) -> tuple[str, object] | None:
    """Return (instance_id, job_model) for the first job matching this type,
    or None. The match accepts either `job_model.job_type == jt` or an
    `instance_id` whose base prefix matches `jt.value` (covers `__species`
    instances)."""
    for iid, jm in (project_state.jobs or {}).items():
        if getattr(jm, "job_type", None) == jt or InstanceId.matches(iid, jt):
            return iid, jm
    return None


def glyph_for(list_type: PickListType) -> str:
    return _PICK_LIST_GLYPH.get(list_type, "circle")


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def candidate_extract_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.TEMPLATE_EXTRACT_PYTOM:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def subtomo_extract_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for instance_id, job_model in state.jobs.items():
        if getattr(job_model, "job_type", None) == JobType.SUBTOMO_EXTRACTION:
            out.append((instance_id, job_model))
    return sorted(out, key=lambda kv: kv[0])


def job_dir_for(state, instance_id: str, job_model, project_path: Path) -> Path | None:
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        d = project_path / rjn.rstrip("/")
        if d.is_dir():
            return d
    mapped = (state.job_path_mapping or {}).get(instance_id)
    if mapped:
        d = project_path / mapped.rstrip("/")
        if d.is_dir():
            return d
    return None


def read_tomograms_table(tomograms_star: Path) -> pd.DataFrame | None:
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


def resolve_volume_for_3dmod(tomo_row: pd.Series, project_path: Path) -> Path | None:
    if "rlnTomoReconstructedTomogram" not in tomo_row.index:
        return None
    p = Path(str(tomo_row["rlnTomoReconstructedTomogram"]))
    if not p.is_absolute():
        p = project_path / p
    if p.exists():
        return p
    return None


def vis_asset_url(asset_path: str) -> str:
    # mtime-keyed cache-buster — see ROADMAP §4.7. When the atlas/manifest
    # regenerates, the URL changes, so the browser doesn't keep serving a
    # stale copy from disk cache against an unchanged path.
    try:
        v = int(Path(asset_path).stat().st_mtime)
    except OSError:
        v = 0
    return f"/api/vis-asset?path={urllib.parse.quote(asset_path, safe='')}&v={v}"


def position_label(tomo_name: str) -> tuple[str, tuple[int, int]]:
    stage, beam = _infer_position(tomo_name)
    if stage == 0:
        return tomo_name.rsplit("_", 1)[-1], (stage, beam)
    return f"Pos {stage} · Beam {beam}", (stage, beam)


def has_any_previews_rendered(state) -> bool:
    if state.project_path is None:
        return False
    for instance_id, job_model in candidate_extract_instances(state):
        job_dir = job_dir_for(state, instance_id, job_model, state.project_path)
        if not job_dir:
            continue
        if (job_dir / "vis" / "preview" / "manifest.json").exists():
            return True
    return False


# ---------------------------------------------------------------------------
# Per-TS journey collector — feeds the 6-pill sidebar strip
# ---------------------------------------------------------------------------


# (key, label, JobType for array stages, or None for synthetic stages handled below).
_PILL_STAGES: list[tuple[str, str, JobType | None]] = [
    ("fs_ctf", "FS/CTF", JobType.FS_MOTION_CTF),
    ("align", "Align", JobType.TS_ALIGNMENT),
    ("ctf", "CTF", JobType.TS_CTF),
    ("recon", "Recon", JobType.TS_RECONSTRUCT),
    ("pick", "Pick", None),
    ("subtomo", "Subtomo", None),
]

# Preprocessing track = the 4 array stages (one shared bar per TS row). The
# particle stages (pick → subtomo) render as a separate per-species track.
PREP_STAGES = _PILL_STAGES[:4]


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
    spec = JOB_SPEC_BY_TYPE.get(jt)
    primary = spec.array_output_star if spec else None
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


def _job_running_or_failed(jm) -> str | None:
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


def _subtomo_extract_status_per_ts(job_dir: Path, jm, expected_ts: set[str] | None = None) -> dict[str, str]:
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
    from services.array_tasks import read_manifest as read_array_manifest
    from services.array_tasks import scan_statuses

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


def collect_dashboard_journey(project_state, project_path: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
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
            if getattr(jm, "job_type", None) != jt and not InstanceId.matches(iid, jt):
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
    for iid, jm in candidate_extract_instances(project_state):
        jd = job_dir_for(project_state, iid, jm, project_path)
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
    for iid, jm in subtomo_extract_instances(project_state):
        jd = job_dir_for(project_state, iid, jm, project_path)
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

    # Imported tomograms (PARTICLES-header import utility, a project-level artifact —
    # NOT a job): a data-less / particle-only project has no array stages, so its
    # tomograms reach the strip only here. No pipeline pills (nothing ran upstream) —
    # surfaced purely so the tomogram is selectable + the Particles section renders.
    imported_star = project_state.imported_tomograms_star_path()
    if imported_star:
        for ts_name in _ts_names_from_star(Path(imported_star)):
            if ts_name not in seen:
                ts_order.append(ts_name)
                seen.add(ts_name)

    # Fill missing pills with "pending" so renderers don't have to defend.
    for ts_name in ts_order:
        row = journey.setdefault(ts_name, {})
        for key, _label, _jt in _PILL_STAGES:
            row.setdefault(key, "pending")

    return journey, ts_order


def _species_label_for(jm, iid: str, manifest: dict) -> str:
    """Display label for a species: manifest species_name → instance suffix →
    job_model.species_id → bare instance id."""
    return str(
        (manifest.get("template") or {}).get("species_name")
        or split_species_id(iid)
        or getattr(jm, "species_id", None)
        or iid
    )


def matching_subtomo_instance(state, species_id):
    """The SUBTOMO_EXTRACTION instance attached to this species_id, or None."""
    for s_iid, s_jm in subtomo_extract_instances(state):
        _, s_sid = resolve_species(state, s_jm, s_iid)
        if s_sid == species_id:
            return s_iid, s_jm
    return None


def recon_mrc_map(state, project_path: Path) -> dict[str, str]:
    """{ts_name: reconstructed-tomogram path} read once from the recon job's
    tomograms.star, so the roster info popover can list the volume without a
    per-row disk read."""
    rec = find_job_by_type(state, JobType.TS_RECONSTRUCT)
    if not rec:
        return {}
    jd = job_dir_for(state, rec[0], rec[1], project_path)
    if jd is None:
        return {}
    df = read_tomograms_table(jd / "tomograms.star")
    if df is None or "rlnTomoName" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, r in df.iterrows():
        mrc = resolve_volume_for_3dmod(r, project_path)
        if mrc:
            out[str(r["rlnTomoName"])] = str(mrc)
    return out


def collect_species_journey(project_state, project_path: Path) -> dict[str, list[dict]]:
    """Per-TS per-species particle-track data for the roster.

    {ts: [{idx, label, color, species_id, pick_status, subtomo_status, n_picks,
    ce_star, subtomo_star, pixel_size_ang, tomo_dims}]}. Species order + color
    follow `candidate_extract_instances` enumeration, so the roster dot matches
    the canvas overlay and the species tabs everywhere."""
    out: dict[str, list[dict]] = {}
    for idx, (iid, jm) in enumerate(candidate_extract_instances(project_state)):
        jd = job_dir_for(project_state, iid, jm, project_path)
        if jd is None:
            continue
        color = SPECIES_OVERLAY_COLORS[idx % len(SPECIES_OVERLAY_COLORS)]
        _, species_id = resolve_species(project_state, jm, iid)
        manifest = read_preview_manifest(jd) or {}
        entries = manifest.get("tomograms") or {}
        label = _species_label_for(jm, iid, manifest)
        pick_status = _candidate_extract_status_per_ts(jd, jm)
        sub_match = matching_subtomo_instance(project_state, species_id)
        sub_status: dict[str, str] = {}
        sub_star = None
        reviewed: dict[str, int] = {}
        if sub_match is not None:
            sub_jd = job_dir_for(project_state, sub_match[0], sub_match[1], project_path)
            if sub_jd is not None:
                from services.visualization import picks_filter

                sub_star = str(sub_jd / "particles.star")
                reviewed = picks_filter.read_reviewed_counts(sub_jd)
                picked_ok = {ts for ts, st in pick_status.items() if st == "ok"}
                sub_status = _subtomo_extract_status_per_ts(sub_jd, sub_match[1], expected_ts=picked_ok)
        ce_star = str(jd / "candidates.star")
        for ts in set(pick_status) | set(sub_status):
            entry = entries.get(ts) or {}
            out.setdefault(ts, []).append(
                {
                    "idx": idx,
                    "label": label,
                    "color": color,
                    "species_id": species_id,
                    "pick_status": pick_status.get(ts, "pending"),
                    "subtomo_status": sub_status.get(ts, "pending"),
                    "n_picks": entry.get("n_picks"),
                    "filtered_count": reviewed.get(ts),  # kept count if reviewed, else None
                    "ce_star": ce_star,
                    "subtomo_star": sub_star,
                    "pixel_size_ang": entry.get("pixel_size_ang"),
                    "tomo_dims": entry.get("tomo_dims_xyz_px"),
                }
            )
    for ts in out:
        out[ts].sort(key=lambda s: s["idx"])
    return out


def journey_signature(journey: dict, species_journey: dict, ts_names: list) -> tuple:
    """Cheap fingerprint of everything the roster renders — prep statuses +
    per-species (status, status, count). Lets the live timer rebuild the sidebar
    only when this actually moves (FingerprintedView discipline), so progress
    ticks don't tear down rows under an in-flight click."""
    parts = []
    for ts in ts_names:
        jr = journey.get(ts, {})
        prep = tuple(jr.get(k, "") for k, _, _ in PREP_STAGES)
        sps = tuple(
            (s["label"], s["pick_status"], s["subtomo_status"], s["n_picks"], s.get("filtered_count"))
            for s in species_journey.get(ts, [])
        )
        parts.append((ts, prep, sps))
    return tuple(parts)


def template_match_instances(state) -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for iid, jm in state.jobs.items():
        if getattr(jm, "job_type", None) == JobType.TEMPLATE_MATCH_PYTOM:
            out.append((iid, jm))
    return sorted(out, key=lambda kv: kv[0])
