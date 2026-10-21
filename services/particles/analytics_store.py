"""The read side of the analytics records: driver sidecars first, else a backfill.

For each job the particle analytics describe, the record is, in order of preference:

1. the sidecar the job's own driver wrote (``<job>/analytics/…``), when its fingerprint still
   matches the files it describes;
2. the project record the server assembled earlier (``Analytics/records/<kind>/<instance>.json``),
   when its fingerprint matches;
3. a backfill computed now from the files a finished job left — PyTOM's ``job.json`` files, the
   candidates star, task logs — and stored as (2).

Backfills read many small files; call these off the event loop. A record that cannot be stored
(no write permission on someone else's project) is still returned, and the failure is logged.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.models_base import JobStatus, JobType, resolve_species
from services.particles import analytics
from services.particles.records import (
    Producer,
    Provenance,
    Record,
    SourceStamp,
    fingerprint_of,
    job_sidecar_dir,
    project_record_path,
    read_record,
    write_record,
)

logger = logging.getLogger(__name__)

TM_HEALTH_KIND = "tm_health"
TM_HEALTH_SCHEMA = 1
TM_SIDECAR_SUBDIR = "tm_tomo"
CANDIDATES_SIDECAR = "candidates.json"
CANDIDATES_SCHEMA = analytics.CANDIDATES_SCHEMA

_NODE_LINE = re.compile(r"^Node:\s*(\S+)", re.MULTILINE)

# Records this process has read or computed, by (kind, job dir); reused while their fingerprint
# holds, so a view polling every few seconds reads no file when nothing changed.
_MEMO: dict[tuple[str, str], Record] = {}


# ── Locating jobs ────────────────────────────────────────────────────────────────────────────────


def job_dir_of(state, project_path: Path, instance_id: str) -> Path | None:
    """The job's directory on disk, or None when it has none yet."""
    jm = state.jobs.get(instance_id)
    if jm is None:
        return None
    stored = (getattr(jm, "paths", None) or {}).get("job_dir")
    if stored and Path(stored).is_dir():
        return Path(stored)
    rel = getattr(jm, "relion_job_name", None) or (state.job_path_mapping or {}).get(instance_id)
    if rel:
        d = Path(project_path) / rel.rstrip("/")
        if d.is_dir():
            return d
    return None


def tm_instance_for_ce(state, ce_instance_id: str) -> str | None:
    """The template-match instance whose ``tmResults`` an extraction job consumed: the recorded
    input path's parent is that job's directory. None when the link is not recorded."""
    ce = state.jobs.get(ce_instance_id)
    tm_results = str((getattr(ce, "paths", None) or {}).get("input_tm_job", "") or "") if ce else ""
    if not tm_results:
        return None
    tm_dir = str(Path(tm_results).parent)
    for iid, jm in state.jobs.items():
        if getattr(jm, "job_type", None) != JobType.TEMPLATE_MATCH_PYTOM:
            continue
        if str((getattr(jm, "paths", None) or {}).get("job_dir", "")) == tm_dir:
            return iid
    return None


def subtomo_instances_for_ce(state, ce_instance_id: str, project_path: Path) -> list[str]:
    """Subtomo-extraction instances whose recorded input optimisation set lies in the extraction
    job's directory, newest job first."""
    ce_dir = job_dir_of(state, project_path, ce_instance_id)
    if ce_dir is None:
        return []
    ce_dir = ce_dir.resolve()
    out = []
    for iid, jm in state.jobs.items():
        if getattr(jm, "job_type", None) != JobType.SUBTOMO_EXTRACTION:
            continue
        src = str((getattr(jm, "paths", None) or {}).get("input_optimisation", "") or "")
        if src and Path(src).parent.resolve() == ce_dir:
            out.append(iid)
    return sorted(out, key=lambda i: _job_number(state, i), reverse=True)


@dataclass(frozen=True, slots=True)
class AnalyticsSource:
    """One thing the Analytics tab can describe: a template match and the extraction made from it
    (either may be absent), or a particle table on its own — a particle set or a cross-project
    merge (`kind` "set" / "merge", read from `particles_star`)."""

    tm_instance: str | None
    ce_instance: str | None
    label: str
    succeeded: bool  # the job whose numbers lead (extraction when there is one) has finished
    kind: str = "jobs"
    ident: str = ""  # set id or merge slug
    particles_star: str = ""
    control: str = ""  # "decoy" / "mirror": the template match ran a control template

    @property
    def key(self) -> str:
        if self.kind != "jobs":
            return f"{self.kind}:{self.ident}"
        return f"{self.tm_instance or '-'}|{self.ce_instance or '-'}"


def sources_for_species(state, species_id: str, project_path: Path | None = None) -> list[AnalyticsSource]:
    """Every (template match, extraction) pair of one species, newest job first. An extraction
    whose template match is unknown still appears; so does a template match nobody extracted.
    With `project_path`, the species' particle sets follow, then the project's cross-project merges
    (a merge spans species, so it is listed under each)."""
    from services.jobs.spec import display_name
    from services.particles.species_jobs import jobs_for_species

    rows = jobs_for_species(state, species_id)
    tm_ids = [iid for iid, jm in rows if jm.job_type == JobType.TEMPLATE_MATCH_PYTOM]
    ce_ids = [iid for iid, jm in rows if jm.job_type == JobType.TEMPLATE_EXTRACT_PYTOM]
    out: list[AnalyticsSource] = []
    used_tm: set[str] = set()
    for ce in ce_ids:
        tm = tm_instance_for_ce(state, ce)
        if tm:
            used_tm.add(tm)
        ce_jm = state.jobs[ce]
        label = f"{display_name(JobType.TEMPLATE_EXTRACT_PYTOM)} {_job_label(ce_jm, ce)}"
        control = _control_of(state, species_id, tm) if tm else ""
        if tm:
            label += f" ← {display_name(JobType.TEMPLATE_MATCH_PYTOM)} {_job_label(state.jobs[tm], tm)}"
        if control:
            label += f" [{control} template]"
        out.append(AnalyticsSource(tm, ce, label, ce_jm.execution_status == JobStatus.SUCCEEDED, control=control))
    for tm in tm_ids:
        if tm in used_tm:
            continue
        tm_jm = state.jobs[tm]
        label = f"{display_name(JobType.TEMPLATE_MATCH_PYTOM)} {_job_label(tm_jm, tm)} (no extraction)"
        control = _control_of(state, species_id, tm)
        if control:
            label += f" [{control} template]"
        out.append(AnalyticsSource(tm, None, label, tm_jm.execution_status == JobStatus.SUCCEEDED, control=control))
    out.sort(key=lambda s: _job_number(state, s.ce_instance or s.tm_instance), reverse=True)
    if project_path is not None:
        out.extend(_table_sources(state, species_id, Path(project_path)))
    return out


CONTROL_SOURCES = {"decoy": "decoy:", "mirror": "mirror:"}


def _control_of(state, species_id: str, tm_instance: str) -> str:
    """ "decoy" or "mirror" when the template match ran a control template (by the source the template
    was registered with), else ""."""
    sp = state.get_species(species_id)
    path = getattr(state.jobs.get(tm_instance), "template_path", "") or ""
    tpl = next((t for t in (sp.templates if sp else []) if t.template_path == path), None)
    source = (tpl.source or "") if tpl else ""
    return next((name for name, prefix in CONTROL_SOURCES.items() if source.startswith(prefix)), "")


def _table_sources(state, species_id: str, project_path: Path) -> list[AnalyticsSource]:
    from services.particles.particle_sets import list_sets
    from services.project_state import MERGED_DIR_NAME

    out = [
        AnalyticsSource(
            None,
            None,
            f"set {ps.name} ({ps.n_particles:,})",
            True,
            kind="set",
            ident=ps.id,
            particles_star=ps.particles_star,
        )
        for ps in reversed(list_sets(project_path))
        if ps.species_id == species_id
    ]
    for m in reversed(state.aggregation_merges or []):
        star = project_path / MERGED_DIR_NAME / m.slug / "particles.star"
        out.append(
            AnalyticsSource(
                None,
                None,
                f"merge {m.name or m.slug} ({m.n_particles:,})",
                star.exists(),
                kind="merge",
                ident=m.slug,
                particles_star=str(star),
            )
        )
    return out


def _job_label(jm, instance_id: str) -> str:
    rel = getattr(jm, "relion_job_name", None)
    return Path(rel.rstrip("/")).name if rel else instance_id


def _job_number(state, instance_id: str | None) -> int:
    jm = state.jobs.get(instance_id) if instance_id else None
    m = re.search(r"job(\d+)", getattr(jm, "relion_job_name", "") or "") if jm else None
    return int(m.group(1)) if m else -1


def species_diameter(state, instance_id: str) -> float | None:
    jm = state.jobs.get(instance_id)
    sp, _sid = resolve_species(state, jm, instance_id)
    d = getattr(sp, "diameter_ang", None) if sp is not None else None
    return float(d) if d else None


# ── TM health: one record per template-match job ─────────────────────────────────────────────────


def tm_sidecar_path(job_dir: Path, tomo: str) -> Path:
    return job_sidecar_dir(job_dir) / TM_SIDECAR_SUBDIR / f"{tomo}.json"


def tm_health_record(project_path: Path, state, instance_id: str) -> Record:
    """Per-tomogram facts of one template-match job: σ, search space, the FP=1 cutoff, tilts,
    defocus, dose, runtime and node, and the score-map scan where the driver wrote one."""
    job_dir = job_dir_of(state, project_path, instance_id)
    if job_dir is None:
        raise FileNotFoundError(f"{instance_id} has no job directory yet")
    tm_results = job_dir / "tmResults"
    side_dir = job_sidecar_dir(job_dir) / TM_SIDECAR_SUBDIR
    fp = fingerprint_of([tm_results, side_dir, job_dir / ".task_manifest.json"])
    memo_key = (TM_HEALTH_KIND, str(job_dir))
    hit = _MEMO.get(memo_key)
    if hit is not None and hit.provenance.fingerprint == fp:
        return hit
    stored_path = project_record_path(project_path, TM_HEALTH_KIND, instance_id)
    stored = _read_stored(stored_path)
    if stored is not None and stored.provenance.fingerprint == fp and stored.provenance.schema == TM_HEALTH_SCHEMA:
        _MEMO[memo_key] = stored
        return stored

    items = _manifest_items(job_dir)
    index_of = {name: i for i, name in enumerate(items)}
    job_jsons = {p.name[: -len("_job.json")]: p for p in sorted(tm_results.glob("*_job.json"))}
    rows: list[dict[str, Any]] = []
    sources: list[SourceStamp] = []
    n_driver = 0
    for tomo in sorted(set(items) | set(job_jsons)):
        side = tm_sidecar_path(job_dir, tomo)
        rec = _read_stored(side)
        scores = tm_results / f"{tomo}_scores.mrc"
        if rec is not None and rec.data.get("tomo"):
            row = dict(rec.data)
            row["source"] = "driver"
            n_driver += 1
            sources.append(SourceStamp.of(side))
        elif tomo in job_jsons:
            jj = job_jsons[tomo]
            try:
                row = analytics.tm_tomogram_data(json.loads(jj.read_text()))
            except (OSError, ValueError) as e:
                # A job.json PyTOM is still writing, or a damaged one: the row says so.
                row = {"tomo": tomo, "error": f"job.json unreadable: {e}"}
            row["tomo"] = tomo
            row["runtime_min"] = _backfill_runtime_min(tm_results, tomo, scores)
            row["node"] = _backfill_node(job_dir, index_of.get(tomo))
            row["scan"] = None
            row["source"] = "job.json"
            sources.append(SourceStamp.of(jj))
        else:
            row = {"tomo": tomo, "source": "pending"}
        row["done"] = scores.exists()
        rows.append(row)

    jm = state.jobs.get(instance_id)
    _sp, species_id = resolve_species(state, jm, instance_id)
    record = Record(
        Provenance(
            kind=TM_HEALTH_KIND,
            schema=TM_HEALTH_SCHEMA,
            module=__name__,
            computed_by="server",
            producer=Producer(JobType.TEMPLATE_MATCH_PYTOM.value, instance_id, str(job_dir)),
            sources=tuple(sources),
            fingerprint=fp,
            note=f"{n_driver} of {len(rows)} tomograms from the driver's sidecars; the rest from PyTOM's job.json",
        ),
        {
            "instance_id": instance_id,
            "species_id": species_id,
            "job_dir": str(job_dir),
            "n_tomograms": len(rows),
            "n_done": sum(1 for r in rows if r.get("done")),
            "n_driver": n_driver,
            "tomograms": rows,
        },
    )
    _store(stored_path, record)
    _MEMO[memo_key] = record
    return record


def write_tm_tomogram_sidecar(
    job_dir: Path,
    tomo: str,
    *,
    instance_id: str,
    diameter_ang: float | None,
    started_at: float | None,
    search_region: dict | None = None,
) -> Record:
    """The template-match driver's per-tomogram sidecar: the job.json facts, the node and runtime of
    the task, and a scan of the score map for its peaks (the one number no backfill can produce
    without reading the 2 GB map again). A failed scan is recorded in the sidecar, not raised.

    With a `search_region` (the job's, from its task manifest) the sidecar records it with the share
    of the tomogram it searched (`searched_pct`), since σ and the false-positive cutoff were
    measured over that volume, and the scan counts only peaks inside it, as extraction does."""
    import socket
    import time

    tm_results = job_dir / "tmResults"
    jj = tm_results / f"{tomo}_job.json"
    scores = tm_results / f"{tomo}_scores.mrc"
    data = analytics.tm_tomogram_data(json.loads(jj.read_text()))
    data["tomo"] = tomo
    data["node"] = socket.gethostname()
    data["runtime_min"] = round((time.time() - started_at) / 60.0, 1) if started_at else None
    data["scan"] = None
    radius = analytics.exclusion_radius_px(diameter_ang, data.get("voxel"))
    mask = _search_region_record(data, tm_results, tomo, search_region)
    if data.get("sigma") and data.get("fp1_sigma"):
        try:
            scan = analytics.scan_score_map(
                scores,
                sigma=float(data["sigma"]),
                cutoff_sigma=float(data["fp1_sigma"]),
                radius_px=radius,
                mask_mrc=mask,
            )
        except (OSError, ValueError, MemoryError) as e:
            logger.exception("Score-map scan failed for %s", scores)
            data["error"] = f"score-map scan failed: {type(e).__name__}: {e}"
        else:
            scan["diameter_ang"] = diameter_ang
            if radius is None:
                scan["note"] = "the species has no diameter, so peaks were not thinned and not counted"
            data["scan"] = scan
    else:
        data["error"] = "job.json carries no job_stats (σ, search space); nothing to scan against"
    record = Record(
        Provenance(
            kind=analytics.TM_TOMOGRAM_KIND,
            schema=analytics.TM_TOMOGRAM_SCHEMA,
            module=analytics.__name__,
            computed_by="driver",
            producer=Producer(JobType.TEMPLATE_MATCH_PYTOM.value, instance_id, str(job_dir)),
            sources=(SourceStamp.of(jj), SourceStamp.of(scores)),
        ),
        data,
    )
    write_record(tm_sidecar_path(job_dir, tomo), record)
    return record


def _search_region_record(data: dict, tm_results: Path, tomo: str, search_region: dict | None) -> Path | None:
    """Record the job's search region in `data` with the share of the tomogram its mask covers, and
    return the mask; None without a region. A mask that cannot be read is recorded as such."""
    from services.segmentation.search_region import search_mask_path

    if not search_region:
        return None
    mask = search_mask_path(tm_results, tomo)
    record = {k: search_region.get(k) for k in ("include", "exclude", "dilation_ang")}
    record["mask"] = str(mask)
    try:
        record["searched_pct"] = round(analytics.mask_fraction(mask) * 100.0, 2)
    except (OSError, ValueError) as e:
        logger.exception("Search mask %s could not be read", mask)
        record["error"] = f"mask unreadable: {type(e).__name__}: {e}"
        mask = None
    data["search_region"] = record
    return mask


def _backfill_runtime_min(tm_results: Path, tomo: str, scores: Path) -> float | None:
    """Minutes from the task linking the tomogram into tmResults (its start) to PyTOM writing the
    score map (its end)."""
    link = next((p for p in (tm_results / f"{tomo}.mrc", tm_results / f"{tomo}.rec") if p.is_symlink()), None)
    if link is None or not scores.exists():
        return None
    try:
        return round((scores.stat().st_mtime - link.lstat().st_mtime) / 60.0, 1)
    except OSError:  # removed between the checks: no runtime to report
        return None


def _backfill_node(job_dir: Path, index: int | None) -> str | None:
    """The node an array task ran on, from the "Node: <host>" line the job template prints."""
    if index is None:
        return None
    try:
        with open(job_dir / f"task_{index}.out", errors="replace") as f:
            head = f.read(4096)
    except FileNotFoundError:
        return None
    m = _NODE_LINE.search(head)
    return m.group(1) if m else None


def _manifest_items(job_dir: Path) -> list[str]:
    from services.array_tasks import manifest_items

    return list(manifest_items(job_dir))


# ── Candidates: one record per extraction job ────────────────────────────────────────────────────


def candidates_sidecar_path(job_dir: Path) -> Path:
    return job_sidecar_dir(job_dir) / CANDIDATES_SIDECAR


def candidates_fingerprint(job_dir: Path) -> str:
    return fingerprint_of([job_dir / "candidates.star", job_dir / "tmResults"])


def candidates_record(project_path: Path, state, instance_id: str) -> Record:
    """The candidate set of one extraction job in σ, per tomogram: counts, cutoff, top and last
    pick, the ladder, the cap, and every score."""
    job_dir = job_dir_of(state, project_path, instance_id)
    if job_dir is None:
        raise FileNotFoundError(f"{instance_id} has no job directory yet")
    if not (job_dir / "candidates.star").exists():
        raise FileNotFoundError(f"{instance_id} has not written candidates.star yet")
    fp = candidates_fingerprint(job_dir)
    memo_key = (analytics.CANDIDATES_KIND, str(job_dir))
    hit = _MEMO.get(memo_key)
    if hit is not None and hit.provenance.fingerprint == fp:
        return hit
    stored_path = project_record_path(project_path, analytics.CANDIDATES_KIND, instance_id)
    for path in (candidates_sidecar_path(job_dir), stored_path):
        rec = _read_stored(path)
        if rec is not None and rec.provenance.fingerprint == fp and rec.provenance.schema == CANDIDATES_SCHEMA:
            _MEMO[memo_key] = rec
            return rec
    jm = state.jobs.get(instance_id)
    record = compute_candidates_record(
        job_dir, jm, instance_id=instance_id, diameter_ang=_ce_diameter(state, jm, instance_id), computed_by="server"
    )
    _store(stored_path, record)
    _MEMO[memo_key] = record
    return record


def _ce_diameter(state, jm, instance_id: str) -> float | None:
    d = getattr(jm, "particle_diameter_ang", None)
    return float(d) if d else species_diameter(state, instance_id)


def compute_candidates_record(
    job_dir: Path, params, *, instance_id: str, diameter_ang: float | None, computed_by: str
) -> Record:
    """The candidates record of an extraction job dir. One function for the driver (which writes it
    as the job's sidecar) and the server's backfill, so both always say the same thing."""
    import pandas as pd
    import starfile

    cands = job_dir / "candidates.star"
    data = starfile.read(cands, always_dict=True)
    table = next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)
    if table is None:
        raise ValueError(f"{cands} has no particle table")
    job_stats: dict[str, dict[str, Any]] = {}
    sources = [SourceStamp.of(cands)]
    for jj in sorted((job_dir / "tmResults").glob("*_job.json")):
        tomo = jj.name[: -len("_job.json")]
        try:
            job_stats[tomo] = analytics.tm_tomogram_data(json.loads(jj.read_text()))
        except (OSError, ValueError) as e:
            logger.warning("Skipping unreadable %s: %s", jj, e)
            continue
        sources.append(SourceStamp.of(jj))
    cutoff_method = getattr(getattr(params, "cutoff_method", None), "value", getattr(params, "cutoff_method", ""))
    note = ""
    try:
        extents = analytics.tomogram_extents(job_dir / "tomograms.star")
        sources.append(SourceStamp.of(job_dir / "tomograms.star"))
    except (OSError, ValueError) as e:
        extents = None
        note = f"no tomogram sizes, so picks near the faces are not counted: {e}"
    out = analytics.candidates_data(
        table,
        job_stats,
        cap=getattr(params, "max_num_particles", None),
        cutoff_method=str(cutoff_method or ""),
        false_positives=getattr(params, "expected_false_positives", None),
        cc_threshold=getattr(params, "cc_threshold", None),
        diameter_ang=diameter_ang,
        extents=extents,
    )
    out["instance_id"] = instance_id
    out["job_dir"] = str(job_dir)
    return Record(
        Provenance(
            kind=analytics.CANDIDATES_KIND,
            schema=analytics.CANDIDATES_SCHEMA,
            module=analytics.__name__,
            computed_by=computed_by,
            producer=Producer(JobType.TEMPLATE_EXTRACT_PYTOM.value, instance_id, str(job_dir)),
            sources=tuple(sources),
            fingerprint=candidates_fingerprint(job_dir),
            note=note,
        ),
        out,
    )


def tomo_sigma_map(ce_job_dir: Path) -> dict[str, float]:
    """{tomogram: σ} of one extraction job, from its sidecar when current, else from the
    candidates star's noise column. For score displays that show LCC ÷ σ."""
    fp = candidates_fingerprint(ce_job_dir)
    hit = _SIGMA_MAP_CACHE.get(str(ce_job_dir))
    if hit is not None and hit[0] == fp:
        return hit[1]
    side = _read_stored(candidates_sidecar_path(ce_job_dir))
    if side is not None and side.provenance.fingerprint == fp:
        out = {r["tomo"]: float(r["sigma"]) for r in side.data.get("tomograms") or [] if r.get("sigma")}
    else:
        out = _sigma_map_from_star(ce_job_dir / "candidates.star")
    _SIGMA_MAP_CACHE[str(ce_job_dir)] = (fp, out)
    return out


_SIGMA_MAP_CACHE: dict[str, tuple[str, dict[str, float]]] = {}


def _sigma_map_from_star(cands: Path) -> dict[str, float]:
    import pandas as pd
    import starfile

    if not cands.exists():
        return {}
    data = starfile.read(cands, always_dict=True)
    table = next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)
    if table is None or analytics.SIGMA_COL not in table.columns:
        return {}
    return {
        str(t): float(g[analytics.SIGMA_COL].astype(float).median())
        for t, g in table.groupby(analytics.TOMO_COL, sort=False)
    }


# ── Storage ──────────────────────────────────────────────────────────────────────────────────────


def _read_stored(path: Path) -> Record | None:
    """A stored record, or None when there is none or it does not parse. A damaged one is logged
    and recomputed: it is a cache of files that still exist, not the only copy of anything."""
    try:
        return read_record(path)
    except (OSError, ValueError, TypeError) as e:
        logger.warning("Ignoring unreadable analytics record %s (recomputing): %s", path, e)
        return None


def _store(path: Path, record: Record) -> None:
    try:
        write_record(path, record)
    except OSError as e:
        # Someone else's project, or a full disk: the record is still returned, only not kept.
        logger.warning("Could not store analytics record %s: %s", path, e)


# ── Subtomo extraction: one record per job ───────────────────────────────────────────────────────

SUBTOMO_SIDECAR = "subtomo.json"


def subtomo_sidecar_path(job_dir: Path) -> Path:
    return job_sidecar_dir(job_dir) / SUBTOMO_SIDECAR


def _subtomo_upstream(job_dir: Path, jm) -> Path | None:
    """The particles star a subtomo job extracted from: recorded in its task manifest, else read
    from the input optimisation set it was given."""
    try:
        recorded = json.loads((job_dir / ".task_manifest.json").read_text()).get("upstream_particles_star")
    except (OSError, ValueError):  # no manifest (a merge-only job), or one being rewritten: ask the optset
        recorded = None
    if recorded:
        return Path(recorded)
    optset = str((getattr(jm, "paths", None) or {}).get("input_optimisation", "") or "")
    if optset and Path(optset).exists():
        from services.subtomo_merge import _parse_optimisation_set

        return _parse_optimisation_set(Path(optset))[0]
    return None


def compute_subtomo_record(job_dir: Path, jm, *, instance_id: str, computed_by: str) -> Record:
    """The subtomo record of a job dir; one function for the driver's sidecar and the backfill."""
    import pandas as pd
    import starfile

    def table(path: Path):
        data = starfile.read(path, always_dict=True)
        return next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)

    particles_star = job_dir / "particles.star"
    particles = table(particles_star)
    if particles is None:
        raise ValueError(f"{particles_star} has no particle table")
    upstream_star = _subtomo_upstream(job_dir, jm)
    upstream = table(upstream_star) if upstream_star is not None and upstream_star.exists() else None
    data = analytics.subtomo_data(particles, upstream)
    data["instance_id"] = instance_id
    data["upstream"] = str(upstream_star) if upstream_star else ""
    return Record(
        Provenance(
            kind=analytics.SUBTOMO_KIND,
            schema=analytics.SUBTOMO_SCHEMA,
            module=analytics.__name__,
            computed_by=computed_by,
            producer=Producer(JobType.SUBTOMO_EXTRACTION.value, instance_id, str(job_dir)),
            sources=(SourceStamp.of(particles_star), *((SourceStamp.of(upstream_star),) if upstream_star else ())),
            fingerprint=fingerprint_of([particles_star]),
            note="" if upstream is not None else "the extraction's input was not found, so losses are not counted",
        ),
        data,
    )


def subtomo_record(project_path: Path, state, instance_id: str) -> Record:
    job_dir = job_dir_of(state, project_path, instance_id)
    if job_dir is None or not (job_dir / "particles.star").exists():
        raise FileNotFoundError(f"{instance_id} has not written particles.star yet")
    fp = fingerprint_of([job_dir / "particles.star"])
    memo_key = (analytics.SUBTOMO_KIND, str(job_dir))
    hit = _MEMO.get(memo_key)
    if hit is not None and hit.provenance.fingerprint == fp:
        return hit
    stored_path = project_record_path(project_path, analytics.SUBTOMO_KIND, instance_id)
    for path in (subtomo_sidecar_path(job_dir), stored_path):
        rec = _read_stored(path)
        if rec is not None and rec.provenance.fingerprint == fp and rec.provenance.schema == analytics.SUBTOMO_SCHEMA:
            _MEMO[memo_key] = rec
            return rec
    record = compute_subtomo_record(job_dir, state.jobs.get(instance_id), instance_id=instance_id, computed_by="server")
    _store(stored_path, record)
    _MEMO[memo_key] = record
    return record


# ── A particle table on its own: a set or a cross-project merge ──────────────────────────────────

TABLE_KIND = "table_scores"


def table_record(project_path: Path, source: AnalyticsSource) -> Record:
    """The σ picture of a particle table that is not one extraction job's — a particle set or a merge
    — per tomogram, from its own score and noise columns. A merge's tomograms also carry the
    project each came from (``data["projects"]``), read from the merge's summary."""
    import pandas as pd
    import starfile

    star = Path(source.particles_star)
    if not star.exists():
        raise FileNotFoundError(f"{star} does not exist")
    summary = star.parent / "merge_summary.json"
    fp = fingerprint_of([star, summary])
    memo_key = (TABLE_KIND, str(star))
    hit = _MEMO.get(memo_key)
    if hit is not None and hit.provenance.fingerprint == fp:
        return hit
    stored_path = project_record_path(project_path, TABLE_KIND, source.key.replace(":", "_"))
    stored = _read_stored(stored_path)
    if stored is not None and stored.provenance.fingerprint == fp and stored.provenance.schema == CANDIDATES_SCHEMA:
        _MEMO[memo_key] = stored
        return stored
    data = starfile.read(star, always_dict=True)
    table = next((v for v in data.values() if isinstance(v, pd.DataFrame) and analytics.TOMO_COL in v.columns), None)
    if table is None:
        raise ValueError(f"{star} has no particle table")
    out = analytics.candidates_data(table, {}, cap=None)
    out["projects"] = _merge_projects(summary) if summary.exists() else {}
    out["label"] = source.label
    record = Record(
        Provenance(
            kind=TABLE_KIND,
            schema=CANDIDATES_SCHEMA,
            module=analytics.__name__,
            computed_by="server",
            producer=Producer(source.kind, source.ident, str(star.parent)),
            sources=(SourceStamp.of(star), SourceStamp.of(summary)),
            fingerprint=fp,
        ),
        out,
    )
    _store(stored_path, record)
    _MEMO[memo_key] = record
    return record


def _merge_projects(summary: Path) -> dict[str, str]:
    """{tomogram: project} of a merge, from its merge_summary.json: each source's optimisation set
    lies inside a project directory (the one holding project_params.json)."""
    out: dict[str, str] = {}
    for src in json.loads(summary.read_text()).get("sources") or []:
        optset = Path(str(src.get("optimisation_set") or src.get("source_input") or ""))
        project = next((p.name for p in optset.parents if (p / "project_params.json").exists()), optset.parent.name)
        for tomo in src.get("tomo_names") or []:
            out[str(tomo)] = project
    return out


# ── Data quality per tomogram (the Journey's facts), for yield against quality ────────────────────


def tomogram_quality(project_path: Path) -> dict[str, dict[str, Any]]:
    """{tomogram: facts} from the tilt-series registry: Warp's CTF fit resolution of the series, the
    tilts in the reconstruction, and how many of those are blank or dim exposures. With several
    CTF or alignment runs of a series, the base instance's (tsCtf, aligntiltsWarp) is read and named
    in the facts."""
    from services.tilt_series import get_registry_for
    from services.tilt_series.tilt_state import exposure_class, exposure_ratios

    def pick(ts, output_type: str, preferred: str):
        outs = {k: o for k, o in ts.outputs.items() if o.output_type == output_type}
        if preferred in outs:
            return preferred, outs[preferred]
        return next(iter(outs.items()), (None, None))

    out: dict[str, dict[str, Any]] = {}
    for ts in get_registry_for(Path(project_path)).all_tilt_series():
        ctf_key, ctf = pick(ts, "ts_ctf", JobType.TS_CTF.value)
        aln_key, aln = pick(ts, "ts_alignment", JobType.TS_ALIGNMENT.value)
        used = {e.frame_id for e in aln.per_frame} if aln is not None and aln.per_frame else None
        frames = [f for f in ts.frames if (f.id in used if used is not None else not f.is_filtered_out)]
        ratios = exposure_ratios(ts)
        classes = [exposure_class(ratios.get(f.id)) for f in frames]
        out[ts.id] = {
            "ctf_resolution": getattr(ctf, "ctf_resolution", None),
            "n_used": len(frames),
            "n_blank": classes.count("blank"),
            "n_dim": classes.count("dim"),
            "ctf_from": ctf_key,
            "alignment_from": aln_key,
        }
    return out
