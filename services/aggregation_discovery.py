"""
Cross-project discovery for aggregation projects.

Walks user-known project base paths, finds completed SubtomoExtraction jobs
(those whose RELION job dir contains an optimisation_set.star), and returns
candidates the merge panel can offer the user as one-click sources.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from services.models_base import JobType

logger = logging.getLogger(__name__)


@dataclass
class SubtomoCandidate:
    project_name: str
    project_path: str
    instance_id: str
    job_dir: str
    optset_path: str
    species_label: Optional[str]  # e.g. "copia" or "Copia (viral)"; None for default instance
    is_aggregation: bool
    n_tomograms: Optional[int]  # None if we couldn't read tomograms.star
    species_id: Optional[str] = None
    species_color: Optional[str] = None
    mnemonic: str = ""
    has_filter: bool = False  # True if a curated particles_filtered.star exists

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TomoCuration:
    """Per-tomogram pick accounting for one SubtomoExtraction source, used to
    drive the fine (per-tomogram) merge selector."""

    ts_name: str
    total: int  # picks in the original particles.star for this tomo
    kept: Optional[int]  # picks in particles_filtered.star; None = no curation (all kept)
    reviewed: bool  # user explicitly reviewed this TS in the curator


def _count_tomograms(job_dir: Path) -> Optional[int]:
    """Best-effort tomogram count from tomograms.star. None on any failure."""
    tomos = job_dir / "tomograms.star"
    if not tomos.exists():
        return None
    try:
        # starfile is already a hard dep of subtomo_merge; safe to import here.
        import starfile

        d = starfile.read(tomos, always_dict=True)
        for v in d.values():
            try:
                cols = list(v.columns)
            except AttributeError:
                continue
            if "rlnTomoName" in cols:
                return int(len(v))
    except Exception as e:
        logger.debug("tomogram count failed for %s: %s", tomos, e)
    return None


def _scan_project(proj_dir: Path, seen_optsets: set) -> List[SubtomoCandidate]:
    params_file = proj_dir / "project_params.json"
    if not params_file.exists():
        return []

    try:
        with open(params_file) as f:
            data = json.load(f)
    except Exception as e:
        logger.debug("skip %s: cannot parse project_params.json: %s", proj_dir, e)
        return []

    project_name = data.get("project_name") or proj_dir.name
    mnemonic = data.get("mnemonic") or ""
    is_aggregation = bool(data.get("is_aggregation", False))
    jobs = data.get("jobs") or {}
    species_list = [s for s in (data.get("species_registry") or []) if isinstance(s, dict) and s.get("id")]
    species_by_id = {s["id"]: s for s in species_list}

    out: List[SubtomoCandidate] = []
    for instance_id, job_data in jobs.items():
        if not isinstance(job_data, dict):
            continue
        if job_data.get("job_type") != JobType.SUBTOMO_EXTRACTION.value:
            continue
        relion_job_name = (job_data.get("relion_job_name") or "").strip()
        if not relion_job_name:
            continue
        job_dir = proj_dir / relion_job_name.rstrip("/")
        optset = job_dir / "optimisation_set.star"
        if not optset.exists():
            continue

        key = str(optset.resolve())
        if key in seen_optsets:
            continue
        seen_optsets.add(key)

        # Resolve the species this subtomo job belongs to, mirroring
        # tomo_dashboard's _resolve_species: instance_id "__" suffix → job's
        # species_id field → single-species fallback. A bare/numbered instance
        # (e.g. "subtomoExtraction__2") has no real species in the suffix, so we
        # fall through rather than show "2".
        sp = None
        sid = instance_id.split("__", 1)[1] if "__" in instance_id else None
        if sid and sid in species_by_id:
            sp = species_by_id[sid]
        if sp is None:
            sid2 = job_data.get("species_id")
            if sid2 and sid2 in species_by_id:
                sp, sid = species_by_id[sid2], sid2
            elif sid2:
                sid = sid2
        if sp is None and len(species_list) == 1:
            sp = species_list[0]
            sid = sp["id"]

        species_id = sp["id"] if sp else (sid if (sid and not sid.isdigit()) else None)
        species_label = (sp.get("name") if sp else None) or species_id
        species_color = sp.get("color") if sp else None

        out.append(
            SubtomoCandidate(
                project_name=project_name,
                project_path=str(proj_dir),
                instance_id=instance_id,
                job_dir=str(job_dir),
                optset_path=key,
                species_label=species_label,
                is_aggregation=is_aggregation,
                n_tomograms=_count_tomograms(job_dir),
                species_id=species_id,
                species_color=species_color,
                mnemonic=mnemonic,
                has_filter=(job_dir / "particles_filtered.star").exists(),
            )
        )
    return out


def discover_subtomo_optimisation_sets(base_paths: Iterable[str]) -> List[SubtomoCandidate]:
    """Walk each base_path's project subdirs and return SubtomoExtraction candidates.

    De-duplicates by absolute optimisation_set.star path, so overlapping base
    paths don't double-list the same job. Sorted by (project_name, instance_id)
    for stable display.
    """
    seen: set = set()
    candidates: List[SubtomoCandidate] = []

    for base_path in base_paths:
        if not base_path:
            continue
        base = Path(base_path).expanduser()
        if not base.is_dir():
            continue
        try:
            proj_dirs = [p for p in base.iterdir() if p.is_dir() and not p.name.startswith(".")]
        except Exception as e:
            logger.debug("cannot list %s: %s", base, e)
            continue
        for proj_dir in proj_dirs:
            candidates.extend(_scan_project(proj_dir, seen))

    candidates.sort(key=lambda c: (c.project_name.lower(), c.instance_id))
    return candidates


def _counts_by_tomo(star_path: Path) -> dict:
    """{rlnTomoName: row_count} from a particles .star. Empty on any failure."""
    if not star_path.exists():
        return {}
    try:
        import starfile

        d = starfile.read(star_path, always_dict=True)
    except Exception as e:
        logger.debug("count-by-tomo failed for %s: %s", star_path, e)
        return {}
    for v in d.values():
        try:
            cols = list(v.columns)
        except AttributeError:
            continue
        if "rlnTomoName" in cols:
            return {str(k): int(n) for k, n in v["rlnTomoName"].astype(str).value_counts().items()}
    return {}


def load_tomo_curation(job_dir: str) -> List[TomoCuration]:
    """Per-tomogram pick accounting for one SubtomoExtraction job dir.

    Lazy (called when the user expands a species node), not part of the
    cross-project scan — reading every particles.star up front would not scale.
    Tomogram universe comes from tomograms.star so tomos with zero kept picks
    still appear; totals from the original particles.star, kept from the
    curated `particles_filtered.star` (None when no curation exists)."""
    from services.visualization.picks_filter import read_reviewed_counts

    jd = Path(job_dir)
    totals = _counts_by_tomo(jd / "particles.star")

    filtered_path = jd / "particles_filtered.star"
    has_filter = filtered_path.exists()
    kept_counts = _counts_by_tomo(filtered_path) if has_filter else {}
    reviewed = set(read_reviewed_counts(jd).keys())

    # Universe of tomo names: prefer tomograms.star, fall back to particles.
    tomo_names: List[str] = []
    tomos_star = jd / "tomograms.star"
    if tomos_star.exists():
        try:
            import starfile

            d = starfile.read(tomos_star, always_dict=True)
            for v in d.values():
                try:
                    if "rlnTomoName" in list(v.columns):
                        tomo_names = [str(x) for x in v["rlnTomoName"].tolist()]
                        break
                except AttributeError:
                    continue
        except Exception as e:
            logger.debug("tomo list read failed for %s: %s", tomos_star, e)
    if not tomo_names:
        tomo_names = sorted(totals.keys())

    out: List[TomoCuration] = []
    for tn in tomo_names:
        out.append(
            TomoCuration(
                ts_name=tn,
                total=int(totals.get(tn, 0)),
                kept=(int(kept_counts.get(tn, 0)) if has_filter else None),
                reviewed=tn in reviewed,
            )
        )
    return out
