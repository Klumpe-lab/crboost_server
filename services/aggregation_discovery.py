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

    def to_dict(self) -> dict:
        return asdict(self)


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
    is_aggregation = bool(data.get("is_aggregation", False))
    jobs = data.get("jobs") or {}
    species_by_id = {s["id"]: s for s in (data.get("species_registry") or []) if isinstance(s, dict) and s.get("id")}

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

        species_label: Optional[str] = None
        if "__" in instance_id:
            sid = instance_id.split("__", 1)[1]
            sp = species_by_id.get(sid)
            species_label = sp["name"] if sp and sp.get("name") else sid

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
