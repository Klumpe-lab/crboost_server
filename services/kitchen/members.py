"""What a project offers a pool, and the plain description a worker builds a member's table from.

A project offers, for one of its species: every candidate-extract job that wrote ``candidates.star``
(kind tm; its subtomo extractions join as columns), and every hand-picked, imported or merged pick
list (kind list; its own extraction joins as columns). Particle sets and merges are not offered:
they enter the kitchen as filters, so no particle shows up twice.

Blocking (stats, star headers, the registry): call in a thread.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.kitchen.models import MemberKind, PickSource, PoolMember
from services.kitchen.snapshots import ProjectView
from services.models_base import JobType, PickListType
from services.particles.analytics_store import (
    candidates_sidecar_path,
    job_dir_of,
    subtomo_instances_for_ce,
    tm_instance_for_ce,
    tomogram_quality,
)
from services.particles.records import fingerprint_of
from services.particles.species_jobs import jobs_for_species
from services.particles.tomo_identity import acquisition_key

logger = logging.getLogger(__name__)

# Bumped when the columns a member build writes change, so cached tables rebuild.
TABLE_SCHEMA = 3

_LIST_SOURCES = {
    PickListType.MANUAL.value: PickSource.HAND,
    PickListType.IMPORTED.value: PickSource.IMPORTED,
    PickListType.MERGED.value: PickSource.MERGED,
}


@dataclass(frozen=True, slots=True)
class MemberOffer:
    kind: MemberKind
    ref: str
    label: str
    star: str
    tomograms_star: str
    extracted: bool
    n_rows: int | None  # from the job's analytics sidecar when it has one; None = not counted yet
    n_tomos: int | None


def project_token(path: Path) -> str:
    """Short, stable name of a project inside member keys: its directory and a hash of its path."""
    path = Path(path).resolve()
    return f"{path.name}-{hashlib.sha1(str(path).encode()).hexdigest()[:6]}"


def member_key(project_path: Path, kind: MemberKind, ref: str) -> str:
    return f"{project_token(project_path)}:{kind.value}:{ref}"


def matching_species(view: ProjectView, catalog_id: str | None) -> str | None:
    """The species of `view` registered from the same lab-catalog entry; never matched by name."""
    if not catalog_id:
        return None
    return next((sp.id for sp in view.state.species_registry if sp.catalog_id == catalog_id), None)


def offered_members(view: ProjectView, species_id: str) -> list[MemberOffer]:
    state, path = view.state, view.path
    out: list[MemberOffer] = []
    for iid, jm in jobs_for_species(state, species_id):
        if jm.job_type != JobType.TEMPLATE_EXTRACT_PYTOM:
            continue
        jd = job_dir_of(state, path, iid)
        if jd is None or not (jd / "candidates.star").exists():
            continue
        n_rows, n_tomos = _sidecar_counts(jd)
        subs = subtomo_instances_for_ce(state, iid, path)
        out.append(
            MemberOffer(
                kind=MemberKind.TM,
                ref=iid,
                label=f"candidates {jd.name}" + (f" · extracted by {len(subs)} job(s)" if subs else ""),
                star=str(jd / "candidates.star"),
                tomograms_star=str(jd / "tomograms.star"),
                extracted=bool(subs),
                n_rows=n_rows,
                n_tomos=n_tomos,
            )
        )
    from services.particles.list_ref import tomograms_star_for

    for pl in state.pick_lists:
        if pl.species_id != species_id or str(pl.list_type.value) not in _LIST_SOURCES or not pl.path:
            continue
        tstar = tomograms_star_for(state, path, species_id, pl.tomo_name)
        out.append(
            MemberOffer(
                kind=MemberKind.LIST,
                ref=f"{species_id}/{pl.tomo_name}/{pl.slug}",
                label=f"{pl.list_type.value} list {pl.label or pl.slug} · {pl.tomo_name}",
                star=str(pl.path),
                tomograms_star=str(tstar or ""),
                extracted=bool(pl.extracted_path),
                n_rows=pl.count,
                n_tomos=1,
            )
        )
    return out


def _sidecar_counts(ce_dir: Path) -> tuple[int | None, int | None]:
    import json

    try:
        data = json.loads(candidates_sidecar_path(ce_dir).read_text()).get("data") or {}
    except (OSError, ValueError):  # no sidecar (an older job): counted when the table is built
        return None, None
    return data.get("n_total"), data.get("n_with_picks")


def tomo_facts(view: ProjectView) -> dict[str, dict[str, Any]]:
    """{tomogram: {ctf_res, n_tilts, n_blank, acq}} from the project's registry."""
    quality = tomogram_quality(view.path, view.registry)
    out: dict[str, dict[str, Any]] = {}
    for ts in view.registry.all_tilt_series():
        q = quality.get(ts.id) or {}
        key = acquisition_key(ts)
        out[ts.id] = {
            "ctf_res": q.get("ctf_resolution"),
            "n_tilts": q.get("n_used"),
            "n_blank": q.get("n_blank"),
            "acq": f"{key[0]}|{key[1]}" if key else "",
        }
    return out


def member_spec(view: ProjectView, member: PoolMember, facts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Everything a worker needs to build `member`'s columns, as plain data. Raises when the member's
    source is gone (job removed, list deleted)."""
    state, path = view.state, view.path
    spec: dict[str, Any] = {
        "key": member.key,
        "kind": member.kind.value,
        "project": str(path),
        "tomos": member.tomos,
        "facts": facts,
        "subtomo": [],
    }
    if member.kind == MemberKind.TM:
        jm = state.jobs.get(member.ref)
        jd = job_dir_of(state, path, member.ref) if jm is not None else None
        if jd is None:
            raise FileNotFoundError(f"{member.ref} has no job directory in {path.name}")
        tm = tm_instance_for_ce(state, member.ref)
        tm_dir = job_dir_of(state, path, tm) if tm else None
        diameter = getattr(jm, "particle_diameter_ang", None)
        if not diameter:
            sp = state.get_species(member.species_id)
            diameter = getattr(sp, "diameter_ang", None) if sp else None
        spec.update(
            source=PickSource.TM.value,
            star=str(jd / "candidates.star"),
            tomograms_star=str(jd / "tomograms.star"),
            cand_sidecar=str(candidates_sidecar_path(jd)),
            tm_dir=str(tm_dir or ""),
            cap=getattr(jm, "max_num_particles", None),
            diameter_a=float(diameter) if diameter else None,
        )
        for sid in subtomo_instances_for_ce(state, member.ref, path):
            sd = job_dir_of(state, path, sid)
            if sd is not None and (sd / "particles.star").exists():
                spec["subtomo"].append({"label": sd.name, "star": str(sd / "particles.star")})
    else:
        species_id, tomo, slug = member.ref.split("/", 2)
        pl = state.get_pick_list(slug, species_id, tomo)
        if pl is None:
            raise FileNotFoundError(f"pick list {member.ref} is gone from {path.name}")
        from services.particles.list_ref import tomograms_star_for

        tstar = tomograms_star_for(state, path, species_id, tomo)
        if tstar is None:
            raise FileNotFoundError(f"no tomograms.star describes {tomo} in {path.name}")
        spec.update(
            source=_LIST_SOURCES.get(pl.list_type.value, PickSource.HAND).value,
            star=str(pl.path),
            tomograms_star=str(tstar),
            tm_dir="",
            cap=None,
            diameter_a=None,
            tomos=[tomo],
        )
        if pl.extracted_path and Path(pl.extracted_path).exists():
            from services.subtomo_merge import _parse_optimisation_set

            particles = _parse_optimisation_set(Path(pl.extracted_path))[0]
            if particles is not None and Path(particles).exists():
                spec["subtomo"].append({"label": "list extraction", "star": str(particles)})
    spec["fingerprint"] = member_fingerprint(spec, view)
    return spec


def member_fingerprint(spec: dict[str, Any], view: ProjectView) -> str:
    paths: list[str | Path] = [spec["star"], spec["tomograms_star"]]
    if spec.get("cand_sidecar"):
        paths.append(spec["cand_sidecar"])
    if spec.get("tm_dir"):
        paths += [Path(spec["tm_dir"]) / "tmResults", Path(spec["tm_dir"]) / "analytics" / "tm_tomo"]
    paths += [s["star"] for s in spec["subtomo"]]
    tomos = ",".join(spec["tomos"]) if spec.get("tomos") else "*"
    index = view.registry.index_path
    registry = index.stat().st_mtime_ns if index.exists() else "-"
    raw = f"{TABLE_SCHEMA}|{tomos}|{fingerprint_of(paths)}|{registry}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]
