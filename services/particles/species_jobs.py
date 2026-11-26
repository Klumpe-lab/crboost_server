"""Species ↔ pipeline-job attribution over the whole particle phase, and drift.

`dashboard_data.ce_instances_by_species` inverts `resolve_species` for candidate-extract
instances only; `jobs_by_species` does the same for every particle-phase job type
(Template Match · Pick candidates · Subtomo · Reconstruct · Class3D), still honoring the
unclaimed bucket — instances `resolve_species` cannot attribute to a REGISTERED species
(bare id + no `species_id` with 2+ species, or a `species_id` naming a deleted species).

`job_drift` says where a job's snapshot differs from its species. A job keeps the values it
was made with; nothing propagates, so the difference is shown (the notch on the roster's
species tag), never fixed. Headless; in-memory reads only.
"""

from __future__ import annotations

import os

from services.jobs.spec import JOB_SPECS, PHASE_PARTICLES
from services.models_base import JobType, SpeciesKind, resolve_species
from services.templating.template_metadata import get_effective_mask_path, get_effective_template_path

PARTICLE_JOB_TYPES: tuple[JobType, ...] = tuple(s.job_type for s in JOB_SPECS if s.phase == PHASE_PARTICLES)
_ORDER = {jt: i for i, jt in enumerate(PARTICLE_JOB_TYPES)}


def particle_instances(state) -> list[tuple[str, object]]:
    """Every particle-phase (instance_id, job_model), pipeline order then instance id."""
    out = [(iid, jm) for iid, jm in (state.jobs or {}).items() if getattr(jm, "job_type", None) in _ORDER]
    return sorted(out, key=lambda kv: (_ORDER[kv[1].job_type], kv[0]))


def jobs_by_species(state) -> tuple[dict[str, list[tuple[str, object]]], list[tuple[str, object]]]:
    """``({species_id: [(iid, jm), ...]}, unclaimed)`` over `particle_instances`."""
    by_species: dict[str, list[tuple[str, object]]] = {}
    unclaimed: list[tuple[str, object]] = []
    known = {sp.id for sp in (getattr(state, "species_registry", None) or [])}
    for iid, jm in particle_instances(state):
        _, sid = resolve_species(state, jm, iid)
        if sid in known:
            by_species.setdefault(sid, []).append((iid, jm))
        else:
            unclaimed.append((iid, jm))
    return by_species, unclaimed


def jobs_for_species(state, species_id: str) -> list[tuple[str, object]]:
    """The particle-phase instances attributed to `species_id` (pipeline order)."""
    return jobs_by_species(state)[0].get(species_id, [])


def _sym(v) -> str:
    return str(getattr(v, "value", v) or "C1")


def job_drift(sp, jm) -> list[tuple[str, str]]:
    """(what, message) per difference between a job's snapshot and its species `sp`: the
    symmetry on Template Match / Reconstruct / Class 3D / Refine 3D, the diameter and (picking
    from segmentations) the feature on Pick candidates, and Template Match's template and mask
    against the species' selected ones. Empty when the job agrees with its species."""
    out: list[tuple[str, str]] = []
    species_sym = sp.symmetry or "C1"
    match getattr(jm, "job_type", None):
        case JobType.TEMPLATE_MATCH_PYTOM:
            job_sym = _sym(getattr(jm, "symmetry", None))
            if job_sym != species_sym:
                out.append(("symmetry", f"job {job_sym} ≠ species {species_sym}"))
            want_t = get_effective_template_path(sp)
            have_t = getattr(jm, "template_path", "") or ""
            if want_t and have_t != want_t:
                out.append(("template", f"job {os.path.basename(have_t) or '—'} ≠ selected {os.path.basename(want_t)}"))
            want_m = get_effective_mask_path(sp)
            have_m = getattr(jm, "mask_path", "") or ""
            if want_m and have_m != want_m:
                out.append(("mask", f"job {os.path.basename(have_m) or '—'} ≠ selected {os.path.basename(want_m)}"))
        case JobType.TEMPLATE_EXTRACT_PYTOM:
            if sp.diameter_ang:
                job_d = float(getattr(jm, "particle_diameter_ang", 0.0) or 0.0)
                if abs(job_d - float(sp.diameter_ang)) > 1e-6:
                    out.append(("Ø", f"job {job_d:g} Å ≠ species {sp.diameter_ang:g} Å"))
            want_f = getattr(sp, "segmentation_feature", None)
            have_f = getattr(jm, "feature", "") or ""
            if want_f and getattr(jm, "from_segmentation", False) and have_f != want_f:
                out.append(("feature", f"job {have_f or '—'} ≠ species {want_f}"))
            # The shape is the species': a segmentation's picks are points or lines by it.
            if getattr(jm, "from_segmentation", False):
                kind = SpeciesKind(getattr(sp, "kind", None) or SpeciesKind.PARTICLE)
                shape = SpeciesKind(getattr(jm, "shape", None) or SpeciesKind.PARTICLE)
                if shape != kind:
                    out.append(("shape", f"job {shape.value} ≠ species {kind.value}"))
                repeat = getattr(sp, "repeat_ang", None)
                spacing = float(getattr(jm, "spacing_ang", 0.0) or 0.0)
                if kind == shape == SpeciesKind.FILAMENT and repeat and abs(spacing - float(repeat)) > 1e-6:
                    out.append(("spacing", f"job {spacing:g} Å ≠ species repeat {repeat:g} Å"))
        case JobType.RECONSTRUCT_PARTICLE | JobType.CLASS3D | JobType.REFINE3D:
            job_sym = _sym(getattr(jm, "symmetry", None))
            if job_sym != species_sym:
                out.append(("symmetry", f"job {job_sym} ≠ species {species_sym}"))
            if getattr(jm, "job_type", None) == JobType.REFINE3D:
                filament = SpeciesKind(getattr(sp, "kind", None) or SpeciesKind.PARTICLE) == SpeciesKind.FILAMENT
                helix = bool(getattr(jm, "helix", False))
                if helix != filament:
                    job_says = "helical" if helix else "not helical"
                    out.append(("helix", f"job {job_says} ≠ species {'filament' if filament else 'particle'}"))
                tube = float(getattr(jm, "helical_outer_diameter", 0.0) or 0.0)
                if helix and filament and sp.diameter_ang and abs(tube - float(sp.diameter_ang)) > 1e-6:
                    out.append(("tube Ø", f"job {tube:g} Å ≠ species {sp.diameter_ang:g} Å"))
        case JobType.M_REFINE if getattr(jm, "makes_species", False):
            # A follow-up round keeps its parent's species; only a job that makes one (a first round, or a species
            # added to a population) takes these.
            job_sym = _sym(getattr(jm, "symmetry", None))
            if job_sym != species_sym:
                out.append(("symmetry", f"job {job_sym} ≠ species {species_sym}"))
            if sp.diameter_ang:
                job_d = float(getattr(jm, "diameter_ang", 0.0) or 0.0)
                if abs(job_d - float(sp.diameter_ang)) > 1e-6:
                    out.append(("Ø", f"job {job_d:g} Å ≠ species {sp.diameter_ang:g} Å"))
    return out
