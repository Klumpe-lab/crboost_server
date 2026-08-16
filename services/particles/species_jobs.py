"""Species ↔ pipeline-job attribution over the whole particle phase (roadmap 10 S4).

`dashboard_data.ce_instances_by_species` inverts `resolve_species` for candidate-extract
instances only; the Species page's Jobs tab needs the same inversion for every
particle-phase job type (Template Match · Pick candidates · Subtomo · Reconstruct ·
Class3D), still honoring the unclaimed bucket — instances `resolve_species` cannot
attribute to a REGISTERED species (bare id + no `species_id` with 2+ species, or a
`species_id` naming a deleted species). Headless; in-memory reads only.
"""

from __future__ import annotations

from services.jobs.spec import JOB_SPECS, PHASE_PARTICLES
from services.models_base import JobType, resolve_species

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
