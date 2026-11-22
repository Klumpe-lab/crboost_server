"""Where the angles of the particles a job reads were last written (roadmap 64 D8 and S5, roadmap 69 G1).

A Pick candidates job that picks from a segmentation with random orientation draws its angles uniformly: they say
nothing about the particle. A refinement that searches locally around them, or a Class 3D that does not align,
keeps them meaningless and returns a confident map of nothing. A column cannot say so for long (RELION and Warp copy
`cbAngleSource` on, and it would still say random after the first refinement), so the walk follows the job's
particle inputs back through the jobs and sets that pass particles on, to the job that last wrote the angles.

What is not followed counts as unknown and refuses nothing: another project's jobs (a kitchen pool's members), a
merge, a pick list, a file named by hand.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.io_slots import JobFileType
from services.models_base import JobType, PickOrientation, SpeciesKind
from services.path_resolution_service import PathResolutionService, is_synthetic_producer

RANDOM = "random"  # a segmentation's picks, orientation random
AXIS = "axis"  # a segmentation's picks along a piece's long or short axis, rot random about it
FILAMENT = "filament"  # a traced filament: tilt and psi along it, rot open
MATCHED = "matched"  # template matching's best angle
REFINED = "refined"  # a Refine 3D, an aligning Class 3D, M
UNKNOWN = "unknown"  # not followed

# The inputs that carry particles (and so their angles) into a job.
PARTICLE_FILES = frozenset({JobFileType.CANDIDATES_STAR, JobFileType.OPTIMISATION_SET_STAR, JobFileType.PARTICLES_STAR})


@dataclass(frozen=True)
class AngleOrigin:
    kind: str
    writer: str  # the instance id or set id that wrote them last ("" when there is none to name)


def _writes(job_model) -> str | None:
    """What a job writes into its particles' angles, or None when it passes on those it read."""
    match job_model.job_type:
        case JobType.TEMPLATE_EXTRACT_PYTOM:
            if not getattr(job_model, "from_segmentation", False):
                return MATCHED
            if SpeciesKind(getattr(job_model, "shape", None) or SpeciesKind.PARTICLE) == SpeciesKind.FILAMENT:
                return FILAMENT
            return RANDOM if job_model.effective_orientation == PickOrientation.RANDOM else AXIS
        case JobType.FILAMENT_PICK:
            return FILAMENT
        case JobType.REFINE3D | JobType.M_REFINE:
            return REFINED
        case JobType.CLASS3D:
            return None if getattr(job_model, "skip_align", False) else REFINED
    return None


def angle_origins(state, instance_id: str, resolver: PathResolutionService | None = None) -> list[AngleOrigin]:
    """The job (or set) that last wrote the angles of each particle input of `instance_id`, as its inputs are
    chosen now. Several when a set joins sources. Blocking: a set is read from its folder."""
    from services.particles.particle_sets import SET_PRODUCER_PREFIX, get_set

    resolver = resolver or PathResolutionService(state)
    project = Path(state.project_path) if state.project_path else None
    out: list[AngleOrigin] = []
    seen: set[tuple[str, str]] = set()

    def from_job(iid: str) -> None:
        if ("job", iid) in seen:
            return
        seen.add(("job", iid))
        job_model = state.jobs.get(iid)
        if job_model is None:
            out.append(AngleOrigin(UNKNOWN, iid))
        elif (kind := _writes(job_model)) is not None:
            out.append(AngleOrigin(kind, iid))
        else:
            inputs_of(iid)

    def from_set(set_id: str) -> None:
        if ("set", set_id) in seen:
            return
        seen.add(("set", set_id))
        pset = get_set(project, set_id) if project is not None else None
        if pset is None:
            out.append(AngleOrigin(UNKNOWN, set_id))
            return
        for parent in pset.recipe.parents:
            here = not parent.project_path or (project is not None and Path(parent.project_path) == project)
            if parent.kind == "job" and here:
                from_job(parent.instance_id)
            elif parent.kind == "set" and here:
                from_set(parent.set_id)
            else:
                out.append(AngleOrigin(UNKNOWN, parent.instance_id or parent.set_id or parent.merge_slug))

    def inputs_of(iid: str) -> None:
        job_model = state.jobs[iid]
        for slot in resolver.get_input_schema_for_job(job_model.job_type, job_model):
            if not PARTICLE_FILES & set(slot.accepts):
                continue
            candidate = resolver.source_candidate_of(iid, slot.key)
            if candidate is None:
                continue
            producer = candidate.producer_instance_id
            if producer.startswith(SET_PRODUCER_PREFIX):
                from_set(producer.removeprefix(SET_PRODUCER_PREFIX))
            elif is_synthetic_producer(producer):
                out.append(AngleOrigin(UNKNOWN, producer))
            else:
                from_job(producer)

    if instance_id in state.jobs:
        inputs_of(instance_id)
    return out


def random_angle_problem(state, job_model, resolver: PathResolutionService | None = None) -> str | None:
    """Why a Refine 3D or Class 3D may not start from the angles it reads, or None: it searches locally
    (sigma_ang > 0; a helical Refine 3D searches around its filament priors instead) or a Class 3D does not align,
    and a segmentation's picks drew those angles at random. Blocking (`angle_origins`)."""
    if job_model.job_type == JobType.REFINE3D:
        if getattr(job_model, "helix", False) or job_model.sigma_ang <= 0:
            return None
        what, fix = (
            f"a local angular search (sigma_ang {job_model.sigma_ang:g}°)",
            "set sigma_ang to -1 for a global one",
        )
    elif job_model.job_type == JobType.CLASS3D:
        if getattr(job_model, "skip_align", False):
            what, fix = "classifying without aligning", "let it align (Skip alignment off) with a global search"
        elif job_model.sigma_ang > 0:
            what, fix = f"a local angular search (sigma_ang {job_model.sigma_ang:g}°)", "set sigma_ang to -1"
        else:
            return None
    else:
        return None
    iid = next((i for i, jm in state.jobs.items() if jm is job_model), None)
    if iid is None:
        return None
    writers = sorted({o.writer for o in angle_origins(state, iid, resolver) if o.kind == RANDOM})
    if not writers:
        return None
    return (
        f"its particles' angles were drawn at random ({', '.join(writers)}: picks from a segmentation, orientation "
        f"random), and {what} keeps them so: the map would be a rotational average refined as if it were not. "
        f"{fix[0].upper()}{fix[1:]}."
    )
