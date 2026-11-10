"""Pipeline edits: putting a job on a project's roster, taking one off it, and excluding a tilt series.

One door for each, shared by every way the UI does it (the roster's rows and "+", the species Jobs tab,
Subset's "Add <job>", the I/O section's "+ Subtomo / + Warp export", a clone) and by the agent API. The
rules live here; callers report what comes back. Adding and removing mark the state dirty and leave the
save to the caller, which owns its policy (the UI's debounced save, the API's save after each write).
"""

from __future__ import annotations

import logging
from typing import Any

from services.aggregation.extraction import apply_aggregation_overrides
from services.jobs import jobtype_paramclass
from services.jobs.spec import JOB_SPEC_BY_TYPE, PHASE_PARTICLES, display_name, instance_display_name, instance_order
from services.jobs.template_match import set_symmetry
from services.models_base import InstanceId, JobStatus, JobType
from services.path_resolution_service import (
    PathResolutionService,
    add_after_pins,
    instance_override_key,
    jobs_in_use,
    override_instance_id,
    pin_sources_made_ambiguous,
)
from services.project_state import ProjectState
from services.result import ErrorCode, err, need, ok
from services.subsets import add_refusal
from services.templating.template_metadata import get_effective_mask_path, get_effective_template_path

logger = logging.getLogger(__name__)

# Never cloned: the tilt filter, a single job; the movie import, written at submit.
NOT_CLONED = (JobType.TILT_FILTER, JobType.IMPORT_MOVIES)


def missing_deps(job_type: JobType, roster: set[str]) -> list[JobType]:
    """The job types `job_type` usually reads from that the roster lacks. A warning, never a refusal: a job
    can read a source set by hand."""

    def type_present(jt: JobType) -> bool:
        return any(InstanceId.matches(s, jt) for s in roster)

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        if type_present(JobType.TS_RECONSTRUCT) or type_present(JobType.DENOISE_PREDICT):
            return []
        return [JobType.TS_RECONSTRUCT]

    # Either extraction path feeds reconstruction.
    if job_type == JobType.RECONSTRUCT_PARTICLE and type_present(JobType.WARP_EXPORT):
        return []

    spec = JOB_SPEC_BY_TYPE.get(job_type)
    deps = spec.dependencies if spec else ()
    return [d for d in deps if not type_present(d)]


def next_instance_id(job_type: JobType, existing_ids: list[str], state_keys: list[str]) -> str:
    """The id a new job of `job_type` takes: the bare type for the first, then `<type>__2`, `__3`, ..."""
    taken = set(existing_ids) | set(state_keys)
    base = job_type.value
    if base not in taken:
        return base
    for n in range(2, 200):
        candidate = str(InstanceId(job_type, str(n)))
        if candidate not in taken:
            return candidate
    return str(InstanceId(job_type, str(len(taken) + 1)))


def in_use_notice(state: ProjectState, instance_id: str) -> str | None:
    """Why `instance_id` cannot be removed or deleted now, or None when it can: it is queued or
    running, or a queued or running job reads it, directly or further down (`jobs_in_use`)."""
    holder = jobs_in_use(state).get(instance_id)
    if holder is None:
        return None
    name = instance_display_name(instance_id, state.jobs.get(instance_id))
    holder_model = state.jobs[holder]
    status = holder_model.execution_status.value.lower()
    if holder == instance_id:
        return f"{name} is {status}; cancel it first."
    return (
        f"{name} feeds {instance_display_name(holder, holder_model)}, which is {status}; "
        "it can go once that has finished or been cancelled."
    )


def species_defaults(job_type: JobType, species) -> dict[str, Any]:
    """The values a new job of `job_type` takes from its species when it is made: template matching's selected
    template and mask and the species' symmetry; candidate extraction's diameter; subtomo extraction's
    extraction geometry; Warp export's voxel size, output box (crop, else box) and diameter, so both extraction
    paths can cut the same particles alike. Only what the species has set: the rest keeps the job's own
    default, and nothing propagates into a job made earlier."""
    out: dict[str, Any] = {}
    ep = getattr(species, "extraction_params", None)
    diameter = getattr(species, "diameter_ang", None)
    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        for field, path in (
            ("template_path", get_effective_template_path(species)),
            ("mask_path", get_effective_mask_path(species)),
        ):
            if path:
                out[field] = path
        if getattr(species, "symmetry", None):
            out["symmetry"] = species.symmetry
    elif job_type == JobType.TEMPLATE_EXTRACT_PYTOM:
        if diameter:
            out["particle_diameter_ang"] = float(diameter)
    elif job_type == JobType.SUBTOMO_EXTRACTION:
        if ep is not None:
            out.update(box_size=ep.box_size, binning=ep.binning, crop_size=ep.crop_size)
    elif job_type == JobType.WARP_EXPORT:
        if ep is not None:
            out.update(binning=ep.binning, box_size=ep.crop_size if ep.crop_size > 0 else ep.box_size)
        if diameter:
            out["diameter_ang"] = float(diameter)
    return out


def _put_on_roster(state: ProjectState, instance_id: str) -> None:
    """`pipeline_order` stays in run order (`instance_order`), which is also the order the resolver reads
    when several refiners of one input are on the roster."""
    if instance_id not in state.pipeline_order:
        state.pipeline_order = sorted([*state.pipeline_order, instance_id], key=instance_order)
        state.mark_dirty()


def _existing_interactive(state: ProjectState, job_type: JobType) -> str | None:
    """The instance of a single-instance interactive job type (the tilt filter), or None."""
    param_cls = jobtype_paramclass().get(job_type)
    if not param_cls or not getattr(param_cls, "IS_INTERACTIVE", False):
        return None
    return next((iid for iid, jm in state.jobs.items() if jm.job_type == job_type), None)


def _restore_tilt_filter_labels(state: ProjectState, instance_id: str) -> None:
    """A tilt filter made again takes the labels the last commit stamped on the registry's frames. Those
    stamps are the filter's committed output (it writes no files of its own; alignment applies the cut when
    it snapshots the tomostars), so a dropped tilt means the user committed and the job may claim SUCCEEDED.
    A commit that dropped nothing leaves no trace; that job starts over unapproved."""
    from services.tilt_series import get_registry_for

    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return
    try:
        reg = get_registry_for(state.project_path)
    except Exception:
        logger.exception("Tilt filter: registry unreadable; the re-added job starts without its committed labels")
        return
    labels = {f.id: "bad" for ts in reg.all_tilt_series() for f in ts.frames if f.is_filtered_out}
    if labels:
        job_model.tilt_labels = labels
        job_model.execution_status = JobStatus.SUCCEEDED


def add_job(
    state: ProjectState,
    job_type: JobType,
    *,
    species_id: str | None = None,
    after: str | None = None,
    reading: dict[str, str] | None = None,
    clone_of: str | None = None,
) -> dict[str, Any]:
    """Put a new job of `job_type` on the roster, with its rules: Subset's refusals and the single instance of
    an interactive job, the species a particle job needs, its prerequisite (tsImport), the species' values
    (`species_defaults`), the tilt filter's committed labels, a clone's settings, and its inputs.

    `after`: a Subset of that finished job's output, or a job reading the Subset `after` (`add_after_pins`).
    `reading`: inputs of the new job set from the start, {slot: source override key}. `clone_of`: a job of the
    same type whose parameters, SLURM settings and inputs set by hand the new one takes. Jobs reading a
    producer the new one ties with stay on it, pinned (`pin_sources_made_ambiguous`). Open while other jobs
    are queued or running: adding changes nothing a submitted job reads.

    `ok(instance_id, added, pins, warnings)`: `added` the ids put on the roster (with the prerequisite;
    empty for an interactive job already there); `pins` {job, slot, source, why: after | reading | kept};
    `warnings` the usual sources the roster lacks. Marks the state dirty; the caller saves."""
    refusal = add_refusal(state, job_type, after, clone_of)
    if refusal:
        return err(refusal)
    spec = JOB_SPEC_BY_TYPE.get(job_type)
    # Phase None: tsImport, added with the jobs that need it, and the per-list extraction.
    if spec is None or spec.phase is None:
        return err(f"{job_type.value} is not a job the roster adds.")

    name = display_name(job_type)
    source = None
    if clone_of is not None:
        if job_type in NOT_CLONED:
            return err(f"{name} is not cloned.")
        source = state.jobs.get(clone_of)
        if source is None:
            return err(f"No job {clone_of} to clone.")
        if source.job_type != job_type:
            return err(f"{clone_of} is a {display_name(source.job_type)}, not a {name}.")

    existing = _existing_interactive(state, job_type)
    if existing is not None:
        added = [] if existing in state.pipeline_order else [existing]
        _put_on_roster(state, existing)
        warnings = [] if added else [f"{name} is a single job; {existing} is on the roster already."]
        return ok(instance_id=existing, added=added, pins=[], warnings=warnings)

    is_particle_job = spec.phase == PHASE_PARTICLES
    species = None
    if species_id is not None:
        if not is_particle_job:
            return err(f"{name} takes no species.")
        species = state.get_species(species_id)
        if species is None:
            return err(f"No species '{species_id}' in this project.")
    elif is_particle_job:
        options = [s.id for s in state.species_registry]
        why = "a particle job works on one species" + ("" if options else "; the project has none yet")
        return err(
            f"{name} needs a particle species.",
            code=ErrorCode.NEEDS_DECISION,
            needs=[need("choice", "species_id", why, options=options)],
        )
    if reading:
        slots = {s.key for s in getattr(spec.param_class, "INPUT_SCHEMA", None) or []}
        unknown = sorted(set(reading) - slots)
        if unknown:
            return err(f"{name} has no input {', '.join(unknown)} (its inputs: {', '.join(sorted(slots))}).")

    instance_id = next_instance_id(job_type, state.roster(), list(state.jobs))
    warnings = []
    missing = missing_deps(job_type, set(state.roster()))
    if missing:
        warnings.append(f"{name} typically requires: " + ", ".join(display_name(d) for d in missing))

    # What automatic selection wires before the addition: a job added beside an existing producer (an
    # A/B copy) leaves the jobs reading that producer where they are.
    sources_before = PathResolutionService(state).automatic_sources()

    added: list[str] = []
    if spec.prerequisite is not None:
        prereq_id = spec.prerequisite.value
        if prereq_id not in state.jobs:
            state.ensure_job_initialized(spec.prerequisite, instance_id=prereq_id)
        if prereq_id not in state.pipeline_order:
            added.append(prereq_id)
            _put_on_roster(state, prereq_id)

    # On the roster before the inputs are worked out: the resolver applies declared refinements only to
    # jobs on it.
    _put_on_roster(state, instance_id)
    added.append(instance_id)
    state.ensure_job_initialized(job_type, instance_id=instance_id)
    job_model = state.jobs[instance_id]
    if job_type == JobType.TILT_FILTER:
        _restore_tilt_filter_labels(state, instance_id)
    if species is not None:
        job_model.species_id = species_id
        for field, value in species_defaults(job_type, species).items():
            if field == "symmetry":
                set_symmetry(job_model, value)  # I1 and I2 start at their own increment
            else:
                setattr(job_model, field, value)
    if source is not None:
        job_model.copy_settings_from(source)

    # With an active merge, every consumer's optimisation-set input reads it; no-op without one.
    apply_aggregation_overrides(state)

    pins: list[dict[str, str]] = []
    if job_model.source_overrides is None:
        job_model.source_overrides = {}
    if after is not None:
        if job_type == JobType.SUBSET:
            # It cuts the output of the job whose tab it was made from.
            job_model.source_overrides["input_star"] = instance_override_key(state.jobs[after].job_type, after)
            pins.append({"job": instance_id, "slot": "input_star", "source": after, "why": "after"})
        else:
            for slot, producer in add_after_pins(state, instance_id, after).items():
                pins.append({"job": instance_id, "slot": slot, "source": producer, "why": "after"})
    if reading:
        job_model.source_overrides.update(reading)
        for slot, key in reading.items():
            pins.append(
                {"job": instance_id, "slot": slot, "source": override_instance_id(key) or key, "why": "reading"}
            )
    for iid, slot, producer in pin_sources_made_ambiguous(state, sources_before):
        pins.append({"job": iid, "slot": slot, "source": producer, "why": "kept"})

    state.mark_dirty()
    return ok(instance_id=instance_id, added=added, pins=pins, warnings=warnings)


def _drop_overrides_naming(state: ProjectState, instance_id: str) -> None:
    """Other jobs' inputs set to the job that leaves go back to automatic. Inputs set to a Subset, and a
    Subset's own, stay set when their job goes: they then read nothing and say so, where clearing them would
    widen them to every tilt series."""
    job_type_str = InstanceId.split(instance_id)[0]
    if job_type_str == JobType.SUBSET.value:
        return
    removed_model = state.jobs.get(instance_id)
    refs = {f"{job_type_str}:External/pending_{instance_id}"}
    relion_name = getattr(removed_model, "relion_job_name", None) if removed_model else None
    if relion_name:
        refs.add(f"{job_type_str}:{relion_name.rstrip('/')}")
    for job_model in state.jobs.values():
        overrides = getattr(job_model, "source_overrides", None)
        if not overrides or job_model.job_type == JobType.SUBSET:
            continue
        for slot in [k for k, v in overrides.items() if v in refs or override_instance_id(v) == instance_id]:
            del overrides[slot]


def remove_job(state: ProjectState, instance_id: str) -> dict[str, Any]:
    """Take a job off the roster. Refused while it is queued or running or a queued or running job reads it,
    and for a job that has run while it still owns a job directory: deleting that one moves its files to
    Trash, which only the UI does (its delete takes the job out of `jobs` first, then calls this). Other
    jobs' inputs set to it go back to automatic. A finished job without a directory (an approved tilt filter)
    stays in `jobs`, off the roster, for what reads its output. Marks the state dirty; the caller saves."""
    notice = in_use_notice(state, instance_id)
    if notice:
        return err(notice)
    if instance_id not in state.pipeline_order:
        return err(f"{instance_id} is not on the roster.")
    job_model = state.jobs.get(instance_id)
    if job_model is not None and job_model.relion_job_name:
        return err(
            f"{instance_display_name(instance_id, job_model)} has run: deleting it moves "
            f"{job_model.relion_job_name.rstrip('/')} to Trash, which is done in the UI."
        )
    _drop_overrides_naming(state, instance_id)
    if job_model is not None and job_model.execution_status != JobStatus.SUCCEEDED:
        del state.jobs[instance_id]
        state.job_path_mapping.pop(instance_id, None)
    # Membership follows the roster for every status: a finished job taken off it is no longer in the
    # pipeline the resolver sees.
    state.pipeline_order = [iid for iid in state.pipeline_order if iid != instance_id]
    state.mark_dirty()
    return ok(instance_id=instance_id)


async def exclude_tilt_series(
    state: ProjectState, ts_id: str, excluded: bool, *, reason: str | None = None
) -> dict[str, Any]:
    """Exclude a tilt series from processing, or bring it back. Forward-only: the drivers skip it from the
    next run on. Refused for the last tilt series still processed, which would hand every downstream job an
    empty input. Saves the tilt-series registry."""
    from services.tilt_series import get_registry_for

    reg = get_registry_for(state.project_path)
    if not reg.has_tilt_series(ts_id):
        return err(f"{ts_id} isn't in the tilt-series registry — reload the project first.")
    if excluded and not reg.get_tilt_series(ts_id).is_excluded:
        active = [t for t in reg.tilt_series_ids() if not reg.get_tilt_series(t).is_excluded]
        if len(active) <= 1:
            return err("Can't exclude the last remaining tilt-series.")
    reg.set_excluded(ts_id, excluded, reason=reason if excluded else None)
    await reg.save_async()
    return ok(ts_id=ts_id, excluded=excluded)
