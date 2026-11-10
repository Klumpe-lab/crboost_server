"""Setting a job up: its parameters, its SLURM settings, its inputs, the tilt filter's mode, model and threshold,
and the project's gain reference.

One door for each, shared by the job page's widgets and by the agent API. The rules live here: a frozen job
refuses, every value is checked against its field (type, bounds, the choices the project offers), and writes
that belong together are made together. Every door marks the state dirty itself, so its write reaches disk with
the next save whoever made it; the caller saves.
"""

from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from services.computing.slurm_service import SlurmConfig
from services.jobs.spec import instance_display_name
from services.jobs.template_match import TM_SYMMETRY_CHOICES, set_symmetry
from services.models_base import DenoiseMethod, JobStatus, JobType, SegmentMethod, resolve_species
from services.path_resolution_service import (
    PathResolutionService,
    instance_override_key,
    override_instance_id,
    unreadable_reason,
)
from services.project_state import ProjectState
from services.result import ErrorCode, err, need, ok

logger = logging.getLogger(__name__)

# The SLURM settings a job's section shows, in its order: (SlurmConfig field, label).
SLURM_FIELDS: tuple[tuple[str, str], ...] = (
    ("partition", "Partition"),
    ("constraint", "Constraint"),
    ("nodes", "Nodes"),
    ("ntasks_per_node", "Tasks/node"),
    ("cpus_per_task", "CPUs/task"),
    ("gres", "GRES"),
    ("mem", "Memory"),
    ("time", "Time limit"),
    ("qos", "QOS"),
)
_SLURM_NAMES = frozenset(name for name, _ in SLURM_FIELDS)

# Denoise Predict's one choice of which model it runs (`denoiser_choices`): it sets the method, IsoNet deconv
# and the model input together.
DENOISER = "denoiser"

_BOUNDS = (("ge", ">=", lambda v, b: v >= b), ("gt", ">", lambda v, b: v > b))
_BOUNDS += (("le", "<=", lambda v, b: v <= b), ("lt", "<", lambda v, b: v < b))


def is_frozen(job_model) -> bool:
    """Queued, running, finished or of unknown state: it keeps the settings it ran or will run with. Only a
    Scheduled or Failed job takes edits (the model drops any other write to a parameter)."""
    return job_model.execution_status not in (JobStatus.SCHEDULED, JobStatus.FAILED)


def frozen_error(instance_id: str, job_model) -> dict[str, Any]:
    status = job_model.execution_status
    again = " Clone it to run it again with other settings." if status == JobStatus.SUCCEEDED else ""
    return err(
        f"{instance_display_name(instance_id, job_model)} is {status.value.lower()}: its settings stay as they are."
        + again,
        code=ErrorCode.JOB_FROZEN,
    )


def field_bounds(job_model, name: str) -> dict[str, float]:
    """The bounds a field declares, by kind: {"ge": 1, "le": 256}."""
    field = type(job_model).model_fields.get(name)
    out: dict[str, float] = {}
    for m in getattr(field, "metadata", None) or []:
        for kind, _, _ in _BOUNDS:
            bound = getattr(m, kind, None)
            if bound is not None:
                out[kind] = bound
    return out


def check_value(job_model, name: str, value: Any) -> tuple[Any, str | None]:
    """`(value as the field takes it, None)`, or `(None, why it is refused)`: the field's type, coerced as the
    model would load it (an enum's members, an int's whole numbers), then the bounds it declares."""
    field = type(job_model).model_fields.get(name)
    if field is None:
        return None, f"{type(job_model).__name__} has no field '{name}'"
    try:
        coerced = TypeAdapter(field.annotation).validate_python(value)
    except ValidationError as e:
        errors = e.errors()
        return None, (errors[0].get("msg", "invalid") if errors else "invalid")
    if coerced is not None:
        bounds = field_bounds(job_model, name)
        for kind, symbol, holds in _BOUNDS:
            if kind in bounds and not holds(coerced, bounds[kind]):
                return None, f"must be {symbol} {bounds[kind]:g}, not {coerced:g}"
    return coerced, None


def project_features(state: ProjectState) -> list[str]:
    """The features this project has segmentations of, or a Segment job is set to make."""
    ids = {f.id for f in state.feature_registry}
    for jm in state.jobs.values():
        if jm.job_type == JobType.SEGMENT and jm.method == SegmentMethod.EASYMODE:
            ids |= set(jm.features)
    return sorted(ids)


@lru_cache(maxsize=4)
def easymode_catalog(cache_dir: str) -> dict:
    """easymode's weights cache, by feature, read once per cache: it is frozen."""
    from services.segmentation.easymode import read_catalog

    return read_catalog(Path(cache_dir))


def denoiser_choices(state: ProjectState, instance_id: str) -> tuple[dict[str, str], str]:
    """Denoise Predict's Denoiser: `({value: label}, the value it shows)`. A cryoCARE or IsoNet model runs as it
    was trained, so each denoise-train job of the project is one entry ("train:<job>") carrying its method and
    IsoNet deconv; with a model file set by hand in Inputs, cryoCARE and IsoNet are offered for the user to say
    which made it ("manual:<method>"); easymode runs its pretrained denoisers, with no train job and no model
    input. What it shows follows what Inputs wired, and is empty with nothing wired: it never picks."""
    job_model = state.jobs[instance_id]
    manual = (job_model.source_overrides or {}).get("model_path", "").startswith("manual:")
    options: dict[str, str] = {}
    for iid, jm in state.jobs.items():
        if jm.job_type != JobType.DENOISE_TRAIN:
            continue
        method = DenoiseMethod(jm.denoise_method)
        deconv = " (deconv)" if method == DenoiseMethod.ISONET and jm.isonet_deconv else ""
        folder = (jm.relion_job_name or "").strip("/").rsplit("/", 1)[-1] or "not run yet"
        options[f"train:{iid}"] = f"{method.value}{deconv} · model from {iid}, {folder}"
    if manual:
        for m in (DenoiseMethod.CRYOCARE, DenoiseMethod.ISONET):
            options[f"manual:{m.value}"] = f"{m.value} · the model file set in Inputs"
    options[DenoiseMethod.EASYMODE.value] = "easymode · pretrained, no train job"

    if job_model.denoise_method == DenoiseMethod.EASYMODE:
        shown = DenoiseMethod.EASYMODE.value
    elif manual:
        shown = f"manual:{DenoiseMethod(job_model.denoise_method).value}"
    else:
        source = PathResolutionService(state).source_of(instance_id, "model_path")
        shown = f"train:{source}" if source else ""
    return options, shown


def _set_denoiser(state: ProjectState, job_model, value: str) -> None:
    kind, _, name = value.partition(":")
    if value == DenoiseMethod.EASYMODE.value:
        job_model.denoise_method = DenoiseMethod.EASYMODE
    elif kind == "manual":
        job_model.denoise_method = DenoiseMethod(name)
    elif kind == "train":
        train = state.jobs[name]
        job_model.denoise_method = DenoiseMethod(train.denoise_method)
        job_model.isonet_deconv = bool(train.isonet_deconv)
        job_model.source_overrides["model_path"] = instance_override_key(JobType.DENOISE_TRAIN, name)


def param_options(state: ProjectState, instance_id: str, name: str) -> list | None:
    """The values a parameter may take when its choices come from the project (a species' templates and masks,
    the project's segmented features, easymode's weights cache, the Denoiser), else None: a free value, checked
    by type and bounds. A list field takes any of them, and keeps the ones it already holds."""
    job_model = state.jobs[instance_id]
    jt = job_model.job_type
    if jt == JobType.TEMPLATE_MATCH_PYTOM:
        if name in ("template_path", "mask_path"):
            species, _ = resolve_species(state, job_model, instance_id)
            if species is None:
                return []
            if name == "template_path":
                return [t.template_path for t in species.templates]
            return ["", *(m.mask_path for m in species.masks)]
        if name == "symmetry":
            return list(TM_SYMMETRY_CHOICES)
        if name in ("search_include", "search_exclude"):
            held = [*job_model.search_include, *job_model.search_exclude]
            return sorted({*project_features(state), *held})
    if jt == JobType.SEGMENT and name == "features":
        from services.segmentation.easymode import easymode_cache_dir

        try:
            catalog = easymode_catalog(str(easymode_cache_dir()))
        except (LookupError, OSError, ValueError):
            # Absent is stated: without the weights cache no feature can be chosen, by the UI either.
            return [*job_model.features]
        return sorted({*catalog, *job_model.features})
    if jt == JobType.DENOISE_PREDICT and name == DENOISER:
        return list(denoiser_choices(state, instance_id)[0])
    return None


def _check_param(state: ProjectState, instance_id: str, job_model, name: str, value: Any) -> tuple[Any, str | None]:
    """What `set_params` writes for one name, or why it refuses it."""
    jt = job_model.job_type
    if jt == JobType.DENOISE_PREDICT:
        if name == DENOISER:
            options = param_options(state, instance_id, name)
            if value not in options:
                return None, f"not one of {', '.join(options)}"
            return value, None
        if name == "denoise_method":
            return None, f"set through '{DENOISER}', which also sets IsoNet deconv and the model input"
        if (
            name == "isonet_deconv"
            and denoiser_choices(state, instance_id)[1] != f"manual:{DenoiseMethod.ISONET.value}"
        ):
            return None, "only for an IsoNet model file set by hand; a train job's model brings its own"
    if name not in job_model.USER_PARAMS:
        listed = sorted(job_model.USER_PARAMS) + ([DENOISER] if jt == JobType.DENOISE_PREDICT else [])
        return None, f"not a parameter of {instance_display_name(instance_id, job_model)} ({', '.join(listed)})"
    coerced, why = check_value(job_model, name, value)
    if why:
        return None, why
    options = param_options(state, instance_id, name)
    if options is not None:
        picked = coerced if isinstance(coerced, list) else [coerced]
        bad = [v for v in picked if v not in options]
        if bad:
            shown = ", ".join(repr(o) for o in options) if options else "none: the project offers no choice"
            return None, f"{', '.join(repr(b) for b in bad)} is not one of {shown}"
        if isinstance(coerced, list):
            coerced = sorted(coerced)
    return coerced, None


def set_params(state: ProjectState, instance_id: str, values: dict[str, Any]) -> dict[str, Any]:
    """Set parameters of a job. Every value is checked first and nothing is written unless all pass:
    `err(rejected={name: why})`. A frozen job refuses with `JOB_FROZEN`. Denoise Predict's `denoiser` sets the
    method, IsoNet deconv and the model input together. `ok(applied={name: value as read back}, unchanged)`.
    Marks the state dirty; the caller saves."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"No job {instance_id} in this project.")
    if is_frozen(job_model):
        return frozen_error(instance_id, job_model)
    checked: dict[str, Any] = {}
    rejected: dict[str, str] = {}
    for name, value in values.items():
        coerced, why = _check_param(state, instance_id, job_model, name, value)
        if why:
            rejected[name] = why
        else:
            checked[name] = coerced
    if rejected:
        return err("Not set: " + "; ".join(f"{n}: {why}" for n, why in rejected.items()), rejected=rejected)

    applied: dict[str, Any] = {}
    unchanged: list[str] = []
    # The Denoiser first: it sets IsoNet deconv, which a value given beside it then overrides.
    for name in sorted(checked, key=lambda n: n != DENOISER):
        value = checked[name]
        if name == DENOISER:
            if denoiser_choices(state, instance_id)[1] == value:
                unchanged.append(name)
                continue
            _set_denoiser(state, job_model, value)
            applied[name] = denoiser_choices(state, instance_id)[1]
            continue
        if getattr(job_model, name) == value:
            unchanged.append(name)
            continue
        is_tm = job_model.job_type == JobType.TEMPLATE_MATCH_PYTOM
        if name == "symmetry" and is_tm and "angular_search" not in checked:
            # An increment still at the old symmetry's default moves to the new one's, as the job tab shows.
            if set_symmetry(job_model, value):
                applied["angular_search"] = job_model.angular_search
        else:
            setattr(job_model, name, value)
        applied[name] = getattr(job_model, name)
    if applied:
        state.mark_dirty()
    return ok(applied=applied, unchanged=unchanged)


def set_slurm(
    state: ProjectState, instance_id: str, values: dict[str, Any] | None = None, *, reset: bool = False
) -> dict[str, Any]:
    """Set a job's SLURM settings (`SLURM_FIELDS`), or with `reset` drop every one set by hand, back to the
    project's defaults and the job type's profile. A value is written only where it differs from what the job
    has now, and is checked as SLURM settings are read. A frozen job refuses with `JOB_FROZEN`: its settings
    are not parameters, so the model's own guard never stops them. A constraint naming a node feature the
    partition lacks is refused at Run, as in the UI. `ok(applied, unchanged, effective)`. Marks the state
    dirty; the caller saves."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"No job {instance_id} in this project.")
    if is_frozen(job_model):
        return frozen_error(instance_id, job_model)
    if reset:
        if job_model.slurm_overrides:
            job_model.clear_slurm_overrides()
            state.mark_dirty()
        return ok(applied={}, unchanged=[], effective=job_model.get_effective_slurm_config().model_dump())

    values = values or {}
    rejected = {n: f"not a SLURM setting ({', '.join(sorted(_SLURM_NAMES))})" for n in values if n not in _SLURM_NAMES}
    effective = job_model.get_effective_slurm_config()
    new = None
    if not rejected:
        try:
            new = SlurmConfig(**{**effective.model_dump(), **values})
        except ValidationError as e:
            for error in e.errors():
                rejected[str(error["loc"][0]) if error.get("loc") else "?"] = error.get("msg", "invalid")
    if rejected:
        return err("Not set: " + "; ".join(f"{n}: {why}" for n, why in rejected.items()), rejected=rejected)

    applied: dict[str, Any] = {}
    unchanged: list[str] = []
    for name in values:
        value = getattr(new, name)
        if value == getattr(effective, name):
            unchanged.append(name)
            continue
        job_model.set_slurm_override(name, value)
        applied[name] = value
    if applied:
        state.mark_dirty()
    return ok(applied=applied, unchanged=unchanged, effective=job_model.get_effective_slurm_config().model_dump())


def _inputs_refusal(instance_id: str, job_model) -> dict[str, Any] | None:
    if is_frozen(job_model):
        return frozen_error(instance_id, job_model)
    if job_model.inputs_fixed:
        return err(
            f"{instance_id} reads the inputs fixed at its last submit: reset them (reset_inputs) to choose again."
        )
    if job_model.job_type == JobType.SUBSET:
        return err("What a Subset cuts is set when it is made; another cut is another Subset.")
    return None


def set_input(
    state: ProjectState,
    instance_id: str,
    slot: str,
    *,
    source: str | None = None,
    path: str | None = None,
    auto: bool = False,
    facet: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Set what one input of a job reads, as its I/O section does: `source`, one of the sources the section
    lists (refused when the input cannot read it, or when it would close a loop); `path`, a file set by hand;
    or `auto`, back to automatic selection. A new source forgets where the job took a kitchen set's facets
    from; `facet` records those choices (project path, or "*" for the whole set -> source), and an empty one
    forgets them. Refused for a frozen job (`JOB_FROZEN`), one whose inputs were fixed at its submit, and a
    Subset. `ok(slot, source)`. Marks the state dirty; the caller saves."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"No job {instance_id} in this project.")
    refusal = _inputs_refusal(instance_id, job_model)
    if refusal:
        return refusal
    given = [k for k, v in (("source", source), ("path", path), ("auto", auto or None)) if v is not None]
    if len(given) > 1:
        return err(f"Give one of source, path or auto, not {' and '.join(given)}.")
    if not given and facet is None:
        return err("Give a source, a path, auto, or facet choices.")
    resolver = PathResolutionService(state)
    schema = {s.key: s for s in resolver.get_input_schema_for_job(job_model.job_type, job_model)}
    if slot not in schema:
        return err(f"{instance_id} has no input {slot} ({', '.join(schema) or 'it reads none'}).")
    if job_model.source_overrides is None:
        job_model.source_overrides = {}

    if source is not None:
        candidates = resolver.get_candidates_for_slot(
            job_model.job_type, slot, consumer_species_id=job_model.species_id, consumer_instance_id=instance_id
        )
        candidate = next((c for c in candidates if c.source_key == source), None)
        if candidate is None:
            listed = ", ".join(c.source_key for c in candidates) or "none"
            return err(f"{source} is not a source {instance_id}.{slot} can read (its sources: {listed}).")
        why = unreadable_reason(job_model.job_type, schema[slot], candidate)
        if why:
            return err(f"{instance_id}.{slot} cannot read {source}: {why}.")
        loop = resolver.cycle_with_override(instance_id, slot, source)
        if loop:
            return err("Not set: this source would close a loop — " + "; ".join(w.describe() for w in loop))
        job_model.source_overrides[slot] = source
        job_model.facet_choices.pop(slot, None)
    elif path is not None:
        job_model.source_overrides[slot] = f"manual:{path}"
    elif auto:
        job_model.source_overrides.pop(slot, None)
        job_model.facet_choices.pop(slot, None)

    if facet is not None:
        if facet:
            job_model.facet_choices.setdefault(slot, {}).update(facet)
        else:
            job_model.facet_choices.pop(slot, None)
    state.mark_dirty()
    return ok(slot=slot, source=job_model.source_overrides.get(slot))


def reset_inputs(state: ProjectState, instance_id: str) -> dict[str, Any]:
    """A Failed job chooses its inputs again at its next Run instead of reading the ones fixed at its last
    submit. If they come out different, it starts over in a new folder; otherwise it resumes where it stopped.
    Marks the state dirty; the caller saves."""
    job_model = state.jobs.get(instance_id)
    if job_model is None:
        return err(f"No job {instance_id} in this project.")
    if not (job_model.inputs_fixed and job_model.execution_status == JobStatus.FAILED):
        return err(f"{instance_id} has no inputs fixed by a failed submit to reset.")
    job_model.submitted_inputs = None
    state.mark_dirty()
    return ok(instance_id=instance_id)


async def set_gain(state: ProjectState, path: str | None) -> dict[str, Any]:
    """The project's gain reference, read by motion correction and the tilt-series import: a file, or None for
    frames already gain-corrected. Marks the state dirty; the caller saves."""
    if path:
        if not await asyncio.to_thread(Path(path).is_file):
            return err(f"No gain reference file at {path}.")
    state.acquisition.gain_reference_path = path or None
    state.mark_dirty()
    return ok(gain_reference_path=state.acquisition.gain_reference_path)


async def set_tilt_filter(
    backend,
    project_path: Path,
    instance_id: str,
    *,
    mode=None,
    model: str | None = None,
    threshold: float | None = None,
) -> dict[str, Any]:
    """The tilt filter's settings: its DL model (one of conf.yaml's `tilt_filter.models`) and threshold, fixed
    while its DL-auto job runs, then its mode (`backend.set_tilt_filter_mode`, which saves and may submit the
    jobs waiting for this filter's review: `resume`). The filter has no parameters for `set_params`."""
    from services.configs.config_service import get_config_service
    from services.jobs.tilt_filter import FilterMode

    state = backend.state_service.state_for(Path(project_path))
    job_model = state.jobs.get(instance_id)
    if job_model is None or job_model.job_type != JobType.TILT_FILTER:
        return err(f"No tilt filter {instance_id} in this project.")
    if (model is not None or threshold is not None) and job_model.auto_in_flight:
        return err("Fixed while the DL-auto job runs.")
    if model is not None:
        options = list(get_config_service().tilt_filter.models)
        if job_model.model and job_model.model not in options:
            options.append(job_model.model)
        if model not in options:
            return err(f"No tilt-filter model {model!r} (conf.yaml tilt_filter.models: {', '.join(options)}).")
    if threshold is not None:
        threshold, why = check_value(job_model, "threshold", threshold)
        if why:
            return err(f"Not set: threshold: {why}", rejected={"threshold": why})
    if mode is not None:
        try:
            mode = FilterMode(mode)
        except ValueError:
            return err(f"No tilt-filter mode {mode!r} ({', '.join(m.value for m in FilterMode)}).")

    if model is not None and model != (job_model.model or get_config_service().tilt_filter.default_model):
        job_model.model = model
        state.mark_dirty()
    if threshold is not None and threshold != job_model.threshold:
        job_model.threshold = threshold
        state.mark_dirty()
    res = ok(mode=job_model.mode.value, model=job_model.model, threshold=job_model.threshold)
    if mode is not None:
        moved = await backend.set_tilt_filter_mode(Path(project_path), instance_id, mode)
        if not moved["success"]:
            return moved
        res.update(mode=job_model.mode.value, **{k: v for k, v in moved.items() if k == "resume"})
    return res


def describe_inputs(state: ProjectState, instance_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Per input slot of a job, the source its I/O section shows, without paths: the producing job (or the key
    of a file or synthetic source), how it was chosen (automatic, set by hand, fixed at submit), the refining
    job an automatic choice moved onto, the sources that tie, and what keeps a required one from having any.
    With the `needs` those leave open (kind "pin")."""
    job_model = state.jobs[instance_id]
    resolver = PathResolutionService(state)
    rows: list[dict[str, Any]] = []
    needs: list[dict[str, Any]] = []
    for slot in resolver.get_input_schema_for_job(job_model.job_type, job_model):
        v = resolver.validate_input_slot(
            job_model.job_type, job_model, slot.key, check_filesystem=False, consumer_instance_id=instance_id
        )
        source = (override_instance_id(v.source_key) or v.source_key) if v.source_key else None
        row: dict[str, Any] = {
            "slot": slot.key,
            "source": source,
            "how": v.input_state.value,
            "required": slot.required,
        }
        if v.refined_by:
            row["refined_by"] = v.refined_by
        if v.tied:
            row["tied"] = [c.producer_instance_id for c in v.tied]
            needs.append(
                need(
                    "pin",
                    f"{instance_id}.{slot.key}",
                    "two sources are equally good; pick one (set_input)",
                    options=[c.source_key for c in v.tied],
                )
            )
        elif not source and slot.required:
            row["problem"] = v.error_message or "no source"
            needs.append(need("pin", f"{instance_id}.{slot.key}", v.error_message or "no source: set one (set_input)"))
        rows.append(row)
    return rows, needs
