"""Change plans over the API: `plan_changes`, `get_plan`, `apply_changes`, `apply_changes_now`.

A plan is every write of a change, sent at once. Proposing changes nothing: each step's input is checked and
its project passed through the gate (`api/reach.py`); the steps that edit a project's state (the roster,
parameters, SLURM, inputs, a job's deletion, a catalog species, the run's plan) are rehearsed in order on a copy
of the project, or on a blank one for a project the plan makes; the rest are described with what can be read
now (sizes, orphans). The plan is stored (`services/agent_plans.py`) as the user reads it: jobs with their fields
by title, now → after, and every step in a sentence. `plan_changes` answers with that as markdown for the
terminal (`services/agent_plan_view.py`).

Two ways to carry it out, the same run of the steps behind both: `apply_changes` after the user approved on the
page (allow-listed, refused before), and `apply_changes_now`, which the user confirms in a terminal dialog the
MCP forwarder opens with the plan in it (`dialog_text`; a one-time token the agent never sees), refused for a plan
that deletes something for good. The steps run in order through the same
dispatch as single calls, the gate checked again per step; the first that fails stops the plan. Nothing is rolled
back: each step stands on its own, as a click in the UI does.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from api.context import ApiContext, open_project
from api.handlers import Call, _plain, _recipe
from api.reach import write_refusal
from services import job_params, pipeline_edit
from services.agent_plan_view import dialog_form, overview, show
from services.agent_plans import (
    Deletion,
    FieldView,
    JobView,
    Plan,
    PlanStatus,
    PlanStep,
    ProjectPlan,
    StepState,
    get_plan_store,
)
from services.background_tasks import get_background_task_registry
from services.jobs.spec import display_name, instance_display_name
from services.models_base import JobType, SpeciesOrigin
from services.path_resolution_service import consumers_of
from services.project_create import ACQUISITION_FIELDS
from services.project_disk_usage import read_cache
from services.project_state import ProjectState, get_project_state_for
from services.project_trash import list_trash
from services.result import ErrorCode, err, ok

logger = logging.getLogger(__name__)

# Deletes for good: Approve turns red and names them; the terminal path refuses them.
PERMANENT = {
    "clear_trash",
    "delete_project",
    "delete_species",
    "delete_template",
    "delete_mask",
    "delete_kitchen_subset",
}
# Never a plan step: plans do not nest.
NOT_A_STEP = {"apply_changes", "apply_changes_now"}

_ACQ_LABELS = {
    "pixel_size_angstrom": "Pixel size (Å)",
    "acceleration_voltage_kv": "Voltage (kV)",
    "spherical_aberration_mm": "Cs (mm)",
    "amplitude_contrast": "Amplitude contrast",
    "dose_per_tilt": "Dose per tilt (e⁻/Å²)",
    "tilt_axis_degrees": "Tilt axis (°)",
    "detector_dimensions": "Detector (px)",
    "invert_tilt_angles": "Invert tilt angles",
    "invert_defocus_hand": "Invert defocus hand",
    "eer_fractions_per_frame": "EER frames per fraction",
}


def _title(name: str) -> str:
    return " ".join(w.capitalize() for w in name.split("_"))


# ── Steps in words ────────────────────────────────────────────────────────────


def _job_name(state: ProjectState | None, iid: str) -> str:
    jm = state.jobs.get(iid) if state is not None else None
    return instance_display_name(iid, jm) if jm is not None else iid


def _field_label(state: ProjectState | None, iid: str, name: str) -> str:
    jm = state.jobs.get(iid) if state is not None else None
    field = type(jm).model_fields.get(name) if jm is not None else None
    return field.title if field is not None and field.title else _title(name)


def _settings_text(state: ProjectState | None, iid: str, values: dict[str, Any] | None) -> str:
    return ", ".join(f"{_field_label(state, iid, k)} {show(v)}" for k, v in (values or {}).items())


def _args_text(a) -> str:
    shown = {k: v for k, v in a.model_dump().items() if k != "project" and v not in (None, [], {}, False, "")}
    return "; ".join(f"{_title(k).lower()} {show(v)}" for k, v in shown.items())


def _job_type_name(value: str) -> str:
    try:
        return display_name(JobType(value))
    except ValueError:
        return value


def sentence(verb: str, a, state: ProjectState | None) -> str:
    """A step as the user reads it; `state` names the jobs (the rehearsal's copy before this step)."""
    j = lambda iid: _job_name(state, iid)  # noqa: E731
    if verb == "create_project":
        if a.tomograms_from:
            sources = "; ".join(
                f"{Path(s.project).name} ({s.job}" + (f", {len(s.names)} tomograms)" if s.names else ")")
                for s in a.tomograms_from
            )
            return f"Make the project {a.name} in {a.base} from the tomograms of {sources}"
        if a.protocol:
            return f"Make the project {a.name} in {a.base} from the protocol {a.protocol}"
        if a.directory or a.frames_glob:
            series = f", {len(a.series)} series" if a.series else ", every series"
            return f"Make the project {a.name} in {a.base} from {a.directory or a.frames_glob}{series}"
        return f"Make the project {a.name} in {a.base}, with no raw data"
    if verb == "add_job":
        text = f"Add {_job_type_name(a.job_type)}"
        if a.species:
            text += f" for {a.species}"
        if a.clone_of:
            text += f", a clone of {j(a.clone_of)}"
        if a.after:
            text += f", after {j(a.after)}"
        if a.reading:
            text += ", reading " + ", ".join(f"{slot} from {src}" for slot, src in a.reading.items())
        return text
    if verb == "remove_job":
        return f"Take {j(a.job)} off the roster"
    if verb == "delete_job":
        return f"Delete {j(a.job)}"
    if verb == "set_params":
        return f"Set {j(a.job)}: {_settings_text(state, a.job, a.values)}"
    if verb == "set_slurm":
        if a.reset:
            return f"Reset {j(a.job)}'s SLURM settings to the project's and the job type's"
        labels = dict(job_params.SLURM_FIELDS)
        return f"Set {j(a.job)}'s SLURM: " + ", ".join(f"{labels.get(k, k)} {show(v)}" for k, v in a.values.items())
    if verb == "set_input":
        if a.facet is not None and not (a.auto or a.source or a.path is not None):
            return f"Take {j(a.job)}'s {a.slot} " + (
                f"from {_facet_text(a.facet)}" if a.facet else "from its set's sources as Run finds them"
            )
        target = "automatic" if a.auto else (a.source or a.path or "automatic")
        text = f"Point {j(a.job)}'s input {a.slot} at {target}"
        return text + (f", taking it from {_facet_text(a.facet)}" if a.facet else "")
    if verb == "reset_inputs":
        return f"Let {j(a.job)} choose its inputs again at its next run"
    if verb == "set_tilt_filter":
        return f"Set {j(a.job)}: {_args_text(a).replace('job ' + a.job + '; ', '')}"
    if verb == "exclude_tilt_series":
        return f"{'Leave out' if a.excluded else 'Bring back'} the tilt series {a.ts}"
    if verb == "set_gain":
        return f"Use {a.path} as the gain reference" if a.path else "Use no gain reference"
    if verb == "import_species":
        return f"Import the species {a.catalog_id} from the lab catalog" + (f" (v{a.version})" if a.version else "")
    if verb == "create_species":
        return f"Make the species {a.name}"
    if verb == "set_species":
        return f"Edit the species {a.species}"
    if verb == "add_template":
        return f"Register {Path(a.path).name} as a template of {a.species}"
    if verb == "add_mask":
        return f"Register {Path(a.path).name} as a mask of {a.species}"
    if verb == "make_template":
        return f"Make a template for {a.species} ({a.kind})"
    if verb == "make_mask":
        return f"Make a mask for {a.species} ({a.kind})"
    if verb == "select_template":
        return f"Select the template {a.template_id} for {a.species}"
    if verb == "select_mask":
        return f"Select the mask {a.mask_id} for {a.species}"
    if verb == "import_tomograms":
        return f"Import tomograms ({a.mode})"
    if verb == "run":
        return "Run the roster"
    if verb == "stop":
        return "Stop the run: cancel every queued and running job"
    if verb == "cancel_job":
        return f"Cancel {j(a.job)}"
    if verb == "stop_after_iteration":
        return f"Let {j(a.job)} run on to its end" if a.withdraw else f"Stop {j(a.job)} once its iteration has landed"
    if verb == "predict_tilts":
        return f"Run {j(a.job)}'s DL prediction"
    if verb == "cancel_predict_tilts":
        return f"Cancel {j(a.job)}'s DL prediction"
    if verb == "cancel_leftovers":
        return "Cancel the leftover SLURM jobs " + ", ".join(a.slurm_ids)
    if verb == "delete_species":
        return f"Delete the species {a.species}: its template and mask files, and its jobs"
    if verb == "delete_template":
        return f"Delete the template {a.template_id} of {a.species}"
    if verb == "delete_mask":
        return f"Delete the mask {a.mask_id} of {a.species}"
    if verb == "clear_trash":
        return "Empty the Trash" + (f": {', '.join(a.names)}" if a.names else "")
    if verb == "delete_project":
        return f"Delete the project {Path(a.project).name}"
    if verb.endswith(("kitchen_sources", "kitchen_source", "kitchen_subset", "kitchen_group", "kitchen_set")):
        return _kitchen_sentence(verb, a)
    if verb == "make_class_set":
        classes = ", ".join(str(c) for c in a.classes)
        return f"Make the set {a.name} of {j(a.job)}'s class{'es' if len(a.classes) > 1 else ''} {classes}"
    if verb == "make_polarity_set":
        undecided = "kept as they are" if a.keep_undecided else "left out"
        return f"Make the set {a.name} of {j(a.job)}'s filaments turned to their majority (undecided {undecided})"
    if verb == "make_expansion_set":
        if a.twist_deg is not None or a.rise_ang is not None:
            how = (
                f"the registrations of a lattice of {a.twist_deg}° and {a.rise_ang} Å with {a.axial_repeat_ang} Å "
                f"along the axis, {a.units_per_repeat} units to a repeat"
            )
        else:
            how = "the operators of its point group"
        return f"Make the set {a.name}: {j(a.job)}'s particles in every pose of {how}, the same images"
    if verb == "make_subparticle_set":
        if a.twist_deg is not None or a.rise_ang is not None:
            where = f"a helix of {a.twist_deg}° and {a.rise_ang} Å at {a.radius_ang} Å from the axis"
        else:
            where = f"the {a.axis or 'direction ' + str(a.direction)} at {a.radius_ang} Å"
        return f"Make the set {a.name}: the {a.species} sub-units of {j(a.job)}'s particles, on {where}"
    text = _args_text(a)
    return verb.replace("_", " ").capitalize() + (f": {text}" if text else "")


def _recipe_text(a) -> str:
    """The recipe in one line, a category filter with its values named (the kitchen's own line counts them)."""
    from services.sta_kitchen.service import describe_draft

    draft = _recipe(a).model_dump()
    named = [f"{f['col']} = {' or '.join(f['allowed'])}" for f in draft["filters"] if f["kind"] == "category"]
    draft["filters"] = [f for f in draft["filters"] if f["kind"] != "category"]
    rest = describe_draft(draft).replace("every row", "every pick")
    return " · ".join(named + ([rest] if rest != "every pick" or not named else []))


def _kitchen_sentence(verb: str, a) -> str:
    """A kitchen step in words; subsets and groups by the names the step gives."""
    where = f"in the {a.species} kitchen"
    if verb == "add_kitchen_sources":
        return f"Add to the {a.species} kitchen: " + "; ".join(f"{Path(x.project).name} {x.job}" for x in a.sources)
    if verb == "remove_kitchen_source":
        return f"Take {a.source} out of the {a.species} kitchen"
    if verb == "create_kitchen_subset":
        group = f" in the group {a.group}" if a.group else ""
        what = f"{len(a.uids):,} picks by uid" if a.uids else _recipe_text(a)
        return f"Make the subset {a.name}{group} {where}: {what}"
    if verb == "edit_kitchen_subset":
        parts = [f"name {a.name}" if a.name else "", f"into the group {a.group}" if a.group else ""]
        parts.append("out of its group" if a.ungroup else "")
        parts.append(f"add {len(a.add_uids):,} picks by uid" if a.add_uids else "")
        parts.append(f"take out {len(a.remove_uids):,} picks by uid" if a.remove_uids else "")
        return f"Edit the subset {a.subset} {where}: " + ", ".join(p for p in parts if p)
    if verb == "delete_kitchen_subset":
        return f"Delete the subset {a.subset} {where} (its picks stay in the pool)"
    if verb == "create_kitchen_group":
        return f"Make the group {a.name} {where}"
    if verb == "rename_kitchen_group":
        return f"Rename the group {a.group} {where} to {a.name}"
    if verb == "delete_kitchen_group":
        return f"Delete the group {a.group} {where} (its subsets stay)"
    if verb == "make_kitchen_set":
        if a.group:
            return f"Make a set of each subset of the group {a.group} {where}: {_recipe_text(a)}"
        source = f"the subset {a.subset}, " if a.subset else ""
        return f"Make the set {a.name or a.subset} {where} from {source}{_recipe_text(a)}"
    return verb


# ── Rehearsal ─────────────────────────────────────────────────────────────────


def _rehearse_delete_job(state: ProjectState, iid: str) -> dict[str, Any]:
    """What `pipeline_edit.delete_job` does to the state, without the move to Trash."""
    notice = pipeline_edit.in_use_notice(state, iid)
    if notice:
        return err(notice)
    jm = state.jobs.get(iid)
    if jm is not None and jm.relion_job_name:
        state.jobs.pop(iid, None)
        state.job_path_mapping.pop(iid, None)
        if iid in state.pipeline_order:
            return pipeline_edit.remove_job(state, iid)
        return ok(instance_id=iid)
    return pipeline_edit.remove_job(state, iid)


def _rehearse(state: ProjectState, verb: str, a) -> dict[str, Any] | None:
    """The step done on the copy, as its door does it; None for a verb not rehearsed."""
    if verb == "add_job":
        try:
            job_type = JobType(a.job_type)
        except ValueError:
            return err(f"No job type {a.job_type!r} (list_job_types lists them).")
        return pipeline_edit.add_job(
            state, job_type, species_id=a.species, after=a.after, reading=a.reading, clone_of=a.clone_of
        )
    if verb == "remove_job":
        return pipeline_edit.remove_job(state, a.job)
    if verb == "delete_job":
        return _rehearse_delete_job(state, a.job)
    if verb == "set_params":
        return job_params.set_params(state, a.job, a.values)
    if verb == "set_slurm":
        return job_params.set_slurm(state, a.job, a.values, reset=a.reset)
    if verb == "set_input":
        return job_params.set_input(state, a.job, a.slot, source=a.source, path=a.path, auto=a.auto, facet=a.facet)
    if verb == "reset_inputs":
        return job_params.reset_inputs(state, a.job)
    return None


async def _rehearse_import_species(state: ProjectState, a) -> dict[str, Any]:
    """A catalog species in the copy as `catalog.import_species` makes it, its files left where the catalog keeps
    them (nothing is copied): enough for the particle jobs after it to take its template, mask and symmetry."""
    from services.particles import catalog

    try:
        record = await asyncio.to_thread(catalog.read_species, a.catalog_id, a.version)
    except (FileNotFoundError, ValueError) as e:
        return err(str(e))
    vdir = catalog.version_dir(record.catalog_id, record.version)
    sp = state.add_species(record.name, origin=SpeciesOrigin.IMPORTED, color=record.color)
    sp.catalog_id, sp.catalog_version = record.catalog_id, record.version
    sp.diameter_ang, sp.symmetry, sp.notes = record.diameter_ang, record.symmetry, record.notes
    if vdir is not None:
        sp.templates = [
            t.model_copy(update={"template_path": str(vdir / catalog.TEMPLATES_DIR / t.template_path)})
            for t in record.templates
        ]
        sp.masks = [
            m.model_copy(update={"mask_path": str(vdir / catalog.MASKS_DIR / m.mask_path)}) for m in record.masks
        ]
    sp.selected_template_id = record.selected_template_id or (sp.templates[0].id if sp.templates else "")
    sp.selected_mask_id = record.selected_mask_id or (sp.masks[0].id if sp.masks else "")
    return ok(species=sp.id)


def _job_settings(jm) -> dict[str, dict[str, Any]]:
    return {
        "param": {n: _plain(getattr(jm, n, None)) for n in sorted(jm.USER_PARAMS)},
        "slurm": _plain(dict(jm.slurm_overrides)),
        "input": _plain(dict(jm.source_overrides)),
    }


def fingerprint(state: ProjectState) -> str:
    """What a plan was proposed against: the roster and every job's settings, the species and their files.
    Statuses are left out, so a run going on does not make a plan stale; a step refused by its own door over a
    status (a job that started meanwhile) is refused when it runs."""
    data = {
        "roster": state.roster(),
        "jobs": {iid: _job_settings(jm) for iid, jm in sorted(state.jobs.items())},
        "species": [
            (s.id, [t.id for t in s.templates], [m.id for m in s.masks], s.selected_template_id, s.selected_mask_id)
            for s in state.species_registry
        ],
    }
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


# ── Jobs as their tabs show them ──────────────────────────────────────────────


def _snapshot(jm) -> dict[str, dict[str, Any]]:
    """A job's fields as values: parameters, effective SLURM settings, inputs set by hand."""
    eff = jm.get_effective_slurm_config()
    return {
        "param": {n: _plain(getattr(jm, n, None)) for n in jm.USER_PARAMS},
        "slurm": {n: _plain(getattr(eff, n, None)) for n, _ in job_params.SLURM_FIELDS},
        "input": {**_plain(dict(jm.source_overrides)), **_facet_rows(jm)},
    }


def _facet_text(facet: dict[str, str]) -> str:
    """Facet choices as words: per source (`<project path>|<instance id>`) the projects it serves; a project taking
    them from a job of its own is named once, as "each project's own <job>"."""
    own: dict[str, list[str]] = {}
    by_source: dict[str, list[str]] = {}
    for project, source in facet.items():
        where, _, iid = source.partition("|")
        if iid and project != "*" and Path(where) == Path(project):
            own.setdefault(iid, []).append(Path(project).name)
        else:
            by_source.setdefault(source, []).append("the whole set" if project == "*" else Path(project).name)
    parts = [f"each project's own {iid} ({', '.join(sorted(names))})" for iid, names in own.items()]
    for source, projects in by_source.items():
        where, _, iid = source.partition("|")
        parts.append((f"{iid} of {Path(where).name}" if iid else source) + f" for {', '.join(sorted(projects))}")
    return "; ".join(parts)


def _facet_rows(jm) -> dict[str, str]:
    """A row per input whose kitchen-set sources were chosen: where each project's picks are taken from."""
    return {
        f"{slot} sources": _facet_text(dict(choices)) for slot, choices in (jm.facet_choices or {}).items() if choices
    }


def _view_name(iid: str, jm) -> str:
    """The job as the roster names it, with its species when it has one."""
    name = instance_display_name(iid, jm)
    return f"{name} · {jm.species_id}" if jm.species_id else name


def _kind_of(value: Any) -> tuple[str, list[Any] | None]:
    if isinstance(value, bool):
        return "bool", None
    if isinstance(value, Enum):
        return "choice", [e.value for e in type(value)]
    if isinstance(value, int):
        return "int", None
    if isinstance(value, float):
        return "float", None
    if isinstance(value, str):
        return "text", None
    return "list", None


def _job_view(state: ProjectState, iid: str, mark: str, base: dict | None, clone_of: str | None) -> JobView:
    jm = state.jobs[iid]
    frozen = job_params.is_frozen(jm)
    view = JobView(
        instance_id=iid,
        name=_view_name(iid, jm),
        status=jm.execution_status.value,
        mark=mark,
        clone_of=clone_of,
        species=jm.species_id,
        frozen=frozen,
    )
    now = _snapshot(jm)
    base = base or now
    for name, info in type(jm).model_fields.items():
        if name not in jm.USER_PARAMS:
            continue
        value = getattr(jm, name)
        kind, choices = _kind_of(value)
        try:
            options = job_params.param_options(state, iid, name)
        except (KeyError, LookupError, OSError, ValueError):
            # Options that cannot be listed in a rehearsal (a species the copy lacks): the field is shown, not offered.
            options = None
        if options is not None and kind != "list":
            kind, choices = "choice", _plain(options)
        view.fields.append(
            FieldView(
                name=name,
                label=info.title or _title(name),
                kind="param",
                before=base["param"].get(name),
                after=now["param"].get(name),
                type=kind,
                choices=choices,
                bounds=job_params.field_bounds(jm, name) or None,
                help=info.description,
                editable=not frozen and kind != "list",
            )
        )
    for name, label in job_params.SLURM_FIELDS:
        after = now["slurm"].get(name)
        kind, _ = _kind_of(after)
        view.fields.append(
            FieldView(
                name=name,
                label=label,
                kind="slurm",
                before=base["slurm"].get(name),
                after=after,
                type=kind,
                editable=not frozen and kind in ("int", "text"),
            )
        )
    b_in, a_in = base["input"], now["input"]
    for slot in sorted(set(b_in) | set(a_in)):
        if b_in.get(slot) != a_in.get(slot):
            view.fields.append(
                FieldView(name=slot, label=slot, kind="input", before=b_in.get(slot), after=a_in.get(slot))
            )
    return view


def _job_views(
    before: ProjectState | None, after: ProjectState, baseline: dict, deleted: set[str], clones: dict[str, str]
) -> list[JobView]:
    """The roster after the plan, with the jobs it takes off left where they stood."""
    old = before.roster() if before is not None else []
    new = after.roster()
    views = [
        _job_view(
            after,
            iid,
            "kept" if iid in old else ("cloned" if iid in clones else "added"),
            baseline.get(iid),
            clones.get(iid),
        )
        for iid in new
        if iid in after.jobs
    ]
    for i, iid in enumerate(old):
        if iid in new or before is None or iid not in before.jobs:
            continue
        jm = before.jobs[iid]
        gone = JobView(
            instance_id=iid,
            name=_view_name(iid, jm),
            status=jm.execution_status.value,
            mark="deleted" if iid in deleted else "removed",
            species=jm.species_id,
            frozen=True,
        )
        prev = next((p for p in reversed(old[:i]) if any(v.instance_id == p for v in views)), None)
        at = 0 if prev is None else next(k for k, v in enumerate(views) if v.instance_id == prev) + 1
        views.insert(at, gone)
    return views


# ── Proposing ─────────────────────────────────────────────────────────────────


def plan_url(ctx: ApiContext, plan_id: str) -> str:
    return f"http://{ctx.host}:{ctx.port}/agent/plan/{plan_id}"


def _sizes(project: Path) -> tuple[dict[str, int], int | None]:
    usage = read_cache(project)
    if usage is None:
        return {}, None
    return dict(usage.jobs or {}), usage.total_bytes


def _trash_sizes(project: Path) -> dict[str, int]:
    return {e.name: e.size_bytes for e in list_trash(project)}


async def _blank_state(target: Path, a) -> tuple[ProjectState, list[tuple[str, str]]]:
    """A project the plan makes, as an empty state with its values: what its steps are rehearsed on. The values
    are what `create_project` is given, or the first source's for tomograms from other projects."""
    state = ProjectState(project_name=target.name, project_path=target)
    values: list[tuple[str, str]] = []
    if a.tomograms_from:
        for s in a.tomograms_from:
            what = f"{len(s.names)} tomograms" if s.names else "every tomogram"
            values.append(("Tomograms", f"{what} of {Path(s.project).name}, {s.job}"))
        src, why = await open_project(a.tomograms_from[0].project)
        if why is None:
            source = get_project_state_for(src)
            state.microscope = source.microscope.model_copy()
            state.acquisition = source.acquisition.model_copy()
            values.append(("Values from", src.name))
    else:
        if a.protocol:
            values.append(("Protocol", a.protocol))
        if a.directory:
            values.append(("Data folder", a.directory))
        if a.frames_glob:
            values.append(("Frames", a.frames_glob))
        if a.mdocs_glob:
            values.append(("Mdocs", a.mdocs_glob))
        values.append(("Series", f"{len(a.series)} chosen" if a.series else "all"))
        for name, (model, attr) in ACQUISITION_FIELDS.items():
            given = getattr(a, name, None)
            if given is not None:
                setattr(getattr(state, model), attr, given)
        values.append(("Gain reference", a.gain_reference or "none"))
    for name, (model, attr) in ACQUISITION_FIELDS.items():
        values.append((_ACQ_LABELS.get(name, _title(name)), show(_plain(getattr(getattr(state, model), attr)))))
    if a.shared:
        values.append(("Lab project", "yes"))
    return state, values


async def build(ctx: ApiContext, title: str, raw_steps: list[dict[str, Any]]) -> dict[str, Any]:
    """Check and rehearse a plan's steps: `ok(steps, projects)`, or the refusal naming the step. Changes
    nothing. Also the store's rebuilder, so an edit on the page is checked as a proposal is."""
    steps: list[PlanStep] = []
    typed: list[Any] = []
    made: dict[str, Path] = {}
    creates: dict[str, Any] = {}
    projects: dict[str, ProjectPlan] = {}

    for i, raw in enumerate(raw_steps, start=1):
        name, args = raw["verb"], raw.get("args") or {}
        verb = ctx.verbs.get(name)
        if verb is None or verb.kind != "write" or name in NOT_A_STEP:
            return err(f"Step {i}: {name!r} is not a write verb a plan can hold.", step=i)
        try:
            a = verb.model.model_validate(args)
        except ValueError as e:
            return err(f"Step {i} ({name}): bad input: {e}", step=i)
        typed.append(a)
        target: Path | None = None
        if name == "create_project":
            target = Path(a.base).expanduser() / a.name.strip()
            if await asyncio.to_thread(target.exists):
                return err(f"Step {i}: {target} exists already: the API makes new projects only.", step=i)
            made[a.name.strip()] = made[str(target)] = target
            creates[str(target)] = a
            projects[str(target)] = ProjectPlan(path=str(target), name=target.name, new=True)
        elif verb.names_project:
            if a.project in made:
                target = made[a.project]
            else:
                target, why = await open_project(a.project)
                if why is not None:
                    return err(f"Step {i} ({name}): {why['error']}", step=i)
                key = str(target)
                if key not in projects:
                    state = get_project_state_for(target)
                    refusal = await write_refusal(state, target)
                    if refusal:
                        return err(
                            f"Step {i} ({name}): {refusal}",
                            step=i,
                            code=ErrorCode.NOT_WRITABLE,
                            ui_url=ctx.ui_url(target),
                        )
                    projects[key] = ProjectPlan(
                        path=key, name=target.name, hand_made=not state.made_by_api, fingerprint=fingerprint(state)
                    )
        steps.append(
            PlanStep(
                index=i,
                verb=name,
                args=args,
                project=str(target) if target is not None else None,
                what=sentence(name, a, None),
                permanent=name in PERMANENT,
            )
        )

    needs: list[dict[str, Any]] = []
    for key, pp in projects.items():
        problem = await _rehearse_project(ctx, Path(key), pp, steps, typed, needs, creates.get(key))
        if problem is not None:
            return problem
    for step, a in zip(steps, typed, strict=True):
        if step.project is None:
            step.what = sentence(step.verb, a, None)

    if needs:
        return err(
            "The plan has open decisions: put them to the user, then propose again with the answers.",
            code=ErrorCode.NEEDS_DECISION,
            needs=needs,
        )
    return ok(steps=steps, projects=projects)


async def _rehearse_project(
    ctx: ApiContext, path: Path, pp: ProjectPlan, steps: list[PlanStep], typed: list[Any], needs: list[dict], spec
) -> dict[str, Any] | None:
    """Rehearse this project's steps on a copy (a blank one for a new project) and fill `pp`. An err to return
    when a step on an existing project is refused; on a new project a step that cannot be previewed is noted
    and runs all the same."""
    if pp.new:
        copy, pp.values = await _blank_state(path, spec)
        before, sizes, total = None, {}, None
    else:
        live = get_project_state_for(path)
        before = await asyncio.to_thread(live.detached_copy)
        copy = await asyncio.to_thread(live.detached_copy)
        sizes, total = await asyncio.to_thread(_sizes, path)
    baseline: dict[str, dict] = {iid: _snapshot(jm) for iid, jm in copy.jobs.items()}
    deleted: set[str] = set()
    clones: dict[str, str] = {}

    for step, a in zip(steps, typed, strict=True):
        if step.project != str(path):
            continue
        step.what = sentence(step.verb, a, copy)
        if step.verb == "create_project":
            step.simulated = True
            continue
        if step.verb == "run":
            if pp.new:
                pp.also.append(step.what + " (what it submits is planned once the project exists)")
                continue
            step.simulated = True
            pp.run = await ctx.backend.pipeline_orchestrator.plan_run(path, state=copy)
            needs.extend(pp.run.get("needs") or [])
            continue
        if step.verb == "delete_job":
            jm = copy.jobs.get(a.job)
            job_dir = jm.relion_job_name.rstrip("/") if jm is not None and jm.relion_job_name else None
            if job_dir is not None:
                pp.deletions.append(
                    Deletion(
                        what=f"{instance_display_name(a.job, jm)} ({job_dir})",
                        size_bytes=sizes.get(job_dir),
                        permanent=False,
                        step=step.index,
                        orphans=[_job_name(copy, o) for o in consumers_of(copy, a.job)],
                    )
                )
                deleted.add(a.job)
        if step.verb == "import_species":
            res = await _rehearse_import_species(copy, a)
        elif step.verb == "set_input" and a.facet:
            why = await asyncio.to_thread(job_params.facet_refusal, copy, a.job, a.slot, a.source)
            res = why or _rehearse(copy, step.verb, a)
        else:
            res = _rehearse(copy, step.verb, a)
        if res is None:
            if not await _describe_deletion(path, step, a, copy, pp, total):
                pp.also.append(step.what)
            continue
        if not res["success"]:
            if pp.new:
                step.note = res["error"]
                pp.also.append(step.what)
                continue
            return err(f"Step {step.index} ({step.what}) on {path.name}: {res['error']}", step=step.index)
        step.simulated = True
        needs.extend(res.get("needs") or [])
        if step.verb == "add_job":
            for iid in res.get("added") or [res.get("instance_id")]:
                if iid and iid in copy.jobs and iid not in baseline:
                    baseline[iid] = _snapshot(copy.jobs[iid])
            if a.clone_of and res.get("instance_id"):
                clones[res["instance_id"]] = _job_name(copy, a.clone_of)

    pp.jobs = _job_views(before, copy, baseline, deleted, clones)
    return None


async def _describe_deletion(path: Path, step: PlanStep, a, state: ProjectState, pp: ProjectPlan, total) -> bool:
    """The deletions that are not rehearsed, with their sizes where they are known. False for a step that
    deletes nothing."""
    if step.verb == "delete_project":
        pp.deletions.append(
            Deletion(what=f"the whole project {path.name}", size_bytes=total, permanent=True, step=step.index)
        )
    elif step.verb == "clear_trash":
        trash = await asyncio.to_thread(_trash_sizes, path)
        names = a.names if a.names is not None else sorted(trash)
        size = sum(trash.get(n, 0) for n in names)
        pp.deletions.append(
            Deletion(what=f"Trash: {', '.join(names) or 'nothing'}", size_bytes=size, permanent=True, step=step.index)
        )
    elif step.verb == "delete_species":
        refs = state.species_references(a.species)
        sp = state.get_species(a.species)
        n_files = len(sp.templates) + len(sp.masks) if sp is not None else 0
        pp.deletions.append(
            Deletion(
                what=f"the species {a.species}: {n_files} template and mask files; its jobs go to Trash",
                size_bytes=None,
                permanent=True,
                step=step.index,
                orphans=[_job_name(state, iid) for iid in refs.get("jobs", [])],
            )
        )
    elif step.verb in ("delete_template", "delete_mask"):
        kind = "template" if step.verb == "delete_template" else "mask"
        file_id = a.template_id if kind == "template" else a.mask_id
        pp.deletions.append(
            Deletion(
                what=f"the {kind} {file_id} of {a.species} (file and sidecar)",
                size_bytes=None,
                permanent=True,
                step=step.index,
            )
        )
    else:
        return False
    return True


def _answer(ctx: ApiContext, plan: Plan, **extra: Any) -> dict[str, Any]:
    url = plan_url(ctx, plan.id)
    return ok(
        plan_id=plan.id,
        revision=plan.revision,
        status=plan.status.value,
        overview=overview(plan, url),
        projects=plan.lines(),
        permanent=[s.what for s in plan.permanent_steps],
        expires_at=plan.expires_at.isoformat(timespec="minutes"),
        ui_url=url,
        **extra,
    )


async def plan_changes(call: Call) -> dict[str, Any]:
    raw = [{"verb": s.verb, "args": s.args} for s in call.args.steps]
    res = await build(call.ctx, call.args.title, raw)
    if not res["success"]:
        return res
    plan = get_plan_store().add(call.args.title, res["steps"], res["projects"])
    if plan.permanent_steps:
        how = (
            "Print `overview` to the user exactly as it is. It deletes something for good, so the user approves it "
            "on the page (ui_url); then apply_changes(plan_id, revision)."
        )
    else:
        how = (
            "Call apply_changes_now(plan_id, revision) now: the server shows the user this plan in a terminal "
            "dialog and applies it only on their yes. They may also edit or approve it on the page (ui_url)."
        )
    return _answer(call.ctx, plan, message="Nothing has changed yet. " + how)


# ── Reading and applying ──────────────────────────────────────────────────────


def _step_row(s: PlanStep) -> dict[str, Any]:
    row: dict[str, Any] = {"step": s.index, "what": s.what, "state": s.state.value}
    if s.result is not None and not s.result.get("success"):
        row["error"] = s.result.get("error")
    return row


async def get_plan(call: Call) -> dict[str, Any]:
    plan = get_plan_store().get(call.args.plan_id)
    if plan is None:
        return err(f"No plan {call.args.plan_id} (plans live in the server's memory: a restart forgets them).")
    return _answer(
        call.ctx,
        plan,
        title=plan.title,
        steps=[_step_row(s) for s in plan.steps],
        error=plan.error,
        task_id=plan.task_id,
    )


async def _wait_for_task(ctx: ApiContext, task_id: str) -> dict[str, Any]:
    registry = get_background_task_registry()
    while True:
        record = registry.get(task_id)
        if record is None or not record.is_running:
            break
        await asyncio.sleep(1.0)
    result = ctx.results.get(task_id)
    if result is not None:
        return result
    if record is not None and record.status == "succeeded":
        return ok(message=record.result_message or "")
    return err(record.error if record is not None and record.error else f"Background task {task_id} did not finish.")


async def _carry_out(ctx: ApiContext, plan: Plan, revision: int | None) -> dict[str, Any]:
    """Run an approved or confirmed plan's steps in the background."""
    url = plan_url(ctx, plan.id)
    if revision is not None and revision != plan.revision:
        return err(
            f"{plan.id} was edited on its page (now revision {plan.revision}): show the user get_plan's overview "
            "and ask again.",
            ui_url=url,
        )
    for key, pp in plan.projects.items():
        if pp.new:
            continue
        if fingerprint(get_project_state_for(Path(key))) != pp.fingerprint:
            plan.status = PlanStatus.FAILED
            plan.error = f"{pp.name} changed after the plan was proposed."
            return err(f"{pp.name} changed after the plan was proposed: propose the change again.", ui_url=url)

    plan.status = PlanStatus.RUNNING

    async def work(progress_cb) -> str:
        total = len(plan.steps)
        for n, step in enumerate(plan.steps):
            progress_cb(n, total, step.what)
            verb = ctx.verbs[step.verb]
            res = await ctx.run_verb(verb, verb.model.model_validate(step.args), plan_id=plan.id)
            if res.get("success") and res.get("task_id"):
                res = await _wait_for_task(ctx, res["task_id"])
            step.result = res
            if not res.get("success"):
                step.state = StepState.FAILED
                for rest in plan.steps[n + 1 :]:
                    rest.state = StepState.NOT_RUN
                plan.status = PlanStatus.FAILED
                plan.error = f"Step {step.index} ({step.what}): {res.get('error')}"
                plan.finished_at = datetime.now()
                raise RuntimeError(plan.error)
            step.state = StepState.DONE
        plan.status = PlanStatus.DONE
        plan.finished_at = datetime.now()
        progress_cb(total, total, "done")
        return f"{plan.title}: {total} steps done"

    plan.task_id = get_background_task_registry().submit(
        work, title=f"Agent plan: {plan.title}", dedup_key=f"plan:{plan.id}"
    )
    return ok(
        task_id=plan.task_id,
        plan_id=plan.id,
        ui_url=url,
        message="Applying the plan step by step in the background: follow it with get_plan.",
    )


def _plan_or_err(plan_id: str) -> tuple[Plan | None, dict[str, Any] | None]:
    plan = get_plan_store().get(plan_id)
    if plan is None:
        return None, err(f"No plan {plan_id} (plans live in the server's memory: a restart forgets them).")
    return plan, None


async def apply_changes(call: Call) -> dict[str, Any]:
    plan, why = _plan_or_err(call.args.plan_id)
    if why is not None:
        return why
    url = plan_url(call.ctx, plan.id)
    if plan.status == PlanStatus.PROPOSED:
        return err(
            f"The user has not approved {plan.id} on its page. To confirm it in the terminal instead, call "
            "apply_changes_now.",
            ui_url=url,
        )
    if plan.status != PlanStatus.APPROVED:
        return err(f"{plan.id} is {plan.status.value}: nothing to apply. Propose the change again.", ui_url=url)
    return await _carry_out(call.ctx, plan, call.args.revision)


async def apply_changes_now(call: Call) -> dict[str, Any]:
    """The terminal path, in two calls. Without a confirmation it changes nothing: it answers the plan as the
    crboost MCP forwarder shows it in a dialog of its own (`confirm`, with a one-time token), and the forwarder,
    on the user's yes there, calls again with the token. The agent never sees the token, so neither it nor an
    allow rule nor auto mode can say yes for the user."""
    plan, why = _plan_or_err(call.args.plan_id)
    if why is not None:
        return why
    url = plan_url(call.ctx, plan.id)
    if not plan.editable:
        return err(f"{plan.id} is {plan.status.value}: nothing to apply. Propose the change again.", ui_url=url)
    if plan.permanent_steps:
        return err(
            f"{plan.id} deletes something for good ({'; '.join(s.what for s in plan.permanent_steps)}): the user "
            "approves that on the page, then apply_changes.",
            ui_url=url,
        )
    if call.args.revision is not None and call.args.revision != plan.revision:
        return err(
            f"{plan.id} was edited on its page (now revision {plan.revision}): show the user get_plan's overview "
            "and ask again.",
            ui_url=url,
        )
    given = call.args.confirmation
    if given is None and not call.args.dialog:
        return err(
            f"{plan.id} waits for the user's yes in a terminal dialog, and this crboost MCP connection does not show "
            "one (a forwarder older than the server: the user reconnects it with /mcp). Until then the user "
            "approves the plan on its page (ui_url), then apply_changes.",
            ui_url=url,
        )
    if given is None:
        token = secrets.token_urlsafe(18)
        message, schema = dialog_form(plan, url)
        plan.confirmation = (token, plan.revision)
        return err(
            f"{plan.id} waits for the user's Accept in the terminal dialog. This client could not show it: the user "
            "approves the plan on its page (ui_url), then apply_changes.",
            ui_url=url,
            confirm={"token": token, "message": message, "schema": schema},
        )
    held, plan.confirmation = plan.confirmation, None
    if held is None or not secrets.compare_digest(given, held[0]) or held[1] != plan.revision:
        return err(f"That confirmation is not {plan.id}'s current one: nothing was applied. Ask again.", ui_url=url)
    plan.status = PlanStatus.APPROVED
    plan.decided_at = datetime.now()
    return await _carry_out(call.ctx, plan, plan.revision)
