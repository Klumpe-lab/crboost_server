"""The verbs' handlers. Each reads in-memory state or calls the door its UI button calls (services/), and answers
with `ok()` / `err()` plus the link to what it touched. The router has already opened the project a verb names
and, for a write, checked that the API made it; it saves and records after a write."""

from __future__ import annotations

import asyncio
import getpass
import socket
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from api.context import ApiContext, record_write
from services import (
    array_status_cache,
    dataset_scan,
    job_params,
    pipeline_edit,
    species_admin,
    species_edit,
    tomogram_import,
)
from services.array_tasks import STATUS_DIR_NAME, manifest_items, scan_statuses
from services.background_tasks import get_background_task_registry
from services.code_version import code_version
from services.configs.config_service import get_config_service
from services.jobs.spec import JOB_SPECS, PARAM_CLASS_BY_TYPE, PHASE_PARTICLES, display_name, instance_display_name
from services.models_base import JobType
from services.path_resolution_service import PathResolutionService, auto_exclusion, unreadable_reason
from services.project_create import ACQUISITION_FIELDS, ProjectSpec, required_values, spec_problems
from services.project_resolve import resolve_project
from services.project_state import TemplateMask, get_project_state_for
from services.result import ErrorCode, err, need, ok
from services.routes import View

API_VERSION = 1


@dataclass
class Call:
    """One verb call: its context, its validated input, and the project it names, opened."""

    ctx: ApiContext
    args: BaseModel
    path: Path | None = None

    @property
    def backend(self):
        return self.ctx.backend

    @property
    def state(self):
        return get_project_state_for(self.path)

    def url(self, view: View = View.PIPELINE, a: str | None = None) -> str:
        return self.ctx.ui_url(self.path, view, a)

    def job(self, instance_id: str) -> tuple[Any, dict[str, Any] | None]:
        job_model = self.state.jobs.get(instance_id)
        if job_model is None:
            listed = ", ".join(self.state.roster()) or "none"
            return None, err(f"No job {instance_id} in {self.path.name} (on its roster: {listed}).")
        return job_model, None


def _plain(value: Any) -> Any:
    """A value as JSON carries it: enums by value, tuples as lists, paths as text."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, Path):
        return str(value)
    return value


# ── Reads ─────────────────────────────────────────────────────────────────────


async def get_server_info(call: Call) -> dict[str, Any]:
    cfg = get_config_service()
    host = socket.gethostname()
    return ok(
        owner=getpass.getuser(),
        host=host,
        port=call.ctx.port,
        code_version=code_version(),
        default_project_base=cfg.default_project_base or str(Path.home()),
        conf_warnings=list(cfg.load_warnings),
        api_version=API_VERSION,
        ui_url=f"http://{call.ctx.host}:{call.ctx.port}/",
    )


async def list_projects(call: Call) -> dict[str, Any]:
    base = call.args.base or await call.backend.get_default_project_base()
    rows = await call.backend.scan_for_projects(base)
    projects = [
        {
            "name": r["name"],
            "path": r["path"],
            "made_by_api": r.get("made_by_api", False),
            "status": r.get("live_status"),
            "pipeline_active": r.get("pipeline_active"),
            "tilt_series": r.get("ts_count"),
            "jobs": r.get("total_jobs_planned"),
            "owner": r.get("owner") or r.get("creator"),
            "modified": r.get("modified"),
            "ui_url": call.ctx.ui_url(r["path"]),
        }
        for r in rows
    ]
    return ok(base=base, projects=projects)


def _job_row(path: Path, state, iid: str) -> dict[str, Any]:
    jm = state.jobs[iid]
    row: dict[str, Any] = {
        "instance_id": iid,
        "job_type": jm.job_type.value,
        "name": instance_display_name(iid, jm),
        "status": jm.execution_status.value,
    }
    if jm.species_id:
        row["species"] = jm.species_id
    if jm.slurm_job_id:
        row["slurm_job_id"] = jm.slurm_job_id
    if jm.relion_job_name:
        row["job_dir"] = jm.relion_job_name.rstrip("/")
    status = array_status_cache.get(path, iid)
    if status is not None:
        p = status.progress
        row["tilt_series"] = {
            "ok": p.n_ok,
            "fail": p.n_fail,
            "running": p.n_running,
            "pending": p.n_pending,
            "skip": p.n_skip,
            "total": p.total,
        }
    return row


async def get_project(call: Call) -> dict[str, Any]:
    state = call.state
    await array_status_cache.refresh(call.path, state.jobs)
    roster = state.roster()
    hold = state.review_hold
    acquisition = {}
    for name, (model, attr) in ACQUISITION_FIELDS.items():
        acquisition[name] = _plain(getattr(getattr(state, model), attr))
    return ok(
        name=state.project_name,
        path=str(call.path),
        ui_url=call.url(),
        made_by_api=state.made_by_api,
        pipeline_active=state.pipeline_active,
        review_hold=None
        if hold is None
        else {"tilt_filter": hold.barrier, "parked": hold.parked, "since": hold.held_at.isoformat(timespec="seconds")},
        jobs=[_job_row(call.path, state, iid) for iid in roster],
        off_roster=[iid for iid in state.jobs if iid not in roster],
        species=[{"id": s.id, "name": s.name, "catalog_id": s.catalog_id} for s in state.species_registry],
        acquisition=acquisition,
        dose_per_tilt_source=state.acquisition.dose_per_tilt_source,
        gain_reference=state.acquisition.gain_reference_path,
        tilt_series=state.import_selected_tilt_series,
    )


def _param_row(state, iid: str, jm, name: str) -> dict[str, Any]:
    field = type(jm).model_fields[name]
    value = getattr(jm, name)
    default = field.get_default(call_default_factory=True)
    row: dict[str, Any] = {"name": name, "value": _plain(value), "default": _plain(default)}
    row["edited"] = row["value"] != row["default"]
    if field.title:
        row["title"] = field.title
    if field.description:
        row["help"] = field.description
    bounds = job_params.field_bounds(jm, name)
    if bounds:
        row["bounds"] = bounds
    annotation = field.annotation
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        row["choices"] = [e.value for e in annotation]
    options = job_params.param_options(state, iid, name)
    if options is not None:
        row["choices"] = _plain(options)
    return row


async def get_job(call: Call) -> dict[str, Any]:
    iid = call.args.job
    jm, why = call.job(iid)
    if why:
        return why
    state = call.state
    params = [_param_row(state, iid, jm, name) for name in sorted(jm.USER_PARAMS)]
    if jm.job_type == JobType.DENOISE_PREDICT:
        options, shown = job_params.denoiser_choices(state, iid)
        params.insert(0, {"name": job_params.DENOISER, "value": shown or None, "choices": options})
    inputs, needs = job_params.describe_inputs(state, iid)
    names = [n for n, _ in job_params.SLURM_FIELDS]
    effective = jm.get_effective_slurm_config()
    outputs = [
        {"slot": s.key, "produces": s.produces.value, "path": jm.paths.get(s.key)}
        for s in getattr(type(jm), "OUTPUT_SCHEMA", None) or []
    ]
    return ok(
        instance_id=iid,
        job_type=jm.job_type.value,
        name=instance_display_name(iid, jm),
        status=jm.execution_status.value,
        frozen=job_params.is_frozen(jm),
        on_roster=iid in state.pipeline_order,
        species=jm.species_id,
        job_dir=jm.relion_job_name.rstrip("/") if jm.relion_job_name else None,
        slurm_job_id=jm.slurm_job_id,
        params=params,
        slurm={
            "effective": {n: getattr(effective, n) for n in names},
            "set_by_hand": {k: v for k, v in (jm.slurm_overrides or {}).items() if k in names},
        },
        inputs=inputs,
        outputs=outputs,
        needs=needs,
        ui_url=call.url(View.JOB, iid),
    )


async def get_job_tasks(call: Call) -> dict[str, Any]:
    iid = call.args.job
    jm, why = call.job(iid)
    if why:
        return why
    if not jm.relion_job_name:
        return err(f"{iid} has not run: it has no tasks yet.")
    job_dir = call.path / jm.relion_job_name.rstrip("/")

    def _read() -> tuple[list[str], dict[str, str], dict[str, str]]:
        items = manifest_items(job_dir)
        statuses = scan_statuses(job_dir, items)
        reasons = {}
        for name, st in statuses.items():
            fail = job_dir / STATUS_DIR_NAME / f"{name}.fail"
            if st == "fail" and fail.is_file():
                reasons[name] = fail.read_text(errors="replace").strip()[:500]
        return items, statuses, reasons

    items, statuses, reasons = await asyncio.to_thread(_read)
    if not items:
        return err(f"{iid} runs no per-tilt-series tasks (no task manifest in {job_dir.name}).")
    rows = [
        {"ts": name, "state": statuses[name], **({"reason": reasons[name]} if name in reasons else {})}
        for name in items
        if call.args.state is None or statuses[name] == call.args.state
    ]
    page = rows[call.args.offset : call.args.offset + call.args.limit]
    return ok(
        total=len(items),
        counts=dict(Counter(statuses.values())),
        matching=len(rows),
        tasks=page,
        offset=call.args.offset,
        ui_url=call.url(View.JOB, iid),
    )


async def get_job_logs(call: Call) -> dict[str, Any]:
    res = await call.backend.get_instance_logs(call.path, call.args.job, ts=call.args.ts, lines=call.args.lines)
    if res["success"]:
        res["ui_url"] = call.url(View.JOB, call.args.job)
    return res


async def list_job_types(call: Call) -> dict[str, Any]:
    types = []
    for spec in JOB_SPECS:
        if spec.phase is None:
            continue
        cls = spec.param_class
        types.append(
            {
                "job_type": spec.job_type.value,
                "name": spec.display_name,
                "phase": spec.phase,
                "requires": [d.value for d in spec.dependencies],
                "added_with": spec.prerequisite.value if spec.prerequisite else None,
                "species": spec.phase == PHASE_PARTICLES,
                "inputs": [
                    {"slot": s.key, "accepts": [t.value for t in s.accepts], "required": s.required}
                    for s in getattr(cls, "INPUT_SCHEMA", None) or []
                ],
                "outputs": [
                    {"slot": s.key, "produces": s.produces.value} for s in getattr(cls, "OUTPUT_SCHEMA", None) or []
                ],
            }
        )
    return ok(job_types=types)


async def get_job_type(call: Call) -> dict[str, Any]:
    try:
        job_type = JobType(call.args.job_type)
    except ValueError:
        return err(f"No job type {call.args.job_type!r} (list_job_types lists them).")
    cls = PARAM_CLASS_BY_TYPE.get(job_type)
    if cls is None:
        return err(f"{job_type.value} is not a pipeline job.")
    sample = cls()
    params = []
    for name in sorted(cls.USER_PARAMS):
        field = cls.model_fields[name]
        row: dict[str, Any] = {
            "name": name,
            "type": getattr(field.annotation, "__name__", str(field.annotation)),
            "default": _plain(field.get_default(call_default_factory=True)),
        }
        if field.title:
            row["title"] = field.title
        if field.description:
            row["help"] = field.description
        bounds = job_params.field_bounds(sample, name)
        if bounds:
            row["bounds"] = bounds
        if isinstance(field.annotation, type) and issubclass(field.annotation, Enum):
            row["choices"] = [e.value for e in field.annotation]
        params.append(row)
    note = "Choices that come from a project (templates, masks, features, the Denoiser) are listed by get_job."
    return ok(job_type=job_type.value, name=display_name(job_type), params=params, note=note)


async def get_input_candidates(call: Call) -> dict[str, Any]:
    iid = call.args.job
    jm, why = call.job(iid)
    if why:
        return why
    resolver = PathResolutionService(call.state)
    schema = {s.key: s for s in resolver.get_input_schema_for_job(jm.job_type, jm)}
    slot = schema.get(call.args.slot)
    if slot is None:
        return err(f"{iid} has no input {call.args.slot} ({', '.join(schema) or 'it reads none'}).")
    rows = []
    for c in resolver.get_candidates_for_slot(
        jm.job_type, slot.key, consumer_species_id=jm.species_id, consumer_instance_id=iid
    ):
        row: dict[str, Any] = {
            "source": c.source_key,
            "producer": c.producer_instance_id,
            "produces": c.produces.value,
            "status": c.execution_status.value,
        }
        if c.species_id:
            row["species"] = c.species_id
        if c.label:
            row["label"] = c.label
        if why_not := unreadable_reason(jm.job_type, slot, c):
            row["unreadable"] = why_not
        if skipped := auto_exclusion(jm.job_type, jm.species_id, c):
            row["automatic_skips_it"] = skipped
        rows.append(row)
    current = next((r for r in job_params.describe_inputs(call.state, iid)[0] if r["slot"] == slot.key), None)
    return ok(slot=slot.key, current=current, candidates=rows, ui_url=call.url(View.JOB, iid))


def _species_row(sp) -> dict[str, Any]:
    return {
        "id": sp.id,
        "name": sp.name,
        "catalog_id": sp.catalog_id,
        "diameter_ang": sp.diameter_ang,
        "symmetry": sp.symmetry,
        "templates": len(sp.templates),
        "masks": len(sp.masks),
    }


async def list_species(call: Call) -> dict[str, Any]:
    from services.particles import catalog

    entries = []
    if catalog.is_enabled():
        entries = [e.model_dump(mode="json") for e in await asyncio.to_thread(catalog.list_catalog)]
    return ok(
        species=[_species_row(sp) for sp in call.state.species_registry],
        catalog=entries,
        catalog_enabled=catalog.is_enabled(),
        ui_url=call.url(View.SPECIES),
    )


async def get_species(call: Call) -> dict[str, Any]:
    sp = call.state.get_species(call.args.species)
    if sp is None:
        listed = ", ".join(s.id for s in call.state.species_registry) or "none"
        return err(f"No species {call.args.species} in this project ({listed}).")
    row = _species_row(sp)
    row.update(
        templates=[
            {"id": t.id, "path": t.template_path, "polarity": t.polarity, "lowpass_ang": t.lowpass_resolution_ang}
            for t in sp.templates
        ],
        masks=[{"id": m.id, "path": m.mask_path, "method": m.method} for m in sp.masks],
        selected_template=sp.selected_template_id or None,
        selected_mask=sp.selected_mask_id or None,
        extraction=sp.extraction_params.model_dump() if sp.extraction_params else None,
        catalog_version=sp.catalog_version,
    )
    return ok(**row, ui_url=call.url(View.SPECIES, sp.id))


async def list_kitchen_subsets(call: Call) -> dict[str, Any]:
    from services.sta_kitchen import subset_sources

    res = await subset_sources.list_subsets(call.path, call.args.species)
    res["ui_url"] = call.url(View.SPECIES, call.args.species)
    return res


async def get_kitchen_subset_sources(call: Call) -> dict[str, Any]:
    from services.sta_kitchen import subset_sources

    res = await subset_sources.subset_sources(call.path, call.args.species, call.args.subset)
    res["ui_url"] = call.url(View.SPECIES, call.args.species)
    return res


async def plan_run(call: Call) -> dict[str, Any]:
    res = await call.backend.pipeline_orchestrator.plan_run(call.path)
    res["ui_url"] = call.url()
    return res


async def get_task(call: Call) -> dict[str, Any]:
    record = get_background_task_registry().get(call.args.task_id)
    result = call.ctx.results.get(call.args.task_id)
    if record is None and result is None:
        return err(f"No task {call.args.task_id} (finished tasks are kept for a few minutes).")
    out: dict[str, Any] = {"task_id": call.args.task_id}
    if record is not None:
        out.update(
            title=record.title,
            status=record.status,
            progress=record.progress_message or None,
            percent=record.progress_pct,
            seconds=round(record.duration_sec),
        )
    if result is not None:
        out["result"] = result
    return ok(**out)


async def inspect_dataset(call: Call) -> dict[str, Any]:
    a = call.args
    res = await dataset_scan.scan(
        a.directory, frames_glob=a.frames_glob, mdocs_glob=a.mdocs_glob, mdocs_directory=a.mdocs_directory
    )
    if not res["success"]:
        return res
    scan = res["scan"].model_dump(mode="json")
    series = scan.pop("series")
    page = series[a.offset : a.offset + a.limit]
    folders = Counter(s["folder"] for s in series)
    return ok(
        **scan,
        n_series=len(series),
        folders=dict(folders),
        series=[{k: s[k] for k in ("name", "folder", "tilts", "source_kind")} for s in page],
        offset=a.offset,
    )


# ── Creation ──────────────────────────────────────────────────────────────────


async def _tomogram_sources(sources) -> tuple[list[dict] | None, dict[str, Any] | None]:
    """Tomogram sources as `commit_imported_tomograms` takes them, each project found by path or name. A source
    is only read: it is never opened as a project."""
    out = []
    for source in sources:
        path = Path(source.project).expanduser()
        if not path.is_absolute():
            found = await asyncio.to_thread(resolve_project, source.project)
            if not found["success"]:
                return None, found
            path = Path(found["path"])
        out.append({"project": str(path), "job": source.job, "names": list(source.names or [])})
    return out, None


async def _create_from_tomograms(call: Call) -> dict[str, Any]:
    """A project without raw data, from other projects' tomogram jobs. Their acquisition values come with them,
    so the call takes none; the sources are checked first (`check_job_sources`: binds, tilt-series stars, one
    set of values), then the project is made and the tomograms imported in the background."""
    a = call.args
    given = [n for n in ACQUISITION_FIELDS if getattr(a, n) is not None]
    given += [n for n in ("directory", "frames_glob", "mdocs_glob", "protocol", "gain_reference") if getattr(a, n)]
    if given:
        return err(f"A project from tomograms takes no {', '.join(given)}: its values come with the tomograms.")
    if not a.tomograms_from:
        return err("tomograms_from: name at least one tomogram job.")
    sources, why = await _tomogram_sources(a.tomograms_from)
    if why:
        return why
    checked = await asyncio.to_thread(
        tomogram_import.check_job_sources, sources, get_config_service().config.container_binds
    )
    if not checked["success"]:
        return checked
    v = checked["values"]
    spec = ProjectSpec(
        name=a.name,
        base=a.base,
        shared=a.shared,
        gain_reference=None,
        acceleration_voltage_kv=v["voltage"],
        spherical_aberration_mm=v["spherical_aberration"],
        amplitude_contrast=v["amplitude_contrast"],
        invert_defocus_hand=v["hand"] < 0,
        pixel_size_angstrom=v["tilt_series_pixel_size"],
        tomograms_from=sources,
    )
    problems = spec_problems(spec)
    if problems:
        return err("Not created: " + "; ".join(problems), problems=problems)
    res = await _submit_create(call, spec)
    if res["success"]:
        res.update(values=v, tomograms=checked["count"])
    return res


async def _submit_create(call: Call, spec: ProjectSpec, *, overview=None, protocol=None, warnings=()) -> dict:
    """Make the project `spec` describes in the background: `ok(task_id, project_path)`, refused when its folder
    exists. The task records the write in the new project when it is made."""
    target = Path(spec.base).expanduser() / spec.name.strip()
    if await asyncio.to_thread(target.exists):
        return err(f"{target} exists already: the API makes new projects only.")
    ctx = call.ctx

    async def work(progress_cb) -> str:
        if protocol is not None:
            from services.protocols.apply import apply_protocol

            given = {name: getattr(spec, name) for name in ACQUISITION_FIELDS}
            given["dose_per_tilt_source"] = spec.dose_per_tilt_source
            res = await apply_protocol(
                ctx.backend,
                protocol,
                project_name=spec.name,
                project_base_path=spec.base,
                movies_glob=spec.frames_glob,
                mdocs_glob=spec.mdocs_glob,
                gain_reference_path=spec.gain_reference,
                shared=spec.shared,
                values=given,
                made_by_api=True,
            )
        else:
            res = await ctx.backend.project_service.create_project(
                spec, overview=overview, made_by_api=True, progress_cb=lambda text: progress_cb(0, 0, text)
            )
        if res.get("project_path"):
            res["ui_url"] = ctx.ui_url(res["project_path"])
            await record_write(Path(res["project_path"]), "create_project", call.args.model_dump(), res)
        ctx.results[task_id] = res
        if not res["success"]:
            raise RuntimeError(res["error"])
        return res.get("message", "")

    task_id = get_background_task_registry().submit(
        work, title=f"Creating {spec.name}", project_path=str(target), dedup_key=f"create:{target}"
    )
    return ok(
        task_id=task_id,
        project_path=str(target),
        ui_url=ctx.ui_url(target),
        warnings=list(warnings),
        message="Creating the project in the background: follow it with get_task.",
    )


async def create_project(call: Call) -> dict[str, Any]:
    """Scan, check the spec, then make the project in the background: the import and the registry take time.
    Returns the task to follow (`get_task`) and where the project will be."""
    a = call.args
    if a.tomograms_from is not None:
        return await _create_from_tomograms(call)
    protocol = None
    if a.protocol:
        from services.protocols.discovery import load_named_protocol

        if a.series is not None or a.ts_names_by_folder:
            return err("A project from a protocol imports every series the mdocs glob matches, as the dialog does.")
        try:
            protocol = await asyncio.to_thread(load_named_protocol, a.protocol)
        except (OSError, ValueError) as e:
            return err(f"Protocol {a.protocol!r}: {e}")
    overview = None
    frames_glob, mdocs_glob, series = a.frames_glob or "", a.mdocs_glob or "", None
    values = {}
    if a.directory or a.frames_glob or a.mdocs_glob:
        res = await dataset_scan.scan(
            a.directory, frames_glob=a.frames_glob, mdocs_glob=a.mdocs_glob, mdocs_directory=a.mdocs_directory
        )
        if not res["success"]:
            return res
        scanned, overview = res["scan"], res["overview"]
        frames_glob, mdocs_glob, values = scanned.frames_glob, scanned.mdocs_glob, scanned.values
        by_name = {s.name: s.mdoc for s in scanned.series}
        all_mdocs = set(by_name.values())
        if a.series is not None:
            unknown = [s for s in a.series if s not in by_name and s not in all_mdocs]
            if unknown:
                return err(f"Not in the dataset: {', '.join(unknown[:10])} (inspect_dataset lists its series).")
            series = sorted({by_name.get(s, s) for s in a.series})
            chosen = set(series)
            for p in overview.positions:
                for ts in p.tilt_series:
                    ts.selected = str(ts.mdoc_path) in chosen
        # One layer for every series, as the landing page's Source row sets it. Both layers whole is a decision:
        # the frames and SerialEM's stack differ in orientation.
        n = len(overview.get_selected_tilt_series())
        both = n > 0 and all(v == n for v in overview.layer_coverage().values())
        if a.source_layer is None and both:
            return err(
                "The selected series have both SerialEM's stacks and their movies: choose the layer",
                code=ErrorCode.NEEDS_DECISION,
                needs=[
                    need(
                        "choice",
                        "source_layer",
                        "the stacks are SerialEM's frame-aligned images; the movies are the raw frames, whose "
                        "orientation, tilt axis, hand and gain reference differ from the stack's",
                        options=["stack", "movies"],
                    )
                ],
            )
        overview.apply_layer(a.source_layer or overview.default_layer())
        problems = dataset_scan.layer_problems(overview)
        if problems:
            return err("Not created: " + "; ".join(problems), problems=problems)
        values = dataset_scan.data_values(overview)
    elif a.source_layer is not None:
        return err("source_layer: a project without raw data imports no series.")
    spec = ProjectSpec(
        name=a.name,
        base=a.base,
        frames_glob=frames_glob,
        mdocs_glob=mdocs_glob,
        series=series,
        ts_names_by_folder=a.ts_names_by_folder,
        source_layer=overview.layer if overview is not None else None,
        shared=a.shared,
        gain_reference=a.gain_reference,
        dose_per_tilt_source=a.dose_per_tilt_source if a.dose_per_tilt is not None else "",
        acquisition_software=overview.acquisition_software if overview is not None else "",
        **{name: getattr(a, name) for name in ACQUISITION_FIELDS},
    )
    problems = spec_problems(spec)
    missing = [n for n in required_values(spec) if getattr(spec, n) is None]
    if missing:
        needs = []
        for name in missing:
            said = values.get(name)
            if said is not None and said.value is not None:
                why = f"the {said.found_in.value} says {said.value}: confirm it, or give the right value"
                if said.hint:
                    why += f" ({said.hint})"
            elif said is not None and said.mixed:
                why = f"the selected series disagree: {', '.join(map(str, said.mixed))}"
            else:
                why = "no mdoc records it" + (f" ({said.hint})" if said is not None and said.hint else "")
            needs.append(need("value", name, why))
        return err(
            "Every acquisition value comes from the user: " + ", ".join(missing),
            code=ErrorCode.NEEDS_DECISION,
            needs=needs,
            problems=problems,
        )
    if problems:
        return err("Not created: " + "; ".join(problems), problems=problems)
    if protocol is not None and not spec.has_raw_data:
        return err("A protocol makes a project from raw data: give the data folder or the globs.")
    warnings = dataset_scan.differences(spec, values) if values else []
    return await _submit_create(call, spec, overview=overview, protocol=protocol, warnings=warnings)


# ── Composition and setting ───────────────────────────────────────────────────


async def add_job(call: Call) -> dict[str, Any]:
    a = call.args
    try:
        job_type = JobType(a.job_type)
    except ValueError:
        return err(f"No job type {a.job_type!r} (list_job_types lists them).")
    res = pipeline_edit.add_job(
        call.state, job_type, species_id=a.species, after=a.after, reading=a.reading, clone_of=a.clone_of
    )
    if res["success"]:
        res["ui_url"] = call.url(View.JOB, res["instance_id"])
    return res


async def remove_job(call: Call) -> dict[str, Any]:
    res = pipeline_edit.remove_job(call.state, call.args.job)
    if res["success"]:
        res["ui_url"] = call.url()
    return res


async def exclude_tilt_series(call: Call) -> dict[str, Any]:
    res = await pipeline_edit.exclude_tilt_series(
        call.state, call.args.ts, call.args.excluded, reason="excluded through the agent API"
    )
    if res["success"]:
        res["ui_url"] = call.url(View.JOURNEY, call.args.ts)
    return res


def _job_answer(call: Call, res: dict[str, Any]) -> dict[str, Any]:
    if res["success"] or res.get("code") == ErrorCode.JOB_FROZEN:
        res["ui_url"] = call.url(View.JOB, call.args.job)
    return res


async def set_params(call: Call) -> dict[str, Any]:
    return _job_answer(call, job_params.set_params(call.state, call.args.job, call.args.values))


async def set_tilt_filter(call: Call) -> dict[str, Any]:
    a = call.args
    res = await job_params.set_tilt_filter(
        call.backend, call.path, a.job, mode=a.mode, model=a.model, threshold=a.threshold
    )
    return _job_answer(call, res)


async def set_slurm(call: Call) -> dict[str, Any]:
    return _job_answer(call, job_params.set_slurm(call.state, call.args.job, call.args.values, reset=call.args.reset))


async def set_input(call: Call) -> dict[str, Any]:
    a = call.args
    res = job_params.set_input(call.state, a.job, a.slot, source=a.source, path=a.path, auto=a.auto, facet=a.facet)
    return _job_answer(call, res)


async def reset_inputs(call: Call) -> dict[str, Any]:
    return _job_answer(call, job_params.reset_inputs(call.state, call.args.job))


async def set_gain(call: Call) -> dict[str, Any]:
    res = await job_params.set_gain(call.state, call.args.path)
    if res["success"]:
        res["ui_url"] = call.url()
    return res


async def predict_tilts(call: Call) -> dict[str, Any]:
    return _job_answer(call, await call.backend.submit_tilt_filter_predict(call.path, call.args.job))


async def cancel_predict_tilts(call: Call) -> dict[str, Any]:
    return _job_answer(call, await call.backend.cancel_tilt_filter_predict(call.path, call.args.job))


async def cancel_job(call: Call) -> dict[str, Any]:
    return _job_answer(call, await call.backend.cancel_job(call.path, call.args.job))


# ── Species and templates ─────────────────────────────────────────────────────


async def import_species(call: Call) -> dict[str, Any]:
    from services.particles import catalog

    if not catalog.is_enabled():
        return err("No lab species catalog is configured (species_catalog_root in conf.yaml).")
    try:
        sp = await asyncio.to_thread(
            catalog.import_species, call.state, call.path, call.args.catalog_id, call.args.version
        )
    except (OSError, ValueError) as e:
        return err(f"Import of {call.args.catalog_id} failed: {e}")
    return ok(species=_species_row(sp), ui_url=call.url(View.SPECIES, sp.id))


def _species_or_err(call: Call):
    sp = call.state.get_species(call.args.species)
    if sp is None:
        return None, err(f"No species {call.args.species} in this project.")
    return sp, None


async def add_template(call: Call) -> dict[str, Any]:
    a = call.args
    sp, why = _species_or_err(call)
    if why:
        return why
    if not await asyncio.to_thread(Path(a.path).is_file):
        return err(f"No file at {a.path}.")
    dst = await asyncio.to_thread(species_admin.ingest_template_file, call.path, sp.id, a.path)
    res = species_admin.register_template(
        call.state, sp.id, str(dst), polarity=a.polarity, source=a.source, lowpass=a.lowpass_ang, imported_from=a.path
    )
    if res["success"]:
        res.update(path=str(dst), ui_url=call.url(View.SPECIES, sp.id))
    return res


async def add_mask(call: Call) -> dict[str, Any]:
    a = call.args
    sp, why = _species_or_err(call)
    if why:
        return why
    if not await asyncio.to_thread(Path(a.path).is_file):
        return err(f"No file at {a.path}.")
    dst = await asyncio.to_thread(species_admin.ingest_template_file, call.path, sp.id, a.path)
    res = species_admin.register_mask(call.state, sp.id, TemplateMask(mask_path=str(dst), method=a.method))
    if res["success"]:
        res.update(path=str(dst), ui_url=call.url(View.SPECIES, sp.id))
    return res


def _in_background(call: Call, verb: str, title: str, key: str, work) -> dict[str, Any]:
    """Run a write whose work takes minutes as a background task: `ok(task_id)` now, its outcome from `get_task`.
    The task saves the project and records the write when it ends."""
    ctx, path, args = call.ctx, call.path, call.args.model_dump()
    url = call.url(View.SPECIES, call.args.species)

    async def run(progress_cb) -> str:
        res = await work(progress_cb)
        if res["success"]:
            await ctx.backend.save_project(path)
        res["ui_url"] = url
        await record_write(path, verb, args, res)
        ctx.results[task_id] = res
        if not res["success"]:
            raise RuntimeError(res["error"])
        return res.get("message", "")

    task_id = get_background_task_registry().submit(run, title=title, project_path=str(path), dedup_key=key)
    return ok(task_id=task_id, message=f"{title}: follow it with get_task.", ui_url=url)


async def make_template(call: Call) -> dict[str, Any]:
    a = call.args
    planned = await species_edit.plan_template(
        call.state,
        a.species,
        a.kind,
        shape=a.shape,
        pdb_id=a.pdb_id,
        emdb_id=a.emdb_id,
        template_id=a.template_id,
        pixel_size=a.pixel_size,
        box_size=a.box_size,
        lowpass=a.lowpass,
    )
    if not planned["success"]:
        return planned
    plan = planned["plan"]
    res = _in_background(
        call,
        "make_template",
        f"Making a {a.kind} template for {a.species}",
        plan.key,
        lambda progress_cb: species_edit.make_template(
            call.backend, call.state, plan, lambda text: progress_cb(0, 0, text)
        ),
    )
    res["values"] = plan.values()
    return res


async def make_mask(call: Call) -> dict[str, Any]:
    a = call.args
    planned = await species_edit.plan_mask(
        call.backend,
        call.state,
        a.species,
        a.kind,
        template_id=a.template_id,
        threshold=a.threshold,
        threshold_method=a.threshold_method,
        extend=a.extend,
        soft_edge=a.soft_edge,
        lowpass=a.lowpass,
        diameter_ang=a.diameter_ang,
    )
    if not planned["success"]:
        return planned
    plan = planned["plan"]
    res = _in_background(
        call,
        "make_mask",
        f"Making a {a.kind} mask for {a.species}",
        plan.key,
        lambda _progress_cb: species_edit.make_mask(call.backend, call.state, plan),
    )
    res["values"] = plan.values()
    return res


async def select_template(call: Call) -> dict[str, Any]:
    res = species_admin.select_template(call.state, call.args.species, call.args.template_id)
    if res["success"]:
        res["ui_url"] = call.url(View.SPECIES, call.args.species)
    return res


async def select_mask(call: Call) -> dict[str, Any]:
    res = species_admin.select_mask(call.state, call.args.species, call.args.mask_id)
    if res["success"]:
        res["ui_url"] = call.url(View.SPECIES, call.args.species)
    return res


async def create_species(call: Call) -> dict[str, Any]:
    a = call.args
    res = species_edit.create_species(
        call.state,
        a.name,
        origin=a.origin,
        color=a.color,
        diameter_ang=a.diameter_ang,
        symmetry=a.symmetry,
        notes=a.notes,
        mask_path=a.mask_path,
    )
    if res["success"]:
        res["ui_url"] = call.url(View.SPECIES, res["species_id"])
    return res


async def set_species(call: Call) -> dict[str, Any]:
    a = call.args
    given = {name: getattr(a, name) for name in ("name", "color", "diameter_ang", "symmetry", "notes")}
    fields = {name: value for name, value in given.items() if name in a.model_fields_set}
    if "extraction" in a.model_fields_set and a.extraction is not None:
        fields["extraction"] = (a.extraction.box_size, a.extraction.binning, a.extraction.crop_size)
    res = species_edit.set_species(call.state, a.species, **fields)
    if res["success"]:
        res["ui_url"] = call.url(View.SPECIES, a.species)
    return res


async def import_tomograms(call: Call) -> dict[str, Any]:
    a = call.args
    job_sources = None
    if a.mode == "jobs":
        if not a.sources:
            return err("The jobs mode takes sources: each a project, its tomogram job, and the names.")
        job_sources, why = await _tomogram_sources(a.sources)
        if why:
            return why
    try:
        res = await call.backend.commit_imported_tomograms(
            call.path,
            mode=a.mode,
            mrc_paths=a.mrc_paths,
            reference_star=a.reference_star,
            job_sources=job_sources,
            pixel_size_angstrom=a.pixel_size_angstrom,
            tomogram_binning=a.tomogram_binning,
            replace=a.replace,
        )
    except (OSError, ValueError, KeyError) as e:
        return err(f"Not imported: {e}")
    if res["success"]:
        res["ui_url"] = call.url(View.TOMOGRAMS)
    return res


# ── Run controls ──────────────────────────────────────────────────────────────


async def run(call: Call) -> dict[str, Any]:
    """Run the roster, as the Run button does: what `plan_run` shows."""
    await call.backend.save_project(call.path, force=True)
    res = await call.backend.start_pipeline(
        project_path=str(call.path),
        scheme_name=f"run_{datetime.now().strftime('%H%M%S')}",
        selected_jobs=call.state.roster(),
        required_paths=[],
    )
    res["ui_url"] = call.url()
    return res


async def stop(call: Call) -> dict[str, Any]:
    res = await call.backend.stop_pipeline(call.path)
    res["pipeline_active"] = call.state.pipeline_active
    res["ui_url"] = call.url()
    return res


async def cancel_leftovers(call: Call) -> dict[str, Any]:
    """Cancel SLURM work a refused Run named as still live in a failed job's directory, and only that."""
    from services.scheduling_and_orchestration.pipeline_orchestrator_service import _run_set

    orchestrator = call.backend.pipeline_orchestrator
    to_run, _ = _run_set(call.state, call.state.roster())
    found = await orchestrator._live_in_reused_dirs(call.path, call.state, to_run)
    named = set(found[1]) if found else set()
    asked = {str(i) for i in call.args.slurm_ids}
    stray = sorted(asked - named)
    if stray:
        return err(
            f"Not cancelled: {', '.join(stray)} {'is' if len(stray) == 1 else 'are'} not work a Run would refuse "
            f"over (still live now: {', '.join(sorted(named)) or 'none'})."
        )
    res = await call.backend.slurm_service.scancel_jobs(sorted(asked))
    res["ui_url"] = call.url()
    return res
