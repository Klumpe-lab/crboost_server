"""Apply = Protocol -> a new project.

Creates the project through the same facade the landing page uses, registers the protocol's
species with their frozen assets, instantiates every stage with its full param snapshot and
persists once. Validation runs before anything touches disk; unknown fields and rejected values
come back as warnings in the result, never as silent substitutions.
Both the Protocols dialog and the `crboost_protocol.py` CLI call this.
"""

from __future__ import annotations

import asyncio
import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from services import species_admin
from services.configs.config_service import get_config_service
from services.jobs.spec import JOB_SPEC_BY_TYPE
from services.models_base import JobType, SpeciesOrigin
from services.pipeline_edit import species_defaults
from services.project_state import ExtractionParams, ProjectState, ProtocolOrigin, TemplateMask
from services.project_create import ProjectSpec, model_defaults
from services.protocols.schema import (
    PROJECT_PROTOCOL_DIRNAME,
    PROTOCOL_FILENAME,
    Protocol,
    ProtocolSpecies,
    ProtocolStage,
    protocol_to_yaml,
)
from services.result import err, ok
from services.templating.template_metadata import read_template_header

logger = logging.getLogger(__name__)

# Tools that are crboost itself (the tilt filter's DL pass runs in our venv) — nothing to
# configure under conf.yaml `tools:`, so the availability check skips them.
_IN_HOUSE_TOOLS = frozenset({"crboost"})


def validate_protocol(protocol: Protocol) -> list[str]:
    """Blocking problems (or []) — everything checkable before a single disk write:
    stage types, duplicate instances, declared species, tools configured in conf.yaml,
    prerequisites/dependencies present as stages (protocols are explicit; nothing is
    auto-added), assets on disk, template apix == the pinned reconstruction pixel size."""
    problems: list[str] = []
    if not protocol.stages:
        problems.append("protocol has no stages")
    cs = get_config_service()
    stage_jobs = {s.job for s in protocol.stages}
    seen: set[str] = set()
    for st in protocol.stages:
        try:
            jt = st.job_type
        except ValueError as e:
            problems.append(str(e))
            continue
        spec = JOB_SPEC_BY_TYPE.get(jt)
        if spec is None:
            problems.append(f"{st.job}: not a pipeline job type")
            continue
        iid = st.instance_id
        if iid in seen:
            problems.append(f"duplicate stage instance '{iid}'")
        seen.add(iid)
        if st.species and protocol.get_species(st.species) is None:
            problems.append(f"{iid}: species '{st.species}' is not declared in the protocol")
        try:
            tool = spec.param_class().get_tool_name()
        except NotImplementedError:
            tool = None
        if tool and tool not in _IN_HOUSE_TOOLS and not cs.is_tool_configured(tool):
            problems.append(f"{iid}: tool '{tool}' is not configured in conf.yaml (tools:)")
        if spec.prerequisite is not None and spec.prerequisite.value not in stage_jobs:
            problems.append(f"{iid}: prerequisite '{spec.prerequisite.value}' is not a stage (nothing is auto-added)")
        for dep in spec.dependencies:
            if dep.value not in stage_jobs:
                problems.append(f"{iid}: depends on '{dep.value}', which is not a stage")
    stage_ids = {s.instance_id for s in protocol.stages if s.job in {jt.value for jt in JobType}}
    for st in protocol.stages:
        for slot, producer in st.inputs.items():
            if producer not in stage_ids:
                problems.append(f"{st.instance_id}.{slot}: input wired to unknown stage '{producer}'")

    for ps in protocol.species:
        for label, entry in (("template", ps.template), ("mask", ps.mask)):
            if entry is None:
                continue
            try:
                p = protocol.asset_path(entry.asset)
            except ValueError as e:
                problems.append(str(e))
                continue
            if not p.exists():
                problems.append(f"species '{ps.id}': {label} asset missing: {p}")

    # A template is apix-bound: it must match the reconstruction pixel size the protocol pins.
    recon = next((s for s in protocol.stages if s.job == JobType.TS_RECONSTRUCT.value), None)
    if recon is not None and "rescale_angpixs" in recon.params and protocol.bundle_dir is not None:
        want = float(recon.params["rescale_angpixs"])
        for ps in protocol.species:
            if ps.template is None or not protocol.asset_path(ps.template.asset).exists():
                continue
            hdr = read_template_header(str(protocol.asset_path(ps.template.asset)))
            if hdr.apix_ang is None:
                problems.append(
                    f"species '{ps.id}': template MRC header has no voxel size; cannot confirm it matches "
                    f"tsReconstruct.rescale_angpixs={want}"
                )
            elif abs(hdr.apix_ang - want) > 0.01:
                problems.append(
                    f"species '{ps.id}': template apix {hdr.apix_ang:.3f} Å ≠ tsReconstruct.rescale_angpixs {want} Å"
                )
    return problems


async def apply_protocol(
    backend,
    protocol: Protocol,
    *,
    project_name: str,
    project_base_path: str | Path,
    movies_glob: str,
    mdocs_glob: str,
    gain_reference_path: str | None = None,
    shared: bool = False,
    dose_per_tilt: float | None = None,
    dose_per_tilt_source: str = "",
    values: dict[str, Any] | None = None,
    made_by_api: bool = False,
) -> dict[str, Any]:
    """Create `<project_base_path>/<project_name>` from `protocol`. `ok(project_path=,
    stages=[instance ids], warnings=[...], load_warnings=[...])` or `err(...)` (validation
    problems in `problems`, project-creation errors verbatim).

    `dose_per_tilt` (+ its source, "user" when typed) overrides the mdocs' value. When the
    mdocs carry no dose and none is passed, project creation uses the scan's estimate and
    records it as "estimated"; it refuses only when no estimate is possible either, so a
    protocol never runs on a silent default dose.

    `values`, every acquisition value (`project_create.ACQUISITION_FIELDS`) with the dose's source, is what
    the agent API passes: they are taken as given, and nothing comes from the mdocs or the model defaults.
    `made_by_api` marks the project as the API's."""
    problems = validate_protocol(protocol)
    if problems:
        return err("Protocol cannot be applied:\n  - " + "\n  - ".join(problems), problems=problems)

    # Layer 1 — dataset facts from the mdocs, the same autodetect the landing page runs; what they do not
    # give, and the values no mdoc carries, take the model defaults, as on the landing page. Given `values`
    # (the agent API) are taken instead, all of them.
    from services.configs.mdoc_service import get_mdoc_service
    from services.dataset_scan import first_mdoc_values

    facts = await asyncio.to_thread(get_mdoc_service().get_autodetect_params, mdocs_glob) or {}
    given = values
    values = dict(given) if given is not None else model_defaults()
    for src, dst in (
        ("pixel_spacing", "pixel_size_angstrom"),
        ("voltage", "acceleration_voltage_kv"),
        ("tilt_axis_angle", "tilt_axis_degrees"),
        ("detector_dimensions", "detector_dimensions"),
    ):
        if facts.get(src) is not None and given is None:
            values[dst] = facts[src]
    if given is not None:
        dose_source = values.pop("dose_per_tilt_source", "user")
    elif dose_per_tilt is not None:
        values["dose_per_tilt"], dose_source = dose_per_tilt, dose_per_tilt_source or "user"
    else:
        first = await asyncio.to_thread(first_mdoc_values, mdocs_glob)
        if "dose_per_tilt" not in first:
            return err("dose per tilt: not in the mdocs and no estimate possible — enter it")
        values["dose_per_tilt"], dose_source = first["dose_per_tilt"], first["dose_per_tilt_source"]
        if dose_source == "estimated":
            logger.warning(
                "%s: dose per tilt not in the mdocs — using the estimate %.2f e/Å²",
                project_name,
                values["dose_per_tilt"],
            )
    if given is None and not str(movies_glob).lower().endswith(".eer"):
        values["eer_fractions_per_frame"] = None
    mdoc_files = sorted(glob.glob(mdocs_glob))
    # frame_extension is NOT guessed from the movies glob: the import records what it
    # actually linked or split (`.mrc` for SerialEM stacks).
    import_summary = {
        "total_positions": len(mdoc_files),
        "selected_positions": len(mdoc_files),
        "total_tilt_series": len(mdoc_files),
        "selected_tilt_series": len(mdoc_files),
        "source_directory": str(Path(movies_glob).parent),
    }
    spec = ProjectSpec(
        name=project_name,
        base=str(project_base_path),
        frames_glob=movies_glob,
        mdocs_glob=mdocs_glob,
        shared=shared,
        gain_reference=str(gain_reference_path) if gain_reference_path else None,
        dose_per_tilt_source=dose_source,
        acquisition_software=facts.get("acquisition_software") or "",
        **values,
    )
    created = await backend.create_project(spec, import_summary=import_summary, made_by_api=made_by_api)
    if not created.get("success"):
        return created
    project_dir = Path(created["project_path"])
    state: ProjectState = backend.state_service.state_for(project_dir)

    warnings = register_protocol_species(state, protocol, project_dir)
    warnings += instantiate_stages(state, protocol)
    state.pipeline_order = protocol.stage_ids()
    state.protocol_origin = ProtocolOrigin(
        name=protocol.name,
        version=protocol.version,
        bundle_dir=str(protocol.bundle_dir or ""),
        applied_at=datetime.now().isoformat(timespec="seconds"),
    )
    # The frozen copy the Protocols view compares against later, whatever happens to the bundle.
    # Written by hand: dump_protocol() would rebind the live object's bundle dir to the project.
    frozen_dir = project_dir / PROJECT_PROTOCOL_DIRNAME
    frozen_dir.mkdir(parents=True, exist_ok=True)
    (frozen_dir / PROTOCOL_FILENAME).write_text(protocol_to_yaml(protocol))
    state.mark_dirty()
    await backend.save_project(project_dir, force=True)
    for line in warnings:
        logger.warning("apply %s: %s", protocol.name, line)
    return ok(
        project_path=str(project_dir),
        stages=protocol.stage_ids(),
        warnings=warnings,
        load_warnings=list(state.load_warnings),
    )


def register_protocol_species(state: ProjectState, protocol: Protocol, project_dir: Path) -> list[str]:
    """Register every protocol species; copy its assets into `templates/<id>/`, registered and selected, so
    the stages take them as a job added from the roster would (`pipeline_edit.species_defaults`). Returns
    the warnings."""
    warnings: list[str] = []
    for ps in protocol.species:
        sp = state.add_species(ps.name, origin=SpeciesOrigin.WORKBENCH.value)
        if sp.id != ps.id:
            # The slug of the display name is not the protocol's id: the instance ids of the
            # stages are `{job}__{ps.id}`, so the species must carry exactly that id.
            if state.get_species(ps.id) is not None:
                warnings.append(f"species '{ps.id}': id already taken in this project; stages bound to it may misfire")
            else:
                state.mutate_species(sp.id, lambda s, new_id=ps.id: setattr(s, "id", new_id))
        state.mutate_species(ps.id, lambda s, ps=ps: _fill_species(s, ps))

        template_id: str | None = None
        if ps.template is not None:
            dst = species_admin.ingest_template_file(project_dir, ps.id, protocol.asset_path(ps.template.asset))
            res = species_admin.register_template(
                state,
                ps.id,
                str(dst),
                polarity=ps.template.polarity,
                source=ps.template.source or f"protocol:{protocol.name}",
                lowpass=ps.template.lowpass_ang,
                imported_from=f"protocol:{protocol.name}/{ps.template.asset}",
                notes=ps.template.notes,
            )
            if res["success"]:
                template_id = res["template_id"]
            else:
                warnings.append(f"species '{ps.id}': template not registered — {res['error']}")
        if ps.mask is not None:
            dst = species_admin.ingest_template_file(project_dir, ps.id, protocol.asset_path(ps.mask.asset))
            mask = TemplateMask(
                mask_path=str(dst),
                method=ps.mask.method,
                threshold=ps.mask.threshold,
                extend_pixels=ps.mask.extend_pixels,
                soft_edge_pixels=ps.mask.soft_edge_pixels,
                lowpass_ang=ps.mask.lowpass_ang,
                derived_from_template_id=template_id,
                notes=ps.mask.notes,
            )
            res = species_admin.register_mask(state, ps.id, mask)
            if not res["success"]:
                warnings.append(f"species '{ps.id}': mask not registered — {res['error']}")
    return warnings


def _fill_species(sp, ps: ProtocolSpecies) -> None:
    sp.diameter_ang = ps.diameter_ang
    sp.symmetry = ps.symmetry
    sp.notes = ps.notes
    sp.extraction_params = ExtractionParams(**ps.extraction.model_dump()) if ps.extraction else None


def instantiate_stages(state: ProjectState, protocol: Protocol) -> list[str]:
    """Create every stage instance and apply its param snapshot. Values are coerced through
    the field's own type (enums, ints — a float can never land in an int field). Unknown
    fields, rejected values and fields the protocol does not cover are all reported."""
    warnings: list[str] = []
    for st in protocol.stages:
        iid = st.instance_id
        state.ensure_job_initialized(st.job_type, instance_id=iid)
        jm = state.jobs[iid]
        cls_name = type(jm).__name__
        for name, value in st.params.items():
            if name not in jm.USER_PARAMS:
                warnings.append(f"{iid}: '{name}' is not a user parameter of {cls_name} — NOT applied")
                continue
            try:
                coerced = _coerce(jm, name, value)
            except ValidationError as e:
                msg = e.errors()[0].get("msg", "invalid") if e.errors() else "invalid"
                warnings.append(f"{iid}: '{name}'={value!r} rejected ({msg}) — code default kept")
                continue
            setattr(jm, name, coerced)

        filled: set[str] = set()
        if st.species:
            jm.species_id = st.species
            sp = state.get_species(st.species)
            if sp is not None:
                filled = _apply_species_defaults(jm, st, sp, warnings)

        uncovered = sorted(jm.USER_PARAMS - set(st.params) - filled)
        if uncovered:
            warnings.append(f"{iid}: not covered by the protocol, code defaults in effect: {', '.join(uncovered)}")

        for slot, producer in st.inputs.items():
            # A stage reference names an instance of the project the protocol was exported from,
            # which need not match the instance ids created here, so explicit wiring is reported
            # and automatic selection is used: it wires each stage to its own species' upstream
            # and reports a tie instead of guessing.
            warnings.append(f"{iid}.{slot}: explicit wiring to '{producer}' NOT applied (automatic selection used)")
    return warnings


def _coerce(jm, name: str, value: Any) -> Any:
    field = type(jm).model_fields[name]
    return TypeAdapter(field.annotation).validate_python(value)


def stage_edits(state: ProjectState, protocol: Protocol) -> dict[str, list[tuple[str, Any, Any]]]:
    """Per stage instance, the user parameters whose current value differs from what the
    protocol pinned: `{instance_id: [(name, protocol_value, current_value)]}`. Compared after
    the same coercion apply used, so `"3"` vs `3` is not an edit; a stage missing from the
    project is absent. Species-shaped defaults are not pins and are not compared. Read-only
    — the Protocols view renders these as "edited" chips."""
    out: dict[str, list[tuple[str, Any, Any]]] = {}
    for st in protocol.stages:
        jm = state.jobs.get(st.instance_id)
        if jm is None:
            continue
        edits: list[tuple[str, Any, Any]] = []
        for name, value in st.params.items():
            if name not in jm.USER_PARAMS:
                continue
            try:
                want = _coerce(jm, name, value)
            except ValidationError:
                continue  # rejected at apply and reported then — the job never held it, so not an edit
            have = getattr(jm, name, None)
            if want != have:
                edits.append((name, want, have))
        if edits:
            out[st.instance_id] = edits
    return out


def _apply_species_defaults(jm, st: ProtocolStage, sp, warnings) -> set[str]:
    """The species' values a job added from the roster takes (`pipeline_edit.species_defaults`), for the
    fields the stage's own params do not pin. Returns the fields filled."""
    defaults = species_defaults(jm.job_type, sp)
    filled: set[str] = set()
    for field, value in defaults.items():
        if field not in st.params:
            setattr(jm, field, value)
            filled.add(field)
    if jm.job_type == JobType.TEMPLATE_MATCH_PYTOM:
        for field, key in (("template_path", "template"), ("mask_path", "mask")):
            if field not in st.params and field not in defaults:
                warnings.append(
                    f"{st.instance_id}: species '{sp.id}' registers no {key}; {field} left empty (TM will refuse)"
                )
    return filled
