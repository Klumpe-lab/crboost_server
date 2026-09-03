"""Apply = Protocol -> a new project (the one engine; roadmap 14 S3).

Creates the project through the same facade the landing page uses, registers the protocol's
species with their frozen assets, instantiates every stage with its FULL param snapshot and
persists once. Validation runs BEFORE anything touches disk; expectation mismatches, schema
drift and unknown fields come back as warnings in the result, never as silent substitutions.
The same function is what a future "create project from protocol" landing action calls.
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
from services.project_state import ExtractionParams, ProjectState, ProtocolOrigin, TemplateMask
from services.protocols.schema import (
    PROJECT_PROTOCOL_DIRNAME,
    PROTOCOL_FILENAME,
    Protocol,
    ProtocolSpecies,
    ProtocolStage,
    protocol_to_yaml,
    schema_fingerprint,
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
) -> dict[str, Any]:
    """Create `<project_base_path>/<project_name>` from `protocol`. `ok(project_path=,
    stages=[instance ids], warnings=[...], load_warnings=[...])` or `err(...)` (validation
    problems in `problems`, project-creation errors verbatim)."""
    problems = validate_protocol(protocol)
    if problems:
        return err("Protocol cannot be applied:\n  - " + "\n  - ".join(problems), problems=problems)

    # Layer 1 — dataset facts from the mdocs, the same autodetect the landing page runs.
    from services.configs.mdoc_service import get_mdoc_service

    facts = await asyncio.to_thread(get_mdoc_service().get_autodetect_params, mdocs_glob) or {}
    detected: dict[str, Any] = {}
    for src, dst in (
        ("pixel_spacing", "pixel_size_angstrom"),
        ("voltage", "acceleration_voltage_kv"),
        ("dose_per_tilt", "dose_per_tilt"),
        ("tilt_axis_angle", "tilt_axis_degrees"),
    ):
        if facts.get(src) is not None:
            detected[dst] = facts[src]
    if gain_reference_path:
        detected["gain_reference_path"] = str(gain_reference_path)
    mdoc_files = sorted(glob.glob(mdocs_glob))
    import_summary = {
        "total_positions": len(mdoc_files),
        "selected_positions": len(mdoc_files),
        "total_tilt_series": len(mdoc_files),
        "selected_tilt_series": len(mdoc_files),
        "source_directory": str(Path(movies_glob).parent),
        "frame_extension": Path(movies_glob).suffix,
    }

    created = await backend.create_project_and_scheme(
        project_name=project_name,
        project_base_path=str(project_base_path),
        selected_jobs=[],
        movies_glob=movies_glob,
        mdocs_glob=mdocs_glob,
        import_summary=import_summary,
        detected_params=detected or None,
        shared=shared,
    )
    if not created.get("success"):
        return created
    project_dir = Path(created["project_path"])
    state: ProjectState = backend.state_service.state_for(project_dir)

    warnings = expectation_warnings(state, protocol, backend.registry_for(project_dir))
    assets, w = register_protocol_species(state, protocol, project_dir)
    warnings += w
    warnings += instantiate_stages(state, protocol, assets)
    state.pipeline_order = protocol.stage_ids()
    state.protocol_origin = ProtocolOrigin(
        name=protocol.name,
        version=protocol.version,
        bundle_dir=str(protocol.bundle_dir or ""),
        applied_at=datetime.now().isoformat(timespec="seconds"),
        stage_fingerprints={iid: schema_fingerprint(type(state.jobs[iid])) for iid in protocol.stage_ids()},
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


def expectation_warnings(state: ProjectState, protocol: Protocol, registry) -> list[str]:
    """`expects:` vs the facts the mdocs produced. Warn, never block."""
    out: list[str] = []
    for key, exp in protocol.expects.items():
        if key == "pixel_size_angstrom":
            actual: float = state.microscope.pixel_size_angstrom
        elif key == "tilt_series_count":
            actual = float(len(registry.tilt_series_ids()))
        elif key == "dose_per_tilt":
            actual = state.acquisition.dose_per_tilt
        elif key == "acceleration_voltage_kv":
            actual = state.microscope.acceleration_voltage_kv
        elif key == "tilt_axis_degrees":
            actual = state.acquisition.tilt_axis_degrees
        else:
            out.append(f"expects.{key}: unknown expectation key — not checked")
            continue
        if not exp.holds(actual):
            out.append(f"expects.{key}: dataset has {actual:g}, protocol was validated at {exp.about:g} ± {exp.tol:g}")
    return out


def register_protocol_species(
    state: ProjectState, protocol: Protocol, project_dir: Path
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Register every protocol species; copy its assets into `templates/<id>/`. Returns
    `({species_id: {"template": path, "mask": path}}, warnings)` — the project-local
    copies the TM stage snapshots (the same values the pipeline builder would take)."""
    warnings: list[str] = []
    assets: dict[str, dict[str, str]] = {}
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

        entry: dict[str, str] = {}
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
                entry["template"] = str(dst)
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
            if res["success"]:
                entry["mask"] = str(dst)
            else:
                warnings.append(f"species '{ps.id}': mask not registered — {res['error']}")
        assets[ps.id] = entry
    return assets, warnings


def _fill_species(sp, ps: ProtocolSpecies) -> None:
    sp.diameter_ang = ps.diameter_ang
    sp.symmetry = ps.symmetry
    sp.notes = ps.notes
    sp.extraction_params = ExtractionParams(**ps.extraction.model_dump()) if ps.extraction else None


def instantiate_stages(state: ProjectState, protocol: Protocol, assets: dict[str, dict[str, str]]) -> list[str]:
    """Create every stage instance and apply its param snapshot. Values are coerced through
    the field's own type (enums, ints — a float can never land in an int field). Unknown
    fields, rejected values and fields the protocol does not cover are all REPORTED."""
    warnings: list[str] = []
    for st in protocol.stages:
        iid = st.instance_id
        state.ensure_job_initialized(st.job_type, instance_id=iid)
        jm = state.jobs[iid]
        cls_name = type(jm).__name__
        expected = schema_fingerprint(type(jm))
        if st.schema_fingerprint and st.schema_fingerprint != expected:
            warnings.append(
                f"{iid}: captured against a different {cls_name} field set "
                f"(fingerprint {st.schema_fingerprint} ≠ {expected}); see the per-field notes"
            )
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
            ps = protocol.get_species(st.species)
            if ps is not None:
                filled = _apply_species_defaults(jm, st, ps, assets.get(st.species, {}), warnings)

        uncovered = sorted(jm.USER_PARAMS - set(st.params) - filled)
        if uncovered:
            warnings.append(f"{iid}: not covered by the protocol, code defaults in effect: {', '.join(uncovered)}")

        for slot, producer in st.inputs.items():
            # The resolver's override keys embed the producer's job DIRECTORY, which a fresh
            # project has not allocated yet; a key written now would silently stop matching
            # the moment the producer is deployed. Until the resolver accepts instance ids,
            # explicit wiring is reported and automatic (species-aware) selection is used.
            warnings.append(f"{iid}.{slot}: explicit wiring to '{producer}' NOT applied (automatic selection used)")
    return warnings


def _coerce(jm, name: str, value: Any) -> Any:
    field = type(jm).model_fields[name]
    return TypeAdapter(field.annotation).validate_python(value)


def stage_edits(state: ProjectState, protocol: Protocol) -> dict[str, list[tuple[str, Any, Any]]]:
    """Per stage instance, the user parameters whose CURRENT value differs from what the
    protocol pinned: `{instance_id: [(name, protocol_value, current_value)]}`. Compared after
    the same coercion apply used, so `"3"` vs `3` is not an edit; a stage the project no
    longer has is absent. Species-shaped defaults are not pins and are not compared. Read-only
    — the Protocols view renders these as "edited" chips (roadmap 16 D7; never "drift")."""
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


def _apply_species_defaults(jm, st: ProtocolStage, ps: ProtocolSpecies, entry: dict[str, str], warnings) -> set[str]:
    """The species-shaped defaults the pipeline builder snapshots at creation (template,
    mask, symmetry, diameter, extraction geometry) — only for fields the stage's own params
    do not pin. Returns the fields filled."""
    filled: set[str] = set()
    iid = st.instance_id
    jt = jm.job_type
    if jt == JobType.TEMPLATE_MATCH_PYTOM:
        for field, key in (("template_path", "template"), ("mask_path", "mask")):
            if field in st.params:
                continue
            path = entry.get(key)
            if path:
                setattr(jm, field, path)
                filled.add(field)
            else:
                warnings.append(f"{iid}: species '{ps.id}' registers no {key}; {field} left empty (TM will refuse)")
        if "symmetry" not in st.params and ps.symmetry:
            jm.symmetry = ps.symmetry
            filled.add("symmetry")
    elif jt == JobType.TEMPLATE_EXTRACT_PYTOM:
        if "particle_diameter_ang" not in st.params and ps.diameter_ang:
            jm.particle_diameter_ang = float(ps.diameter_ang)
            filled.add("particle_diameter_ang")
    elif jt == JobType.SUBTOMO_EXTRACTION and ps.extraction is not None:
        for field in ("box_size", "binning", "crop_size"):
            if field not in st.params:
                setattr(jm, field, getattr(ps.extraction, field))
                filled.add(field)
    return filled
