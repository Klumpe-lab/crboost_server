"""Export = project -> Protocol (the authoring tool; roadmap 14 S1).

Walks `state.jobs` in pipeline order and keeps, per job, the FULL `USER_PARAMS` snapshot,
the species binding and the IO wiring; the species a stage references is exported with the
template / mask the stage actually ran on, copied into `assets/` when a bundle dir is given.
The result is meant to be hand-edited: an exported protocol says what a project DID, the
author decides what the protocol should PIN.

What does not travel, and is reported instead of dropped silently: `manual:/abs/path`
source overrides, overrides that name no job in the project, and assets when no bundle dir
was given to copy them into.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

from services.models_base import InstanceId, JobType
from services.path_resolution_service import PathResolutionService
from services.project_state import ProjectState
from services.protocols.schema import (
    ASSETS_DIRNAME,
    Expectation,
    Protocol,
    ProtocolExtraction,
    ProtocolMask,
    ProtocolSpecies,
    ProtocolStage,
    ProtocolTemplate,
    schema_fingerprint,
)
from services.templating.template_metadata import get_effective_mask_path, get_effective_template_path

logger = logging.getLogger(__name__)


def export_protocol(
    state: ProjectState, *, name: str, bundle_dir: Path | str | None = None, description: str = ""
) -> tuple[Protocol, list[str]]:
    """Build a Protocol from a project's state. Returns `(protocol, warnings)`; the caller
    dumps it (`dump_protocol`). With `bundle_dir` the species assets are copied into
    `<bundle_dir>/assets/` and the protocol is bound to that directory."""
    warnings: list[str] = []
    bundle = Path(bundle_dir).expanduser().resolve() if bundle_dir else None
    resolver = PathResolutionService(state)

    order = [iid for iid in (state.pipeline_order or list(state.jobs)) if iid in state.jobs]
    stages: list[ProtocolStage] = []
    # species id -> (template path, mask path) the TM stage ran on
    tm_assets: dict[str, tuple[str, str]] = {}
    for iid in order:
        jm = state.jobs[iid]
        jt = jm.job_type
        if jt is None or jt == JobType.EXTRACT_PICK_LIST:
            continue  # per-list interactive extraction is not a pipeline stage
        sid = _bound_species(state, iid, jm)
        params = jm.model_dump(mode="json", include=set(jm.USER_PARAMS))
        if jt == JobType.TEMPLATE_MATCH_PYTOM:
            # Volumes travel as species assets, not as job params (the paths are project-local).
            tpl, msk = params.pop("template_path", ""), params.pop("mask_path", "")
            if sid:
                tm_assets[sid] = (tpl, msk)
        stages.append(
            ProtocolStage(
                job=jt.value,
                species=sid,
                params=params,
                inputs=_portable_inputs(state, resolver, iid, jm, warnings),
                schema_fingerprint=schema_fingerprint(type(jm)),
            )
        )

    species = [
        _export_species(state, sid, tm_assets.get(sid), stages, bundle, warnings)
        for sid in dict.fromkeys(s.species for s in stages if s.species)
    ]

    expects = {"pixel_size_angstrom": Expectation(about=state.microscope.pixel_size_angstrom)}
    if state.import_selected_tilt_series > 0:
        expects["tilt_series_count"] = Expectation(about=float(state.import_selected_tilt_series))

    protocol = Protocol(
        name=name,
        description=description,
        provenance={"exported_from": state.project_name, "exported_at": datetime.now().isoformat(timespec="seconds")},
        expects=expects,
        species=species,
        stages=stages,
    )
    if bundle is not None:
        protocol._bundle_dir = bundle
    return protocol, warnings


def _bound_species(state: ProjectState, iid: str, jm) -> str | None:
    """The species a job is EXPLICITLY bound to (instance-id suffix or `species_id`), or
    None. Deliberately not `resolve_species`: its single-species fallback would tag every
    preprocessing job with the project's only species."""
    suffix = InstanceId.split(iid)[1]
    for candidate in (suffix, getattr(jm, "species_id", None)):
        if candidate and state.get_species(candidate) is not None:
            return candidate
    return None


def _portable_inputs(state: ProjectState, resolver: PathResolutionService, iid: str, jm, warnings: list[str]) -> dict:
    """`source_overrides` ("jobtype:External/job005") rewritten as stage references."""
    inputs: dict[str, str] = {}
    for slot, ref in (getattr(jm, "source_overrides", None) or {}).items():
        if ref.startswith("manual:"):
            warnings.append(f"{iid}.{slot}: manual path override is not portable — NOT exported ({ref})")
            continue
        _, _, instance_path = ref.partition(":")
        producer = next(
            (p for p, pm in state.jobs.items() if resolver._get_instance_path(p, pm) == instance_path.rstrip("/")), None
        )
        if producer is None:
            warnings.append(f"{iid}.{slot}: override '{ref}' names no job in this project — NOT exported")
            continue
        inputs[slot] = producer
    return inputs


def _export_species(
    state: ProjectState,
    sid: str,
    tm_paths: tuple[str, str] | None,
    stages: list[ProtocolStage],
    bundle: Path | None,
    warnings: list[str],
) -> ProtocolSpecies:
    sp = state.get_species(sid)
    tpl_path = (tm_paths[0] if tm_paths else "") or get_effective_template_path(sp)
    msk_path = (tm_paths[1] if tm_paths else "") or get_effective_mask_path(sp)

    template = None
    if tpl_path:
        entry = next((t for t in sp.templates if t.template_path == tpl_path), None)
        template = ProtocolTemplate(
            asset=_freeze_asset(tpl_path, f"{sid}_template", bundle, warnings),
            polarity=entry.polarity if entry else "black",
            source=entry.source if entry else None,
            lowpass_ang=entry.lowpass_resolution_ang if entry else None,
            notes=entry.notes if entry else "",
        )
    mask = None
    if msk_path:
        entry = next((m for m in sp.masks if m.mask_path == msk_path), None)
        mask = ProtocolMask(
            asset=_freeze_asset(msk_path, f"{sid}_mask", bundle, warnings),
            method=entry.method if entry else None,
            threshold=entry.threshold if entry else None,
            extend_pixels=entry.extend_pixels if entry else None,
            soft_edge_pixels=entry.soft_edge_pixels if entry else None,
            lowpass_ang=entry.lowpass_ang if entry else None,
            notes=entry.notes if entry else "",
        )

    extraction = None
    if sp.extraction_params is not None:
        extraction = ProtocolExtraction(**sp.extraction_params.model_dump())
    else:
        sub = next((s for s in stages if s.species == sid and s.job == JobType.SUBTOMO_EXTRACTION.value), None)
        if sub is not None and all(k in sub.params for k in ("box_size", "crop_size", "binning")):
            extraction = ProtocolExtraction(
                box_size=int(sub.params["box_size"]),
                crop_size=int(sub.params["crop_size"]),
                binning=float(sub.params["binning"]),
            )

    return ProtocolSpecies(
        id=sid,
        name=sp.name,
        diameter_ang=sp.diameter_ang,
        symmetry=sp.symmetry,
        notes=sp.notes,
        template=template,
        mask=mask,
        extraction=extraction,
    )


def _freeze_asset(src: str, stem: str, bundle: Path | None, warnings: list[str]) -> str:
    """Copy `src` into the bundle's assets/ and return the bundle-relative path. Without a
    bundle dir the relative path is still written — and the copy reported as owed."""
    rel = f"{ASSETS_DIRNAME}/{stem}{Path(src).suffix or '.mrc'}"
    if bundle is None:
        warnings.append(f"asset {rel} NOT copied (no bundle dir given): copy {src} there by hand")
        return rel
    if not Path(src).exists():
        warnings.append(f"asset {rel} NOT copied: source missing ({src})")
        return rel
    dst = bundle / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return rel
