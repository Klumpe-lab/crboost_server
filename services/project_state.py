# services/project_state.py
from __future__ import annotations
import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, PrivateAttr, SerializeAsAny, field_validator

from services.models_base import (
    InstanceId,
    JobType,
    # Re-exports: many modules import these via services.project_state.
    # The `as X` alias marks them as intentional so autofixes don't strip them.
    JobCategory as JobCategory,
    JobStatus as JobStatus,
    PickListType,
    ListExtractionState,
    MicroscopeParams,
    AcquisitionParams,
    species_palette_color,
)
from services.computing.slurm_service import SlurmConfig
from services.job_models import (
    AbstractJobParams,
    jobtype_paramclass,
)

logger = logging.getLogger(__name__)

# ── Schema version ────────────────────────────────────────────────────────────
#
# Bump MINOR when adding new optional fields (backwards-compatible).
# Bump MAJOR when removing/renaming fields, changing semantics, or restructuring
# in a way that older code cannot safely ignore.
#
# The version is stamped into every project_params.json on save and checked on
# load.  A major mismatch emits a loud warning; a missing version (pre-versioning
# files) is treated as (0, 0).

SCHEMA_VERSION: tuple[int, int] = (3, 2)  # 3.2: +use_afterok_orchestrator + job_dir_counter (P1.A)


def _afterok_global_default() -> bool:
    """DEV TOGGLE (temporary): repo-config global override (`use_afterok_orchestrator: true` in
    conf.yaml) so the afterok orchestrator can be exercised on every project without per-project
    project_params.json edits. Applied as the field default (fresh projects) and OR'd into the
    load path (existing projects); a per-project True always wins. Remove with the config field
    once the afterok path is validated. See ORCHESTRATOR_REPLACEMENT_PLAN.md §6a."""
    try:
        from services.configs.config_service import get_config_service

        return bool(get_config_service().config.use_afterok_orchestrator)
    except Exception:
        return False


# Sentinel `owner` value marking a project as shared/lab-owned rather than
# belonging to one user. `owner` is mutable (transfer); `created_by` stays as
# immutable provenance. None ⇒ owned by `created_by` (the legacy default).
SHARED_OWNER = "@lab"

# Project-relative location for aggregation merge outputs. Stable name, not under
# External/ so it can't collide with the schemer's jobNNN allocation. Each named
# merge writes its own MergedSources/<slug>/ folder (legacy projects wrote a flat
# MergedSources/optimisation_set.star).
MERGED_DIR_NAME = "MergedSources"


# ─── Sidecar helpers ────────────────────────────────────────────────────
# Each registered template / mask file has a `<file>.meta.json` sidecar
# that pins its UUID, so file renames within the project keep the
# registration intact when sidecars travel with the file. Sidecar
# contents are minimal: `{"id": <uuid>, "kind": "template" | "mask"}`.
# All other metadata lives on the species model (the source of truth).


def _sidecar_path_for(file_path: str) -> Path:
    p = Path(file_path)
    return p.with_name(p.name + ".meta.json")


def _sidecar_read_id(file_path: str) -> str | None:
    sp = _sidecar_path_for(file_path)
    if not sp.exists():
        return None
    try:
        data = json.loads(sp.read_text())
        return data.get("id") or None
    except Exception as e:
        logger.warning("Could not read sidecar %s: %s", sp, e)
        return None


def _sidecar_write(file_path: str, entry_id: str, kind: str) -> None:
    sp = _sidecar_path_for(file_path)
    try:
        sp.write_text(json.dumps({"id": entry_id, "kind": kind}, indent=2))
    except OSError as e:
        logger.warning("Could not write sidecar %s: %s", sp, e)


def sidecar_ensure(file_path: str, kind: str) -> str:
    """Return the UUID for `file_path`; read existing sidecar or create
    a new one. Public — called by the workbench whenever it registers
    a template or mask so future migrations / discovery can recognise
    the file."""
    existing = _sidecar_read_id(file_path)
    if existing:
        return existing
    new_id = uuid.uuid4().hex
    _sidecar_write(file_path, new_id, kind)
    return new_id


def slugify(name: str) -> str:
    """Convert a display name to a filesystem-safe species id."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s-]+", "_", s)
    return s.strip("_") or "species"


class TemplateMask(BaseModel):
    """A mask volume registered to a species. v3 makes masks a sibling
    of templates (not owned by any one template). The `relion_mask_create`
    knobs and the `derived_from_template_id` provenance let the user
    later answer "how did I make this mask and from what?". Box/apix are
    read from the MRC header on demand — never persisted on the model.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mask_path: str
    method: Literal["spherical", "cylindrical", "relion", "manual", "imported"] | None = None

    threshold: float | None = None
    extend_pixels: float | None = None  # --extend_inimask
    soft_edge_pixels: float | None = None  # --width_soft_edge
    lowpass_ang: float | None = None  # --lowpass

    # Soft link back to the template the mask was derived from (UUID).
    # Optional — imported masks don't have a known source.
    derived_from_template_id: str | None = None

    created_at: datetime | None = None
    notes: str = ""


class ParticleTemplate(BaseModel):
    """A specific template volume + its metadata. v3 allows a species to
    register many templates (the user selects which one is "current"
    via species.selected_template_id). Masks are NOT nested here in v3 —
    they live on species.masks as sibling first-class objects.

    `lowpass_resolution_ang` records the resolution this template was
    filtered to (None = unfiltered) — purely metadata, doesn't re-apply
    the filter. pixel_size_ang and box_px are read from the MRC header
    on demand.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    template_path: str
    polarity: Literal["white", "black"] = "black"
    lowpass_resolution_ang: float | None = None

    # Provenance (all optional — populated as known, never required).
    source: str | None = None  # "PDB:6Z6J" / "EMDB-1234" / "imported" / "basic_shape:550:550:550"
    imported_from: str | None = None
    created_at: datetime | None = None
    notes: str = ""


class TemplateWorkbenchUIState(BaseModel):
    """Pure UI widget state for the template workbench. Used to remember
    layout preferences between sessions. Deliberately small."""

    auto_box: bool = True
    apply_lowpass: bool = False
    basic_shape_def: str = "550:550:550"


class ExtractionParams(BaseModel):
    """Per-species subtomogram extraction geometry.

    NO defaults on purpose. A species picked de novo has no template-matching job
    to inherit box/bin/crop from, and guessing them silently produces
    wrong-but-plausible extractions that are painful to trace back. Absent means
    "the user has not decided yet" — the extract dialog must ask.
    """

    box_size: int
    binning: float
    crop_size: int


class ParticleSpecies(BaseModel):
    id: str  # slug, used as folder name and instance suffix
    name: str  # display label
    color: str = "#3b82f6"

    # How this species came to exist: "workbench" (template-driven), "manual"
    # (created de novo for hand picking), "imported". Empty on species that
    # pre-date the field — treat as "workbench".
    origin: str = ""

    # Set once the user commits extraction geometry for a de-novo species.
    # None means undecided, never "use some default" — see ExtractionParams.
    extraction_params: ExtractionParams | None = None

    # Particle-intrinsic properties; species-level (not per-job).
    diameter_ang: float | None = None
    symmetry: str = "C1"
    notes: str = ""

    # ── v3 decoupled collections ─────────────────────────────────────────
    # Templates and masks are independent registers; the workbench manages
    # both, the user selects which is "active" per category. New
    # generations APPEND (not replace) so prior work isn't lost.
    templates: list[ParticleTemplate] = Field(default_factory=list)
    masks: list[TemplateMask] = Field(default_factory=list)
    selected_template_id: str = ""
    selected_mask_id: str = ""

    workbench_ui: TemplateWorkbenchUIState = Field(default_factory=TemplateWorkbenchUIState)

    # ── Convenience accessors ────────────────────────────────────────────

    def get_selected_template(self) -> ParticleTemplate | None:
        if not self.selected_template_id:
            return None
        return next((t for t in self.templates if t.id == self.selected_template_id), None)

    def get_selected_mask(self) -> TemplateMask | None:
        if not self.selected_mask_id:
            return None
        return next((m for m in self.masks if m.id == self.selected_mask_id), None)

    def get_template_by_id(self, template_id: str) -> ParticleTemplate | None:
        return next((t for t in self.templates if t.id == template_id), None)

    def get_mask_by_id(self, mask_id: str) -> TemplateMask | None:
        return next((m for m in self.masks if m.id == mask_id), None)


def _migrate_v1_to_v2(data: dict[str, Any]) -> None:
    """Idempotent v1→v2 migration. Mutates `data` in place.

    PR 1 of the template-first-class refactor is additive: old fields
    (species.template_path, species.mask_path, species.workbench, the
    particle_diameter_ang on the candidate-extract job, symmetry on the
    TM job) stay on disk so code paths not yet ported keep working. New
    mirrors land on species.template, species.diameter_ang,
    species.symmetry, species.workbench_ui. Old fields are removed in
    PR 3 once all readers have flipped.
    """
    if tuple(data.get("schema_version", (0, 0)))[:2] >= (2, 0):
        return

    species_by_id: dict[str, dict[str, Any]] = {
        s["id"]: s for s in data.get("species_registry", []) if isinstance(s, dict) and "id" in s
    }

    for sp in species_by_id.values():
        wb = sp.get("workbench") or {}

        # Template object: build from v1 template_path/mask_path. setdefault
        # keeps a partially-migrated species's existing template intact on
        # re-run (idempotency).
        if not sp.get("template") and sp.get("template_path"):
            tpath = sp["template_path"]
            polarity = "white" if "_white" in tpath else "black"
            mask_obj: dict[str, Any] | None = None
            if sp.get("mask_path"):
                mask_obj = {"mask_path": sp["mask_path"]}
            sp["template"] = {
                "template_path": tpath,
                "polarity": polarity,
                "lowpass_resolution_ang": wb.get("template_resolution"),
                "mask": mask_obj,
            }

        # Workbench UI subset (drops pixel_size, box_size, template_resolution
        # — those move to MRC-header reads / ParticleTemplate respectively).
        if not sp.get("workbench_ui"):
            sp["workbench_ui"] = {
                k: wb[k] for k in ("auto_box", "apply_lowpass", "basic_shape_def", "auto_infer_seed") if k in wb
            }

    # Lift particle-intrinsic fields off job models onto the species. Old
    # fields stay on the job model for now (PR 1 is additive); PR 3 removes
    # them once readers have flipped.
    for jm in data.get("jobs", {}).values():
        if not isinstance(jm, dict):
            continue
        sid = jm.get("species_id")
        if not sid or sid not in species_by_id:
            continue
        sp = species_by_id[sid]

        if jm.get("job_type") == JobType.TEMPLATE_EXTRACT_PYTOM.value and "particle_diameter_ang" in jm:
            sp.setdefault("diameter_ang", jm["particle_diameter_ang"])

        if jm.get("job_type") == JobType.TEMPLATE_MATCH_PYTOM.value and "symmetry" in jm:
            sp.setdefault("symmetry", jm["symmetry"])

    data["schema_version"] = [2, 0]


def _migrate_v2_to_v3(data: dict[str, Any], project_root: Path | None) -> None:
    """v2 → v3 migration: decouple masks from templates; promote both to
    sibling collections on the species (`species.templates` /
    `species.masks`) with UUID identity. Drop the seed concept entirely
    (mask-creation uses the white template + thresholding; seeds were a
    fidelity optimization we don't need). Hard cut — v2 fields are
    removed at migration time. The filesystem `templates/<sid>/` folder
    is walked to register any orphan .mrc files left behind by prior
    workbench sessions.

    Idempotent and version-guarded.
    """
    if tuple(data.get("schema_version", (0, 0)))[:2] >= (3, 0):
        return

    for sp in data.get("species_registry", []):
        if not isinstance(sp, dict) or "id" not in sp:
            continue
        sid = sp["id"]

        # Existing collections (might already be partially populated on
        # idempotent re-run).
        templates = sp.setdefault("templates", [])
        masks = sp.setdefault("masks", [])
        template_paths_in_collection = {
            t.get("template_path") for t in templates if isinstance(t, dict) and t.get("template_path")
        }
        mask_paths_in_collection = {m.get("mask_path") for m in masks if isinstance(m, dict) and m.get("mask_path")}

        # ── Pull v2 species.template into species.templates ────────────
        selected_template_id: str | None = sp.get("selected_template_id") or None
        v2tpl = sp.get("template")
        v2_template_id: str | None = None
        if isinstance(v2tpl, dict) and v2tpl.get("template_path"):
            tpath = v2tpl["template_path"]
            v2_template_id = sidecar_ensure(tpath, "template")
            if tpath not in template_paths_in_collection:
                templates.append(
                    {
                        "id": v2_template_id,
                        "template_path": tpath,
                        "polarity": v2tpl.get("polarity", "black"),
                        "lowpass_resolution_ang": v2tpl.get("lowpass_resolution_ang"),
                        "source": v2tpl.get("source"),
                        "imported_from": v2tpl.get("imported_from"),
                        "created_at": v2tpl.get("created_at"),
                        "notes": v2tpl.get("notes", ""),
                    }
                )
                template_paths_in_collection.add(tpath)
            if not selected_template_id:
                selected_template_id = v2_template_id

            # ── v2 nested mask → species.masks ─────────────────────────
            v2mask = v2tpl.get("mask")
            if isinstance(v2mask, dict) and v2mask.get("mask_path"):
                mpath = v2mask["mask_path"]
                mid = sidecar_ensure(mpath, "mask")
                if mpath not in mask_paths_in_collection:
                    masks.append(
                        {
                            "id": mid,
                            "mask_path": mpath,
                            "method": v2mask.get("method"),
                            "threshold": v2mask.get("threshold"),
                            "extend_pixels": v2mask.get("extend_pixels"),
                            "soft_edge_pixels": v2mask.get("soft_edge_pixels"),
                            "lowpass_ang": v2mask.get("lowpass_ang"),
                            "derived_from_template_id": v2_template_id,
                            "created_at": v2mask.get("created_at"),
                            "notes": v2mask.get("notes", ""),
                        }
                    )
                    mask_paths_in_collection.add(mpath)
                if not sp.get("selected_mask_id"):
                    sp["selected_mask_id"] = mid

        # ── Walk templates/<sid>/ for orphans ──────────────────────────
        if project_root is not None:
            species_dir = project_root / "templates" / sid
            if species_dir.exists():
                try:
                    files = sorted(species_dir.glob("*.mrc"))
                except OSError as e:
                    logger.warning("Could not list templates dir %s: %s", species_dir, e)
                    files = []
                for f in files:
                    fname = f.name
                    fpath = str(f)
                    # Seeds are no longer first-class — skip the binary
                    # ellipsoid precursors left by basic-shape generation.
                    if fname.endswith("_seed.mrc"):
                        continue
                    is_mask = fname.endswith("_mask.mrc")
                    if is_mask:
                        if fpath in mask_paths_in_collection:
                            continue
                        mid = sidecar_ensure(fpath, "mask")
                        masks.append(
                            {
                                "id": mid,
                                "mask_path": fpath,
                                "method": "imported",  # unknown provenance
                            }
                        )
                        mask_paths_in_collection.add(fpath)
                    else:
                        if fpath in template_paths_in_collection:
                            continue
                        if "_white" in fname:
                            polarity = "white"
                        elif "_black" in fname:
                            polarity = "black"
                        else:
                            polarity = "black"
                        entry_id = sidecar_ensure(fpath, "template")
                        templates.append({"id": entry_id, "template_path": fpath, "polarity": polarity})
                        template_paths_in_collection.add(fpath)

        # Default selected_template_id to first template entry if not set
        if not selected_template_id and templates:
            selected_template_id = templates[0].get("id")
        sp["selected_template_id"] = selected_template_id or ""
        sp.setdefault("selected_mask_id", "")

        # ── Hard cut: drop v2 fields ───────────────────────────────────
        for legacy in ("template", "template_path", "mask_path", "workbench"):
            sp.pop(legacy, None)
        # workbench_ui loses the obsolete auto_infer_seed knob.
        wb_ui = sp.get("workbench_ui")
        if isinstance(wb_ui, dict):
            wb_ui.pop("auto_infer_seed", None)

    data["schema_version"] = [3, 0]


class AggregationSource(BaseModel):
    """One selected merge source for an aggregation project.

    Identifies a SubtomoExtraction optimisation_set (one project × one species)
    plus an optional per-tomogram include-list. `tomo_names is None` means "all
    tomograms in the set" (the common case); a list narrows the merge to those
    rlnTomoName values. The remaining fields are a display cache so the merge
    card can render a source without re-walking the foreign project on disk."""

    optset_path: str
    tomo_names: list[str] | None = None  # None = all tomos in this set
    # Tomos (by rlnTomoName) the user forced to ORIGINAL picks instead of the
    # curated/filtered set. Only meaningful for tomos that have a curated set;
    # absence => use curated where available. Per-tomo mutually-exclusive
    # curated/original choice surfaced in the merge selector.
    original_tomos: list[str] = Field(default_factory=list)
    project_name: str = ""
    project_path: str = ""
    species_id: str = ""
    species_label: str = ""


class AggregationMergeSource(BaseModel):
    """One contributing source recorded in a completed merge — a flat row for
    the 'what made the cut' table. Acquisition metadata (box/apix/binning) is
    captured so the user can spot anything that shouldn't have been co-merged."""

    project_name: str = ""
    species_label: str = ""
    n_particles: int = 0
    n_tomograms: int = 0
    box_size: int | None = None
    pixel_size: float | None = None
    binning: float | None = None


class AggregationMerge(BaseModel):
    """A named, recorded merge output. Each merge writes its own
    MergedSources/<slug>/ folder so different (species-subset) merges coexist;
    the registry on ProjectState tracks them and which one is active for
    downstream consumers."""

    slug: str  # filesystem-safe folder name under MergedSources/
    name: str = ""
    description: str = ""
    created_at: datetime = Field(default_factory=datetime.now)
    n_particles: int = 0
    n_tomograms: int = 0
    n_sources: int = 0
    sources: list[AggregationMergeSource] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class PickList(BaseModel):
    """One pick list in the per-(species, tomo) curation workbench.

    Only workbench-AUTHORED lists (manual/imported/merged) are persisted here.
    The `auto` (PyTOM candidates.star) and `filtered` (particles_filtered.star)
    lists are disk-backed and synthesized at render time, never stored — so the
    registry can't go stale against the files the resolver already owns. `slug`
    is unique within a (species_id, tomo_name)."""

    slug: str  # unique within (species_id, tomo_name); filesystem-safe
    label: str = ""
    list_type: PickListType = PickListType.MANUAL
    species_id: str = ""
    tomo_name: str = ""
    path: str = ""  # the .star/.coords file backing this list
    count: int = 0  # cached pick count, for display
    color: str = "#3b82f6"  # per-list overlay color
    visible: bool = True  # persisted so the toggle survives reloads
    parent_slugs: list[str] = Field(default_factory=list)  # provenance for derived lists
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = ""

    # ── Per-list subtomo extraction tracking ─────────────────────────────────
    # Manual/imported/merged lists are raw COORDINATES (never extracted), so they
    # can't go downstream until their picks are subtomo-extracted. Extraction is
    # scoped PER LIST (each list's particles land in their own output, recorded in
    # `extracted_path`) and the user triggers it per list — neither fully automatic
    # (no throwaway re-extractions) nor tedious. We persist the durable facts and
    # DERIVE the state (extraction_state()) so no stored boolean can drift from disk
    # truth (cf. the stuck-yellow stale-flag bug). `extracted_path` is the
    # optimisation_set.star produced by extracting THIS list — the per-list handle
    # the (future) authoritative-list resolver forwards downstream.
    extracted_path: str = ""  # optimisation_set.star from extracting THIS list ("" = never extracted)
    extracted_count: int = 0  # picks covered by that extraction (≠ count ⇒ picks added/removed ⇒ stale)
    extracted_at: datetime | None = None  # when the extraction ran (vs source mtime ⇒ in-place edits ⇒ stale)

    # How many picks survive the user's keep/drop curation in the cutout sheet
    # (None = no filter committed = all `count` kept). The keep/drop auto-commits to
    # `<slug>_filtered.star`; this is the cached kept count for the table, and the
    # filtered star is what the merge + per-list extraction actually consume.
    filtered_count: int | None = None

    def extraction_state(self) -> ListExtractionState:
        """Derived per-list extraction status: NOT_EXTRACTED (coords only) / STALE
        (picks changed since extraction) / EXTRACTED (current). Computed from the
        durable facts + cheap disk checks, never a stored flag."""
        if not self.extracted_path or not Path(self.extracted_path).exists():
            return ListExtractionState.NOT_EXTRACTED
        # Picks changed since extraction? Count delta is the cheap add/remove signal;
        # the source star's mtime (vs when extraction ran) catches in-place edits that
        # keep the same count (a moved pick). Per-list extraction consumes the KEPT
        # subset, so compare against filtered_count (the kept count) when a keep/drop
        # filter is committed — else extracting 4-of-7 would read as stale at once.
        expected_count = self.filtered_count if self.filtered_count is not None else self.count
        if self.extracted_count != expected_count:
            return ListExtractionState.STALE
        if self.extracted_at is not None and self.path:
            try:
                # Extraction consumes the curated subset (<stem>_filtered.star) when a
                # keep/drop filter is committed; re-curation rewrites THAT file, not the
                # base star, so a same-count swap (drop A, keep B) only bumps the filtered
                # star's mtime. Stat whichever file the extraction actually consumed.
                base = Path(self.path)
                consumed = base.with_name(f"{base.stem}_filtered.star")
                src = consumed if consumed.exists() else base
                if src.exists() and src.stat().st_mtime > self.extracted_at.timestamp() + 1.0:
                    return ListExtractionState.STALE
            except OSError:
                pass
        return ListExtractionState.EXTRACTED

    def mark_extracted(self, optimisation_set_path: str, n: int) -> None:
        """Record a completed per-list extraction. Caller persists ProjectState."""
        self.extracted_path = str(optimisation_set_path)
        self.extracted_count = int(n)
        self.extracted_at = datetime.now()


class ImportedTomograms(BaseModel):
    """Tomograms injected into a project via the PARTICLES-header import utility — a
    project-level artifact, NOT a pipeline job. The committed ``tomograms.star`` lives at
    ``star_path`` (``Tomograms/tomograms.star``, project-relative); the source fields are
    provenance + let the import dialog reopen with the prior selection. See
    services/tomogram_import.py + PARTICLE_PROJECT_ROADMAP.md (P1, un-job-ified)."""

    star_path: str = ""  # project-relative (or absolute) path to the committed tomograms.star
    source_mode: str = "synthesize"  # "synthesize" | "reference"
    source_paths: list[str] = Field(default_factory=list)  # selected .mrc files (synthesize)
    reference_star: str = ""  # an existing tomograms.star (reference mode)
    pixel_size_angstrom: float = 0.0  # user override (0 ⇒ derived from MRC header)
    tomogram_binning: float = 1.0
    optics_group_name: str = "opticsGroup1"
    count: int = 0  # cached tomogram count, for display
    imported_at: datetime = Field(default_factory=datetime.now)


class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    schema_version: tuple[int, int] = Field(default=SCHEMA_VERSION)
    project_name: str = "Untitled"
    # Cosmetic three-word nickname (e.g. "amber-vagrant-fermi"). Set at
    # project creation so future loads see the same mnemonic; legacy
    # projects without one get a deterministic fallback derived from the
    # project path at display time.
    mnemonic: str = ""
    project_path: Path | None = None
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)
    created_by: str | None = None
    # Mutable ownership for sharing/transfer. None ⇒ owned by `created_by`;
    # SHARED_OWNER ("@lab") ⇒ shared/lab project; otherwise a username the
    # project was transferred to. Pure attribution/grouping metadata — never
    # affects the on-disk location.
    owner: str | None = None
    job_path_mapping: dict[str, str] = Field(default_factory=dict)

    # Transient load report (roadmap 03 stage 5): human-readable messages for every
    # block load() had to drop or reset (schema drift, corrupt sub-payloads). Shown
    # once in the UI after project open so silent data loss becomes visible.
    # exclude=True — never persisted; it describes THIS load, not the project.
    load_warnings: list[str] = Field(default_factory=list, exclude=True)

    movies_glob: str = ""
    mdocs_glob: str = ""

    # Aggregation projects skip raw-data import. Particles arrive via merging
    # optimisation_set.star files from existing projects; the merge step is a
    # standalone workspace card (not a pipeline job). Sources persist here so
    # the user can re-merge after adding more datasets.
    is_aggregation: bool = False
    aggregation_sources: list[AggregationSource] = Field(default_factory=list)
    aggregation_merges: list[AggregationMerge] = Field(default_factory=list)
    active_merge_slug: str = ""

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    slurm_defaults: SlurmConfig = Field(default_factory=SlurmConfig.from_config_defaults)

    jobs: dict[str, SerializeAsAny[AbstractJobParams]] = Field(default_factory=dict)
    # Persisted pipeline membership + submit order (instance_ids). Historically the
    # participating set + order lived ONLY in UIState.selected_jobs (per-browser-tab
    # NiceGUI storage); persisting it here lets a tab-less submitter/reconciler know
    # the run set. Written through at deploy; backfilled from `jobs` on load for
    # legacy projects. See ORCHESTRATOR_REPLACEMENT_PLAN.md §6 (P1.0).
    pipeline_order: list[str] = Field(default_factory=list)
    species_registry: list[ParticleSpecies] = Field(default_factory=list)
    # Per-(species, tomo) manual-curation workbench. Only workbench-authored
    # lists (manual/imported/merged) persist here; `auto`/`filtered` are
    # disk-backed and synthesized at render time so this never goes stale
    # against the files the resolver owns. See ARTIAX_BRIDGE_PLAN.md
    # "## Curation workbench — the multi-list model".
    pick_lists: list[PickList] = Field(default_factory=list)
    # Per-(species, tomo) AUTHORITATIVE pick list: which list downstream tools
    # (per-list extraction / aggregation) consume. Keyed by `_auth_key(species, tomo)`
    # → slug ("auto" or a workbench-list slug). Absent ⇒ "auto" (the candidate set,
    # the historical default). Exactly one list is authoritative per (species, tomo).
    authoritative_pick_lists: dict[str, str] = Field(default_factory=dict)
    # Tomograms injected via the PARTICLES-header import utility (particle-only projects
    # with no upstream recon). A project-level artifact, not a job — see ImportedTomograms
    # / services/tomogram_import.py. None ⇒ no import committed.
    imported_tomograms: ImportedTomograms | None = None
    pipeline_active: bool = Field(default=False)

    # Orchestrator rework (P1.A): per-project opt-in to the SLURM afterok submit path
    # (submit_chain) instead of relion_schemer. Default False (schemer) so a test
    # project can exercise the afterok DAG while existing projects are unaffected.
    # Live status under this flag requires the P1.B reconciler. See
    # ORCHESTRATOR_REPLACEMENT_PLAN.md §6.
    use_afterok_orchestrator: bool = Field(default_factory=_afterok_global_default)
    # Sole job-dir-number allocator for the afterok path. Seeded once from
    # default_pipeline.star's rlnPipeLineJobCounter at the first submit_chain, then
    # monotonic -- always consumes a slot (no reuse-on-rerun), which fixes the
    # job-number off-by-one. 0 = unseeded.
    job_dir_counter: int = 0

    # Dataset import summary (set at project creation)
    import_total_positions: int = 0
    import_selected_positions: int = 0
    import_total_tilt_series: int = 0
    import_selected_tilt_series: int = 0
    import_source_directory: str = ""
    import_frame_extension: str = ""
    # Per-TS/per-tilt import details + filter labels used to be mirrored here
    # (import_position_details / import_tilt_series_details / tilt_metadata /
    # tilt_filter_labels); the TiltSeriesRegistry is the single source now
    # (roadmap 02 stage 4). Old JSON keys are ignored on load and dropped on
    # the next save (forward-only migration).
    tilt_filter_png_dir: str | None = None

    _dirty: bool = PrivateAttr(default=False)
    # Non-persisted change counter for species / templates / masks / pick lists /
    # authoritative choices (roadmap 08 §1). Wake-up input for poll gates only;
    # DOM gates use precise tuples (species_identity). Never bumped by mark_dirty.
    _registry_rev: int = PrivateAttr(default=0)

    @field_validator("aggregation_sources", mode="before")
    @classmethod
    def _migrate_aggregation_sources(cls, v):
        """Coerce legacy `List[str]` (bare optset paths) into AggregationSource.
        Pre-fine-selection projects stored only paths; treat each as all-tomos."""
        if not isinstance(v, list):
            return v
        return [{"optset_path": s} if isinstance(s, str) else s for s in v]

    def mark_dirty(self):
        self._dirty = True

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    @property
    def registry_rev(self) -> int:
        return self._registry_rev

    def bump_registry_rev(self) -> None:
        """Non-persisted change counter for species / templates / masks / pick lists /
        authoritative choices. Wake-up input for poll gates (the journey's 4 s outer
        gate, the workbench panel's 3 s observe); DOM gates use precise tuples
        (species_identity) so a bump alone never rebuilds a pane."""
        self._registry_rev += 1

    def species_identity(self) -> tuple:
        """Precise fingerprint of what species pills / tabs / dots draw (id, name,
        color per species) — the DOM-gate input for those renders."""
        return tuple((s.id, s.name, s.color) for s in self.species_registry)

    def mutate_species(self, species_id: str, fn) -> bool:
        """The one way to edit a species in place (workbench header, swatch, template
        / mask appends, selects). Marks dirty + bumps the rev; caller persists.
        Returns False when the species is unknown."""
        sp = self.get_species(species_id)
        if sp is None:
            return False
        fn(sp)
        self.mark_dirty()
        self.bump_registry_rev()
        return True

    @property
    def effective_owner(self) -> str | None:
        """Attribution target: explicit `owner` if set, else `created_by`."""
        return self.owner or self.created_by

    @property
    def is_shared(self) -> bool:
        return self.owner == SHARED_OWNER

    # ─── Aggregation merge resolution ────────────────────────────────────────
    # The merged optimisation_set.star is a project-level resource (built by the
    # aggregation merge card, not a pipeline job). These accessors live on
    # ProjectState — not the UI — so the path resolver can surface the active
    # merged optset as a first-class producer without a `ui.` import.

    def active_merge(self) -> AggregationMerge | None:
        """The merge downstream consumers wire to: the explicitly-active one, else
        the newest recorded merge (None if no merge has been recorded)."""
        merges = self.aggregation_merges or []
        if not merges:
            return None
        if self.active_merge_slug:
            m = next((x for x in merges if x.slug == self.active_merge_slug), None)
            if m:
                return m
        return merges[-1]

    def active_merged_optset(self) -> Path | None:
        """Resolved optimisation_set.star of the active merge (slug folder). Falls
        back to a legacy flat MergedSources/optimisation_set.star (pre-registry
        projects). None if nothing exists on disk yet."""
        if self.project_path is None:
            return None
        root = self.project_path
        m = self.active_merge()
        if m is not None:
            p = root / MERGED_DIR_NAME / m.slug / "optimisation_set.star"
            if p.exists():
                return p
        legacy = root / MERGED_DIR_NAME / "optimisation_set.star"
        return legacy if legacy.exists() else None

    def active_merged_optset_instance_path(self) -> str | None:
        """Stable instance_path identifying the synthetic merged-sources producer for
        the active merge. Shared by the resolver candidate and `source_overrides` so
        the two always agree on the same key. Tracks the SAME file
        active_merged_optset() resolves to (slug folder vs legacy flat) so the key can
        never describe a different merge than the path. None when no merged optset exists."""
        if self.project_path is None:
            return None
        root = self.project_path
        m = self.active_merge()
        if m is not None and (root / MERGED_DIR_NAME / m.slug / "optimisation_set.star").exists():
            return f"{MERGED_DIR_NAME}/{m.slug}"
        legacy = root / MERGED_DIR_NAME / "optimisation_set.star"
        return MERGED_DIR_NAME if legacy.exists() else None

    def save_if_dirty(self, path: Path | None = None):
        if self.is_dirty:
            self.save(path)

    def get_species(self, species_id: str) -> ParticleSpecies | None:
        return next((s for s in self.species_registry if s.id == species_id), None)

    def add_species(self, name: str, *, origin: str = "workbench", color: str = "") -> ParticleSpecies:
        """Create a new species entry from a display name. Caller is responsible
        for ensuring the name is not blank before calling.

        `color` defaults to a deterministic palette slot for the generated id, so
        every species is visually distinct on the shared tomogram canvas instead of
        all sharing one blue. Pass an explicit color to override.
        """
        sid = slugify(name)
        # Avoid id collisions by appending a counter if needed
        existing_ids = {s.id for s in self.species_registry}
        base = sid
        n = 2
        while sid in existing_ids:
            sid = f"{base}_{n}"
            n += 1
        species = ParticleSpecies(id=sid, name=name, color=color or species_palette_color(sid), origin=origin)
        self.species_registry.append(species)
        self.update_modified()
        # mark_dirty: StateService.save_project writes only when dirty (or forced) —
        # without this a created species was silently not persisted (roadmap 08 H1).
        self.mark_dirty()
        self.bump_registry_rev()
        return species

    def species_references(self, species_id: str) -> dict[str, list[str]]:
        """Everything in this project that points at `species_id`.

        Feeds both the delete-confirmation dialog (so the user sees what a delete
        takes with it) and `remove_species` itself, so the two can never disagree
        about what a species owns.
        """
        pick_lists = [f"{pl.tomo_name}/{pl.slug}" for pl in self.pick_lists if pl.species_id == species_id]
        authoritative = [k for k in self.authoritative_pick_lists if k.split("\x1f", 1)[0] == species_id]
        # A per-particle job names its species EITHER on the model (`species_id`) or in
        # its instance-id suffix (`templatematching__ribosome`) — both are explicit, and
        # a delete that misses one leaves an orphan job in the roster. Deliberately NOT
        # `resolve_species`: its single-species fallback would attribute every per-particle
        # job to the last remaining species, so deleting that species would take the whole
        # particle chain with it.
        jobs = [
            iid
            for iid, jm in (self.jobs or {}).items()
            if getattr(jm, "species_id", None) == species_id or InstanceId.split(iid)[1] == species_id
        ]
        # Overrides pointing at one of this species' pick-list producers. The resolver
        # key is "<jobtype>:<instance_path>" and a per-list producer's instance path is
        # `pick_list__<species>__<tomo>__<slug>` (path_resolution_service.
        # pick_list_producer_id). Match on the SPECIES-scoped prefix, never on slug:
        # every hand-picked list is slugged "manual", so a slug match would purge other
        # species' overrides too.
        from services.path_resolution_service import pick_list_producer_prefix_for_species

        prefix = pick_list_producer_prefix_for_species(species_id)
        overrides: list[str] = []
        for iid, jm in (self.jobs or {}).items():
            for slot, value in (getattr(jm, "source_overrides", None) or {}).items():
                if prefix in str(value):
                    overrides.append(f"{iid}:{slot}")
        return {
            "pick_lists": pick_lists,
            "authoritative_pick_lists": authoritative,
            "jobs": jobs,
            "source_overrides": overrides,
        }

    def remove_species(self, species_id: str) -> bool:
        """Drop a species from the registry AND purge everything referencing it.

        Returns True if removed. File cleanup (templates/<sid>/, Curation/<sid>/)
        is the caller's responsibility — this method only mutates in-memory state.

        Previously this dropped only the registry entry, leaving pick lists,
        authoritative-list choices and resolver overrides pointing at a species
        that no longer exists; those dangling refs then resolved to nothing at
        deploy time, far from the delete that caused them.
        """
        before = len(self.species_registry)
        self.species_registry = [s for s in self.species_registry if s.id != species_id]
        removed = len(self.species_registry) < before
        if not removed:
            return False

        refs = self.species_references(species_id)
        self.pick_lists = [pl for pl in self.pick_lists if pl.species_id != species_id]
        for key in refs["authoritative_pick_lists"]:
            self.authoritative_pick_lists.pop(key, None)
        for ref in refs["source_overrides"]:
            iid, _, slot = ref.rpartition(":")
            overrides = getattr(self.jobs.get(iid), "source_overrides", None)
            if overrides:
                overrides.pop(slot, None)

        self.update_modified()
        self.mark_dirty()
        self.bump_registry_rev()
        return removed

    # ── Curation workbench: per-(species, tomo) pick-list registry ───────────
    # Only workbench-authored lists (manual/imported/merged) live here; auto/
    # filtered are disk-synthesized at render time. See PickList docstring.

    def get_pick_lists(self, species_id: str, tomo_name: str) -> list[PickList]:
        return [pl for pl in self.pick_lists if pl.species_id == species_id and pl.tomo_name == tomo_name]

    def get_pick_list(self, slug: str, species_id: str, tomo_name: str) -> PickList | None:
        for pl in self.pick_lists:
            if pl.slug == slug and pl.species_id == species_id and pl.tomo_name == tomo_name:
                return pl
        return None

    def add_pick_list(self, pick_list: PickList) -> PickList:
        """Register (upsert) a workbench-authored list, replacing any existing
        entry with the same (slug, species, tomo). Marks dirty; caller persists."""
        self.remove_pick_list(pick_list.slug, pick_list.species_id, pick_list.tomo_name)
        self.pick_lists.append(pick_list)
        self.mark_dirty()
        self.bump_registry_rev()
        return pick_list

    def remove_pick_list(self, slug: str, species_id: str, tomo_name: str) -> bool:
        def _matches(pl: PickList) -> bool:
            return pl.slug == slug and pl.species_id == species_id and pl.tomo_name == tomo_name

        before = len(self.pick_lists)
        self.pick_lists = [pl for pl in self.pick_lists if not _matches(pl)]
        removed = len(self.pick_lists) != before
        if removed:
            self.mark_dirty()
            self.bump_registry_rev()
        return removed

    @staticmethod
    def _auth_key(species_id: str, tomo_name: str) -> str:
        return f"{species_id}\x1f{tomo_name}"

    def get_authoritative_slug(self, species_id: str, tomo_name: str) -> str:
        """Slug of the list downstream tools consume for this (species, tomo).
        Defaults to 'auto' (the candidate set) when nothing was chosen."""
        return self.authoritative_pick_lists.get(self._auth_key(species_id, tomo_name), "auto")

    def set_authoritative_slug(self, species_id: str, tomo_name: str, slug: str) -> None:
        """Choose the authoritative list for this (species, tomo) — the one downstream
        per-list extraction / aggregation consume. Marks dirty; caller persists.
        'auto' is stored explicitly so a switch back from a workbench list persists."""
        self.authoritative_pick_lists[self._auth_key(species_id, tomo_name)] = slug
        self.mark_dirty()
        self.bump_registry_rev()

    def set_imported_tomograms(self, record: ImportedTomograms) -> None:
        """Record the committed tomogram-import artifact (replaces any prior import).
        Marks dirty; caller persists."""
        self.imported_tomograms = record
        self.mark_dirty()

    def imported_tomograms_star_path(self) -> str | None:
        """Absolute path to the committed imported tomograms.star, or None. Resolves a
        project-relative star_path against project_path; returns None when it can't be
        made absolute (no project_path) rather than handing back a half-resolved path."""
        rec = self.imported_tomograms
        if not rec or not rec.star_path:
            return None
        p = Path(rec.star_path)
        if not p.is_absolute():
            if not self.project_path:
                return None
            p = Path(self.project_path) / p
        return str(p)

    def ensure_job_initialized(
        self, job_type: JobType, instance_id: str | None = None, template_path: Path | None = None
    ):
        if instance_id is None:
            instance_id = job_type.value

        if instance_id in self.jobs:
            return

        from services.configs.config_service import get_config_service

        param_class_map = jobtype_paramclass()
        param_class = param_class_map.get(job_type)

        if not param_class:
            raise ValueError(f"Unknown job type: {job_type}")

        job_params = param_class()
        job_params._project_state = self

        if hasattr(job_params, "rescale_angpixs") and self.microscope.pixel_size_angstrom > 0:
            binning = get_config_service().processing_defaults.reconstruction_binning
            computed = round(self.microscope.pixel_size_angstrom * binning, 2)
            job_params.rescale_angpixs = computed
            logger.info(
                "Auto-set rescale_angpixs = %s (%s * %s)", computed, self.microscope.pixel_size_angstrom, binning
            )

        self.jobs[instance_id] = job_params
        self.update_modified()

    def update_modified(self):
        self.modified_at = datetime.now()

    def save(self, path: Path | None = None):
        """Atomic file write via tempfile + rename."""
        save_path = path or (
            self.project_path / "project_params.json" if self.project_path else Path("project_params.json")
        )
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # Always stamp the current code's schema version on save
        self.schema_version = SCHEMA_VERSION

        data = self.model_dump(exclude={"project_path"})
        data["project_path"] = str(self.project_path) if self.project_path else None
        fd, tmp_path = tempfile.mkstemp(dir=str(save_path.parent), suffix=".tmp", prefix=".project_params_")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2, default=str)
            os.rename(tmp_path, str(save_path))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        self._dirty = False

    @classmethod
    def load(cls, path: Path):
        if not path.exists():
            raise FileNotFoundError(f"Project params file not found: {path}")

        with open(path) as f:
            data = json.load(f)

        # ── Version check ─────────────────────────────────────────────
        file_ver = tuple(data.get("schema_version", (0, 0)))
        code_major, code_minor = SCHEMA_VERSION
        file_major, file_minor = file_ver[0], file_ver[1] if len(file_ver) > 1 else 0

        if file_major == 0 and file_minor == 0:
            logger.info("Loading pre-versioned project file: %s", path.name)
        elif file_major != code_major:
            logger.warning(
                "Major schema mismatch! File=%s.%s, Code=%s.%s. Project: %s. Data may not load correctly.",
                file_major,
                file_minor,
                code_major,
                code_minor,
                path.parent.name,
            )
        elif file_minor != code_minor:
            logger.info(
                "Minor schema difference: file=%s.%s, code=%s.%s. Will be upgraded on next save.",
                file_major,
                file_minor,
                code_major,
                code_minor,
            )

        # Snapshot before each major migration so a user can recover if
        # something downstream is wrong. Backups are written once
        # (no overwrite) so successive opens after a real save don't keep
        # mutating them.
        if file_major < 2:
            backup_path = path.parent / f"{path.name}.v1.bak"
            if not backup_path.exists():
                try:
                    shutil.copy2(path, backup_path)
                    logger.info("Saved v1 schema backup to %s", backup_path)
                except OSError as e:
                    logger.warning("Could not write v1 schema backup: %s", e)
            _migrate_v1_to_v2(data)

        if file_major < 3 and tuple(data.get("schema_version", (0, 0)))[:2] < (3, 0):
            backup_path = path.parent / f"{path.name}.v2.bak"
            if not backup_path.exists():
                try:
                    shutil.copy2(path, backup_path)
                    logger.info("Saved v2 schema backup to %s", backup_path)
                except OSError as e:
                    logger.warning("Could not write v2 schema backup: %s", e)
            _migrate_v2_to_v3(data, project_root=path.parent)

        project_state = cls(
            schema_version=SCHEMA_VERSION,
            project_name=data.get("project_name", "Untitled"),
            mnemonic=data.get("mnemonic", ""),
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            modified_at=datetime.fromisoformat(data.get("modified_at", datetime.now().isoformat())),
            created_by=data.get("created_by"),
            owner=data.get("owner"),
            movies_glob=data.get("movies_glob", ""),
            mdocs_glob=data.get("mdocs_glob", ""),
            microscope=MicroscopeParams(**data.get("microscope", {})),
            acquisition=AcquisitionParams(**data.get("acquisition", {})),
            slurm_defaults=(
                SlurmConfig(**data["slurm_defaults"])
                if "slurm_defaults" in data
                else SlurmConfig.from_config_defaults()
            ),
            pipeline_active=data.get("pipeline_active", False),
        )

        project_state.job_path_mapping = data.get("job_path_mapping", {})
        try:
            project_state.species_registry = [ParticleSpecies(**s) for s in data.get("species_registry", [])]
        except Exception as e:
            logger.exception("Could not load species registry")
            project_state.load_warnings.append(f"Species registry could not be loaded and was reset ({e})")
            project_state.species_registry = []

        it = data.get("imported_tomograms")
        if it:
            try:
                project_state.imported_tomograms = ImportedTomograms(**it)
            except Exception as e:
                logger.exception("Could not load imported_tomograms")
                project_state.load_warnings.append(f"Imported-tomograms record could not be loaded ({e})")

        # Restore aggregation state (cross-project merge). load() is field-by-field,
        # so these MUST be restored explicitly -- otherwise a reloaded project (a UI
        # restart OR a SLURM driver loading from disk) silently loses its merge
        # registry, the synthetic `mergedSources` resolver candidate vanishes, and
        # consumers fail to resolve input_optimisation at drive time. is_aggregation
        # gates the merge-card UI + override self-heal, so it must survive too.
        project_state.is_aggregation = data.get("is_aggregation", False)
        project_state.active_merge_slug = data.get("active_merge_slug", "")
        try:
            # Mirror the _migrate_aggregation_sources validator (direct construction
            # bypasses it): coerce a legacy bare-path str into {"optset_path": str}.
            project_state.aggregation_sources = [
                AggregationSource(**({"optset_path": s} if isinstance(s, str) else s))
                for s in data.get("aggregation_sources", [])
            ]
        except Exception as e:
            logger.exception("Could not load aggregation_sources")
            project_state.load_warnings.append(f"Aggregation sources could not be loaded and were reset ({e})")
            project_state.aggregation_sources = []
        try:
            project_state.aggregation_merges = [AggregationMerge(**m) for m in data.get("aggregation_merges", [])]
        except Exception as e:
            logger.exception("Could not load aggregation_merges")
            project_state.load_warnings.append(f"Aggregation merges could not be loaded and were reset ({e})")
            project_state.aggregation_merges = []

        # Restore curation workbench pick lists (same field-by-field drop bug).
        try:
            project_state.pick_lists = [PickList(**p) for p in data.get("pick_lists", [])]
        except Exception as e:
            logger.exception("Could not load pick_lists")
            project_state.load_warnings.append(f"Curation pick lists could not be loaded and were reset ({e})")
            project_state.pick_lists = []
        project_state.authoritative_pick_lists = data.get("authoritative_pick_lists", {})

        # Restore dataset import summary
        project_state.import_total_positions = data.get("import_total_positions", 0)
        project_state.import_selected_positions = data.get("import_selected_positions", 0)
        project_state.import_total_tilt_series = data.get("import_total_tilt_series", 0)
        project_state.import_selected_tilt_series = data.get("import_selected_tilt_series", 0)
        project_state.import_source_directory = data.get("import_source_directory", "")
        project_state.import_frame_extension = data.get("import_frame_extension", "")
        # import_position_details / import_tilt_series_details / tilt_metadata /
        # tilt_filter_labels keys from older projects are deliberately ignored —
        # the TiltSeriesRegistry is the single source (dropped on next save).

        project_state.tilt_filter_png_dir = data.get("tilt_filter_png_dir")

        param_class_map = jobtype_paramclass()

        for instance_id, job_data in data.get("jobs", {}).items():
            try:
                job_type_value = job_data.get("job_type") or instance_id
                job_type = JobType.from_string(job_type_value)
                param_class = param_class_map.get(job_type)
                if param_class:
                    job_params = param_class(**job_data)
                    job_params._project_state = project_state
                    project_state.jobs[instance_id] = job_params
                else:
                    logger.warning(
                        "No param class for job type '%s' (instance '%s'), skipping", job_type_value, instance_id
                    )
                    project_state.load_warnings.append(
                        f"Job '{instance_id}' skipped — unknown job type '{job_type_value}'"
                    )
            except Exception as e:
                logger.exception("Skipping job instance '%s' - failed to deserialize", instance_id)
                project_state.load_warnings.append(f"Job '{instance_id}' could not be loaded and was skipped ({e})")

        # pipeline_order (P1.0): use the persisted value; for legacy projects that
        # predate the field, backfill from the loaded job set in file order -- every
        # initialized job is part of the pipeline. The authoritative value is the
        # write-through at deploy.
        _persisted_order = data.get("pipeline_order") or list(project_state.jobs.keys())
        # Drop instance_ids whose job failed to deserialize (e.g. a removed JobType like
        # the former 'importtomograms') so pipeline_order never references a ghost job.
        project_state.pipeline_order = [iid for iid in _persisted_order if iid in project_state.jobs]

        # Orchestrator rework (P1.A): explicit restore (load() is field-by-field, not
        # cls(**data)). Both are additive optional fields, so defaulting keeps legacy
        # projects on the schemer path untouched.
        project_state.use_afterok_orchestrator = (
            data.get("use_afterok_orchestrator", False) or _afterok_global_default()
        )
        project_state.job_dir_counter = data.get("job_dir_counter", 0)

        return project_state


# =========================================================================
# Path-keyed ProjectState registry
#
# Replaces the old module-level _project_state singleton.
# Each project directory gets exactly one ProjectState instance.
# Two browser tabs on the same project share the same instance.
# Two tabs on different projects get different instances.
# Two server processes (different users/ports) have completely
# separate registries (separate Python processes, separate memory).
# =========================================================================

_project_states: dict[Path, ProjectState] = {}


def get_project_state_for(project_path: Path) -> ProjectState:
    """Get or create ProjectState for a specific project directory.

    Backend/service code that has a project_path available should use
    this directly (via StateService.state_for(path)).
    """
    resolved = project_path.resolve()
    if resolved not in _project_states:
        params_file = resolved / "project_params.json"
        if params_file.exists():
            _project_states[resolved] = ProjectState.load(params_file)
        else:
            state = ProjectState()
            state.project_path = resolved
            _project_states[resolved] = state
    return _project_states[resolved]


def set_project_state_for(project_path: Path, state: ProjectState):
    """Insert or replace a ProjectState in the registry."""
    _project_states[project_path.resolve()] = state


def remove_project_state(project_path: Path):
    """Remove from registry (e.g. when closing a project)."""
    _project_states.pop(project_path.resolve(), None)


class StateService:
    """Manages persistence of ProjectState to disk.

    - Backend/service code uses .state_for(path) — always an explicit path.
    - UI code resolves tab context via ui/current_project.py, never here.
    - save_project is serialized with an asyncio.Lock
    """

    def __init__(self):
        self._save_lock = asyncio.Lock()

    def state_for(self, project_path: Path) -> ProjectState:
        """Explicit accessor for backend/service code that has a path."""
        return get_project_state_for(project_path)

    async def update_from_mdoc(self, mdocs_glob: str, project_path: Path | None = None):
        from services.configs.mdoc_service import get_mdoc_service

        mdoc_service = get_mdoc_service()
        logger.info("Parsing mdocs from: %s", mdocs_glob)
        mdoc_data = mdoc_service.get_autodetect_params(mdocs_glob)
        logger.info("Mdoc autodetect result: %s", mdoc_data)
        if not mdoc_data:
            return

        # CHANGED: explicit path when available (initialize_new_project
        # calls this before the UI tab has a project_path set)
        if project_path:
            s = self.state_for(project_path)
        else:
            s = self.state

        if "dose_per_tilt" in mdoc_data:
            s.acquisition.dose_per_tilt = mdoc_data["dose_per_tilt"]
            logger.info("Set dose_per_tilt = %s", mdoc_data["dose_per_tilt"])
        if "pixel_spacing" in mdoc_data:
            s.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data:
            s.microscope.acceleration_voltage_kv = mdoc_data["voltage"]
        if "tilt_axis_angle" in mdoc_data:
            s.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]
        s.update_modified()

    async def load_project(self, project_json_path: Path):
        try:
            project_path = project_json_path.parent.resolve()
            # In-memory state is authoritative — it may be ahead of disk
            # while a pipeline is running (sync_all_jobs updates job
            # execution_status/relion_job_name on every tick). Re-reading
            # from disk here would clobber those in-flight updates, which
            # was the root cause of jobs appearing stuck at the deploy-time
            # Scheduled state after a pipeline run. get_project_state_for
            # already loads from disk only when no instance is registered.
            state = get_project_state_for(project_path)
            if state.project_path is None:
                state.project_path = project_path
            return True
        except Exception:
            return False

    async def save_project(self, project_path: Path, *, force: bool = False):
        """Persist the registered ProjectState for `project_path` (skipped when
        the state isn't dirty, unless `force`). Explicit path only — UI-triggered
        saves go through backend.save_project (roadmap 01 stage 4)."""
        async with self._save_lock:
            state = get_project_state_for(project_path)
            if not state.project_path:
                logger.warning("save_project: state for %s has no project_path — nothing saved", project_path)
                return
            target_path = state.project_path / "project_params.json"
            if force or state.is_dirty:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, state.save, target_path)


_state_service_instance: StateService | None = None


def get_state_service() -> StateService:
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance
