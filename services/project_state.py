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
from typing import Dict, Any, Literal, Optional, Tuple, Type, List

from pydantic import BaseModel, Field, PrivateAttr, SerializeAsAny

from services.models_base import (
    JobStatus,
    MicroscopeType,
    AlignmentMethod,
    JobCategory,
    JobType,
    MicroscopeParams,
    AcquisitionParams,
)
from services.computing.slurm_service import SlurmConfig
from services.job_models import (
    AbstractJobParams,
    CandidateExtractPytomParams,
    Class3DParams,
    DenoisePredictParams,
    DenoiseTrainParams,
    FsMotionCtfParams,
    ImportMoviesParams,
    ReconstructParticleParams,
    SubtomoExtractionParams,
    TemplateMatchPytomParams,
    TsAlignmentParams,
    TsCtfParams,
    TsReconstructParams,
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

SCHEMA_VERSION: Tuple[int, int] = (3, 0)


# ─── Sidecar helpers ────────────────────────────────────────────────────
# Each registered template / mask file has a `<file>.meta.json` sidecar
# that pins its UUID, so file renames within the project keep the
# registration intact when sidecars travel with the file. Sidecar
# contents are minimal: `{"id": <uuid>, "kind": "template" | "mask"}`.
# All other metadata lives on the species model (the source of truth).


def _sidecar_path_for(file_path: str) -> Path:
    p = Path(file_path)
    return p.with_name(p.name + ".meta.json")


def _sidecar_read_id(file_path: str) -> Optional[str]:
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
    method: Optional[Literal["spherical", "cylindrical", "relion", "manual", "imported"]] = None

    threshold: Optional[float] = None
    extend_pixels: Optional[float] = None  # --extend_inimask
    soft_edge_pixels: Optional[float] = None  # --width_soft_edge
    lowpass_ang: Optional[float] = None  # --lowpass

    # Soft link back to the template the mask was derived from (UUID).
    # Optional — imported masks don't have a known source.
    derived_from_template_id: Optional[str] = None

    created_at: Optional[datetime] = None
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
    lowpass_resolution_ang: Optional[float] = None

    # Provenance (all optional — populated as known, never required).
    source: Optional[str] = None  # "PDB:6Z6J" / "EMDB-1234" / "imported" / "basic_shape:550:550:550"
    imported_from: Optional[str] = None
    created_at: Optional[datetime] = None
    notes: str = ""


class TemplateWorkbenchUIState(BaseModel):
    """Pure UI widget state for the template workbench. Used to remember
    layout preferences between sessions. Deliberately small."""

    auto_box: bool = True
    apply_lowpass: bool = False
    basic_shape_def: str = "550:550:550"


class ParticleSpecies(BaseModel):
    id: str  # slug, used as folder name and instance suffix
    name: str  # display label
    color: str = "#3b82f6"

    # Particle-intrinsic properties; species-level (not per-job).
    diameter_ang: Optional[float] = None
    symmetry: str = "C1"
    notes: str = ""

    # ── v3 decoupled collections ─────────────────────────────────────────
    # Templates and masks are independent registers; the workbench manages
    # both, the user selects which is "active" per category. New
    # generations APPEND (not replace) so prior work isn't lost.
    templates: List[ParticleTemplate] = Field(default_factory=list)
    masks: List[TemplateMask] = Field(default_factory=list)
    selected_template_id: str = ""
    selected_mask_id: str = ""

    workbench_ui: TemplateWorkbenchUIState = Field(default_factory=TemplateWorkbenchUIState)

    # ── Convenience accessors ────────────────────────────────────────────

    def get_selected_template(self) -> Optional[ParticleTemplate]:
        if not self.selected_template_id:
            return None
        return next((t for t in self.templates if t.id == self.selected_template_id), None)

    def get_selected_mask(self) -> Optional[TemplateMask]:
        if not self.selected_mask_id:
            return None
        return next((m for m in self.masks if m.id == self.selected_mask_id), None)

    def get_template_by_id(self, template_id: str) -> Optional[ParticleTemplate]:
        return next((t for t in self.templates if t.id == template_id), None)

    def get_mask_by_id(self, mask_id: str) -> Optional[TemplateMask]:
        return next((m for m in self.masks if m.id == mask_id), None)


class ImportPositionSummary(BaseModel):
    """Per-position summary persisted at project creation."""

    stage_position: int
    beam_count: int = 1
    tilt_count: int = 0
    selected: bool = True


class ImportTiltSeriesSummary(BaseModel):
    """Per-tilt-series record persisted at project creation."""

    stage_position: int
    beam_position: int
    tilt_count: int = 0
    selected: bool = True
    mdoc_filename: str = ""


def _migrate_v1_to_v2(data: Dict[str, Any]) -> None:
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

    species_by_id: Dict[str, Dict[str, Any]] = {
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
            mask_obj: Optional[Dict[str, Any]] = None
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
                k: wb[k]
                for k in ("auto_box", "apply_lowpass", "basic_shape_def", "auto_infer_seed")
                if k in wb
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


def _migrate_v2_to_v3(data: Dict[str, Any], project_root: Optional[Path]) -> None:
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
        mask_paths_in_collection = {
            m.get("mask_path") for m in masks if isinstance(m, dict) and m.get("mask_path")
        }

        # ── Pull v2 species.template into species.templates ────────────
        selected_template_id: Optional[str] = sp.get("selected_template_id") or None
        v2tpl = sp.get("template")
        v2_template_id: Optional[str] = None
        if isinstance(v2tpl, dict) and v2tpl.get("template_path"):
            tpath = v2tpl["template_path"]
            v2_template_id = sidecar_ensure(tpath, "template")
            if tpath not in template_paths_in_collection:
                templates.append({
                    "id": v2_template_id,
                    "template_path": tpath,
                    "polarity": v2tpl.get("polarity", "black"),
                    "lowpass_resolution_ang": v2tpl.get("lowpass_resolution_ang"),
                    "source": v2tpl.get("source"),
                    "imported_from": v2tpl.get("imported_from"),
                    "created_at": v2tpl.get("created_at"),
                    "notes": v2tpl.get("notes", ""),
                })
                template_paths_in_collection.add(tpath)
            if not selected_template_id:
                selected_template_id = v2_template_id

            # ── v2 nested mask → species.masks ─────────────────────────
            v2mask = v2tpl.get("mask")
            if isinstance(v2mask, dict) and v2mask.get("mask_path"):
                mpath = v2mask["mask_path"]
                mid = sidecar_ensure(mpath, "mask")
                if mpath not in mask_paths_in_collection:
                    masks.append({
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
                    })
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
                        masks.append({
                            "id": mid,
                            "mask_path": fpath,
                            "method": "imported",  # unknown provenance
                        })
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
                        templates.append({
                            "id": entry_id,
                            "template_path": fpath,
                            "polarity": polarity,
                        })
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


class ProjectState(BaseModel):
    """Complete project state with direct global parameter access"""

    schema_version: Tuple[int, int] = Field(default=SCHEMA_VERSION)
    project_name: str = "Untitled"
    project_path: Optional[Path] = None
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[str] = None
    job_path_mapping: Dict[str, str] = Field(default_factory=dict)

    movies_glob: str = ""
    mdocs_glob: str = ""

    # Aggregation projects skip raw-data import. Particles arrive via merging
    # optimisation_set.star files from existing projects; the merge step is a
    # standalone workspace card (not a pipeline job). Sources persist here so
    # the user can re-merge after adding more datasets.
    is_aggregation: bool = False
    aggregation_sources: List[str] = Field(default_factory=list)

    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    slurm_defaults: SlurmConfig = Field(default_factory=SlurmConfig.from_config_defaults)

    jobs: Dict[str, SerializeAsAny[AbstractJobParams]] = Field(default_factory=dict)
    species_registry: List[ParticleSpecies] = Field(default_factory=list)
    pipeline_active: bool = Field(default=False)

    # Dataset import summary (set at project creation)
    import_total_positions: int = 0
    import_selected_positions: int = 0
    import_total_tilt_series: int = 0
    import_selected_tilt_series: int = 0
    import_source_directory: str = ""
    import_frame_extension: str = ""
    import_position_details: List[ImportPositionSummary] = Field(default_factory=list)
    import_tilt_series_details: List[ImportTiltSeriesSummary] = Field(default_factory=list)

    # Per-tilt MDOC metadata, keyed by frame filename stem (= cryoBoostKey)
    tilt_metadata: Dict[str, Dict[str, float]] = Field(default_factory=dict)

    # Tilt filtering (standalone tool, not a pipeline job)
    tilt_filter_labels: Dict[str, str] = Field(default_factory=dict)
    tilt_filter_png_dir: Optional[str] = None

    _dirty: bool = PrivateAttr(default=False)

    def mark_dirty(self):
        self._dirty = True

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    def save_if_dirty(self, path: Optional[Path] = None):
        if self.is_dirty:
            self.save(path)

    def get_species(self, species_id: str) -> Optional[ParticleSpecies]:
        return next((s for s in self.species_registry if s.id == species_id), None)

    def add_species(self, name: str, color: str = "#3b82f6") -> ParticleSpecies:
        """Create a new species entry from a display name. Caller is responsible
        for ensuring the name is not blank before calling."""
        sid = slugify(name)
        # Avoid id collisions by appending a counter if needed
        existing_ids = {s.id for s in self.species_registry}
        base = sid
        n = 2
        while sid in existing_ids:
            sid = f"{base}_{n}"
            n += 1
        species = ParticleSpecies(id=sid, name=name, color=color)
        self.species_registry.append(species)
        self.update_modified()
        return species

    def remove_species(self, species_id: str) -> bool:
        """Drop a species from the registry. Returns True if removed.
        File cleanup (templates/<sid>/) is the caller's responsibility —
        this method only mutates the in-memory registry."""
        before = len(self.species_registry)
        self.species_registry = [s for s in self.species_registry if s.id != species_id]
        removed = len(self.species_registry) < before
        if removed:
            self.update_modified()
        return removed

    def ensure_job_initialized(
        self, job_type: JobType, instance_id: Optional[str] = None, template_path: Optional[Path] = None
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

    def save(self, path: Optional[Path] = None):
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

        with open(path, "r") as f:
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
            project_path=Path(data["project_path"]) if data.get("project_path") else None,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            modified_at=datetime.fromisoformat(data.get("modified_at", datetime.now().isoformat())),
            created_by=data.get("created_by"),
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
            logger.warning("Could not load species registry: %s", e)
            project_state.species_registry = []

        # Restore dataset import summary
        project_state.import_total_positions = data.get("import_total_positions", 0)
        project_state.import_selected_positions = data.get("import_selected_positions", 0)
        project_state.import_total_tilt_series = data.get("import_total_tilt_series", 0)
        project_state.import_selected_tilt_series = data.get("import_selected_tilt_series", 0)
        project_state.import_source_directory = data.get("import_source_directory", "")
        project_state.import_frame_extension = data.get("import_frame_extension", "")
        try:
            project_state.import_position_details = [
                ImportPositionSummary(**pd) for pd in data.get("import_position_details", [])
            ]
            project_state.import_tilt_series_details = [
                ImportTiltSeriesSummary(**td) for td in data.get("import_tilt_series_details", [])
            ]
        except Exception as e:
            logger.warning("Could not load import details: %s", e)
            project_state.import_position_details = []
            project_state.import_tilt_series_details = []

        # Restore per-tilt MDOC metadata
        project_state.tilt_metadata = data.get("tilt_metadata", {})

        # Restore tilt filter state
        project_state.tilt_filter_labels = data.get("tilt_filter_labels", {})
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
            except Exception as e:
                logger.warning("Skipping job instance '%s' - failed to deserialize: %s", instance_id, e)

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

_project_states: Dict[Path, ProjectState] = {}


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


def get_project_state() -> ProjectState:
    """Convenience for UI code: resolves project_path from the current
    browser tab's UIStateManager.

    Falls back to a detached blank ProjectState if no project is loaded
    yet (landing page before create/load). This means all existing
    get_project_state() callsites in UI code work unchanged.
    """
    try:
        from ui.ui_state import get_ui_state_manager

        ui_mgr = get_ui_state_manager()
        if ui_mgr.project_path:
            return get_project_state_for(ui_mgr.project_path)
    except RuntimeError:
        # No client connection (background task, server startup, etc.)
        pass
    return ProjectState()


def set_project_state(new_state: ProjectState):
    """Legacy setter -- routes into the registry if the state has a project_path,
    otherwise falls back to replacing the tab-context entry."""
    if new_state.project_path:
        set_project_state_for(new_state.project_path, new_state)
    else:
        # Pre-creation state (landing page). Just park it in the registry
        # under a sentinel key; get_project_state() won't find it via
        # tab context anyway, and it'll be replaced once a real path exists.
        pass


class StateService:
    """Manages persistence of ProjectState to disk.

    - UI code accesses .state (resolves via tab context)
    - Backend code with an explicit path uses .state_for(path)
    - save_project is serialized with an asyncio.Lock
    """

    def __init__(self):
        self._save_lock = asyncio.Lock()

    def state_for(self, project_path: Path) -> ProjectState:
        """Explicit accessor for backend/service code that has a path."""
        return get_project_state_for(project_path)

    @property
    def state(self) -> ProjectState:
        """Tab-context accessor. Backend code should prefer state_for(path)."""
        return get_project_state()

    async def update_from_mdoc(self, mdocs_glob: str, project_path: Optional[Path] = None):
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

    async def ensure_job_initialized(self, job_type: JobType, template_path: Optional[Path] = None):
        self.state.ensure_job_initialized(job_type, template_path)

    async def load_project(self, project_json_path: Path):
        try:
            new_state = ProjectState.load(project_json_path)
            project_path = new_state.project_path or project_json_path.parent
            new_state.project_path = project_path  # <-- fix
            set_project_state_for(project_path, new_state)
            return True
        except Exception:
            return False

    # in StateService.save_project(), replace the final save call:

    async def save_project(
        self, save_path: Optional[Path] = None, project_path: Optional[Path] = None, force: bool = False
    ):
        async with self._save_lock:
            if project_path:
                state = get_project_state_for(project_path)
            else:
                state = get_project_state()

            if save_path:
                target_path = save_path
            elif state.project_path:
                target_path = state.project_path / "project_params.json"
            else:
                return

            loop = asyncio.get_event_loop()
            if force:
                await loop.run_in_executor(None, state.save, target_path)
            else:
                if state.is_dirty:
                    await loop.run_in_executor(None, state.save, target_path)


_state_service_instance: Optional[StateService] = None


def get_state_service() -> StateService:
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = StateService()
    return _state_service_instance
