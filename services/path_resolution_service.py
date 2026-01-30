# services/path_resolution_service.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING

from services.io_slots import (
    InputSlot,
    OutputSlot,
    JobFileType,
    ResolvedInput,
    ResolvedOutput,
    ResolvedManifest,
)

from services.job_models import TemplateMatchPytomParams
from services.models_base import JobType, JobStatus

if TYPE_CHECKING:
    from services.project_state import ProjectState
    from services.job_models import AbstractJobParams


@dataclass(frozen=True)
class OutputCandidate:
    produces: JobFileType
    producer_job_type: JobType
    producer_output_key: str
    path: str

    # metadata for scoring
    execution_status: JobStatus
    relion_job_number: int  # 0 if unknown


class PathResolutionError(ValueError):
    pass


class PathResolutionService:
    """
    Stage 3: schema-based path resolution.
    - No orchestrator changes required.
    - No filesystem existence checks.
    - Deterministic tie-breaking.
    """

    def __init__(self, state: "ProjectState"):
        self.state = state

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def resolve_all_paths(
        self,
        job_type: JobType,
        job_model: "AbstractJobParams",
        job_dir: Path,
        instance_id: Optional[str] = None,
        return_manifest: bool = False,
    ) -> Dict[str, Any] | Tuple[Dict[str, Any], ResolvedManifest]:
        """
        Resolve inputs + outputs using schemas and return a dict compatible with job_model.paths.
        """
        outputs_manifest = self.resolve_outputs(job_type, job_dir)
        inputs_manifest = self.resolve_inputs(job_type, job_model)

        manifest = ResolvedManifest(
            job_type=job_type.value,
            instance_id=instance_id,
            inputs=inputs_manifest,
            outputs=outputs_manifest,
        )

        paths = manifest.as_paths_dict()
        if return_manifest:
            return paths, manifest
        return paths

    def resolve_outputs(self, job_type: JobType, job_dir: Path) -> List[ResolvedOutput]:
        schema = self._get_output_schema(job_type)
        resolved: List[ResolvedOutput] = []
        for slot in schema:
            resolved_path = str((job_dir / slot.path_template).resolve())
            resolved.append(
                ResolvedOutput(
                    output_key=slot.key,
                    produces=slot.produces,
                    path=resolved_path,
                )
            )
        return resolved

    def resolve_inputs(
        self,
        job_type: JobType,
        job_model: "AbstractJobParams",
    ) -> List[ResolvedInput]:
        """
        Resolve inputs for target job by looking at existing jobs in ProjectState and matching
        their declared outputs by JobFileType.
        """
        input_schema = self._get_input_schema(job_type)

        # Build once: JobFileType -> list[OutputCandidate]
        index = self._build_output_index()

        resolved_inputs: List[ResolvedInput] = []
        missing_required: List[str] = []

        for slot in input_schema:
            chosen = self._choose_candidate_for_slot(slot, index)

            if chosen is None:
                if slot.required:
                    missing_required.append(
                        f"{slot.key} accepts={ [t.value for t in slot.accepts] }"
                    )
                # optional: skip
                continue

            resolved_inputs.append(
                ResolvedInput(
                    input_key=slot.key,
                    chosen_type=chosen.produces,
                    source_job_type=chosen.producer_job_type.value,
                    source_instance_id=None,
                    source_output_key=chosen.producer_output_key,
                    path=chosen.path,
                )
            )

        if missing_required:
            raise PathResolutionError(
                f"Cannot resolve required inputs for {job_type.value}: " + "; ".join(missing_required)
            )

        return resolved_inputs

    # -------------------------------------------------------------------------
    # Indexing producers
    # -------------------------------------------------------------------------

    def _build_output_index(self) -> Dict[JobFileType, List[OutputCandidate]]:
        """
        Collect candidates from existing job models in state.

        IMPORTANT: We never check file existence. Paths may be predicted.
        """
        project_root = self._project_root()

        # candidate lists per type
        index: Dict[JobFileType, List[OutputCandidate]] = {t: [] for t in JobFileType}

        for producer_job_type, producer_model in self.state.jobs.items():
            out_schema = self._get_output_schema(producer_job_type)
            if not out_schema:
                continue

            relion_job_number = int(getattr(producer_model, "relion_job_number", 0) or 0)
            status = getattr(producer_model, "execution_status", JobStatus.UNKNOWN)

            for slot in out_schema:
                path = self._get_producer_output_path(
                    producer_job_type=producer_job_type,
                    producer_model=producer_model,
                    slot=slot,
                    project_root=project_root,
                )
                if not path:
                    continue

                index[slot.produces].append(
                    OutputCandidate(
                        produces=slot.produces,
                        producer_job_type=producer_job_type,
                        producer_output_key=slot.key,
                        path=path,
                        execution_status=status,
                        relion_job_number=relion_job_number,
                    )
                )

        # Stable sorting baseline for determinism (tie-break later)
        for t, lst in index.items():
            index[t] = sorted(
                lst,
                key=lambda c: (c.producer_job_type.value, c.producer_output_key, c.path),
            )

        return index

    def _get_producer_output_path(
        self,
        producer_job_type: JobType,
        producer_model: "AbstractJobParams",
        slot: OutputSlot,
        project_root: Path,
    ) -> Optional[str]:
        """
        How to obtain a producer's output path.

        Priority:
          1) producer_model.paths[slot.key] if present
          2) infer from producer_model.relion_job_name (job dir) + slot.path_template
          3) infer from state.job_path_mapping (job dir) + slot.path_template
        """
        # 1) stored in paths
        stored = (producer_model.paths or {}).get(slot.key)
        if stored:
            return str(Path(stored))

        # 2) infer from relion job name (External/job005/)
        relion_job_name = getattr(producer_model, "relion_job_name", None)
        if relion_job_name:
            job_dir = (project_root / relion_job_name.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        # 3) infer from job_path_mapping (also External/job005)
        mapped = (self.state.job_path_mapping or {}).get(producer_job_type.value)
        if mapped:
            job_dir = (project_root / mapped.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        return None

    # -------------------------------------------------------------------------
    # Candidate selection / scoring
    # -------------------------------------------------------------------------

    def _choose_candidate_for_slot(
        self,
        slot: InputSlot,
        index: Dict[JobFileType, List[OutputCandidate]],
    ) -> Optional[OutputCandidate]:
        """
        Find best candidate among accepted types using deterministic scoring.

        Rule order:
          1) preferred_source match (if any)
          2) highest relion_job_number
          3) SUCCEEDED preferred
          4) stable order (already sorted)
        """
        candidates: List[OutputCandidate] = []
        for t in slot.accepts:
            candidates.extend(index.get(t, []))

        if not candidates:
            return None

        preferred_job_type = self._parse_preferred_source(slot.preferred_source)

        def score(c: OutputCandidate) -> Tuple[int, int, int]:
            pref = 1 if (preferred_job_type is not None and c.producer_job_type == preferred_job_type) else 0
            newest = c.relion_job_number
            succeeded = 1 if c.execution_status == JobStatus.SUCCEEDED else 0
            return (pref, newest, succeeded)

        # choose max by (pref, newest, succeeded), with stable fallback ordering
        best = max(candidates, key=lambda c: (score(c), c.producer_job_type.value, c.producer_output_key, c.path))
        return best

    def _parse_preferred_source(self, preferred: Optional[str]) -> Optional[JobType]:
        """
        preferred_source is intended to be a JobType.value string.
        If it isn't valid, ignore it safely.
        """
        if not preferred:
            return None
        try:
            return JobType.from_string(preferred)
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Schema access
    # -------------------------------------------------------------------------

    def _get_input_schema(self, job_type: JobType) -> List[InputSlot]:
        job_model = self.state.jobs.get(job_type)
        cls = job_model.__class__ if job_model else None
        schema = getattr(cls, "INPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _get_output_schema(self, job_type: JobType) -> List[OutputSlot]:
        job_model = self.state.jobs.get(job_type)
        cls = job_model.__class__ if job_model else None
        schema = getattr(cls, "OUTPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _project_root(self) -> Path:
        if not self.state.project_path:
            raise PathResolutionError("ProjectState.project_path is not set")
        return Path(self.state.project_path).resolve()



# -----------------------------------------------------------------------------
# Context Paths Helper
# -----------------------------------------------------------------------------
#
# TODO (IO Slots Cleanup - Future Work):
# =======================================
#
# The paths below are "context paths" - paths derived from project structure
# that drivers need but that aren't true inputs/outputs in the wiring sense.
# This function exists to bridge the gap between schema-based I/O resolution
# and what drivers currently expect in their `paths` dict.
#
# REDUNDANCY ISSUES:
# - Many jobs need the same paths (e.g., warp_tiltseries_settings appears in
#   4+ job types). Each legacy resolve_paths() duplicated this logic.
# - Some paths like `warp_dir` are redundant with schema outputs like
#   `output_processing`.
#
# FUTURE OPTIONS TO CLEAN THIS UP:
#
# 1. **Context Slots**: Add a third slot type alongside InputSlot/OutputSlot
#    that declares project-level dependencies. The schema would become:
#      INPUT_SCHEMA = [...]
#      OUTPUT_SCHEMA = [...]
#      CONTEXT_SCHEMA = [
#          ContextSlot(key="warp_settings", path_template="warp_tiltseries.settings"),
#      ]
#
# 2. **Driver Self-Resolution**: Have drivers derive these directly from
#    ProjectState. They already have access via get_driver_context(), so
#    instead of receiving `paths["warp_tiltseries_settings"]`, they'd compute:
#      settings = project_state.project_path / "warp_tiltseries.settings"
#    This pushes path logic to drivers but reduces orchestrator complexity.
#
# 3. **ProjectPaths Object**: Create a dedicated object attached to ProjectState
#    that pre-computes all standard project paths once:
#      state.paths.warp_frameseries_settings
#      state.paths.tomostar_dir
#    Jobs reference these rather than computing them.
#
# 4. **Hybrid**: Use CONTEXT_SCHEMA for job-specific context (like tilt_series_dir
#    for Import) and ProjectPaths for universal paths (settings files).
#
# For now, we centralize the logic here to unblock schema-based resolution
# while keeping the driver interface unchanged. When you tackle this cleanup,
# grep for usages of these keys in drivers/ to understand actual dependencies.
#
# -----------------------------------------------------------------------------


def get_context_paths(
    job_type: JobType,
    job_model: "AbstractJobParams",
    job_dir: Path,
) -> Dict[str, str]:
    """
    Returns context paths - paths derived from project structure that aren't
    true inputs/outputs but are needed by drivers.
    
    These supplement the schema-resolved I/O paths to give drivers everything
    they need in `context["paths"]`.
    
    See the module-level TODO above for cleanup plans.
    """
    project_root = job_model.project_root
    
    # -------------------------------------------------------------------------
    # Universal paths (all jobs need these)
    # -------------------------------------------------------------------------
    paths: Dict[str, str] = {
        "job_dir": str(job_dir),
        "project_root": str(project_root),
    }
    
    # -------------------------------------------------------------------------
    # Raw data directories
    # - frames_dir: where movie frames live (symlinked during import)
    # - mdoc_dir: where mdoc metadata files live
    # -------------------------------------------------------------------------
    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF]:
        paths["frames_dir"] = str(project_root / "frames")
    
    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT]:
        paths["mdoc_dir"] = str(project_root / "mdoc")
    
    # -------------------------------------------------------------------------
    # Warp settings files
    # - These are master settings files at project root that WarpTools reads
    # - Created by first job that needs them, reused by subsequent jobs
    # -------------------------------------------------------------------------
    if job_type in [JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT]:
        paths["warp_frameseries_settings"] = str(project_root / "warp_frameseries.settings")
    
    if job_type in [JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT, JobType.TS_CTF, JobType.TS_RECONSTRUCT]:
        paths["warp_tiltseries_settings"] = str(project_root / "warp_tiltseries.settings")
    
    # -------------------------------------------------------------------------
    # tomostar_dir: Warp's per-tomogram metadata files
    # - Jobs that interact with Warp's tilt series pipeline need this
    # -------------------------------------------------------------------------
    if job_type in [
        JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT,
        JobType.TS_CTF, JobType.TS_RECONSTRUCT
    ]:
        paths["tomostar_dir"] = str(project_root / "tomostar")
    
    # -------------------------------------------------------------------------
    # Job-specific context paths
    # -------------------------------------------------------------------------
    
    # Import creates per-tilt-series star files in this subdirectory
    if job_type == JobType.IMPORT_MOVIES:
        paths["tilt_series_dir"] = str(job_dir / "tilt_series")
    
    # DenoisePredict needs to know where reconstruction half-maps are
    # This is awkward - it's technically an input but structured differently
    # TODO: Consider making this a proper InputSlot that accepts WARP_TILTSERIES_DIR
    if job_type == JobType.DENOISE_PREDICT:
        # reconstruct_base is resolved by schema (it's in INPUT_SCHEMA)
        # but we need output_dir for the denoised tomograms
        paths["output_dir"] = str(job_dir / "denoised")
    
    # TemplateMatch needs user-provided template and mask paths
    # These are config, not job outputs - they come from the model itself
    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        tm_model = job_model  # type: TemplateMatchPytomParams
        if hasattr(tm_model, 'template_path') and tm_model.template_path:
            paths["template_path"] = str(Path(tm_model.template_path))
        if hasattr(tm_model, 'mask_path') and tm_model.mask_path:
            paths["mask_path"] = str(Path(tm_model.mask_path))
    
    return paths