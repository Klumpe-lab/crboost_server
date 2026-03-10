from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING

from services.io_slots import InputSlot, OutputSlot, JobFileType, ResolvedInput, ResolvedOutput, ResolvedManifest

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

    # Instance identification
    instance_path: str  # e.g., "External/job005"

    # metadata for scoring
    execution_status: JobStatus
    relion_job_number: int  # 0 if unknown

    species_id: Optional[str] = None  # propagated from producing job model

    @property
    def source_key(self) -> str:
        """Key format for source_overrides: 'jobtype:instance_path'"""
        return f"{self.producer_job_type.value}:{self.instance_path}"

    @property
    def display_name(self) -> str:
        """Human-readable name for UI dropdowns"""
        status_icon = {
            JobStatus.SUCCEEDED: "ok",
            JobStatus.RUNNING: "running",
            JobStatus.FAILED: "failed",
            JobStatus.SCHEDULED: "scheduled",
        }.get(self.execution_status, "?")
        base = f"{self.instance_path} ({self.producer_job_type.value}) [{status_icon}]"
        return f"{base} [{self.species_id}]" if self.species_id else base


@dataclass
class InputSlotValidation:
    """Result of validating an input slot's current configuration."""

    slot_key: str
    is_valid: bool
    source_key: Optional[str]
    resolved_path: Optional[str]
    error_message: Optional[str] = None
    file_exists: bool = False
    is_user_override: bool = False
    awaiting_upstream: bool = False


class PathResolutionError(ValueError):
    pass


class PathResolutionService:
    """
    Stage 3: schema-based path resolution.
    Now with user override support, candidate enumeration for UI,
    and species-aware scoring.
    """

    def __init__(self, state: "ProjectState", active_instance_ids: Optional[set] = None):
        self.state = state
        self._active_instance_ids = active_instance_ids
        self._output_index: Optional[Dict[JobFileType, List[OutputCandidate]]] = None

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
        Respects source_overrides from job_model and species-aware scoring.
        """
        outputs_manifest = self.resolve_outputs(job_type, job_dir)
        inputs_manifest = self.resolve_inputs(job_type, job_model)

        manifest = ResolvedManifest(
            job_type=job_type.value, instance_id=instance_id, inputs=inputs_manifest, outputs=outputs_manifest
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
            resolved.append(ResolvedOutput(output_key=slot.key, produces=slot.produces, path=resolved_path))
        return resolved

    def resolve_inputs(self, job_type: JobType, job_model: "AbstractJobParams") -> List[ResolvedInput]:
        """
        Resolve inputs for target job. Checks source_overrides first, then falls
        back to species-aware automatic selection.
        """
        input_schema = self._get_input_schema(job_type)
        index = self._build_output_index()
        overrides = getattr(job_model, "source_overrides", {}) or {}
        consumer_species_id = getattr(job_model, "species_id", None)

        resolved_inputs: List[ResolvedInput] = []
        missing_required: List[str] = []

        for slot in input_schema:
            chosen = None

            # 1. Check for user override first
            override_key = overrides.get(slot.key)
            if override_key:
                chosen = self._resolve_override(slot, override_key, index)

            # 2. Fall back to species-aware automatic selection
            if chosen is None:
                chosen = self._choose_candidate_for_slot(slot, index, consumer_species_id)

            if chosen is None:
                if slot.required:
                    missing_required.append(f"{slot.key} accepts={[t.value for t in slot.accepts]}")
                continue

            resolved_inputs.append(
                ResolvedInput(
                    input_key=slot.key,
                    chosen_type=chosen.produces,
                    source_job_type=chosen.producer_job_type.value,
                    source_instance_id=chosen.instance_path,
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
    # Candidate enumeration for UI
    # -------------------------------------------------------------------------

    def get_candidates_for_slot(
        self, job_type: JobType, slot_key: str, consumer_species_id: Optional[str] = None
    ) -> List[OutputCandidate]:
        """
        Get all valid candidates for a specific input slot, sorted with
        species-matched candidates first. Unmatched candidates are included
        but ranked lower -- the UI can dim them to signal the mismatch.
        """
        input_schema = self._get_input_schema(job_type)
        slot = next((s for s in input_schema if s.key == slot_key), None)

        if not slot:
            return []

        index = self._build_output_index()
        candidates = [c for t in slot.accepts for c in index.get(t, [])]

        def sort_key(c: OutputCandidate) -> Tuple[int, int, int, str]:
            # Lower value = sorted earlier
            species_rank = 0 if (consumer_species_id and c.species_id == consumer_species_id) else 1
            status_order = {
                JobStatus.SUCCEEDED: 0,
                JobStatus.RUNNING: 1,
                JobStatus.SCHEDULED: 2,
                JobStatus.FAILED: 3,
            }.get(c.execution_status, 4)
            return (species_rank, status_order, -c.relion_job_number, c.instance_path)

        return sorted(candidates, key=sort_key)

    def get_input_schema_for_job(self, job_type: JobType) -> List[InputSlot]:
        """Expose input schema for UI rendering."""
        return self._get_input_schema(job_type)

    def get_output_schema_for_job(self, job_type: JobType) -> List[OutputSlot]:
        """Expose output schema for UI rendering."""
        return self._get_output_schema(job_type)

    def validate_input_slot(
        self, job_type: JobType, job_model: "AbstractJobParams", slot_key: str, check_filesystem: bool = True
    ) -> InputSlotValidation:
        """
        Validate a single input slot's current configuration.
        Returns detailed validation result for UI feedback.
        Uses species-aware candidate selection.
        """
        input_schema = self._get_input_schema(job_type)
        slot = next((s for s in input_schema if s.key == slot_key), None)

        if not slot:
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=False,
                source_key=None,
                resolved_path=None,
                error_message=f"Unknown input slot: {slot_key}",
            )

        overrides = getattr(job_model, "source_overrides", {}) or {}
        override_key = overrides.get(slot_key)
        is_user_override = override_key is not None
        consumer_species_id = getattr(job_model, "species_id", None)

        index = self._build_output_index()
        chosen = None

        if override_key:
            chosen = self._resolve_override(slot, override_key, index)
            if chosen is None and override_key.startswith("manual:"):
                manual_path = override_key[7:]
                file_exists = Path(manual_path).exists() if check_filesystem else True
                return InputSlotValidation(
                    slot_key=slot_key,
                    is_valid=file_exists or not slot.required,
                    source_key=override_key,
                    resolved_path=manual_path,
                    file_exists=file_exists,
                    is_user_override=True,
                    error_message=None if file_exists else f"File not found: {manual_path}",
                )

        if chosen is None:
            chosen = self._choose_candidate_for_slot(slot, index, consumer_species_id)

        if chosen is None:
            return InputSlotValidation(
                slot_key=slot_key,
                is_valid=not slot.required,
                source_key=None,
                resolved_path=None,
                error_message=(
                    f"No valid source found (accepts: {[t.value for t in slot.accepts]})" if slot.required else None
                ),
                is_user_override=is_user_override,
            )

        is_pending = "pending_" in chosen.path
        source_in_flight = chosen.execution_status in (JobStatus.RUNNING, JobStatus.SCHEDULED)
        pipeline_active = getattr(self.state, "pipeline_active", False)

        expect_file_later = is_pending or (source_in_flight and pipeline_active)

        file_exists = True
        if check_filesystem and not expect_file_later:
            file_exists = Path(chosen.path).exists()

        is_valid = file_exists or expect_file_later or not slot.required

        if expect_file_later:
            error_message = None
        elif not file_exists:
            error_message = f"File not found: {chosen.path}"
        else:
            error_message = None

        return InputSlotValidation(
            slot_key=slot_key,
            is_valid=is_valid,
            source_key=chosen.source_key,
            resolved_path=chosen.path,
            file_exists=file_exists if not expect_file_later else False,
            is_user_override=is_user_override,
            error_message=error_message,
            awaiting_upstream=expect_file_later,
        )

    def validate_all_inputs(
        self, job_type: JobType, job_model: "AbstractJobParams", check_filesystem: bool = True
    ) -> List[InputSlotValidation]:
        """Validate all input slots for a job."""
        input_schema = self._get_input_schema(job_type)
        return [self.validate_input_slot(job_type, job_model, slot.key, check_filesystem) for slot in input_schema]

    # -------------------------------------------------------------------------
    # Override resolution
    # -------------------------------------------------------------------------

    def _resolve_override(
        self, slot: InputSlot, override_key: str, index: Dict[JobFileType, List[OutputCandidate]]
    ) -> Optional[OutputCandidate]:
        """
        Resolve a user override to a candidate.

        override_key format:
          - "jobtype:instance_path" e.g., "tsReconstruct:External/job005"
          - "manual:/path/to/file" for manual paths (handled separately)
        """
        if override_key.startswith("manual:"):
            return None

        if ":" not in override_key:
            return None

        job_type_str, instance_path = override_key.split(":", 1)

        for accepted_type in slot.accepts:
            for candidate in index.get(accepted_type, []):
                if candidate.producer_job_type.value == job_type_str and candidate.instance_path == instance_path:
                    return candidate

        return None

    # -------------------------------------------------------------------------
    # Indexing producers
    # -------------------------------------------------------------------------

    def _build_output_index(self) -> Dict[JobFileType, List[OutputCandidate]]:
        if self._output_index is not None:
            return self._output_index

        project_root = self._project_root()
        index: Dict[JobFileType, List[OutputCandidate]] = {t: [] for t in JobFileType}

        for instance_id, producer_model in self.state.jobs.items():
            producer_job_type = producer_model.job_type
            if producer_job_type is None:
                continue

            out_schema = self._get_output_schema(producer_job_type)
            if not out_schema:
                continue

            relion_job_number = int(getattr(producer_model, "relion_job_number", 0) or 0)
            status = getattr(producer_model, "execution_status", JobStatus.UNKNOWN)
            instance_path = self._get_instance_path(instance_id, producer_model)
            species_id = getattr(producer_model, "species_id", None)

            for slot in out_schema:
                path = self._get_producer_output_path(
                    instance_id=instance_id,
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
                        instance_path=instance_path,
                        execution_status=status,
                        relion_job_number=relion_job_number,
                        species_id=species_id,
                    )
                )

        for t, lst in index.items():
            index[t] = sorted(lst, key=lambda c: (c.producer_job_type.value, c.instance_path, c.producer_output_key))

        self._output_index = index
        return index

    def _get_instance_path(self, instance_id: str, job_model: "AbstractJobParams") -> str:
        relion_job_name = getattr(job_model, "relion_job_name", None)
        if relion_job_name:
            return relion_job_name.rstrip("/")

        mapped = (self.state.job_path_mapping or {}).get(instance_id)
        if mapped:
            return mapped.rstrip("/")

        from services.models_base import JobCategory

        category = getattr(job_model, "JOB_CATEGORY", JobCategory.EXTERNAL)
        return f"{category.value}/pending_{instance_id}"

    def _get_producer_output_path(
        self,
        instance_id: str,
        producer_job_type: JobType,
        producer_model: "AbstractJobParams",
        slot: OutputSlot,
        project_root: Path,
    ) -> Optional[str]:
        stored = (producer_model.paths or {}).get(slot.key)
        if stored:
            return str(Path(stored))

        relion_job_name = getattr(producer_model, "relion_job_name", None)
        if relion_job_name:
            job_dir = (project_root / relion_job_name.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        mapped = (self.state.job_path_mapping or {}).get(instance_id)
        if mapped:
            job_dir = (project_root / mapped.rstrip("/")).resolve()
            return str((job_dir / slot.path_template).resolve())

        status = getattr(producer_model, "execution_status", None)
        if status is not None:
            from services.models_base import JobCategory

            category = getattr(producer_model, "JOB_CATEGORY", JobCategory.EXTERNAL)
            predicted_dir = project_root / category.value / f"pending_{instance_id}"
            return str((predicted_dir / slot.path_template).resolve())

        return None

    def invalidate_cache(self):
        """Call when state changes to rebuild the output index."""
        self._output_index = None

    # -------------------------------------------------------------------------
    # Candidate selection / scoring
    # -------------------------------------------------------------------------

    def _choose_candidate_for_slot(
        self,
        slot: InputSlot,
        index: Dict[JobFileType, List[OutputCandidate]],
        consumer_species_id: Optional[str] = None,
    ) -> Optional[OutputCandidate]:
        """
        Find best candidate among accepted types using deterministic scoring.

        Priority order (highest to lowest):
          1. Species match -- candidate produced by a job tagged with the same species_id
          2. Preferred source job type (from slot.preferred_source)
          3. Most recent job number
          4. Succeeded status
        """
        candidates: List[OutputCandidate] = []
        for t in slot.accepts:
            candidates.extend(index.get(t, []))

        if not candidates:
            return None

        preferred_job_type = self._parse_preferred_source(slot.preferred_source)

        def score(c: OutputCandidate) -> Tuple[int, int, int, int]:
            succeeded = 1 if c.execution_status == JobStatus.SUCCEEDED else 0
            # Species match only counts if the candidate has actually run.
            # This prevents a job's own pending output from circularly winning
            # over a real succeeded upstream (e.g. TM output TOMOGRAMS_STAR
            # outranking tsReconstruct's TOMOGRAMS_STAR as TM's own input).
            species_match = 1 if (consumer_species_id and c.species_id == consumer_species_id and succeeded) else 0
            pref = 1 if (preferred_job_type and c.producer_job_type == preferred_job_type) else 0
            return (species_match, pref, c.relion_job_number, succeeded)

        return max(candidates, key=lambda c: (score(c), c.producer_job_type.value, c.producer_output_key, c.path))

    def _parse_preferred_source(self, preferred: Optional[str]) -> Optional[JobType]:
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
        from services.job_models import jobtype_paramclass

        cls = jobtype_paramclass().get(job_type)
        schema = getattr(cls, "INPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _get_output_schema(self, job_type: JobType) -> List[OutputSlot]:
        from services.job_models import jobtype_paramclass

        cls = jobtype_paramclass().get(job_type)
        schema = getattr(cls, "OUTPUT_SCHEMA", None) if cls else None
        return list(schema) if schema else []

    def _project_root(self) -> Path:
        if not self.state.project_path:
            raise PathResolutionError("ProjectState.project_path is not set")
        return Path(self.state.project_path).resolve()


# -----------------------------------------------------------------------------
# Context Paths Helper (unchanged)
# -----------------------------------------------------------------------------


def get_context_paths(job_type: JobType, job_model: "AbstractJobParams", job_dir: Path) -> Dict[str, str]:
    project_root = job_model.project_root
    paths: Dict[str, str] = {"job_dir": str(job_dir), "project_root": str(project_root)}

    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF, JobType.TS_ALIGNMENT]:
        paths["mdoc_dir"] = str(project_root / "mdoc")

    if job_type in [JobType.IMPORT_MOVIES, JobType.FS_MOTION_CTF]:
        paths["frames_dir"] = str(project_root / "frames")

    if job_type in [
        JobType.IMPORT_MOVIES,
        JobType.FS_MOTION_CTF,
        JobType.TS_ALIGNMENT,
        JobType.TS_CTF,
        JobType.TS_RECONSTRUCT,
    ]:
        paths["tomostar_dir"] = str(project_root / "tomostar")

    if job_type == JobType.IMPORT_MOVIES:
        paths["tilt_series_dir"] = str(job_dir / "tilt_series")

    if job_type == JobType.DENOISE_PREDICT:
        paths["output_dir"] = str(job_dir / "denoised")

    if job_type == JobType.TEMPLATE_MATCH_PYTOM:
        tm_model = job_model
        if hasattr(tm_model, "template_path") and tm_model.template_path:
            paths["template_path"] = str(Path(tm_model.template_path))
        if hasattr(tm_model, "mask_path") and tm_model.mask_path:
            paths["mask_path"] = str(Path(tm_model.mask_path))

    return paths
