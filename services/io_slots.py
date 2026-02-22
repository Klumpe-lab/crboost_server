# services/io_slots.py
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple
from pydantic import BaseModel, Field, ConfigDict, field_validator


# -----------------------------------------------------------------------------
# Core "artifact types" used for wiring. Keep this stable & explicit.
# -----------------------------------------------------------------------------

class JobFileType(str, Enum):
    # Import / raw
    TILT_SERIES_STAR = "tilt_series_star"                 # Import/jobXXX/tilt_series.star

    WARP_FRAMESERIES_SETTINGS = "warp_frameseries_settings"
                                                           # Warp frameseries
    FS_MOTION_CTF_STAR       = "fs_motion_and_ctf_star"    # External/jobXXX/fs_motion_and_ctf.star
    WARP_FRAMESERIES_DIR     = "warp_frameseries_dir"      # External/jobXXX/warp_frameseries/
    WARP_TILTSERIES_SETTINGS = "warp_tiltseries_settings"  # External/jobXXX/warp_tiltseries.settings

                                                           # Warp tiltseries
    ALIGNED_TILT_SERIES_STAR = "aligned_tilt_series_star"  # External/jobXXX/aligned_tilt_series.star
    TS_CTF_TILT_SERIES_STAR  = "ts_ctf_tilt_series_star"   # External/jobXXX/ts_ctf_tilt_series.star
    WARP_TILTSERIES_DIR      = "warp_tiltseries_dir"       # External/jobXXX/warp_tiltseries/

                                                         # Tomograms
    TOMOGRAMS_STAR          = "tomograms_star"           # External/jobXXX/tomograms.star
    DENOISED_TOMOGRAMS_STAR = "denoised_tomograms_star"  # External/jobXXX/denoised/tomograms.star (or similar)

    # Denoising model
    DENOISE_MODEL_TAR = "denoise_model_tar"               # External/jobXXX/denoising_model.tar.gz

                                                     # Template matching / picking
    TM_RESULTS_DIR        = "tm_results_dir"         # External/jobXXX/tmResults/
    CANDIDATES_STAR       = "candidates_star"        # External/jobXXX/candidates.star
    OPTIMISATION_SET_STAR = "optimisation_set_star"  # External/jobXXX/optimisation_set.star

    # Subtomo extraction
    PARTICLES_STAR = "particles_star"                     # External/jobXXX/particles.star
    # STA reconstruction / refinement
    REFERENCE_MAP = "reference_map"    # merged.mrc from reconstruct_particle (or refined map)
    HALF_MAP      = "half_map"         # half1.mrc (half2 derived by RELION naming convention)


# -----------------------------------------------------------------------------
# Slot models
# -----------------------------------------------------------------------------

class OutputSlot(BaseModel):
    """
    Declares: "this job produces artifact type X at logical key K".

    path_template is intentionally dumb in Stage 0: it's a relative path under job_dir.
    Later (Stage 3) PathResolutionService can interpret it.
    """
    model_config = ConfigDict(validate_assignment=True)

    key: str = Field(..., description="Logical output name used in job_model.paths")
    produces: JobFileType
    path_template: str = Field(
        ...,
        description="Relative path under job_dir, e.g. 'tomograms.star' or 'warp_tiltseries/'."
    )
    is_dir: bool = Field(default=False, description="If true, output is a directory-like artifact")
    description: str = Field(default="")

    @field_validator("key")
    @classmethod
    def _key_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("OutputSlot.key must be non-empty")
        return v


class InputSlot(BaseModel):
    """
    Declares: "this job needs something compatible with types in accepts[]".

    preferred_source is not used in Stage 0, but we add it now because
    it’s central to your denoise-vs-reconstruct case.
    """
    model_config = ConfigDict(validate_assignment=True)

    key: str = Field(..., description="Logical input name used in job_model.paths")
    accepts: List[JobFileType] = Field(..., min_length=1)
    required: bool = Field(default=True)

    # Preference knobs (used by resolver later)
    preferred_source: Optional[str] = Field(
        default=None,
        description="JobType string (or later: job instance id) preferred as source if available"
    )

    allow_multiple: bool = Field(
        default=False,
        description="If true, resolver may provide a list of paths rather than a single path"
    )

    description: str = Field(default="")

    @field_validator("key")
    @classmethod
    def _key_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("InputSlot.key must be non-empty")
        return v


# -----------------------------------------------------------------------------
# Resolution record types (still just data in Stage 0)
# -----------------------------------------------------------------------------

class ResolvedInput(BaseModel):
    """
    Result of wiring one InputSlot to one upstream OutputSlot.
    """
    model_config = ConfigDict(validate_assignment=True)

    input_key: str
    chosen_type: JobFileType

    # Identity of the producing job
    source_job_type: str
    source_instance_id: Optional[str] = None  # future-proof

    # Identity of what output we used
    source_output_key: str

    # Resolved path value (string to match your job_model.paths storage)
    path: str


class ResolvedOutput(BaseModel):
    """
    Concrete output path for an OutputSlot.
    """
    model_config = ConfigDict(validate_assignment=True)

    output_key: str
    produces: JobFileType
    path: str


class ResolvedManifest(BaseModel):
    """
    The final product of add-time resolution:
      - which inputs were satisfied by which upstream outputs
      - and what this job's outputs will be
    """
    model_config = ConfigDict(validate_assignment=True)

    job_type: str
    instance_id: Optional[str] = None

    inputs : List[ResolvedInput]  = Field(default_factory=list)
    outputs: List[ResolvedOutput] = Field(default_factory=list)

    def as_paths_dict(self) -> Dict[str, Any]:
        """
        Convert to the eventual job_model.paths payload.

        - inputs become {input_key: path} (or list of paths if allow_multiple later)
        - outputs become {output_key: path}
        """
        d: Dict[str, Any] = {}
        for ri in self.inputs:
            # In Stage 0 we only store single paths; allow_multiple comes later.
            d[ri.input_key] = ri.path
        for ro in self.outputs:
            d[ro.output_key] = ro.path
        return d


# -----------------------------------------------------------------------------
# Lightweight schema validation helpers (optional but useful even in Stage 0)
# -----------------------------------------------------------------------------

def validate_schema_uniqueness(
    input_schema: Sequence[InputSlot],
    output_schema: Sequence[OutputSlot],
) -> Tuple[bool, List[str]]:
    """
    Pure helper you can use in tests to catch typos early.
    """
    errors: List[str] = []

    in_keys = [s.key for s in input_schema]
    out_keys = [s.key for s in output_schema]

    if len(in_keys) != len(set(in_keys)):
        errors.append(f"Duplicate InputSlot.key found: {in_keys}")

    if len(out_keys) != len(set(out_keys)):
        errors.append(f"Duplicate OutputSlot.key found: {out_keys}")

    overlap = set(in_keys) & set(out_keys)
    if overlap:
        errors.append(f"InputSlot.key overlaps OutputSlot.key: {sorted(overlap)}")

    return (len(errors) == 0), errors
