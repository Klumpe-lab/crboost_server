"""First-class Pydantic models for TiltSeries / Frame / Tomogram.

Identity rules (enforced by callers, documented here):

- A `TiltSeries.id` is assigned once, at import, from the MDOC filename stem
  (e.g. mdoc = "proj_Position_10.mdoc" → id = "proj_Position_10"). It matches
  what WarpTools writes as the per-TS XML filename and tomostar filename.

- A `Frame.id` is assigned once, at import, from the raw frame filename stem,
  using `Path(raw_frame_filename).stem` (strips only the last extension). For
  `Position_10_001_12.00_20260204_171524_EER.eer` this is
  `Position_10_001_12.00_20260204_171524_EER`.

  This matches what `Path(rlnMicrographMovieName).stem` yields in RELION STARs,
  so the registry can be cross-referenced with on-disk STARs during migration.

- `Frame.tilt_index` is the 0-based index into the TS's acquisition order (Z).
  It matches `<Node Z=...>` in WarpTools XMLs.

Outputs are per-job-type discriminated unions, keyed in each entity's
`outputs` dict by the job instance_id (not JobType) so species-scoped or
multiple-pass job runs can coexist.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


# ─────────────────────────────────────────────────────────────────────────────
# Per-job output types
# ─────────────────────────────────────────────────────────────────────────────


class _OutputBase(BaseModel):
    """Common fields for all per-job outputs."""

    model_config = ConfigDict(extra="forbid")
    job_instance_id: str
    job_dir: Path
    attached_at: datetime = Field(default_factory=datetime.now)


# ── Frame outputs ────────────────────────────────────────────────────────────


class FsMotionCtfFrameOutput(_OutputBase):
    """Result of fs_motion_and_ctf for a single frame (per-movie Warp XML)."""

    output_type: Literal["fs_motion_ctf"] = "fs_motion_ctf"

    averaged_mrc: Path
    even_mrc: Path
    odd_mrc: Path
    ctf_image: Path

    defocus_u_angstrom: float
    defocus_v_angstrom: float
    defocus_angle: float
    ctf_astigmatism: float

    warp_xml_path: Path


FrameOutput = Annotated[Union[FsMotionCtfFrameOutput], Field(discriminator="output_type")]


# ── TiltSeries outputs ───────────────────────────────────────────────────────


class TsCtfPerFrameCtf(BaseModel):
    """Per-frame CTF result extracted from a per-TS WarpTools XML's <GridCTF>."""

    model_config = ConfigDict(extra="forbid")
    frame_id: str
    z_index: int
    defocus_u_angstrom: float
    defocus_v_angstrom: float
    defocus_angle: float
    ctf_astigmatism: float


class TsCtfTiltSeriesOutput(_OutputBase):
    """Result of ts_ctf for one tilt-series."""

    output_type: Literal["ts_ctf"] = "ts_ctf"

    warp_xml_path: Path
    are_angles_inverted: bool  # maps to rlnTomoHand = -1 (True) or +1 (False)
    per_frame: List[TsCtfPerFrameCtf] = Field(default_factory=list)


class TsAlignmentPerFrame(BaseModel):
    """Per-frame alignment result (shifts + rotation) from AreTomo/IMOD."""

    model_config = ConfigDict(extra="forbid")
    frame_id: str
    z_index: int
    tilt_x_deg: float
    tilt_y_deg: float  # refined tilt angle (from alignment)
    z_rot_deg: float
    x_shift_angstrom: float
    y_shift_angstrom: float


class TsAlignmentTiltSeriesOutput(_OutputBase):
    """Result of ts_alignment for one tilt-series."""

    output_type: Literal["ts_alignment"] = "ts_alignment"

    alignment_method: Literal["aretomo", "imod"]
    alignment_angpix: float
    aln_file: Optional[Path] = None  # AreTomo .st.aln
    xf_file: Optional[Path] = None   # IMOD .xf
    tlt_file: Optional[Path] = None  # IMOD .tlt
    per_frame: List[TsAlignmentPerFrame] = Field(default_factory=list)


TiltSeriesOutput = Annotated[
    Union[TsCtfTiltSeriesOutput, TsAlignmentTiltSeriesOutput],
    Field(discriminator="output_type"),
]


# ── Tomogram outputs ─────────────────────────────────────────────────────────


class TsReconstructTomogramOutput(_OutputBase):
    """Result of ts_reconstruct for one tomogram."""

    output_type: Literal["ts_reconstruct"] = "ts_reconstruct"

    reconstructed_mrc: Path
    half1_mrc: Path
    half2_mrc: Path
    binning: float
    tomogram_pixel_size_angstrom: float
    size_x: int
    size_y: int
    size_z: int


TomogramOutput = Annotated[Union[TsReconstructTomogramOutput], Field(discriminator="output_type")]


# ─────────────────────────────────────────────────────────────────────────────
# Core entities
# ─────────────────────────────────────────────────────────────────────────────


class Frame(BaseModel):
    """One tilt frame = one raw movie file = one row in a per-TS tilt STAR.

    Assigned at import. Identity (`id`, `tilt_series_id`, `tilt_index`) is
    immutable after construction. Only `outputs` grow over time.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    tilt_series_id: str
    raw_path: Path
    raw_filename: str  # cached basename for O(1) lookups

    tilt_index: int                # 0-based, matches Warp's <Node Z=...>
    nominal_tilt_angle_deg: float
    pre_exposure_e_per_a2: float = 0.0
    acquisition_time: Optional[datetime] = None

    # Per-job artifacts, keyed by job instance_id
    outputs: Dict[str, FrameOutput] = Field(default_factory=dict)


class Tomogram(BaseModel):
    """1:1 with TiltSeries in v1. Separate entity so multi-binning or alt
    reconstruction paths can be modeled without schema churn later.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    tilt_series_id: str
    outputs: Dict[str, TomogramOutput] = Field(default_factory=dict)


class TiltSeries(BaseModel):
    """A tilt-series is the unit of parallelization in the post-refactor pipeline.

    A TS is the scope addressed by one SLURM array task, one WarpTools per-TS
    XML, one tomostar file, and one output tomogram.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    mdoc_path: Path
    mdoc_filename: str

    stage_position: int
    beam_position: int

    frames: List[Frame] = Field(default_factory=list)
    tomogram: Optional[Tomogram] = None

    # Per-job artifacts at the TS scope, keyed by job instance_id
    outputs: Dict[str, TiltSeriesOutput] = Field(default_factory=dict)

    # Provenance / filter state
    is_selected: bool = True
    is_filtered_out: bool = False
    filter_reason: Optional[str] = None

    # ── derived views ──────────────────────────────────────────────────────

    @property
    def frame_count(self) -> int:
        return len(self.frames)

    def frame_by_index(self, z: int) -> Frame:
        if z < 0 or z >= len(self.frames):
            raise KeyError(f"z={z} out of range for TS {self.id} with {len(self.frames)} frames")
        return self.frames[z]

    def frame_by_id(self, frame_id: str) -> Frame:
        for f in self.frames:
            if f.id == frame_id:
                return f
        raise KeyError(f"Frame {frame_id!r} not found in TS {self.id}")

    def frame_by_filename(self, name: str) -> Frame:
        """Look up by raw filename (basename, with or without directory).
        Matches either the full filename or its stem."""
        target = Path(name).name
        target_stem = Path(target).stem
        for f in self.frames:
            if f.raw_filename == target or f.id == target_stem:
                return f
        raise KeyError(f"Frame {name!r} not found in TS {self.id}")
