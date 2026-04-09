# services/dataset_models.py
"""
Pydantic models for parsed cryo-ET dataset structure.

Represents the hierarchical naming convention from the microscope:
  Position_{stage}_{tilt_idx}_{angle}_{timestamp}_EER.eer       (beam 1, implicit)
  Position_{stage}_{beam}_{tilt_idx}_{angle}_{timestamp}_EER.eer (beam 2+)
  Position_{stage}.mdoc       (beam 1)
  Position_{stage}_{beam}.mdoc (beam 2+)
"""

from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field


class TiltInfo(BaseModel):
    """Single tilt within a tilt-series."""

    z_value: int
    tilt_angle: float
    frame_filename: str
    frame_path: Optional[Path] = None
    mdoc_stats: Dict[str, float] = Field(default_factory=dict)


class TiltSeriesInfo(BaseModel):
    """One tilt-series = one mdoc file = one beam position at one stage position."""

    stage_position: int
    beam_position: int  # 1 for implicit (Position_X.mdoc), 2+ for explicit
    mdoc_filename: str
    mdoc_path: Path
    tilts: List[TiltInfo] = Field(default_factory=list)
    selected: bool = True

    # Per-mdoc acquisition parameters (extracted from mdoc header / first ZValue)
    pixel_size: Optional[float] = None  # angstrom
    voltage: Optional[float] = None  # kV
    dose_per_tilt: Optional[float] = None  # e-/A^2
    tilt_axis: Optional[float] = None  # degrees

    @property
    def tilt_count(self) -> int:
        return len(self.tilts)

    @property
    def angle_range(self) -> Tuple[float, float]:
        if not self.tilts:
            return (0.0, 0.0)
        angles = [t.tilt_angle for t in self.tilts]
        return (min(angles), max(angles))

    @property
    def missing_frames(self) -> int:
        return sum(1 for t in self.tilts if t.frame_path is None)

    @property
    def ts_label(self) -> str:
        if self.beam_position == 1:
            return f"Position_{self.stage_position}"
        return f"Position_{self.stage_position}_{self.beam_position}"


class StagePositionInfo(BaseModel):
    """Grouping of tilt-series at one stage position."""

    stage_position: int
    tilt_series: List[TiltSeriesInfo] = Field(default_factory=list)
    selected: bool = True

    @property
    def beam_count(self) -> int:
        return len(self.tilt_series)

    @property
    def total_tilts(self) -> int:
        return sum(ts.tilt_count for ts in self.tilt_series)


class AcquisitionSummary(BaseModel):
    """Summary of unique acquisition parameters across all tilt-series."""

    pixel_sizes: List[float] = Field(default_factory=list)
    voltages: List[float] = Field(default_factory=list)
    doses: List[float] = Field(default_factory=list)
    tilt_axes: List[float] = Field(default_factory=list)
    tilt_counts: List[int] = Field(default_factory=list)
    angle_ranges: List[Tuple[float, float]] = Field(default_factory=list)

    @property
    def is_consistent(self) -> bool:
        return (
            len(self.pixel_sizes) <= 1
            and len(self.voltages) <= 1
            and len(self.doses) <= 1
            and len(self.tilt_axes) <= 1
            and len(self.tilt_counts) <= 1
        )

    def param_warnings(self) -> List[Tuple[str, str, str]]:
        """Returns list of (param_key, label, detail) for inconsistent params."""
        w: List[Tuple[str, str, str]] = []
        if len(self.pixel_sizes) > 1:
            vals = ", ".join(f"{v:.3f}" for v in self.pixel_sizes)
            w.append(("pixel_size", "Mixed pixel sizes", f"{vals} \u212b"))
        if len(self.voltages) > 1:
            vals = ", ".join(f"{v:.0f}" for v in self.voltages)
            w.append(("voltage", "Mixed voltages", f"{vals} kV"))
        if len(self.doses) > 1:
            vals = ", ".join(f"{v:.1f}" for v in self.doses)
            w.append(("dose_per_tilt", "Mixed dose/tilt", f"{vals} e\u207b/\u212b\u00b2"))
        if len(self.tilt_axes) > 1:
            vals = ", ".join(f"{v:.1f}" for v in self.tilt_axes)
            w.append(("tilt_axis", "Mixed tilt axes", f"{vals}\u00b0"))
        if len(self.angle_ranges) > 1:
            vals = ", ".join(f"[{lo:+.0f}\u00b0..{hi:+.0f}\u00b0]" for lo, hi in self.angle_ranges)
            w.append(("angle_range", "Mixed angle ranges", vals))
        return w


class DatasetOverview(BaseModel):
    """Complete parsed dataset structure."""

    source_directory: str
    frame_extension: str = ""
    positions: List[StagePositionInfo] = Field(default_factory=list)
    parse_warnings: List[str] = Field(default_factory=list)
    acquisition_summary: AcquisitionSummary = Field(default_factory=AcquisitionSummary)

    @property
    def total_tilt_series(self) -> int:
        return sum(p.beam_count for p in self.positions)

    @property
    def selected_tilt_series(self) -> int:
        return sum(1 for p in self.positions for ts in p.tilt_series if ts.selected)

    @property
    def total_frames(self) -> int:
        return sum(p.total_tilts for p in self.positions)

    @property
    def selected_frames(self) -> int:
        return sum(ts.tilt_count for p in self.positions for ts in p.tilt_series if ts.selected)

    def get_selected_tilt_series(self) -> List[TiltSeriesInfo]:
        return [ts for p in self.positions for ts in p.tilt_series if ts.selected]

    def selected_acquisition_summary(self) -> AcquisitionSummary:
        """Compute summary only from selected tilt-series."""
        pxs: Set[float] = set()
        vs: Set[float] = set()
        ds: Set[float] = set()
        tas: Set[float] = set()
        tcs: Set[int] = set()
        ars: Set[Tuple[float, float]] = set()
        for ts in self.get_selected_tilt_series():
            if ts.pixel_size is not None:
                pxs.add(round(ts.pixel_size, 3))
            if ts.voltage is not None:
                vs.add(round(ts.voltage, 0))
            if ts.dose_per_tilt is not None:
                ds.add(round(ts.dose_per_tilt, 1))
            if ts.tilt_axis is not None:
                tas.add(round(ts.tilt_axis, 1))
            tcs.add(ts.tilt_count)
            lo, hi = ts.angle_range
            ars.add((round(lo, 0), round(hi, 0)))
        return AcquisitionSummary(
            pixel_sizes=sorted(pxs),
            voltages=sorted(vs),
            doses=sorted(ds),
            tilt_axes=sorted(tas),
            tilt_counts=sorted(tcs),
            angle_ranges=sorted(ars),
        )
