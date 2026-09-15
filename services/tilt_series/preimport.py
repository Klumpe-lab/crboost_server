# services/tilt_series/preimport.py
"""
Pre-import representation of a cryo-ET dataset: what the mdoc/frame parser sees
BEFORE a project exists and before registry ingest.

These models represent the hierarchical naming convention from the microscope:
  Position_{stage}_{tilt_idx}_{angle}_{timestamp}_EER.eer       (beam 1, implicit)
  Position_{stage}_{beam}_{tilt_idx}_{angle}_{timestamp}_EER.eer (beam 2+)
  Position_{stage}.mdoc       (beam 1)
  Position_{stage}_{beam}.mdoc (beam 2+)

They are transient parse results (never persisted; the selection cache stores only
{mdoc_filename: bool}). The registry entities in `services.tilt_series.models`
(`TiltSeries`/`Frame`) are the post-import single source of truth; the conversion
from this representation lives in `services.tilt_series.build`
(`build_from_dataset_overview` / `_build_one_ts`, mapping `ts_label` -> `TiltSeries.id`
and `TiltInfo` -> `Frame`). This module must stay import-light (pydantic + the mdoc
facts dataclasses) — `build.py` imports it, and parser services import `build`, so
importing either from here would create a cycle.
"""

from __future__ import annotations

from pathlib import Path
from statistics import median
from typing import Literal

from pydantic import BaseModel, Field

from services.configs.mdoc_service import DoseEstimate

SourceKind = Literal["movies", "stack", "missing"]


class TiltInfo(BaseModel):
    """Single tilt within a tilt-series."""

    z_value: int
    tilt_angle: float
    frame_filename: str
    frame_path: Path | None = None
    mdoc_stats: dict[str, float] = Field(default_factory=dict)
    date_time: str | None = None  # raw mdoc DateTime string (mdoc_stats is float-only)


class TiltSeriesInfo(BaseModel):
    """One tilt-series = one mdoc file. `ts_name` is the mdoc name minus `.mdoc` (and a
    trailing `.mrc`/`.st`); stage/beam are decoration when the name carries the
    `Position_{stage}[_{beam}]` suffix, None otherwise (SerialEM names)."""

    ts_name: str
    stage_position: int | None = None
    beam_position: int | None = None  # 1 for implicit (Position_X.mdoc), 2+ for explicit
    mdoc_filename: str
    mdoc_path: Path
    tilts: list[TiltInfo] = Field(default_factory=list)
    selected: bool = True

    # Which layer of the delivery the scan chose for this series (roadmap 18 D1):
    # "movies" = one file per SubFramePath resolved; "stack" = <mdoc dir>/<ImageFile>
    # with nz == sections, split at Create; "missing" = neither.
    source_kind: SourceKind = "missing"
    stack_path: Path | None = None
    stack_nz: int | None = None

    # Per-mdoc acquisition parameters (extracted from mdoc header / first ZValue)
    pixel_size: float | None = None  # angstrom
    voltage: float | None = None  # kV
    dose_per_tilt: float | None = None  # e-/A^2 — None when the mdoc has no (non-zero) ExposureDose
    dose_estimate: DoseEstimate | None = None  # only when dose_per_tilt is None
    tilt_axis: float | None = None  # degrees
    acquisition_software: str = ""  # "SerialEM" / "Tomo5" / ""
    software_version: str = ""
    detector_dimensions: tuple[int, int] | None = None

    @property
    def tilt_count(self) -> int:
        return len(self.tilts)

    @property
    def angle_range(self) -> tuple[float, float]:
        if not self.tilts:
            return (0.0, 0.0)
        angles = [t.tilt_angle for t in self.tilts]
        return (min(angles), max(angles))

    @property
    def missing_frames(self) -> int:
        """Unresolved movie files. A stack series has no per-tilt files until Create
        splits it, so it never counts as missing here (`source_kind` says so)."""
        if self.source_kind != "movies":
            return 0
        return sum(1 for t in self.tilts if t.frame_path is None)

    @property
    def ts_label(self) -> str:
        return self.ts_name


class StagePositionInfo(BaseModel):
    """Grouping of tilt-series at one stage position; `stage_position` is None for the
    single flat group of a dataset whose names carry no Position suffix."""

    stage_position: int | None
    tilt_series: list[TiltSeriesInfo] = Field(default_factory=list)
    selected: bool = True

    @property
    def beam_count(self) -> int:
        return len(self.tilt_series)

    @property
    def total_tilts(self) -> int:
        return sum(ts.tilt_count for ts in self.tilt_series)


class AcquisitionSummary(BaseModel):
    """Summary of unique acquisition parameters across all tilt-series."""

    pixel_sizes: list[float] = Field(default_factory=list)
    voltages: list[float] = Field(default_factory=list)
    doses: list[float] = Field(default_factory=list)
    tilt_axes: list[float] = Field(default_factory=list)
    tilt_counts: list[int] = Field(default_factory=list)
    angle_ranges: list[tuple[float, float]] = Field(default_factory=list)
    dose_missing: int = 0  # tilt-series whose mdoc records no dose per tilt
    dose_estimates: list[float] = Field(default_factory=list)  # per-series estimate (incident, else transmitted)

    @classmethod
    def from_tilt_series(cls, tilt_series: list[TiltSeriesInfo]) -> AcquisitionSummary:
        """Unique acquisition values across `tilt_series`."""
        pxs: set[float] = set()
        vs: set[float] = set()
        ds: set[float] = set()
        tas: set[float] = set()
        tcs: set[int] = set()
        ars: set[tuple[float, float]] = set()
        dose_missing = 0
        estimates: list[float] = []
        for ts in tilt_series:
            if ts.pixel_size is not None:
                pxs.add(round(ts.pixel_size, 3))
            if ts.voltage is not None:
                vs.add(round(ts.voltage, 0))
            if ts.dose_per_tilt is not None:
                ds.add(round(ts.dose_per_tilt, 1))
            else:
                dose_missing += 1
                if ts.dose_estimate is not None:
                    estimates.append(ts.dose_estimate.value)
            if ts.tilt_axis is not None:
                tas.add(round(ts.tilt_axis, 1))
            tcs.add(ts.tilt_count)
            lo, hi = ts.angle_range
            ars.add((round(lo, 0), round(hi, 0)))
        return cls(
            pixel_sizes=sorted(pxs),
            voltages=sorted(vs),
            doses=sorted(ds),
            tilt_axes=sorted(tas),
            tilt_counts=sorted(tcs),
            angle_ranges=sorted(ars),
            dose_missing=dose_missing,
            dose_estimates=estimates,
        )

    @property
    def is_consistent(self) -> bool:
        return (
            len(self.pixel_sizes) <= 1
            and len(self.voltages) <= 1
            and len(self.doses) <= 1
            and len(self.tilt_axes) <= 1
            and len(self.tilt_counts) <= 1
        )

    def param_warnings(self) -> list[tuple[str, str, str]]:
        """Returns list of (param_key, label, detail) for inconsistent params."""
        w: list[tuple[str, str, str]] = []
        if len(self.pixel_sizes) > 1:
            vals = ", ".join(f"{v:.3f}" for v in self.pixel_sizes)
            w.append(("pixel_size", "Mixed pixel sizes", f"{vals} Å"))
        if len(self.voltages) > 1:
            vals = ", ".join(f"{v:.0f}" for v in self.voltages)
            w.append(("voltage", "Mixed voltages", f"{vals} kV"))
        if len(self.doses) > 1:
            vals = ", ".join(f"{v:.1f}" for v in self.doses)
            w.append(("dose_per_tilt", "Mixed dose/tilt", f"{vals} e⁻/Å²"))
        if len(self.tilt_axes) > 1:
            vals = ", ".join(f"{v:.1f}" for v in self.tilt_axes)
            w.append(("tilt_axis", "Mixed tilt axes", f"{vals}°"))
        if len(self.angle_ranges) > 1:
            vals = ", ".join(f"[{lo:+.0f}°..{hi:+.0f}°]" for lo, hi in self.angle_ranges)
            w.append(("angle_range", "Mixed angle ranges", vals))
        return w


class DatasetOverview(BaseModel):
    """Complete parsed dataset structure."""

    source_directory: str
    frame_extension: str = ""
    positions: list[StagePositionInfo] = Field(default_factory=list)
    parse_warnings: list[str] = Field(default_factory=list)
    acquisition_summary: AcquisitionSummary = Field(default_factory=AcquisitionSummary)
    # Dataset-wide facts read from the mdoc headers (first series that states them).
    acquisition_software: str = ""
    software_version: str = ""
    detector_dimensions: tuple[int, int] | None = None

    @property
    def total_tilt_series(self) -> int:
        return sum(p.beam_count for p in self.positions)

    def source_kinds(self) -> dict[str, int]:
        """Selected tilt-series per source layer, e.g. {"stack": 4}."""
        counts: dict[str, int] = {}
        for ts in self.get_selected_tilt_series():
            counts[ts.source_kind] = counts.get(ts.source_kind, 0) + 1
        return counts

    @property
    def unresolved_selected(self) -> int:
        """Selected tilt-series with neither movies nor a stack on disk."""
        return sum(1 for ts in self.get_selected_tilt_series() if ts.source_kind == "missing")

    def dose_estimate(self) -> float | None:
        """Median of the selected series' estimates (incident where the fit held, else
        transmitted); None when no series could be estimated or none needs it."""
        values = self.selected_acquisition_summary().dose_estimates
        return round(median(values), 2) if values else None

    def dose_estimate_detail(self) -> str:
        """The fit behind `dose_estimate()`, from the first selected series that has one."""
        for ts in self.get_selected_tilt_series():
            if ts.dose_per_tilt is None and ts.dose_estimate is not None:
                return ts.dose_estimate.describe()
        return ""

    def source_line(self) -> str:
        """One line naming what the scan concluded, e.g.
        `SerialEM 4.1.10 · aligned stacks (.mrc, 39 slices/series; raw movies left alone)
        · 5760×4092 · dose: estimated 4.3 e⁻/Å² (not in mdoc)`."""
        parts: list[str] = []
        software = f"{self.acquisition_software} {self.software_version}".strip()
        if software:
            parts.append(software)
        kinds = self.source_kinds()
        selected = self.get_selected_tilt_series()
        counts = sorted({ts.tilt_count for ts in selected})
        slices = f"{counts[0]}" if len(counts) == 1 else f"{counts[0]}–{counts[-1]}" if counts else "?"
        if kinds.get("stack"):
            movies_too = any(t.frame_path is not None for ts in selected if ts.source_kind == "stack" for t in ts.tilts)
            note = "; raw movies left alone" if movies_too else ""
            parts.append(f"aligned stacks (.mrc, {slices} slices/series{note})")
        if kinds.get("movies"):
            ext = self.frame_extension or "?"
            parts.append(f"movies ({ext}, {slices} tilts/series)")
        if kinds.get("missing"):
            parts.append(f"{kinds['missing']} series without movies or stack")
        if self.detector_dimensions:
            parts.append(f"{self.detector_dimensions[0]}×{self.detector_dimensions[1]}")
        summary = self.selected_acquisition_summary()
        if summary.dose_missing:
            est = self.dose_estimate()
            parts.append(f"dose: estimated {est:.1f} e⁻/Å² (not in mdoc)" if est is not None else "dose: not in mdoc")
        elif summary.doses:
            parts.append("dose: " + ", ".join(f"{d:.1f}" for d in summary.doses) + " e⁻/Å² (mdoc)")
        return " · ".join(parts)

    @property
    def selected_tilt_series(self) -> int:
        return sum(1 for p in self.positions for ts in p.tilt_series if ts.selected)

    @property
    def total_frames(self) -> int:
        return sum(p.total_tilts for p in self.positions)

    @property
    def selected_frames(self) -> int:
        return sum(ts.tilt_count for p in self.positions for ts in p.tilt_series if ts.selected)

    def get_selected_tilt_series(self) -> list[TiltSeriesInfo]:
        return [ts for p in self.positions for ts in p.tilt_series if ts.selected]

    def selected_acquisition_summary(self) -> AcquisitionSummary:
        """Compute summary only from selected tilt-series."""
        return AcquisitionSummary.from_tilt_series(self.get_selected_tilt_series())
