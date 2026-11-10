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
{mdoc path: bool}). The registry entities in `services.tilt_series.models`
(`TiltSeries`/`Frame`) are the post-import single source of truth; the conversion
from this representation lives in `services.tilt_series.build`
(`build_from_dataset_overview` / `_build_one_ts`, mapping `ts_label` -> `TiltSeries.id`
and `TiltInfo` -> `Frame`). This module must stay import-light (pydantic + the mdoc
facts dataclasses) — `build.py` imports it, and parser services import `build`, so
importing either from here would create a cycle.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Literal

from pydantic import BaseModel, Field

from services.configs.mdoc_service import DoseEstimate, duplicate_ts_names
from services.stack_import import frames_are_mirrored, frames_tilt_axis

SourceKind = Literal["movies", "stack", "missing"]
Layer = Literal["movies", "stack"]


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

    # Which layer of the delivery this series is imported from: "movies" = one file per
    # SubFramePath; "stack" = the raw SerialEM stack (`stack_candidates`), split at Create;
    # "missing" = the project's layer is not there for this series.
    source_kind: SourceKind = "missing"
    # What the series has on disk, whatever is chosen: the stack (when its nz matches the
    # sections) and whether every movie resolved.
    stack_path: Path | None = None
    stack_nz: int | None = None
    movies_complete: bool = False
    # SerialEM's per-movie mdoc of the first movie: gain reference (resolved path, or the
    # name when not found), RotationAndFlip of the saved frames, Binning (0.5 = super-res).
    gain_reference_name: str = ""
    gain_reference: Path | None = None
    frames_rotation_flip: int | None = None
    frames_binning: float | None = None
    # PACEtomo's `<area>_ts_<NNN>`: the navigator area (one stage position) and the target
    # (one beam-shift position around it). Labels only; Tomo5 positions stay the ints above.
    area_label: str | None = None
    target_label: str | None = None

    # Per-mdoc acquisition parameters (extracted from mdoc header / first ZValue)
    pixel_size: float | None = None  # angstrom
    voltage: float | None = None  # kV
    dose_per_tilt: float | None = None  # e-/A^2 — None when the mdoc has no (non-zero) ExposureDose
    dose_estimate: DoseEstimate | None = None  # only when dose_per_tilt is None
    tilt_axis: float | None = None  # degrees, in the orientation of the chosen layer
    mdoc_tilt_axis: float | None = None  # as the mdoc states it (SerialEM: the stack's orientation)
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
    label: str | None = None  # PACEtomo's navigator area when the group is one

    @property
    def beam_count(self) -> int:
        return len(self.tilt_series)

    @property
    def total_tilts(self) -> int:
        return sum(ts.tilt_count for ts in self.tilt_series)


def aggregate_to_positions(tilt_series_list: list[TiltSeriesInfo]) -> list[StagePositionInfo]:
    """Group tilt-series by stage position when every series has one (Tomo5 names), by
    PACEtomo area when every series has one (`<area>_ts_<NNN>`); otherwise a single flat
    group, `stage_position=None`, sorted by name."""
    if not all(ts.stage_position is not None for ts in tilt_series_list):
        if tilt_series_list and all(ts.area_label for ts in tilt_series_list):
            areas: dict[str, list[TiltSeriesInfo]] = defaultdict(list)
            for ts in tilt_series_list:
                areas[ts.area_label].append(ts)
            return [
                StagePositionInfo(
                    stage_position=None, label=area, tilt_series=sorted(areas[area], key=lambda ts: ts.ts_name)
                )
                for area in sorted(areas)
            ]
        flat = sorted(tilt_series_list, key=lambda ts: ts.ts_name)
        return [StagePositionInfo(stage_position=None, tilt_series=flat)] if flat else []

    groups: dict[int, list[TiltSeriesInfo]] = defaultdict(list)
    for ts in tilt_series_list:
        groups[ts.stage_position].append(ts)

    positions = []
    for stage_pos in sorted(groups.keys()):
        series = sorted(groups[stage_pos], key=lambda ts: ts.beam_position or 1)
        positions.append(StagePositionInfo(stage_position=stage_pos, tilt_series=series))
    return positions


class AcquisitionSummary(BaseModel):
    """Summary of unique acquisition parameters across all tilt-series."""

    pixel_sizes: list[float] = Field(default_factory=list)
    voltages: list[float] = Field(default_factory=list)
    doses: list[float] = Field(default_factory=list)
    tilt_axes: list[float] = Field(default_factory=list)
    tilt_counts: list[int] = Field(default_factory=list)
    angle_ranges: list[tuple[float, float]] = Field(default_factory=list)
    dose_missing: int = 0  # tilt-series whose mdoc records no dose per tilt
    dose_estimates: list[float] = Field(default_factory=list)  # per-series incident dose, where the fit held
    dose_transmitted_only: list[float] = Field(default_factory=list)  # transmitted dose of series without a fit

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
        transmitted_only: list[float] = []
        for ts in tilt_series:
            if ts.pixel_size is not None:
                pxs.add(round(ts.pixel_size, 3))
            if ts.voltage is not None:
                vs.add(round(ts.voltage, 0))
            if ts.dose_per_tilt is not None:
                ds.add(round(ts.dose_per_tilt, 1))
            else:
                dose_missing += 1
                if ts.dose_estimate is not None and ts.dose_estimate.incident is not None:
                    estimates.append(ts.dose_estimate.incident)
                elif ts.dose_estimate is not None:
                    transmitted_only.append(ts.dose_estimate.transmitted_low_tilt)
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
            dose_transmitted_only=transmitted_only,
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
    # The layer the whole project imports from, the user's pick (`apply_layer`); None while
    # no layer covers every selected series.
    layer: Layer | None = None
    ignored_mdocs: dict[str, int] = Field(default_factory=dict)  # `classify_mdoc` kind → count
    # Resolved gain references that hold different images (the scan compares them by
    # content); a frames project needs exactly one image.
    gain_references_differ: bool = False

    def layer_coverage(self) -> dict[str, int]:
        """Selected series each layer can serve: `{"stack": n, "movies": n}`."""
        sel = self.get_selected_tilt_series()
        return {
            "stack": sum(1 for ts in sel if ts.stack_path is not None),
            "movies": sum(1 for ts in sel if ts.movies_complete),
        }

    def default_layer(self) -> Layer | None:
        """The layer every selected series has; when both do, SerialEM's stack (its
        frame-aligned images) and Tomo5's movies; None when neither covers all."""
        n = len(self.get_selected_tilt_series())
        cov = self.layer_coverage()
        full = [k for k in ("stack", "movies") if n and cov[k] == n]
        if len(full) == 2:
            return "stack" if self.acquisition_software == "SerialEM" else "movies"
        return full[0] if full else None

    def apply_layer(self, layer: Layer | None) -> None:
        """Make `layer` the project's: every series' `source_kind` (the layer or "missing"),
        its tilt axis (the frames' orientation for movies, `frames_tilt_axis`; None when it
        cannot be derived) and the frame extension."""

        self.layer = layer
        exts: list[str] = []
        for p in self.positions:
            for ts in p.tilt_series:
                if layer == "stack":
                    ts.source_kind = "stack" if ts.stack_path is not None else "missing"
                    ts.tilt_axis = ts.mdoc_tilt_axis
                elif layer == "movies":
                    ts.source_kind = "movies" if ts.movies_complete else "missing"
                    rf = ts.frames_rotation_flip
                    if ts.mdoc_tilt_axis is None or rf is None or ts.acquisition_software != "SerialEM":
                        ts.tilt_axis = ts.mdoc_tilt_axis
                    else:
                        ts.tilt_axis = frames_tilt_axis(ts.mdoc_tilt_axis, rf)
                    first = next((t.frame_path for t in ts.tilts if t.frame_path is not None), None)
                    if first is not None and ts.selected:
                        exts.append(first.suffix.lower())
                else:
                    ts.source_kind = "missing"
                    ts.tilt_axis = ts.mdoc_tilt_axis
        if layer == "stack":
            self.frame_extension = ".mrc"
        elif layer == "movies" and exts:
            self.frame_extension = max(set(exts), key=exts.count)
        self.acquisition_summary = AcquisitionSummary.from_tilt_series(
            [ts for p in self.positions for ts in p.tilt_series]
        )

    def frames_gain_references(self) -> dict[str, list[str]]:
        """Gain reference of the selected series' frames → the series that name it (the
        resolved path, or `missing: <name>` when SerialEM named a file that is not there)."""
        out: dict[str, list[str]] = {}
        for ts in self.get_selected_tilt_series():
            if not ts.gain_reference_name:
                continue
            key = str(ts.gain_reference) if ts.gain_reference else f"missing: {ts.gain_reference_name}"
            out.setdefault(key, []).append(ts.ts_name)
        return out

    def frames_gain(self) -> str | None:
        """The one gain reference the selected series' frames need: the first resolved path
        when every series names one and they hold the same image; None otherwise."""
        refs = self.frames_gain_references()
        if not refs or any(k.startswith("missing: ") for k in refs) or self.gain_references_differ:
            return None
        named = sum(len(v) for v in refs.values())
        if named != len(self.get_selected_tilt_series()):
            return None
        return sorted(refs)[0]

    def frames_mirrored(self) -> bool | None:
        """Whether the selected series' frames are a mirror of SerialEM's images (operations
        4–7); None when their frame mdocs do not say, or disagree."""

        ops = {ts.frames_rotation_flip for ts in self.get_selected_tilt_series()}
        if len(ops) != 1 or None in ops:
            return None
        return frames_are_mirrored(ops.pop())

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
        """Median incident dose of the selected series whose zero-thickness fit held; the
        median transmitted dose only when no series fitted (it is not what the specimen
        received). None when no series could be estimated or none needs it."""
        s = self.selected_acquisition_summary()
        values = s.dose_estimates or s.dose_transmitted_only
        return round(median(values), 2) if values else None

    def dose_estimate_detail(self) -> str:
        """How `dose_estimate()` came about, e.g. `incident fit in 17/23 series (4.2–4.6);
        6 without a fit (transmitted 1.0–1.5), assumed the same beam`."""
        s = self.selected_acquisition_summary()
        n = len(s.dose_estimates) + len(s.dose_transmitted_only)
        if not n:
            return ""
        if not s.dose_estimates:
            lo, hi = min(s.dose_transmitted_only), max(s.dose_transmitted_only)
            return f"no series fitted; transmitted {lo:.1f}–{hi:.1f} at low tilt, below what the specimen received"
        lo, hi = min(s.dose_estimates), max(s.dose_estimates)
        text = f"incident fit in {len(s.dose_estimates)}/{n} series ({lo:.1f}–{hi:.1f})"
        if s.dose_transmitted_only:
            tlo, thi = min(s.dose_transmitted_only), max(s.dose_transmitted_only)
            text += (
                f"; {len(s.dose_transmitted_only)} without a fit (transmitted {tlo:.1f}–{thi:.1f}), "
                "assumed the same beam"
            )
        return text

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
            movies_too = any(ts.movies_complete for ts in selected if ts.source_kind == "stack")
            note = "; raw movies left alone" if movies_too else ""
            parts.append(f"aligned stacks (.mrc, {slices} slices/series{note})")
        if kinds.get("movies"):
            ext = self.frame_extension or "?"
            parts.append(f"movies ({ext}, {slices} tilts/series)")
        if kinds.get("missing"):
            what = {"stack": "a stack", "movies": "complete movies"}.get(self.layer or "", "movies or a stack")
            parts.append(f"{kinds['missing']} series without {what}")
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

    def duplicate_selected_names(self, *, by_folder: bool = False) -> dict[str, list[Path]]:
        """Selected series whose names collide on import (see `duplicate_ts_names`)."""
        return duplicate_ts_names((ts.mdoc_path for ts in self.get_selected_tilt_series()), by_folder=by_folder)

    def folder_name_collisions(self) -> dict[str, list[Path]]:
        """Series names that mdocs of different folders share. Every series counts, selected
        or not: this is the shape of the dataset (several acquisition sessions, each
        restarting `Position_N`), not of the current selection."""
        everything = duplicate_ts_names(ts.mdoc_path for p in self.positions for ts in p.tilt_series)
        return {name: paths for name, paths in everything.items() if len({p.parent for p in paths}) > 1}

    def folder_overviews(self) -> list[DatasetOverview]:
        """One overview per mdoc folder, in path order. They hold this overview's own
        `TiltSeriesInfo` objects, so a selection made in one is the selection here. Parse
        warnings stay with this overview."""
        by_folder: dict[Path, list[TiltSeriesInfo]] = defaultdict(list)
        for p in self.positions:
            for ts in p.tilt_series:
                by_folder[ts.mdoc_path.parent].append(ts)
        out: list[DatasetOverview] = []
        for folder in sorted(by_folder):
            series = by_folder[folder]
            positions = aggregate_to_positions(series)
            for pos in positions:
                pos.selected = any(ts.selected for ts in pos.tilt_series)
            software = next((ts for ts in series if ts.acquisition_software), None)
            dims = next((ts.detector_dimensions for ts in series if ts.detector_dimensions), None)
            out.append(
                DatasetOverview(
                    source_directory=str(folder),
                    frame_extension=self.frame_extension,
                    positions=positions,
                    acquisition_summary=AcquisitionSummary.from_tilt_series(series),
                    acquisition_software=software.acquisition_software if software else "",
                    software_version=software.software_version if software else "",
                    detector_dimensions=dims,
                    layer=self.layer,
                )
            )
        return out

    def selected_acquisition_summary(self) -> AcquisitionSummary:
        """Compute summary only from selected tilt-series."""
        return AcquisitionSummary.from_tilt_series(self.get_selected_tilt_series())
