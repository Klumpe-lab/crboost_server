"""Looking at a data folder before a project is made of it: the series, the movie kind, the mdocs, gain
reference candidates, folders that reuse series names, and for each acquisition value what the data says.

The landing page's rules for turning a folder into globs live here and nowhere else: the frames glob is the
folder and the movie extension, the mdocs sit beside the frames unless they are elsewhere, and a SerialEM
delivery keeps one folder per series, so mdocs not found at one depth are looked for at the other. The scan
itself is the dataset parser's (`DatasetOverview`), run off the event loop.
"""

from __future__ import annotations

import asyncio
import glob
import logging
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from services.configs.config_service import get_config_service
from services.tilt_series.preimport import DatasetOverview

logger = logging.getLogger(__name__)

_DEFAULT_MOVIES_EXT = "*.eer"
_DEFAULT_MDOCS_EXT = "*.mdoc"
# Names a gain reference usually goes by; offered, never chosen.
_GAIN_PATTERNS = ("*gain*", "*Gain*", "*GAIN*", "CountRef*", "*.gain", "*.dm4")


class FoundIn(StrEnum):
    MDOC = "mdoc"
    ESTIMATE = "estimate"
    NONE = "none"


class DataValue(BaseModel):
    """What the data says about one acquisition value: the value and where it was read, the several values
    the selected series disagree on (then no value), and a hint when the data only suggests one."""

    value: Any = None
    found_in: FoundIn = FoundIn.NONE
    mixed: list = Field(default_factory=list)
    hint: str = ""


class ScanSeries(BaseModel):
    name: str
    mdoc: str
    folder: str
    tilts: int
    source_kind: str
    selected: bool


class DatasetScan(BaseModel):
    """A folder as Create would import it. `values` holds what the data says per acquisition value
    (`project_create.ACQUISITION_FIELDS`); `folder_collisions` the series names several folders reuse, which
    need folder-qualified names or one project per folder."""

    frames_glob: str
    mdocs_glob: str
    movie_kind: str
    frame_extension: str
    acquisition_software: str = ""
    series: list[ScanSeries] = Field(default_factory=list)
    n_tilts: int = 0
    unresolved: int = 0
    gain_candidates: list[str] = Field(default_factory=list)
    folder_collisions: dict[str, list[str]] = Field(default_factory=dict)
    values: dict[str, DataValue] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


def default_extensions() -> tuple[str, str]:
    """(movies, mdocs) extensions of conf.yaml's default data globs, `*.eer` and `*.mdoc` without them."""
    movies, mdocs = get_config_service().default_data_globs
    movies_ext = "*" + movies.split("*")[-1] if movies and "*" in movies else _DEFAULT_MOVIES_EXT
    mdocs_ext = "*" + mdocs.split("*")[-1] if mdocs and "*" in mdocs else _DEFAULT_MDOCS_EXT
    return movies_ext, mdocs_ext


def glob_in(directory: str, extension: str) -> str:
    """A folder and an extension as a glob; the folder alone without an extension."""
    if not directory or not directory.strip():
        return ""
    if not extension:
        return directory.strip()
    return str(Path(directory.strip()) / extension)


def co_located_mdocs_glob(frames_glob: str, mdocs_extension: str) -> str:
    """The mdocs glob for mdocs kept beside the frames: the frames' folder and the mdocs extension."""
    if not frames_glob:
        return ""
    parent = str(Path(frames_glob).parent) if "*" in frames_glob else frames_glob
    return str(Path(parent) / mdocs_extension)


def other_depth(mdocs_glob: str, extension: str) -> tuple[str, str]:
    """`(glob, extension)` one folder deeper (`*/*.mdoc`, one folder per series), or one up when the extension
    already looks a folder deep."""
    other = f"*/{extension}" if not extension.startswith("*/") else extension[len("*/") :]
    base = Path(mdocs_glob).parents[1 if extension.startswith("*/") else 0]
    return str(base / other), other


def matching_files(pattern: str) -> int:
    return sum(1 for m in glob.glob(pattern) if Path(m).is_file())


def movie_kind(overview: DatasetOverview) -> str:
    """eer, tiff, mrc for movies (by extension), stacks for SerialEM tilt stacks split at Create, missing."""
    kinds = overview.source_kinds()
    if kinds.get("stack"):
        return "stacks"
    ext = (overview.frame_extension or "").lower()
    if not ext or not kinds.get("movies"):
        return "missing"
    return {".eer": "eer", ".tif": "tiff", ".tiff": "tiff", ".mrc": "mrc"}.get(ext, ext.lstrip("."))


def data_values(overview: DatasetOverview) -> dict[str, DataValue]:
    """What the selected series say about each acquisition value, as the mdocs record it. Several values among
    them (compared at the summary's rounding) are listed as mixed, with no value. Cs, amplitude contrast, both
    hands and the EER grouping are never in an mdoc."""
    s = overview.selected_acquisition_summary()
    selected = overview.get_selected_tilt_series()

    def from_list(values: list, attr: str) -> DataValue:
        if len(values) == 1:
            raw = next((getattr(ts, attr) for ts in selected if getattr(ts, attr) is not None), values[0])
            return DataValue(value=raw, found_in=FoundIn.MDOC)
        if len(values) > 1:
            return DataValue(found_in=FoundIn.MDOC, mixed=list(values))
        return DataValue()

    dose = from_list(s.doses, "dose_per_tilt") if s.dose_missing == 0 else DataValue()
    if s.dose_missing:
        estimate = overview.dose_estimate()
        if estimate is not None:
            dose = DataValue(
                value=round(estimate, 3),
                found_in=FoundIn.ESTIMATE,
                hint=f"{s.dose_missing} series record no dose; " + overview.dose_estimate_detail(),
            )
    software = overview.acquisition_software
    return {
        "pixel_size_angstrom": from_list(s.pixel_sizes, "pixel_size"),
        "acceleration_voltage_kv": from_list(s.voltages, "voltage"),
        "spherical_aberration_mm": DataValue(),
        "amplitude_contrast": DataValue(),
        "dose_per_tilt": dose,
        "tilt_axis_degrees": from_list(s.tilt_axes, "tilt_axis"),
        "detector_dimensions": DataValue(value=list(overview.detector_dimensions), found_in=FoundIn.MDOC)
        if overview.detector_dimensions
        else DataValue(),
        "invert_tilt_angles": DataValue(hint=f"acquisition software: {software or 'not stated in the mdocs'}"),
        "invert_defocus_hand": DataValue(),
        "eer_fractions_per_frame": DataValue(),
    }


def differences(spec, values: dict[str, DataValue]) -> list[str]:
    """Each value of `spec` that differs from what the data says, naming both."""
    out: list[str] = []
    for name, said in values.items():
        given = getattr(spec, name, None)
        if given is None:
            continue
        if said.mixed:
            out.append(f"{name}: {given} given; the selected series have several ({', '.join(map(str, said.mixed))})")
            continue
        if said.found_in == FoundIn.NONE or said.value is None:
            continue
        data = tuple(said.value) if isinstance(said.value, list) else said.value
        mine = tuple(given) if isinstance(given, (list, tuple)) else given
        if isinstance(data, float) or isinstance(mine, float):
            same = abs(float(data) - float(mine)) < 1e-3 * max(1.0, abs(float(data)))
        else:
            same = data == mine
        if not same:
            out.append(f"{name}: {given} given; the {said.found_in.value} says {said.value}")
    return out


def gain_candidates(*folders: Path) -> list[str]:
    """Files in these folders named as gain references usually are; offered, never applied."""
    seen: dict[str, None] = {}
    for folder in folders:
        if folder is None or not folder.is_dir():
            continue
        for pattern in _GAIN_PATTERNS:
            for p in sorted(folder.glob(pattern)):
                if p.is_file():
                    seen.setdefault(str(p), None)
    return list(seen)


def first_mdoc_values(mdocs_glob: str) -> dict[str, Any]:
    """What a Create without a scan takes from the first tilt-series mdoc: pixel size, voltage, tilt axis,
    detector size and software, and the dose with its source, the mdoc's ExposureDose or the scan's estimate.
    {} when no mdoc is usable; no dose key when neither gives one."""
    from services.configs.mdoc_service import get_mdoc_service

    found = get_mdoc_service().first_mdoc_facts(mdocs_glob)
    if found is None:
        return {}
    facts = found[0]
    out: dict[str, Any] = {}
    for key, value in (
        ("pixel_size_angstrom", facts.pixel_size),
        ("acceleration_voltage_kv", facts.voltage),
        ("tilt_axis_degrees", facts.tilt_axis),
        ("detector_dimensions", facts.detector_dimensions),
    ):
        if value is not None:
            out[key] = value
    if facts.software:
        out["acquisition_software"] = facts.software
    if facts.dose_per_tilt is not None:
        out["dose_per_tilt"], out["dose_per_tilt_source"] = facts.dose_per_tilt, "mdoc"
    elif facts.dose_estimate is not None:
        out["dose_per_tilt"], out["dose_per_tilt_source"] = facts.dose_estimate.value, "estimated"
    return out


def _scan_sync(frames_glob: str, mdocs_glob: str, progress_cb) -> tuple[DatasetScan, DatasetOverview]:
    from services.configs.dataset_parsing_service import get_dataset_parsing_service

    frames_dir = str(Path(frames_glob).parent) if frames_glob and "*" in frames_glob else None
    overview = get_dataset_parsing_service().parse_dataset(mdocs_glob, frames_dir, progress_cb)
    ext = overview.frame_extension
    if ext and frames_glob and not frames_glob.endswith(f"*{ext}"):
        # The scan chose another layer (`.mrc` frames split from stacks): the frames glob follows it.
        frames_glob = str(Path(frames_glob).parent / f"*{ext}")
    series = [
        ScanSeries(
            name=ts.ts_name,
            mdoc=str(ts.mdoc_path),
            folder=str(ts.mdoc_path.parent),
            tilts=ts.tilt_count,
            source_kind=ts.source_kind,
            selected=ts.selected,
        )
        for p in overview.positions
        for ts in p.tilt_series
    ]
    folders = {Path(frames_glob).parent if frames_glob else None, Path(mdocs_glob).parent}
    scan = DatasetScan(
        frames_glob=frames_glob,
        mdocs_glob=mdocs_glob,
        movie_kind=movie_kind(overview),
        frame_extension=ext,
        acquisition_software=overview.acquisition_software,
        series=series,
        n_tilts=overview.total_frames,
        unresolved=overview.unresolved_selected,
        gain_candidates=gain_candidates(*(f for f in folders if f is not None)),
        folder_collisions={name: [str(p) for p in paths] for name, paths in overview.folder_name_collisions().items()},
        values=data_values(overview),
        warnings=list(overview.parse_warnings),
    )
    return scan, overview


async def scan(
    directory: str | None = None,
    *,
    frames_glob: str | None = None,
    mdocs_glob: str | None = None,
    mdocs_directory: str | None = None,
    progress_cb=None,
) -> dict[str, Any]:
    """Scan a data folder, or globs, as the landing page does. From a folder: its movies at conf.yaml's
    default extension, the mdocs beside them or in `mdocs_directory`, one folder deeper when none sit at the
    first depth. `ok(scan=DatasetScan, overview=DatasetOverview)`, or why nothing could be scanned."""
    from services.result import err, ok

    movies_ext, mdocs_ext = default_extensions()
    if directory:
        frames_glob = frames_glob or glob_in(directory, movies_ext)
        mdocs_glob = mdocs_glob or (
            glob_in(mdocs_directory, mdocs_ext) if mdocs_directory else co_located_mdocs_glob(frames_glob, mdocs_ext)
        )
    if not mdocs_glob:
        return err("Give a data folder, or the frames and mdocs globs.")
    if await asyncio.to_thread(matching_files, mdocs_glob) == 0:
        ext = Path(mdocs_glob).name if "*" in Path(mdocs_glob).name else mdocs_ext
        retry, _ = other_depth(mdocs_glob, ext)
        if await asyncio.to_thread(matching_files, retry) == 0:
            return err(f"No mdocs at {mdocs_glob} or {retry}.")
        mdocs_glob = retry
    scanned, overview = await asyncio.to_thread(_scan_sync, frames_glob or "", mdocs_glob, progress_cb)
    return ok(scan=scanned, overview=overview)
