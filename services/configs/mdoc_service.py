# services/mdoc_service.py
import glob
import logging
import math
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# One mdoc interpretation
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DoseEstimate:
    """Dose per tilt derived from the per-section `DoseRate × ExposureTime / PixelSpacing²`
    when the mdoc carries no `ExposureDose`. `transmitted_low_tilt` is what came through
    the lamella at the lowest |tilt|; `incident` is the zero-thickness extrapolation of
    `ln dose` against `1 / cos(θ − θ₀)` (None when the fit is not trustworthy) — the
    number the specimen actually received. Always an estimate; the acquirer's value
    replaces it."""

    transmitted_low_tilt: float
    incident: float | None
    pretilt_deg: float | None
    t_over_lambda: float | None
    rms: float | None
    n_tilts: int

    @property
    def value(self) -> float:
        return self.incident if self.incident is not None else self.transmitted_low_tilt

    def describe(self) -> str:
        if self.incident is None:
            return f"transmitted {self.transmitted_low_tilt:.1f} at low tilt; no zero-thickness fit"
        return (
            f"transmitted {self.transmitted_low_tilt:.1f}, incident {self.incident:.1f}, "
            f"pretilt {self.pretilt_deg:+.1f}°, t/λ {self.t_over_lambda:.2f}"
        )


@dataclass(frozen=True)
class MdocFacts:
    """Acquisition facts of one mdoc — the only interpretation of an mdoc's header and
    first section (scan, autodetect, protocol apply and the stack split all read this)."""

    software: str = ""  # "SerialEM" / "Tomo5" / ""
    software_version: str = ""
    pixel_size: float | None = None
    voltage: float | None = None
    dose_per_tilt: float | None = None  # None when absent or 0 — never a default
    dose_estimate: DoseEstimate | None = None
    tilt_axis: float | None = None
    detector_dimensions: tuple[int, int] | None = None
    image_file: str = ""
    n_sections: int = 0


def ts_name_from_mdoc(mdoc_name: str) -> str:
    """`area_5-A_ts_002.mrc.mdoc` → `area_5-A_ts_002`; `Position_1.mdoc` → `Position_1`
    (the series name is the mdoc name minus `.mdoc` minus a trailing `.mrc`/`.st`, so
    TS ids never carry a dot)."""
    name = mdoc_name[: -len(".mdoc")] if mdoc_name.endswith(".mdoc") else mdoc_name
    for stack_ext in (".mrc", ".st"):
        if name.endswith(stack_ext):
            return name[: -len(stack_ext)]
    return name


_T_LINE_RE = re.compile(r"^\[T\s*=\s*(.*?)\]?$")
_SERIALEM_VERSION_RE = re.compile(r"SerialEM Version\s+(\S+)")
_TOMO5_VERSION_RE = re.compile(r"Tomography[_ ]v?(\d+(?:\.\d+)*)")


def parse_header_facts(header_text: str) -> tuple[dict[str, str], list[str]]:
    """Header `key = value` pairs plus the fragments of `[T = …]` lines.

    SerialEM writes `[T = Tilt axis angle = 85.0, binning = 1  spot = 4  camera = 2]`:
    the bracket is stripped, the leading `T =` dropped, and the rest split on commas /
    two-plus spaces into `key = value` pairs (so `Tilt axis angle` becomes a header key
    instead of the key `[T`). Fragments without `=` (`SerialEM: Acquired on …`,
    `Tomography: TITAN…`) are returned as notes — they name the software."""
    kv: dict[str, str] = {}
    notes: list[str] = []
    for raw in header_text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        m = _T_LINE_RE.match(line)
        if m:
            for frag in re.split(r",|\s{2,}", m.group(1)):
                frag = frag.strip()
                if not frag:
                    continue
                if "=" in frag:
                    k, v = frag.split("=", 1)
                    kv.setdefault(k.strip(), v.strip())
                else:
                    notes.append(frag)
            continue
        if "=" in line and not line.startswith("["):
            k, v = line.split("=", 1)
            kv.setdefault(k.strip(), v.strip())
    return kv, notes


def _float_or_none(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        # Non-numeric mdoc value — the fact stays unknown; every consumer shows the gap.
        return None


def _detect_software(kv: dict[str, str], notes: list[str]) -> tuple[str, str]:
    text = " ".join(notes) + " " + kv.get("Version", "")
    if "SerialEM" in text:
        m = _SERIALEM_VERSION_RE.search(text)
        return "SerialEM", (m.group(1) if m else "")
    if "Tomography" in text:
        m = _TOMO5_VERSION_RE.search(text)
        return "Tomo5", (m.group(1) if m else "")
    return "", ""


def acquisition_from_mdoc(parsed: dict[str, Any]) -> MdocFacts:
    """`MdocFacts` from `MdocService.parse_mdoc_file()` output."""
    kv, notes = parse_header_facts(parsed.get("header", ""))
    sections = parsed.get("data", [])
    first = sections[0] if sections else {}
    software, version = _detect_software(kv, notes)

    def pick(key: str) -> str | None:
        return kv.get(key) if key in kv else first.get(key)

    # Tilt axis. SerialEM: the `Tilt axis angle` header line, else |RotationAngle|.
    # Tomo5: |RotationAngle| of the first section.
    tilt_axis = None
    if software == "SerialEM" and "Tilt axis angle" in kv:
        tilt_axis = _float_or_none(kv["Tilt axis angle"])
    if tilt_axis is None and "RotationAngle" in first:
        rot = _float_or_none(first["RotationAngle"])
        tilt_axis = abs(rot) if rot is not None else None
    if tilt_axis is None and "Tilt axis angle" in kv:
        tilt_axis = _float_or_none(kv["Tilt axis angle"])

    dims = None
    size = pick("ImageSize")
    if size:
        parts = size.split()
        if len(parts) >= 2 and parts[0].lstrip("-").isdigit() and parts[1].lstrip("-").isdigit():
            dims = (abs(int(parts[0])), abs(int(parts[1])))

    # ExposureDose of the first section (header as fallback); 0 means "not recorded".
    dose = _float_or_none(first.get("ExposureDose", kv.get("ExposureDose")))
    dose = round(dose, 2) if dose is not None and dose > 0 else None
    pixel_size = _float_or_none(pick("PixelSpacing"))
    estimate = None
    if dose is None and pixel_size:
        estimate = estimate_incident_dose(sections, pixel_size)

    return MdocFacts(
        software=software,
        software_version=version,
        pixel_size=pixel_size,
        voltage=_float_or_none(pick("Voltage")),
        dose_per_tilt=dose,
        dose_estimate=estimate,
        tilt_axis=tilt_axis,
        detector_dimensions=dims,
        image_file=kv.get("ImageFile", ""),
        n_sections=len(sections),
    )


_PRETILT_SCAN_DEG = [x * 0.5 for x in range(-40, 41)]  # −20 … +20 in 0.5° steps
_MIN_FIT_TILTS = 10
_MIN_FIT_SPAN_DEG = 30.0
_MAX_FIT_RMS = 0.05


def estimate_incident_dose(sections: list[dict[str, Any]], pixel_size: float) -> DoseEstimate | None:
    """Per-tilt dose from `DoseRate × ExposureTime / PixelSpacing²`.

    The lamella attenuates the beam as `exp(−t / (λ·cos(θ − θ₀)))`, so `ln dose` is linear
    in `1 / cos(θ − θ₀)`: the intercept is the incident dose, the slope `−t/λ`, θ₀ the
    lamella pretilt (scanned, best rms kept). The fit needs every section to carry
    `DoseRate`, `ExposureTime` and `TiltAngle`, at least 10 tilts spanning ≥ 30°, and an
    rms of the ln-residual below 0.05 — otherwise only the transmitted value exists.
    Returns None when no section carries the rates at all."""
    points: list[tuple[float, float]] = []  # (tilt_angle, transmitted dose per tilt)
    for sec in sections:
        rate = _float_or_none(sec.get("DoseRate"))
        exposure = _float_or_none(sec.get("ExposureTime"))
        angle = _float_or_none(sec.get("TiltAngle"))
        if rate is None or exposure is None or angle is None or rate <= 0 or exposure <= 0:
            continue
        points.append((angle, rate * exposure / (pixel_size * pixel_size)))
    if not points:
        return None

    low = min(points, key=lambda p: abs(p[0]))
    transmitted = round(low[1], 2)
    n = len(points)
    span = max(p[0] for p in points) - min(p[0] for p in points)
    complete = n == len(sections) and n >= _MIN_FIT_TILTS and span >= _MIN_FIT_SPAN_DEG
    if not complete:
        return DoseEstimate(transmitted, None, None, None, None, n)

    best: tuple[float, float, float, float] | None = None  # (rms, pretilt, intercept, slope)
    ys = [math.log(d) for _, d in points]
    for pretilt in _PRETILT_SCAN_DEG:
        xs = []
        for angle, _ in points:
            c = math.cos(math.radians(angle - pretilt))
            if c <= 0.05:
                break
            xs.append(1.0 / c)
        if len(xs) != n:
            continue
        sx, sy = sum(xs), sum(ys)
        sxx = sum(x * x for x in xs)
        sxy = sum(x * y for x, y in zip(xs, ys, strict=True))
        denom = n * sxx - sx * sx
        if abs(denom) < 1e-12:
            continue
        slope = (n * sxy - sx * sy) / denom
        intercept = (sy - slope * sx) / n
        rms = math.sqrt(sum((y - intercept - slope * x) ** 2 for x, y in zip(xs, ys, strict=True)) / n)
        if best is None or rms < best[0]:
            best = (rms, pretilt, intercept, slope)

    if best is None or best[0] > _MAX_FIT_RMS or best[3] > 0:
        return DoseEstimate(transmitted, None, None, None, best[0] if best else None, n)
    rms, pretilt, intercept, slope = best
    return DoseEstimate(
        transmitted_low_tilt=transmitted,
        incident=round(math.exp(intercept), 2),
        pretilt_deg=pretilt,
        t_over_lambda=round(-slope, 3),
        rms=round(rms, 4),
        n_tilts=n,
    )


class MdocService:
    """Singleton service for all .mdoc file interactions."""

    def first_mdoc_facts(self, mdocs_glob: str) -> tuple[MdocFacts, dict[str, Any]] | None:
        """`(MdocFacts, parsed)` of the first tilt-series mdoc the glob matches (sorted;
        mdocs without a `[ZValue]` section — SerialEM per-movie mdocs — are skipped).
        None when nothing usable matches."""
        for p in sorted(glob.glob(mdocs_glob)):
            if not (os.path.isfile(p) and p.endswith(".mdoc")):
                continue
            try:
                parsed = self.parse_mdoc_file(Path(p))
            except Exception:
                logger.exception("MdocService failed to parse %s", p)
                return None
            if parsed["data"]:
                return acquisition_from_mdoc(parsed), parsed
        return None

    def get_autodetect_params(self, mdocs_glob: str) -> dict[str, Any]:
        """
        Parse the first VALID mdoc file found by the glob.
        """
        found = self.first_mdoc_facts(mdocs_glob)
        if found is None:
            return {}
        facts, parsed = found
        first = parsed["data"][0]
        result: dict[str, Any] = {
            "acquisition_software": facts.software or "Tomo5",
            "software_version": facts.software_version,
            "dose_estimate": facts.dose_estimate,
            "invert_tilt_angles": facts.software != "SerialEM",
        }
        if facts.tilt_axis is not None:
            result["tilt_axis_angle"] = facts.tilt_axis
        if facts.pixel_size is not None:
            result["pixel_spacing"] = facts.pixel_size
        if facts.voltage is not None:
            result["voltage"] = facts.voltage
        if facts.detector_dimensions is not None:
            result["detector_dimensions"] = facts.detector_dimensions
        if facts.dose_per_tilt is not None:
            result["dose_per_tilt"] = facts.dose_per_tilt
            result["frame_dose"] = facts.dose_per_tilt
        for key, out in (("Magnification", "nominal_magnification"), ("SpotSize", "spot_size"), ("Binning", "binning")):
            if key in first and first[key].lstrip("-").isdigit():
                result[out] = int(first[key])
        if ".eer" in first.get("SubFramePath", "").lower():
            result["eer_fractions_per_frame"] = 32
        return result

    def parse_all_mdoc_files(self, mdocs_glob: str) -> dict[str, Any]:
        """
        Parse ALL mdoc files and return comprehensive statistics.
        """
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            return {}

        result = {
            "mdoc_files": [],
            "tilt_series_count": 0,
            "total_tilts": 0,
            "tilt_range": (0, 0),
            "consistent_params": True,
        }

        pixel_sizes = set()
        voltages = set()
        dose_rates = set()
        tilt_angles = []

        for mdoc_file in mdoc_files:
            if not os.path.isfile(mdoc_file):
                continue

            mdoc_path = Path(mdoc_file)
            try:
                parsed = self.parse_mdoc_file(mdoc_path)
                data_sections = parsed["data"]

                if data_sections:
                    # Extract parameters from first section of each file
                    first_section = data_sections[0]

                    if "PixelSpacing" in first_section:
                        pixel_sizes.add(float(first_section["PixelSpacing"]))
                    if "Voltage" in first_section:
                        voltages.add(float(first_section["Voltage"]))
                    if "ExposureDose" in first_section:
                        dose_rates.add(float(first_section["ExposureDose"]))

                    # Collect all tilt angles
                    for section in data_sections:
                        if "TiltAngle" in section:
                            tilt_angles.append(float(section["TiltAngle"]))

                    result["total_tilts"] += len(data_sections)
                    result["mdoc_files"].append(mdoc_path.name)

            except Exception as e:
                logger.warning("Failed to parse %s: %s", mdoc_file, e)
                continue

        result["tilt_series_count"] = len(result["mdoc_files"])

        if tilt_angles:
            result["tilt_range"] = (min(tilt_angles), max(tilt_angles))

        result["consistent_params"] = len(pixel_sizes) <= 1 and len(voltages) <= 1 and len(dose_rates) <= 1

        if pixel_sizes:
            result["pixel_size"] = next(iter(pixel_sizes))
        if voltages:
            result["voltage"] = next(iter(voltages))
        if dose_rates:
            result["dose_per_frame"] = next(iter(dose_rates))
            result["dose_per_tilt"] = round(next(iter(dose_rates)), 2)

        return result

    def parse_mdoc_file(self, mdoc_path: Path) -> dict[str, Any]:
        """
        Fully parse an mdoc file into headers and data sections.
        """
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False

        with open(mdoc_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith("[ZValue"):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {"ZValue": line.split("=")[1].strip().strip("]")}
                    in_zvalue_section = True
                elif in_zvalue_section and "=" in line:
                    key, value = [x.strip() for x in line.split("=", 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)

        if current_section:
            data_sections.append(current_section)

        return {"header": "\n".join(header_lines), "data": data_sections}

    def write_mdoc_file(self, mdoc_data: dict[str, Any], output_path: Path):
        """
        Writes a parsed mdoc data structure back to a file.
        """
        with open(output_path, "w") as f:
            f.write(mdoc_data["header"] + "\n")
            for section in mdoc_data["data"]:
                z_value = section.pop("ZValue", None)
                if z_value is not None:
                    f.write(f"[ZValue = {z_value}]\n")
                for key, value in section.items():
                    f.write(f"{key} = {value}\n")
                f.write("\n")


_mdoc_service_instance: MdocService | None = None


@lru_cache
def get_mdoc_service() -> MdocService:
    """Get or create the MdocService singleton"""
    global _mdoc_service_instance
    if _mdoc_service_instance is None:
        _mdoc_service_instance = MdocService()
    return _mdoc_service_instance
