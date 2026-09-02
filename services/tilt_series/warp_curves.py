"""1-D CTF-fit curves from WarpTools XML — the "measured power spectrum vs fitted
CTF" overlay (CryoSPARC's CTF-fit diagnostic), per movie (frameseries XML) or
per tilt (tiltseries XML).

What Warp writes, all as ``freq|value;freq|value;…`` text with freq in cycles per
pixel (0 → 0.5 = Nyquist):

    frameseries <Movie>      <PS1D>  rotational average of the background-
                             subtracted power spectrum (256 bins for a 512 window)
                             <SimulatedScale>  the fitted envelope — PS1D's value
                             at the CTF maxima, on a coarser frequency grid
                             <SimulatedBackground>  fitted floor (all-zero when the
                             background was subtracted before fitting)
    tiltseries <TiltSeries>  <TiltPS1D ID=z> + <TiltSimulatedScale ID=z> per tilt;
                             the tilt's defocus in <GridCTF Z=z>; no per-tilt
                             background

The CTF model is computed HERE from the fitted parameters in the same XML
(<CTF> Defocus / Cs / Voltage / Amplitude / PhaseShift + PixelSize), so measured
and model come from one self-describing file — nothing is taken from job params.
Parses are memoized by (path, mtime); the tiltseries XML (~300 KB, one curve per
tilt) is parsed once and every tilt is served from memory.
"""

from __future__ import annotations

import bisect
import logging
import math
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)


class CtfFit1D(NamedTuple):
    freq_cyc_px: list[float]  # PS1D sampling, cycles per pixel
    ps1d: list[float]  # measured (background-subtracted) radial power
    scale: list[float | None]  # SimulatedScale interpolated onto freq_cyc_px; None outside its support
    background: list[float]  # SimulatedBackground interpolated (0 when absent)
    pixel_size_a: float
    defocus_um: float  # mean defocus — what the radial average was fit with
    cs_mm: float
    voltage_kv: float
    amplitude_contrast: float
    phase_shift_pi: float  # Warp stores PhaseShift in units of π
    fit_range_nyq: tuple[float, float]  # OptionsCTF RangeMin / RangeMax, fraction of Nyquist
    ctf_resolution_a: float | None  # Warp's fit-resolution estimate for this fit


# str(path) -> (mtime, parsed). XMLs are immutable once the job finished, so an
# mtime key is a safe cache (same pattern as frameseries_quality._MEMO).
_MEMO: dict[str, tuple[float, object]] = {}


def _memo(path: Path, build: Callable[[ET.Element], object]) -> object | None:
    """``build(root)`` for the XML at ``path``, cached by mtime. None (logged)
    when the file is missing, unparseable, or lacks what ``build`` needs."""
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    key = str(path)
    hit = _MEMO.get(key)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    try:
        value = build(ET.parse(path).getroot())
    except (ET.ParseError, ValueError, TypeError) as e:
        logger.warning("Could not read CTF-fit curves from %s: %s", path, e)
        return None
    _MEMO[key] = (mtime, value)
    return value


def _pairs(text: str | None) -> tuple[list[float], list[float]]:
    """Warp's ``freq|value;…`` curve text → (freqs, values)."""
    xs: list[float] = []
    ys: list[float] = []
    for item in (text or "").split(";"):
        if "|" not in item:
            continue
        a, b = item.split("|", 1)
        try:
            xs.append(float(a))
            ys.append(float(b))
        except ValueError:
            continue
    return xs, ys


def _interp(xs: list[float], ys: list[float], x: float) -> float | None:
    """Linear interpolation on ascending ``xs``; None outside their support."""
    if not xs or x < xs[0] or x > xs[-1]:
        return None
    hi = bisect.bisect_left(xs, x)
    if hi == 0:
        return ys[0]
    lo = hi - 1
    if hi >= len(xs) or xs[hi] == xs[lo]:
        return ys[lo]
    t = (x - xs[lo]) / (xs[hi] - xs[lo])
    return ys[lo] + t * (ys[hi] - ys[lo])


def _param(block: ET.Element | None, name: str) -> float | None:
    if block is None:
        return None
    p = block.find(f"Param[@Name='{name}']")
    try:
        return float(p.get("Value"))  # type: ignore[union-attr]
    except (AttributeError, TypeError, ValueError):
        return None


def _required(block: ET.Element | None, name: str) -> float:
    v = _param(block, name)
    if v is None:
        raise ValueError(f"missing <CTF> parameter {name!r}")
    return v


def _positive(v: object) -> float | None:
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _common(root: ET.Element) -> dict:
    """The fit parameters shared by every curve in one XML (frame or TS)."""
    ctf = root.find("CTF")
    if ctf is None:
        raise ValueError("no <CTF> block")
    opts = root.find("OptionsCTF")
    return {
        "pixel_size_a": _required(ctf, "PixelSize"),
        "cs_mm": _required(ctf, "Cs"),
        "voltage_kv": _required(ctf, "Voltage"),
        "amplitude_contrast": _required(ctf, "Amplitude"),
        "phase_shift_pi": _param(ctf, "PhaseShift") or 0.0,
        "fit_range_nyq": (_param(opts, "RangeMin") or 0.0, _param(opts, "RangeMax") or 1.0),
        "ctf_resolution_a": _positive(root.get("CTFResolutionEstimate")),
    }


# ── Frameseries (one fit per movie) ──────────────────────────────────────────


def _build_frame_fit(root: ET.Element) -> CtfFit1D:
    freqs, ps = _pairs(root.findtext("PS1D"))
    if not freqs:
        raise ValueError("no <PS1D> curve")
    sx, sy = _pairs(root.findtext("SimulatedScale"))
    bx, by = _pairs(root.findtext("SimulatedBackground"))
    return CtfFit1D(
        freq_cyc_px=freqs,
        ps1d=ps,
        scale=[_interp(sx, sy, f) for f in freqs],
        background=[_interp(bx, by, f) or 0.0 for f in freqs],
        defocus_um=_required(root.find("CTF"), "Defocus"),
        **_common(root),
    )


def read_frameseries_fit(xml_path: Path | str) -> CtfFit1D | None:
    """The CTF fit of one movie (``<fsMotion job>/warp_frameseries/<frame>.xml``),
    or None when the XML is missing / carries no PS1D."""
    return _memo(Path(xml_path), _build_frame_fit)  # type: ignore[return-value]


# ── Tiltseries (one fit per tilt, in one XML) ────────────────────────────────


def _build_ts_fits(root: ET.Element) -> dict:
    grid = root.find("GridCTF")
    if grid is None:
        raise ValueError("no <GridCTF>")
    defocus: dict[int, float] = {}
    for n in grid.findall("Node"):
        try:
            defocus[int(n.get("Z"))] = float(n.get("Value"))
        except (TypeError, ValueError):
            continue
    ps = {int(el.get("ID")): _pairs(el.text) for el in root.findall("TiltPS1D") if el.get("ID")}
    scale = {int(el.get("ID")): _pairs(el.text) for el in root.findall("TiltSimulatedScale") if el.get("ID")}
    return {"common": _common(root), "defocus": defocus, "ps": ps, "scale": scale}


def read_tiltseries_fit(xml_path: Path | str, z: int) -> CtfFit1D | None:
    """The CTF fit of tilt ``z`` (the <Node Z> / <TiltPS1D ID> index) in a
    ts_ctf per-TS XML, or None when the XML / that tilt's curve is absent."""
    data = _memo(Path(xml_path), _build_ts_fits)
    if not isinstance(data, dict):
        return None
    curve = data["ps"].get(z)
    dz = data["defocus"].get(z)
    if curve is None or dz is None or not curve[0]:
        return None
    freqs, vals = curve
    sx, sy = data["scale"].get(z, ([], []))
    return CtfFit1D(
        freq_cyc_px=freqs,
        ps1d=vals,
        scale=[_interp(sx, sy, f) for f in freqs],
        background=[0.0] * len(freqs),
        defocus_um=dz,
        **data["common"],
    )


# ── The model + the overlay ──────────────────────────────────────────────────


def electron_wavelength_a(voltage_kv: float) -> float:
    """Relativistic electron wavelength (Å): 12.2643 / √(V (1 + 0.978466e-6 V)), V in volts."""
    v = voltage_kv * 1000.0
    return 12.2643247 / math.sqrt(v * (1.0 + 0.978466e-6 * v))


def ctf_squared(freq_cyc_px: list[float], fit: CtfFit1D) -> list[float]:
    """CTF²(f) from the fit's own parameters — the standard weak-phase CTF used by
    Warp / CTFFIND / RELION: χ(f) = π λ f² Δz − ½ π Cs λ³ f⁴ + φ with f in 1/Å,
    Δz underfocus-positive, CTF = −(√(1−A²) sin χ + A cos χ). Squared, so the sign
    conventions that differ between packages drop out."""
    lam = electron_wavelength_a(fit.voltage_kv)
    dz = fit.defocus_um * 1.0e4  # Å
    cs = fit.cs_mm * 1.0e7  # Å
    a = fit.amplitude_contrast
    phi = fit.phase_shift_pi * math.pi
    k1 = math.pi * lam * dz
    k2 = 0.5 * math.pi * cs * lam**3
    root = math.sqrt(max(0.0, 1.0 - a * a))
    out: list[float] = []
    for fpx in freq_cyc_px:
        f2 = (fpx / fit.pixel_size_a) ** 2
        chi = k1 * f2 - k2 * f2 * f2 + phi
        c = root * math.sin(chi) + a * math.cos(chi)
        out.append(c * c)
    return out


def normalized_overlay(fit: CtfFit1D) -> tuple[list[float], list[float | None], list[float]]:
    """(frequency in 1/Å, measured ÷ envelope, CTF² model) from the fit window's low
    end to Nyquist. Measured = (PS1D − background) / SimulatedScale, so the Thon
    rings sit on the model's 0…1 scale at every frequency instead of fading with
    the envelope; None where the envelope is undefined or non-positive."""
    lo = 0.5 * fit.fit_range_nyq[0]
    model = ctf_squared(fit.freq_cyc_px, fit)
    f_out: list[float] = []
    m_out: list[float | None] = []
    c_out: list[float] = []
    for f, p, s, b, c in zip(fit.freq_cyc_px, fit.ps1d, fit.scale, fit.background, model, strict=True):
        if f < lo:
            continue
        f_out.append(f / fit.pixel_size_a)
        m_out.append(None if s is None or s <= 0 else (p - b) / s)
        c_out.append(c)
    return f_out, m_out, c_out


def fit_window_inv_a(fit: CtfFit1D) -> tuple[float, float]:
    """The frequency window (1/Å) Warp fitted the CTF over (OptionsCTF range)."""
    nyquist = 0.5 / fit.pixel_size_a
    return fit.fit_range_nyq[0] * nyquist, min(fit.fit_range_nyq[1], 1.0) * nyquist
