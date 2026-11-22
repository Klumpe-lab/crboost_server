"""What a map needs before and after RELION resamples it into a template.

- **Axis order.** An MRC header says which of X, Y, Z its columns, rows and sections are (MAPC, MAPR, MAPS).
  `relion_image_handler` reads the array as it lies, so a map deposited in another order (EMD-14150: 3, 1, 2)
  came out turned: a filament along Z lay along X.
- **The pixel size asked for.** RELION rescales to the nearest even box, so the pixel size it writes is the old
  extent over that box: 392 px of 1.102 Å asked for 12.4 Å came out as 34 px of 12.705 Å. Padded to 405 px the
  same map rescales to 36 px of 12.3975 Å. `rescale_padding` finds that box.
- **A filament's length.** A deposited filament map holds a few repeats. A 13-protofilament microtubule, or any
  filament whose lattice repeats along its axis without a turn, maps onto itself when shifted by that repeat,
  so copies shifted by whole repeats fill the box (`extend_along_z`).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import mrcfile
import numpy as np
from scipy import ndimage

# A template's pixel size may differ from the one asked for by this fraction at most.
PIXEL_TOLERANCE = 5.0e-4
# How many even boxes above the nearest one are tried for one the padded map rescales to within the tolerance.
PAD_REACH = 60
# Where along its axis a filament's map counts as filament: its wall's density above this fraction of the plateau.
EXTENT_LEVEL = 0.5
# The map's own repeat is looked for within this fraction of the one given, and taken when the wall, shifted by
# it, agrees with itself at least this well (the correlation of what varies along the axis).
REPEAT_SEARCH = 0.08
REPEAT_MIN_CORRELATION = 0.5


def standard_order(data: np.ndarray, mapc: int, mapr: int, maps: int) -> np.ndarray:
    """`data` (sections, rows, columns as the file holds them) as [z, y, x]. The header's MAPC, MAPR, MAPS name
    the spatial axis (1 = X, 2 = Y, 3 = Z) of the columns, rows and sections."""
    held = (int(maps), int(mapr), int(mapc))  # the spatial axis of array axes 0, 1, 2
    if sorted(held) != [1, 2, 3]:
        raise ValueError(f"The header's axis order (MAPC, MAPR, MAPS = {mapc}, {mapr}, {maps}) names no X, Y and Z")
    return np.transpose(data, [held.index(3), held.index(2), held.index(1)])


def relion_rescaled_box(n: int, apix: float, target_apix: float) -> int:
    """The box `relion_image_handler --rescale_angpix` makes of `n` pixels: the nearest whole number, made even."""
    box = round(n * apix / target_apix)
    return box - box % 2


def rescale_padding(n: int, apix: float, target_apix: float) -> tuple[int, int] | None:
    """The box (≥ `n`) to pad a map to so that RELION's rescale lands on `target_apix` within `PIXEL_TOLERANCE`,
    and the box it then makes: `(padded, rescaled)`. None when no box within `PAD_REACH` even boxes does."""
    first = relion_rescaled_box(n, apix, target_apix)
    for rescaled in range(max(first, 2), first + 2 * PAD_REACH + 1, 2):
        padded = round(rescaled * target_apix / apix)
        if padded < n or relion_rescaled_box(padded, apix, target_apix) != rescaled:
            continue
        if abs(padded * apix / rescaled - target_apix) <= PIXEL_TOLERANCE * target_apix:
            return padded, rescaled
    return None


@dataclass(frozen=True)
class Prepared:
    path: str  # what RELION reads: the map itself when nothing had to change
    notes: tuple[str, ...]
    written: bool


def prepare_for_rescale(src: str, out_dir: str, target_apix: float) -> Prepared:
    """`src` as RELION must read it to give a template of `target_apix`: in X, Y, Z order, and padded (about its
    centre, with the level of its faces taken off first) to the box `rescale_padding` names. Written beside the
    template as `<stem>_prepared.mrc` only when one of the two was needed. Raises ValueError for a map without a
    pixel size, one that is not a cube, or one no padding brings to the pixel size."""
    with mrcfile.open(src, mode="r", permissive=True) as m:
        header = m.header
        order = (int(header.mapc), int(header.mapr), int(header.maps))
        apix = float(m.voxel_size.x)
        shape = tuple(int(x) for x in m.data.shape)
        data = None
        if apix <= 0:
            raise ValueError(f"{Path(src).name} states no pixel size")
        if len(shape) != 3 or len(set(shape)) != 1:
            raise ValueError(f"{Path(src).name} is not a cube ({shape}): a template is resampled from one")
        n = shape[0]
        plan = rescale_padding(n, apix, target_apix)
        if plan is None:
            raise ValueError(
                f"{Path(src).name} ({n} px of {apix:g} Å) cannot be padded to a box that RELION rescales to "
                f"{target_apix:g} Å within {PIXEL_TOLERANCE:.2%}"
            )
        padded, _ = plan
        turned = order != (1, 2, 3)
        if not turned and padded == n:
            return Prepared(path=src, notes=(), written=False)
        data = standard_order(np.asarray(m.data, dtype=np.float32), *order)

    notes = []
    if turned:
        notes.append(f"axes reordered from the header's MAPC, MAPR, MAPS = {order[0]}, {order[1]}, {order[2]}")
    if padded != n:
        faces = np.concatenate([f.ravel() for ax in range(3) for f in (data.take(0, axis=ax), data.take(-1, axis=ax))])
        level = float(faces.mean())
        before = (padded - n) // 2
        out = np.zeros((padded,) * 3, dtype=np.float32)
        out[before : before + n, before : before + n, before : before + n] = data - level
        data = out
        notes.append(f"padded from {n} to {padded} px so the rescale lands on {target_apix:g} Å")
    dst = str(Path(out_dir) / f"{Path(src).stem}_prepared.mrc")
    with mrcfile.new(dst, overwrite=True) as m:
        m.set_data(np.ascontiguousarray(data))
        m.voxel_size = apix
    return Prepared(path=dst, notes=tuple(notes), written=True)


def pixel_problem(path: str, target_apix: float) -> str | None:
    """Why a resampled template is not at the pixel size asked for, or None."""
    with mrcfile.open(path, mode="r", permissive=True, header_only=True) as m:
        apix = float(m.voxel_size.x)
    if abs(apix - target_apix) <= PIXEL_TOLERANCE * target_apix:
        return None
    return f"{Path(path).name} came out at {apix:.4f} Å a pixel, not the {target_apix:g} Å asked for"


@dataclass(frozen=True)
class Extension:
    repeat_ang: float  # the shift used: the map's own repeat when it could be measured, else the one given
    repeat_given_ang: float
    repeat_correlation: float | None  # how well the wall agrees with itself shifted by the measured repeat
    copies: int  # how many shifted copies meet somewhere in the box, at most
    extent_px: tuple[int, int]  # the slices of the source that counted as filament, first and last
    extent_ang: float
    wall_radius_ang: float
    centre_offset_ang: float  # the wall's centre from the box's, in the plane across the axis


def _wall(volume: np.ndarray) -> tuple[np.ndarray, float, float, np.ndarray]:
    """A filament along Z: its density per slice within its wall's ring, the ring's radius (pixels), the
    distance (pixels) of the cross-section's centre of density from the box's, and the ring (a mask)."""
    n = volume.shape[1]
    section = volume.mean(axis=0)
    centre = n / 2.0
    yy, xx = np.indices(section.shape)
    r = np.hypot(yy - centre, xx - centre)
    radial = ndimage.mean(section, labels=np.rint(r).astype(int), index=np.arange(0, n // 2))
    background = float(np.median(radial[-max(2, n // 16) :]))
    contrast = radial - background
    if -float(contrast.min()) > float(contrast.max()):
        raise ValueError("The map's wall is darker than what surrounds it: extend the white volume")
    peak = int(np.argmax(contrast))
    ring = np.abs(r - peak) <= max(1.0, 0.25 * peak)
    weight = np.clip(section - background, 0.0, None) * (r <= 1.6 * max(peak, 1))
    total = float(weight.sum())
    offset = float(np.hypot((weight * (yy - centre)).sum(), (weight * (xx - centre)).sum()) / total) if total else 0.0
    profile = volume[:, ring].mean(axis=1) - background
    return profile, float(peak), offset, ring


def measured_repeat(wall: np.ndarray, voxel_ang: float, repeat_ang: float) -> tuple[float, float] | None:
    """The repeat along Z of a filament's wall (`wall`: slices × pixels of the wall's ring, over the filament's
    full-strength stretch) nearest `repeat_ang`: the shift, within `REPEAT_SEARCH` of it, at which what varies
    along the axis agrees best with itself, and that correlation. The wall is shifted by fractions of a pixel in
    Fourier space and compared where the two overlap. None when the stretch is too short to overlap itself by a
    third, or when the agreement has no peak inside the range (a map filtered past its lattice has nothing
    varying along the axis)."""
    length = wall.shape[0]
    lo, hi = (1 - REPEAT_SEARCH) * repeat_ang / voxel_ang, (1 + REPEAT_SEARCH) * repeat_ang / voxel_ang
    margin = 2
    if length - int(np.ceil(hi)) - 2 * margin < max(3, length // 3):
        return None
    varying = wall - wall.mean(axis=0)
    if not np.any(varying):
        return None
    padded = np.zeros((2 * length, wall.shape[1]))
    padded[:length] = varying
    spectrum = np.fft.rfft(padded, axis=0)
    k = np.fft.rfftfreq(2 * length)

    def agreement(shift: float) -> float:
        moved = np.fft.irfft(spectrum * np.exp(-2j * np.pi * k * shift)[:, None], n=2 * length, axis=0)[:length]
        rows = slice(int(np.ceil(shift)) + margin, length - margin)
        a, b = varying[rows], moved[rows]
        scale = np.sqrt((a * a).sum() * (b * b).sum())
        return float((a * b).sum() / scale) if scale > 0 else 0.0

    shifts = np.linspace(lo, hi, 65)
    values = np.array([agreement(t) for t in shifts])
    best = int(np.argmax(values))
    if best in (0, len(shifts) - 1):
        return None
    y0, y1, y2 = values[best - 1 : best + 2]
    bend = y0 - 2 * y1 + y2
    refined = shifts[best] + (0.5 * (y0 - y2) / bend if bend < 0 else 0.0) * (shifts[1] - shifts[0])
    return float(refined * voxel_ang), float(y1)


def extend_along_z(volume: np.ndarray, voxel_ang: float, repeat_ang: float) -> tuple[np.ndarray, Extension]:
    """A filament along Z, filling the box along Z: the stretch of `volume` that is filament (its wall's density
    above `EXTENT_LEVEL` of the plateau), blended with copies of itself shifted by whole repeats. Each copy is
    weighted by a window over that stretch; the result is the weighted mean. The shift is the map's own repeat
    near `repeat_ang` when its lattice shows along the axis (`measured_repeat`), so that the copies lie in
    register whatever the map's scale; else `repeat_ang`. Raises ValueError when the map holds less than one
    repeat of filament, or when shifted copies leave part of the box empty."""
    if volume.ndim != 3 or len(set(volume.shape)) != 1:
        raise ValueError(f"The map is not a cube ({volume.shape})")
    if repeat_ang <= 0 or voxel_ang <= 0:
        raise ValueError("The repeat and the pixel size must be positive")
    n = volume.shape[0]
    profile, radius_px, offset_px, ring = _wall(volume)
    plateau = float(np.percentile(profile, 90))
    if plateau <= 0:
        raise ValueError("The map shows no wall around its Z axis: is the filament along Z, and white?")
    inside = np.flatnonzero(profile >= EXTENT_LEVEL * plateau)
    first, last = int(inside.min()), int(inside.max())
    extent_ang = (last - first + 1) * voxel_ang
    if extent_ang < repeat_ang:
        raise ValueError(
            f"The map holds {extent_ang:.0f} Å of filament, less than one repeat of {repeat_ang:g} Å: nothing to "
            "extend it with"
        )
    given = repeat_ang
    full = np.flatnonzero(profile >= 0.9 * plateau)  # the stretch without its fading ends
    found = measured_repeat(volume[full.min() : full.max() + 1][:, ring].astype(np.float64), voxel_ang, given)
    correlation = found[1] if found else None
    if found and found[1] >= REPEAT_MIN_CORRELATION:
        repeat_ang = found[0]
    # A window over the filament's stretch, rising over a fifth of it at each end (a pixel at the least).
    ramp = max(1.0, 0.2 * (last - first + 1))
    z = np.arange(n, dtype=float)
    window = np.clip(np.minimum(z - first + 1, last - z + 1) / ramp, 0.0, 1.0)

    # Shifts by whole repeats are fractions of a pixel: done on a Z axis padded three times, in Fourier space.
    pad = n
    weighted = np.zeros((3 * n, n, n), dtype=np.float64)
    weighted[pad : pad + n] = volume * window[:, None, None]
    weights = np.zeros(3 * n, dtype=np.float64)
    weights[pad : pad + n] = window
    f_volume = np.fft.rfft(weighted, axis=0)
    f_weights = np.fft.rfft(weights)
    k = np.fft.rfftfreq(3 * n)
    step = repeat_ang / voxel_ang
    reach = int(np.ceil(1.5 * n / step))
    phase = np.zeros(len(k), dtype=np.complex128)
    for i in range(-reach, reach + 1):
        phase += np.exp(-2j * np.pi * k * i * step)
    total = np.fft.irfft(f_volume * phase[:, None, None], n=3 * n, axis=0)[pad : pad + n]
    norm = np.fft.irfft(f_weights * phase, n=3 * n)[pad : pad + n]
    if norm.min() < 0.5:
        raise ValueError(
            f"Copies shifted by {repeat_ang:g} Å leave part of the box without filament: the map's {extent_ang:.0f} Å "
            "are too short for that repeat"
        )
    out = (total / norm[:, None, None]).astype(np.float32)
    return out, Extension(
        repeat_ang=float(repeat_ang),
        repeat_given_ang=float(given),
        repeat_correlation=correlation,
        copies=round(float(norm.max())),
        extent_px=(first, last),
        extent_ang=float(extent_ang),
        wall_radius_ang=radius_px * voxel_ang,
        centre_offset_ang=offset_px * voxel_ang,
    )


def extend_file(src: str, dst: str, dst_negative: str, repeat_ang: float) -> Extension:
    """`extend_along_z` on the template `src`, written to `dst` with its negative beside it; never overwrites."""
    with mrcfile.open(src, mode="r", permissive=True) as m:
        data = np.asarray(m.data, dtype=np.float32)
        voxel = float(m.voxel_size.x)
    if voxel <= 0:
        raise ValueError(f"{Path(src).name} states no pixel size")
    out, info = extend_along_z(data, voxel, repeat_ang)
    for path, values in ((dst, out), (dst_negative, -out)):
        with mrcfile.new(path, overwrite=False) as m:
            m.set_data(values)
            m.voxel_size = voxel
    return info
