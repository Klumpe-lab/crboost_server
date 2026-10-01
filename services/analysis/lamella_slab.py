"""Where a lamella sits along a tomogram's Z, from the per-slice contrast profile.

Inside the sample every XY slice carries density variation, so the standard deviation per slice rises
across the slab — into a plateau for a flat lamella, a peak for an inclined one. Outside it the profile
falls to a minimum on each side, and in Warp tomograms rises again towards the ends of the volume, so
the outermost slices are not a clean background. `fit_slab` therefore works between the minima that
flank the profile's peak:

- the half-maximum edges — the outermost crossings, between the minima, of the level half-way from the
  higher minimum to the peak — give the slab's centre and thickness;
- the slab's full extent reaches out to the flanking minima, but no further than half a thickness
  beyond each half-maximum edge (a flat valley would otherwise carry it to the end of the volume).

Volumes are (Z, Y, X) arrays whose slice i sits at i·voxel along Z, so the volume centre is slice nz/2 —
the convention of Warp and warpylib reconstructions.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# A peak rising less than this fraction above the higher flanking minimum is not a clear slab: clean
# lamella profiles at ~25 Å per slice measured 0.3-0.7, an ambiguous one (flanks that never return to a
# common background) 0.16. Rejecting sends a series to the full box, which cuts nothing.
MIN_CONTRAST = 0.2
# Fewer slices than this between the half-maximum edges is not a lamella.
MIN_SLICES = 5


@dataclass(frozen=True, slots=True)
class SlabFit:
    found: bool
    reason: str  # why no slab was accepted; "" when found
    center_offset_angstrom: float  # half-maximum centre minus volume centre, along Z
    thickness_angstrom: float  # between the half-maximum edges
    lower_angstrom: float  # full extent, relative to the volume centre (negative = below it)
    upper_angstrom: float
    background: float  # the higher flanking minimum
    peak: float


def _none(reason: str, background: float = float("nan"), peak: float = float("nan")) -> SlabFit:
    return SlabFit(False, reason, 0.0, 0.0, 0.0, 0.0, background, peak)


def z_profile(volume: np.ndarray, *, xy_fraction: float = 0.6) -> np.ndarray:
    """Standard deviation of every XY slice of a (Z, Y, X) volume over the central `xy_fraction` of Y and X."""
    if volume.ndim != 3:
        raise ValueError(f"expected a (Z, Y, X) volume, got shape {volume.shape}")
    nz, ny, nx = volume.shape
    my, mx = max(1, round(ny * xy_fraction)), max(1, round(nx * xy_fraction))
    y0, x0 = (ny - my) // 2, (nx - mx) // 2
    core = np.asarray(volume[:, y0 : y0 + my, x0 : x0 + mx], dtype=np.float64)
    return core.reshape(nz, -1).std(axis=1)


def fit_slab(profile, voxel_angstrom: float) -> SlabFit:
    """The slab in a Z profile (one value per slice, `voxel_angstrom` apart)."""
    p = np.asarray(profile, dtype=float)
    nz = p.size
    if nz < 10 or not np.all(np.isfinite(p)):
        return _none(f"the profile has {nz} slices or non-finite values")
    s = np.convolve(np.pad(p, 1, mode="edge"), np.ones(3) / 3, mode="valid")
    k = int(np.argmax(s))
    peak = float(s[k])
    if k == 0 or k == nz - 1:
        return _none("the profile peaks at the end of the volume", peak=peak)
    i_lo = int(np.argmin(s[:k]))
    i_hi = k + 1 + int(np.argmin(s[k + 1 :]))
    background = float(max(s[i_lo], s[i_hi]))
    if background <= 0 or peak - background < MIN_CONTRAST * background:
        return _none(
            f"the peak {peak:.4g} rises less than {MIN_CONTRAST:.0%} above its flanks ({background:.4g})",
            background,
            peak,
        )
    level = background + 0.5 * (peak - background)
    lo = i_lo + 1 + int(np.argmax(s[i_lo + 1 : k + 1] >= level))  # first slice at or above the level
    hi = i_hi - 1 - int(np.argmax(s[k:i_hi][::-1] >= level))  # last one
    # Sub-slice edges: linear interpolation across the neighbouring slice below the level.
    f_lo = (lo - 1) + (level - s[lo - 1]) / (s[lo] - s[lo - 1])
    f_hi = hi + (s[hi] - level) / (s[hi] - s[hi + 1])
    width = f_hi - f_lo
    if width < MIN_SLICES:
        return _none(f"the slab spans only {width:.1f} slices", background, peak)
    extent_lo = max(float(i_lo), f_lo - width / 2)
    extent_hi = min(float(i_hi), f_hi + width / 2)
    centre = nz / 2
    return SlabFit(
        True,
        "",
        center_offset_angstrom=float(((f_lo + f_hi) / 2 - centre) * voxel_angstrom),
        thickness_angstrom=float(width * voxel_angstrom),
        lower_angstrom=float((extent_lo - centre) * voxel_angstrom),
        upper_angstrom=float((extent_hi - centre) * voxel_angstrom),
        background=background,
        peak=peak,
    )


def centred_box_z(fit: SlabFit, full_z_angstrom: float) -> float:
    """Z extent (Å) of the smallest box centred on the volume centre that holds the slab's full extent,
    never larger than the full volume. The box cannot follow an off-centre slab, so it grows by twice
    the offset."""
    if not fit.found:
        raise ValueError(f"no slab to fit a box to: {fit.reason}")
    return min(full_z_angstrom, 2 * max(abs(fit.lower_angstrom), abs(fit.upper_angstrom)))
