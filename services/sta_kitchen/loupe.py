"""The loupe (roadmap 40 S6.7): one pick up close, as one PNG: X/Y, X/Z and Y/Z slabs of its reconstruction
around it, and under them the same slabs of its template match's template turned to the pick's angles.

The template is turned the way the matched angles mean it: R = ZYZ(rot, tilt, psi) (intrinsic, degrees),
transposed, applied to the template. Of six conventions (ZYZ, zyz, ZXZ, each as is and transposed) it is
the one that made the template agree with the tomogram at each of the twelve best ribosome picks of
AGG412_20260311_MyrGFP_Grid2 job027: normalised cross-correlation 0.18 against 0.07–0.08 for the others and
for random turns.

Worker-safe: plain arguments, writes only `out`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np

LOUPE_PX = 240  # each view, on screen and in the PNG
GAP_PX = 4
_P3 = np.eye(3)[::-1]  # (x, y, z) <-> array index order (z, y, x)


def _views(vol: np.ndarray, half_slab: int) -> list[np.ndarray]:
    """X/Y, X/Z and Y/Z means of a slab through the middle of a (z, y, x) box, each with its second axis
    (Y, then Z, then Z) up."""
    c = [s // 2 for s in vol.shape]
    lo, hi = (lambda k: max(0, c[k] - half_slab)), (lambda k: c[k] + half_slab + 1)
    xy = vol[lo(0) : hi(0)].mean(axis=0)  # (y, x)
    xz = vol[:, lo(1) : hi(1)].mean(axis=1)  # (z, x)
    yz = vol[:, :, lo(2) : hi(2)].mean(axis=2)  # (z, y)
    return [np.flipud(v) for v in (xy, xz, yz)]


def _u8(views: list[np.ndarray]) -> list[np.ndarray]:
    """One 1–99 % stretch for the three views of one volume, so their contrast compares."""
    flat = np.concatenate([v[np.isfinite(v)].ravel() for v in views])
    lo, hi = np.percentile(flat, [1.0, 99.0]) if flat.size else (0.0, 1.0)
    hi = hi if hi > lo else lo + 1.0
    return [
        (np.clip((np.nan_to_num(v, nan=(lo + hi) / 2) - lo) / (hi - lo), 0, 1) * 255).astype(np.uint8) for v in views
    ]


def turned_template(
    template: np.ndarray, tpl_apix: float, angles: tuple[float, float, float], box: int, apix: float
) -> np.ndarray:
    """The template turned to `angles` (rot, tilt, psi) and resampled onto a `box`³ grid of `apix` Å, centred."""
    from scipy.ndimage import affine_transform
    from scipy.spatial.transform import Rotation

    r1 = Rotation.from_euler("ZYZ", angles, degrees=True).as_matrix()
    m = (_P3 @ r1 @ _P3) * (apix / tpl_apix)
    c_out = np.full(3, (box - 1) / 2.0)
    c_tpl = (np.array(template.shape, dtype=float) - 1) / 2.0
    return affine_transform(
        template.astype(np.float32),
        m,
        offset=c_tpl - m @ c_out,
        output_shape=(box,) * 3,
        order=1,
        mode="constant",
        cval=float(np.median(template)),
    )


def render_loupe(
    recon: str,
    xyz_px: tuple[float, float, float],
    half_box: int,
    half_slab: int,
    apix: float,
    template: str | None,
    angles: tuple[float, float, float] | None,
    out: str,
) -> dict[str, Any]:
    """Write the loupe PNG for one pick to `out`: the pick's three views, and the template's under them when
    `template` and `angles` are given. Returns {"template": bool, "why": str}."""
    import mrcfile
    from PIL import Image

    x, y, z = (round(v) for v in xyz_px)
    n = 2 * half_box
    with mrcfile.mmap(recon, mode="r", permissive=True) as m:
        d = m.data
        box = np.full((n, n, n), np.nan, dtype=np.float32)
        lo = [max(0, c - half_box) for c in (z, y, x)]
        hi = [min(s, c + half_box) for s, c in zip(d.shape, (z, y, x), strict=True)]
        if any(h <= lo_ for lo_, h in zip(lo, hi, strict=True)):
            raise ValueError("the box around this pick lies outside its reconstruction")
        o = [lo_ - (c - half_box) for lo_, c in zip(lo, (z, y, x), strict=True)]
        box[o[0] : o[0] + hi[0] - lo[0], o[1] : o[1] + hi[1] - lo[1], o[2] : o[2] + hi[2] - lo[2]] = d[
            lo[0] : hi[0], lo[1] : hi[1], lo[2] : hi[2]
        ]
    rows = [_u8(_views(box, half_slab))]
    why = ""
    if template and angles is not None:
        try:
            with mrcfile.open(template, permissive=True) as t:
                tpl, tpl_apix = np.asarray(t.data, dtype=np.float32), float(t.voxel_size.x)
            if not tpl_apix > 0:
                why = "the template's pixel size is not in its header"
            else:
                rows.append(_u8(_views(turned_template(tpl, tpl_apix, angles, n, apix), half_slab)))
        except (OSError, ValueError) as e:
            why = f"the template could not be read: {e}"
    else:
        why = "no template-match angles for this pick" if template else "no template for this pick's source"
    w = 3 * LOUPE_PX + 2 * GAP_PX
    sheet = Image.new("L", (w, len(rows) * LOUPE_PX + (len(rows) - 1) * GAP_PX), 255)
    for r, views in enumerate(rows):
        for k, v in enumerate(views):
            img = Image.fromarray(v, mode="L").resize((LOUPE_PX, LOUPE_PX), Image.LANCZOS)
            sheet.paste(img, (k * (LOUPE_PX + GAP_PX), r * (LOUPE_PX + GAP_PX)))
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(out).with_name(f".{Path(out).name}.{os.getpid()}.tmp")
    sheet.save(tmp, format="PNG")
    os.replace(tmp, out)
    return {"template": len(rows) > 1, "why": why}
