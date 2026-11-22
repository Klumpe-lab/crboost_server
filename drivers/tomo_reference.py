"""The reference RELION's tomo refinement programs (relion_tomo_refine_ctf, relion_tomo_align) compare the tilt images
with: two half maps, their mask and their FSC, checked before RELION runs.

RELION's tomo reference loader pads a smaller reference to the box and compares it with the tilt images voxel for
voxel without reading its pixel size. A binned refinement's half maps would be compared at the wrong scale and give a
fit that is wrong without any error, so the half maps must be at the tilt series' pixel size, one grid with their
mask, and the FSC must have been measured on these very half maps.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import mrcfile
import pandas as pd
import starfile

# Pixel sizes agree when they differ by less than this (Å): headers round differently.
APIX_TOLERANCE = 1e-3


def geometry(path: Path) -> tuple[tuple[int, int, int], float]:
    with mrcfile.open(path, header_only=True, permissive=True) as m:
        h = m.header
        return (int(h.nx), int(h.ny), int(h.nz)), float(m.voxel_size.x)


def read_table(path: Path, block: str | None = None) -> pd.DataFrame:
    """One loop of a star file: the named block, else the only one."""
    blocks = starfile.read(path, always_dict=True)
    tables = {k: v for k, v in blocks.items() if isinstance(v, pd.DataFrame)}
    if block is not None and block in tables:
        return tables[block]
    if len(tables) == 1:
        return next(iter(tables.values()))
    raise ValueError(f"{path} holds {sorted(tables)}, not one table" + (f" named {block}" if block else ""))


def tilt_series_stars(tomograms_star: Path) -> dict[str, Path]:
    """Each tomogram's tilt-series star, as the tomograms star names it (relative to the star's folder if so)."""
    table = read_table(tomograms_star, "global")
    out = {}
    for name, ts_star in zip(table["rlnTomoName"], table["rlnTomoTiltSeriesStarFile"], strict=True):
        p = Path(str(ts_star))
        out[str(name)] = p if p.is_absolute() else (tomograms_star.parent / p)
    return out


def tilt_series_pixel_size(tomograms_star: Path) -> float:
    table = read_table(tomograms_star, "global")
    if "rlnTomoTiltSeriesPixelSize" not in table.columns:
        raise ValueError(f"{tomograms_star} has no rlnTomoTiltSeriesPixelSize: the tilt images' pixel size is unknown")
    sizes = sorted({round(float(v), 4) for v in table["rlnTomoTiltSeriesPixelSize"]})
    if len(sizes) != 1:
        raise ValueError(
            f"The tomograms in {tomograms_star} have tilt images at {sizes} Å/px; one reference serves one pixel size: "
            "refine each pixel size's tomograms in a job of its own"
        )
    return sizes[0]


@dataclass(frozen=True)
class Reference:
    half1: Path
    half2: Path
    mask: Path
    fsc: Path
    size: int  # its box, px
    apix: float
    fsc_resolution: float | None  # the PostProcess's masked FSC 0.143, Å

    def describe(self) -> str:
        res = f", masked FSC 0.143 at {self.fsc_resolution:.2f} Å" if self.fsc_resolution is not None else ""
        return f"{self.half1}, {self.half2.name} ({self.size} px at {self.apix:g} Å){res}"


def check_reference(half1: Path, half2: Path, mask: Path, fsc: Path, ts_apix: float, program: str) -> Reference:
    """The half maps and the mask one cubic grid at the tilt images' pixel size, and the FSC measured on these half
    maps; else a ValueError naming what to make instead."""
    g1, g2, gm = geometry(half1), geometry(half2), geometry(mask)
    for label, (box, apix) in (("half map 2", g2), ("mask", gm)):
        if box != g1[0] or abs(apix - g1[1]) > APIX_TOLERANCE:
            raise ValueError(
                f"The {label} is {box} at {apix:g} Å, half map 1 {g1[0]} at {g1[1]:g} Å: the reference's half maps "
                "and mask must be one grid"
            )
    ref_box, ref_apix = g1
    if len(set(ref_box)) != 1:
        raise ValueError(f"The half maps are {ref_box}: the reference must be a cube")
    if abs(ref_apix - ts_apix) > APIX_TOLERANCE:
        raise ValueError(
            f"The reference half maps are at {ref_apix:g} Å/px and the tilt images at {ts_apix:g} Å/px. "
            f"{program} compares the two voxel for voxel and does not rescale the reference: use half maps at the "
            "tilt series' pixel size (a Refine 3D or a Reconstruct particle at binning 1), with a mask and a "
            "PostProcess made from them"
        )

    fsc_general = starfile.read(fsc, always_dict=True).get("general")
    if fsc_general is None:
        raise ValueError(f"{fsc} has no data_general block: is it a PostProcess's postprocess.star?")
    fsc_row = fsc_general.iloc[0] if hasattr(fsc_general, "iloc") else fsc_general
    fsc_half1 = Path(str(fsc_row.get("rlnUnfilteredMapHalf1", "")))
    if fsc_half1.resolve() != half1.resolve():
        raise ValueError(
            f"The FSC ({fsc}) was measured on {fsc_half1}, the reference is {half1}: give the PostProcess of "
            "these half maps, whose FSC says how far the reference can be trusted"
        )
    fsc_res = fsc_row.get("rlnFinalResolution")
    return Reference(half1, half2, mask, fsc, ref_box[0], ref_apix, float(fsc_res) if fsc_res is not None else None)


def box_for(reference: Reference, box_size: int) -> int:
    """The box RELION cuts around each particle in the tilt images: `box_size`, or the reference's for 0. RELION only
    pads the reference up to the box, by the same width on each side."""
    box = box_size or reference.size
    if box < reference.size:
        raise ValueError(
            f"The box ({box} px) is smaller than the reference ({reference.size} px): RELION only pads the reference "
            "up to the box. Set a box of at least the reference's, or 0 for the reference's own"
        )
    if box % 2 or reference.size % 2:
        raise ValueError(
            f"The box ({box} px) and the reference ({reference.size} px) must both be even: RELION pads the reference "
            "by the same width on each side"
        )
    return box
