"""Warp's local tilt-series geometry for RELION's tomo programs (HOTFIXES H26; roadmap 53 §15.8–§15.9).

A Warp tilt-series XML holds the rigid per-tilt model the tilt-series star also holds (Angles, AxisAngle,
AxisOffsetX/Y), and what the star cannot hold: local image warps (GridMovementX/Y, written by miss-alignment's
[N,N] iterations), volume warps, level angles. Warp applies all of it (tsReconstruct, Warp export). RELION's programs
that cut particles from the tilt images (relion_tomo_reconstruct_particle, _refine_ctf, _subtomo, and frame
alignment when it comes) read the star alone. On data aligned with miss-alignment that placed every particle 8–17 Å
RMS off in every tilt: a bin-1 reference of 20 Å where the same particles give 9 Å, and a CTF refine that ran away.

The bridge: per particle and tilt, the offset that takes RELION's rigid placement to Warp's, as a RELION
trajectories file (`rlnTomoTrajectoriesFile`, the format Polish writes) named in an optimisation set the job hands
RELION. `containers/miss_alignment/warp_trajectories.py` computes it with warpylib, the code miss-alignment runs on;
`drivers/warp_bridge.py` runs it. This module finds each tilt series' XML and checks it against the star.

Checked (roadmap 53 §15.8): with the offsets, RELION's reconstruction from the tilt images equals the one from Warp's
stacks (9.2 against 9.1 Å; sign-flipped 20.3 Å); relion_refine ignores trajectories on 2D stacks and carries them
into its output set, as CTF refine does; RELION matches trajectories by particle name, so a file holding more
particles serves a subset. Nothing here assumes miss-alignment ran: an XML with nothing beyond the rigid model asks
for no trajectories, and the job runs as before.

After RELION's frame alignment (roadmap 53 §15.11) a set carries RELION's whole model instead: refined tilt-series
stars and RELION's own motion, which Warp's warps must not be added to. The frame alignment job marks its tomograms
star (`cbGeometry`, below); a marked set is handed on as it is, once its trajectories cover every particle and each
RELION-model tilt series still holds the rigid model its motion belongs to.
"""

from __future__ import annotations

import json
import math
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
import starfile

from services.analysis.alignment_changes import movement_grid_stats
from services.particles.coord_merge import _absolutize_paths
from services.particles.set_origins import TILT_DIR, Origin, origins_of, project_of, write_renamed_tilt_star
from services.subtomo_merge import (
    _parse_optimisation_set,
    _read_input_particles_lenient,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    optimisation_set_trajectories,
    write_optimisation_set,
)
from services.tilt_series.adapters.miss_align import read_xml_rows

WARP_GEOMETRY_TOOL = "miss_alignment"  # the container that holds warpylib
TRAJECTORIES_NAME = "warp_trajectories.star"
REPORT_NAME = "warp_trajectories.json"
MANIFEST_NAME = "warp_trajectories_manifest.json"
NAMED_PARTICLES_NAME = "warp_particles.star"
BRIDGED_SET_NAME = "warp_optimisation_set.star"
# In a job's directory: the tilt-series stars without the tilts that hold no alignment, and the set naming them.
ALIGNED_TILTS_DIR = "aligned_tilts"

# The origin marker, a column of the tomograms star: whose model a tilt series' star and the set's trajectories for
# its particles are. RELION keeps unknown labels through its programs ("will ignore (but maintain)"), so the marker
# travels with the set through CTF refine, extraction and refinement.
GEOMETRY_COL = "cbGeometry"
# For a RELION_MODEL row: the tilt-series star its motion was refined with (frame alignment's own output).
GEOMETRY_TILT_STAR_COL = "cbGeometryTiltStar"
RELION_MODEL = "relion"  # RELION's whole model (frame alignment): its tilt-series star and its motion
WARP_MODEL = "warp"  # Warp's rigid star; the set's trajectories hold the bridge's offsets where Warp has more
GEOMETRY_MARKS = (RELION_MODEL, WARP_MODEL)
# The rigid model in a tilt-series star; a CTF refine rewrites the star but none of these.
RIGID_STAR_COLS = ("rlnTomoXTilt", "rlnTomoYTilt", "rlnTomoZRot", "rlnTomoXShiftAngst", "rlnTomoYShiftAngst")
RIGID_SAME = 1e-3  # degrees or Å: the same model, written again
NO_STAR = "-"  # GEOMETRY_TILT_STAR_COL of a row that names none (a star loop holds no empty field)

# How far the star may sit from the XML's rigid model and still be its alignment (missAlign's emitted stars agree to
# ytilt exact, zrot <= 0.025 deg, shifts <= 1.2 A: roadmap 21).
RIGID_TOL = {"tilt_y_deg": 0.05, "z_rot_deg": 0.1, "x_shift_angstrom": 2.0, "y_shift_angstrom": 2.0}
STAR_COLUMNS = {
    "tilt_y_deg": "rlnTomoYTilt",
    "z_rot_deg": "rlnTomoZRot",
    "x_shift_angstrom": "rlnTomoXShiftAngst",
    "y_shift_angstrom": "rlnTomoYShiftAngst",
}
CENTRED = [f"rlnCenteredCoordinate{a}Angst" for a in "XYZ"]


@dataclass(frozen=True)
class SeriesGeometry:
    """One tilt series whose Warp model holds more than the star: what the container script needs."""

    name: str
    xml: Path
    tilt_star: Path
    movies: list[str]  # the star's rows, in order: RELION's frame index
    image_dims_ang: tuple[float, float]
    volume_dims_ang: tuple[float, float, float]
    local: str  # what lies beyond the rigid model, for the log

    def manifest_entry(self) -> dict:
        return {
            "name": self.name,
            "xml": str(self.xml),
            "movies": self.movies,
            "image_dims_ang": list(self.image_dims_ang),
            "volume_dims_ang": list(self.volume_dims_ang),
        }


@dataclass
class BridgePlan:
    input_set: Path
    particles_star: Path
    tomograms_star: Path
    trajectories: Path | None  # what the input set already names
    series: list[SeriesGeometry] = field(default_factory=list)  # local geometry to carry
    rigid_only: list[str] = field(default_factory=list)  # XML found, nothing beyond the rigid model
    unlocated: list[str] = field(default_factory=list)  # no Warp XML found
    relion_model: list[str] = field(default_factory=list)  # marked RELION_MODEL: frame alignment's model
    magnified: list[str] = field(default_factory=list)  # Warp's model holds a magnification (M's), RELION's none
    marked: bool = False  # every particle tomogram carries a geometry marker

    @property
    def needed(self) -> bool:
        return bool(self.series)


def first_table(path: Path, column: str) -> pd.DataFrame:
    for block in starfile.read(path, always_dict=True).values():
        if isinstance(block, pd.DataFrame) and column in block.columns:
            return block
    raise ValueError(f"{path}: no table with {column}")


def _abs(raw: str, base: Path) -> Path:
    p = Path(str(raw))
    return p if p.is_absolute() else (base / p).resolve()


def find_warp_xml(row: pd.Series, origin: Origin | None, tomograms_dir: Path) -> Path | None:
    """The Warp XML of a tomogram row's tilt series, wherever crboost keeps it: beside the tsCtf job's star
    (`<job>/warp_tiltseries/<name>.xml`), in a Warp export's staging (`<job>/.staging/task_<name>/warp_tiltseries/`,
    the copy Warp cut the stacks with), or, for a particle set's tomogram, beside its star in its own project."""
    name = str(row["rlnTomoName"])
    job = _abs(row["rlnTomoTiltSeriesStarFile"], tomograms_dir).parent.parent
    candidates = [
        job / "warp_tiltseries" / f"{name}.xml",
        job / ".staging" / f"task_{name}" / "warp_tiltseries" / f"{name}.xml",
    ]
    if origin is not None:
        candidates.append(Path(origin.tilt_star).parent.parent / "warp_tiltseries" / f"{origin.tomo}.xml")
    return next((c for c in candidates if c.is_file()), None)


def _grid_max(root: ET.Element, tag: str) -> float:
    el = root.find(tag)
    if el is None:
        return 0.0
    values = [abs(float(n.get("Value", "0"))) for n in el.findall("Node")]
    return max(values, default=0.0)


IDENTITY_MAG = (1.0, 0.0, 0.0, 1.0)


@dataclass(frozen=True)
class BeyondStar:
    """What a Warp XML holds, besides the rigid model and the local warps, that RELION's tilt-series star cannot:
    per-tilt stage angles (GridAngleX/Y/Z; M's --refine_stageangles), a magnification (M's --refine_mag; Warp's 2 x 2
    as the XML writes it) and Zernike terms of the CTF (M's --ctf_zernike*). Cs, which a tomogram row holds, is read
    along. None of them moves a particle in Warp (its positions come from the rigid model and the warps,
    TiltSeries.GetPositionInAllTilts): Warp applies the stage angles to each tilt's projection direction and the
    magnification and the Zernike terms where it back-projects, so its particle-series stacks carry none of them."""

    stage_angle_max_deg: float
    magnification: tuple[float, float, float, float]
    zernike_odd: tuple[float, ...]
    zernike_even: tuple[float, ...]
    cs_mm: float | None

    @property
    def magnified(self) -> bool:
        return any(abs(a - b) > 1e-6 for a, b in zip(self.magnification, IDENTITY_MAG, strict=True))

    @property
    def zernike(self) -> bool:
        return any(v != 0 for v in (*self.zernike_odd, *self.zernike_even))


def xml_ctf_params(root: ET.Element) -> dict[str, str]:
    """The <CTF> block's parameters of a tilt-series XML, by name."""
    ctf = root.find("CTF")
    return {p.get("Name"): p.get("Value") or "" for p in ctf.findall("Param")} if ctf is not None else {}


def _numbers(raw: str, sep: str) -> tuple[float, ...]:
    return tuple(float(v) for v in raw.replace(sep, " ").split()) if raw.strip() else ()


def beyond_star(xml: Path) -> BeyondStar:
    root = ET.parse(xml).getroot()
    raw = root.get("MagnificationCorrection") or "1, 0, 0, 1"
    mag = _numbers(raw, ",")
    if len(mag) != 4:
        raise ValueError(f"{xml}: MagnificationCorrection {raw!r} is not a 2 x 2")
    ctf = xml_ctf_params(root)
    cs = ctf.get("Cs", "").strip()
    return BeyondStar(
        stage_angle_max_deg=max(_grid_max(root, f"GridAngle{a}") for a in "XYZ"),
        magnification=(mag[0], mag[1], mag[2], mag[3]),
        zernike_odd=_numbers(ctf.get("ZernikeCoeffsOdd", ""), ";"),
        zernike_even=_numbers(ctf.get("ZernikeCoeffsEven", ""), ";"),
        cs_mm=float(cs) if cs else None,
    )


def magnification_parts(mag: tuple[float, float, float, float]) -> dict[str, float]:
    """A 2 x 2 magnification in percent: its isotropic part (the singular values' geometric mean), its anisotropic
    part (their ratio) and its diagonal as written."""
    m = np.array(mag, dtype=float).reshape(2, 2)
    s = np.linalg.svd(m, compute_uv=False)
    return {
        "isotropic_pct": (math.sqrt(s[0] * s[1]) - 1) * 100,
        "anisotropic_pct": (s[0] / s[1] - 1) * 100,
        "x_pct": (m[0, 0] - 1) * 100,
        "y_pct": (m[1, 1] - 1) * 100,
    }


def local_geometry(xml: Path) -> str | None:
    """What the XML holds beyond the rigid model the star can hold, in words; None when nothing. Raises for the
    per-tilt angle corrections, which the bridge cannot carry. A magnification is not geometry the bridge carries:
    Warp applies it only where it back-projects, never to a particle's position (`BeyondStar`; `plan` lists the
    tilt series that hold one)."""
    root = ET.parse(xml).getroot()
    parts = []
    grid = movement_grid_stats(xml)
    if grid is not None:
        parts.append(
            f"image warps {grid.dims}, {grid.rms_angstrom:.1f} Å RMS at the nodes, max {grid.max_angstrom:.1f}"
        )
    warp = max(_grid_max(root, f"GridVolumeWarp{a}") for a in "XYZ")
    if warp > 0:
        parts.append(f"volume warps up to {warp:.1f} Å")
    level = [float(root.get(f"LevelAngle{a}", "0") or 0) for a in "XY"]
    if any(level):
        parts.append(f"level angles {level[0]:.2f}°/{level[1]:.2f}°")
    angle = max(_grid_max(root, f"GridAngle{a}") for a in "XYZ")
    if angle > 0:
        raise ValueError(
            f"{xml} holds per-tilt angle corrections (GridAngle, up to {angle:.2f}°): a RELION trajectory can move a "
            "particle but not turn it, so RELION cannot be given this model"
        )
    return "; ".join(parts) or None


def unaligned_movies(tilts: pd.DataFrame, name: str) -> list[str]:
    """The movies of a tilt-series star's tilts that hold no rigid alignment. WarpTools ts_import leaves tilts out
    (its intensity walk from the zero tilt, its max tilt, its masked fraction; none can be turned off), and crboost's
    stars keep their rows with the alignment empty. Warp's model, reconstruction and exports have no such tilt, and
    RELION would project it with no geometry. A tilt holding part of an alignment is a broken star: raises."""
    cols = [c for c in RIGID_STAR_COLS if c in tilts.columns]
    if not cols:
        return []
    empty = tilts[cols].apply(pd.to_numeric, errors="coerce").isna()
    partial = empty.any(axis=1) & ~empty.all(axis=1)
    if partial.any():
        raise ValueError(f"{name}: {int(partial.sum())} tilts hold part of a rigid alignment ({', '.join(cols)})")
    return [Path(str(m)).name for m in tilts.loc[empty.all(axis=1), "rlnMicrographMovieName"]]


def _tilt_table(path: Path, name: str) -> tuple[str, pd.DataFrame]:
    for block, df in starfile.read(path, always_dict=True).items():
        if isinstance(df, pd.DataFrame) and "rlnMicrographMovieName" in df.columns:
            return str(block), df
    raise ValueError(f"{name}: {path} holds no per-tilt table")


def _check_visible_frames(particles_star: Path, kept: dict[str, int]) -> None:
    """Per-particle visibility (2D stacks) is one flag per tilt of the star it was cut against: it must count the
    tilts kept, or it belongs to the full star and cannot be read against the shorter one."""
    particles, _, _ = _read_input_particles_lenient(particles_star)
    if "rlnTomoVisibleFrames" not in particles.columns:
        return
    for name, n in kept.items():
        rows = particles[particles["rlnTomoName"].astype(str) == name]["rlnTomoVisibleFrames"].astype(str)
        lengths = {len(v.strip("[]").split(",")) for v in rows}
        if lengths - {n}:
            raise ValueError(
                f"{name}: its particles' rlnTomoVisibleFrames count {sorted(lengths)} tilts, the tilts that hold an "
                f"alignment are {n}: the images were cut against another tilt list"
            )


def drop_unaligned_tilts(input_set: Path, job_dir: Path) -> tuple[Path, dict[str, list[str]]]:
    """The set a RELION tilt-image program can read: `input_set` when every tilt of its tomograms holds a rigid
    alignment; else one in `job_dir/aligned_tilts/` whose tilt-series stars leave out the tilts that hold none
    (`unaligned_movies`), the rest as it was. Returns it and {tomogram: the movies left out}. Writes only into
    `job_dir`."""
    particles_star, tomograms_star = _parse_optimisation_set(input_set)
    tomograms = _read_tomograms_star(tomograms_star)
    base = tomograms_star.parent
    found: dict[str, tuple[Path, str, list[str], int]] = {}
    for _, row in tomograms.iterrows():
        name = str(row["rlnTomoName"])
        src = _abs(row["rlnTomoTiltSeriesStarFile"], base)
        block, tilts = _tilt_table(src, name)
        gone = unaligned_movies(tilts, name)
        if len(gone) == len(tilts):
            raise ValueError(f"{name}: no tilt of {src} holds an alignment")
        if gone:
            found[name] = (src, block, gone, len(tilts) - len(gone))
    if not found:
        return input_set, {}
    _check_visible_frames(particles_star, {n: v[3] for n, v in found.items()})
    out_dir = job_dir / ALIGNED_TILTS_DIR
    shutil.rmtree(out_dir, ignore_errors=True)  # an earlier attempt's copies must not pass for this one's
    project = project_of(tomograms_star, base)
    rows = []
    for _, row in tomograms.iterrows():
        row = _absolutize_paths(row, project)
        name = str(row["rlnTomoName"])
        if name in found:
            src, block, gone, _ = found[name]
            dst = out_dir / TILT_DIR / f"{name}.star"
            write_renamed_tilt_star(src, dst, block, name, project_of(src, project), drop=frozenset(gone))
            row["rlnTomoTiltSeriesStarFile"] = str(dst.resolve())
        rows.append(row)
    _write_tomograms_star(out_dir / "tomograms.star", pd.DataFrame(rows))
    out = out_dir / "optimisation_set.star"
    write_optimisation_set(
        out,
        particles_star=particles_star,
        tomograms_star=out_dir / "tomograms.star",
        trajectories_star=optimisation_set_trajectories(input_set),
    )
    return out, {n: v[2] for n, v in found.items()}


def check_rigid_agrees(xml: Path, tilts: pd.DataFrame, name: str) -> None:
    """The star must hold the XML's rigid model, tilt by tilt (matched by movie): else the XML is not this star's
    alignment and its warps would be added to some other geometry."""
    rows = {r["movie"]: r for r in read_xml_rows(xml)}
    worst: dict[str, float] = {}
    for _, t in tilts.iterrows():
        movie = Path(str(t["rlnMicrographMovieName"])).name
        r = rows.get(movie) or next((v for k, v in rows.items() if Path(k).stem == Path(movie).stem), None)
        if r is None:
            raise ValueError(f"{name}: the star's tilt {movie} has no tilt in {xml}")
        for key, col in STAR_COLUMNS.items():
            worst[key] = max(worst.get(key, 0.0), abs(float(t[col]) - r[key]))
    off = {k: v for k, v in worst.items() if v > RIGID_TOL[k]}
    if off:
        raise ValueError(
            f"{name}: the tilt-series star and {xml} disagree on the rigid alignment "
            f"({', '.join(f'{k} by {v:.3g}' for k, v in off.items())}): the XML is not this star's alignment"
        )


def _dims(row: pd.Series, xml: Path, tilts: pd.DataFrame, tomograms_dir: Path, name: str):
    """Image and volume extents in Å, as RELION has them (the tilt images, the tomogram row), checked against
    what the XML says when it says anything."""
    apix = float(row["rlnTomoTiltSeriesPixelSize"])
    volume = tuple(float(row[f"rlnTomoSize{a}"]) * apix for a in "XYZ")
    root = ET.parse(xml).getroot()

    def attr(key: str) -> list[float]:
        raw = root.get(key) or ""
        vals = [float(v) for v in raw.replace(",", " ").split()]
        return vals if vals and any(vals) else []

    image: tuple[float, ...] = ()
    micrograph = _abs(tilts["rlnMicrographName"].iloc[0], tomograms_dir)
    if micrograph.is_file():
        with mrcfile.open(micrograph, header_only=True, permissive=True) as m:
            image = (float(m.header.nx) * apix, float(m.header.ny) * apix)
    xml_image, xml_volume = attr("ImageDimensionsAngstrom"), attr("VolumeDimensionsAngstrom")
    if not image:
        if not xml_image:
            raise ValueError(f"{name}: neither the tilt image {micrograph} nor {xml} gives the image extent")
        image = tuple(xml_image[:2])
    for label, ours, theirs in (("image", image, xml_image), ("volume", volume, xml_volume)):
        if theirs and any(abs(a - b) > 1.01 * apix for a, b in zip(ours, theirs, strict=False)):
            raise ValueError(
                f"{name}: the {label} is {[round(v, 1) for v in ours]} Å for RELION and "
                f"{[round(v, 1) for v in theirs]} Å in {xml}: one of them describes another geometry"
            )
    return (image[0], image[1]), volume


def plan(input_set: Path) -> BridgePlan:
    """What the bridge has to carry for an optimisation set's particles. Reads, writes nothing."""
    particles_star, tomograms_star = _parse_optimisation_set(input_set)
    p = BridgePlan(input_set, particles_star, tomograms_star, optimisation_set_trajectories(input_set))
    particles, _, _ = _read_input_particles_lenient(particles_star)
    if particles.empty:
        return p
    tomograms = _read_tomograms_star(tomograms_star)
    origins = origins_of(tomograms) or {}
    rows = tomograms.set_index(tomograms["rlnTomoName"].astype(str))
    names = sorted(set(particles["rlnTomoName"].astype(str)))
    marks = {}
    for name in names:
        if name not in rows.index:
            raise ValueError(f"The particles name tomogram {name}, absent from {tomograms_star}")
        marks[name] = geometry_mark(rows.loc[name])
    p.marked = all(m in GEOMETRY_MARKS for m in marks.values())
    for name in names:
        row = rows.loc[name]
        if marks[name] == RELION_MODEL:
            check_relion_rigid(row, tomograms_star.parent, name)
            p.relion_model.append(name)
            continue
        xml = find_warp_xml(row, origins.get(name), tomograms_star.parent)
        if xml is None:
            p.unlocated.append(name)
            continue
        local = local_geometry(xml)
        if beyond_star(xml).magnified:
            p.magnified.append(name)
        if local is None:
            p.rigid_only.append(name)
            continue
        tilt_star = _abs(row["rlnTomoTiltSeriesStarFile"], tomograms_star.parent)
        tilts = first_table(tilt_star, "rlnMicrographMovieName")
        check_rigid_agrees(xml, tilts, name)
        image, volume = _dims(row, xml, tilts, tomograms_star.parent, name)
        movies = [Path(str(m)).name for m in tilts["rlnMicrographMovieName"]]
        p.series.append(SeriesGeometry(name, xml, tilt_star, movies, image, volume, local))
    if p.series:
        missing = [c for c in CENTRED if c not in particles.columns]
        if missing:
            raise ValueError(
                f"{particles_star} lacks {missing}: the particles' positions in Å from the tomogram centre are what "
                "Warp's local geometry is evaluated at"
            )
    return p


def geometry_mark(row: pd.Series) -> str:
    """A tomogram row's origin marker, "" when it has none (an unmarked set, or a row a merge left empty)."""
    value = row.get(GEOMETRY_COL)
    return value.strip() if isinstance(value, str) else ""


def relion_model_names(tomograms: pd.DataFrame) -> list[str]:
    """The tomograms marked as carrying RELION's whole model (frame alignment)."""
    if GEOMETRY_COL not in tomograms.columns:
        return []
    marked = tomograms[tomograms[GEOMETRY_COL].astype(str).str.strip() == RELION_MODEL]
    return sorted(marked["rlnTomoName"].astype(str))


def _rigid_rows(star: Path) -> pd.DataFrame:
    tilts = first_table(star, "rlnMicrographName")
    missing = [c for c in RIGID_STAR_COLS if c not in tilts.columns]
    if missing:
        raise ValueError(f"{star} has no {missing}: no rigid model to compare")
    return tilts


def check_relion_rigid(row: pd.Series, tomograms_dir: Path, name: str) -> None:
    """A RELION_MODEL row's tilt-series star must hold the rigid model its motion was refined with (the star
    `GEOMETRY_TILT_STAR_COL` names): a CTF refine rewrites the star and keeps it; anything else would apply the motion
    to another model."""
    star = _abs(row["rlnTomoTiltSeriesStarFile"], tomograms_dir)
    raw = row.get(GEOMETRY_TILT_STAR_COL)
    if not isinstance(raw, str) or raw.strip() in ("", NO_STAR):
        raise ValueError(
            f"{name} is marked {RELION_MODEL!r} but names no {GEOMETRY_TILT_STAR_COL}: its model is unknown"
        )
    ref = _abs(raw.strip(), tomograms_dir)
    if star.resolve() == ref.resolve():
        return
    if not ref.is_file():
        raise ValueError(f"{name}: {ref}, the tilt-series star its motion belongs to, is gone")
    now, then = _rigid_rows(star), _rigid_rows(ref)
    by_name = then.set_index(then["rlnMicrographName"].astype(str))
    worst = 0.0
    for _, t in now.iterrows():
        key = str(t["rlnMicrographName"])
        if key not in by_name.index:
            raise ValueError(f"{name}: tilt {key} of {star} is not in {ref}, the tilt series its motion belongs to")
        worst = max(worst, *(abs(float(t[c]) - float(by_name.loc[key, c])) for c in RIGID_STAR_COLS))
    if worst > RIGID_SAME:
        raise ValueError(
            f"{name}: {star} holds another rigid model than {ref} (off by {worst:.3g}), the one the set's motion was "
            "refined with: RELION would apply that motion to another geometry"
        )


def check_marked(p: BridgePlan) -> None:
    """A set that carries RELION's model somewhere is handed on as it is only when every particle tomogram is
    marked and every particle has a trajectory (RELION's motion, or the bridge's offsets for the `warp` rows)."""
    if not p.marked:
        raise ValueError(
            f"{len(p.relion_model)} tilt series carry RELION's own model from a frame alignment "
            f"({', '.join(p.relion_model[:3])}{' …' if len(p.relion_model) > 3 else ''}) and others carry no "
            f"{GEOMETRY_COL} marker: one set cannot hold both, since RELION would place one kind with the other's "
            "trajectories. Take the set from one frame alignment's output"
        )
    if not (p.relion_model or p.series):
        return
    if p.trajectories is None or not p.trajectories.is_file():
        raise ValueError(
            f"{p.input_set} names no trajectories file, but its tilt series carry RELION's motion or Warp's local "
            "geometry: the set lost its trajectories"
        )
    names = particle_names(p.particles_star)
    if names is None:
        raise ValueError(f"{p.particles_star} has no rlnTomoParticleName: RELION matches trajectories by name")
    missing = sorted(set(names) - trajectory_names(p.trajectories))
    if missing:
        raise ValueError(
            f"{len(missing)} particles of {p.particles_star} have no trajectory in {p.trajectories} "
            f"({', '.join(missing[:3])}{' …' if len(missing) > 3 else ''})"
        )


def require_images(particles_star: Path) -> None:
    """A refinement reads 2D stacks: refuse particles that name none. Frame alignment's output names none on
    purpose: its stacks were cut with the old model, so the set goes through Subtomo Extraction first."""
    particles, _, _ = _read_input_particles_lenient(particles_star)
    if "rlnImageName" not in particles.columns:
        raise ValueError(
            f"{particles_star} names no particle images (rlnImageName). A frame alignment's output has none, since "
            "its geometry changed after the stacks were cut: cut the set anew with Subtomo Extraction and refine that"
        )


def trajectory_names(path: Path) -> set[str]:
    """The particle names a trajectories file holds a block for (a line scan: the file can be large)."""
    names = set()
    with open(path) as f:
        for line in f:
            if line.startswith("data_") and not line.startswith("data_general"):
                names.add(line[5:].strip())
    return names


def particle_names(particles_star: Path) -> pd.Series | None:
    particles, _, _ = _read_input_particles_lenient(particles_star)
    return particles["rlnTomoParticleName"].astype(str) if "rlnTomoParticleName" in particles.columns else None


def carried(p: BridgePlan) -> bool:
    """True when the input set already names this bridge's trajectories for every one of its particles (a set
    downstream of a bridged job: relion_refine and CTF refine carry the line)."""
    if p.trajectories is None or p.trajectories.name != TRAJECTORIES_NAME or not p.trajectories.is_file():
        return False
    names = particle_names(p.particles_star)
    return names is not None and set(names) <= trajectory_names(p.trajectories)


def named_particles(p: BridgePlan, job_dir: Path) -> Path:
    """The particles star with a name per particle. RELION matches trajectories by rlnTomoParticleName; a set that
    has none (template-matching candidates) gets `<tomogram>/<n>` (n from 1 in file order per tomogram), written
    beside the job's other bridge files. A set with unique names is used as it is."""
    particles, optics, general = _read_input_particles_lenient(p.particles_star)
    names = particles.get("rlnTomoParticleName")
    if names is not None and not names.astype(str).duplicated().any():
        return p.particles_star
    tomo = particles["rlnTomoName"].astype(str)
    particles = particles.copy()
    particles["rlnTomoParticleName"] = tomo + "/" + (particles.groupby(tomo).cumcount() + 1).astype(str)
    out = job_dir / NAMED_PARTICLES_NAME
    _write_particles_star(out, optics_df=optics, particles_df=particles, general_kv=general)
    return out


def write_manifest(p: BridgePlan, job_dir: Path, particles_star: Path) -> Path:
    path = job_dir / MANIFEST_NAME
    path.write_text(
        json.dumps(
            {
                "particles": str(particles_star),
                "out": str(job_dir / TRAJECTORIES_NAME),
                "report": str(job_dir / REPORT_NAME),
                "series": [s.manifest_entry() for s in p.series],
            },
            indent=1,
        )
    )
    return path


def write_bridged_set(p: BridgePlan, job_dir: Path, particles_star: Path) -> Path:
    out = job_dir / BRIDGED_SET_NAME
    write_optimisation_set(
        out,
        particles_star=particles_star,
        tomograms_star=p.tomograms_star,
        trajectories_star=job_dir / TRAJECTORIES_NAME,
    )
    return out


def report_lines(job_dir: Path, p: BridgePlan) -> list[str]:
    """One line per tilt series (what its XML holds, how far that moves its particles) and a summary."""
    report = json.loads((job_dir / REPORT_NAME).read_text())
    local = {s.name: s.local for s in p.series}
    lines = []
    for name, s in report["series"].items():
        lines.append(
            f"{name}: {local.get(name, '?')}; at its {s['particles']} particles x {s['frames']} tilts "
            f"{s['offset_rms_ang']:.1f} Å RMS, largest {s['offset_max_ang']:.1f} Å"
        )
    rms = [s["offset_rms_ang"] for s in report["series"].values()]
    if rms:
        lines.append(
            f"{report['particles']} particles over {len(rms)} tilt series, "
            f"{min(rms):.1f}–{max(rms):.1f} Å RMS per series, written to {job_dir / TRAJECTORIES_NAME}"
        )
    return lines
