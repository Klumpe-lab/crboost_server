"""Warp's M for a particle set: its population's sources, the particles M reads, and the set M hands back.

Files only; drivers/m_refine.py runs MTools and MCore.

**Sources.** M writes its refinements into a source's XMLs, so a source is a copy, in the job's folder, of the tilt
series the particles sit on: one per Warp settings file (a tsCtf job in its project), found as Warp Export finds
them (`warp_export.tsctf_sources`, through the set's origins when it records them). A source folder holds

    warp_tiltseries.settings   the tsCtf job's, DataFolder and ProcessingFolder absolute and inside the copy (MTools
                               resolves relative folders against its working directory; M writes into the second)
    tomostar/<ts>.tomostar     the tsCtf job's, movie paths absolute
    warp_tiltseries/<ts>.xml   the tsCtf job's XML (Warp's whole model), DataDirectory pointed at tomostar/;
                               create_source writes <source>.source beside them
    files.txt                  one <ts>.tomostar per line (create_source --files reads only this form)
    tilt_series/<ts>.star      after a round: the tsCtf job's per-tilt star with M's per-tilt CTF
    m_source.json              what the folder holds and where each piece came from

Each tilt series is checked before M starts: its volume three ways (the settings, the tomogram row, the XML), the
files M opens for every tilt (the average, the frame series' XML, the raw movie that XML names), and the tomostar's
movies against the XML's.

**The particles M reads.** create_species --particles_relion reads legacy columns only: positions in Å from the
tomogram's corner, centred - origin + volume / 2 (RELION 5 and Warp both centre a tomogram at half its size, and M
takes the volume from the source's settings), the angles as they are, the halves, the tilt series by its name in its
source. Columns not starting with rln ride along into M's particle star; cbTomoParticleName keys the set back.

**The set M hands back.** The rows that went in, with M's positions (corner - volume / 2, origins zero) and angles,
the halves checked, and no images: the stacks were cut with another model. Its tomograms are the tilt series M
refined; each names its source copy's per-tilt star and carries the XML's Cs, voltage and amplitude contrast. A set
that records origins names that star as the origin's, so Warp Export and the bridge find M's XML beside it. The set
carries no geometry marker and no trajectories: the bridge computes its offsets from M's XMLs.

**Several species.** A population holds any number; each has its own particles, map and mask, and every round
refines the tilt series' model from all of them. A species whose particles M holds in several temporal samples hands
RELION one pose a particle, the mean of its samples. A species whose symmetry M expanded holds one row per copy;
`label_copies` names each copy after its parent (`<name>_<k>`, cbParentName, cbParentUid, cbSubIndex, a new
cbPickUid), so the set back holds every copy, the parent's row under each.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
import starfile

from services.configs.metadata_service import WarpXmlParser
from services.particles import warp_geometry as wg
from services.particles.coords import CENTERED_COLS
from services.particles.pick_uids import parse_cell, uid_cell
from services.particles.poses import euler_angles
from services.particles.set_origins import ORIGIN_TILT_STAR, origins_of, project_of, tilt_block, write_renamed_tilt_star
from services.particles.warp_export import (
    ANGLE_COLS,
    ORIGIN_ANGST_COLS,
    ORIGIN_PIXEL_COLS,
    SUBTOMO_ANGLE_COLS,
    TOMOSTAR_SUFFIX,
    WarpVolume,
    check_volume_agrees,
    read_warp_volume,
    settings_with_folders,
    tsctf_sources,
)
from services.subtomo_merge import (
    _read_input_particles_lenient,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    write_optimisation_set,
)
from services.tilt_series.adapters.ts_ctf import warp_defocus_uv
from services.tilt_series.models import frame_id_to_warp_key

# In the job's folder.
M_DIR = "m"
POPULATION = "m"  # m/m.population
SOURCES_DIR = "sources"  # m/sources/<source>/
SOURCES_RECORD = "sources.json"  # m/sources.json
IMPORT_STAR = "particles_in.star"  # m/particles_in.star
BINARY_MASK = "mask_binary.mrc"  # m/mask_binary.mrc
# In a source folder.
SETTINGS_NAME = "warp_tiltseries.settings"
TOMOSTAR_DIR = "tomostar"
PROCESSING_DIR = "warp_tiltseries"
TILT_DIR = "tilt_series"
FILES_NAME = "files.txt"
SOURCE_RECORD = "m_source.json"
# A Mask job's own files beside its mask.mrc (drivers/mask_create.py).
MASK_LOWPASS = "map_lowpass.mrc"
MASK_STATS = "mask_stats.json"

TOMO = "rlnTomoName"
NAME = "rlnTomoParticleName"
SUBSET = "rlnRandomSubset"
NAME_COL = "cbTomoParticleName"  # carried through M: the key of the set back
UID_COL = "cbPickUid"
# An expanded species' copies (label_copies), carried through M like the name; as subparticles.py names them.
PARENT_NAME_COL = "cbParentName"
PARENT_UID_COL = "cbParentUid"
INDEX_COL = "cbSubIndex"
ASSEMBLY_COL = "cbAssembly"
LEGACY_COORDS = ["rlnCoordinateX", "rlnCoordinateY", "rlnCoordinateZ"]
M_POS = [f"wrpCoordinate{a}1" for a in "XYZ"]
M_ANGLES = [f"wrpAngle{a}1" for a in ("Rot", "Tilt", "Psi")]
M_SUBSET = "wrpRandomSubset"
M_SOURCE = "wrpSourceName"
CTF_COLS = ("rlnDefocusU", "rlnDefocusV", "rlnDefocusAngle", "rlnCtfAstigmatism")
DATA_DIR_RE = re.compile(r'DataDirectory="[^"]*"')
_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


def population_path(job_dir: Path) -> Path:
    return job_dir / M_DIR / f"{POPULATION}.population"


def safe_name(text: str) -> str:
    """A name M can use in file names."""
    return _SAFE.sub("_", text).strip("_") or "species"


# ── Warp's files


def xml_movies(xml: Path) -> list[str]:
    """The tilt series' movies in Warp's tilt order (MoviePath), as written (absolute)."""
    node = ET.parse(xml).getroot().find("MoviePath")
    return [line.strip() for line in (node.text or "").splitlines() if line.strip()] if node is not None else []


def _read_tomostar(path: Path) -> tuple[list[str], list[list[str]]]:
    header, rows = [], []
    for line in path.read_text().splitlines():
        s = line.strip()
        if s.startswith("_"):
            header.append(s.split()[0])
        elif s and not s.startswith(("data_", "loop_", "#")):
            rows.append(s.split())
    if "_wrpMovieName" not in header:
        raise ValueError(f"{path} has no _wrpMovieName column")
    return header, rows


def tomostar_movies(path: Path) -> list[str]:
    """A tomostar's movies, absolute (a relative one is read against the tomostar's folder, as Warp reads it)."""
    header, rows = _read_tomostar(path)
    col = header.index("_wrpMovieName")
    return [r[col] if Path(r[col]).is_absolute() else os.path.normpath(path.parent / r[col]) for r in rows]


def write_tomostar_absolute(src: Path, dst: Path) -> None:
    header, rows = _read_tomostar(src)
    col = header.index("_wrpMovieName")
    for row, movie in zip(rows, tomostar_movies(src), strict=True):
        row[col] = movie
    lines = ["", "data_", "", "loop_", *[f"{h} #{i + 1}" for i, h in enumerate(header)]]
    lines += ["  ".join(r) for r in rows]
    dst.write_text("\n".join(lines) + "\n")


def write_xml_copy(src: Path, dst: Path, tomostar_dir: Path) -> None:
    """The XML as it is, its DataDirectory pointed at the copy's tomostar folder."""
    text, n = DATA_DIR_RE.subn(f'DataDirectory="{tomostar_dir}"', src.read_text(), count=1)
    if n != 1:
        raise ValueError(f"{src} has no DataDirectory attribute")
    dst.write_text(text)


def _xml_volume_ang(xml: Path) -> tuple[float, ...]:
    raw = ET.parse(xml).getroot().get("VolumeDimensionsAngstrom") or ""
    vals = tuple(float(v) for v in raw.replace(",", " ").split())
    return vals if len(vals) == 3 and any(vals) else ()


def check_reads(name: str, movies: list[str]) -> None:
    """What M opens for each tilt, read only: the tilt average, the frame series' XML, and the raw movie that XML's
    DataDirectory names (its header gives the dimensions, its first 512 kB the tilt series' data hash)."""
    for m in movies:
        mp = Path(m)
        avg = mp.parent / "average" / f"{mp.stem}.mrc"
        frame_xml = mp.parent / f"{mp.stem}.xml"
        for p in (avg, frame_xml):
            if not p.is_file():
                raise FileNotFoundError(f"{name}: no {p} (M reads every tilt's average and frame-series XML)")
        data_dir = ET.parse(frame_xml).getroot().get("DataDirectory") or str(mp.parent)
        raw = Path(data_dir) / mp.name
        if not raw.exists():
            raise FileNotFoundError(f"{name}: the raw movie {raw} is gone (M reads its header and its data hash)")


def _settings_params(path: Path) -> dict[str, dict[str, str]]:
    root = ET.parse(path).getroot()
    return {sec.tag: {p.get("Name"): p.get("Value") or "" for p in sec.findall("Param")} for sec in root}


def _check_settings(path: Path) -> None:
    """A settings file whose gain or defects correction is on names a file; the copy keeps that path, so it must be
    absolute and there."""
    imp = _settings_params(path).get("Import", {})
    for flag, key in (("CorrectGain", "GainPath"), ("CorrectDefects", "DefectsPath")):
        if imp.get(flag) == "True":
            p = Path(imp.get(key, ""))
            if not p.is_absolute() or not p.is_file():
                raise ValueError(f"{path}: {flag} is on and {key} is {str(p)!r}, which a copy elsewhere cannot read")


# ── the plan


@dataclass
class SeriesPlan:
    name: str  # in the set (rlnTomoName)
    item: str  # in its source: its name in its own project (the XML's, the tomostar's)
    xml_from: str
    tomostar_from: str
    tilt_star_from: str  # the per-tilt star of the XML's model, its block named `item`
    particles: int
    tilts: int


@dataclass
class SourcePlan:
    name: str
    project: str
    settings_from: str
    apix: float
    dims: tuple[int, int, int]
    series: list[SeriesPlan] = field(default_factory=list)


def plan_sources(particles: pd.DataFrame, tomograms: pd.DataFrame, tomograms_star: Path) -> list[SourcePlan]:
    """One source per Warp settings file behind the set's tilt series with particles, every tilt series checked.
    Reads, writes nothing; raises naming the tilt series and what is wrong."""
    if particles.empty:
        raise ValueError("The set holds no particles")
    counts = particles[TOMO].astype(str).value_counts()
    rows = tomograms.set_index(tomograms[TOMO].astype(str), drop=False)
    absent = sorted(set(counts.index) - set(rows.index))
    if absent:
        raise ValueError(f"The particles name tomograms absent from {tomograms_star}: {absent[:5]}")
    relion = [t for t in wg.relion_model_names(tomograms) if t in counts.index]
    if relion:
        raise ValueError(
            f"{len(relion)} tilt series carry RELION's model from a frame alignment ({', '.join(relion[:3])}): M "
            "reads Warp's model, and these particles were placed in RELION's"
        )
    origins = origins_of(tomograms) or {}
    by_settings: dict[str, SourcePlan] = {}
    items: dict[str, str] = {}
    for name in sorted(counts.index):
        row = rows.loc[name]
        origin = origins.get(name)
        src = tsctf_sources(row, SETTINGS_NAME, TOMOSTAR_DIR, origin=origin)
        volume = read_warp_volume(src.settings)
        check_volume_agrees(row, volume)
        row_dims = tuple(round(float(row[f"rlnTomoSize{a}"])) for a in "XYZ")
        if row_dims != volume.dims:
            raise ValueError(
                f"{name}: the tomogram is {list(row_dims)} px in its row and {list(volume.dims)} px in "
                f"{src.settings}; M places the particles in the settings' volume, so the two must agree"
            )
        xml_volume = _xml_volume_ang(src.xml)
        ours = np.asarray(volume.dims, dtype=float) * volume.apix
        if xml_volume and np.abs(np.asarray(xml_volume) - ours).max() > 1.01 * volume.apix:
            raise ValueError(f"{name}: {src.xml} says the volume is {list(xml_volume)} Å, its settings {list(ours)} Å")
        item = origin.tomo if origin is not None else name
        if items.setdefault(item, name) != name:
            raise ValueError(
                f"Two tilt series of the set are named {item} in their projects ({items[item]}, {name}): M matches "
                "particles to tilt series by that name"
            )
        movies = xml_movies(src.xml)
        if not movies:
            raise ValueError(f"{name}: {src.xml} lists no movies")
        if tomostar_movies(src.tomostar) != movies:
            raise ValueError(f"{name}: the movies of {src.tomostar} are not {src.xml}'s MoviePath list, in order")
        check_reads(name, movies)
        key = str(src.settings.resolve())
        plan = by_settings.get(key)
        if plan is None:
            _check_settings(src.settings)
            project = origin.project if origin is not None else str(project_of(src.tilt_star, tomograms_star.parent))
            plan = by_settings[key] = SourcePlan("", project, str(src.settings), volume.apix, volume.dims)
        plan.series.append(
            SeriesPlan(name, item, str(src.xml), str(src.tomostar), str(src.tilt_star), int(counts[name]), len(movies))
        )
    plans = sorted(by_settings.values(), key=lambda p: (p.project, p.settings_from))
    taken: set[str] = set()
    shared = [p for p in plans if sum(q.project == p.project for q in plans) > 1]
    for p in plans:
        base = safe_name(Path(p.project).name)
        if p in shared:
            base = f"{base}_{safe_name(Path(p.settings_from).parent.name)}"
        p.name, n = base, 2
        while p.name in taken:
            p.name, n = f"{base}_{n}", n + 1
        taken.add(p.name)
    return plans


def _source_record(plan: SourcePlan, folder: Path) -> dict:
    record = asdict(plan)
    record["folder"] = str(folder)
    record["dims"] = list(plan.dims)
    for s in record["series"]:
        s["xml_before"] = s["xml_from"]  # the model this round starts from
    return record


def rename_apart(plans: list[SourcePlan], taken: set[str]) -> None:
    """Rename the plans whose names a population's sources already take (`<name>_2`, …), in place."""
    taken = set(taken)
    for p in plans:
        base, n = p.name, 2
        while p.name in taken:
            p.name, n = f"{base}_{n}", n + 1
        taken.add(p.name)


def write_sources(plans: list[SourcePlan], m_dir: Path, existing: dict | None = None) -> dict:
    """The source folders under `m_dir/sources/`, and their record (`m_dir/sources.json`, with `existing`'s sources
    before them when given), which it returns."""
    sources = dict((existing or {}).get("sources", {}))
    clash = sorted({p.name for p in plans} & set(sources))
    if clash:
        raise ValueError(f"The population has sources named {clash} already")
    for plan in plans:
        d = m_dir / SOURCES_DIR / plan.name
        (d / TOMOSTAR_DIR).mkdir(parents=True)
        (d / PROCESSING_DIR).mkdir()
        settings = Path(plan.settings_from)
        (d / SETTINGS_NAME).write_text(
            settings_with_folders(settings.read_text(), str(d / TOMOSTAR_DIR), str(d / PROCESSING_DIR), settings)
        )
        for s in plan.series:
            write_tomostar_absolute(Path(s.tomostar_from), d / TOMOSTAR_DIR / f"{s.item}{TOMOSTAR_SUFFIX}")
            write_xml_copy(Path(s.xml_from), d / PROCESSING_DIR / f"{s.item}.xml", d / TOMOSTAR_DIR)
        (d / FILES_NAME).write_text("".join(f"{s.item}{TOMOSTAR_SUFFIX}\n" for s in plan.series))
        record = _source_record(plan, d)
        (d / SOURCE_RECORD).write_text(json.dumps(record, indent=1))
        sources[plan.name] = record
    manifest = {"sources": sources}
    (m_dir / SOURCES_RECORD).write_text(json.dumps(manifest, indent=1))
    return manifest


def series_index(manifest: dict) -> dict[str, tuple[dict, dict]]:
    """{name in the set: (its source's record, its own)}."""
    return {s["name"]: (src, s) for src in manifest["sources"].values() for s in src["series"]}


def source_volume(source: dict) -> WarpVolume:
    return WarpVolume(apix=float(source["apix"]), dims=tuple(int(v) for v in source["dims"]))


# ── a follow-up round's copy


def species_params(path: Path) -> dict[str, str]:
    return {p.get("Name"): p.get("Value") or "" for p in ET.parse(path).getroot().findall("Param")}


def species_files(population: Path) -> dict[str, Path]:
    """Every species of an M population, by its name (the species file's Name, else the file's stem)."""
    out: dict[str, Path] = {}
    for s in ET.parse(population).getroot().iter("Species"):
        rel = s.get("Path")
        if not rel:
            continue
        path = (population.parent / rel).resolve()
        name = species_params(path).get("Name") or path.stem
        if name in out:
            raise ValueError(f"{population} names two species {name!r}")
        out[name] = path
    if not out:
        raise ValueError(f"{population} names no species")
    return out


def temporal_samples(species: Path) -> int:
    """How many samples M holds of each particle's pose along the exposure."""
    p = species_params(species)
    return max(int(p.get("TemporalResolutionMovement") or 1), int(p.get("TemporalResolutionRotation") or 1))


def particles_star(species: Path) -> Path:
    """The species' particle star, which M reads and writes beside the species file."""
    return species.parent / f"{species_params(species).get('Name') or species.stem}_particles.star"


def copy_round(parent_dir: Path, job_dir: Path) -> dict:
    """An earlier round's population, every species (its files as they are now) and sources (the settings' folders
    and the XMLs' DataDirectory pointed into this job) under `job_dir/m/`; returns the sources' record, whose
    `xml_before` names the parent's XMLs. Nothing of the parent is changed."""
    src_m, dst_m = parent_dir / M_DIR, job_dir / M_DIR
    population = population_path(parent_dir)
    dst_m.mkdir(parents=True)
    shutil.copy2(population, dst_m / population.name)
    for species in species_files(population).values():
        species_dst = dst_m / species.parent.relative_to(src_m.resolve())
        species_dst.mkdir(parents=True)
        for f in species.parent.iterdir():
            if f.is_file():
                shutil.copy2(f, species_dst / f.name)
    parent = json.loads((src_m / SOURCES_RECORD).read_text())
    sources = {}
    for name, rec in parent["sources"].items():
        s, d = Path(rec["folder"]), dst_m / SOURCES_DIR / name
        (d / TOMOSTAR_DIR).mkdir(parents=True)
        (d / PROCESSING_DIR).mkdir()
        (d / SETTINGS_NAME).write_text(
            settings_with_folders(
                (s / SETTINGS_NAME).read_text(), str(d / TOMOSTAR_DIR), str(d / PROCESSING_DIR), s / SETTINGS_NAME
            )
        )
        for f in (s / TOMOSTAR_DIR).iterdir():
            shutil.copy2(f, d / TOMOSTAR_DIR / f.name)
        for f in (s / PROCESSING_DIR).glob("*.source"):
            shutil.copy2(f, d / PROCESSING_DIR / f.name)
        series = []
        for ser in rec["series"]:
            before = s / PROCESSING_DIR / f"{ser['item']}.xml"
            write_xml_copy(before, d / PROCESSING_DIR / before.name, d / TOMOSTAR_DIR)
            series.append(dict(ser, xml_before=str(before)))
        shutil.copy2(s / FILES_NAME, d / FILES_NAME)
        record = dict(rec, folder=str(d), series=series)
        (d / SOURCE_RECORD).write_text(json.dumps(record, indent=1))
        sources[name] = record
    manifest = {"sources": sources}
    (dst_m / SOURCES_RECORD).write_text(json.dumps(manifest, indent=1))
    return manifest


# ── the species' inputs


def binary_mask(mask_path: Path) -> tuple[np.ndarray, float, dict]:
    """The Mask job's surface made binary again: its low-passed map at its threshold, within its radial limits, no
    extension and no soft edge (M grows and smooths the mask with the resolution itself). Returns the mask, its
    pixel size and what it was made from."""
    job = mask_path.parent
    missing = [p.name for p in (job / MASK_STATS, job / MASK_LOWPASS) if not p.is_file()]
    if missing:
        raise ValueError(
            f"M's mask is a Mask job's surface made binary again (its low-passed map at its threshold); {mask_path} "
            f"has no {' and no '.join(missing)} beside it. Take the mask from a Mask job"
        )
    stats = json.loads((job / MASK_STATS).read_text())
    # The threshold as the Mask job handed it to relion_mask_create (six significant digits), so the surface is the
    # one its mask was made from, voxel for voxel.
    threshold = float(f"{float(stats['threshold']):.6g}")
    with mrcfile.open(job / MASK_LOWPASS, permissive=True) as m:
        lowpassed = np.asarray(m.data, dtype=np.float32)
        apix = float(m.voxel_size.x)
    keep = lowpassed >= threshold
    inner, outer = float(stats.get("inner_radius_ang") or 0), float(stats.get("outer_radius_ang") or 0)
    if inner > 0 or outer > 0:
        z, y, x = (np.arange(n, dtype=np.float32) - n // 2 for n in lowpassed.shape)
        r = np.sqrt(z[:, None, None] ** 2 + y[None, :, None] ** 2 + x[None, None, :] ** 2) * np.float32(apix)
        if outer > 0:
            keep &= r <= outer
        if inner > 0:
            keep &= r >= inner
    fraction = float(keep.mean())
    if fraction == 0:
        raise ValueError(f"Nothing of {job / MASK_LOWPASS} reaches its threshold {threshold:g}")
    with mrcfile.open(mask_path, header_only=True, permissive=True) as m:
        shape = (int(m.header.nz), int(m.header.ny), int(m.header.nx))
    if shape != keep.shape:
        raise ValueError(f"{mask_path} is {shape[::-1]} px, the low-passed map beside it {keep.shape[::-1]}")
    info = {"threshold": threshold, "lowpass_ang": stats.get("lowpass_ang"), "radii_ang": [inner, outer]}
    return keep.astype(np.float32), apix, info | {"fraction": fraction}


def window(data: np.ndarray, box: int) -> np.ndarray:
    """A cube `box` voxels wide around the map's centre (voxel n // 2, as RELION has it), cut out or padded with
    zeros, as relion_image_handler --new_box windows a map."""
    if data.ndim != 3 or len(set(data.shape)) != 1:
        raise ValueError(f"a cubic map was expected, not {data.shape}")
    n = data.shape[0]
    out = np.zeros((box, box, box), dtype=data.dtype)
    off = n // 2 - box // 2
    lo, hi = max(0, -off), min(box, n - off)
    dst, src = slice(lo, hi), slice(lo + off, hi + off)
    out[dst, dst, dst] = data[src, src, src]
    return out


def read_map(path: Path) -> tuple[np.ndarray, float]:
    with mrcfile.open(path, permissive=True) as m:
        return np.asarray(m.data, dtype=np.float32).copy(), float(m.voxel_size.x)


def write_map(path: Path, data: np.ndarray, apix: float) -> None:
    with mrcfile.new(path, overwrite=True) as m:
        m.set_data(np.ascontiguousarray(data, dtype=np.float32))
        m.voxel_size = apix


# ── the particles M reads


def m_import_table(particles: pd.DataFrame, manifest: dict) -> pd.DataFrame:
    """The set's particles as create_species --particles_relion reads them (with --angpix_coords 1
    --angpix_shifts 1): positions in Å from the corner of the volume M knows, origins folded in."""
    index = series_index(manifest)
    missing = [c for c in (TOMO, NAME, *CENTERED_COLS, *ANGLE_COLS, SUBSET) if c not in particles.columns]
    if missing:
        raise ValueError(f"The particles have no {missing}: M needs named, placed, oriented particles in two halves")
    if [c for c in ORIGIN_PIXEL_COLS if c in particles.columns]:
        raise ValueError("The particles' origins are in pixels; RELION 5 writes them in Å")
    subtomo = [c for c in SUBTOMO_ANGLE_COLS if c in particles.columns]
    if subtomo and particles[subtomo].astype(float).abs().to_numpy().max() > 0:
        raise ValueError(
            f"The particles carry {subtomo} (a RELION extraction's): their orientation is those angles and rlnAngle* "
            "together, and M reads rlnAngle* alone"
        )
    names = particles[NAME].astype(str)
    if names.duplicated().any():
        raise ValueError("Particle names repeat; the set M hands back is matched by name")
    subsets = set(particles[SUBSET].astype(int))
    if not subsets <= {1, 2}:
        raise ValueError(f"rlnRandomSubset holds {sorted(subsets)}; M takes the halves 1 and 2")
    tomo = particles[TOMO].astype(str)
    unknown = sorted(set(tomo) - set(index))
    if unknown:
        raise ValueError(f"No source for the tilt series {unknown[:5]}")
    centre = np.stack([source_volume(index[t][0]).centre_angst for t in tomo])
    pos = particles[CENTERED_COLS].to_numpy(dtype=float) + centre
    origin_cols = [c for c in ORIGIN_ANGST_COLS if c in particles.columns]
    if origin_cols:
        if len(origin_cols) != 3:
            raise ValueError(f"The particles hold some origin columns only: {origin_cols}")
        pos = pos - particles[ORIGIN_ANGST_COLS].to_numpy(dtype=float)
    outside = ((pos < 0) | (pos > 2 * centre)).any(axis=1)
    if outside.any():
        raise ValueError(f"{int(outside.sum())} particles fall outside their tomogram in Warp's frame")
    out = pd.DataFrame({c: pos[:, i] for i, c in enumerate(LEGACY_COORDS)})
    for c in ANGLE_COLS:
        out[c] = particles[c].astype(float).to_numpy()
    out[TOMO] = [index[t][1]["item"] for t in tomo]
    out[SUBSET] = particles[SUBSET].astype(int).to_numpy()
    if UID_COL in particles.columns and particles[UID_COL].notna().all():
        out[UID_COL] = particles[UID_COL].astype(str).to_numpy()
    out[NAME_COL] = names.to_numpy()
    return out


def write_plain_star(df: pd.DataFrame, path: Path) -> None:
    """One loop under `data_`, as M reads and writes particle stars."""
    for c in df.columns:
        if df[c].dtype == object and df[c].astype(str).str.contains(r"\s").any():
            raise ValueError(f"{c} holds a value with a space, which a star loop cannot")
    lines = ["", "data_", "", "loop_", *[f"_{c} #{i + 1}" for i, c in enumerate(df.columns)]]
    for row in df.itertuples(index=False):
        lines.append("  ".join(f"{v:.6f}" if isinstance(v, float) else str(v) for v in row))
    path.write_text("\n".join(lines) + "\n")


def read_plain_star(path: Path) -> pd.DataFrame:
    for block in starfile.read(path, always_dict=True).values():
        if isinstance(block, pd.DataFrame):
            return block
    raise ValueError(f"{path} holds no table")


def _hash12(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def label_copies(star: Path, tag: str) -> dict:
    """Name the copies an expansion made (MTools expand_symmetry writes each particle's copies one after another,
    each with its parent's columns): `<parent>_<k>` in the order M wrote them, the parent's name and uid beside, the
    copy's index, a uid of its own (from the parent's, `tag` and k). Rewrites M's particle star; returns the counts."""
    df = read_plain_star(star)
    if NAME_COL not in df.columns:
        raise ValueError(f"{star} has no {NAME_COL}: the copies cannot be traced to their particles")
    names = df[NAME_COL].astype(str)
    per = names.value_counts()
    if per.min() < 2:
        raise ValueError(f"The expansion left {int((per < 2).sum())} particles with no copy: nothing was expanded")
    if per.nunique() != 1:
        raise ValueError(f"The expansion made {sorted(per.unique())} copies of different particles: one count expected")
    k = df.groupby(names, sort=False).cumcount()
    df[PARENT_NAME_COL] = names.to_numpy()
    if UID_COL in df.columns:
        parents = [parse_cell(v) or _hash12(f"parent|{n}") for v, n in zip(df[UID_COL], names, strict=True)]
    else:
        parents = [_hash12(f"parent|{n}") for n in names]
    df[PARENT_UID_COL] = [uid_cell(p) for p in parents]
    df[ASSEMBLY_COL] = df[PARENT_UID_COL]
    df[INDEX_COL] = k.to_numpy()
    df[UID_COL] = [uid_cell(_hash12(f"m_expand|{p}|{tag}|{i}")) for p, i in zip(parents, k, strict=True)]
    df[NAME_COL] = (names + "_" + k.astype(str)).to_numpy()
    write_plain_star(df, star)
    return {"parents": len(per), "copies": len(df), "per_parent": int(per.iloc[0])}


def m_samples(df: pd.DataFrame) -> int:
    """How many temporal samples M's particle star holds (wrpCoordinateX1, X2, …)."""
    n = 0
    while f"wrpCoordinateX{n + 1}" in df.columns:
        n += 1
    if n == 0:
        raise ValueError(f"M's particle star has no {M_POS[0]}")
    return n


def m_pose(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Each particle's position (Å from the volume's corner) and orientation (RELION's 3 × 3) in M's particle star;
    with several temporal samples, their mean: the positions averaged, the rotations' mean projected back onto a
    rotation (chordal L2 mean)."""
    n = m_samples(df)
    pos = np.mean([df[[f"wrpCoordinate{a}{t}" for a in "XYZ"]].to_numpy(float) for t in range(1, n + 1)], axis=0)
    mats = np.mean(
        [
            euler_matrices(df[[f"wrpAngle{a}{t}" for a in ("Rot", "Tilt", "Psi")]].to_numpy(float))
            for t in range(1, n + 1)
        ],
        axis=0,
    )
    if n > 1:
        u, _, vt = np.linalg.svd(mats)
        fix = np.ones((len(mats), 3))
        fix[:, 2] = np.sign(np.linalg.det(u @ vt))
        mats = (u * fix[:, None, :]) @ vt
    return pos, mats


def angles_of(mats: np.ndarray) -> np.ndarray:
    """RELION's (rot, tilt, psi) in degrees of each 3 × 3, as (n, 3)."""
    return np.stack(euler_angles(mats), axis=1)


def m_moves(before: pd.DataFrame, after: pd.DataFrame, manifest: dict) -> dict[str, dict]:
    """How far a round moved and turned a species' particles, per tilt series (by its name in the set): M's particle
    star before the round against after it, matched by name."""
    b = before.set_index(before[NAME_COL].astype(str))
    a = after.set_index(after[NAME_COL].astype(str))
    if set(b.index) != set(a.index):
        raise ValueError(f"M's particles before and after the round differ: {len(set(b.index) ^ set(a.index))} names")
    a = a.loc[b.index]
    (pb, mb), (pa, ma) = m_pose(b), m_pose(a)
    moved = np.linalg.norm(pa - pb, axis=1)
    rel = ma @ np.transpose(mb, (0, 2, 1))
    turned = np.degrees(np.arccos(np.clip((np.trace(rel, axis1=1, axis2=2) - 1) / 2, -1.0, 1.0)))
    by_item = {s["item"]: name for name, (_src, s) in series_index(manifest).items()}
    series = b[M_SOURCE].astype(str).map(by_item)
    if series.isna().any():
        raise ValueError(f"M's particles name tilt series no source has: {sorted(set(b[M_SOURCE][series.isna()]))[:5]}")
    return {
        t: {
            "particles": int(sel.sum()),
            "moved_rms_ang": float(np.sqrt((moved[sel] ** 2).mean())),
            "moved_max_ang": float(moved[sel].max()),
            "turned_rms_deg": float(np.sqrt((turned[sel] ** 2).mean())),
            "turned_max_deg": float(turned[sel].max()),
        }
        for t in sorted(set(series))
        for sel in [(series == t).to_numpy()]
    }


def expand_base(base: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    """The set's rows for an expanded species: each copy in M's star (`label_copies`) gets its parent's row, named
    and traced as the copy. Rows of the set that M holds as they are stay as they are."""
    names = base[NAME].astype(str)
    own = m[NAME_COL].astype(str).isin(set(names)).to_numpy()
    if own.all():
        return base
    if PARENT_NAME_COL not in m.columns:
        raise ValueError(f"M's particle star names {int((~own).sum())} particles the set does not hold")
    copies = m[~own]
    parent = copies[PARENT_NAME_COL].astype(str)
    unknown = sorted(set(parent) - set(names))
    if unknown:
        raise ValueError(f"{len(unknown)} copies in M's particle star have no parent in the set: {unknown[:3]}")
    by_name = base.set_index(names, drop=False)
    rows = by_name.loc[parent.tolist()].reset_index(drop=True)
    rows[NAME] = copies[NAME_COL].astype(str).to_numpy()
    for col in (UID_COL, PARENT_UID_COL, ASSEMBLY_COL, INDEX_COL):
        if col in copies.columns:
            rows[col] = copies[col].to_numpy()
    expanded = set(parent)
    if m.loc[own, NAME_COL].astype(str).isin(expanded).any():
        raise ValueError("M holds a particle both as it is and as copies")
    return pd.concat([base[~names.isin(expanded)], rows], ignore_index=True)


# ── the set M hands back


def euler_matrices(angles_deg: np.ndarray) -> np.ndarray:
    """(n, 3, 3) rotation matrices of RELION's (rot, tilt, psi), as its Euler_angles2matrix builds them."""
    a, b, g = np.radians(angles_deg).T
    ca, sa, cb, sb, cg, sg = np.cos(a), np.sin(a), np.cos(b), np.sin(b), np.cos(g), np.sin(g)
    cc, cs, sc, ss = cb * ca, cb * sa, sb * ca, sb * sa
    return np.stack(
        [
            np.stack([cg * cc - sg * sa, cg * cs + sg * ca, -cg * sb], axis=-1),
            np.stack([-sg * cc - cg * sa, -sg * cs + cg * ca, sg * sb], axis=-1),
            np.stack([sc, ss, cb], axis=-1),
        ],
        axis=-2,
    )


def rotation_between(before_deg: np.ndarray, after_deg: np.ndarray) -> np.ndarray:
    """The angle (°) of the rotation that takes each particle's pose before to its pose after."""
    rel = euler_matrices(after_deg) @ np.transpose(euler_matrices(before_deg), (0, 2, 1))
    cos = (np.trace(rel, axis1=1, axis2=2) - 1) / 2
    return np.degrees(np.arccos(np.clip(cos, -1.0, 1.0)))


def xml_tilt_ctf(xml: Path) -> dict[str, tuple[float, float, float]]:
    """Per tilt of a Warp XML, by Warp's key for its movie: RELION's defocus U, V (Å) and angle, converted as the
    tsCtf job converts them."""
    df = WarpXmlParser(str(xml)).data_df
    return {
        str(r.cryoBoostKey): (*warp_defocus_uv(float(r.defocus_value), float(r.defocus_delta)), float(r.defocus_angle))
        for r in df.itertuples()
    }


def tilt_star_with_m_ctf(src: Path, src_block: str, xml: Path, dst: Path, dst_block: str) -> int:
    """`src`'s per-tilt table as `dst` (block `dst_block`, file names absolute), each tilt the XML holds with the
    XML's defocus, astigmatism and angle. A tilt the XML lacks (Warp's import left it out) keeps its values, as in
    the tsCtf job's star. Returns how many tilts took the XML's."""
    write_renamed_tilt_star(src, dst, src_block, dst_block, project_of(src, src.parent))
    df = tilt_block(dst, dst_block)
    absent = [c for c in CTF_COLS[:3] if c not in df.columns]
    if absent:
        raise ValueError(f"{src} has no {absent}: no per-tilt CTF to put M's into")
    cols = [c for c in CTF_COLS if c in df.columns]
    ctf = xml_tilt_ctf(xml)
    keys = df["rlnMicrographMovieName"].map(lambda m: frame_id_to_warp_key(Path(Path(str(m)).name).stem))
    matched = 0
    for i, key in keys.items():
        if key in ctf:
            u, v, angle = ctf[key]
            df.loc[i, cols] = [u, v, angle, u - v][: len(cols)]
            matched += 1
    if matched != len(ctf):
        raise ValueError(f"{xml}: {len(ctf) - matched} of its tilts have no row in {src}")
    starfile.write({dst_block: df}, dst, overwrite=True)
    return matched


def relion_set_from_m(
    m_particles: pd.DataFrame,
    base_particles_star: Path,
    base_tomograms_star: Path,
    manifest: dict,
    out_dir: Path,
    *,
    subset_ok: bool = False,
) -> dict:
    """M's particles as a RELION 5 set in `out_dir` (particles.star, tomograms.star, optimisation_set.star), the
    rows of the set that went in keyed by cbTomoParticleName; an expanded species' copies each under their parent's
    row (`expand_base`); several temporal samples as their mean pose (`m_pose`). `subset_ok`: the set may hold
    particles M was not given (a species added on the population's tilt series only); they are left out. Returns
    the counts."""
    need = [*M_POS, *M_ANGLES, M_SUBSET, M_SOURCE, NAME_COL]
    missing = [c for c in need if c not in m_particles.columns]
    if missing:
        raise ValueError(f"M's particle star has no {missing}")
    base, optics, general = _read_input_particles_lenient(base_particles_star)
    base = expand_base(base, m_particles)
    index = series_index(manifest)
    m = m_particles.set_index(m_particles[NAME_COL].astype(str))
    if m.index.duplicated().any():
        raise ValueError("M's particle star names a particle twice")
    names = base[NAME].astype(str)
    left_out = 0
    if subset_ok:
        given = names.isin(set(m.index)).to_numpy()
        left_out = int((~given).sum())
        base = base[given].reset_index(drop=True)
        names = base[NAME].astype(str)
    if set(names) != set(m.index):
        raise ValueError(
            f"M hands back {len(m)} particles and {len(base)} went in; {len(set(names) ^ set(m.index))} names "
            "are in one and not the other"
        )
    rows = m.loc[names.tolist()]
    series = base[TOMO].astype(str)
    items = np.array([index[t][1]["item"] for t in series])
    wrong_source = int((rows[M_SOURCE].astype(str).to_numpy() != items).sum())
    if wrong_source:
        raise ValueError(f"M placed {wrong_source} particles on another tilt series than they came from")
    flipped = int((rows[M_SUBSET].astype(int).to_numpy() != base[SUBSET].astype(int).to_numpy()).sum())
    if flipped:
        raise ValueError(f"M hands back {flipped} particles in the other half than they went in")
    if UID_COL in rows.columns and UID_COL in base.columns:
        if (rows[UID_COL].astype(str).to_numpy() != base[UID_COL].astype(str).to_numpy()).any():
            raise ValueError(f"M hands back particles whose {UID_COL} is not the one they went in with")

    centre = np.stack([source_volume(index[t][0]).centre_angst for t in series])
    pos, mats = m_pose(rows)
    out = base.copy()
    out[CENTERED_COLS] = pos - centre
    out[ANGLE_COLS] = angles_of(mats) if m_samples(rows) > 1 else rows[M_ANGLES].to_numpy(dtype=float)
    for c in ORIGIN_ANGST_COLS:
        if c in out.columns:
            out[c] = 0.0
    out = out.drop(columns=[c for c in ("rlnImageName", *LEGACY_COORDS) if c in out.columns])

    tomograms = _read_tomograms_star(base_tomograms_star)
    tomograms = tomograms[tomograms[TOMO].astype(str).isin(set(series))].copy().reset_index(drop=True)
    for i, row in tomograms.iterrows():
        source, ser = index[str(row[TOMO])]
        d, item = Path(source["folder"]), ser["item"]
        xml = d / PROCESSING_DIR / f"{item}.xml"
        item_star = d / TILT_DIR / f"{item}.star"
        tilt_star_with_m_ctf(Path(ser["tilt_star_from"]), item, xml, item_star, item)
        set_star = item_star
        if ser["name"] != item:
            if any(s["item"] == ser["name"] for s in source["series"]):
                raise ValueError(f"{ser['name']} is the set's name of {item} and another tilt series' own name")
            set_star = d / TILT_DIR / f"{ser['name']}.star"
            write_renamed_tilt_star(item_star, set_star, item, ser["name"], d)
        tomograms.at[i, "rlnTomoTiltSeriesStarFile"] = str(set_star)
        if ORIGIN_TILT_STAR in tomograms.columns:
            tomograms.at[i, ORIGIN_TILT_STAR] = str(item_star)
        ctf = wg.xml_ctf_params(ET.parse(xml).getroot())
        for col, key in (
            ("rlnSphericalAberration", "Cs"),
            ("rlnVoltage", "Voltage"),
            ("rlnAmplitudeContrast", "Amplitude"),
        ):
            if col in tomograms.columns and ctf.get(key, "").strip():
                tomograms.at[i, col] = float(ctf[key])
    tomograms = tomograms.drop(columns=[c for c in (wg.GEOMETRY_COL, wg.GEOMETRY_TILT_STAR_COL) if c in tomograms])

    for label, df in (("particles", out), ("tomograms", tomograms)):
        empty = [c for c in df.columns if df[c].isna().any() or (df[c].astype(str).str.strip() == "").any()]
        if empty:
            raise ValueError(f"The {label} to write have empty fields in {empty}: a star file cannot hold them")
    out_dir.mkdir(parents=True, exist_ok=True)
    particles_star, tomograms_star = out_dir / "particles.star", out_dir / "tomograms.star"
    _write_particles_star(particles_star, optics_df=optics, particles_df=out, general_kv=general)
    _write_tomograms_star(tomograms_star, tomograms)
    write_optimisation_set(
        out_dir / "optimisation_set.star", particles_star=particles_star, tomograms_star=tomograms_star
    )
    return {"particles": len(out), "left_out": left_out, "tilt_series": int(series.nunique())}


# ── what a round changed


def _grid(root: ET.Element, tag: str) -> tuple[tuple[int, ...], np.ndarray]:
    node = root.find(tag)
    if node is None:
        return (), np.array([])
    dims = tuple(int(node.get(k)) for k in ("Width", "Height", "Depth") if node.get(k) is not None)
    return dims, np.array([float(n.get("Value")) for n in node.findall("Node")])


def model_changes(before: Path, after: Path) -> dict:
    """One tilt series' Warp model after a round against before it: defocus and astigmatism per tilt, stage angles,
    the image warp, Cs, magnification (as M's 2 x 2 and its parts), Zernike terms."""
    b, a = ET.parse(before).getroot(), ET.parse(after).getroot()
    movies = [Path(m).name for m in xml_movies(after)]
    _, z_b = _grid(b, "GridCTF")
    _, z_a = _grid(a, "GridCTF")
    _, d_b = _grid(b, "GridCTFDefocusDelta")
    _, d_a = _grid(a, "GridCTFDefocusDelta")
    out: dict = {"tilts": len(movies)}
    if len(z_a) == len(z_b) == len(movies) and len(d_a) == len(d_b) == len(movies):
        dz = z_a - z_b
        out |= {
            "defocus_shift_um": float(dz.mean()),
            "defocus_shift_sd_um": float(dz.std()),
            "defocus_shift_max_um": float(dz[np.abs(dz).argmax()]),
            "astigmatism_before_um": float((2 * d_b).mean()),
            "astigmatism_after_um": float((2 * d_a).mean()),
            "per_tilt": [
                {
                    "movie": m,
                    "defocus_before_um": float(zb),
                    "defocus_after_um": float(za),
                    "astigmatism_before_um": float(2 * db),
                    "astigmatism_after_um": float(2 * da),
                }
                for m, zb, za, db, da in zip(movies, z_b, z_a, d_b, d_a, strict=True)
            ],
        }
    dims_b, wx_b = _grid(b, "GridMovementX")
    dims_a, wx_a = _grid(a, "GridMovementX")
    _, wy_b = _grid(b, "GridMovementY")
    _, wy_a = _grid(a, "GridMovementY")
    out["warp_grid"] = list(dims_a[:2])
    out["warp_change_rms_ang"] = (
        float(np.sqrt(((wx_a - wx_b) ** 2 + (wy_a - wy_b) ** 2).mean())) if dims_a == dims_b and wx_a.size else None
    )
    beyond_b, beyond_a = wg.beyond_star(before), wg.beyond_star(after)
    out |= {
        "stage_angle_max_deg": beyond_a.stage_angle_max_deg,
        "cs_before_mm": beyond_b.cs_mm,
        "cs_after_mm": beyond_a.cs_mm,
        "magnification": list(beyond_a.magnification),
        "magnification_pct": wg.magnification_parts(beyond_a.magnification),
        "zernike_odd": list(beyond_a.zernike_odd),
        "zernike_even": list(beyond_a.zernike_even),
        "volume_warp_max_ang": max(float(np.abs(_grid(a, f"GridVolumeWarp{x}")[1]).max(initial=0)) for x in "XYZ"),
    }
    return out
