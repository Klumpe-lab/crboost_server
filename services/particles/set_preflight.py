"""What a job reading a particle set is checked for at Run, before anything is submitted.

A set is checked as files, whatever made it: its rows, its tomograms, each tomogram's per-tilt star and each
tomogram's own project's registry. A kitchen set read through its picks' facets (`set_facets`) is checked as the job
will read it, built from the chosen sources, before it is written (`check_input`). Problems refuse Run and are named
under the job's input; notes are said there and refuse nothing. The stars are RELION's, Warp's or a set writer's, so
every check reads what it relies on.

The last check of each job is kept in this server's memory for the job page (`last_check`); a restart forgets it,
and the next Run checks again.

Blocking (star reads, registry loads): call `check_set` in a thread.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from services.jobs.spec import display_name
from services.models_base import JobType

# Jobs that read an optimisation set in `input_optimisation`, and what each needs of it.
NEEDS_IMAGES = frozenset({JobType.CLASS3D, JobType.REFINE3D})
NEEDS_ANGLES_FOR_POSITIONS = frozenset({JobType.RECONSTRUCT_PARTICLE, JobType.WARP_EXPORT})
BINS_TILT_SERIES = frozenset({JobType.SUBTOMO_EXTRACTION, JobType.RECONSTRUCT_PARTICLE, JobType.WARP_EXPORT})
ANGLE_COLS = ("rlnAngleRot", "rlnAngleTilt", "rlnAnglePsi")
OPTICS_CONSTANTS = ("rlnVoltage", "rlnSphericalAberration", "rlnAmplitudeContrast", "rlnTomoTiltSeriesPixelSize")
IMAGE_GEOMETRY = ("rlnImagePixelSize", "rlnImageSize", "rlnTomoSubtomogramBinning")
# A tilt series RELION can project: these four (and each tilt's image name), else it uses identity matrices.
ALIGNMENT_COLS = ("rlnMicrographName", "rlnTomoYTilt", "rlnTomoZRot", "rlnTomoXShiftAngst", "rlnTomoYShiftAngst")
CTF_COLS = ("rlnDefocusU",)
SHOWN = 3  # names listed in a line before "…"
_READ_THREADS = 8


@dataclass(frozen=True)
class SetCheck:
    set_id: str
    at: str  # when it ran, HH:MM
    problems: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


_LAST: dict[tuple[str, str], SetCheck] = {}


def last_check(project_path: Path | str, instance_id: str) -> SetCheck | None:
    """The last check of a job's set input in this server, if any."""
    return _LAST.get((str(Path(project_path).resolve()), instance_id))


def remember(project_path: Path | str, instance_id: str, check: SetCheck) -> None:
    _LAST[(str(Path(project_path).resolve()), instance_id)] = check


def _names(items, n: int = SHOWN) -> str:
    items = list(items)
    return ", ".join(map(str, items[:n])) + (", …" if len(items) > n else "")


@dataclass
class _Found:
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def check_set(project_path: Path, set_id: str, job_type: JobType, *, diameters: dict[str, float | None]) -> SetCheck:
    """What a `job_type` job reading set `set_id` of `project_path` as written needs of it: the kind of rows it reads,
    one particle once, tomograms its tools can read, one image scale. Never raises for what it finds: a set that
    cannot be read is a problem. `diameters`: each species' diameter (Å) by id, the set's for doubles."""
    from services.particles.particle_sets import get_set
    from services.subtomo_merge import _parse_optimisation_set, _read_input_particles_lenient, _read_tomograms_star

    at = datetime.now().strftime("%H:%M")
    pset = get_set(project_path, set_id)
    if pset is None:
        return SetCheck(set_id, at, (f"the set {set_id} is gone from this project",))
    try:
        particles_star, tomograms_star = _parse_optimisation_set(Path(pset.optimisation_set))
        particles, optics, _kv = _read_input_particles_lenient(particles_star)
        tomograms = _read_tomograms_star(tomograms_star)
    except (OSError, ValueError, KeyError) as e:
        return SetCheck(set_id, at, (f"its files cannot be read: {e}",))
    return _check(project_path, pset, job_type, particles, optics, tomograms, diameters=diameters)


def check_input(project_path: Path, pset, job_type: JobType, built, *, diameters: dict[str, float | None]) -> SetCheck:
    """The same checks on what a job reading a kitchen set will read (`set_facets.build_input`), before it is
    written: the rows its chosen sources give, a renamed tomogram's per-tilt star read where its copy comes from."""
    return _check(
        project_path,
        pset,
        job_type,
        built.particles,
        built.optics,
        built.tomograms,
        diameters=diameters,
        tilt_copies=built.tilt_copies,
        notes=built.notes,
    )


def check_facet_input(
    project_path: Path, pset, job_type: JobType, slot, choices: dict[str, str], *, diameters: dict[str, float | None]
):
    """A job reading kitchen set `pset` through input `slot`: where its picks' facet comes from (`choices` where the job
    chose), what that gives the job, and the checks on it. Returns the check and, when nothing refuses, (the facet plan,
    what the job reads) for the orchestrator to write. Never raises for what it finds."""
    from services.particles.set_facets import build_input, facet_of, resolve

    at = datetime.now().strftime("%H:%M")
    try:
        plan = resolve(project_path, pset, facet_of(slot), choices)
        if plan.problems:
            return SetCheck(pset.id, at, tuple(plan.problems)), None
        built = build_input(plan, pset)
    except (OSError, ValueError, KeyError) as e:
        return SetCheck(pset.id, at, (f"what its picks are read from cannot be read: {e}",)), None
    check = check_input(project_path, pset, job_type, built, diameters=diameters)
    return check, (None if check.problems else (plan, built))


def _check(
    project_path: Path,
    pset,
    job_type: JobType,
    particles: pd.DataFrame,
    optics: pd.DataFrame | None,
    tomograms: pd.DataFrame,
    *,
    diameters: dict[str, float | None],
    tilt_copies: dict[str, tuple[str, str, str]] | None = None,
    notes: list[str] | tuple[str, ...] = (),
) -> SetCheck:
    out = _Found(notes=list(notes))
    images = "rlnImageName" in particles.columns
    _check_level(out, job_type, particles, images)
    _check_rows(out, pset, particles, diameters.get(pset.species_id))
    _check_tomograms(out, project_path, particles, tomograms, job_type, images, tilt_copies or {})
    if images:
        _check_image_geometry(out, optics)
    if job_type == JobType.REFINE3D:
        _check_halves(out, particles)
    return SetCheck(pset.id, datetime.now().strftime("%H:%M"), tuple(out.problems), tuple(out.notes))


def _check_level(out: _Found, job_type: JobType, particles: pd.DataFrame, images: bool) -> None:
    from services.particles.warp_export import SUBTOMO_ANGLE_COLS

    if job_type in NEEDS_IMAGES and not images:
        out.problems.append(
            "it holds positions only, no particle images: extract it first (Subtomo extraction or Warp export) and "
            "read that job's particles"
        )
    if job_type == JobType.WARP_EXPORT and any(c in particles.columns for c in SUBTOMO_ANGLE_COLS):
        out.problems.append(
            "its rows carry RELION's subtomogram angles (an extraction's rows), which Warp export refuses: export the "
            "positions they were extracted from"
        )
    if job_type in NEEDS_ANGLES_FOR_POSITIONS and not images:
        if not all(c in particles.columns for c in ANGLE_COLS):
            out.problems.append(
                f"its picks carry no angles ({', '.join(ANGLE_COLS)}): {display_name(job_type)} needs each pick's "
                "orientation "
                "(template-match picks have one, hand picks do not)"
            )
        else:
            bad = ~np.isfinite(particles[list(ANGLE_COLS)].apply(pd.to_numeric, errors="coerce")).all(axis=1)
            if bad.any():
                out.problems.append(f"{int(bad.sum()):,} of {len(particles):,} picks have no angles")


def _check_rows(out: _Found, pset, particles: pd.DataFrame, diameter_a: float | None) -> None:
    from scipy.spatial import cKDTree

    from services.particles.coords import CENTERED_COLS
    from services.particles.particle_sets import SetOp
    from services.particles.pick_uids import UID_COL, read_uid_column

    col = read_uid_column(particles)
    if col is None:
        if pset.recipe.op == SetOp.POOL:
            out.notes.append(f"its rows carry no {UID_COL} (made before pick uids were written)")
    else:
        out.problems += [f"{p}: one particle twice, or rows of another star" for p in col.problems()]
    missing = [c for c in CENTERED_COLS if c not in particles.columns]
    if missing:
        out.problems.append(f"its rows have no centred coordinates ({', '.join(missing)})")
        return
    if not diameter_a:
        out.notes.append("the species has no diameter, so doubles by position were not looked for")
        return
    radius = diameter_a / 2.0
    xyz = particles[CENTERED_COLS].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float64)
    names = particles["rlnTomoName"].astype(str).to_numpy()
    pairs, where = 0, []
    for tomo in np.unique(names):
        p = xyz[names == tomo]
        p = p[np.isfinite(p).all(axis=1)]
        if len(p) < 2:
            continue
        k = len(cKDTree(p).query_pairs(radius))
        if k:
            pairs += k
            where.append(tomo)
    if pairs:
        out.problems.append(
            f"{pairs:,} pairs of particles closer than Ø/2 ({radius:g} Å) on {len(where)} tomogram(s) "
            f"({_names(where)}): the same particle twice?"
        )


def _check_tomograms(
    out: _Found,
    project_path: Path,
    particles: pd.DataFrame,
    tomograms: pd.DataFrame,
    job_type: JobType,
    images: bool,
    tilt_copies: dict[str, tuple[str, str, str]],
) -> None:
    from services.particles.set_origins import origins_of

    names = tomograms["rlnTomoName"].astype(str).tolist()
    if len(set(names)) != len(names):
        dup = sorted({n for n in names if names.count(n) > 1})
        out.problems.append(f"{len(dup)} tomogram name(s) appear twice in its tomograms.star ({_names(dup)})")
    absent = sorted(set(particles["rlnTomoName"].astype(str)) - set(names))
    if absent:
        out.problems.append(f"{len(absent)} tomogram(s) of its particles have no tomogram row ({_names(absent)})")
    try:
        origins = origins_of(tomograms)
    except ValueError as e:
        out.problems.append(f"its tomograms.star: {e}")
        origins = None
    where = {n: (o.project, o.tomo) for n, o in origins.items()} if origins else {}
    for n in names:
        where.setdefault(n, (str(project_path), n))
    _check_registries(out, where)
    _check_hand(out, tomograms)
    _check_optics(out, tomograms)
    if not images and job_type in BINS_TILT_SERIES and "rlnTomoTiltSeriesPixelSize" in tomograms.columns:
        sizes = sorted({round(float(v), 4) for v in tomograms["rlnTomoTiltSeriesPixelSize"]})
        if len(sizes) > 1:
            out.problems.append(
                f"its tilt series are at {len(sizes)} pixel sizes ({_names(f'{s:g} Å' for s in sizes)}): "
                f"{display_name(job_type)} bins each by the same factor, so the particles would come out at "
                f"{len(sizes)} scales"
            )
    _check_tilt_stars(out, tomograms, tilt_copies)


def _check_registries(out: _Found, where: dict[str, tuple[str, str]]) -> None:
    """Every tomogram known to its own project's registry, one acquisition per tomogram, and which are excluded
    from processing in their own project (said, not applied)."""
    from services.sta_kitchen.snapshots import project_view
    from services.particles.tomo_identity import acquisition_key

    by_project: dict[str, list[str]] = {}
    for name, (project, _there) in where.items():
        by_project.setdefault(project, []).append(name)
    unknown: list[str] = []
    excluded: list[str] = []
    acq: dict[tuple, list[str]] = {}
    for project, names in sorted(by_project.items()):
        label = Path(project).name
        try:
            registry = project_view(Path(project)).registry
        except (OSError, ValueError) as e:
            out.problems.append(f"the registry of {label} cannot be read: {e}")
            continue
        if registry.load_problem:
            out.problems.append(f"{label}: {registry.load_problem}")
        for name in names:
            there = where[name][1]
            if not registry.has_tilt_series(there):
                unknown.append(f"{there} in {label}")
                continue
            ts = registry.get_tilt_series(there)
            if ts.is_excluded:
                excluded.append(f"{there} in {label}")
            key = acquisition_key(ts)
            if key is not None:
                acq.setdefault(key, []).append(name)
    if unknown:
        out.problems.append(f"{len(unknown)} tomogram(s) unknown to their own project's registry ({_names(unknown)})")
    twice = [names for names in acq.values() if len(names) > 1]
    if twice:
        out.problems.append(
            f"{len(twice)} acquisition(s) appear as two tomograms or more ({_names(' = '.join(n) for n in twice)}): "
            "one tilt series' particles would be counted twice"
        )
    if excluded:
        out.notes.append(
            f"{len(excluded)} tomogram(s) are excluded from processing in their own project ({_names(excluded)}): "
            "a job reading a set applies no exclusions, so it reads them"
        )


def _check_hand(out: _Found, tomograms: pd.DataFrame) -> None:
    if "rlnTomoHand" not in tomograms.columns:
        out.notes.append("its tomograms state no handedness (rlnTomoHand): that they agree is not checked")
        return
    hands = pd.to_numeric(tomograms["rlnTomoHand"], errors="coerce")
    stated = sorted({int(h) for h in hands if math.isfinite(h)})
    unstated = int((~np.isfinite(hands.to_numpy(dtype=float))).sum())
    if len(stated) > 1:
        out.problems.append(
            f"its tomograms disagree on handedness ({', '.join(f'{h:+d}' for h in stated)}): a mirrored tomogram "
            "gives mirror-image particles"
        )
    elif unstated and stated:
        out.problems.append(f"{unstated} of {len(tomograms)} tomograms state no handedness, the others {stated[0]:+d}")


def _check_optics(out: _Found, tomograms: pd.DataFrame) -> None:
    """Tomograms sharing an optics group name must share its constants: RELION fits one group as one."""
    cols = [c for c in OPTICS_CONSTANTS if c in tomograms.columns]
    if "rlnOpticsGroupName" not in tomograms.columns or not cols:
        return
    for name, rows in tomograms.groupby(tomograms["rlnOpticsGroupName"].astype(str)):
        values = rows[cols].astype(float).round(4)
        differ = [c[3:] for c in cols if values[c].nunique() > 1]
        if differ:
            out.problems.append(
                f"optics group {name} holds tomograms with different {', '.join(differ)}: RELION would treat them "
                "as one"
            )
    settings = tomograms[cols].astype(float).round(4).drop_duplicates()
    if len(settings) > 1:
        out.notes.append(f"its tomograms hold {len(settings)} optics settings")


def _check_image_geometry(out: _Found, optics: pd.DataFrame | None) -> None:
    if optics is None:
        out.problems.append("its particle images come with no optics table")
        return
    for c in IMAGE_GEOMETRY:
        if c in optics.columns:
            values = sorted({str(v) for v in optics[c].dropna()})
            if len(values) > 1:
                out.problems.append(
                    f"its particle images differ in {c[3:]} ({', '.join(values)}): one job reads one image size "
                    "and scale"
                )


def _check_tilt_stars(out: _Found, tomograms: pd.DataFrame, tilt_copies: dict[str, tuple[str, str, str]]) -> None:
    """Each tomogram's per-tilt star readable under its name in the set (RELION reads block data_<name>), with an
    alignment and a CTF fit. A tomogram in `tilt_copies` gets a copy of another star's block under its name when the
    job's input is written: that block is read."""
    from services.particles.star_columns import block_labels

    def one(item: tuple[str, str]) -> str | None:
        name, path = item
        block = name
        if name in tilt_copies:
            path, block = tilt_copies[name][0], tilt_copies[name][1]
        if not path or not Path(path).is_absolute():
            return f"{name}: its per-tilt star is named relatively ({path or 'none'})"
        try:
            labels = block_labels(path, block)
        except OSError as e:
            return f"{name}: its per-tilt star cannot be read ({e.strerror or e})"
        if labels is None:
            return f"{name}: {Path(path).name} has no block data_{block}"
        if not all(c in labels for c in ALIGNMENT_COLS):
            return f"{name}: its per-tilt star has no alignment"
        if not all(c in labels for c in CTF_COLS):
            return f"{name}: its per-tilt star has no CTF fit"
        return None

    if "rlnTomoTiltSeriesStarFile" not in tomograms.columns:
        out.problems.append("its tomograms.star names no per-tilt stars")
        return
    names, stars = tomograms["rlnTomoName"].astype(str), tomograms["rlnTomoTiltSeriesStarFile"].astype(str)
    rows = list(zip(names, stars, strict=True))
    with ThreadPoolExecutor(max_workers=_READ_THREADS, thread_name_prefix="set-check") as pool:
        bad = [b for b in pool.map(one, rows) if b]
    if bad:
        out.problems.append(f"{len(bad)} tomogram(s) cannot be read by RELION: " + "; ".join(bad[:SHOWN]))


def _check_halves(out: _Found, particles: pd.DataFrame) -> None:
    from services.particles.pick_uids import HALF_COL

    if HALF_COL not in particles.columns:
        out.notes.append(
            f"its rows carry no {HALF_COL}: relion_refine draws the halves itself, so they differ from any other "
            "refinement of these particles"
        )
        return
    halves = pd.to_numeric(particles[HALF_COL], errors="coerce")
    other = int((~halves.isin([1, 2])).sum())
    if other:
        out.problems.append(f"{other:,} rows hold a {HALF_COL} that is not 1 or 2: relion_refine refuses the star")
    elif halves.nunique() < 2:
        out.problems.append(f"every row is in half {int(halves.iloc[0])}: relion_refine needs both halves")
