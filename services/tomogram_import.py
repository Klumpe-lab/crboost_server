"""Tomogram import — a project-level utility, not a pipeline job.

Particle-only / data-less projects have no preprocessing pipeline to produce a
``tomograms.star``; this module lets the PARTICLES-header import utility inject one,
either by synthesizing it from a set of reconstructed ``.mrc`` files (reading dims +
voxel size from each header) or by referencing an existing ``tomograms.star``.

There is no ``JobType`` here: nothing in the manual-picking flow resolves the
tomograms.star as a job *output* — the dashboard, ArtiaX and per-list optset build
read it directly — so modelling it as a SLURM/IO job buys nothing. The committed star
is recorded on ``ProjectState.imported_tomograms``.

Heavy deps (pandas, starfile, mrcfile) are imported lazily so importing this module
stays light.

Schema mirrors a real tsReconstruct ``data_global`` block:
synthesize writes the 13 picking/TM-relevant columns (omits halfmaps +
``rlnTomoTiltSeriesStarFile``, which are the denoise / TemplateMatch paths).
"""

from __future__ import annotations

import re
from enum import Enum
from pathlib import Path


class TomoImportMode(str, Enum):
    REFERENCE = "reference"
    SYNTHESIZE = "synthesize"
    JOBS = "jobs"


# What counts as a reconstructed tomogram on disk. `.rec` is IMOD/etomo's own name for the
# same MRC container and is what a tilt-series reconstructed outside Warp usually arrives as;
# without it half the plausible source directories look empty.
TOMOGRAM_SUFFIXES: tuple[str, ...] = (".mrc", ".rec")


TOMO_IMPORT_REFERENCE = TomoImportMode.REFERENCE.value
TOMO_IMPORT_SYNTHESIZE = TomoImportMode.SYNTHESIZE.value
TOMO_IMPORT_JOBS = TomoImportMode.JOBS.value

# The jobs whose tomograms another project takes ready for template matching: their rows name each tomogram's
# per-tilt-series star (Reconstruct writes its TS CTF input's, Denoise Predict passes them through).
TOMOGRAM_JOB_TYPES = ("tsReconstruct", "denoisepredict")
# What template matching reads from each tomogram's per-tilt-series star.
TILT_STAR_COLUMNS = ("rlnTomoYTilt", "rlnDefocusU", "rlnMicrographPreExposure")
# Beside Tomograms/tomograms.star: the tilt-series star of the tomograms that came from jobs.
TILT_SERIES_STAR = "tilt_series.star"
# Path columns of a tomograms star: absolute in what an import writes. The first two must exist for a tomogram
# taken from a job.
_PATH_COLS = (
    "rlnTomoReconstructedTomogram",
    "rlnTomoTiltSeriesStarFile",
    "rlnTomoReconstructedTomogramHalf1",
    "rlnTomoReconstructedTomogramHalf2",
)
_REQUIRED_PATH_COLS = _PATH_COLS[:2]


def import_refusal(state) -> str | None:
    """Why tomograms cannot be imported into this project, or None. A project with a Reconstruct job takes its
    tomograms from the pipeline: an import beside it would give its particle jobs two tomogram sources."""
    from services.models_base import JobType

    if any(getattr(jm, "job_type", None) == JobType.TS_RECONSTRUCT for jm in state.jobs.values()):
        return "Tomograms already come from the pipeline above: this project has a Reconstruct job."
    return None


def probe_mrc_metadata(paths: list[Path]) -> list[dict]:
    """Per-file MRC header metadata for the pre-commit preview (headers only, no data).

    Each entry: ``{name, path, nx, ny, nz, voxel_size, has_voxel_size, error}``. A
    missing voxel size (``has_voxel_size=False``) is surfaced, not defaulted, so the
    import widget can flag it and let the user supply a pixel size before committing."""
    import mrcfile

    out: list[dict] = []
    for raw in paths:
        p = Path(raw)
        rec = {
            "name": p.name,
            "path": str(p),
            "nx": None,
            "ny": None,
            "nz": None,
            "voxel_size": None,
            "has_voxel_size": False,
            "error": "",
        }
        try:
            with mrcfile.open(str(p), permissive=True, header_only=True) as mrc:
                rec["nx"], rec["ny"], rec["nz"] = int(mrc.header.nx), int(mrc.header.ny), int(mrc.header.nz)
                try:
                    vs = float(mrc.voxel_size.x)
                except Exception:
                    vs = 0.0
                if vs > 0:
                    rec["voxel_size"] = vs
                    rec["has_voxel_size"] = True
        except Exception as e:  # unreadable / not an MRC — report, don't crash the preview
            rec["error"] = str(e)
        out.append(rec)
    return out


def _safe_tomo_name(stem: str, project_tag: str, seen: set) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{project_tag}_{stem}" if project_tag else stem)
    name = base
    i = 2
    while name in seen:
        name = f"{base}_{i}"
        i += 1
    seen.add(name)
    return name


def _absolute(value, star_dir: Path, project_root: Path, *, column: str, strict: bool):
    """`value` as an absolute path: as it is when absolute, else against the star's folder, else against the
    project root, whichever holds it (a Denoise Predict star names its tomograms from the project root). Neither
    holding it: raises when `strict`, else the star-folder form. An empty cell stays as it is."""
    if not value or not isinstance(value, str) or value == "None":
        return value
    p = Path(value)
    if p.is_absolute():
        return value
    for base in (star_dir, project_root):
        if (base / p).exists():
            return str((base / p).resolve())
    if strict:
        raise FileNotFoundError(f"{column} {value}: neither in {star_dir} nor under {project_root}")
    return str((star_dir / p).resolve())


def _star_rows(star: Path):
    """The tomogram block (the one with rlnTomoName) of a tomograms star, as written."""
    import starfile

    if not star.is_file():
        raise FileNotFoundError(f"tomograms.star not found: {star}")
    for block in starfile.read(star, always_dict=True).values():
        if hasattr(block, "columns") and "rlnTomoName" in block.columns:
            return block.copy()
    raise ValueError(f"no tomogram block (rlnTomoName) found in {star}")


def _absolutize(df, star: Path, project_root: Path, *, strict: bool):
    for col in _PATH_COLS:
        if col in df.columns:
            df[col] = [
                _absolute(v, star.parent, project_root, column=col, strict=strict and col in _REQUIRED_PATH_COLS)
                for v in df[col]
            ]
    return df


def _reference_rows(reference_star: str):
    from services.particles.set_origins import project_of

    ref = Path(reference_star).expanduser()
    df = _star_rows(ref)
    return _absolutize(df, ref, project_of(ref, ref.parent), strict=False)


def _project_jobs(project: Path) -> dict:
    """A CryoBoost project's saved jobs, read from its project_params.json: never loaded as a project, so never
    written."""
    import json

    params = Path(project) / "project_params.json"
    if not params.is_file():
        raise FileNotFoundError(f"{project} is not a CryoBoost project (no project_params.json)")
    return json.loads(params.read_text()).get("jobs") or {}


def tomogram_jobs(project: str | Path) -> list[dict]:
    """The jobs of a CryoBoost project whose tomograms another project can take: its Reconstruct and Denoise
    Predict jobs with a tomograms star. `[{job, job_type, star, count}]`."""
    out = []
    for iid, job in _project_jobs(Path(project)).items():
        if job.get("job_type") not in TOMOGRAM_JOB_TYPES or not job.get("relion_job_name"):
            continue
        star = Path(project) / job["relion_job_name"] / "tomograms.star"
        if star.is_file():
            out.append({"job": iid, "job_type": job["job_type"], "star": str(star), "count": len(_star_rows(star))})
    return out


def job_rows(source: dict):
    """One source's tomograms (`TomogramJobSource` fields): the rows its tomogram job wrote for the named ones
    (every one when none are named), with all their columns, the paths absolute, and where each comes from
    (`cbOrigin*`). Raises naming what is missing."""
    from services.particles.set_origins import ORIGIN_PROJECT, ORIGIN_TILT_STAR, ORIGIN_TOMO

    project = Path(source["project"]).expanduser().resolve()
    jobs = _project_jobs(project)
    job = jobs.get(source["job"])
    if job is None:
        listed = ", ".join(i for i, j in jobs.items() if j.get("job_type") in TOMOGRAM_JOB_TYPES) or "none"
        raise ValueError(f"{project.name} has no job {source['job']} (its tomogram jobs: {listed})")
    if job.get("job_type") not in TOMOGRAM_JOB_TYPES:
        raise ValueError(f"{project.name}'s {source['job']} makes no tomograms: Reconstruct or Denoise Predict")
    star = project / (job.get("relion_job_name") or "") / "tomograms.star"
    if not job.get("relion_job_name") or not star.is_file():
        raise FileNotFoundError(f"{project.name}'s {source['job']} has written no tomograms.star")
    df = _star_rows(star)
    names = list(source.get("names") or [])
    if names:
        have = set(df["rlnTomoName"].astype(str))
        missing = [n for n in names if n not in have]
        if missing:
            raise ValueError(
                f"{project.name}'s {source['job']} has no tomogram {', '.join(missing[:5])}"
                + (f" (+{len(missing) - 5} more)" if len(missing) > 5 else "")
            )
        df = df[df["rlnTomoName"].astype(str).isin(names)].copy()
    if "rlnTomoTiltSeriesStarFile" not in df.columns:
        raise ValueError(f"{star} names no tilt-series star (rlnTomoTiltSeriesStarFile)")
    _absolutize(df, star, project, strict=True)
    df[ORIGIN_PROJECT] = str(project)
    df[ORIGIN_TOMO] = df["rlnTomoName"].astype(str)
    df[ORIGIN_TILT_STAR] = df["rlnTomoTiltSeriesStarFile"]
    return df.reset_index(drop=True)


def preview_job_metadata(project: str, job: str) -> list[dict]:
    """A tomogram job's tomograms as the import dialog's review table shows them (`probe_mrc_metadata`'s
    shape), the voxel size the tomogram's: tilt-series pixel size × binning."""
    df = job_rows({"project": project, "job": job})

    def num(row, col):
        value = row.get(col)
        return float(value) if value is not None and value == value else None

    out = []
    for _, row in df.iterrows():
        px, binning = num(row, "rlnTomoTiltSeriesPixelSize"), num(row, "rlnTomoTomogramBinning") or 1.0
        voxel = px * binning if px else None
        out.append(
            {
                "name": str(row["rlnTomoName"]),
                "path": str(row["rlnTomoReconstructedTomogram"]),
                "nx": int(num(row, "rlnTomoSizeX") or 0) or None,
                "ny": int(num(row, "rlnTomoSizeY") or 0) or None,
                "nz": int(num(row, "rlnTomoSizeZ") or 0) or None,
                "voxel_size": voxel,
                "has_voxel_size": bool(voxel and voxel > 0),
                "error": "",
            }
        )
    return out


def check_job_sources(sources: list[dict], binds: list[str]) -> dict:
    """What taking these sources' tomograms would bring, or why it cannot: every tomogram and tilt-series star
    under a container bind (else no job could read it), every tilt-series star holding `TILT_STAR_COLUMNS` (else
    template matching cannot weight the tilts), and one set of the values template matching takes from the
    project and one voxel size, which one template has to match. `ok(values, count)` with the shared values;
    a `needs` item listing the groups when they differ."""
    import pandas as pd

    from services.particles.set_origins import ORIGIN_PROJECT, ORIGIN_TOMO, tilt_block
    from services.result import ErrorCode, err, need, ok

    try:
        df = pd.concat([job_rows(s) for s in sources], ignore_index=True)
    except (OSError, ValueError) as e:
        return err(f"Not imported: {e}")
    if not len(df):
        return err("Not imported: the sources hold no tomograms.")
    roots = [Path(b).resolve() for b in binds]
    problems = []
    for col in _REQUIRED_PATH_COLS:
        for name, value in zip(df["rlnTomoName"], df[col], strict=True):
            if not any(Path(value).is_relative_to(r) for r in roots):
                problems.append(f"{name}: {value} is outside the container binds ({', '.join(binds) or 'none'})")
    for name, origin, path in zip(df["rlnTomoName"], df[ORIGIN_TOMO], df["rlnTomoTiltSeriesStarFile"], strict=True):
        try:
            lacking = [c for c in TILT_STAR_COLUMNS if c not in tilt_block(Path(path), origin).columns]
        except (OSError, ValueError) as e:
            problems.append(f"{name}: {e}")
            continue
        if lacking:
            problems.append(f"{name}: {Path(path).name} lacks {', '.join(lacking)}")
    if problems:
        shown = "; ".join(problems[:8]) + (f" (+{len(problems) - 8} more)" if len(problems) > 8 else "")
        return err(f"Not imported: {shown}", problems=problems)

    def column(name):
        return df[name].astype(float) if name in df.columns else pd.Series([float("nan")] * len(df))

    keys = pd.DataFrame(
        {
            "voltage": column("rlnVoltage"),
            "spherical_aberration": column("rlnSphericalAberration"),
            "amplitude_contrast": column("rlnAmplitudeContrast"),
            "voxel_size": (column("rlnTomoTiltSeriesPixelSize") * column("rlnTomoTomogramBinning")).round(3),
            "hand": column("rlnTomoHand"),
        }
    )
    groups: dict[tuple, list[str]] = {}
    for key, name, project in zip(map(tuple, keys.values.tolist()), df["rlnTomoName"], df[ORIGIN_PROJECT], strict=True):
        groups.setdefault(key, []).append(f"{Path(project).name}/{name}")
    if len(groups) > 1:
        options = [
            {**dict(zip(keys.columns, key, strict=True)), "tomograms": len(names), "first": names[:3]}
            for key, names in groups.items()
        ]
        return err(
            f"Not imported: the tomograms come in {len(groups)} groups of voltage, Cs, amplitude contrast, voxel "
            "size and hand. Take one group, or make one project per group.",
            code=ErrorCode.NEEDS_DECISION,
            needs=[need("choice", "sources", "template matching takes one set of these values", options=options)],
        )
    values = dict(zip(keys.columns, next(iter(groups)), strict=True))
    if any(v != v for v in values.values()):
        unknown = [k for k, v in values.items() if v != v]
        return err(f"Not imported: the sources' tomograms.star records no {', '.join(unknown)}.")
    pixel = sorted(set(column("rlnTomoTiltSeriesPixelSize").round(4)))
    values["tilt_series_pixel_size"] = pixel[0] if len(pixel) == 1 else None
    return ok(values=values, count=len(df))


def voxel_sizes(star_path: str | Path) -> list[float]:
    """The distinct voxel sizes of a tomograms star's tomograms, in Å: tilt-series pixel size × tomogram binning,
    over the rows that hold both."""
    df = _reference_rows(str(star_path))
    if "rlnTomoTiltSeriesPixelSize" not in df.columns:
        return []
    binning = df["rlnTomoTomogramBinning"] if "rlnTomoTomogramBinning" in df.columns else 1.0
    sizes = (df["rlnTomoTiltSeriesPixelSize"].astype(float) * binning).dropna()
    return sorted({round(float(v), 3) for v in sizes if v > 0})


def preview_reference_metadata(reference_star: str) -> list[dict]:
    """Project an existing ``tomograms.star`` into per-row preview dicts shaped exactly
    like :func:`probe_mrc_metadata`, so the import dialog's reference-mode review table
    reuses the same renderer. Reuses ``_reference_rows`` so block-selection / absolutize /
    missing-block guards stay single-sourced. Never raises on a missing pixel size — it is
    surfaced (``has_voxel_size=False``) for the UI to flag, never silently defaulted."""
    import pandas as pd

    df = _reference_rows(reference_star)

    def _num(row, col):
        if col not in df.columns:
            return None
        try:
            v = row[col]
            return float(v) if pd.notna(v) else None
        except (TypeError, ValueError):
            return None

    out: list[dict] = []
    for _, row in df.iterrows():
        px = _num(row, "rlnTomoTiltSeriesPixelSize")
        if px is None:
            px = _num(row, "rlnMicrographOriginalPixelSize")
        nx, ny, nz = _num(row, "rlnTomoSizeX"), _num(row, "rlnTomoSizeY"), _num(row, "rlnTomoSizeZ")
        recon = str(row["rlnTomoReconstructedTomogram"]) if "rlnTomoReconstructedTomogram" in df.columns else ""
        name = str(row["rlnTomoName"]) if "rlnTomoName" in df.columns else (Path(recon).name or "—")
        out.append(
            {
                "name": name,
                "path": recon,
                "nx": int(nx) if nx else None,
                "ny": int(ny) if ny else None,
                "nz": int(nz) if nz else None,
                "voxel_size": px,
                "has_voxel_size": bool(px and px > 0),
                "error": "",
            }
        )
    return out


def _synthesize_rows(
    mrc_paths: list[Path],
    *,
    pixel_size_angstrom: float,
    tomogram_binning: float,
    optics_group_name: str,
    voltage: float,
    spherical_aberration: float,
    amplitude_contrast: float,
    invert_defocus_hand: bool,
    project_tag: str,
):
    import pandas as pd
    import mrcfile

    paths = sorted(p for p in (Path(x) for x in mrc_paths) if p.is_file())
    if not paths:
        return None

    binning = float(tomogram_binning or 1.0)
    hand = -1 if invert_defocus_hand else 1
    seen: set = set()
    rows = []
    for mrc_path in paths:
        with mrcfile.open(str(mrc_path), permissive=True, header_only=True) as mrc:
            nx, ny, nz = int(mrc.header.nx), int(mrc.header.ny), int(mrc.header.nz)
            try:
                recon_apix = float(mrc.voxel_size.x)
            except Exception:
                recon_apix = 0.0

        # Unbinned tilt-series pixel size: explicit override wins; else derive from the
        # recon voxel size and binning (recon voxel size == ts_px * binning). If neither
        # is available, raise rather than default to 1.0 Å/px; the import widget's
        # pre-commit preview surfaces a missing voxel_size so the user supplies
        # `pixel_size_angstrom` first.
        if pixel_size_angstrom > 0:
            ts_px = float(pixel_size_angstrom)
        elif recon_apix > 0:
            ts_px = recon_apix / binning
        else:
            raise ValueError(
                f"{mrc_path.name}: MRC header has no voxel size and no pixel-size override was given. "
                "Set the pixel size on the import before committing."
            )

        rows.append(
            {
                "rlnTomoName": _safe_tomo_name(mrc_path.stem, project_tag, seen),
                "rlnVoltage": float(voltage),
                "rlnSphericalAberration": float(spherical_aberration),
                "rlnAmplitudeContrast": float(amplitude_contrast),
                "rlnMicrographOriginalPixelSize": ts_px,
                "rlnTomoHand": hand,
                "rlnOpticsGroupName": optics_group_name,
                # Unbinned tomogram dims (RELION convention) = recon dims * binning.
                "rlnTomoSizeX": round(nx * binning),
                "rlnTomoSizeY": round(ny * binning),
                "rlnTomoSizeZ": round(nz * binning),
                "rlnTomoTiltSeriesPixelSize": ts_px,
                "rlnTomoReconstructedTomogram": str(mrc_path.resolve()),
                "rlnTomoTomogramBinning": binning,
            }
        )
    return pd.DataFrame(rows)


def _fold_batch_rows(df, seen_names: set, seen_paths: set) -> tuple:
    """Fold one batch's rows into the merged star, against what earlier batches already put
    there. Returns ``(rows, renamed, skipped)``.

    Two collisions, both reported (same rule as ``drivers/subtomo_merge.py``'s
    cross-project guard):

      * **same recon file** — already imported by an earlier batch, so the row is dropped.
        Re-importing a directory you imported before must not double every tomogram.
      * **same tomogram name, different file** — the row is renamed (``<name>__2``). The
        earlier row keeps the name: picks, curation saves and pick lists are keyed on it,
        and renaming the incumbent would strand every one of them.
    """
    renamed: list[dict] = []
    skipped: list[dict] = []
    keep: list[int] = []
    final_names: list[str] = []
    names = df["rlnTomoName"].astype(str).tolist()
    if "rlnTomoReconstructedTomogram" in df.columns:
        paths = df["rlnTomoReconstructedTomogram"].astype(str).tolist()
    else:
        paths = [""] * len(names)

    for i, (name, path) in enumerate(zip(names, paths, strict=True)):
        if path and path in seen_paths:
            skipped.append({"name": name, "path": path, "reason": "already imported by an earlier batch"})
            continue
        final = name
        if final in seen_names:
            j = 2
            while f"{name}__{j}" in seen_names:
                j += 1
            final = f"{name}__{j}"
            renamed.append({"name": name, "renamed_to": final, "path": path})
        seen_names.add(final)
        if path:
            seen_paths.add(path)
        keep.append(i)
        final_names.append(final)

    rows = df.iloc[keep].copy()
    rows["rlnTomoName"] = final_names
    return rows, renamed, skipped


# The project's values template matching takes, by their names in `check_job_sources`' values.
_PROJECT_VALUES = (
    ("acceleration_voltage_kv", "voltage"),
    ("spherical_aberration_mm", "spherical_aberration"),
    ("amplitude_contrast", "amplitude_contrast"),
)


def project_value_mismatch(state, values: dict) -> list[str]:
    """Where the project's values differ from those tomograms from jobs bring (`check_job_sources`): the ones
    template matching takes from the project, and the defocus hand."""
    out = []
    for attr, key in _PROJECT_VALUES:
        mine = float(getattr(state.microscope, attr))
        if abs(mine - values[key]) > 1e-6:
            out.append(f"{key}: the project's {mine:g}, the tomograms' {values[key]:g}")
    inverted = values["hand"] < 0
    if state.acquisition.invert_defocus_hand != inverted:
        out.append(
            f"defocus hand: the project's {'inverted' if not inverted else 'not inverted'}, the tomograms' "
            f"{'inverted' if inverted else 'not inverted'} (rlnTomoHand {values['hand']:g})"
        )
    return out


def take_tomogram_values(state, values: dict) -> None:
    """A project with no values of its own (no raw data, no other tomograms) records those its tomograms from jobs
    bring: the ones template matching takes from it, the defocus hand, and the tilt-series pixel size when they
    share one. Marks dirty."""
    for attr, key in _PROJECT_VALUES:
        setattr(state.microscope, attr, values[key])
    state.acquisition.invert_defocus_hand = values["hand"] < 0
    if values.get("tilt_series_pixel_size"):
        state.microscope.pixel_size_angstrom = values["tilt_series_pixel_size"]
    state.mark_dirty()


def write_tomograms_star(
    out_path: Path,
    *,
    batches: list[dict],
    voltage: float = 300.0,
    spherical_aberration: float = 2.7,
    amplitude_contrast: float = 0.10,
    invert_defocus_hand: bool = True,
    project_tag: str = "",
    require_last: bool = True,
) -> dict:
    """Write ``out_path`` (a ``tomograms.star``) from a list of import batches, in order.

    Each batch is a dict of the fields ``ProjectState.ImportBatch`` carries
    (``source_mode``, ``source_paths`` | ``reference_star`` | ``job_sources``,
    ``pixel_size_angstrom``, ``tomogram_binning``, ``optics_group_name``); ``synthesize`` builds
    rows from the recon files, ``reference`` copies an existing ``tomograms.star`` (absolutizing
    its paths), ``jobs`` takes other projects' tomogram jobs' rows with where each comes from,
    and writes their tilt-series star beside it (`_write_tilt_series`).

    The whole star is rebuilt from the whole list on every commit — it is a pure function of
    the batches, so a batch can be dropped later without any in-place surgery, and the
    re-read cost (MRC headers) is paid off the event loop by the caller.

    Returns ``{"count": total_rows, "per_batch": [{"count", "renamed", "skipped", "error"}],
    "tilt_series_star": path | None}``.
    A prior batch that cannot be read (its reference star moved, its recon files are gone)
    contributes ``error`` and zero rows instead of failing the whole rebuild — nothing in the
    import dialog can repair a source directory that moved. The last batch is the one being
    added right now, and ``require_last`` makes its failure fatal before anything is written:
    otherwise a failed import would still rewrite the committed star from the prior batches,
    quietly dropping the rows of any prior batch that had gone unreadable. Also raises when
    nothing at all could be written."""
    import pandas as pd
    import starfile

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    reports: list[dict] = []
    seen_names: set = set()
    seen_paths: set = set()
    for spec in batches:
        report = {"count": 0, "renamed": [], "skipped": [], "error": ""}
        try:
            mode = spec.get("source_mode") or TOMO_IMPORT_SYNTHESIZE
            if mode == TOMO_IMPORT_JOBS:
                import pandas as pd

                sources = spec.get("job_sources") or []
                if not sources:
                    raise ValueError("no tomogram jobs to import from")
                df = pd.concat([job_rows(s) for s in sources], ignore_index=True)
            elif mode == TOMO_IMPORT_REFERENCE:
                df = _reference_rows(spec.get("reference_star") or "")
            else:
                df = _synthesize_rows(
                    [Path(p) for p in (spec.get("source_paths") or [])],
                    pixel_size_angstrom=float(spec.get("pixel_size_angstrom") or 0.0),
                    tomogram_binning=float(spec.get("tomogram_binning") or 1.0),
                    optics_group_name=spec.get("optics_group_name") or "opticsGroup1",
                    voltage=voltage,
                    spherical_aberration=spherical_aberration,
                    amplitude_contrast=amplitude_contrast,
                    invert_defocus_hand=invert_defocus_hand,
                    project_tag=project_tag,
                )
        except (OSError, ValueError) as e:
            # Reported, not swallowed: the batch's own row in the dialog carries this text.
            report["error"] = str(e)
            df = None
        if df is not None and len(df):
            rows, renamed, skipped = _fold_batch_rows(df, seen_names, seen_paths)
            frames.append(rows)
            report.update(count=len(rows), renamed=renamed, skipped=skipped)
        reports.append(report)

    # Both guards run before the write: out_path is the committed star, and rewriting it
    # from a rebuild the caller is about to reject would be a silent side effect of a
    # failed import.
    if require_last and reports and reports[-1]["error"]:
        raise ValueError(reports[-1]["error"])
    if not frames:
        problems = "; ".join(r["error"] for r in reports if r["error"])
        raise ValueError(f"tomogram import produced no rows{f' ({problems})' if problems else ''}")

    merged = pd.concat(frames, ignore_index=True)
    tilt_series = _write_tilt_series(merged, out_path.parent)
    # starfile maps the dict key to the block name -> 'global' becomes data_global.
    starfile.write({"global": merged}, out_path, overwrite=True)
    return {"count": len(merged), "per_batch": reports, "tilt_series_star": tilt_series}


def _write_tilt_series(merged, out_dir: Path) -> str | None:
    """For the tomograms that came from jobs (`cbOrigin*`): `tilt_series.star` beside the tomograms star, each
    tomogram's name and per-tilt-series star. A tomogram renamed here (two sources' `Position_1`) gets a copy of
    its star with the block renamed, in `tilt_series/`, which its row then names; nothing is written in a
    source. Returns the star's path, or None (and removes an earlier one) when no tomogram came from a job."""
    import starfile

    from services.particles.set_origins import (
        ORIGIN_PROJECT,
        ORIGIN_TILT_STAR,
        ORIGIN_TOMO,
        TILT_DIR,
        write_renamed_tilt_star,
    )

    star = out_dir / TILT_SERIES_STAR
    if ORIGIN_PROJECT not in merged.columns:
        star.unlink(missing_ok=True)
        return None
    from_jobs = merged[ORIGIN_PROJECT].notna() & (merged[ORIGIN_PROJECT].astype(str).str.len() > 0)
    for i in merged.index[from_jobs]:
        name, there = str(merged.at[i, "rlnTomoName"]), str(merged.at[i, ORIGIN_TOMO])
        if name != there:
            copy = out_dir / TILT_DIR / f"{name}.star"
            write_renamed_tilt_star(
                Path(merged.at[i, ORIGIN_TILT_STAR]), copy, there, name, Path(merged.at[i, ORIGIN_PROJECT])
            )
            merged.at[i, "rlnTomoTiltSeriesStarFile"] = str(copy)
    starfile.write(
        {"global": merged.loc[from_jobs, ["rlnTomoName", "rlnTomoTiltSeriesStarFile"]].reset_index(drop=True)},
        star,
        overwrite=True,
    )
    return str(star)
