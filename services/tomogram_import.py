"""Tomogram import — a project-level utility (NOT a pipeline job).

Particle-only / data-less projects have no preprocessing pipeline to produce a
``tomograms.star``; this module lets the PARTICLES-header import utility inject one,
either by SYNTHESIZING it from a set of reconstructed ``.mrc`` files (reading dims +
voxel size from each header) or by REFERENCING an existing ``tomograms.star``.

There is deliberately no ``JobType`` here: nothing in the manual-picking flow resolves
the tomograms.star as a job *output* — the dashboard, ArtiaX and per-list optset build
read it directly — so modelling it as a SLURM/IO job bought nothing. The committed star
is recorded on ``ProjectState.imported_tomograms``.

Heavy deps (pandas, starfile, mrcfile) are imported lazily so importing this module
stays light.

Schema mirrors a real tsReconstruct ``data_global`` block (post_handedness_fix oracle):
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


# What counts as a reconstructed tomogram on disk. `.rec` is IMOD/etomo's own name for the
# same MRC container and is what a tilt-series reconstructed outside Warp usually arrives as
# — excluding it made half the plausible source directories look empty (de-novo S5).
TOMOGRAM_SUFFIXES: tuple[str, ...] = (".mrc", ".rec")


TOMO_IMPORT_REFERENCE = TomoImportMode.REFERENCE.value
TOMO_IMPORT_SYNTHESIZE = TomoImportMode.SYNTHESIZE.value


def probe_mrc_metadata(paths: list[Path]) -> list[dict]:
    """Per-file MRC header metadata for the pre-commit preview (headers only, no data).

    Each entry: ``{name, path, nx, ny, nz, voxel_size, has_voxel_size, error}``. A
    missing voxel size (``has_voxel_size=False``) is surfaced — NOT silently defaulted —
    so the import widget can flag it and let the user supply a pixel size before
    committing (CLAUDE.md 'Surfacing uncertainty')."""
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


def _absolutize(value, base: Path) -> str:
    if not value or not isinstance(value, str):
        return value
    p = Path(value)
    return str(p if p.is_absolute() else (base / p).resolve())


def _safe_tomo_name(stem: str, project_tag: str, seen: set) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{project_tag}_{stem}" if project_tag else stem)
    name = base
    i = 2
    while name in seen:
        name = f"{base}_{i}"
        i += 1
    seen.add(name)
    return name


def _reference_rows(reference_star: str):
    import starfile

    ref = Path(reference_star).expanduser()
    if not ref.is_file():
        raise FileNotFoundError(f"reference tomograms.star not found: {ref}")
    data = starfile.read(ref, always_dict=True)
    df = None
    for block in data.values():
        if hasattr(block, "columns") and "rlnTomoName" in block.columns:
            df = block.copy()
            break
    if df is None:
        raise ValueError(f"no tomogram block (rlnTomoName) found in {ref}")
    base = ref.parent
    for col in (
        "rlnTomoReconstructedTomogram",
        "rlnTomoReconstructedTomogramHalf1",
        "rlnTomoReconstructedTomogramHalf2",
        "rlnTomoTiltSeriesStarFile",
    ):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: _absolutize(v, base))
    return df


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
        # is available we RAISE rather than silently defaulting to 1.0 Å/px — the import
        # widget's pre-commit preview surfaces a missing voxel_size so the user supplies
        # `pixel_size_angstrom` first (CLAUDE.md 'Surfacing uncertainty').
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
    """Fold ONE batch's rows into the merged star, against what earlier batches already put
    there. Returns ``(rows, renamed, skipped)``.

    Two collisions, both reported and neither silent (same rule as
    ``drivers/subtomo_merge.py``'s cross-project guard):

      * **same recon file** — already imported by an earlier batch, so the row is DROPPED.
        Re-importing a directory you imported before must not double every tomogram.
      * **same tomogram name, different file** — the row is RENAMED (``<name>__2``). The
        EARLIER row keeps the name on purpose: picks, curation saves and pick lists are
        keyed on it, and renaming the incumbent would strand every one of them.
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
    """Write ``out_path`` (a ``tomograms.star``) from a LIST of import batches, in order.

    Each batch is a dict of the fields ``ProjectState.ImportBatch`` carries
    (``source_mode``, ``source_paths`` | ``reference_star``, ``pixel_size_angstrom``,
    ``tomogram_binning``, ``optics_group_name``); ``synthesize`` builds rows from the recon
    files, ``reference`` copies an existing ``tomograms.star`` (absolutizing its paths).

    The whole star is rebuilt from the whole list on every commit — it is a pure function of
    the batches, so a batch can be dropped later without any in-place surgery, and the
    re-read cost (MRC headers) is paid off the event loop by the caller.

    Returns ``{"count": total_rows, "per_batch": [{"count", "renamed", "skipped", "error"}]}``.
    A PRIOR batch that cannot be read (its reference star moved, its recon files are gone)
    contributes ``error`` and zero rows instead of failing the whole rebuild — nothing in the
    import dialog can repair a source directory that moved. The LAST batch is the one being
    added right now, and ``require_last`` makes its failure fatal BEFORE anything is written:
    otherwise a failed import would still have rewritten the committed star from the prior
    batches, quietly dropping the rows of any prior batch that had gone unreadable. Also
    raises when nothing at all could be written."""
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
            if (spec.get("source_mode") or TOMO_IMPORT_SYNTHESIZE) == TOMO_IMPORT_REFERENCE:
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

    # Both guards run BEFORE the write: out_path is the committed star, and rewriting it
    # from a rebuild the caller is about to reject would be a silent side effect of a
    # failed import.
    if require_last and reports and reports[-1]["error"]:
        raise ValueError(reports[-1]["error"])
    if not frames:
        problems = "; ".join(r["error"] for r in reports if r["error"])
        raise ValueError(f"tomogram import produced no rows{f' ({problems})' if problems else ''}")

    merged = pd.concat(frames, ignore_index=True)
    # starfile maps the dict key to the block name -> 'global' becomes data_global.
    starfile.write({"global": merged}, out_path, overwrite=True)
    return {"count": len(merged), "per_batch": reports}
