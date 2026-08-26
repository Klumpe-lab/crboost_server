"""Bridge between crboost picks and ChimeraX/ArtiaX ``.coords`` files.

ArtiaX ``.coords`` format (decoded from a real ArtiaX 0.6 save, 2026-06-02):
    - Plain text, one particle per line, three whitespace-separated floats: ``X Y Z``.
    - No header, no orientation columns (positions only — matches our positions-only design).
    - Values are PHYSICAL ÅNGSTRÖM from the volume CORNER origin, i.e. ``voxel_index * pixel_size``,
      using the tomogram's pixel size (ArtiaX reads it from the MRC header — confirmed 6.2 Å). A
      mid-slice click on a 512-voxel-thick tomogram saved ``Z = 1584.1 Å = 255.5 * 6.2``.

Mapping to our RELION-5 centered-Å convention (see :mod:`services.particles.coords`)::

    voxel               = artiax_corner_angst / pixel_size
    centered_angst      = (voxel - N / 2) * pixel_size          # = artiax_corner_angst - (N/2)*pixel_size
    artiax_corner_angst = voxel * pixel_size,  voxel = centered_angst / pixel_size + N / 2

Because we own both directions and exchange in ArtiaX's corner-Å (= voxel*px) space, the ``N/2``
centering term cancels in any our→ArtiaX→our round trip regardless of the half-voxel parity
ambiguity. See ``docs/ARTIAX_BRIDGE_PLAN.md``.

Since roadmap 10-S3 the volume ArtiaX opens may be a block-binned DISPLAY copy of the recon rather
than the recon itself (it loads in seconds instead of ~20 s). That shifts the corner origin by one
exact constant — ``(N-1)/2 * pixel_size`` — which export subtracts and import adds; see
:func:`display_corner_offset`. It is recorded per curation dir in ``manifest.json``, so no caller
has to infer it, and it is 0.0 whenever the full-res volume is what opens.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from collections.abc import Sequence

import numpy as np
import pandas as pd
import starfile

from services.particles.coords import TomogramGeometry, centered_angst_dataframe, picks_centered_angst

logger = logging.getLogger(__name__)


def read_coords_file(path: Path) -> np.ndarray:
    """Read an ArtiaX ``.coords`` file → ``(N, 3)`` array of corner-Å ``X Y Z``.

    Lenient: skips blank/comment lines and any line without three float-parseable tokens.
    """
    rows = []
    for line in Path(path).read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        toks = s.split()
        try:
            rows.append([float(toks[0]), float(toks[1]), float(toks[2])])
        except (IndexError, ValueError):
            continue
    return np.asarray(rows, dtype=float).reshape(-1, 3)


def write_coords_file(coords_corner_angst: np.ndarray, path: Path) -> None:
    """Write ``(N, 3)`` corner-Å coordinates as an ArtiaX ``.coords`` file (``X Y Z``, no header)."""
    arr = np.asarray(coords_corner_angst, dtype=float).reshape(-1, 3)
    np.savetxt(str(path), arr, fmt="%.4f", delimiter=" ")


def artiax_to_centered_angst(coords_corner_angst: np.ndarray, frame: TomogramGeometry) -> np.ndarray:
    """ArtiaX corner-Å → RELION-5 centered-Å for one tomogram."""
    voxel = np.asarray(coords_corner_angst, dtype=float).reshape(-1, 3) / frame.pixel_size
    return frame.to_centered_angst(voxel)


def centered_angst_to_artiax(centered_angst: np.ndarray, frame: TomogramGeometry) -> np.ndarray:
    """RELION-5 centered-Å → ArtiaX corner-Å for one tomogram."""
    voxel = frame.to_voxel(np.asarray(centered_angst, dtype=float).reshape(-1, 3))
    return voxel * frame.pixel_size


def _find_table(star_path: Path, required_col: str) -> pd.DataFrame:
    d = starfile.read(star_path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame) and required_col in v.columns:
            return v
    raise ValueError(f"No table with column {required_col!r} in {star_path}")


def frame_for_tomo(tomograms_star: Path, tomo_name: str, project_root: Path | None = None) -> TomogramGeometry:
    tomo_df = _find_table(Path(tomograms_star), "rlnTomoName")
    rows = tomo_df[tomo_df["rlnTomoName"] == tomo_name]
    if rows.empty:
        raise ValueError(f"Tomogram {tomo_name!r} not found in {tomograms_star}")
    return TomogramGeometry.from_tomo_row(rows.iloc[0], project_root=project_root)


def export_tomo_picks_to_coords(
    candidates_star: Path,
    tomograms_star: Path,
    tomo_name: str,
    out_path: Path,
    project_root: Path | None = None,
    *,
    corner_offset_angst: float = 0.0,
) -> int:
    """Write our picks for one tomogram to an ArtiaX ``.coords`` file. Returns the pick count.

    ``corner_offset_angst`` is the display-binning term (see :func:`display_corner_offset`):
    the picks are written in the frame of the volume ArtiaX actually opens, which is the
    display-binned copy when one was generated. Zero when ArtiaX opens the full-res recon.
    """
    parts = _find_table(Path(candidates_star), "rlnTomoName")
    parts = parts[parts["rlnTomoName"] == tomo_name]
    frame = frame_for_tomo(tomograms_star, tomo_name, project_root=project_root)
    coords = centered_angst_to_artiax(picks_centered_angst(parts), frame) - float(corner_offset_angst)
    write_coords_file(coords, Path(out_path))
    logger.info("Wrote %d picks for %s -> %s", len(coords), tomo_name, out_path)
    return len(coords)


def import_coords_to_centered_star(
    coords_path: Path,
    tomograms_star: Path,
    tomo_name: str,
    out_star: Path,
    project_root: Path | None = None,
    *,
    corner_offset_angst: float = 0.0,
) -> int:
    """Ingest an ArtiaX ``.coords`` (manual picks) → a RELION-5 centered-Å particles
    star for one tomogram. The inverse of :func:`export_tomo_picks_to_coords`, using
    the SAME :class:`TomogramGeometry`, so the ``N/2`` centering cancels and an
    export→ArtiaX→import round trip is parity-exact. Returns the pick count.

    Minimal schema: ``rlnTomoName`` + the three ``rlnCenteredCoordinate*Angst`` columns
    (positions only — refinement derives angles). Merge into a `_combined` set adds any
    further columns. Lands at e.g. ``Curation/<species>/<tomo>/manual__<stem>.star``.

    ``corner_offset_angst`` undoes the display-binning term the export applied (see
    :func:`display_corner_offset`), so a pick placed on the display copy maps back into the
    full-res frame exactly. The caller reads it from the dir's ``manifest.json``.
    """
    coords = read_coords_file(Path(coords_path)) + float(corner_offset_angst)
    frame = frame_for_tomo(Path(tomograms_star), tomo_name, project_root=project_root)
    centered = artiax_to_centered_angst(coords, frame)
    df = centered_angst_dataframe(centered)
    df.insert(0, "rlnTomoName", tomo_name)
    out_star = Path(out_star)
    out_star.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({"particles": df}, out_star, overwrite=True)
    logger.info("Imported %d manual picks for %s -> %s", len(df), tomo_name, out_star)
    return len(df)


# ── ChimeraX/ArtiaX startup-session (.cxc) generation ──────────────────────────
#
# crboost knows the tomogram (recon path, pixel size, binned size) and our picks;
# ChimeraX/ArtiaX know nothing. A startup `.cxc` carries that knowledge into the
# session so the user never types a path or a pixel size — the worker launches
# `chimerax open_<tomo>.cxc` (CB_CXC env) and the session comes up preloaded.
#
# The command backbone below is the set CONFIRMED to work by hand (see
# ARTIAX_BRIDGE_PLAN.md "Landed (session 1)"): `artiax start`, `artiax open tomo`,
# bare `open <f>.coords` (ArtiaX auto-detects the format and the picks land on the
# density with no flip), `lighting simple`. Steps whose exact ArtiaX subcommand is
# not yet confirmed (forcing a particle-list pixel size, creating an empty manual
# list) are left as comments for the user to fill in on the first runtime test,
# rather than emitted as commands that could error and halt the script.


def _cxc_quote(path) -> str:
    """Quote a path for a ChimeraX command line. ChimeraX accepts a double-quoted
    string for an argument containing whitespace; embedded quotes are doubled.
    Cluster paths rarely need it, but be defensive."""
    s = str(path)
    if any(c in s for c in ' \t"'):
        return '"' + s.replace('"', '""') + '"'
    return s


def _safe_slug(name: str) -> str:
    """Filesystem-safe slug for a tomogram name (which can contain ``/`` etc.)."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(name)).strip("_") or "tomo"


def curation_dir(project_root, tomo_name: str, *, species_id: str = "", species_label: str = "") -> Path:
    """The per-(species, tomo) curation directory — one self-describing home for a
    single tomogram's ArtiaX bundle (``open.cxc`` + exported ``auto.coords``), the
    user's saved ``.coords``, the imported ``manual.star`` / ``merged.star``, the
    raw-save ``imports/`` archive, and (later) per-list extraction output.

    Keyed on ``species_id`` (the stable PickList-registry key; ``species_label`` then
    ``tomo_name`` are fallbacks) so prepare / import / merge / discover / prescan all
    resolve the SAME directory. A ``.coords`` under here belongs to ``tomo_name``,
    period — even with a generic filename — which is what kills the cross-tomo save
    bleed (a per-species dir could not attribute a generic save to a tomogram).
    """
    sp_slug = _safe_slug(species_id or species_label or tomo_name)
    return Path(project_root) / "Curation" / sp_slug / _safe_slug(tomo_name)


def user_coords_saves(curation_dir: Path) -> list[Path]:
    """The user's saved ArtiaX ``.coords`` in ONE tomogram's curation dir, newest first.

    Every ``.coords`` under a ``curation_dir`` is that tomogram's by construction, so
    newest-wins is bleed-proof. EXCLUDES crboost's own exports (``auto.coords``,
    ``*_ref.coords``); the non-recursive glob skips the ``imports/`` archive. Match is by
    extension + mtime, NOT a fixed name — the user may name the save anything. Shared by
    the explicit import (``session_service._discover_manual_coords``) and the server-side
    ``CurationWatcher`` so both apply ONE rule. Empty when the dir does not exist."""
    d = Path(curation_dir)
    if not d.is_dir():
        return []
    found: list[Path] = []
    for c in d.glob("*.coords"):
        if c.name == "auto.coords" or c.name.endswith("_ref.coords"):
            continue  # crboost's reference exports, not the user's save
        found.append(c)
    found.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return found


# ── The staging contract: manifest.json (roadmap 10-S2) ───────────────────────
#
# A `.coords` file carries zero identity — three floats per line. Under Model B the
# ONE moment identity is declared is when crboost prepares a scoped launch, and this
# is where it is written down: `Curation/<species>/<tomo>/manifest.json`, beside the
# generated `.cxc`. Everything downstream reads it instead of reversing the directory
# NAMES back into a species and a tomogram (which cannot survive a slug collision, and
# which is fragility #1/#3 of §1 in the roadmap). A dir with no manifest still falls
# back to slug matching, so pre-existing projects keep working.

MANIFEST_NAME = "manifest.json"
MANIFEST_SCHEMA = 1


def manifest_path(curation_dir: Path) -> Path:
    return Path(curation_dir) / MANIFEST_NAME


def read_manifest(curation_dir: Path) -> dict | None:
    """The declared scope of one curation dir, or None when it has none (a dir that
    predates 10-S2, or one the user created by hand). Never raises: an unreadable or
    malformed manifest is reported as absent and the caller falls back to slug
    attribution — which is surfaced, never silently guessed at."""
    p = manifest_path(curation_dir)
    try:
        data = json.loads(p.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        logger.warning("Unreadable curation manifest %s: %s", p, e)
        return None
    return data if isinstance(data, dict) else None


def write_manifest(curation_dir: Path, **fields) -> Path:
    """Write/replace the dir's manifest, preserving any keys a previous write left
    (so ``launch_curation_session`` can stamp ``launched_at`` onto the scope the bundle
    declared without re-deriving it)."""
    d = Path(curation_dir)
    d.mkdir(parents=True, exist_ok=True)
    data = read_manifest(d) or {}
    data.update(fields)
    data["schema"] = MANIFEST_SCHEMA
    p = manifest_path(d)
    p.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    return p


# ── Display-binned recon (roadmap 10-S3) ──────────────────────────────────────
#
# ArtiaX takes ~20 s to open a 1 GB / 268 M-voxel reconstruction, and the cost is
# COMPUTE, not I/O (ARTIAX_BRIDGE_PLAN.md:412-440). Model B has no in-session swap, so
# "change tomogram" means relaunch — which is only tolerable if the open is seconds.
# We therefore open a block-mean-binned copy, cached beside the recon.
#
# COORDINATES. ArtiaX writes `.coords` as `continuous_voxel_index * header_apix` from the
# volume CORNER. Block-binning by N maps binned voxel v onto full-res voxel
# u = v*N + (N-1)/2 (the block's CENTRE), so
#
#     corner_full = corner_display + (N-1)/2 * pixel_size_full
#
# — one constant, uniform on all three axes, exact. It is applied on export (subtract)
# and on import (add), and recorded in the manifest so the ingest side never has to
# infer it. It is NOT a rounding fudge: with N=1 it is 0.0 and every path is byte-for-byte
# what it was before. Trailing voxels that don't fill a block are cropped from the FAR
# edge, which leaves the corner origin — and therefore the whole mapping — untouched.

DISPLAY_SUFFIX = "_cbdisp"


def display_corner_offset(bin_factor: int, pixel_size_full: float) -> float:
    """``(N-1)/2 * pixel_size`` — the corner-Å shift between a block-binned display copy
    and the full-res volume. 0.0 for N<=1."""
    n = int(bin_factor)
    return 0.0 if n <= 1 else (n - 1) / 2.0 * float(pixel_size_full)


def display_recon_path(recon: Path, bin_factor: int) -> Path:
    r = Path(recon)
    return r.with_name(f"{r.stem}{DISPLAY_SUFFIX}{int(bin_factor)}.mrc")


def ensure_display_recon(recon: Path, bin_factor: int, pixel_size_full: float, size_full: Sequence[int]) -> dict:
    """The volume an ArtiaX session should actually open for ``recon``, generating the
    binned copy once and reusing it forever after.

    Returns ``{path, pixel_size, size, bin, corner_offset_angst, generated, note}``.
    ``note`` is non-empty exactly when the display copy could NOT be made and the caller
    is getting the full-res volume back instead (slow open) — it names the reason so the
    control center can say so rather than leave the user wondering why a launch takes
    20 s. Never raises: a failed downsample degrades to the original.

    Block-mean over N³ voxels, streamed one output Z-slab at a time (peak RSS ≈ one input
    slab), written to a uniquely-named temp file and ``os.replace``d in, so neither a crash
    nor two launches racing on the same tomogram (two species share one recon, and their
    curate clicks are guarded separately) can leave a half-written volume ArtiaX would open.
    """
    src = Path(recon)
    n = int(bin_factor)
    dims = [int(v) for v in size_full]
    full = {
        "path": str(src),
        "pixel_size": float(pixel_size_full),
        "size": dims,
        "bin": 1,
        "corner_offset_angst": 0.0,
        "generated": False,
        "note": "",
    }
    if n <= 1:
        return full
    dest = display_recon_path(src, n)
    off = display_corner_offset(n, pixel_size_full)
    binned = {
        "path": str(dest),
        "pixel_size": float(pixel_size_full) * n,
        "size": [v // n for v in dims],
        "bin": n,
        "corner_offset_angst": off,
        "generated": False,
        "note": "",
    }
    if dest.exists():
        return binned
    try:
        import mrcfile
    except ImportError as e:  # environment without mrcfile — say so, don't pretend
        return {**full, "note": f"display copy unavailable (mrcfile not importable: {e})"}

    tmp = dest.with_name(f".{dest.name}.tmp{uuid.uuid4().hex[:8]}")
    try:
        with mrcfile.mmap(str(src), mode="r", permissive=True) as m:
            data = m.data
            if data is None or data.ndim != 3:
                return {**full, "note": f"display copy skipped ({src.name} is not a 3-D MRC)"}
            nz, ny, nx = (int(v) for v in data.shape)
            bz, by, bx = nz // n, ny // n, nx // n
            if min(bz, by, bx) < 2:
                return {**full, "note": f"display copy skipped ({nx}x{ny}x{nz} too small to bin by {n})"}
            with mrcfile.new_mmap(str(tmp), shape=(bz, by, bx), mrc_mode=2, overwrite=True) as out:
                for k in range(bz):
                    slab = np.asarray(data[k * n : (k + 1) * n, : by * n, : bx * n], dtype=np.float32)
                    out.data[k] = slab.reshape(n, by, n, bx, n).mean(axis=(0, 2, 4))
                out.voxel_size = tuple(float(pixel_size_full) * n for _ in range(3))
        os.replace(tmp, dest)
    except (OSError, ValueError, MemoryError) as e:
        logger.warning("Could not build display recon for %s: %s", src, e)
        try:
            tmp.unlink()
        except OSError:
            pass
        return {**full, "note": f"display copy failed ({type(e).__name__}: {e})"}
    logger.info("Display recon (bin %d) for %s -> %s", n, src.name, dest)
    return {**binned, "size": [bx, by, bz], "generated": True}


def session_chimerax_commands(recon_mrc, auto_coords: Path | None = None) -> list[str]:
    """The ChimeraX command lines that load a tomogram + our picks into ArtiaX.

    This is the single source of the load backbone: the ``.cxc`` bakes these in for
    the auto-launch path, and the UI surfaces the same lines verbatim as copyable
    "paste these into ChimeraX" guidance for an already-running session — so the two
    never drift.
    """
    cmds = ["artiax start", f"artiax open tomo {_cxc_quote(recon_mrc)}"]
    if auto_coords is not None:
        cmds.append(f"open {_cxc_quote(auto_coords)}")
    cmds.append("lighting simple")
    return cmds


# NOTE (roadmap 10-S1, Model B): the in-session "swap to this tomogram" command
# sequence used to live here (`swap_chimerax_commands`) and was POSTed over the REST
# channel to re-point an ALREADY-running ArtiaX. It is DELETED. A session's scope is
# declared once, at launch, and nothing drives the viewer afterwards — see
# docs/roadmaps/picking_ui/10-external-picker-contract.md §1 for the five ways the
# swap made a session's *meaning* untrustworthy. Changing tomogram = relaunch (S3 makes
# that cheap) or ArtiaX's own file dialog, whose saves the staging contract catches.


def build_session_cxc(
    recon_mrc,
    auto_coords: Path | None = None,
    *,
    tomo_name: str = "",
    species: str = "",
    pixel_size: float | None = None,
    tomo_size: Sequence[int] | None = None,
    manual_coords: Path | None = None,
    window_size: Sequence[int] | None = None,
) -> str:
    """Build a ChimeraX startup ``.cxc`` that preloads one tomogram + our picks in ArtiaX.

    Pure string generation (no numpy), so it runs anywhere. ``pixel_size`` / ``tomo_size``
    are baked into the header for transparency (crboost knows them; ChimeraX reads the
    binned px from the MRC header). ``manual_coords`` is named in a comment as the save
    target for the user's manual picks.
    """
    lines: list[str] = ["# crboost ChimeraX/ArtiaX curation session — AUTO-GENERATED, safe to tweak."]
    if tomo_name:
        lines.append(f"#   tomogram : {tomo_name}")
    if species:
        lines.append(f"#   species  : {species}")
    if pixel_size is not None:
        lines.append(f"#   pixel    : {float(pixel_size):.4f} A/px (binned recon)")
    if tomo_size is not None:
        lines.append(f"#   size     : {' x '.join(str(int(v)) for v in tomo_size)} vox (binned)")
    lines.append("set bgColor black")
    if window_size is not None:
        lines.append(f"windowsize {int(window_size[0])} {int(window_size[1])}")
    lines.extend(session_chimerax_commands(recon_mrc, auto_coords))
    lines.append("# Manual picks: in the ArtiaX panel create a NEW particle list and pick into it")
    lines.append("# (do NOT add to the auto list opened above), then save that list as a .coords file")
    if manual_coords is not None:
        lines.append(f"# into:  {Path(manual_coords).parent}")
    lines.append("# Every .coords saved there becomes its OWN pick list, named after the file;")
    lines.append("# saving again under the same name UPDATES that list. crboost ingests within seconds.")
    return "\n".join(lines) + "\n"


def prepare_curation_bundle(
    candidates_star: Path | None,
    tomograms_star: Path,
    tomo_name: str,
    out_dir: Path,
    *,
    species: str = "",
    species_id: str = "",
    project_path: Path | None = None,
    coords_label: str = "auto",
    project_root: Path | None = None,
    window_size: Sequence[int] | None = None,
    display_bin: int = 1,
) -> dict:
    """Materialize everything a ChimeraX/ArtiaX session needs to open one
    tomogram preloaded with a reference pick list — and DECLARE that session's scope.

    This is the one moment identity exists (roadmap 10-S2): the manifest written here
    says which species and which tomogram this directory is for, so a ``.coords`` saved
    into it later is attributable without reversing directory names. ``out_dir`` is the
    per-(species, tomo) :func:`curation_dir`, so the exports use plain, tomo-implicit
    names. Exports the picks in ``candidates_star`` (any star with ``rlnTomoName`` +
    centered-Å coords — the PyTOM auto list, or a workbench manual/merged star) to a
    ``.coords`` and writes ``open.cxc`` loading it. ``coords_label`` names that reference
    export: ``"auto"`` → ``auto.coords`` / ``open.cxc``; any other label (a re-curation of
    a specific list) → ``<label>_ref.coords`` / ``open__<label>.cxc`` so the import scan
    can tell crboost's reference export from the user's own save. Launch with ``CB_CXC``
    pointing at ``cxc_path``.

    ``display_bin`` > 1 opens a block-binned display copy of the recon instead of the
    full-res volume (10-S3, generated once and cached beside it). Both the reference
    export and — via the manifest — the ingest of the user's saves then work in that
    volume's frame; see :func:`display_corner_offset`. ``display_note`` in the result is
    non-empty exactly when the copy could not be made and the session is getting the slow
    full-res open.

    ``candidates_star=None`` is the de-novo case: there are no reference picks to
    preload, so no ``.coords`` is written and the ``.cxc`` opens the bare tomogram.
    The user creates a particle list in ArtiaX and saves it; ingest is unchanged.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    frame = frame_for_tomo(Path(tomograms_star), tomo_name, project_root=project_root)
    recon = frame.recon_path
    if recon is None or not Path(recon).exists():
        raise FileNotFoundError(
            f"No reconstructed tomogram on disk for {tomo_name!r} "
            "(rlnTomoReconstructedTomogram) — curation needs the binned recon to open in ArtiaX."
        )
    disp = ensure_display_recon(Path(recon), display_bin, frame.pixel_size, frame.size)
    open_recon = Path(disp["path"])
    offset = float(disp["corner_offset_angst"])

    is_auto = coords_label == "auto"
    ref_coords: Path | None = None
    n = 0
    if candidates_star is not None:
        ref_coords = out_dir / ("auto.coords" if is_auto else f"{_safe_slug(coords_label)}_ref.coords")
        n = export_tomo_picks_to_coords(
            Path(candidates_star), Path(tomograms_star), tomo_name, ref_coords, project_root, corner_offset_angst=offset
        )
    manual_coords = out_dir / "manual.coords"
    cxc_path = out_dir / ("open.cxc" if is_auto else f"open__{_safe_slug(coords_label)}.cxc")
    cxc_path.write_text(
        build_session_cxc(
            open_recon,
            ref_coords,
            tomo_name=tomo_name,
            species=species,
            pixel_size=disp["pixel_size"],
            tomo_size=disp["size"],
            manual_coords=manual_coords,
            window_size=window_size,
        )
    )
    write_manifest(
        out_dir,
        species_id=species_id,
        species_label=species,
        tomo_name=tomo_name,
        project_path=str(project_path) if project_path else (str(project_root) if project_root else ""),
        tomograms_star=str(tomograms_star),
        declared_at=datetime.now().isoformat(timespec="seconds"),
        reference_exports=[ref_coords.name] if ref_coords is not None else [],
        cxc=cxc_path.name,
        recon=str(recon),
        open_recon=str(open_recon),
        display_bin=int(disp["bin"]),
        corner_offset_angst=offset,
        display_note=disp["note"],
    )
    logger.info("Curation bundle for %s [%s] -> %s (%d picks)", tomo_name, coords_label, cxc_path, n)
    return {
        "cxc_path": str(cxc_path),
        "auto_coords": str(ref_coords) if ref_coords is not None else None,
        "manual_coords": str(manual_coords),
        "curation_dir": str(out_dir),
        "manifest_path": str(manifest_path(out_dir)),
        "recon": str(recon),
        "open_recon": str(open_recon),
        "display_bin": int(disp["bin"]),
        "display_generated": bool(disp["generated"]),
        "display_note": disp["note"],
        "corner_offset_angst": offset,
        "pixel_size": float(disp["pixel_size"]),
        "tomo_size": [int(v) for v in disp["size"]],
        "auto_count": int(n),
        "coords_label": coords_label,
        "commands": session_chimerax_commands(open_recon, ref_coords),
    }


def _cli(argv=None) -> int:
    ap = argparse.ArgumentParser(description="crboost <-> ArtiaX .coords bridge")
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("export", help="write our picks for a tomogram as an ArtiaX .coords file")
    e.add_argument("--candidates", required=True, type=Path)
    e.add_argument("--tomograms", required=True, type=Path)
    e.add_argument("--tomo", required=True)
    e.add_argument("--out", required=True, type=Path)
    e.add_argument("--project-root", type=Path, default=None)

    i = sub.add_parser("import", help="ingest a manual .coords -> RELION-5 centered-A particles star")
    i.add_argument("--coords", required=True, type=Path)
    i.add_argument("--tomograms", required=True, type=Path)
    i.add_argument("--tomo", required=True)
    i.add_argument("--out", required=True, type=Path)
    i.add_argument("--project-root", type=Path, default=None)

    b = sub.add_parser("bundle", help="export picks + write an open_<tomo>.cxc to preload a session")
    b.add_argument("--candidates", required=True, type=Path)
    b.add_argument("--tomograms", required=True, type=Path)
    b.add_argument("--tomo", required=True)
    b.add_argument("--out-dir", required=True, type=Path)
    b.add_argument("--species", default="")
    b.add_argument("--project-root", type=Path, default=None)
    b.add_argument("--display-bin", type=int, default=1, help="open a block-binned display copy of the recon (10-S3)")

    r = sub.add_parser("selftest", help="pure round-trip check (no ArtiaX): centered->coords->centered")
    r.add_argument("--candidates", required=True, type=Path)
    r.add_argument("--tomograms", required=True, type=Path)
    r.add_argument("--tomo", required=True)
    r.add_argument("--project-root", type=Path, default=None)

    args = ap.parse_args(argv)

    if args.cmd == "export":
        n = export_tomo_picks_to_coords(args.candidates, args.tomograms, args.tomo, args.out, args.project_root)
        print(f"wrote {n} picks -> {args.out}")
        return 0

    if args.cmd == "import":
        n = import_coords_to_centered_star(args.coords, args.tomograms, args.tomo, args.out, args.project_root)
        print(f"imported {n} manual picks -> {args.out}")
        return 0

    if args.cmd == "bundle":
        info = prepare_curation_bundle(
            args.candidates,
            args.tomograms,
            args.tomo,
            args.out_dir,
            species=args.species,
            project_root=args.project_root,
            display_bin=args.display_bin,
        )
        print(f"bundle for {args.tomo} ({info['auto_count']} auto picks):")
        print(f"  cxc:    {info['cxc_path']}")
        print(f"  recon:  {info['recon']}")
        print(f"  opens:  {info['open_recon']}")
        print(f"  bin:    {info['display_bin']}  (corner offset {info['corner_offset_angst']:.4f} A)")
        if info["display_note"]:
            print(f"  NOTE:   {info['display_note']}")
        print(f"  launch: CB_CXC={info['cxc_path']} containers/chimerax_artiax/launch_curation_vnc.sh")
        return 0

    parts = _find_table(Path(args.candidates), "rlnTomoName")
    parts = parts[parts["rlnTomoName"] == args.tomo]
    frame = frame_for_tomo(args.tomograms, args.tomo, project_root=args.project_root)
    centered = picks_centered_angst(parts)
    back = artiax_to_centered_angst(centered_angst_to_artiax(centered, frame), frame)
    max_err = float(np.max(np.abs(centered - back))) if len(centered) else 0.0
    print(f"tomo={args.tomo} n={len(centered)} pixel_size={frame.pixel_size} size={frame.size.tolist()}")
    print(f"max round-trip error (A): {max_err:.3e}")
    return 0 if max_err < 1e-6 else 1


if __name__ == "__main__":
    raise SystemExit(_cli())
