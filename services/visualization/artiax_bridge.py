"""Bridge between crboost picks and ChimeraX/ArtiaX ``.coords`` files.

ArtiaX ``.coords`` format (decoded from a real ArtiaX 0.6 save, 2026-06-02):
    - Plain text, one particle per line, three whitespace-separated floats: ``X Y Z``.
    - No header, no orientation columns (positions only — matches our positions-only design).
    - Values are PHYSICAL ÅNGSTRÖM from the volume CORNER origin, i.e. ``voxel_index * pixel_size``,
      using the tomogram's pixel size (ArtiaX reads it from the MRC header — confirmed 6.2 Å). A
      mid-slice click on a 512-voxel-thick tomogram saved ``Z = 1584.1 Å = 255.5 * 6.2``.

Mapping to our RELION-5 centered-Å convention (see :mod:`services.visualization.coords`)::

    voxel               = artiax_corner_angst / pixel_size
    centered_angst      = (voxel - N / 2) * pixel_size          # = artiax_corner_angst - (N/2)*pixel_size
    artiax_corner_angst = voxel * pixel_size,  voxel = centered_angst / pixel_size + N / 2

Because we own both directions and exchange in ArtiaX's corner-Å (= voxel*px) space, the ``N/2``
centering term cancels in any our→ArtiaX→our round trip regardless of the half-voxel parity
ambiguity. See ``docs/ARTIAX_BRIDGE_PLAN.md``.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from collections.abc import Sequence

import numpy as np
import pandas as pd
import starfile

from services.visualization.coords import TomogramGeometry, centered_angst_dataframe, picks_centered_angst

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
    candidates_star: Path, tomograms_star: Path, tomo_name: str, out_path: Path, project_root: Path | None = None
) -> int:
    """Write our picks for one tomogram to an ArtiaX ``.coords`` file. Returns the pick count."""
    parts = _find_table(Path(candidates_star), "rlnTomoName")
    parts = parts[parts["rlnTomoName"] == tomo_name]
    frame = frame_for_tomo(tomograms_star, tomo_name, project_root=project_root)
    coords = centered_angst_to_artiax(picks_centered_angst(parts), frame)
    write_coords_file(coords, Path(out_path))
    logger.info("Wrote %d picks for %s -> %s", len(coords), tomo_name, out_path)
    return len(coords)


def import_coords_to_centered_star(
    coords_path: Path, tomograms_star: Path, tomo_name: str, out_star: Path, project_root: Path | None = None
) -> int:
    """Ingest an ArtiaX ``.coords`` (manual picks) → a RELION-5 centered-Å particles
    star for one tomogram. The inverse of :func:`export_tomo_picks_to_coords`, using
    the SAME :class:`TomogramGeometry`, so the ``N/2`` centering cancels and an
    export→ArtiaX→import round trip is parity-exact. Returns the pick count.

    Minimal schema: ``rlnTomoName`` + the three ``rlnCenteredCoordinate*Angst`` columns
    (positions only — refinement derives angles). Merge into a `_combined` set adds any
    further columns. Lands at e.g. ``Curation/<species>/<tomo>/manual.star``.
    """
    coords = read_coords_file(Path(coords_path))
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


def swap_chimerax_commands(
    recon_mrc, pick_file=None, *, clear: bool = True, force_apix: float | None = None, cwd=None
) -> list[str]:
    """The in-session "swap to this tomogram" command sequence — what crboost POSTs
    over the REST channel (see ``backend.send_chimerax_command``) to load a new
    tomogram + its picks into an ALREADY-running ArtiaX, instead of relaunching.

    ``clear`` prepends ``close session`` (a hard wipe of every open model — ArtiaX
    itself is dropped, then re-armed by the ``artiax start`` in the load backbone).
    ``close session`` is the safe clear because ArtiaX model ids are not stable
    across opens, so targeting individual ``close #N`` would be fragile. Verified
    end-to-end as one semicolon-joined command (2026-06-10). ``force_apix`` adds a
    pixel-size override for the just-opened tomogram (``#1.1.1`` after a fresh
    clear) for the rare case the MRC header pixel size can't be trusted.

    ``cwd`` appends a ChimeraX ``cd <dir>`` so ArtiaX's "Save particle list" dialog
    defaults to THIS tomogram's curation dir (the worker only sets the process cwd
    at launch — after a swap it points at the wrong tomo). Appended LAST so that even
    if a ChimeraX build lacks ``cd`` the tomogram + picks have already loaded (and the
    benign error is non-fatal; see ``backend.send_chimerax_command``).
    """
    cmds: list[str] = ["close session"] if clear else []
    cmds.extend(session_chimerax_commands(recon_mrc, pick_file))
    if force_apix is not None:
        cmds.append(f"artiax tomo #1.1.1 pixelSize {float(force_apix)}")
    if cwd is not None:
        cmds.append(f"cd {_cxc_quote(cwd)}")
    return cmds


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
        lines.append(f"# at:  {manual_coords}")
    lines.append("# — crboost ingests that .coords back into the pipeline.")
    return "\n".join(lines) + "\n"


def prepare_curation_bundle(
    candidates_star: Path,
    tomograms_star: Path,
    tomo_name: str,
    out_dir: Path,
    *,
    species: str = "",
    coords_label: str = "auto",
    project_root: Path | None = None,
    window_size: Sequence[int] | None = None,
) -> dict:
    """Materialize everything a ChimeraX/ArtiaX session needs to open one
    tomogram preloaded with a reference pick list.

    ``out_dir`` is the per-(species, tomo) :func:`curation_dir`, so the exports use
    plain, tomo-implicit names. Exports the picks in ``candidates_star`` (any star
    with ``rlnTomoName`` + centered-Å coords — the PyTOM auto list, or a workbench
    manual/merged star) to a ``.coords`` and writes ``open.cxc`` loading it.
    ``coords_label`` names that reference export: ``"auto"`` → ``auto.coords`` /
    ``open.cxc``; any other label (a re-curation of a specific list) →
    ``<label>_ref.coords`` / ``open__<label>.cxc`` so the import scan can tell
    crboost's reference export from the user's own save. Launch with ``CB_CXC``
    pointing at ``cxc_path``.
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
    is_auto = coords_label == "auto"
    ref_coords = out_dir / ("auto.coords" if is_auto else f"{_safe_slug(coords_label)}_ref.coords")
    n = export_tomo_picks_to_coords(Path(candidates_star), Path(tomograms_star), tomo_name, ref_coords, project_root)
    manual_coords = out_dir / "manual.coords"
    cxc_path = out_dir / ("open.cxc" if is_auto else f"open__{_safe_slug(coords_label)}.cxc")
    cxc_path.write_text(
        build_session_cxc(
            recon,
            ref_coords,
            tomo_name=tomo_name,
            species=species,
            pixel_size=frame.pixel_size,
            tomo_size=frame.size,
            manual_coords=manual_coords,
            window_size=window_size,
        )
    )
    logger.info("Curation bundle for %s [%s] -> %s (%d picks)", tomo_name, coords_label, cxc_path, n)
    return {
        "cxc_path": str(cxc_path),
        "auto_coords": str(ref_coords),
        "manual_coords": str(manual_coords),
        "recon": str(recon),
        "pixel_size": float(frame.pixel_size),
        "tomo_size": [int(v) for v in frame.size],
        "auto_count": int(n),
        "coords_label": coords_label,
        "commands": session_chimerax_commands(recon, ref_coords),
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
        )
        print(f"bundle for {args.tomo} ({info['auto_count']} auto picks):")
        print(f"  cxc:    {info['cxc_path']}")
        print(f"  recon:  {info['recon']}")
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
