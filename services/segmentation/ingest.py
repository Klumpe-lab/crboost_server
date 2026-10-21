"""Segmentations made outside crboost, registered on the project's tomograms without copying them.

A file names its tomogram and feature as `<tomogram>__<feature>.mrc`, the way easymode and Ais
name theirs; a file named otherwise (MemBrain's `<tomogram>_<model>_segmented.mrc`) is matched to
the longest tomogram name it starts with and takes the feature the import gives. The names are
those of one reconstruct job's tomograms: their tilt-series ids, the stems of their volumes and of
the denoised volumes made from them. The import says which reconstruction the files were computed
on, since two reconstructions of one tilt series share names and grids but not coordinates.

A file on another grid than its tomogram is listed and skipped, never resampled; one whose
encoding cannot be read off its data type and values (`infer_full_scale`) is skipped unless the
import names it. Each batch is a SEGMENTATIONS producer `importedSegmentations__<batch>`, its
segmentations.json and overlay masks in `Segmentations/imports/<batch>/`; the server registers its
volumes in the tilt-series registry from that manifest (`backend.commit_segmentation_import`).
"""

from __future__ import annotations

import glob
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path

import numpy as np

from services.job_lineage import job_dirs, recon_of
from services.models_base import JobType
from services.path_resolution_service import IMPORTED_SEGMENTATIONS_PREFIX as PRODUCER_PREFIX
from services.segmentation.manifest import SegmentationEntry, SegmentationManifest, feature_slug
from services.segmentation.volumes import infer_full_scale, probe_volume, summarize_volume

IMPORTS_DIR = Path("Segmentations") / "imports"
METHOD = "import"
# What may follow a tomogram's name in a file named after it.
_SEPARATORS = "_-."


def producer_id(batch: str) -> str:
    return f"{PRODUCER_PREFIX}{batch}"


def batch_of(producer: str) -> str | None:
    """The import batch a producer id names, or None for a job."""
    return producer[len(PRODUCER_PREFIX) :] if producer.startswith(PRODUCER_PREFIX) else None


def batch_dir(project_path: Path, batch: str) -> Path:
    return Path(project_path) / IMPORTS_DIR / batch


def new_batch_id(taken: set[str]) -> str:
    base = datetime.now().strftime("%Y%m%d-%H%M%S")
    batch, n = base, 2
    while batch in taken:
        batch, n = f"{base}-{n}", n + 1
    return batch


def list_volume_files(source: str) -> list[str]:
    """The .mrc files directly in a folder, or the ones a glob matches, sorted."""
    s = str(Path(source).expanduser()) if source and source.strip() else ""
    if not s:
        return []
    p = Path(s)
    paths = p.iterdir() if p.is_dir() else (Path(x) for x in glob.glob(s))
    return sorted(str(x) for x in paths if x.suffix.lower() == ".mrc" and x.is_file())


def tomogram_names(state, tilt_series: Iterable, recon_instance: str) -> dict[str, dict]:
    """``{name: {"ts", "mrc"}}`` for every tomogram the reconstruct job `recon_instance` made, from
    the registry's `tilt_series`: the tilt-series id and the stem of the volume name the
    reconstruction itself, the stem of each denoised volume made from it names that volume."""
    dirs = job_dirs(state)
    denoisers = [
        iid
        for iid, jm in state.jobs.items()
        if jm.job_type == JobType.DENOISE_PREDICT and recon_of(state, iid, dirs) == recon_instance
    ]
    names: dict[str, dict] = {}
    for ts in tilt_series:
        outputs = ts.tomogram.outputs if ts.tomogram is not None else {}
        recon = outputs.get(recon_instance)
        if recon is None or recon.output_type != "ts_reconstruct":
            continue
        for iid in denoisers:
            out = outputs.get(iid)
            if out is not None and out.output_type == "denoise_predict":
                names[Path(out.denoised_mrc).stem] = {"ts": ts.id, "mrc": str(out.denoised_mrc)}
        # Last: a denoised volume named like the reconstruction is the same grid; the
        # reconstruction is what the name stands for.
        mrc = str(recon.reconstructed_mrc)
        names[ts.id] = names[Path(mrc).stem] = {"ts": ts.id, "mrc": mrc}
    return names


def _match_prefix(stem: str, names: Iterable[str]) -> str | None:
    """The longest tomogram name `stem` starts with, followed by _ - or . and not by "_<digit>":
    `Position_29_5_…` continues the name `Position_29` into another tilt series' (`Position_29_5`),
    which may be missing from this reconstruction."""

    def follows(n: str) -> bool:
        rest = stem[len(n) :]
        return rest[:1] in tuple(_SEPARATORS) and not (rest[:1] == "_" and rest[1:2].isdigit())

    hits = [n for n in names if stem.startswith(n) and len(stem) > len(n) and follows(n)]
    return max(hits, key=len) if hits else None


def split_name(path: str, names: Iterable[str], feature: str) -> tuple[str | None, str | None, str]:
    """``(tomogram name, feature, why not)`` for one file: `<tomogram>__<feature>.mrc` names both,
    the tomogram exactly; any other name that starts with a tomogram's (`_match_prefix`) takes
    `feature`. A file named exactly like a tomogram is that tomogram, not a segmentation of it."""
    names = list(names)
    stem = Path(path).stem
    base, sep, suffix = stem.rpartition("__")
    if sep and suffix:
        if base not in names:
            return None, None, f"no tomogram of this reconstruction is named {base}"
        return base, feature_slug(suffix), ""
    if stem in names:
        return None, None, "named like the tomogram itself; a segmentation is named <tomogram>__<feature>.mrc"
    name = _match_prefix(stem, names)
    if name is None:
        return None, None, "its name starts with no tomogram name of this reconstruction"
    if not feature.strip():
        return name, None, "its name has no __<feature>: give the feature these files segment"
    return name, feature_slug(feature), ""


def _tomogram_grid(mrc: str) -> dict | str:
    """The grid of a tomogram, from its header: shape (z, y, x), bytes per voxel and voxel size;
    or why it could not be read."""
    import mrcfile

    try:
        with mrcfile.open(mrc, header_only=True, permissive=True) as m:
            h = m.header
            return {
                "shape_zyx": [int(h.nz), int(h.ny), int(h.nx)],
                "itemsize": int(mrcfile.utils.data_dtype_from_header(h).itemsize),
                "voxel": float(m.voxel_size.x),
            }
    except (OSError, ValueError) as e:
        return str(e)


def probe_volumes(paths: list[str], progress_cb: Callable[[int, int, str], None] | None = None) -> dict[str, dict]:
    """``{path: probe_volume(path)}``, or ``{"error": why}`` for a file that is not a volume. For a
    worker: every value is read, to know the encoding."""
    out: dict[str, dict] = {}
    for i, path in enumerate(paths):
        if progress_cb is not None:
            progress_cb(i, len(paths), Path(path).name)
        try:
            out[path] = probe_volume(Path(path))
        except (OSError, ValueError) as e:
            out[path] = {"error": str(e)}
    return out


def registered_paths(state, tilt_series: Iterable) -> dict[str, str]:
    """``{volume path: what registered it}`` for every segmentation volume the registry holds: a
    file registered once is not registered again by another import."""
    out: dict[str, str] = {}
    for ts in tilt_series:
        outputs = ts.tomogram.outputs if ts.tomogram is not None else {}
        for producer, output in outputs.items():
            if output.output_type != "segmentation":
                continue
            batch = batch_of(producer)
            rec = state.get_segmentation_import(batch) if batch is not None else None
            label = rec.label if rec is not None else producer
            for vol in output.volumes.values():
                out[str(vol.path)] = label
    return out


def plan_import(
    files: list[str],
    names: dict[str, dict],
    feature: str,
    full_scale: float | None,
    probes: dict[str, dict],
    registered: dict[str, str],
) -> list[dict]:
    """One row per file: what it registers as and what it is (`ok`), or why it is skipped
    (`reason`). `full_scale` is the encoding the import names; None reads it off each file.
    Files whose name matches no tomogram need no probe; the others need theirs in `probes`.
    A file the registry already holds (`registered`) is skipped."""
    grids: dict[str, dict | str] = {}
    claimed: dict[tuple[str, str], str] = {}
    rows = []
    for path in files:
        row: dict = {"path": path, "name": Path(path).name, "ts": None, "feature": None, "ok": False, "reason": ""}
        rows.append(row)
        name, feat, why = split_name(path, names, feature)
        if name is not None:
            row["ts"] = names[name]["ts"]
        if feat is None:
            row["reason"] = why
            continue
        if path in registered:
            row["reason"] = f"registered already, by {registered[path]}"
            continue
        target = names[name]
        row.update(feature=feat, mrc=target["mrc"])
        grid = grids.get(target["mrc"])
        if grid is None:
            grid = grids[target["mrc"]] = _tomogram_grid(target["mrc"])
        if isinstance(grid, str):
            row["reason"] = f"the header of its tomogram {Path(target['mrc']).name} is unreadable: {grid}"
            continue
        probe = probes.get(path) or {"error": "not read"}
        if "error" in probe:
            row["reason"] = f"unreadable: {probe['error']}"
            continue
        row.update(
            dtype=probe["dtype"],
            vmin=probe["min"],
            vmax=probe["max"],
            tomo_shape_zyx=grid["shape_zyx"],
            itemsize=grid["itemsize"],
            voxel=grid["voxel"],
        )
        if list(probe["shape_zyx"]) != list(grid["shape_zyx"]):
            z, y, x = probe["shape_zyx"]
            tz, ty, tx = grid["shape_zyx"]
            row["reason"] = f"its grid {x} × {y} × {z} is not its tomogram's {tx} × {ty} × {tz}"
            continue
        if full_scale:
            dt = np.dtype(probe["dtype"])
            if dt.kind in "iu" and full_scale > np.iinfo(dt).max:
                row["reason"] = f"{dt} never reaches {full_scale:g}; choose another encoding"
                continue
            row["full_scale"], row["encoding"] = float(full_scale), "as chosen"
        else:
            # easymode and Ais name their volumes <tomogram>__<feature>: one holding only 0 and 1 is one
            # with next to nothing segmented, not a mask (MemBrain's masks are named otherwise).
            fs, how = infer_full_scale(
                probe["dtype"], probe["min"], probe["max"], mask_if_binary="__" not in Path(path).stem
            )
            row["encoding"] = how
            if fs is None:
                row["reason"] = f"encoding not known ({how}); choose one"
                continue
            row["full_scale"] = fs
        key = (row["ts"], feat)
        if key in claimed:
            row["reason"] = f"{feat} of {row['ts']} comes from {claimed[key]} already"
            continue
        claimed[key] = row["name"]
        row["ok"] = True
    return rows


def summarize_import(
    rows: list[dict], vis_dir: str, progress_cb: Callable[[int, int, str], None] | None = None
) -> list[dict]:
    """The overlay masks and fraction of every row the plan accepted, made as the Segment job makes
    them, into `vis_dir`. A volume that cannot be read now is returned with the reason and not
    registered. For a worker."""
    todo = [r for r in rows if r.get("ok")]
    out = [r for r in rows if not r.get("ok")]
    for i, r in enumerate(todo):
        if progress_cb is not None:
            progress_cb(i, len(todo), r["name"])
        stem = f"{r['ts']}__{r['feature']}"
        xy, xz = Path(vis_dir) / f"{stem}_xy.png", Path(vis_dir) / f"{stem}_xz.png"
        try:
            s = summarize_volume(
                Path(r["path"]),
                slab_shape_zyx=r["tomo_shape_zyx"],
                slab_itemsize=r["itemsize"],
                xy_png=xy,
                xz_png=xz,
                full_scale=r["full_scale"],
            )
        except (OSError, ValueError, MemoryError) as e:
            out.append({**r, "ok": False, "reason": f"could not be read: {e}"})
            continue
        out.append({**r, "fraction_pct": s["fraction_pct"], "overlay_xy": str(xy), "overlay_xz": str(xz)})
    return out


def import_manifest(
    batch: str, rows: list[dict], *, recon_instance: str, source: str, feature: str, full_scale: float | None
) -> SegmentationManifest:
    """The batch's manifest: one entry per registered volume, on its tomogram's grid and voxel size,
    with the file referenced where it is."""
    entries = [
        SegmentationEntry(
            tomogram=r["ts"],
            feature=r["feature"],
            path=r["path"],
            shape_zyx=r["tomo_shape_zyx"],
            voxel_angstrom=r["voxel"],
            full_scale=r["full_scale"],
            source_mrc=r["mrc"],
            recon_instance=recon_instance,
            fraction_pct=r["fraction_pct"],
            overlay_xy=r["overlay_xy"],
            overlay_xz=r["overlay_xz"],
        )
        for r in rows
        if r.get("ok")
    ]
    return SegmentationManifest(
        producer=producer_id(batch),
        method=METHOD,
        parameters={
            "source": source,
            "feature": feature.strip() or None,
            "full_scale": full_scale,
            "recon_instance": recon_instance,
        },
        entries=entries,
    )
