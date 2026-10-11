"""Readouts of the jobs after extraction: Class 3D and Reconstruct particle.

Class 3D (RELION ``relion_refine`` without auto-refine, ``--o run``): per iteration, the class
distribution and resolution (``run_itNNN_model.star``); for the last iteration, which class every
particle went to (``run_itNNN_data.star``), crossed with its template-matching score in σ, its
tomogram and its project; the central slices of every class map; and a gallery of each class's most
and least confidently assigned members (their extracted 2D stacks, averaged over visible tilts).

Reconstruct particle: the central slices of ``merged.mrc`` and the FSC between ``half1.mrc`` and
``half2.mrc``, unmasked and inside a soft sphere of the particle's diameter.

Each is a record (services/particles/records.py): the job's driver writes it as a sidecar with its
images under ``<job>/analytics/``; the server backfills it for jobs that finished earlier, with the
images under ``Analytics/records/<kind>/<instance>/``.
"""

from __future__ import annotations

import json
import logging
import re
from itertools import pairwise
from pathlib import Path
from typing import Any

import numpy as np

from services.models_base import JobType
from services.particles import analytics
from services.particles.records import (
    Producer,
    Provenance,
    Record,
    SourceStamp,
    fingerprint_of,
    job_sidecar_dir,
    project_record_path,
    read_record,
    write_record,
)

logger = logging.getLogger(__name__)

CLASS3D_KIND = "class3d"
CLASS3D_SCHEMA = 1
RECONSTRUCT_KIND = "reconstruct"
RECONSTRUCT_SCHEMA = 1

# σ bins of the class × σ table: below the lowest, between neighbours, and at or above the highest.
SIGMA_BIN_EDGES = (7.0, 8.0, 9.0, 10.0, 12.0)
# Members per class in each half of the gallery (most and least confidently assigned).
GALLERY_PER_END = 8
GALLERY_TILE_PX = 96
# A class is concentrated when this many tomograms hold more than CONCENTRATION_RATIO times the share
# of it that they hold of all particles (and the class has at least CONCENTRATION_MIN members).
CONCENTRATION_TOP = 3
CONCENTRATION_RATIO = 2.0
CONCENTRATION_MIN = 20
CONCENTRATION_RULE = (
    f"Its {CONCENTRATION_TOP} biggest tomograms hold more than {CONCENTRATION_RATIO:g}× the share of this class that "
    f"they hold of all particles (classes of {CONCENTRATION_MIN} or more): a class concentrated in a few tomograms is "
    "often a batch artefact (one bad lamella, one ice patch). A marker; nothing is dropped."
)

_IT_RE = re.compile(r"run_it(\d{3})_data\.star$")


def sigma_bin_labels() -> list[str]:
    e = SIGMA_BIN_EDGES
    return [f"<{e[0]:g}", *[f"{a:g}–{b:g}" for a, b in pairwise(e)], f"≥{e[-1]:g}"]


def _bin_index(sig: np.ndarray) -> np.ndarray:
    return np.digitize(np.nan_to_num(sig, nan=-np.inf), SIGMA_BIN_EDGES)


# ── Class 3D ─────────────────────────────────────────────────────────────────────────────────────


def class3d_iterations(job_dir: Path) -> list[int]:
    return sorted(int(m.group(1)) for p in job_dir.glob("run_it*_data.star") if (m := _IT_RE.search(p.name)))


def _star_blocks(path: Path) -> dict[str, Any]:
    import starfile

    return starfile.read(path, always_dict=True)


def _model_classes(model_star: Path) -> list[dict[str, Any]]:
    import pandas as pd

    df = _star_blocks(model_star).get("model_classes")
    if not isinstance(df, pd.DataFrame):
        return []
    out = []
    for i, r in enumerate(df.itertuples(index=False), start=1):
        row = r._asdict()
        out.append(
            {
                "class": i,
                "distribution": float(row.get("rlnClassDistribution", 0.0)),
                "resolution_ang": float(row["rlnEstimatedResolution"]) if "rlnEstimatedResolution" in row else None,
                "accuracy_rot": float(row["rlnAccuracyRotations"]) if "rlnAccuracyRotations" in row else None,
            }
        )
    return out


def class3d_data(job_dir: Path, *, input_particles: Path | None, assets_dir: Path) -> dict[str, Any]:
    """The Class 3D readout of one job dir; images go to `assets_dir`."""
    import pandas as pd

    its = class3d_iterations(job_dir)
    if not its:
        raise FileNotFoundError(f"{job_dir} has no run_itNNN_data.star yet")
    final = its[-1]
    iterations = []
    for it in its:
        model = job_dir / f"run_it{it:03d}_model.star"
        if model.exists():
            iterations.append({"iteration": it, "classes": _model_classes(model)})
    data_star = job_dir / f"run_it{final:03d}_data.star"
    blocks = _star_blocks(data_star)
    df = next((v for v in blocks.values() if isinstance(v, pd.DataFrame) and "rlnClassNumber" in v.columns), None)
    if df is None:
        raise ValueError(f"{data_star} has no rlnClassNumber column")
    sig = analytics.particle_sigma_scores(df)
    sigma_from = "data.star"
    if sig is None and input_particles is not None and input_particles.exists():
        sig = _sigma_by_particle_name(df, input_particles)
        sigma_from = str(input_particles) if sig is not None else ""
    classes = df["rlnClassNumber"].astype(int).to_numpy()
    tomos = df[analytics.TOMO_COL].astype(str).to_numpy() if analytics.TOMO_COL in df.columns else None
    n_classes = int(classes.max()) if classes.size else 0
    projects = _projects_for(input_particles)
    out: dict[str, Any] = {
        "final_iteration": final,
        "n_particles": len(df),
        "n_classes": n_classes,
        "iterations": iterations,
        "data_star": str(data_star),
        "sigma_from": sigma_from,
        "sigma_bins": sigma_bin_labels(),
        "classes": [],
    }
    bins = _bin_index(sig) if sig is not None else None
    total_by_tomo = _counts(tomos) if tomos is not None else {}
    for k in range(1, n_classes + 1):
        members = classes == k
        row: dict[str, Any] = {"class": k, "n": int(members.sum())}
        if bins is not None:
            row["sigma_bins"] = [int(np.count_nonzero(members & (bins == b))) for b in range(len(SIGMA_BIN_EDGES) + 1)]
        if tomos is not None:
            by_tomo = _counts(tomos[members])
            top = sorted(by_tomo.items(), key=lambda kv: -kv[1])[:CONCENTRATION_TOP]
            row["top_tomograms"] = [[t, n] for t, n in top]
            row["n_tomograms"] = len(by_tomo)
            share_class = sum(n for _, n in top) / max(1, row["n"])
            share_all = sum(total_by_tomo.get(t, 0) for t, _ in top) / max(1, len(df))
            row["concentration"] = round(share_class / share_all, 2) if share_all > 0 else None
            row["concentrated"] = bool(
                row["n"] >= CONCENTRATION_MIN and share_all > 0 and share_class / share_all > CONCENTRATION_RATIO
            )
            if projects:
                row["by_project"] = _counts(np.array([projects.get(t, "(unknown)") for t in tomos[members]]))
        out["classes"].append(row)
    out["class_maps"] = _render_class_maps(job_dir, final, n_classes, assets_dir)
    out["galleries"] = _render_galleries(df, classes, assets_dir)
    return out


def _counts(values: np.ndarray) -> dict[str, int]:
    keys, counts = np.unique(values, return_counts=True)
    return {str(k): int(n) for k, n in zip(keys, counts, strict=True)}


def _sigma_by_particle_name(df, input_particles: Path) -> np.ndarray | None:
    """σ scores joined from the input particles by rlnTomoParticleName, when the class output does
    not carry the score columns itself."""
    import pandas as pd

    if "rlnTomoParticleName" not in df.columns:
        return None
    src = next(
        (
            v
            for v in _star_blocks(input_particles).values()
            if isinstance(v, pd.DataFrame) and "rlnTomoParticleName" in v.columns
        ),
        None,
    )
    if src is None:
        return None
    s = analytics.particle_sigma_scores(src)
    if s is None:
        return None
    by_name = dict(zip(src["rlnTomoParticleName"].astype(str), s, strict=True))
    return np.array([by_name.get(n, np.nan) for n in df["rlnTomoParticleName"].astype(str)], dtype=float)


def _projects_for(input_particles: Path | None) -> dict[str, str]:
    """{tomogram: project} when the input is a cross-project merge (its summary sits beside it)."""
    if input_particles is None:
        return {}
    summary = input_particles.parent / "merge_summary.json"
    if not summary.exists():
        return {}
    from services.particles.analytics_store import _merge_projects

    return _merge_projects(summary)


def _render_class_maps(job_dir: Path, final: int, n_classes: int, assets_dir: Path) -> list[dict[str, Any]]:
    out = []
    for k in range(1, n_classes + 1):
        mrc = job_dir / f"run_it{final:03d}_class{k:03d}.mrc"
        if not mrc.exists():
            out.append({"class": k, "png": None, "map": str(mrc)})
            continue
        png = assets_dir / f"class{k:03d}_slices.png"
        out.append({"class": k, "png": str(render_central_slices(mrc, png)), "map": str(mrc)})
    return out


def _render_galleries(df, classes: np.ndarray, assets_dir: Path) -> dict[str, Any]:
    """Per class, the GALLERY_PER_END most and least confidently assigned members (by
    rlnMaxValueProbDistribution, else the template-matching σ) as one atlas each, rendered from
    their extracted 2D stacks."""
    from services.visualization.cutout_filters import get_filter_presets
    from services.visualization.preview_render import render_pick_cutouts_atlas

    if "rlnImageName" not in df.columns:
        return {"by": "", "classes": {}, "note": "the class output names no particle images"}
    if "rlnMaxValueProbDistribution" in df.columns:
        rank, by = df["rlnMaxValueProbDistribution"].astype(float).to_numpy(), "assignment probability"
    else:
        sig = analytics.particle_sigma_scores(df)
        if sig is None:
            return {"by": "", "classes": {}, "note": "nothing to rank members by"}
        rank, by = np.nan_to_num(sig, nan=-np.inf), "score (σ)"
    raw = [p for p in get_filter_presets() if p.get("key") == "raw"]
    images = df["rlnImageName"].astype(str).to_numpy()
    visible = df["rlnTomoVisibleFrames"].astype(str).to_numpy() if "rlnTomoVisibleFrames" in df.columns else None
    from services.particles.subtomo_link import _parse_visible_frames

    galleries: dict[str, Any] = {}
    for k in np.unique(classes):
        idx = np.flatnonzero(classes == k)
        order = idx[np.argsort(-rank[idx], kind="stable")]
        for end, picks in (("best", order[:GALLERY_PER_END]), ("worst", order[::-1][:GALLERY_PER_END])):
            entries = [
                {
                    "mrcs": Path(images[i].split("@", 1)[-1]),
                    "visible_frames": _parse_visible_frames(visible[i]) if visible is not None else None,
                }
                for i in picks
            ]
            atlas = assets_dir / f"class{int(k):03d}_{end}.png"
            index = assets_dir / f"class{int(k):03d}_{end}.json"
            meta = render_pick_cutouts_atlas(
                entries, atlas, index, tile_px=GALLERY_TILE_PX, cols=GALLERY_PER_END, filters=raw
            )
            layout = None
            if meta is not None:
                written = json.loads(Path(meta["index_path"]).read_text())
                layout = {key: written[key] for key in ("tile_px", "cols", "rows", "index")}
            galleries.setdefault(str(int(k)), {})[end] = {
                "atlas": meta["atlas_path"] if meta else None,
                "layout": layout,
                "values": [round(float(rank[i]), 3) for i in picks],
            }
    return {"by": by, "classes": galleries}


# ── Reconstruct particle ─────────────────────────────────────────────────────────────────────────

FSC_THRESHOLDS = (0.143, 0.5)
MASK_EDGE_PX = 5.0


def reconstruct_data(job_dir: Path, *, diameter_ang: float | None, assets_dir: Path) -> dict[str, Any]:
    import mrcfile

    merged, h1, h2 = job_dir / "merged.mrc", job_dir / "half1.mrc", job_dir / "half2.mrc"
    if not merged.exists():
        raise FileNotFoundError(f"{job_dir} has no merged.mrc yet")
    out: dict[str, Any] = {
        "merged": str(merged),
        "slices_png": str(render_central_slices(merged, assets_dir / "merged_slices.png")),
    }
    if not (h1.exists() and h2.exists()):
        out["fsc"] = None
        out["note"] = "no half maps beside merged.mrc"
        return out
    with mrcfile.open(str(h1), permissive=True) as m:
        a = np.asarray(m.data, dtype=np.float32)
        voxel = float(m.voxel_size.x)
    with mrcfile.open(str(h2), permissive=True) as m:
        b = np.asarray(m.data, dtype=np.float32)
    if a.shape != b.shape:
        raise ValueError(f"half maps differ in shape: {a.shape} vs {b.shape}")
    curves = {"unmasked": fsc_curve(a, b, voxel)}
    if diameter_ang:
        mask = soft_sphere(a.shape, diameter_ang / 2.0 / voxel, MASK_EDGE_PX)
        curves["sphere"] = fsc_curve(a * mask, b * mask, voxel)
    out["voxel"] = voxel
    out["mask_diameter_ang"] = diameter_ang
    out["fsc"] = {
        name: {
            "freq": [round(f, 5) for f in freq],
            "fsc": [round(v, 4) for v in fsc],
            "resolution": {str(t): crossing(freq, fsc, t) for t in FSC_THRESHOLDS},
        }
        for name, (freq, fsc) in curves.items()
    }
    return out


def soft_sphere(shape: tuple[int, ...], radius_px: float, edge_px: float) -> np.ndarray:
    """1 inside `radius_px` of the box centre, a raised cosine over `edge_px`, 0 beyond."""
    grids = np.ogrid[tuple(slice(0, n) for n in shape)]
    r = np.sqrt(sum((g - n // 2) ** 2 for g, n in zip(grids, shape, strict=True)))
    t = np.clip((r - radius_px) / edge_px, 0.0, 1.0)
    return (0.5 * (1 + np.cos(np.pi * t))).astype(np.float32)


def fsc_curve(a: np.ndarray, b: np.ndarray, voxel: float) -> tuple[list[float], list[float]]:
    """Fourier shell correlation of two equal-shape cubic volumes: (frequency in 1/Å, FSC) per shell
    out to Nyquist."""
    n = a.shape[0]
    fa, fb = np.fft.rfftn(a), np.fft.rfftn(b)
    kz = np.fft.fftfreq(a.shape[0])[:, None, None]
    ky = np.fft.fftfreq(a.shape[1])[None, :, None]
    kx = np.fft.rfftfreq(a.shape[2])[None, None, :]
    shell = np.rint(np.sqrt(kz**2 + ky**2 + kx**2) * n).astype(int).ravel()
    n_shells = n // 2 + 1
    keep = shell < n_shells
    cross = np.bincount(shell[keep], weights=(fa * np.conj(fb)).real.ravel()[keep], minlength=n_shells)
    pa = np.bincount(shell[keep], weights=(np.abs(fa) ** 2).ravel()[keep], minlength=n_shells)
    pb = np.bincount(shell[keep], weights=(np.abs(fb) ** 2).ravel()[keep], minlength=n_shells)
    with np.errstate(divide="ignore", invalid="ignore"):
        fsc = np.where(pa * pb > 0, cross / np.sqrt(pa * pb), 0.0)
    freq = np.arange(n_shells) / (n * voxel)
    return freq[1:].tolist(), fsc[1:].tolist()


def crossing(freq: list[float], fsc: list[float], threshold: float) -> float | None:
    """Resolution (Å) where the FSC first falls below `threshold`, interpolated between shells;
    None when it never does (resolution at Nyquist or better)."""
    for i in range(1, len(fsc)):
        if fsc[i] < threshold <= fsc[i - 1]:
            f = freq[i - 1] + (fsc[i - 1] - threshold) / (fsc[i - 1] - fsc[i]) * (freq[i] - freq[i - 1])
            return round(1.0 / f, 2) if f > 0 else None
    return None


# ── Images ───────────────────────────────────────────────────────────────────────────────────────


def render_central_slices(mrc: Path, out_png: Path, *, tile_px: int = 160) -> Path:
    """The central XY, XZ and YZ slices of a volume side by side, each normalised to its 1–99th
    percentile."""
    import mrcfile
    from PIL import Image

    with mrcfile.mmap(str(mrc), mode="r", permissive=True) as m:
        v = m.data
        nz, ny, nx = v.shape
        slices = [
            np.asarray(v[nz // 2], dtype=np.float32),
            np.asarray(v[:, ny // 2, :], dtype=np.float32),
            np.asarray(v[:, :, nx // 2], dtype=np.float32),
        ]
    tiles = []
    for s in slices:
        lo, hi = (float(x) for x in np.percentile(s, [1.0, 99.0]))
        u8 = (np.clip((s - lo) / (hi - lo if hi > lo else 1.0), 0, 1) * 255).astype(np.uint8)
        tiles.append(np.asarray(Image.fromarray(u8, mode="L").resize((tile_px, tile_px), Image.LANCZOS)))
    out_png.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(np.concatenate(tiles, axis=1), mode="L").save(str(out_png), format="PNG", optimize=True)
    return out_png


# ── Records ──────────────────────────────────────────────────────────────────────────────────────


def _input_particles(jm) -> Path | None:
    optset = str((getattr(jm, "paths", None) or {}).get("input_optimisation", "") or "")
    if not optset or not Path(optset).exists():
        return None
    from services.subtomo_merge import _parse_optimisation_set

    return _parse_optimisation_set(Path(optset))[0]


def compute_class3d_record(job_dir: Path, jm, *, instance_id: str, assets_dir: Path, computed_by: str) -> Record:
    its = class3d_iterations(job_dir)
    final_star = job_dir / f"run_it{its[-1]:03d}_data.star" if its else job_dir / "run_data.star"
    input_particles = _input_particles(jm)
    data = class3d_data(job_dir, input_particles=input_particles, assets_dir=assets_dir)
    data["instance_id"] = instance_id
    return Record(
        Provenance(
            kind=CLASS3D_KIND,
            schema=CLASS3D_SCHEMA,
            module=__name__,
            computed_by=computed_by,
            producer=Producer(JobType.CLASS3D.value, instance_id, str(job_dir)),
            sources=(SourceStamp.of(final_star), *((SourceStamp.of(input_particles),) if input_particles else ())),
            fingerprint=fingerprint_of([final_star]),
        ),
        data,
    )


def compute_reconstruct_record(
    job_dir: Path, *, instance_id: str, diameter_ang: float | None, assets_dir: Path, computed_by: str
) -> Record:
    data = reconstruct_data(job_dir, diameter_ang=diameter_ang, assets_dir=assets_dir)
    data["instance_id"] = instance_id
    merged = job_dir / "merged.mrc"
    return Record(
        Provenance(
            kind=RECONSTRUCT_KIND,
            schema=RECONSTRUCT_SCHEMA,
            module=__name__,
            computed_by=computed_by,
            producer=Producer(JobType.RECONSTRUCT_PARTICLE.value, instance_id, str(job_dir)),
            sources=tuple(SourceStamp.of(job_dir / n) for n in ("merged.mrc", "half1.mrc", "half2.mrc")),
            fingerprint=fingerprint_of([merged, job_dir / "half1.mrc", job_dir / "half2.mrc"]),
        ),
        data,
    )


def sidecar_paths(job_dir: Path, kind: str) -> tuple[Path, Path]:
    """(record, assets dir) of a driver-written readout."""
    return job_sidecar_dir(job_dir) / f"{kind}.json", job_sidecar_dir(job_dir) / kind


def readout_record(project_path: Path, state, instance_id: str) -> Record:
    """The Class 3D or Reconstruct particle readout of one job: the driver's sidecar when current,
    else the stored backfill when current, else a backfill computed now (images under the project's
    Analytics/records/)."""
    from services.particles.analytics_store import job_dir_of, species_diameter

    jm = state.jobs.get(instance_id)
    job_dir = job_dir_of(state, project_path, instance_id)
    if jm is None or job_dir is None:
        raise FileNotFoundError(f"{instance_id} has no job directory yet")
    if jm.job_type == JobType.CLASS3D:
        kind, schema = CLASS3D_KIND, CLASS3D_SCHEMA
        its = class3d_iterations(job_dir)
        if not its:
            raise FileNotFoundError(f"{instance_id} has written no iteration yet")
        fp = fingerprint_of([job_dir / f"run_it{its[-1]:03d}_data.star"])
    elif jm.job_type == JobType.RECONSTRUCT_PARTICLE:
        kind, schema = RECONSTRUCT_KIND, RECONSTRUCT_SCHEMA
        if not (job_dir / "merged.mrc").exists():
            raise FileNotFoundError(f"{instance_id} has not written merged.mrc yet")
        fp = fingerprint_of([job_dir / n for n in ("merged.mrc", "half1.mrc", "half2.mrc")])
    else:
        raise ValueError(f"{instance_id} is neither a Class 3D nor a Reconstruct particle job")
    stored_path = project_record_path(project_path, kind, instance_id)
    for path in (sidecar_paths(job_dir, kind)[0], stored_path):
        try:
            rec = read_record(path)
        except (OSError, ValueError, TypeError) as e:
            logger.warning("Ignoring unreadable readout %s: %s", path, e)
            rec = None
        if rec is not None and rec.provenance.fingerprint == fp and rec.provenance.schema == schema:
            return rec
    assets = stored_path.with_suffix("")
    if kind == CLASS3D_KIND:
        rec = compute_class3d_record(job_dir, jm, instance_id=instance_id, assets_dir=assets, computed_by="server")
    else:
        rec = compute_reconstruct_record(
            job_dir,
            instance_id=instance_id,
            diameter_ang=species_diameter(state, instance_id),
            assets_dir=assets,
            computed_by="server",
        )
    try:
        write_record(stored_path, rec)
    except OSError as e:
        logger.warning("Could not store readout %s: %s", stored_path, e)
    return rec


def class_iteration_table(record: Record) -> tuple[list[int], dict[int, list[float]]]:
    """(iterations, {class: distribution per iteration}) for the convergence chart."""
    its = [it["iteration"] for it in record.data.get("iterations") or []]
    per: dict[int, list[float]] = {}
    for it in record.data.get("iterations") or []:
        for c in it["classes"]:
            per.setdefault(int(c["class"]), []).append(float(c["distribution"]))
    return its, per
