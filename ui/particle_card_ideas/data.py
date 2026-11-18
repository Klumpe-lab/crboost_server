"""What the particle-card prototypes read, all of it read-only: a Class 3D's classes (share, count, Class 3D's own
resolution estimate, where its particles came from, the share at every iteration), a Refine 3D's gold-standard FSC,
an export's particles per project and tomogram, a particle set's particles per project, and every map as crisp
pictures (projections and central slices along z, y and x) at the map's own sampling.

The readers are module-level functions with plain arguments and results, so they run in a worker process
(`services.workers.run_in_worker`); the pictures come back as PNG data URLs, which needs no asset route and no
file written next to the project's data.
"""

from __future__ import annotations

import base64
import io
import json
import re
from pathlib import Path
from typing import Any

import numpy as np

HOME = Path("/groups/klumpe/_crboost_data_api/AGG412_STA")

# The jobs the page shows, by role.
CLASS3D_MAIN = "job013"  # the 412 at bin 4: K 6 from a featureless shell, done
CLASS3D_SMALL = "job014"  # the capsid class again, K 3
CLASS3D_RUNNING = "job005"  # the ribosome of one project, K 4, still iterating
REFINE = "job015"
MASK = "job016"
POSTPROCESS = "job017"
EXPORT = "job003"

_IT_RE = re.compile(r"run_it(\d{3})_model\.star$")


def project_of(tomo: str) -> str:
    """The project a pooled tomogram name came from: the part before its position."""
    for cut in ("_Position_", "_position_", "_TS_", "_ts_"):
        if cut in tomo:
            return tomo.split(cut, 1)[0]
    return tomo


def short_project(name: str) -> str:
    """A project's name without the campaign's prefix and the rerun suffix: what tells two of them apart."""
    s = name.removeprefix("AGG412_").removesuffix("_rerun")
    s = s.replace("_412_", "_").removesuffix("_412")
    head, _, rest = s.partition("_")
    if head.isdigit() and len(head) == 8 and rest:
        s = rest  # a session date in front of a name: the name tells them apart
    return s or name


def _star(path: Path) -> dict:
    import starfile

    return starfile.read(path, always_dict=True)


def _frame(blocks: dict, column: str):
    import pandas as pd

    return next((v for v in blocks.values() if isinstance(v, pd.DataFrame) and column in v.columns), None)


def _counts(values) -> dict[str, int]:
    keys, n = np.unique(np.asarray(values, dtype=str), return_counts=True)
    return {str(k): int(c) for k, c in sorted(zip(keys, n, strict=True), key=lambda kv: -kv[1])}


def _finite(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if np.isfinite(f) and f > 0 else None


# ── pictures ─────────────────────────────────────────────────────────────────


def _png(img: np.ndarray, px: int) -> str:
    from PIL import Image

    lo, hi = (float(x) for x in np.percentile(img, (0.5, 99.5)))
    u8 = (np.clip((img - lo) / (hi - lo if hi > lo else 1.0), 0, 1) * 255).astype(np.uint8)
    pic = Image.fromarray(u8, mode="L")
    if pic.size[0] != px:
        pic = pic.resize((px, px), Image.LANCZOS)
    buf = io.BytesIO()
    pic.save(buf, "PNG", optimize=True)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def map_views(mrc: str, px: int = 256) -> dict[str, Any]:
    """A map's projections (the sum along z, y and x) and central slices, each `px` square, protein white; its box
    and voxel size. A projection shows a 3-D class as a whole, a slice shows its inside."""
    import mrcfile

    with mrcfile.mmap(mrc, mode="r", permissive=True) as m:
        v = np.asarray(m.data, dtype=np.float32)
        apix = float(m.voxel_size.x)
    n = v.shape[0]
    return {
        "box": n,
        "apix": apix,
        "proj": [_png(v.sum(axis=a), px) for a in (0, 1, 2)],
        "slice": [_png(s, px) for s in (v[n // 2], v[:, n // 2], v[:, :, n // 2])],
    }


# ── Class 3D ─────────────────────────────────────────────────────────────────


def _model_classes(model: Path) -> list[dict[str, Any]]:
    df = _frame(_star(model), "rlnClassDistribution")
    if df is None:
        return []
    out = []
    for i, r in enumerate(df.to_dict("records"), start=1):
        out.append(
            {
                "class": i,
                "share": float(r.get("rlnClassDistribution", 0.0)),
                "res": _finite(r.get("rlnEstimatedResolution")),
                "acc_rot": _finite(r.get("rlnAccuracyRotations")),
            }
        )
    return out


def class3d(job: str, project: str = str(HOME)) -> dict[str, Any]:
    """A Class 3D's last iteration: per class its share, count, Class 3D's estimate, rotational accuracy, the
    projects and tomograms its particles came from, and its share at every iteration; the map of each class."""
    job_dir = Path(project) / "External" / job
    its = sorted(int(m.group(1)) for p in job_dir.glob("run_it*_model.star") if (m := _IT_RE.search(p.name)))
    if not its:
        return {"job": job, "iterations": [], "classes": []}
    final = its[-1]
    history = {it: _model_classes(job_dir / f"run_it{it:03d}_model.star") for it in its}
    data = job_dir / f"run_it{final:03d}_data.star"
    df = _frame(_star(data), "rlnClassNumber") if data.exists() else None
    total = 0 if df is None else len(df)
    classes = []
    for c in history[final]:
        k = c["class"]
        row = {**c, "n": round(c["share"] * total), "projects": {}, "n_tomos": 0, "top_tomo": None}
        if df is not None:
            members = df[df["rlnClassNumber"].astype(int) == k]
            row["n"] = len(members)
            if "rlnTomoName" in members.columns and len(members):
                tomos = members["rlnTomoName"].astype(str).to_numpy()
                by_tomo = _counts(tomos)
                row["n_tomos"] = len(by_tomo)
                row["top_tomo"] = next(iter(by_tomo.items()))
                row["projects"] = _counts([short_project(project_of(t)) for t in tomos])
        row["history"] = [next((x["share"] for x in history[it] if x["class"] == k), 0.0) for it in its]
        row["map"] = str(job_dir / f"run_it{final:03d}_class{k:03d}.mrc")
        classes.append(row)
    done = (job_dir / "RELION_JOB_EXIT_SUCCESS").exists()
    return {
        "job": job,
        "iterations": its,
        "final": final,
        "n_particles": total,
        "classes": classes,
        "done": done,
        "set": _set_read(project, job),
    }


# ── Refine 3D, mask, PostProcess ─────────────────────────────────────────────


def _crossing(freq: list[float], fsc: list[float], at: float) -> float | None:
    """The first spatial frequency (1/Å) past the low shells where the curve drops below `at`, interpolated."""
    for i in range(1, len(fsc)):
        if freq[i] > 0 and fsc[i - 1] >= at > fsc[i]:
            t = (fsc[i - 1] - at) / ((fsc[i - 1] - fsc[i]) or 1)
            return freq[i - 1] + t * (freq[i] - freq[i - 1])
    return None


def refine3d(job: str, project: str = str(HOME)) -> dict[str, Any]:
    job_dir = Path(project) / "External" / job
    model = job_dir / "run_model.star"
    out: dict[str, Any] = {"job": job, "converged": model.exists(), "set": _set_read(project, job)}
    if not model.exists():
        return out
    blocks = _star(model)
    general = blocks.get("model_general") or {}
    curve = blocks.get("model_class_1")
    freq = [float(x) for x in curve["rlnResolution"]]
    fsc = [float(x) for x in curve["rlnGoldStandardFsc"]]
    out.update(
        res=_finite(general.get("rlnCurrentResolution")),
        apix=float(general.get("rlnPixelSize", 0) or 0),
        box=int(general.get("rlnOriginalImageSize", 0) or 0),
        freq=freq,
        fsc=fsc,
        cross_143=_crossing(freq, fsc, 0.143),
        cross_5=_crossing(freq, fsc, 0.5),
        map=str(job_dir / "run_class001.mrc"),
    )
    data = job_dir / "run_data.star"
    df = _frame(_star(data), "rlnTomoName") if data.exists() else None
    if df is not None:
        out["n_particles"] = len(df)
        out["projects"] = _counts([short_project(project_of(t)) for t in df["rlnTomoName"].astype(str)])
    return out


def mask(job: str, project: str = str(HOME)) -> dict[str, Any]:
    job_dir = Path(project) / "External" / job
    stats = job_dir / "mask_stats.json"
    return {
        "job": job,
        "stats": json.loads(stats.read_text()) if stats.exists() else None,
        "map": str(job_dir / "mask.mrc"),
    }


def postprocess(job: str, project: str = str(HOME)) -> dict[str, Any]:
    job_dir = Path(project) / "External" / job
    failed = (job_dir / "RELION_JOB_EXIT_FAILURE").exists()
    err = (job_dir / "run.err").read_text(errors="replace").strip().splitlines() if failed else []
    return {"job": job, "failed": failed, "last_error": next((x for x in reversed(err) if x.strip()), "")}


# ── exports and sets ─────────────────────────────────────────────────────────


def export(job: str, project: str = str(HOME)) -> dict[str, Any]:
    """A Warp export's particles per project and per tomogram."""
    job_dir = Path(project) / "External" / job
    df = _frame(_star(job_dir / "particles.star"), "rlnTomoName")
    tomos = df["rlnTomoName"].astype(str).to_numpy()
    by_tomo = _counts(tomos)
    return {
        "job": job,
        "n_particles": len(df),
        "n_tomos": len(by_tomo),
        "per_tomo": list(by_tomo.values()),
        "projects": _counts([short_project(project_of(t)) for t in tomos]),
        "set": _set_read(project, job),
    }


def _job_input_set(params: dict, *, job: str | None = None, instance: str | None = None) -> str | None:
    """The set a job reads (by its folder or its instance id), from its recorded input."""
    jobs = params["jobs"]
    if instance is not None:
        jm = jobs.get(instance)
    else:
        jm = next((j for j in jobs.values() if (j.get("relion_job_name") or "").rstrip("/").endswith(job or "")), None)
    if jm is None:
        return None
    src = (jm.get("source_overrides") or {}).get("input_optimisation", "")
    return src.split("particle_set__", 1)[1] if "particle_set__" in src else None


def _job_folder(params: dict, instance: str) -> str:
    jm = params["jobs"].get(instance) or {}
    return (jm.get("relion_job_name") or instance).rstrip("/").split("/")[-1]


def set_info(project: str, set_id: str, params: dict | None = None) -> dict[str, Any]:
    """A particle set from its set.json alone (no star read): id, name, size, tomograms, particles per tomogram and
    per project, how it was made, and the chain of steps back to the picks, each with the particles it held."""
    params = params or json.loads((Path(project) / "project_params.json").read_text())
    meta = json.loads((Path(project) / "Analytics" / "sets" / set_id / "set.json").read_text())
    per_tomo = {str(k): int(v) for k, v in (meta.get("per_tomo") or {}).items()}
    projects: dict[str, int] = {}
    for t, n in per_tomo.items():
        name = short_project(project_of(t))
        projects[name] = projects.get(name, 0) + n
    out = {
        "id": set_id,
        "short": set_id.split("_", 1)[0],
        "name": meta.get("name", set_id),
        "n": int(meta.get("n_particles") or sum(per_tomo.values())),
        "n_tomos": int(meta.get("n_tomograms") or len(per_tomo)),
        "per_tomo": per_tomo,
        "projects": dict(sorted(projects.items(), key=lambda kv: -kv[1])),
        "how": (meta.get("recipe") or {}).get("op", ""),
    }
    out["chain"] = _chain(project, meta, params, depth=0)
    return out


def _chain(project: str, meta: dict, params: dict, depth: int) -> list[dict[str, Any]]:
    """The steps from this set back to the picks: this set, then what it was cut from (a classification, another
    set), then where the pool came from. Each step: kind (set / job / source), what to call it, its particles."""
    steps = [
        {"kind": "set", "id": meta["id"].split("_", 1)[0], "words": meta.get("name", ""), "n": meta.get("n_particles")}
    ]
    recipe = meta.get("recipe") or {}
    parents = recipe.get("parents") or []
    funnel = meta.get("funnel") or []
    if depth > 4 or not parents:
        return steps
    first = parents[0]
    if recipe.get("op") == "class_subset" and first.get("kind") == "job":
        classes = (recipe.get("params") or {}).get("classes") or []
        it = (recipe.get("params") or {}).get("iteration")
        before = funnel[0]["kept"] if funnel else None
        steps.append(
            {
                "kind": "job",
                "id": _job_folder(params, first.get("instance_id", "")),
                "words": f"class {', '.join(map(str, classes))} of Class 3D" + (f", it {it}" if it else ""),
                "n": before,
            }
        )
        parent_set = _job_input_set(params, instance=first.get("instance_id"))
        if parent_set:
            sub = json.loads((Path(project) / "Analytics" / "sets" / parent_set / "set.json").read_text())
            steps += _chain(project, sub, params, depth + 1)
        return steps
    if first.get("kind") == "set" and first.get("set_id"):
        sub = json.loads((Path(project) / "Analytics" / "sets" / first["set_id"] / "set.json").read_text())
        return steps + _chain(project, sub, params, depth + 1)
    filters = ((recipe.get("params") or {}).get("draft") or {}).get("filters") or []
    cut = next((f for f in filters if f.get("col") == "sigma" and f.get("lo") is not None), None)
    words = f"{len(parents)} pick list{'s' if len(parents) != 1 else ''}"
    if cut:
        words += f", ≥ {cut['lo']:g} σ"
    steps.append({"kind": "source", "id": "", "words": words, "n": None})
    return steps


def _set_read(project: str, job: str) -> dict[str, Any] | None:
    """The particle set a job reads, as `set_info` gives it."""
    params = json.loads((Path(project) / "project_params.json").read_text())
    set_id = _job_input_set(params, job=job)
    return set_info(project, set_id, params) if set_id else None


def everything(project: str = str(HOME)) -> dict[str, Any]:
    """Every reader the page needs but the pictures, in one call."""
    return {
        "main": class3d(CLASS3D_MAIN, project),
        "small": class3d(CLASS3D_SMALL, project),
        "running": class3d(CLASS3D_RUNNING, project),
        "refine": refine3d(REFINE, project),
        "mask": mask(MASK, project),
        "post": postprocess(POSTPROCESS, project),
        "export": export(EXPORT, project),
    }


def pictures(paths: list[str], px: int) -> dict[str, dict[str, Any]]:
    """`map_views` of several maps, keyed by path; a map not on disk is left out."""
    return {p: map_views(p, px) for p in paths if Path(p).exists()}
