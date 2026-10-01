#!/usr/bin/env python3
"""Particle-free alignment QC for tilt-series refinements (miss-alignment and friends).

    changes BEFORE_DIR AFTER_DIR
        Per series, what a refinement changed per tilt with the rigid volume shift removed
        (the numbers the missAlign driver writes to missalign_changes.json).

    controls --processing DIR --settings FILE --angpix A --out DIR
        Copies of an alignment's XMLs with known perturbations — a pure Z-gauge change (every score
        must stay put) and Gaussian per-tilt shifts along / across the tilt axis (the calibration
        curve) — plus an sbatch array that reconstructs them with tilt-split halves in the Warp
        container. --processing is the XML folder a tsReconstruct job read (its tsCtf input), --settings
        the alignment job's warp_tiltseries.settings (its tomostar/ sits beside it).

    reconstruct-task SPEC
        One task of that array (run by SLURM).

    score --arm NAME=PATH ... --reference NAME --out DIR     |     score --controls DIR --out DIR
        Tilt-split FSC between the even/odd half-tomograms of each arm (a tsReconstruct job run with
        halfmap_tilts, or a controls set), per XY tile inside the lamella, and per series the median
        band score of every arm against the reference: ln(SSNR_arm / SSNR_ref), positive = more
        self-consistent. Writes per_ts.csv, summary.csv, summary.md (and fsc_curves.png when
        matplotlib is available); per-arm FSC curves are cached under DIR/fsc/.

Frame-split halves (halfmap_frames) go through `score` the same way; since both halves share every
tilt's geometry they must score equal across arms — the negative control.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import mrcfile  # noqa: E402
import numpy as np  # noqa: E402

from services.analysis.alignment_changes import compare_alignments, read_alignment, rigid_residuals  # noqa: E402
from services.analysis.tilt_split_fsc import (  # noqa: E402
    FscCurves,
    crossover_frequency,
    fit_inclined_slab,
    paired_tile_scores,
    tile_fsc,
)

_RECON_NAME = re.compile(r"^(?P<ts>.+)_(?P<apix>\d+\.\d+)Apx\.mrc$")


# ── changes ───────────────────────────────────────────────────────────────────


def cmd_changes(args) -> int:
    before, after = Path(args.before), Path(args.after)
    names = sorted(p.stem for p in after.glob("*.xml") if (before / p.name).is_file())
    if not names:
        print(f"no XML present in both {before} and {after}", file=sys.stderr)
        return 1
    print(f"{'tilt-series':<40} {'tilts':>5} {'rms Å':>8} {'max Å':>8} {'rigid Å':>8} {'>thr':>5}  local warp")
    for name in names:
        row = compare_alignments(before / f"{name}.xml", after / f"{name}.xml", threshold_angstrom=args.threshold)
        warp = row["local_warp"]
        grid = f"{warp['dims']} {warp['rms_angstrom']:.1f}/{warp['max_angstrom']:.1f} Å" if warp else "-"
        print(
            f"{name:<40} {row['n_tilts']:>5} {row['residual_rms_angstrom']:>8.1f} {row['residual_max_angstrom']:>8.1f} "
            f"{row['gauge_rms_angstrom']:>8.1f} {len(row['tilts_over_threshold']):>5}  {grid}"
        )
    return 0


# ── controls ──────────────────────────────────────────────────────────────────


def _replace_list(text: str, name: str, values) -> str:
    """Replace the newline-separated per-tilt list of element `name`, leaving the rest of the XML as it is."""
    pattern = re.compile(rf"(<{name}>)(.*?)(</{name}>)", re.S)
    body = "\n".join(f"{v:.9g}" for v in values)
    new, n = pattern.subn(lambda m: m.group(1) + body + m.group(3), text, count=1)
    if n != 1:
        raise ValueError(f"no <{name}> list to replace")
    return new


def control_shifts(kind: str, angles_deg, axis_angle_deg, magnitude: float, rng) -> tuple[np.ndarray, np.ndarray]:
    """Per-tilt (ΔAxisOffsetX, ΔAxisOffsetY) in Å for one control.

    Warp projects a volume point through Rz(-a)·Ry(θ) (a = axis angle), then adds AxisOffsetX/Y in the image
    frame, so the tilt axis runs along (-sin a, cos a) in the image and across it along (cos a, sin a).
    - gauge: the volume moved by `magnitude` Å along Z — Δ = magnitude·sin θ across the axis; changes
      nothing but where the reconstruction sits.
    - along / across: independent Gaussian shifts of σ = `magnitude` Å per tilt along / across the axis,
      with their rigid (gauge) part projected out so all of it is misalignment."""
    theta = np.deg2rad(np.asarray(angles_deg, dtype=float))
    a = np.deg2rad(np.asarray(axis_angle_deg, dtype=float))
    if kind == "gauge":
        d = magnitude * np.sin(theta)
        return d * np.cos(a), d * np.sin(a)
    delta = rng.normal(0.0, magnitude, theta.size)
    if kind == "along":
        dx, dy = -delta * np.sin(a), delta * np.cos(a)
    elif kind == "across":
        dx, dy = delta * np.cos(a), delta * np.sin(a)
    else:
        raise ValueError(f"unknown control kind {kind!r}")
    fit = rigid_residuals(np.rad2deg(theta), axis_angle_deg, dx, dy)
    return fit.residual_x, fit.residual_y


def cmd_controls(args) -> int:
    from services.computing.slurm_service import SlurmConfig, write_sbatch_script
    from services.configs.config_service import get_config_service
    from services.jobs.spec import driver_launch_prefix

    processing, settings, out = Path(args.processing).resolve(), Path(args.settings).resolve(), Path(args.out).resolve()
    if not (settings.parent / "tomostar").is_dir():
        print(f"{settings.parent / 'tomostar'} not found: --settings must be the alignment job's", file=sys.stderr)
        return 1
    series = sorted(args.series or [p.stem for p in processing.glob("*.xml")])
    if not series:
        print(f"no tilt-series XMLs in {processing}", file=sys.stderr)
        return 1
    sigmas = [float(s) for s in args.sigmas.split(",")]
    sets = [("baseline", "none", 0.0), (f"gauge_z{args.gauge_voxels:g}", "gauge", args.gauge_voxels * args.angpix)]
    sets += [(f"{kind}_s{s:g}", kind, s * args.angpix) for kind in ("along", "across") for s in sigmas]

    spec: dict = {
        "settings": str(settings),
        "angpix": args.angpix,
        "out": str(out),
        # The frame averages Warp reconstructs from sit in the project (settings: <project>/External/jobNNN/).
        "binds": [str(out), str(processing), str(settings.parents[2])],
        "sets": {},
        "tasks": [],
    }
    for set_index, (name, kind, magnitude) in enumerate(sets):
        target = out / name / "warp_tiltseries"
        target.mkdir(parents=True, exist_ok=True)
        injected = {}
        for ts_index, ts in enumerate(series):
            src = processing / f"{ts}.xml"
            text = src.read_text(encoding="utf-8-sig")
            if kind != "none":
                geometry = read_alignment(src)
                rng = np.random.default_rng([args.seed, set_index, ts_index])
                dx, dy = control_shifts(kind, geometry.angles, geometry.axis_angle, magnitude, rng)
                text = _replace_list(text, "AxisOffsetX", geometry.offset_x + dx)
                text = _replace_list(text, "AxisOffsetY", geometry.offset_y + dy)
                injected[ts] = round(float(np.sqrt(np.mean(dx**2 + dy**2))), 3)
            (target / f"{ts}.xml").write_text(text, encoding="utf-8")
            spec["tasks"].append({"set": name, "ts": ts})
        spec["sets"][name] = {"kind": kind, "magnitude_angstrom": magnitude, "injected_rms_angstrom": injected}
    spec_path = out / "controls.json"
    spec_path.write_text(json.dumps(spec, indent=2))

    cfg = SlurmConfig.from_config_defaults()
    profile = get_config_service().get_job_resource_profile("tsReconstruct")
    if profile is not None:
        for field, value in profile.model_dump(exclude_none=True).items():
            setattr(cfg, field, value)
    command = (
        f"{driver_launch_prefix(server_dir=REPO, driver_script=Path(__file__).resolve())} reconstruct-task {spec_path}"
    )
    n = len(spec["tasks"])
    script = write_sbatch_script(out / "reconstruct_array.sh", cfg, command, array=f"0-{n - 1}%{args.throttle}")
    print(f"{len(sets)} sets x {len(series)} tilt-series = {n} reconstructions; submit with:\n  sbatch {script}")
    return 0


def cmd_reconstruct_task(args) -> int:
    from drivers.array_job_base import stage_per_ts_environment
    from drivers.driver_base import run_tool
    from drivers.ts_reconstruct import build_reconstruct_command
    from services.jobs.ts_reconstruct import TsReconstructParams

    spec = json.loads(Path(args.spec).read_text())
    index = int(os.environ["SLURM_ARRAY_TASK_ID"]) if args.index is None else args.index
    task = spec["tasks"][index]
    set_dir = Path(spec["out"]) / task["set"]
    staged_settings, staged_processing = stage_per_ts_environment(
        set_dir, task["ts"], set_dir / "warp_tiltseries", Path(spec["settings"])
    )
    params = TsReconstructParams(
        rescale_angpixs=spec["angpix"], halfmap_frames=0, halfmap_tilts=1, deconv=0, perdevice=1
    )
    command = build_reconstruct_command(params, staged_settings, staged_processing, set_dir / "output")
    print(f"[align_qc] {task['set']} / {task['ts']}: {command}", flush=True)
    run_tool(command, tool_name=params.get_tool_name(), cwd=set_dir, binds=spec["binds"])
    shutil.rmtree(staged_settings.parent, ignore_errors=True)
    return 0


# ── score ─────────────────────────────────────────────────────────────────────


def reconstruction_dir(path: Path) -> Path:
    """The folder holding <ts>_<apix>Apx.mrc and its even/ + odd/ halves, from a tsReconstruct job dir,
    a controls set's output, or that folder itself."""
    for candidate in (
        path / "warp_tiltseries" / "reconstruction",
        path / "output" / "reconstruction",
        path / "reconstruction",
        path,
    ):
        if (candidate / "even").is_dir() and (candidate / "odd").is_dir():
            return candidate
    raise FileNotFoundError(f"no reconstruction/ with even/ and odd/ halves under {path}")


def reconstructions(folder: Path) -> dict[str, str]:
    """Tilt-series name -> reconstruction file name, for those with both halves."""
    found = {}
    for mrc in folder.glob("*.mrc"):
        m = _RECON_NAME.match(mrc.name)
        if m and (folder / "even" / mrc.name).is_file() and (folder / "odd" / mrc.name).is_file():
            found[m["ts"]] = mrc.name
    return found


def measure(folder: Path, name: str, cache: Path, *, tile: int, tilt_step: float, force: bool) -> dict | None:
    """FSC curves of one reconstruction's tilt halves, cached as npz. None when no lamella shows."""
    if cache.is_file() and not force:
        data = np.load(cache)
        return {k: data[k] for k in data.files}
    with mrcfile.mmap(folder / name, mode="r", permissive=True) as full:
        voxel = float(full.voxel_size.x)
        slab = fit_inclined_slab(full.data)
    if slab is None:
        return None
    thickness = 2 * slab.half_thickness * voxel
    k_c = crossover_frequency(thickness, tilt_step)
    with (
        mrcfile.mmap(folder / "even" / name, mode="r", permissive=True) as even,
        mrcfile.mmap(folder / "odd" / name, mode="r", permissive=True) as odd,
    ):
        curves = tile_fsc(even.data, odd.data, slab, voxel, tile=tile, axial_radius=k_c / 2)
    record = {
        "tiles": curves.tiles,
        "shell_freq": curves.shell_freq,
        "shell_fsc": curves.shell_fsc,
        "shell_n": curves.shell_n,
        "axial_freq": curves.axial_freq,
        "axial_fsc": curves.axial_fsc,
        "axial_n": curves.axial_n,
        "axial_radius": np.float64(curves.axial_radius),
        "voxel": np.float64(voxel),
        "thickness_angstrom": np.float64(thickness),
        "k_c": np.float64(k_c),
        "slab": np.array([slab.z0, slab.slope_x, slab.slope_y, slab.half_thickness]),
    }
    cache.parent.mkdir(parents=True, exist_ok=True)
    np.savez(cache, **record)
    return record


def _curves(record: dict) -> FscCurves:
    return FscCurves(
        tiles=record["tiles"],
        shell_freq=record["shell_freq"],
        shell_fsc=record["shell_fsc"],
        shell_n=record["shell_n"],
        axial_freq=record["axial_freq"],
        axial_fsc=record["axial_fsc"],
        axial_n=record["axial_n"],
        axial_radius=float(record["axial_radius"]),
    )


def _band(text: str) -> tuple[float, float]:
    """'100,30' (resolutions in Å) -> (1/100, 1/30) in 1/Å."""
    lo, hi = (float(v) for v in text.split(","))
    return 1.0 / lo, 1.0 / hi


def _summary(values: np.ndarray, rng) -> dict:
    """Median with a bootstrap 95 % CI over series, the win rate, and a Wilcoxon signed-rank p."""
    v = values[np.isfinite(values)]
    if v.size == 0:
        return {"n": 0, "median": np.nan, "ci_lo": np.nan, "ci_hi": np.nan, "win_rate": np.nan, "wilcoxon_p": np.nan}
    boot = np.median(rng.choice(v, size=(2000, v.size), replace=True), axis=1)
    try:
        from scipy.stats import wilcoxon
    except ImportError:  # scipy is optional here: the p-value column stays empty without it
        p = np.nan
    else:
        p = float(wilcoxon(v).pvalue) if v.size >= 6 and np.any(v != 0) else np.nan
    return {
        "n": int(v.size),
        "median": float(np.median(v)),
        "ci_lo": float(np.percentile(boot, 2.5)),
        "ci_hi": float(np.percentile(boot, 97.5)),
        "win_rate": float(np.mean(v > 0)),
        "wilcoxon_p": p,
    }


def cmd_score(args) -> int:
    out = Path(args.out).resolve()
    injected: dict[str, dict] = {}
    if args.controls:
        controls = Path(args.controls).resolve()
        spec = json.loads((controls / "controls.json").read_text())
        arms = {name: controls / name for name in spec["sets"]}
        injected = {name: s["injected_rms_angstrom"] for name, s in spec["sets"].items()}
        reference = "baseline"
    else:
        arms = dict(a.split("=", 1) for a in args.arm)
        arms = {name: Path(path).resolve() for name, path in arms.items()}
        reference = args.reference
    if reference not in arms:
        print(f"reference {reference!r} is not one of the arms {sorted(arms)}", file=sys.stderr)
        return 1

    folders = {name: reconstruction_dir(path) for name, path in arms.items()}
    files = {name: reconstructions(folder) for name, folder in folders.items()}
    common = sorted(set.intersection(*(set(f) for f in files.values())))
    if args.series:
        common = [ts for ts in common if ts in set(args.series)]
    if not common:
        print(
            f"no tilt-series with tilt halves in every arm: { ({k: sorted(v) for k, v in files.items()}) }",
            file=sys.stderr,
        )
        return 1
    print(f"{len(arms)} arms x {len(common)} tilt-series; reference {reference}", flush=True)

    records: dict[str, dict[str, dict]] = {name: {} for name in arms}
    for name in arms:
        for ts in common:
            cache = out / "fsc" / f"tile{args.tile}_step{args.tilt_step:g}" / name / f"{ts}.npz"
            record = measure(
                folders[name], files[name][ts], cache, tile=args.tile, tilt_step=args.tilt_step, force=args.force
            )
            if record is None:
                print(f"  {name} / {ts}: no lamella found in the full tomogram; left out", flush=True)
                continue
            records[name][ts] = record
            print(
                f"  {name} / {ts}: {len(record['tiles'])} tiles, lamella {float(record['thickness_angstrom']):.0f} Å",
                flush=True,
            )

    shell_band, axial_band = _band(args.shell_band), _band(args.axial_band)
    rows = []
    for name in arms:
        if name == reference:
            continue
        for ts in common:
            ref, test = records[reference].get(ts), records[name].get(ts)
            if ref is None or test is None:
                continue
            shell, axial = paired_tile_scores(_curves(ref), _curves(test), shell_band=shell_band, axial_band=axial_band)
            rows.append(
                {
                    "arm": name,
                    "reference": reference,
                    "tilt_series": ts,
                    "shell_score": float(np.nanmedian(shell)) if np.isfinite(shell).any() else np.nan,
                    "axial_score": float(np.nanmedian(axial)) if np.isfinite(axial).any() else np.nan,
                    "tiles": int(shell.size),
                    "thickness_angstrom": round(float(test["thickness_angstrom"]), 1),
                    "k_c_inv_angstrom": round(float(test["k_c"]), 5),
                    "injected_rms_angstrom": injected.get(name, {}).get(ts, ""),
                }
            )
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "per_ts.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["arm"])
        writer.writeheader()
        writer.writerows(rows)

    rng = np.random.default_rng(0)
    summary = []
    for name in arms:
        if name == reference:
            continue
        mine = [r for r in rows if r["arm"] == name]
        for metric in ("shell_score", "axial_score"):
            stats = _summary(np.array([r[metric] for r in mine], dtype=float), rng)
            summary.append({"arm": name, "reference": reference, "metric": metric, **stats})
    with open(out / "summary.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary[0]) if summary else ["arm"])
        writer.writeheader()
        writer.writerows(summary)

    lines = [
        f"# Tilt-split FSC vs {reference}",
        "",
        f"Bands: shells {args.shell_band} Å, axial {args.axial_band} Å; tile {args.tile} px; "
        f"tilt step {args.tilt_step}°. "
        "Score = median over tiles of mean ln(SSNR_arm / SSNR_ref); "
        "positive = more self-consistent than the reference.",
        "",
        "| arm | metric | series | median | 95 % CI | win rate | Wilcoxon p |",
        "|---|---|---|---|---|---|---|",
    ]
    for s in summary:
        lines.append(
            f"| {s['arm']} | {s['metric']} | {s['n']} | {s['median']:+.3f} | {s['ci_lo']:+.3f} … {s['ci_hi']:+.3f} | "
            f"{s['win_rate']:.2f} | {s['wilcoxon_p']:.3g} |"
        )
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    _plot(out, records, reference)
    return 0


def _plot(out: Path, records: dict[str, dict[str, dict]], reference: str) -> None:
    """Median tile FSC per arm, shells and axial, over all series (fsc_curves.png)."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:  # plotting is optional; the CSVs carry the result
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for name, per_ts in records.items():
        if not per_ts:
            continue
        for ax, kind in zip(axes, ("shell", "axial"), strict=True):
            curves = np.concatenate([r[f"{kind}_fsc"] for r in per_ts.values()], axis=0)
            freq = next(iter(per_ts.values()))[f"{kind}_freq"]
            ax.plot(freq, np.nanmedian(curves, axis=0), lw=2 if name == reference else 1, label=name)
    for ax, title in zip(axes, ("full shells", "cylinder around the tilt axis"), strict=True):
        ax.set_title(title)
        ax.set_xlabel("spatial frequency (1/Å)")
        ax.set_ylabel("FSC (median over tiles)")
        ax.axhline(0, color="0.7", lw=0.5)
    axes[0].legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "fsc_curves.png", dpi=120)


# ── CLI ───────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("changes", help="rigid-removed per-tilt change between two XML folders")
    p.add_argument("before")
    p.add_argument("after")
    p.add_argument("--threshold", type=float, default=12.4, help="Å; tilts changed by more are counted")
    p.set_defaults(func=cmd_changes)

    p = sub.add_parser("controls", help="write perturbed XML sets + a reconstruction array")
    p.add_argument("--processing", required=True, help="XML folder a tsReconstruct job read (its tsCtf input)")
    p.add_argument("--settings", required=True, help="the alignment job's warp_tiltseries.settings")
    p.add_argument("--angpix", type=float, required=True, help="reconstruction pixel size; shifts are in its px")
    p.add_argument("--out", required=True)
    p.add_argument("--sigmas", default="0.5,1,2,3,5", help="per-tilt shift σ, px")
    p.add_argument("--gauge-voxels", type=float, default=40.0, help="Z shift of the gauge control, px")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--series", nargs="*", help="limit to these tilt-series")
    p.add_argument("--throttle", type=int, default=8, help="concurrent array tasks")
    p.set_defaults(func=cmd_controls)

    p = sub.add_parser("reconstruct-task", help="one task of a controls array (SLURM runs this)")
    p.add_argument("spec")
    p.add_argument("--index", type=int, help="task index (default: SLURM_ARRAY_TASK_ID)")
    p.set_defaults(func=cmd_reconstruct_task)

    p = sub.add_parser("score", help="tilt-split FSC of each arm against a reference")
    p.add_argument("--arm", action="append", default=[], help="NAME=PATH (tsReconstruct job dir or reconstruction/)")
    p.add_argument("--reference", help="arm the others are scored against")
    p.add_argument("--controls", help="a controls --out folder: its sets are the arms, baseline the reference")
    p.add_argument("--out", required=True)
    p.add_argument("--tile", type=int, default=256, help="XY tile edge, px")
    p.add_argument("--shell-band", default="100,30", help="resolution band of the shell score, Å")
    p.add_argument("--axial-band", default="40,15", help="resolution band of the axial score, Å")
    p.add_argument("--tilt-step", type=float, default=3.0, help="degrees; sets k_c and the axial cylinder radius")
    p.add_argument("--series", nargs="*", help="limit to these tilt-series")
    p.add_argument("--force", action="store_true", help="recompute cached FSC curves")
    p.set_defaults(func=cmd_score)

    args = parser.parse_args()
    if args.command == "score" and not args.controls and (not args.arm or not args.reference):
        parser.error("score needs --controls, or --arm (repeated) and --reference")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
