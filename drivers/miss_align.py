#!/usr/bin/env python3
"""miss-alignment driver — learned tilt-series alignment refinement.

Single (non-array) GPU-node job. It refines the Warp XMLs of the tilt-series in its input star
(aligntiltsWarp's output) and emits exactly what aligntiltsWarp emits — star + tilt_series/ +
warp_tiltseries/ — with the refined geometry, so a consumer cannot tell the two apart.
See docs/miss-alignment.md.

Flow:
  1. get_driver_context() + validate the three upstream inputs and the job settings (schedule,
     LR milestones, z_box, GPU plan); log the pixel size each macro-iteration runs at.
  2. Resume if a prior attempt checkpointed a macro-iteration here (after checking the inputs and
     the schedule so far are unchanged); otherwise stage the input star's XMLs as copies (the tool
     rewrites them in place — never the upstream ones) and their tilt stacks as symlinks (nothing
     when the tool rebuilds the stacks), stamp physical dims onto the XMLs in-container (the tool's
     warpylib rejects crboost's dimensionless XML), fit the volume box's Z extent when asked, and
     check that XML -> star reproduces the input star before any GPU time is spent.
  3. Write config.yaml from params + the resolved schedule.
  4. Run `miss-alignment train` (container-wrapped) for the remaining iterations.
  5. Require the last iteration's checkpoint, give the XMLs their full volume box back, report
     what changed per tilt (missalign_changes.json), then write the star + tilt_series/ from the
     refined XMLs and record the alignment in the registry.
"""

import sys
import os
import getpass
import json
import re
import shutil
import traceback
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import mrcfile
import numpy as np
import yaml

try:
    from drivers.array_job_base import read_tilt_series_names_from_input_star
    from drivers.driver_base import ToolCommand, get_driver_context, run_tool, require_producer_input
    from services.analysis.alignment_changes import compare_alignments
    from services.analysis.lamella_slab import centred_box_z, fit_slab, z_profile
    from services.configs.starfile_service import StarfileService
    from services.jobs.miss_align import MissAlignParams, ScheduleEntry, parse_device_list, parse_z_box
    from services.tilt_series import clear_registry, get_registry_for
    from services.tilt_series.adapters import MissAlignIngestAdapter
except ImportError as e:
    print(f"FATAL: Could not import services. {e}", file=sys.stderr)
    sys.exit(1)


# Largest XML -> star disagreement that is still rounding on the unrefined XMLs (the XML and the
# star are written from the same alignment at different precisions: measured <= 0.03° and
# <= 1.6 Å). Anything past these is a sign, unit or row-mapping error in the conversion.
MAPPING_LIMITS = {"rlnTomoYTilt": 0.1, "rlnTomoZRot": 0.5, "rlnTomoXShiftAngst": 10.0, "rlnTomoYShiftAngst": 10.0}

# Pixel size of the coarse reconstructions the Z box is fitted on.
ZBOX_RECONSTRUCTION_ANGSTROM = 25.0


# Runs in the tool's container (warpylib and torch live there). Stamps the physical dimensions the
# tool needs onto every staged XML; with "coarse" set, also reconstructs each series at a coarse
# pixel size from the alignment job's stack, for the Z-box fit. The stack is loaded and rescaled
# the way the tool loads its own (size_rounding_factors included), on the CPU.
STAMP_SCRIPT = """\
import json, sys
import mrcfile
import numpy as np
import torch
from warpylib import TiltSeries
from warpylib.ops import rescale

spec = json.load(open(sys.argv[1]))
coarse = spec.get("coarse")
for name, series in sorted(spec["series"].items()):
    xml = f"warp_tiltseries/{name}.xml"
    img_x, img_y = series["image"]
    ts = TiltSeries(xml)
    ts.image_dimensions_physical = torch.tensor([img_x, img_y], dtype=torch.float32)
    ts.volume_dimensions_physical = torch.tensor(spec["volume"], dtype=torch.float32)
    ts.ctf.pixel_size = spec["apix"]
    ts.save_meta(xml)
    print(f"[stamp] {name}: image {img_x:.1f} x {img_y:.1f} A, volume {spec['volume']} A", flush=True)
    if not coarse:
        continue
    with mrcfile.open(series["stack"], permissive=True) as mrc:
        images = torch.from_numpy(np.asarray(mrc.data, dtype=np.float32).copy())
        stack_apix = float(mrc.voxel_size.x)
    factor = coarse["downsample"]
    pixel = stack_apix * factor
    if factor > 1:
        size = ((images.shape[-2] // factor + 1) // 2 * 2, (images.shape[-1] // factor + 1) // 2 * 2)
        images = rescale(images, size=size)
    ts.size_rounding_factors = torch.tensor(
        [images.shape[-1] * pixel / img_x, images.shape[-2] * pixel / img_y, 1.0], dtype=torch.float32
    )
    with torch.no_grad():
        volume = ts.reconstruct_full(
            tilt_data=images, pixel_size=pixel, volume_dimensions_physical=tuple(spec["volume"]), apply_ctf=False
        )
    out = f"{coarse['dir']}/{name}.mrc"
    with mrcfile.new(out, overwrite=True) as mrc:
        mrc.set_data(volume.numpy().astype(np.float32))
        mrc.voxel_size = pixel
    print(f"[zbox] {name}: coarse reconstruction {tuple(volume.shape)} at {pixel:.2f} A", flush=True)
"""


@dataclass(frozen=True)
class StackInfo:
    path: Path
    extent_x: float  # Å, from the MRC cell
    extent_y: float
    apix: float


def read_stacks(upstream_processing: Path, ts_names: list[str]) -> dict[str, StackInfo]:
    """The alignment job's tilt stack per series. Its MRC cell is the physical image extent: Warp puts
    the image centre at half of it, and it holds whatever the stacks are later rebuilt at. The
    settings' Headerless* sizes describe headerless raw formats only, not the images."""
    stacks = {}
    for ts in ts_names:
        st = upstream_processing / "tiltstack" / ts / f"{ts}.st"
        with mrcfile.open(st, header_only=True, permissive=True) as mrc:
            x, y = float(mrc.header.cella.x), float(mrc.header.cella.y)
            apix = float(mrc.voxel_size.x)
        if x <= 0 or y <= 0 or apix <= 0:
            raise ValueError(f"{st}: MRC cell {x} x {y} Å at {apix} Å/px — cannot stamp the image dimensions.")
        stacks[ts] = StackInfo(path=st, extent_x=x, extent_y=y, apix=apix)
    return stacks


def common_stack_apix(stacks: dict[str, StackInfo]) -> float:
    """One pixel size for all stacks: the tool applies each iteration's downsample to every series."""
    values = sorted({round(s.apix, 4) for s in stacks.values()})
    if len(values) != 1:
        raise ValueError(f"the tilt stacks differ in pixel size ({values} Å/px); one downsample cannot serve them all")
    return values[0]


def allocated_gpu_count(default: int) -> int:
    """GPUs SLURM actually gave this job (renumbered 0..N-1 inside the --nv container).

    Read from SLURM's own count first so a manual `--gres` override still maps correctly; fall
    back to CUDA_VISIBLE_DEVICES, then the declared num_gpus, then 1. Never returns < 1. Unset
    inside the container by the wrapper, but the driver runs natively where SLURM sets them.
    """
    v = os.environ.get("SLURM_GPUS_ON_NODE", "")
    if v.isdigit() and int(v) > 0:
        return int(v)
    cvd = os.environ.get("CUDA_VISIBLE_DEVICES", "").strip()
    if cvd:
        n = len([x for x in cvd.split(",") if x.strip()])
        if n > 0:
            return n
    return max(1, default)


def device_plan(params: MissAlignParams, n_gpus: int) -> tuple[list[int], list[int]]:
    """(training GPUs, one GPU index per reconstruction worker). The first training_gpus cards train;
    the reconstruction workers go where reconstruction_devices says, else on every other card, else
    (all cards train) one per training card."""
    n_train = params.training_gpus
    if n_train > n_gpus:
        raise ValueError(f"training_gpus={n_train} but only {n_gpus} GPU(s) are allocated; raise num_gpus")
    training = list(range(n_train))
    if params.reconstruction_devices.strip():
        return training, parse_device_list(params.reconstruction_devices, n_gpus)
    if n_gpus > n_train:
        return training, list(range(n_train, n_gpus))
    return training, list(training)


def dataloaders_per_trainer(params: MissAlignParams, n_train: int, n_recon: int) -> int:
    """CPU data-loading workers per training GPU, within the tool's pool constraint
    pool_size // (training GPUs x workers) >= 2 x batch_size. Auto: the allocated CPUs left after the
    reconstruction workers and one main process per trainer."""
    cpus = int(os.environ.get("SLURM_CPUS_PER_TASK") or 1)
    cap = params.pool_size // (2 * params.batch_size * n_train)
    if cap < 1:
        raise ValueError(
            f"pool_size ({params.pool_size}) must be >= 2 x batch_size x training_gpus "
            f"({2 * params.batch_size * n_train}); raise pool_size or lower batch_size."
        )
    if params.dataloader_workers > 0:
        n = min(params.dataloader_workers, cap)
        if params.dataloader_workers > cap:
            print(
                f"[DRIVER] WARNING: dataloader_workers={params.dataloader_workers} exceeds the pool cap {cap} "
                f"(pool_size // (2 x batch_size x training_gpus)); clamped to {n}. Raise pool_size for more.",
                flush=True,
            )
        if n * n_train + n_recon > cpus:
            print(
                f"[DRIVER] WARNING: {n * n_train} data-loading + {n_recon} reconstruction workers > {cpus} "
                f"allocated CPUs — oversubscription slows loading. Raise cpus_per_task to match.",
                flush=True,
            )
        mode = "pinned"
    else:
        n = max(1, min((cpus - n_recon - n_train) // n_train, cap))
        mode = "auto"
    print(f"[DRIVER] dataloaders per trainer={n} ({mode}, cpus={cpus}, pool cap={cap})", flush=True)
    return n


def read_settings_dims(settings_path: Path) -> dict:
    """Parse warp_tiltseries.settings for the physical dims miss-alignment needs.

    apix comes from the settings PixelSize, not MicroscopeParams.pixel_size_angstrom, which
    defaults to 1.35 even when unset. The .settings file is the authoritative per-run source.
    Raises on any missing/blank/non-positive value.
    """
    root = ET.parse(settings_path).getroot()

    def _param(section: str, name: str) -> str:
        parent = root.find(section)
        if parent is None:
            raise ValueError(f"<{section}> section missing in {settings_path}")
        for p in parent.findall("Param"):
            if p.get("Name") == name:
                value = p.get("Value")
                if value is None or value.strip() == "":
                    raise ValueError(f"{section}/{name} is blank in {settings_path}")
                return value
        raise ValueError(f"{section}/{name} not found in {settings_path}")

    apix = float(_param("Import", "PixelSize"))
    if apix <= 0:
        raise ValueError(f"Non-positive PixelSize ({apix}) in {settings_path} — cannot stamp dims.")
    dims = {
        "apix": apix,
        "VX": int(float(_param("Tomo", "DimensionsX"))),
        "VY": int(float(_param("Tomo", "DimensionsY"))),
        "VZ": int(float(_param("Tomo", "DimensionsZ"))),
    }
    for k in ("VX", "VY", "VZ"):
        if dims[k] <= 0:
            raise ValueError(f"Non-positive dimension {k}={dims[k]} in {settings_path} — cannot stamp dims.")
    return dims


def log_schedule(entries: tuple[ScheduleEntry, ...], stack_apix: float, patch_size: int) -> None:
    """One line per macro-iteration: the entry, the pixel size it runs at, the patch field of view."""
    print(f"[DRIVER] Schedule on {stack_apix:.4g} Å/px stacks:", flush=True)
    for i, entry in enumerate(entries, 1):
        ds = entry.effective_downsample(stack_apix)
        pixel = entry.pixel_angstrom(stack_apix)
        line = (
            f"[DRIVER]   iter {i}: {entry} -> ds{ds} = {pixel:.4g} Å, "
            f"patch {patch_size} px = {patch_size * pixel:.0f} Å"
        )
        note = entry.rounding_note(stack_apix)
        print(line + (f"  WARNING: {note}" if note else ""), flush=True)


def build_config(
    params: MissAlignParams,
    training_directory: Path,
    entries: tuple[ScheduleEntry, ...],
    stack_apix: float,
    milestones: list[int],
) -> dict:
    """Assemble the miss-alignment config.yaml."""
    return {
        "general": {
            "training_directory": str(training_directory),
            "apply_ctf": False,  # CTF doubles cost with no alignment benefit
            "iteration_settings": [e.config_entry(stack_apix) for e in entries],
            "seed": params.seed,
        },
        "model_training": {
            "model_architecture": "default",
            "model_checkpoint": None,
            "loss_margin": 0.5,
            "learning_rate": 1.0e-3,
            "weight_decay": 1.0e-4,
            "max_epochs_per_iteration": params.max_epochs_per_iteration,
            "warmup_steps": 500,
            "multistep_lr_scheduler": {"milestones": milestones, "gamma": 0.5},
        },
        "data_loading": {
            "batch_size": params.batch_size,
            "patch_size": params.patch_size,
            "steps_per_epoch": params.steps_per_epoch,
        },
        "shift_generation": {
            "trajectory_probability": 0.5,
            "trajectory_max_shift": 10.0,
            "jitter_probability": 0.5,
            "jitter_max_std": 2.0,
            "outlier_probability": 0.5,
            "outlier_max_shift": 20.0,
            "fracture_probability": 0.5,
            "fracture_max_shift": 20.0,
        },
        "tilt_series_alignment": {
            "patch_size": params.patch_size,
            "patch_overlap": 0.1,
            "batch_size": params.batch_size,
        },
    }


def link_tree(src: Path, dst: Path) -> None:
    """Mirror `src` as real directories of symlinked files, so anything the tool adds lands here rather
    than in the upstream job. Only for stacks the tool reads and never writes."""
    dst.mkdir(parents=True, exist_ok=True)
    for child in src.iterdir():
        if child.is_dir():
            link_tree(child, dst / child.name)
        else:
            (dst / child.name).symlink_to(child.resolve())


def stage_inputs(
    ts_names: list[str], upstream_processing: Path, settings_src: Path, job_dir: Path, *, link_stacks: bool
) -> Path:
    """Fresh stage of the input star's tilt-series only. The tool refines every *.xml it finds in
    its training directory, so a TS the star dropped (failed or muted upstream) must not be here.

    Without `link_stacks` the tool writes tiltstack/<ts>/ itself (--prepare-stacks); it opens the
    .st, .rawtlt and thumbnails for writing, which through a symlink would overwrite the alignment
    job's own files, so nothing is linked."""
    staged = job_dir / "warp_tiltseries"
    if staged.exists():
        shutil.rmtree(staged)
    (staged / "tiltstack").mkdir(parents=True)
    for ts in ts_names:
        xml = upstream_processing / f"{ts}.xml"
        stack_dir = upstream_processing / "tiltstack" / ts
        if not xml.is_file() or not stack_dir.is_dir():
            raise FileNotFoundError(
                f"{ts} is in the input star but {xml.name} or tiltstack/{ts}/ is missing under {upstream_processing}"
            )
        shutil.copy2(xml, staged / xml.name)
        if link_stacks:
            link_tree(stack_dir, staged / "tiltstack" / ts)
    shutil.copy(settings_src, job_dir / "warp_tiltseries.settings")
    return staged


def check_frame_averages(staged: Path, ts_names: list[str], stacks: dict[str, StackInfo]) -> None:
    """--prepare-stacks rebuilds each stack from the frame averages the XML's movie paths lead to,
    resolved the way the tool resolves them (DataDirectory / MoviePath -> <movie dir>/average/<stem>.mrc),
    and takes the original pixel size from the first average's header. Check, before any GPU time,
    that every average exists and that the first covers the same physical image as the stack the
    alignment was done on."""
    missing: list[str] = []
    for ts in ts_names:
        root = ET.parse(staged / f"{ts}.xml").getroot()
        base = Path(root.get("DataDirectory") or staged)
        movies = [line.strip() for line in (root.findtext("MoviePath") or "").splitlines() if line.strip()]
        if not movies:
            raise ValueError(f"{ts}.xml has no <MoviePath> entries; --prepare-stacks cannot find its frames")
        averages = [(base / m).parent / "average" / f"{Path(m).stem}.mrc" for m in movies]
        missing += [str(a) for a in averages if not a.is_file()]
        if not averages[0].is_file():
            continue
        with mrcfile.open(averages[0], header_only=True, permissive=True) as mrc:
            extent = int(mrc.header.nx) * float(mrc.voxel_size.x)
        if abs(extent / stacks[ts].extent_x - 1) > 0.01:
            raise ValueError(
                f"{averages[0]} covers {extent:.1f} Å in X (header pixel size x width) but the aligned stack "
                f"covers {stacks[ts].extent_x:.1f} Å; the rebuilt stack would not match the alignment."
            )
    if missing:
        raise FileNotFoundError(
            f"{len(missing)} frame average(s) missing for --prepare-stacks, e.g. {missing[:3]}. "
            f"Set prepare_stacks_apix to 0 to align the existing stacks."
        )


_VOLUME_ATTR = re.compile(r'VolumeDimensionsAngstrom="[^"]*"')


def set_volume_dimensions(xml_path: Path, dims_angstrom: list[float]) -> None:
    """Rewrite only the XML's VolumeDimensionsAngstrom (the box the tool tiles its patches over and
    centres on the volume centre), in warpylib's own format."""
    text = xml_path.read_text(encoding="utf-8")
    value = ", ".join(f"{v:.9g}" for v in dims_angstrom)
    new, n = _VOLUME_ATTR.subn(f'VolumeDimensionsAngstrom="{value}"', text, count=1)
    if n != 1:
        raise RuntimeError(
            f"{xml_path}: no VolumeDimensionsAngstrom attribute to set — was the dimension stamp skipped?"
        )
    xml_path.write_text(new, encoding="utf-8")


def fit_z_boxes(zbox_dir: Path, ts_names: list[str], full_z: float) -> dict[str, float]:
    """Per series, the Z extent of the box fitted to the lamella in its coarse reconstruction; the
    full extent where no clear slab shows. Writes the fits to zbox/fits.json."""
    boxes: dict[str, float] = {}
    record: dict[str, dict] = {}
    for ts in ts_names:
        with mrcfile.open(zbox_dir / f"{ts}.mrc", permissive=True) as mrc:
            volume = np.asarray(mrc.data, dtype=np.float32)
            voxel = float(mrc.voxel_size.x)
        fit = fit_slab(z_profile(volume), voxel)
        if fit.found:
            boxes[ts] = centred_box_z(fit, full_z)
            print(
                f"[DRIVER] z_box {ts}: lamella {fit.thickness_angstrom:.0f} Å at half maximum, centre "
                f"{fit.center_offset_angstrom:+.0f} Å from the volume centre, extent "
                f"{fit.lower_angstrom:+.0f}..{fit.upper_angstrom:+.0f} Å -> box Z {boxes[ts]:.0f} Å "
                f"(full {full_z:.0f} Å)",
                flush=True,
            )
        else:
            boxes[ts] = full_z
            print(f"[DRIVER] z_box {ts}: no clear slab ({fit.reason}) -> full box Z {full_z:.0f} Å", flush=True)
        record[ts] = {
            "found": fit.found,
            "reason": fit.reason,
            "thickness_angstrom": round(fit.thickness_angstrom, 1),
            "center_offset_angstrom": round(fit.center_offset_angstrom, 1),
            "extent_angstrom": [round(fit.lower_angstrom, 1), round(fit.upper_angstrom, 1)],
            "background": fit.background,
            "peak": fit.peak,
            "box_z_angstrom": round(boxes[ts], 1),
        }
    (zbox_dir / "fits.json").write_text(json.dumps(record, indent=2))
    return boxes


def last_checkpointed_iteration(staged: Path) -> int:
    """Highest N >= 1 with iterN/model.ckpt, else 0. The tool writes iterN/ (XMLs + model.ckpt)
    after macro-iteration N and, given --start-at-iteration N, resumes from that checkpoint.
    iter0/ is the pre-training XML snapshot and holds no model."""
    done = [int(d.name[4:]) for d in staged.glob("iter*") if d.name[4:].isdigit() and (d / "model.ckpt").is_file()]
    return max((n for n in done if n >= 1), default=0)


def run_manifest(input_star: Path, ts_names: list[str], schedule: list[dict], params: MissAlignParams) -> dict:
    """What a checkpointed run was started from; a resume must match it."""
    return {
        "input_star": str(input_star),
        "input_star_mtime": input_star.stat().st_mtime,
        "tilt_series": ts_names,
        "schedule": schedule,
        "prepare_stacks_apix": params.prepare_stacks_apix,
        "z_box": str(parse_z_box(params.z_box)),
    }


def check_resume(manifest_path: Path, current: dict, resume_from: int) -> None:
    """Refuse to continue a run whose inputs, stacks, box or already-run iterations differ from what it
    was started with; later iterations may change (a schedule can be extended)."""
    if not manifest_path.is_file():
        print(f"[DRIVER] No {manifest_path.name} (run started by an older driver); resuming unchecked.", flush=True)
        return
    saved = json.loads(manifest_path.read_text())
    changed = [
        key for key in ("input_star", "tilt_series", "prepare_stacks_apix", "z_box") if saved.get(key) != current[key]
    ]
    if abs(saved.get("input_star_mtime", 0) - current["input_star_mtime"]) > 1e-3:
        changed.append("input_star (rewritten since)")
    if saved.get("schedule", [])[:resume_from] != current["schedule"][:resume_from]:
        changed.append(f"schedule of iterations 1..{resume_from}")
    if changed:
        raise RuntimeError(
            f"This job checkpointed iteration {resume_from}, but since then these changed: {', '.join(changed)}. "
            f"Resuming would mix two runs. Restore the settings, or delete {manifest_path.parent / 'warp_tiltseries'} "
            f"(or add a new missAlign job) to start over."
        )


def input_tomo_dimensions(input_star: Path, dims: dict) -> str:
    """The input star's own rlnTomoSizeX/Y/Z as 'XxYxZ' (the emitted star keeps them);
    the settings' tomogram dimensions when the star carries none."""
    df = StarfileService().read(input_star).get("global")
    cols = ("rlnTomoSizeX", "rlnTomoSizeY", "rlnTomoSizeZ")
    if df is not None and len(df) and all(c in df.columns for c in cols):
        return "x".join(str(int(df[c].iloc[0])) for c in cols)
    return f"{dims['VX']}x{dims['VY']}x{dims['VZ']}"


def preflight_conversion(
    ts_names: list[str],
    job_dir: Path,
    instance_id: str,
    input_star: Path,
    project_path: Path,
    tomo_dims: str,
    stack_angpix: float,
) -> None:
    """Run the whole XML -> star path on the staged, still-unrefined XMLs before any GPU time:
    their geometry must reproduce the star the same alignment produced, and ingest + emit must
    go through (registry identity, per-TS stars). The emitted copy and the in-memory registry
    attachments are discarded; the real emit after training reloads the registry from disk."""
    adapter = MissAlignIngestAdapter(
        registry=get_registry_for(project_path), job_dir=job_dir, job_instance_id=instance_id
    )
    worst, compared = adapter.star_reproduction_error(input_star, project_path)
    if compared == 0:
        raise RuntimeError(f"No tilt of {input_star} matched a staged XML by movie name — cannot convert back.")
    summary = ", ".join(f"{col} {worst[col]:.3f}" for col in worst)
    print(f"[DRIVER] XML -> star check over {compared} tilts, max |diff|: {summary}", flush=True)
    over = {col: worst[col] for col, limit in MAPPING_LIMITS.items() if worst[col] > limit}
    if over:
        raise RuntimeError(f"XML -> star conversion does not reproduce {input_star}: {over} exceed {MAPPING_LIMITS}")

    scratch = job_dir / "preflight"
    try:
        ingested = adapter.ingest(ts_names, stack_angpix=stack_angpix)
        adapter.emit_star(
            input_star_path=input_star,
            output_star_path=scratch / "aligned_tilt_series.star",
            project_root=project_path,
            tomo_dimensions=tomo_dims,
        )
    finally:
        clear_registry(project_path)
        shutil.rmtree(scratch, ignore_errors=True)
    print(f"[DRIVER] Pre-flight emit OK for {len(ingested)}/{len(ts_names)} tilt-series", flush=True)


def write_changes_report(job_dir: Path, staged: Path, ts_names: list[str], stack_apix: float) -> None:
    """missalign_changes.json + one log line per series: the per-tilt shift change against iter0/ (the
    input as the tool saw it) with the rigid volume shift removed, the size of that rigid part, and any
    local image-warp grid. Warp reconstructions apply the grids; the RELION star cannot hold them."""
    threshold = 2 * stack_apix
    report: dict = {
        "threshold": f"tilts changed by more than 2 px of the {stack_apix:.4g} Å/px input stacks ({threshold:.4g} Å)",
        "tilt_series": {},
    }
    for ts in ts_names:
        before, after = staged / "iter0" / f"{ts}.xml", staged / f"{ts}.xml"
        try:
            row = compare_alignments(before, after, threshold_angstrom=threshold)
        except (OSError, ET.ParseError, ValueError) as e:
            # A series the tool set aside has no refined XML; the report says so rather than failing the run.
            report["tilt_series"][ts] = {"error": f"{type(e).__name__}: {e}"}
            print(f"[DRIVER] changes {ts}: not compared ({type(e).__name__}: {e})", flush=True)
            continue
        report["tilt_series"][ts] = row
        warp = row["local_warp"]
        changed = f"{row['residual_rms_angstrom']:.1f} Å rms / {row['residual_max_angstrom']:.1f} Å max"
        gauge = f"{row['gauge_rms_angstrom']:.1f} Å rms"
        over = f"{len(row['tilts_over_threshold'])} tilt(s) > {threshold:.4g} Å"
        grid = (
            f"; local warp {warp['dims']}, {warp['rms_angstrom']:.1f} Å rms / {warp['max_angstrom']:.1f} Å max"
            if warp
            else ""
        )
        print(f"[DRIVER] changes {ts}: {changed} with the rigid shift ({gauge}) removed; {over}{grid}", flush=True)
    # Written whole then renamed: the job tab of downstream extraction jobs reads it while this job runs.
    tmp = job_dir / "missalign_changes.json.tmp"
    tmp.write_text(json.dumps(report, indent=2))
    os.replace(tmp, job_dir / "missalign_changes.json")


def main():
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START (miss_align) ---", flush=True)

    try:
        (_project_state, params, local_params_data, job_dir, project_path, _job_type) = get_driver_context(
            MissAlignParams
        )
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Node: {os.uname().nodename}", flush=True)
    print(f"CWD: {job_dir}", flush=True)

    try:
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]
        instance_id = local_params_data["instance_id"]

        input_star = paths["input_star"]
        upstream_processing = paths["input_processing"]  # aligntiltsWarp warp_tiltseries/
        settings_src = paths["warp_tiltseries_settings"]
        output_star = paths["output_star"]

        require_producer_input(upstream_processing, "aligntiltsWarp warp_tiltseries dir")
        require_producer_input(settings_src, "warp_tiltseries.settings")
        require_producer_input(input_star, "aligned_tilt_series.star")

        # Settings first: a bad schedule, milestone list, box or GPU plan fails here, not hours in.
        entries = params.schedule()
        milestones = params.milestones()
        z_box = parse_z_box(params.z_box)
        if z_box != "full" and any(e.is_volume_warp for e in entries):
            raise ValueError(
                f"z_box={params.z_box!r} with a [X,Y,Z,T] volume-warp entry: that grid is laid over the volume "
                f"box, so fitting the box would move it. Use z_box=full with volume-warp schedules."
            )
        warning = params.epoch_cap_warning()
        if warning:
            print(f"[DRIVER] WARNING: {warning}", flush=True)
        n_gpus = allocated_gpu_count(default=params.num_gpus)
        training_devices, recon_devices = device_plan(params, n_gpus)
        n_dataloaders = dataloaders_per_trainer(params, len(training_devices), len(recon_devices))

        # Before any GPU time: the emit at the end needs the registry, and the star is the scope.
        registry = get_registry_for(project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {project_path}. Reload the project in the UI to "
                f"backfill the registry from mdocs, then restart this job."
            )
        ts_names = read_tilt_series_names_from_input_star(input_star)
        if not ts_names:
            raise ValueError(f"No tilt-series in input star {input_star}")
        n_iters = len(entries)
        print(
            f"[DRIVER] {len(ts_names)} tilt-series, {n_iters} macro-iteration(s) ({params.iteration_preset.value}), "
            f"seed {params.seed}, LR milestones {milestones}, z_box {params.z_box}",
            flush=True,
        )

        dims = read_settings_dims(settings_src)
        print(f"[DRIVER] Settings dims: {dims}", flush=True)
        stacks = read_stacks(upstream_processing, ts_names)
        input_apix = common_stack_apix(stacks)
        stack_apix = params.prepare_stacks_apix if params.prepare_stacks_apix > 0 else input_apix
        if params.prepare_stacks_apix > 0:
            print(
                f"[DRIVER] Stacks are rebuilt from the frame averages at {stack_apix:.4g} Å/px "
                f"(alignment job's: {input_apix:.4g} Å/px)",
                flush=True,
            )
        log_schedule(entries, stack_apix, params.patch_size)
        schedule = [e.config_entry(stack_apix) for e in entries]
        full_volume = [dims["VX"] * dims["apix"], dims["VY"] * dims["apix"], dims["VZ"] * dims["apix"]]

        # 1. Resume from the last checkpointed macro-iteration, or stage afresh.
        staged_processing = job_dir / "warp_tiltseries"
        manifest_path = job_dir / "run_manifest.json"
        manifest = run_manifest(input_star, ts_names, schedule, params)
        resume_from = last_checkpointed_iteration(staged_processing)
        if resume_from > 0:
            check_resume(manifest_path, manifest, resume_from)
            print(
                f"[DRIVER] RESUME: iter{resume_from}/model.ckpt exists in {staged_processing}; "
                f"continuing at macro-iteration {resume_from + 1} of {n_iters} (no re-stage).",
                flush=True,
            )
        else:
            print(f"[DRIVER] Staging {len(ts_names)} tilt-series from {upstream_processing}", flush=True)
            stage_inputs(
                ts_names, upstream_processing, settings_src, job_dir, link_stacks=params.prepare_stacks_apix <= 0
            )
            if params.prepare_stacks_apix > 0:
                check_frame_averages(staged_processing, ts_names, stacks)

            # 2. Stamp physical dims onto every staged XML (in-container), plus the coarse
            #    reconstructions the Z box is fitted on. Only on a fresh stage — on resume the XMLs are
            #    already stamped and partially refined.
            zbox_dir = job_dir / "zbox"
            stamp_spec = {
                "apix": dims["apix"],
                "volume": full_volume,
                "series": {ts: {"image": [s.extent_x, s.extent_y], "stack": str(s.path)} for ts, s in stacks.items()},
                "coarse": None,
            }
            if z_box == "auto":
                zbox_dir.mkdir(exist_ok=True)
                factor = max(1, round(ZBOX_RECONSTRUCTION_ANGSTROM / input_apix))
                stamp_spec["coarse"] = {"downsample": factor, "dir": zbox_dir.name}
            (job_dir / "stamp_dims.json").write_text(json.dumps(stamp_spec, indent=2))
            (job_dir / "stamp_dims.py").write_text(STAMP_SCRIPT)
            run_tool(
                "python stamp_dims.py stamp_dims.json",
                tool_name=params.get_tool_name(),
                cwd=job_dir,
                binds=additional_binds,
            )

            if z_box != "full":
                full_z = full_volume[2]
                if z_box == "auto":
                    boxes = fit_z_boxes(zbox_dir, ts_names, full_z)
                else:
                    boxes = dict.fromkeys(ts_names, min(float(z_box), full_z))
                    print(
                        f"[DRIVER] z_box: {boxes[ts_names[0]]:.0f} Å for every series (full {full_z:.0f} Å)", flush=True
                    )
                for ts, box_z in boxes.items():
                    set_volume_dimensions(staged_processing / f"{ts}.xml", [full_volume[0], full_volume[1], box_z])

            preflight_conversion(
                ts_names,
                job_dir,
                instance_id,
                input_star,
                project_path,
                input_tomo_dimensions(input_star, dims),
                input_apix,
            )
            manifest_path.write_text(json.dumps(manifest, indent=2))

        # 3. Write config.yaml.
        config = build_config(params, staged_processing, entries, stack_apix, milestones)
        with open(job_dir / "config.yaml", "w") as f:
            yaml.safe_dump(config, f, sort_keys=False, default_flow_style=False)

        # 4. Run `miss-alignment train` for the iterations not yet checkpointed. --cleanenv wipes
        #    USER/LOGNAME, and when the container's passwd cannot resolve our uid (an LDAP/SSSD
        #    cluster with a bind-mounted static /etc/passwd), getpass.getuser() throws "getpwuid():
        #    uid not found"; setting USER/LOGNAME short-circuits that lookup. The container wrapper
        #    disables tqdm for every tool, but the tool's progress bars are its only output for
        #    hours of training and the only thing keeping run_command's idle watchdog from killing
        #    it, so they are switched back on here. Several training GPUs rendezvous over NCCL, whose
        #    peer-to-peer path can stall across PCIe switches on datacenter nodes; the tool's docs
        #    prescribe NCCL_P2P_DISABLE=1 (shared memory instead, negligible cost).
        if resume_from >= n_iters:
            print(f"[DRIVER] All {n_iters} macro-iterations already checkpointed; skipping training.", flush=True)
        else:
            username = os.environ.get("USER") or getpass.getuser()
            print(
                f"[DRIVER] GPUs={n_gpus}: training-devices={training_devices} reconstruction-devices={recon_devices}",
                flush=True,
            )
            env = f"env -u TQDM_DISABLE USER={username} LOGNAME={username} OMP_NUM_THREADS=1 MKL_NUM_THREADS=1"
            if len(training_devices) > 1:
                env += " NCCL_P2P_DISABLE=1"
            train_cmd = (
                ToolCommand(env)
                .raw("miss-alignment train")
                .opt("--config-file", "config.yaml")
                .opt("--training-devices", ",".join(str(d) for d in training_devices))
                .opt("--reconstruction-devices", ",".join(str(d) for d in recon_devices))
                .opt("--pool-size", params.pool_size)
                .opt("--dataloaders-per-trainer", n_dataloaders)
                .opt("--start-at-iteration", resume_from)
            )
            # The stacks are rebuilt once, on a fresh stage; a resumed run reads the ones already built.
            if params.prepare_stacks_apix > 0 and resume_from == 0:
                train_cmd.opt("--prepare-stacks", params.prepare_stacks_apix)
            print(f"[DRIVER] Train command: {train_cmd}", flush=True)
            run_tool(train_cmd, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        # 5. The run is complete only when the last macro-iteration checkpointed; iter0/ and the
        #    staged XMLs exist before any training, so neither proves work.
        final_ckpt = staged_processing / f"iter{n_iters}" / "model.ckpt"
        if not final_ckpt.is_file():
            raise RuntimeError(
                f"miss-alignment exited without {final_ckpt.relative_to(job_dir)} — macro-iteration "
                f"{n_iters} did not complete (last checkpoint: iter{last_checkpointed_iteration(staged_processing)})."
            )

        # 6. Downstream Warp jobs set the box from their settings file; the output XMLs carry the full
        #    box anyway so they read the same as the alignment job's. iterN/ keep the fitted boxes.
        if z_box != "full":
            for ts in ts_names:
                xml = staged_processing / f"{ts}.xml"
                if xml.is_file():
                    set_volume_dimensions(xml, full_volume)

        # 7. Report the change, emit the drop-in star + tilt_series/ from the refined XMLs, and record
        #    the alignment.
        write_changes_report(job_dir, staged_processing, ts_names, input_apix)
        registry = get_registry_for(project_path)
        adapter = MissAlignIngestAdapter(registry=registry, job_dir=job_dir, job_instance_id=instance_id)
        ingested = adapter.ingest(ts_names, stack_angpix=stack_apix)
        adapter.emit_star(
            input_star_path=input_star,
            output_star_path=output_star,
            project_root=project_path,
            tomo_dimensions=input_tomo_dimensions(input_star, dims),
        )
        registry.save()
        print(
            f"[DRIVER] Refined {len(ingested)}/{len(ts_names)} tilt-series over {n_iters} macro-iteration(s); "
            f"wrote {output_star}",
            flush=True,
        )

        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
