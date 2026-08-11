#!/usr/bin/env python3
"""miss-alignment driver — learned tilt-series alignment refinement.

Single (non-array) multi-GPU-node job. It consumes aligntiltsWarp's warp_tiltseries/
(the required initial coarse alignment), refines the per-TS Warp XMLs in place, and
exposes the refined warp_tiltseries/ for tsCtf/tsReconstruct. See docs/miss-alignment.md.

Flow:
  1. get_driver_context() + validate the three upstream inputs.
  2. Stage a COPY of warp_tiltseries/ + .settings + the aligned star into the job dir
     (miss-alignment refines XMLs IN PLACE — never mutate the upstream aligntiltsWarp output).
  3. Stamp physical dims onto every staged XML from warp_tiltseries.settings (crboost's thin
     XML has zero ImageDimensionsAngstrom; the tool's warpylib.TiltSeries() rejects it). This
     runs INSIDE the container (needs warpylib + torch).
  4. Write config.yaml from params + the selected iteration schedule.
  5. Run `miss-alignment train` (container-wrapped).
  6. Verify the refined warp_tiltseries/ then signal success.
"""

import sys
import os
import getpass
import shutil
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

try:
    from drivers.driver_base import get_driver_context, run_command, require_producer_input
    from services.computing.container_service import get_container_service
    from services.jobs.miss_align import MissAlignParams, MISS_ALIGN_SCHEDULES
except ImportError as e:
    print(f"FATAL: Could not import services. {e}", file=sys.stderr)
    sys.exit(1)


# Self-contained dim-stamping script, run in-container (warpylib + torch live only in the
# miss_alignment SIF). Dims are passed as argv so no string formatting is needed here. The
# physical extents are pixels x apix (Å), scale-invariant, so they stay correct under any
# later downsampling. See docs/miss-alignment.md §6.
STAMP_SCRIPT = """\
import glob, sys, torch
from warpylib import TiltSeries

apix = float(sys.argv[1]); W = int(sys.argv[2]); H = int(sys.argv[3])
VX = int(sys.argv[4]); VY = int(sys.argv[5]); VZ = int(sys.argv[6])
xmls = sorted(glob.glob("warp_tiltseries/*.xml"))
if not xmls:
    print("[stamp] no XMLs under warp_tiltseries/", file=sys.stderr)
    sys.exit(1)
for x in xmls:
    ts = TiltSeries(x)
    ts.image_dimensions_physical = torch.tensor([W * apix, H * apix], dtype=torch.float32)
    ts.volume_dimensions_physical = torch.tensor([VX * apix, VY * apix, VZ * apix], dtype=torch.float32)
    ts.ctf.pixel_size = apix
    ts.save_meta(x)
print(f"[stamp] stamped {len(xmls)} XML(s): apix={apix} img={W}x{H} vol={VX}x{VY}x{VZ}")
"""


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


def read_settings_dims(settings_path: Path) -> dict:
    """Parse warp_tiltseries.settings for the physical dims miss-alignment needs.

    apix MUST come from the settings PixelSize — NOT MicroscopeParams.pixel_size_angstrom,
    which defaults to 1.35 (the apix-default trap). The .settings file is the authoritative
    per-run source. Raises on any missing/blank/non-positive value (never invent a default).
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
        "W": int(float(_param("Import", "HeaderlessWidth"))),
        "H": int(float(_param("Import", "HeaderlessHeight"))),
        "VX": int(float(_param("Tomo", "DimensionsX"))),
        "VY": int(float(_param("Tomo", "DimensionsY"))),
        "VZ": int(float(_param("Tomo", "DimensionsZ"))),
    }
    for k in ("W", "H", "VX", "VY", "VZ"):
        if dims[k] <= 0:
            raise ValueError(f"Non-positive dimension {k}={dims[k]} in {settings_path} — cannot stamp dims.")
    return dims


def build_config(params: MissAlignParams, training_directory: Path) -> dict:
    """Assemble the miss-alignment config.yaml. Schedule comes from the shared
    MISS_ALIGN_SCHEDULES table so it matches the walltime estimate in the param class."""
    schedule = MISS_ALIGN_SCHEDULES.get(params.iteration_preset)
    if not schedule:
        raise ValueError(f"Unknown iteration_preset {params.iteration_preset!r}")

    # Single-trainer pool constraint (dataloaders-per-trainer=1): pool_size >= 2*batch_size.
    if params.pool_size < 2 * params.batch_size:
        raise ValueError(
            f"pool_size ({params.pool_size}) must be >= 2*batch_size ({2 * params.batch_size}); "
            f"raise pool_size or lower batch_size."
        )

    return {
        "general": {
            "training_directory": str(training_directory),
            "apply_ctf": False,  # CTF doubles cost with no alignment benefit (docs §5)
            "iteration_settings": schedule,
            "seed": 45132,
        },
        "model_training": {
            "model_architecture": "default",
            "model_checkpoint": None,
            "loss_margin": 0.5,
            "learning_rate": 1.0e-3,
            "weight_decay": 1.0e-4,
            "max_epochs_per_iteration": params.max_epochs_per_iteration,
            "warmup_steps": 500,
            "multistep_lr_scheduler": {"milestones": [5, 15], "gamma": 0.5},
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


def main():
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START (miss_align) ---", flush=True)

    try:
        (_project_state, params, local_params_data, job_dir, _project_path, _job_type) = get_driver_context(
            MissAlignParams
        )
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Node: {os.uname().nodename}", flush=True)
    print(f"CWD: {job_dir}", flush=True)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]

        input_star = paths["input_star"]
        upstream_processing = paths["input_processing"]  # aligntiltsWarp warp_tiltseries/
        settings_src = paths["warp_tiltseries_settings"]

        require_producer_input(upstream_processing, "aligntiltsWarp warp_tiltseries dir")
        require_producer_input(settings_src, "warp_tiltseries.settings")
        require_producer_input(input_star, "aligned_tilt_series.star")

        # P1 aligns the EXISTING aligned tiltstack/*.st (the proven path). prepare_stacks_apix > 0 would
        # rebuild the stacks from RAW frames, which needs tilt_movie_paths in each XML (crboost's thin XML
        # has none) plus the frames + tomostar bound in-container (additional_binds is empty until P2 wires
        # them). Refuse up front rather than stage + train for hours and only then fail (never-fail-silently).
        if params.prepare_stacks_apix > 0:
            raise RuntimeError(
                f"prepare_stacks_apix={params.prepare_stacks_apix} is not supported yet — rebuilding tilt "
                f"stacks from raw frames requires the frames + tomostar bound in-container (a P2 feature). "
                f"Set prepare_stacks_apix=0 to align the existing tiltstack/*.st (the proven path)."
            )

        # 1. Stage COPIES into the job dir. miss-alignment refines the XMLs IN PLACE and writes an
        #    iterN/ snapshot + model.ckpt per macro-iteration, so on a RE-RUN we resume from the last
        #    checkpoint rather than restart: if a prior attempt left iter*/ snapshots AND a model.ckpt
        #    here, keep them (no re-stage) and start at the next iteration. A fresh run — or an
        #    incomplete one with no checkpoint — stages a clean copy from the upstream aligntiltsWarp
        #    output. (PENDING RUNTIME: confirm the tool auto-loads warp_tiltseries/model.ckpt when
        #    --start-at-iteration > 0.)
        staged_processing = job_dir / "warp_tiltseries"
        prior_iters = (
            sorted(d for d in staged_processing.glob("iter*") if d.is_dir()) if staged_processing.exists() else []
        )
        resume_from = len(prior_iters) if (prior_iters and (staged_processing / "model.ckpt").exists()) else 0
        if resume_from > 0:
            print(
                f"[DRIVER] RESUME: {resume_from} macro-iteration(s) already done in {staged_processing}; "
                f"continuing at iteration {resume_from} from model.ckpt (no re-stage).",
                flush=True,
            )
        else:
            if staged_processing.exists():
                shutil.rmtree(staged_processing)
            print(f"[DRIVER] Staging {upstream_processing} -> {staged_processing}", flush=True)
            shutil.copytree(upstream_processing, staged_processing)
            shutil.copy(settings_src, job_dir / "warp_tiltseries.settings")
            shutil.copy(input_star, job_dir / "aligned_tilt_series.star")

        container = get_container_service()

        # 2. Stamp physical dims from the settings onto every staged XML (in-container). Only on a
        #    fresh stage — on resume the XMLs are already stamped + partially refined, and
        #    re-stamping would rewrite them needlessly.
        if resume_from == 0:
            dims = read_settings_dims(job_dir / "warp_tiltseries.settings")
            print(f"[DRIVER] Settings dims: {dims}", flush=True)
            stamp_path = job_dir / "stamp_dims.py"
            stamp_path.write_text(STAMP_SCRIPT)
            stamp_cmd = (
                f"python stamp_dims.py {dims['apix']} {dims['W']} {dims['H']} {dims['VX']} {dims['VY']} {dims['VZ']}"
            )
            wrapped_stamp = container.wrap_command_for_tool(
                command=stamp_cmd, cwd=job_dir, tool_name="miss_alignment", additional_binds=additional_binds
            )
            run_command(wrapped_stamp, cwd=job_dir)

        # 3. Write config.yaml.
        config = build_config(params, staged_processing)
        with open(job_dir / "config.yaml", "w") as f:
            yaml.safe_dump(config, f, sort_keys=False, default_flow_style=False)

        # 4. Run `miss-alignment train`. --cleanenv wipes host env and --no-home means $HOME
        #    is unset, so point HOME/MPLCONFIGDIR at a writable job-dir subdir (torch/matplotlib
        #    caches). P1 is single-GPU: index 0 within the gpu:1 allocation.
        jobtmp = job_dir / ".miss_align_home"
        (jobtmp / "mpl").mkdir(parents=True, exist_ok=True)
        # --cleanenv wipes USER/LOGNAME, and on this LDAP/SSSD cluster the bind-mounted static
        # /etc/passwd can't resolve our uid → getpass.getuser() (torch.compile's inductor cache-dir
        # setup) throws "getpwuid(): uid not found". Set USER/LOGNAME so that lookup short-circuits,
        # and point TORCHINDUCTOR_CACHE_DIR at the job dir so torch skips default_cache_dir() entirely
        # (and keeps its compile cache writable + isolated).
        username = os.environ.get("USER") or getpass.getuser()
        # dataloaders-per-trainer == PyTorch DataLoader workers. At 1 the GPU starves waiting on the
        # CPU-side pool sampler (Lightning warns "num_workers ... may be a bottleneck") — the dominant
        # slowdown in practice. dataloader_workers=0 auto-scales to the allocated CPUs (minus headroom
        # for the trainer main process + recon worker); a positive value pins it. Either way it is
        # clamped to the tool's pool constraint pool//n_partitions >= 2*batch_size (docs §4) so it never
        # raises at datamodule construction; oversubscribing the allocated CPUs is warned, not clamped.
        cpus = int(os.environ.get("SLURM_CPUS_PER_TASK") or 1)
        max_loaders_by_pool = params.pool_size // (2 * params.batch_size)
        if params.dataloader_workers > 0:
            n_dataloaders = min(params.dataloader_workers, max_loaders_by_pool)
            if params.dataloader_workers > max_loaders_by_pool:
                print(
                    f"[DRIVER] WARNING: dataloader_workers={params.dataloader_workers} exceeds the pool cap "
                    f"{max_loaders_by_pool} (pool_size//(2*batch_size)); clamped to {n_dataloaders}. "
                    f"Raise pool_size to use more workers.",
                    flush=True,
                )
            if n_dataloaders > cpus:
                print(
                    f"[DRIVER] WARNING: {n_dataloaders} dataloader workers > {cpus} allocated CPUs — "
                    f"oversubscription may SLOW loading. Raise cpus_per_task to match.",
                    flush=True,
                )
            mode = "pinned"
        else:
            n_dataloaders = max(1, min(cpus - 2, max_loaders_by_pool)) if cpus > 2 else 1
            mode = "auto"
        print(f"[DRIVER] dataloaders={n_dataloaders} ({mode}, cpus={cpus}, cap={max_loaders_by_pool})", flush=True)
        # GPU device split. Map the allocated GPUs (0..N-1): GPU 0 -> training, the rest -> the
        # reconstruction pool, so recon and training run on SEPARATE cards instead of contending for
        # one (docs §4). A single GPU keeps the shared 0/0 mode. Driven by the num_gpus job param
        # (which sets --gres); we read the real allocation so a manual gres override still maps right.
        n_gpus = allocated_gpu_count(default=params.num_gpus)
        training_devices = "0"
        recon_devices = "0" if n_gpus == 1 else ",".join(str(i) for i in range(1, n_gpus))
        print(f"[DRIVER] GPUs={n_gpus}: training-devices={training_devices} recon-devices={recon_devices}", flush=True)
        train_parts = [
            f"env HOME={jobtmp} MPLCONFIGDIR={jobtmp}/mpl USER={username} LOGNAME={username} "
            f"TORCHINDUCTOR_CACHE_DIR={jobtmp}/torchinductor OMP_NUM_THREADS=1 MKL_NUM_THREADS=1",
            "miss-alignment train",
            "--config-file config.yaml",
            f"--training-devices {training_devices}",
            f"--reconstruction-devices {recon_devices}",
            f"--pool-size {params.pool_size}",
            f"--dataloaders-per-trainer {n_dataloaders}",
            f"--start-at-iteration {resume_from}",
        ]
        # (--prepare-stacks is intentionally NOT appended here: prepare_stacks_apix > 0 is refused
        #  above until P2 wires the raw-frame + tomostar binds. Re-enable it there, not here.)
        train_cmd = " ".join(train_parts)
        print(f"[DRIVER] Train command: {train_cmd}", flush=True)
        wrapped_train = container.wrap_command_for_tool(
            command=train_cmd, cwd=job_dir, tool_name="miss_alignment", additional_binds=additional_binds
        )
        run_command(wrapped_train, cwd=job_dir)

        # 5. Verify the refined output. The XMLs were staged in before training, so their mere
        #    presence is not proof of work; miss-alignment writes a warp_tiltseries/iterN/ snapshot
        #    per macro-iteration (docs/miss-alignment.md §4, confirmed by the §9 smoke run). Require
        #    at least one iter*/ dir so a silent no-op (tool exits 0 without refining) fails loudly.
        refined_xmls = list(staged_processing.glob("*.xml"))
        if not refined_xmls:
            raise RuntimeError(f"miss-alignment produced no XMLs in {staged_processing}")
        iter_dirs = sorted(d for d in staged_processing.glob("iter*") if d.is_dir())
        if not iter_dirs:
            raise RuntimeError(
                f"miss-alignment wrote no iter*/ snapshot under {staged_processing} — "
                f"training did not complete a macro-iteration."
            )
        print(
            f"[DRIVER] Refined {len(refined_xmls)} tilt-series XML(s) across {len(iter_dirs)} macro-iteration(s).",
            flush=True,
        )

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(f"{type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        print("--- SLURM JOB END (Exit Code: 1) ---", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
