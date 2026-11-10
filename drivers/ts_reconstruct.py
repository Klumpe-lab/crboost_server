#!/usr/bin/env python3
"""
ts_reconstruct driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Submitted by the afterok chain via the standard qsub.sh.
          Reads the input STAR, persists a task manifest, builds run_array.sh from
          config/qsub.sh with `#SBATCH --array=...` injected and per-task SLURM
          resources from job_model.get_effective_slurm_config(), sbatches it, polls
          squeue until the array is empty, then runs metadata aggregation. Exit code
          0 only if every tilt-series has a `.ok` status file.

- Set:    TASK mode. One tilt-series per array index. Reads the manifest, picks its
          TS, idempotently skips if the reconstruction MRC already exists, otherwise
          stages a per-TS input_processing dir (symlinking only this TS's XML),
          runs `WarpTools ts_reconstruct`, and atomically writes
          `.task_status/{ts_name}.{ok|fail}`.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
code all live in ArrayDriver; this file is the ts_reconstruct-specific hooks.
"""

import math
import sys
import traceback
from pathlib import Path

from drivers.array_job_base import (
    STATUS_DIR_NAME,
    ArrayDriver,
    ArrayResults,
    chunk_item_header,
    read_tilt_series_names_from_input_star,
    stage_per_ts_environment,
    stage_tilt_series,
    write_status_atomic,
)
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input, run_tool
from services.computing.slurm_service import SlurmConfig, scale_per_task
from services.job_models import TsReconstructParams
from services.tilt_series import get_registry_for
from services.tilt_series.adapters import TsReconstructIngestAdapter


def reconstruction_mrc_path(job_dir: Path, ts_name: str, rescale_angpixs: float) -> Path:
    """Mirror the convention in metadata_service.update_ts_reconstruct_metadata()."""
    rec_res = f"{rescale_angpixs:.2f}"
    return job_dir / "warp_tiltseries" / "reconstruction" / f"{ts_name}_{rec_res}Apx.mrc"


def build_reconstruct_command(
    params: TsReconstructParams,
    settings_file: Path,
    input_processing: Path,
    output_processing: Path,
    task_dir: Path | None = None,
) -> ToolCommand:
    """`task_dir`: where WarpTools keeps its work queue. It defaults to a `tasks/` dir inside
    output_processing, which one running WarpTools locks; array tasks that share the output dir need
    one each, or every concurrent task but one fails ("Another manager is already using queue dir")."""
    cmd = (
        ToolCommand("WarpTools ts_reconstruct")
        .opt_path("--settings", settings_file, quote=True)
        .opt_path("--input_processing", input_processing, quote=True)
        .opt_path("--output_processing", output_processing, quote=True)
        .opt("--angpix", params.rescale_angpixs)
    )
    if task_dir is not None:
        cmd.opt_path("--task_dir", task_dir, quote=True)
    # --halfmap_frames and --deconv are Warp switches: their presence enables them,
    # whatever value follows (`--halfmap_frames 0` still fails with "Can't find
    # half-averages"). Emit each only when the parameter is 1.
    if params.halfmap_frames == 1:
        cmd.flag("--halfmap_frames")
    if params.halfmap_tilts == 1:
        cmd.flag("--halfmap_tilts")
    if params.deconv == 1:
        cmd.flag("--deconv")
    return cmd.opt("--perdevice", params.perdevice).flag("--dont_invert")


def first_movie_path(tomostar: Path) -> Path:
    """The first `_wrpMovieName` of a tomostar (absolute after per-TS staging)."""
    for line in tomostar.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("data_", "loop_", "_", "#")):
            continue
        return Path(stripped.split()[0])
    raise ValueError(f"tomostar has no data rows: {tomostar}")


def require_even_odd_halves(staged_tomostar: Path) -> None:
    """Fail BEFORE Warp when the frame-series job wrote no even/odd half-averages.

    Warp keys the halves as `<warp_frameseries>/average/{even,odd}/<movie stem>.mrc`
    (the fs_motion_ctf adapter's `even_mrc`). Single-frame input (SerialEM stacks
    split into one .mrc per tilt) leaves those folders empty, and Warp's own error
    only surfaces after the whole per-TS staging ran.
    """
    movie = first_movie_path(staged_tomostar)
    even = movie.parent / "average" / "even" / f"{movie.stem}.mrc"
    if not even.exists():
        raise FileNotFoundError(
            f"no even/odd halves for this tilt-series ({even} missing) — "
            f"set halfmap_frames to 0 (CryoCARE unavailable: the input has no frame halves)"
        )


class TsReconstructDriver(ArrayDriver):
    params_class = TsReconstructParams
    job_name = "ts_reconstruct"
    driver_script = Path(__file__).resolve()
    retry_attempts = 3

    # ---------------- supervisor ----------------

    def items_per_task(self, ctx: DriverContext[TsReconstructParams]) -> int:
        return ctx.params.tilt_series_per_task

    def per_task_slurm_config(self, ctx: DriverContext[TsReconstructParams]) -> SlurmConfig:
        """The SLURM tab's resources are one tilt series': a task runs `perdevice` at once, in as many rounds as
        its tilt series take."""
        p = ctx.params
        cfg = ctx.params.get_effective_slurm_config()
        if p.tilt_series_per_task == 1:
            return cfg
        workers = min(p.perdevice, p.tilt_series_per_task)
        cfg = scale_per_task(cfg, resources=workers, time=math.ceil(p.tilt_series_per_task / workers))
        self.log(
            f"{p.tilt_series_per_task} tilt series a task, {workers} Warp worker(s): {cfg.cpus_per_task} CPUs, "
            f"{cfg.mem}, {cfg.time} per task"
        )
        return cfg

    def enumerate_items(self, ctx: DriverContext[TsReconstructParams]) -> list[str]:
        if ctx.params.halfmap_frames == 1 and ctx.params.halfmap_tilts == 1:
            raise ValueError(
                "halfmap_frames and halfmap_tilts are both 1; Warp writes either kind of half-tomogram to the "
                "same even/odd folders and refuses both. Set one of them to 0."
            )
        input_star = ctx.paths["input_star"]
        require_producer_input(input_star, "Input STAR")
        ts_names = read_tilt_series_names_from_input_star(input_star)
        if not ts_names:
            raise ValueError(f"No tilt-series found in input STAR: {input_star}")
        return ts_names

    def aggregate(self, ctx: DriverContext[TsReconstructParams], results: ArrayResults) -> None:
        # Aggregate metadata via the TiltSeries registry. Fail loud on an
        # empty registry.
        registry = get_registry_for(ctx.project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {ctx.project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, "
                f"then restart this job."
            )
        adapter = TsReconstructIngestAdapter(
            registry=registry, job_dir=ctx.job_dir, job_instance_id=ctx.instance_id, warp_folder="warp_tiltseries"
        )
        adapter.ingest(results.ok, rescale_angpixs=ctx.params.rescale_angpixs, frame_pixel_size=ctx.params.pixel_size)
        adapter.emit_star(
            ctx.paths["input_star"], ctx.paths["output_star"], excluded_ids=set(results.skipped) | set(results.dropped)
        )
        registry.save()

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[TsReconstructParams], item: str) -> bool:
        # Covers the window where the artifact exists but no `.ok` was recorded
        # (orphaned or superseded run) — the supervisor-side skip only sees status files.
        out_mrc = reconstruction_mrc_path(ctx.job_dir, item, ctx.params.rescale_angpixs)
        if out_mrc.exists() and out_mrc.stat().st_size > 0:
            self.log(f"Reconstruction already exists, skipping: {out_mrc}")
            return True
        return False

    def stage(self, ctx: DriverContext[TsReconstructParams], item: str):
        staged_settings, staged_processing = stage_per_ts_environment(
            ctx.job_dir, item, ctx.paths["input_processing"], ctx.paths["warp_tiltseries_settings"]
        )
        self.log(f"Staged settings: {staged_settings}")
        if ctx.params.halfmap_frames == 1:
            require_even_odd_halves(staged_settings.parent / "tomostar" / f"{item}.tomostar")
        return staged_settings, staged_processing

    def build_command(self, ctx: DriverContext[TsReconstructParams], item: str, staged) -> ToolCommand:
        staged_settings, staged_processing = staged
        task_dir = staged_processing.parent / "tasks"
        task_dir.mkdir(exist_ok=True)
        return build_reconstruct_command(
            params=ctx.params,
            settings_file=staged_settings,
            input_processing=staged_processing,
            output_processing=ctx.paths["output_processing"],
            task_dir=task_dir,
        )

    def verify_outputs(self, ctx: DriverContext[TsReconstructParams], item: str, staged) -> None:
        out_mrc = reconstruction_mrc_path(ctx.job_dir, item, ctx.params.rescale_angpixs)
        if not out_mrc.exists():
            raise FileNotFoundError(f"WarpTools reported success but expected output MRC missing: {out_mrc}")

    def run_chunk(self, ctx: DriverContext[TsReconstructParams], entries: list[tuple[int, str]]) -> bool:
        """A chunk's tilt series in one Warp call with `perdevice` workers (roadmap 45 P10): the container and
        Warp start once for all of them. Each keeps its own marker; a failed call fails the tilt series it ran and
        removes what they left, so a rerun redoes them whole."""
        status_dir = ctx.job_dir / STATUS_DIR_NAME
        leader = entries[0][0]
        todo: list[tuple[int, str]] = []
        for idx, item in entries:
            if idx != leader:
                (ctx.job_dir / f"task_{idx}.out").write_text(chunk_item_header(item, leader))
            if self.task_already_done(ctx, item):
                write_status_atomic(status_dir, item, ok=True)
            else:
                todo.append((idx, item))
        self.log(f"Chunk of {len(entries)}: {len(todo)} to reconstruct in one call")
        if not todo:
            return True

        stage_root = ctx.job_dir / ".staging" / f"chunk_{todo[0][1]}"
        try:
            staged_settings, staged_processing = stage_tilt_series(
                stage_root,
                [item for _, item in todo],
                ctx.paths["input_processing"],
                ctx.paths["warp_tiltseries_settings"],
            )
        except Exception as e:
            print(f"[TASK {leader}] FATAL ERROR staging the chunk: {e}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            for idx, item in todo:
                self._write_fail(ctx, idx, item, e)
            return False

        runnable = []
        for idx, item in todo:
            try:
                if ctx.params.halfmap_frames == 1:
                    require_even_odd_halves(staged_settings.parent / "tomostar" / f"{item}.tomostar")
                runnable.append((idx, item))
            except Exception as e:
                print(f"[TASK {idx}] FATAL ERROR for ts={item}: {e}", file=sys.stderr, flush=True)
                self._write_fail(ctx, idx, item, e)
                (staged_settings.parent / "tomostar" / f"{item}.tomostar").unlink()
        if not runnable:
            return False

        try:
            cmd = self.build_command(ctx, runnable[0][1], (staged_settings, staged_processing))
            self.log(f"Command: {cmd}")
            run_tool(
                cmd,
                tool_name=ctx.params.get_tool_name(),
                cwd=ctx.job_dir,
                binds=ctx.additional_binds,
                attempts=self.retry_attempts,
                label=f"{self.job_name} chunk of {len(runnable)}",
            )
        except Exception as e:
            print(f"[TASK {leader}] FATAL ERROR for the chunk's Warp call: {e}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)
            for idx, item in runnable:
                out_mrc = reconstruction_mrc_path(ctx.job_dir, item, ctx.params.rescale_angpixs)
                for f in (out_mrc, out_mrc.parent / "even" / out_mrc.name, out_mrc.parent / "odd" / out_mrc.name):
                    f.unlink(missing_ok=True)
                self._write_fail(ctx, idx, item, e)
            return False

        ok = len(runnable) == len(todo)
        for idx, item in runnable:
            try:
                self.verify_outputs(ctx, item, None)
                write_status_atomic(status_dir, item, ok=True)
                print(f"[TASK {idx}] {item} done", flush=True)
            except Exception as e:
                print(f"[TASK {idx}] FATAL ERROR for ts={item}: {e}", file=sys.stderr, flush=True)
                self._write_fail(ctx, idx, item, e)
                ok = False
        return ok


if __name__ == "__main__":
    TsReconstructDriver().main()
