#!/usr/bin/env python3
"""
ts_reconstruct driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Submitted by relion_schemer via the standard qsub.sh.
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
markers all live in ArrayDriver; this file is the ts_reconstruct-specific hooks.
"""

import sys
from pathlib import Path

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    ArrayDriver,
    ArrayResults,
    read_tilt_series_names_from_input_star,
    stage_per_ts_environment,
)
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input
from services.job_models import TsReconstructParams
from services.tilt_series import get_registry_for
from services.tilt_series.adapters import TsReconstructIngestAdapter


def reconstruction_mrc_path(job_dir: Path, ts_name: str, rescale_angpixs: float) -> Path:
    """Mirror the convention in metadata_service.update_ts_reconstruct_metadata()."""
    rec_res = f"{rescale_angpixs:.2f}"
    return job_dir / "warp_tiltseries" / "reconstruction" / f"{ts_name}_{rec_res}Apx.mrc"


def build_reconstruct_command(
    params: TsReconstructParams, settings_file: Path, input_processing: Path, output_processing: Path
) -> ToolCommand:
    return (
        ToolCommand("WarpTools ts_reconstruct")
        .opt_path("--settings", settings_file, quote=True)
        .opt_path("--input_processing", input_processing, quote=True)
        .opt_path("--output_processing", output_processing, quote=True)
        .opt("--angpix", params.rescale_angpixs)
        .opt("--halfmap_frames", params.halfmap_frames)
        .opt("--deconv", params.deconv)
        .opt("--perdevice", params.perdevice)
        .flag("--dont_invert")
    )


class TsReconstructDriver(ArrayDriver):
    params_class = TsReconstructParams
    job_name = "ts_reconstruct"
    driver_script = Path(__file__).resolve()
    retry_attempts = 3

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[TsReconstructParams]) -> list[str]:
        input_star = ctx.paths["input_star"]
        require_producer_input(input_star, "Input STAR")
        ts_names = read_tilt_series_names_from_input_star(input_star)
        if not ts_names:
            raise ValueError(f"No tilt-series found in input STAR: {input_star}")
        return ts_names

    def aggregate(self, ctx: DriverContext[TsReconstructParams], results: ArrayResults) -> None:
        # Aggregate metadata via the TiltSeries registry. Fail loud on an
        # empty registry rather than fall back to the legacy path.
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
        adapter.emit_star(ctx.paths["input_star"], ctx.paths["output_star"], excluded_ids=set(results.skipped))
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
        return staged_settings, staged_processing

    def build_command(self, ctx: DriverContext[TsReconstructParams], item: str, staged) -> ToolCommand:
        staged_settings, staged_processing = staged
        return build_reconstruct_command(
            params=ctx.params,
            settings_file=staged_settings,
            input_processing=staged_processing,
            output_processing=ctx.paths["output_processing"],
        )

    def verify_outputs(self, ctx: DriverContext[TsReconstructParams], item: str, staged) -> None:
        out_mrc = reconstruction_mrc_path(ctx.job_dir, item, ctx.params.rescale_angpixs)
        if not out_mrc.exists():
            raise FileNotFoundError(f"WarpTools reported success but expected output MRC missing: {out_mrc}")


if __name__ == "__main__":
    TsReconstructDriver().main()
