#!/usr/bin/env python3
# drivers/ts_ctf.py
"""
ts_ctf driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Copies alignment XMLs into the output dir, runs
          ts_defocus_hand globally (needs all TS for handedness decision),
          then dispatches per-TS ts_ctf tasks via SLURM array.

- Set:    TASK mode. Stages a per-TS environment, runs ts_ctf for one TS,
          copies result XML back to the shared output dir.
"""

import shutil
import sys
from pathlib import Path

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    ArrayDriver,
    ArrayResults,
    copy_tomostar_with_absolute_paths,
    get_previously_succeeded,
    read_tilt_series_names_from_input_star,
)
from drivers.driver_base import DriverContext, ToolCommand, run_tool, require_producer_input
from services.job_models import TsCtfParams
from services.tilt_series import get_registry_for
from services.tilt_series.adapters import TsCtfIngestAdapter


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def run_defocus_hand_globally(
    params: TsCtfParams, settings_file: Path, output_processing: Path, job_dir: Path, additional_binds: list
) -> None:
    """
    Run ts_defocus_hand on ALL tilt-series at once. This is a global step because
    the handedness decision needs statistics across multiple TS.
    """

    def defocus_hand(mode_flag: str) -> str:
        return (
            ToolCommand("WarpTools ts_defocus_hand")
            .opt_path("--settings", settings_file, quote=True)
            .opt_path("--output_processing", output_processing, quote=True)
            .flag(mode_flag)
            .render()
        )

    check_cmd = defocus_hand("--check")
    set_flip_cmd = defocus_hand("--set_flip")
    set_noflip_cmd = defocus_hand("--set_noflip")

    if params.defocus_hand == "auto":
        hand_cmd = (
            f"hand_output=$({check_cmd} 2>&1); "
            f'echo "$hand_output"; '
            f'if echo "$hand_output" | grep -q "should be set to \'flip\'"; then '
            f"  {set_flip_cmd}; "
            f"else "
            f"  {set_noflip_cmd}; "
            f"fi"
        )
    elif params.defocus_hand == "set_flip":
        hand_cmd = " && ".join([check_cmd, set_flip_cmd])
    else:
        hand_cmd = " && ".join([check_cmd, set_noflip_cmd])

    # Retried like every other tool invocation in these drivers: --check and
    # set_flip/set_noflip are idempotent, so a transient GPU-worker crash self-heals.
    run_tool(
        hand_cmd,
        tool_name=params.get_tool_name(),
        cwd=job_dir,
        binds=additional_binds,
        attempts=3,
        label="ts_defocus_hand",
    )


def stage_ctf_environment(
    job_dir: Path, ts_name: str, output_processing: Path, settings_file: Path, tomostar_dir: Path
) -> Path:
    """
    Build a per-TS staging directory for CTF estimation.

    .staging/task_{ts_name}/
    ├── warp_tiltseries.settings
    ├── tomostar/
    │   └── {ts_name}.tomostar
    └── warp_tiltseries/
        └── {ts_name}.xml       # copy of the defocus-hand-updated XML
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    # 1. Copy settings
    staged_settings = stage_root / settings_file.name
    shutil.copy2(str(settings_file), str(staged_settings))

    # 2. Stage tomostar
    staged_tomostar_dir = stage_root / "tomostar"
    staged_tomostar_dir.mkdir(parents=True, exist_ok=True)
    src_tomostar = tomostar_dir / f"{ts_name}.tomostar"
    # Fail loud, matching stage_per_ts_environment. WarpTools enumerates items from the
    # settings DataFolder (= tomostar), so a missing tomostar means the tool sees zero
    # items, leaves the staged XML untouched, and the task green-ticks having done
    # nothing. A TS with no work to do must arrive as an explicit `.skip`, never a no-op `.ok`.
    if not src_tomostar.exists():
        raise FileNotFoundError(f"Tomostar not found: {src_tomostar}")
    dst_tomostar = staged_tomostar_dir / f"{ts_name}.tomostar"
    copy_tomostar_with_absolute_paths(src_tomostar, dst_tomostar, tomostar_dir)

    # 3. Stage the XML as a real copy (defocus_hand already updated it).
    # A symlink would cause WarpTools to write through to the shared file,
    # making the final copy-back a SameFileError.
    staged_processing = stage_root / "warp_tiltseries"
    staged_processing.mkdir(parents=True, exist_ok=True)
    src_xml = output_processing / f"{ts_name}.xml"
    if not src_xml.exists():
        raise FileNotFoundError(f"Per-TS XML not found: {src_xml}")
    dst_xml = staged_processing / f"{ts_name}.xml"
    if dst_xml.exists() or dst_xml.is_symlink():
        dst_xml.unlink()
    shutil.copy2(str(src_xml), str(dst_xml))

    return stage_root


def build_ctf_command(params: TsCtfParams) -> ToolCommand:
    """Build the ts_ctf command to run inside a staged environment."""
    cmd = (
        ToolCommand("WarpTools ts_ctf")
        .opt("--settings", "warp_tiltseries.settings")
        .opt("--input_processing", "warp_tiltseries")
        .opt("--output_processing", "warp_tiltseries")
        .opt("--window", params.window)
        .opt("--range_low", params.range_min)
        .opt("--range_high", params.range_max)
        .opt("--defocus_min", params.defocus_min)
        .opt("--defocus_max", params.defocus_max)
        .opt("--voltage", round(params.voltage))
        .opt("--cs", params.spherical_aberration)
        .opt("--amplitude", params.amplitude_contrast)
        .opt("--perdevice", params.perdevice)
    )
    if params.do_phase:
        cmd.flag("--fit_phase")
    return cmd


class TsCtfDriver(ArrayDriver):
    params_class = TsCtfParams
    job_name = "ts_ctf"
    driver_script = Path(__file__).resolve()
    retry_attempts = 3

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[TsCtfParams]) -> list[str]:
        require_producer_input(ctx.paths["input_processing"], "Input processing dir")
        input_star = ctx.paths.get("input_star")
        if not input_star:
            raise FileNotFoundError("Input STAR path did not resolve from the upstream job")
        require_producer_input(input_star, "Input STAR")

        # Authoritative TS list = input STAR (alignment output). Globbing
        # *.xml in input_processing is unsafe: WarpTools writes a {ts}.xml
        # even for tilt-series alignment failed on (they're flagged
        # "unselected" inside the XML), so an XML-glob would silently
        # resurrect excluded TS and ts_ctf would waste compute on them.
        ts_names = read_tilt_series_names_from_input_star(input_star)
        if not ts_names:
            raise ValueError(f"No tilt-series found in input STAR: {input_star}")
        return ts_names

    def pre_dispatch(self, ctx: DriverContext[TsCtfParams], items: list[str]) -> None:
        """Stage the shared inputs, then run ts_defocus_hand across ALL tilt-series.

        Handedness is a global decision (it needs statistics over many TS), so unlike
        every other per-TS job this supervisor runs real compute before dispatch.
        """
        input_processing = ctx.paths["input_processing"]
        settings_file = ctx.paths["warp_tiltseries_settings"]
        output_processing = ctx.paths.get("output_processing", ctx.job_dir / "warp_tiltseries")
        in_scope = set(items)

        # Copy alignment XMLs into our output dir — but only for the in-scope TS.
        # Excluded XMLs would also poison the global defocus-hand step (it operates on
        # every XML in output_processing). TS already holding `.ok` are also excluded
        # from the copy: they are never re-dispatched (submit_array_job skips them), so
        # their XMLs in output_processing carry task-written CTF results that a re-copy
        # would silently clobber.
        output_processing.mkdir(parents=True, exist_ok=True)
        already_ok = get_previously_succeeded(ctx.job_dir)
        copied = 0
        skipped = 0
        preserved = 0
        for xml_file in input_processing.glob("*.xml"):
            if xml_file.stem not in in_scope:
                skipped += 1
            elif xml_file.stem in already_ok:
                preserved += 1
            else:
                shutil.copy2(str(xml_file), str(output_processing / xml_file.name))
                copied += 1
        msg = f"Copied {copied} alignment XMLs to {output_processing}"
        if preserved:
            msg += f" (preserved {preserved} CTF-updated XMLs of already-succeeded TS)"
        if skipped:
            msg += f" (skipped {skipped} excluded by alignment output STAR)"
        self.log(msg)

        # Copy settings into job dir for staging
        local_settings = ctx.job_dir / settings_file.name
        if not local_settings.exists():
            shutil.copy2(str(settings_file), str(local_settings))

        # Find tomostar dir (from the alignment job or tsImport)
        tomostar_dir = settings_file.parent / "tomostar"
        local_tomostar = ctx.job_dir / "tomostar"
        if not local_tomostar.exists() and tomostar_dir.exists():
            shutil.copytree(str(tomostar_dir), str(local_tomostar))

        self.log("Running ts_defocus_hand globally...")
        run_defocus_hand_globally(ctx.params, local_settings, output_processing, ctx.job_dir, ctx.additional_binds)
        self.log("Defocus hand detection complete.")

    def aggregate(self, ctx: DriverContext[TsCtfParams], results: ArrayResults) -> None:
        # If the registry is empty (legacy project), we can't proceed — the user must
        # reload the project so the backend backfills mdoc-derived identity. Fail loud
        # rather than fall back to the old string-keyed merge (the path that produced
        # the silent-corruption bug).
        registry = get_registry_for(ctx.project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {ctx.project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, "
                f"then restart this job."
            )
        adapter = TsCtfIngestAdapter(
            registry=registry, job_dir=ctx.job_dir, job_instance_id=ctx.instance_id, warp_folder="warp_tiltseries"
        )
        adapter.ingest(results.ok)
        adapter.emit_star(ctx.paths["input_star"], ctx.paths["output_star"], excluded_ids=set(results.skipped))
        registry.save()

    # ---------------- task ----------------

    def stage(self, ctx: DriverContext[TsCtfParams], item: str) -> Path:
        stage_root = stage_ctf_environment(
            ctx.job_dir,
            item,
            ctx.job_dir / "warp_tiltseries",
            ctx.job_dir / "warp_tiltseries.settings",
            ctx.job_dir / "tomostar",
        )
        self.log(f"Staged at: {stage_root}")
        return stage_root

    def build_command(self, ctx: DriverContext[TsCtfParams], item: str, staged: Path) -> ToolCommand:
        return build_ctf_command(ctx.params)

    def task_cwd(self, ctx: DriverContext[TsCtfParams], item: str, staged: Path) -> Path:
        # ts_ctf runs INSIDE the staging dir: its command uses relative paths
        # (--settings warp_tiltseries.settings) so only this TS is in scope.
        return staged

    def collect(self, ctx: DriverContext[TsCtfParams], item: str, staged: Path) -> None:
        # Copy the updated XML back to the shared output dir. Staging copied this file
        # IN (and raises if the source was absent), so its absence now means the tool
        # destroyed it — raise rather than skip the copy-back and still write `.ok`.
        staged_xml = staged / "warp_tiltseries" / f"{item}.xml"
        if not staged_xml.exists():
            raise FileNotFoundError(f"ts_ctf reported success but the staged XML is gone: {staged_xml}")
        shutil.copy2(str(staged_xml), str(ctx.job_dir / "warp_tiltseries" / f"{item}.xml"))


if __name__ == "__main__":
    TsCtfDriver().main()
