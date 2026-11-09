#!/usr/bin/env python
# drivers/ts_import.py
"""
ts_import driver — lightweight metadata assembly job.

Runs WarpTools ts_import + create_settings to convert mdocs and processed
frame-series into tomostar files and a warp_tiltseries.settings file.
No GPU work — purely metadata assembly. Runs as a single SLURM job (not an array).
"""

import os
import shutil
import sys
import threading
import traceback
from pathlib import Path

from drivers.array_job_base import read_tilt_series_names_from_input_star, split_cpus
from drivers.driver_base import ToolCommand, add_gain_options, get_driver_context, run_tool, start_supervisor
from services.jobs.ts_import import TsImportParams


def stage_upstream_mdocs(mdoc_dir: Path, input_star: Path, pattern: str, job_dir: Path) -> Path:
    """Copy into job_dir/mdoc the mdocs of the tilt series listed in the input STAR.

    ts_import exits 1 when any mdoc names a frame series it cannot find, so a single tilt series
    that failed motion correction would fail the whole import. The input STAR lists the ones that
    made it; only their mdocs go to ts_import.
    """
    ts_names = set(read_tilt_series_names_from_input_star(input_star))
    if not ts_names:
        raise RuntimeError(f"No tilt series listed in {input_star}")

    mdocs = sorted(mdoc_dir.glob(pattern))
    kept = [m for m in mdocs if m.name.removesuffix(".mdoc") in ts_names]
    left_out = [m.name for m in mdocs if m not in kept]
    no_mdoc = sorted(ts_names - {m.name.removesuffix(".mdoc") for m in kept})
    if not kept:
        raise FileNotFoundError(f"No mdoc in {mdoc_dir} matching '{pattern}' belongs to a tilt series in {input_star}")
    if left_out:
        print(f"[DRIVER] Leaving out {len(left_out)} mdoc(s) not in the input STAR: {', '.join(left_out)}", flush=True)
    if no_mdoc:
        print(f"[DRIVER] WARNING: no mdoc matching '{pattern}' for: {', '.join(no_mdoc)}", flush=True)

    staged = job_dir / "mdoc"
    if staged.exists():
        shutil.rmtree(staged)
    staged.mkdir()
    for m in kept:
        shutil.copy2(m, staged / m.name)
    print(f"[DRIVER] Staged {len(kept)} mdoc(s) in {staged}", flush=True)
    return staged


def ts_import_command(params: TsImportParams, mdoc_dir: Path, frameseries_rel: str, output: str) -> ToolCommand:
    """WarpTools ts_import over the mdocs in mdoc_dir, writing one .tomostar per tilt series into `output`.

    A tomostar names its frames relative to its own folder, so `output` must sit at the depth of job_dir/tomostar
    for the tomostars to stay right when they are moved there.
    """
    ts_import = (
        ToolCommand("WarpTools ts_import")
        .opt_path("--mdocs", mdoc_dir, quote=True)
        .opt_path("--pattern", params.mdoc_pattern, quote=True)
        .opt_path("--frameseries", frameseries_rel, quote=True)
        .opt("--output", output)
        .opt("--tilt_exposure", params.dose_per_tilt)
        .opt("--override_axis", params.tilt_axis_angle)
        .opt("--min_intensity", params.min_intensity)
    )
    if not params.invert_tilt_angles:
        ts_import.flag("--dont_invert")
    if params.do_at_most > 0:
        ts_import.opt("--do_at_most", params.do_at_most)
    return ts_import


def create_settings_command(params: TsImportParams) -> str:
    """WarpTools create_settings for the tomostars in job_dir/tomostar, checked by its output file."""
    create_settings = (
        ToolCommand("WarpTools create_settings")
        .opt("--folder_data", "tomostar")
        .raw("--extension '*.tomostar'")
        .opt("--folder_processing", "warp_tiltseries")
        .opt("--output", "warp_tiltseries.settings")
        .opt("--angpix", params.pixel_size)
        .opt("--exposure", params.dose_per_tilt)
        .opt("--tomo_dimensions", params.tomo_dimensions)
    )
    add_gain_options(create_settings, params.gain_path, getattr(params, "gain_operations", None))

    # See build_warp_commands in fs_motion_and_ctf: create_settings exits 0 on a
    # rejected option, so the settings file has to be checked, not the exit code.
    settings_gate = (
        "test -f warp_tiltseries.settings || "
        "{ echo 'ERROR: WarpTools create_settings produced no warp_tiltseries.settings "
        '(it exits 0 on unknown options — check its output above for "Option ... is unknown")\' >&2; '
        "exit 1; }"
    )
    return f"{{ test -f warp_tiltseries.settings || ({create_settings.render()}); {settings_gate}; }}"


def import_tilt_series(params: TsImportParams, paths: dict, job_dir: Path, binds) -> None:
    """ts_import in `params.import_calls` calls at once (never more than the job has CPUs), each over a share of
    the staged mdocs and on a share of the CPUs, each into its own folder beside tomostar/; then their tomostars
    land in tomostar/ in one rename, so tomostar/ exists only whole. Each call's output is printed after the calls
    end, in order."""
    mdocs = sorted(paths["mdoc_dir"].glob(params.mdoc_pattern))
    calls = max(1, min(params.import_calls, len(mdocs), len(os.sched_getaffinity(0))))
    # The frame series relative to job_dir, which is every call's working directory.
    frameseries_rel = os.path.relpath(str(paths["input_processing"]), str(job_dir))
    parts = [(job_dir / f"mdoc_part{c}", f"tomostar_part{c}") for c in range(calls)]
    for c, (mdoc_part, out_part) in enumerate(parts):
        for d in (mdoc_part, job_dir / out_part):
            if d.exists():
                shutil.rmtree(d)
        mdoc_part.mkdir()
        for m in mdocs[c::calls]:
            shutil.copy2(m, mdoc_part / m.name)
        (job_dir / f"{out_part}.log").unlink(missing_ok=True)

    cpu_groups = split_cpus(calls) if calls > 1 else [None]
    failed: dict[int, BaseException] = {}

    def call(c: int) -> None:
        mdoc_part, out_part = parts[c]
        command = ts_import_command(params, mdoc_part, frameseries_rel, out_part).render()
        try:
            run_tool(
                f"{command} > {out_part}.log 2>&1",
                tool_name=params.get_tool_name(),
                cwd=job_dir,
                binds=binds,
                cpus=cpu_groups[c],
            )
        except Exception as e:  # reported with the call's output below, and fails the job there
            traceback.print_exc(file=sys.stderr)
            failed[c] = e

    print(f"[DRIVER] ts_import over {len(mdocs)} mdoc(s) in {calls} call(s) at once", flush=True)
    threads = [threading.Thread(target=call, args=(c,), name=f"ts_import{c}") for c in range(calls)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for c, (mdoc_part, out_part) in enumerate(parts):
        log = job_dir / f"{out_part}.log"
        n = len(list(mdoc_part.iterdir()))
        print(
            f"[DRIVER] --- ts_import call {c + 1} of {calls} ({n} mdoc(s)){': FAILED' if c in failed else ''} ---",
            flush=True,
        )
        print(log.read_text() if log.exists() else "(no output)", flush=True)
    if failed:
        raise RuntimeError(
            f"ts_import failed in {len(failed)} of {calls} call(s): "
            + "; ".join(f"call {c + 1}: {failed[c]}" for c in sorted(failed))
        )

    assembling = job_dir / "tomostar.assembling"
    if assembling.exists():
        shutil.rmtree(assembling)
    assembling.mkdir()
    for _, out_part in parts:
        for f in (job_dir / out_part).glob("*.tomostar"):
            if (assembling / f.name).exists():
                raise RuntimeError(f"Two ts_import calls wrote {f.name}")
            f.rename(assembling / f.name)
    tomostar_dir = job_dir / "tomostar"
    if tomostar_dir.exists():  # left by an earlier run with no tomostar in it, or the import would be skipped
        shutil.rmtree(tomostar_dir)
    assembling.rename(tomostar_dir)
    for mdoc_part, out_part in parts:
        shutil.rmtree(mdoc_part)
        shutil.rmtree(job_dir / out_part)
        (job_dir / f"{out_part}.log").unlink(missing_ok=True)


def main():
    start_supervisor()
    print("[DRIVER] ts_import driver started.", flush=True)

    try:
        (_project_state, params, local_params_data, job_dir, _project_path, job_type) = get_driver_context(
            TsImportParams
        )
    except Exception as e:
        print(f"[DRIVER] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    try:
        print(f"[DRIVER] Params loaded for job type {job_type} in {job_dir}", flush=True)
        paths = {k: Path(v) for k, v in local_params_data["paths"].items()}
        additional_binds = local_params_data["additional_binds"]
        paths["mdoc_dir"] = stage_upstream_mdocs(paths["mdoc_dir"], paths["input_star"], params.mdoc_pattern, job_dir)

        tomostar_dir = job_dir / "tomostar"
        done = sorted(tomostar_dir.glob("*.tomostar")) if tomostar_dir.is_dir() else []
        if done:
            print(f"[DRIVER] {len(done)} tomostar file(s) already in {tomostar_dir}: ts_import skipped", flush=True)
        else:
            import_tilt_series(params, paths, job_dir, additional_binds)

        command_str = create_settings_command(params)
        print(f"[DRIVER] Command: {command_str}", flush=True)
        run_tool(command_str, tool_name=params.get_tool_name(), cwd=job_dir, binds=additional_binds)

        # Verify outputs exist
        settings_file = job_dir / "warp_tiltseries.settings"
        tomostar_files = list(tomostar_dir.glob("*.tomostar")) if tomostar_dir.exists() else []

        if not tomostar_files:
            raise FileNotFoundError(f"ts_import produced no tomostar files in {tomostar_dir}")
        if not settings_file.exists():
            raise FileNotFoundError(f"create_settings did not produce {settings_file}")

        print(f"[DRIVER] Created {len(tomostar_files)} tomostar files", flush=True)

        print("[DRIVER] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print("[DRIVER] FATAL ERROR: Job failed.", file=sys.stderr, flush=True)
        print(str(e), file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        print("[DRIVER] Job failed.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
