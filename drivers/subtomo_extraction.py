#!/usr/bin/env python3
"""
Driver for RELION subtomogram extraction — supervisor + per-TS SLURM array task.

Mode is determined by `SLURM_ARRAY_TASK_ID`:

  - Unset: SUPERVISOR mode. Parses the upstream optimisation_set.star to
           get its particles.star + tomograms.star, slices both by
           `rlnTomoName` into per-TS staging dirs, submits a SLURM array,
           merges per-TS outputs back into job_dir/particles.star + the
           consolidated Subtomograms/<TS>/ tree, writes
           optimisation_set.star, and optionally runs the additional-source
           merge against any aggregation sources.

  - Set:   TASK mode. Reads the manifest, locates its TS's staged
           optimisation_set, runs `relion_tomo_subtomo` writing into the
           task's own staging dir, and atomically reports per-TS status.

`merge_only` is a special supervisor short-circuit that skips both
slicing and array submission — used by aggregation projects to fuse
additional optimisation sets into the job without an extraction pass.

Output layout after the supervisor merge:
  <job_dir>/
    particles.star           # merged across all TS
    tomograms.star           # copied verbatim from upstream
    optimisation_set.star    # points at the above (RELION key-value)
    Subtomograms/
      <ts_name>/             # one subdir per TS, moved from staging
        *_stack2d.mrcs       # numbering is per-TS independent (no collisions)
    .staging/                # per-TS scratch (intermediate; safe to delete
                             # after a successful run, but keep around for
                             # idempotent re-run of failed tasks)
    .task_manifest.json
    .task_status/{ts}.{ok,fail}
"""

import os
import shutil
import sys
import traceback
from pathlib import Path
from typing import List

import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.array_job_base import (
    STATUS_DIR_NAME,
    collect_task_results,
    install_cancel_handler,
    preflight_registry,
    read_manifest,
    submit_array_job,
    wait_for_array_completion,
    write_status_atomic,
)
from drivers.driver_base import get_driver_context, run_command
from drivers.subtomo_merge import (
    _parse_optimisation_set,
    _read_particles_star,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    merge_optimisation_sets_into_jobdir,
    write_optimisation_set,
)
from services.computing.container_service import get_container_service
from services.job_models import SubtomoExtractionParams

DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# Entry
# ----------------------------------------------------------------------


def main():
    print("--- SLURM JOB START (Subtomogram Extraction) ---", flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- subtomo_extraction: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- subtomo_extraction: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(SubtomoExtractionParams)
    except Exception as e:
        (Path.cwd() / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)

    try:
        # Aggregation short-circuit: skip extraction entirely, just merge
        # supplied optimisation sets into job_dir. Mirrors the original
        # one-shot driver's merge_only branch verbatim.
        if params.merge_only:
            print("[SUPERVISOR] merge_only=True, skipping extraction.", flush=True)
            if not params.additional_sources:
                raise RuntimeError("merge_only=True but no additional_sources to merge.")
            _run_additional_sources_merge(params, job_dir)
            (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
            print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
            return

        paths = {k: Path(v) for k, v in context["paths"].items()}
        instance_id = context["instance_id"]

        input_optimisation = paths["input_optimisation"]
        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")

        upstream_particles_star, upstream_tomograms_star = _parse_optimisation_set(input_optimisation)
        print(
            f"[SUPERVISOR] Upstream particles: {upstream_particles_star}\n"
            f"[SUPERVISOR] Upstream tomograms: {upstream_tomograms_star}",
            flush=True,
        )

        optics_df, particles_df, general_kv = _read_particles_star(upstream_particles_star)
        tomograms_df = _read_tomograms_star(upstream_tomograms_star)

        # Enumerate TS from the particles file — these are the TS that have
        # picks to extract. Tomograms.star can list more (e.g. tomos with
        # zero picks); we don't dispatch tasks for those.
        ts_names = sorted(particles_df["rlnTomoName"].astype(str).unique().tolist())
        if not ts_names:
            raise RuntimeError("Upstream particles.star contained no tomograms (rlnTomoName column empty)")
        print(f"[SUPERVISOR] {len(ts_names)} TS with picks to extract", flush=True)

        # Stage per-TS optimisation sets under .staging/task_<ts>/.
        staging_root = job_dir / ".staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        for ts_name in ts_names:
            _stage_per_ts(staging_root, ts_name, optics_df, particles_df, tomograms_df, general_kv)
        print(f"[SUPERVISOR] Staged per-TS inputs under {staging_root}", flush=True)

        preflight_registry(project_path, ts_names, job_name="subtomo_extraction")

        manifest_extra = {
            "input_optimisation_star": str(input_optimisation),
            "upstream_particles_star": str(upstream_particles_star),
            "upstream_tomograms_star": str(upstream_tomograms_star),
        }

        per_task_cfg = params.get_effective_slurm_config()

        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=ts_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
            manifest_extra=manifest_extra,
        )

        if array_job_id is not None:
            install_cancel_handler(array_job_id, job_dir)
            wait_for_array_completion(array_job_id, poll_secs=15)
        else:
            print("[SUPERVISOR] No array submitted (all TS previously succeeded)", flush=True)

        results = collect_task_results(job_dir, ts_names)
        print(f"[SUPERVISOR] Status: {results.summary}", flush=True)
        if results.failed:
            print(f"[SUPERVISOR] FAILED tilt-series: {results.failed}", flush=True)
        if results.missing:
            print(f"[SUPERVISOR] MISSING tilt-series: {results.missing}", flush=True)

        if not results.all_succeeded:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            print("[SUPERVISOR] Marking job as FAILED (some TS did not succeed)", flush=True)
            sys.exit(1)

        # Merge per-TS outputs into job_dir's canonical particles.star /
        # Subtomograms/ tree.
        _merge_per_ts_outputs(job_dir, ts_names, general_kv, upstream_tomograms_star)

        # Aggregation merge (additional_sources) — opt-in, runs only if the
        # job model has sources configured.
        if params.additional_sources:
            print(
                f"[SUPERVISOR] Merging {len(params.additional_sources)} additional source(s) on top of extraction...",
                flush=True,
            )
            _run_additional_sources_merge(params, job_dir)

        (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
        print("[SUPERVISOR] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[SUPERVISOR] FATAL ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        sys.exit(1)


# ----------------------------------------------------------------------
# Per-TS staging
# ----------------------------------------------------------------------


def _stage_per_ts(
    staging_root: Path,
    ts_name: str,
    optics_df: pd.DataFrame,
    particles_df: pd.DataFrame,
    tomograms_df: pd.DataFrame,
    general_kv: dict,
) -> None:
    """Write per-TS particles.star, tomograms.star, and a key-value
    optimisation_set.star into `.staging/task_<ts>/`. Idempotent — overwrites
    on every supervisor run."""
    task_dir = staging_root / f"task_{ts_name}"
    task_dir.mkdir(parents=True, exist_ok=True)

    ts_particles = particles_df[particles_df["rlnTomoName"].astype(str) == ts_name].reset_index(drop=True)
    ts_tomograms = tomograms_df[tomograms_df["rlnTomoName"].astype(str) == ts_name].reset_index(drop=True)

    if len(ts_particles) == 0:
        raise RuntimeError(
            f"No particles for TS {ts_name} (supervisor enumerated from this column; should not happen)"
        )
    if len(ts_tomograms) == 0:
        raise RuntimeError(
            f"No tomogram row for TS {ts_name} in upstream tomograms.star — "
            f"upstream pipeline is inconsistent."
        )

    # Preserve only the optics groups actually referenced by this TS's
    # particles. Multi-optics-group projects (rare today, possible after
    # merge of multiple datasets) would otherwise carry the union into
    # every task.
    optics_groups = set(ts_particles["rlnOpticsGroup"].astype(str).unique())
    ts_optics = optics_df[optics_df["rlnOpticsGroup"].astype(str).isin(optics_groups)].reset_index(drop=True)
    if len(ts_optics) == 0:
        raise RuntimeError(
            f"No optics rows match this TS's particles (referenced groups: {sorted(optics_groups)})."
        )

    particles_out = task_dir / "particles.star"
    tomograms_out = task_dir / "tomograms.star"
    optset_out = task_dir / "optimisation_set.star"

    _write_particles_star(particles_out, optics_df=ts_optics, particles_df=ts_particles, general_kv=general_kv)
    _write_tomograms_star(tomograms_out, ts_tomograms)
    write_optimisation_set(optset_out, particles_star=particles_out, tomograms_star=tomograms_out)


# ----------------------------------------------------------------------
# Task mode — one TS per array index
# ----------------------------------------------------------------------


def run_task_mode(array_idx: int):
    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(SubtomoExtractionParams)
    except Exception as e:
        print(f"[TASK {array_idx}] BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    try:
        manifest = read_manifest(job_dir)
    except FileNotFoundError:
        print(f"[TASK {array_idx}] Manifest missing — supervisor never wrote one", file=sys.stderr, flush=True)
        sys.exit(1)

    ts_names = manifest.get("items") or []
    if array_idx >= len(ts_names):
        print(f"[TASK {array_idx}] Array index out of range ({len(ts_names)} items)", file=sys.stderr, flush=True)
        sys.exit(1)

    ts_name = ts_names[array_idx]
    status_dir = job_dir / STATUS_DIR_NAME
    print(f"[TASK {array_idx}] TS: {ts_name}", flush=True)

    try:
        staging_dir = job_dir / ".staging" / f"task_{ts_name}"
        per_ts_optset = staging_dir / "optimisation_set.star"
        if not per_ts_optset.exists():
            raise FileNotFoundError(f"Staged optimisation set not found: {per_ts_optset}")

        # RELION writes Subtomograms/<TS>/*.mrcs and particles.star relative
        # to --o. We put per-TS output into <staging>/task_<ts>/out/ so the
        # supervisor can collect from there without worrying about cross-TS
        # collisions. Subtomogram numbering is per-TS independent (RELION
        # restarts the counter at 1 inside each TS subdir).
        out_dir = staging_dir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Idempotent skip: if a prior run already produced outputs, just
        # write .ok and exit. (submit_array_job already filters previously-
        # OK items at the SLURM-array level, so this is belt-and-braces.)
        if (out_dir / "particles.star").exists() and (out_dir / "Subtomograms").exists():
            print(f"[TASK {array_idx}] {ts_name} already extracted — skipping", flush=True)
            write_status_atomic(status_dir, ts_name, ok=True)
            return

        cmd_parts = [
            "relion_tomo_subtomo",
            "--o",
            str(out_dir) + "/",
            "--i",
            str(per_ts_optset),
            "--b",
            str(params.box_size),
            "--bin",
            str(int(params.binning)),
        ]
        if params.crop_size > 0:
            cmd_parts.extend(["--crop", str(params.crop_size)])
        if params.max_dose > 0:
            cmd_parts.extend(["--max_dose", str(params.max_dose)])
        if params.min_frames > 1:
            cmd_parts.extend(["--min_frames", str(params.min_frames)])
        if params.do_stack2d:
            cmd_parts.append("--stack2d")
        if params.do_float16:
            cmd_parts.append("--float16")

        cmd_str = " ".join(cmd_parts)
        print(f"[TASK {array_idx}] Command: {cmd_str}", flush=True)

        # Bind both the staging dir (read input optimisation set) and the
        # upstream optimisation set's directory (relion follows the
        # absolute paths inside it). Dedup to avoid noisy mounts.
        additional_binds = list(context["additional_binds"])
        additional_binds.append(str(staging_dir.resolve()))
        additional_binds.append(str(per_ts_optset.parent.resolve()))
        additional_binds = sorted(set(additional_binds))

        container_service = get_container_service()
        wrapped_cmd = container_service.wrap_command_for_tool(
            cmd_str, cwd=out_dir, tool_name="relion", additional_binds=additional_binds
        )
        run_command(wrapped_cmd, cwd=out_dir)

        if not (out_dir / "particles.star").exists():
            raise RuntimeError(f"relion_tomo_subtomo did not produce particles.star in {out_dir}")
        # Subtomograms dir is the actual stack output; if missing the
        # particles file is referencing files that don't exist.
        if not (out_dir / "Subtomograms").exists():
            raise RuntimeError(f"relion_tomo_subtomo did not produce Subtomograms/ in {out_dir}")

        write_status_atomic(status_dir, ts_name, ok=True)
        print(f"[TASK {array_idx}] {ts_name} OK", flush=True)

    except Exception as e:
        print(f"[TASK {array_idx}] FAILED for {ts_name}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        try:
            write_status_atomic(status_dir, ts_name, ok=False)
        except Exception:
            pass
        sys.exit(1)


# ----------------------------------------------------------------------
# Supervisor merge — per-TS outputs → canonical job_dir layout
# ----------------------------------------------------------------------


def _merge_per_ts_outputs(
    job_dir: Path,
    ts_names: List[str],
    upstream_general_kv: dict,
    upstream_tomograms_star: Path,
) -> None:
    """Concatenate per-TS particles.star into job_dir/particles.star and
    move each task's Subtomograms/<TS>/ subdir into job_dir/Subtomograms/.

    RELION writes per-particle `rlnImageName` paths as absolute, pointing
    into the per-TS staging out/ dir. After moving the subdirs up to
    job_dir, we string-replace the staging prefix in `rlnImageName` so the
    merged particles.star references the consolidated layout.
    """
    final_subtomos_dir = job_dir / "Subtomograms"
    final_subtomos_dir.mkdir(parents=True, exist_ok=True)

    all_optics_dfs: List[pd.DataFrame] = []
    all_particles_dfs: List[pd.DataFrame] = []

    for ts_name in ts_names:
        task_out = job_dir / ".staging" / f"task_{ts_name}" / "out"
        ts_particles_star = task_out / "particles.star"
        if not ts_particles_star.exists():
            raise RuntimeError(f"Expected per-TS particles missing: {ts_particles_star}")

        optics_df, particles_df, _ = _read_particles_star(ts_particles_star)

        # Move per-TS Subtomograms/<TS>/ subdir up to job_dir/Subtomograms/<TS>/.
        # On re-run, if the target already exists (e.g. previous successful
        # run), replace it — we just re-extracted the same particles.
        task_subtomos = task_out / "Subtomograms"
        if task_subtomos.exists():
            for child in task_subtomos.iterdir():
                if not child.is_dir():
                    continue
                target = final_subtomos_dir / child.name
                if target.exists():
                    shutil.rmtree(target)
                shutil.move(str(child), str(target))

        # Rewrite rlnImageName paths from staging to job_dir.
        if "rlnImageName" in particles_df.columns:
            old_prefix = str(task_out.resolve())
            new_prefix = str(job_dir.resolve())
            particles_df["rlnImageName"] = (
                particles_df["rlnImageName"].astype(str).str.replace(old_prefix, new_prefix, n=1, regex=False)
            )

        all_optics_dfs.append(optics_df)
        all_particles_dfs.append(particles_df)

    # Concat + dedup optics (multi-optics-group projects with shared imaging
    # params end up with a single row after drop_duplicates).
    optics_merged = pd.concat(all_optics_dfs, ignore_index=True).drop_duplicates().reset_index(drop=True)
    particles_merged = pd.concat(all_particles_dfs, ignore_index=True)

    _write_particles_star(
        job_dir / "particles.star",
        optics_df=optics_merged,
        particles_df=particles_merged,
        general_kv=upstream_general_kv,
    )

    # Tomograms.star is copied verbatim from upstream — the subtomo
    # extraction doesn't change tomogram metadata. (Downstream RELION
    # readers expect job_dir/tomograms.star to exist.)
    target_tomograms = job_dir / "tomograms.star"
    shutil.copy2(upstream_tomograms_star, target_tomograms)

    write_optimisation_set(
        job_dir / "optimisation_set.star",
        particles_star=job_dir / "particles.star",
        tomograms_star=target_tomograms,
    )

    print(
        f"[SUPERVISOR] Merged: {len(particles_merged)} particles across "
        f"{len(ts_names)} TS → {job_dir / 'particles.star'}",
        flush=True,
    )


def _run_additional_sources_merge(params, job_dir: Path) -> None:
    """Run the aggregation merge against `params.additional_sources`.
    Mirrors the legacy one-shot driver's `run_merge` body."""
    additional_sources = list(params.additional_sources or [])
    if not additional_sources:
        return

    has_primary = (job_dir / "optimisation_set.star").exists()
    print(
        f"[SUPERVISOR] Merging {len(additional_sources)} additional source(s) "
        f"({'with' if has_primary else 'without'} primary)...",
        flush=True,
    )
    for i, src in enumerate(additional_sources):
        print(f"  [{i + 1}] {src}", flush=True)

    summary = merge_optimisation_sets_into_jobdir(
        job_dir=job_dir, additional_sources=additional_sources, allow_no_primary=not has_primary
    )
    print(
        f"[SUPERVISOR] Aggregation merge: {summary['totals']['n_particles']} particles, "
        f"{summary['totals']['n_tomograms']} tomograms",
        flush=True,
    )


if __name__ == "__main__":
    main()
