#!/usr/bin/env python3
"""
extract_candidates_pytom driver — supervisor + per-tomogram SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Stages the upstream tmResults dir (copy+patch JSONs,
          symlink score/angle MRCs) into the job dir ONCE. Determines pixel
          size, preflight-checks the registry, writes the manifest, and
          submits the per-tomogram array. On completion merges per-tomogram
          `*_particles.star` files into `candidates.star`, cleans rlnTomoName
          suffixes, copies tomograms.star, writes optimisation_set.star, and
          generates IMOD visualization (non-fatal).

- Set:    TASK mode. One tomogram per array index. Reads the manifest, finds
          its `{tomo}_{apix}Apx_job.json` in the staged tmResults dir, runs
          `pytom_extract_candidates.py -j <job.json>`, and atomically writes
          `.task_status/{tomo}.{ok|fail}`.
"""

import json
import os
import shutil
import sys
import traceback
from pathlib import Path
from typing import List

import pandas as pd
import starfile

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    collect_task_results,
    install_cancel_handler,
    preflight_registry,
    read_manifest,
    submit_array_job,
    wait_for_array_completion,
    write_status_atomic,
    STATUS_DIR_NAME,
)
from drivers.driver_base import get_driver_context, run_command
from drivers.subtomo_merge import write_optimisation_set
from services.computing.container_service import get_container_service
from services.job_models import CandidateExtractPytomParams, ExtractionCutoffMethod


DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# Helpers (shared supervisor + task)
# ----------------------------------------------------------------------


def get_pixel_size_from_star(tomograms_star: Path) -> float:
    """Extract pixel size from tomograms.star metadata."""
    try:
        data = starfile.read(tomograms_star)
        if isinstance(data, dict):
            df = list(data.values())[0]
        else:
            df = data
        ts_pixs = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0])
        binning = float(df.get("rlnTomoTomogramBinning", pd.Series([1])).iloc[0])
        return ts_pixs * binning
    except Exception as e:
        print(f"[WARN] Could not read pixel size from {tomograms_star}: {e}")
        return None


def cleanup_tomo_names(candidates_star: Path, apix_fallback: float) -> int:
    """Remove the pixel size suffix from rlnTomoName in the merged candidates STAR."""
    try:
        data = starfile.read(candidates_star, always_dict=True)
        df = None
        for val in data.values():
            if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
                df = val
                break
        if df is None:
            return 0

        if "rlnTomoTiltSeriesPixelSize" in df.columns and "rlnTomoTomogramBinning" in df.columns:
            apix = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0]) * float(df["rlnTomoTomogramBinning"].iloc[0])
        elif "rlnTomoTiltSeriesPixelSize" in df.columns:
            apix = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0])
        else:
            apix = apix_fallback

        suffix = f"_{apix:.2f}Apx"
        df["rlnTomoName"] = df["rlnTomoName"].str.replace(suffix, "", regex=False)
        starfile.write(data, candidates_star, overwrite=True)
        print(f"[SUPERVISOR] Cleaned rlnTomoName suffix '{suffix}' from {len(df)} particles")
        return len(df)
    except Exception as e:
        print(f"[WARN] Could not clean tomo names: {e}")
        return 0


def build_extract_base_cmd(params: CandidateExtractPytomParams, apix: float) -> List[str]:
    base_cmd = [
        "pytom_extract_candidates.py",
        "-n", str(params.max_num_particles),
        "--particle-diameter", str(int(params.particle_diameter_ang / 2.0 / apix) * apix),
        "--relion5-compat",
        "--log", "debug",
    ]
    if params.cutoff_method == ExtractionCutoffMethod.FALSE_POSITIVES:
        base_cmd.extend(["--number-of-false-positives", str(params.cutoff_value)])
    elif params.cutoff_method == ExtractionCutoffMethod.MANUAL:
        base_cmd.extend(["-c", str(params.cutoff_value)])

    if params.score_filter_method == "tophat":
        base_cmd.append("--tophat-filter")
        if params.score_filter_value != "None" and ":" in params.score_filter_value:
            conn, bins = params.score_filter_value.split(":")
            base_cmd.extend(["--tophat-connectivity", conn, "--tophat-bins", bins])
    return base_cmd


def stage_upstream_tm_results(upstream: Path, local: Path) -> int:
    """Copy-and-patch JSONs, symlink other artifacts from upstream tmResults to local."""
    local.mkdir(exist_ok=True)
    linked = 0
    for f in upstream.iterdir():
        target = local / f.name
        if target.exists():
            continue
        if f.suffix == ".json":
            with open(f, "r") as src:
                data = json.load(src)
            data["output_dir"] = str(local)
            with open(target, "w") as dst:
                json.dump(data, dst, indent=4)
        else:
            os.symlink(f.resolve(), target)
        linked += 1
    return linked


def tomo_job_json_path(local_tm_results: Path, tomo_name: str) -> Path:
    """pytom writes `{tomo_name}_job.json`. The supervisor staged this into `job_dir/tmResults/`."""
    return local_tm_results / f"{tomo_name}_job.json"


def tomo_particles_star_path(local_tm_results: Path, tomo_name: str) -> Path:
    """pytom writes `{tomo_name}_particles.star` under the output_dir."""
    return local_tm_results / f"{tomo_name}_particles.star"


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    os.environ["TQDM_DISABLE"] = "1"
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- extract_candidates_pytom: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- extract_candidates_pytom: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(
            CandidateExtractPytomParams
        )
    except Exception as e:
        (Path.cwd() / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        instance_id = context["instance_id"]

        upstream_results = paths["input_tm_job"]
        input_tomograms = paths.get("input_tomograms")

        if not upstream_results.exists():
            raise FileNotFoundError(f"Upstream tmResults not found at {upstream_results}")
        if not input_tomograms or not input_tomograms.exists():
            raise FileNotFoundError(f"Input tomograms.star not found at {input_tomograms}")

        apix = None
        if params.apix_score_map != "auto":
            apix = float(params.apix_score_map)
        else:
            apix = get_pixel_size_from_star(input_tomograms)
        if apix is None:
            raise RuntimeError(
                "Could not determine score map pixel size. "
                "Set apix_score_map explicitly or check tomograms.star."
            )
        print(f"[SUPERVISOR] Score map pixel size: {apix:.2f} A/px", flush=True)
        print(f"[SUPERVISOR] Particle diameter: {params.particle_diameter_ang} A", flush=True)

        local_tm_results = job_dir / "tmResults"
        linked = stage_upstream_tm_results(upstream_results, local_tm_results)
        if linked == 0 and not any(local_tm_results.iterdir()):
            raise RuntimeError("No files staged from upstream tmResults")
        print(f"[SUPERVISOR] Staged {linked} files from upstream", flush=True)

        # Enumerate tomogram names from the staged *_job.json files — pytom
        # writes exactly one `{tomo_name}_job.json` per tomogram during TM.
        job_jsons = sorted(local_tm_results.glob("*_job.json"))
        if not job_jsons:
            raise RuntimeError(f"No *_job.json files found under {local_tm_results}")

        suffix = "_job.json"
        tomo_names: List[str] = sorted(j.name[: -len(suffix)] for j in job_jsons)
        print(f"[SUPERVISOR] Found {len(tomo_names)} tomograms to extract", flush=True)

        preflight_registry(project_path, tomo_names, job_name="extract_candidates_pytom")

        manifest_extra = {
            "apix": apix,
            "input_tomograms_star": str(input_tomograms),
        }

        per_task_cfg = params.get_effective_slurm_config()

        array_job_id = submit_array_job(
            job_dir=job_dir,
            project_path=project_path,
            instance_id=instance_id,
            ts_names=tomo_names,
            per_task_cfg=per_task_cfg,
            array_throttle=params.array_throttle,
            driver_script=DRIVER_SCRIPT,
            manifest_extra=manifest_extra,
        )

        if array_job_id is not None:
            install_cancel_handler(array_job_id, job_dir)
            wait_for_array_completion(array_job_id, poll_secs=15)
        else:
            print("[SUPERVISOR] No array submitted (all tomograms previously succeeded)", flush=True)

        results = collect_task_results(job_dir, tomo_names)
        print(f"[SUPERVISOR] Status: {results.summary}", flush=True)
        if results.failed:
            print(f"[SUPERVISOR] FAILED tomograms: {results.failed}", flush=True)
        if results.missing:
            print(f"[SUPERVISOR] MISSING tomograms: {results.missing}", flush=True)

        if not results.all_succeeded:
            (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
            print("[SUPERVISOR] Marking job as FAILED (some tomograms did not succeed)", flush=True)
            sys.exit(1)

        # ---- Aggregate per-tomogram particle lists ----
        candidates_star = job_dir / "candidates.star"
        star_files = sorted(local_tm_results.glob("*_particles.star"))
        if not star_files:
            raise RuntimeError("No *_particles.star files produced by tasks")
        if len(star_files) == 1:
            shutil.copy(star_files[0], candidates_star)
            print(f"[SUPERVISOR] Single tomogram — copied {star_files[0].name}", flush=True)
        else:
            dfs = []
            for f in star_files:
                data = starfile.read(f, always_dict=True)
                for val in data.values():
                    if isinstance(val, pd.DataFrame):
                        dfs.append(val)
                        break
            merged = pd.concat(dfs, ignore_index=True)
            starfile.write({"particles": merged}, candidates_star, overwrite=True)
            print(f"[SUPERVISOR] Merged {len(merged)} particles from {len(star_files)} tomograms", flush=True)

        if not candidates_star.exists():
            raise RuntimeError("candidates.star was not created")

        n_particles = cleanup_tomo_names(candidates_star, apix)
        print(f"[SUPERVISOR] Extracted {n_particles} particles total", flush=True)

        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(input_tomograms, output_tomograms)

        try:
            from services.visualization.imod_vis import generate_candidate_vis
            print("[SUPERVISOR] Generating IMOD visualization...", flush=True)
            generate_candidate_vis(
                candidates_star=candidates_star,
                tomograms_star=output_tomograms,
                particle_diameter_ang=float(params.particle_diameter_ang),
                output_dir=job_dir,
                project_root=project_path,
            )
        except Exception as vis_err:
            print(f"[SUPERVISOR WARN] Visualization generation failed (non-fatal): {vis_err}", flush=True)

        write_optimisation_set(
            job_dir / "optimisation_set.star",
            particles_star=candidates_star,
            tomograms_star=output_tomograms,
        )
        print("[SUPERVISOR] Created optimisation_set.star with absolute paths", flush=True)

        (job_dir / "RELION_JOB_EXIT_SUCCESS").touch()
        print("[SUPERVISOR] Job finished successfully.", flush=True)
        sys.exit(0)

    except Exception as e:
        print(f"[SUPERVISOR] FATAL ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        (job_dir / "RELION_JOB_EXIT_FAILURE").touch()
        sys.exit(1)


# ----------------------------------------------------------------------
# Task mode
# ----------------------------------------------------------------------


def run_task_mode(array_idx: int):
    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(
            CandidateExtractPytomParams
        )
    except Exception as e:
        print(f"[TASK {array_idx}] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    status_dir = job_dir / STATUS_DIR_NAME
    tomo_name = None
    try:
        manifest = read_manifest(job_dir)
        tomo_names = manifest["ts_names"]
        if array_idx >= len(tomo_names):
            raise IndexError(f"SLURM_ARRAY_TASK_ID {array_idx} out of range (manifest has {len(tomo_names)})")
        tomo_name = tomo_names[array_idx]
        print(f"[TASK {array_idx}] tomo_name={tomo_name}", flush=True)

        apix = float(manifest["apix"])
        additional_binds = list(context.get("additional_binds", []))

        local_tm_results = job_dir / "tmResults"
        job_json = tomo_job_json_path(local_tm_results, tomo_name)
        if not job_json.exists():
            raise FileNotFoundError(f"Staged job.json missing for {tomo_name}: {job_json}")

        out_star = tomo_particles_star_path(local_tm_results, tomo_name)
        if out_star.exists() and out_star.stat().st_size > 0:
            print(f"[TASK {array_idx}] Particles already extracted, skipping: {out_star}", flush=True)
            write_status_atomic(status_dir, tomo_name, ok=True)
            sys.exit(0)

        base_cmd = build_extract_base_cmd(params, apix)
        cmd = base_cmd + ["-j", str(job_json)]
        cmd_str = " ".join(cmd)
        print(f"[TASK {array_idx}] Command: {cmd_str}", flush=True)

        wrapped = get_container_service().wrap_command_for_tool(
            command=cmd_str, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )
        run_command(wrapped, cwd=job_dir)

        if not out_star.exists():
            raise FileNotFoundError(
                f"pytom_extract_candidates reported success but particles STAR missing: {out_star}"
            )

        write_status_atomic(status_dir, tomo_name, ok=True)
        print(f"[TASK {array_idx}] {tomo_name} done", flush=True)
        sys.exit(0)

    except Exception as e:
        label = tomo_name or f"_unknown_idx{array_idx}"
        print(f"[TASK {array_idx}] FATAL ERROR for tomo={label}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        try:
            write_status_atomic(status_dir, label, ok=False)
        except Exception as inner:
            print(f"[TASK {array_idx}] Could not write fail status: {inner}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
