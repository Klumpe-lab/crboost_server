#!/usr/bin/env python3
"""
template_match_pytom driver — supervisor + per-tomogram SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Submitted by relion_schemer via the standard qsub.sh.
          Reads the tomograms STAR, preflight-checks the registry, writes the
          per-tomogram text inputs (tilt angles / defocus / dose) ONCE, persists
          a task manifest with per-tomogram metadata, builds run_array.sh, and
          sbatches the array. Polls squeue until the array is empty, then emits
          the output tomograms STAR. Exit code 0 only if every tomogram has a
          .ok status file.

- Set:    TASK mode. One tomogram per array index. Reads the manifest, picks
          its tomogram, idempotently skips if `tmResults/{name}_scores.mrc`
          already exists, otherwise symlinks the tomogram MRC into tmResults/
          and runs `pytom_match_template.py` with the per-tomogram text inputs
          prepared by the supervisor. Atomically writes
          `.task_status/{name}.{ok|fail}`.

Tomograms are 1:1 with tilt-series in v1 (Tomogram.tilt_series_id == ts_id), so
the manifest keys off ts_names / tilt_series_ids() directly.
"""

import os
import shutil
import sys
import traceback
from pathlib import Path
from typing import Dict, List

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
from services.computing.container_service import get_container_service
from services.job_models import TemplateMatchPytomParams


# TEMPORARY: Use pytom 0.10-style text file inputs instead of --relion5-tomograms-star.
# Set to True to replicate GT pipeline behavior for score comparison.
LEGACY_TEXT_INPUT = True

DRIVER_SCRIPT = Path(__file__).resolve()


# ----------------------------------------------------------------------
# STAR helpers (shared)
# ----------------------------------------------------------------------


def _get_df_from_star(path: Path) -> pd.DataFrame:
    d = starfile.read(path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame):
            return v
    raise ValueError(f"No dataframe blocks found in {path}")


def _resolve_star_path(base_dir: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()


def generate_legacy_text_files(
    tiltseries_global_star: Path, output_dir: Path
) -> Dict[str, Dict[str, Path]]:
    """
    Replicate old CryoBoost's generatePytomInputFiles: extract tilt angles,
    defocus (in um), and dose from per-tilt star files into plain text files
    that pytom 0.10 expects.

    Returns: {tomo_name: {"tlt": Path, "defocus": Path, "dose": Path}}
    """
    ts_df = _get_df_from_star(tiltseries_global_star)
    ts_base = tiltseries_global_star.parent

    tlt_dir = output_dir / "tiltAngleFiles"
    def_dir = output_dir / "defocusFiles"
    dose_dir = output_dir / "doseFiles"
    for d in (tlt_dir, def_dir, dose_dir):
        d.mkdir(parents=True, exist_ok=True)

    result: Dict[str, Dict[str, Path]] = {}
    for _, row in ts_df.iterrows():
        name = str(row["rlnTomoName"])
        ts_star = _resolve_star_path(ts_base, str(row["rlnTomoTiltSeriesStarFile"]))
        if not ts_star.exists():
            raise FileNotFoundError(f"Per-tilt star not found: {ts_star}")

        tilt_df = _get_df_from_star(ts_star)

        tlt_path = tlt_dir / f"{name}.tlt"
        tilt_df["rlnTomoNominalStageTiltAngle"].to_csv(tlt_path, index=False, header=False)

        def_path = def_dir / f"{name}.txt"
        (tilt_df["rlnDefocusU"] / 10000).to_csv(def_path, index=False, header=False)

        dose_path = dose_dir / f"{name}.txt"
        tilt_df["rlnMicrographPreExposure"].to_csv(dose_path, index=False, header=False)

        result[name] = {"tlt": tlt_path, "defocus": def_path, "dose": dose_path}
        print(f"  [LEGACY] {name}: {len(tilt_df)} tilts written", flush=True)

    return result


def make_pytom_tomograms_star(
    *, tomograms_star: Path, tiltseries_global_star: Path, out_star: Path
) -> Path:
    """Build the patched tomograms STAR with absolute rlnTomoTiltSeriesStarFile paths."""
    tomo_df = _get_df_from_star(tomograms_star).copy()
    ts_df = _get_df_from_star(tiltseries_global_star).copy()

    if "rlnTomoName" not in tomo_df.columns:
        raise KeyError(f"{tomograms_star} missing rlnTomoName")
    if "rlnTomoName" not in ts_df.columns or "rlnTomoTiltSeriesStarFile" not in ts_df.columns:
        raise KeyError(f"{tiltseries_global_star} missing rlnTomoName or rlnTomoTiltSeriesStarFile")

    ts_base = tiltseries_global_star.parent
    name_to_ts = {}
    for _, r in ts_df.iterrows():
        name = str(r["rlnTomoName"])
        ts_path = _resolve_star_path(ts_base, str(r["rlnTomoTiltSeriesStarFile"]))
        name_to_ts[name] = str(ts_path)

    patched = 0
    for i, r in tomo_df.iterrows():
        name = str(r["rlnTomoName"])
        if name in name_to_ts:
            tomo_df.at[i, "rlnTomoTiltSeriesStarFile"] = name_to_ts[name]
            patched += 1

    if patched == 0:
        raise RuntimeError(
            "Could not patch any rlnTomoTiltSeriesStarFile entries. "
            "Check that rlnTomoName matches between tomograms.star and ts_ctf_tilt_series.star."
        )

    # TEMPORARY: Neutralize rlnTomoHand while investigating score compression vs GT.
    # The GT (old CryoBoost + pytom 0.10) never passed handedness to pytom.
    if "rlnTomoHand" in tomo_df.columns:
        print(f"[TEMP-DEBUG] Overriding rlnTomoHand from {tomo_df['rlnTomoHand'].tolist()} -> 1")
        tomo_df["rlnTomoHand"] = 1

    out_star.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({"global": tomo_df}, out_star, overwrite=True)
    return out_star


def get_gpu_split(requested_split: str) -> List[str]:
    if requested_split in ["auto", "None", ""]:
        return ["2", "2", "1"]
    return requested_split.split(":")


def resolve_tomogram_path(
    raw_path: str, *, tomograms_star: Path, project_root: Path
) -> Path:
    """Resolve rlnTomoReconstructedTomogram against common conventions."""
    rel = Path(raw_path)
    if rel.is_absolute():
        return rel
    for c in (tomograms_star.parent / rel, project_root / rel):
        if c.exists():
            return c
    return tomograms_star.parent / rel


def scores_mrc_path(job_dir: Path, tomo_name: str) -> Path:
    return job_dir / "tmResults" / f"{tomo_name}_scores.mrc"


def build_pytom_base_cmd(params: TemplateMatchPytomParams, state, template_file: Path, mask_file: Path,
                         tm_results_dir: Path, gpu_ids: List[str]) -> List[str]:
    """The per-task base command before per-tomogram args are appended."""
    base_cmd = [
        "pytom_match_template.py",
        "-t", str(template_file),
        "-d", str(tm_results_dir),
        "-m", str(mask_file),
        "--angular-search", str(params.angular_search),
        "--voltage", str(state.microscope.acceleration_voltage_kv),
        "--spherical-aberration", str(state.microscope.spherical_aberration_mm),
        "--amplitude-contrast", str(state.microscope.amplitude_contrast),
        "--per-tilt-weighting",
        "--log", "debug",
        "-g",
    ] + gpu_ids

    if params.gpu_split != "None":
        base_cmd.extend(["-s"] + get_gpu_split(params.gpu_split))
    if params.spectral_whitening:
        base_cmd.append("--spectral-whitening")
    if getattr(params, "random_phase_correction", False):
        base_cmd.append("--random-phase-correction")
    if params.non_spherical_mask:
        base_cmd.append("--non-spherical-mask")
    if params.bandpass_filter != "None" and ":" in params.bandpass_filter:
        low, high = params.bandpass_filter.split(":")
        base_cmd.extend(["--low-pass", low, "--high-pass", high])
    if params.symmetry != "C1" and str(params.symmetry).startswith("C"):
        base_cmd.extend(["--z-axis-rotational-symmetry", str(params.symmetry)[1:]])

    return base_cmd


# ----------------------------------------------------------------------
# Mode dispatch
# ----------------------------------------------------------------------


def main():
    os.environ["TQDM_DISABLE"] = "1"
    print("Python", sys.version, flush=True)
    array_idx_env = os.environ.get("SLURM_ARRAY_TASK_ID")
    if array_idx_env is None:
        print("--- template_match_pytom: SUPERVISOR mode ---", flush=True)
        run_supervisor_mode()
    else:
        print(f"--- template_match_pytom: TASK mode (array idx {array_idx_env}) ---", flush=True)
        run_task_mode(int(array_idx_env))


# ----------------------------------------------------------------------
# Supervisor mode
# ----------------------------------------------------------------------


def run_supervisor_mode():
    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(
            TemplateMatchPytomParams
        )
    except Exception as e:
        fail_dir = Path.cwd()
        (fail_dir / "RELION_JOB_EXIT_FAILURE").touch()
        print(f"[SUPERVISOR] FATAL BOOTSTRAP ERROR: {e}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

    print(f"[SUPERVISOR] CWD (job dir): {job_dir}", flush=True)

    try:
        paths = {k: Path(v) for k, v in context["paths"].items()}
        instance_id = context["instance_id"]

        input_star_tomos = paths["input_tomograms"]
        input_star_ts = paths["input_tiltseries"]
        template_file = paths.get("template_path")
        mask_file = paths.get("mask_path")

        if not input_star_tomos.exists():
            raise FileNotFoundError(f"Input tomograms STAR missing: {input_star_tomos}")
        if not input_star_ts.exists():
            raise FileNotFoundError(f"Input tiltseries STAR missing: {input_star_ts}")
        if template_file is None or not Path(template_file).exists():
            raise FileNotFoundError(f"Template file missing: {template_file}")
        if mask_file is None or not Path(mask_file).exists():
            raise FileNotFoundError(f"Mask file missing: {mask_file}")

        tm_results_dir = job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        tomo_df = _get_df_from_star(input_star_tomos)
        required_cols = {"rlnTomoName", "rlnTomoReconstructedTomogram"}
        missing = required_cols - set(tomo_df.columns)
        if missing:
            raise KeyError(f"tomograms.star missing columns {missing}. Have: {list(tomo_df.columns)}")

        tomo_names = sorted(tomo_df["rlnTomoName"].astype(str).tolist())
        print(f"[SUPERVISOR] Found {len(tomo_names)} tomograms in input STAR", flush=True)

        # Tomograms are 1:1 with TS in v1 — preflight the registry on TS IDs.
        preflight_registry(project_path, tomo_names, job_name="template_match_pytom")

        # Prepare per-tomogram inputs ONCE so tasks don't each re-parse STARs.
        legacy_files: Dict[str, Dict[str, Path]] = {}
        if LEGACY_TEXT_INPUT:
            print("[SUPERVISOR] LEGACY MODE: generating text files for pytom 0.10", flush=True)
            legacy_files = generate_legacy_text_files(
                tiltseries_global_star=input_star_ts, output_dir=job_dir
            )
        else:
            patched_tomos = make_pytom_tomograms_star(
                tomograms_star=input_star_tomos,
                tiltseries_global_star=input_star_ts,
                out_star=job_dir / "tomograms_for_pytom.star",
            )
            print(f"[SUPERVISOR] Patched tomograms STAR for PyTOM: {patched_tomos}", flush=True)

            ts_staging_dir = job_dir / "tilt_series"
            ts_staging_dir.mkdir(exist_ok=True)
            patched_df = _get_df_from_star(patched_tomos)
            for _, row in patched_df.iterrows():
                ts_star_abs = Path(row["rlnTomoTiltSeriesStarFile"])
                link_target = ts_staging_dir / ts_star_abs.name
                if not link_target.exists():
                    if ts_star_abs.exists():
                        os.symlink(ts_star_abs.resolve(), link_target)
                    else:
                        raise FileNotFoundError(
                            f"Tilt series star not found: {ts_star_abs}\n"
                            f"Cannot stage for PyTOM. Check upstream CTF job output."
                        )

        # Build per-tomogram metadata for the manifest — tasks use this to
        # avoid re-reading the tomograms STAR.
        raw_tomo_paths: Dict[str, str] = {}
        for _, row in tomo_df.iterrows():
            name = str(row["rlnTomoName"])
            raw_tomo_paths[name] = str(row["rlnTomoReconstructedTomogram"])

        manifest_extra = {
            "input_tomograms_star": str(input_star_tomos),
            "raw_tomo_paths": raw_tomo_paths,
            "legacy_text_input": LEGACY_TEXT_INPUT,
            "patched_tomograms_star": None if LEGACY_TEXT_INPUT else str(job_dir / "tomograms_for_pytom.star"),
            "template_path": str(template_file),
            "mask_path": str(mask_file),
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
            wait_for_array_completion(array_job_id, poll_secs=30)
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

        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(input_star_tomos, output_tomograms)
        print(f"[SUPERVISOR] Copied tomograms.star to {output_tomograms}", flush=True)

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
            TemplateMatchPytomParams
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

        raw_tomo_paths = manifest.get("raw_tomo_paths") or {}
        raw_tomo_path = raw_tomo_paths.get(tomo_name)
        if not raw_tomo_path:
            raise KeyError(f"manifest missing raw_tomo_paths['{tomo_name}']")

        input_star_tomos = Path(manifest["input_tomograms_star"])
        template_file = Path(manifest["template_path"])
        mask_file = Path(manifest["mask_path"])
        use_legacy = bool(manifest.get("legacy_text_input", True))
        patched_tomograms_star = manifest.get("patched_tomograms_star")

        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = list(context.get("additional_binds", []))
        additional_binds.append(str(template_file.parent.resolve()))
        additional_binds.append(str(mask_file.parent.resolve()))
        additional_binds = sorted(set(additional_binds))

        tm_results_dir = job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        out_scores = scores_mrc_path(job_dir, tomo_name)
        if out_scores.exists() and out_scores.stat().st_size > 0:
            print(f"[TASK {array_idx}] Scores already exist, skipping: {out_scores}", flush=True)
            write_status_atomic(status_dir, tomo_name, ok=True)
            sys.exit(0)

        tomo_path = resolve_tomogram_path(
            raw_tomo_path, tomograms_star=input_star_tomos, project_root=project_path
        )
        if not tomo_path.exists():
            raise FileNotFoundError(
                f"Tomogram file does not exist for {tomo_name}.\n"
                f"  STAR entry: {raw_tomo_path}\n"
                f"  Resolved:   {tomo_path}"
            )

        local_tomo = tm_results_dir / f"{tomo_name}{tomo_path.suffix or '.mrc'}"
        if not local_tomo.exists():
            os.symlink(tomo_path.resolve(), local_tomo)

        gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")
        base_cmd = build_pytom_base_cmd(
            params=params, state=state, template_file=template_file,
            mask_file=mask_file, tm_results_dir=tm_results_dir, gpu_ids=gpu_ids,
        )

        cmd = base_cmd.copy()
        cmd.extend(["-v", str(local_tomo)])
        if use_legacy:
            cmd.extend(["--tilt-angles", str(job_dir / "tiltAngleFiles" / f"{tomo_name}.tlt")])
            cmd.extend(["--defocus", str(job_dir / "defocusFiles" / f"{tomo_name}.txt")])
            cmd.extend(["--dose-accumulation", str(job_dir / "doseFiles" / f"{tomo_name}.txt")])
        else:
            if not patched_tomograms_star:
                raise RuntimeError("Non-legacy mode requires patched_tomograms_star in manifest")
            cmd.extend(["--relion5-tomograms-star", patched_tomograms_star])

        cmd_str = " ".join(cmd)
        print(f"[TASK {array_idx}] Command: {cmd_str}", flush=True)

        wrapped = get_container_service().wrap_command_for_tool(
            command=cmd_str, cwd=job_dir, tool_name=params.get_tool_name(), additional_binds=additional_binds
        )
        run_command(wrapped, cwd=job_dir)

        if not out_scores.exists():
            raise FileNotFoundError(
                f"pytom_match_template reported success but expected output missing: {out_scores}"
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
