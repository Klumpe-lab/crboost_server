#!/usr/bin/env python3
import sys
import os
import pandas as pd
import starfile
import traceback
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.driver_base import get_driver_context, run_command
from services.computing.container_service import get_container_service
from services.job_models import TemplateMatchPytomParams

# TEMPORARY: Use pytom 0.10-style text file inputs instead of --relion5-tomograms-star.
# Set to True to replicate GT pipeline behavior for score comparison.
LEGACY_TEXT_INPUT = True

def _get_df_from_star(path: Path) -> pd.DataFrame:
    d = starfile.read(path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame):
            return v
    raise ValueError(f"No dataframe blocks found in {path}")


def _resolve_star_path(base_dir: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()


def generate_legacy_text_files(tiltseries_global_star: Path,
    output_dir: Path,
) -> dict[str, dict[str, Path]]:
    """
    Replicate old CryoBoost's generatePytomInputFiles:
    extract tilt angles, defocus (in um), and dose from per-tilt star files
    into plain text files that pytom 0.10 expects.

    Returns: {tomo_name: {"tlt": Path, "defocus": Path, "dose": Path}}
    """
    ts_df = _get_df_from_star(tiltseries_global_star)
    ts_base = tiltseries_global_star.parent

    tlt_dir = output_dir / "tiltAngleFiles"
    def_dir = output_dir / "defocusFiles"
    dose_dir = output_dir / "doseFiles"
    for d in (tlt_dir, def_dir, dose_dir):
        d.mkdir(parents=True, exist_ok=True)

    result = {}
    for _, row in ts_df.iterrows():
        name = str(row["rlnTomoName"])
        ts_star = _resolve_star_path(ts_base, str(row["rlnTomoTiltSeriesStarFile"]))

        if not ts_star.exists():
            raise FileNotFoundError(f"Per-tilt star not found: {ts_star}")

        tilt_df = _get_df_from_star(ts_star)

        # Tilt angles (degrees) -- old CB used rlnTomoYTilt
        tlt_path = tlt_dir / f"{name}.tlt"
        # tilt_df["rlnTomoYTilt"].to_csv(tlt_path, index=False, header=False)
        tilt_df["rlnTomoNominalStageTiltAngle"].to_csv(tlt_path, index=False, header=False)

        # Defocus in microns -- old CB divided rlnDefocusU by 10000
        def_path = def_dir / f"{name}.txt"
        (tilt_df["rlnDefocusU"] / 10000).to_csv(def_path, index=False, header=False)

        # Dose accumulation (e/A^2)
        dose_path = dose_dir / f"{name}.txt"
        tilt_df["rlnMicrographPreExposure"].to_csv(dose_path, index=False, header=False)

        result[name] = {"tlt": tlt_path, "defocus": def_path, "dose": dose_path}
        print(f"  [LEGACY] {name}: {len(tilt_df)} tilts written", flush=True)

    return result


def make_pytom_tomograms_star(
    *,
    tomograms_star: Path,
    tiltseries_global_star: Path,
    out_star: Path,
) -> Path:
    tomo_df = _get_df_from_star(tomograms_star).copy()
    ts_df   = _get_df_from_star(tiltseries_global_star).copy()

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

    # TEMPORARY: Neutralize rlnTomoHand to test whether defocus_handedness=-1
    # (read by pytom 0.12 from STAR) is causing score compression vs GT which
    # used the text-file interface and got defocus_handedness=0.
    # The GT (old CryoBoost + pytom 0.10) never passed handedness to pytom.
    # Set to 1 (not 0) because pytom may use it as a multiplier on the defocus gradient.
    # TODO: remove once root cause is confirmed
    if "rlnTomoHand" in tomo_df.columns:
        print(f"[TEMP-DEBUG] Overriding rlnTomoHand from {tomo_df['rlnTomoHand'].tolist()} -> 1")
        tomo_df["rlnTomoHand"] = 1

    out_star.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({"global": tomo_df}, out_star, overwrite=True)
    return out_star


def get_gpu_split(requested_split: str) -> list:
    if requested_split in ["auto", "None", ""]:
        return ["2", "2", "1"]
    return requested_split.split(":")


def resolve_tomogram_path(raw_path: str | Path, *, tomograms_star: Path, project_root: Path) -> Path:
    """
    Robust resolver for rlnTomoReconstructedTomogram.

    Common conventions:
      1) path relative to the tomograms.star directory (job dir)
      2) path relative to project root
      3) absolute path
    """
    rel = Path(raw_path)

    if rel.is_absolute():
        return rel

    candidates = [
        tomograms_star.parent / rel,
        project_root / rel,
    ]
    for c in candidates:
        if c.exists():
            return c

    return candidates[0]


def main():
    os.environ["TQDM_DISABLE"] = "1"
    print("--- SLURM JOB START (Template Matching) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context(TemplateMatchPytomParams)
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Unpack resolved paths
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = list(context.get("additional_binds", []))

        input_star_tomos = paths["input_tomograms"]
        input_star_ts = paths["input_tiltseries"]

        template_file = paths.get("template_path")
        mask_file = paths.get("mask_path")

        # 2. Validation
        if not input_star_tomos.exists():
            raise FileNotFoundError(f"Input tomograms STAR missing: {input_star_tomos}")
        if not input_star_ts.exists():
            raise FileNotFoundError(f"Input tiltseries STAR missing: {input_star_ts}")

        if template_file is None or not template_file.exists():
            raise FileNotFoundError(f"Template file missing: {template_file}")

        if mask_file is None or not mask_file.exists():
            raise FileNotFoundError(f"Mask file missing: {mask_file}")

        # 3. Setup output
        tm_results_dir = job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        # 4. Construct base command
        gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")

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

        # Optional flags
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

        # 5. Read tomograms.star
        tomo_df = starfile.read(input_star_tomos)
        if isinstance(tomo_df, dict):
            tomo_df = list(tomo_df.values())[0]

        required_cols = {"rlnTomoName", "rlnTomoReconstructedTomogram"}
        missing = required_cols - set(tomo_df.columns)
        if missing:
            raise KeyError(f"tomograms.star missing columns {missing}. Have: {list(tomo_df.columns)}")

        container_service = get_container_service()

        # Bind template/mask parents (in case they are outside project binds)
        additional_binds.append(str(template_file.parent.resolve()))
        additional_binds.append(str(mask_file.parent.resolve()))
        additional_binds = list(set(additional_binds))
        legacy_files = None
        if LEGACY_TEXT_INPUT:
            print("[DRIVER] LEGACY MODE: generating text files for pytom 0.10", flush=True)
            legacy_files = generate_legacy_text_files(
                tiltseries_global_star=input_star_ts,
                output_dir=job_dir,
            )
            # base_cmd.extend(["--tomogram-ctf-model", "phase-flip"])
        else:
            patched_tomos = make_pytom_tomograms_star(
                tomograms_star=input_star_tomos,
                tiltseries_global_star=input_star_ts,
                out_star=job_dir / "tomograms_for_pytom.star",
            )
            print(f"[DRIVER] Using patched tomograms STAR for PyTOM: {patched_tomos}", flush=True)

            # base_cmd.extend(["--tomogram-ctf-model", "phase-flip"])



            ts_staging_dir = job_dir / "tilt_series"
            ts_staging_dir.mkdir(exist_ok=True)
            patched_df = _get_df_from_star(patched_tomos)
            for _, row in patched_df.iterrows():
                ts_star_abs = Path(row["rlnTomoTiltSeriesStarFile"])
                link_target = ts_staging_dir / ts_star_abs.name
                if not link_target.exists():
                    if ts_star_abs.exists():
                        os.symlink(ts_star_abs.resolve(), link_target)
                        print(f"  [STAGE] {ts_star_abs.name} -> {ts_star_abs}")
                    else:
                        raise FileNotFoundError(
                            f"Tilt series star not found: {ts_star_abs}\n"
                            f"Cannot stage for PyTOM. Check upstream CTF job output."
                        )

        # 6. Iterate tomograms
        for _, row in tomo_df.iterrows():
            tomo_name = str(row["rlnTomoName"])
            raw_tomo_path = row["rlnTomoReconstructedTomogram"]

            tomo_path = resolve_tomogram_path(
                raw_tomo_path,
                tomograms_star=input_star_tomos,
                project_root=project_path,
            )

            print(f"[DRIVER] Processing {tomo_name}...", flush=True)
            print(f"[DEBUG] raw tomo path: {raw_tomo_path}", flush=True)
            print(f"[DEBUG] resolved tomo path: {tomo_path}", flush=True)

            if not tomo_path.exists():
                raise FileNotFoundError(
                    f"Tomogram file does not exist for {tomo_name}.\n"
                    f"  STAR entry: {raw_tomo_path}\n"
                    f"  Resolved:   {tomo_path}\n"
                    f"  tomograms.star: {input_star_tomos}"
                )

            local_tomo = tm_results_dir / f"{tomo_name}{tomo_path.suffix or '.mrc'}"
            if not local_tomo.exists():
                os.symlink(tomo_path.resolve(), local_tomo)

            cmd = base_cmd.copy()
            cmd.extend(["-v", str(local_tomo)])

            if LEGACY_TEXT_INPUT:
                files = legacy_files[tomo_name]
                cmd.extend(["--tilt-angles", str(files["tlt"])])
                cmd.extend(["--defocus", str(files["defocus"])])
                cmd.extend(["--dose-accumulation", str(files["dose"])])
            else:
                cmd.extend(["--relion5-tomograms-star", str(patched_tomos)])

            cmd_str = " ".join(cmd)
            wrapped_cmd = container_service.wrap_command_for_tool(
                cmd_str, cwd=job_dir, tool_name="pytom", additional_binds=additional_binds
            )

            run_command(wrapped_cmd, cwd=job_dir)

        import shutil
        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(input_star_tomos, output_tomograms)
        print(f"[DRIVER] Copied tomograms.star to {output_tomograms}", flush=True)

        success_file.touch()
        print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)

    except Exception as e:
        print(f"[DRIVER] FATAL: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        failure_file.touch()
        sys.exit(1)


if __name__ == "__main__":
    main()
