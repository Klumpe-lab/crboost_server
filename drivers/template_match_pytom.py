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

def _get_df_from_star(path: Path) -> pd.DataFrame:
    d = starfile.read(path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame):
            return v
    raise ValueError(f"No dataframe blocks found in {path}")

def _resolve_star_path(base_dir: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()

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

    # Build mapping: tomo name -> ABS path to per-tomo tilt-series star
    ts_base = tiltseries_global_star.parent
    name_to_ts = {}
    for _, r in ts_df.iterrows():
        name = str(r["rlnTomoName"])
        ts_path = _resolve_star_path(ts_base, str(r["rlnTomoTiltSeriesStarFile"]))
        name_to_ts[name] = str(ts_path)

    # Patch tomograms df
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

    out_star.parent.mkdir(parents=True, exist_ok=True)
    starfile.write({"global": tomo_df}, out_star, overwrite=True)
    return out_star




def get_gpu_split(requested_split: str) -> list:
    """Parses the '2:1:1' string into list arguments for PyTOM."""
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
        tomograms_star.parent / rel,  # preferred (matches your project)
        project_root / rel,  # fallback
    ]
    for c in candidates:
        if c.exists():
            return c

    # If nothing exists, return the best guess (the preferred convention),
    # but leave a clear error to the caller.
    return candidates[0]


def main():
    os.environ["TQDM_DISABLE"] = "1"
    print("--- SLURM JOB START (Template Matching) ---", flush=True)

    try:
        (state, params, context, job_dir, project_path, job_type) = get_driver_context()
    except Exception as e:
        print(f"[DRIVER] BOOTSTRAP ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    success_file = job_dir / "RELION_JOB_EXIT_SUCCESS"
    failure_file = job_dir / "RELION_JOB_EXIT_FAILURE"

    try:
        # 1. Unpack resolved paths
        paths = {k: Path(v) for k, v in context["paths"].items()}
        additional_binds = list(context.get("additional_binds", []))

        input_star_tomos = paths["input_tomograms"]  # tomograms.star (upstream)
        input_star_ts = paths["input_tiltseries"]  # ts_ctf_tilt_series.star (upstream)

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
            "--tomogram-ctf-model", "phase-flip",
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

        patched_tomos = make_pytom_tomograms_star(
            tomograms_star=input_star_tomos,
            tiltseries_global_star=input_star_ts,
            out_star=job_dir / "tomograms_for_pytom.star",
        )

        print(f"[DRIVER] Using patched tomograms STAR for PyTOM: {patched_tomos}", flush=True)
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

            # PyTOM derives tomogram ID from volume filename stem.
            # WarpTools names files as {TomoName}_{pixelsize}Apx.mrc but
            # rlnTomoName is just {TomoName}. Symlink to match.

            local_tomo = tm_results_dir / f"{tomo_name}{tomo_path.suffix or '.mrc'}"
            if not local_tomo.exists():
                os.symlink(tomo_path.resolve(), local_tomo)


            cmd = base_cmd.copy()
            cmd.extend(["-v", str(local_tomo)])
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
