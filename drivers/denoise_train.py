#!/usr/bin/env python3
import sys
import os
from pathlib import Path
import traceback
import json
from services.computing.container_service import get_container_service
import starfile

# Direct imports for validation since we are inside the container
try:
    import numpy as np
    import mrcfile
    from cryocare.internals.CryoCAREDataModule import CryoCARE_DataModule

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False
    print("[WARN] Could not import cryocare/mrcfile/numpy. Validation will be skipped.", file=sys.stderr)

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from drivers.driver_base import get_driver_context, run_command
    from services.job_models import DenoiseTrainParams
    from services.models_base import DenoiseMethod, IsoNetRefineMethod
except ImportError:
    print("FATAL: Could not import services.", file=sys.stderr)
    sys.exit(1)


def run_isonet_train(params, paths, job_dir, project_path, additional_binds):
    """IsoNet refine (training) path — single global job (one model from many tomograms).

    Chain (all inside the isonet container): prepare_star -> [deconv] -> make_mask -> refine,
    then tar the refined model dir to the OUTPUT_SCHEMA model slot (denoising_model.tar.gz; inner
    dir 'isonet_maps/' so the archive is self-identifying vs cryoCARE's 'denoising_model/').

    Flag NAMES are from the IsoNet2 HEAD Fire signatures; the top-level command set matches the
    installed sif. ISONET-ASSUMPTION markers flag conventions to confirm with a smoke run
    (isonet.py <cmd> --help; a 1-2 TS prepare_star+refine). See ISONET_INTEGRATION_PLAN.md.
    """
    import shlex

    container = get_container_service()
    input_star = paths["input_star"]

    def isonet(cmd: str):
        wrapped = container.wrap_command_for_tool(
            command=cmd, cwd=job_dir, tool_name="isonet", additional_binds=additional_binds
        )
        run_command(wrapped, cwd=job_dir)

    tomo_df = starfile.read(input_star)
    if isinstance(tomo_df, dict):
        tomo_df = tomo_df.get("global", next(iter(tomo_df.values())))
    if not (
        "rlnTomoReconstructedTomogramHalf1" in tomo_df.columns
        and "rlnTomoReconstructedTomogramHalf2" in tomo_df.columns
    ):
        raise ValueError("IsoNet train needs even/odd halves (Half1/Half2 columns) from tsReconstruct.")

    # 1. Stage the filtered FULL + even/odd tomograms into per-job dirs prepare_star enumerates.
    # The full reconstruction populates rlnTomoName (prepare_star leaves it None for even/odd-only),
    # which make_mask/refine read; the even/odd halves drive noise2noise. Matched by basename.
    filter_str = params.tomograms_for_training
    full_dir = job_dir / "isonet_input" / "full"
    even_dir = job_dir / "isonet_input" / "even"
    odd_dir = job_dir / "isonet_input" / "odd"
    for d in (full_dir, even_dir, odd_dir):
        d.mkdir(parents=True, exist_ok=True)
    n = 0
    for _, row in tomo_df.iterrows():
        if filter_str not in str(row["rlnTomoReconstructedTomogram"]):
            continue
        full = resolve_tomo_path(project_path, input_star, str(row["rlnTomoReconstructedTomogram"]))
        even = resolve_tomo_path(project_path, input_star, str(row["rlnTomoReconstructedTomogramHalf1"]))
        odd = resolve_tomo_path(project_path, input_star, str(row["rlnTomoReconstructedTomogramHalf2"]))
        if not (full.exists() and even.exists() and odd.exists()):
            print(f"[ISONET] WARN missing full/halves for {full.name}", flush=True)
            continue
        for src, dst_dir in ((full, full_dir), (even, even_dir), (odd, odd_dir)):
            link = dst_dir / src.name
            if link.exists() or link.is_symlink():
                link.unlink()
            link.symlink_to(src.resolve())
        n += 1
    if n == 0:
        raise ValueError(f"No tomograms matched tomograms_for_training={filter_str!r} for IsoNet training.")
    print(f"[ISONET] Staged {n} even/odd pair(s) for training", flush=True)

    first = tomo_df.iloc[0]
    cs = float(first.get("rlnSphericalAberration", 2.7))
    voltage = float(first.get("rlnVoltage", 300.0))
    ac = float(first.get("rlnAmplitudeContrast", 0.1))

    prep = "isonet_prep.star"
    # 2. prepare_star — build the IsoNet registry from the staged full + even/odd dirs.
    isonet(
        f"isonet.py prepare_star --full {shlex.quote(str(full_dir))} "
        f"--even {shlex.quote(str(even_dir))} --odd {shlex.quote(str(odd_dir))} "
        f"--star_name {prep} --pixel_size auto --cs {cs} --voltage {voltage} --ac {ac}"
    )

    # input_column threads through: rlnDeconvTomoName if we deconv, else rlnTomoName (Warp already
    # deconvolved -- deconv OFF by default, params.isonet_deconv).
    input_col = "rlnTomoName"
    if params.isonet_deconv:
        isonet(f"isonet.py deconv --star_file {prep} --output_dir deconv")
        input_col = "rlnDeconvTomoName"

    # 3. make_mask — focus training on specimen regions.
    isonet(f"isonet.py make_mask --star_file {prep} --input_column {input_col} --output_dir mask")

    # 4. refine — the training. Writes .pt checkpoint(s) into isonet_maps/.
    # IsoNet `refine` rejects --method auto when the prep star carries BOTH the full volume and
    # even/odd halves (it can't tell self-supervised isonet2 from noise2noise isonet2-n2n). Train
    # always stages even/odd, so 'auto' resolves to isonet2-n2n (missing-wedge + denoising), matching
    # the isonet_method field doc; an explicitly chosen method is passed through unchanged.
    refine_method = params.isonet_method.value
    if params.isonet_method == IsoNetRefineMethod.AUTO:
        refine_method = IsoNetRefineMethod.ISONET2_N2N.value
    isonet(
        f"isonet.py refine --star_file {prep} --output_dir isonet_maps "
        f"--method {refine_method} --input_column {input_col}"
    )

    model_dir = job_dir / "isonet_maps"
    pts = list(model_dir.glob("**/*.pt"))  # ISONET-ASSUMPTION: refine emits .pt under output_dir
    if not pts:
        raise RuntimeError(f"IsoNet refine produced no .pt model under {model_dir}")
    newest = max(pts, key=lambda p: p.stat().st_mtime)
    print(f"[ISONET] refine produced {len(pts)} checkpoint(s); newest={newest.name}", flush=True)

    # 5. Archive to the model slot (denoising_model.tar.gz; inner 'isonet_maps/').
    run_command("tar -czf denoising_model.tar.gz isonet_maps", cwd=job_dir)


def resolve_tomo_path(project_root: Path, star_path: Path, relative_path: str) -> Path:
    path_obj = Path(relative_path)
    if path_obj.is_absolute():
        return path_obj
    p1 = project_root / relative_path
    if p1.exists():
        return p1
    p2 = star_path.parent / relative_path
    if p2.exists():
        return p2
    return p1


def derive_half_map_path(base_path: Path, half: str) -> Path:
    if base_path.parent.name == "reconstruction":
        return base_path.parent / half / base_path.name
    return base_path.parent / half / base_path.name


def validate_input_mrc(path: Path):
    """Checks an MRC file for NaNs, Infs, or Zero Variance."""
    if not HAS_DEPS:
        return

    print(f"[VALIDATION] Checking input MRC: {path.name}...", flush=True)
    try:
        with mrcfile.mmap(str(path), mode="r", permissive=True) as mrc:
            data = mrc.data
            d_min, d_max = float(data.min()), float(data.max())
            d_mean, d_std = float(data.mean()), float(data.std())

            if np.isnan(d_mean) or np.isnan(d_std):
                raise ValueError(f"Input file {path.name} contains NaNs (Mean/Std is NaN).")

            if np.isinf(d_mean) or np.isinf(d_std):
                raise ValueError(f"Input file {path.name} contains Infs.")

            if d_std < 1e-6:
                raise ValueError(f"Input file {path.name} is flat (StdDev < 1e-6). Check reconstruction.")

            print(f"    -> OK. Range=[{d_min:.2f}, {d_max:.2f}] Mean={d_mean:.2f} Std={d_std:.2f}", flush=True)

    except Exception as e:
        print(f"[FATAL] Validation failed for {path}: {e}", file=sys.stderr)
        raise e


def validate_extracted_data(train_data_path: Path):
    """Loads the extracted dataset and checks for corruption."""
    if not HAS_DEPS:
        return

    print(f"[VALIDATION] verifying extracted patches in {train_data_path}...", flush=True)
    try:
        dm = CryoCARE_DataModule()
        dm.load(str(train_data_path))

        dataset = dm.train_dataset
        if len(dataset) == 0:
            raise ValueError("Extracted dataset is empty!")

        # Check calculated stats (this is what caused your IndexError previously)
        print(f"    -> Dataset Stats: Mean={dm.mean:.4f}, Std={dm.std:.4f}", flush=True)

        if np.isnan(dm.mean) or np.isnan(dm.std):
            raise ValueError("Calculated Normalization Stats are NaN! Data is corrupt.")

        # Randomly sample 20 patches to check for NaNs inside the arrays
        print("    -> Sampling 20 random patches for NaN check...", flush=True)
        indices = np.random.choice(len(dataset), size=min(20, len(dataset)), replace=False)

        for i in indices:
            # __getitem__ returns (sample, target) or (x, y)
            sample_tuple = dataset[i]
            # Handle variable return types just in case
            sample_data = sample_tuple[0] if isinstance(sample_tuple, tuple) else sample_tuple

            if np.isnan(sample_data).any():
                raise ValueError(f"Patch {i} contains NaNs!")
            if np.isinf(sample_data).any():
                raise ValueError(f"Patch {i} contains Infs!")

        print("    -> Patches look clean.", flush=True)

    except Exception as e:
        print(f"[FATAL] Post-extraction validation failed: {e}", file=sys.stderr)
        raise e


def main():
    print("Python", sys.version, flush=True)
    print("--- SLURM JOB START ---", flush=True)

    try:
        (_project_state, params, local_params_data, job_dir, project_path, _job_type) = get_driver_context(
            DenoiseTrainParams
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

        if not input_star.exists():
            raise FileNotFoundError(f"Input STAR file not found: {input_star}")

        # Training is a single global reduction for both methods (one model from many tomograms);
        # only the tool chain differs. cryoCARE = extract even/odd patches -> train; IsoNet =
        # prepare_star -> [deconv] -> make_mask -> refine.
        if params.denoise_method == DenoiseMethod.ISONET:
            run_isonet_train(params, paths, job_dir, project_path, additional_binds)
            success_file.touch()
            print("--- SLURM JOB END (Exit Code: 0) ---", flush=True)
            sys.exit(0)

        tomo_df = starfile.read(input_star)
        if isinstance(tomo_df, dict):
            tomo_df = next(iter(tomo_df.values()))

        target_even = []
        target_odd = []
        filter_str = params.tomograms_for_training

        # Column resolution
        col_name = "rlnTomoReconstructedTomogram"
        if col_name not in tomo_df.columns:
            possible_cols = [c for c in tomo_df.columns if "Name" in c or "Tomogram" in c]
            col_name = possible_cols[0] if possible_cols else None

        if not col_name:
            raise ValueError("Could not find tomogram filename column in STAR file.")

        # Prefer the explicit half-map columns the per-TS reconstruct now emits; fall back to
        # deriving reconstruction/{even,odd}/<name> for older projects that lack them.
        has_half_cols = (
            "rlnTomoReconstructedTomogramHalf1" in tomo_df.columns
            and "rlnTomoReconstructedTomogramHalf2" in tomo_df.columns
        )

        found_count = 0
        for _, row in tomo_df.iterrows():
            raw_path = str(row[col_name])
            if filter_str not in raw_path:
                continue
            if has_half_cols:
                even = resolve_tomo_path(project_path, input_star, str(row["rlnTomoReconstructedTomogramHalf1"]))
                odd = resolve_tomo_path(project_path, input_star, str(row["rlnTomoReconstructedTomogramHalf2"]))
            else:
                full_path = resolve_tomo_path(project_path, input_star, raw_path)
                even = derive_half_map_path(full_path, "even")
                odd = derive_half_map_path(full_path, "odd")

            if even.exists() and odd.exists():
                # --- VALIDATION PHASE 1: INPUT FILES ---
                validate_input_mrc(even)
                validate_input_mrc(odd)

                target_even.append(str(even))
                target_odd.append(str(odd))
                found_count += 1
            else:
                print(f"[WARN] Missing halves for {Path(raw_path).name}", flush=True)

        if found_count == 0:
            raise ValueError(f"No valid tomograms found for filter '{filter_str}'")

        container_service = get_container_service()

        # ==========================================
        # CONFIGURATION & EXTRACTION
        # ==========================================

        # --- FIX: Safe normalization logic ---
        # 1. Total available patches
        total_extracted_patches = found_count * params.number_training_subvolumes

        # 2. Training set size (CryoCARE reserves 10% for validation by default)
        split_ratio = 0.9
        training_set_size = int(total_extracted_patches * split_ratio)

        # 3. Clamp normalization samples to Training Set Size to prevent IndexError
        safe_norm_samples = min(2000, training_set_size)

        print(
            f"[DRIVER] Config: Total Patches={total_extracted_patches} | "
            f"Train Set={training_set_size} | Norm Samples={safe_norm_samples}"
        )

        config_json_path = job_dir / "train_config.json"

        # Safe LR
        safe_lr = 0.00001

        train_config = {
            "even": target_even,
            "odd": target_odd,
            "patch_shape": [params.subvolume_dimensions] * 3,
            "num_slices": params.number_training_subvolumes,
            "split": split_ratio,
            "tilt_axis": "Y",
            "n_normalization_samples": int(safe_norm_samples),
            "path": str(job_dir / "train_data"),
            "train_data": str(job_dir / "train_data"),
            "model_name": "denoising_model",
            "epochs": 10,
            "steps_per_epoch": 200,
            "batch_size": 16,
            "unet_kern_size": 3,
            "unet_n_depth": 3,
            "unet_n_first": 16,
            "learning_rate": safe_lr,
            "gpu_id": 0,
        }

        with open(config_json_path, "w") as f:
            json.dump(train_config, f, indent=4)

        print("[DRIVER] Extracting training data...", flush=True)
        extract_cmd = f"cryoCARE_extract_train_data.py --conf {config_json_path.name}"

        wrapped_extract = container_service.wrap_command_for_tool(
            command=extract_cmd, cwd=job_dir, tool_name="cryocare", additional_binds=additional_binds
        )
        run_command(wrapped_extract, cwd=job_dir)

        # ==========================================
        # VALIDATION PHASE 2: EXTRACTED PATCHES
        # ==========================================
        validate_extracted_data(job_dir / "train_data")

        # ==========================================
        # TRAINING
        # ==========================================
        print("[DRIVER] Training model...", flush=True)
        train_cmd = f"cryoCARE_train.py --conf {config_json_path.name}"

        wrapped_train = container_service.wrap_command_for_tool(
            command=train_cmd, cwd=job_dir, tool_name="cryocare", additional_binds=additional_binds
        )
        run_command(wrapped_train, cwd=job_dir)

        # ==========================================
        # ARCHIVING
        # ==========================================
        print("[DRIVER] Archiving model...", flush=True)

        # The model is output to {path}/{model_name} as defined in config.
        # Here: job_dir/train_data/denoising_model
        model_output_dir = job_dir / "train_data" / "denoising_model"

        if model_output_dir.exists():
            # We want to create the tarball in the job_dir (one level up from train_data)
            # We use -C to change directory to train_data so the tarball structure starts at 'denoising_model'
            # (not train_data/denoising_model)
            tar_cmd = "tar -czf ../denoising_model.tar.gz denoising_model"
            run_command(tar_cmd, cwd=job_dir / "train_data")
        else:
            print(f"[ERROR] Model directory not found at expected path: {model_output_dir}", file=sys.stderr)
            raise RuntimeError("Training failed to produce a model directory.")

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
