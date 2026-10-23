#!/usr/bin/env python3
"""
denoise_predict driver — supervisor + per-tomogram SLURM array task.

Symmetric with the other per-TS jobs (ts_reconstruct, ts_ctf, ...): one driver script
that dispatches on the SLURM_ARRAY_TASK_ID env var.

- Unset:  SUPERVISOR mode. Submitted by the orchestrator via the lightweight CPU-only
          supervisor sbatch. Enumerates tomograms from the reconstruct tomograms.star,
          pre-skips those filtered out by denoising_tomo_name, submits a child SLURM array
          (one tomogram per task) with PER-TASK GPU resources, polls squeue until the array
          drains, then aggregates the denoised tomograms.star. Its exit code becomes
          RELION_JOB_EXIT_{SUCCESS,FAILURE} (the array tasks never write one).

- Set:    TASK mode. One tomogram per array index. Reads the manifest, resolves this
          tomogram's even/odd halves, idempotently skips if the denoised MRC already exists,
          otherwise runs cryoCARE_predict.py (or isonet.py predict, or easymode denoise), and atomically writes
          .task_status/{ts}.{ok|fail}.

Denoise method (cryoCARE | IsoNet | easymode) is selected per-job via params.denoise_method and only
affects the per-task work (the `execute` override dispatches the IsoNet and easymode paths). easymode
runs its pretrained denoisers on the full reconstruction, with no model from a train job: at 10 Å, the
voxel size they were trained on, then put back on the reconstruction's grid like the other two.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
code all live in ArrayDriver; this file is the denoise-specific hooks.
"""

import json
import os
import shlex
import shutil
import sys
import tarfile
from pathlib import Path

import starfile
from pydantic import BaseModel

from drivers.array_job_base import ArrayDriver, ArrayResults, read_manifest, write_skip_status, STATUS_DIR_NAME
from drivers.driver_base import DriverContext, ToolCommand, print_cmd_only, run_tool, require_producer_input
from drivers.isonet_star import annotate_prep_star, log_facts, read_tomo_facts
from drivers.segment import OFFLINE_PROXY
from services.job_models import DenoisePredictParams
from services.models_base import DenoiseMethod
from services.segmentation.easymode import TOOL_NAME as EASYMODE_TOOL, easymode_cache_dir
from services.segmentation.grid import map_back, processing_shape, write_resampled
from services.tilt_series import DenoisePredictTomogramOutput, get_registry_for

# Column in the reconstruct tomograms.star that holds the full reconstructed tomogram path.
TOMO_COL = "rlnTomoReconstructedTomogram"
# Stale-after-denoising half-map columns; dropped from the denoised output STAR.
HALF_COLS = ("rlnTomoReconstructedTomogramHalf1", "rlnTomoReconstructedTomogramHalf2")


# ----------------------------------------------------------------------
# Shared helpers
# ----------------------------------------------------------------------


def _global_block(data):
    """starfile.read returns a DataFrame (single unnamed block) or a dict of blocks.
    Return (block_name_or_None, df) preferring the RELION 'global' block."""
    if isinstance(data, dict):
        if "global" in data:
            return "global", data["global"]
        name = next(iter(data))
        return name, data[name]
    return None, data


def read_tomo_map(input_star: Path) -> tuple[list[str], dict[str, str]]:
    """Return (sorted ts_names, {ts_name: reconstructed-tomogram basename}) from the
    reconstruct tomograms.star. The array maps index N -> ts_names[N]; the basename map
    lets each task resolve its even/odd halves without re-parsing the whole STAR.

    Strict 'global'-block read: a star without one is malformed input, not something
    to guess a block for.
    """
    data = starfile.read(input_star, always_dict=True)
    df = data.get("global")
    if df is None:
        raise ValueError(f"No 'global' block in {input_star} (blocks: {list(data.keys())})")
    ts_names: list[str] = []
    tomo_basenames: dict[str, str] = {}
    for _, row in df.iterrows():
        ts = str(row["rlnTomoName"])
        tomo_basenames[ts] = Path(str(row[TOMO_COL])).name
        ts_names.append(ts)
    return sorted(ts_names), tomo_basenames


def prepare_isonet_model(job_dir: Path, model_tar: Path) -> Path:
    """Untar the IsoNet model archive (once, in the supervisor) and return the newest .pt.

    Also enforces the method contract: an IsoNet model tar has an inner 'isonet_maps/' dir; a
    cryoCARE tar has 'denoising_model/'. A mismatch here means denoise_method=IsoNet was pointed at
    a cryoCARE-trained model (or vice-versa) -- fail loud rather than feed the wrong tool."""
    dest = job_dir / ".isonet_model"
    dest.mkdir(exist_ok=True)
    with tarfile.open(model_tar, "r:gz") as t:
        t.extractall(dest)
    maps = dest / "isonet_maps"
    if not maps.exists():
        raise RuntimeError(
            f"Model archive {model_tar} is not an IsoNet model (no isonet_maps/ inside; found "
            f"{[p.name for p in dest.iterdir()]}). denoise_method=IsoNet needs an IsoNet-trained model."
        )
    pts = sorted(maps.glob("**/*.pt"), key=lambda p: p.stat().st_mtime)
    if not pts:
        raise RuntimeError(f"No .pt checkpoint found under {maps}")
    return pts[-1]


def aggregate_output_star(
    input_star: Path,
    job_dir: Path,
    output_dir: Path,
    project_path: Path,
    ok_ts_names: list[str],
    tomo_basenames: dict[str, str],
) -> None:
    """Build job_dir/tomograms.star from the reconstruct STAR, keeping only rows whose tomogram
    was denoised (.ok), with TOMO_COL repointed at output_dir/<basename> and the now-stale
    half-map columns dropped. Downstream (template matching) reads TOMO_COL from this STAR."""
    import pandas as pd

    _, df = _global_block(starfile.read(input_star, always_dict=True))
    ok_set = set(ok_ts_names)
    keep_rows = []
    for _, row in df.iterrows():
        ts = str(row["rlnTomoName"])
        if ts not in ok_set:
            continue
        denoised = output_dir / tomo_basenames[ts]
        new_row = row.copy()
        try:
            new_row[TOMO_COL] = str(denoised.relative_to(project_path))
        except ValueError:
            new_row[TOMO_COL] = str(denoised)
        keep_rows.append(new_row)

    out_df = pd.DataFrame(keep_rows)
    for col in HALF_COLS:
        if col in out_df.columns:
            out_df = out_df.drop(columns=col)

    out_path = job_dir / "tomograms.star"
    # The block name is a pipeline contract, never inherited from the input.
    starfile.write({"global": out_df}, out_path, overwrite=True)
    print(f"[SUPERVISOR] Wrote denoised STAR with {len(out_df)} tomogram(s): {out_path}", flush=True)


def stamp_denoise_registry(
    project_path: Path,
    job_dir: Path,
    instance_id: str,
    params: DenoisePredictParams,
    ok_ts_names: list[str],
    tomo_basenames: dict[str, str],
    output_dir: Path,
    model_path: Path,
) -> None:
    """Record each denoised tomogram in the registry (denoise is the last tomogram-scoped
    preprocessing step). Done here in the single-threaded supervisor — never in the parallel
    array tasks, which would race on the shared registry JSON.

    Fails loud: downstream jobs read from the registry, so a missed stamp is stale data.
    Any failure here fails the job; the denoised MRCs stay on disk and a re-run skips the
    already-done compute, so only this cheap recording step repeats."""
    registry = get_registry_for(project_path)
    if not registry.tilt_series_ids():
        raise RuntimeError(
            f"TiltSeries registry is empty for project {project_path}. "
            f"Reload the project in the UI to backfill the registry from mdocs, then restart this job."
        )
    stamped = 0
    for ts in ok_ts_names:
        basename = tomo_basenames.get(ts)
        if not basename:
            raise ValueError(f"No tomogram basename recorded for denoised TS '{ts}'")
        registry.attach_tomogram_output(
            ts,
            DenoisePredictTomogramOutput(
                job_instance_id=instance_id,
                job_dir=job_dir,
                denoised_mrc=output_dir / basename,
                denoise_method=params.denoise_method.value,
                model_path=model_path,
            ),
        )
        stamped += 1
    registry.save()
    print(f"[SUPERVISOR] Stamped denoise output on {stamped} registry tomogram(s)", flush=True)


def _apply_inherited_method(project_state, params, tag: str) -> None:
    """Override denoise_method + isonet_deconv from the denoisetrain job that produced the
    model, so predict always runs what the model was trained as (never a stale/default that
    disagrees). Best-effort; prepare_isonet_model's tar-structure check stays the final guard.

    Both fields are USER_PARAMS, which AbstractJobParams.__setattr__ freezes on a RUNNING job --
    and a driver always sees its job as RUNNING. This is a run-time override, not a user edit,
    so it writes through BaseModel.__setattr__ past that guard."""
    try:
        method, deconv = params.inherited_from_train(project_state)
    except Exception as e:
        print(f"{tag} WARN: could not inherit denoise method from train: {e}", file=sys.stderr, flush=True)
        return
    if method is not None and method != params.denoise_method:
        print(
            f"{tag} Inheriting denoise_method={method.value} from denoise-train (was {params.denoise_method.value})",
            flush=True,
        )
        BaseModel.__setattr__(params, "denoise_method", method)
    if deconv is not None:
        BaseModel.__setattr__(params, "isonet_deconv", deconv)


class DenoisePredictDriver(ArrayDriver):
    params_class = DenoisePredictParams
    job_name = "denoise_predict"
    driver_script = Path(__file__).resolve()

    def post_bootstrap(self, ctx: DriverContext[DenoisePredictParams]) -> None:
        _apply_inherited_method(ctx.state, ctx.params, self._prefix)

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[DenoisePredictParams]) -> list[str]:
        if ctx.params.uses_trained_model:
            require_producer_input(ctx.paths["model_path"], "Denoise model archive")
        require_producer_input(ctx.paths["input_star"], "Input tomograms STAR")

        ts_names, tomo_basenames = read_tomo_map(ctx.paths["input_star"])
        if not ts_names:
            raise ValueError(f"No tomograms found in input STAR: {ctx.paths['input_star']}")
        self.log(f"{len(ts_names)} tomogram(s) in input STAR (method={ctx.params.denoise_method.value})")
        self._tomo_basenames = tomo_basenames
        return ts_names

    def pre_dispatch(self, ctx: DriverContext[DenoisePredictParams], items: list[str]) -> None:
        status_dir = ctx.job_dir / STATUS_DIR_NAME
        # Pre-skip tomograms filtered out by denoising_tomo_name so they never spawn a GPU task.
        flt = ctx.params.denoising_tomo_name
        if flt:
            skipped = [ts for ts in items if flt not in self._tomo_basenames[ts]]
            for ts in skipped:
                write_skip_status(status_dir, ts, reason=f"filtered out by denoising_tomo_name={flt!r}")
            self.log(f"{len(skipped)} tomogram(s) pre-skipped by filter; {len(items) - len(skipped)} to run")

        # For IsoNet, untar the model ONCE here (shared NFS) and stash the .pt for the tasks.
        # cryoCARE tasks read the tar directly (cryoCARE_predict accepts the .tar.gz as "path").
        self._isonet_model_pt = None
        if ctx.params.denoise_method == DenoiseMethod.ISONET:
            self._isonet_model_pt = prepare_isonet_model(ctx.job_dir, ctx.paths["model_path"])
            self.log(f"Staged IsoNet model: {self._isonet_model_pt}")

        # easymode reads its weights from the frozen cache; the file is checked here so a missing
        # denoiser fails the job before any GPU task starts.
        self._easymode_cache = self._easymode_weights = None
        if ctx.params.denoise_method == DenoiseMethod.EASYMODE:
            self._easymode_cache = easymode_cache_dir()
            self._easymode_weights = self._easymode_cache / "models" / f"{ctx.params.easymode_denoiser.value}_direct.h5"
            if not self._easymode_weights.is_file():
                raise FileNotFoundError(
                    f"easymode's '{ctx.params.easymode_denoiser.value}' denoiser is not in the cache: "
                    f"{self._easymode_weights}"
                )
            p = ctx.params
            self.log(f"easymode {p.easymode_denoiser.value}, TTA {p.easymode_tta}, {p.easymode_iter} iteration(s)")

    def manifest_extras(self, ctx: DriverContext[DenoisePredictParams], items: list[str]) -> dict:
        extras = {"tomo_basenames": self._tomo_basenames}
        if self._isonet_model_pt is not None:
            extras["isonet_model_pt"] = str(self._isonet_model_pt)
        if self._easymode_cache is not None:
            extras["easymode_cache"] = str(self._easymode_cache)
        return extras

    def aggregate(self, ctx: DriverContext[DenoisePredictParams], results: ArrayResults) -> None:
        if not results.ok:
            self.log("WARN: no tomograms denoised (all filtered out?); writing empty STAR")
        aggregate_output_star(
            ctx.paths["input_star"],
            ctx.job_dir,
            ctx.paths["output_dir"],
            ctx.project_path,
            results.ok,
            self._tomo_basenames,
        )
        stamp_denoise_registry(
            ctx.project_path,
            ctx.job_dir,
            ctx.instance_id,
            ctx.params,
            results.ok,
            self._tomo_basenames,
            ctx.paths["output_dir"],
            self._easymode_weights or ctx.paths["model_path"],
        )

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[DenoisePredictParams], item: str) -> bool:
        manifest = read_manifest(ctx.job_dir)
        tomo_basename = manifest.get("tomo_basenames", {}).get(item)
        if not tomo_basename:
            raise ValueError(f"No tomogram basename in manifest for ts={item}")
        out_mrc = ctx.paths["output_dir"] / tomo_basename
        # Idempotency: skip tomograms already denoised on a previous attempt.
        if out_mrc.exists() and out_mrc.stat().st_size > 0:
            self.log(f"Denoised output already exists, skipping: {out_mrc}")
            return True
        return False

    def stage(self, ctx: DriverContext[DenoisePredictParams], item: str):
        manifest = read_manifest(ctx.job_dir)
        tomo_basename = manifest["tomo_basenames"][item]
        self.log(f"ts={item} tomo={tomo_basename}")

        reconstruct_base = ctx.paths["reconstruct_base"]
        output_dir = ctx.paths["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)

        staged = {
            "tomo_basename": tomo_basename,
            "even": reconstruct_base / "reconstruction" / "even" / tomo_basename,
            "odd": reconstruct_base / "reconstruction" / "odd" / tomo_basename,
            "full": reconstruct_base / "reconstruction" / tomo_basename,
            "out_mrc": output_dir / tomo_basename,
            "isonet_model_pt": manifest.get("isonet_model_pt"),
            "easymode_cache": manifest.get("easymode_cache"),
        }

        if ctx.params.denoise_method == DenoiseMethod.EASYMODE:
            if not staged["full"].exists():
                raise FileNotFoundError(f"Missing reconstruction for {tomo_basename}: {staged['full']}")
            if not staged["easymode_cache"]:
                raise RuntimeError("Manifest missing easymode_cache (the supervisor did not settle easymode's cache)")
        elif ctx.params.denoise_method == DenoiseMethod.ISONET:
            # The isonet2-n2n model predicts on the even/odd halves (averaged); an isonet2 model
            # uses the full (rlnTomoName). Stage all three, exactly as denoise_train did.
            for label in ("full", "even", "odd"):
                if not staged[label].exists():
                    raise FileNotFoundError(f"Missing {label} reconstruction for {tomo_basename}: {staged[label]}")
            if not staged["isonet_model_pt"]:
                raise RuntimeError("Manifest missing isonet_model_pt (supervisor did not stage the IsoNet model)")
        else:
            if not staged["even"].exists() or not staged["odd"].exists():
                raise FileNotFoundError(
                    f"Missing even/odd halves for {tomo_basename}: {staged['even']} / {staged['odd']}"
                )
        return staged

    def execute(self, ctx: DriverContext[DenoisePredictParams], item: str, staged) -> None:
        if ctx.params.denoise_method == DenoiseMethod.ISONET:
            self._run_isonet_predict(ctx, item, staged)
        elif ctx.params.denoise_method == DenoiseMethod.EASYMODE:
            self._run_easymode_denoise(ctx, item, staged)
        else:
            # Default path: build_command (cryoCARE) → run_tool with
            # tool_name=params.get_tool_name() (method-conditional → "cryocare").
            super().execute(ctx, item, staged)

    def build_command(self, ctx: DriverContext[DenoisePredictParams], item: str, staged) -> ToolCommand:
        """cryoCARE_predict.py treats `output` as a directory and writes output_dir/basename(even)
        inside it -- which equals output_dir/<tomo_basename>."""
        params = ctx.params
        base_tiles = (params.ntiles_z, params.ntiles_y, params.ntiles_x)
        n_tiles_z, n_tiles_y, n_tiles_x = self._calculate_memory_aware_tiles(
            staged["even"], base_tiles=base_tiles, max_tiles=(8, 8, 8)
        )
        self.log(f"Tiles: z={n_tiles_z} y={n_tiles_y} x={n_tiles_x}")
        cfg = {
            "path": str(ctx.paths["model_path"]),
            "even": str(staged["even"]),
            "odd": str(staged["odd"]),
            "n_tiles": [n_tiles_z, n_tiles_y, n_tiles_x],
            "output": str(ctx.paths["output_dir"]),
            "gpu_id": 0,
            "overwrite": True,
        }
        # predict_{idx}.json keyed by array index — each task writes its own.
        idx = os.environ["SLURM_ARRAY_TASK_ID"]
        cfg_name = f"predict_{idx}.json"
        with open(ctx.job_dir / cfg_name, "w") as f:
            json.dump(cfg, f, indent=4)
        # Env prefix rides in front of the executable — in-band, as the shell needs it.
        return ToolCommand("TF_FORCE_GPU_ALLOW_GROWTH=true TF_GPU_ALLOCATOR=cuda_malloc_async cryoCARE_predict.py").opt(
            "--conf", cfg_name
        )

    def _calculate_memory_aware_tiles(self, tomogram_path: Path, base_tiles=(4, 4, 4), max_tiles=(8, 8, 8)) -> tuple:
        """Calculate optimal tiling based on tomogram dimensions. Returns (n_tiles_z, n_tiles_y, n_tiles_x)."""
        try:
            import mrcfile

            with mrcfile.open(tomogram_path, "r") as mrc:
                dims = mrc.data.shape  # (z, y, x) for tomograms
            self.log(f"Tomogram dimensions: {dims}")
            tiles = list(base_tiles)
            for i, dim in enumerate(dims):
                if dim > 1000:
                    tiles[i] = min(tiles[i] * 2, max_tiles[i])
                elif dim > 2000:
                    tiles[i] = min(tiles[i] * 3, max_tiles[i])
            return tuple(tiles)
        except Exception as e:
            # Tiling is a performance heuristic, not correctness: fall back to
            # the configured base tiles rather than fail the tomogram.
            self.log(f"WARN: Could not read tomogram for tiling calculation: {e}")
            return base_tiles

    def _run_isonet_predict(self, ctx: DriverContext[DenoisePredictParams], item: str, staged) -> None:
        """Per-tomogram IsoNet prediction. Stage this tomo's full + even/odd reconstructions into a
        one-file-each dir and build the prep STAR with prepare_star --full --even --odd (full ->
        rlnTomoName, even/odd -> rlnTomoReconstructedTomogramHalf1/2), mirroring denoise_train. The
        model denoise_train produces is isonet2-n2n (noise2noise): predict reads the even/odd halves,
        denoises each, and averages -- an isonet2 (single-map) model instead reads rlnTomoName, so
        staging all three keeps predict correct for either method. [deconv], then predict into a
        per-task dir and move the single corrected MRC to the canonical denoised path (out_mrc).
        Predicting into an isolated per-task dir sidesteps IsoNet's output-filename convention --
        ISONET-ASSUMPTION: predict writes exactly one full-size .mrc."""
        job_dir = ctx.job_dir
        out_mrc = staged["out_mrc"]
        stage = job_dir / ".staging" / f"task_{item}"
        full_dir, even_dir, odd_dir, corrected = stage / "full", stage / "even", stage / "odd", stage / "corrected"
        for d in (full_dir, even_dir, odd_dir, corrected):
            d.mkdir(parents=True, exist_ok=True)
        # prepare_star matches full/even/odd by basename across the three dirs (as in denoise_train).
        for src, dst_dir in ((staged["full"], full_dir), (staged["even"], even_dir), (staged["odd"], odd_dir)):
            link = dst_dir / src.name
            if link.exists() or link.is_symlink():
                link.unlink()
            link.symlink_to(src.resolve())

        prep = stage / "isonet_prep.star"

        # tool_name stays the "isonet" literal in this IsoNet-only helper: the branch, not
        # params, is what makes it IsoNet here, and params.get_tool_name() would answer
        # "cryocare" if this were ever called off the ISONET branch.
        def isonet(cmd: ToolCommand):
            self.log(f"Command: {cmd}")
            run_tool(cmd, tool_name="isonet", cwd=job_dir, binds=ctx.additional_binds)

        isonet(
            ToolCommand("isonet.py prepare_star")
            .opt_path("--full", full_dir, quote=True)
            .opt_path("--even", even_dir, quote=True)
            .opt_path("--odd", odd_dir, quote=True)
            .opt_path("--star_name", prep, quote=True)
            .opt("--pixel_size", "auto")
        )
        # Defocus, tilt range and optics as measured, not prepare_star's defaults (see denoise_train).
        facts = read_tomo_facts(ctx.paths["input_star"], ctx.project_path, {staged["full"].name})
        annotate_prep_star(prep, facts)
        log_facts(facts, self.log)
        input_col = "rlnTomoName"
        if ctx.params.isonet_deconv:
            input_col = "rlnDeconvTomoName"
            if facts[staged["full"].name].warp_deconv is None:
                # Warp reconstructs with CTF phase flipping, so IsoNet must not flip again.
                isonet(
                    ToolCommand("isonet.py deconv")
                    .opt_path("--star_file", prep, quote=True)
                    .opt_path("--output_dir", stage / "deconv", quote=True)
                    .opt("--phaseflipped", True)
                )
        isonet(
            ToolCommand("isonet.py predict")
            .opt_path("--star_file", prep, quote=True)
            .opt_path("--model", Path(staged["isonet_model_pt"]), quote=True)
            .opt("--input_column", input_col)
            .opt_path("--output_dir", corrected, quote=True)
            # Warp's reconstructions are CTF-corrected already; a model trained with a CTF_mode
            # would otherwise phase-flip the input again (see denoise_train's refine call).
            .opt("--isCTFflipped", True)
        )

        mrcs = list(corrected.glob("*.mrc"))
        if not mrcs:
            raise FileNotFoundError(f"IsoNet predict produced no MRC in {corrected}")
        # predict may also drop small preview-slice MRCs (save_slices defaults True); the corrected
        # tomogram is by far the largest volume in the per-task dir.
        produced = max(mrcs, key=lambda p: p.stat().st_size)
        out_mrc.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(produced), str(out_mrc))
        # IsoNet writes its output with a 1 Å voxel size; carry the reconstruction's over so viewers
        # and downstream jobs reading the header see the real scale.
        import mrcfile

        with mrcfile.open(staged["full"], header_only=True, permissive=True) as src:
            apix = src.voxel_size
        with mrcfile.mmap(out_mrc, mode="r+") as dst:
            dst.voxel_size = apix
        self.log(f"IsoNet corrected -> {out_mrc} (voxel size {float(apix.x):g} Å)")

    def _run_easymode_denoise(self, ctx: DriverContext[DenoisePredictParams], item: str, staged) -> None:
        """Per-tomogram `easymode denoise`. The reconstruction goes into a directory of its own,
        resampled to 10 Å (spectrum cropped) when it is finer, as the Segment job gives it to easymode;
        the denoiser writes into another, and its volume is put back on the reconstruction's grid at
        the canonical denoised path (out_mrc). Each task has its own EASYMODE_CACHE_DIR, with the
        cache's models/ linked into it, and the offline proxy (both as drivers/segment.py explains)."""
        import mrcfile
        import numpy as np

        p = ctx.params
        full, out_mrc = staged["full"], staged["out_mrc"]
        stage = ctx.job_dir / ".staging" / f"task_{item}"
        in_dir, tool_dir, em_dir = stage / "in", stage / "out", stage / "easymode"
        for d in (in_dir, tool_dir, em_dir):
            d.mkdir(parents=True, exist_ok=True)
        # easymode skips outputs that exist, so what an interrupted attempt left must go.
        for old in tool_dir.glob("*.mrc"):
            old.unlink()
        cache = Path(staged["easymode_cache"])
        models = em_dir / "models"
        if models.is_symlink() or models.exists():
            models.unlink()
        models.symlink_to(cache / "models")

        with mrcfile.open(full, header_only=True, permissive=True) as m:
            h = m.header
            shape = (int(h.nz), int(h.ny), int(h.nx))
            apix = float(m.voxel_size.x)
        if apix <= 1.0:
            raise ValueError(f"{full.name}: header voxel size {apix:g} Å; easymode reads 1 Å or less as unset")
        grid = processing_shape(shape, apix)
        staged_in = in_dir / full.name
        if staged_in.is_symlink() or staged_in.exists():
            staged_in.unlink()
        if grid is None:
            staged_in.symlink_to(full.resolve())
            self.log(f"Denoising at the reconstruction's {apix:g} Å")
        elif print_cmd_only():
            self.log(f"Would resample {full.name} to 10 Å: {grid[2]} × {grid[1]} × {grid[0]}")
        else:
            self.log(f"Resampling {full.name} ({apix:g} Å) to 10 Å: {grid[2]} × {grid[1]} × {grid[0]}")
            write_resampled(full, staged_in, grid, tmp_dir=stage)

        env = " ".join(
            f"{name}={shlex.quote(str(value))}"
            for name, value in (
                ("EASYMODE_CACHE_DIR", em_dir),
                ("HF_HOME", em_dir / "hf"),
                ("https_proxy", OFFLINE_PROXY),
                ("HTTPS_PROXY", OFFLINE_PROXY),
            )
        )
        cmd = (
            ToolCommand(f"{env} easymode denoise")
            .opt_path("--data", in_dir, quote=True)
            .opt_path("--output", tool_dir, quote=True)
            .opt("--method", p.easymode_denoiser.value)
            .opt("--tta", p.easymode_tta)
            .opt("--iter", p.easymode_iter)
            .opt("--gpu", 0)
        )
        self.log(f"Command: {cmd}")
        run_tool(
            cmd,
            tool_name=EASYMODE_TOOL,
            cwd=ctx.job_dir,
            binds=[*ctx.additional_binds, full.resolve().parent, cache],
            label=f"{self.job_name} {item}",
        )
        if print_cmd_only():
            return

        mrcs = list(tool_dir.glob("*.mrc"))
        if len(mrcs) != 1:
            names = sorted(m.name for m in mrcs)
            raise FileNotFoundError(f"easymode denoise left {names} in {tool_dir}, expected one MRC")
        produced = mrcs[0]
        expected = grid or shape
        with mrcfile.mmap(produced, mode="r", permissive=True) as m:
            got = tuple(int(n) for n in m.data.shape)
            if got != tuple(expected):
                raise RuntimeError(f"easymode denoise wrote {produced.name} as {got} (z, y, x), expected {expected}")
            volume = np.array(m.data, dtype=np.float32)
        if grid is not None:
            volume = map_back(volume, shape)
        tmp = stage / f".tmp_{full.name}"
        with mrcfile.new(tmp, overwrite=True) as m:
            m.set_data(volume)
            m.voxel_size = apix
        out_mrc.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp), str(out_mrc))
        # The resampled copy and the tool's own volume are not needed once the denoised one is in place.
        produced.unlink()
        if not staged_in.is_symlink():
            staged_in.unlink()
        self.log(f"easymode {p.easymode_denoiser.value} -> {out_mrc} ({shape[2]} × {shape[1]} × {shape[0]})")

    def verify_outputs(self, ctx: DriverContext[DenoisePredictParams], item: str, staged) -> None:
        if not staged["out_mrc"].exists():
            raise FileNotFoundError(f"Prediction reported success but output missing: {staged['out_mrc']}")


if __name__ == "__main__":
    DenoisePredictDriver().main()
