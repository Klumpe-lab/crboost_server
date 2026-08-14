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

No per-task staging isolation on purpose (census #18): pytom takes explicit
per-tomogram args, outputs are name-keyed, so tasks cannot collide. The task
reads its inputs from the manifest snapshot, not the drive-time resolver
(census #20): all array tasks of one submission must see identical,
already-validated template/mask/star inputs even if project state changes
mid-array.

Tomograms are 1:1 with tilt-series in v1 (Tomogram.tilt_series_id == ts_id), so
the manifest keys off ts_names / tilt_series_ids() directly.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
markers all live in ArrayDriver; this file is the template-match-specific hooks.
"""

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import starfile

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import ArrayDriver, ArrayResults, read_manifest
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input
from services.configs.starfile_service import StarfileService
from services.job_models import TemplateMatchPytomParams


# TEMPORARY: Use pytom 0.10-style text file inputs instead of --relion5-tomograms-star.
# Set to True to replicate GT pipeline behavior for score comparison.
LEGACY_TEXT_INPUT = True


# ----------------------------------------------------------------------
# STAR helpers (shared)
# ----------------------------------------------------------------------


def _get_df_from_star(path: Path) -> pd.DataFrame:
    """First DataFrame block regardless of name — needed for per-tilt stars,
    whose single block is named after the TS, not 'global'."""
    d = starfile.read(path, always_dict=True)
    for v in d.values():
        if isinstance(v, pd.DataFrame):
            return v
    raise ValueError(f"No dataframe blocks found in {path}")


def read_global_block(path: Path) -> pd.DataFrame:
    """The 'global' block of a pipeline-written STAR (census #21: the named
    block, via StarfileService, is the canonical read path for enumeration)."""
    star_data = StarfileService().read(path)
    df = star_data.get("global")
    if df is None:
        raise ValueError(f"No 'global' block in {path} (blocks: {list(star_data.keys())})")
    return df


def _resolve_star_path(base_dir: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()


def generate_legacy_text_files(tiltseries_global_star: Path, output_dir: Path) -> dict[str, dict[str, Path]]:
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

    result: dict[str, dict[str, Path]] = {}
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


def make_pytom_tomograms_star(*, tomograms_star: Path, tiltseries_global_star: Path, out_star: Path) -> Path:
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


def get_gpu_split(requested_split: str) -> list[str]:
    if requested_split in ["auto", "None", ""]:
        return ["2", "2", "1"]
    return requested_split.split(":")


def resolve_tomogram_path(raw_path: str, *, tomograms_star: Path, project_root: Path) -> Path:
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


def build_pytom_base_cmd(
    params: TemplateMatchPytomParams,
    state,
    template_file: Path,
    mask_file: Path,
    tm_results_dir: Path,
    gpu_ids: list[str],
    angle_list_file: Path | None = None,
) -> ToolCommand:
    """The per-task base command before per-tomogram args are appended.

    Symmetry routing:
      - `angle_list_file` provided → `--angular-search <file>` (used for D/T/O/I
        point groups; the file contains one ZXZ Euler triple per line in
        radians and was generated at supervisor start by services.templating
        .angle_lists.generate_asymmetric_unit_angles).
      - Cn (n>=2) → `--angular-search <float>` plus `--z-axis-rotational-symmetry N`
        (PyTOM's dedicated flag, simpler than rolling our own angle list).
      - C1 → `--angular-search <float>` only.
    """
    base_cmd = (
        ToolCommand("pytom_match_template.py")
        .opt_path("-t", template_file, quote=False)
        .opt_path("-d", tm_results_dir, quote=False)
        .opt_path("-m", mask_file, quote=False)
        .opt("--voltage", state.microscope.acceleration_voltage_kv)
        .opt("--spherical-aberration", state.microscope.spherical_aberration_mm)
        .opt("--amplitude-contrast", state.microscope.amplitude_contrast)
        .flag("--per-tilt-weighting")
        .opt("--log", "debug")
        .raw(" ".join(["-g", *gpu_ids]))
    )

    sym = str(params.symmetry) if params.symmetry else "C1"
    if angle_list_file is not None:
        base_cmd.opt_path("--angular-search", angle_list_file, quote=False)
    else:
        base_cmd.opt("--angular-search", params.angular_search)
        if sym != "C1" and sym.startswith("C"):
            base_cmd.opt("--z-axis-rotational-symmetry", sym[1:])

    if params.gpu_split != "None":
        base_cmd.raw(" ".join(["-s", *get_gpu_split(params.gpu_split)]))
    if params.spectral_whitening:
        base_cmd.flag("--spectral-whitening")
    if getattr(params, "random_phase_correction", False):
        base_cmd.flag("--random-phase-correction")
    if params.non_spherical_mask:
        base_cmd.flag("--non-spherical-mask")
    if params.bandpass_filter != "None" and ":" in params.bandpass_filter:
        low, high = params.bandpass_filter.split(":")
        base_cmd.opt("--low-pass", low).opt("--high-pass", high)

    return base_cmd


class TemplateMatchPytomDriver(ArrayDriver):
    params_class = TemplateMatchPytomParams
    job_name = "template_match_pytom"
    driver_script = Path(__file__).resolve()

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[TemplateMatchPytomParams]) -> list[str]:
        input_star_tomos = ctx.paths["input_tomograms"]
        input_star_ts = ctx.paths["input_tiltseries"]
        require_producer_input(input_star_tomos, "Input tomograms STAR")
        require_producer_input(input_star_ts, "Input tiltseries STAR")

        template_file = ctx.paths.get("template_path")
        mask_file = ctx.paths.get("mask_path")
        if template_file is None or not Path(template_file).exists():
            raise FileNotFoundError(f"Template file missing: {template_file}")
        if mask_file is None or not Path(mask_file).exists():
            raise FileNotFoundError(f"Mask file missing: {mask_file}")

        tomo_df = read_global_block(input_star_tomos)
        required_cols = {"rlnTomoName", "rlnTomoReconstructedTomogram"}
        missing = required_cols - set(tomo_df.columns)
        if missing:
            raise KeyError(f"tomograms.star missing columns {missing}. Have: {list(tomo_df.columns)}")

        self._tomo_df = tomo_df
        return sorted(tomo_df["rlnTomoName"].astype(str).tolist())

    def pre_dispatch(self, ctx: DriverContext[TemplateMatchPytomParams], items: list[str]) -> None:
        job_dir = ctx.job_dir
        input_star_ts = ctx.paths["input_tiltseries"]

        tm_results_dir = job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        # Prepare per-tomogram inputs ONCE so tasks don't each re-parse STARs.
        if LEGACY_TEXT_INPUT:
            self.log("LEGACY MODE: generating text files for pytom 0.10")
            generate_legacy_text_files(tiltseries_global_star=input_star_ts, output_dir=job_dir)
        else:
            patched_tomos = make_pytom_tomograms_star(
                tomograms_star=ctx.paths["input_tomograms"],
                tiltseries_global_star=input_star_ts,
                out_star=job_dir / "tomograms_for_pytom.star",
            )
            self.log(f"Patched tomograms STAR for PyTOM: {patched_tomos}")

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

        # Non-Cn symmetries need a custom angle list — PyTOM has no flag for
        # D/T/O/I, only the dedicated --z-axis-rotational-symmetry for Cn.
        # Generate once at supervisor start so all array tasks reuse the same
        # file; tasks rebuild build_pytom_base_cmd per-tomo but the angle file
        # is shared.
        self._angle_list_path = None
        sym = str(ctx.params.symmetry) if ctx.params.symmetry else "C1"
        from services.templating.angle_lists import (
            needs_angle_list,
            generate_asymmetric_unit_angles,
            write_angle_list_file,
            expected_angle_count,
        )

        if needs_angle_list(sym):
            try:
                inc_deg = float(ctx.params.angular_search)
            except (TypeError, ValueError):
                inc_deg = 12.0
            angles = generate_asymmetric_unit_angles(sym, inc_deg)
            self._angle_list_path = job_dir / f"angles_{sym}.txt"
            write_angle_list_file(angles, self._angle_list_path)
            est = expected_angle_count(sym, inc_deg)
            self.log(f"symmetry={sym}: wrote {len(angles)} angles (estimate {est}) → {self._angle_list_path}")

    def manifest_extras(self, ctx: DriverContext[TemplateMatchPytomParams], items: list[str]) -> dict:
        raw_tomo_paths: dict[str, str] = {}
        for _, row in self._tomo_df.iterrows():
            raw_tomo_paths[str(row["rlnTomoName"])] = str(row["rlnTomoReconstructedTomogram"])

        return {
            "input_tomograms_star": str(ctx.paths["input_tomograms"]),
            "raw_tomo_paths": raw_tomo_paths,
            "legacy_text_input": LEGACY_TEXT_INPUT,
            "patched_tomograms_star": None if LEGACY_TEXT_INPUT else str(ctx.job_dir / "tomograms_for_pytom.star"),
            "template_path": str(ctx.paths["template_path"]),
            "mask_path": str(ctx.paths["mask_path"]),
            "angle_list_path": str(self._angle_list_path) if self._angle_list_path else None,
        }

    def aggregate(self, ctx: DriverContext[TemplateMatchPytomParams], results: ArrayResults) -> None:
        # Census #19 (maintainer decision): never edit primary files — excluded
        # tomograms stay as rows in the output star; downstream consults the
        # registry/project state for mutedness, not row absence.
        output_tomograms = ctx.job_dir / "tomograms.star"
        shutil.copy2(ctx.paths["input_tomograms"], output_tomograms)
        self.log(f"Copied tomograms.star to {output_tomograms}")

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[TemplateMatchPytomParams], item: str) -> bool:
        # Covers the crash-after-output-before-status window for the most
        # expensive per-item tool in the pipeline (census #23).
        out_scores = scores_mrc_path(ctx.job_dir, item)
        if out_scores.exists() and out_scores.stat().st_size > 0:
            self.log(f"Scores already exist, skipping: {out_scores}")
            return True
        return False

    def stage(self, ctx: DriverContext[TemplateMatchPytomParams], item: str):
        manifest = read_manifest(ctx.job_dir)

        raw_tomo_paths = manifest.get("raw_tomo_paths") or {}
        raw_tomo_path = raw_tomo_paths.get(item)
        if not raw_tomo_path:
            raise KeyError(f"manifest missing raw_tomo_paths['{item}']")

        input_star_tomos = Path(manifest["input_tomograms_star"])

        tm_results_dir = ctx.job_dir / "tmResults"
        tm_results_dir.mkdir(exist_ok=True)

        tomo_path = resolve_tomogram_path(raw_tomo_path, tomograms_star=input_star_tomos, project_root=ctx.project_path)
        if not tomo_path.exists():
            raise FileNotFoundError(
                f"Tomogram file does not exist for {item}.\n  STAR entry: {raw_tomo_path}\n  Resolved:   {tomo_path}"
            )

        local_tomo = tm_results_dir / f"{item}{tomo_path.suffix or '.mrc'}"
        if not local_tomo.exists():
            os.symlink(tomo_path.resolve(), local_tomo)

        return {
            "local_tomo": local_tomo,
            "tm_results_dir": tm_results_dir,
            "template_file": Path(manifest["template_path"]),
            "mask_file": Path(manifest["mask_path"]),
            "use_legacy": bool(manifest.get("legacy_text_input", True)),
            "patched_tomograms_star": manifest.get("patched_tomograms_star"),
            "angle_list_path": manifest.get("angle_list_path"),
        }

    def build_command(self, ctx: DriverContext[TemplateMatchPytomParams], item: str, staged) -> ToolCommand:
        gpu_ids = os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")
        angle_list_str = staged["angle_list_path"]
        cmd = build_pytom_base_cmd(
            params=ctx.params,
            state=ctx.state,
            template_file=staged["template_file"],
            mask_file=staged["mask_file"],
            tm_results_dir=staged["tm_results_dir"],
            gpu_ids=gpu_ids,
            angle_list_file=Path(angle_list_str) if angle_list_str else None,
        )

        cmd.opt_path("-v", staged["local_tomo"], quote=False)
        if staged["use_legacy"]:
            cmd.opt_path("--tilt-angles", ctx.job_dir / "tiltAngleFiles" / f"{item}.tlt", quote=False)
            cmd.opt_path("--defocus", ctx.job_dir / "defocusFiles" / f"{item}.txt", quote=False)
            cmd.opt_path("--dose-accumulation", ctx.job_dir / "doseFiles" / f"{item}.txt", quote=False)
        else:
            if not staged["patched_tomograms_star"]:
                raise RuntimeError("Non-legacy mode requires patched_tomograms_star in manifest")
            cmd.opt_path("--relion5-tomograms-star", staged["patched_tomograms_star"], quote=False)

        return cmd

    def task_binds(self, ctx: DriverContext[TemplateMatchPytomParams], item: str, staged) -> list:
        return [staged["template_file"].parent.resolve(), staged["mask_file"].parent.resolve()]

    def verify_outputs(self, ctx: DriverContext[TemplateMatchPytomParams], item: str, staged) -> None:
        out_scores = scores_mrc_path(ctx.job_dir, item)
        if not out_scores.exists():
            raise FileNotFoundError(f"pytom_match_template reported success but expected output missing: {out_scores}")


if __name__ == "__main__":
    os.environ["TQDM_DISABLE"] = "1"
    TemplateMatchPytomDriver().main()
