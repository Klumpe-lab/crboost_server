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

Enumeration is from the TM output (one `*_job.json` per tomogram TM actually
processed), not the input star — the coordinate list follows what TM produced.
Interim per census #25: the star-minus-TM difference is reported LOUDLY so a
TM-partial run can't silently narrow extraction. Staging is one shared
job-local dir on purpose (census #26): pytom's job.json embeds output_dir and
tasks write distinct name-keyed files, so there is nothing to isolate. Note
the `target.exists(): continue` skip means a re-run never re-patches a changed
upstream file.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
markers all live in ArrayDriver; this file is the extraction-specific hooks.
"""

import json
import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import starfile

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import ArrayDriver, ArrayResults, read_manifest
from drivers.driver_base import DriverContext, ToolCommand
from services.subtomo_merge import write_optimisation_set
from services.job_models import CandidateExtractPytomParams, ExtractionCutoffMethod


# ----------------------------------------------------------------------
# Helpers (shared supervisor + task)
# ----------------------------------------------------------------------


def get_pixel_size_from_star(tomograms_star: Path) -> float:
    """Extract pixel size from tomograms.star metadata."""
    try:
        data = starfile.read(tomograms_star)
        if isinstance(data, dict):
            df = next(iter(data.values()))
        else:
            df = data
        ts_pixs = float(df["rlnTomoTiltSeriesPixelSize"].iloc[0])
        binning = float(df.get("rlnTomoTomogramBinning", pd.Series([1])).iloc[0])
        return ts_pixs * binning
    except Exception as e:
        print(f"[WARN] Could not read pixel size from {tomograms_star}: {e}")
        return None


def cleanup_tomo_names(candidates_star: Path, apix_fallback: float) -> int:
    """Remove the pixel size suffix from rlnTomoName in the merged candidates STAR.

    Raises on any failure (census #28, maintainer decision): a suffixed
    rlnTomoName silently breaks every downstream join (subtomo/refine see zero
    matches), which is exactly the wrong-but-plausible failure class the
    never-fail-silently policy targets.
    """
    data = starfile.read(candidates_star, always_dict=True)
    df = None
    for val in data.values():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            df = val
            break
    if df is None:
        raise ValueError(f"No rlnTomoName data block found in {candidates_star}")

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


def build_extract_base_cmd(params: CandidateExtractPytomParams, apix: float) -> ToolCommand:
    base_cmd = (
        ToolCommand("pytom_extract_candidates.py")
        .opt("-n", params.max_num_particles)
        .opt("--particle-diameter", int(params.particle_diameter_ang / 2.0 / apix) * apix)
        .flag("--relion5-compat")
        .opt("--log", "debug")
    )
    if params.cutoff_method == ExtractionCutoffMethod.FALSE_POSITIVES:
        base_cmd.opt("--number-of-false-positives", params.expected_false_positives)
    elif params.cutoff_method == ExtractionCutoffMethod.MANUAL:
        base_cmd.opt("-c", params.cc_threshold)

    if params.score_filter_method == "tophat":
        base_cmd.flag("--tophat-filter")
        if params.score_filter_value != "None" and ":" in params.score_filter_value:
            conn, bins = params.score_filter_value.split(":")
            base_cmd.opt("--tophat-connectivity", conn).opt("--tophat-bins", bins)
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
            with open(f) as src:
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


def read_star_tomo_names(tomograms_star: Path) -> list[str]:
    """rlnTomoName values from the input tomograms star (first DataFrame block)."""
    data = starfile.read(tomograms_star, always_dict=True)
    for val in data.values():
        if isinstance(val, pd.DataFrame) and "rlnTomoName" in val.columns:
            return sorted(val["rlnTomoName"].astype(str).tolist())
    return []


class ExtractCandidatesPytomDriver(ArrayDriver):
    params_class = CandidateExtractPytomParams
    job_name = "extract_candidates_pytom"
    driver_script = Path(__file__).resolve()
    poll_secs = 15

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[CandidateExtractPytomParams]) -> list[str]:
        upstream_results = ctx.paths["input_tm_job"]
        input_tomograms = ctx.paths.get("input_tomograms")

        if not upstream_results.exists():
            raise FileNotFoundError(f"Upstream tmResults not found at {upstream_results}")
        if not input_tomograms or not input_tomograms.exists():
            raise FileNotFoundError(f"Input tomograms.star not found at {input_tomograms}")

        if ctx.params.apix_score_map != "auto":
            apix = float(ctx.params.apix_score_map)
        else:
            apix = get_pixel_size_from_star(input_tomograms)
        if apix is None:
            raise RuntimeError(
                "Could not determine score map pixel size. Set apix_score_map explicitly or check tomograms.star."
            )
        self._apix = apix
        self.log(f"Score map pixel size: {apix:.2f} A/px")
        self.log(f"Particle diameter: {ctx.params.particle_diameter_ang} A")

        local_tm_results = ctx.job_dir / "tmResults"
        linked = stage_upstream_tm_results(upstream_results, local_tm_results)
        if linked == 0 and not any(local_tm_results.iterdir()):
            raise RuntimeError("No files staged from upstream tmResults")
        self.log(f"Staged {linked} files from upstream")

        # Enumerate tomogram names from the staged *_job.json files — pytom
        # writes exactly one `{tomo_name}_job.json` per tomogram during TM.
        job_jsons = sorted(local_tm_results.glob("*_job.json"))
        if not job_jsons:
            raise RuntimeError(f"No *_job.json files found under {local_tm_results}")

        suffix = "_job.json"
        tomo_names: list[str] = sorted(j.name[: -len(suffix)] for j in job_jsons)

        # Census #25 (interim, maintainer decision): TM output is the
        # enumeration source, but a tomogram present in the input star and
        # absent from TM output must be reported LOUDLY, never dropped in
        # silence.
        star_only = sorted(set(read_star_tomo_names(input_tomograms)) - set(tomo_names))
        if star_only:
            self.log(
                f"WARNING: {len(star_only)} tomogram(s) in the input star have NO template-matching "
                f"output (*_job.json) and will NOT be extracted: {star_only}"
            )

        return tomo_names

    def manifest_extras(self, ctx: DriverContext[CandidateExtractPytomParams], items: list[str]) -> dict:
        return {"apix": self._apix, "input_tomograms_star": str(ctx.paths["input_tomograms"])}

    def per_task_slurm_config(self, ctx: DriverContext[CandidateExtractPytomParams]):
        per_task_cfg = ctx.params.get_effective_slurm_config()
        # Tophat morphology inflates per-task memory/runtime substantially:
        # scipy.ndimage opening on a ~2 GB float32 score volume allocates
        # 6–10 GB of intermediate buffers on top of PyTOM's ~5 GB baseline
        # (scores + angles + Python + CUDA). The 8G default that's fine for
        # non-tophat extraction gets OOM-killed here. Bump only when the
        # filter is actually enabled so non-tophat runs stay efficient.
        if ctx.params.score_filter_method == "tophat":
            bumped_mem = "24G"
            bumped_time = "0:30:00"
            self.log(
                f"tophat-filter enabled — bumping per-task SLURM "
                f"(mem {per_task_cfg.mem}→{bumped_mem}, time {per_task_cfg.time}→{bumped_time}) "
                "to avoid OOM during morphological opening."
            )
            per_task_cfg = per_task_cfg.model_copy(update={"mem": bumped_mem, "time": bumped_time})
        return per_task_cfg

    def aggregate(self, ctx: DriverContext[CandidateExtractPytomParams], results: ArrayResults) -> None:
        job_dir = ctx.job_dir
        local_tm_results = job_dir / "tmResults"
        input_tomograms = ctx.paths["input_tomograms"]

        # ---- Aggregate per-tomogram particle lists ----
        candidates_star = job_dir / "candidates.star"
        star_files = sorted(local_tm_results.glob("*_particles.star"))
        # Drop any per-TS particle files for excluded tilt-series (a prior run
        # may have left them on disk; the muted TS must not re-enter the merge).
        if results.skipped:
            excluded_set = set(results.skipped)
            star_files = [f for f in star_files if f.name[: -len("_particles.star")] not in excluded_set]
        if not star_files:
            # Deliberately stricter than all_succeeded (census #27): an empty
            # candidates.star is a poisoned contract for downstream extraction.
            raise RuntimeError("No *_particles.star files produced by tasks")
        if len(star_files) == 1:
            shutil.copy(star_files[0], candidates_star)
            self.log(f"Single tomogram — copied {star_files[0].name}")
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
            self.log(f"Merged {len(merged)} particles from {len(star_files)} tomograms")

        if not candidates_star.exists():
            raise RuntimeError("candidates.star was not created")

        n_particles = cleanup_tomo_names(candidates_star, self._apix)
        self.log(f"Extracted {n_particles} particles total")

        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(input_tomograms, output_tomograms)

        try:
            from services.visualization.imod_vis import generate_candidate_vis

            self.log("Generating IMOD visualization...")
            generate_candidate_vis(
                candidates_star=candidates_star,
                tomograms_star=output_tomograms,
                particle_diameter_ang=float(ctx.params.particle_diameter_ang),
                output_dir=job_dir,
                project_root=ctx.project_path,
            )
        except Exception as vis_err:
            # Visualization is a convenience artifact — its failure must not
            # fail a job whose scientific outputs are complete.
            print(f"[SUPERVISOR WARN] Visualization generation failed (non-fatal): {vis_err}", flush=True)

        try:
            from services.visualization.preview_orchestrator import generate_candidate_previews

            self.log("Rendering candidate preview PNGs...")
            # Pass `project_state` so the orchestrator can walk this
            # project's SUBTOMO_EXTRACTION jobs and build a per-pick
            # cutout atlas. Without it the orchestrator silently sets
            # cutout_atlas=None on every entry — the dashboard then
            # complains "Subtomo cutout atlas not built" even when the
            # subtomo extraction completed and coords match exactly.
            preview_summary = generate_candidate_previews(
                candidates_star=candidates_star,
                tomograms_star=output_tomograms,
                particle_diameter_ang=float(ctx.params.particle_diameter_ang),
                output_dir=job_dir,
                project_root=ctx.project_path,
                project_state=ctx.state,
                instance_id=ctx.instance_id,
                job_model=ctx.params,
            )
            self.log(
                "Previews: "
                f"{len(preview_summary['ok'])} rendered, "
                f"{len(preview_summary['skipped_cached'])} cached, "
                f"{len(preview_summary['missing_volume'])} missing volume, "
                f"{len(preview_summary['errored'])} errored"
            )
        except Exception as preview_err:
            # Same convenience-artifact reasoning as the IMOD visualization.
            print(f"[SUPERVISOR WARN] Preview rendering failed (non-fatal): {preview_err}", flush=True)

        write_optimisation_set(
            job_dir / "optimisation_set.star", particles_star=candidates_star, tomograms_star=output_tomograms
        )
        self.log("Created optimisation_set.star with absolute paths")

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[CandidateExtractPytomParams], item: str) -> bool:
        out_star = tomo_particles_star_path(ctx.job_dir / "tmResults", item)
        if out_star.exists() and out_star.stat().st_size > 0:
            self.log(f"Particles already extracted, skipping: {out_star}")
            return True
        return False

    def stage(self, ctx: DriverContext[CandidateExtractPytomParams], item: str):
        local_tm_results = ctx.job_dir / "tmResults"
        job_json = tomo_job_json_path(local_tm_results, item)
        if not job_json.exists():
            raise FileNotFoundError(f"Staged job.json missing for {item}: {job_json}")
        return job_json

    def build_command(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged) -> ToolCommand:
        apix = float(read_manifest(ctx.job_dir)["apix"])
        return build_extract_base_cmd(ctx.params, apix).opt_path("-j", staged, quote=False)

    def verify_outputs(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged) -> None:
        out_star = tomo_particles_star_path(ctx.job_dir / "tmResults", item)
        if not out_star.exists():
            raise FileNotFoundError(f"pytom_extract_candidates reported success but particles STAR missing: {out_star}")


if __name__ == "__main__":
    os.environ["TQDM_DISABLE"] = "1"
    ExtractCandidatesPytomDriver().main()
