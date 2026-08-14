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

Supervisor-side staging is deliberate (census #45): slicing a RELION
optimisation set needs the full particles/tomograms tables in memory, which
only the supervisor parses — per-task re-parsing of the whole upstream star
N times would be waste.

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

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
markers all live in ArrayDriver; this file is the subtomo-specific hooks.
"""

import json
import shutil
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from drivers.array_job_base import ArrayDriver, ArrayResults, load_excluded_ts, write_skip_status, STATUS_DIR_NAME
from drivers.driver_base import DriverContext, ToolCommand
from services.subtomo_merge import (
    _parse_optimisation_set,
    _read_input_particles_lenient,
    _read_particles_star,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    merge_optimisation_sets_into_jobdir,
    write_optimisation_set,
)
from services.job_models import SubtomoExtractionParams


# ----------------------------------------------------------------------
# Per-TS staging
# ----------------------------------------------------------------------


def _stage_per_ts(
    staging_root: Path,
    ts_name: str,
    optics_df: pd.DataFrame | None,
    particles_df: pd.DataFrame,
    tomograms_df: pd.DataFrame,
    general_kv: dict,
) -> None:
    """Write per-TS particles.star, tomograms.star, and a key-value
    optimisation_set.star into `.staging/task_<ts>/`. Idempotent — overwrites
    on every supervisor run.

    `optics_df=None` for TM-candidate-style inputs: in that case the staged
    particles.star has no optics block (matches the input shape;
    relion_tomo_subtomo sources optics from tomograms.star)."""
    task_dir = staging_root / f"task_{ts_name}"
    task_dir.mkdir(parents=True, exist_ok=True)

    ts_particles = particles_df[particles_df["rlnTomoName"].astype(str) == ts_name].reset_index(drop=True)
    ts_tomograms = tomograms_df[tomograms_df["rlnTomoName"].astype(str) == ts_name].reset_index(drop=True)

    if len(ts_particles) == 0:
        raise RuntimeError(f"No particles for TS {ts_name} (supervisor enumerated from this column; should not happen)")
    if len(ts_tomograms) == 0:
        raise RuntimeError(
            f"No tomogram row for TS {ts_name} in upstream tomograms.star — upstream pipeline is inconsistent."
        )

    # Preserve only the optics groups actually referenced by this TS's
    # particles. Multi-optics-group projects (rare today, possible after
    # merge of multiple datasets) would otherwise carry the union into
    # every task. Skipped entirely when the input has no optics block.
    ts_optics: pd.DataFrame | None = None
    if optics_df is not None and "rlnOpticsGroup" in ts_particles.columns:
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
# Supervisor merge — per-TS outputs → canonical job_dir layout
# ----------------------------------------------------------------------


def _merge_per_ts_outputs(
    job_dir: Path, ts_names: list[str], upstream_general_kv: dict, upstream_tomograms_star: Path
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

    all_optics_dfs: list[pd.DataFrame] = []
    all_particles_dfs: list[pd.DataFrame] = []
    empty_extracts: list[str] = []

    for ts_name in ts_names:
        task_out = job_dir / ".staging" / f"task_{ts_name}" / "out"
        ts_particles_star = task_out / "particles.star"
        if not ts_particles_star.exists():
            raise RuntimeError(f"Expected per-TS particles missing: {ts_particles_star}")

        # RELION writes optics-only stars (no data_particles block) when
        # extraction yields zero particles — e.g. all candidates fell out
        # of bounds during 2D-stack assembly. Tolerate it: skip the TS in
        # the merge so the rest of the job still produces a valid output.
        optics_df, particles_df, _ = _read_particles_star(ts_particles_star, allow_empty_particles=True)
        if particles_df.empty:
            empty_extracts.append(ts_name)
            continue

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
    # params end up with a single row after drop_duplicates). If every per-TS
    # extract was empty we still want to write a valid particles.star so the
    # journey browser sees the job as completed-with-zero rather than failed.
    if all_optics_dfs:
        optics_merged = pd.concat(all_optics_dfs, ignore_index=True).drop_duplicates().reset_index(drop=True)
        particles_merged = pd.concat(all_particles_dfs, ignore_index=True)
    else:
        optics_merged = pd.DataFrame()
        particles_merged = pd.DataFrame(columns=["rlnTomoName", "rlnImageName", "rlnOpticsGroup"])

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
        job_dir / "optimisation_set.star", particles_star=job_dir / "particles.star", tomograms_star=target_tomograms
    )

    extracted_ts = len(ts_names) - len(empty_extracts)
    print(
        f"[SUPERVISOR] Merged: {len(particles_merged)} particles across "
        f"{extracted_ts}/{len(ts_names)} TS → {job_dir / 'particles.star'}",
        flush=True,
    )
    if empty_extracts:
        print(
            f"[SUPERVISOR] {len(empty_extracts)} TS had picks but extracted 0 particles "
            f"(out-of-bounds during 2D-stack assembly or similar): {empty_extracts}",
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


class SubtomoExtractionDriver(ArrayDriver):
    params_class = SubtomoExtractionParams
    job_name = "subtomo_extraction"
    driver_script = Path(__file__).resolve()
    poll_secs = 15

    # ---------------- supervisor ----------------

    def whole_job_short_circuit(self, ctx: DriverContext[SubtomoExtractionParams]) -> bool:
        # Aggregation short-circuit: skip extraction entirely, just merge
        # supplied optimisation sets into job_dir. Mirrors the original
        # one-shot driver's merge_only branch verbatim.
        if ctx.params.merge_only:
            self.log("merge_only=True, skipping extraction.")
            if not ctx.params.additional_sources:
                raise RuntimeError("merge_only=True but no additional_sources to merge.")
            _run_additional_sources_merge(ctx.params, ctx.job_dir)
            return True

        input_optimisation = ctx.paths["input_optimisation"]
        if not input_optimisation.exists():
            raise FileNotFoundError(f"Input optimisation_set.star not found: {input_optimisation}")

        upstream_particles_star, upstream_tomograms_star = _parse_optimisation_set(input_optimisation)
        self.log(f"Upstream particles: {upstream_particles_star}")
        self.log(f"Upstream tomograms: {upstream_tomograms_star}")

        # Lenient read: upstream may be a TM `candidates.star` (particles-
        # only, no optics) or a real RELION particles.star (optics +
        # particles). relion_tomo_subtomo sources optics from
        # tomograms.star, so missing optics in the input is fine.
        particles_df, optics_df, general_kv = _read_input_particles_lenient(upstream_particles_star)
        tomograms_df = _read_tomograms_star(upstream_tomograms_star)

        self._input_optimisation = input_optimisation
        self._upstream_particles_star = upstream_particles_star
        self._upstream_tomograms_star = upstream_tomograms_star
        self._particles_df = particles_df
        self._optics_df = optics_df
        self._general_kv = general_kv
        self._tomograms_df = tomograms_df

        ts_with_picks = sorted(particles_df["rlnTomoName"].astype(str).unique().tolist())
        all_upstream_ts = sorted(tomograms_df["rlnTomoName"].astype(str).unique().tolist())
        if not ts_with_picks:
            # Whole-job equivalent of the per-TS .skip path: upstream picking
            # produced 0 candidates across every tomogram, so there is nothing
            # to extract. Mark every TS as .skip and a sidecar so the UI can
            # render a clear "no picks anywhere — skipped" banner. We exit
            # SUCCESS rather than fail because (a) the schemer would otherwise
            # halt the whole pipeline on an upstream-data condition that's
            # diagnostic, not a job bug, and (b) it mirrors the existing per-TS
            # SKIP semantics which are also "succeeded but did no work".
            status_dir = ctx.job_dir / STATUS_DIR_NAME
            status_dir.mkdir(parents=True, exist_ok=True)
            for f in status_dir.glob("*.skip"):
                f.unlink()
            for ts in all_upstream_ts:
                write_skip_status(status_dir, ts, reason="no candidates anywhere (upstream picks=0)")
            sentinel = ctx.job_dir / ".skipped_no_candidates.json"
            sentinel.write_text(
                json.dumps(
                    {
                        "reason": "upstream_particles_empty",
                        "upstream_particles_star": str(upstream_particles_star),
                        "upstream_tomograms_star": str(upstream_tomograms_star),
                        "n_upstream_tomograms": len(all_upstream_ts),
                        "n_with_picks": 0,
                        "message": (
                            "Upstream candidate extraction produced 0 picks across all tomograms. "
                            "Subtomo extraction has nothing to extract; this job is a no-op skip."
                        ),
                    },
                    indent=2,
                )
            )
            self.log(
                f"Upstream produced 0 picks across all {len(all_upstream_ts)} tomograms. "
                f"Marking job as skipped (no work); wrote sentinel {sentinel.name}."
            )
            return True

        return False

    def enumerate_items(self, ctx: DriverContext[SubtomoExtractionParams]) -> list[str]:
        # The manifest covers EVERY upstream tilt-series — the ones with
        # picks get extracted, the empty ones get a .skip marker so the
        # per-TS UI tracker shows them as deliberately blank rather than
        # silently dropping the rows. Without this the user sees N reconstructions
        # but suddenly fewer subtomo rows and can't tell whether the gap is
        # intentional or a bug.
        ts_with_picks = sorted(self._particles_df["rlnTomoName"].astype(str).unique().tolist())
        all_upstream_ts = sorted(self._tomograms_df["rlnTomoName"].astype(str).unique().tolist())

        # Manifest order: keep upstream tomograms.star order as authoritative.
        # Fall back to including any extra TS that show up only in particles
        # (defensive — shouldn't happen, but don't drop them silently).
        extra_pick_only = sorted(set(ts_with_picks) - set(all_upstream_ts))
        ts_names = all_upstream_ts + extra_pick_only
        with_picks_set = set(ts_with_picks)
        self._empty_ts = [t for t in ts_names if t not in with_picks_set]

        # Excluded TS are muted — never staged, dispatched, or merged. The
        # base's apply_exclusions writes their .skip markers (and clears any
        # stale .ok/.fail — census #44); here we only need the filter.
        self._excluded_set = load_excluded_ts(ctx.project_path)
        excluded_ts = [t for t in ts_names if t in self._excluded_set]
        self._ts_with_picks = [t for t in ts_with_picks if t not in self._excluded_set]

        self.log(
            f"{len(self._ts_with_picks)} TS with picks to extract; "
            f"{len(self._empty_ts)} empty TS will be marked SKIP (no upstream candidates); "
            f"{len(excluded_ts)} excluded from processing"
        )
        if self._empty_ts:
            self.log(f"Empty TS (skipped): {self._empty_ts}")
        if excluded_ts:
            self.log(f"Excluded TS (skipped): {excluded_ts}")

        return ts_names

    def preflight_scope(self, ctx: DriverContext[SubtomoExtractionParams], items: list[str]) -> list[str]:
        # Preflight only the TS we'll actually dispatch.
        return self._ts_with_picks

    def pre_dispatch(self, ctx: DriverContext[SubtomoExtractionParams], items: list[str]) -> None:
        # Clear stale .skip markers, then pre-write fresh ones for the
        # currently-empty TS. submit_array_job's sparse-array logic treats
        # .skip the same as .ok — those indices never get dispatched, but
        # they remain in the manifest so the UI renders 1 row per upstream TS.
        # (The base's apply_exclusions re-writes the excluded TS's markers
        # right after this hook.)
        status_dir = ctx.job_dir / STATUS_DIR_NAME
        if status_dir.is_dir():
            for f in status_dir.glob("*.skip"):
                f.unlink()
        for ts in self._empty_ts:
            if ts in self._excluded_set:
                continue
            write_skip_status(status_dir, ts, reason="no candidates above template-matching threshold")

        # Stage per-TS optimisation sets ONLY for the TS that have picks.
        # Skipped TS never get a staging dir — no task will run for them.
        staging_root = ctx.job_dir / ".staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        for ts_name in self._ts_with_picks:
            _stage_per_ts(
                staging_root, ts_name, self._optics_df, self._particles_df, self._tomograms_df, self._general_kv
            )
        self.log(f"Staged per-TS inputs for {len(self._ts_with_picks)} TS under {staging_root}")

    def manifest_extras(self, ctx: DriverContext[SubtomoExtractionParams], items: list[str]) -> dict:
        return {
            "input_optimisation_star": str(self._input_optimisation),
            "upstream_particles_star": str(self._upstream_particles_star),
            "upstream_tomograms_star": str(self._upstream_tomograms_star),
        }

    def aggregate(self, ctx: DriverContext[SubtomoExtractionParams], results: ArrayResults) -> None:
        # Merge per-TS outputs into job_dir's canonical particles.star /
        # Subtomograms/ tree. Only the TS with picks produced outputs;
        # skipped TS never staged or wrote anything.
        _merge_per_ts_outputs(ctx.job_dir, self._ts_with_picks, self._general_kv, self._upstream_tomograms_star)

        # Aggregation merge (additional_sources) — opt-in, runs only if the
        # job model has sources configured.
        if ctx.params.additional_sources:
            self.log(f"Merging {len(ctx.params.additional_sources)} additional source(s) on top of extraction...")
            _run_additional_sources_merge(ctx.params, ctx.job_dir)

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[SubtomoExtractionParams], item: str) -> bool:
        # Idempotent skip: if a prior run already produced outputs, just
        # write .ok and exit. (submit_array_job already filters previously-
        # OK items at the SLURM-array level, so this is belt-and-braces.)
        out_dir = ctx.job_dir / ".staging" / f"task_{item}" / "out"
        if (out_dir / "particles.star").exists() and (out_dir / "Subtomograms").exists():
            self.log(f"{item} already extracted — skipping")
            return True
        return False

    def stage(self, ctx: DriverContext[SubtomoExtractionParams], item: str):
        staging_dir = ctx.job_dir / ".staging" / f"task_{item}"
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
        return {"staging_dir": staging_dir, "per_ts_optset": per_ts_optset, "out_dir": out_dir}

    def build_command(self, ctx: DriverContext[SubtomoExtractionParams], item: str, staged) -> ToolCommand:
        params = ctx.params
        cmd = (
            ToolCommand("relion_tomo_subtomo")
            .opt_path("--o", f"{staged['out_dir']}/", quote=False)
            .opt_path("--i", staged["per_ts_optset"], quote=False)
            .opt("--b", params.box_size)
            .opt("--bin", int(params.binning))
        )
        if params.crop_size > 0:
            cmd.opt("--crop", params.crop_size)
        if params.max_dose > 0:
            cmd.opt("--max_dose", params.max_dose)
        if params.min_frames > 1:
            cmd.opt("--min_frames", params.min_frames)
        if params.do_stack2d:
            cmd.flag("--stack2d")
        if params.do_float16:
            cmd.flag("--float16")
        return cmd

    def task_binds(self, ctx: DriverContext[SubtomoExtractionParams], item: str, staged) -> list:
        # Bind both the staging dir (read input optimisation set) and the
        # upstream optimisation set's directory (relion follows the
        # absolute paths inside it).
        return [staged["staging_dir"].resolve(), staged["per_ts_optset"].parent.resolve()]

    def task_cwd(self, ctx: DriverContext[SubtomoExtractionParams], item: str, staged) -> Path:
        return staged["out_dir"]

    def verify_outputs(self, ctx: DriverContext[SubtomoExtractionParams], item: str, staged) -> None:
        out_dir = staged["out_dir"]
        if not (out_dir / "particles.star").exists():
            raise RuntimeError(f"relion_tomo_subtomo did not produce particles.star in {out_dir}")
        # Subtomograms dir is the actual stack output; if missing the
        # particles file is referencing files that don't exist.
        if not (out_dir / "Subtomograms").exists():
            raise RuntimeError(f"relion_tomo_subtomo did not produce Subtomograms/ in {out_dir}")


if __name__ == "__main__":
    SubtomoExtractionDriver().main()
