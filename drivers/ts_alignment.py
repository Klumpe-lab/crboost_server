#!/usr/bin/env python
# drivers/ts_alignment.py
"""
ts_alignment driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Refreshes the job-local snapshot of the producer's
          tomostar dir + settings, enumerates tilt-series from the
          TiltSeriesRegistry (the enumeration authority — census #38/#39
          pilot), runs a dispatch-time drift check (registry vs tomostar dir
          vs input star), writes the task manifest, submits a SLURM array with
          one task per TS, polls until completion, then aggregates alignment
          metadata.

- Set:    TASK mode. Stages a per-TS environment (single tomostar + settings),
          runs ts_aretomo or ts_etomo_patches for one tilt-series.

Enumeration/staging semantics (census #38/#39, maintainer decision):
- The registry is the source of truth for WHICH tilt-series exist; the old
  `*.tomostar` glob could resurrect stale files or silently omit a TS whose
  file a partial producer write lost.
- The whole-dir tomostar snapshot is refreshed on EVERY supervisor run (it
  used to be copied once and reused stale across re-runs). The snapshot
  itself stays: it marries the settings file with the possibly-different
  producer's tomostar dir (tilt filter) and insulates a running array from
  producer churn.
- A live (non-muted) registry TS missing from the tomostar dir or the input
  star is DRIFT: it stays in the manifest and its task fails fast with the
  reason (containment rule — others proceed, job ends FAILED). Extra files
  the registry doesn't know are warned about and never dispatched.

Tolerant tally (census #41, deliberate): per-TS alignment failure is normal in
cryo-ET — failed/missing TS are warned about and dropped from aggregation;
only a total wipeout fails the job. The failed-TS-absent-downstream gap is
owned by docs/roadmaps/05-per-ts-top-up.md.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
markers all live in ArrayDriver; this file is the alignment-specific hooks.
"""

import shutil
import sys
from pathlib import Path

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import (
    ArrayDriver,
    ArrayResults,
    read_manifest,
    read_tilt_series_names_from_input_star,
    stage_per_ts_environment,
)
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input
from services.jobs.ts_alignment import TsAlignmentParams
from services.models_base import AlignmentMethod
from services.tilt_series import get_registry_for
from services.tilt_series.adapters import TsAlignmentIngestAdapter
from services.tilt_series_service import drop_tilts_from_tomostar


def build_alignment_command(params: TsAlignmentParams) -> ToolCommand | str:
    """Build the alignment command to run inside the staged environment.

    Returns a shell fragment (not a ToolCommand) for an unimplemented method: the
    error shim is compound shell, not a tool invocation.
    """
    if params.alignment_method == AlignmentMethod.ARETOMO:
        cmd = (
            ToolCommand("WarpTools ts_aretomo")
            .opt("--settings", "warp_tiltseries.settings")
            .opt("--output_processing", "warp_tiltseries")
            .opt("--angpix", params.rescale_angpixs)
            .opt("--alignz", int(params.sample_thickness_nm * 10))
            .opt("--perdevice", params.perdevice)
        )
        if params.patch_x > 0 and params.patch_y > 0:
            cmd.opt("--patches", f"{params.patch_x}x{params.patch_y}")
        if params.axis_iter > 0:
            cmd.opt("--axis_iter", params.axis_iter)
            cmd.opt("--axis_batch", min(params.axis_batch, 1))
        return cmd

    if params.alignment_method == AlignmentMethod.IMOD:
        return (
            ToolCommand("WarpTools ts_etomo_patches")
            .opt("--settings", "warp_tiltseries.settings")
            .opt("--output_processing", "warp_tiltseries")
            .opt("--angpix", params.rescale_angpixs)
            .opt("--patch_size", int(params.imod_patch_size * 10))
        )

    return f"echo 'ERROR: Alignment method {params.alignment_method} not implemented'; exit 1;"


def collect_per_ts_outputs(job_dir: Path, ts_name: str) -> None:
    """
    Copy alignment outputs from the per-TS staging dir into the shared job dir.
    - warp_tiltseries/{ts_name}.xml → job_dir/warp_tiltseries/{ts_name}.xml
    - warp_tiltseries/tiltstack/{ts_name}/ → job_dir/warp_tiltseries/tiltstack/{ts_name}/
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    staged_warp = stage_root / "warp_tiltseries"

    shared_warp = job_dir / "warp_tiltseries"
    shared_warp.mkdir(parents=True, exist_ok=True)

    # Copy XML
    src_xml = staged_warp / f"{ts_name}.xml"
    if src_xml.exists():
        shutil.copy2(str(src_xml), str(shared_warp / f"{ts_name}.xml"))

    # Copy tiltstack directory
    src_tiltstack = staged_warp / "tiltstack" / ts_name
    if src_tiltstack.exists():
        dst_tiltstack = shared_warp / "tiltstack" / ts_name
        if dst_tiltstack.exists():
            shutil.rmtree(str(dst_tiltstack))
        shutil.copytree(str(src_tiltstack), str(dst_tiltstack))


def has_alignment_output(warp_dir: Path, ts_name: str, method: AlignmentMethod) -> bool:
    """True if the per-TS tiltstack dir under `warp_dir` holds real alignment output.

    WarpTools `ts_aretomo` / `ts_etomo_patches` exit 0 even when AreTomo or
    etomo fail to align an individual tilt-series — they just flag the item
    "unselected" and still write `{ts}.xml`. So the XML is NOT a success
    signal; the alignment matrices are: `.st.aln` for AreTomo, `.xf` + `.tlt`
    for IMOD.
    """
    tiltstack = warp_dir / "tiltstack" / ts_name
    if not tiltstack.is_dir():
        return False
    if method == AlignmentMethod.ARETOMO:
        return any(tiltstack.glob("*.st.aln"))
    if method == AlignmentMethod.IMOD:
        return any(tiltstack.glob("*.xf")) and any(tiltstack.glob("*.tlt"))
    return False


class TsAlignmentDriver(ArrayDriver):
    params_class = TsAlignmentParams
    job_name = "ts_alignment"
    driver_script = Path(__file__).resolve()

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[TsAlignmentParams]) -> list[str]:
        tomostar_dir = ctx.paths["tomostar_dir"]
        require_producer_input(tomostar_dir, "Tomostar directory")

        settings_file = ctx.paths["warp_tiltseries_settings"]
        require_producer_input(settings_file, "Settings file")

        input_star = ctx.paths["input_star"]
        require_producer_input(input_star, "Input STAR")

        # Census #38: the registry is the enumeration authority; the sources on
        # disk are checked against it, never trusted as the item list. It also
        # carries the tilt-filter's verdict (per-frame is_filtered_out), so it is
        # loaded before the snapshot below, which applies that cut.
        registry = get_registry_for(ctx.project_path)

        # Refresh the job-local snapshot on EVERY supervisor run (census #39):
        # tasks stage from stable job-local paths, but a supervisor re-run must
        # see the producer's CURRENT tomostars (re-trimmed tilts, added/removed
        # TS), never a stale first-run copy.
        #
        # The tilt-filter cut is applied HERE, at consumption, instead of by the
        # filter writing a tomostar dir of its own: the filter is interactive and
        # may be committed before, after, or never relative to tsImport, and a
        # separate producer dir made downstream wiring depend on that ordering (a
        # pending filter silently fell back to the untrimmed tomostar). Reading the
        # verdict from the registry collapses both paths -- filter off and filter
        # on -- onto one: an empty drop set copies the tomostars verbatim.
        local_tomostar_dir = ctx.job_dir / "tomostar"
        if local_tomostar_dir.exists():
            shutil.rmtree(str(local_tomostar_dir))
        drop_frames = registry.filtered_out_frame_ids()
        kept, dropped = drop_tilts_from_tomostar(tomostar_dir, local_tomostar_dir, drop_frames)
        if drop_frames:
            self.log(f"Tilt filter applied from registry: {kept} tilts kept, {dropped} dropped")
        local_settings = ctx.job_dir / settings_file.name
        shutil.copy2(str(settings_file), str(local_settings))

        reg_ids = registry.tilt_series_ids()
        if not reg_ids:
            raise RuntimeError(
                f"TiltSeries registry is empty for project {ctx.project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, then restart this job."
            )
        excluded = set(registry.excluded_ids())

        tomostar_set = {f.stem for f in local_tomostar_dir.glob("*.tomostar")}
        star_set = set(read_tilt_series_names_from_input_star(input_star))

        # Dispatch-time drift check. A live TS missing from a source fails THAT
        # TS visibly (recorded in the manifest, its task raises the reason);
        # extras the registry doesn't know are warned and never dispatched.
        drift: dict[str, str] = {}
        for ts in reg_ids:
            if ts in excluded:
                continue
            missing_from = [
                label
                for label, present in (("tomostar dir", ts in tomostar_set), ("input star", ts in star_set))
                if not present
            ]
            if missing_from:
                drift[ts] = f"registry TS '{ts}' missing from: {', '.join(missing_from)} (producer drift)"
                self.log(f"ERROR: {drift[ts]} — its task will FAIL")
        extras = sorted((tomostar_set | star_set) - set(reg_ids))
        if extras:
            self.log(
                f"WARNING: {len(extras)} tilt-series on disk are unknown to the registry and will "
                f"NOT be aligned (stale files?): {extras}"
            )

        self._drift = drift
        return sorted(reg_ids)

    def manifest_extras(self, ctx: DriverContext[TsAlignmentParams], items: list[str]) -> dict | None:
        return {"drift_ts": self._drift} if self._drift else None

    def tally_acceptable(self, ctx: DriverContext[TsAlignmentParams], results: ArrayResults) -> bool:
        # Per-TS alignment failure is normal in cryo-ET — AreTomo simply can't
        # solve every tilt-series. One bad TS must NOT abort the whole job and
        # halt the pipeline: drop the failed/missing tilt-series and carry the
        # rest forward. Only a total wipeout (nothing aligned) is fatal.
        if not results.ok:
            self.log("No tilt-series aligned successfully — failing the job")
            return False
        excluded_ts = sorted(results.failed + results.missing)
        if excluded_ts:
            n_total = len(results.ok) + len(results.skipped) + len(excluded_ts)
            print(
                f"[SUPERVISOR] WARNING: excluding {len(excluded_ts)}/{n_total} tilt-series "
                f"that failed to align: {excluded_ts}",
                file=sys.stderr,
                flush=True,
            )
            self.log(f"Continuing with {len(results.ok)} aligned tilt-series.")
        return True

    def aggregate(self, ctx: DriverContext[TsAlignmentParams], results: ArrayResults) -> None:
        aligned_ts = results.ok

        # Aggregate metadata via the TiltSeries registry. Fail loud on an
        # empty registry rather than fall back to the legacy string-keyed
        # merge — that's the silent-corruption path this refactor retires.
        self.log(f"Aggregating alignment metadata for {len(aligned_ts)} tilt-series...")

        input_star_path = ctx.paths.get("input_star")
        output_star_path = ctx.paths.get("output_star", ctx.job_dir / "aligned_tilt_series.star")

        if not input_star_path or not Path(input_star_path).exists():
            raise FileNotFoundError(f"tsAlignment aggregation requires an existing input STAR; got: {input_star_path}")

        registry = get_registry_for(ctx.project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {ctx.project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, "
                f"then restart this job."
            )
        adapter = TsAlignmentIngestAdapter(registry=registry, job_dir=ctx.job_dir, job_instance_id=ctx.instance_id)
        adapter.ingest(
            aligned_ts, alignment_method=ctx.params.alignment_method, alignment_angpix=ctx.params.rescale_angpixs
        )
        adapter.emit_star(
            input_star_path=Path(input_star_path),
            output_star_path=Path(output_star_path),
            project_root=ctx.project_path,
            tomo_dimensions=ctx.params.tomo_dimensions if hasattr(ctx.params, "tomo_dimensions") else "4096x4096x2048",
        )
        registry.save()
        self.log("Metadata aggregation successful.")

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[TsAlignmentParams], item: str) -> bool:
        # Idempotency: skip only if a PREVIOUS run left REAL alignment output
        # for this TS. The {ts}.xml alone is not proof — WarpTools writes it
        # even for tilt-series AreTomo failed on (see has_alignment_output),
        # so a bare-XML check would let a failed TS masquerade as done on retry.
        shared_warp = ctx.job_dir / "warp_tiltseries"
        shared_xml = shared_warp / f"{item}.xml"
        if shared_xml.exists() and has_alignment_output(shared_warp, item, ctx.params.alignment_method):
            self.log(f"Alignment output already exists, skipping: {item}")
            return True
        return False

    def stage(self, ctx: DriverContext[TsAlignmentParams], item: str):
        manifest = read_manifest(ctx.job_dir)
        drift = manifest.get("drift_ts", {})
        if item in drift:
            # Recorded by the supervisor's dispatch-time drift check: fail this
            # TS here, visibly, with the reason it cannot be aligned.
            raise FileNotFoundError(f"Cannot align '{item}': {drift[item]}")

        local_settings = ctx.job_dir / "warp_tiltseries.settings"
        staged_settings, _staged_processing = stage_per_ts_environment(
            ctx.job_dir, item, input_processing=None, settings_file=local_settings
        )
        stage_root = staged_settings.parent
        self.log(f"Staged at: {stage_root}")
        return stage_root

    def build_command(self, ctx: DriverContext[TsAlignmentParams], item: str, staged) -> ToolCommand | str:
        return build_alignment_command(ctx.params)

    def task_cwd(self, ctx: DriverContext[TsAlignmentParams], item: str, staged) -> Path:
        return staged

    def verify_outputs(self, ctx: DriverContext[TsAlignmentParams], item: str, staged) -> None:
        # Verify REAL alignment output landed in the STAGED dir. WarpTools
        # exits 0 and still writes {ts}.xml even when AreTomo fails to align
        # this tilt-series, so the XML is not a success signal — check the
        # alignment matrices.
        staged_warp = staged / "warp_tiltseries"
        staged_xml = staged_warp / f"{item}.xml"
        if not staged_xml.exists():
            raise FileNotFoundError(f"Alignment produced no XML for {item} (expected {staged_xml})")
        if not has_alignment_output(staged_warp, item, ctx.params.alignment_method):
            raise RuntimeError(
                f"No alignment output for {item}: WarpTools produced no .st.aln/.xf in "
                f"warp_tiltseries/tiltstack/{item}/. AreTomo likely failed to align this "
                f"tilt-series (see container output above)."
            )

    def collect(self, ctx: DriverContext[TsAlignmentParams], item: str, staged) -> None:
        collect_per_ts_outputs(ctx.job_dir, item)


if __name__ == "__main__":
    TsAlignmentDriver().main()
