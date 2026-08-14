#!/usr/bin/env python
# drivers/fs_motion_and_ctf.py
"""
fs_motion_and_ctf driver — supervisor + per-tilt-series SLURM array task.

Mode is determined by the SLURM_ARRAY_TASK_ID env var:

- Unset:  SUPERVISOR mode. Reads the import STAR to group frames by tilt-series,
          writes a manifest with the TS→frame mapping, submits a SLURM array
          (one task per TS), polls until completion, then aggregates per-frame
          XML metadata into the output STAR.

- Set:    TASK mode. Creates a staging directory with only this TS's frames
          (symlinked), runs WarpTools create_settings + fs_motion_and_ctf on
          that subset, then copies resulting XMLs and averages into the shared
          warp_frameseries/ output directory.

The mode dispatch, both bootstraps, manifest lookup, exclusions, tally and exit
markers all live in ArrayDriver; this file is the fs_motion-specific hooks.
"""

import shutil
import sys
from pathlib import Path

server_dir = Path(__file__).parent.parent
sys.path.insert(0, str(server_dir))

from drivers.array_job_base import ArrayDriver, ArrayResults, read_manifest
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input
from services.configs.starfile_service import StarfileService
from services.jobs.fs_motion_ctf import FsMotionCtfParams
from services.tilt_series import get_registry_for
from services.tilt_series.adapters import FsMotionCtfIngestAdapter


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def read_ts_frame_mapping(input_star: Path, project_root: Path) -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Parse the import STAR to build a mapping: ts_name → [frame_filename, ...].

    The import STAR has a `global` block with one row per TS, each pointing to
    a per-TS star file. The per-TS star file lists frames via rlnMicrographMovieName.

    rlnTomoTiltSeriesStarFile paths are relative to the project root
    (e.g. "Import/job001/tilt_series/xxx.star").

    Returns (mapping, unresolved) where `unresolved` maps each TS whose per-TS
    star could not be found to a reason string. Those TS still enter the
    manifest so their tasks fail visibly (census #12) instead of the TS being
    silently dropped from the run.
    """
    star_svc = StarfileService()
    star_data = star_svc.read(input_star)
    global_df = star_data.get("global")
    if global_df is None or len(global_df) == 0:
        return {}, {}

    input_star_dir = input_star.parent
    mapping: dict[str, list[str]] = {}
    unresolved: dict[str, str] = {}

    for _, row in global_df.iterrows():
        ts_name = str(row["rlnTomoName"])
        ts_star_rel = row["rlnTomoTiltSeriesStarFile"]

        # Try project root first (RELION convention), then input_star_dir as fallback
        ts_star_path = None
        for base in [project_root, input_star_dir]:
            candidate = (base / ts_star_rel).resolve()
            if candidate.exists():
                ts_star_path = candidate
                break

        if ts_star_path is None:
            unresolved[ts_name] = (
                f"per-TS star not found: tried {project_root / ts_star_rel} and {input_star_dir / ts_star_rel}"
            )
            continue

        ts_data = star_svc.read(ts_star_path)
        # The per-TS star has one data block named after the TS
        ts_df = next(iter(ts_data.values()))
        frames = ts_df["rlnMicrographMovieName"].astype(str).tolist()
        # These are relative paths like "frames/xxx.eer" — extract just filenames
        mapping[ts_name] = [Path(f).name for f in frames]

    return mapping, unresolved


def build_warp_commands(params: FsMotionCtfParams, frames_rel: str, extension: str) -> str:
    """Build WarpTools create_settings + fs_motion_and_ctf command for a staged frame dir.

    `extension` is the frame glob (e.g. "*.eer"), embedded single-quoted so the shell
    hands it to WarpTools rather than expanding it.
    """
    gain_path = params.gain_path if params.gain_path and params.gain_path != "None" else ""
    gain_ops_str = params.gain_operations if params.gain_operations else ""

    # Negative sign on --eer_ngroups reinterprets the value as "EER fractions" (RELION
    # semantics): fewer, higher-SNR output sub-frames instead of many low-SNR ones.
    # For low-dose cryo-ET this is what gives motion correction enough signal per group
    # to produce a sharp average; dropping the sign collapses CTF fits into the floor
    # of the defocus search range on low-SNR (high-tilt) images. The old CryoBoost and
    # the pre-refactor driver both applied this sign for EER inputs.
    create_settings = (
        ToolCommand("WarpTools create_settings")
        .opt("--folder_data", frames_rel)
        .raw(f"--extension '{extension}'")
        .opt("--folder_processing", "warp_frameseries")
        .opt("--output", "warp_frameseries.settings")
        .opt("--angpix", params.pixel_size)
        .opt("--eer_ngroups", f"-{params.eer_ngroups}")
    )
    if gain_path:
        create_settings.opt_path("--gain_reference", gain_path, quote=True)
        if gain_ops_str:
            create_settings.opt("--gain_operations", gain_ops_str)

    run_main = (
        ToolCommand("WarpTools fs_motion_and_ctf")
        .opt("--settings", "warp_frameseries.settings")
        .opt("--m_grid", params.m_grid)
        .opt("--m_range_min", params.m_range_min)
        .opt("--m_range_max", params.m_range_max)
        .opt("--m_bfac", params.m_bfac)
        .opt("--c_grid", params.c_grid)
        .opt("--c_window", params.c_window)
        .opt("--c_range_min", params.c_range_min)
        .opt("--c_range_max", params.c_range_max)
        .opt("--c_defocus_min", params.defocus_min_microns)
        .opt("--c_defocus_max", params.defocus_max_microns)
        .opt("--c_voltage", round(float(params.voltage)))
        .opt("--c_cs", params.spherical_aberration)
        .opt("--c_amplitude", params.amplitude_contrast)
        .opt("--perdevice", params.perdevice)
        .flag("--out_averages")
        .opt("--out_skip_first", params.out_skip_first)
        .opt("--out_skip_last", params.out_skip_last)
    )
    if params.out_average_halves:
        run_main.flag("--out_average_halves")
    if params.c_use_sum:
        run_main.flag("--c_use_sum")
    if params.do_phase:
        run_main.flag("--c_fit_phase")

    return f"test -f warp_frameseries.settings || ({create_settings.render()}) && {run_main.render()}"


def detect_frame_extension(frames_dir: Path) -> str:
    """Detect the frame file extension from a directory."""
    for ext in [".eer", ".tiff", ".tif", ".mrc"]:
        if any(frames_dir.glob(f"*{ext}")):
            return f"*{ext}"
    return "*.eer"


def stage_fs_environment(job_dir: Path, ts_name: str, frame_filenames: list[str], project_frames_dir: Path) -> Path:
    """
    Build a per-TS staging directory with only this TS's frames symlinked.

    .staging/task_{ts_name}/
    ├── frames/              ← symlinks to this TS's frame files only
    ├── warp_frameseries.settings  ← created by WarpTools
    └── warp_frameseries/    ← WarpTools output dir

    A missing source frame raises (census #13): motion/CTF-correcting a partial
    frame set and green-ticking it hides data loss the operator must see.
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    stage_root.mkdir(parents=True, exist_ok=True)

    staged_frames = stage_root / "frames"
    staged_frames.mkdir(parents=True, exist_ok=True)

    missing = []
    for fname in frame_filenames:
        src = project_frames_dir / fname
        dst = staged_frames / fname
        if not dst.exists() and not dst.is_symlink():
            if src.exists():
                dst.symlink_to(src.resolve())
            else:
                missing.append(str(src))

    if missing:
        raise FileNotFoundError(
            f"{len(missing)}/{len(frame_filenames)} frame file(s) missing for tilt-series '{ts_name}' "
            f"(first missing: {missing[0]}) — refusing to process a partial frame set"
        )

    return stage_root


def collect_fs_outputs(job_dir: Path, ts_name: str) -> None:
    """
    Copy per-TS warp_frameseries outputs into the shared job-level output dir.

    Each frame produces a unique XML and unique average MRCs (named by frame stem),
    so there are no collisions between TS tasks.
    """
    stage_root = job_dir / ".staging" / f"task_{ts_name}"
    staged_warp = stage_root / "warp_frameseries"
    shared_warp = job_dir / "warp_frameseries"
    shared_warp.mkdir(parents=True, exist_ok=True)

    if not staged_warp.exists():
        return

    # Copy XMLs (per-frame metadata)
    for xml_file in staged_warp.glob("*.xml"):
        shutil.copy2(str(xml_file), str(shared_warp / xml_file.name))

    # Copy subdirectories (average/, powerspectrum/, etc.)
    for subdir in staged_warp.iterdir():
        if subdir.is_dir():
            shared_subdir = shared_warp / subdir.name
            shared_subdir.mkdir(parents=True, exist_ok=True)
            for f in subdir.iterdir():
                if f.is_file():
                    shutil.copy2(str(f), str(shared_subdir / f.name))
                elif f.is_dir():
                    # Nested subdirs like average/even/, average/odd/
                    shared_nested = shared_subdir / f.name
                    shared_nested.mkdir(parents=True, exist_ok=True)
                    for nf in f.iterdir():
                        if nf.is_file():
                            shutil.copy2(str(nf), str(shared_nested / nf.name))


class FsMotionCtfDriver(ArrayDriver):
    params_class = FsMotionCtfParams
    job_name = "fs_motion_and_ctf"
    driver_script = Path(__file__).resolve()

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[FsMotionCtfParams]) -> list[str]:
        input_star = ctx.paths["input_star"]
        require_producer_input(input_star, "Input STAR")

        mapping, unresolved = read_ts_frame_mapping(input_star, ctx.project_path)
        ts_names = sorted(set(mapping) | set(unresolved))
        if not ts_names:
            raise ValueError(f"No tilt-series/frames found in input STAR: {input_star}")

        total_frames = sum(len(frames) for frames in mapping.values())
        self.log(f"{total_frames} total frames across {len(ts_names)} tilt-series")
        for ts in sorted(mapping):
            self.log(f"  {ts}: {len(mapping[ts])} frames")
        for ts in sorted(unresolved):
            self.log(f"ERROR: '{ts}' has no resolvable per-TS star — its task will FAIL: {unresolved[ts]}")

        self._ts_frame_map = mapping
        self._unresolved = unresolved
        return ts_names

    def manifest_extras(self, ctx: DriverContext[FsMotionCtfParams], items: list[str]) -> dict:
        extras = {"ts_frames": self._ts_frame_map}
        if self._unresolved:
            extras["unresolved_ts"] = self._unresolved
        return extras

    def aggregate(self, ctx: DriverContext[FsMotionCtfParams], results: ArrayResults) -> None:
        output_processing_dir = ctx.paths.get("output_processing", ctx.job_dir / "warp_frameseries")
        output_processing_dir.mkdir(parents=True, exist_ok=True)

        # Aggregate metadata via the TiltSeries registry. If the registry is
        # empty (legacy project), fail loud rather than fall back to the old
        # string-keyed merge — that's the path that produced the
        # silent-corruption bug we explicitly guarded against.
        registry = get_registry_for(ctx.project_path)
        if not registry.tilt_series_ids():
            raise RuntimeError(
                f"TiltSeries registry is empty for project {ctx.project_path}. "
                f"Reload the project in the UI to backfill the registry from mdocs, "
                f"then restart this job."
            )
        adapter = FsMotionCtfIngestAdapter(
            registry=registry, job_dir=ctx.job_dir, job_instance_id=ctx.instance_id, warp_folder="warp_frameseries"
        )
        adapter.ingest(results.ok)
        adapter.emit_star(
            ctx.paths["input_star"],
            ctx.paths["output_star"],
            project_root=ctx.project_path,
            excluded_ids=set(results.skipped),
        )
        registry.save()
        self.log("Metadata processing successful.")

    # ---------------- task ----------------

    def stage(self, ctx: DriverContext[FsMotionCtfParams], item: str):
        manifest = read_manifest(ctx.job_dir)
        unresolved = manifest.get("unresolved_ts", {})
        if item in unresolved:
            # Census #12: the supervisor could not resolve this TS's per-TS star.
            # The TS stays in the manifest so it fails HERE, visibly, instead of
            # silently vanishing from the run.
            raise FileNotFoundError(f"Cannot process '{item}': {unresolved[item]}")
        frame_filenames = manifest["ts_frames"][item]
        self.log(f"{len(frame_filenames)} frames")

        project_frames_dir = ctx.paths.get("frames_dir", ctx.project_path / "frames")
        stage_root = stage_fs_environment(ctx.job_dir, item, frame_filenames, project_frames_dir)
        self.log(f"Staged at: {stage_root}")

        ext = detect_frame_extension(stage_root / "frames")
        return stage_root, ext

    def build_command(self, ctx: DriverContext[FsMotionCtfParams], item: str, staged) -> str:
        _stage_root, ext = staged
        # Compound shell (test/&&) — composed as a string, cwd is the staging root.
        return build_warp_commands(ctx.params, "frames", ext)

    def task_cwd(self, ctx: DriverContext[FsMotionCtfParams], item: str, staged) -> Path:
        stage_root, _ext = staged
        return stage_root

    def verify_outputs(self, ctx: DriverContext[FsMotionCtfParams], item: str, staged) -> None:
        # Census #14: WarpTools writes one XML per frame; a zero/short XML count
        # after exit 0 means degenerate output and must not green-tick.
        stage_root, _ext = staged
        staged_warp = stage_root / "warp_frameseries"
        n_frames = sum(1 for _ in (stage_root / "frames").iterdir())
        n_xml = len(list(staged_warp.glob("*.xml"))) if staged_warp.is_dir() else 0
        if n_xml < n_frames:
            raise FileNotFoundError(
                f"WarpTools reported success but produced {n_xml}/{n_frames} per-frame XMLs in {staged_warp}"
            )

    def collect(self, ctx: DriverContext[FsMotionCtfParams], item: str, staged) -> None:
        self.log("Collecting outputs...")
        collect_fs_outputs(ctx.job_dir, item)


if __name__ == "__main__":
    FsMotionCtfDriver().main()
