#!/usr/bin/env python3
"""
Driver for Warp particle export — supervisor + per-TS SLURM array task.

`WarpTools ts_export_particles --2d` cuts each particle from every tilt image with the
tilt series' full Warp model (local image warps included) and writes one stack per
particle holding its visible tilts. The job reads the same optimisation set as
subtomoExtraction and writes the same envelope.

  - SUPERVISOR: reads the upstream optimisation set; per TS with picks, finds the tsCtf
    job behind its tomogram row (XML, settings, tomostar), stages a one-TS Warp
    environment, writes Warp's input star and the per-tilt star reordered into Warp's
    frame order; submits the array; then merges every TS into particles.star,
    tomograms.star and optimisation_set.star.

  - TASK: runs ts_export_particles inside its staging dir, checks every stack the output
    star names exists, and moves the stacks to <job_dir>/particleseries/<TS>/.

Output layout:
  <job_dir>/
    particles.star            # RELION 5, 2D stacks, centred Å coordinates
    tomograms.star            # upstream tomogram rows, per-tilt stars re-pointed below
    optimisation_set.star
    tilt_series/<TS>.star     # tsCtf per-tilt rows in Warp's frame order
    particleseries/<TS>/*.mrcs
    .staging/task_<TS>/       # settings, tomostar, XML copy, Warp input/output stars
"""

import shutil
from pathlib import Path

import pandas as pd
import starfile

from drivers.array_job_base import (
    STATUS_DIR_NAME,
    ArrayDriver,
    ArrayResults,
    copy_tomostar_with_absolute_paths,
    load_excluded_ts,
    write_skip_status,
)
from drivers.driver_base import DriverContext, ToolCommand, require_producer_input
from drivers.ts_ctf import LOCAL_SETTINGS_NAME, LOCAL_TOMOSTAR_NAME
from services.jobs.warp_export import WarpExportParams
from services.particles.warp_export import (
    check_volume_agrees,
    check_warp_optics,
    optics_key,
    optics_table,
    read_tomostar_movies,
    read_warp_volume,
    relion_particles_from_warp,
    tilt_table_in_warp_order,
    tsctf_sources,
    warp_input_particles,
)
from services.subtomo_merge import (
    _parse_optimisation_set,
    _read_input_particles_lenient,
    _read_particles_star,
    _read_tomograms_star,
    _write_particles_star,
    _write_tomograms_star,
    write_optimisation_set,
)

WARP_INPUT_STAR = "warp_input.star"
TILT_STAR = "tilt_series.star"
OUT_DIR = "out"
PROCESSING_DIR = "warp_tiltseries"
PARTICLESERIES_DIR = "particleseries"


def _task_dir(job_dir: Path, ts: str) -> Path:
    return job_dir / ".staging" / f"task_{ts}"


def _first_table(path: Path) -> pd.DataFrame:
    for block in starfile.read(path, always_dict=True).values():
        if isinstance(block, pd.DataFrame):
            return block
    raise ValueError(f"No table in {path}")


def _stage_ts(task_dir: Path, ts: str, particles: pd.DataFrame, tomo_row: pd.Series) -> None:
    """Everything one Warp call needs, built from the tilt series' own tsCtf job."""
    src = tsctf_sources(tomo_row, LOCAL_SETTINGS_NAME, LOCAL_TOMOSTAR_NAME)
    volume = read_warp_volume(src.settings)
    check_volume_agrees(tomo_row, volume)

    task_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src.settings, task_dir / LOCAL_SETTINGS_NAME)
    (task_dir / LOCAL_TOMOSTAR_NAME).mkdir(exist_ok=True)
    staged_tomostar = task_dir / LOCAL_TOMOSTAR_NAME / src.tomostar.name
    copy_tomostar_with_absolute_paths(src.tomostar, staged_tomostar, src.tomostar.parent)
    # A copy, not a symlink: the export re-saves the XML.
    (task_dir / PROCESSING_DIR).mkdir(exist_ok=True)
    shutil.copy2(src.xml, task_dir / PROCESSING_DIR / src.xml.name)

    starfile.write(
        {"particles": warp_input_particles(particles, ts, volume)}, task_dir / WARP_INPUT_STAR, overwrite=True
    )

    movies = read_tomostar_movies(_first_table(staged_tomostar))
    ordered = tilt_table_in_warp_order(_first_table(src.tilt_star), movies, ts)
    starfile.write({ts: ordered}, task_dir / TILT_STAR, overwrite=True)


class WarpExportDriver(ArrayDriver):
    params_class = WarpExportParams
    job_name = "warp_export"
    driver_script = Path(__file__).resolve()
    poll_secs = 15

    # ---------------- supervisor ----------------

    def whole_job_short_circuit(self, ctx: DriverContext[WarpExportParams]) -> bool:
        params = ctx.params
        if params.box_size < 2 or params.box_size % 2:
            raise ValueError(f"box_size must be even and positive (Warp's --box), got {params.box_size}")
        if params.binning <= 0 or params.diameter_ang <= 0:
            raise ValueError(f"binning and diameter_ang must be positive, got {params.binning}, {params.diameter_ang}")

        input_optimisation = ctx.paths["input_optimisation"]
        require_producer_input(input_optimisation, "Input optimisation set")
        particles_star, tomograms_star = _parse_optimisation_set(input_optimisation)
        self.log(f"Upstream particles: {particles_star}")
        self.log(f"Upstream tomograms: {tomograms_star}")

        self._input_optimisation = input_optimisation
        self._particles_df, _, _ = _read_input_particles_lenient(particles_star)
        self._tomograms_df = _read_tomograms_star(tomograms_star)

        if self._particles_df.empty:
            status_dir = ctx.job_dir / STATUS_DIR_NAME
            for ts in self._tomograms_df["rlnTomoName"].astype(str):
                write_skip_status(status_dir, ts, reason="no candidates anywhere (upstream picks=0)")
            self.log("Upstream produced 0 picks; nothing to export (every TS marked skip).")
            return True
        return False

    def enumerate_items(self, ctx: DriverContext[WarpExportParams]) -> list[str]:
        all_ts = self._tomograms_df["rlnTomoName"].astype(str).tolist()
        with_picks = set(self._particles_df["rlnTomoName"].astype(str))
        orphans = sorted(with_picks - set(all_ts))
        if orphans:
            raise ValueError(f"Picks name tilt series absent from the upstream tomograms.star: {orphans[:10]}")
        excluded = load_excluded_ts(ctx.project_path)
        self._empty_ts = [t for t in all_ts if t not in with_picks]
        self._ts_with_picks = [t for t in all_ts if t in with_picks and t not in excluded]
        self.log(
            f"{len(self._ts_with_picks)} TS with picks to export; {len(self._empty_ts)} without picks (skip); "
            f"{len([t for t in all_ts if t in excluded])} excluded from processing"
        )
        return all_ts

    def preflight_scope(self, ctx: DriverContext[WarpExportParams], items: list[str]) -> list[str]:
        return self._ts_with_picks

    def pre_dispatch(self, ctx: DriverContext[WarpExportParams], items: list[str]) -> None:
        status_dir = ctx.job_dir / STATUS_DIR_NAME
        if status_dir.is_dir():
            for f in status_dir.glob("*.skip"):
                f.unlink()
        for ts in self._empty_ts:
            write_skip_status(status_dir, ts, reason="no candidates above template-matching threshold")

        tomo_rows = self._tomograms_df.set_index(self._tomograms_df["rlnTomoName"].astype(str))
        names = self._particles_df["rlnTomoName"].astype(str)
        for ts in self._ts_with_picks:
            _stage_ts(_task_dir(ctx.job_dir, ts), ts, self._particles_df[names == ts], tomo_rows.loc[ts])
        self.log(f"Staged {len(self._ts_with_picks)} TS under {ctx.job_dir / '.staging'}")

    def manifest_extras(self, ctx: DriverContext[WarpExportParams], items: list[str]) -> dict:
        return {"input_optimisation_star": str(self._input_optimisation)}

    def aggregate(self, ctx: DriverContext[WarpExportParams], results: ArrayResults) -> None:
        params = ctx.params
        premultiplied = not params.dont_premultiply
        dropped = set(results.dropped)
        exported = [ts for ts in self._ts_with_picks if ts not in dropped]
        tomo_rows = self._tomograms_df.set_index(self._tomograms_df["rlnTomoName"].astype(str))

        tilt_dir = ctx.job_dir / "tilt_series"
        tilt_dir.mkdir(exist_ok=True)
        per_ts: list[pd.DataFrame] = []
        keys: list[tuple] = []
        output_apix: float | None = None
        kept_rows = []
        empty: list[str] = []
        for ts in exported:
            task_dir = _task_dir(ctx.job_dir, ts)
            tomo_row = tomo_rows.loc[ts]
            warp_optics, warp_particles, _ = _read_particles_star(
                task_dir / OUT_DIR / "particles.star", allow_empty_particles=True
            )
            ts_apix = check_warp_optics(warp_optics, tomo_row, box=params.box_size, premultiplied=premultiplied)
            if output_apix is None:
                output_apix = ts_apix
            elif abs(ts_apix - output_apix) > 1e-4:
                raise ValueError(f"{ts}: output pixel size {ts_apix} Å, other tilt series {output_apix} Å")

            volume = read_warp_volume(task_dir / LOCAL_SETTINGS_NAME)
            n_frames = len(_first_table(task_dir / TILT_STAR))
            particles = relion_particles_from_warp(
                warp_particles,
                ts,
                volume,
                output_apix=ts_apix,
                n_frames=n_frames,
                image_dir=(ctx.job_dir / PARTICLESERIES_DIR / ts).resolve(),
            )
            if particles.empty:
                empty.append(ts)
                continue

            key = optics_key(tomo_row)
            if key not in keys:
                keys.append(key)
            particles["rlnOpticsGroup"] = keys.index(key) + 1
            per_ts.append(particles)

            tilt_star = tilt_dir / f"{ts}.star"
            shutil.copy2(task_dir / TILT_STAR, tilt_star)
            row = tomo_row.copy()
            row["rlnTomoName"] = ts
            row["rlnTomoTiltSeriesStarFile"] = str(tilt_star.resolve())
            if "rlnTomoFrameCount" in row.index:
                row["rlnTomoFrameCount"] = n_frames
            kept_rows.append(row)

        if not per_ts:
            raise RuntimeError(f"No particles left after export across {len(exported)} tilt series")

        particles_df = pd.concat(per_ts, ignore_index=True)
        optics_df = optics_table(keys, output_apix=output_apix, box=params.box_size, premultiplied=premultiplied)
        _write_particles_star(
            ctx.job_dir / "particles.star",
            optics_df=optics_df,
            particles_df=particles_df,
            general_kv={"rlnTomoSubTomosAre2DStacks": 1},
        )
        _write_tomograms_star(ctx.job_dir / "tomograms.star", pd.DataFrame(kept_rows).reset_index(drop=True))
        write_optimisation_set(
            ctx.job_dir / "optimisation_set.star",
            particles_star=ctx.job_dir / "particles.star",
            tomograms_star=ctx.job_dir / "tomograms.star",
        )
        self.log(
            f"Merged {len(particles_df)} particles from {len(per_ts)}/{len(exported)} TS at {output_apix:.4g} Å/px "
            f"→ {ctx.job_dir / 'particles.star'}"
        )
        if empty:
            self.log(f"{len(empty)} TS exported no particle visible in any tilt: {empty}")

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[WarpExportParams], item: str) -> bool:
        done = (_task_dir(ctx.job_dir, item) / OUT_DIR / "particles.star").exists() and (
            ctx.job_dir / PARTICLESERIES_DIR / item
        ).is_dir()
        if done:
            self.log(f"{item} already exported — skipping")
        return done

    def stage(self, ctx: DriverContext[WarpExportParams], item: str) -> Path:
        task_dir = _task_dir(ctx.job_dir, item)
        if not (task_dir / WARP_INPUT_STAR).exists():
            raise FileNotFoundError(f"Staged Warp input not found: {task_dir / WARP_INPUT_STAR}")
        out_dir = task_dir / OUT_DIR
        if out_dir.exists():
            shutil.rmtree(out_dir)
        stale = task_dir / PROCESSING_DIR / PARTICLESERIES_DIR
        if stale.exists():
            shutil.rmtree(stale)
        out_dir.mkdir()
        return task_dir

    def build_command(self, ctx: DriverContext[WarpExportParams], item: str, staged: Path) -> ToolCommand:
        params = ctx.params
        volume = read_warp_volume(staged / LOCAL_SETTINGS_NAME)
        cmd = (
            ToolCommand("WarpTools ts_export_particles")
            .opt("--settings", LOCAL_SETTINGS_NAME)
            .opt("--input_processing", PROCESSING_DIR)
            .opt("--output_processing", PROCESSING_DIR)
            .opt("--input_star", WARP_INPUT_STAR)
            .opt("--coords_angpix", volume.apix)
            .opt("--output_star", f"{OUT_DIR}/particles.star")
            .opt("--output_angpix", round(volume.apix * params.binning, 4))
            .opt("--box", params.box_size)
            .opt("--diameter", round(params.diameter_ang))
            .flag("--2d")
            .opt("--max_missing_tilts", params.max_missing_tilts)
            .opt("--perdevice", params.perdevice)
        )
        if params.dont_premultiply:
            cmd.flag("--dont_premultiply")
        return cmd

    def task_cwd(self, ctx: DriverContext[WarpExportParams], item: str, staged: Path) -> Path:
        # Relative paths keep Warp inside the staging dir: only this TS's tomostar is in scope.
        return staged

    def task_binds(self, ctx: DriverContext[WarpExportParams], item: str, staged: Path) -> list:
        return [staged.resolve()]

    def verify_outputs(self, ctx: DriverContext[WarpExportParams], item: str, staged: Path) -> None:
        out_star = staged / OUT_DIR / "particles.star"
        if not out_star.exists():
            raise RuntimeError(f"ts_export_particles wrote no {out_star} (Warp skipped {item})")
        _, particles, _ = _read_particles_star(out_star, allow_empty_particles=True)
        missing = [p for p in particles["rlnImageName"].astype(str) if not (staged / p).exists()]
        if missing:
            raise RuntimeError(
                f"{len(missing)}/{len(particles)} particle stacks named in {out_star} are missing: {missing[:3]}"
            )

    def collect(self, ctx: DriverContext[WarpExportParams], item: str, staged: Path) -> None:
        src = staged / PROCESSING_DIR / PARTICLESERIES_DIR / item
        if not src.is_dir():
            raise RuntimeError(f"ts_export_particles wrote no particle series dir {src}")
        dst = ctx.job_dir / PARTICLESERIES_DIR / item
        dst.parent.mkdir(exist_ok=True)
        if dst.exists():
            shutil.rmtree(dst)
        shutil.move(str(src), str(dst))


if __name__ == "__main__":
    WarpExportDriver().main()
