#!/usr/bin/env python3
"""
Pick candidates from a feature's segmentations: the segmentation half of the Pick candidates driver.
drivers/extract_candidates_pytom.py starts it when the job's source is segmentation, in the supervisor and in
every array task.

- Supervisor: reads the segmentations' manifest (a Segment job's, or an import's) and checks each volume of the
  chosen feature against the frame the picks will live on: the tomograms star the segmented tomograms trace to.
  One array task per tomogram with a volume. Afterwards merges the per-tomogram rows into candidates.star, copies
  the frame as tomograms.star, and writes a coordinates-only optimisation_set.star and summary.json.
- Task: Ais's blob pick (`ais pick`, in the easymode image, one process) on the tomogram's volume, linked alone
  into ais/<tomo>/in; then, in this environment, its picks become candidate rows (services/particles/seg_picks.py)
  with the context of the segmentations' other features (services/particles/pick_context.py): picks/<tomo>.star and
  picks/<tomo>.json. Ais's own star stays in ais/<tomo>/out as the raw record.
"""

import json
import os
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path

import mrcfile
import numpy as np
import pandas as pd
import starfile

from drivers.array_job_base import ArrayDriver, ArrayResults, preflight_origins, preflight_registry
from drivers.candidate_views import write_candidate_views
from drivers.driver_base import DriverContext, ToolCommand, print_cmd_only, require_producer_input
from services.io_slots import JobFileType
from services.job_models import CandidateExtractPytomParams
from services.models_base import JobType, PickOrientation
from services.particles.coords import pixel_size_from_tomo_row
from services.particles.pick_context import context_columns, tomogram_context
from services.particles.seg_picks import (
    AIS_ORIENT,
    EDGE,
    MARK_COL,
    OUTPUT_COLUMNS,
    SCORE_COL,
    Detection,
    ais_size_px,
    ais_threshold,
    job_seed,
    min_piece_voxels,
    pick_rows,
    tomogram_rng,
)
from services.particles.set_origins import origins_of
from services.segmentation.manifest import SegmentationEntry, SegmentationManifest
from services.segmentation.search_region import read_segmentations
from services.segmentation.volumes import entry_dtype
from services.subtomo_merge import _read_tomograms_star, write_optimisation_set

AIS_DIR = "ais"
PICKS_DIR = "picks"
# A segmentation's voxel and its tomogram row's pixel size may differ by this fraction at most.
VOXEL_TOLERANCE = 0.01


@dataclass(frozen=True)
class Staged:
    entry: SegmentationEntry
    dtype: np.dtype
    work: Path  # ais/<tomo>: in/, out/ and Ais's config dirs


def frame_star(manifest: SegmentationManifest, state, project_path: Path) -> Path:
    """The tomograms star the segmented tomograms trace to: the one a Segment job read, or, for an import, the
    output of the reconstruct job it was registered on."""
    named = manifest.parameters.get("input_tomograms")
    if named:
        return Path(str(named))
    recon = manifest.parameters.get("recon_instance")
    if not recon:
        raise ValueError(
            f"The segmentations of {manifest.producer} name neither the tomograms star they segmented nor the "
            "reconstruct job they were computed on"
        )
    jm = state.jobs.get(recon)
    if jm is None or jm.job_type != JobType.TS_RECONSTRUCT or not jm.relion_job_name:
        raise ValueError(
            f"The segmentations of {manifest.producer} were computed on {recon}, which is not a reconstruct job of "
            "this project with a job directory"
        )
    slot = next(s for s in type(jm).OUTPUT_SCHEMA if s.produces == JobFileType.TOMOGRAMS_STAR)
    return Path(project_path) / jm.relion_job_name.rstrip("/") / slot.path_template


def detection(p: CandidateExtractPytomParams) -> Detection:
    return Detection(
        p_threshold=p.p_threshold,
        min_piece=p.min_piece,
        one_per_piece=p.one_per_piece,
        binning=p.binning,
        orientation=p.effective_orientation,
        diameter_ang=p.particle_diameter_ang,
    )


def read_star_block(path: Path) -> pd.DataFrame:
    """The first table of a star file; a star with no rows has its columns."""
    for block in starfile.read(path, always_dict=True).values():
        if isinstance(block, pd.DataFrame):
            return block
    return pd.DataFrame()


def context_features(manifest: SegmentationManifest, feature: str) -> list[str]:
    """The features every pick records as context: all the segmentations hold but the one picked, whose score
    already says it."""
    return sorted(set(manifest.features()) - {feature})


class PickFromSegmentationDriver(ArrayDriver):
    params_class = CandidateExtractPytomParams
    job_name = "pick_from_segmentation"
    # Every mode starts through the Pick candidates dispatcher, which reads the job's source.
    driver_script = Path(__file__).resolve().with_name("extract_candidates_pytom.py")
    poll_secs = 15

    _read: tuple[Path, SegmentationManifest, dict[str, SegmentationEntry]] | None = None
    _frame: Path | None = None
    _origins: dict | None = None

    def _volumes(self, ctx: DriverContext[CandidateExtractPytomParams]):
        """The segmentations input, its manifest, and the chosen feature's volume of each tomogram; read once."""
        if self._read is None:
            seg_json = ctx.paths.get("input_segmentations")
            if seg_json is None:
                raise FileNotFoundError("No segmentations input was resolved for this job")
            require_producer_input(seg_json, "Input segmentations")
            manifest = read_segmentations(seg_json)
            feature = ctx.params.feature.strip()
            if not feature:
                raise ValueError("No feature is chosen to pick from the segmentations")
            listed = [e for e in manifest.entries if e.feature == feature]
            if not listed:
                raise ValueError(f"{seg_json} has no volumes of {feature!r}; it holds {manifest.features()}")
            entries = {e.tomogram: e for e in listed}
            if len(entries) < len(listed):
                raise ValueError(f"{seg_json} lists a tomogram's {feature} volume more than once")
            self._read = (seg_json, manifest, entries)
        return self._read

    # ---------------- supervisor ----------------

    def enumerate_items(self, ctx: DriverContext[CandidateExtractPytomParams]) -> list[str]:
        p = ctx.params
        seg_json, manifest, entries = self._volumes(ctx)
        frame = frame_star(manifest, ctx.state, ctx.project_path)
        require_producer_input(frame, "The tomograms star the segmentations trace to")
        tomo_df = _read_tomograms_star(frame)
        rows = tomo_df.set_index(tomo_df["rlnTomoName"].astype(str))
        absent = sorted(t for t in entries if t not in rows.index)
        if absent:
            raise ValueError(
                f"{frame} has no rows for {len(absent)} tomogram(s) with a {p.feature} volume: {absent[:10]}"
            )
        for tomo, e in sorted(entries.items()):
            apix = pixel_size_from_tomo_row(rows.loc[tomo])
            if abs(apix - e.voxel_angstrom) > VOXEL_TOLERANCE * e.voxel_angstrom:
                raise ValueError(
                    f"{tomo}: the segmentation's voxel is {e.voxel_angstrom:g} Å but its tomogram row says {apix:g} Å; "
                    "the picks would land in the wrong place"
                )
            if not (np.isfinite(e.full_scale) and e.full_scale > 0):
                raise ValueError(f"{e.path.name}: the manifest states no full scale ({e.full_scale!r}) to read p by")
        self._frame = frame
        self._origins = origins_of(tomo_df)
        self.log(f"Segmentations: {seg_json} ({manifest.producer}, {manifest.method})")
        self.log(f"{len(entries)} tomogram(s) with a {p.feature} volume; frame: {frame}")
        orient = p.effective_orientation.value
        self.log(
            f"Detection: p ≥ {p.p_threshold:g}, pieces ≥ {p.min_piece:g} of a sphere of Ø {p.particle_diameter_ang:g} "
            f"Å, binning {p.binning}, {'one pick per piece' if p.one_per_piece else 'peaks a Ø apart'}; angles {orient}"
        )
        return sorted(entries)

    def preflight(self, ctx: DriverContext[CandidateExtractPytomParams], items: list[str]) -> None:
        """Against this project's registry, or, for tomograms of other projects, each against its own's."""
        if self._origins is None:
            preflight_registry(ctx.project_path, items, job_name=self.job_name)
        else:
            preflight_origins(self._origins, items, job_name=self.job_name)

    def excluded_items(self, ctx: DriverContext[CandidateExtractPytomParams]) -> set[str] | None:
        # Excluding a tilt series of another project is that project's decision.
        return set() if self._origins is not None else None

    def manifest_extras(self, ctx: DriverContext[CandidateExtractPytomParams], items: list[str]) -> dict:
        seg_json, _, _ = self._volumes(ctx)
        return {
            "segmentations": str(seg_json),
            "feature": ctx.params.feature,
            "tomograms_star": str(self._frame),
            "seed": job_seed(ctx.instance_id),
        }

    def aggregate(self, ctx: DriverContext[CandidateExtractPytomParams], results: ArrayResults) -> None:
        p = ctx.params
        job_dir = ctx.job_dir
        seg_json, manifest, _ = self._volumes(ctx)
        blocks, per_tomo = [], {}
        for item in results.ok:
            per_tomo[item] = json.loads((job_dir / PICKS_DIR / f"{item}.json").read_text())
            rows = read_star_block(job_dir / PICKS_DIR / f"{item}.star")
            if len(rows):
                blocks.append(rows)
        features = context_features(manifest, p.feature)
        empty = pd.DataFrame(columns=[*OUTPUT_COLUMNS, *context_columns(features)])
        table = pd.concat(blocks, ignore_index=True) if blocks else empty

        summary = {
            "source": "segmentation",
            "feature": p.feature,
            "segmentations": str(seg_json),
            "producer": manifest.producer,
            "tomograms_star": str(self._frame),
            "detection": {
                "p_threshold": p.p_threshold,
                "min_piece": p.min_piece,
                "one_per_piece": p.one_per_piece,
                "binning": p.binning,
                "orientation": p.effective_orientation.value,
                "diameter_ang": p.particle_diameter_ang,
                "seed": job_seed(ctx.instance_id),
            },
            "context_features": features,
            "tomograms": len(per_tomo),
            "tomograms_with_picks": sum(1 for s in per_tomo.values() if s["picks"]),
            "picks": len(table),
            "edge": int((table[MARK_COL] == EDGE).sum()),
            "median_score": float(table[SCORE_COL].median()) if len(table) else None,
            "skipped": results.skipped,
            "dropped": results.dropped,
            "per_tomogram": per_tomo,
        }
        (job_dir / "summary.json").write_text(json.dumps(summary, indent=2))
        self.log(
            f"{summary['picks']} picks in {summary['tomograms_with_picks']} of {summary['tomograms']} tomograms; "
            f"{summary['edge']} within ½ Ø of the volume's edge (marked)"
        )
        if table.empty:
            raise RuntimeError(
                f"No picks of {p.feature} in any tomogram: see summary.json (pieces per tomogram), the p threshold "
                "and the min piece"
            )

        candidates_star = job_dir / "candidates.star"
        starfile.write({"particles": table}, candidates_star, overwrite=True)
        output_tomograms = job_dir / "tomograms.star"
        shutil.copy2(self._frame, output_tomograms)
        write_candidate_views(ctx, candidates_star, output_tomograms, self.log)
        write_optimisation_set(
            job_dir / "optimisation_set.star", particles_star=candidates_star, tomograms_star=output_tomograms
        )
        self.log("Wrote candidates.star, tomograms.star and optimisation_set.star")

    # ---------------- task ----------------

    def task_already_done(self, ctx: DriverContext[CandidateExtractPytomParams], item: str) -> bool:
        out = ctx.job_dir / PICKS_DIR / f"{item}.star"
        if out.is_file() and out.stat().st_size > 0:
            self.log(f"Picks already written, skipping: {out}")
            return True
        return False

    def stage(self, ctx: DriverContext[CandidateExtractPytomParams], item: str) -> Staged:
        """The tomogram's volume, checked against its manifest entry, linked alone into a fresh ais/<tomo>/in:
        `ais pick` picks every volume of the feature in the folder it is given."""
        _, _, entries = self._volumes(ctx)
        entry = entries[item]
        dtype = entry_dtype(entry)
        work = ctx.job_dir / AIS_DIR / item
        if work.exists():
            shutil.rmtree(work)
        for sub in ("in", "out", ".config", ".mpl"):
            (work / sub).mkdir(parents=True)
        (work / "in" / f"{item}__{ctx.params.feature}.mrc").symlink_to(entry.path.resolve())
        return Staged(entry=entry, dtype=dtype, work=work)

    def build_command(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged: Staged) -> ToolCommand:
        p = ctx.params
        e = staged.entry
        env = " ".join(
            f"{k}={shlex.quote(str(v))}"
            for k, v in (("AIS_CONFIG_DIR", staged.work / ".config"), ("MPLCONFIGDIR", staged.work / ".mpl"))
        )
        min_voxels = min_piece_voxels(p.min_piece, p.particle_diameter_ang, e.voxel_angstrom, p.binning)
        cmd = (
            ToolCommand(f"{env} ais pick")
            .opt_path("-d", staged.work / "in", quote=True)
            .opt("-t", shlex.quote(p.feature))
            .opt_path("-ou", staged.work / "out", quote=True)
            .opt("-threshold", ais_threshold(p.p_threshold, staged.dtype, e.full_scale))
            # Spacing and size in voxels of the volume: Ais would otherwise convert Å by the voxel size in the
            # volume's header, which an imported volume may not carry (Ais then takes 10 Å).
            .opt("-spacing-px", p.particle_diameter_ang / e.voxel_angstrom)
            .opt("-size-px", ais_size_px(min_voxels, p.binning))
            .opt("-b", p.binning)
            # The margin: picks near the edge are kept and marked. `-margin` would parse as `-m argin`.
            .opt("-m", 0)
            .opt("-p", 1)
        )
        if p.one_per_piece:
            cmd.flag("-centroid")
        orientation = p.effective_orientation
        if orientation != PickOrientation.RANDOM:
            cmd.opt("-orient", AIS_ORIENT[orientation])
        return cmd

    def task_binds(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged: Staged) -> list:
        return [str(staged.entry.path.resolve().parent)]

    def execute(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged: Staged) -> None:
        super().execute(ctx, item, staged)
        if not print_cmd_only():
            self._write_picks(ctx, item, staged)

    def _write_picks(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged: Staged) -> None:
        """Ais's picks of this tomogram as candidate rows, with their summary; the star last, in one rename, since
        its presence marks the tomogram done."""
        p = ctx.params
        e = staged.entry
        ais_star = staged.work / "out" / f"{item}__{p.feature}_coords.star"
        if not ais_star.is_file():
            raise FileNotFoundError(f"ais pick exited 0 but wrote no {ais_star.name}")
        ais = read_star_block(ais_star)
        with mrcfile.mmap(e.path, mode="r", permissive=True) as m:
            rows, summary = pick_rows(
                ais,
                m.data,
                tomo=item,
                voxel_angstrom=e.voxel_angstrom,
                full_scale=e.full_scale,
                det=detection(p),
                rng=tomogram_rng(job_seed(ctx.instance_id), item),
            )
        _, manifest, _ = self._volumes(ctx)
        others = {c.feature: c for c in manifest.entries if c.tomogram == item and c.feature != p.feature}
        for c in others.values():
            if tuple(c.shape_zyx) != tuple(e.shape_zyx) or abs(c.voxel_angstrom - e.voxel_angstrom) > (
                VOXEL_TOLERANCE * e.voxel_angstrom
            ):
                raise ValueError(
                    f"{item}: its {c.feature} volume ({tuple(c.shape_zyx)} at {c.voxel_angstrom:g} Å) is not on the "
                    f"grid of its {p.feature} volume ({tuple(e.shape_zyx)} at {e.voxel_angstrom:g} Å)"
                )
        rows, summary["context"] = tomogram_context(
            rows, others, context_features(manifest, p.feature), p.particle_diameter_ang
        )
        out = ctx.job_dir / PICKS_DIR
        out.mkdir(exist_ok=True)
        (out / f"{item}.json").write_text(json.dumps(summary, indent=2))
        tmp = out / f".{item}.star.tmp"
        starfile.write({"particles": rows}, tmp, overwrite=True)
        os.replace(tmp, out / f"{item}.star")
        median = f"{summary['median_score']:.2f}" if summary["median_score"] is not None else "-"
        self.log(
            f"{summary['picks']} picks in {summary['pieces']} pieces (median score {median}, {summary['edge']} near "
            f"the edge); angles: {summary['angle_sources'] or 'none'}"
        )

    def verify_outputs(self, ctx: DriverContext[CandidateExtractPytomParams], item: str, staged: Staged) -> None:
        out = ctx.job_dir / PICKS_DIR / f"{item}.star"
        if not out.is_file():
            raise FileNotFoundError(f"The picks of {item} were not written: {out}")
