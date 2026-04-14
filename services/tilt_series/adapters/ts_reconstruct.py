"""ts_reconstruct ingest adapter — registry-driven replacement for
`MetadataTranslator.update_ts_reconstruct_metadata`.

Flow:

    1. Ingest: for every TS in scope, locate the reconstruction MRCs under
       `{job_dir}/warp_tiltseries/reconstruction/`, read dims from the MRC
       header, attach a `TsReconstructTomogramOutput` to the TS's Tomogram
       via the registry.

    2. emit_star: read the input STAR (tsCTF's output — carries the
       rlnTomoSizeX/Y/Z values written by tsAlignment), overlay
       reconstruction paths + binning + pixel-size columns on each TS row,
       write `tomograms.star` with absolute paths for downstream consumption.

Identity is TS-scoped only — there's exactly one reconstructed tomogram per
TS in v1, so there's no per-frame resolution to manage.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from services.configs.starfile_service import StarfileService
from services.tilt_series.models import TsReconstructTomogramOutput
from services.tilt_series.registry import TiltSeriesRegistry

logger = logging.getLogger(__name__)


class TsReconstructIngestAdapter:
    def __init__(
        self,
        registry: TiltSeriesRegistry,
        job_dir: Path,
        *,
        job_instance_id: str = "tsReconstruct",
        warp_folder: str = "warp_tiltseries",
        starfile_service: Optional[StarfileService] = None,
    ):
        self.registry = registry
        self.job_dir = Path(job_dir)
        self.job_instance_id = job_instance_id
        self.warp_folder = warp_folder
        self.rec_dir = (self.job_dir / warp_folder / "reconstruction").resolve()
        self.starfile_service = starfile_service or StarfileService()

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(
        self,
        expected_ts_ids: Iterable[str],
        *,
        rescale_angpixs: float,
        frame_pixel_size: float,
    ) -> None:
        """Populate the registry with per-TS reconstruction outputs.

        `rescale_angpixs` is the reconstructed-tomogram pixel size (params.rescale_angpixs);
        `frame_pixel_size` is the raw frame pixel size. Binning = rescale / frame.
        """
        expected = sorted(set(expected_ts_ids))
        if not expected:
            raise ValueError("ingest called with no expected TS ids")

        if frame_pixel_size <= 0:
            raise ValueError(f"frame_pixel_size must be > 0, got {frame_pixel_size}")
        if rescale_angpixs <= 0:
            raise ValueError(f"rescale_angpixs must be > 0, got {rescale_angpixs}")

        missing_in_registry = [t for t in expected if not self.registry.has_tilt_series(t)]
        if missing_in_registry:
            raise RuntimeError(
                f"tsReconstruct ingest aborted. TS missing from registry: {missing_in_registry}. "
                f"Reload the project to backfill the registry from mdocs."
            )

        rec_res = f"{rescale_angpixs:.2f}"
        binning = rescale_angpixs / frame_pixel_size

        problems: Dict[str, str] = {}
        for ts_id in expected:
            rec_path = self.rec_dir / f"{ts_id}_{rec_res}Apx.mrc"
            half1_path = self.rec_dir / "even" / f"{ts_id}_{rec_res}Apx.mrc"
            half2_path = self.rec_dir / "odd" / f"{ts_id}_{rec_res}Apx.mrc"

            if not rec_path.exists():
                problems[ts_id] = f"reconstruction MRC missing at {rec_path}"
                continue

            try:
                size_x, size_y, size_z = self._read_mrc_dims(rec_path)
            except Exception as e:
                problems[ts_id] = f"could not read MRC header from {rec_path}: {e}"
                continue

            output = TsReconstructTomogramOutput(
                job_instance_id=self.job_instance_id,
                job_dir=self.job_dir,
                reconstructed_mrc=rec_path,
                half1_mrc=half1_path,
                half2_mrc=half2_path,
                binning=binning,
                tomogram_pixel_size_angstrom=rescale_angpixs,
                size_x=size_x,
                size_y=size_y,
                size_z=size_z,
            )
            self.registry.attach_tomogram_output(ts_id, output)
            logger.info(
                "tsReconstruct: ingested %s (%dx%dx%d, %.2f A/px)",
                ts_id, size_x, size_y, size_z, rescale_angpixs,
            )

        if problems:
            detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(problems.items()))
            raise RuntimeError(
                f"tsReconstruct ingest failed for {len(problems)} tilt-series:\n  - {detail}"
            )

    def emit_star(self, input_star_path: Path, output_star_path: Path) -> None:
        """Write `tomograms.star` — the single-block global STAR that carries
        reconstruction paths + pixel size + binning for downstream job types.

        All paths in the output are absolute (matching the pre-refactor writer):
        - rlnTomoReconstructedTomogram, half1, half2
        - rlnTomoTiltSeriesStarFile (resolved against the input STAR's dir)
        """
        in_star_data = self.starfile_service.read(input_star_path)
        in_ts_df = in_star_data.get("global")
        if in_ts_df is None:
            raise ValueError(f"No 'global' block in {input_star_path}")

        in_star_dir = input_star_path.parent
        out_ts_df = in_ts_df.copy()

        problems: List[str] = []
        for idx, row in out_ts_df.iterrows():
            ts_id = str(row["rlnTomoName"])
            if not self.registry.has_tilt_series(ts_id):
                problems.append(f"{ts_id}: not in registry")
                continue
            ts = self.registry.get_tilt_series(ts_id)
            tomogram = ts.tomogram
            rec_output = None
            if tomogram is not None:
                rec_output = tomogram.outputs.get(self.job_instance_id)
            if rec_output is None or rec_output.output_type != "ts_reconstruct":
                problems.append(f"{ts_id}: no ingested tsReconstruct output in registry")
                continue

            out_ts_df.at[idx, "rlnTomoReconstructedTomogram"] = str(rec_output.reconstructed_mrc)
            out_ts_df.at[idx, "rlnTomoReconstructedTomogramHalf1"] = str(rec_output.half1_mrc)
            out_ts_df.at[idx, "rlnTomoReconstructedTomogramHalf2"] = str(rec_output.half2_mrc)
            # tomogram_pixel_size / binning = rescale / (rescale / frame_pixel_size) = frame_pixel_size
            frame_angpix = rec_output.tomogram_pixel_size_angstrom / rec_output.binning
            out_ts_df.at[idx, "rlnTomoTiltSeriesPixelSize"] = frame_angpix
            out_ts_df.at[idx, "rlnTomoTomogramBinning"] = rec_output.binning

            # Resolve the per-TS tilt_series STAR to an absolute path. The
            # legacy writer did this with a silent warning on miss; we fail
            # loud instead.
            ts_star_rel = row["rlnTomoTiltSeriesStarFile"]
            ts_star_abs = (in_star_dir / ts_star_rel).resolve()
            if not ts_star_abs.exists():
                problems.append(f"{ts_id}: per-TS STAR not found at {ts_star_abs}")
                continue
            out_ts_df.at[idx, "rlnTomoTiltSeriesStarFile"] = str(ts_star_abs)

        if problems:
            raise RuntimeError(
                f"tsReconstruct emit_star: {len(problems)} problem(s):\n  - "
                + "\n  - ".join(problems)
            )

        output_star_path.parent.mkdir(parents=True, exist_ok=True)
        self.starfile_service.write({"global": out_ts_df}, output_star_path)
        logger.info("tsReconstruct: wrote output STAR to %s", output_star_path)

    # ── Internals ──────────────────────────────────────────────────────────

    @staticmethod
    def _read_mrc_dims(path: Path) -> tuple[int, int, int]:
        import mrcfile
        with mrcfile.open(str(path), header_only=True, mode="r") as mrc:
            # MRC header is (nz, ny, nx) in many conventions; mrcfile normalizes
            # to data.shape = (z, y, x). Use header fields for reliability.
            return int(mrc.header.nx), int(mrc.header.ny), int(mrc.header.nz)
