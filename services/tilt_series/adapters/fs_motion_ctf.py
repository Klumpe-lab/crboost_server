"""fs_motion_and_ctf ingest adapter — registry-driven replacement for
`MetadataTranslator.update_fs_motion_and_ctf_metadata`.

Flow:

    1. Ingest: for every Frame of every TS in scope, locate the per-movie XML
       at `{job_dir}/warp_frameseries/{frame.id}.xml`, parse its <CTF> block,
       attach a `FsMotionCtfFrameOutput` to the Frame via the registry.

    2. emit_star: read the input STAR (ts_import's tilt_series.star — has no
       motion/CTF columns yet), overlay motion + CTF values from the registry
       onto each tilt row, write out the same hierarchical layout with a
       global block + per-TS STARs under `{output_dir}/tilt_series/`.

Identity is resolved per-frame via the registry — `TS.frame_by_filename(movie)`
returns a Frame whose `id` is the lookup key into the registry's per-frame
outputs. No `Path(stem)` heuristics, no `_EER` replace chains.

The per-movie XMLs produced by WarpTools fs_motion_and_ctf are named
`{frame_stem}.xml` where `frame_stem == Path(raw_frame_filename).stem`, which
is exactly `Frame.id`. So the lookup is a direct `job_dir/warp_frameseries/
{frame.id}.xml`.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from services.configs.starfile_service import StarfileService
from services.tilt_series.models import FsMotionCtfFrameOutput, TiltSeries
from services.tilt_series.registry import TiltSeriesRegistry

logger = logging.getLogger(__name__)


# Placeholder values the legacy writer emitted — preserved so downstream RELION
# schemas keep working. These are per-frame columns that fs_motion doesn't
# actually compute; they exist only because the RELION STAR schema expects them.
_LEGACY_MOTION_PLACEHOLDER = 0.000001


class FsMotionCtfIngestAdapter:
    def __init__(
        self,
        registry: TiltSeriesRegistry,
        job_dir: Path,
        *,
        job_instance_id: str = "fsMotionAndCtf",
        warp_folder: str = "warp_frameseries",
        starfile_service: Optional[StarfileService] = None,
    ):
        self.registry = registry
        self.job_dir = Path(job_dir)
        self.job_instance_id = job_instance_id
        self.warp_folder = warp_folder
        self.warp_dir = self.job_dir / warp_folder
        self.starfile_service = starfile_service or StarfileService()

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(self, expected_ts_ids: Iterable[str]) -> None:
        """Populate registry with per-frame motion+CTF outputs for every frame in
        each expected TS. Fail loud on missing XMLs."""
        expected = sorted(set(expected_ts_ids))
        if not expected:
            raise ValueError("ingest called with no expected TS ids")

        missing_in_registry = [t for t in expected if not self.registry.has_tilt_series(t)]
        if missing_in_registry:
            raise RuntimeError(
                f"fs_motion_and_ctf ingest aborted. "
                f"TS missing from registry: {missing_in_registry}. "
                f"Reload the project to backfill the registry from mdocs."
            )

        unresolved: List[str] = []
        for ts_id in expected:
            ts = self.registry.get_tilt_series(ts_id)
            for frame in ts.frames:
                xml_path = self.warp_dir / f"{frame.id}.xml"
                if not xml_path.exists():
                    unresolved.append(f"{ts_id}/{frame.id}: {xml_path}")
                    continue
                output = self._build_frame_output(frame.id, xml_path)
                self.registry.attach_frame_output(frame.id, output)
            logger.info("fs_motion_and_ctf: ingested %d frames for TS %s", ts.frame_count, ts_id)

        if unresolved:
            sample = unresolved[:5]
            suffix = "..." if len(unresolved) > 5 else ""
            raise RuntimeError(
                f"fs_motion_and_ctf ingest: {len(unresolved)} frame XML(s) missing "
                f"(sample): {sample}{suffix}"
            )

    def emit_star(self, input_star_path: Path, output_star_path: Path, project_root: Path) -> None:
        """Write the hierarchical STAR (global block + per-TS STARs) consumed by
        the ts_alignment job.

        `project_root` is needed because the ts_import input STAR uses paths
        relative to project root (RELION convention), while subsequent job STARs
        use paths relative to the STAR itself. Both resolutions are tried.
        """
        output_star_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_dir = output_star_path.parent / "tilt_series"
        tilt_dir.mkdir(exist_ok=True)

        in_star_data = self.starfile_service.read(input_star_path)
        in_ts_df = in_star_data.get("global")
        if in_ts_df is None:
            raise ValueError(f"No 'global' block in {input_star_path}")

        in_star_dir = input_star_path.parent
        out_ts_df = in_ts_df.copy()

        unresolved: List[str] = []
        for _, ts_row in in_ts_df.iterrows():
            ts_id = str(ts_row["rlnTomoName"])
            per_ts_rel = ts_row["rlnTomoTiltSeriesStarFile"]
            per_ts_in = self._resolve_per_ts_path(per_ts_rel, in_star_dir, project_root)
            if per_ts_in is None:
                unresolved.append(f"{ts_id}: input per-TS STAR not found (rel={per_ts_rel!r})")
                continue
            if not self.registry.has_tilt_series(ts_id):
                unresolved.append(f"{ts_id}: not in registry")
                continue

            ts = self.registry.get_tilt_series(ts_id)
            updated_df, errors = self._apply_motion_ctf_to_tilt_df(
                ts=ts,
                tilt_df=self._read_only_block(per_ts_in),
            )
            if errors:
                unresolved.extend(f"{ts_id}: {e}" for e in errors)
                continue

            self.starfile_service.write({ts_id: updated_df}, tilt_dir / f"{ts_id}.star")

        if unresolved:
            raise RuntimeError(
                "fs_motion_and_ctf emit_star: " + str(len(unresolved)) + " per-TS problem(s):\n  - "
                + "\n  - ".join(unresolved)
            )

        # Rewrite per-TS paths in the global block to point to the new tilt_dir.
        out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoName"].apply(
            lambda name: f"tilt_series/{name}.star"
        )

        self.starfile_service.write({"global": out_ts_df}, output_star_path)
        logger.info("fs_motion_and_ctf: wrote output STAR to %s", output_star_path)

    # ── Internals ──────────────────────────────────────────────────────────

    def _build_frame_output(self, frame_id: str, xml_path: Path) -> FsMotionCtfFrameOutput:
        """Parse one per-movie WarpTools XML. The paths to the averaged / even /
        odd / powerspectrum MRCs follow WarpTools's fixed output layout."""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        ctf = root.find(".//CTF")
        if ctf is None:
            raise ValueError(f"No CTF data in {xml_path}")

        defocus_value = float(ctf.find(".//Param[@Name='Defocus']").get("Value"))
        defocus_angle = float(ctf.find(".//Param[@Name='DefocusAngle']").get("Value"))
        defocus_delta = float(ctf.find(".//Param[@Name='DefocusDelta']").get("Value"))

        # Legacy quirk: fs_motion writes U == V and stuffs delta into astigmatism.
        # Replicated exactly to preserve on-disk STAR layout (byte-for-byte
        # compat with the pre-refactor writer).
        defocus_u = defocus_value * 10000.0
        defocus_v = defocus_value * 10000.0
        astig = defocus_delta * 10000.0

        base = self.warp_dir / "average"
        return FsMotionCtfFrameOutput(
            job_instance_id=self.job_instance_id,
            job_dir=self.job_dir,
            averaged_mrc=base / f"{frame_id}.mrc",
            even_mrc=base / "even" / f"{frame_id}.mrc",
            odd_mrc=base / "odd" / f"{frame_id}.mrc",
            ctf_image=self.warp_dir / "powerspectrum" / f"{frame_id}.mrc",
            defocus_u_angstrom=defocus_u,
            defocus_v_angstrom=defocus_v,
            defocus_angle=defocus_angle,
            ctf_astigmatism=astig,
            warp_xml_path=xml_path,
        )

    def _resolve_per_ts_path(
        self, per_ts_rel: str, in_star_dir: Path, project_root: Path
    ) -> Optional[Path]:
        """Try (in_star_dir / rel) then (project_root / rel). The ts_import STAR
        uses project-root-relative paths (RELION convention); later-job STARs
        use paths relative to the STAR itself."""
        for base in (in_star_dir, project_root):
            cand = (base / per_ts_rel).resolve()
            if cand.exists():
                return cand
        return None

    def _read_only_block(self, path: Path) -> pd.DataFrame:
        data = self.starfile_service.read(path)
        return next(iter(data.values())).copy()

    def _apply_motion_ctf_to_tilt_df(
        self, ts: TiltSeries, tilt_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, List[str]]:
        """Overlay per-frame motion+CTF outputs onto the tilt DataFrame.

        Resolution: tilt_row['rlnMicrographMovieName'] → Frame via
        TS.frame_by_filename → FsMotionCtfFrameOutput from frame.outputs."""
        errors: List[str] = []
        if "rlnMicrographMovieName" not in tilt_df.columns:
            errors.append("per-TS STAR has no rlnMicrographMovieName column")
            return tilt_df, errors

        seen_frame_ids: Dict[str, int] = {}
        for idx, row in tilt_df.iterrows():
            movie_name = row["rlnMicrographMovieName"]
            try:
                frame = ts.frame_by_filename(movie_name)
            except KeyError:
                errors.append(f"row {idx}: movie {movie_name!r} not in registry TS {ts.id}")
                continue

            out = frame.outputs.get(self.job_instance_id)
            if out is None or out.output_type != "fs_motion_ctf":
                errors.append(
                    f"row {idx}: frame {frame.id!r} has no fs_motion_and_ctf output in registry"
                )
                continue

            seen_frame_ids[frame.id] = seen_frame_ids.get(frame.id, 0) + 1

            tilt_df.at[idx, "rlnMicrographName"] = str(out.averaged_mrc)
            tilt_df.at[idx, "rlnMicrographNameEven"] = str(out.even_mrc)
            tilt_df.at[idx, "rlnMicrographNameOdd"] = str(out.odd_mrc)
            tilt_df.at[idx, "rlnDefocusU"] = out.defocus_u_angstrom
            tilt_df.at[idx, "rlnDefocusV"] = out.defocus_v_angstrom
            tilt_df.at[idx, "rlnCtfAstigmatism"] = out.ctf_astigmatism
            tilt_df.at[idx, "rlnDefocusAngle"] = out.defocus_angle
            tilt_df.at[idx, "rlnCtfImage"] = str(out.ctf_image)

            # Legacy placeholders preserved byte-for-byte from
            # MetadataTranslator._merge_warp_metadata.
            tilt_df.at[idx, "rlnAccumMotionTotal"] = _LEGACY_MOTION_PLACEHOLDER
            tilt_df.at[idx, "rlnAccumMotionEarly"] = _LEGACY_MOTION_PLACEHOLDER
            tilt_df.at[idx, "rlnAccumMotionLate"] = _LEGACY_MOTION_PLACEHOLDER
            tilt_df.at[idx, "rlnCtfMaxResolution"] = _LEGACY_MOTION_PLACEHOLDER
            tilt_df.at[idx, "rlnMicrographMetadata"] = "None"
            tilt_df.at[idx, "rlnCtfFigureOfMerit"] = "None"

        dups = [fid for fid, n in seen_frame_ids.items() if n > 1]
        if dups:
            errors.append(f"multiple tilt rows resolved to same frame id(s): {dups}")

        return tilt_df, errors
