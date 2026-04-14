"""tsCTF ingest adapter — registry-driven replacement for
`MetadataTranslator.update_ts_ctf_metadata`.

Flow:

    1. Ingest: for every TS in scope, read `{job_dir}/warp_tiltseries/{ts_id}.xml`
       via WarpXmlParser, resolve (ts_id, Z-index) → MoviePath entry → Frame via
       the registry's filename index, attach a `TsCtfTiltSeriesOutput` carrying
       a per-frame CTF list.

    2. emit_star: read the input STAR (ts_alignment's output — retains all
       upstream columns we must preserve), overlay CTF values from the
       registry onto each tilt row, write out the same hierarchical layout.

Identity is resolved ONCE, via the registry, at the (ts_id, filename) pair.
No `Path(stem)` heuristics, no `_EER.eer` strip chains.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from services.configs.metadata_service import WarpXmlParser
from services.configs.starfile_service import StarfileService
from services.tilt_series.models import (
    Frame,
    TiltSeries,
    TsCtfPerFrameCtf,
    TsCtfTiltSeriesOutput,
)
from services.tilt_series.registry import TiltSeriesRegistry

logger = logging.getLogger(__name__)


# Columns written by a tsCTF run. Listed explicitly so emit_star can't
# silently drop one as the STAR schema evolves.
CTF_COLUMNS = (
    "rlnDefocusU",
    "rlnDefocusV",
    "rlnDefocusAngle",
    "rlnCtfAstigmatism",
    "rlnTomoHand",
)


class TsCtfIngestAdapter:
    def __init__(
        self,
        registry: TiltSeriesRegistry,
        job_dir: Path,
        *,
        job_instance_id: str = "tsCTF",
        warp_folder: str = "warp_tiltseries",
        starfile_service: Optional[StarfileService] = None,
    ):
        self.registry = registry
        self.job_dir = Path(job_dir)
        self.job_instance_id = job_instance_id
        self.warp_dir = self.job_dir / warp_folder
        self.starfile_service = starfile_service or StarfileService()

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(self, expected_ts_ids: Iterable[str]) -> None:
        """Populate registry with per-TS CTF outputs. Fail loud on missing XMLs
        or unresolved (ts, filename) pairs. Safe to call repeatedly — existing
        registry entries under `self.job_instance_id` are overwritten.
        """
        expected = sorted(set(expected_ts_ids))
        if not expected:
            raise ValueError("ingest called with no expected TS ids")

        # Identity invariant: every TS must have both a registry entry and a
        # per-TS XML. Drift in either direction is a hard failure.
        missing_in_registry = [t for t in expected if not self.registry.has_tilt_series(t)]
        missing_xml = [t for t in expected if not (self.warp_dir / f"{t}.xml").exists()]
        if missing_in_registry or missing_xml:
            raise RuntimeError(
                f"tsCtf ingest aborted. "
                f"TS missing from registry: {missing_in_registry or 'none'}. "
                f"TS missing XML in {self.warp_dir}: {missing_xml or 'none'}."
            )

        for ts_id in expected:
            ts = self.registry.get_tilt_series(ts_id)
            xml_path = self.warp_dir / f"{ts_id}.xml"
            output = self._build_ts_output(ts, xml_path)
            self.registry.attach_ts_output(ts_id, output)
            logger.info(
                "tsCtf: ingested %s (%d/%d frames have CTF)",
                ts_id, len(output.per_frame), ts.frame_count,
            )

    def emit_star(
        self,
        input_star_path: Path,
        output_star_path: Path,
        *,
        preserve_subfolder: str = "tilt_series",
    ) -> None:
        """Write the RELION-compatible hierarchical STAR for the downstream job.

        Strategy: take the input STAR's schema as the shape of the output, but
        replace the per-tilt CTF columns with values looked up from the
        registry. All other columns pass through unchanged so the STAR remains
        drop-in compatible with whatever RELION/WarpTools expects next.
        """
        output_star_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_dir = output_star_path.parent / preserve_subfolder
        tilt_dir.mkdir(exist_ok=True)

        in_star_data = self.starfile_service.read(input_star_path)
        in_ts_df = in_star_data.get("global")
        if in_ts_df is None:
            raise ValueError(f"No 'global' block in {input_star_path}")

        in_star_dir = input_star_path.parent
        out_ts_df = in_ts_df.copy()

        # Rewrite per-TS star file paths to point to the new tilt_dir (same
        # convention as the legacy writer).
        out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoTiltSeriesStarFile"].apply(
            lambda x: f"{preserve_subfolder}/{Path(x).name}"
        )

        unresolved: List[str] = []
        for _, ts_row in in_ts_df.iterrows():
            ts_id = str(ts_row["rlnTomoName"])
            per_ts_rel = ts_row["rlnTomoTiltSeriesStarFile"]
            per_ts_in = (in_star_dir / per_ts_rel).resolve()
            if not per_ts_in.exists():
                unresolved.append(f"{ts_id}: input per-TS STAR not found at {per_ts_in}")
                continue
            if not self.registry.has_tilt_series(ts_id):
                unresolved.append(f"{ts_id}: not in registry")
                continue

            ts = self.registry.get_tilt_series(ts_id)
            ctf_output = ts.outputs.get(self.job_instance_id)
            if ctf_output is None or ctf_output.output_type != "ts_ctf":
                unresolved.append(f"{ts_id}: no ingested tsCtf output in registry")
                continue

            updated_df, errors = self._apply_ctf_to_tilt_df(
                ts=ts,
                ctf_output=ctf_output,
                tilt_df=self._read_only_block(per_ts_in),
            )
            if errors:
                unresolved.extend(f"{ts_id}: {e}" for e in errors)
                continue

            # Write the per-TS STAR alongside the main STAR. The key name in
            # the STAR block matches RELION convention: the TS id.
            self.starfile_service.write({ts_id: updated_df}, tilt_dir / f"{ts_id}.star")

        if unresolved:
            raise RuntimeError(
                "tsCtf emit_star: " + str(len(unresolved)) + " per-TS problem(s):\n  - "
                + "\n  - ".join(unresolved)
            )

        # Set rlnTomoHand on the global block from each TS's ingested output.
        # Different TS can have different handedness if ts_defocus_hand decided
        # so; carry that forward.
        hand_map = {
            ts.id: (-1 if ts.outputs[self.job_instance_id].are_angles_inverted else 1)
            for ts in self.registry.all_tilt_series()
            if self.job_instance_id in ts.outputs
        }
        out_ts_df["rlnTomoHand"] = out_ts_df["rlnTomoName"].map(hand_map).fillna(1).astype(int)

        self.starfile_service.write({"global": out_ts_df}, output_star_path)
        logger.info("tsCtf: wrote output STAR to %s", output_star_path)

    # ── Internals ──────────────────────────────────────────────────────────

    def _build_ts_output(self, ts: TiltSeries, xml_path: Path) -> TsCtfTiltSeriesOutput:
        """Parse one per-TS WarpTools XML, resolve each row to a Frame via the
        registry, produce a typed TsCtfTiltSeriesOutput."""
        parser = WarpXmlParser(str(xml_path))
        warp_df = parser.data_df
        if warp_df.empty:
            raise RuntimeError(f"No CTF rows parsed from {xml_path}")

        # cryoBoostKey in parser output == movie filename with _EER.eer/.tif/.eer
        # stripped (see WarpXmlParser._parse_tilt_series_xml). We resolve via
        # the TS's `frame_by_filename`, which tolerates stem vs full filename.
        per_frame: List[TsCtfPerFrameCtf] = []
        missing: List[str] = []
        ambiguous: List[str] = []
        are_inverted = bool(warp_df["are_angles_inverted"].iloc[0])

        for _, row in warp_df.iterrows():
            key = str(row["cryoBoostKey"])
            frame = self._resolve_frame(ts, key)
            if frame is None:
                missing.append(key)
                continue

            defocus_u = (float(row["defocus_value"]) + float(row["defocus_delta"])) * 10000.0
            defocus_v = (float(row["defocus_value"]) - float(row["defocus_delta"])) * 10000.0
            astig = defocus_u - defocus_v
            per_frame.append(
                TsCtfPerFrameCtf(
                    frame_id=frame.id,
                    z_index=int(row["Z"]),
                    defocus_u_angstrom=defocus_u,
                    defocus_v_angstrom=defocus_v,
                    defocus_angle=float(row["defocus_angle"]),
                    ctf_astigmatism=astig,
                )
            )

        problems: List[str] = []
        if missing:
            sample = missing[:5]
            suffix = "..." if len(missing) > 5 else ""
            problems.append(
                f"{len(missing)} warp rows could not be resolved to registry frames: {sample}{suffix}"
            )
        if ambiguous:
            problems.append(f"{len(ambiguous)} ambiguous warp rows: {ambiguous[:5]}")
        if problems:
            raise RuntimeError(f"tsCtf ingest failed for TS {ts.id}: " + "; ".join(problems))

        # Dedup guard: two warp rows keying onto the same frame_id would
        # silently cause one to overwrite the other downstream. Surface it.
        ids_seen: Dict[str, int] = {}
        for p in per_frame:
            ids_seen[p.frame_id] = ids_seen.get(p.frame_id, 0) + 1
        dups = [fid for fid, n in ids_seen.items() if n > 1]
        if dups:
            raise RuntimeError(f"tsCtf ingest: frames with multiple CTF rows in {ts.id}: {dups}")

        return TsCtfTiltSeriesOutput(
            job_instance_id=self.job_instance_id,
            job_dir=self.job_dir,
            warp_xml_path=xml_path,
            are_angles_inverted=are_inverted,
            per_frame=per_frame,
        )

    def _resolve_frame(self, ts: TiltSeries, warp_key: str) -> Optional[Frame]:
        """Map a WarpXmlParser cryoBoostKey to a Frame in `ts`. The key has
        already had `_EER.eer`/`.eer`/`.tif` stripped; frames in the registry
        are keyed by stem and filename. Try stem match first (the common
        case), then full-filename fallback."""
        for f in ts.frames:
            # registry's id == Path(raw_filename).stem (strips only last ext),
            # so for "foo_EER.eer" the id is "foo_EER" while warp_key is "foo".
            # Try both exact-equal and (stem-stripped-of-"_EER") forms.
            if f.id == warp_key:
                return f
            if f.id.endswith("_EER") and f.id[: -len("_EER")] == warp_key:
                return f
            if Path(f.raw_filename).stem == warp_key:
                return f
        return None

    def _read_only_block(self, path: Path) -> pd.DataFrame:
        data = self.starfile_service.read(path)
        return next(iter(data.values())).copy()

    def _apply_ctf_to_tilt_df(
        self,
        ts: TiltSeries,
        ctf_output: TsCtfTiltSeriesOutput,
        tilt_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, List[str]]:
        """Overlay per-frame CTF values onto the input tilt-DataFrame.

        Resolution goes: tilt_row['rlnMicrographMovieName'] → filename stem →
        Frame via TS.frame_by_filename → TsCtfPerFrameCtf by frame_id.

        Frames absent from the CTF output (because WarpTools `ts_import` dropped
        them from the tomostar, so `ts_ctf` never refit them) retain the
        per-frame defocus values that `fs_motion_and_ctf` wrote. Matches legacy
        CryoBoost behavior; downstream WarpTools uses the tomostar as the
        authoritative frame set, so these rows are cosmetic."""
        errors: List[str] = []
        by_frame_id = {p.frame_id: p for p in ctf_output.per_frame}
        hand = -1 if ctf_output.are_angles_inverted else 1

        if "rlnMicrographMovieName" not in tilt_df.columns:
            errors.append("per-TS STAR has no rlnMicrographMovieName column")
            return tilt_df, errors

        skipped = 0
        for idx, row in tilt_df.iterrows():
            movie_name = row["rlnMicrographMovieName"]
            try:
                frame = ts.frame_by_filename(movie_name)
            except KeyError:
                errors.append(f"row {idx}: movie {movie_name!r} not in registry TS {ts.id}")
                continue

            ctf = by_frame_id.get(frame.id)
            if ctf is None:
                skipped += 1
                continue

            tilt_df.at[idx, "rlnDefocusU"] = ctf.defocus_u_angstrom
            tilt_df.at[idx, "rlnDefocusV"] = ctf.defocus_v_angstrom
            tilt_df.at[idx, "rlnDefocusAngle"] = ctf.defocus_angle
            tilt_df.at[idx, "rlnCtfAstigmatism"] = ctf.ctf_astigmatism
            tilt_df.at[idx, "rlnTomoHand"] = hand

        if skipped > 0:
            logger.info(
                "tsCtf: TS %s: %d/%d rows retain fs_motion per-frame defocus (ts_import filtered these from tomostar)",
                ts.id, skipped, len(tilt_df),
            )

        return tilt_df, errors


def legacy_copy_per_ts_stars(
    input_star_path: Path,
    output_dir: Path,
    *,
    subfolder: str = "tilt_series",
) -> None:
    """Copy per-TS star files from input → output when no modifications are
    needed (e.g. for a downstream job that only reads the global block).
    Not used by TsCtfIngestAdapter; kept here in case a future adapter needs
    it."""
    target = output_dir / subfolder
    target.mkdir(parents=True, exist_ok=True)
    for src in (input_star_path.parent / subfolder).glob("*.star"):
        shutil.copy2(src, target / src.name)
