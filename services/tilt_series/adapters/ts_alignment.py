"""ts_alignment ingest adapter — registry-driven replacement for
`MetadataTranslator.update_ts_alignment_metadata`.

Flow:

    1. Ingest: for every TS in scope, parse AreTomo `.st.aln` or IMOD
       `.xf`/`.tlt` under `{job_dir}/warp_tiltseries/tiltstack/{ts_id}/`,
       resolve each row's movie (via the per-TS tomostar's `wrpMovieName`)
       to a `Frame` in the registry, attach a `TsAlignmentTiltSeriesOutput`
       carrying per-frame shifts + refined tilt angle + Z-rotation.

    2. emit_star: read the input STAR (fsMotionAndCtf's output — preserves the
       motion/CTF columns we must carry forward), overlay the 5 alignment
       columns onto each tilt row, write the hierarchical STAR + the
       `all_tilts.star` sidecar the legacy writer produced.

Identity is resolved ONCE, at ingest, via tomostar → Frame. The
`_assert_ts_identity_consistency` invariant (tomostar ∩ per-TS XML ∩
tiltstack dir sets must agree) is enforced as an ingest precondition — that
check lived in `MetadataTranslator` before this refactor; it belongs in the
adapter now.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from services.configs.starfile_service import StarfileService
from services.models_base import AlignmentMethod
from services.tilt_series.models import (
    TiltSeries,
    TsAlignmentPerFrame,
    TsAlignmentTiltSeriesOutput,
)
from services.tilt_series.registry import TiltSeriesRegistry

logger = logging.getLogger(__name__)


# Column indices in the aln_data numpy array. The shape is dictated by the
# AreTomo / IMOD parsers; exposed as named indices here so the apply step
# isn't a minefield of magic numbers.
_ALN_COL_INDEX = 0
_ALN_COL_ZROT = 1
_ALN_COL_XSHIFT = 3
_ALN_COL_YSHIFT = 4
_ALN_COL_TILT = 9


class TsAlignmentIngestAdapter:
    def __init__(
        self,
        registry: TiltSeriesRegistry,
        job_dir: Path,
        *,
        job_instance_id: str = "tsAlignment",
        warp_folder: str = "warp_tiltseries",
        tomostar_folder: str = "tomostar",
        starfile_service: Optional[StarfileService] = None,
    ):
        self.registry = registry
        self.job_dir = Path(job_dir)
        self.job_instance_id = job_instance_id
        self.warp_dir = self.job_dir / warp_folder
        self.tiltstack_dir = self.warp_dir / "tiltstack"
        self.tomostar_dir = self.job_dir / tomostar_folder
        self.starfile_service = starfile_service or StarfileService()

    # ── Public API ─────────────────────────────────────────────────────────

    def ingest(
        self,
        expected_ts_ids: Iterable[str],
        alignment_method: AlignmentMethod,
        *,
        alignment_angpix: float = 0.0,
    ) -> None:
        """Populate the registry with per-TS alignment outputs.

        `alignment_angpix` is the binned-stack pixel size used for shift
        conversion. If 0, auto-infer from the first `.st` MRC header — same
        behavior as the pre-refactor code.
        """
        expected = sorted(set(expected_ts_ids))
        if not expected:
            raise ValueError("ingest called with no expected TS ids")

        self._assert_ts_identity_consistency(set(expected))

        shift_angpix = alignment_angpix if alignment_angpix > 0 else self._infer_alignment_angpix()
        logger.info(
            "tsAlignment: shift pixel size = %.4f A/px (%s)",
            shift_angpix, "explicit" if alignment_angpix > 0 else "inferred from .st header",
        )

        missing_in_registry = [t for t in expected if not self.registry.has_tilt_series(t)]
        if missing_in_registry:
            raise RuntimeError(
                f"tsAlignment ingest aborted. TS missing from registry: {missing_in_registry}. "
                f"Reload the project to backfill the registry from mdocs."
            )

        problems: Dict[str, str] = {}
        for ts_id in expected:
            ts = self.registry.get_tilt_series(ts_id)
            try:
                output = self._build_ts_output(ts, alignment_method, shift_angpix)
            except RuntimeError as e:
                problems[ts_id] = str(e)
                continue
            self.registry.attach_ts_output(ts_id, output)
            logger.info(
                "tsAlignment: ingested %s (%d/%d frames aligned)",
                ts_id, len(output.per_frame), ts.frame_count,
            )

        if problems:
            detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(problems.items()))
            raise RuntimeError(
                f"tsAlignment ingest failed for {len(problems)} tilt-series:\n  - {detail}"
            )

    def emit_star(
        self,
        input_star_path: Path,
        output_star_path: Path,
        project_root: Path,
        *,
        tomo_dimensions: str,
    ) -> None:
        """Write the RELION-compatible hierarchical STAR with alignment columns
        overlaid. `tomo_dimensions` is a WxHxD string fed into rlnTomoSizeX/Y/Z
        on the global block."""
        output_star_path.parent.mkdir(parents=True, exist_ok=True)
        tilt_dir = output_star_path.parent / "tilt_series"
        tilt_dir.mkdir(exist_ok=True)

        in_star_data = self.starfile_service.read(input_star_path)
        in_ts_df = in_star_data.get("global")
        if in_ts_df is None:
            raise ValueError(f"No 'global' block in {input_star_path}")

        in_star_dir = input_star_path.parent
        out_ts_df = in_ts_df.copy()

        # Resolve frame pixel size for the rlnTomoTiltSeriesPixelSize column.
        # Any of three possible source columns, matching the legacy cascade.
        pixel_size_col = next(
            (
                c
                for c in ("rlnMicrographOriginalPixelSize", "rlnTomoTiltSeriesPixelSize", "rlnMicrographPixelSize")
                if c in in_ts_df.columns
            ),
            None,
        )
        frame_angpix = float(in_ts_df[pixel_size_col].iloc[0]) if pixel_size_col else 1.35

        all_tilts_list: List[pd.DataFrame] = []
        problems: Dict[str, str] = {}
        for _, ts_row in in_ts_df.iterrows():
            ts_id = str(ts_row["rlnTomoName"])
            # Strict identity: rlnTomoName MUST equal the tilt_series STAR stem
            # — divergence means upstream corruption.
            per_ts_rel = ts_row["rlnTomoTiltSeriesStarFile"]
            if Path(per_ts_rel).stem != ts_id:
                problems[ts_id] = (
                    f"rlnTomoName={ts_id!r} does not match tilt_series filename stem "
                    f"{Path(per_ts_rel).stem!r}"
                )
                continue

            per_ts_in = self._resolve_per_ts_path(per_ts_rel, in_star_dir, project_root)
            if per_ts_in is None:
                problems[ts_id] = f"input per-TS STAR not found (rel={per_ts_rel!r})"
                continue
            if not self.registry.has_tilt_series(ts_id):
                problems[ts_id] = "not in registry"
                continue
            ts = self.registry.get_tilt_series(ts_id)
            aln_output = ts.outputs.get(self.job_instance_id)
            if aln_output is None or aln_output.output_type != "ts_alignment":
                problems[ts_id] = "no ingested tsAlignment output in registry"
                continue

            tilt_df = self._read_only_block(per_ts_in)
            updated, errors = self._apply_alignment_to_tilt_df(ts, aln_output, tilt_df)
            if errors:
                problems[ts_id] = "; ".join(errors)
                continue

            self.starfile_service.write({ts_id: updated}, tilt_dir / f"{ts_id}.star")

            # Build the {ts-row-expanded + per-tilt} wide DataFrame that the
            # legacy writer dumped into all_tilts.star for downstream jobs.
            ts_row_df = pd.concat([pd.DataFrame(ts_row).T] * len(updated), ignore_index=True)
            ts_row_df.index = updated.index
            all_tilts_list.append(pd.concat([ts_row_df, updated], axis=1))

        if problems:
            detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(problems.items()))
            raise RuntimeError(f"tsAlignment emit_star failed for {len(problems)} tilt-series:\n  - {detail}")

        # Rewrite global-block columns.
        out_ts_df["rlnTomoTiltSeriesStarFile"] = out_ts_df["rlnTomoName"].apply(
            lambda name: f"tilt_series/{name}.star"
        )

        if out_ts_df["rlnTomoName"].duplicated().any():
            dups = out_ts_df.loc[out_ts_df["rlnTomoName"].duplicated(keep=False), "rlnTomoName"].tolist()
            raise RuntimeError(f"duplicate rlnTomoName values in alignment output STAR: {sorted(set(dups))}")

        try:
            size_x, size_y, size_z = (int(v) for v in tomo_dimensions.split("x"))
        except ValueError as e:
            raise ValueError(f"Invalid tomo_dimensions {tomo_dimensions!r}; expected 'WxHxD'") from e
        out_ts_df["rlnTomoSizeX"] = size_x
        out_ts_df["rlnTomoSizeY"] = size_y
        out_ts_df["rlnTomoSizeZ"] = size_z
        out_ts_df["rlnTomoTiltSeriesPixelSize"] = frame_angpix

        self.starfile_service.write({"global": out_ts_df}, output_star_path)

        if all_tilts_list:
            all_tilts_df = pd.concat(all_tilts_list, ignore_index=True)
            self.starfile_service.write(
                {"all_tilts": all_tilts_df}, output_star_path.parent / "all_tilts.star"
            )

        logger.info("tsAlignment: wrote output STAR to %s", output_star_path)

    # ── Internals ──────────────────────────────────────────────────────────

    def _build_ts_output(
        self, ts: TiltSeries, alignment_method: AlignmentMethod, shift_angpix: float
    ) -> TsAlignmentTiltSeriesOutput:
        """Parse alignment files for one TS and build the typed output."""
        ts_tiltstack = self.tiltstack_dir / ts.id
        if not ts_tiltstack.is_dir():
            raise RuntimeError(f"no tiltstack dir at {ts_tiltstack}")

        aln_data, aln_file, xf_file, tlt_file = self._parse_alignment_files(
            ts_tiltstack, alignment_method
        )
        if aln_data is None:
            raise RuntimeError(f"no alignment output parsed in {ts_tiltstack}")

        # Sort by the "Index" column so tomostar[i] corresponds to aln_data[i].
        aln_data = aln_data[aln_data[:, _ALN_COL_INDEX].argsort()]

        # Read the tomostar to establish the per-row movie identity.
        tomostar_path = self.tomostar_dir / f"{ts.id}.tomostar"
        if not tomostar_path.exists():
            raise RuntimeError(f"tomostar not found at {tomostar_path}")
        tomostar_data = self.starfile_service.read(tomostar_path)
        tomostar_df = next(iter(tomostar_data.values()))
        if "wrpMovieName" not in tomostar_df.columns:
            raise RuntimeError(f"tomostar {tomostar_path} has no wrpMovieName column")

        if len(tomostar_df) != len(aln_data):
            raise RuntimeError(
                f"tomostar row count ({len(tomostar_df)}) disagrees with alignment row count "
                f"({len(aln_data)}) for TS {ts.id}"
            )

        per_frame: List[TsAlignmentPerFrame] = []
        unresolved: List[str] = []
        for i, tomo_row in tomostar_df.iterrows():
            movie_name = str(tomo_row["wrpMovieName"])
            try:
                frame = ts.frame_by_filename(movie_name)
            except KeyError:
                unresolved.append(f"row {i}: {movie_name}")
                continue

            per_frame.append(
                TsAlignmentPerFrame(
                    frame_id=frame.id,
                    z_index=frame.tilt_index,
                    tilt_x_deg=0.0,
                    tilt_y_deg=-1.0 * float(aln_data[i, _ALN_COL_TILT]),
                    z_rot_deg=float(aln_data[i, _ALN_COL_ZROT]),
                    x_shift_angstrom=float(aln_data[i, _ALN_COL_XSHIFT]) * shift_angpix,
                    y_shift_angstrom=float(aln_data[i, _ALN_COL_YSHIFT]) * shift_angpix,
                )
            )

        if unresolved:
            raise RuntimeError(
                f"{len(unresolved)} tomostar row(s) could not be resolved to registry frames in "
                f"TS {ts.id}: {unresolved[:5]}"
            )

        if not per_frame:
            raise RuntimeError(
                f"no movie names in the tomostar matched the registry for TS {ts.id} — "
                f"likely a cross-TS contamination of the staging dir"
            )

        method_literal: str = "aretomo" if alignment_method == AlignmentMethod.ARETOMO else "imod"
        return TsAlignmentTiltSeriesOutput(
            job_instance_id=self.job_instance_id,
            job_dir=self.job_dir,
            alignment_method=method_literal,  # type: ignore[arg-type]
            alignment_angpix=shift_angpix,
            aln_file=aln_file,
            xf_file=xf_file,
            tlt_file=tlt_file,
            per_frame=per_frame,
        )

    def _parse_alignment_files(
        self, ts_tiltstack: Path, alignment_method: AlignmentMethod
    ) -> Tuple[Optional[np.ndarray], Optional[Path], Optional[Path], Optional[Path]]:
        """Locate + parse the alignment output for one TS. Returns
        (aln_data, aln_file_path, xf_file_path, tlt_file_path)."""
        if alignment_method == AlignmentMethod.ARETOMO:
            aln_files = sorted(ts_tiltstack.glob("*.st.aln"))
            if len(aln_files) > 1:
                raise RuntimeError(f"expected 1 .st.aln, found {len(aln_files)}: {aln_files}")
            if not aln_files:
                return None, None, None, None
            return self._read_aretomo_aln(aln_files[0]), aln_files[0], None, None

        if alignment_method == AlignmentMethod.IMOD:
            xf_files = sorted(ts_tiltstack.glob("*.xf"))
            tlt_files = sorted(ts_tiltstack.glob("*.tlt"))
            if len(xf_files) > 1 or len(tlt_files) > 1:
                raise RuntimeError(
                    f"expected 1 .xf and 1 .tlt, found {len(xf_files)} .xf / {len(tlt_files)} .tlt"
                )
            if not xf_files or not tlt_files:
                return None, None, None, None
            return (
                self._read_imod_xf_tlt(xf_files[0], tlt_files[0]),
                None, xf_files[0], tlt_files[0],
            )

        raise RuntimeError(f"alignment method {alignment_method} not implemented")

    @staticmethod
    def _read_aretomo_aln(aln_file: Path) -> Optional[np.ndarray]:
        data = []
        with open(aln_file) as f:
            for line in f:
                if line.startswith("# Local Alignment"):
                    break
                if not line.startswith("#"):
                    try:
                        numbers = [float(x) for x in line.split()]
                        if numbers:
                            data.append(numbers)
                    except ValueError:
                        continue
        if not data:
            return None
        return np.array(data)

    @staticmethod
    def _read_imod_xf_tlt(xf_file: Path, tlt_file: Path) -> Optional[np.ndarray]:
        df1 = pd.read_csv(xf_file, delim_whitespace=True, header=None, names=["m1", "m2", "m3", "m4", "tx", "ty"])
        df2 = pd.read_csv(tlt_file, delim_whitespace=True, header=None, names=["tilt_angle"])
        combined = pd.concat([df1, df2], axis=1)

        results_x, results_y, tilt_ang = [], [], []
        for _, row in combined.iterrows():
            M = np.array([[row["m1"], row["m2"]], [row["m3"], row["m4"]]])
            M = np.linalg.inv(M)
            v = np.array([row["tx"], row["ty"]]) * -1
            result = np.dot(M, v)
            angle = np.degrees(np.arctan2(M[1, 0], M[0, 0]))
            results_x.append(result[0])
            results_y.append(result[1])
            tilt_ang.append(angle)

        data_np = np.zeros((len(combined), 10))
        data_np[:, _ALN_COL_INDEX] = np.arange(0, len(combined))
        data_np[:, _ALN_COL_ZROT] = tilt_ang
        data_np[:, _ALN_COL_XSHIFT] = results_x
        data_np[:, _ALN_COL_YSHIFT] = results_y
        data_np[:, _ALN_COL_TILT] = combined["tilt_angle"]
        return data_np

    def _infer_alignment_angpix(self) -> float:
        """Read the pixel size of the binned tilt stack from the first .st
        MRC header. All TS in one alignment job share the same binning, so any
        .st file is authoritative."""
        if not self.tiltstack_dir.exists():
            raise FileNotFoundError(
                f"No tiltstack directory at {self.tiltstack_dir}. "
                f"Cannot determine alignment pixel size for shift conversion."
            )
        st_files = list(self.tiltstack_dir.glob("*/*.st"))
        if not st_files:
            raise FileNotFoundError(
                f"No .st files under {self.tiltstack_dir}. "
                f"Cannot determine alignment pixel size for shift conversion."
            )
        import mrcfile
        with mrcfile.open(st_files[0], header_only=True, mode="r") as mrc:
            voxel_x = float(mrc.voxel_size.x)
            if voxel_x <= 0:
                raise ValueError(
                    f"Invalid pixel size {voxel_x} in {st_files[0]}. "
                    f"Cannot determine alignment pixel size for shift conversion."
                )
            return voxel_x

    def _assert_ts_identity_consistency(self, expected_ts_ids: set) -> None:
        """The three independent sources of per-TS identity — tomostar files,
        per-TS XMLs, and tiltstack dirs — MUST agree. Any drift means the
        upstream array-job staging corrupted something, and silently picking
        one source's value for another TS is exactly the failure mode this
        refactor exists to prevent."""
        tomostar_stems = (
            {p.stem for p in self.tomostar_dir.glob("*.tomostar")}
            if self.tomostar_dir.is_dir() else set()
        )
        xml_stems = (
            {p.stem for p in self.warp_dir.glob("*.xml")}
            if self.warp_dir.is_dir() else set()
        )
        tiltstack_stems = (
            {p.name for p in self.tiltstack_dir.iterdir() if p.is_dir()}
            if self.tiltstack_dir.is_dir() else set()
        )

        mismatches: List[str] = []
        if tomostar_stems != tiltstack_stems:
            mismatches.append(
                f"tomostar vs tiltstack: only-tomostar={sorted(tomostar_stems - tiltstack_stems)}, "
                f"only-tiltstack={sorted(tiltstack_stems - tomostar_stems)}"
            )
        if tomostar_stems != xml_stems:
            mismatches.append(
                f"tomostar vs xml: only-tomostar={sorted(tomostar_stems - xml_stems)}, "
                f"only-xml={sorted(xml_stems - tomostar_stems)}"
            )
        input_missing = expected_ts_ids - tomostar_stems
        if input_missing:
            mismatches.append(f"expected TS with no tomostar: {sorted(input_missing)}")

        if mismatches:
            raise RuntimeError(
                f"TS identity consistency violated in {self.job_dir}; refusing to ingest "
                f"to avoid silent cross-TS contamination.\n  - " + "\n  - ".join(mismatches)
            )

    def _resolve_per_ts_path(
        self, per_ts_rel: str, in_star_dir: Path, project_root: Path
    ) -> Optional[Path]:
        for base in (in_star_dir, project_root):
            cand = (base / per_ts_rel).resolve()
            if cand.exists():
                return cand
        return None

    def _read_only_block(self, path: Path) -> pd.DataFrame:
        data = self.starfile_service.read(path)
        return next(iter(data.values())).copy()

    def _apply_alignment_to_tilt_df(
        self,
        ts: TiltSeries,
        aln_output: TsAlignmentTiltSeriesOutput,
        tilt_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Overlay the five alignment columns onto the per-TS tilt DataFrame.

        Resolution: tilt_row['rlnMicrographMovieName'] → Frame via
        TS.frame_by_filename → TsAlignmentPerFrame by frame_id.

        Frames absent from the alignment output (because WarpTools `ts_import`
        dropped them from the tomostar via the hardcoded outward-intensity walk —
        see `ImportTiltseries.cs:335-350`) are left in the STAR with NaN
        alignment columns. This mirrors the authoritative legacy CryoBoost
        behavior (`CryoBoost/src/warp/tsAlignment.py:132-140`) which iterated
        over the tomostar and left unmatched STAR rows un-overlaid. Downstream
        WarpTools treats the tomostar as the authoritative frame set, so those
        NaN rows are cosmetic and never processed by ts_ctf/ts_reconstruct."""
        errors: List[str] = []
        by_frame_id = {p.frame_id: p for p in aln_output.per_frame}

        if "rlnMicrographMovieName" not in tilt_df.columns:
            errors.append("per-TS STAR has no rlnMicrographMovieName column")
            return tilt_df, errors

        for col in ("rlnTomoXTilt", "rlnTomoYTilt", "rlnTomoZRot", "rlnTomoXShiftAngst", "rlnTomoYShiftAngst"):
            if col not in tilt_df.columns:
                tilt_df[col] = float("nan")

        skipped = 0
        for idx, row in tilt_df.iterrows():
            movie_name = row["rlnMicrographMovieName"]
            try:
                frame = ts.frame_by_filename(movie_name)
            except KeyError:
                errors.append(f"row {idx}: movie {movie_name!r} not in registry TS {ts.id}")
                continue

            aln = by_frame_id.get(frame.id)
            if aln is None:
                skipped += 1
                continue

            tilt_df.at[idx, "rlnTomoXTilt"] = aln.tilt_x_deg
            tilt_df.at[idx, "rlnTomoYTilt"] = aln.tilt_y_deg
            tilt_df.at[idx, "rlnTomoZRot"] = aln.z_rot_deg
            tilt_df.at[idx, "rlnTomoXShiftAngst"] = aln.x_shift_angstrom
            tilt_df.at[idx, "rlnTomoYShiftAngst"] = aln.y_shift_angstrom

        if skipped > 0:
            logger.info(
                "tsAlignment: TS %s: %d/%d rows left un-aligned (WarpTools ts_import dropped them from tomostar)",
                ts.id, skipped, len(tilt_df),
            )

        return tilt_df, errors
