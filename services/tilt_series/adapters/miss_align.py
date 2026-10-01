"""miss-alignment ingest adapter.

miss-alignment refines Warp tilt-series XMLs in place. Their rigid per-tilt geometry is
row-parallel to <MoviePath>: <Angles>, <AxisAngle>, <AxisOffsetX/Y> (Å). Reading it into a
`TsAlignmentTiltSeriesOutput` lets the inherited `emit_star` write the same star +
`tilt_series/` an alignment job writes, with the refined values:

    rlnTomoYTilt = -Angles     rlnTomoZRot = AxisAngle     rlnTomoX/YShiftAngst = AxisOffsetX/Y

Local image warps (<GridMovementX/Y>, written by [N,N] iterations) have no RELION star column;
the driver reports their size (services/analysis/alignment_changes.py) so it is visible they are left out.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path

from services.tilt_series.adapters._base import opt_float, xml_text_lines
from services.tilt_series.adapters.ts_alignment import TsAlignmentIngestAdapter
from services.tilt_series.models import TsAlignmentPerFrame, TsAlignmentTiltSeriesOutput

logger = logging.getLogger(__name__)

# Star column -> key of a row from read_xml_rows.
STAR_COLUMNS = {
    "rlnTomoYTilt": "tilt_y_deg",
    "rlnTomoZRot": "z_rot_deg",
    "rlnTomoXShiftAngst": "x_shift_angstrom",
    "rlnTomoYShiftAngst": "y_shift_angstrom",
}


def read_xml_rows(xml_path: Path) -> list[dict]:
    """One dict per tilt of a Warp tilt-series XML, in star terms, keyed by movie basename."""
    root = ET.parse(xml_path).getroot()
    lists = {
        name: xml_text_lines(root.find(name))
        for name in ("MoviePath", "Angles", "AxisAngle", "AxisOffsetX", "AxisOffsetY")
    }
    n = len(lists["MoviePath"])
    if n == 0:
        raise RuntimeError(f"{xml_path}: no <MoviePath> entries")
    uneven = {name: len(values) for name, values in lists.items() if len(values) != n}
    if uneven:
        raise RuntimeError(f"{xml_path}: per-tilt lists disagree with {n} <MoviePath> entries: {uneven}")
    fov = xml_text_lines(root.find("FOVFraction"))
    return [
        {
            "movie": Path(lists["MoviePath"][i]).name,
            "tilt_y_deg": -float(lists["Angles"][i]),
            "z_rot_deg": float(lists["AxisAngle"][i]),
            "x_shift_angstrom": float(lists["AxisOffsetX"][i]),
            "y_shift_angstrom": float(lists["AxisOffsetY"][i]),
            "fov_fraction": opt_float(fov[i]) if len(fov) == n else None,
        }
        for i in range(n)
    ]


class MissAlignIngestAdapter(TsAlignmentIngestAdapter):
    """Alignment output from refined Warp XMLs instead of AreTomo/IMOD files; `emit_star` is inherited."""

    def ingest(self, expected_ts_ids: Iterable[str], *, stack_angpix: float) -> list[str]:  # type: ignore[override]
        """Attach one output per TS whose XML converts; drop (and warn about) the rest, like an
        alignment job does. Only a total wipeout raises. Returns the ingested TS ids.

        `stack_angpix` is the pixel size of the stacks the refinement read, recorded with the output (the
        XML shifts are already in Å). It is passed in because the job's own stacks may not exist yet when
        the tool rebuilds them itself."""
        expected = sorted(set(expected_ts_ids))
        if not expected:
            raise ValueError("ingest called with no expected TS ids")
        missing = [t for t in expected if not self.registry.has_tilt_series(t)]
        if missing:
            raise RuntimeError(
                f"missAlign ingest aborted. TS missing from registry: {missing}. "
                f"Reload the project to backfill the registry from mdocs."
            )

        problems: dict[str, str] = {}
        ingested: list[str] = []
        for ts_id in expected:
            ts = self.registry.get_tilt_series(ts_id)
            try:
                rows = read_xml_rows(self.warp_dir / f"{ts_id}.xml")
            except (OSError, ET.ParseError, RuntimeError, ValueError) as e:
                problems[ts_id] = str(e)
                continue
            per_frame: list[TsAlignmentPerFrame] = []
            unresolved: list[str] = []
            for row in rows:
                try:
                    frame = ts.frame_by_filename(row["movie"])
                except KeyError:
                    unresolved.append(row["movie"])
                    continue
                per_frame.append(
                    TsAlignmentPerFrame(
                        frame_id=frame.id,
                        z_index=frame.tilt_index,
                        tilt_x_deg=0.0,
                        tilt_y_deg=row["tilt_y_deg"],
                        z_rot_deg=row["z_rot_deg"],
                        x_shift_angstrom=row["x_shift_angstrom"],
                        y_shift_angstrom=row["y_shift_angstrom"],
                        fov_fraction=row["fov_fraction"],
                    )
                )
            if unresolved:
                problems[ts_id] = f"{len(unresolved)} XML movie(s) not in the registry: {unresolved[:5]}"
                continue
            self.registry.attach_ts_output(
                ts_id,
                TsAlignmentTiltSeriesOutput(
                    job_instance_id=self.job_instance_id,
                    job_dir=self.job_dir,
                    alignment_method="miss_alignment",
                    alignment_angpix=stack_angpix,
                    per_frame=per_frame,
                ),
            )
            ingested.append(ts_id)

        if problems:
            detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(problems.items()))
            logger.warning(
                "missAlign: %d/%d tilt-series left out of the output:\n  - %s", len(problems), len(expected), detail
            )
        if not ingested:
            detail = "\n  - ".join(f"{tid}: {reason}" for tid, reason in sorted(problems.items()))
            raise RuntimeError(f"missAlign ingest failed for ALL {len(expected)} tilt-series:\n  - {detail}")
        return ingested

    def star_reproduction_error(self, input_star_path: Path, project_root: Path) -> tuple[dict[str, float], int]:
        """Largest |XML-derived - star value| per alignment column, over every tilt of every TS in
        `input_star_path`, matched by movie basename; plus the number of tilts compared. Run on the
        unrefined XMLs of the alignment job that wrote the star, it measures the mapping itself."""
        in_df = self.starfile_service.read(input_star_path).get("global")
        if in_df is None:
            raise ValueError(f"No 'global' block in {input_star_path}")
        worst = dict.fromkeys(STAR_COLUMNS, 0.0)
        compared = 0
        for _, ts_row in in_df.iterrows():
            ts_id = str(ts_row["rlnTomoName"])
            per_ts = self._resolve_per_ts_path(
                ts_row["rlnTomoTiltSeriesStarFile"], input_star_path.parent, project_root
            )
            if per_ts is None:
                raise FileNotFoundError(f"per-TS STAR of {ts_id} not found next to {input_star_path}")
            by_movie = {row["movie"]: row for row in read_xml_rows(self.warp_dir / f"{ts_id}.xml")}
            for _, tilt in self._read_only_block(per_ts).iterrows():
                row = by_movie.get(Path(str(tilt["rlnMicrographMovieName"])).name)
                if row is None:
                    continue  # a tilt ts_import left out: absent from the XML, NaN in the star
                for col, key in STAR_COLUMNS.items():
                    value = opt_float(tilt.get(col))
                    if value is not None:
                        worst[col] = max(worst[col], abs(row[key] - value))
                compared += 1
        return worst, compared
