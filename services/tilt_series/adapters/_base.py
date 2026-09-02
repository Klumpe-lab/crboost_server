"""Shared internals for the per-job-type ingest adapters.

The four adapters had copy-pasted the same constructor head, the same two STAR
helpers, and the same excluded-ids filtering — three copies of `_read_only_block`,
two of `_resolve_per_ts_path`, three of the exclusion pattern. They live here now;
each adapter keeps only its parsing core and its own STAR overlay.

Deliberately NOT an abstract base: `ingest` and `emit_star` have genuinely
different signatures per job type (tsReconstruct needs pixel sizes, ts_alignment
needs an alignment method and returns the surviving TS list, fs_motion needs a
project root), and forcing them into one shape would mean **kwargs soup. The
contract this base owns is identity + I/O plumbing, not the ingest verbs.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pandas as pd

from services.configs.starfile_service import StarfileService
from services.tilt_series.registry import TiltSeriesRegistry


def pos_float(v: object) -> float | None:
    """Coerce a WarpTools XML attribute / Param to a positive float, else None.
    Warp writes 0 / -1 for "not computed", and a resolution, motion or intensity
    of 0 is never a real measurement — so non-positive is "absent", not a value."""
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def opt_float(v: object) -> float | None:
    """Coerce to float, None when unparseable / NaN (a real 0 stays 0)."""
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def xml_text_lines(el) -> list[str]:
    """The non-empty lines of a Warp XML element whose text is a newline list
    (<MoviePath>, <FOVFraction>, <Angles>, …). Empty for a missing element."""
    if el is None or not el.text:
        return []
    return [ln.strip() for ln in el.text.split("\n") if ln.strip()]


class BaseIngestAdapter:
    """Identity + STAR plumbing shared by every ingest adapter.

    Subclasses set `DEFAULT_WARP_FOLDER` when their artifacts live somewhere other
    than `warp_tiltseries/`, and override `__init__` only to derive extra job-dir
    paths (calling `super().__init__` first).
    """

    # Job-dir subfolder holding this job type's Warp artifacts.
    DEFAULT_WARP_FOLDER: ClassVar[str] = "warp_tiltseries"

    def __init__(
        self,
        registry: TiltSeriesRegistry,
        job_dir: Path,
        *,
        job_instance_id: str,
        warp_folder: str | None = None,
        starfile_service: StarfileService | None = None,
    ):
        self.registry = registry
        self.job_dir = Path(job_dir)
        self.job_instance_id = job_instance_id
        self.warp_folder = warp_folder or self.DEFAULT_WARP_FOLDER
        self.warp_dir = self.job_dir / self.warp_folder
        self.starfile_service = starfile_service or StarfileService()

    # ── STAR helpers ───────────────────────────────────────────────────────

    def _read_only_block(self, path: Path) -> pd.DataFrame:
        """The sole data block of a per-TS STAR, as a copy safe to overlay onto."""
        data = self.starfile_service.read(path)
        return next(iter(data.values())).copy()

    def _resolve_per_ts_path(self, per_ts_rel: str, in_star_dir: Path, project_root: Path) -> Path | None:
        """Try (in_star_dir / rel) then (project_root / rel). The ts_import STAR
        uses project-root-relative paths (RELION convention); later-job STARs
        use paths relative to the STAR itself."""
        for base in (in_star_dir, project_root):
            cand = (base / per_ts_rel).resolve()
            if cand.exists():
                return cand
        return None

    # ── Excluded (user-muted) tilt-series ──────────────────────────────────

    @staticmethod
    def _excluded_set(excluded_ids: set[str] | None) -> set[str]:
        """Normalize the caller's muted-TS ids to a set of strings.

        A muted TS is intentionally not ingested, so `emit_star` must skip it in
        the per-TS loop rather than report its absent registry output as a failure.
        """
        return {str(t) for t in (excluded_ids or ())}

    @staticmethod
    def _drop_excluded(df: pd.DataFrame, excluded: set[str]) -> pd.DataFrame:
        """Remove muted TS rows from a global block. No-op when nothing is muted."""
        if not excluded:
            return df
        return df[~df["rlnTomoName"].astype(str).isin(excluded)].reset_index(drop=True)
