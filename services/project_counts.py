"""How much data a project holds — the two numbers the workspace rail badges.

The workspace has no cheap way to say "your tomograms arrived". The counts live behind
the Tomograms wall and the project-hub popover, so a user who imports volumes into an
empty project sees nothing move. These two functions are the answer: one number per
rail icon, recomputed off the event loop and repainted only when it changes.

Both are deliberately CHEAP — star rows and in-memory registry entries, no MRC headers
(that is ``services.visualization.tomo_geometry.all_geometries``' job, and it sits on a
render path, not a poll). The one disk cost is an ``exists()`` per tomogram row, and it
is memoized on the source stars' mtimes so an unchanged project costs two ``stat`` calls
per tick.

Nothing here invents a number. A source that cannot be read contributes zero rows and
says so in ``unreadable``; a row whose volume is not on disk is counted in ``missing``
rather than folded into the total silently (CLAUDE.md 'Surfacing uncertainty') — the
badge tooltip is where the user reads it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TomogramCounts:
    """Tomograms this project knows about, split by where they came from."""

    total: int = 0  # unique rlnTomoName across every source (first source wins)
    reconstructed: int = 0  # rows from the TS_RECONSTRUCT job's tomograms.star
    imported: int = 0  # rows from the project-level imported tomograms.star
    missing: int = 0  # rows whose rlnTomoReconstructedTomogram is not on disk
    unreadable: tuple[str, ...] = ()  # stars that exist but did not parse

    @property
    def on_disk(self) -> int:
        return self.total - self.missing


@dataclass(frozen=True)
class TiltSeriesCounts:
    """Tilt-series this project imported, and how many are in the processing set."""

    total: int = 0
    selected: int = 0

    @property
    def excluded(self) -> int:
        return max(self.total - self.selected, 0)


# project key -> (source fingerprint, counts). The fingerprint is the (path, mtime) of
# every tomograms.star that feeds the count, so a project whose stars have not moved
# never re-stats its volumes.
_TOMO_CACHE: dict[str, tuple[tuple, TomogramCounts]] = {}


def _star_fingerprint(sources: list[tuple[str, Path]]) -> tuple:
    out = []
    for source, star in sources:
        try:
            mt = star.stat().st_mtime_ns
        except OSError:
            mt = -1
        out.append((source, str(star), mt))
    return tuple(out)


def tomogram_counts(project_state, project_path: Path) -> TomogramCounts:
    """Tomograms this project has, by source. Disk-only — call it from a thread.

    Rows are counted with the SAME precedence and de-duplication as
    ``tomo_geometry.all_geometries`` (reconstruct job first, then the imported star,
    unique on ``rlnTomoName``), so the badge can never disagree with the wall it points
    at about how many tomograms exist.
    """
    from services.visualization.tomo_geometry import (
        SOURCE_IMPORTED,
        SOURCE_RECONSTRUCT,
        read_tomo_table,
        tomogram_star_sources,
    )

    project_path = Path(project_path)
    sources = tomogram_star_sources(project_state, project_path)
    fingerprint = _star_fingerprint(sources)
    key = str(project_path)
    hit = _TOMO_CACHE.get(key)
    if hit is not None and hit[0] == fingerprint:
        return hit[1]

    seen: set[str] = set()
    per_source = {SOURCE_RECONSTRUCT: 0, SOURCE_IMPORTED: 0}
    missing = 0
    unreadable: list[str] = []
    for source, star in sources:
        df = read_tomo_table(star)
        if df is None:
            if star.exists():
                unreadable.append(str(star))
            continue
        has_recon_col = "rlnTomoReconstructedTomogram" in df.columns
        for _, row in df.iterrows():
            name = str(row["rlnTomoName"])
            if name in seen:
                continue
            seen.add(name)
            per_source[source] = per_source.get(source, 0) + 1
            raw = str(row["rlnTomoReconstructedTomogram"]) if has_recon_col else ""
            if not raw:
                missing += 1
                continue
            p = Path(raw)
            if not p.is_absolute():
                p = project_path / p
            if not p.exists():
                missing += 1

    counts = TomogramCounts(
        total=len(seen),
        reconstructed=per_source.get(SOURCE_RECONSTRUCT, 0),
        imported=per_source.get(SOURCE_IMPORTED, 0),
        missing=missing,
        unreadable=tuple(unreadable),
    )
    _TOMO_CACHE[key] = (fingerprint, counts)
    return counts


def tilt_series_counts(project_state, project_path: Path) -> TiltSeriesCounts:
    """Tilt-series in the registry, and how many are in the processing set.

    The registry is the store (roadmap 02); a pre-registry project has none, so the
    counts recorded on ``ProjectState`` at import time stand in — the same fallback the
    project-hub parameter pane makes."""
    from services.tilt_series import get_registry_for

    try:
        all_ts = list(get_registry_for(Path(project_path)).all_tilt_series())
    except Exception:
        logger.exception("Could not read the tilt-series registry for %s", project_path)
        all_ts = []
    if all_ts:
        return TiltSeriesCounts(total=len(all_ts), selected=sum(1 for ts in all_ts if ts.is_selected))
    total = int(getattr(project_state, "import_total_tilt_series", 0) or 0)
    selected = int(getattr(project_state, "import_selected_tilt_series", 0) or 0)
    return TiltSeriesCounts(total=total, selected=selected or total)
