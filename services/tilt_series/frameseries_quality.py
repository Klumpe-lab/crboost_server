"""Real per-tilt CTF-fit resolution + beam-induced motion, read straight from
the WarpTools frameseries XML.

WarpTools' RELION-star export writes PLACEHOLDERS for the two columns a QC view
most wants — ``rlnCtfMaxResolution`` and ``rlnAccumMotion*`` come out as ``1e-6``
and ``rlnCtfFigureOfMerit`` as the literal ``"None"``. The genuine per-tilt
values live only as root attributes of the ``<Movie>`` element in the per-movie
XML at ``<fsMotion job>/warp_frameseries/<frame_stem>.xml``:

    - ``CTFResolutionEstimate`` — resolution (Å) the CTF was fit to (lower = better)
    - ``MeanFrameMovement``     — average beam-induced motion (WarpTools units; see note)

This module parses just those two attributes, memoized by ``(path, mtime)`` so a
render path can read them on every tick without re-parsing. The ingest adapter
(``services/tilt_series/adapters/fs_motion_ctf.py``) already parses the ``<CTF>``
block of the same XMLs for defocus; this is the read-time complement for the
quality scalars it doesn't ingest.

Units note: ``MeanFrameMovement`` carries no unit in the file. Warp convention is
Ångström (not pixels) — surfaced as unverified where displayed. See
``docs/preprocessing-metrics-inventory.md`` §4 and §8.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)

WARP_FRAMESERIES_DIR = "warp_frameseries"


class FrameQuality(NamedTuple):
    ctf_resolution: float | None  # Å, lower is better
    mean_frame_movement: float | None  # WarpTools units (≈ Å)


# str(path) -> (mtime, FrameQuality). The XMLs are immutable once the job
# finishes, so an mtime key is a safe, cheap cache (mirrors the atlas-index
# memo pattern in the dashboard).
_MEMO: dict[str, tuple[float, FrameQuality]] = {}


def stem_for_movie(movie_name: object) -> str:
    """Frame stem == ``Path(movie).stem`` — this is both the WarpTools XML
    basename and the registry ``Frame.id`` (see the fs_motion_ctf adapter)."""
    return Path(str(movie_name)).stem


def read_frame_quality(warp_dir: Path, frame_stem: str) -> FrameQuality | None:
    """Return the ``FrameQuality`` for one tilt movie, or ``None`` if the XML is
    missing / unparseable. ``warp_dir`` is the ``warp_frameseries`` folder of the
    FS-motion job; ``frame_stem`` is ``stem_for_movie(rlnMicrographMovieName)``."""
    xml_path = Path(warp_dir) / f"{frame_stem}.xml"
    try:
        mtime = xml_path.stat().st_mtime
    except OSError:
        return None
    key = str(xml_path)
    hit = _MEMO.get(key)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    try:
        root = ET.parse(xml_path).getroot()  # <Movie ...>
        q = FrameQuality(
            ctf_resolution=_positive_float(root.get("CTFResolutionEstimate")),
            mean_frame_movement=_positive_float(root.get("MeanFrameMovement")),
        )
    except Exception as e:
        logger.warning("Could not parse frameseries XML %s: %s", xml_path, e)
        return None
    _MEMO[key] = (mtime, q)
    return q


def quality_series(warp_dir: Path, movie_names: list) -> tuple[list, list]:
    """Read (ctf_resolution, mean_frame_movement) for a list of movie names,
    returning two parallel lists aligned to ``movie_names`` (None where the XML
    is missing). Convenience for building per-tilt chart series."""
    res: list = []
    motion: list = []
    for mv in movie_names:
        q = read_frame_quality(warp_dir, stem_for_movie(mv))
        res.append(q.ctf_resolution if q else None)
        motion.append(q.mean_frame_movement if q else None)
    return res, motion


def _positive_float(v: object) -> float | None:
    """Coerce to float; treat non-positive as absent. WarpTools uses -1 / 0
    sentinels for "not computed", and a resolution/motion of 0 is never a real
    measurement — so we surface those as None rather than plotting a spurious
    point (no invented defaults)."""
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None
