"""The X/Y and X/Z slabs under a frame's dots.

A home-project frame uses the slabs the Journey and the tomogram wall use (``<job>/vis/slabs/``, the
same Render key), so the picture is rendered once for all of them. Another project's frame shows its
own slabs in place when they are current, and otherwise renders into the home kitchen's ``slabs/``:
nothing is written into a project the kitchen does not own.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from services.kitchen import store
from services.renders import Render
from services.visualization.preview_render import is_output_stale, render_recon_slabs


def _job_dir(recon: Path) -> Path | None:
    """The job directory a reconstruction was written into: the nearest ancestor with a job.star."""
    for parent in recon.parents:
        if (parent / "job.star").exists():
            return parent
        if (parent / "project_params.json").exists():
            return None
    return None


def slab_paths(home: Path, species_id: str, frame: dict[str, Any]) -> tuple[Path, Path, Render | None]:
    """(X/Y png, X/Z png, the Render that makes them or None when they are current in place).
    Blocking: stats."""
    recon = Path(frame["recon"])
    tomo = frame["tomo"]
    job = _job_dir(recon)
    in_home = recon.is_relative_to(home)
    if job is not None:
        xy, xz = job / "vis" / "slabs" / f"{tomo}_xy.png", job / "vis" / "slabs" / f"{tomo}_xz.png"
        if in_home:
            return xy, xz, _render(xy, xz, recon, tomo, home)
        if not is_output_stale(xy, [recon]) and not is_output_stale(xz, [recon]):
            return xy, xz, None
    base = store.slabs_dir(home, species_id, frame["key"])
    xy, xz = base / f"{tomo}_xy.png", base / f"{tomo}_xz.png"
    return xy, xz, _render(xy, xz, recon, tomo, home)


def _render(xy: Path, xz: Path, recon: Path, tomo: str, home: Path) -> Render:
    return Render(
        key=f"slabs:{xy}",
        title=f"Tomogram slabs · {tomo}",
        subtitle="X/Y and X/Z slabs under the picks",
        fn=render_recon_slabs,
        args=(recon, xy, xz),
        outputs=(xy, xz),
        sources=(recon,),
        quiet=True,
        project_path=str(home),
    )
