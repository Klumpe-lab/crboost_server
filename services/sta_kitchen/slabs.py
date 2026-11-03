"""The X/Y and X/Z slabs under a frame's dots, as display-sized JPEGs in the home kitchen's ``slabs/``.

A slab PNG is a megabyte and more at the volume's full width; the by-tomogram cards show it a few
hundred pixels wide, so the browser gets a copy VIEW_WIDTH wide (tens of KB). A home-project frame's
copies are made from the slabs the Journey and the tomogram wall use (``<job>/vis/slabs/``, the same
Render key), so the volume is read once for all of them. Another project's frame uses its own slabs in
place when they are current; otherwise its copies are rendered from the volume directly, and no PNG is
kept: nothing is written into a project the kitchen does not own.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from services.sta_kitchen import store
from services.renders import Render
from services.visualization.preview_render import (
    is_output_stale,
    render_recon_slabs,
    render_xy_slab_preview,
    render_xz_slab_preview,
)

# About twice the width a card shows a slab at (280 px; 340 at L): sharp on a 2× screen. A noisy slab
# compresses poorly, so the width is what keeps the file small.
VIEW_WIDTH = 560
VIEW_QUALITY = 80


def _job_dir(recon: Path) -> Path | None:
    """The job directory a reconstruction was written into: the nearest ancestor with a job.star."""
    for parent in recon.parents:
        if (parent / "job.star").exists():
            return parent
        if (parent / "project_params.json").exists():
            return None
    return None


def slab_views(home: Path, species_id: str, frame: dict[str, Any]) -> tuple[Path, Path, list[Render]]:
    """(X/Y jpg, X/Z jpg, the renders that make them, in order). Blocking: stats."""
    recon = Path(frame["recon"])
    tomo = frame["tomo"]
    base = store.slabs_dir(home, species_id, frame["key"])
    xy_jpg, xz_jpg = base / f"{tomo}_xy.jpg", base / f"{tomo}_xz.jpg"
    job = _job_dir(recon)
    if job is not None:
        xy, xz = job / "vis" / "slabs" / f"{tomo}_xy.png", job / "vis" / "slabs" / f"{tomo}_xz.png"
        copies = _render(
            f"slabview:{xy_jpg}", tomo, home, make_views, (xy, xz, xy_jpg, xz_jpg), (xy_jpg, xz_jpg), (xy, xz)
        )
        if recon.is_relative_to(home):
            shared = _render(f"slabs:{xy}", tomo, home, render_recon_slabs, (recon, xy, xz), (xy, xz), (recon,))
            return xy_jpg, xz_jpg, [shared, copies]
        if not is_output_stale(xy, [recon]) and not is_output_stale(xz, [recon]):
            return xy_jpg, xz_jpg, [copies]
    direct = _render(
        f"slabview:{xy_jpg}", tomo, home, views_from_volume, (recon, xy_jpg, xz_jpg), (xy_jpg, xz_jpg), (recon,)
    )
    return xy_jpg, xz_jpg, [direct]


def _render(key: str, tomo: str, home: Path, fn, args: tuple, outputs: tuple, sources: tuple) -> Render:
    return Render(
        key=key,
        title=f"Tomogram slabs · {tomo}",
        subtitle="X/Y and X/Z slabs under the picks",
        fn=fn,
        args=args,
        outputs=outputs,
        sources=sources,
        quiet=True,
        project_path=str(home),
    )


def make_views(xy_png: Path, xz_png: Path, xy_jpg: Path, xz_jpg: Path) -> str:
    """The display copies of two slab PNGs; worker-safe."""
    for png, jpg in ((xy_png, xy_jpg), (xz_png, xz_jpg)):
        _to_jpeg(Path(png), Path(jpg))
    return "slab views written"


def views_from_volume(recon: Path, xy_jpg: Path, xz_jpg: Path) -> str:
    """The display copies straight from a volume of another project, through PNGs that are not kept;
    worker-safe."""
    with tempfile.TemporaryDirectory(prefix="crboost-slab-") as tmp:
        for name, fn, jpg in (("X/Y", render_xy_slab_preview, xy_jpg), ("X/Z", render_xz_slab_preview, xz_jpg)):
            png = Path(tmp) / f"{name[-1]}.png"
            if fn(Path(recon), png, max_dim=VIEW_WIDTH) is None:
                raise RuntimeError(f"{name} slab renderer wrote nothing for {recon}; see the server log")
            _to_jpeg(png, Path(jpg))
    return "slab views written"


def _to_jpeg(png: Path, jpg: Path) -> None:
    from PIL import Image

    img = Image.open(png)
    img = img.convert("L") if img.mode not in ("L", "RGB") else img
    if img.width > VIEW_WIDTH:
        img = img.resize((VIEW_WIDTH, max(1, round(img.height * VIEW_WIDTH / img.width))), Image.LANCZOS)
    jpg.parent.mkdir(parents=True, exist_ok=True)
    tmp = jpg.with_name(f".{jpg.name}.{os.getpid()}.tmp")
    img.save(tmp, format="JPEG", quality=VIEW_QUALITY)
    os.replace(tmp, jpg)
