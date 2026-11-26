"""Glyphs from rendered structures, for /dev/glyphs (roadmap 77): six techniques from realism to minimalism.

Reads the renders `render_in_chimerax.py` made (`<name>_soft.png`, ambient occlusion on white; `<name>_line.png`,
flat white with silhouette edges; a second part drawn grey in both) and writes, for each, one ink coverage per
technique, scaled down to the sizes the app draws a species mark at (each at twice its size, for sharp screens):

- render: the soft render's shading as ink (the lit side pale), and its inverse for dark themes;
- toon: the shading in three tones and the outline;
- dither: the shading as one-bit pixels (an ordered dither) on the glyph's own pixel grid, crisp at any size;
- ink: the outline over a faint fill, the second part a shade deeper;
- silhouette: two flat tones, nothing inside;
- outline: the silhouette and the parts' boundary traced into smooth vector curves (SVG, one pixel at any size).

    ./crboost python scripts/glyph_renders/make_glyphs.py RENDERS_DIR name ... [--only toon]

writes `~/.crboost/glyph_ideas/renders.json`, which /dev/glyphs shows when it is there.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
from pathlib import Path

import numpy as np
from PIL import Image
from scipy import ndimage
from skimage import measure

OUT = Path.home() / ".crboost" / "glyph_ideas" / "renders.json"
SIZES = (9, 11, 14, 22, 40, 64)


def _load(path: Path) -> tuple[np.ndarray, np.ndarray]:
    im = np.asarray(Image.open(path).convert("RGBA"), dtype=np.float32) / 255
    return im[..., 3], im[..., :3].mean(axis=2)


def _box(alpha: np.ndarray) -> tuple[int, int, int]:
    """A square around what is drawn, with a little room."""
    rows, cols = np.nonzero(alpha > 0.02)
    r0, r1, c0, c1 = rows.min(), rows.max(), cols.min(), cols.max()
    side = int(max(r1 - r0, c1 - c0) * 1.06) + 2
    return int((r0 + r1) / 2 - side / 2), int((c0 + c1) / 2 - side / 2), side


def _crop(a: np.ndarray, box: tuple[int, int, int]) -> np.ndarray:
    r, c, side = box
    pad = side
    padded = np.pad(a, pad)
    return padded[r + pad : r + pad + side, c + pad : c + pad + side]


def _png(coverage: np.ndarray, px: int) -> str:
    """A coverage map as a mask PNG (black, its alpha the coverage) at `px`, as a data URI."""
    img = Image.fromarray((np.clip(coverage, 0, 1) * 255).astype(np.uint8), "L").resize((px, px), Image.LANCZOS)
    rgba = Image.new("RGBA", (px, px), (0, 0, 0, 0))
    rgba.putalpha(img)
    buf = io.BytesIO()
    rgba.save(buf, "PNG", optimize=True)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


_BAYER = np.array([[0, 8, 2, 10], [12, 4, 14, 6], [3, 11, 1, 9], [15, 7, 13, 5]]) / 16 + 1 / 32


def _dither_png(coverage: np.ndarray, px: int) -> str:
    """The coverage dithered to one bit on a px grid, each pixel doubled (sharp on a 2x screen and on a 1x one)."""
    small = (
        np.asarray(
            Image.fromarray((np.clip(coverage, 0, 1) * 255).astype(np.uint8), "L").resize((px, px), Image.LANCZOS),
            dtype=np.float32,
        )
        / 255
    )
    bits = small > np.tile(_BAYER, (px // 4 + 1, px // 4 + 1))[:px, :px]
    big = np.repeat(np.repeat(bits, 2, axis=0), 2, axis=1)
    rgba = Image.new("RGBA", (2 * px, 2 * px), (0, 0, 0, 0))
    rgba.putalpha(Image.fromarray((big * 255).astype(np.uint8), "L"))
    buf = io.BytesIO()
    rgba.save(buf, "PNG", optimize=True)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()


def _chaikin(pts: np.ndarray, rounds: int = 3) -> np.ndarray:
    for _ in range(rounds):
        nxt = np.roll(pts, -1, axis=0)
        pts = np.stack([0.75 * pts + 0.25 * nxt, 0.25 * pts + 0.75 * nxt], axis=1).reshape(-1, 2)
    return pts


def _outline_svg(alpha: np.ndarray, grey: np.ndarray) -> str:
    """The silhouette and the boundary of the second part, smoothed, in a 24-unit box."""
    n = alpha.shape[0]
    small = 160 / n
    a = ndimage.zoom(ndimage.gaussian_filter(alpha, n / 160), small, order=1)
    g = ndimage.zoom(ndimage.gaussian_filter(grey, n / 160), small, order=1)
    d = []
    for field, level, min_len in ((a, 0.5, 30), (g, 0.5, 30)):
        for c in measure.find_contours(np.pad(field, 2), level):
            if len(c) < min_len:
                continue
            p = _chaikin(measure.approximate_polygon(c, 1.2)[:-1])
            xy = [((x - 2) * 24 / a.shape[0], (y - 2) * 24 / a.shape[0]) for y, x in p]
            d.append("M" + " ".join(f"{x:.2f} {y:.2f}" for x, y in xy) + "Z")
    hair = "fill='none' stroke='black' stroke-width='1' stroke-linejoin='round' vector-effect='non-scaling-stroke'"
    return f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'><path d='{''.join(d)}' {hair}/></svg>"


def techniques(soft_path: Path, line_path: Path, only: set[str] | None = None) -> dict:
    a_soft, l_soft = _load(soft_path)
    a_line, l_line = _load(line_path)
    box = _box(np.maximum(a_soft, a_line))
    a_soft, l_soft, a_line, l_line = (_crop(x, box) for x in (a_soft, l_soft, a_line, l_line))
    lines = a_line * (l_line < 0.35)  # the silhouette edges, drawn black
    flat = np.where(l_line > 0.85, 1.0, 0.0) * a_line  # the white part of the flat render
    grey = ((l_line > 0.35) & (l_line <= 0.85)) * a_line  # the second part, drawn grey
    shade = 1 - l_soft  # 0 lit, up to ~0.7 in the deepest shadow
    bands = np.select([l_soft > 0.82, l_soft > 0.58], [0.28, 0.58], 0.88)
    cov = {
        "render": (a_soft * (0.28 + 0.72 * np.clip(shade / 0.7, 0, 1)), a_soft * (0.22 + 0.78 * np.clip(l_soft, 0, 1))),
        "toon": (np.maximum(a_soft * bands, lines), np.maximum(a_soft * (1.16 - bands), lines)),
        "ink": (np.maximum(lines, 0.10 * flat + 0.34 * grey),) * 2,
        "silhouette": (0.92 * flat + 0.5 * grey,) * 2,
    }
    out = {}
    for name, (light, dark) in cov.items():
        if only and name not in only:
            continue
        out[name] = {"light": {px: _png(light, 2 * px) for px in SIZES}}
        if dark is not light:
            out[name]["dark"] = {px: _png(dark, 2 * px) for px in SIZES}
    if only and "dither" not in only and "outline" not in only:
        return out
    dith = (a_soft * (0.15 + 0.85 * np.clip(shade / 0.7, 0, 1)), a_soft * (0.1 + 0.9 * np.clip(l_soft, 0, 1)))
    out["dither"] = {
        "light": {px: _dither_png(np.maximum(dith[0], lines), px) for px in SIZES},
        "dark": {px: _dither_png(np.maximum(dith[1], lines), px) for px in SIZES},
    }
    out["outline"] = {"svg": _outline_svg(a_line, ndimage.binary_closing(grey > 0.5, iterations=3).astype(float))}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("renders", type=Path)
    ap.add_argument("names", nargs="+", help="render names: <name>_soft.png and <name>_line.png in RENDERS")
    ap.add_argument("--only", help="techniques to make, comma-separated (default all)")
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None
    out = {n: techniques(args.renders / f"{n}_soft.png", args.renders / f"{n}_line.png", only) for n in args.names}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out) + "\n")
    print("wrote", OUT, f"{OUT.stat().st_size / 1e3:.0f} kB")


if __name__ == "__main__":
    main()
