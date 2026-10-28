"""The kitchen's cutouts: cut on demand from each row's own reconstruction, one physical scale.

A tile is the mean of a fixed-Å Z slab through a box of fixed Å around the pick, normalised with its
frame's bounds (1–99 % of the first batch's cutouts and a sample of the frame's picks, NORM_SAMPLE in
all, computed once and kept in ``norm.json``, so a noise tile stays grey), flipped to the slab pictures'
convention (Y up), resized to TILE_PX and written as a JPEG. A pick whose box falls outside its volume
gets a ``.none`` file saying so, and is not cut again.

Tiles live in ``<kitchen>/tiles/<frame>/<spec>/``: the spec names the volume's mtime and the box, slab
and tile sizes, so changing any of them starts a new directory. Requests for one frame that arrive
together are cut in one worker call (`TileBatcher`).
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.visualization.recon_cutouts import _crop_slab

logger = logging.getLogger(__name__)

TILE_PX = 192
TILE_QUALITY = 90
TILE_SCHEMA = 2
NORM_SAMPLE = 8
NORM_FILE = "norm.json"
# Box half-edge limits in voxels: below, a tile shows nothing; above, one tile reads megabytes.
HALF_BOX_MIN, HALF_BOX_MAX = 8, 128
# Without a species diameter there is no common scale: every frame gets this window.
FALLBACK_BOX_PX = 48
# How long a batch waits for more requests of its frame before it is cut.
BATCH_DELAY_S = 0.02
OUTSIDE = "the box around this pick lies outside its reconstruction"


@dataclass(frozen=True, slots=True)
class TileSpec:
    recon: str
    out_dir: str
    half_box: int
    half_slab: int
    scaled: bool  # False: the fallback window, no common physical scale


def tile_spec(frame: dict[str, Any], tiles_root: Path, box_a: float | None, slab_a: float) -> TileSpec | str:
    """How the frame's tiles are cut, or why they cannot be."""
    recon = frame.get("recon") or ""
    if not recon or not frame.get("recon_exists"):
        return "its reconstruction is not on disk"
    apix = frame.get("apix")
    if not apix or apix <= 0:
        return "the reconstruction's pixel size is unknown"
    try:
        mtime = os.stat(recon).st_mtime_ns
    except OSError:
        return "its reconstruction is not on disk"
    if box_a:
        half_box = int(min(max(round(box_a / 2.0 / apix), HALF_BOX_MIN), HALF_BOX_MAX))
    else:
        half_box = FALLBACK_BOX_PX // 2
    half_slab = max(0, round(slab_a / apix) // 2)
    raw = f"{TILE_SCHEMA}|{mtime}|{half_box}|{half_slab}|{TILE_PX}|{TILE_QUALITY}"
    spec = hashlib.sha1(raw.encode()).hexdigest()[:8]
    return TileSpec(recon, str(tiles_root / spec), half_box, half_slab, bool(box_a))


# ── Worker side ──────────────────────────────────────────────────────────────────────────────────

# Open volumes in this worker, by (path, mtime): a frame asked for again is not mapped again.
_OPEN: OrderedDict[tuple[str, int], Any] = OrderedDict()
_OPEN_MAX = 4


def _volume(path: str):
    import mrcfile

    key = (path, os.stat(path).st_mtime_ns)
    hit = _OPEN.get(key)
    if hit is not None:
        _OPEN.move_to_end(key)
        return hit.data
    m = mrcfile.mmap(path, mode="r", permissive=True)
    _OPEN[key] = m
    while len(_OPEN) > _OPEN_MAX:
        _key, old = _OPEN.popitem(last=False)
        old.close()
    return m.data


def _bounds(out: Path, data, crops: list, sample: list[tuple[float, float, float]], half_box: int, half_slab: int):
    """The frame's normalisation, from norm.json; else from `crops` (the batch's own cutouts) and as many
    of `sample` as make NORM_SAMPLE, then kept. A cutout read costs ~25 ms on NFS: the sample is small."""
    path = out / NORM_FILE
    try:
        b = json.loads(path.read_text())
        return float(b["lo"]), float(b["hi"])
    except (OSError, ValueError, KeyError):  # not computed yet for this spec
        pass
    crops = [c for c in crops if c is not None][:NORM_SAMPLE]
    for x, y, z in sample[: max(0, NORM_SAMPLE - len(crops))]:
        crops.append(_crop(data, x, y, z, half_box, half_slab))
    crops = [c for c in crops if c is not None]
    if crops:
        flat = np.concatenate([c.ravel() for c in crops])
        flat = flat[np.isfinite(flat)]
    else:
        flat = np.zeros(0)
    lo, hi = (float(v) for v in np.percentile(flat, [1.0, 99.0])) if flat.size else (0.0, 1.0)
    if hi <= lo:
        hi = lo + 1.0
    _write_atomic(path, json.dumps({"lo": lo, "hi": hi, "n_sample": len(crops)}).encode())
    return lo, hi


def _crop(data, x: float, y: float, z: float, half_box: int, half_slab: int) -> np.ndarray | None:
    """`_crop_slab`, padded with NaN where the box leaves the volume: every tile keeps the same scale."""
    xi, yi = round(x), round(y)
    frame = _crop_slab(data, xi, yi, round(z), half_box, half_slab)
    if frame is None:
        return None
    full = np.full((2 * half_box, 2 * half_box), np.nan, dtype=np.float32)
    y0, x0 = max(0, yi - half_box) - (yi - half_box), max(0, xi - half_box) - (xi - half_box)
    full[y0 : y0 + frame.shape[0], x0 : x0 + frame.shape[1]] = frame[: 2 * half_box - y0, : 2 * half_box - x0]
    return full


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def render_tiles(
    spec: TileSpec, items: list[tuple[str, float, float, float]], sample: list[tuple[float, float, float]]
) -> dict[str, str]:
    """Cut `items` ((uid, x, y, z) in binned voxels) from `spec.recon` into `spec.out_dir`. Returns
    {uid: ""} for a tile written, {uid: reason} for one that cannot be cut. Worker-safe."""
    from PIL import Image

    out = Path(spec.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    data = _volume(spec.recon)
    frames = [_crop(data, x, y, z, spec.half_box, spec.half_slab) for _u, x, y, z in items]
    lo, hi = _bounds(out, data, frames, sample, spec.half_box, spec.half_slab)
    result: dict[str, str] = {}
    for (uid, _x, _y, _z), frame in zip(items, frames, strict=True):
        if frame is None:
            _write_atomic(out / f"{uid}.none", OUTSIDE.encode())
            result[uid] = OUTSIDE
            continue
        u8 = (np.clip((np.nan_to_num(frame, nan=(lo + hi) / 2) - lo) / (hi - lo), 0.0, 1.0) * 255.0).astype(np.uint8)
        img = Image.fromarray(np.flipud(u8), mode="L").resize((TILE_PX, TILE_PX), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=TILE_QUALITY)
        _write_atomic(out / f"{uid}.jpg", buf.getvalue())
        result[uid] = ""
    return result


# ── Server side ──────────────────────────────────────────────────────────────────────────────────


@dataclass
class _Batch:
    spec: TileSpec
    sample: list[tuple[float, float, float]]
    items: dict[str, tuple[float, float, float]] = field(default_factory=dict)
    done: asyncio.Future | None = None


class TileBatcher:
    """Collects tile requests per frame for BATCH_DELAY_S and cuts each frame's in one worker call."""

    def __init__(self) -> None:
        self._open: dict[str, _Batch] = {}
        self._tasks: set[asyncio.Task] = set()
        # (frame dir, uid) -> the batch cutting it, so a tile asked for twice is cut once.
        self._inflight: dict[tuple[str, str], asyncio.Future] = {}

    def add(self, spec: TileSpec, uid: str, xyz: tuple[float, float, float], sample) -> asyncio.Future:
        hit = self._inflight.get((spec.out_dir, uid))
        if hit is not None:
            return hit
        batch = self._open.get(spec.out_dir)
        if batch is None:
            batch = _Batch(spec, sample, done=asyncio.get_running_loop().create_future())
            self._open[spec.out_dir] = batch
            task = asyncio.get_running_loop().create_task(self._flush(spec.out_dir))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        batch.items[uid] = xyz
        self._inflight[(spec.out_dir, uid)] = batch.done
        return batch.done

    async def _flush(self, key: str) -> None:
        from services.workers import WorkKind, run_in_worker

        await asyncio.sleep(BATCH_DELAY_S)
        batch = self._open.pop(key)
        items = [(u, *xyz) for u, xyz in batch.items.items()]
        try:
            result = await run_in_worker(WorkKind.KITCHEN_TILE, render_tiles, batch.spec, items, batch.sample)
        except Exception as e:
            # Every request waiting on this batch gets the reason; the next request tries again.
            logger.exception("Kitchen tiles of %s could not be cut", batch.spec.recon)
            result = {u: f"cutting failed: {type(e).__name__}: {e}" for u, *_xyz in items}
        for uid, *_xyz in items:
            self._inflight.pop((key, uid), None)
        if not batch.done.done():
            batch.done.set_result(result)


BATCHER = TileBatcher()


def cached_tile(spec: TileSpec, uid: str) -> tuple[Path | None, str | None]:
    """(the tile's file, None) when cut, (None, reason) when it cannot be, (None, None) when not yet
    cut. Blocking: two stats."""
    out = Path(spec.out_dir)
    jpg = out / f"{uid}.jpg"
    if jpg.exists():
        return jpg, None
    none = out / f"{uid}.none"
    if none.exists():
        try:
            return None, none.read_text() or OUTSIDE
        except OSError:
            return None, OUTSIDE
    return None, None


def mean_image(paths: list[str], out: str) -> int:
    """The unaligned mean of tiles, normalised 1–99 % and written as a PNG; worker-safe. Returns how many
    tiles went in."""
    from PIL import Image

    acc, n = None, 0
    for p in paths:
        try:
            a = np.asarray(Image.open(p), dtype=np.float32)
        except OSError:  # a tile removed meanwhile: the mean is of the others
            continue
        acc = a if acc is None else acc + a
        n += 1
    if acc is None:
        raise FileNotFoundError("none of the tiles could be read")
    acc /= n
    lo, hi = np.percentile(acc, [1.0, 99.0])
    u8 = (np.clip((acc - lo) / max(hi - lo, 1e-6), 0.0, 1.0) * 255.0).astype(np.uint8)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(out).with_name(f".{Path(out).name}.{os.getpid()}.tmp")
    Image.fromarray(u8, mode="L").save(tmp, format="PNG")
    os.replace(tmp, out)
    return n
