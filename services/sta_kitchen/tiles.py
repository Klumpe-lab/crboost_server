"""The kitchen's cutouts: cut on demand from each row's own reconstruction, one physical scale.

A tile is the mean of a fixed-Å Z slab through a box of fixed Å around the pick (`CUTOUT_BOX_FACTOR` × the
species' diameter unless the pool says otherwise), normalised with its frame's bounds (1–99 % of the first
batch's cutouts, topped up from the frame's other picks to NORM_SAMPLE, computed once and kept in
``norm.json``, so a noise tile stays grey), flipped to the slab pictures' convention (Y up), resized to
TILE_PX and written as a JPEG. A pick whose box falls outside its volume gets a ``.none`` file saying so,
and is not cut again.

Tiles live in ``<kitchen>/tiles/<frame>/<spec>/``: the spec names the volume's mtime and the box, slab
and tile sizes, so changing any of them starts a new directory. The browser asks for a screen of tiles
in one request (`service.tile_records`): what is cut already comes from memory or from one listing of
the frame's directory (`STORE`); the rest is cut by `BATCHER`, every frame's tiles at once, several
frames per worker call on threads (a cutout's read waits on NFS, and numpy and PIL let go of the GIL
while they copy, average and encode), the frames asked for first cut first.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import os
import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from services.visualization.recon_cutouts import _crop_slab

logger = logging.getLogger(__name__)

TILE_PX = 160
TILE_QUALITY = 85
TILE_SCHEMA = 2
# Cutouts a frame's normalisation is taken from. A cutout costs ~1.3 MB of NFS reads (its rows are 260 bytes
# in 4 KB pages), so on a σ-sorted screen, one or two tiles from each of dozens of frames, the sample is
# most of what is read.
NORM_SAMPLE = 3
NORM_FILE = "norm.json"
# Box half-edge limits in voxels: below, a tile shows nothing; above, one tile reads megabytes.
HALF_BOX_MIN, HALF_BOX_MAX = 8, 128
# Without a species diameter there is no common scale: every frame gets this window.
FALLBACK_BOX_PX = 48
# How long a batch waits for more requests of its frame before it is cut.
BATCH_DELAY_S = 0.02
# Worker calls cutting at once (the KITCHEN_TILE cap of services/workers.py), the frames each call takes,
# and the threads it cuts them on.
TILE_SLOTS = 3
FRAMES_PER_CALL = 4
FRAME_THREADS = 4
# The cutouts the server keeps in memory, the most recently served first.
STORE_BYTES = 96 << 20
OUTSIDE = "the box around this pick lies outside its reconstruction"


@dataclass(frozen=True, slots=True)
class TileSpec:
    recon: str
    out_dir: str
    half_box: int
    half_slab: int
    scaled: bool  # False: the fallback window, no common physical scale
    # "recon": cut from the reconstruction; "ext": each pick's extracted subtomogram (`render_ext_tiles`)
    kind: str = "recon"


def ext_spec(tiles_root: Path, star: str, mtime: float) -> TileSpec:
    """How a frame's tiles of one extraction job's subtomograms (`star`: its particles.star, at `mtime`) are
    made: a directory of their own, so a new extraction starts a new one."""
    raw = f"ext|{TILE_SCHEMA}|{star}|{mtime}|{TILE_PX}|{TILE_QUALITY}"
    return TileSpec("", str(tiles_root / f"x{hashlib.sha1(raw.encode()).hexdigest()[:8]}"), 0, 0, False, "ext")


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


def record(uid: str, data: bytes | str) -> bytes:
    """One record of a tiles response: the 12-character uid, then b"j" and a JPEG, or b"r" and why there is
    none (UTF-8), each preceded by its length (uint32 LE). ``sta_kitchen.js`` `TileReader` reads them."""
    body, kind = (data, b"j") if isinstance(data, bytes) else (data.encode(), b"r")
    return uid.encode()[:12].ljust(12, b"_") + kind + struct.pack("<I", len(body)) + body


# ── Worker side ──────────────────────────────────────────────────────────────────────────────────

# Normalisation bounds this worker has read, by tile directory.
_NORMS: dict[str, tuple[float, float]] = {}


def _bounds(out: Path, data, crops: list, sample: list[tuple[float, float, float]], half_box: int, half_slab: int):
    """The frame's normalisation, from norm.json; else from `crops` (the batch's own cutouts) and as many
    of `sample` as make NORM_SAMPLE, then kept."""
    hit = _NORMS.get(str(out))
    if hit is not None:
        return hit
    path = out / NORM_FILE
    try:
        b = json.loads(path.read_text())
        _NORMS[str(out)] = float(b["lo"]), float(b["hi"])
        return _NORMS[str(out)]
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
    _NORMS[str(out)] = lo, hi
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


def render_frames(jobs: list[tuple[TileSpec, list[tuple[str, float, float, float]], list]]) -> list[dict]:
    """`render_tiles` for several frames, FRAME_THREADS at a time; a frame that cannot be read gives its
    tiles the reason and leaves the others cut. Worker-safe."""
    from concurrent.futures import ThreadPoolExecutor

    def one(job) -> dict[str, bytes | str]:
        spec, items, sample = job
        try:
            if spec.kind == "ext":
                return render_ext_tiles(spec, items)
            return render_tiles(spec, items, sample)
        except (OSError, ValueError) as e:
            logger.exception("Kitchen tiles of %s could not be cut", spec.recon)
            return {u: f"cutting failed: {type(e).__name__}: {e}" for u, *_xyz in items}

    if len(jobs) == 1:
        return [one(jobs[0])]
    with ThreadPoolExecutor(min(len(jobs), FRAME_THREADS)) as pool:
        return list(pool.map(one, jobs))


def render_tiles(
    spec: TileSpec, items: list[tuple[str, float, float, float]], sample: list[tuple[float, float, float]]
) -> dict[str, bytes | str]:
    """Cut `items` ((uid, x, y, z) in binned voxels) from `spec.recon` into `spec.out_dir`. Returns
    {uid: the JPEG written} and {uid: reason} for one that cannot be cut."""
    import mrcfile
    from PIL import Image

    out = Path(spec.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # Mapped for this call only: a crop is a copy, and mapping again costs a header read.
    with mrcfile.mmap(spec.recon, mode="r", permissive=True) as m:
        frames = [_crop(m.data, x, y, z, spec.half_box, spec.half_slab) for _u, x, y, z in items]
        lo, hi = _bounds(out, m.data, frames, sample, spec.half_box, spec.half_slab)
    result: dict[str, bytes | str] = {}
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
        result[uid] = buf.getvalue()
    return result


def render_ext_tiles(spec: TileSpec, items: list[tuple[str, str, tuple | None]]) -> dict[str, bytes | str]:
    """Tiles of extracted subtomograms: (uid, its .mrcs, its visible tilts) → the mean of the visible
    tilts (a 3D subtomogram: of its Z), as the Journey's extracted cutouts are made, normalised with the
    bounds of the first NORM_SAMPLE of the frame's (kept in norm.json), flipped to Y up like every tile.
    A stack that cannot be read gives its reason and is tried again next time."""
    from PIL import Image

    from services.visualization.preview_render import _load_subtomo_mean_frame

    out = Path(spec.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    means = [_load_subtomo_mean_frame(Path(mrcs), list(visible) if visible else None) for _u, mrcs, visible in items]
    key = str(out)
    lo_hi = _NORMS.get(key)
    if lo_hi is None:
        try:
            b = json.loads((out / NORM_FILE).read_text())
            lo_hi = float(b["lo"]), float(b["hi"])
        except (OSError, ValueError, KeyError):  # not computed yet for this extraction and frame
            arrs = [m[0] for m in means if m[0] is not None][:NORM_SAMPLE]
            flat = np.concatenate([a.ravel() for a in arrs]) if arrs else np.zeros(0)
            flat = flat[np.isfinite(flat)]
            lo, hi = (float(v) for v in np.percentile(flat, [1.0, 99.0])) if flat.size else (0.0, 1.0)
            lo_hi = lo, (hi if hi > lo else lo + 1.0)
            if arrs:
                _write_atomic(
                    out / NORM_FILE, json.dumps({"lo": lo_hi[0], "hi": lo_hi[1], "n_sample": len(arrs)}).encode()
                )
        _NORMS[key] = lo_hi
    lo, hi = lo_hi
    result: dict[str, bytes | str] = {}
    for (uid, _mrcs, _visible), (arr, err, _apix) in zip(items, means, strict=True):
        if arr is None:
            result[uid] = f"its extracted subtomogram could not be read: {err}"
            continue
        u8 = (np.clip((np.nan_to_num(arr, nan=(lo + hi) / 2) - lo) / (hi - lo), 0.0, 1.0) * 255.0).astype(np.uint8)
        img = Image.fromarray(np.flipud(u8), mode="L").resize((TILE_PX, TILE_PX), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=TILE_QUALITY)
        _write_atomic(out / f"{uid}.jpg", buf.getvalue())
        result[uid] = buf.getvalue()
    return result


# ── Server side ──────────────────────────────────────────────────────────────────────────────────


class TileStore:
    """The tiles cut already: the bytes of the most recently served (up to STORE_BYTES), and for each tile
    directory which uids are cut or cannot be, from one listing of it instead of a stat per tile on NFS.
    Only this server writes the directories, so a listing stays true once `put` keeps it up. Thread-safe:
    `lookup` runs in a thread, `put` on the loop."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._bytes: OrderedDict[tuple[str, str], bytes] = OrderedDict()
        self._size = 0
        self._dirs: dict[str, dict[str, str]] = {}  # tile directory -> {uid: "" when cut, else the reason}
        self._lock = threading.Lock()

    def _listing(self, out_dir: str) -> dict[str, str]:
        with self._lock:
            hit = self._dirs.get(out_dir)
        if hit is not None:
            return hit
        found: dict[str, str] = {}
        try:
            with os.scandir(out_dir) as entries:
                for e in entries:
                    stem, _dot, ext = e.name.rpartition(".")
                    if ext == "jpg":
                        found[stem] = ""
                    elif ext == "none":
                        try:
                            found[stem] = Path(e.path).read_text() or OUTSIDE
                        except OSError:  # removed meanwhile: it is cut again on the next ask
                            continue
        except FileNotFoundError:  # nothing cut for this spec yet
            pass
        with self._lock:
            return self._dirs.setdefault(out_dir, found)

    def lookup(self, wanted: list[tuple[str, str]]) -> dict[str, bytes | str]:
        """{uid: JPEG | why there is none} for the (tile directory, uid) pairs cut already. Blocking."""
        out: dict[str, bytes | str] = {}
        for out_dir, uid in wanted:
            with self._lock:
                data = self._bytes.get((out_dir, uid))
                if data is not None:
                    self._bytes.move_to_end((out_dir, uid))
            if data is not None:
                out[uid] = data
                continue
            state = self._listing(out_dir).get(uid)
            if state is None:
                continue
            if state:
                out[uid] = state
                continue
            try:
                data = (Path(out_dir) / f"{uid}.jpg").read_bytes()
            except FileNotFoundError:  # removed since the listing: cut again
                with self._lock:
                    self._dirs.get(out_dir, {}).pop(uid, None)
                continue
            self._remember(out_dir, uid, data)
            out[uid] = data
        return out

    def put(self, out_dir: str, results: dict[str, bytes | str]) -> None:
        """What a worker just cut, or found outside the volume, in `out_dir`; a failure is not kept, so
        the next request tries again."""
        with self._lock:
            listing = self._dirs.get(out_dir)
            if listing is not None:
                for uid, data in results.items():
                    if isinstance(data, bytes) or data == OUTSIDE:
                        listing[uid] = "" if isinstance(data, bytes) else data
        for uid, data in results.items():
            if isinstance(data, bytes):
                self._remember(out_dir, uid, data)

    def _remember(self, out_dir: str, uid: str, data: bytes) -> None:
        with self._lock:
            old = self._bytes.pop((out_dir, uid), None)
            if old is not None:
                self._size -= len(old)
            self._bytes[(out_dir, uid)] = data
            self._size += len(data)
            while self._size > self._max and self._bytes:
                _key, dropped = self._bytes.popitem(last=False)
                self._size -= len(dropped)


@dataclass(eq=False)
class Batch:
    spec: TileSpec
    sample: list[tuple[float, float, float]]
    done: asyncio.Future
    items: dict[str, tuple[float, float, float]] = field(default_factory=dict)
    waiters: int = 0  # requests still waiting for it


class TileBatcher:
    """Collects tile requests per frame for BATCH_DELAY_S, then cuts the frames asked for together in
    worker calls of FRAMES_PER_CALL, TILE_SLOTS calls at a time, in the order the frames were asked for.
    A tile asked for twice is cut once; a frame whose every request has gone (the browser scrolled on and
    dropped it) is not cut at all."""

    def __init__(self) -> None:
        self._open: dict[str, Batch] = {}
        self._inflight: dict[tuple[str, str], Batch] = {}  # (tile directory, uid) -> the batch cutting it
        self._window: list[Batch] = []  # batches opened since the last flush, in order
        self._tasks: set[asyncio.Task] = set()
        self._slots: asyncio.Semaphore | None = None

    def request(self, spec: TileSpec, items: list[tuple[str, tuple[float, float, float]]], sample) -> list[Batch]:
        """The batches that will cut `items` of one frame; the caller waits on their `done` and hands them
        back to `release` once it stops waiting."""
        loop = asyncio.get_running_loop()
        batches: list[Batch] = []
        for uid, xyz in items:
            batch = self._inflight.get((spec.out_dir, uid))
            if batch is None:
                batch = self._open.get(spec.out_dir)
                if batch is None:
                    batch = self._open[spec.out_dir] = Batch(spec, sample, loop.create_future())
                    if not self._window:
                        self._spawn(self._flush())
                    self._window.append(batch)
                batch.items[uid] = xyz
                self._inflight[(spec.out_dir, uid)] = batch
            if batch not in batches:
                batches.append(batch)
        for batch in batches:
            batch.waiters += 1
        return batches

    @staticmethod
    def release(batches: list[Batch]) -> None:
        for batch in batches:
            batch.waiters -= 1

    def _spawn(self, coro) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _flush(self) -> None:
        await asyncio.sleep(BATCH_DELAY_S)
        window, self._window = self._window, []
        for batch in window:
            if self._open.get(batch.spec.out_dir) is batch:
                del self._open[batch.spec.out_dir]
        for i in range(0, len(window), FRAMES_PER_CALL):
            self._spawn(self._cut(window[i : i + FRAMES_PER_CALL]))

    async def _cut(self, batches: list[Batch]) -> None:
        from services.workers import WorkKind, run_in_worker

        if self._slots is None:
            self._slots = asyncio.Semaphore(TILE_SLOTS)
        results: list[dict[str, bytes | str]] = [{} for _b in batches]
        try:
            async with self._slots:
                live = [i for i, b in enumerate(batches) if b.waiters > 0]
                if live:
                    jobs = [
                        (batches[i].spec, [(u, *xyz) for u, xyz in batches[i].items.items()], batches[i].sample)
                        for i in live
                    ]
                    cut = await run_in_worker(WorkKind.KITCHEN_TILE, render_frames, jobs)
                    for i, result in zip(live, cut, strict=True):
                        results[i] = result
                        STORE.put(batches[i].spec.out_dir, result)
        except Exception as e:
            # Every request waiting on these frames gets the reason; the next request tries again.
            logger.exception("Kitchen tiles of %d frame(s) could not be cut", len(batches))
            results = [{u: f"cutting failed: {type(e).__name__}: {e}" for u in b.items} for b in batches]
        finally:
            for batch, result in zip(batches, results, strict=True):
                for uid in batch.items:
                    if self._inflight.get((batch.spec.out_dir, uid)) is batch:
                        del self._inflight[(batch.spec.out_dir, uid)]
                if not batch.done.done():
                    batch.done.set_result(result)


STORE = TileStore(STORE_BYTES)
BATCHER = TileBatcher()


def mean_image(tiles: list[bytes], out: str) -> int:
    """The unaligned mean of JPEG tiles, normalised 1–99 % and written as a PNG; worker-safe. Returns how
    many tiles went in."""
    from PIL import Image

    acc, n = None, 0
    for data in tiles:
        a = np.asarray(Image.open(io.BytesIO(data)), dtype=np.float32)
        acc = a if acc is None else acc + a
        n += 1
    if acc is None:
        raise ValueError("no tiles to average")
    acc /= n
    lo, hi = np.percentile(acc, [1.0, 99.0])
    u8 = (np.clip((acc - lo) / max(hi - lo, 1e-6), 0.0, 1.0) * 255.0).astype(np.uint8)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(out).with_name(f".{Path(out).name}.{os.getpid()}.tmp")
    Image.fromarray(u8, mode="L").save(tmp, format="PNG")
    os.replace(tmp, out)
    return n
