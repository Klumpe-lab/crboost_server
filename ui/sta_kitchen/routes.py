"""The kitchen's routes: the pool table, a screen of cutouts, a slab, a mean.

Each answers only for a pool a tab of this server has opened, in a home project this server has
loaded, and only for picks and frames of that pool's table: a URL cannot name an arbitrary file.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from fastapi import Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from nicegui import app

from services.http_responses import NO_GZIP
from services.sta_kitchen import service, store
from services.sta_kitchen.snapshots import is_installed

_IMMUTABLE = "public, max-age=31536000, immutable"
_LOUPE_NAME = re.compile(r"[0-9a-f]{12}-[0-9a-f]{8}\.png")
logger = logging.getLogger(__name__)
# Cutouts one tiles request may ask for: a screen of S tiles and the overscan around it.
MAX_TILES = 600


def _pool(home: str, species: str) -> service.LoadedPool | None:
    lp = service.loaded(Path(home), species)
    if lp is None or lp.table is None or not is_installed(lp.home):
        return None
    return lp


def _missing(reason: str) -> Response:
    return Response(content=reason, status_code=404, media_type="text/plain")


@app.get("/api/kitchen/table")
async def kitchen_table(home: str, species: str, v: str = "") -> Response:
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    data = await service.payload(lp)
    if data is None:
        return _missing("the pool table is not built yet")
    cache = _IMMUTABLE if v == lp.table.version else "no-cache"
    # Compressed once per version by the service; the browser inflates it as it reads.
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Cache-Control": cache, "Content-Encoding": "gzip"},
    )


@app.post("/api/kitchen/tiles")
async def kitchen_tiles(request: Request, home: str, species: str, v: str = "", src: str = "recon") -> Response:
    """The cutouts of the uids in the body ({"uids": [...]}, the visible ones first), streamed as records
    (`services/sta_kitchen/tiles.py` `record`) as they are found or cut. `v` names the tile settings the
    browser keeps them under; the cutouts are always those of the pool's current settings. `src` "ext":
    the picks' extracted subtomograms instead."""
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    body = await request.json()
    uids = [str(u) for u in (body.get("uids") or [])][:MAX_TILES]
    return StreamingResponse(
        service.tile_records(lp, uids, "ext" if src == "ext" else "recon"),
        media_type="application/octet-stream",
        headers={"Cache-Control": "no-store", **NO_GZIP},
    )


@app.get("/api/kitchen/slab")
async def kitchen_slab(home: str, species: str, frame: int, which: str = "xy", v: str = "") -> Response:
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    path, reason = await service.slab(lp, frame, "xz" if which == "xz" else "xy")
    if path is None:
        return _missing(reason)
    # A slab read in place from another project can be re-rendered there: a short TTL, not immutable.
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=300", **NO_GZIP})


@app.get("/api/kitchen/loupe")
async def kitchen_loupe(home: str, species: str, uid: str) -> Response:
    """One pick up close (`service.loupe`): JSON naming its picture and what is in it, made on the first ask."""
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    try:
        info = await service.loupe(lp, uid)
    except (OSError, ValueError) as e:
        logger.exception("Kitchen loupe of %s could not be made", uid)
        info = {"error": f"the loupe could not be made: {e}"}
    return JSONResponse(info, headers={"Cache-Control": "no-store"})


@app.get("/api/kitchen/loupe.png")
async def kitchen_loupe_png(home: str, species: str, name: str) -> Response:
    lp = _pool(home, species)
    if lp is None or not _LOUPE_NAME.fullmatch(name):
        return _missing("no such loupe")
    path = store.kitchen_dir(lp.home, lp.species_id) / "loupe" / name
    if not await asyncio.to_thread(path.exists):
        return _missing("no such loupe")
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": _IMMUTABLE, **NO_GZIP})


@app.get("/api/kitchen/average")
async def kitchen_average(home: str, species: str, which: str = "kept", v: str = "") -> Response:
    lp = _pool(home, species)
    entry = ((lp.averages or {}).get(which) or {}) if lp is not None else {}
    if not entry.get("file") or entry.get("digest") != v:
        return _missing("no such average")
    return FileResponse(entry["file"], media_type="image/png", headers={"Cache-Control": _IMMUTABLE, **NO_GZIP})
