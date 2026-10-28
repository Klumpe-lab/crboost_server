"""The kitchen's three routes: the pool table, a cutout, a slab.

Each answers only for a pool a tab of this server has opened, in a home project this server has
loaded, and only for picks and frames of that pool's table: a URL cannot name an arbitrary file.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import Response
from fastapi.responses import FileResponse
from nicegui import app

from services.http_responses import NO_GZIP
from services.kitchen import service
from services.kitchen.snapshots import is_installed

_IMMUTABLE = "public, max-age=31536000, immutable"


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
    return Response(content=data, media_type="application/octet-stream", headers={"Cache-Control": cache, **NO_GZIP})


@app.get("/api/kitchen/tile")
async def kitchen_tile(home: str, species: str, uid: str, v: str = "") -> Response:
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    path, reason = await service.tile(lp, uid)
    if path is None:
        return _missing(reason)
    return FileResponse(path, media_type="image/jpeg", headers={"Cache-Control": _IMMUTABLE, **NO_GZIP})


@app.get("/api/kitchen/slab")
async def kitchen_slab(home: str, species: str, frame: int, which: str = "xy", v: str = "") -> Response:
    lp = _pool(home, species)
    if lp is None:
        return _missing("no such pool open in this server")
    path, reason = await service.slab(lp, frame, "xz" if which == "xz" else "xy")
    if path is None:
        return _missing(reason)
    # A slab read in place from another project can be re-rendered there: a short TTL, not immutable.
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "public, max-age=300", **NO_GZIP})


@app.get("/api/kitchen/average")
async def kitchen_average(home: str, species: str, which: str = "kept", v: str = "") -> Response:
    lp = _pool(home, species)
    entry = ((lp.averages or {}).get(which) or {}) if lp is not None else {}
    if not entry.get("file") or entry.get("digest") != v:
        return _missing("no such average")
    return FileResponse(entry["file"], media_type="image/png", headers={"Cache-Control": _IMMUTABLE, **NO_GZIP})
