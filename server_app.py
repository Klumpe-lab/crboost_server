"""The web app: FastAPI routes, the NiceGUI UI and the background services, and serving it on a port crboost
has already bound. Heavy to import (NiceGUI, the backend), so crboost imports it only once the port is settled."""

import asyncio
import functools
import io
import os
import socket
import warnings
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from nicegui import app as ng_app, ui
from nicegui.storage import Storage
from starlette.middleware.sessions import SessionMiddleware

from backend import CryoBoostBackend
from services import workers
from services.configs.config_service import REPO_ROOT
from services.http_responses import NO_GZIP, json_headers
from services.loop_watchdog import LoopWatchdog
from services.project_state import is_in_loaded_project
from services.server_auth import (
    NICEGUI_STORAGE_DIR,
    STORAGE_SECRET_FILE,
    TokenGate,
    read_or_create_secret,
    session_cookie_name,
)
from ui.main_ui import create_ui_router

warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

# Ctrl-C waits this long for open browser tabs to let go before closing them, instead of for good.
_GRACEFUL_SHUTDOWN_S = 5


@functools.lru_cache(maxsize=512)
def _preview_thumb(path: str, mtime_ns: int, size: int = 512) -> bytes | None:
    """A `size` px JPEG of a preview PNG (a WarpTools one is about 1 MB at 1024 px): the projects
    roster's hover card and the tomogram wall's tiles fetch these, often over an SSH tunnel. Keyed
    on mtime so a re-reconstruction is never served stale. None without Pillow."""
    try:
        from PIL import Image
    except ImportError:
        # Bare venv without Pillow: the caller serves the full PNG instead.
        return None
    with Image.open(path) as im:
        small = im.convert("L")
        small.thumbnail((size, size))
        buf = io.BytesIO()
        small.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def setup_app(port: int):
    """Configures and returns the FastAPI app."""
    app = FastAPI()

    @app.get("/api/tilt-thumb")
    def serve_tilt_thumb(path: str):
        try:
            p = Path(path).resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            return {"error": "not found"}
        if p.suffix != ".png" or not p.is_file():
            return {"error": "not found"}
        if not is_in_loaded_project(p):
            return {"error": "outside project roots"}
        return FileResponse(p, media_type="image/png", headers={"Cache-Control": "public, max-age=86400", **NO_GZIP})

    @app.get("/api/vis-asset")
    def serve_vis_asset(path: str, v: str = "", thumb: int = 0):
        # Path-traversal guard: resolve and require the file to live under a
        # currently-loaded project root. The project-state registry is the
        # authoritative list of roots a user has opened in this session.
        # Serves both PNG (panel/atlas images) and JSON (stamps_index, manifests)
        # from the same endpoint, since the candidate-preview UI needs both.
        try:
            resolved = Path(path).resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            return {"error": "not found"}
        media_by_suffix = {".png": "image/png", ".json": "application/json"}
        media = media_by_suffix.get(resolved.suffix)
        if media is None:
            return {"error": "unsupported asset type"}
        if not is_in_loaded_project(resolved):
            return {"error": "outside project roots"}
        # JSON manifests change on every regen — never cache them. A PNG URL carrying the file's mtime
        # (`v`, from vis_asset_url) names one version of the file, so the browser may keep it for good:
        # a rewrite changes the URL. Without `v`, a short TTL keeps the copy self-correcting.
        if resolved.suffix == ".json":
            return FileResponse(resolved, media_type=media, headers=json_headers(resolved.stat().st_size, "no-cache"))
        cache_header = "public, max-age=31536000, immutable" if v not in ("", "0") else "public, max-age=300"
        # `thumb` (vis_asset_url): a downscaled JPEG of a PNG, for a wall of tiles that would
        # otherwise pull and decode a full-size picture per tile.
        if thumb and resolved.suffix == ".png":
            small = _preview_thumb(str(resolved), resolved.stat().st_mtime_ns, min(max(thumb, 64), 2048))
            if small is not None:
                return Response(
                    content=small, media_type="image/jpeg", headers={"Cache-Control": cache_header, **NO_GZIP}
                )
        return FileResponse(resolved, media_type=media, headers={"Cache-Control": cache_header, **NO_GZIP})

    @app.get("/api/project-preview")
    def serve_project_preview(path: str):
        # Projects-roster hover: the roster lists projects nobody has loaded, so
        # vis-asset's loaded-roots guard cannot apply. Narrower instead: only a WarpTools
        # reconstruction PNG, and only inside a directory tree that holds a project.
        try:
            resolved = Path(path).resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            return {"error": "not found"}
        recon_dir = resolved.parent
        is_recon_png = recon_dir.name == "reconstruction" and recon_dir.parent.name == "warp_tiltseries"
        if resolved.suffix != ".png" or not is_recon_png:
            return {"error": "not a tomogram preview"}
        if not any((p / "project_params.json").is_file() for p in resolved.parents):
            return {"error": "outside any project"}
        cache = {"Cache-Control": "public, max-age=300", **NO_GZIP}
        thumb = _preview_thumb(str(resolved), resolved.stat().st_mtime_ns)
        if thumb is None:
            return FileResponse(resolved, media_type="image/png", headers=cache)
        return Response(content=thumb, media_type="image/jpeg", headers=cache)

    app.mount("/static", StaticFiles(directory=REPO_ROOT / "static"), name="static")
    # mtime-based cache-buster: the browser refetches main.css whenever we edit it,
    # so dev iteration doesn't require Cmd-Shift-R after every CSS change.
    css_path = REPO_ROOT / "static" / "main.css"
    css_version = int(css_path.stat().st_mtime) if css_path.exists() else 0
    ui.add_head_html(f"""
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap"
              rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css?v={css_version}">
    """)

    backend = CryoBoostBackend(REPO_ROOT)
    create_ui_router(backend)

    # Single server-side pipeline observer: runs reconcile_afterok centrally
    # on a 3-s tick for every project with pipeline_active=True, and on
    # startup loads the projects that were mid-pipeline when uvicorn last
    # died, so the tick picks their chains up again. See docs/architecture.md.
    async def _start_pipeline_monitor():
        # Logs where the event loop was whenever it stalls, to the log file (services/loop_watchdog.py).
        LoopWatchdog(asyncio.get_running_loop()).start()
        workers.warm_up()
        await backend.pipeline_monitor.start()
        await backend.curation_watcher.start()

    async def _stop_pipeline_monitor():
        await backend.pipeline_monitor.stop()
        await backend.curation_watcher.stop()
        workers.shutdown()

    # add_event_handler is the non-deprecated spelling of @app.on_event.
    app.add_event_handler("startup", _start_pipeline_monitor)
    app.add_event_handler("shutdown", _stop_pipeline_monitor)

    # Browser storage belongs to the user running the server, not to the install: files under ~/.crboost,
    # and a session cookie named per port. NiceGUI only adds its own SessionMiddleware (cookie "session")
    # when none is registered on its app, so ours must be in place before ui.run_with.
    NICEGUI_STORAGE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    Storage.path = NICEGUI_STORAGE_DIR
    storage_secret = os.environ.get("CRBOOST_STORAGE_SECRET") or read_or_create_secret(STORAGE_SECRET_FILE)
    ng_app.add_middleware(SessionMiddleware, secret_key=storage_secret, session_cookie=session_cookie_name(port))

    # Default reconnect_timeout is 3s, which sets ping_interval=4s / ping_timeout=2s
    # (nicegui/nicegui.py:129-130). Over an SSH tunnel any latency blip trips the
    # 2s pong deadline → socket drops → client teardown after 3s → full rebuild.
    # 30s is generous for tunneled sessions and still catches real disconnects.
    reconnect_timeout = float(os.environ.get("CRBOOST_RECONNECT_TIMEOUT", "30"))
    ui.run_with(app, title="CryoBoost Server", storage_secret=storage_secret, reconnect_timeout=reconnect_timeout)
    return app


def serve(app, token: str, port: int, sock: socket.socket) -> None:
    """Serve `app` behind the token gate on `sock`, which crboost bound and listens on."""
    # log_config=None: uvicorn otherwise reinstalls its own logging config at
    # run() time, undoing setup_logging() and re-enabling the per-request
    # access log ("GET / 200 OK", "connection open", ...).
    config = uvicorn.Config(
        TokenGate(app, token, port), port=port, log_config=None, timeout_graceful_shutdown=_GRACEFUL_SHUTDOWN_S
    )
    uvicorn.Server(config).run(sockets=[sock])
