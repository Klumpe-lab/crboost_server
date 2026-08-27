#!/usr/bin/env python3
import os
import socket
import argparse
from pathlib import Path
import warnings

from ui.main_ui import create_ui_router
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from nicegui import ui

from backend import CryoBoostBackend
from services.event_log import EVENTS_LOGGER_NAME
import logging

import sys
sys.dont_write_bytecode = True

class SuppressPruneStorageError(logging.Filter):
    def filter(self, record):
        return "Request is not set" not in record.getMessage()

warnings.filterwarnings("ignore", message="Pydantic serializer warnings")


def setup_logging(debug: bool = False):
    # Terminal contract: only warnings/errors and the handful of pipeline events
    # routed through `services.event_log` (pipeline started / finished / failed,
    # jobs queued, job status changes) reach the console. Module-level
    # logger.info() is diagnostic and stays hidden unless --debug.
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.WARNING,
        # name:lineno makes every record trackable to its file (roadmap 03 stage 2)
        format="%(asctime)s %(levelname).1s %(name)s:%(lineno)d %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger(EVENTS_LOGGER_NAME).setLevel(logging.INFO)
    # Quiet down noisy third-party loggers
    logging.getLogger("nicegui").setLevel(logging.WARNING)
    logging.getLogger("nicegui").addFilter(SuppressPruneStorageError())
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


_BOLD, _CYAN, _RESET = "\x1b[1m", "\x1b[1;36m", "\x1b[0m"


def banner_lines(port: int, hostname: str) -> list[str]:
    return [
        f"{_BOLD}CryoBoost running on{_RESET} {_CYAN}http://localhost:{port}{_RESET}",
        f"{_BOLD}Tunnel from your local machine:{_RESET} "
        f"{_CYAN}ssh -f -N -L {port}:localhost:{port} $USER@{hostname}{_RESET}",
    ]


def install_sticky_banner(lines: list[str]) -> None:
    """Pin `lines` to the top of the terminal (DECSTBM scroll region) so log
    output scrolls underneath. Plain print when stdout isn't a TTY (redirected
    to a file, systemd, ...) or CRBOOST_PLAIN_LOG is set."""
    import atexit
    import shutil

    if not sys.stdout.isatty() or os.environ.get("CRBOOST_PLAIN_LOG"):
        print("\n".join(lines) + "\n")
        return
    rows = shutil.get_terminal_size().lines
    top = len(lines) + 1
    out = sys.stdout
    out.write("\x1b[2J\x1b[H")  # clear screen, cursor home
    out.write("\n".join(lines) + "\n")
    out.write(f"\x1b[{top};{rows}r\x1b[{top};1H")  # scroll region below the banner, cursor into it
    out.flush()

    def _restore():
        out.write(f"\x1b[r\x1b[{rows};1H\n")
        out.flush()

    atexit.register(_restore)


def _is_under(child: Path, parent) -> bool:
    """True if `child` resolves to a path under `parent`. Tolerates parent
    being either a Path or a string — _project_states keys are sometimes one,
    sometimes the other depending on how the project was opened."""
    try:
        child.resolve().relative_to(Path(parent).resolve())
        return True
    except (ValueError, TypeError, OSError):
        return False


def setup_app():
    """Configures and returns the FastAPI app."""
    app = FastAPI()

    @app.get("/api/tilt-thumb")
    def serve_tilt_thumb(path: str):
        p = Path(path)
        if p.exists() and p.is_file() and p.suffix == ".png":
            return FileResponse(p, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
        return {"error": "not found"}

    @app.get("/api/vis-asset")
    def serve_vis_asset(path: str):
        # Path-traversal guard: resolve and require the file to live under a
        # currently-loaded project root. The project-state registry is the
        # authoritative list of roots a user has opened in this session.
        # Serves both PNG (panel/atlas images) and JSON (stamps_index, manifests)
        # from the same endpoint, since the candidate-preview UI needs both.
        from services.project_state import _project_states

        try:
            resolved = Path(path).resolve(strict=True)
        except (FileNotFoundError, RuntimeError):
            return {"error": "not found"}
        media_by_suffix = {".png": "image/png", ".json": "application/json"}
        media = media_by_suffix.get(resolved.suffix)
        if media is None:
            return {"error": "unsupported asset type"}
        roots = [pr for pr in _project_states.keys() if pr is not None]
        if not any(_is_under(resolved, root) for root in roots):
            return {"error": "outside project roots"}
        # JSON manifests change on every regen — never cache them. PNGs (atlas,
        # tomo previews) are addressed by mtime-keyed URLs from the UI, so a
        # short TTL is fine and keeps unbusted accesses self-correcting.
        if resolved.suffix == ".json":
            cache_header = "no-cache"
        else:
            cache_header = "public, max-age=300"
        return FileResponse(resolved, media_type=media, headers={"Cache-Control": cache_header})

    app.mount("/static", StaticFiles(directory="static"), name="static")
    # mtime-based cache-buster: the browser refetches main.css whenever we edit it,
    # so dev iteration doesn't require Cmd-Shift-R after every CSS change.
    css_path = Path("static/main.css")
    css_version = int(css_path.stat().st_mtime) if css_path.exists() else 0
    ui.add_head_html(f'''
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap"
              rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css?v={css_version}">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    create_ui_router(backend)

    # Single server-side pipeline observer: runs sync_all_jobs centrally
    # on a 3-s tick for every project with pipeline_active=True, and on
    # startup scans the configured project base for projects that were
    # mid-pipeline when uvicorn last died — re-deploys their remaining
    # jobs from a fresh scheme. See docs/architecture.md.
    async def _start_pipeline_monitor():
        await backend.pipeline_monitor.start()
        await backend.curation_watcher.start()

    async def _stop_pipeline_monitor():
        await backend.pipeline_monitor.stop()
        await backend.curation_watcher.stop()

    # add_event_handler is the non-deprecated spelling of @app.on_event.
    app.add_event_handler("startup", _start_pipeline_monitor)
    app.add_event_handler("shutdown", _stop_pipeline_monitor)

    storage_secret = os.environ.get("CRBOOST_STORAGE_SECRET", "crboost-change-me")

    # Default reconnect_timeout is 3s, which sets ping_interval=4s / ping_timeout=2s
    # (nicegui/nicegui.py:129-130). Over an SSH tunnel any latency blip trips the
    # 2s pong deadline → socket drops → client teardown after 3s → full rebuild.
    # 30s is generous for tunneled sessions and still catches real disconnects.
    reconnect_timeout = float(os.environ.get("CRBOOST_RECONNECT_TIMEOUT", "30"))
    ui.run_with(
        app,
        title="CryoBoost Server",
        storage_secret=storage_secret,
        reconnect_timeout=reconnect_timeout,
    )
    return app


if __name__ in {"__main__", "__mp_main__"}:
    
    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable DEBUG-level logging')

    args     = parser.parse_args()
    setup_logging(debug=args.debug)
    # Expose the bind port to the running app (the status strip on the landing
    # page surfaces it); setup_app() runs before uvicorn so an env var is the
    # simplest single source of truth.
    os.environ["CRBOOST_PORT"] = str(args.port)
    app      = setup_app()
    install_sticky_banner(banner_lines(args.port, socket.gethostname()))

    # log_config=None: uvicorn otherwise reinstalls its own logging config at
    # run() time, undoing setup_logging() and re-enabling the per-request
    # access log ("GET / 200 OK", "connection open", ...).
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)