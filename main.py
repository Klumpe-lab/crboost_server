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
import logging

import sys
sys.dont_write_bytecode = True

class SuppressPruneStorageError(logging.Filter):
    def filter(self, record):
        return "Request is not set" not in record.getMessage()

warnings.filterwarnings("ignore", message="Pydantic serializer warnings")


def setup_logging(debug: bool = False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down noisy third-party loggers
    logging.getLogger("nicegui").setLevel(logging.WARNING)
    logging.getLogger("nicegui").addFilter(SuppressPruneStorageError())
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


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
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css?v={css_version}">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    create_ui_router(backend) 
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

def get_local_ip():
    """Get the local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"

if __name__ in {"__main__", "__mp_main__"}:
    
    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable DEBUG-level logging')

    args     = parser.parse_args()
    setup_logging(debug=args.debug)
    app      = setup_app()
    local_ip = get_local_ip()
    hostname = socket.gethostname()
    
    print("\n" + "="*60)
    print("CryoBoost Server Starting")
    print(f"Access URLs:")
    print(f"  Local:   http://localhost:{args.port}")
    print(f"  Network: http://{local_ip}:{args.port}")
    print("\nTo access in the browser from your local machine, establish port-forwarding from remote to your local terminal.")
    print("\nRun this in a local terminal:")
    print(f"ssh -f -N -L {args.port}:localhost:{args.port} $USER@{hostname}")
    print("="*60 + "\n")

    uvicorn.run(
        app, 
        host=args.host, 
        port=args.port,
    )