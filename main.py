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


def setup_app():
    """Configures and returns the FastAPI app."""
    app = FastAPI()

    @app.get("/api/tilt-thumb")
    def serve_tilt_thumb(path: str):
        p = Path(path)
        if p.exists() and p.is_file() and p.suffix == ".png":
            return FileResponse(p, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
        return {"error": "not found"}

    app.mount("/static", StaticFiles(directory="static"), name="static")
    ui.add_head_html('''
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    create_ui_router(backend) 
    storage_secret = os.environ.get("CRBOOST_STORAGE_SECRET", "crboost-change-me")
    ui.run_with(app, title="CryoBoost Server", storage_secret=storage_secret)
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