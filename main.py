#!/usr/bin/env python3
import asyncio
import socket
import argparse
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from nicegui import ui

from ui import create_ui_router
from backend import CryoBoostBackend

async def setup_app():
    """Configures and returns the FastAPI app."""
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="static"), name="static")

    ui.add_head_html('''
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    create_ui_router(backend) 

    # Run debug immediately
    print("=== RUNNING CONTAINER DEBUG ===")
    await backend.debug_container_environment(Path("/users/artem.kushner/cryoboost_projects/nu6"))
    print("=== DEBUG COMPLETE ===")

    ui.run_with(app, title="CryoBoost Server")
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
    
    # Run the async setup
    app = asyncio.run(setup_app())

    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()

    local_ip = get_local_ip()
    hostname = socket.gethostname()
    
    print("CryoBoost Server Starting")
    print(f"Access URLs:")
    print(f"  Local:   http://localhost:{args.port}")
    print(f"  Network: http://{local_ip}:{args.port}")
    print("\nTo access from another machine, use an SSH tunnel:")
    print(f"ssh -L 8081:{hostname}:{args.port} [YOUR_USERNAME]@{hostname}")
    print("-" * 30)

    uvicorn.run(
        app, 
        host=args.host, 
        port=args.port,
    )