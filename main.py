#!/usr/bin/env python3
import socket
import argparse
from pathlib import Path

# NEW: Import FastAPI and StaticFiles
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from nicegui import ui
from backend import CryoBoostBackend
from ui import build_ui

# NEW: Create a FastAPI app instance
app = FastAPI()

# NEW: Manually "mount" the static directory.
# This tells FastAPI that any request to "/static/..." should serve a file from the "static" folder.
app.mount("/static", StaticFiles(directory="static"), name="static")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    return parser.parse_args()

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

def main():
    """Main function"""
    args = parse_arguments()
    
    # Link to Google Font and our external stylesheet (this part is unchanged)
    ui.add_head_html('''
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
        <link rel="stylesheet" href="/static/main.css">
    ''')
    
    backend = CryoBoostBackend(Path.cwd())
    build_ui(backend)

    local_ip = get_local_ip()
    hostname = socket.gethostname()
    
    print("CryoBoost Server Starting 🚀")
    print(f"Access URLs:")
    print(f"  Local:   http://localhost:{args.port}")
    print(f"  Network: http://{local_ip}:{args.port}")
    print("\nTo access from another machine, use an SSH tunnel:")
    print(f"  ssh -L 8081:localhost:{args.port} your_user@{hostname}")
    print("-" * 30)

    # UPDATED: Use ui.run_with() to attach NiceGUI to our custom FastAPI app
    ui.run_with(
        app,
        host=args.host, 
        port=args.port, 
        title="CryoBoost Server", 
        reload=False
    )

if __name__ in {"__main__", "__mp_main__"}:
    main()