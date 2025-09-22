#!/usr/bin/env python3
"""
CryoBoost Test Server - Smart Port Handling
Automatically finds available ports and handles command line arguments
"""

import os
import sys
import socket
import argparse
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
from nicegui import ui

click_count = 0
server_dir = Path.cwd()

def find_free_port(start_port=8080, max_attempts=100):
    """Find a free port starting from start_port"""
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"Could not find a free port in range {start_port}-{start_port + max_attempts}")

def is_port_in_use(port):
    """Check if a port is already in use"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', port))
            return False
    except OSError:
        return True

def get_local_ip():
    """Get the local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except:
        return "localhost"

def get_cluster_safe_ports():
    """Get a list of ports that are typically safe to use on clusters"""
    # Common safe port ranges for user applications on clusters
    safe_ranges = [
        range(8000, 8100),   # Common web app range
        range(9000, 9100),   # Alternative web range
        range(3000, 3100),   # Node.js style
        range(5000, 5100),   # Flask style
        range(7000, 7100),   # Custom apps
    ]
    
    safe_ports = []
    for port_range in safe_ranges:
        safe_ports.extend(list(port_range))
    
    return safe_ports

# Create FastAPI app
app = FastAPI(title="CryoBoost Test Server")

# FastAPI routes
@app.get("/api/status")
async def get_status():
    return {
        "status": "running",
        "server_directory": str(server_dir),
        "click_count": click_count
    }

def write_hello_world():
    """Write hello world file with timestamp"""
    global click_count
    click_count += 1
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    filepath = server_dir / "hello_world.txt"
    content = f"Hello World! (Click #{click_count})\nWritten at: {timestamp}\nServer directory: {server_dir}\n"
    
    try:
        with open(filepath, "w") as f:
            f.write(content)
        
        ui.notify(f"✅ File written successfully! Click #{click_count}", type="positive")
        print(f"✅ File written: {filepath}")
        return True
        
    except Exception as e:
        ui.notify(f"❌ Error writing file: {str(e)}", type="negative")
        print(f"❌ Error writing file: {e}")
        return False

def check_file():
    filepath = server_dir / "hello_world.txt"
    if filepath.exists():
        with open(filepath, 'r') as f:
            content = f.read()
        ui.notify(f"📄 File exists! Size: {len(content)} chars", type="info")
        print(f"📄 File exists: {filepath}")
    else:
        ui.notify("❌ File does not exist yet", type="warning")
        print("❌ hello_world.txt not found")

def show_running_services():
    """Show what's currently running on common ports"""
    common_ports = [8080, 8000, 8888, 3000, 5000, 9000]
    print("\n🔍 Port usage check:")
    for port in common_ports:
        status = "IN USE" if is_port_in_use(port) else "FREE"
        print(f"   Port {port}: {status}")

# NiceGUI Interface
@ui.page('/ui')
def ui_page():
    """Main UI page"""
    ui.colors(primary='#1976d2')
    
    with ui.column().classes('w-full max-w-md mx-auto mt-10 gap-4'):
        ui.label('🧪 CryoBoost Test Server').classes('text-3xl font-bold text-center text-primary')
        ui.separator()
        
        with ui.card().classes('w-full p-4'):
            ui.label('Server Information').classes('text-lg font-semibold')
            ui.label(f'📁 Directory: {server_dir}').classes('text-sm text-gray-600')
            ui.label(f'🔢 Click count: {click_count}').classes('text-sm text-gray-600')
        
        ui.button(
            '✍️ Write Hello World File', 
            on_click=write_hello_world,
            color='primary'
        ).classes('w-full h-12 text-lg')
        
        ui.button(
            '🔍 Check If File Exists', 
            on_click=check_file,
            color='secondary'
        ).classes('w-full')
        
        with ui.expansion('📋 Instructions', icon='help').classes('w-full'):
            ui.markdown("""
            **How to test:**
            1. Click "Write Hello World File" 
            2. Check your server directory for `hello_world.txt`
            3. Use "Check If File Exists" to verify
            
            **Available endpoints:**
            - This UI: `/ui`
            - API Status: `/api/status` 
            - API Docs: `/docs`
            """)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='CryoBoost Test Server')
    parser.add_argument('--port', type=int, default=None, 
                        help='Port to run the server on (default: auto-detect)')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--find-port', action='store_true',
                        help='Automatically find a free port')
    parser.add_argument('--show-ports', action='store_true',
                        help='Show current port usage and exit')
    
    return parser.parse_args()

def main():
    """Main function with smart port handling"""
    args = parse_arguments()
    
    if args.show_ports:
        show_running_services()
        return
    
    local_ip = get_local_ip()
    
    # Smart port selection
    if args.port:
        # User specified a port
        if is_port_in_use(args.port):
            print(f"❌ Port {args.port} is already in use!")
            print("🔍 Checking alternative ports...")
            show_running_services()
            
            if args.find_port:
                port = find_free_port(args.port)
                print(f"✅ Found free port: {port}")
            else:
                print("💡 Use --find-port to automatically find a free port, or choose a different port")
                return
        else:
            port = args.port
    else:
        # Auto-select a safe port
        print("🔍 Auto-selecting a free port...")
        safe_ports = get_cluster_safe_ports()
        
        port = None
        for candidate_port in safe_ports:
            if not is_port_in_use(candidate_port):
                port = candidate_port
                break
        
        if port is None:
            print("❌ Could not find any free ports in safe ranges")
            port = find_free_port(8080)  # Last resort
        
        print(f"✅ Selected port: {port}")
    
    print(f"\n🚀 Starting CryoBoost Test Server...")
    print(f"📁 Server directory: {server_dir}")
    print(f"🐍 Python version: {sys.version}")
    
    print(f"\n🌐 Server URLs:")
    print(f"   📱 Main UI:      http://localhost:{port}/ui")
    print(f"   📱 Network UI:   http://{local_ip}:{port}/ui")
    print(f"   🔌 API Status:   http://localhost:{port}/api/status")
    print(f"   📚 API Docs:     http://localhost:{port}/docs")
    print(f"\n⏳ Starting server on {args.host}:{port}...")
    
    try:
        ui.run_with(app)
        
        # Start with uvicorn for explicit control
        import uvicorn
        uvicorn.run(app, host=args.host, port=port, log_level="info")
        
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("   - Try a different port with --port <number>")
        print("   - Check what's using ports with --show-ports")
        print("   - Use --find-port to auto-find a free port")

if __name__ == "__main__":
    main()
