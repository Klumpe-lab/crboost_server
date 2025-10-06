#!/usr/bin/env python3
"""
CryoBoost Server - Main Application
Multi-tab interface with Setup and Schema Editor
"""

import os
import sys
import socket
import argparse
import subprocess
import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
from nicegui import ui

class CryoBoostBackend:
    def __init__(self, server_dir):
        self.server_dir = Path(server_dir)
        self.job_definitions = [
            {"job_type": "importmovies", "input_job_type": None},
            {"job_type": "fsMotionAndCtf", "input_job_type": "importmovies"},
            {"job_type": "filtertilts", "input_job_type": "fsMotionAndCtf"},
            {"job_type": "filtertiltsinter", "input_job_type": "filtertilts"},
            {"job_type": "aligntiltsWarp", "input_job_type": "fsMotionAndCtf"},
            {"job_type": "tsCtf", "input_job_type": "aligntiltsWarp"},
            {"job_type": "tsReconstruct", "input_job_type": "tsCtf"},
            {"job_type": "denoisetrain", "input_job_type": "tsReconstruct"},
            {"job_type": "denoisepredict", "input_job_type": "tsReconstruct"},
            {"job_type": "templatematching", "input_job_type": "tsReconstruct"},
            {"job_type": "subtomogExtr", "input_job_type": "tsReconstruct"}
        ]
        self.selected_jobs = []
    
    async def run_slurm_command(self, command):
        """Execute a SLURM command and return output"""
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                return {
                    "success": True,
                    "output": stdout.decode(),
                    "error": None
                }
            else:
                return {
                    "success": False,
                    "output": stdout.decode() if stdout else "",
                    "error": stderr.decode()
                }
        except Exception as e:
            return {
                "success": False,
                "output": "",
                "error": str(e)
            }

backend = None

app = FastAPI(title="CryoBoost Server")

@app.get("/api/slurm-info")
async def get_slurm_info():
    """API endpoint to get SLURM info"""
    result = await backend.run_slurm_command("sinfo")
    return result

@app.get("/api/job-definitions")
async def get_job_definitions():
    """Get available job definitions"""
    return {"jobs": backend.job_definitions}

@app.get("/api/selected-jobs")
async def get_selected_jobs():
    """Get currently selected jobs"""
    return {"selected": backend.selected_jobs}

@app.post("/api/add-job/{job_index}")
async def add_job(job_index: int):
    """Add a job to the selected list"""
    if 0 <= job_index < len(backend.job_definitions):
        job = backend.job_definitions[job_index]
        if job not in backend.selected_jobs:
            backend.selected_jobs.append(job)
    return {"selected": backend.selected_jobs}

@app.post("/api/remove-job/{job_index}")
async def remove_job(job_index: int):
    """Remove a job from the selected list"""
    if 0 <= job_index < len(backend.selected_jobs):
        backend.selected_jobs.pop(job_index)
    return {"selected": backend.selected_jobs}

def create_setup_page():
    """Create the Setup page"""
    with ui.column().classes('w-full p-4'):
        ui.label('Workflow Setup').classes('text-2xl font-bold mb-4')
        
        with ui.card().classes('w-full max-w-2xl'):
            ui.label('SLURM Cluster Information').classes('text-lg font-semibold mb-2')
            
            output_area = ui.textarea(
                label='Cluster Status',
                value='Click "Get SLURM Info" to view cluster status...'
            ).classes('w-full h-64').props('readonly')
            
            async def get_slurm_info():
                output_area.value = "Loading..."
                result = await backend.run_slurm_command("sinfo")
                
                if result["success"]:
                    output_area.value = result["output"]
                else:
                    error_msg = result["error"] if result["error"] else "Unknown error"
                    output_area.value = f"Error executing sinfo:\n{error_msg}"
            
            ui.button(
                'Get SLURM Info',
                on_click=get_slurm_info
            ).classes('mt-2')

def create_schema_editor():
    """Create the Schema Editor page"""
    with ui.column().classes('w-full p-4'):
        ui.label('Schema Editor').classes('text-2xl font-bold mb-4')
        
        with ui.row().classes('w-full gap-4'):
            with ui.card().classes('flex-1'):
                ui.label('Available Jobs').classes('text-lg font-semibold mb-2')
                
                available_container = ui.column().classes('w-full')
                
                def refresh_available_jobs():
                    available_container.clear()
                    for i, job in enumerate(backend.job_definitions):
                        with available_container:
                            with ui.row().classes('w-full items-center justify-between p-2 border rounded'):
                                with ui.column().classes('flex-1'):
                                    ui.label(f"Job Type: {job['job_type']}").classes('font-medium')
                                    input_type = job['input_job_type'] if job['input_job_type'] else 'None'
                                    ui.label(f"Input: {input_type}").classes('text-sm text-gray-600')
                                
                                ui.button(
                                    '+',
                                    on_click=lambda j=i: add_job_to_selected(j)
                                ).classes('w-8 h-8')
                
                def add_job_to_selected(job_index):
                    job = backend.job_definitions[job_index]
                    if job not in backend.selected_jobs:
                        backend.selected_jobs.append(job)
                        refresh_selected_jobs()
                
                refresh_available_jobs()
            
            with ui.card().classes('flex-1'):
                ui.label('Selected Jobs').classes('text-lg font-semibold mb-2')
                
                selected_container = ui.column().classes('w-full')
                
                def refresh_selected_jobs():
                    selected_container.clear()
                    for i, job in enumerate(backend.selected_jobs):
                        with selected_container:
                            with ui.row().classes('w-full items-center justify-between p-2 border rounded bg-blue-50'):
                                with ui.column().classes('flex-1'):
                                    ui.label(f"Job Type: {job['job_type']}").classes('font-medium')
                                    input_type = job['input_job_type'] if job['input_job_type'] else 'None'
                                    ui.label(f"Input: {input_type}").classes('text-sm text-gray-600')
                                
                                ui.button(
                                    '-',
                                    on_click=lambda j=i: remove_job_from_selected(j)
                                ).classes('w-8 h-8')
                
                def remove_job_from_selected(job_index):
                    if 0 <= job_index < len(backend.selected_jobs):
                        backend.selected_jobs.pop(job_index)
                        refresh_selected_jobs()
                
                refresh_selected_jobs()

# Main UI
@ui.page('/')
def main_page():
    """Main page with tabs"""
    ui.colors(primary='#1976d2')
    
    with ui.column().classes('w-full'):
        ui.label('CryoBoost Server').classes('text-3xl font-bold text-center text-primary mb-4')
        
        with ui.tabs().classes('w-full') as tabs:
            setup_tab = ui.tab('Setup')
            schema_tab = ui.tab('Schema Editor')
        
        with ui.tab_panels(tabs, value=setup_tab).classes('w-full'):
            with ui.tab_panel(setup_tab):
                create_setup_page()
            
            with ui.tab_panel(schema_tab):
                create_schema_editor()

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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='CryoBoost Server')
    parser.add_argument('--port', type=int, default=8081, 
                        help='Port to run the server on (default: 8081)')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--find-port', action='store_true',
                        help='Automatically find a free port if specified port is in use')
    
    return parser.parse_args()

def main():
    """Main function"""
    global backend
    
    args = parse_arguments()
    backend = CryoBoostBackend(Path.cwd())
    
    local_ip = get_local_ip()
    
    # Port selection logic
    if is_port_in_use(args.port):
        if args.find_port:
            port = find_free_port(args.port)
            print(f"Port {args.port} in use, using {port} instead")
        else:
            print(f"Error: Port {args.port} is already in use")
            print("Use --find-port to automatically find a free port")
            return
    else:
        port = args.port
    
    print(f"CryoBoost Server Starting")
    print(f"Server directory: {backend.server_dir}")
    print(f"Python version: {sys.version.split()[0]}")
    print(f"")
    print(f"Access URLs:")
    print(f"  Local:   http://localhost:{port}")
    print(f"  Network: http://{local_ip}:{port}")
    print(f"  API:     http://localhost:{port}/docs")
    print(f"")
    print(f"SSH Tunnel command:")
    print(f"  ssh -L 8080:localhost:{port} username@{socket.gethostname()}")
    print(f"")
    
    try:
        ui.run_with(app)
        import uvicorn
        uvicorn.run(app, host=args.host, port=port, log_level="warning")
    except Exception as e:
        print(f"Error starting server: {e}")

if __name__ == "__main__":
    main()
