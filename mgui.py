#!/usr/bin/env python3
"""
Simple Molstar Viewer with NiceGUI
Backend fetches remote files to avoid CORS issues on firewalled headnodes.
"""

import gzip
import ssl
import urllib.request
from pathlib import Path

from fastapi.responses import FileResponse, HTMLResponse
from nicegui import app, ui

# --- CONFIG ---
PORT = 8085
TEMPLATES_DIR = Path("/users/artem.kushner/dev/crboost_server/templates").resolve()
TEMPLATES_DIR.mkdir(exist_ok=True)

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE


# --- Backend: File Fetching ---
def fetch_pdb_file(pdb_id: str) -> tuple[bool, str]:
    """Fetch PDB/CIF from RCSB, return local path."""
    pdb_id = pdb_id.lower().strip()
    out_path = TEMPLATES_DIR / f"{pdb_id}.cif"
    if out_path.exists():
        return True, str(out_path)
    
    url = f"https://files.rcsb.org/download/{pdb_id}.cif"
    try:
        with urllib.request.urlopen(url, context=SSL_CONTEXT) as response:
            out_path.write_bytes(response.read())
        return True, str(out_path)
    except Exception as e:
        return False, str(e)


def fetch_emdb_file(emdb_id: str) -> tuple[bool, str]:
    """Fetch map from EMDB, decompress, return local path."""
    numeric_id = emdb_id.upper().replace("EMD-", "").replace("EMD", "").strip()
    out_path = TEMPLATES_DIR / f"emd_{numeric_id}.map"
    if out_path.exists():
        return True, str(out_path)
    
    url = f"https://ftp.ebi.ac.uk/pub/databases/emdb/structures/EMD-{numeric_id}/map/emd_{numeric_id}.map.gz"
    
    try:
        with urllib.request.urlopen(url, context=SSL_CONTEXT) as response:
            compressed = response.read()
        decompressed = gzip.decompress(compressed)
        out_path.write_bytes(decompressed)
        return True, str(out_path)
    except Exception as e:
        return False, str(e)


# --- API: Serve files to Molstar ---
@app.get("/api/file")
def serve_file(path: str):
    """Serve a file by absolute path (for Molstar to fetch)."""
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


# --- Molstar HTML ---
MOLSTAR_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body, #app { width: 100%; height: 100%; overflow: hidden; background: #1a1a1a; }
    </style>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/molstar@latest/build/viewer/molstar.css">
    <script src="https://cdn.jsdelivr.net/npm/molstar@latest/build/viewer/molstar.js"></script>
</head>
<body>
    <div id="app"></div>
    <script>
        let viewer = null;
        
        async function initViewer() {
            const viewerElement = document.getElementById('app');
            viewer = await molstar.Viewer.create(viewerElement, {
                layoutIsExpanded: false,
                layoutShowControls: false,
                layoutShowRemoteState: false,
                layoutShowSequence: false,
                layoutShowLog: false,
                viewportShowExpand: false,
                viewportShowSelectionMode: false,
                viewportShowAnimation: false,
            });
            console.log('Molstar viewer initialized');
        }
        
        async function loadStructure(url, format, isBinary) {
            if (!viewer) return;
            try {
                await viewer.plugin.clear();
                await viewer.loadStructureFromUrl(url, format, isBinary);
                console.log('Structure loaded:', url);
            } catch (e) {
                console.error('Failed to load structure:', e);
            }
        }
        
        async function loadVolume(url) {
            if (!viewer) return;
            try {
                await viewer.plugin.clear();
                await viewer.loadVolumeFromUrl(
                    { url: url, format: 'ccp4', isBinary: true },
                    [{ type: 'relative', value: 2.0, color: 0x3388ff, alpha: 0.8 }]
                );
                console.log('Volume loaded:', url);
            } catch (e) {
                console.error('Failed to load volume:', e);
            }
        }
        
        window.addEventListener('message', async (e) => {
            if (!e.data || !e.data.action) return;
            if (e.data.action === 'load_structure') {
                await loadStructure(e.data.url, e.data.format, e.data.isBinary);
            } else if (e.data.action === 'load_volume') {
                await loadVolume(e.data.url);
            }
        });
        
        window.onload = initViewer;
    </script>
</body>
</html>
"""

@app.get("/molstar")
def molstar_viewer():
    return HTMLResponse(MOLSTAR_HTML)


# --- UI ---
@ui.page("/")
def main_page():
    # Header
    with ui.header().classes("bg-white border-b border-gray-200 px-6 py-3"):
        with ui.row().classes("w-full items-center justify-between"):
            with ui.row().classes("items-center gap-3"):
                ui.icon("science", size="sm").classes("text-blue-600")
                ui.label("Template Viewer").classes("text-lg font-semibold text-gray-800")
                ui.label(f"{TEMPLATES_DIR}").classes("text-xs text-gray-400 font-mono")
    
    # Main content
    with ui.row().classes("w-full h-[calc(100vh-60px)] p-4 gap-4 bg-gray-50"):
        
        # LEFT PANEL: Controls
        with ui.column().classes("w-72 gap-3 shrink-0"):
            
            # Remote Sources Card
            with ui.card().classes("w-full"):
                with ui.row().classes("items-center gap-2 mb-3"):
                    ui.icon("cloud_download", size="xs").classes("text-blue-500")
                    ui.label("Remote Sources").classes("font-semibold text-sm text-gray-700")
                
                # PDB
                with ui.column().classes("gap-2 mb-4"):
                    ui.label("PDB").classes("text-xs font-medium text-gray-500 uppercase")
                    with ui.row().classes("w-full gap-2"):
                        pdb_input = ui.input(placeholder="e.g. 1abc").props("dense").classes("flex-grow")
                        
                        async def load_pdb():
                            if not pdb_input.value:
                                return
                            ui.notify(f"Fetching {pdb_input.value}...", type="info")
                            ok, result = fetch_pdb_file(pdb_input.value)
                            if ok:
                                load_structure_in_viewer(result)
                                refresh_files()
                                ui.notify("Loaded!", type="positive")
                            else:
                                ui.notify(f"Error: {result}", type="negative")
                        
                        ui.button(icon="download", on_click=load_pdb).props("flat dense")
                
                ui.separator()
                
                # EMDB
                with ui.column().classes("gap-2 mt-3"):
                    ui.label("EMDB").classes("text-xs font-medium text-gray-500 uppercase")
                    with ui.row().classes("w-full gap-2"):
                        emdb_input = ui.input(placeholder="e.g. 1234").props("dense").classes("flex-grow")
                        
                        async def load_emdb():
                            if not emdb_input.value:
                                return
                            ui.notify(f"Fetching EMD-{emdb_input.value}...", type="info")
                            ok, result = fetch_emdb_file(emdb_input.value)
                            if ok:
                                load_volume_in_viewer(result)
                                refresh_files()
                                ui.notify("Loaded!", type="positive")
                            else:
                                ui.notify(f"Error: {result}", type="negative")
                        
                        ui.button(icon="download", on_click=load_emdb).props("flat dense")
            
            # Local Files Card
            with ui.card().classes("w-full flex-grow"):
                with ui.row().classes("items-center justify-between mb-2"):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("folder", size="xs").classes("text-amber-500")
                        ui.label("Local Files").classes("font-semibold text-sm text-gray-700")
                    ui.button(icon="refresh", on_click=lambda: refresh_files()).props("flat dense round size=sm")
                
                file_list = ui.scroll_area().classes("w-full flex-grow")
                
                def refresh_files():
                    file_list.clear()
                    if not TEMPLATES_DIR.exists():
                        with file_list:
                            ui.label("Directory not found").classes("text-red-500 text-xs p-2")
                        return
                    
                    extensions = {".pdb", ".cif", ".mrc", ".map", ".rec", ".ccp4"}
                    files = sorted([f for f in TEMPLATES_DIR.iterdir() if f.suffix.lower() in extensions])
                    
                    with file_list:
                        if not files:
                            ui.label("No files").classes("text-gray-400 italic text-xs p-2")
                            return
                        
                        for f in files:
                            is_vol = is_volume_file(f)
                            icon = "view_in_ar" if is_vol else "account_tree"
                            color = "text-blue-400" if is_vol else "text-green-400"
                            
                            with ui.row().classes("w-full items-center hover:bg-gray-100 rounded px-2 py-1 cursor-pointer gap-2").on("click", lambda p=f: load_local_file(p)):
                                ui.icon(icon, size="xs").classes(color)
                                ui.label(f.name).classes("text-xs text-gray-700 truncate")
                
                def load_local_file(path: Path):
                    if is_volume_file(path):
                        load_volume_in_viewer(str(path))
                    else:
                        load_structure_in_viewer(str(path))
                    ui.notify(f"Loaded {path.name}", type="positive")
                
                refresh_files()
        
        # RIGHT PANEL: Viewer
        with ui.card().classes("flex-grow h-full p-0 overflow-hidden"):
            ui.element("iframe").props('src="/molstar" id="molstar-frame"').classes("w-full h-full").style("border: none;")
            
            def load_structure_in_viewer(file_path: str):
                file_url = f"/api/file?path={file_path}"
                fmt, is_binary = get_structure_format(file_path)
                ui.run_javascript(f'''
                    document.getElementById('molstar-frame').contentWindow.postMessage(
                        {{ action: 'load_structure', url: '{file_url}', format: '{fmt}', isBinary: {str(is_binary).lower()} }},
                        '*'
                    );
                ''')
            
            def load_volume_in_viewer(file_path: str):
                file_url = f"/api/file?path={file_path}"
                ui.run_javascript(f'''
                    document.getElementById('molstar-frame').contentWindow.postMessage(
                        {{ action: 'load_volume', url: '{file_url}' }},
                        '*'
                    );
                ''')


def get_structure_format(path: str) -> tuple[str, bool]:
    ext = Path(path).suffix.lower()
    if ext == ".pdb":
        return "pdb", False
    elif ext == ".cif":
        return "mmcif", False
    elif ext == ".bcif":
        return "mmcif", True
    return "mmcif", False


def is_volume_file(path: Path) -> bool:
    return path.suffix.lower() in {".mrc", ".map", ".rec", ".ccp4"}


if __name__ in {"__main__", "__mp_main__"}:
    print(f"Starting on http://localhost:{PORT}")
    print(f"Templates dir: {TEMPLATES_DIR}")
    ui.run(title="Template Viewer", port=PORT, favicon="🔬")