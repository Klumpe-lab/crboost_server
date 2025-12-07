#!/usr/bin/env python3
"""
Simple Molstar Viewer with NiceGUI
Backend fetches remote files to avoid CORS issues on firewalled headnodes.
"""

import gzip
import shutil
import urllib.request
from pathlib import Path

from fastapi.responses import FileResponse, HTMLResponse
from nicegui import app, ui

# --- CONFIG ---
PORT = 8085
LOCAL_FILES_DIR = Path("/users/artem.kushner/dev/crboost_server/templates").resolve()
CACHE_DIR = Path("./fetched_files").resolve()
CACHE_DIR.mkdir(exist_ok=True)

import ssl
import urllib.request

SSL_CONTEXT                = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode    = ssl.CERT_NONE


def fetch_pdb_file(pdb_id: str) -> tuple[bool, str]:
    """Fetch PDB/CIF from RCSB, return local path."""
    pdb_id = pdb_id.lower().strip()
    out_path = CACHE_DIR / f"{pdb_id}.cif"
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
    out_path = CACHE_DIR / f"emd_{numeric_id}.map"
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


# --- API: Molstar HTML (served inline to avoid static file issues) ---
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
            if (!viewer) {
                console.error('Viewer not initialized');
                return;
            }
            try {
                await viewer.plugin.clear();
                await viewer.loadStructureFromUrl(url, format, isBinary);
                console.log('Structure loaded:', url);
            } catch (e) {
                console.error('Failed to load structure:', e);
            }
        }
        
        async function loadVolume(url) {
            if (!viewer) {
                console.error('Viewer not initialized');
                return;
            }
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
        
        // Listen for messages from parent
        window.addEventListener('message', async (e) => {
            if (!e.data || !e.data.action) return;
            
            if (e.data.action === 'load_structure') {
                await loadStructure(e.data.url, e.data.format, e.data.isBinary);
            } else if (e.data.action === 'load_volume') {
                await loadVolume(e.data.url);
            }
        });
        
        // Init on load
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
    ui.label("Molstar Viewer").classes("text-2xl font-bold mb-4")
    
    with ui.row().classes("w-full gap-4"):
        # LEFT: Controls
        with ui.column().classes("w-80 gap-4"):
            # -- Remote PDB --
            with ui.card().classes("w-full"):
                ui.label("Load from PDB").classes("font-bold")
                pdb_input = ui.input("PDB ID (e.g. 1abc)").classes("w-full")
                
                async def load_pdb():
                    if not pdb_input.value:
                        ui.notify("Enter a PDB ID", type="warning")
                        return
                    ui.notify(f"Fetching {pdb_input.value}...")
                    ok, result = fetch_pdb_file(pdb_input.value)
                    if ok:
                        load_structure_in_viewer(result)
                        ui.notify("Loaded!", type="positive")
                    else:
                        ui.notify(f"Error: {result}", type="negative")
                
                ui.button("Fetch & Load", on_click=load_pdb).classes("w-full")
            
            # -- Remote EMDB --
            with ui.card().classes("w-full"):
                ui.label("Load from EMDB").classes("font-bold")
                emdb_input = ui.input("EMDB ID (e.g. 1234)").classes("w-full")
                
                async def load_emdb():
                    if not emdb_input.value:
                        ui.notify("Enter an EMDB ID", type="warning")
                        return
                    ui.notify(f"Fetching EMD-{emdb_input.value}...")
                    ok, result = fetch_emdb_file(emdb_input.value)
                    if ok:
                        load_volume_in_viewer(result)
                        ui.notify("Loaded!", type="positive")
                    else:
                        ui.notify(f"Error: {result}", type="negative")
                
                ui.button("Fetch & Load", on_click=load_emdb).classes("w-full")
            
            # -- Local Files Browser --
            with ui.card().classes("w-full"):
                ui.label("Local Files").classes("font-bold")
                ui.label(str(LOCAL_FILES_DIR)).classes("text-xs text-gray-500 break-all")
                
                file_list = ui.column().classes("w-full max-h-48 overflow-auto")
                
                def refresh_local_files():
                    file_list.clear()
                    if not LOCAL_FILES_DIR.exists():
                        with file_list:
                            ui.label("Directory not found").classes("text-red-500")
                        return
                    
                    extensions = {".pdb", ".cif", ".mrc", ".map", ".rec", ".ccp4"}
                    files = sorted([f for f in LOCAL_FILES_DIR.iterdir() if f.suffix.lower() in extensions])
                    
                    with file_list:
                        if not files:
                            ui.label("No files found").classes("text-gray-400 italic")
                        for f in files:
                            ui.button(
                                f.name,
                                on_click=lambda p=f: load_local_file(p)
                            ).props("flat dense align=left").classes("w-full text-left text-sm")
                
                def load_local_file(path: Path):
                    if is_volume_file(path):
                        load_volume_in_viewer(str(path))
                    else:
                        load_structure_in_viewer(str(path))
                    ui.notify(f"Loaded {path.name}", type="positive")
                
                ui.button("Refresh", on_click=refresh_local_files, icon="refresh").props("flat").classes("w-full")
                refresh_local_files()
        
        # RIGHT: Molstar Viewer
        with ui.column().classes("flex-grow"):
            ui.element("iframe").props('src="/molstar" id="molstar-frame"').style(
                "width: 800px; height: 500px; border: 1px solid #333; border-radius: 4px;"
            )
            
            def load_structure_in_viewer(file_path: str):
                """Load a structure (pdb/cif) in the viewer."""
                file_url = f"/api/file?path={file_path}"
                fmt, is_binary = get_structure_format(file_path)
                js = f'''
                    document.getElementById('molstar-frame').contentWindow.postMessage(
                        {{ action: 'load_structure', url: '{file_url}', format: '{fmt}', isBinary: {str(is_binary).lower()} }},
                        '*'
                    );
                '''
                ui.run_javascript(js)
            
            def load_volume_in_viewer(file_path: str):
                """Load a volume (mrc/map) in the viewer."""
                file_url = f"/api/file?path={file_path}"
                js = f'''
                    document.getElementById('molstar-frame').contentWindow.postMessage(
                        {{ action: 'load_volume', url: '{file_url}' }},
                        '*'
                    );
                '''
                ui.run_javascript(js)


def get_structure_format(path: str) -> tuple[str, bool]:
    """Return (format, isBinary) for structure files."""
    ext = Path(path).suffix.lower()
    if ext == ".pdb":
        return "pdb", False
    elif ext == ".cif":
        return "mmcif", False
    elif ext == ".bcif":
        return "mmcif", True
    return "mmcif", False


def is_volume_file(path: Path) -> bool:
    """Check if file is a volume format."""
    return path.suffix.lower() in {".mrc", ".map", ".rec", ".ccp4"}

def get_format(path: Path) -> str:
    """Determine Molstar format from file extension."""
    ext = path.suffix.lower()
    return {
        ".pdb": "pdb",
        ".cif": "mmcif",
        ".mrc": "ccp4",
        ".map": "ccp4",
        ".rec": "ccp4",
        ".ccp4": "ccp4",
    }.get(ext, "pdb")


if __name__ in {"__main__", "__mp_main__"}:
    print(f"Starting on http://localhost:{PORT}")
    print(f"Local files dir: {LOCAL_FILES_DIR}")
    print(f"Cache dir: {CACHE_DIR}")
    ui.run(title="Molstar Viewer", port=PORT)