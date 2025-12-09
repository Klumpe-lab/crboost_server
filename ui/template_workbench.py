from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import asyncio
from pathlib import Path
from typing import Optional

# --- MOLSTAR HTML & ROUTING ---
MOLSTAR_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body, #app { width: 100%; height: 100%; overflow: hidden; background: #000; }
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
                viewer.plugin.behaviors.layout.leftPanelState.next('collapsed');
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

# Register routes if not already registered by main.py
@app.get("/molstar")
def molstar_viewer():
    return HTMLResponse(MOLSTAR_HTML)

@app.get("/api/file")
def serve_file(path: str):
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


class TemplateWorkbench:
    def __init__(self, backend, project_path: str, container: ui.element):
        """
        Inline Template Workbench Component.
        Integrates file management, remote fetching, generation, and visualization.
        """
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        
        # Ensure output folder exists
        os.makedirs(self.output_folder, exist_ok=True)

        # --- State ---
        self.is_processing = False
        self.container = container
        
        # Generator Defaults
        self.pixel_size = 1.5
        self.box_size = 128
        self.basic_shape_def = "100:100:100"
        
        # Inputs
        self.pdb_input_val = ""
        self.emdb_input_val = ""
        
        # Mask Defaults
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20
        
        # Selection State
        self.selected_file_path = None
        self.last_generated_result = None

        # UI References
        self.file_list_container = None
        
        # Render
        self.render()
        
        # Initial file load
        asyncio.create_task(self.refresh_files())

    def safe_notify(self, message: str, type: str = "info"):
        ui.notify(message, type=type, close_button=True)

    def render(self):
        with self.container:
            # Main container with a fixed height and border
            # We use h-[750px] to give the viewer enough vertical space
            with ui.row().classes("w-full h-[750px] border border-gray-200 rounded-lg overflow-hidden gap-0 bg-white shadow-sm flex-nowrap"):
                
                # =========================================================
                # LEFT PANEL: Vertical Stack
                # =========================================================
                # flex-col, h-full, shrink-0 ensures this column stays fixed width and organizes children vertically
                with ui.column().classes("w-[450px] h-full bg-white border-r border-gray-200 flex flex-col shrink-0 relative p-0 gap-0"):
                    
                    # Loading Overlay
                    with ui.column().bind_visibility_from(self, 'is_processing').classes("absolute inset-0 z-50 bg-white/60 backdrop-blur-sm items-center justify-center"):
                        ui.spinner(size="lg")
                    
                    # --- 1. Global Settings (Fixed Height, Shrink-0) ---
                    with ui.column().classes("p-3 w-full gap-3 border-b border-gray-100 bg-gray-50 shrink-0"):
                        ui.label("Global Settings").classes("text-[10px] font-bold text-gray-400 uppercase tracking-wider")
                        
                        # Row 1: Global Settings
                        with ui.row().classes("w-full gap-2 items-center"):
                            ui.number("Apix (Å)", value=self.pixel_size, step=0.1, format="%.2f").bind_value(self, "pixel_size").props("dense outlined").classes("flex-1 bg-white")
                            ui.number("Box (px)", value=self.box_size, step=2).bind_value(self, "box_size").props("dense outlined").classes("flex-1 bg-white")
                            
                        # Row 2: Downloaders
                        ui.label("Fetch Sources").classes("text-[10px] font-bold text-gray-400 uppercase tracking-wider mt-1")
                        with ui.row().classes("w-full gap-1 items-center"):
                            ui.input(placeholder="PDB ID (e.g. 3j7z)").bind_value(self, "pdb_input_val").props("dense outlined").classes("flex-grow bg-white text-sm")
                            ui.button(icon="download", on_click=self.handle_fetch_pdb).props("flat dense color=primary").tooltip("Download PDB Structure")
                        
                        with ui.row().classes("w-full gap-1 items-center"):
                            ui.input(placeholder="EMDB ID (e.g. 30210)").bind_value(self, "emdb_input_val").props("dense outlined").classes("flex-grow bg-white text-sm")
                            ui.button(icon="download", on_click=self.handle_fetch_emdb).props("flat dense color=primary").tooltip("Download EMDB Map")

                    # --- 2. Action Tabs (Fixed Height, Shrink-0) ---
                    with ui.column().classes("w-full border-b border-gray-200 bg-white shrink-0"):
                        
                        # Feedback Area
                        self.feedback_container = ui.column().classes("w-full px-3 pt-2")

                        with ui.tabs().classes("w-full text-left bg-white h-8 min-h-0 border-b border-gray-100") as tabs:
                            self.t_shape = ui.tab("Shape").classes("h-8 text-xs min-h-0 px-4")
                            self.t_pdb = ui.tab("PDB").classes("h-8 text-xs min-h-0 px-4")
                            self.t_mask = ui.tab("Mask").classes("h-8 text-xs min-h-0 px-4")

                        # Tab Panels
                        with ui.tab_panels(tabs, value=self.t_shape).classes("w-full p-3 bg-gray-50 h-auto") as self.tab_panels:
                            
                            # Tab: Synthetic Shape
                            with ui.tab_panel(self.t_shape).classes("p-0 flex flex-col gap-2"):
                                ui.label("Generate synthetic ellipsoid").classes("text-[10px] text-gray-500")
                                ui.input("Dims (x:y:z Å)", placeholder="100:100:100").bind_value(self, "basic_shape_def").props("dense outlined").classes("w-full bg-white")
                                ui.button("Generate Shape", icon="category", on_click=self.handle_basic_shape).props("color=primary unelevated dense w-full")

                            # Tab: PDB Simulation
                            with ui.tab_panel(self.t_pdb).classes("p-0 flex flex-col gap-2"):
                                ui.label("Simulate density from structure").classes("text-[10px] text-gray-500")
                                ui.input("PDB File/ID").bind_value(self, "pdb_input_val").props("dense outlined").classes("w-full bg-white").tooltip("Select a .cif/.pdb from the list")
                                ui.button("Simulate Map", icon="biotech", on_click=self.handle_simulate_pdb).props("color=primary unelevated dense w-full")

                            # Tab: Mask Creation
                            with ui.tab_panel(self.t_mask).classes("p-0 flex flex-col gap-2"):
                                # Context Label
                                with ui.row().classes("w-full items-center gap-1 mb-1"):
                                    if self.selected_file_path and "mrc" in self.selected_file_path:
                                         ui.label(f"Ref: {os.path.basename(self.selected_file_path)[:20]}...").classes("text-[10px] font-mono text-blue-600 bg-blue-50 px-1 rounded")
                                    else:
                                         ui.label("Select a volume first").classes("text-[10px] text-orange-500 italic")
                                
                                with ui.row().classes("w-full gap-2"):
                                    ui.number("Thr", value=0.001, format="%.4f").bind_value(self, "mask_threshold").props("dense outlined").classes("flex-1 bg-white").tooltip("Threshold")
                                    ui.number("Soft", value=6).bind_value(self, "mask_soft_edge").props("dense outlined").classes("flex-1 bg-white").tooltip("Soft Edge (px)")
                                
                                with ui.row().classes("w-full gap-2"):
                                    ui.number("Ext", value=3).bind_value(self, "mask_extend").props("dense outlined").classes("flex-1 bg-white").tooltip("Extend (px)")
                                    ui.number("Low", value=20).bind_value(self, "mask_lowpass").props("dense outlined").classes("flex-1 bg-white").tooltip("Lowpass (Å)")

                                ui.button("Create Mask", icon="architecture", on_click=self.handle_mask).props("color=secondary unelevated dense w-full")

                    # --- 3. File List Header (Shrink-0) ---
                    # Just a simple header block
                    with ui.row().classes("w-full items-center justify-between px-3 py-1 bg-white border-b border-gray-100 z-10 shrink-0 border-t border-gray-200"):
                        ui.label("Templates & Maps").classes("text-[10px] font-bold text-gray-400 uppercase tracking-wider")
                        ui.button(icon="refresh", on_click=self.refresh_files).props("flat round dense size=xs color=grey")

                    # --- 4. File List Container (Flex Grow) ---
                    # This captures all remaining vertical space in the 450px column
                    self.file_list_container = ui.scroll_area().classes("w-full flex-grow p-0 bg-white")

                # =========================================================
                # RIGHT PANEL: Viewer 
                # =========================================================
                with ui.column().classes("flex-grow h-full bg-black relative p-0 overflow-hidden"):
                    ui.element("iframe").props('src="/molstar" id="molstar-frame"').classes("w-full h-full border-none")
                    
                    # Legend Overlay
                    with ui.row().classes("absolute bottom-3 right-3 bg-black/70 px-3 py-2 rounded text-[10px] text-gray-200 backdrop-blur-md pointer-events-none select-none"):
                        ui.label("L-Click: Rotate • R-Click: Pan • Scroll: Zoom")

    # --- LOGIC ---

    async def refresh_files(self):
        self.file_list_container.clear()
        
        # Add a tiny loading skeleton
        with self.file_list_container:
            ui.skeleton().classes("w-full h-6 mb-1 opacity-50")

        # Fetch files
        files = await self.backend.template_service.list_template_files_async(self.output_folder)
        
        self.file_list_container.clear()
        with self.file_list_container:
            if not files:
                with ui.column().classes("w-full items-center justify-center py-8 text-gray-300 gap-2"):
                    ui.icon("folder_off", size="sm")
                    ui.label("No templates yet").classes("text-xs")
                return
            
            # Render File List
            for f_path in files:
                fname = os.path.basename(f_path)
                is_vol = any(x in fname.lower() for x in [".mrc", ".map", ".rec"])
                
                # Visuals
                icon = "view_in_ar" if is_vol else "account_tree"
                text_color = "text-blue-600" if is_vol else "text-green-600"
                bg_hover = "hover:bg-blue-50" if is_vol else "hover:bg-green-50"
                
                # Selection Highlight
                is_selected = self.selected_file_path == f_path
                bg_selected = "bg-blue-100" if is_selected and is_vol else ("bg-green-100" if is_selected else "")
                
                # Compact Row
                row = ui.row().classes(f"w-full items-center gap-2 px-3 py-1.5 border-b border-gray-50 group transition-colors cursor-pointer {bg_hover} {bg_selected}")
                with row:
                    # Icon
                    ui.icon(icon, size="xs").classes(f"{text_color} opacity-70")
                    
                    # Name
                    lbl = ui.label(fname).classes("text-xs text-gray-700 font-mono truncate flex-grow leading-tight")
                    lbl.style("max-width: 200px") 
                    
                    # Hover Actions
                    with ui.row().classes("gap-1 items-center opacity-0 group-hover:opacity-100 transition-opacity"):
                        # Delete
                        btn_del = ui.button(icon="delete", on_click=lambda _, p=f_path: self.handle_delete(p))
                        btn_del.props("flat dense round size=xs color=red").classes("ml-1")

                # Row Click Handler
                row.on("click", lambda _, p=f_path: self.select_file(p))

    def update_feedback(self, message: str, type: str="success"):
        """Show a small dismissible alert above the tabs"""
        self.feedback_container.clear()
        if not message: return
        
        color = "bg-green-50 text-green-800 border-green-200" if type == "success" else "bg-red-50 text-red-800 border-red-200"
        icon = "check_circle" if type == "success" else "error"
        
        with self.feedback_container:
            with ui.row().classes(f"w-full items-center gap-2 px-2 py-1 rounded border {color} mb-2"):
                ui.icon(icon, size="xs")
                ui.label(message).classes("text-[10px] font-bold flex-grow truncate")
                ui.button(icon="close", on_click=self.feedback_container.clear).props("flat round dense size=xs")

    async def handle_delete(self, path: str):
        await self.backend.template_service.delete_file_async(path)
        ui.notify(f"Deleted {os.path.basename(path)}", type="info", position="bottom-right")
        if self.selected_file_path == path:
            self.selected_file_path = None
        await self.refresh_files()

    def select_file(self, path: str):
        self.selected_file_path = path
        
        # 1. Load into Viewer
        if any(x in path.lower() for x in [".mrc", ".map", ".rec", ".ccp4"]):
            # Check for preview logic (Optional optimization)
            preview = path.replace(".mrc", "_preview.mrc")
            to_load = preview if os.path.exists(preview) else path
            
            self._viewer_load_volume(to_load)
            
            # Switch to Mask Tab & Calc Threshold
            self.tab_panels.set_value(self.t_mask)
            
            # Auto-calc threshold if it's a map
            asyncio.create_task(self._auto_threshold(path))
            
        else:
            # Structure
            self._viewer_load_structure(path)
            
            # Set as input for PDB Simulation & Switch Tab
            self.pdb_input_val = path
            self.tab_panels.set_value(self.t_pdb)

        # Refresh list to show highlight
        self.refresh_files()

    async def _auto_threshold(self, path):
        thr = await self.backend.template_service.calculate_auto_threshold_async(path)
        self.mask_threshold = round(thr, 4)

    # --- Javascript Bridges ---

    def _viewer_load_volume(self, path):
        url = f"/api/file?path={path}"
        ui.run_javascript(f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_volume', url: '{url}' }}, '*');")

    def _viewer_load_structure(self, path):
        url = f"/api/file?path={path}"
        fmt = "pdb" if path.lower().endswith(".pdb") else "mmcif"
        ui.run_javascript(f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_structure', url: '{url}', format: '{fmt}' }}, '*');")

    # --- Handlers ---

    async def handle_fetch_pdb(self):
        if not self.pdb_input_val: return
        self.is_processing = True
        ui.notify(f"Fetching {self.pdb_input_val}...", type="ongoing", timeout=2)
        res = await self.backend.template_service.fetch_pdb_async(self.pdb_input_val, self.output_folder)
        self.is_processing = False
        if res["success"]: 
            await self.refresh_files()
            self.select_file(res["path"])
            self.update_feedback(f"Fetched {os.path.basename(res['path'])}")
        else:
            ui.notify(f"Error: {res['error']}", type="negative")

    async def handle_fetch_emdb(self):
        if not self.emdb_input_val: return
        self.is_processing = True
        ui.notify(f"Fetching EMD-{self.emdb_input_val}...", type="ongoing", timeout=2)
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val, self.output_folder)
        self.is_processing = False
        if res["success"]: 
            await self.refresh_files()
            self.select_file(res["path"])
            self.update_feedback(f"Fetched {os.path.basename(res['path'])}")
        else:
            ui.notify(f"Error: {res['error']}", type="negative")

    async def handle_basic_shape(self):
        self.is_processing = True
        ui.notify("Generating shape...", type="ongoing", timeout=1)
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size)
        )
        self.is_processing = False
        if res["success"]:
            await self.refresh_files()
            self.select_file(res["path_black"])
            self.update_feedback("Shape generated successfully")
        else:
            ui.notify(f"Error: {res.get('error')}", type="negative")

    async def handle_simulate_pdb(self):
        self.is_processing = True
        try:
            # 1. Resolve Path
            pdb_target = self.pdb_input_val
            if len(pdb_target) == 4 and not os.path.exists(pdb_target):
                 # Auto-fetch if ID provided
                res_fetch = await self.backend.template_service.fetch_pdb_async(pdb_target, self.output_folder)
                if not res_fetch["success"]: raise Exception(res_fetch["error"])
                pdb_target = res_fetch["path"]
            
            ui.notify("Simulating density...", type="ongoing", timeout=2)
            
            # 2. Simulate
            res_sim = await asyncio.to_thread(
                self.backend.pdb_service.simulate_map_from_pdb,
                pdb_target, self.pixel_size, int(self.box_size), self.output_folder
            )
            if not res_sim["success"]: raise Exception(res_sim["error"])

            # 3. Process
            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"], self.output_folder, self.pixel_size, int(self.box_size)
            )
            
            if res_proc["success"]:
                await self.refresh_files()
                self.select_file(res_proc["path_black"])
                self.update_feedback("Simulation complete")
            else:
                raise Exception(res_proc.get("error"))

        except Exception as e:
            ui.notify(str(e), type="negative")
            self.update_feedback(str(e), type="error")
        finally:
            self.is_processing = False

    async def handle_mask(self):
        if not self.selected_file_path: 
            ui.notify("No volume selected", type="warning")
            return
            
        self.is_processing = True
        ui.notify("Creating mask...", type="ongoing", timeout=1)
        
        try:
            # Determine input file (prefer white/positive density if we selected a black one)
            input_vol = self.selected_file_path
            if "_black.mrc" in input_vol:
                white_var = input_vol.replace("_black.mrc", "_white.mrc")
                if os.path.exists(white_var): input_vol = white_var
            
            output_mask = input_vol.replace("_white.mrc", "_mask.mrc").replace(".mrc", "_mask.mrc")
            if output_mask == input_vol: output_mask += "_mask.mrc"

            res = await self.backend.template_service.create_mask_relion(
                input_vol, output_mask, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )
            
            if res["success"]:
                await self.refresh_files()
                self.select_file(res["path"]) # Select the new mask
                self.update_feedback("Mask created successfully")
            else:
                raise Exception(res.get('error'))

        except Exception as e:
            ui.notify(str(e), type="negative")
            self.update_feedback(str(e), type="error")
        finally:
            self.is_processing = False
