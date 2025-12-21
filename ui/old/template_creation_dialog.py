from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import asyncio
from pathlib import Path
from typing import Callable

# --- MOLSTAR HTML & ROUTING ---
# We define these routes here so the iframe works immediately.

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


# Register endpoints (Safe to run multiple times, NiceGUI handles duplicates)
@app.get("/molstar")
def molstar_viewer():
    return HTMLResponse(MOLSTAR_HTML)


@app.get("/api/file")
def serve_file(path: str):
    """Serve a file by absolute path (for Molstar to fetch)."""
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


class TemplateCreationDialog:
    def __init__(self, backend, project_path: str, on_success: Callable[[str, str], None] = None):
        """
        Integrated Dialog: View, Download, Generate, and Mask.
        """
        self.backend = backend
        self.project_path = project_path
        self.on_success = on_success

        # State: Processing Flag
        self.is_processing = False

        # State: Configuration Defaults
        self.pixel_size = 1.5
        self.box_size = 128
        self.output_folder = os.path.join(project_path, "templates")

        # Ensure folder exists
        os.makedirs(self.output_folder, exist_ok=True)

        # State: Input Fields
        self.basic_shape_def = "100:100:100"
        self.pdb_input_val = ""
        self.emdb_input_val = ""

        # State: Mask Parameters
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20

        # State: Results
        self.generated_template_path = None
        self.generated_mask_path = None

        # UI Elements containers
        self.file_list_container = None
        self.files_cache = []

        # UI Initialization
        self.dialog = ui.dialog()

        # --- LAYOUT FIX: Force width to 90vw and remove max-width constraints ---
        with self.dialog, ui.card().classes("w-[90vw] max-w-none h-[90vh] flex flex-col p-0 relative overflow-hidden"):
            # --- OVERLAY ---
            with (
                ui.column()
                .bind_visibility_from(self, "is_processing")
                .classes(
                    "absolute top-0 left-0 w-full h-full justify-center items-center z-50 bg-white/80 backdrop-blur-sm"
                )
            ):
                with ui.column().classes("items-center bg-white p-6 rounded-xl shadow-2xl border border-gray-100"):
                    ui.spinner(size="3rem", color="primary", thickness=3)
                    ui.label("Processing...").classes("text-gray-700 font-bold mt-2 animate-pulse")

            self.render_ui()

    def open(self):
        self.dialog.open()
        # Trigger initial file load after a brief delay to ensure UI is ready
        asyncio.create_task(self.refresh_files())

    def safe_notify(self, message: str, type: str = "info"):
        try:
            self.dialog.client.notify(message, type=type, close_button=True)
        except Exception as e:
            print(f"[UI NOTIFY ERROR] {message} ({e})")

    def render_ui(self):
        # --- HEADER ---
        with ui.row().classes(
            "w-full justify-between items-center bg-gray-50 border-b border-gray-200 px-4 py-3 shrink-0"
        ):
            with ui.row().classes("items-center gap-2"):
                ui.icon("biotech", size="sm").classes("text-blue-600")
                ui.label("Template Workbench").classes("text-lg font-bold text-gray-800")
                ui.label(self.output_folder).classes(
                    "text-xs text-gray-400 font-mono hidden md:block truncate max-w-md"
                )

            with ui.row().classes("gap-2"):
                ui.button("Cancel", on_click=self.dialog.close).props("flat color=grey dense")
                ui.button("Use Results", on_click=self.finish).props("color=green unelevated icon=check dense")

        # --- BODY: SPLIT VIEW ---
        # The gap-0 ensures no whitespace between sidebar and viewer
        with ui.row().classes("w-full flex-grow overflow-hidden gap-0"):
            # --- LEFT PANEL: Controls (Fixed Width) ---
            with ui.column().classes(
                "w-[400px] h-full overflow-y-auto border-r border-gray-200 bg-white p-4 gap-6 shrink-0"
            ):
                # 1. Output Settings
                with ui.expansion("Global Settings", icon="settings", value=True).classes(
                    "w-full border rounded-lg shadow-sm"
                ):
                    with ui.column().classes("p-3 w-full gap-3"):
                        with ui.row().classes("w-full gap-2"):
                            ui.number("Pix Size (Å)", value=self.pixel_size, step=0.1).bind_value(
                                self, "pixel_size"
                            ).props("dense outlined").classes("flex-1")
                            ui.number("Box Size (px)", value=self.box_size, step=2).bind_value(self, "box_size").props(
                                "dense outlined"
                            ).classes("flex-1")

                # 2. Remote Sources
                with ui.expansion("Remote Sources", icon="cloud_download").classes(
                    "w-full border rounded-lg shadow-sm"
                ):
                    with ui.column().classes("p-3 w-full gap-4"):
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.input(placeholder="PDB ID (e.g. 3j7z)").bind_value(self, "pdb_input_val").props(
                                "dense outlined"
                            ).classes("flex-grow")
                            ui.button(icon="download", on_click=self.handle_fetch_pdb).props("flat dense color=primary")
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.input(placeholder="EMDB ID (e.g. 30210)").bind_value(self, "emdb_input_val").props(
                                "dense outlined"
                            ).classes("flex-grow")
                            ui.button(icon="download", on_click=self.handle_fetch_emdb).props(
                                "flat dense color=primary"
                            )

                # 3. File List
                with ui.card().classes("w-full p-0 border border-gray-200 shadow-none"):
                    with ui.row().classes("w-full justify-between items-center bg-gray-50 px-3 py-2 border-b"):
                        ui.label("Templates & Sources").classes("text-xs font-bold text-gray-600 uppercase")
                        ui.button(icon="refresh", on_click=self.refresh_files).props(
                            "flat round dense size=xs color=grey"
                        )

                    self.file_list_container = ui.column().classes("w-full max-h-48 overflow-y-auto p-0 gap-0")

                # 4. Generator Tools
                with ui.tabs().classes("w-full text-left border-b text-gray-700") as tabs:
                    t_shape = ui.tab("Shape")
                    t_pdb = ui.tab("From PDB")
                    t_mask = ui.tab("Mask")

                with ui.tab_panels(tabs, value=t_shape).classes("w-full border border-t-0 rounded-b-lg p-4 shadow-sm"):
                    # Tab: Shape
                    with ui.tab_panel(t_shape):
                        ui.label("Synthetic Ellipsoid").classes("text-xs text-gray-400 mb-2")
                        ui.input("Diameter (x:y:z Å)", placeholder="100:500:100").bind_value(
                            self, "basic_shape_def"
                        ).props("dense outlined").classes("w-full mb-3")
                        ui.button("Generate Shape", icon="category", on_click=self.handle_basic_shape).props(
                            "color=primary unelevated w-full"
                        )

                    # Tab: PDB Simulation
                    with ui.tab_panel(t_pdb):
                        ui.label("Simulate Map from PDB").classes("text-xs text-gray-400 mb-2")
                        ui.input("PDB Path / Code", placeholder="Select file above or type").bind_value(
                            self, "pdb_input_val"
                        ).props("dense outlined").classes("w-full mb-3")
                        ui.button("Simulate Density", icon="biotech", on_click=self.handle_simulate_pdb).props(
                            "color=primary unelevated w-full"
                        )

                    # Tab: Masking
                    with ui.tab_panel(t_mask):
                        self.mask_control_area = ui.column().classes("w-full gap-3")
                        self.render_mask_controls()

            # --- RIGHT PANEL: Viewer (Flex Grow) ---
            # This will take up the remaining space (approx 90vw - 400px)
            with ui.card().classes("flex-grow h-full p-0 rounded-none border-none bg-black relative"):
                ui.element("iframe").props('src="/molstar" id="molstar-frame"').classes("w-full h-full").style(
                    "border: none;"
                )

                with ui.column().classes(
                    "absolute bottom-4 right-4 bg-black/60 p-3 rounded text-white text-xs backdrop-blur-sm pointer-events-none"
                ):
                    ui.label("Controls:").classes("font-bold mb-1")
                    ui.label("Left Click + Drag: Rotate")
                    ui.label("Right Click + Drag: Pan")
                    ui.label("Scroll: Zoom")

    def render_mask_controls(self):
        self.mask_control_area.clear()
        with self.mask_control_area:
            if not self.generated_template_path:
                ui.label("Generate or select a template first.").classes("text-xs text-orange-500 italic")
            else:
                base = os.path.basename(self.generated_template_path)
                ui.label(f"Input: {base}").classes("text-xs font-mono text-gray-600 truncate w-full mb-2")

                with ui.grid(columns=2).classes("w-full gap-2"):
                    ui.number("Thr", value=0.001, format="%.4f").bind_value(self, "mask_threshold").props(
                        "dense outlined"
                    ).tooltip("Threshold")
                    ui.number("Ext", value=3).bind_value(self, "mask_extend").props("dense outlined").tooltip(
                        "Extend (px)"
                    )
                    ui.number("Soft", value=6).bind_value(self, "mask_soft_edge").props("dense outlined").tooltip(
                        "Soft Edge (px)"
                    )
                    ui.number("Low", value=20).bind_value(self, "mask_lowpass").props("dense outlined").tooltip(
                        "Lowpass (Å)"
                    )

                ui.button("Create Mask", icon="architecture", on_click=self.handle_mask).props(
                    "color=secondary unelevated w-full mt-2"
                )

    # --- LOGIC: FETCHING & LISTING ---

    async def refresh_files(self):
        self.file_list_container.clear()

        # Add loading skeleton
        with self.file_list_container:
            ui.skeleton().classes("w-full h-8 mb-1")
            ui.skeleton().classes("w-full h-8")

        # Fetch
        files = await self.backend.template_service.list_template_files_async(self.output_folder)
        self.files_cache = files

        self.file_list_container.clear()
        with self.file_list_container:
            if not files:
                ui.label("No files found.").classes("text-xs text-gray-400 p-2 italic")
                return

            for f_path in files:
                fname = os.path.basename(f_path)
                is_vol = self._is_volume(fname)
                icon = "view_in_ar" if is_vol else "account_tree"
                color = "text-blue-500" if is_vol else "text-green-500"

                # --- ROW CONSTRUCTION (FIXED) ---
                # We save the row object to 'row_el' so we can attach events to it directly
                row_el = ui.row().classes(
                    "w-full items-center px-2 py-1.5 hover:bg-blue-50 cursor-pointer border-b border-gray-50 group transition-colors"
                )

                with row_el:
                    ui.icon(icon, size="xs").classes(color)
                    ui.label(fname).classes(
                        "text-xs text-gray-700 truncate flex-grow group-hover:text-blue-700 font-mono"
                    )
                    ui.icon("visibility", size="xs").classes(
                        "text-gray-300 group-hover:text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity"
                    )

                # Attach click handler directly to the row element
                row_el.on("click", lambda _, p=f_path: self.select_file(p))

    def select_file(self, path: str):
        """Load file in viewer and set as active input for generators."""
        # 1. Load in Viewer
        if self._is_volume(path):
            self._viewer_load_volume(path)
            # 2. Set as input for Masking
            if "_black" in path or "_white" in path or ".mrc" in path:
                self.generated_template_path = path
                self.render_mask_controls()
                # Auto-calculate threshold if it's a volume
                asyncio.create_task(self._auto_threshold(path))
        else:
            self._viewer_load_structure(path)
            # 2. Set as input for PDB sim
            self.pdb_input_val = path

        self.safe_notify(f"Selected: {os.path.basename(path)}")

    async def handle_fetch_pdb(self):
        if not self.pdb_input_val:
            return
        self.is_processing = True
        res = await self.backend.template_service.fetch_pdb_async(self.pdb_input_val, self.output_folder)
        self.is_processing = False

        if res["success"]:
            self.safe_notify(f"Downloaded {self.pdb_input_val}", "positive")
            await self.refresh_files()
            self.select_file(res["path"])
        else:
            self.safe_notify(f"Error: {res['error']}", "negative")

    async def handle_fetch_emdb(self):
        if not self.emdb_input_val:
            return
        self.is_processing = True
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val, self.output_folder)
        self.is_processing = False

        if res["success"]:
            self.safe_notify(f"Downloaded EMD-{self.emdb_input_val}", "positive")
            await self.refresh_files()
            self.select_file(res["path"])
        else:
            self.safe_notify(f"Error: {res['error']}", "negative")

    # --- LOGIC: GENERATION ---

    async def handle_basic_shape(self):
        self.is_processing = True
        try:
            res = await self.backend.template_service.generate_basic_shape_async(
                self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size)
            )
            if not res["success"]:
                raise Exception(res["error"])

            self._handle_gen_success(res)
        except Exception as e:
            self.safe_notify(str(e), "negative")
        finally:
            self.is_processing = False

    async def handle_simulate_pdb(self):
        self.is_processing = True
        try:
            # 1. Resolve PDB Path (Code or File)
            pdb_target = self.pdb_input_val
            if len(pdb_target) == 4 and not os.path.exists(pdb_target):
                # Fetch first if it looks like an ID and file doesn't exist
                res_fetch = await self.backend.template_service.fetch_pdb_async(pdb_target, self.output_folder)
                if not res_fetch["success"]:
                    raise Exception(res_fetch["error"])
                pdb_target = res_fetch["path"]

            # 2. Simulate
            res_sim = await asyncio.to_thread(
                self.backend.pdb_service.simulate_map_from_pdb,
                pdb_target,
                self.pixel_size,
                int(self.box_size),
                self.output_folder,
            )
            if not res_sim["success"]:
                raise Exception(res_sim["error"])

            # 3. Post-process (Crop/Pad/Invert)
            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"], self.output_folder, self.pixel_size, int(self.box_size)
            )
            self._handle_gen_success(res_proc)

        except Exception as e:
            self.safe_notify(str(e), "negative")
        finally:
            self.is_processing = False

    async def handle_mask(self):
        if not self.generated_template_path:
            return
        self.is_processing = True
        try:
            # Logic to find the "white" (non-inverted) map for masking
            input_vol = self.generated_template_path.replace("_black.mrc", "_white.mrc")
            if not os.path.exists(input_vol):
                input_vol = self.generated_template_path

            output_mask = input_vol.replace("_white.mrc", "_mask.mrc").replace(".mrc", "_mask.mrc")
            if output_mask == input_vol:
                output_mask += "_mask.mrc"

            res = await self.backend.template_service.create_mask_relion(
                input_vol, output_mask, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )
            if not res["success"]:
                raise Exception(res["error"])

            self.generated_mask_path = res["path"]
            self.safe_notify("Mask created!", "positive")
            await self.refresh_files()
            self.select_file(self.generated_mask_path)

        except Exception as e:
            self.safe_notify(str(e), "negative")
        finally:
            self.is_processing = False

    def _handle_gen_success(self, res):
        self.generated_template_path = res["path_black"]
        self.safe_notify("Template generated!", "positive")
        # Trigger UI update via refresh
        asyncio.create_task(self.refresh_files())
        # Load the result in viewer
        self.select_file(self.generated_template_path)

    async def _auto_threshold(self, path):
        thr = await self.backend.template_service.calculate_auto_threshold_async(path)
        self.mask_threshold = round(thr, 4)

    # --- VIEWER HELPERS ---

    def _viewer_load_structure(self, file_path: str):
        file_url = f"/api/file?path={file_path}"
        fmt, is_binary = self._get_structure_format(file_path)
        ui.run_javascript(f"""
            document.getElementById('molstar-frame').contentWindow.postMessage(
                {{ action: 'load_structure', url: '{file_url}', format: '{fmt}', isBinary: {str(is_binary).lower()} }},
                '*'
            );
        """)

    def _viewer_load_volume(self, file_path: str):
        file_url = f"/api/file?path={file_path}"
        ui.run_javascript(f"""
            document.getElementById('molstar-frame').contentWindow.postMessage(
                {{ action: 'load_volume', url: '{file_url}' }},
                '*'
            );
        """)

    def _get_structure_format(self, path: str) -> tuple[str, bool]:
        ext = Path(path).suffix.lower()
        if ext == ".pdb":
            return "pdb", False
        elif ext == ".cif":
            return "mmcif", False
        elif ext == ".bcif":
            return "mmcif", True
        return "mmcif", False

    def _is_volume(self, path_str: str) -> bool:
        return Path(path_str).suffix.lower() in {".mrc", ".map", ".rec", ".ccp4"}

    def finish(self):
        if self.on_success and self.generated_template_path:
            self.on_success(self.generated_template_path, self.generated_mask_path)
        self.dialog.close()
