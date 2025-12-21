# ui/template_workbench.py
from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import asyncio
from pathlib import Path

from services.project_state import JobType, get_project_state, get_state_service

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
        }
        
        async function loadStructure(url, format) {
            if (!viewer) return;
            try {
                await viewer.plugin.clear();
                await viewer.loadStructureFromUrl(url, format, false);
                viewer.plugin.behaviors.layout.leftPanelState.next('collapsed');
            } catch (e) { console.error('Load failed:', e); }
        }
        
        async function loadVolume(url) {
            if (!viewer) return;
            try {
                await viewer.plugin.clear();
                await viewer.loadVolumeFromUrl(
                    { url: url, format: 'ccp4', isBinary: true },
                    [{ type: 'relative', value: 2.0, color: 0x3388ff, alpha: 0.8 }]
                );
            } catch (e) { console.error('Load failed:', e); }
        }
        
        window.addEventListener('message', async (e) => {
            if (!e.data || !e.data.action) return;
            if (e.data.action === 'load_structure') {
                await loadStructure(e.data.url, e.data.format);
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


@app.get("/api/file")
def serve_file(path: str):
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


class TemplateWorkbench:
    """Three-column template workbench: Controls | Viewer | Files"""

    def __init__(self, backend, project_path: str):
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        os.makedirs(self.output_folder, exist_ok=True)

        self.is_processing = False

        # Settings
        self.pixel_size = 1.5
        self.box_size = 128
        self.basic_shape_def = "100:100:100"
        self.pdb_input_val = ""
        self.emdb_input_val = ""

        # Mask settings
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20

        # UI refs
        self.file_list_container = None
        self.mask_btn = None
        self.mask_ref_label = None

        self._render()
        asyncio.create_task(self.refresh_files())

    # =========================================================
    # STATE
    # =========================================================

    def _get_tm_params(self):
        state = get_project_state()
        return state.jobs.get(JobType.TEMPLATE_MATCH_PYTOM)

    def _get_current_template_path(self) -> str:
        params = self._get_tm_params()
        return params.template_path if params else ""

    def _get_current_mask_path(self) -> str:
        params = self._get_tm_params()
        return params.mask_path if params else ""

    async def _set_template_path(self, path: str):
        params = self._get_tm_params()
        if params:
            params.template_path = path
            await get_state_service().save_project()

    async def _set_mask_path(self, path: str):
        params = self._get_tm_params()
        if params:
            params.mask_path = path
            await get_state_service().save_project()

    # =========================================================
    # RENDER
    # =========================================================

    def _render(self):
        # Fixed height container for the whole workbench
        with ui.column().classes("w-full"):
            # Selection indicators at top
            with ui.row().classes("w-full gap-4 p-3 bg-gray-50 border-b border-gray-200"):
                with ui.row().classes("flex-1 items-center gap-2"):
                    ui.icon("view_in_ar", size="xs").classes("text-blue-600")
                    ui.label("Template:").classes("text-xs font-bold text-gray-600")
                    self.template_label = ui.label("Not set").classes("text-xs font-mono text-gray-500 italic")
                with ui.row().classes("flex-1 items-center gap-2"):
                    ui.icon("architecture", size="xs").classes("text-purple-600")
                    ui.label("Mask:").classes("text-xs font-bold text-gray-600")
                    self.mask_label = ui.label("Not set").classes("text-xs font-mono text-gray-500 italic")

            # Three columns with fixed height
            with ui.row().classes("w-full items-stretch").style("height: 480px;"):
                # =====================
                # COLUMN 1: Controls (narrow)
                # =====================
                with ui.column().classes(
                    "w-[500px] shrink-0 p-3 border-r border-gray-200 gap-2 bg-white overflow-y-auto"
                ):
                    # Global settings
                    self._section("Settings", "tune")
                    with ui.row().classes("w-full gap-2"):
                        ui.number("Apix", value=self.pixel_size, step=0.1, format="%.2f").bind_value(
                            self, "pixel_size"
                        ).props("dense outlined").classes("flex-1")
                        ui.number("Box", value=self.box_size, step=2).bind_value(self, "box_size").props(
                            "dense outlined"
                        ).classes("flex-1")

                    ui.separator().classes("my-1")

                    # Fetch
                    self._section("Fetch", "cloud_download")
                    with ui.row().classes("w-full gap-1"):
                        ui.input(placeholder="PDB ID").bind_value(self, "pdb_input_val").props(
                            "dense outlined"
                        ).classes("flex-1")
                        ui.button(icon="download", on_click=self._fetch_pdb).props("flat dense color=primary")
                    with ui.row().classes("w-full gap-1"):
                        ui.input(placeholder="EMDB ID").bind_value(self, "emdb_input_val").props(
                            "dense outlined"
                        ).classes("flex-1")
                        ui.button(icon="download", on_click=self._fetch_emdb).props("flat dense color=primary")

                    ui.separator().classes("my-1")

                    # Shape
                    self._section("Shape", "category")
                    with ui.row().classes("w-full gap-1"):
                        ui.input(placeholder="x:y:z Å").bind_value(self, "basic_shape_def").props(
                            "dense outlined"
                        ).classes("flex-1")
                        ui.button(icon="play_arrow", on_click=self._gen_shape).props("flat dense color=primary")

                    ui.separator().classes("my-1")

                    # Simulate
                    self._section("Simulate PDB", "biotech")
                    with ui.row().classes("w-full gap-1"):
                        ui.input(placeholder="PDB file/ID").bind_value(self, "pdb_input_val").props(
                            "dense outlined"
                        ).classes("flex-1")
                        ui.button(icon="play_arrow", on_click=self._simulate_pdb).props("flat dense color=primary")

                    ui.separator().classes("my-1")

                    # Mask - only works on selected template
                    self._section("Create Mask", "architecture")
                    self.mask_ref_label = ui.label("Set a template first").classes(
                        "text-[10px] text-orange-500 italic mb-1"
                    )
                    with ui.row().classes("w-full gap-2"):
                        ui.number("Thr", format="%.4f").bind_value(self, "mask_threshold").props(
                            "dense outlined"
                        ).classes("flex-1")
                        ui.number("Ext").bind_value(self, "mask_extend").props("dense outlined").classes("flex-1")
                    with ui.row().classes("w-full gap-2 mt-1"):
                        ui.number("Soft").bind_value(self, "mask_soft_edge").props("dense outlined").classes("flex-1")
                        ui.number("LP").bind_value(self, "mask_lowpass").props("dense outlined").classes("flex-1")
                    self.mask_btn = (
                        ui.button("Create Mask", icon="play_arrow", on_click=self._create_mask)
                        .props("unelevated dense color=secondary disabled")
                        .classes("w-full mt-2")
                    )

                # =====================
                # COLUMN 2: Viewer (smaller width)
                # =====================
                with (
                    ui.element("div")
                    .classes("flex-1 min-w-[300px] max-w-[500px] bg-black relative")
                    .style("height: 100%;")
                ):
                    ui.element("iframe").props('src="/molstar" id="molstar-frame"').style(
                        "width: 100%; height: 100%; border: none;"
                    )
                    with ui.row().classes(
                        "absolute bottom-2 left-2 bg-black/60 px-2 py-1 rounded text-[9px] text-gray-400"
                    ):
                        ui.label("LMB: Rotate | RMB: Pan | Scroll: Zoom")

                # =====================
                # COLUMN 3: Files (wider)
                # =====================
                with ui.column().classes("w-[400px] shrink-0 border-l border-gray-200 bg-white"):
                    with ui.row().classes(
                        "w-full items-center justify-between p-2 border-b border-gray-100 bg-gray-50"
                    ):
                        ui.label("Files").classes("text-xs font-bold text-gray-600 uppercase")
                        ui.button(icon="refresh", on_click=self.refresh_files).props(
                            "flat round dense size=xs color=grey"
                        )

                    self.file_list_container = ui.column().classes("w-full p-2 gap-1 overflow-y-auto flex-1")

    def _section(self, title: str, icon: str):
        with ui.row().classes("items-center gap-1"):
            ui.icon(icon, size="xs").classes("text-gray-400")
            ui.label(title).classes("text-[10px] font-bold text-gray-500 uppercase")

    def _update_selection_labels(self):
        t_path = self._get_current_template_path()
        m_path = self._get_current_mask_path()

        if t_path:
            self.template_label.set_text(os.path.basename(t_path))
            self.template_label.classes(replace="text-xs font-mono text-blue-700")
        else:
            self.template_label.set_text("Not set")
            self.template_label.classes(replace="text-xs font-mono text-gray-500 italic")

        if m_path:
            self.mask_label.set_text(os.path.basename(m_path))
            self.mask_label.classes(replace="text-xs font-mono text-purple-700")
        else:
            self.mask_label.set_text("Not set")
            self.mask_label.classes(replace="text-xs font-mono text-gray-500 italic")

        # Update mask creation UI based on template selection
        self._update_mask_ui()

    def _update_mask_ui(self):
        """Enable/disable mask creation based on template selection."""
        t_path = self._get_current_template_path()

        if t_path and any(x in t_path.lower() for x in [".mrc", ".map", ".rec"]):
            # Template is a volume - enable mask creation
            if self.mask_ref_label:
                self.mask_ref_label.set_text(os.path.basename(t_path))
                self.mask_ref_label.classes(replace="text-[10px] text-blue-600 font-mono")
            if self.mask_btn:
                self.mask_btn.props(remove="disabled")
            # Auto-calc threshold
            asyncio.create_task(self._auto_threshold(t_path))
        else:
            # No template or not a volume
            if self.mask_ref_label:
                self.mask_ref_label.set_text("Set a volume template first")
                self.mask_ref_label.classes(replace="text-[10px] text-orange-500 italic")
            if self.mask_btn:
                self.mask_btn.props(add="disabled")

    # =========================================================
    # FILE LIST
    # =========================================================

    async def refresh_files(self):
        if not self.file_list_container:
            return

        self._update_selection_labels()
        self.file_list_container.clear()

        files = await self.backend.template_service.list_template_files_async(self.output_folder)
        current_template = self._get_current_template_path()
        current_mask = self._get_current_mask_path()

        with self.file_list_container:
            if not files:
                ui.label("No files yet").classes("text-xs text-gray-400 italic py-4 text-center w-full")
                return

            for f_path in files:
                fname = os.path.basename(f_path)
                # is_vol = any(x in fname.lower() for x in [".mrc", ".map", ".rec"]) # unused now for icons
                is_template = f_path == current_template
                is_mask = f_path == current_mask

                # Row styling
                bg = "bg-blue-50" if is_template else ("bg-purple-50" if is_mask else "hover:bg-gray-50")
                border = ""
                if is_template:
                    border = "border-l-4 border-l-blue-500"
                elif is_mask:
                    border = "border-l-4 border-l-purple-500"
                else:
                    border = "border-l-4 border-l-transparent"

                # Row Container
                with ui.row().classes(f"w-full items-center justify-between gap-2 px-2 py-1.5 rounded {bg} {border}"):
                    # Clickable name area - visualize on click
                    with (
                        ui.row()
                        .classes("flex-1 items-center gap-2 min-w-0 cursor-pointer")
                        .on("click", lambda p=f_path: self._visualize(p))
                    ):
                        # Icon removed as per request
                        ui.label(fname).classes("text-[11px] font-mono text-gray-700 truncate flex-1").tooltip(f_path)

                        # Badges
                        if is_template:
                            ui.label("T").classes(
                                "text-[8px] font-bold text-white bg-blue-500 w-4 h-4 rounded flex items-center justify-center shrink-0"
                            )
                        if is_mask:
                            ui.label("M").classes(
                                "text-[8px] font-bold text-white bg-purple-500 w-4 h-4 rounded flex items-center justify-center shrink-0"
                            )

                    # Actions - always visible
                    with ui.row().classes("gap-0 shrink-0"):
                        # NOTE: Removed asyncio.create_task here to fix RuntimeError
                        ui.button(icon="view_in_ar", on_click=lambda p=f_path: self._toggle_template(p)).props(
                            f"flat round dense size=xs {'color=blue' if is_template else 'color=grey'}"
                        ).tooltip("Set as Template")
                        ui.button(icon="architecture", on_click=lambda p=f_path: self._toggle_mask(p)).props(
                            f"flat round dense size=xs {'color=purple' if is_mask else 'color=grey'}"
                        ).tooltip("Set as Mask")
                        ui.button(icon="delete", on_click=lambda p=f_path: self._delete(p)).props(
                            "flat round dense size=xs color=red"
                        ).tooltip("Delete")

    def _visualize(self, path: str):
        """Visualize a file in molstar."""
        if any(x in path.lower() for x in [".mrc", ".map", ".rec", ".ccp4"]):
            preview = path.replace(".mrc", "_preview.mrc")
            to_load = preview if os.path.exists(preview) else path
            url = f"/api/file?path={to_load}"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_volume', url: '{url}' }}, '*');"
            )
        else:
            url = f"/api/file?path={path}"
            fmt = "pdb" if path.lower().endswith(".pdb") else "mmcif"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_structure', url: '{url}', format: '{fmt}' }}, '*');"
            )

    async def _auto_threshold(self, path):
        if not path or not os.path.exists(path):
            return
        thr = await self.backend.template_service.calculate_auto_threshold_async(path)
        self.mask_threshold = round(thr, 4)

    async def _toggle_template(self, path: str):
        if self._get_current_template_path() == path:
            await self._set_template_path("")
        else:
            await self._set_template_path(path)
            # Auto-visualize when setting template
            self._visualize(path)
        await self.refresh_files()

    async def _toggle_mask(self, path: str):
        if self._get_current_mask_path() == path:
            await self._set_mask_path("")
        else:
            await self._set_mask_path(path)
        await self.refresh_files()

    async def _delete(self, path: str):
        if path == self._get_current_template_path():
            await self._set_template_path("")
        if path == self._get_current_mask_path():
            await self._set_mask_path("")
        await self.backend.template_service.delete_file_async(path)
        await self.refresh_files()

    # =========================================================
    # ACTIONS
    # =========================================================

    async def _fetch_pdb(self):
        if not self.pdb_input_val:
            return
        res = await self.backend.template_service.fetch_pdb_async(
            self.pdb_input_val.strip().lower(), self.output_folder
        )
        if res["success"]:
            await self.refresh_files()
            self._visualize(res["path"])

    async def _fetch_emdb(self):
        if not self.emdb_input_val:
            return
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val.strip(), self.output_folder)
        if res["success"]:
            await self.refresh_files()
            self._visualize(res["path"])

    async def _gen_shape(self):
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size)
        )
        if res["success"]:
            await self.refresh_files()
            self._visualize(res["path_black"])

    async def _simulate_pdb(self):
        try:
            pdb_target = self.pdb_input_val
            if len(pdb_target) == 4 and not os.path.exists(pdb_target):
                res = await self.backend.template_service.fetch_pdb_async(pdb_target, self.output_folder)
                if not res["success"]:
                    return
                pdb_target = res["path"]

            res_sim = await asyncio.to_thread(
                self.backend.pdb_service.simulate_map_from_pdb,
                pdb_target,
                self.pixel_size,
                int(self.box_size),
                self.output_folder,
            )
            if not res_sim["success"]:
                return

            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"], self.output_folder, self.pixel_size, int(self.box_size)
            )
            if res_proc["success"]:
                await self.refresh_files()
                self._visualize(res_proc["path_black"])
        except Exception as e:
            print(f"[WORKBENCH] Simulation error: {e}")

    async def _create_mask(self):
        """Create mask from the currently selected TEMPLATE (not arbitrary file)."""
        template_path = self._get_current_template_path()

        if not template_path:
            return

        if not any(x in template_path.lower() for x in [".mrc", ".map", ".rec"]):
            return

        try:
            # Use white version if available (better for masking)
            input_vol = template_path
            if "_black.mrc" in input_vol:
                white = input_vol.replace("_black.mrc", "_white.mrc")
                if os.path.exists(white):
                    input_vol = white

            base = (
                os.path.basename(input_vol)
                .replace("_white.mrc", "")
                .replace("_black.mrc", "")
                .replace(".mrc", "")
                .replace(".map", "")
            )
            output = os.path.join(self.output_folder, f"{base}_mask.mrc")

            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )
            if res["success"]:
                await self.refresh_files()
                self._visualize(res["path"])
        except Exception as e:
            print(f"[WORKBENCH] Mask creation error: {e}")
