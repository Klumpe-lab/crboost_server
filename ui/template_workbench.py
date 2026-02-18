from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import json
import asyncio
from pathlib import Path
import mrcfile
from nicegui import context

from services.project_state import JobType, get_project_state, get_state_service

# Color palette for the Molstar viewer session
COLOR_PALETTE = [
    0x5C6BC0,
    0x7986CB,
    0x9FA8DA,
    0x42A5F5,
    0x64B5F6,
    0x90CAF9,
    0x26C6DA,
    0x4DD0E1,
    0x80DEEA,
    0x26A69A,
    0x4DB6AC,
    0x80CBC4,
    0x66BB6A,
    0x81C784,
    0xA5D6A7,
    0x9CCC65,
    0xAED581,
    0xC5E1A5,
    0xFFA726,
    0xFFB74D,
    0xFFCC80,
    0xFFEE58,
    0xFFF176,
    0xFFF59D,
    0xEF5350,
    0xE57373,
    0xEF9A9A,
    0xEC407A,
    0xF06292,
    0xF48FB1,
    0xAB47BC,
    0xBA68C8,
    0xCE93D8,
    0x7E57C2,
    0x9575CD,
    0xB39DDB,
]

MOLSTAR_EMBED_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        html, body, #app { width: 100%; height: 100%; overflow: hidden; background: #1a1a1a; }
    </style>
</head>
<body>
    <div id="app"></div>
    <script type="module" src="/static/molstar/embed.js"></script>
</body>
</html>
"""


@app.get("/molstar-workbench")
def molstar_workbench_viewer():
    return HTMLResponse(MOLSTAR_EMBED_HTML)


@app.get("/api/file")
def serve_file(path: str):
    p = Path(path)
    if p.exists() and p.is_file():
        return FileResponse(p, media_type="application/octet-stream")
    return {"error": "not found"}


class TemplateWorkbench:
    """Integrated Template Workbench with Metadata Inspection and PDB/MRC Services."""

    def __init__(self, backend, project_path: str):
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        os.makedirs(self.output_folder, exist_ok=True)

        self.project_raw_apix = None
        self.project_binning = None
        self.project_tomo_apix = None

        self.pixel_size = 10.0
        self.box_size = 128
        self.auto_box = True
        self.template_resolution = 20.0

        self.basic_shape_def = "550:550:550"
        self.pdb_input_val = ""
        self.emdb_input_val = ""
        self.structure_path = ""

        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20
        self.threshold_method = "flexible_bounds"

        self.masking_active = False
        self.viewer_ready = False
        self.loaded_items = []

        self.file_list_container = None
        self.session_list_container = None
        self.log_container = None
        self.mask_btn = None
        self.mask_source_label = None
        self.box_input = None
        self.size_estimate_label = None
        self.template_label = None
        self.mask_label = None
        self.structure_label = None
        self.auto_box_checkbox = None
        self.warning_container = None
        self.simulate_btn = None
        self.resample_btn = None
        self.validation_dot = None
        self.meta_label = None

        self._last_logged_box = None
        self.session_item_containers = {}

        self._load_project_parameters()

        self.client = None
        self._render()
        self.client = context.client 

        ui.timer(0, self.refresh_files, once=True)
        ui.timer(0, self._test_iframe_loaded, once=True)   # or delete this; see below


    def _load_project_parameters(self):
        """Load pixel size and binning from project state."""
        try:
            state = get_project_state()
            if hasattr(state, "microscope") and state.microscope:
                raw_pix = getattr(state.microscope, "pixel_size_angstrom", None)
                if raw_pix and raw_pix > 0:
                    self.project_raw_apix = raw_pix

            if hasattr(state, "jobs") and state.jobs:
                for job_type, job_params in state.jobs.items():
                    if "reconstruct" in job_type.value.lower():
                        for field in ["rescale_angpixs", "binned_angpix", "output_angpix"]:
                            val = getattr(job_params, field, None)
                            if val and float(val) > 0:
                                self.project_tomo_apix = float(val)
                                if self.project_raw_apix:
                                    self.project_binning = round(self.project_tomo_apix / self.project_raw_apix, 1)
                                break
                        break

            if self.project_tomo_apix:
                self.pixel_size = self.project_tomo_apix
            elif self.project_raw_apix:
                self.pixel_size = self.project_raw_apix
        except Exception as e:
            print(f"[TEMPLATE_WORKBENCH] Load project params error: {e}")

    async def _on_local_click(self, path):
        """View file and update property panel."""
        fname = os.path.basename(path)
        ext = Path(path).suffix.lower()
        file_url = f"/api/file?path={path}"
        self._log(f"Viewing: {fname}")

        asyncio.create_task(self._update_metadata_panel(path))

        if ext in [".mrc", ".map", ".rec", ".ccp4"]:
            self._post_to_viewer("load_volume", url=file_url)
        elif ext in [".pdb", ".cif"]:
            fmt = "pdb" if ext == ".pdb" else "mmcif"
            self._post_to_viewer("load_structure", url=file_url, format=fmt)

    async def _update_metadata_panel(self, path: str):
        """Fetches header data using PdbService and mrcfile."""
        ext = Path(path).suffix.lower()
        info = ""

        if ext in [".mrc", ".map"]:
            try:
                with mrcfile.open(path, header_only=True) as mrc:
                    nx, ny, nz = mrc.header.nx, mrc.header.ny, mrc.header.nz
                    apix = mrc.voxel_size.x
                    info = f"**DIM:** {nx}x{ny}x{nz}\n\n**APIX:** {apix:.3f}Å"

                    if self.project_tomo_apix:
                        is_valid = abs(apix - self.project_tomo_apix) < 0.01
                        self.validation_dot.classes(replace="text-green-500" if is_valid else "text-red-500")
            except:
                info = "MRC Header Read Error"

        elif ext in [".pdb", ".cif"]:
            res = await self.backend.pdb_service.get_structure_metadata(path)
            if res.get("success"):
                bb = res["bbox"]
                info = f"**RES:** {res['residues']} | **SYM:** {res['symmetry']}\n\n**BOX:** {bb[0]}x{bb[1]}x{bb[2]}Å"
            else:
                info = f"PDB Error: {res.get('error')}"

        if self.meta_label:
            self.meta_label.set_content(info)

    async def _align_pdb(self):
        """Align selected PDB to principal axes."""
        if not self.structure_path:
            ui.notify("Select a structure in the tray first", type="warning")
            return

        target = self.structure_path.replace(".cif", "_aligned.cif").replace(".pdb", "_aligned.pdb")
        self._log("Aligning structure to principal axes (SVD)...")
        res = await self.backend.pdb_service.align_to_principal_axes(self.structure_path, target)

        if res["success"]:
            self._log(f"Alignment complete: {os.path.basename(target)}")
            await self.refresh_files()
        else:
            self._log(f"Alignment Error: {res.get('error')}")

    async def _simulate_pdb(self):
        if not self.structure_path:
            ui.notify("Select a structure first", type="warning")
            return

        # Disable the buttons so users don't double-submit
        if self.simulate_btn:
            self.simulate_btn.set_enabled(False)
        if self.resample_btn:
            self.resample_btn.set_enabled(False)

        n = ui.notification("Simulating density… this can take a while", type="ongoing", spinner=True, timeout=None)

        try:
            self._log(f"Simulating density from {os.path.basename(self.structure_path)}...")

            res = await self.backend.pdb_service.simulate_map_from_pdb(
                pdb_path=self.structure_path,
                output_folder=self.output_folder,
                target_apix=self.pixel_size,
                target_box=int(self.box_size),
                resolution=self.template_resolution,
            )

            if res.get("success"):
                self._log(f"Simulation finished: {os.path.basename(res['path_black'])}")
                ui.notify("Simulation finished", type="positive")
                await self.refresh_files()
            else:
                self._log(f"Simulation failed: {res.get('error')}")
                ui.notify("Simulation failed (see log)", type="negative", timeout=8000)

        finally:
            n.dismiss()
            # Re-enable according to current selection logic
            self._update_selection_labels()
            if self.resample_btn:
                self.resample_btn.set_enabled(True)  # or keep your WIP behavior


    async def _resample_emdb(self):
        """Resample existing map via Relion."""
        if not self.structure_path:
            return
        self._log(f"Resampling map via Relion: {os.path.basename(self.structure_path)}...")
        res = await self.backend.template_service.process_volume_async(
            self.structure_path, self.output_folder, self.pixel_size, int(self.box_size), self.template_resolution
        )
        if res["success"]:
            self._log(f"Resampled pair created: {os.path.basename(res['path_white'])}")
            await self.refresh_files()
        else:
            self._log(f"Resampling Error: {res.get('error')}")

    def _render(self):
        with ui.column().classes("w-full gap-0 bg-white"):
            with ui.row().classes("w-full gap-6 px-4 py-2 bg-gray-50 border-b items-center"):
                self.validation_dot = ui.icon("fiber_manual_record", size="14px").classes("text-gray-300")
                self._render_header_indicators()

            with ui.row().classes("w-full gap-0 border-b").style("height: 480px; overflow: hidden;"):
                with ui.column().classes("w-[40%] p-4 gap-3 border-r overflow-y-auto h-full"):
                    self._render_template_creation_panel()

                with ui.column().classes("w-[25%] p-4 gap-3 border-r overflow-y-auto h-full"):
                    self._render_mask_creation_panel()

                with ui.column().classes("flex-1 p-4 gap-2 bg-gray-50/30 overflow-hidden h-full"):
                    self._render_logs_panel()

            with ui.row().classes("w-full gap-0").style("height: 450px; overflow: hidden;"):
                with ui.column().classes("w-1/4 p-4 border-r bg-gray-50/10 flex flex-col h-full"):
                    self._render_tray_header("Available Locally", "folder")
                    self.file_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")

                    ui.separator().classes("my-2")
                    with ui.card().tight().classes("w-full p-2 bg-blue-50/40 border border-blue-100 shadow-none"):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            ui.icon("analytics", size="14px").classes("text-blue-400")
                            ui.label("Header Properties").classes("text-[10px] font-bold uppercase text-blue-500")
                        self.meta_label = ui.markdown("Select file...").classes(
                            "text-[10px] font-mono leading-tight text-gray-600"
                        )

                with ui.column().classes("w-1/4 p-4 border-r bg-white h-full"):
                    with ui.row().classes("w-full items-center justify-between mb-3"):
                        self._render_tray_header("In Viewer", "layers")
                        ui.button(icon="delete_sweep", on_click=lambda: self._post_to_viewer("clear")).props(
                            "flat round dense size=sm color=red"
                        )
                    self.session_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")

                with ui.column().classes("flex-1 bg-black relative overflow-hidden h-full"):
                    ui.element("iframe").props('src="/molstar-workbench" id="molstar-frame"').classes(
                        "absolute inset-0 w-full h-full border-none"
                    )

        ui.on("molstar_event", self._handle_viewer_event)
        ui.run_javascript("""
            window.addEventListener('message', function(event) {
                const iframe = document.getElementById('molstar-frame');
                if (iframe && event.source === iframe.contentWindow && event.data.type) {
                    emitEvent('molstar_event', event.data);
                }
            });
        """)

    def _render_header_indicators(self):
        indicators = [
            ("view_in_ar", "Active Template", "template_label", "text-blue-600"),
            ("architecture", "Active Mask", "mask_label", "text-purple-600"),
            ("biotech", "Structure Source", "structure_label", "text-emerald-600"),
        ]
        for icon, label, attr, color in indicators:
            with ui.row().classes("items-center gap-2"):
                ui.icon(icon, size="xs").classes(color)
                ui.label(f"{label}:").classes("text-xs font-medium text-gray-600")
                setattr(self, attr, ui.label("Not set").classes("text-xs font-mono text-gray-400"))

    def _render_template_creation_panel(self):
        self._section_title("1. Template Generation", "settings")

        with ui.row().classes("w-full gap-2 items-start"):
            ui.number(
                "Pixel Size (Å)",
                value=self.pixel_size,
                step=0.1,
                on_change=self._on_pixel_size_changed,
            ).bind_value(self, "pixel_size").props("dense outlined").classes("flex-1")

            self.box_input = (
                ui.number("Box (px)", value=self.box_size, step=32, min=32)
                .bind_value(self, "box_size")
                .props("dense outlined")
                .classes("flex-1")
            )
            self.box_input.on_value_change(self._on_box_size_changed)

            if self.auto_box:
                self.box_input.disable()
            else:
                self.box_input.enable()
            ui.number("LP (Å)", value=self.template_resolution, step=5).bind_value(self, "template_resolution").props(
                "dense outlined"
            ).classes("w-16")

        with ui.row().classes("w-full justify-between items-center px-1"):
            self.auto_box_checkbox = (
                ui.checkbox("Auto-calculate Box", value=self.auto_box)
                .props("dense")
                .classes("text-[11px] text-gray-500")
            )
            self.auto_box_checkbox.on_value_change(self._on_auto_box_toggle)

            self.size_estimate_label = ui.label("—").classes("text-[10px] font-mono text-gray-400")

        self.warning_container = ui.column().classes("w-full gap-1 my-1")
        self._update_size_estimate()
        ui.separator().classes("my-2 opacity-50")

        with ui.column().classes("w-full gap-4"):
            with ui.column().classes("w-full gap-1 bg-gray-50/50 p-2 rounded-lg"):
                ui.label("Ellipsoid Creation").classes("text-[10px] font-bold text-gray-500 uppercase")
                with ui.row().classes("w-full gap-2 items-center"):
                    ui.input(label="x:y:z (Å)", placeholder="550:550:550").bind_value(
                        self, "basic_shape_def"
                    ).props("dense outlined").classes("flex-1").on("update:model-value", self._on_shape_changed)
                    ui.button("Generate", icon="add_box", on_click=self._gen_shape).props(
                        "unelevated dense color=primary"
                    ).classes("px-4")

            with ui.column().classes("w-full gap-2 bg-blue-50/30 p-2 rounded-lg"):
                ui.label("Structure / Map Processing").classes("text-[10px] font-bold text-blue-500 uppercase")
                with ui.row().classes("w-full gap-2"):
                    with ui.column().classes("flex-1 gap-1"):
                        ui.input(label="PDB ID", placeholder="7xyz").bind_value(self, "pdb_input_val").props(
                            "dense outlined"
                        )
                        ui.button("Fetch PDB", icon="cloud_download", on_click=self._fetch_pdb).props(
                            "flat dense color=primary"
                        )
                    with ui.column().classes("flex-1 gap-1"):
                        ui.input(label="EMDB ID", placeholder="30210").bind_value(self, "emdb_input_val").props(
                            "dense outlined"
                        )
                        ui.button("Fetch Map", icon="cloud_download", on_click=self._fetch_emdb).props(
                            "flat dense color=primary"
                        )

                with ui.row().classes("w-full gap-2"):
                    with ui.element("div").classes("flex-1"):
                        self.simulate_btn = (
                            ui.button("Simulate (Pymol/Cistem)", icon="science", on_click=self._simulate_pdb)
                            .props("unelevated dense color=blue-7 outline")
                            .classes("w-full text-[11px]")
                        )
                        self.simulate_tooltip = ui.tooltip("").classes("text-xs")

                    with ui.element("div").classes("flex-1"):
                        self.resample_btn = (
                            ui.button("Resample (Relion)", icon="layers", on_click=self._resample_emdb)
                            .props("unelevated dense color=grey-4 outline")
                            .classes("w-full text-[11px]")
                        )
                        self.resample_tooltip = ui.tooltip("Work in Progress").classes("text-xs")


    def _render_mask_creation_panel(self):
        self._section_title("2. Mask Creation", "architecture")
        self.mask_source_label = ui.label("Select a volume first").classes("text-[11px] text-orange-500 italic mb-1")
        with ui.column().classes("w-full gap-2"):
            ui.select(
                ["flexible_bounds", "otsu", "isodata", "li", "yen"], value=self.threshold_method, label="Method"
            ).props("dense outlined").classes("w-full").on_value_change(self._on_threshold_method_changed)
            ui.number("Threshold", format="%.4f").bind_value(self, "mask_threshold").props("dense outlined").classes(
                "w-full"
            )
            with ui.row().classes("w-full gap-2"):
                ui.number("Ext", suffix="px").bind_value(self, "mask_extend").props("dense outlined").classes("flex-1")
                ui.number("Soft", suffix="px").bind_value(self, "mask_soft_edge").props("dense outlined").classes(
                    "flex-1"
                )
                ui.number("LP", suffix="Å").bind_value(self, "mask_lowpass").props("dense outlined").classes("flex-1")
            self.mask_btn = (
                ui.button("Create Mask", icon="auto_fix_high", on_click=self._create_mask)
                .bind_enabled_from(self, "masking_active", backward=lambda x: not x)
                .props("unelevated dense color=secondary")
                .classes("w-full mt-2")
            )

    def _render_logs_panel(self):
        self._section_title("Activity Log", "terminal")
        self.log_container = ui.column().classes("w-full gap-1 flex-1 overflow-y-auto")
        self.busy_overlay = ui.element('div').classes(
            "absolute inset-0 bg-white/70 backdrop-blur-sm flex items-center justify-center z-50"
        ).style("display:none;")
        with self.busy_overlay:
            with ui.card().classes("p-6 items-center"):
                ui.spinner(size="lg")
                ui.label("Running simulation…").classes("text-sm text-gray-700 mt-2")


    def _section_title(self, title: str, icon: str):
        with ui.row().classes("items-center gap-2 mb-2"):
            ui.icon(icon, size="16px").classes("text-gray-400")
            ui.label(title).classes("text-xs font-bold text-gray-700 uppercase tracking-wide")

    def _render_tray_header(self, title: str, icon: str):
        with ui.row().classes("items-center gap-2 mb-2"):
            ui.icon(icon, size="14px").classes("text-gray-400")
            ui.label(title).classes("text-[10px] font-bold text-gray-500 uppercase tracking-widest")

    async def refresh_files(self):
        if not self.file_list_container:
            return
        self.file_list_container.clear()
        files = await self.backend.template_service.list_template_files_async(self.output_folder)
        params = self._get_tm_params()
        cur_t, cur_m = (params.template_path, params.mask_path) if params else ("", "")
        with self.file_list_container:
            for f_path in files:
                self._render_local_row(f_path, cur_t, cur_m)
        self._update_selection_labels()

    def _render_local_row(self, path, cur_t, cur_m):
        fname = os.path.basename(path)
        is_t, is_m, is_s = path == cur_t, path == cur_m, path == self.structure_path
        bg = (
            "bg-blue-50/50"
            if is_t
            else ("bg-purple-50/50" if is_m else ("bg-emerald-50/50" if is_s else "hover:bg-gray-50"))
        )

        with ui.row().classes(f"w-full items-center gap-1 px-3 py-1 rounded {bg} group"):
            with (
                ui.row()
                .classes("flex-1 items-center gap-2 min-w-0 cursor-pointer")
                .on("click", lambda: self._on_local_click(path))
            ):
                ui.icon("insert_drive_file", size="14px").classes("text-gray-400")
                ui.label(fname).classes("text-[11px] font-mono text-gray-700 truncate")

            with ui.row().classes("gap-0 shrink-0"):

                ui.button(icon="biotech", on_click=lambda p=path: self._toggle_structure(p)).props(
                    f"flat round dense size=sm color={'emerald' if is_s else 'grey'}"
                )
                if path.lower().endswith((".mrc", ".map")):

                    ui.button(icon="view_in_ar", on_click=lambda p=path: self._toggle_template(p)).props(
                        f"flat round dense size=sm color={'blue' if is_t else 'grey'}"
                    )

                    ui.button(icon="architecture", on_click=lambda p=path: self._toggle_mask(p)).props(
                        f"flat round dense size=sm color={'purple' if is_m else 'grey'}"
                    )

                ui.button(icon="delete", on_click=lambda p=path: self._delete(p))

    def _update_session_tray(self):
        if not self.session_list_container:
            return
        current_item_ids = {item.get("id") for item in self.loaded_items}
        for item_id in list(self.session_item_containers.keys()):
            if item_id not in current_item_ids:
                refs = self.session_item_containers[item_id]
                self.session_list_container.remove(refs["container"])
                del self.session_item_containers[item_id]
        for item in self.loaded_items:
            item_id = item.get("id", "unknown")
            if item_id in self.session_item_containers:
                self._update_session_item(item_id, item)
            else:
                self._create_session_item(item)

    def _create_session_item(self, item):
        item_id = item.get("id", "unknown")
        item_type = item.get("type", "unknown")
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        color_hex = f"#{color:06x}" if isinstance(color, int) else "#CCCCCC"

        with self.session_list_container:
            container = ui.column().classes("w-full gap-1 p-2 border border-gray-200 rounded bg-white shadow-sm")
            with container:
                with ui.row().classes("w-full items-center gap-2"):
                    color_btn = (
                        ui.button(icon="palette")
                        .props("flat round dense size=xs")
                        .classes("shrink-0")
                        .style(f"color: {color_hex}")
                    )
                    with ui.menu().props("auto-close") as color_menu:
                        with ui.grid(columns=6).classes("gap-1 p-2"):
                            for palette_color in COLOR_PALETTE:
                                palette_hex = f"#{palette_color:06x}"
                                ui.button().props("flat dense").style(
                                    f"background-color: {palette_hex}; width: 24px; height: 24px; min-width: 24px;"
                                ).on("click", lambda c=palette_color, iid=item_id: self._change_item_color(iid, c))
                    color_btn.on("click", color_menu.open)
                    ui.label(item_id).classes("text-[10px] font-bold text-gray-700 truncate flex-1")
                    vis_btn = ui.button(
                        icon="visibility" if visible else "visibility_off",
                        on_click=lambda iid=item_id: self._toggle_visibility_from_ui(iid),
                    ).props("flat round dense size=xs")
                    del_btn = ui.button(
                        icon="delete", on_click=lambda iid=item_id: self._delete_viewer_item(iid)
                    ).props("flat round dense size=xs color=red")

                iso_row, iso_slider, iso_label = None, None, None
                if item_type == "map":
                    iso_value = item.get("isoValue", 1.5)
                    is_inv = item.get("isInverted", False)
                    stats = item.get("stats", {})
                    abs_val = stats.get("mean", 0) + (iso_value * stats.get("sigma", 1))
                    with ui.row().classes("w-full items-center gap-2 mt-1 pt-1 border-t border-gray-100") as iso_row:
                        ui.label("ISO:").classes("text-[9px] text-gray-500 shrink-0")
                        iso_slider = (
                            ui.slider(min=0.5, max=5.0, step=0.1, value=abs(iso_value)).props("dense").classes("flex-1")
                        )
                        iso_slider.on(
                            "change", lambda e, iid=item_id, inv=is_inv: self._change_iso_value(iid, e.args, inv)
                        )
                        iso_label = ui.label(f"{iso_value:.1f}σ ({abs_val:.3f})").classes(
                            "text-[9px] font-mono text-gray-600 shrink-0 w-24"
                        )

            self.session_item_containers[item_id] = {
                "container": container,
                "vis_btn": vis_btn,
                "color_btn": color_btn,
                "iso_label": iso_label,
                "iso_slider": iso_slider,
            }

    def _update_session_item(self, item_id, item):
        refs = self.session_item_containers.get(item_id)
        if not refs:
            return
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        refs["vis_btn"].props(f"icon={'visibility' if visible else 'visibility_off'}")
        refs["color_btn"].style(f"color: #{color:06x}")
        if item.get("type") == "map" and refs["iso_label"]:
            iso_value = item.get("isoValue", 1.5)
            stats = item.get("stats", {})
            abs_val = stats.get("mean", 0) + (iso_value * stats.get("sigma", 1))
            refs["iso_label"].set_text(f"{iso_value:.1f}σ ({abs_val:.3f})")
            refs["iso_slider"].value = abs(iso_value)

    def _toggle_visibility_from_ui(self, iid):
        current_item = next((item for item in self.loaded_items if item.get("id") == iid), None)
        if current_item:
            self._toggle_visibility(iid, not current_item.get("visible", True))

    async def _toggle_template(self, path):
        p = self._get_tm_params()
        if p:
            p.template_path = "" if p.template_path == path else path
            if p.template_path:
                potential_mask = p.template_path.replace("_white.mrc", "_mask.mrc").replace("_black.mrc", "_mask.mrc")
                if os.path.exists(potential_mask):
                    p.mask_path = potential_mask
                    self._log(f"Auto-selected matching mask: {os.path.basename(potential_mask)}")
        await get_state_service().save_project()
        await self.refresh_files()

    async def _toggle_mask(self, path):
        p = self._get_tm_params()
        if p:
            p.mask_path = "" if p.mask_path == path else path
        await get_state_service().save_project()
        await self.refresh_files()

    async def _toggle_structure(self, path):
        self.structure_path = "" if self.structure_path == path else path
        if self.structure_path:
            ext = Path(path).suffix.lower()
            if ext in [".pdb", ".cif"]:
                self.pdb_input_val = os.path.basename(path).split(".")[0]
            await self._on_local_click(path)
        await self.refresh_files()

    async def _fetch_pdb(self):
        if not self.pdb_input_val:
            return
        self._log(f"Fetching PDB: {self.pdb_input_val}")
        res = await self.backend.template_service.fetch_pdb_async(
            self.pdb_input_val.strip().lower(), self.output_folder
        )
        if res["success"]:
            self._log(f"Fetched PDB: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _fetch_emdb(self):
        if not self.emdb_input_val:
            return
        self._log(f"Fetching EMDB: {self.emdb_input_val}")
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val.strip(), self.output_folder)
        if res["success"]:
            self._log(f"Fetched EMDB: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _gen_shape(self):
        if self.auto_box:
            self._recalculate_auto_box()
        self._log(f"Generating ellipsoid: {self.basic_shape_def}...")
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size), self.template_resolution
        )
        if res["success"]:
            self._log(f"Generated shape pair: {os.path.basename(res['path_white'])}")
            await self.refresh_files()
        else:
            self._log(f"Generation failed: {res.get('error')}")

    async def _create_mask(self):
        template_path = self._get_tm_params().template_path if self._get_tm_params() else None
        if not template_path:
            return
        self.masking_active = True
        n = ui.notification("Relion: Creating mask...", type="ongoing", spinner=True, timeout=None)
        try:
            # Derive the base name to look for a seed file
            base = Path(template_path).stem.replace("_white", "").replace("_black", "")
            prefix = base.split("_box")[0]  # "ellipsoid_550_550_550_apix11.80"
            seed_candidates = [
                os.path.join(self.output_folder, f)
                for f in os.listdir(self.output_folder)
                if f.endswith("_seed.mrc") and prefix in f
            ]

            if seed_candidates and os.path.exists(seed_candidates[0]):
                # Use the seed with binary-appropriate defaults
                input_vol = seed_candidates[0]
                threshold = 0.5
                extend = 5
                soft = 5
                self._log(f"Using mask seed: {os.path.basename(input_vol)} (threshold={threshold})")
            else:
                # Fallback for PDB/EMDB templates: use white template with user params
                input_vol = (
                    template_path.replace("_black.mrc", "_white.mrc") 
                    if "_black.mrc" in template_path else template_path
                )
                threshold = self.mask_threshold
                extend = self.mask_extend
                soft = self.mask_soft_edge
                self._log(f"No seed found, using white template with threshold={threshold}")

            output = os.path.join(
                self.output_folder, 
                f"{Path(template_path).stem.replace('_white', '').replace('_black', '')}_mask.mrc"
            )
            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, threshold, extend, soft, self.mask_lowpass
            )
            if res["success"]:
                self._log(f"Mask created: {os.path.basename(output)}")
                p = self._get_tm_params()
                if p:
                    p.mask_path = output
                await get_state_service().save_project()
                await self.refresh_files()
        finally:
            n.dismiss()
            self.masking_active = False

    def _get_tm_params(self):
        return get_project_state().jobs.get(JobType.TEMPLATE_MATCH_PYTOM)

    def _log(self, msg: str):
        if self.log_container:
            with self.log_container:
                ui.label(f"• {msg}").classes("text-[10px] text-gray-600 font-mono leading-tight")
        if self.client and self.log_container:
            self.client.run_javascript(
                f"const el=document.getElementById('c{self.log_container.id}');"
                f"if (el) el.scrollTop = el.scrollHeight;"
            )


    def _on_pixel_size_changed(self, e=None):
        # tolerate transient None during editing
        val = getattr(e, "value", None) if e is not None else self.pixel_size
        if val is None:
            self._update_size_estimate()
            return

        try:
            self.pixel_size = float(val)
        except (TypeError, ValueError):
            return

        self._update_size_estimate()
        self._recalculate_auto_box()

    def _on_box_size_changed(self, e=None):
        # NiceGUI number can emit None (cleared input / invalid intermediate state)
        val = None
        if e is not None:
            val = getattr(e, "value", None)

        if val is None:
            # Keep UI responsive; don't compute with None
            if self.size_estimate_label:
                self.size_estimate_label.set_text("—")
                self.size_estimate_label.classes(replace="text-[10px] font-mono text-gray-400")
            return

        try:
            self.box_size = int(val)
        except (TypeError, ValueError):
            return

        self._update_size_estimate()


    def _on_shape_changed(self):
        self._recalculate_auto_box()

    def _on_auto_box_toggle(self, e):
        self.auto_box = e.value

        if not self.box_input:
            return

        if self.auto_box:
            self.box_input.disable()
            self._recalculate_auto_box()
        else:
            self.box_input.enable()


    def _recalculate_auto_box(self):
        if not self.auto_box:
            return
        try:
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            new_box = int(((max(dims) / self.pixel_size) * 1.3 + 31) // 32) * 32
            if new_box != self._last_logged_box:
                self.box_size, self._last_logged_box = max(new_box, 96), new_box
                self._update_size_estimate()
        except:
            pass

    def _update_size_estimate(self):
        if not self.size_estimate_label:
            return

        try:
            if self.box_size is None:
                raise ValueError("box_size is None")
            box = int(self.box_size)
            if box <= 0:
                raise ValueError("box_size <= 0")
        except (TypeError, ValueError):
            self.size_estimate_label.set_text("—")
            self.size_estimate_label.classes(replace="text-[10px] font-mono text-gray-400")
            return

        est = round((box ** 3 * 4) / (1024 * 1024), 1)
        self.size_estimate_label.set_text(f"~{est} MB")
        self.size_estimate_label.classes(
            replace=f"text-[10px] font-mono {'text-red-600' if est > 100 else 'text-green-600'}"
        )


    def _post_to_viewer(self, action: str, **kwargs):
        if not self.client:
            return
        payload = {"action": action, **kwargs}
        self.client.run_javascript(
            "const f = document.getElementById('molstar-frame');"
            "if (f && f.contentWindow) f.contentWindow.postMessage(%s, '*');"
            % json.dumps(payload)
        )


    def _handle_viewer_event(self, e):
        event_type = e.args.get("type")
        if event_type == "ready":
            self.viewer_ready = True
            self._post_to_viewer("getItems")
        elif event_type == "itemsChanged":
            self.loaded_items = e.args.get("items", [])
            self._update_session_tray()
        elif event_type == "error":
            self._log(f"Viewer Error: {e.args.get('message')}")

    async def _test_iframe_loaded(self):
        await asyncio.sleep(1)
        self._post_to_viewer("getItems")

    async def _delete(self, path):
        await self.backend.template_service.delete_file_async(path)
        await self.refresh_files()

    def _change_item_color(self, iid, color):
        self._post_to_viewer("setColor", itemId=iid, color=color)

    def _toggle_visibility(self, iid, vis):
        self._post_to_viewer("setVisibility", itemId=iid, visible=vis)

    def _change_iso_value(self, iid, val, inv):
        self._post_to_viewer("setIsoValue", itemId=iid, isoValue=-abs(val) if inv else val)

    def _delete_viewer_item(self, iid):
        self._post_to_viewer("deleteItem", itemId=iid)

    async def _on_threshold_method_changed(self, e):
        self.threshold_method = e.value
        t_path = self._get_tm_params().template_path if self._get_tm_params() else None
        if t_path and os.path.exists(t_path):
            thresholds = await self.backend.template_service.calculate_thresholds_async(t_path, self.mask_lowpass)
            if e.value in thresholds:
                self.mask_threshold = round(thresholds[e.value], 4)

    def _update_selection_labels(self):
        p = self._get_tm_params()
        t, m = (p.template_path, p.mask_path) if p else ("", "")
        
        # Update standard labels
        if self.template_label:
            self.template_label.set_text(os.path.basename(t) or "Not set")
        if self.mask_label:
            self.mask_label.set_text(os.path.basename(m) or "Not set")
        if self.structure_label:
            self.structure_label.set_text(os.path.basename(self.structure_path) or "Not set")

        # --- Simulate Button Logic ---
        is_structure = self.structure_path.lower().endswith((".pdb", ".cif"))
        if self.simulate_btn:
            self.simulate_btn.set_enabled(is_structure)
            
            # Update tooltip based on state
            if is_structure:
                msg = f"The same {self.pixel_size}Å / {int(self.box_size)}px / {self.template_resolution}Å params will be used."
                self.simulate_tooltip.set_text(msg)
            else:
                self.simulate_tooltip.set_text("Select a PDB/CIF structure in the tray first")

        # --- Resample Button Logic ---
        is_map = self.structure_path.lower().endswith((".mrc", ".map"))
        if self.resample_btn:
            # Keep it disabled as per your "Work in Progress" requirement, 
            # or enable it if you are ready to use the Relion resample logic:
            # self.resample_btn.set_enabled(is_map) 
            pass
