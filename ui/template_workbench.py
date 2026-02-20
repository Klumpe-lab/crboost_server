from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import json
import asyncio
from pathlib import Path
import mrcfile
from nicegui import context

from services.project_state import JobType, get_project_state, get_state_service

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
    def __init__(self, backend, project_path: str):
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        os.makedirs(self.output_folder, exist_ok=True)

        self.project_raw_apix = None
        self.project_binning = None
        self.project_tomo_apix = None

        self.pixel_size = 10.0
        self.box_size = 96
        self.auto_box = True

        self.template_resolution = None
        self.apply_lowpass = False

        self.basic_shape_def = "550:550:550"
        self.pdb_input_val = ""
        self.emdb_input_val = ""
        self.structure_path = ""

        self.mask_threshold = 0.5
        self.mask_extend = 5
        self.mask_soft_edge = 5
        self.mask_lowpass = 20
        self.threshold_method = "flexible_bounds"

        self.masking_active = False
        self.viewer_ready = False
        self.loaded_items = []

        # UI refs
        self.file_list_container = None
        self.session_list_container = None
        self.log_container = None
        self.mask_btn = None
        self.mask_source_label = None
        self.mask_method_row = None
        self.box_input = None
        self.lp_input = None
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

        self.auto_infer_seed = True
        self._last_mask_template = ""   # tracks which template defaults were last applied for
        self._last_was_seed = None      # tracks last seed status to detect seed<->non-seed transitions

        self._load_project_parameters()
        # Resolve auto-box immediately so the UI opens with the correct value
        self._recalculate_auto_box()

        self.client = None
        self._render()
        self.client = context.client

        ui.timer(0, self.refresh_files, once=True)
        ui.timer(0, self._test_iframe_loaded, once=True)

    # ------------------------------------------------------------------
    # PROJECT PARAMETER LOADING
    # ------------------------------------------------------------------
    def _save_workbench_params(self):
        """Persist current workbench UI params into the TM job model and save to disk."""
        tm_params = self._get_tm_params()
        if tm_params is None or not hasattr(tm_params, "workbench"):
            return
        wb = tm_params.workbench
        wb.pixel_size = self.pixel_size
        wb.box_size = self.box_size
        wb.auto_box = self.auto_box
        wb.apply_lowpass = self.apply_lowpass
        wb.template_resolution = self.template_resolution
        wb.auto_infer_seed = self.auto_infer_seed
        wb.basic_shape_def = self.basic_shape_def
        asyncio.create_task(get_state_service().save_project())

    def _load_project_parameters(self):
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

            # Hydrate workbench params from persisted state
            tm_params = self._get_tm_params()
            if tm_params and hasattr(tm_params, "workbench"):
                wb = tm_params.workbench
                if wb.pixel_size > 0:
                    self.pixel_size = wb.pixel_size
                elif self.project_tomo_apix:
                    self.pixel_size = self.project_tomo_apix
                elif self.project_raw_apix:
                    self.pixel_size = self.project_raw_apix
                # other fields always load (they don't have project-derived defaults)
                self.auto_infer_seed = wb.auto_infer_seed
                self.box_size = wb.box_size
                self.auto_box = wb.auto_box
                self.apply_lowpass = wb.apply_lowpass
                self.template_resolution = wb.template_resolution
                self.basic_shape_def = wb.basic_shape_def
            elif self.project_tomo_apix:
                self.pixel_size = self.project_tomo_apix
            elif self.project_raw_apix:
                self.pixel_size = self.project_raw_apix

        except Exception as e:
            print(f"[TEMPLATE_WORKBENCH] Load project params error: {e}")

    # ------------------------------------------------------------------
    # RENDER
    # ------------------------------------------------------------------

    def _render(self):
        with ui.column().classes("w-full gap-0"):
            # ---- top strip: status indicators ----
            with ui.row().classes("w-full gap-4 px-3 py-1 bg-gray-50 border-b items-center"):
                self.validation_dot = ui.icon("fiber_manual_record", size="12px").classes("text-gray-300")
                for icon_name, label, attr, color in [
                    ("view_in_ar", "Template", "template_label", "text-blue-500"),
                    ("architecture", "Mask", "mask_label", "text-purple-500"),
                    ("biotech", "Source", "structure_label", "text-emerald-500"),
                ]:
                    with ui.row().classes("items-center gap-1"):
                        ui.icon(icon_name, size="12px").classes(color)
                        ui.label(f"{label}:").classes("text-[10px] font-medium text-gray-500")
                        setattr(
                            self,
                            attr,
                            ui.label("—").classes("text-[10px] font-mono text-gray-400 max-w-[160px] truncate"),
                        )

            # ---- main control row ----
            with ui.row().classes("w-full gap-0 border-b").style("height: 400px; overflow: hidden;"):
                with ui.column().classes("w-[38%] p-3 gap-2 border-r overflow-y-auto h-full"):
                    self._render_template_panel()

                with ui.column().classes("w-[28%] p-3 gap-2 border-r overflow-y-auto h-full"):
                    self._render_mask_panel()

                with ui.column().classes("flex-1 p-3 gap-1 bg-gray-50/40 overflow-hidden h-full"):
                    self._render_log_panel()

            # ---- bottom row: file browser | session tray | viewer ----
            with ui.row().classes("w-full gap-0").style("height: 420px; overflow: hidden;"):
                # File browser
                with ui.column().classes("w-[32%] p-3 border-r bg-gray-50/10 flex flex-col h-full gap-1"):
                    with ui.row().classes("items-center gap-1 mb-1 shrink-0"):
                        ui.icon("folder", size="13px").classes("text-gray-400")
                        ui.label("Available Locally").classes(
                            "text-[10px] font-bold text-gray-500 uppercase tracking-widest"
                        )
                    self.file_list_container = ui.column().classes("w-full gap-0.5 overflow-y-auto flex-1")

                    ui.separator().classes("my-1 shrink-0")
                    with (
                        ui.card()
                        .tight()
                        .classes("w-full p-2 bg-blue-50/40 border border-blue-100 shadow-none shrink-0")
                    ):
                        with ui.row().classes("items-center gap-1 mb-1"):
                            ui.icon("analytics", size="12px").classes("text-blue-400")
                            ui.label("Header").classes("text-[9px] font-bold uppercase text-blue-500")
                        self.meta_label = ui.markdown("Select file…").classes(
                            "text-[9px] font-mono leading-tight text-gray-600"
                        )

                # Session tray
                with ui.column().classes("w-[22%] p-3 border-r bg-white h-full flex flex-col"):
                    with ui.row().classes("items-center justify-between mb-2 shrink-0"):
                        with ui.row().classes("items-center gap-1"):
                            ui.icon("layers", size="12px").classes("text-gray-400")
                            ui.label("In Viewer").classes(
                                "text-[9px] font-bold text-gray-500 uppercase tracking-widest"
                            )
                        ui.button(icon="delete_sweep", on_click=lambda: self._post_to_viewer("clear")).props(
                            "flat round dense size=xs color=red"
                        )
                    self.session_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")

                # Molstar viewer
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

    # ------------------------------------------------------------------
    # TEMPLATE PANEL
    # ------------------------------------------------------------------

    def _render_template_panel(self):
        with ui.row().classes("items-center gap-1 mb-1"):
            ui.icon("settings", size="14px").classes("text-gray-400")
            ui.label("Template Generation").classes("text-[10px] font-bold text-gray-700 uppercase tracking-wide")

        # Pixel size / box / LP on one row
        with ui.row().classes("w-full gap-2 items-end"):
            ui.number(
                "Pixel Size (Å)", value=self.pixel_size, step=0.1, on_change=self._on_pixel_size_changed
            ).bind_value(self, "pixel_size").props("dense outlined").classes("flex-1")

            self.box_input = (
                ui.number("Box (px)", value=self.box_size, step=32, min=32)
                .bind_value(self, "box_size")
                .props("dense outlined")
                .classes("w-20")
            )
            self.box_input.on_value_change(self._on_box_size_changed)

            self.lp_input = (
                ui.number("LP (Å)", value=60, step=5)
                .bind_value(self, "template_resolution")
                .props("dense outlined")
                .classes("w-16")
            )
            self.lp_input.on_value_change(lambda _: self._save_workbench_params())
            self.lp_input.disable()

        # Auto-box + lowpass toggles on one compact row
        with ui.row().classes("w-full items-center gap-3 px-0.5"):
            self.auto_box_checkbox = (
                ui.checkbox("Auto box", value=self.auto_box).props("dense").classes("text-[10px] text-gray-500")
            )
            self.auto_box_checkbox.on_value_change(self._on_auto_box_toggle)
            if self.auto_box:
                self.box_input.disable()

            lp_checkbox = (
                ui.checkbox("Lowpass filter", value=self.apply_lowpass)
                .props("dense")
                .classes("text-[10px] text-gray-500")
            )

            def _on_lp_toggle(e):
                self.apply_lowpass = e.value
                if not e.value:
                    self.template_resolution = None
                    self.lp_input.disable()
                else:
                    self.template_resolution = 60.0
                    self.lp_input.enable()
                self._save_workbench_params()   # <--

            lp_checkbox.on_value_change(_on_lp_toggle)

            self.size_estimate_label = ui.label("—").classes("text-[9px] font-mono text-gray-400 ml-auto")

        self._update_size_estimate()
        self.warning_container = ui.column().classes("w-full gap-1")

        ui.separator().classes("my-1 opacity-40")

        # Ellipsoid creation
        with ui.column().classes("w-full gap-1 bg-gray-50/60 px-2 py-2 rounded"):
            ui.label("Ellipsoid").classes("text-[9px] font-bold text-gray-500 uppercase")
            with ui.row().classes("w-full gap-2 items-center"):
                ui.input(label="x:y:z (Å)", placeholder="550:550:550").bind_value(self, "basic_shape_def").props(
                    "dense outlined"
                ).classes("flex-1").on("update:model-value", self._on_shape_changed)
                ui.button("Generate", icon="add_box", on_click=self._gen_shape).props(
                    "unelevated dense color=primary size=sm"
                )

        # Structure / map processing
        with ui.column().classes("w-full gap-1 bg-blue-50/30 px-2 py-2 rounded"):
            ui.label("Structure / Map").classes("text-[9px] font-bold text-blue-500 uppercase")
            with ui.row().classes("w-full gap-2"):
                with ui.column().classes("flex-1 gap-1"):
                    ui.input(label="PDB ID", placeholder="7xyz").bind_value(self, "pdb_input_val").props(
                        "dense outlined"
                    )
                    ui.button("Fetch PDB", icon="cloud_download", on_click=self._fetch_pdb).props(
                        "flat dense color=primary size=sm"
                    )
                with ui.column().classes("flex-1 gap-1"):
                    ui.input(label="EMDB ID", placeholder="30210").bind_value(self, "emdb_input_val").props(
                        "dense outlined"
                    )
                    ui.button("Fetch Map", icon="cloud_download", on_click=self._fetch_emdb).props(
                        "flat dense color=primary size=sm"
                    )

            with ui.row().classes("w-full gap-2"):
                with ui.element("div").classes("flex-1"):
                    self.simulate_btn = (
                        ui.button("Simulate (Pymol/Cistem)", icon="science", on_click=self._simulate_pdb)
                        .props("unelevated dense color=blue-7 outline size=sm")
                        .classes("w-full text-[10px]")
                    )
                    self.simulate_tooltip = ui.tooltip("").classes("text-xs")
                with ui.element("div").classes("flex-1"):
                    self.resample_btn = (
                        ui.button("Resample (Relion)", icon="layers", on_click=self._resample_emdb)
                        .props("unelevated dense color=grey-4 outline size=sm")
                        .classes("w-full text-[10px]")
                    )
                    ui.tooltip("Work in Progress").classes("text-xs")

    # ------------------------------------------------------------------
    # MASK PANEL
    # ------------------------------------------------------------------

    def _render_mask_panel(self):
        with ui.row().classes("items-center gap-1 mb-1"):
                ui.icon("architecture", size="14px").classes("text-gray-400")
                ui.label("Mask Creation").classes("text-[10px] font-bold text-gray-700 uppercase tracking-wide")

        with ui.row().classes("w-full items-center gap-2 mb-1"):
            ui.checkbox("Auto-infer seed", value=self.auto_infer_seed).props("dense").classes(
                "text-[10px] text-gray-500"
            ).bind_value(self, "auto_infer_seed").on_value_change(
                lambda _: self._save_workbench_params()
            )
            ui.label("(uses binary seed for masking when found)").classes("text-[8px] text-gray-400 italic")


        self.mask_source_label = ui.label("Select a template first").classes("text-[10px] text-orange-500 italic")

        with ui.column().classes("w-full gap-2"):
            # Method selector -- hidden for binary seeds
            with ui.column().classes("w-full gap-1") as self.mask_method_row:
                ui.label("Threshold method (non-seed maps only)").classes("text-[9px] text-gray-400")
                ui.select(
                    ["flexible_bounds", "otsu", "isodata", "li", "yen"], value=self.threshold_method, label="Method"
                ).props("dense outlined").classes("w-full").on_value_change(self._on_threshold_method_changed)

            ui.number("Threshold", format="%.4f").bind_value(self, "mask_threshold").props("dense outlined").classes(
                "w-full"
            )

            ui.label("Ext: grow outward (px)  |  Soft: rolloff width (px)  |  LP: lowpass (Å)").classes(
                "text-[8px] text-gray-400 leading-tight"
            )

            with ui.row().classes("w-full gap-1"):
                ui.number("Ext", suffix="px").bind_value(self, "mask_extend").props("dense outlined").classes("flex-1")
                ui.number("Soft", suffix="px").bind_value(self, "mask_soft_edge").props("dense outlined").classes(
                    "flex-1"
                )
                ui.number("LP", suffix="Å").bind_value(self, "mask_lowpass").props("dense outlined").classes("flex-1")

            with ui.column().classes("w-full gap-0.5 bg-gray-50 rounded px-2 py-1.5"):
                ui.label("Defaults").classes("text-[8px] font-bold text-gray-500 uppercase")
                ui.label("Seed:   threshold=0.5 / ext=5 / soft=5").classes("text-[8px] font-mono text-gray-500")
                ui.label("Map:    threshold=auto / ext=3 / soft=6").classes("text-[8px] font-mono text-gray-500")

            self.mask_btn = (
                ui.button("Create Mask", icon="auto_fix_high", on_click=self._create_mask)
                .bind_enabled_from(self, "masking_active", backward=lambda x: not x)
                .props("unelevated dense color=secondary size=sm")
                .classes("w-full mt-1")
            )

    # ------------------------------------------------------------------
    # LOG PANEL
    # ------------------------------------------------------------------

    def _render_log_panel(self):
        with ui.row().classes("items-center gap-1 mb-1 shrink-0"):
            ui.icon("terminal", size="14px").classes("text-gray-400")
            ui.label("Activity Log").classes("text-[10px] font-bold text-gray-700 uppercase tracking-wide")
        self.log_container = ui.column().classes("w-full gap-0.5 flex-1 overflow-y-auto")

    # ------------------------------------------------------------------
    # FILE ROW
    # ------------------------------------------------------------------

    async def refresh_files(self):
        if not self.file_list_container:
            return
        self.file_list_container.clear()
        files = await self.backend.template_service.list_template_files_async(self.output_folder)
        params = self._get_tm_params()
        cur_t, cur_m = (params.template_path, params.mask_path) if params else ("", "")
        with self.file_list_container:
            for f_path in files:
                self._render_file_row(f_path, cur_t, cur_m)
        self._update_selection_labels()

    def _render_file_row(self, path, cur_t, cur_m):
        fname = os.path.basename(path)
        is_seed = fname.endswith("_seed.mrc")
        is_t = path == cur_t
        is_m = path == cur_m
        is_s = path == self.structure_path
        bg = "bg-blue-50" if is_t else ("bg-purple-50" if is_m else ("bg-emerald-50" if is_s else "hover:bg-gray-50"))

        with ui.row().classes(f"w-full items-center gap-0.5 px-1 py-0.5 rounded {bg} group"):
            with (
                ui.row()
                .classes("flex-1 items-center gap-1 min-w-0 cursor-pointer")
                .on("click", lambda p=path: self._on_local_click(p))
            ):
                ui.icon("insert_drive_file", size="12px").classes("text-gray-300")
                ui.label(fname).classes("text-[10px] font-mono text-gray-700 truncate")
                if is_seed:
                    ui.label("seed").classes("text-[8px] font-bold text-orange-400 uppercase ml-1 shrink-0")

            with ui.row().classes("gap-0 shrink-0 opacity-60 group-hover:opacity-100"):
                ui.button(icon="biotech", on_click=lambda p=path: self._toggle_structure(p)).props(
                    f"flat round dense size=xs color={'emerald' if is_s else 'grey'}"
                )
                if path.lower().endswith((".mrc", ".map")) and not is_seed:
                    ui.button(icon="view_in_ar", on_click=lambda p=path: self._toggle_template(p)).props(
                        f"flat round dense size=xs color={'blue' if is_t else 'grey'}"
                    )
                    ui.button(icon="architecture", on_click=lambda p=path: self._toggle_mask(p)).props(
                        f"flat round dense size=xs color={'purple' if is_m else 'grey'}"
                    )
                ui.button(icon="close", on_click=lambda p=path: self._delete(p)).props(
                    "flat round dense size=xs color=grey"
                )

    # ------------------------------------------------------------------
    # SESSION TRAY (now horizontal cards below viewer)
    # ------------------------------------------------------------------

    def _update_session_tray(self):
        if not self.session_list_container:
            return
        current_ids = {item.get("id") for item in self.loaded_items}
        for iid in list(self.session_item_containers.keys()):
            if iid not in current_ids:
                refs = self.session_item_containers[iid]
                self.session_list_container.remove(refs["container"])
                del self.session_item_containers[iid]
        for item in self.loaded_items:
            iid = item.get("id", "unknown")
            if iid in self.session_item_containers:
                self._update_session_item(iid, item)
            else:
                self._create_session_item(item)

    def _create_session_item(self, item):
        iid = item.get("id", "unknown")
        item_type = item.get("type", "unknown")
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        color_hex = f"#{color:06x}" if isinstance(color, int) else "#CCCCCC"

        with self.session_list_container:
            container = ui.card().tight().classes("p-1.5 bg-white border border-gray-200 shadow-none shrink-0")
            with container:
                with ui.row().classes("items-center gap-1"):
                    color_btn = ui.button(icon="circle").props("flat round dense size=xs").style(f"color: {color_hex}")
                    with ui.menu().props("auto-close") as color_menu:
                        with ui.grid(columns=6).classes("gap-0.5 p-1"):
                            for pc in COLOR_PALETTE:
                                ui.button().props("flat dense").style(
                                    f"background:{f'#{pc:06x}'};width:18px;height:18px;min-width:18px;"
                                ).on("click", lambda c=pc, i=iid: self._change_item_color(i, c))
                    color_btn.on("click", color_menu.open)
                    ui.label(iid).classes("text-[9px] font-mono text-gray-700 max-w-[120px] truncate")
                    vis_btn = ui.button(
                        icon="visibility" if visible else "visibility_off",
                        on_click=lambda i=iid: self._toggle_visibility_from_ui(i),
                    ).props("flat round dense size=xs")
                    ui.button(icon="close", on_click=lambda i=iid: self._delete_viewer_item(i)).props(
                        "flat round dense size=xs color=red"
                    )

                iso_slider, iso_label = None, None
                if item_type == "map":
                    iso_value = item.get("isoValue", 1.5)
                    is_inv = item.get("isInverted", False)
                    stats = item.get("stats", {})
                    abs_val = stats.get("mean", 0) + iso_value * stats.get("sigma", 1)
                    with ui.row().classes("items-center gap-1 mt-0.5"):
                        ui.label("ISO:").classes("text-[8px] text-gray-400 shrink-0")
                        iso_slider = (
                            ui.slider(min=0.5, max=5.0, step=0.1, value=abs(iso_value)).props("dense").classes("w-20")
                        )
                        iso_slider.on("change", lambda e, i=iid, inv=is_inv: self._change_iso_value(i, e.args, inv))
                        iso_label = ui.label(f"{iso_value:.1f}σ").classes("text-[8px] font-mono text-gray-500 shrink-0")

        self.session_item_containers[iid] = {
            "container": container,
            "vis_btn": vis_btn,
            "color_btn": color_btn,
            "iso_label": iso_label,
            "iso_slider": iso_slider,
        }

    def _update_session_item(self, iid, item):
        refs = self.session_item_containers.get(iid)
        if not refs:
            return
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        refs["vis_btn"].props(f"icon={'visibility' if visible else 'visibility_off'}")
        refs["color_btn"].style(f"color: #{color:06x}")
        if item.get("type") == "map" and refs["iso_label"]:
            iso_value = item.get("isoValue", 1.5)
            refs["iso_label"].set_text(f"{iso_value:.1f}σ")
            if refs["iso_slider"]:
                refs["iso_slider"].value = abs(iso_value)

    # ------------------------------------------------------------------
    # MASK SOURCE PANEL UPDATE
    # ------------------------------------------------------------------

    async def _update_mask_source_panel(self):
        p = self._get_tm_params()
        template_path = p.template_path if p else ""

        if not template_path or not os.path.exists(template_path):
            if self.mask_source_label:
                self.mask_source_label.set_text("Select a template first")
                self.mask_source_label.classes(replace="text-[10px] text-orange-500 italic")
            self._last_mask_template = ""
            self._last_was_seed = None
            return

        base = Path(template_path).stem.replace("_white", "").replace("_black", "")
        prefix = base.split("_box")[0]

        # Resolve seed candidate
        seed_path = None
        if self.auto_infer_seed:
            candidates = [
                os.path.join(self.output_folder, f)
                for f in os.listdir(self.output_folder)
                if f.endswith("_seed.mrc") and prefix in f
            ]
            if candidates and os.path.exists(candidates[0]):
                seed_path = candidates[0]

        template_changed = template_path != self._last_mask_template
        seed_status_changed = (seed_path is not None) != self._last_was_seed
        apply_defaults = template_changed or seed_status_changed

        self._last_mask_template = template_path
        self._last_was_seed = seed_path is not None

        if seed_path:
            seed_name = os.path.basename(seed_path)
            if self.mask_source_label:
                self.mask_source_label.set_text(f"Seed: {seed_name}  |  threshold fixed at 0.5")
                self.mask_source_label.classes(replace="text-[10px] text-green-600 italic")
            if apply_defaults:
                self.mask_threshold = 0.5
                self.mask_extend = 5
                self.mask_soft_edge = 5
            if self.mask_method_row:
                self.mask_method_row.set_visibility(False)
        else:
            white_path = template_path.replace("_black.mrc", "_white.mrc")
            source = white_path if os.path.exists(white_path) else template_path
            if self.mask_source_label:
                self.mask_source_label.set_text(f"Input: {os.path.basename(source)}")
                self.mask_source_label.classes(replace="text-[10px] text-blue-600 italic")
            if self.mask_method_row:
                self.mask_method_row.set_visibility(True)
            if apply_defaults:
                thresholds = await self.backend.template_service.calculate_thresholds_async(source, self.mask_lowpass)
                method = self.threshold_method if self.threshold_method in thresholds else "flexible_bounds"
                self.mask_threshold = round(thresholds[method], 4)
                self._log(f"Threshold ({method}): {self.mask_threshold:.4f}")

    # ------------------------------------------------------------------
    # ACTIONS
    # ------------------------------------------------------------------

    async def _on_local_click(self, path):
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
                info = "MRC read error"
        elif ext in [".pdb", ".cif"]:
            res = await self.backend.pdb_service.get_structure_metadata(path)
            if res.get("success"):
                bb = res["bbox"]
                info = f"**RES:** {res['residues']} | **SYM:** {res['symmetry']}\n\n**BOX:** {bb[0]}x{bb[1]}x{bb[2]}Å"
            else:
                info = f"PDB Error: {res.get('error')}"
        if self.meta_label:
            self.meta_label.set_content(info)

    async def _toggle_template(self, path):
        p = self._get_tm_params()
        if p:
            p.template_path = "" if p.template_path == path else path
            if p.template_path:
                potential_mask = p.template_path.replace("_white.mrc", "_mask.mrc").replace("_black.mrc", "_mask.mrc")
                if os.path.exists(potential_mask):
                    p.mask_path = potential_mask
                    self._log(f"Auto-selected mask: {os.path.basename(potential_mask)}")
        await get_state_service().save_project()
        await self.refresh_files()
        await self._update_mask_source_panel()

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
            self._log(f"Fetched: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _fetch_emdb(self):
        if not self.emdb_input_val:
            return
        self._log(f"Fetching EMDB: {self.emdb_input_val}")
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val.strip(), self.output_folder)
        if res["success"]:
            self._log(f"Fetched: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _gen_shape(self):
        if self.auto_box:
            self._recalculate_auto_box()
        lp_str = f"lp={self.template_resolution}Å" if self.template_resolution else "no lowpass"
        self._log(f"Generating ellipsoid {self.basic_shape_def} @ {self.pixel_size}Å/px, {lp_str}")
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size), self.template_resolution
        )
        if res["success"]:
            self._log(f"Created: {os.path.basename(res['path_white'])}")
            await self.refresh_files()
        else:
            self._log(f"Generation failed: {res.get('error')}")

    async def _simulate_pdb(self):
        if not self.structure_path:
            ui.notify("Select a structure first", type="warning")
            return
        if self.simulate_btn:
            self.simulate_btn.set_enabled(False)
        n = ui.notification("Simulating density…", type="ongoing", spinner=True, timeout=None)
        try:
            self._log(f"Simulating from {os.path.basename(self.structure_path)}…")
            res = await self.backend.pdb_service.simulate_map_from_pdb(
                pdb_path=self.structure_path,
                output_folder=self.output_folder,
                target_apix=self.pixel_size,
                target_box=int(self.box_size),
                resolution=self.template_resolution,
            )
            if res.get("success"):
                self._log(f"Done: {os.path.basename(res['path_black'])}")
                ui.notify("Simulation finished", type="positive")
                await self.refresh_files()
            else:
                self._log(f"Simulation failed: {res.get('error')}")
                ui.notify("Simulation failed (see log)", type="negative", timeout=8000)
        finally:
            n.dismiss()
            self._update_selection_labels()
            if self.simulate_btn:
                self.simulate_btn.set_enabled(True)

    async def _resample_emdb(self):
        if not self.structure_path:
            return
        self._log(f"Resampling: {os.path.basename(self.structure_path)}…")
        res = await self.backend.template_service.process_volume_async(
            self.structure_path, self.output_folder, self.pixel_size, int(self.box_size), self.template_resolution
        )
        if res["success"]:
            self._log(f"Resampled: {os.path.basename(res['path_white'])}")
            await self.refresh_files()
        else:
            self._log(f"Resample error: {res.get('error')}")

    async def _create_mask(self):
        p = self._get_tm_params()
        template_path = p.template_path if p else None
        if not template_path:
            return
        self.masking_active = True
        n = ui.notification("Creating mask…", type="ongoing", spinner=True, timeout=None)
        try:
            base = Path(template_path).stem.replace("_white", "").replace("_black", "")
            prefix = base.split("_box")[0]
            seed_candidates = []
            if self.auto_infer_seed:
                seed_candidates = [
                    os.path.join(self.output_folder, f)
                    for f in os.listdir(self.output_folder)
                    if f.endswith("_seed.mrc") and prefix in f
                ]
            if seed_candidates and os.path.exists(seed_candidates[0]):
                input_vol = seed_candidates[0]
                threshold, extend, soft = 0.5, self.mask_extend, self.mask_soft_edge
                self._log(f"Mask from seed: {os.path.basename(input_vol)}")
            else:
                white_path = template_path.replace("_black.mrc", "_white.mrc")
                input_vol = white_path if os.path.exists(white_path) else template_path
                threshold, extend, soft = self.mask_threshold, self.mask_extend, self.mask_soft_edge
                self._log(f"Mask from map: {os.path.basename(input_vol)} threshold={threshold}")

            output = os.path.join(
                self.output_folder, f"{Path(template_path).stem.replace('_white', '').replace('_black', '')}_mask.mrc"
            )
            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, threshold, extend, soft, self.mask_lowpass
            )
            if res["success"]:
                self._log(f"Mask created: {os.path.basename(output)}")
                if p:
                    p.mask_path = output
                await get_state_service().save_project()
                await self.refresh_files()
            else:
                self._log(f"Mask failed: {res.get('error')}")
        finally:
            n.dismiss()
            self.masking_active = False

    async def _align_pdb(self):
        if not self.structure_path:
            ui.notify("Select a structure first", type="warning")
            return
        target = self.structure_path.replace(".cif", "_aligned.cif").replace(".pdb", "_aligned.pdb")
        self._log("Aligning to principal axes…")
        res = await self.backend.pdb_service.align_to_principal_axes(self.structure_path, target)
        if res["success"]:
            self._log(f"Aligned: {os.path.basename(target)}")
            await self.refresh_files()
        else:
            self._log(f"Alignment error: {res.get('error')}")

    async def _delete(self, path):
        await self.backend.template_service.delete_file_async(path)
        await self.refresh_files()

    # ------------------------------------------------------------------
    # THRESHOLD / METHOD
    # ------------------------------------------------------------------

    async def _on_threshold_method_changed(self, e):
        self.threshold_method = e.value
        p = self._get_tm_params()
        template_path = p.template_path if p else ""
        if not template_path or not os.path.exists(template_path):
            return
        base = Path(template_path).stem.replace("_white", "").replace("_black", "")
        prefix = base.split("_box")[0]
        seed_candidates = [
            os.path.join(self.output_folder, f)
            for f in os.listdir(self.output_folder)
            if f.endswith("_seed.mrc") and prefix in f
        ]
        if seed_candidates and os.path.exists(seed_candidates[0]):
            return  # seed always 0.5
        white_path = template_path.replace("_black.mrc", "_white.mrc")
        source = white_path if os.path.exists(white_path) else template_path
        thresholds = await self.backend.template_service.calculate_thresholds_async(source, self.mask_lowpass)
        if e.value in thresholds:
            self.mask_threshold = round(thresholds[e.value], 4)

    # ------------------------------------------------------------------
    # BOX / PIXEL SIZE / SHAPE CHANGE HANDLERS
    # ------------------------------------------------------------------

    def _on_pixel_size_changed(self, e=None):
        val = getattr(e, "value", None) if e is not None else self.pixel_size
        if val is None:
            return
        try:
            self.pixel_size = float(val)
        except (TypeError, ValueError):
            return
        self._update_size_estimate()
        self._recalculate_auto_box()
        self._save_workbench_params()  

    def _on_box_size_changed(self, e=None):
        val = getattr(e, "value", None) if e is not None else None
        if val is None:
            if self.size_estimate_label:
                self.size_estimate_label.set_text("—")
            return
        try:
            self.box_size = int(val)
        except (TypeError, ValueError):
            return
        self._update_size_estimate()
        self._save_workbench_params()   # <--

    def _on_shape_changed(self):
        self._recalculate_auto_box()
        self._save_workbench_params()   # <--

    def _on_auto_box_toggle(self, e):
        self.auto_box = e.value
        if not self.box_input:
            return
        if self.auto_box:
            self.box_input.disable()
            self._recalculate_auto_box()
        else:
            self.box_input.enable()

        self._save_workbench_params()   # <--

    def _recalculate_auto_box(self):
        if not self.auto_box:
            return
        try:
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            new_box = max(int(((max(dims) / self.pixel_size) * 1.3 + 31) // 32) * 32, 96)
            self.box_size = new_box
            self._last_logged_box = new_box
            self._update_size_estimate()
        except Exception:
            pass

    def _update_size_estimate(self):
        if not self.size_estimate_label:
            return
        try:
            box = int(self.box_size)
            if box <= 0:
                raise ValueError
        except (TypeError, ValueError):
            self.size_estimate_label.set_text("—")
            return
        est = round((box**3 * 4) / (1024 * 1024), 1)
        self.size_estimate_label.set_text(f"~{est} MB")
        self.size_estimate_label.classes(
            replace=f"text-[9px] font-mono {'text-red-500' if est > 100 else 'text-green-600'}"
        )

    # ------------------------------------------------------------------
    # SELECTION LABELS
    # ------------------------------------------------------------------

    def _update_selection_labels(self):
        p = self._get_tm_params()
        t, m = (p.template_path, p.mask_path) if p else ("", "")
        if self.template_label:
            self.template_label.set_text(os.path.basename(t) or "—")
        if self.mask_label:
            self.mask_label.set_text(os.path.basename(m) or "—")
        if self.structure_label:
            self.structure_label.set_text(os.path.basename(self.structure_path) or "—")
        is_structure = self.structure_path.lower().endswith((".pdb", ".cif"))
        if self.simulate_btn:
            self.simulate_btn.set_enabled(is_structure)
            if is_structure:
                self.simulate_tooltip.set_text(
                    f"{self.pixel_size}Å / {int(self.box_size)}px / {self.template_resolution or '45'}Å LP"
                )
            else:
                self.simulate_tooltip.set_text("Select a PDB/CIF structure first")

    # ------------------------------------------------------------------
    # VIEWER BRIDGE
    # ------------------------------------------------------------------

    def _post_to_viewer(self, action: str, **kwargs):
        if not self.client:
            return
        payload = {"action": action, **kwargs}
        self.client.run_javascript(
            "const f=document.getElementById('molstar-frame');"
            "if(f&&f.contentWindow)f.contentWindow.postMessage(%s,'*');" % json.dumps(payload)
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
            self._log(f"Viewer error: {e.args.get('message')}")

    async def _test_iframe_loaded(self):
        await asyncio.sleep(1)
        self._post_to_viewer("getItems")

    def _toggle_visibility_from_ui(self, iid):
        item = next((i for i in self.loaded_items if i.get("id") == iid), None)
        if item:
            self._toggle_visibility(iid, not item.get("visible", True))

    def _change_item_color(self, iid, color):
        self._post_to_viewer("setColor", itemId=iid, color=color)

    def _toggle_visibility(self, iid, vis):
        self._post_to_viewer("setVisibility", itemId=iid, visible=vis)

    def _change_iso_value(self, iid, val, inv):
        self._post_to_viewer("setIsoValue", itemId=iid, isoValue=-abs(val) if inv else val)

    def _delete_viewer_item(self, iid):
        self._post_to_viewer("deleteItem", itemId=iid)

    # ------------------------------------------------------------------
    # MISC
    # ------------------------------------------------------------------

    def _get_tm_params(self):
        return get_project_state().jobs.get(JobType.TEMPLATE_MATCH_PYTOM)

    def _log(self, msg: str):
        if self.log_container:
            with self.log_container:
                ui.label(f"• {msg}").classes("text-[9px] text-gray-600 font-mono leading-tight")
            if self.client:
                self.client.run_javascript(
                    f"const el=document.getElementById('c{self.log_container.id}');if(el)el.scrollTop=el.scrollHeight;"
                )
