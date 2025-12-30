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
    """Template workbench with clean layout."""

    def __init__(self, backend, project_path: str):
        print("[DEBUG] TemplateWorkbench.__init__ START")
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        os.makedirs(self.output_folder, exist_ok=True)
        print("[DEBUG] TemplateWorkbench.__init__ output_folder created")

        # Project-derived values
        self.project_raw_apix = None
        self.project_binning = None
        self.project_tomo_apix = None

        # Template settings
        self.pixel_size = 10.0
        self.box_size = 128
        self.auto_box = True
        self.template_resolution = 20.0

        # Inputs
        self.basic_shape_def = "550:550:550"
        self.pdb_input_val = ""
        self.emdb_input_val = ""
        self.structure_path = ""

        # Mask settings
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20
        self.threshold_method = "flexible_bounds"

        # Async Status
        self.masking_active = False

        # UI refs
        self.file_list_container = None
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

        # Track for log deduplication
        self._last_logged_box = None
        print("[DEBUG] TemplateWorkbench.__init__ attributes set")

        print("[DEBUG] TemplateWorkbench.__init__ calling _load_project_parameters")
        self._load_project_parameters()
        print("[DEBUG] TemplateWorkbench.__init__ _load_project_parameters done")
        
        print("[DEBUG] TemplateWorkbench.__init__ calling _render")
        self._render()
        print("[DEBUG] TemplateWorkbench.__init__ _render done")
        
        print("[DEBUG] TemplateWorkbench.__init__ creating refresh_files task")
        asyncio.create_task(self.refresh_files())
        print("[DEBUG] TemplateWorkbench.__init__ COMPLETE")

    # =========================================================
    # PROJECT PARAMETER LOADING
    # =========================================================

    def _load_project_parameters(self):
        """Load pixel size and binning from project state."""
        print("[DEBUG] _load_project_parameters START")
        try:
            state = get_project_state()
            print(f"[DEBUG] _load_project_parameters got state: {state.project_name}")

            # Get raw pixel size from microscope params
            if hasattr(state, "microscope") and state.microscope:
                raw_pix = getattr(state.microscope, "pixel_size_angstrom", None)
                if raw_pix and raw_pix > 0:
                    self.project_raw_apix = raw_pix

            # Get tomogram pixel size from reconstruction job
            self.project_tomo_apix = None
            self.project_binning = None

            if hasattr(state, "jobs") and state.jobs:
                for job_type, job_params in state.jobs.items():
                    job_name = job_type.value.lower()
                    if "reconstruct" in job_name:
                        for field in ["rescale_angpixs", "binned_angpix", "output_angpix"]:
                            val = getattr(job_params, field, None)
                            if val is not None and float(val) > 0:
                                self.project_tomo_apix = float(val)
                                if self.project_raw_apix:
                                    self.project_binning = round(self.project_tomo_apix / self.project_raw_apix, 1)
                                break
                        break

            # Set default pixel size
            if self.project_tomo_apix:
                self.pixel_size = self.project_tomo_apix
            elif self.project_raw_apix:
                self.pixel_size = self.project_raw_apix

        except Exception as e:
            print(f"[TEMPLATE_WORKBENCH] Could not load project params: {e}")

    def _log(self, message: str):
        """Log to UI panel with auto-scroll."""
        print(f"[LOG] {message}")
        if self.log_container:
            with self.log_container:
                ui.label(f"• {message}").classes("text-[10px] text-gray-600 font-mono leading-tight")
            # Ensure the log scrolls to the bottom when new entries arrive
            ui.run_javascript(f"const el = document.getElementById('c{self.log_container.id}'); if (el) el.scrollTop = el.scrollHeight;")

    # =========================================================
    # CALCULATIONS
    # =========================================================

    def _estimate_file_size_mb(self) -> float:
        """Estimate MRC file size: box³ × 4 bytes (float32)."""
        if not self.box_size or self.box_size <= 0:
            return 0
        return round((int(self.box_size) ** 3 * 4) / (1024 * 1024), 1)

    def _estimate_box_for_dimension(self, dim_ang: float) -> int:
        """Estimate box size for particle dimension."""
        if not self.pixel_size or self.pixel_size <= 0:
            return 128
        dim_pix = dim_ang / self.pixel_size
        padded = dim_pix * 1.2
        offset = 32
        box = int((padded + offset - 1) // offset) * offset
        return max(box, 96)

    def _update_size_estimate(self):
        """Update file size display."""
        if self.size_estimate_label:
            est = self._estimate_file_size_mb()
            if est >= 1000:
                text = f"~{est / 1000:.1f} GB"
            else:
                text = f"~{est} MB"
            self.size_estimate_label.set_text(text)
            color = "text-red-600" if est > 100 else ("text-orange-500" if est > 20 else "text-green-600")
            self.size_estimate_label.classes(replace=f"text-xs font-mono {color}")

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
        print("[DEBUG] TemplateWorkbench._render() START")
        with ui.column().classes("w-full gap-0 bg-white"):
            print("[DEBUG] _render: header status bar START")
            # 1. Header status bar
            with ui.row().classes("w-full gap-6 px-4 py-2 bg-gray-50 border-b border-gray-200 items-center"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("view_in_ar", size="xs").classes("text-blue-600")
                    ui.label("Active Template:").classes("text-xs font-medium text-gray-600")
                    self.template_label = ui.label("Not set").classes("text-xs font-mono text-gray-400")
                with ui.row().classes("items-center gap-2"):
                    ui.icon("architecture", size="xs").classes("text-purple-600")
                    ui.label("Active Mask:").classes("text-xs font-medium text-gray-600")
                    self.mask_label = ui.label("Not set").classes("text-xs font-mono text-gray-400")
                with ui.row().classes("items-center gap-2"):
                    ui.icon("biotech", size="xs").classes("text-emerald-600")
                    ui.label("Structure Source:").classes("text-xs font-medium text-gray-600")
                    self.structure_label = ui.label("Not set").classes("text-xs font-mono text-gray-400")
            print("[DEBUG] _render: header status bar END")

            print("[DEBUG] _render: TOP SECTION START")
            # 2. TOP SECTION - overflow:hidden forces the height constraint
            with ui.row().classes("w-full gap-0 border-b border-gray-200").style("height: 500px; overflow: hidden;"):
                print("[DEBUG] _render: template creation panel START")
                with ui.column().classes("w-[42%] p-4 gap-3 border-r border-gray-100 overflow-y-auto h-full"):
                    self._render_template_creation_panel()
                print("[DEBUG] _render: template creation panel END")

                print("[DEBUG] _render: mask creation panel START")
                with ui.column().classes("w-[25%] p-4 gap-3 border-r border-gray-100 overflow-y-auto h-full"):
                    self._render_mask_creation_panel()
                print("[DEBUG] _render: mask creation panel END")

                print("[DEBUG] _render: logs panel START")
                with ui.column().classes("flex-1 p-4 gap-2 bg-gray-50/30 overflow-hidden h-full"):
                    self._render_logs_panel()
                print("[DEBUG] _render: logs panel END")
            print("[DEBUG] _render: TOP SECTION END")

            print("[DEBUG] _render: BOTTOM SECTION START")
            # 3. BOTTOM SECTION - overflow:hidden here too
            with ui.row().classes("w-full gap-0").style("height: 450px; overflow: hidden;"):
                print("[DEBUG] _render: files panel START")
                with ui.column().classes("w-1/3 p-4 border-r border-gray-200 bg-gray-50/10 overflow-y-auto h-full"):
                    self._render_files_panel()
                print("[DEBUG] _render: files panel END")

                print("[DEBUG] _render: molstar iframe START")
                # Molstar needs h-full to actually get height from parent
                with ui.column().classes("flex-1 bg-black relative overflow-hidden h-full"):
                    ui.element("iframe").props('src="/molstar" id="molstar-frame"').classes(
                        "absolute inset-0 w-full h-full border-none"
                    )
                    with ui.row().classes(
                        "absolute bottom-4 left-4 bg-black/70 backdrop-blur-md px-3 py-1.5 "
                        "rounded-full text-[10px] text-gray-300 border border-white/10 pointer-events-none z-10"
                    ):
                        ui.icon("mouse", size="xs").classes("mr-1")
                        ui.label("LMB: Rotate | RMB: Pan | Scroll: Zoom")
                print("[DEBUG] _render: molstar iframe END")
            print("[DEBUG] _render: BOTTOM SECTION END")

        print("[DEBUG] TemplateWorkbench._render() COMPLETE")

    def _render_template_creation_panel(self):
        """Standardized template generation inputs."""
        self._section_title("1. Template Generation", "settings")

        # Project Reference Block
        if self.project_raw_apix or self.project_tomo_apix:
            with ui.row().classes("w-full items-center bg-blue-50/50 p-2 rounded-lg border border-blue-100 mb-2 gap-3"):
                with ui.column().classes("gap-0"):
                    ui.label("RAW").classes("text-[8px] text-blue-400 uppercase font-bold")
                    ui.label(f"{self.project_raw_apix} Å" if self.project_raw_apix else "---").classes(
                        "text-xs font-mono text-blue-700"
                    )

                if self.project_binning:
                    with ui.column().classes("gap-0"):
                        ui.label("BIN").classes("text-[8px] text-blue-400 uppercase font-bold")
                        ui.label(f"{self.project_binning}x").classes("text-xs font-mono text-blue-700")

                with ui.column().classes("gap-0 flex-1"):
                    ui.label("TOMO").classes("text-[8px] text-blue-400 uppercase font-bold")
                    ui.label(f"{self.project_tomo_apix} Å" if self.project_tomo_apix else "---").classes(
                        "text-xs font-mono text-blue-800 font-bold"
                    )

                ui.button("Sync", icon="sync", on_click=self._use_tomo_apix).props(
                    "flat dense size=sm color=primary"
                ).classes("text-[10px]")

        # Main Configuration Grid
        with ui.row().classes("w-full gap-2 items-start"):
            ui.number(
                "Pixel Size (Å)", value=self.pixel_size, step=0.1, on_change=self._on_pixel_size_changed
            ).bind_value(self, "pixel_size").props("dense outlined").classes("flex-1")

            self.box_input = (
                ui.number("Box (px)", value=self.box_size, step=32, on_change=self._on_box_size_changed)
                .bind_value(self, "box_size")
                .props("dense outlined")
                .classes("flex-1")
            )

            if self.auto_box:
                self.box_input.props(add="disable")

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
            self.size_estimate_label = ui.label("~8 MB").classes("text-[10px] font-mono text-green-600")

        self.warning_container = ui.column().classes("w-full gap-1 my-1")
        self._update_size_estimate()
        self._update_warnings()

        ui.separator().classes("my-2 opacity-50")

        # Creation Sub-panels
        with ui.column().classes("w-full gap-4"):
            # A. Basic Shape Generation
            with ui.column().classes("w-full gap-1 bg-gray-50/50 p-2 rounded-lg"):
                ui.label("Ellipsoid Creation").classes("text-[10px] font-bold text-gray-500 uppercase")
                with ui.row().classes("w-full gap-2 items-center"):
                    ui.input(label="x:y:z (Å)", placeholder="550:550:550").bind_value(self, "basic_shape_def").props(
                        "dense outlined"
                    ).classes("flex-1").on("update:model-value", self._on_shape_changed)
                    ui.button("Generate", icon="add_box", on_click=self._gen_shape).props(
                        "unelevated dense color=primary"
                    ).classes("px-4")

            # B. PDB/EMDB Fetch & Simulation
            with ui.column().classes("w-full gap-2 bg-blue-50/30 p-2 rounded-lg"):
                ui.label("Structure / Map Processing").classes("text-[10px] font-bold text-blue-500 uppercase")

                # Fetch Row
                with ui.row().classes("w-full gap-2"):
                    with ui.column().classes("flex-1 gap-1"):
                        with ui.row().classes("w-full gap-1"):
                            ui.input(label="PDB ID", placeholder="7xyz").bind_value(self, "pdb_input_val").props(
                                "dense outlined"
                            ).classes("flex-1")
                            ui.button(icon="cloud_download", on_click=self._fetch_pdb).props(
                                "flat dense color=primary"
                            ).tooltip("Download PDB/CIF")

                    with ui.column().classes("flex-1 gap-1"):
                        with ui.row().classes("w-full gap-1"):
                            ui.input(label="EMDB ID", placeholder="30210").bind_value(self, "emdb_input_val").props(
                                "dense outlined"
                            ).classes("flex-1")
                            ui.button(icon="cloud_download", on_click=self._fetch_emdb).props(
                                "flat dense color=primary"
                            ).tooltip("Download EMDB Map")

                ui.separator().classes("opacity-30")

                # Dispatch Row
                with ui.row().classes("w-full gap-2"):
                    self.simulate_btn = (
                        ui.button("Simulate from PDB", icon="science", on_click=self._simulate_pdb)
                        .props("unelevated dense color=blue-7 outline")
                        .classes("flex-1 text-[11px]")
                        .props("disable")
                    )

                    self.resample_btn = (
                        ui.button("Resample from EMDB", icon="layers", on_click=self._resample_emdb)
                        .props("unelevated dense color=blue-7 outline")
                        .classes("flex-1 text-[11px]")
                        .props("disable")
                    )

    def _render_mask_creation_panel(self):
        """Mask creation parameters."""
        self._section_title("2. Mask Creation", "architecture")

        self.mask_source_label = ui.label("Select a volume first").classes("text-[11px] text-orange-500 italic mb-1")

        with ui.column().classes("w-full gap-2"):
            self.threshold_method_select = (
                ui.select(
                    options=["flexible_bounds", "otsu", "isodata", "li", "yen"],
                    value=self.threshold_method,
                    label="Method",
                )
                .props("dense outlined")
                .classes("w-full")
                .on_value_change(self._on_threshold_method_changed)
            )

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

    def _update_warnings(self):
        """Update warning display."""
        if not self.warning_container:
            return

        self.warning_container.clear()
        warnings = self._get_warnings()

        if not warnings:
            return

        with self.warning_container:
            for w in warnings:
                colors = {
                    "error": ("error", "bg-red-50 border-red-100", "text-red-700"),
                    "warning": ("warning", "bg-yellow-50 border-yellow-100", "text-yellow-700"),
                    "info": ("info", "bg-blue-50 border-blue-100", "text-blue-700"),
                }
                icon, bg, txt = colors.get(w["level"], colors["info"])
                with ui.row().classes(f"w-full items-start gap-1 p-1 rounded border {bg}"):
                    ui.icon(icon, size="14px").classes(txt)
                    ui.label(w["text"]).classes(f"text-[9px] {txt} flex-1 leading-tight")

    def _on_pixel_size_changed(self):
        """Handle pixel size change."""
        self._update_size_estimate()
        self._update_warnings()
        if self.auto_box:
            self._recalculate_auto_box()

    def _on_box_size_changed(self):
        """Handle box size change."""
        self._update_size_estimate()
        self._update_warnings()

    def _on_shape_changed(self):
        """Handle shape change."""
        if self.auto_box:
            self._recalculate_auto_box()

    def _on_auto_box_toggle(self, e):
        """Toggle auto-box checkbox."""
        self.auto_box = e.value
        if self.box_input:
            if self.auto_box:
                self.box_input.props(add="disable")
                self._recalculate_auto_box()
                self._log("Auto-box: enabled")
            else:
                self.box_input.props(remove="disable")
                self._log(f"Auto-box: disabled (box={self.box_size}px, manual editing enabled)")

    def _on_threshold_method_changed(self, e):
        """Handle threshold method change."""
        self.threshold_method = e.value
        self._log(f"Threshold method: {self.threshold_method}")
        asyncio.create_task(self._recalc_threshold_for_method(self.threshold_method))

    async def _recalc_threshold_for_method(self, method: str):
        """Recalculate threshold."""
        t_path = self._get_current_template_path()
        if not t_path or not os.path.exists(t_path):
            return

        thresholds = await self.backend.template_service.calculate_thresholds_async(t_path, self.mask_lowpass)
        if method in thresholds:
            self.mask_threshold = round(thresholds[method], 4)
            self._log(f"New threshold ({method}): {self.mask_threshold} [LP={self.mask_lowpass}Å]")

    async def _recalc_threshold(self):
        """Recalc with current method."""
        await self._recalc_threshold_for_method(self.threshold_method)

    def _recalculate_auto_box(self):
        """Recalc box from shape."""
        if not self.auto_box:
            return
        try:
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            max_dim = max(dims)
            if not self.pixel_size or self.pixel_size <= 0:
                return
            new_box = self._estimate_box_for_dimension(max_dim)
            if new_box != self._last_logged_box:
                self._last_logged_box = new_box
                self.box_size = new_box
                self._log(f"Auto-box: {max_dim}Å → {new_box}px")
                self._update_size_estimate()
                self._update_warnings()
        except:
            pass

    def _use_tomo_apix(self):
        """Sync to project tomo pixel size."""
        if self.project_tomo_apix:
            self.pixel_size = self.project_tomo_apix
            self._log(f"Synced to tomogram: {self.project_tomo_apix}Å")
            if self.auto_box:
                self._recalculate_auto_box()
            self._update_size_estimate()
            self._update_warnings()

    def _update_selection_labels(self):
        """Refresh path labels in header."""
        t_path = self._get_current_template_path()
        m_path = self._get_current_mask_path()
        s_path = self.structure_path

        if self.template_label:
            self.template_label.set_text(os.path.basename(t_path) if t_path else "Not set")
            self.template_label.classes(replace=f"text-xs font-mono {'text-blue-700' if t_path else 'text-gray-400'}")

        if self.mask_label:
            self.mask_label.set_text(os.path.basename(m_path) if m_path else "Not set")
            self.mask_label.classes(replace=f"text-xs font-mono {'text-purple-700' if m_path else 'text-gray-400'}")

        if self.structure_label:
            self.structure_label.set_text(os.path.basename(s_path) if s_path else "Not set")
            self.structure_label.classes(
                replace=f"text-xs font-mono {'text-emerald-700' if s_path else 'text-gray-400'}"
            )

        # Update Mask Source Hint
        is_volume = t_path and any(x in t_path.lower() for x in [".mrc", ".map", ".rec"])
        if self.mask_source_label:
            if is_volume:
                self.mask_source_label.set_text(f"Source: {os.path.basename(t_path)}")
                self.mask_source_label.classes(replace="text-[11px] text-blue-600 font-mono mb-1")
                if self.mask_btn:
                    self.mask_btn.props(remove="disabled")
            else:
                self.mask_source_label.set_text("Select a volume template first")
                self.mask_source_label.classes(replace="text-[11px] text-orange-500 italic mb-1")
                if self.mask_btn:
                    self.mask_btn.props(add="disabled")

        # Update Dispatch Button States
        if s_path:
            is_coord = any(x in s_path.lower() for x in [".pdb", ".cif", ".ent"])
            is_map = any(x in s_path.lower() for x in [".mrc", ".map", ".rec", ".ccp4"])

            if self.simulate_btn:
                self.simulate_btn.props("remove disable" if is_coord else "add disable")
            if self.resample_btn:
                self.resample_btn.props("remove disable" if is_map else "add disable")
        else:
            if self.simulate_btn:
                self.simulate_btn.props("add disable")
            if self.resample_btn:
                self.resample_btn.props("add disable")

    def _get_warnings(self) -> list:
        """Sanity check warnings."""
        warnings = []
        if self.project_tomo_apix and self.pixel_size:
            if self.pixel_size < self.project_tomo_apix * 0.9:
                warnings.append({"level": "warning", "text": f"Finer than tomos ({self.project_tomo_apix}Å)."})
            elif (
                self.project_raw_apix
                and abs(self.pixel_size - self.project_raw_apix) < 0.1
                and self.project_binning > 1.5
            ):
                warnings.append({"level": "error", "text": "Using RAW pixels! Templates will be massive."})
        return warnings

    def _render_files_panel(self):
        """Files list container."""
        with ui.row().classes("w-full items-center justify-between mb-3 border-b border-gray-200 pb-2"):
            ui.label("Available Data").classes("text-xs font-bold text-gray-600 uppercase tracking-widest")
            ui.button(icon="refresh", on_click=self.refresh_files).props("flat round dense size=sm")
        self.file_list_container = ui.column().classes("w-full gap-1")

    def _render_logs_panel(self):
        """Activity log container that scrolls within its parent."""
        self._section_title("Activity Log", "terminal")
        # This container scrolls - give it flex-1 to fill available space and overflow-y-auto
        self.log_container = ui.column().classes("w-full gap-1 flex-1 overflow-y-auto")

    def _section_title(self, title: str, icon: str):
        """Styled header."""
        with ui.row().classes("items-center gap-2 mb-2"):
            ui.icon(icon, size="16px").classes("text-gray-400")
            ui.label(title).classes("text-xs font-bold text-gray-700 uppercase tracking-wide")

    async def refresh_files(self):
            """List files from directory with distinct action areas."""
            if not self.file_list_container:
                return
            self._update_selection_labels()
            self.file_list_container.clear()
            files = await self.backend.template_service.list_template_files_async(self.output_folder)
            
            current_template = self._get_current_template_path()
            current_mask = self._get_current_mask_path()
            current_struct = self.structure_path

            with self.file_list_container:
                if not files:
                    ui.label("No files found").classes("text-xs text-gray-400 italic py-4")
                    return
                for f_path in files:
                    fname = os.path.basename(f_path)
                    ext = Path(f_path).suffix.lower()
                    
                    is_template = f_path == current_template
                    is_mask = f_path == current_mask
                    is_struct = f_path == current_struct
                    
                    bg = "bg-blue-50/50" if is_template else ("bg-purple-50/50" if is_mask else ("bg-emerald-50/50" if is_struct else "hover:bg-gray-50"))
                    border = "border-l-4 border-blue-500" if is_template else ("border-l-4 border-purple-500" if is_mask else ("border-l-4 border-emerald-500" if is_struct else "border-l-4 border-transparent"))
                    
                    with ui.row().classes(
                        f"w-full items-center gap-1 px-3 py-1 rounded-r-lg {bg} {border} transition-colors group"
                    ):
                        # AREA 1: Click for Visualization ONLY
                        with ui.row().classes("flex-1 items-center gap-2 min-w-0 cursor-pointer").on("click", lambda p=f_path: self._visualize(p)):
                            icon_type = "architecture" if "_mask" in fname else ("view_in_ar" if ext in [".mrc", ".map", ".rec", ".ccp4"] else "biotech")
                            ui.icon(icon_type, size="14px").classes("text-gray-400")
                            ui.label(fname).classes("text-[11px] font-mono text-gray-700 truncate")
                        
                        # AREA 2: Controls (Selectors) - Clicking these will NOT trigger visualization
                        with ui.row().classes("gap-0 shrink-0"):
                            # Structure Selector (Coords or Maps)
                            if ext in [".pdb", ".cif", ".mrc", ".map", ".rec", ".ccp4", ".ent"]:
                                ui.button(icon="biotech", on_click=lambda p=f_path: self._toggle_structure(p)).props(
                                    f"flat round dense size=sm color={'emerald' if is_struct else 'grey'}"
                                ).tooltip("Set as processing source")
                                
                            # Template/Mask Selectors (Volumes ONLY)
                            if ext in [".mrc", ".map", ".rec", ".ccp4"]:
                                ui.button(icon="view_in_ar", on_click=lambda p=f_path: self._toggle_template(p)).props(
                                    f"flat round dense size=sm color={'blue' if is_template else 'grey'}"
                                ).tooltip("Set as template")
                                
                                ui.button(icon="architecture", on_click=lambda p=f_path: self._toggle_mask(p)).props(
                                    f"flat round dense size=sm color={'purple' if is_mask else 'grey'}"
                                ).tooltip("Set as mask")
                            
                            ui.button(icon="delete", on_click=lambda p=f_path: self._delete(p)).props(
                                "flat round dense size=sm color=red"
                            ).tooltip("Delete file")

    def _visualize(self, path: str):
        """Post message to Molstar iframe."""
        self._log(f"Viewing: {os.path.basename(path)}")
        if any(x in path.lower() for x in [".mrc", ".map", ".rec", ".ccp4"]):
            url = f"/api/file?path={path}"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_volume', url: '{url}' }}, '*');"
            )
        else:
            url = f"/api/file?path={path}"
            fmt = "pdb" if path.lower().endswith(".pdb") else "mmcif"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage({{ action: 'load_structure', url: '{url}', format: '{fmt}' }}, '*');"
            )

    async def _toggle_template(self, path: str):
            """Set as active template with type guardrails."""
            ext = Path(path).suffix.lower()
            if ext not in [".mrc", ".map", ".rec", ".ccp4"]:
                ui.notify("Template must be a volume file (MRC/MAP)", type="warning")
                return

            if self._get_current_template_path() == path:
                await self._set_template_path("")
                self._log("Template unset")
            else:
                await self._set_template_path(path)
                self._log(f"Template set: {os.path.basename(path)}")
            await self.refresh_files()

    async def _toggle_mask(self, path: str):
        """Set as active mask with type guardrails."""
        ext = Path(path).suffix.lower()
        if ext not in [".mrc", ".map", ".rec", ".ccp4"]:
            ui.notify("Mask must be a volume file (MRC/MAP)", type="warning")
            return

        if self._get_current_mask_path() == path:
            await self._set_mask_path("")
            self._log("Mask unset")
        else:
            await self._set_mask_path(path)
            self._log(f"Mask set: {os.path.basename(path)}")
        await self.refresh_files()

    async def _toggle_structure(self, path: str):
        """Set as active structure for simulation/resampling."""
        if self.structure_path == path:
            self.structure_path = ""
            self._log("Structure source unset")
        else:
            self.structure_path = path
            fname = os.path.basename(path)
            ext = Path(path).suffix.lower()

            # Autopopulate fields
            if ext in [".pdb", ".cif", ".ent"]:
                self.pdb_input_val = fname.split(".")[0]
            elif ext in [".mrc", ".map"]:
                if "emd_" in fname:
                    self.emdb_input_val = fname.split("_")[1].split(".")[0]

            self._log(f"Structure source: {fname}")
            self._visualize(path)

        await self.refresh_files()

    async def _delete(self, path: str):
        """Delete from disk."""
        if path == self._get_current_template_path():
            await self._set_template_path("")
        if path == self._get_current_mask_path():
            await self._set_mask_path("")
        if path == self.structure_path:
            self.structure_path = ""

        await self.backend.template_service.delete_file_async(path)
        self._log(f"Deleted: {os.path.basename(path)}")
        await self.refresh_files()

    async def _fetch_pdb(self):
        """Fetch structure from RCSB."""
        if not self.pdb_input_val:
            ui.notify("Enter a PDB ID first", type="warning")
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
        """Fetch map from EMDB."""
        if not self.emdb_input_val:
            ui.notify("Enter an EMDB ID first", type="warning")
            return
        self._log(f"Fetching EMDB: {self.emdb_input_val}")
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val.strip(), self.output_folder)
        if res["success"]:
            self._log(f"Fetched EMDB: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _gen_shape(self):
        """Generate MRC from ellipsoid def."""
        if self.auto_box:
            self._recalculate_auto_box()
        self._log(f"Generating ellipsoid: {self.basic_shape_def}...")
        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size), self.template_resolution
        )
        if res["success"]:
            self._log(f"Generated shape: {os.path.basename(res['path_black'])}")
            await self.refresh_files()
        else:
            self._log(f"Generation failed: {res.get('error')}")

    async def _simulate_pdb(self):
        """Generate map from currently selected Structure path (if coord file)."""
        if not self.structure_path:
            return

        self._log(f"Simulating map from structure: {os.path.basename(self.structure_path)}...")
        res_sim = await asyncio.to_thread(
            self.backend.pdb_service.simulate_map_from_pdb,
            self.structure_path,
            self.pixel_size,
            int(self.box_size),
            self.output_folder,
        )
        if res_sim["success"]:
            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"], self.output_folder, self.pixel_size, int(self.box_size), self.template_resolution
            )
            if res_proc["success"]:
                self._log(f"Simulation created: {os.path.basename(res_proc['path_black'])}")
                await self.refresh_files()
        else:
            self._log(f"Simulation failed: {res_sim.get('error')}")

    async def _resample_emdb(self):
        """Resample currently selected Structure path (if map file)."""
        if not self.structure_path:
            return

        self._log(f"Resampling map: {os.path.basename(self.structure_path)}...")
        res_proc = await self.backend.template_service.process_volume_async(
            self.structure_path, self.output_folder, self.pixel_size, int(self.box_size), self.template_resolution
        )
        if res_proc["success"]:
            self._log(f"Resampled map created: {os.path.basename(res_proc['path_black'])}")
            await self.refresh_files()
        else:
            self._log(f"Resampling failed: {res_proc.get('error')}")

    async def _create_mask(self):
        """Dispatches Relion mask creation."""
        template_path = self._get_current_template_path()
        if not template_path:
            return

        self.masking_active = True
        n = ui.notify("Relion: Creating mask...", type="ongoing", timeout=0)

        try:
            input_vol = (
                template_path.replace("_black.mrc", "_white.mrc") if "_black.mrc" in template_path else template_path
            )
            base = os.path.basename(input_vol).split(".")[0].replace("_white", "").replace("_black", "")
            output = os.path.join(self.output_folder, f"{base}_mask.mrc")

            self._log(f"Dispatched relion_mask_create for {os.path.basename(input_vol)}...")
            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )

            if n:
                n.dismiss()
            if res["success"]:
                ui.notify(f"Mask created: {os.path.basename(output)}", type="positive")
                self._log(f"Mask created: {os.path.basename(output)}")
                await self.refresh_files()
            else:
                ui.notify(f"Masking failed: {res.get('error')}", type="negative")
                self._log(f"Masking failed: {res.get('error')}")
        except Exception as e:
            if n:
                n.dismiss()
            ui.notify(f"UI Error: {e}", type="negative")
            self._log(f"UI Error: {e}")
        finally:
            self.masking_active = False
