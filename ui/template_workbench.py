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
        self.backend = backend
        self.project_path = project_path
        self.output_folder = os.path.join(project_path, "templates")
        os.makedirs(self.output_folder, exist_ok=True)

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

        # Mask settings
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20
        self.threshold_method = "flexible_bounds"

        # UI refs
        self.file_list_container = None
        self.log_container = None
        self.mask_btn = None
        self.mask_source_label = None
        self.box_input = None
        self.size_estimate_label = None
        self.template_label = None
        self.mask_label = None
        self.auto_box_checkbox = None

        # Track for log deduplication
        self._last_logged_box = None

        self._load_project_parameters()
        self._render()
        asyncio.create_task(self.refresh_files())

    # =========================================================
    # PROJECT PARAMETER LOADING
    # =========================================================

    def _load_project_parameters(self):
        """Load pixel size and binning from project state."""
        try:
            state = get_project_state()

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
        """Log to UI panel."""
        print(f"[LOG] {message}")
        if self.log_container:
            with self.log_container:
                ui.label(f"• {message}").classes("text-[10px] text-gray-600 font-mono leading-tight")

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
        with ui.column().classes("w-full gap-0"):
            # Header bar
            with ui.row().classes("w-full gap-6 px-4 py-2 bg-gray-50 border-b border-gray-200 items-center"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("view_in_ar", size="xs").classes("text-blue-600")
                    ui.label("Template:").classes("text-xs font-medium text-gray-600")
                    self.template_label = ui.label("Not set").classes("text-xs font-mono text-gray-400")
                with ui.row().classes("items-center gap-2"):
                    ui.icon("architecture", size="xs").classes("text-purple-600")
                    ui.label("Mask:").classes("text-xs font-medium text-gray-600")
                    self.mask_label = ui.label("Not set").classes("text-xs font-mono text-gray-400")

            # ROW 1: Three columns - Controls | Files | Logs
            with ui.row().classes("w-full gap-0 border-b border-gray-200").style("min-height: 380px;"):
                # Column 1: Controls
                with ui.column().classes("flex-1 p-4 gap-4 overflow-y-auto border-r border-gray-100"):
                    self._render_controls()

                # Column 2: Files
                with ui.column().classes("flex-1 p-4 gap-2 overflow-y-auto border-r border-gray-100"):
                    self._render_files_panel()

                # Column 3: Logs
                with ui.column().classes("flex-1 p-4 gap-2 overflow-y-auto"):
                    self._render_logs_panel()

            # ROW 2: Molstar viewer
            with ui.column().classes("w-full p-4"):
                with ui.element("div").classes("w-full bg-black rounded-lg relative").style("height: 320px;"):
                    ui.element("iframe").props('src="/molstar" id="molstar-frame"').style(
                        "width: 100%; height: 100%; border: none; border-radius: 8px;"
                    )
                    with ui.row().classes(
                        "absolute bottom-2 left-2 bg-black/60 px-2 py-1 rounded text-[9px] text-gray-400"
                    ):
                        ui.label("LMB: Rotate | RMB: Pan | Scroll: Zoom")

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
                    "error": ("error", "bg-red-50 border-red-200", "text-red-700"),
                    "warning": ("warning", "bg-yellow-50 border-yellow-200", "text-yellow-700"),
                    "info": ("info", "bg-blue-50 border-blue-200", "text-blue-700"),
                }
                icon, bg, txt = colors.get(w["level"], colors["info"])
                with ui.row().classes(f"w-full items-start gap-1 p-1.5 rounded border {bg}"):
                    ui.icon(icon, size="xs").classes(txt)
                    ui.label(w["text"]).classes(f"text-[10px] {txt} flex-1")

    def _render_controls(self):
        """Render control panels without card shadows."""
        # Project Reference
        self._section_title("Project Reference")
        if self.project_raw_apix:
            with ui.row().classes("w-full items-center gap-6 mb-2"):
                with ui.column().classes("gap-0"):
                    ui.label("Raw").classes("text-[9px] text-gray-400 uppercase")
                    ui.label(f"{self.project_raw_apix} Å").classes("text-sm font-mono")
                if self.project_binning:
                    with ui.column().classes("gap-0"):
                        ui.label("Binning").classes("text-[9px] text-gray-400 uppercase")
                        ui.label(f"{self.project_binning}×").classes("text-sm font-mono")
                if self.project_tomo_apix:
                    with ui.column().classes("gap-0"):
                        ui.label("Tomogram").classes("text-[9px] text-gray-400 uppercase")
                        ui.label(f"{self.project_tomo_apix} Å").classes("text-sm font-mono font-bold text-blue-700")
                    ui.button("Use", on_click=self._use_tomo_apix).props("flat dense size=sm color=primary")
        else:
            ui.label("Not detected from project").classes("text-xs text-gray-400 italic")

        ui.separator().classes("my-3")

        # Template Creation
        self._section_title("Template Creation")

        # Settings row
        with ui.row().classes("w-full gap-3 items-start mb-2"):
            with ui.column().classes("gap-1"):
                ui.number(
                    "Pixel Size (Å)",
                    value=self.pixel_size,
                    step=0.1,
                    format="%.2f",
                    on_change=self._on_pixel_size_changed,
                ).bind_value(self, "pixel_size").props("dense outlined").classes("w-28")

            with ui.column().classes("gap-1"):
                self.box_input = (
                    ui.number("Box Size (px)", value=self.box_size, step=32, on_change=self._on_box_size_changed)
                    .bind_value(self, "box_size")
                    .props("dense outlined")
                    .classes("w-28")
                )
                # Set initial disabled state
                if self.auto_box:
                    self.box_input.props(add="disable")

            with ui.column().classes("gap-1"):
                ui.number("LP Res (Å)", value=self.template_resolution, step=5, min=1).bind_value(
                    self, "template_resolution"
                ).props("dense outlined").classes("w-24").tooltip(
                    "Low-pass filter resolution for smoothing the generated template"
                )

            with ui.column().classes("gap-0 pt-4"):
                self.size_estimate_label = ui.label("~8 MB").classes("text-xs font-mono text-green-600")
                ui.label("box³×4B").classes("text-[8px] text-gray-400").tooltip(
                    "File size = box³ × 4 bytes (float32). Pixel size affects physical dimensions, not file size."
                )

        # Auto-box toggle - don't use bind_value, manage state manually
        with ui.row().classes("w-full items-center gap-2 mb-2"):
            self.auto_box_checkbox = (
                ui.checkbox("Auto-calculate box from particle dimensions", value=self.auto_box)
                .props("dense")
                .classes("text-xs")
            )
            self.auto_box_checkbox.on_value_change(self._on_auto_box_toggle)

        # Warnings container
        self.warning_container = ui.column().classes("w-full gap-1 mb-2")

        self._update_size_estimate()
        self._update_warnings()

        ui.separator().classes("my-2")

        # Ellipsoid
        ui.label("Ellipsoid (x:y:z Å)").classes("text-[10px] font-bold text-gray-500 uppercase mb-1")
        with ui.row().classes("w-full gap-2 mb-3"):
            ui.input(placeholder="550:550:550").bind_value(self, "basic_shape_def").props("dense outlined").classes(
                "flex-1"
            ).on("update:model-value", self._on_shape_changed).tooltip("Ellipsoid diameters in Ångstroms (x:y:z)")
            ui.button("Create", on_click=self._gen_shape).props("unelevated dense color=primary")

        # From PDB
        ui.label("From PDB").classes("text-[10px] font-bold text-gray-500 uppercase mb-1")
        with ui.row().classes("w-full gap-2 mb-3"):
            ui.input(placeholder="PDB ID or file path").bind_value(self, "pdb_input_val").props(
                "dense outlined"
            ).classes("flex-1")
            ui.button("Create", on_click=self._simulate_pdb).props("unelevated dense color=primary")

        # Fetch
        with ui.row().classes("w-full gap-4"):
            with ui.column().classes("flex-1 gap-1"):
                ui.label("Fetch EMDB").classes("text-[10px] font-bold text-gray-500 uppercase")
                with ui.row().classes("w-full gap-1"):
                    ui.input(placeholder="e.g. 30210").bind_value(self, "emdb_input_val").props(
                        "dense outlined"
                    ).classes("flex-1")
                    ui.button(icon="download", on_click=self._fetch_emdb).props("flat dense color=primary")
            with ui.column().classes("flex-1 gap-1"):
                ui.label("Fetch PDB").classes("text-[10px] font-bold text-gray-500 uppercase")
                with ui.row().classes("w-full gap-1"):
                    ui.input(placeholder="e.g. 3j7z").bind_value(self, "pdb_input_val").props("dense outlined").classes(
                        "flex-1"
                    )
                    ui.button(icon="download", on_click=self._fetch_pdb).props("flat dense color=primary")

        ui.separator().classes("my-3")

        # Mask Creation
        self._section_title("Mask Creation")
        self.mask_source_label = ui.label("Select a volume template first").classes(
            "text-xs text-orange-500 italic mb-2"
        )

        with ui.row().classes("w-full gap-2 items-end mb-2"):
            self.threshold_method_select = (
                ui.select(
                    options=["flexible_bounds", "otsu", "isodata", "li", "yen"],
                    value=self.threshold_method,
                    label="Method",
                )
                .props("dense outlined")
                .classes("w-36")
                .tooltip("Thresholding algorithm: flexible_bounds (mean+1.85σ), otsu/isodata/li/yen (automatic)")
            )
            # Use on_value_change to get the new value directly
            self.threshold_method_select.on_value_change(self._on_threshold_method_changed)

            ui.number("Threshold", format="%.4f").bind_value(self, "mask_threshold").props("dense outlined").classes(
                "flex-1"
            ).tooltip("Density threshold for initial binarization. Voxels above this become mask.")
            ui.button(icon="calculate", on_click=self._recalc_threshold).props("flat dense").tooltip(
                "Recalculate threshold using selected method"
            )

        with ui.row().classes("w-full gap-2 mb-2"):
            ui.number("Extend (px)").bind_value(self, "mask_extend").props("dense outlined").classes("flex-1").tooltip(
                "Expand mask by this many pixels after thresholding (default: 3)"
            )
            ui.number("Soft Edge (px)").bind_value(self, "mask_soft_edge").props("dense outlined").classes(
                "flex-1"
            ).tooltip("Width of cosine soft edge falloff in pixels (default: 6)")
            ui.number("LP (Å)").bind_value(self, "mask_lowpass").props("dense outlined").classes("flex-1").tooltip(
                "Low-pass filter applied before thresholding to smooth mask boundaries (default: 20Å)"
            )

        self.mask_btn = (
            ui.button("Create Mask", icon="play_arrow", on_click=self._create_mask)
            .props("unelevated dense color=secondary disabled")
            .classes("w-full")
        )

    def _on_pixel_size_changed(self):
        """Handle pixel size change."""
        self._update_size_estimate()
        self._update_warnings()
        # Only recalculate if auto_box is enabled
        if self.auto_box:
            self._recalculate_auto_box()

    def _on_box_size_changed(self):
        """Handle manual box change."""
        self._update_size_estimate()
        self._update_warnings()

    def _on_shape_changed(self):
        """Handle shape definition change."""
        if self.auto_box:
            self._recalculate_auto_box()

    def _on_auto_box_toggle(self, e):
        """Handle auto-box checkbox toggle."""
        # e.value contains the new checkbox state
        new_value = e.value
        self.auto_box = new_value

        if self.box_input:
            if new_value:
                self.box_input.props(add="disable")
                self._recalculate_auto_box()
                self._log("Auto-box: enabled")
            else:
                self.box_input.props(remove="disable")
                self._log(f"Auto-box: disabled (box={self.box_size}px, manual editing enabled)")

    def _on_threshold_method_changed(self, e):
        """Handle threshold method selection change."""
        new_method = e.value
        self.threshold_method = new_method
        self._log(f"Threshold method changed to: {new_method}")
        # Recalculate with the new method
        asyncio.create_task(self._recalc_threshold_for_method(new_method))

    async def _recalc_threshold_for_method(self, method: str):
        """Recalculate threshold using specified method."""
        t_path = self._get_current_template_path()
        if not t_path or not os.path.exists(t_path):
            self._log(f"Cannot calculate threshold: no template selected")
            return

        thresholds = await self.backend.template_service.calculate_thresholds_async(t_path, self.mask_lowpass)

        if method in thresholds:
            new_thresh = round(thresholds[method], 4)
            self.mask_threshold = new_thresh
            self._log(
                f"Threshold ({method}): {new_thresh} [LP={self.mask_lowpass}Å, source={os.path.basename(t_path)}]"
            )
        else:
            self._log(f"Threshold method '{method}' not found in results")

    async def _recalc_threshold(self):
        """Recalculate threshold using current method."""
        await self._recalc_threshold_for_method(self.threshold_method)

    async def _auto_threshold(self, path: str):
        """Auto-calculate threshold when template is selected."""
        if not path or not os.path.exists(path):
            return
        await self._recalc_threshold_for_method(self.threshold_method)

    def _recalculate_auto_box(self):
        """Recalculate box from shape dimensions."""
        if not self.auto_box:
            return

        try:
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            max_dim = max(dims)

            if not self.pixel_size or self.pixel_size <= 0:
                self._log(f"Cannot auto-calculate box: invalid pixel size ({self.pixel_size})")
                return

            new_box = self._estimate_box_for_dimension(max_dim)

            if new_box != self._last_logged_box:
                self._last_logged_box = new_box
                self.box_size = new_box
                self._log(
                    f"Auto-box: {max_dim}Å / {self.pixel_size}Å/px × 1.2 → {new_box}px "
                    f"[{self._estimate_file_size_mb()} MB]"
                )
                self._update_size_estimate()
                self._update_warnings()

        except (ValueError, AttributeError, ZeroDivisionError) as e:
            # Don't log parse errors for partial input
            pass

    def _use_tomo_apix(self):
        """Use tomogram pixel size."""
        if self.project_tomo_apix:
            old_pix = self.pixel_size
            self.pixel_size = self.project_tomo_apix
            self._log(f"Pixel size: {old_pix}Å → {self.project_tomo_apix}Å (tomogram reconstruction)")
            if self.auto_box:
                self._recalculate_auto_box()
            self._update_size_estimate()
            self._update_warnings()

    def _update_selection_labels(self):
        """Update template/mask labels and mask panel state."""
        t_path = self._get_current_template_path()
        m_path = self._get_current_mask_path()

        if t_path:
            self.template_label.set_text(os.path.basename(t_path))
            self.template_label.classes(replace="text-xs font-mono text-blue-700")
        else:
            self.template_label.set_text("Not set")
            self.template_label.classes(replace="text-xs font-mono text-gray-400")

        if m_path:
            self.mask_label.set_text(os.path.basename(m_path))
            self.mask_label.classes(replace="text-xs font-mono text-purple-700")
        else:
            self.mask_label.set_text("Not set")
            self.mask_label.classes(replace="text-xs font-mono text-gray-400")

        # Update mask panel state
        is_volume = t_path and any(x in t_path.lower() for x in [".mrc", ".map", ".rec"])
        if is_volume:
            self.mask_source_label.set_text(f"Source: {os.path.basename(t_path)}")
            self.mask_source_label.classes(replace="text-xs text-blue-600 font-mono mb-2")
            self.mask_btn.props(remove="disabled")
            # Auto-calculate threshold for newly selected template
            asyncio.create_task(self._auto_threshold(t_path))
        else:
            self.mask_source_label.set_text("Select a volume template first")
            self.mask_source_label.classes(replace="text-xs text-orange-500 italic mb-2")
            self.mask_btn.props(add="disabled")

    def _get_warnings(self) -> list:
        """Generate contextual warnings based on current settings."""
        warnings = []

        # Pixel size vs tomogram
        if self.project_tomo_apix and self.pixel_size:
            if self.pixel_size < self.project_tomo_apix * 0.9:
                warnings.append(
                    {
                        "level": "warning",
                        "text": f"Pixel size ({self.pixel_size}Å) is finer than tomograms ({self.project_tomo_apix}Å) - "
                        f"template will have unnecessary detail and larger file size",
                    }
                )
            elif (
                self.project_raw_apix
                and abs(self.pixel_size - self.project_raw_apix) < 0.1
                and self.project_binning
                and self.project_binning > 1.5
            ):
                warnings.append(
                    {
                        "level": "error",
                        "text": f"Using RAW pixel size ({self.project_raw_apix}Å) instead of tomogram ({self.project_tomo_apix}Å) - "
                        f"templates will be ~{self.project_binning**3:.0f}× larger than needed!",
                    }
                )

        # File size warnings
        est_size = self._estimate_file_size_mb()
        if est_size > 500:
            warnings.append(
                {
                    "level": "error",
                    "text": f"Estimated file size: {est_size} MB - extremely large! Consider coarser pixel size or smaller box.",
                }
            )
        elif est_size > 100:
            warnings.append({"level": "warning", "text": f"Estimated file size: {est_size} MB - quite large"})

        # LP resolution vs Nyquist
        if self.pixel_size and self.template_resolution:
            nyquist = self.pixel_size * 2
            if self.template_resolution < nyquist:
                warnings.append(
                    {
                        "level": "info",
                        "text": f"LP resolution ({self.template_resolution}Å) is beyond Nyquist limit ({nyquist}Å) - no additional detail possible",
                    }
                )

        # Box size sanity
        if self.box_size and self.box_size > 512:
            warnings.append(
                {"level": "warning", "text": f"Large box size ({self.box_size}px) - template matching will be slow"}
            )

        return warnings

    def _render_files_panel(self):
        """Render files list."""
        with ui.row().classes("w-full items-center justify-between mb-2"):
            self._section_title("Templates & Masks")
            ui.button(icon="refresh", on_click=self.refresh_files).props("flat round dense size=xs")

        self.file_list_container = ui.column().classes("w-full gap-1")

    def _render_logs_panel(self):
        """Render logs panel."""
        self._section_title("Activity Log")
        self.log_container = ui.column().classes("w-full gap-0 overflow-y-auto").style("max-height: 340px;")

        # Initial log entries
        if self.project_tomo_apix:
            self._log(
                f"Project: raw={self.project_raw_apix}Å, tomo={self.project_tomo_apix}Å ({self.project_binning}× binning)"
            )
        elif self.project_raw_apix:
            self._log(f"Project: raw={self.project_raw_apix}Å (binning not detected)")

    def _section_title(self, title: str):
        ui.label(title).classes("text-xs font-bold text-gray-700 uppercase tracking-wide")

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
                ui.label("No files yet").classes("text-xs text-gray-400 italic py-4")
                return

            for f_path in files:
                fname = os.path.basename(f_path)
                is_template = f_path == current_template
                is_mask = f_path == current_mask

                bg = "bg-blue-50" if is_template else ("bg-purple-50" if is_mask else "hover:bg-gray-50")
                border = (
                    "border-l-2 border-blue-500"
                    if is_template
                    else ("border-l-2 border-purple-500" if is_mask else "border-l-2 border-transparent")
                )

                with ui.row().classes(f"w-full items-center gap-1 px-2 py-1 rounded {bg} {border}"):
                    with (
                        ui.row()
                        .classes("flex-1 items-center gap-1 min-w-0 cursor-pointer")
                        .on("click", lambda p=f_path: self._visualize(p))
                    ):
                        ui.label(fname).classes("text-[10px] font-mono text-gray-700 truncate").tooltip(f_path)
                        if is_template:
                            ui.label("T").classes("text-[7px] font-bold text-white bg-blue-500 px-1 rounded")
                        if is_mask:
                            ui.label("M").classes("text-[7px] font-bold text-white bg-purple-500 px-1 rounded")

                    with ui.row().classes("gap-0 shrink-0"):
                        ui.button(icon="view_in_ar", on_click=lambda p=f_path: self._toggle_template(p)).props(
                            f"flat round dense size=xs {'color=blue' if is_template else 'color=grey'}"
                        )
                        ui.button(icon="architecture", on_click=lambda p=f_path: self._toggle_mask(p)).props(
                            f"flat round dense size=xs {'color=purple' if is_mask else 'color=grey'}"
                        )
                        ui.button(icon="delete", on_click=lambda p=f_path: self._delete(p)).props(
                            "flat round dense size=xs color=red"
                        )

    def _visualize(self, path: str):
        """Visualize in molstar."""
        if any(x in path.lower() for x in [".mrc", ".map", ".rec", ".ccp4"]):
            preview = path.replace(".mrc", "_preview.mrc")
            to_load = preview if os.path.exists(preview) else path
            url = f"/api/file?path={to_load}"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage("
                f"{{ action: 'load_volume', url: '{url}' }}, '*');"
            )
            self._log(f"Viewing: {os.path.basename(path)}")
        else:
            url = f"/api/file?path={path}"
            fmt = "pdb" if path.lower().endswith(".pdb") else "mmcif"
            ui.run_javascript(
                f"document.getElementById('molstar-frame').contentWindow.postMessage("
                f"{{ action: 'load_structure', url: '{url}', format: '{fmt}' }}, '*');"
            )
            self._log(f"Viewing: {os.path.basename(path)}")

    async def _toggle_template(self, path: str):
        if self._get_current_template_path() == path:
            await self._set_template_path("")
            self._log(f"Template unset")
        else:
            await self._set_template_path(path)
            self._visualize(path)
            self._log(f"Template set: {os.path.basename(path)}")
        await self.refresh_files()

    async def _toggle_mask(self, path: str):
        if self._get_current_mask_path() == path:
            await self._set_mask_path("")
            self._log(f"Mask unset")
        else:
            await self._set_mask_path(path)
            self._log(f"Mask set: {os.path.basename(path)}")
        await self.refresh_files()

    async def _delete(self, path: str):
        fname = os.path.basename(path)
        if path == self._get_current_template_path():
            await self._set_template_path("")
        if path == self._get_current_mask_path():
            await self._set_mask_path("")
        await self.backend.template_service.delete_file_async(path)
        self._log(f"Deleted: {fname}")
        await self.refresh_files()

    # =========================================================
    # ACTIONS
    # =========================================================

    async def _fetch_pdb(self):
        if not self.pdb_input_val:
            return
        pdb_id = self.pdb_input_val.strip().lower()
        self._log(f"Fetching PDB: {pdb_id}")
        res = await self.backend.template_service.fetch_pdb_async(pdb_id, self.output_folder)
        if res["success"]:
            self._log(f"Downloaded: {os.path.basename(res['path'])}")
            await self.refresh_files()
            self._visualize(res["path"])
        else:
            self._log(f"Failed to fetch PDB: {res.get('error', 'unknown')}")

    async def _fetch_emdb(self):
        if not self.emdb_input_val:
            return
        emdb_id = self.emdb_input_val.strip()
        self._log(f"Fetching EMDB: {emdb_id}")
        res = await self.backend.template_service.fetch_emdb_map_async(emdb_id, self.output_folder)
        if res["success"]:
            self._log(f"Downloaded: {os.path.basename(res['path'])}")
            await self.refresh_files()
            self._visualize(res["path"])
        else:
            self._log(f"Failed to fetch EMDB: {res.get('error', 'unknown')}")

    async def _gen_shape(self):
        """Generate ellipsoid template."""
        if self.auto_box:
            self._recalculate_auto_box()

        self._log(
            f"Generating ellipsoid: dims={self.basic_shape_def}Å, "
            f"pixel={self.pixel_size}Å, box={self.box_size}px, LP={self.template_resolution}Å"
        )

        res = await self.backend.template_service.generate_basic_shape_async(
            self.basic_shape_def,
            self.pixel_size,
            self.output_folder,
            int(self.box_size),
            lowpass_res=self.template_resolution,
        )
        if res["success"]:
            self._log(f"Created: {os.path.basename(res['path_black'])}")
            if "box_size" in res:
                self.box_size = res["box_size"]
            self._update_size_estimate()
            await self.refresh_files()
            self._visualize(res["path_black"])
        else:
            self._log(f"Failed: {res.get('error', 'unknown')}")

    async def _simulate_pdb(self):
        """Simulate EM density from PDB."""
        try:
            pdb_target = self.pdb_input_val.strip()
            if not pdb_target:
                return

            # Fetch if just ID
            if len(pdb_target) == 4 and not os.path.exists(pdb_target):
                self._log(f"Fetching PDB: {pdb_target}")
                res = await self.backend.template_service.fetch_pdb_async(pdb_target.lower(), self.output_folder)
                if not res["success"]:
                    self._log(f"Failed to fetch: {res.get('error')}")
                    return
                pdb_target = res["path"]

            self._log(
                f"Simulating from PDB: {os.path.basename(pdb_target)}, "
                f"pixel={self.pixel_size}Å, box={self.box_size}px, LP={self.template_resolution}Å"
            )

            res_sim = await asyncio.to_thread(
                self.backend.pdb_service.simulate_map_from_pdb,
                pdb_target,
                self.pixel_size,
                int(self.box_size),
                self.output_folder,
            )
            if not res_sim["success"]:
                self._log(f"Simulation failed: {res_sim.get('error')}")
                return

            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"],
                self.output_folder,
                self.pixel_size,
                int(self.box_size),
                resolution=self.template_resolution,
            )
            if res_proc["success"]:
                self._log(f"Created: {os.path.basename(res_proc['path_black'])}")
                self._update_size_estimate()
                await self.refresh_files()
                self._visualize(res_proc["path_black"])
            else:
                self._log(f"Processing failed: {res_proc.get('error')}")

        except Exception as e:
            self._log(f"Error: {e}")

    async def _create_mask(self):
        """Create mask from template."""
        template_path = self._get_current_template_path()
        if not template_path:
            return

        try:
            # Use white version if available
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

            self._log(
                f"Creating mask: source={os.path.basename(input_vol)}, "
                f"thresh={self.mask_threshold}, extend={self.mask_extend}px, "
                f"soft={self.mask_soft_edge}px, LP={self.mask_lowpass}Å"
            )

            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )
            if res["success"]:
                self._log(f"Created: {os.path.basename(res['path'])}")
                await self.refresh_files()
                self._visualize(res["path"])
            else:
                self._log(f"Failed: {res.get('error')}")

        except Exception as e:
            self._log(f"Error: {e}")
