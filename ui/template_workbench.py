from nicegui import ui, app
from fastapi.responses import FileResponse, HTMLResponse
import os
import json
import asyncio
from pathlib import Path

from services.project_state import JobType, get_project_state, get_state_service

# Color palette (same as in TypeScript)
COLOR_PALETTE = [
    0x5c6bc0, 0x7986cb, 0x9fa8da, 0x42a5f5, 0x64b5f6, 0x90caf9,
    0x26c6da, 0x4dd0e1, 0x80deea, 0x26a69a, 0x4db6ac, 0x80cbc4,
    0x66bb6a, 0x81c784, 0xa5d6a7, 0x9ccc65, 0xaed581, 0xc5e1a5,
    0xffa726, 0xffb74d, 0xffcc80, 0xffee58, 0xfff176, 0xfff59d,
    0xef5350, 0xe57373, 0xef9a9a, 0xec407a, 0xf06292, 0xf48fb1,
    0xab47bc, 0xba68c8, 0xce93d8, 0x7e57c2, 0x9575cd, 0xb39ddb,
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
    """Integrated Template Workbench with Double-Tray synchronization and service logic."""

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
        self.structure_path = ""

        # Mask settings
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20
        self.threshold_method = "flexible_bounds"

        # Async Status
        self.masking_active = False
        self.viewer_ready = False
        self.loaded_items = []

        # UI refs
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

        self._last_logged_box = None

        self._load_project_parameters()
        self._render()

        asyncio.create_task(self.refresh_files())
        asyncio.create_task(self._test_iframe_loaded())

        self.session_item_containers = {}  # Map item_id -> container element

    # =========================================================
    # BRIDGE COMMUNICATION
    # =========================================================
    
    # =========================================================
    # PROJECT PARAMETERS
    # =========================================================
    def _update_session_tray(self):
        if not self.session_list_container:
            return
        
        # Get current item IDs
        current_item_ids = {item.get("id") for item in self.loaded_items}
        
        # Remove items that no longer exist
        for item_id in list(self.session_item_containers.keys()):
            if item_id not in current_item_ids:
                refs = self.session_item_containers[item_id]
                container = refs['container']  # FIX: get the actual container element
                self.session_list_container.remove(container)
                del self.session_item_containers[item_id]
        
        # Add or update items
        for item in self.loaded_items:
            item_id = item.get("id", "unknown")
            
            if item_id in self.session_item_containers:
                # Update existing item
                self._update_session_item(item_id, item)
            else:
                # Create new item
                self._create_session_item(item)
        
    def _create_session_item(self, item):
        """Create a new session item with all controls"""
        item_id = item.get("id", "unknown")
        item_type = item.get("type", "unknown")
        visible = item.get("visible", True)
        color = item.get("color", 0xCCCCCC)
        
        color_hex = f"#{color:06x}" if isinstance(color, int) else "#CCCCCC"
        
        with self.session_list_container:
            container = ui.column().classes("w-full gap-1 p-2 border border-gray-200 rounded bg-white shadow-sm")
            
            with container:
                # Header row
                with ui.row().classes("w-full items-center gap-2"):
                    color_btn = ui.button(icon="palette").props("flat round dense size=xs").classes("shrink-0")
                    color_btn.style(f"color: {color_hex}")
                    
                    with ui.menu().props("auto-close") as color_menu:
                        with ui.grid(columns=6).classes("gap-1 p-2"):
                            for palette_color in COLOR_PALETTE:
                                palette_hex = f"#{palette_color:06x}"
                                ui.button().props("flat dense").style(
                                    f"background-color: {palette_hex}; width: 24px; height: 24px; min-width: 24px;"
                                ).on("click", lambda c=palette_color, iid=item_id: self._change_item_color(iid, c))
                    
                    color_btn.on("click", color_menu.open)
                    
                    name_label = ui.label(item_id).classes("text-[10px] font-bold text-gray-700 truncate flex-1")
                    
                    # FIX: Handler that looks up current visibility instead of capturing it
                    def toggle_vis_handler(iid):
                    # Look up current state dynamically
                        current_item = next((item for item in self.loaded_items if item.get("id") == iid), None)
                        if current_item:
                            current_visible = current_item.get("visible", True)
                            self._toggle_visibility(iid, not current_visible)
                    
                    vis_btn = ui.button(
                        icon="visibility" if visible else "visibility_off",
                        on_click=lambda iid=item_id: toggle_vis_handler(iid),
                    ).props("flat round dense size=xs")
                    
                    del_btn = ui.button(
                        icon="delete", 
                        on_click=lambda iid=item_id: self._delete_viewer_item(iid)
                    ).props("flat round dense size=xs color=red")
                    
                    # Stop propagation after DOM is ready
                    def setup_stop_propagation():
                        ui.run_javascript(f"""
                            const visBtn = document.getElementById('c{vis_btn.id}');
                            const delBtn = document.getElementById('c{del_btn.id}');
                            
                            if (visBtn) {{
                                visBtn.addEventListener('click', (e) => {{
                                    e.stopPropagation();
                                }}, false);
                            }}
                            
                            if (delBtn) {{
                                delBtn.addEventListener('click', (e) => {{
                                    e.stopPropagation();
                                }}, false);
                            }}
                        """)
                    
                    ui.timer(0.1, setup_stop_propagation, once=True)
                
                # ISO controls for volumes
                iso_row = None
                iso_slider = None
                iso_label = None
                
                if item_type == "map":
                    iso_value = item.get("isoValue", 1.5)
                    is_inverted = item.get("isInverted", False)
                    stats = item.get("stats", {})
                    
                    abs_iso_value = abs(iso_value)
                    
                    # Calculate absolute value
                    mean = stats.get("mean", 0)
                    sigma = stats.get("sigma", 1)
                    absolute_value = mean + (iso_value * sigma)
                    
                    with ui.row().classes("w-full items-center gap-2 mt-1 pt-1 border-t border-gray-100") as iso_row:
                        ui.label("ISO:").classes("text-[9px] text-gray-500 shrink-0")
                        
                        iso_slider = ui.slider(
                            min=0.5, max=5.0, step=0.1, value=abs_iso_value
                        ).props("dense").classes("flex-1")
                        
                        iso_slider.on(
                            "change",
                            lambda e, iid=item_id, inv=is_inverted: self._change_iso_value(iid, e.args, inv)
                        )
                        
                        # Display both sigma and absolute value
                        display_sigma = f"-{abs_iso_value:.1f}σ" if is_inverted else f"{abs_iso_value:.1f}σ"
                        display_abs = f"({absolute_value:.3f})"
                        
                        iso_label = ui.label(f"{display_sigma} {display_abs}").classes(
                            "text-[9px] font-mono text-gray-600 shrink-0 w-24"
                        )
            
            # Store references
            self.session_item_containers[item_id] = {
                'container': container,
                'name_label': name_label,
                'vis_btn': vis_btn,
                'color_btn': color_btn,
                'iso_row': iso_row,
                'iso_slider': iso_slider,
                'iso_label': iso_label,
            }


    def _update_session_item(self, item_id, item):
            """Update an existing session item without recreating it"""
            refs = self.session_item_containers.get(item_id)
            if not refs:
                return
            
            item_type = item.get("type", "unknown")
            visible = item.get("visible", True)
            color = item.get("color", 0xCCCCCC)
            
            # Update visibility button icon
            refs['vis_btn'].props(f"icon={'visibility' if visible else 'visibility_off'}")
            
            # Update color
            color_hex = f"#{color:06x}" if isinstance(color, int) else "#CCCCCC"
            refs['color_btn'].style(f"color: {color_hex}")
            
            # Update ISO display if it's a map
            if item_type == "map" and refs['iso_label']:
                iso_value = item.get("isoValue", 1.5)
                is_inverted = item.get("isInverted", False)
                stats = item.get("stats", {})
                
                abs_iso_value = abs(iso_value)
                
                # Update slider value (without triggering change event)
                if refs['iso_slider']:
                    refs['iso_slider'].value = abs_iso_value
                
                # Calculate and display absolute value
                mean = stats.get("mean", 0)
                sigma = stats.get("sigma", 1)
                absolute_value = mean + (iso_value * sigma)
                
                display_sigma = f"-{abs_iso_value:.1f}σ" if is_inverted else f"{abs_iso_value:.1f}σ"
                display_abs = f"({absolute_value:.3f})"
                
                refs['iso_label'].set_text(f"{display_sigma} {display_abs}")




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

    def _log(self, message: str):
        """Log to UI panel with auto-scroll."""
        if self.log_container:
            with self.log_container:
                ui.label(f"• {message}").classes("text-[10px] text-gray-600 font-mono leading-tight")
            ui.run_javascript(
                f"const el = document.getElementById('c{self.log_container.id}'); if (el) el.scrollTop = el.scrollHeight;"
            )

    # =========================================================
    # CALCULATIONS
    # =========================================================

    def _estimate_file_size_mb(self) -> float:
        if not self.box_size or self.box_size <= 0:
            return 0
        return round((int(self.box_size) ** 3 * 4) / (1024 * 1024), 1)

    def _estimate_box_for_dimension(self, dim_ang: float) -> int:
        if not self.pixel_size or self.pixel_size <= 0:
            return 128
        dim_pix = dim_ang / self.pixel_size
        padded = dim_pix * 1.2
        offset = 32
        box = int((padded + offset - 1) // offset) * offset
        return max(box, 96)

    def _update_size_estimate(self):
        if self.size_estimate_label:
            est = self._estimate_file_size_mb()
            text = f"~{est / 1000:.1f} GB" if est >= 1000 else f"~{est} MB"
            self.size_estimate_label.set_text(text)
            color = "text-red-600" if est > 100 else ("text-orange-500" if est > 20 else "text-green-600")
            self.size_estimate_label.classes(replace=f"text-xs font-mono {color}")

    # =========================================================
    # RENDER
    # =========================================================

    def _render(self):
        with ui.column().classes("w-full gap-0 bg-white"):
            # 1. Header status bar
            with ui.row().classes("w-full gap-6 px-4 py-2 bg-gray-50 border-b border-gray-200 items-center"):
                self._render_header_indicators()

            # 2. TOP SECTION (Generation / Masking)
            with ui.row().classes("w-full gap-0 border-b border-gray-200").style("height: 480px; overflow: hidden;"):
                with ui.column().classes("w-[40%] p-4 gap-3 border-r border-gray-100 overflow-y-auto h-full"):
                    self._render_template_creation_panel()

                with ui.column().classes("w-[25%] p-4 gap-3 border-r border-gray-100 overflow-y-auto h-full"):
                    self._render_mask_creation_panel()

                with ui.column().classes("flex-1 p-4 gap-2 bg-gray-50/30 overflow-hidden h-full"):
                    self._render_logs_panel()

            # 3. BOTTOM SECTION (Double Tray & Viewer)
            with ui.row().classes("w-full gap-0").style("height: 450px; overflow: hidden;"):
                # Tray 1: Local Files
                with ui.column().classes("w-1/4 p-4 border-r border-gray-200 bg-gray-50/10 overflow-hidden h-full"):
                    self._render_tray_header("Available Locally", "folder")
                    self.file_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")

                # Tray 2: Viewer Session
                with ui.column().classes("w-1/4 p-4 border-r border-gray-200 bg-white overflow-hidden h-full"):
                    with ui.row().classes("w-full items-center justify-between mb-3"):
                        self._render_tray_header("In Viewer", "layers")
                        ui.button(icon="delete_sweep", on_click=lambda: self._post_to_viewer("clear")).props(
                            "flat round dense size=sm color=red"
                        )
                    self.session_list_container = ui.column().classes("w-full gap-1 overflow-y-auto flex-1")

                # Molstar Viewer iframe
                with ui.column().classes("flex-1 bg-black relative overflow-hidden h-full"):
                    ui.element("iframe").props('src="/molstar-workbench" id="molstar-frame"').classes(
                        "absolute inset-0 w-full h-full border-none"
                    )
    
    # Set up bidirectional communication
        ui.run_javascript("""
            console.log("[UI] Setting up message listener");
            
            window.addEventListener('message', function(event) {
                console.log("[UI] Received message:", event.data);
                
                // Only handle messages from our iframe
                const iframe = document.getElementById('molstar-frame');
                if (!iframe || event.source !== iframe.contentWindow) {
                    console.log("[UI] Message not from our iframe, ignoring");
                    return;
                }
                
                // Only handle messages with a 'type' property
                if (!event.data || !event.data.type) {
                    console.log("[UI] Message missing type property, ignoring");
                    return;
                }
                
                console.log("[UI] Valid molstar event:", event.data.type);
                
                // Send to Python backend
                emitEvent('molstar_event', event.data);
            });
            
            console.log("[UI] Message listener installed");
        """)
        
        # Register Python event handler
        ui.on("molstar_event", self._handle_viewer_event)
        
        # Wait for iframe to load, then test communication
        ui.timer(20.0, self._test_iframe_loaded, once=True)


    def _handle_viewer_event(self, e):
        """Handle events emitted by the TypeScript bridge."""
        print(f"[UI HANDLER] Received event: {e.args}")
        
        if not isinstance(e.args, dict):
            print(f"[UI HANDLER] Event args is not a dict: {type(e.args)}")
            return
        
        event_type = e.args.get('type')
        print(f"[UI HANDLER] Event type: {event_type}")
        
        if event_type == 'ready':
            print("[UI HANDLER] Viewer is ready")
            self.viewer_ready = True
            self._post_to_viewer("getItems")
        
        elif event_type == 'itemsChanged':
            items = e.args.get('items', [])
            print(f"[UI HANDLER] Items changed, count: {len(items)}")
            self.loaded_items = items
            self._update_session_tray()
        
        elif event_type == 'structureLoaded':
            item = e.args.get('item')
            print(f"[UI HANDLER] Structure loaded: {item}")
            self._log(f"Loaded structure: {item.get('id') if item else 'unknown'}")
        
        elif event_type == 'mapLoaded':
            item = e.args.get('item')
            print(f"[UI HANDLER] Map loaded: {item}")
            self._log(f"Loaded map: {item.get('id') if item else 'unknown'}")
        
        elif event_type == 'error':
            error_msg = e.args.get('message', 'Unknown error')
            print(f"[UI HANDLER] Error: {error_msg}")
            self._log(f"Error: {error_msg}")


    async def _test_iframe_loaded(self):
        """Test iframe communication after it loads"""
        print("[DEBUG] Testing iframe communication...")
        
        # Test 1: Request items
        print("[DEBUG] Test 1: Requesting items...")
        self._post_to_viewer("getItems")
        
        # Test 2: Wait a bit then check if ready
        await asyncio.sleep(1)
        print(f"[DEBUG] Viewer ready status: {self.viewer_ready}")
        print(f"[DEBUG] Loaded items count: {len(self.loaded_items)}")


    def _post_to_viewer(self, action: str, **kwargs):
        """Send a command to the Molstar iframe via postMessage."""
        payload = {**kwargs}
        payload['action'] = action
        ui.run_javascript(f"""
            const frame = document.getElementById('molstar-frame');
            if (frame && frame.contentWindow) {{
                console.log("[UI] Sending command to iframe:", {json.dumps(payload)});
                frame.contentWindow.postMessage({json.dumps(payload)}, '*');
            }} else {{
                console.error("[UI] Iframe not found or no contentWindow");
            }}
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

    def _render_tray_header(self, title: str, icon: str):
        with ui.row().classes("items-center gap-2 mb-2"):
            ui.icon(icon, size="14px").classes("text-gray-400")
            ui.label(title).classes("text-[10px] font-bold text-gray-500 uppercase tracking-widest")

    def _render_template_creation_panel(self):
        """Standardized template generation inputs."""
        self._section_title("1. Template Generation", "settings")

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

        ui.separator().classes("my-2 opacity-50")
        with ui.column().classes("w-full gap-4"):
            with ui.column().classes("w-full gap-1 bg-gray-50/50 p-2 rounded-lg"):
                ui.label("Ellipsoid Creation").classes("text-[10px] font-bold text-gray-500 uppercase")
                with ui.row().classes("w-full gap-2 items-center"):
                    ui.input(label="x:y:z (Å)", placeholder="550:550:550").bind_value(self, "basic_shape_def").props(
                        "dense outlined"
                    ).classes("flex-1").on("update:model-value", self._on_shape_changed)
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
                ui.separator().classes("opacity-30")
                with ui.row().classes("w-full gap-2"):
                    self.simulate_btn = (
                        ui.button("Simulate from PDB", icon="science", on_click=self._simulate_pdb)
                        .props("unelevated dense color=blue-7 outline disable")
                        .classes("flex-1 text-[11px]")
                    )
                    self.resample_btn = (
                        ui.button("Resample from EMDB", icon="layers", on_click=self._resample_emdb)
                        .props("unelevated dense color=blue-7 outline disable")
                        .classes("flex-1 text-[11px]")
                    )

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

    def _section_title(self, title: str, icon: str):
        with ui.row().classes("items-center gap-2 mb-2"):
            ui.icon(icon, size="16px").classes("text-gray-400")
            ui.label(title).classes("text-xs font-bold text-gray-700 uppercase tracking-wide")

    # =========================================================
    # CORE ACTIONS (INTEGRATING YOUR SERVICES)
    # =========================================================

    def _on_local_click(self, path):
        """Dispatches a local file URL to the viewer."""
        fname = os.path.basename(path)
        ext = Path(path).suffix.lower()
        file_url = f"/api/file?path={path}"
        self._log(f"Viewing: {fname}")

        if ext in [".mrc", ".map", ".rec", ".ccp4"]:
            self._post_to_viewer("load_volume", url=file_url)
        elif ext in [".pdb", ".cif"]:
            fmt = "pdb" if ext == ".pdb" else "mmcif"
            self._post_to_viewer("load_structure", url=file_url, format=fmt)

    async def _gen_shape(self):
        """Generate basic shape ellipsoid."""
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

    async def _fetch_pdb(self):
        """Fetch coordinate file from RCSB."""
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
        """Fetch map from EMDB."""
        if not self.emdb_input_val:
            return
        self._log(f"Fetching EMDB: {self.emdb_input_val}")
        res = await self.backend.template_service.fetch_emdb_map_async(self.emdb_input_val.strip(), self.output_folder)
        if res["success"]:
            self._log(f"Fetched EMDB: {os.path.basename(res['path'])}")
            await self.refresh_files()
        else:
            self._log(f"Fetch failed: {res.get('error')}")

    async def _simulate_pdb(self):
        """Simulate map from coordinate file."""
        if not self.structure_path:
            return
        self._log(f"Simulating map: {os.path.basename(self.structure_path)}...")
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
        """Resample map to project apix."""
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
        n = ui.notification("Relion: Creating mask...", type="ongoing", spinner=True, timeout=None)
        try:
            input_vol = (
                template_path.replace("_black.mrc", "_white.mrc") if "_black.mrc" in template_path else template_path
            )
            base = os.path.basename(input_vol).split(".")[0].replace("_white", "").replace("_black", "")
            output = os.path.join(self.output_folder, f"{base}_mask.mrc")
            res = await self.backend.template_service.create_mask_relion(
                input_vol, output, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )
            if res["success"]:
                self._log(f"Mask created: {os.path.basename(output)}")
                await self.refresh_files()
        finally:
            n.dismiss()
            self.masking_active = False

    # =========================================================
    # TRAY UPDATES
    # =========================================================

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
                ui.button(icon="biotech", on_click=lambda: self._toggle_structure(path)).props(
                    f"flat round dense size=sm color={'emerald' if is_s else 'grey'}"
                )
                if path.lower().endswith((".mrc", ".map")):
                    ui.button(icon="view_in_ar", on_click=lambda: self._toggle_template(path)).props(
                        f"flat round dense size=sm color={'blue' if is_t else 'grey'}"
                    )
                    ui.button(icon="architecture", on_click=lambda: self._toggle_mask(path)).props(
                        f"flat round dense size=sm color={'purple' if is_m else 'grey'}"
                    )
                ui.button(icon="delete", on_click=lambda: self._delete(path)).props(
                    "flat round dense size=sm color=red"
                )

    def _change_item_color(self, item_id: str, color: int):
        """Change color of an item in the viewer"""
        print(f"[UI] Changing color of {item_id} to {color}")
        self._post_to_viewer("setColor", itemId=item_id, color=color)


    def _toggle_visibility(self, item_id: str, visible: bool):
        """Toggle visibility of an item"""
        print(f"[UI] Setting visibility of {item_id} to {visible}")
        self._post_to_viewer("setVisibility", itemId=item_id, visible=visible)


    def _change_iso_value(self, item_id: str, slider_value: float, is_inverted: bool = False):
        """Change ISO value of a volume, applying sign if inverted"""
        # Apply negative sign for inverted volumes
        actual_iso_value = -abs(slider_value) if is_inverted else slider_value
        print(f"[UI] Setting ISO value of {item_id} to {actual_iso_value} (slider: {slider_value}, inverted: {is_inverted})")
        self._post_to_viewer("setIsoValue", itemId=item_id, isoValue=actual_iso_value)


    def _delete_viewer_item(self, item_id: str):
        """Delete an item from the viewer"""
        print(f"[UI] Deleting {item_id}")
        self._post_to_viewer("deleteItem", itemId=item_id)   





    # =========================================================
    # STATE HELPERS
    # =========================================================

    def _get_tm_params(self):
        return get_project_state().jobs.get(JobType.TEMPLATE_MATCH_PYTOM)

    def _get_current_template_path(self):
        p = self._get_tm_params()
        return p.template_path if p else ""

    async def _toggle_template(self, path):
        p = self._get_tm_params()
        if p:
            p.template_path = "" if p.template_path == path else path
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
            fname, ext = os.path.basename(path), Path(path).suffix.lower()
            if ext in [".pdb", ".cif"]:
                self.pdb_input_val = fname.split(".")[0]
            self._on_local_click(path)
        await self.refresh_files()

    async def _delete(self, path):
        await self.backend.template_service.delete_file_async(path)
        await self.refresh_files()

    def _update_selection_labels(self):
        p = self._get_tm_params()
        t, m = (p.template_path, p.mask_path) if p else ("", "")
        if self.template_label:
            self.template_label.set_text(os.path.basename(t) or "Not set")
        if self.mask_label:
            self.mask_label.set_text(os.path.basename(m) or "Not set")
        if self.structure_label:
            self.structure_label.set_text(os.path.basename(self.structure_path) or "Not set")

        is_vol = t and any(x in t.lower() for x in [".mrc", ".map"])
        if self.mask_source_label:
            self.mask_source_label.set_text(f"Source: {os.path.basename(t)}" if is_vol else "Select a volume first")
        if self.simulate_btn:
            self.simulate_btn.props(
                "remove disable" if self.structure_path.lower().endswith((".pdb", ".cif")) else "add disable"
            )
        if self.resample_btn:
            self.resample_btn.props(
                "remove disable" if self.structure_path.lower().endswith((".mrc", ".map")) else "add disable"
            )

    def _on_pixel_size_changed(self):
        self._update_size_estimate()
        self._recalculate_auto_box()

    def _on_box_size_changed(self):
        self._update_size_estimate()

    def _on_shape_changed(self):
        self._recalculate_auto_box()

    def _on_auto_box_toggle(self, e):
        self.auto_box = e.value
        self.box_input.props(f"{'add' if e.value else 'remove'} disable")
        if e.value:
            self._recalculate_auto_box()

    async def _on_threshold_method_changed(self, e):
        self.threshold_method = e.value
        t_path = self._get_current_template_path()
        if t_path and os.path.exists(t_path):
            thresholds = await self.backend.template_service.calculate_thresholds_async(t_path, self.mask_lowpass)
            if e.value in thresholds:
                self.mask_threshold = round(thresholds[e.value], 4)

    def _recalculate_auto_box(self):
        if not self.auto_box:
            return
        try:
            dims = [float(x) for x in self.basic_shape_def.split(":")]
            new_box = self._estimate_box_for_dimension(max(dims))
            if new_box != self._last_logged_box:
                self.box_size, self._last_logged_box = new_box, new_box
                self._log(f"Auto-box: {max(dims)}Å → {new_box}px")
                self._update_size_estimate()
        except:
            pass

    def _use_tomo_apix(self):
        if self.project_tomo_apix:
            self.pixel_size = self.project_tomo_apix
            self._recalculate_auto_box()
            self._update_size_estimate()
