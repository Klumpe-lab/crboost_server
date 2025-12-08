from nicegui import ui
import os
import asyncio
from typing import Callable


class TemplateCreationDialog:
    def __init__(self, backend, project_path: str, on_success: Callable[[str, str], None] = None):
        """
        A streamlined dialog for creating templates (Shape/PDB) and Masks.
        """
        self.backend = backend
        self.project_path = project_path
        self.on_success = on_success

        # State: Processing Flag (Controls the overlay)
        self.is_processing = False

        # State: Configuration Defaults
        self.pixel_size = 1.5
        self.box_size = 128
        self.output_folder = os.path.join(project_path, "templates")

        # State: Input Fields
        self.basic_shape_def = "100:500:100"
        self.pdb_id = ""

        # State: Mask Parameters
        self.mask_threshold = 0.001
        self.mask_extend = 3
        self.mask_soft_edge = 6
        self.mask_lowpass = 20

        # State: Results
        self.generated_template_path = None
        self.generated_mask_path = None

        # UI Initialization
        self.dialog = ui.dialog()

        # 'relative' class is crucial for the absolute overlay to position correctly
        with self.dialog, ui.card().classes("w-[800px] h-auto max-h-[90vh] flex flex-col p-0 relative overflow-hidden"):
            # --- UNIFIED LOADING OVERLAY ---
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

    def safe_notify(self, message: str, type: str = "info"):
        """
        Safely sends a notification.
        Uses self.dialog.client to avoid 'parent element deleted' errors
        if the triggering button was removed from the DOM during processing.
        """
        try:
            # Use the dialog's client reference directly
            self.dialog.client.notify(message, type=type, close_button=True)
        except Exception as e:
            # Fallback if connection completely lost
            print(f"[UI NOTIFY ERROR] {message} ({e})")

    def render_ui(self):
        # Header
        with ui.row().classes("w-full justify-between items-center bg-gray-50 border-b p-4"):
            ui.label("Generate Template & Mask").classes("text-lg font-bold text-gray-800")
            ui.button(icon="close", on_click=self.dialog.close).props("flat round dense text-color=gray")

        # Scrollable Content Body
        with ui.column().classes("w-full flex-grow overflow-y-auto p-6 gap-6"):
            # --- Section 1: Global Configuration ---
            with ui.column().classes("w-full gap-2"):
                ui.label("1. Output Settings").classes("text-xs font-bold text-gray-500 uppercase tracking-wide")
                with ui.grid(columns=3).classes("w-full gap-4"):
                    ui.number("Pixel Size (Å)", value=self.pixel_size, step=0.1, format="%.2f").bind_value(
                        self, "pixel_size"
                    ).classes("w-full")
                    ui.number("Box Size (px)", value=self.box_size, step=2).bind_value(self, "box_size").classes(
                        "w-full"
                    )
                    ui.input("Output Folder", value=self.output_folder).bind_value(self, "output_folder").classes(
                        "w-full"
                    )

            # --- Section 2: Template Source ---
            with ui.column().classes("w-full gap-2"):
                ui.label("2. Generate Template").classes("text-xs font-bold text-gray-500 uppercase tracking-wide")

                with ui.card().classes("w-full shadow-sm border border-gray-200 p-0"):
                    with ui.tabs().classes("w-full text-left bg-gray-50 border-b text-gray-700") as tabs:
                        t_shape = ui.tab("Basic Shape")
                        t_pdb = ui.tab("From PDB")

                    with ui.tab_panels(tabs, value=t_shape).classes("w-full p-6"):
                        # Tab: Basic Shape
                        with ui.tab_panel(t_shape):
                            with ui.column().classes("w-full gap-3"):
                                ui.label("Generate a synthetic ellipsoid.").classes("text-sm text-gray-600")
                                with ui.row().classes("w-full items-end gap-4"):
                                    ui.input(
                                        "Diameter (x:y:z in Ang)", value="100:100:100", placeholder="100:500:100"
                                    ).bind_value(self, "basic_shape_def").classes("flex-grow")
                                    ui.button(
                                        "Generate Shape", icon="category", on_click=self.handle_basic_shape
                                    ).props("color=primary unelevated")

                        # Tab: PDB
                        with ui.tab_panel(t_pdb):
                            with ui.column().classes("w-full gap-3"):
                                ui.label("Simulate density from structure.").classes("text-sm text-gray-600")
                                with ui.row().classes("w-full items-end gap-4"):
                                    ui.input("PDB Code / File", placeholder="1A1A or path/to/file.pdb").bind_value(
                                        self, "pdb_id"
                                    ).classes("flex-grow")
                                    ui.button("Simulate", icon="biotech", on_click=self.handle_pdb).props(
                                        "color=primary unelevated"
                                    )

            # --- Section 3: Results & Masking ---
            self.results_area = ui.column().classes("w-full gap-6")
            self.render_results_and_mask()

        # Footer
        with ui.row().classes("w-full justify-end border-t p-4 gap-3 bg-gray-50"):
            ui.button("Cancel", on_click=self.dialog.close).props("flat color=grey")
            ui.button("Use Results", on_click=self.finish).props("color=green unelevated icon=check")

    def render_results_and_mask(self):
        """Refreshes the results preview and the mask generation section."""
        self.results_area.clear()

        with self.results_area:
            # A. Results Display
            if self.generated_template_path:
                with ui.row().classes("w-full items-center gap-3 bg-green-50 p-3 rounded border border-green-200"):
                    ui.icon("check_circle", color="green").classes("text-xl")
                    # min-w-0 allows shrinking for truncation
                    with ui.column().classes("gap-0 flex-1 min-w-0"):
                        ui.label("Template Created").classes("text-xs font-bold text-green-800 uppercase")
                        ui.label(os.path.basename(self.generated_template_path)).classes(
                            "text-sm font-mono text-gray-700 truncate w-full"
                        )
                        ui.label(self.generated_template_path).classes(
                            "text-[10px] text-gray-500 break-all leading-tight"
                        )

            if self.generated_mask_path:
                with ui.row().classes("w-full items-center gap-3 bg-blue-50 p-3 rounded border border-blue-200"):
                    ui.icon("check_circle", color="blue").classes("text-xl")
                    with ui.column().classes("gap-0 flex-1 min-w-0"):
                        ui.label("Mask Created").classes("text-xs font-bold text-blue-800 uppercase")
                        ui.label(os.path.basename(self.generated_mask_path)).classes(
                            "text-sm font-mono text-gray-700 truncate w-full"
                        )
                        ui.label(self.generated_mask_path).classes("text-[10px] text-gray-500 break-all leading-tight")

            # B. Mask Generation Section
            with ui.column().classes("w-full gap-2"):
                ui.label("3. Generate Mask").classes("text-xs font-bold text-gray-500 uppercase tracking-wide")

                with ui.card().classes("w-full border border-gray-200 p-4 shadow-sm"):
                    # The "Template Used" Reference
                    if self.generated_template_path:
                        white_ref = os.path.basename(self.generated_template_path.replace("_black.mrc", "_white.mrc"))
                        with ui.row().classes(
                            "w-full items-center gap-2 mb-4 bg-gray-50 p-2 rounded border border-gray-100"
                        ):
                            ui.icon("info", color="grey").classes("text-sm")
                            ui.label(f"Input for mask: {white_ref}").classes("text-xs font-mono text-gray-600")
                    else:
                        with ui.row().classes(
                            "w-full items-center gap-2 mb-4 bg-yellow-50 p-2 rounded border border-yellow-100"
                        ):
                            ui.icon("warning", color="orange").classes("text-sm")
                            ui.label("Please generate a template first.").classes("text-xs text-yellow-700 italic")

                    with ui.grid(columns=4).classes("w-full gap-4"):
                        ui.number("Threshold", value=0.001, step=0.001, format="%.4f").bind_value(
                            self, "mask_threshold"
                        ).tooltip("Binarization threshold")
                        ui.number("Extend (px)", value=3).bind_value(self, "mask_extend").tooltip("Dilate mask")
                        ui.number("Soft Edge (px)", value=6).bind_value(self, "mask_soft_edge").tooltip(
                            "Gaussian soft edge"
                        )
                        ui.number("Lowpass (Å)", value=20).bind_value(self, "mask_lowpass").tooltip(
                            "Filter before thresholding"
                        )

                    ui.separator().classes("my-4")

                    btn = ui.button("Generate Mask", icon="architecture", on_click=self.handle_mask).props(
                        "color=secondary unelevated w-full"
                    )

                    if not self.generated_template_path:
                        btn.disable()

    # --- Async Handlers ---

    async def handle_basic_shape(self):
        self.is_processing = True
        try:
            await asyncio.sleep(0.1)
            res = await self.backend.template_service.generate_basic_shape_async(
                self.basic_shape_def, self.pixel_size, self.output_folder, int(self.box_size)
            )
            if not res["success"]:
                raise Exception(res["error"])
            self._success_template(res)
        except Exception as e:
            self.safe_notify(str(e), type="negative")
            print(f"[UI ERROR] Shape generation failed: {e}")
        finally:
            self.is_processing = False

    async def handle_pdb(self):
        if not self.pdb_id:
            self.safe_notify("Enter PDB code or file path", type="warning")
            return

        self.is_processing = True
        try:
            await asyncio.sleep(0.1)
            pdb_target = self.pdb_id
            if not os.path.exists(pdb_target) and len(pdb_target) == 4:
                res_fetch = await asyncio.to_thread(self.backend.pdb_service.fetch_pdb, pdb_target, self.output_folder)
                if not res_fetch["success"]:
                    raise Exception(res_fetch["error"])
                pdb_target = res_fetch["path"]

            res_sim = await asyncio.to_thread(
                self.backend.pdb_service.simulate_map_from_pdb,
                pdb_target,
                self.pixel_size,
                int(self.box_size),
                self.output_folder,
            )
            if not res_sim["success"]:
                raise Exception(res_sim["error"])

            res_proc = await self.backend.template_service.process_volume_async(
                res_sim["path"], self.output_folder, self.pixel_size, int(self.box_size)
            )
            self._success_template(res_proc)

        except Exception as e:
            self.safe_notify(str(e), type="negative")
            print(f"[UI ERROR] PDB simulation failed: {e}")
        finally:
            self.is_processing = False

    async def handle_mask(self):
        if not self.generated_template_path:
            return

        self.is_processing = True
        try:
            await asyncio.sleep(0.1)

            input_vol = self.generated_template_path.replace("_black.mrc", "_white.mrc")
            if not os.path.exists(input_vol):
                input_vol = self.generated_template_path

            output_mask = input_vol.replace("_white.mrc", "_mask.mrc")
            if output_mask == input_vol:
                output_mask = input_vol.replace(".mrc", "_mask.mrc")

            res = await self.backend.template_service.create_mask_relion(
                input_vol, output_mask, self.mask_threshold, self.mask_extend, self.mask_soft_edge, self.mask_lowpass
            )

            if not res["success"]:
                raise Exception(res["error"])

            self.generated_mask_path = res["path"]

            # 1. Update UI (This kills the "Generate Mask" button)
            self.render_results_and_mask()

            # 2. Notify Safely (Using dialog context, not button context)
            self.safe_notify("Mask created successfully", type="positive")

        except Exception as e:
            self.safe_notify(str(e), type="negative")
            print(f"[UI ERROR] Mask generation failed: {e}")
        finally:
            self.is_processing = False

    def _success_template(self, res):
        self.generated_template_path = res["path_black"]
        if "path_white" in res:
            asyncio.create_task(self._update_threshold(res["path_white"]))

        self.render_results_and_mask()
        self.safe_notify("Template generated successfully", type="positive")

    async def _update_threshold(self, path):
        try:
            thr = await self.backend.template_service.calculate_auto_threshold_async(path)
            self.mask_threshold = round(thr, 4)
        except Exception as e:
            print(f"[UI WARN] Failed to auto-calc threshold: {e}")

    def finish(self):
        if self.on_success and self.generated_template_path:
            self.on_success(self.generated_template_path, self.generated_mask_path)
        self.dialog.close()
