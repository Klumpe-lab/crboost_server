#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from nicegui import app, ui

# --- 1. SETUP PATHS & IMPORTS ---
# Resolve absolute paths to ensure we find files regardless of execution dir
CURRENT_DIR = Path(__file__).parent.resolve()
STATIC_DIR = "/users/artem.kushner/dev/crboost_server/static"

# Add paths to find backend
sys.path.append(str(CURRENT_DIR))
sys.path.append(str(CURRENT_DIR.parent))

try:
    import backend
except ImportError:
    print("Warning: Backend module not found. Using mocks.")

    class backend:
        @staticmethod
        def fetch_pdb(*args):
            return False, "Backend missing"

        @staticmethod
        def simulate_map_from_pdb(*args):
            return False, "Backend missing"

        @staticmethod
        def download_emdb(*args):
            return False, "Backend missing"

        @staticmethod
        def process_volume_numpy(*args):
            return False, "Backend missing"

        @staticmethod
        def create_ellipsoid(*args):
            return False, "Backend missing"


# --- 2. CONFIGURATION ---
PORT = 8085
OUTPUT_DEFAULT = "tmpOut/templates"
TEMPLATE_DIR = Path("/users/artem.kushner/dev/crboost_server/projects/tmp0ut").resolve()

# 2a. Serve Filesystem (Mount Root to /fs)
# This allows http://localhost:8085/fs/users/... to access /users/...
app.add_static_files("/fs", "/")

# 2b. Serve Static HTML (Mount static folder)
# Ensure static/molstar.html exists at this path!
app.add_static_files("/static", str(STATIC_DIR))


# --- 3. VIEWER COMPONENT ---
class MolstarViewer:
    def __init__(self, height="600px"):
        self.container = ui.element("div").classes("w-full h-full bg-black relative")
        self.height = height
        self.debug_link = None

    def view(self, file_path: Path):
        self.container.clear()

        # Logic: /users/artem/file.mrc -> /fs/users/artem/file.mrc
        # lstrip('/') ensures we don't get //users...
        url_path = f"/fs/{str(file_path).lstrip('/')}"

        # Viewer URL
        viewer_url = f"/static/molstar.html?url={url_path}"

        print(f"[GUI] Loading Viewer URL: {viewer_url}")  # Debug print to console

        with self.container:
            # Add a debug link overlay (top right) to test if file is accessible
            ui.link("DEBUG: Open in Tab", viewer_url, new_tab=True).classes(
                "absolute top-0 right-0 z-50 text-xs text-white bg-red-500 px-1 opacity-50 hover:opacity-100"
            )

            # The Iframe
            ui.element("iframe").props(f'src="{viewer_url}"').style(
                f"width: 100%; height: {self.height}; border: none;"
            )


# --- 4. MAIN PAGE ---
@ui.page("/")
def main():
    # Main Container
    with ui.column().classes("w-full max-w-7xl mx-auto p-4"):
        ui.label("CryoBoost Template Factory").classes("text-2xl font-bold mb-4")

        # =========================================================
        # SECTION A: GENERATION TOOLS
        # =========================================================
        with ui.expansion("Generation Tools", value=True).classes("w-full mb-4 border rounded"):
            with ui.column().classes("p-4 w-full"):
                # Config Row
                with ui.row().classes("w-full gap-4 mb-4"):
                    template_apix = ui.number("Template Pixelsize (Å)", value=1.5, format="%.2f")
                    out_folder = ui.input("Output Folder", value=OUTPUT_DEFAULT).classes("flex-grow")

                # Tools Columns
                with ui.row().classes("w-full gap-4"):
                    # PDB Column
                    with ui.card().classes("w-1/2"):
                        ui.label("1. From PDB").classes("text-lg font-bold text-primary")
                        pdb_path = ui.input("PDB File Path").classes("w-full")

                        with ui.row().classes("w-full items-center"):
                            pdb_code = ui.input("PDB Code").classes("w-24")

                            async def fetch_pdb():
                                ui.notify(f"Fetching {pdb_code.value}...")
                                s, r = backend.fetch_pdb(pdb_code.value, out_folder.value)
                                if s:
                                    pdb_path.value = os.path.abspath(r)
                                    ui.notify("Fetched!", type="positive")
                                else:
                                    ui.notify(r, type="negative")

                            ui.button("Get", on_click=fetch_pdb).props("flat dense")

                        ui.separator().classes("my-2")
                        sim_res = ui.number("Res (Å)", value=20.0)

                        async def run_sim():
                            if not pdb_path.value:
                                return
                            name = Path(pdb_path.value).stem
                            out_name = f"{name}_sim_{sim_res.value}.mrc"
                            out_p = os.path.join(out_folder.value, out_name)
                            ui.notify("Simulating...")
                            s, m = backend.simulate_map_from_pdb(
                                pdb_path.value, out_p, template_apix.value, 96, sim_res.value, 0
                            )
                            if s:
                                ui.notify("Done", type="positive")
                                # Trigger refresh of browser (manual for now)
                            else:
                                ui.notify(m, type="negative")

                        ui.button("Simulate", on_click=run_sim).classes("w-full bg-primary text-white")

                    # Map Column
                    with ui.card().classes("w-1/2"):
                        ui.label("2. From Map").classes("text-lg font-bold text-secondary")
                        map_path = ui.input("Map File Path").classes("w-full")

                        with ui.row().classes("w-full items-center"):
                            eid = ui.input("EMDB ID").classes("w-24")

                            async def fetch_emdb():
                                ui.notify(f"Fetching {eid.value}...")
                                s, r = backend.download_emdb(eid.value, out_folder.value)
                                if s:
                                    map_path.value = os.path.abspath(r)
                                    ui.notify("Fetched!", type="positive")
                                else:
                                    ui.notify(r, type="negative")

                            ui.button("Get", on_click=fetch_emdb).props("flat dense")

                        ui.separator().classes("my-2")
                        tm_res = ui.number("Lowpass (Å)", value=30.0)

                        async def gen_temp():
                            if not map_path.value:
                                return
                            bn = Path(map_path.value).stem
                            out_b = os.path.join(out_folder.value, f"{bn}_black.mrc")
                            ui.notify("Processing...")
                            s, _ = backend.process_volume_numpy(
                                map_path.value, out_b, template_apix.value, 96, True, tm_res.value
                            )
                            if s:
                                ui.notify("Done", type="positive")
                            else:
                                ui.notify("Error", type="negative")

                        ui.button("Generate Template", on_click=gen_temp).classes("w-full bg-secondary text-white")

        # =========================================================
        # SECTION B: BROWSER & VIEWER
        # =========================================================
        ui.label("Template Browser").classes("text-xl font-bold mt-4 mb-2")

        # We define the columns first to establish layout structure
        with ui.row().classes("w-full h-[700px] border border-gray-300 rounded overflow-hidden"):
            # 1. Define Layout Areas
            left_drawer = ui.column().classes("w-1/4 h-full bg-gray-50 border-r p-0")
            right_view = ui.column().classes("w-3/4 h-full p-0 bg-black relative")

            # 2. Initialize Viewer in RIGHT Column
            with right_view:
                viewer = MolstarViewer(height="100%")
                # Default placeholder text
                with viewer.container:
                    ui.label("Select a file to view").classes("text-gray-500 m-auto")

            # 3. Initialize Browser in LEFT Column
            with left_drawer:
                ui.label(f"{TEMPLATE_DIR.name}/").classes("text-xs font-mono p-2 bg-gray-200 w-full")

                # File List Container
                file_list_area = ui.scroll_area().classes("w-full flex-grow")

                def refresh_files():
                    file_list_area.clear()
                    if not TEMPLATE_DIR.exists():
                        with file_list_area:
                            ui.label("Dir missing").classes("text-red-500 p-2")
                        return

                    files = sorted(
                        [f for f in TEMPLATE_DIR.glob("*") if f.suffix.lower() in [".mrc", ".map", ".rec", ".ccp4"]]
                    )

                    with file_list_area:
                        if not files:
                            ui.label("No files").classes("text-gray-400 italic p-2")
                        for f in files:
                            # CLICK HANDLER: Calls viewer.view(path) which updates the iframe in right col
                            ui.button(f.name, on_click=lambda p=f: viewer.view(p)).props("flat align=left").classes(
                                "w-full text-left normal-case text-sm px-3 py-1 hover:bg-gray-200 border-b border-gray-100"
                            )

                # Refresh Button
                ui.button("Refresh List", on_click=refresh_files).props("outline size=sm icon=refresh").classes(
                    "w-full m-1"
                )
                refresh_files()  # Load initially


if __name__ in {"__main__", "__mp_main__"}:
    print(f"Starting GUI on port {PORT}")
    print(f"Serving Static: {STATIC_DIR}")
    print(f"Mounting FS: /")
    ui.run(title="CryoBoost Templates", port=PORT)
