"""Launch + connect UI for a ChimeraX+ArtiaX curation session.

Option A (no in-crboost proxying): crboost submits the VNC desktop as a SLURM
job and hands the user the exact connection details — one SSH tunnel + a viewer
+ a password. The single hop (your Mac → login node → compute node) is the same
`ssh -L` you'd run for anything else.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from nicegui import ui

logger = logging.getLogger(__name__)

_BOX = "font-mono text-xs bg-gray-100 px-2 py-1 rounded flex-grow break-all"


def _copy_js(text: str) -> str:
    return "navigator.clipboard.writeText(" + json.dumps(text) + ")"


async def open_curation_session_dialog(backend, project_path: Optional[Path]) -> None:
    # Scrollable card: the connect instructions are tall, and a NiceGUI dialog
    # won't scroll on its own — without max-height + overflow the lower steps
    # (the tunnel + password) get clipped off the bottom of the screen.
    with ui.dialog() as dialog, ui.card().classes("w-[46rem] max-w-full").style(
        "max-height: 88vh; display: flex; flex-direction: column; gap: 8px;"
    ):
        ui.label("ChimeraX + ArtiaX curation session").classes("text-lg font-bold")
        body = ui.column().classes("w-full gap-2").style("overflow-y: auto; flex: 1 1 auto; min-height: 0;")
        with ui.row().classes("w-full justify-end mt-1"):
            ui.button("Close", on_click=lambda: (_stop_timer(), dialog.close())).props("flat")
    dialog.open()

    state = {"job_id": None, "session_dir": None, "timer": None, "done": False}

    def _stop_timer():
        if state["timer"] is not None:
            state["timer"].cancel()
            state["timer"] = None

    def _copy_btn(text: str):
        ui.button(icon="content_copy", on_click=lambda t=text: ui.run_javascript(_copy_js(t))).props(
            "flat dense round"
        ).tooltip("Copy")

    def _copy_row(text: str):
        with ui.row().classes("items-center gap-1 w-full"):
            ui.label(text).classes(_BOX)
            _copy_btn(text)

    def _kv_copy(key: str, value: str):
        with ui.row().classes("items-center gap-2 w-full"):
            ui.label(key).classes("text-sm text-gray-600").style("width: 84px; min-width: 84px;")
            ui.label(value).classes(_BOX)
            _copy_btn(value)

    def _step(badge: str, title: str):
        with ui.row().classes("items-center gap-2 mt-2"):
            ui.label(badge).classes(
                "flex items-center justify-center bg-blue-600 text-white rounded-full text-xs font-bold"
            ).style("width: 20px; height: 20px; min-width: 20px;")
            ui.label(title).classes("font-medium")

    def render_waiting(msg: str):
        body.clear()
        with body:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="lg")
                ui.label(msg)
            ui.label(
                "This usually takes a few seconds once a compute node is free. You can leave this open."
            ).classes("text-xs text-gray-400")

    def render_error(msg: str):
        _stop_timer()
        body.clear()
        with body:
            ui.label("The session did not start").classes("text-red-600 font-medium")
            ui.label(msg or "(no detail)").classes(
                "font-mono text-xs whitespace-pre-wrap bg-gray-100 p-2 rounded w-full"
            )
            ui.button("Retry", icon="refresh", on_click=_start).props("flat")

    def render_ready(info: dict):
        _stop_timer()
        body.clear()
        tunnel = info.get("tunnel_cmd") or ""
        port = info.get("port")
        password = info.get("password") or ""
        node = info.get("node") or "?"
        login_host = info.get("login_host") or ""
        user = info.get("user") or ""
        viewer_addr = f"localhost:{port}"
        with body:
            with ui.row().classes("items-center gap-2"):
                ui.icon("check_circle", color="green").classes("text-2xl")
                ui.label("Your ChimeraX + ArtiaX session is running.").classes("font-medium text-green-700")
            ui.label(
                f"It's on compute node {node} (SLURM job {state['job_id']}). It stays up until you quit "
                "ChimeraX or press Stop — you can disconnect and reconnect the viewer without losing it."
            ).classes("text-xs text-gray-600")
            ui.separator()

            _step("1", "Get a VNC viewer (one-time)")
            ui.markdown(
                "A VNC viewer shows the remote ChimeraX window on your Mac. **Any** VNC viewer works — "
                "e.g. RealVNC Viewer, TigerVNC, or TurboVNC — or macOS's built-in **Screen Sharing** "
                "(no install needed). You only need one."
            ).classes("text-sm")

            _step("2", "Open the SSH tunnel — one Terminal window on your Mac")
            ui.label(
                "This securely forwards a local port to the session's node. Paste it into Terminal and "
                "LEAVE THAT WINDOW OPEN for the whole session — it will sit there with no output, which is "
                "correct (it's holding the tunnel). If it prompts, that's your normal cluster login."
            ).classes("text-xs text-gray-600")
            _copy_row(tunnel)

            _step("3", "Open the viewer and enter the password")
            ui.markdown(
                "**Any VNC viewer:** open it, connect to the address below, then enter the password.\n\n"
                "**macOS Screen Sharing (no install):** in Terminal run "
                f"`open vnc://{viewer_addr}`, or in Finder press ⌘K and enter `vnc://{viewer_addr}`."
            ).classes("text-sm")
            _kv_copy("Address", viewer_addr)
            _kv_copy("Password", password)
            ui.label("(One-time password, only for this session.)").classes("text-xs text-gray-400")
            ui.separator()

            _step("✓", "Once it opens — in ChimeraX / ArtiaX")
            ui.markdown(
                "- Open your tomogram: **File ▸ Open**, or type `open /path/to/tomogram.mrc` in the ChimeraX "
                "command line at the bottom of the window.\n"
                "- Open auto/curated picks the same way: `open picks.coords`.\n"
                "- Use the **ArtiaX** panel to scrub slices and place/curate particles, then save the list "
                "as a `.coords` file on the cluster.\n"
                "- *(crboost will soon pre-load the right tomogram + picks for you — for now open them manually.)*"
            ).classes("text-sm")

            _step("⏹", "When you're done")
            ui.label(
                "Quit ChimeraX (File ▸ Quit) or press Stop below — either one frees the compute node. "
                "Closing this dialog leaves the session running so you can reconnect later."
            ).classes("text-xs text-gray-600")

            with ui.expansion("Troubleshooting", icon="help_outline").classes("w-full text-sm"):
                ui.markdown(
                    f"- **Viewer won't connect:** the Terminal running the tunnel (step 2) must still be open, "
                    f"and the address must be exactly `{viewer_addr}`.\n"
                    "- **Black or grey screen for a few seconds:** normal — the desktop and ChimeraX are still "
                    "starting; give it ~5–10 s.\n"
                    "- **\"Connection refused\":** the tunnel isn't up (step 2 was closed or errored) or the "
                    "session ended. Reopen the tunnel, or relaunch from the button.\n"
                    "- **Password rejected:** copy it again above — it's case-sensitive.\n"
                    f"- **Session details:** node `{node}`, rfb port `{port}`, login `{user}@{login_host}`."
                )

            with ui.row().classes("w-full justify-between items-center mt-2"):
                ui.label(f"SLURM job {state['job_id']} · node {node}").classes("text-xs text-gray-500")
                ui.button("Stop session", color="red", icon="stop", on_click=_stop).props("flat dense")

    async def _stop():
        if state["job_id"]:
            await backend.stop_curation_session(state["job_id"])
        _stop_timer()
        ui.notify("Curation session stopped", type="info")
        dialog.close()

    async def _poll():
        if state["done"] or not state["session_dir"]:
            return
        info = await backend.get_curation_session_info(state["session_dir"], state["job_id"])
        status = info.get("status")
        if status == "ready":
            state["done"] = True
            render_ready(info)
        elif status == "pending":
            render_waiting(f"Waiting for a compute node (SLURM {info.get('slurm_state', 'PENDING')})…")
        elif status == "starting":
            render_waiting("Node allocated — starting ChimeraX…")
        elif status == "ended":
            state["done"] = True
            render_error(info.get("detail") or "The session job ended before it came up.")

    async def _start():
        state["done"] = False
        _stop_timer()
        render_waiting("Submitting curation session…")
        result = await backend.launch_curation_session(project_path)
        if not result.get("success"):
            render_error(result.get("error") or "launch failed")
            return
        state["job_id"] = result.get("slurm_job_id")
        state["session_dir"] = result.get("session_dir")
        render_waiting(f"Submitted SLURM job {state['job_id']} — waiting for a node…")
        await _poll()
        if not state["done"]:
            state["timer"] = ui.timer(3.0, _poll)

    await _start()
