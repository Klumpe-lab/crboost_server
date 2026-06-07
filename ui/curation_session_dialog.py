"""Single curation control center for a ChimeraX+ArtiaX manual-picking session.

Option A (no in-crboost proxying): crboost submits the VNC desktop as a SLURM job
and hands the user the exact connection details — one SSH tunnel + a viewer + a
password. ONE dialog, opened from the journeys gallery's per-tomo "Curate in
ArtiaX" button (and the sidebar), ALWAYS shows session status; when live it shows
the full connection info AND — for a specific tomogram — the copy-paste commands
to load it + the `.coords` save target; when off, a Start button. The session is
per-project and reused (find_active_curation_session) — at most one lives at a
time, so reconnecting keeps the user's one tunnel + viewer.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from nicegui import ui

logger = logging.getLogger(__name__)

_BOX = "font-mono text-xs bg-gray-100 px-2 py-1 rounded flex-grow break-all"
_BLOCK = "font-mono text-xs bg-gray-100 px-2 py-1 rounded flex-grow whitespace-pre"


def _copy_js(text: str) -> str:
    return "navigator.clipboard.writeText(" + json.dumps(text) + ")"


async def open_curation_control_center(backend, project_path: Optional[Path], *, bundle: Optional[dict] = None) -> None:
    """Open the curation control center.

    ``bundle`` (from ``backend.prepare_curation_bundle``) ties the dialog to one
    (species, tomogram): Start preloads it, and the running view shows the paste-in
    commands to load it + the ``.coords`` save target. With no bundle (sidebar) it's
    a project-level session start/connect panel. The dialog checks liveness itself,
    so both entry points behave identically — there is no separate chooser.
    """
    cxc_path = (bundle or {}).get("cxc_path")
    commands = (bundle or {}).get("commands") or []
    manual_coords = (bundle or {}).get("manual_coords") or ""
    tomo_name = (bundle or {}).get("tomo_name") or ""
    auto_count = (bundle or {}).get("auto_count")
    cmd_block = "\n".join(commands)

    with (
        ui.dialog() as dialog,
        ui.card()
        .classes("w-[46rem] max-w-full")
        .style("max-height: 88vh; display: flex; flex-direction: column; gap: 8px;"),
    ):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("view_in_ar", color="indigo").classes("text-2xl")
            ui.label("Curate in ChimeraX + ArtiaX").classes("text-lg font-bold")
            if tomo_name:
                ui.label(tomo_name).classes("text-[11px] font-mono text-gray-500")
        body = ui.column().classes("w-full gap-2").style("overflow-y: auto; flex: 1 1 auto; min-height: 0;")
        with ui.row().classes("w-full justify-end mt-1"):
            ui.button("Close", on_click=lambda: (_stop_timer(), dialog.close())).props("flat")
    dialog.open()

    # done = connection info shown (stop polling); busy = an action is mid-flight
    # (re-entry guard so a double-click can't submit/stop twice).
    state = {"job_id": None, "session_dir": None, "timer": None, "done": False, "busy": False}

    def _stop_timer():
        if state["timer"] is not None:
            state["timer"].cancel()
            state["timer"] = None

    # ── copyable-at-all-times primitives ──────────────────────────────────────
    def _copy_btn(text: str, tip: str = "Copy"):
        ui.button(icon="content_copy", on_click=lambda t=text: ui.run_javascript(_copy_js(t))).props(
            "flat dense round"
        ).tooltip(tip)

    def _copy_row(text: str):
        with ui.row().classes("items-center gap-1 w-full"):
            ui.label(text).classes(_BOX)
            _copy_btn(text)

    def _kv_copy(key: str, value: str):
        with ui.row().classes("items-center gap-2 w-full"):
            ui.label(key).classes("text-sm text-gray-600").style("width: 84px; min-width: 84px;")
            ui.label(value).classes(_BOX)
            _copy_btn(value)

    def _cmd_box(block: str):
        with ui.row().classes("items-start gap-1 w-full"):
            ui.label(block).classes(_BLOCK)
            _copy_btn(block, "Copy all commands")

    def _step(badge: str, title: str):
        with ui.row().classes("items-center gap-2 mt-2"):
            ui.label(badge).classes(
                "flex items-center justify-center bg-blue-600 text-white rounded-full text-xs font-bold"
            ).style("width: 20px; height: 20px; min-width: 20px;")
            ui.label(title).classes("font-medium")

    def _load_steps_block():
        """The 'load THIS tomogram' instructions — paste-in for now (REST
        auto-dispatch is the planned upgrade). Shown live (actionable) and, in the
        off state, inside an expansion so the commands are copyable before launch."""
        ui.markdown(
            "**① switch to your VNC viewer · ② click the ChimeraX command line (bottom of the window) · ③ paste:**"
        ).classes("text-sm")
        _cmd_box(cmd_block)
        ui.markdown(
            "Then in the **ArtiaX** panel create a **new particle list**, pick into it (don't add to the auto "
            "list), and **save it as a `.coords`** at the path below — crboost ingests it back."
        ).classes("text-sm")
        if manual_coords:
            _kv_copy("Save to", manual_coords)

    # ── render states ─────────────────────────────────────────────────────────
    def render_waiting(msg: str):
        body.clear()
        with body:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="lg")
                ui.label(msg)
            ui.label("This usually takes a few seconds once a compute node is free. You can leave this open.").classes(
                "text-xs text-gray-400"
            )

    def render_error(msg: str):
        _stop_timer()
        body.clear()
        with body:
            ui.label("The session did not start").classes("text-red-600 font-medium")
            ui.label(msg or "(no detail)").classes(
                "font-mono text-xs whitespace-pre-wrap bg-gray-100 p-2 rounded w-full"
            )
            ui.button("Try again", icon="refresh", on_click=lambda: _start()).props("flat")

    def render_off():
        _stop_timer()
        body.clear()
        with body:
            with ui.row().classes("items-center gap-2"):
                ui.icon("radio_button_unchecked", color="grey").classes("text-2xl")
                ui.label("No curation session is running").classes("font-medium text-gray-700")
            if tomo_name:
                n = f" with {auto_count} auto picks" if auto_count is not None else ""
                ui.label(
                    f"Start one to open {tomo_name}{n} preloaded in ChimeraX + ArtiaX on a GPU node "
                    "(~10–30 s to land + start). The tunnel command + password appear right here once it's up."
                ).classes("text-sm text-gray-600")
            else:
                ui.label(
                    "Start one to open a ChimeraX + ArtiaX VNC desktop on a GPU node. The tunnel command + "
                    "password appear right here once it's up. (Open a tomogram from a species gallery's "
                    "“Curate in ArtiaX” to preload it.)"
                ).classes("text-sm text-gray-600")
            ui.button("Start session", icon="play_arrow", color="indigo", on_click=lambda: _start()).props("no-caps")
            if commands:
                with ui.expansion("What you'll do once it's open", icon="list").classes("w-full text-sm mt-1"):
                    _load_steps_block()

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
                ui.label(f"Session running on {node}").classes("font-medium text-green-700")
            ui.label(
                f"SLURM job {state['job_id']} — it stays up until you quit ChimeraX or press Stop; you can "
                "disconnect and reconnect the viewer without losing it."
            ).classes("text-xs text-gray-600")
            ui.separator()

            # ── CONNECT (the part the user needs and never saved) ──
            ui.label("Connect to it").classes("text-sm font-semibold text-gray-700")
            _step("1", "Get a VNC viewer (one-time)")
            ui.markdown(
                "A VNC viewer shows the remote ChimeraX window on your Mac. **Any** works — RealVNC, TigerVNC, "
                "TurboVNC — or macOS's built-in **Screen Sharing** (no install). You only need one."
            ).classes("text-sm")
            _step("2", "Open the SSH tunnel — one Terminal window, leave it open")
            ui.label(
                "Securely forwards a local port to the session's node. Paste it into Terminal and LEAVE THAT "
                "WINDOW OPEN for the whole session — it sits there with no output, which is correct (it's holding "
                "the tunnel). If it prompts, that's your normal cluster login."
            ).classes("text-xs text-gray-600")
            _copy_row(tunnel)
            _step("3", "Open the viewer and enter the password")
            ui.markdown(
                "**Any VNC viewer:** connect to the address below, then enter the password.  "
                f"**macOS Screen Sharing:** in Terminal run `open vnc://{viewer_addr}`, or in Finder press ⌘K "
                f"and enter `vnc://{viewer_addr}`."
            ).classes("text-sm")
            _kv_copy("Address", viewer_addr)
            _kv_copy("Password", password)
            ui.label("(One-time password, only for this session.)").classes("text-xs text-gray-400")
            ui.separator()

            # ── LOAD THIS TOMOGRAM (what to do next) ──
            if commands:
                ui.label("Load this tomogram").classes("text-sm font-semibold text-gray-700")
                if cxc_path:
                    ui.label(
                        "If you started the session from THIS tomogram, it's already loaded — give ChimeraX a few "
                        "seconds. Otherwise (or to reload it), paste the commands below."
                    ).classes("text-xs text-gray-500")
                _load_steps_block()
            else:
                ui.markdown(
                    "Open a tomogram from a species gallery's **Curate in ArtiaX** button to load it (and its "
                    "auto/curated picks) here with one paste — or in ChimeraX type "
                    "`artiax start` then `artiax open tomo <recon>.mrc` and `open <picks>.coords`."
                ).classes("text-sm text-gray-500")

            with ui.expansion("Troubleshooting", icon="help_outline").classes("w-full text-sm"):
                ui.markdown(
                    f"- **Viewer won't connect:** the Terminal running the tunnel (step 2) must still be open, and "
                    f"the address must be exactly `{viewer_addr}`.\n"
                    "- **Black or grey screen for a few seconds:** normal — the desktop and ChimeraX are still "
                    "starting; give it ~5–10 s.\n"
                    '- **"Connection refused":** the tunnel isn\'t up (step 2 closed/errored) or the session ended. '
                    "Reopen the tunnel, or press Stop and Start again.\n"
                    "- **Password rejected:** copy it again above — it's case-sensitive.\n"
                    f"- **Session details:** node `{node}`, rfb port `{port}`, login `{user}@{login_host}`."
                )

            with ui.row().classes("w-full justify-between items-center mt-2"):
                ui.label(f"SLURM job {state['job_id']} · node {node}").classes("text-xs text-gray-500")
                ui.button("Stop session", color="red", icon="stop", on_click=lambda: _stop()).props("flat dense")

    # ── actions (re-entry guarded) ────────────────────────────────────────────
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
        if state["busy"]:
            return
        state["busy"] = True
        try:
            state["done"] = False
            _stop_timer()
            render_waiting("Submitting curation session…")
            result = await backend.launch_curation_session(project_path, cxc_path=cxc_path)
            if not result.get("success"):
                render_error(result.get("error") or "launch failed")
                return
            state["job_id"] = result.get("slurm_job_id")
            state["session_dir"] = result.get("session_dir")
            render_waiting(f"Submitted SLURM job {state['job_id']} — waiting for a node…")
            await _poll()
            if not state["done"]:
                state["timer"] = ui.timer(3.0, _poll)
        finally:
            state["busy"] = False

    async def _stop():
        if state["busy"]:
            return
        state["busy"] = True
        try:
            if state["job_id"]:
                await backend.stop_curation_session(state["job_id"])
            _stop_timer()
            state.update({"job_id": None, "session_dir": None, "done": False})
            ui.notify("Curation session stopped", type="info")
            render_off()  # back to the start state — NOT an auto-relaunch (that was the double-submit)
        finally:
            state["busy"] = False

    # ── entry: status-first. Reconnect a live session (no sbatch) or show Start ──
    render_waiting("Checking for a running session…")
    active = await backend.find_active_curation_session(project_path) if project_path else None
    if active:
        state["job_id"] = active.get("slurm_job_id")
        state["session_dir"] = active.get("session_dir")
        await _poll()
        if not state["done"]:
            state["timer"] = ui.timer(3.0, _poll)
    else:
        render_off()
