"""napari for one tomogram: start napari on a GPU node showing it, or show it in the napari already running.

crboost does not proxy the desktop: napari runs on a VNC desktop as a SLURM job (the curation worker with
CB_APP=napari, services/curation/session_service.py) and this dialog hands over what to connect with, an SSH
tunnel, an address and a password, the same three the ChimeraX control center gives. A user has one napari
session. While it runs, any tomogram's napari chip offers to show that tomogram in it, which replaces the layers
crboost opened there; layers the user made stay.

The dialog keeps its shape: the connection rows are built once and filled when the session comes up, and one
poll (every 3 s while the dialog is open) moves the status, the rows and the line saying what the window shows.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from nicegui import ui

from services.configs.config_service import get_config_service

from ui.components.buttons import house_button, icon_button
from ui.components.copyable import _write_clipboard
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.job_plugins._field_styles import CLR_VALUE, HELPER_STYLE, LABEL_STYLE, MONO, PANEL_TITLE_STYLE, ROW_STYLE

logger = logging.getLogger(__name__)

POLL_S = 3.0
_VALUE_STYLE = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 1 1 0; min-width: 0; word-break: break-all;"
_STATUS = {
    "off": ("○ No session", "cb-badge-todo"),
    "pending": ("◌ Queued", "cb-badge-queued"),
    "starting": ("◌ Starting", "cb-badge-stale"),
    "ready": ("● Live", "cb-badge-ok"),
    "ended": ("✕ Ended", "cb-badge-err"),
}
_flight = SingleFlight()


def _when(iso: str | None) -> str:
    """SLURM's start estimate as the clock time, with the date when it is not today."""
    if not iso:
        return ""
    try:
        t = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    return t.strftime("%H:%M") if t.date() == datetime.now().date() else t.strftime("%d %b %H:%M")


def _showing(info: dict, status: str) -> tuple[str, str]:
    """What the window shows, as (text, badge class): the request it answered, or the one it has not yet."""
    requested = info.get("requested") or {}
    opened = info.get("opened") or {}
    if not requested:
        return "", ""
    title = requested.get("title") or "?"
    if opened.get("seq") != requested.get("seq"):
        if status == "ready":
            return f"Opening {title}…", "cb-badge-queued"
        return f"Opens {title} once the session is up.", "cb-badge-todo"
    if not opened.get("ok"):
        return f"Not opened: {opened.get('error') or 'no volume could be read'}", "cb-badge-err"
    layer = (opened.get("layers") or [{}])[0]
    z, y, x = layer.get("shape") or (0, 0, 0)
    voxel = layer.get("voxel_size")
    software = "llvmpipe" in str(opened.get("renderer") or "").lower()
    tail = " · software rendering" if software else ""
    if not voxel:
        return f"Showing {title} · {x} × {y} × {z} · voxel size unknown (the header has none){tail}", "cb-badge-stale"
    return f"Showing {title} · {voxel[2]:.2f} Å/px · {x} × {y} × {z}{tail}", "cb-badge-ok"


async def open_napari_dialog(backend, volume: str | Path) -> None:
    """The dialog behind a tomogram's napari chip, for the volume at `volume` (an absolute path)."""
    async with _flight("open") as acquired:
        if not acquired:
            return
        if backend is None:
            ui.notify("The server's backend is not running.", type="negative")
            return
        path = str(volume)
        name = Path(path).stem
        volumes = [{"name": name, "path": path}]
        config = get_config_service()
        passwordless = config.curation.passwordless_vnc
        software_offered = bool(config.napari.software.partition)
        active = await backend.find_active_napari_session()
        _build(backend, path, name, volumes, passwordless, software_offered, active)


def _build(
    backend, path: str, name: str, volumes: list[dict], passwordless: bool, software_offered: bool, active: dict | None
) -> None:
    sv = {"session_dir": None, "job_id": None, "status": "off", "tunnel": "", "address": "", "password": ""}
    state = {"timer": None, "polling": False, "closed": False}
    flight = SingleFlight()

    with (
        dialog_host(),
        ui.dialog() as dialog,
        ui.card().style("width: 520px; max-width: 94vw; padding: 14px 16px; gap: 10px;"),
    ):
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px;"):
            ui.label("napari").style(PANEL_TITLE_STYLE)
            ui.label(name).style(f"{MONO} font-size: 11px; color: {CLR_VALUE}; min-width: 0;").classes(
                "ellipsis"
            ).tooltip(path)
            ui.space()
            status_lbl = ui.label("").style(f"{MONO} font-size: 10px; white-space: nowrap;")
            with status_lbl:
                status_tip = ui.tooltip("")
        showing_lbl = ui.label("").style(f"{MONO} font-size: 10px; line-height: 1.45;")

        ui.label(
            "napari runs on a cluster node and you see it through a VNC viewer. Once per session: paste the tunnel "
            "into a terminal on your computer, then open the address in the viewer and give the password."
        ).style(f"{HELPER_STYLE} line-height: 1.45;")

        def _row(label: str, key: str, tip: str):
            with ui.row().classes("no-wrap").style(ROW_STYLE + " align-items: center;"):
                ui.label(label).style(LABEL_STYLE + " width: 64px;")
                value = ui.label("—").style(_VALUE_STYLE)

                async def _copy() -> None:
                    if sv[key]:
                        await _write_clipboard(sv[key])

                icon_button("content_copy", _copy, tooltip=tip)
            return value

        tunnel_lbl = _row("Tunnel", "tunnel", "Copy the tunnel command")
        address_lbl = _row("Address", "address", "Copy the VNC address")
        password_lbl = _row("Password", "password", "Copy the password")
        viewer_lbl = ui.label("").style(f"{HELPER_STYLE} line-height: 1.45;")

        with ui.row().classes("w-full items-center no-wrap").style("gap: 6px; margin-top: 4px;"):
            stop_btn = house_button("Stop session", lambda: _stop(), kind="danger")
            ui.space()
            house_button("Close", lambda: dialog.close())
            gpu_btn = house_button(
                "Start on a full GPU",
                lambda: _act(software=False),
                tooltip="VirtualGL on a full B200, A100 or RTX 6000: 3-D at full resolution. It waits while those "
                "cards are taken, which can be hours.",
            )
            action_btn = house_button("Start now", lambda: _act(software=software_offered), kind="accent")
            with action_btn:
                action_tip = ui.tooltip("")

    def _apply(info: dict | None = None) -> None:
        status = sv["status"]
        text, klass = _STATUS[status]
        info_ = info or {}
        node = info_.get("node")
        if node and status in ("starting", "ready"):
            text = f"{text} on {node}"
        elif status == "pending" and info_.get("expected_start"):
            text = f"{text}, start expected {_when(info_['expected_start'])}"
        status_lbl.set_text(text)
        status_lbl.classes(replace=klass)
        tip = f"SLURM job {sv['job_id']}" if sv["job_id"] else "No napari session is running."
        if status == "pending" and info_.get("slurm_reason"):
            tip += f", waiting: {info_['slurm_reason']} (Priority/Resources: the nodes it may use are taken)"
            if software_offered:
                tip += ". To look now, stop it and use Start now."
        status_tip.set_text(tip)
        live = status in ("pending", "starting", "ready")
        stop_btn.set_visibility(live)
        gpu_btn.set_visibility(software_offered and not live)
        action_btn.set_text("Show this tomogram" if live else ("Start now" if software_offered else "Start napari"))
        if live:
            action_tip.set_text("Replace what the running napari shows with this tomogram; layers you made stay.")
        elif software_offered:
            action_tip.set_text(
                "Start napari at once with software rendering (on an interactive MIG slice): 2-D as quick as on a "
                "full GPU, 3-D from a half-size copy."
            )
        else:
            action_tip.set_text("Start napari on a GPU node, showing this tomogram. It waits while the GPUs are taken.")
        if status == "ready" and info is not None:
            sv["tunnel"] = info.get("tunnel_cmd") or ""
            sv["address"] = f"localhost:{info['port']}" if info.get("port") else ""
            sv["password"] = info.get("password") or ""
        elif status != "ready":
            sv.update(tunnel="", address="", password="")
        tunnel_lbl.set_text(sv["tunnel"] or "—")
        address_lbl.set_text(sv["address"] or "—")
        if status == "ready" and passwordless:
            password_lbl.set_text("none: curation.passwordless_vnc is on, anyone reaching the port gets the desktop")
        else:
            password_lbl.set_text(sv["password"] or "—")
        screen_sharing = f" at vnc://{sv['address']}" if sv["address"] else ""
        viewer_lbl.set_text(
            f"Any VNC viewer: TurboVNC, TigerVNC, RealVNC, or the Mac's Screen Sharing{screen_sharing}."
        )
        if info is not None:
            if status == "ended":
                showing_lbl.set_text(f"The session ended. {info.get('detail') or ''}".strip())
                showing_lbl.classes(replace="cb-badge-err")
            else:
                text, klass = _showing(info, status)
                showing_lbl.set_text(text)
                showing_lbl.classes(replace=klass)

    def _start_polling() -> None:
        if state["timer"] is None and not state["closed"]:
            with dialog:
                state["timer"] = ui.timer(POLL_S, _poll)

    def _stop_polling() -> None:
        if state["timer"] is not None:
            state["timer"].cancel()
            state["timer"] = None

    async def _poll() -> None:
        if state["polling"] or not sv["session_dir"]:
            return
        state["polling"] = True
        try:
            info = await backend.get_napari_session_info(sv["session_dir"], sv["job_id"])
        finally:
            state["polling"] = False
        if state["closed"]:
            return
        sv["status"] = info.get("status") if info.get("status") in _STATUS else "pending"
        if sv["status"] == "ended":
            _stop_polling()
            sv["session_dir"] = sv["job_id"] = None
        _apply(info)

    async def _act(software: bool = False) -> None:
        async with flight("act") as acquired:
            if not acquired:
                return
            if sv["session_dir"]:
                res = await backend.open_in_napari_session(sv["session_dir"], volumes, name)
            else:
                res = await backend.launch_napari_session(volumes, name, software=software)
                if res.get("success"):
                    sv.update(session_dir=res["session_dir"], job_id=res["slurm_job_id"], status="pending")
            if not res.get("success"):
                ui.notify(res.get("error") or "napari could not be started.", type="negative", multi_line=True)
                showing_lbl.set_text(res.get("error") or "")
                showing_lbl.classes(replace="cb-badge-err")
                return
            _apply()
            _start_polling()
            await _poll()

    async def _stop() -> None:
        async with flight("stop") as acquired:
            if not acquired or not sv["job_id"]:
                return
            res = await backend.stop_napari_session(sv["job_id"])
            if not res.get("success"):
                ui.notify(res.get("error") or "The session could not be stopped.", type="negative")
                return
            _stop_polling()
            sv.update(session_dir=None, job_id=None, status="off")
            showing_lbl.set_text("")
            _apply()

    def _on_value(e) -> None:
        if e.value:
            return
        state["closed"] = True
        _stop_polling()
        dialog.delete()

    dialog.on_value_change(_on_value)

    if active:
        sv.update(session_dir=active.get("session_dir"), job_id=active.get("slurm_job_id"))
        sv["status"] = "ready" if active.get("status") == "ready" else "pending"
        _apply(active)
        _start_polling()
    else:
        _apply()
    dialog.open()
