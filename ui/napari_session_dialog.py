"""napari for one tomogram: show it in the viewer already running, or start one.

The viewer is EisGreis (conf.yaml `napari.eisgreis`), and it runs in one of two places:

- the user's own remote desktop of the site's Open OnDemand portal, where the user started `eisgreis-crboost` (or
  the desktop did, User settings): crboost finds it (services/curation/napari_link.py) and *Show in my desktop*
  writes the tomogram into it. With none running, the dialog says how to start one (*In your desktop*).
- a VNC desktop crboost starts on a GPU node as a SLURM job (the curation worker with CB_APP=napari,
  services/curation/session_service.py). crboost does not proxy that desktop: where the site has a portal (conf.yaml
  `ood:`), *Open in browser* opens it in a tab through the portal; elsewhere this dialog hands over an SSH tunnel, an
  address and a password, the same three the ChimeraX control center gives. With the portal, the napari chip itself
  goes straight to the tab (ui/napari_open_page.py) and this dialog sits behind the chip's ▾.

A user has one viewer at a time (the newest started). While it runs, any tomogram's napari chip offers to show that
tomogram in it, which replaces the tomogram crboost opened there; the user's own layers stay.

The dialog keeps its shape: a header with the status pill, then *Session* (where and how it runs) and *Showing*
(what the window holds, where its saves go, what crboost added) as label/value rows, then the connection rows where
there is no portal, or *In your desktop* while nothing runs. Every row is built once and filled; one poll (every 3 s
while the dialog is open) moves them.
"""

from __future__ import annotations

import html
import logging
from datetime import datetime
from pathlib import Path

from nicegui import ui

from services.configs.config_service import get_config_service

from ui.components.buttons import house_button, icon_button, link_to, set_button_kind
from ui.components.copyable import _write_clipboard
from ui.components.dialogs import dialog_host
from ui import napari_status
from ui.components.reactive import SingleFlight
from ui.job_plugins._field_styles import (
    CLR_SUBLABEL,
    CLR_VALUE,
    HELPER_STYLE,
    LABEL_STYLE,
    LABEL_VOICE,
    MONO,
    PANEL_TITLE_STYLE,
    ROW_STYLE,
    group_title,
)

logger = logging.getLogger(__name__)

POLL_S = 3.0
_VALUE_STYLE = f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 1 1 0; min-width: 0; word-break: break-all;"
# One line, cut with an ellipsis; the whole value is in the row's tooltip.
_CELL_STYLE = (
    f"{MONO} font-size: 11px; color: {CLR_VALUE}; flex: 1 1 0; min-width: 0; "
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
)
_KEY_STYLE = LABEL_STYLE + " width: 72px; flex-shrink: 0;"
_PILL_STYLE = (
    f"{MONO} font-size: 10px; white-space: nowrap; padding: 1px 9px; border-radius: 10px; "
    "border: 1px solid currentColor; line-height: 16px;"
)
_STATUS = {
    "off": ("○ No session", "cb-badge-todo"),
    "pending": ("◌ Queued", "cb-badge-queued"),
    "starting": ("◌ Starting", "cb-badge-stale"),
    "ready": ("● Live", "cb-badge-ok"),
    "ended": ("✕ Ended", "cb-badge-err"),
}
_flight = SingleFlight()


def when_text(iso: str | None) -> str:
    """SLURM's start estimate as the clock time, with the date when it is not today."""
    if not iso:
        return ""
    try:
        t = datetime.fromisoformat(iso)
    except ValueError:
        return iso
    return t.strftime("%H:%M") if t.date() == datetime.now().date() else t.strftime("%d %b %H:%M")


def _showing(info: dict, status: str) -> dict[str, tuple[str, str]]:
    """The *Showing* rows as {row: (text, badge class)}: the tomogram (or the request it has not answered yet), its
    grid, where the colleague's viewer saves, and what crboost added. Rows it cannot fill are absent."""
    requested = info.get("requested") or {}
    opened = info.get("opened") or {}
    if not requested:
        return {}
    title = requested.get("title") or "?"
    if opened.get("seq") != requested.get("seq"):
        if status == "ready":
            return {"tomogram": (f"Opening {title}…", "cb-badge-queued")}
        return {"tomogram": (f"{title}, once the session is up", "cb-badge-todo")}
    if not opened.get("ok"):
        return {"tomogram": (f"Not opened: {opened.get('error') or 'no volume could be read'}", "cb-badge-err")}
    rows = {"tomogram": (title, "")}
    layer = (opened.get("layers") or [{}])[0]
    z, y, x = layer.get("shape") or (0, 0, 0)
    voxel = layer.get("voxel_size")
    if voxel:
        rows["grid"] = (f"{x} × {y} × {z} · {voxel[2]:.2f} Å/px", "")
    else:
        rows["grid"] = (f"{x} × {y} × {z} · voxel size unknown (the header has none)", "cb-badge-stale")
    if opened.get("viewer") in ("eisgreis", "companion"):
        # Where its comments and particle STARs go: the tomogram's project, or the session folder outside every one.
        if opened.get("outputs") and requested.get("outputs"):
            rows["saves"] = (str(opened["outputs"]), "")
        elif opened.get("outputs"):
            rows["saves"] = (f"{opened['outputs']} (the tomogram is outside every project)", "cb-badge-stale")
        else:
            rows["saves"] = ("the tomogram is outside every project", "cb-badge-stale")
        bundle = opened.get("bundle")
        if isinstance(bundle, dict) and bundle.get("error"):
            rows["crboost"] = (f"not shown: {bundle['error']}", "cb-badge-err")
        elif isinstance(bundle, dict):
            n_picks, n_segs = len(bundle.get("picks") or []), len(bundle.get("segmentations") or [])
            text = f"{n_picks} pick layer{'' if n_picks == 1 else 's'}"
            text += f" · {n_segs} segmentation{'' if n_segs == 1 else 's'}"
            volumes = bundle.get("volumes") or []
            denoised = [v.split(" (")[0].removeprefix("denoised · ") for v in volumes if "denoised" in v]
            if denoised:
                text += f" · denoised: {', '.join(denoised)} (the viewer's crboost volume dock)"
            left_out = len(bundle.get("notes") or []) + len(bundle.get("not_shown") or [])
            rows["crboost"] = (text + (f" · {left_out} left out" if left_out else ""), "")
    return rows


def _rendering(info: dict) -> str:
    """How the window draws: software (a MIG slice, no GPU graphics) or the card VirtualGL found."""
    renderer = str((info.get("opened") or {}).get("renderer") or "")
    if not renderer:
        return ""
    if "llvmpipe" in renderer.lower():
        return "software (CPU): 2-D quick, 3-D from binned copies"
    return renderer.split("/")[0]


def _faq_link(url: str) -> str:
    url = html.escape(url, quote=True)
    style = "color: inherit; text-decoration: underline;"
    return f'<a href="{url}" target="_blank" rel="noopener" style="{style}">{url}</a>'


def connect_help(portal_url: str | None, walltime: str) -> None:
    """The *Can't connect?* section, the dialog's and the open page's: what to do when the desktop does not show,
    or its tab or viewer was closed. `portal_url`: the OnDemand portal (conf.yaml `ood.url`), None without one."""
    reopen = (
        "napari keeps running. Open it again from any tomogram's napari chip (that also shows that tomogram), or "
        "with Open in browser in the session dialog behind the chip's ▾. The Desktop row there copies this "
        "session's link; a new session gets a new one."
    )
    if portal_url:
        items = [
            (
                "noVNC says “Failed to connect to server”",
                f"You are not signed in to OnDemand, or the sign-in expired. Open {_faq_link(portal_url)}, sign in, "
                "then reload the napari tab (or open it again as below).",
            ),
            ("I closed the napari tab", reopen),
        ]
    else:
        items = [
            (
                "The viewer cannot connect",
                "The tunnel must stay open: run it again in a terminal on your computer, then connect the viewer "
                "to the address with the password.",
            ),
            ("I closed the viewer", "napari keeps running: connect again with the same address and password."),
        ]
    items += [
        (
            "The desktop is black, or says Disconnected",
            f"The session may have ended: it stops after its time limit ({html.escape(walltime)}) or on Stop. The "
            "session dialog's status says; start again from a tomogram's napari chip.",
        ),
        (
            "Several tabs, or done",
            "Every tab shows the same desktop, and closing one stops nothing. When you are done, Stop session in "
            "the dialog: the session holds its node until then.",
        ),
    ]
    with ui.column().classes("w-full").style("gap: 5px;"):
        group_title("Can't connect?")
        for question, answer in items:
            with ui.column().classes("w-full").style("gap: 0;"):
                ui.label(question).style(f"{LABEL_VOICE} line-height: 1.4;")
                # Our own text, the portal URL escaped in _faq_link.
                ui.html(answer, sanitize=False).style(f"{HELPER_STYLE} line-height: 1.45;")


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
        portal = config.ood.url if config.ood.enabled else None
        active = await backend.find_active_napari_session()
        _build(backend, path, name, volumes, passwordless, software_offered, portal, config.napari.time, active)


def desktop_form_url() -> str | None:
    """The portal page where a user opens a Remote Desktop: the form conf.yaml names, else the portal itself; None
    without a portal."""
    config = get_config_service()
    if not config.ood.url:
        return None
    form = config.napari.desktop.form
    return config.ood.url.rstrip("/") + (form if form.startswith("/") else f"/{form}") if form else config.ood.url


def desktop_command() -> str | None:
    """What a user runs in a desktop's terminal so crboost can show tomograms there; None without an install."""
    install = get_config_service().napari.eisgreis
    return f"{Path(install).expanduser() / 'eisgreis-crboost'}" if install else None


def desktop_help(portal_url: str | None) -> None:
    """*In your desktop*: how to start EisGreis in the user's own remote desktop, where crboost then finds it. The
    dialog's and the open page's."""
    config = get_config_service()
    command = desktop_command()
    hint = config.napari.desktop.hint
    with ui.column().classes("w-full").style("gap: 3px;"):
        group_title(
            "In your desktop",
            tooltip="EisGreis in a Remote Desktop of your own: no job of crboost's, and the desktop's resources.",
        )
        steps = []
        if portal_url:
            steps.append(
                f"Open a Remote Desktop at {_faq_link(portal_url)}" + (f": {html.escape(hint)}." if hint else ".")
            )
        else:
            steps.append("Open a remote desktop on the cluster" + (f": {html.escape(hint)}." if hint else "."))
        steps.append(
            "In its terminal run the command below; leave it running. EisGreis keeps a tomogram's FFT in memory from "
            "24 GB, and draws 3-D on the GPU in a desktop with a full GPU."
        )
        steps.append(
            "Then a tomogram's napari chip shows it there. To have every desktop start it, waiting with no window: "
            "User settings (the person icon on the start page)."
        )
        for i, step in enumerate(steps, start=1):
            # Our own text, the portal URL and the site's hint escaped.
            ui.html(f"{i}. {step}", sanitize=False).style(f"{HELPER_STYLE} line-height: 1.45;")
        if command:
            with ui.row().classes("no-wrap").style(ROW_STYLE + " align-items: center;"):
                ui.label("Command").style(_KEY_STYLE)
                ui.label(command).style(_CELL_STYLE).tooltip(command)

                async def _copy() -> None:
                    await _write_clipboard(command)

                icon_button("content_copy", _copy, tooltip="Copy the command")
        else:
            ui.label(
                "napari.eisgreis is not set in conf.yaml: ./crboost eisgreis-install installs EisGreis and prints "
                "the line."
            ).style(f"{HELPER_STYLE} line-height: 1.45;").classes("cb-badge-err")


def _build(
    backend,
    path: str,
    name: str,
    volumes: list[dict],
    passwordless: bool,
    software_offered: bool,
    portal: str | None,
    walltime: str,
    active: dict | None,
) -> None:
    sv = {
        "session_dir": None,
        "job_id": None,
        "route": None,  # "desktop" (the user's own) or "job" (a node of ours)
        "status": "off",
        "tunnel": "",
        "address": "",
        "password": "",
        "browser_url": "",
        "saves": "",
    }
    state = {"timer": None, "polling": False, "closed": False, "linked": ""}
    flight = SingleFlight()
    # Without an install crboost can start no viewer; one already running in a desktop is still reachable.
    installed = bool(get_config_service().napari.eisgreis)

    with (
        dialog_host(),
        ui.dialog() as dialog,
        ui.card().style("width: 560px; max-width: 94vw; padding: 16px 18px; gap: 12px;"),
    ):
        how = (
            "napari (EisGreis) runs in your own remote desktop, or crboost starts it on a cluster node, which opens in "
            "a browser tab through the cluster's OnDemand portal (the first time, it asks you to sign in)."
            if portal
            else "napari (EisGreis) runs in your own remote desktop, or crboost starts it on a cluster node, which you "
            "see through a VNC viewer: once per session, paste the tunnel into a terminal on your computer, then open "
            "the address in the viewer and give the password."
        )
        with ui.row().classes("w-full items-center no-wrap").style("gap: 8px;"):
            ui.label("napari").style(PANEL_TITLE_STYLE).tooltip(how)
            ui.label(name).style(f"{MONO} font-size: 11px; color: {CLR_SUBLABEL}; min-width: 0;").classes(
                "ellipsis"
            ).tooltip(path)
            ui.space()
            status_lbl = ui.label("").style(_PILL_STYLE)
            with status_lbl:
                status_tip = ui.tooltip("")

        def _kv(label: str, copy_key: str | None = None, tip: str = ""):
            """A label/value row; the value cut to one line, whole in its tooltip. `copy_key`: a copy button for
            that value of `sv`."""
            with ui.row().classes("no-wrap").style(ROW_STYLE + " align-items: center;"):
                ui.label(label).style(_KEY_STYLE)
                value = ui.label("—").style(_CELL_STYLE)
                with value:
                    value_tip = ui.tooltip("")
                if copy_key:

                    async def _copy() -> None:
                        if sv[copy_key]:
                            await _write_clipboard(sv[copy_key])

                    icon_button("content_copy", _copy, tooltip=tip)
            return value, value_tip

        def _fill(cell, text: str, klass: str = "") -> None:
            value, value_tip = cell
            value.set_text(text or "—")
            value.classes(replace=klass)
            value_tip.set_text(text)

        with ui.column().classes("w-full").style("gap: 2px;"):
            group_title("Session")
            node_cell = _kv("Node")
            render_cell = _kv("Rendering")
            job_cell = _kv("SLURM job")
            # The desktop's link, to open again after its tab was closed. Shown without its fragment, which holds
            # the VNC password; the copy is the whole link.
            desktop_cell = (
                _kv("Desktop", "browser_url", "Copy the link to napari's desktop (this session only)")
                if portal
                else None
            )
        with ui.column().classes("w-full").style("gap: 2px;"):
            group_title("Showing")
            tomo_cell = _kv("Tomogram")
            grid_cell = _kv("Grid")
            saves_cell = _kv("Saves to", "saves", "Copy the folder")
            crboost_cell = _kv("From crboost")

        # With the portal the desktop listens on the node's localhost only, so there is no tunnel to hand over
        # (roadmap 55 D1): the link is the whole connection.
        if portal:
            tunnel_lbl = address_lbl = password_lbl = viewer_lbl = None
        else:
            with ui.column().classes("w-full").style("gap: 2px;"):
                group_title("Connect")
                tunnel_lbl = _kv("Tunnel", "tunnel", "Copy the tunnel command")
                address_lbl = _kv("Address", "address", "Copy the VNC address")
                password_lbl = _kv("Password", "password", "Copy the password")
                viewer_lbl = ui.label("").style(f"{HELPER_STYLE} line-height: 1.45;")
        ui.label(how).style(f"{HELPER_STYLE} line-height: 1.45; margin-top: 2px;")
        with ui.column().classes("w-full").style("gap: 0;") as desktop_box:
            desktop_help(desktop_form_url())
        with ui.column().classes("w-full").style("gap: 0;") as help_box:
            connect_help(portal, walltime)

        with ui.row().classes("w-full items-center no-wrap").style("gap: 6px; margin-top: 4px;"):
            stop_btn = house_button("Stop session", lambda: _stop(), kind="danger")
            ui.space()
            house_button("Close", lambda: dialog.close())
            gpu_btn = house_button(
                "Start on a full GPU",
                lambda: _act(software=False),
                tooltip="crboost starts EisGreis on a cluster node, VirtualGL on a full B200, A100 or RTX 6000: 3-D at "
                "full resolution. It waits while those cards are taken, which can be hours.",
            )
            action_btn = house_button("Start now", lambda: _act(software=software_offered), kind="accent")
            with action_btn:
                action_tip = ui.tooltip("")
            browser_btn = house_button(
                "Open in browser",
                kind="accent",
                tooltip="Open napari's desktop in a new tab, through the cluster's OnDemand portal. Any number of "
                "tabs show the same desktop.",
            )
            browser_btn.set_visibility(False)

    def _apply(info: dict | None = None) -> None:
        status = sv["status"]
        text, klass = _STATUS[status]
        info_ = info or {}
        node = info_.get("node") if status in ("starting", "ready") else None
        if node and sv["route"] == "desktop":
            waiting = not ((info_.get("desktop") or {}).get("window", True))
            where = f"your desktop on {node}" + (", waiting for a tomogram (no window yet)" if waiting else "")
        elif node:
            where = node
        elif status == "pending":
            expected = when_text(info_.get("expected_start"))
            where = f"queued, start expected {expected}" if expected else "queued"
        else:
            where = ""
        status_lbl.set_text(text)
        status_lbl.classes(replace=klass)
        _fill(node_cell, where)
        _fill(job_cell, str(sv["job_id"] or ""))
        if info is not None:
            _fill(render_cell, _rendering(info))
        elif status == "off":
            _fill(render_cell, "")
        if sv["route"] == "desktop":
            tip = "EisGreis in your own remote desktop" + (f" (SLURM job {sv['job_id']})" if sv["job_id"] else "")
        else:
            tip = f"SLURM job {sv['job_id']}" if sv["job_id"] else "No napari session is running."
        if status == "pending" and info_.get("slurm_reason"):
            tip += f", waiting: {info_['slurm_reason']} (Priority/Resources: the nodes it may use are taken)"
            if software_offered:
                tip += ". To look now, stop it and use Start now."
        status_tip.set_text(tip)
        live = status in ("pending", "starting", "ready")
        in_desktop = live and sv["route"] == "desktop"
        stop_btn.set_visibility(live and not in_desktop)
        gpu_btn.set_visibility(software_offered and not live and installed)
        action_btn.set_visibility(installed or in_desktop)
        desktop_box.set_visibility(not live)
        help_box.set_visibility(not in_desktop)
        if in_desktop:
            action_btn.set_text("Show in my desktop")
        else:
            action_btn.set_text(
                "Show this tomogram" if live else ("Start now" if software_offered else "Start on a node")
            )
        if in_desktop:
            action_tip.set_text("Show this tomogram in EisGreis in your desktop; layers you made stay.")
        elif live:
            action_tip.set_text("Replace what the running napari shows with this tomogram; layers you made stay.")
        elif software_offered:
            action_tip.set_text(
                "Start napari at once with software rendering (on an interactive MIG slice): 2-D as quick as on a "
                "full GPU, 3-D from a half-size copy."
            )
        else:
            action_tip.set_text(
                "crboost starts EisGreis on a GPU node, showing this tomogram. It waits while the GPUs are taken."
            )
        if status == "ready" and info is not None:
            sv["tunnel"] = info.get("tunnel_cmd") or ""
            sv["address"] = f"localhost:{info['port']}" if info.get("port") else ""
            sv["password"] = info.get("password") or ""
            sv["browser_url"] = info.get("browser_url") or ""
        elif status != "ready":
            sv.update(tunnel="", address="", password="", browser_url="")
        if in_desktop:
            set_button_kind(action_btn, "accent")
            browser_btn.set_visibility(False)
            if desktop_cell is not None:
                _fill(desktop_cell, "")
            if info is not None:
                _show_info(info, status)
            return
        if portal:
            if state["linked"] != sv["browser_url"]:  # written when it changes, not on every poll
                state["linked"] = sv["browser_url"]
                link_to(browser_btn, sv["browser_url"] or None)
                _fill(desktop_cell, sv["browser_url"].split("#")[0])
                browser_btn.set_visibility(bool(sv["browser_url"]))
                # Once the desktop is up, opening it is the dialog's one primary action.
                set_button_kind(action_btn, "default" if sv["browser_url"] else "accent")
            if info is not None and status == "ready" and not sv["browser_url"]:
                _fill(
                    tomo_cell,
                    "This session started before the portal was configured, so it cannot open in a browser: "
                    "stop it and start again.",
                    "cb-badge-err",
                )
                return
            if info is not None:
                _show_info(info, status)
            return
        _fill(tunnel_lbl, sv["tunnel"])
        _fill(address_lbl, sv["address"])
        if status == "ready" and passwordless:
            _fill(password_lbl, "none: curation.passwordless_vnc is on, anyone reaching the port gets the desktop")
        else:
            _fill(password_lbl, sv["password"])
        screen_sharing = f" at vnc://{sv['address']}" if sv["address"] else ""
        viewer_lbl.set_text(
            f"Any VNC viewer: TurboVNC, TigerVNC, RealVNC, or the Mac's Screen Sharing{screen_sharing}."
        )
        if info is not None:
            _show_info(info, status)

    def _show_info(info: dict, status: str) -> None:
        if status == "ended":
            _clear_showing()
            _fill(tomo_cell, f"The session ended. {info.get('detail') or ''}".strip(), "cb-badge-err")
            return
        rows = _showing(info, status)
        sv["saves"] = rows["saves"][0] if "saves" in rows and not rows["saves"][1] else ""
        cells = {"tomogram": tomo_cell, "grid": grid_cell, "saves": saves_cell, "crboost": crboost_cell}
        for key, cell in cells.items():
            _fill(cell, *rows.get(key, ("", "")))

    def _clear_showing() -> None:
        sv["saves"] = ""
        for cell in (tomo_cell, grid_cell, saves_cell, crboost_cell):
            _fill(cell, "")

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
            sv["session_dir"] = sv["job_id"] = sv["route"] = None
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
                    sv.update(session_dir=res["session_dir"], job_id=res["slurm_job_id"], status="pending", route="job")
            if not res.get("success"):
                ui.notify(res.get("error") or "napari could not be started.", type="negative", multi_line=True)
                _fill(tomo_cell, res.get("error") or "", "cb-badge-err")
                return
            _apply()
            _start_polling()
            await _poll()
            await napari_status.poll(backend, force=True)

    async def _stop() -> None:
        async with flight("stop") as acquired:
            if not acquired or not sv["job_id"]:
                return
            res = await backend.stop_napari_session(sv["job_id"])
            if not res.get("success"):
                ui.notify(res.get("error") or "The session could not be stopped.", type="negative")
                return
            _stop_polling()
            sv.update(session_dir=None, job_id=None, status="off", route=None)
            _clear_showing()
            _apply()
            await napari_status.poll(backend, force=True)

    def _on_value(e) -> None:
        if e.value:
            return
        state["closed"] = True
        _stop_polling()
        dialog.delete()

    dialog.on_value_change(_on_value)

    if active:
        sv.update(
            session_dir=active.get("session_dir"), job_id=active.get("slurm_job_id"), route=active.get("route") or "job"
        )
        sv["status"] = "ready" if active.get("status") == "ready" else "pending"
        _apply(active)
        _start_polling()
    else:
        _apply()
    dialog.open()
