"""`/napari/open?volume=<path>`: the page behind a tomogram's `napari` chip when the site has an Open OnDemand
portal (conf.yaml `ood:`). One click on the chip opens this page in a new tab; the page shows the tomogram in the
user's napari session, starting one with *Start now* when none runs, and as soon as the session's desktop is up it
moves the tab on to the portal's noVNC, which shows napari. When the session is EisGreis in the user's own remote
desktop, the tomogram goes there and the page says so: that desktop has its own tab already.

The chip is a real link because a browser opens a tab only from the click itself: a tab opened later from the
server, once the session is up, would be blocked as a popup. So the tab exists at once and waits here.

A user has one napari session. While it runs, a click on another tomogram's chip shows that tomogram in it and
opens one more tab onto the same desktop (noVNC connects as a shared client); the session dialog behind the chip's
▾ keeps the full-GPU start and Stop.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from urllib.parse import urlencode

from nicegui import ui

from services.configs.config_service import get_config_service
from services.result import err, ok

from ui.components.buttons import house_button
from ui.job_plugins._field_styles import CLR_VALUE, HELPER_STYLE, MONO, PANEL_TITLE_STYLE
from ui.napari_session_dialog import POLL_S, connect_help, when_text

logger = logging.getLogger(__name__)

# Two clicks in quick succession (two tabs) must not each find no session and start two: the second waits for
# the first's launch, then finds it and shows its tomogram in it.
_session_lock = asyncio.Lock()


def open_url(volume: str | Path) -> str:
    """The address of this page for the volume at `volume` (an absolute path)."""
    return "/napari/open?" + urlencode({"volume": str(volume)})


async def _show_or_start(backend, volume: str) -> dict:
    """Show `volume` in the user's napari session, starting one (*Start now* where the site offers it) when none
    runs. `ok(session_dir, slurm_job_id, started)` or the launch's `err`."""
    name = Path(volume).stem
    volumes = [{"name": name, "path": volume}]
    async with _session_lock:
        active = await backend.find_active_napari_session()
        if active:
            res = await backend.open_in_napari_session(active["session_dir"], volumes, name)
            if not res.get("success"):
                return res
            return ok(
                session_dir=active["session_dir"],
                slurm_job_id=active.get("slurm_job_id"),
                started=False,
                route=active.get("route") or "job",
                node=active.get("node"),
            )
        software = bool(get_config_service().napari.software.partition)
        res = await backend.launch_napari_session(volumes, name, software=software)
        if not res.get("success"):
            return res
        return ok(session_dir=res["session_dir"], slurm_job_id=res["slurm_job_id"], started=True, route="job")


def render_napari_open_page(backend, volume: str) -> None:
    name = Path(volume).stem if volume else ""
    with ui.column().style("max-width: 560px; margin: 12vh auto 0; padding: 0 16px; gap: 10px;") as page:
        with ui.row().classes("items-center no-wrap").style("gap: 8px;"):
            ui.label("napari").style(PANEL_TITLE_STYLE)
            ui.label(name).style(f"{MONO} font-size: 11px; color: {CLR_VALUE};").tooltip(volume)
        status_lbl = ui.label("").style(f"{MONO} font-size: 11px; line-height: 1.5;")
        detail_lbl = ui.label("").style(f"{HELPER_STYLE} line-height: 1.5; white-space: pre-line;")
        actions = ui.row().classes("items-center").style("gap: 6px;")

    def _say(text: str, klass: str = "", detail: str = "") -> None:
        status_lbl.set_text(text)
        status_lbl.classes(replace=klass)
        detail_lbl.set_text(detail)

    if backend is None:
        _say("The server's backend is not running.", "cb-badge-err")
        return
    if not get_config_service().ood.enabled:
        _say(
            "No Open OnDemand portal is configured (conf.yaml ood:), so napari opens through its session dialog: "
            "the napari chip on the tomogram.",
            "cb-badge-err",
        )
        return
    if not volume:
        _say("No tomogram named: this page opens from a tomogram's napari chip.", "cb-badge-err")
        return

    # Read while the tab waits: what to do if the desktop then does not show, or its tab is closed.
    with page:
        connect_help(get_config_service().ood.url, get_config_service().napari.time)

    sv: dict = {"session_dir": None, "job_id": None, "route": "job", "polling": False, "done": False}

    def _stop_polling() -> None:
        poll_timer.deactivate()

    async def _restart() -> None:
        """A session that serves no websocket (started before the portal was configured) cannot be reached from
        here: stop it and start one that does."""
        actions.clear()
        if sv["job_id"]:
            res = await backend.stop_napari_session(sv["job_id"])
            if not res.get("success"):
                _say(res.get("error") or "The session could not be stopped.", "cb-badge-err")
                return
        # Until no session answers: a stopped one counts as gone at once, one cancelled outright (a pending job)
        # leaves squeue a moment later.
        for _ in range(20):
            await asyncio.sleep(2)
            if not await backend.find_active_napari_session():
                break
        await _begin()

    async def _poll() -> None:
        if sv["polling"] or sv["done"] or not sv["session_dir"]:
            return
        sv["polling"] = True
        try:
            info = await backend.get_napari_session_info(sv["session_dir"], sv["job_id"])
        finally:
            sv["polling"] = False
        status = info.get("status")
        if sv["route"] == "desktop":
            _desktop_progress(info)
            return
        if status == "ready":
            url = info.get("browser_url")
            if url:
                sv["done"] = True
                _stop_polling()
                _say(f"Live on {info.get('node') or '?'}: opening the desktop…", "cb-badge-ok")
                ui.navigate.to(url)
                return
            _stop_polling()
            _say(
                "Your napari session started before the portal was configured, so it cannot open in a browser.",
                "cb-badge-err",
                "Stop it and start one that can, showing this tomogram.",
            )
            actions.clear()
            with actions:
                house_button("Stop and start again", _restart, kind="accent")
        elif status == "ended":
            _stop_polling()
            _say("The session ended before it came up.", "cb-badge-err", info.get("detail") or "")
            actions.clear()
            with actions:
                house_button("Try again", _restart, kind="accent")
        elif status == "starting":
            _say(f"◌ Starting on {info.get('node') or 'a node'}…", "cb-badge-stale")
        else:
            text = "◌ Queued"
            if info.get("expected_start"):
                text += f", start expected {when_text(info['expected_start'])}"
            reason = info.get("slurm_reason")
            _say(
                text,
                "cb-badge-queued",
                f"SLURM job {sv['job_id']}"
                + (f", waiting: {reason}" if reason else "")
                + ". The tab moves on to napari once it runs; you can leave it open.",
            )

    def _desktop_progress(info: dict) -> None:
        """EisGreis in the user's own desktop: there is no tab to move on to, so say where the tomogram went."""
        requested, opened = info.get("requested") or {}, info.get("opened") or {}
        if info.get("status") == "ended":
            sv["done"] = True
            _stop_polling()
            _say("EisGreis in your desktop has stopped.", "cb-badge-err", info.get("detail") or "")
            return
        if requested and opened.get("seq") == requested.get("seq"):
            sv["done"] = True
            _stop_polling()
            if opened.get("ok"):
                _say(
                    f"● Shown in EisGreis in your desktop on {info.get('node') or '?'}.",
                    "cb-badge-ok",
                    "Switch to your desktop's tab; you can close this one.",
                )
            else:
                _say(f"Not shown in your desktop: {opened.get('error') or 'no volume could be read'}", "cb-badge-err")
            return
        _say(f"◌ Opening it in EisGreis in your desktop on {info.get('node') or '?'}…", "cb-badge-queued")

    async def _begin() -> None:
        _say("◌ Asking for your napari session…", "cb-badge-queued")
        try:
            res = await _show_or_start(backend, volume)
        except Exception as e:  # reported on the page and in the log; the page is all the user has here
            logger.exception("napari open page: could not show or start a session for %s", volume)
            res = err(f"{type(e).__name__}: {e}")
        if not res.get("success"):
            _say(res.get("error") or "napari could not be started.", "cb-badge-err")
            return
        sv.update(session_dir=res["session_dir"], job_id=res["slurm_job_id"], route=res.get("route"), done=False)
        _say("◌ Starting napari…" if res["started"] else "◌ Showing it in your napari…", "cb-badge-queued")
        poll_timer.activate()
        await _poll()

    # Built once, here, and switched on and off: a timer built in a button's handler would live in the row
    # that `actions.clear()` empties.
    poll_timer = ui.timer(POLL_S, _poll, active=False)
    ui.timer(0, _begin, once=True)
