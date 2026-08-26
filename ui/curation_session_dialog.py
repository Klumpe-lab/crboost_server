"""Single curation control center for a ChimeraX+ArtiaX manual-picking session.

Option A (no in-crboost proxying): crboost submits the VNC desktop as a SLURM job
and hands the user the exact connection details — one SSH tunnel + a viewer + a
password. ONE panel, opened from the Picks & curation tab's per-tomogram "curate"
(and from its session chip). It is a **one-stop shop with a STABLE layout**: a
status chip (live / starting / off / failed) drives the chrome, Start/Stop swap in
place, and the Connect + Scope + Save sections are built ONCE — the live values
(tunnel / address / password) fill into fixed slots when the session comes up
rather than the whole panel re-rendering into a different shape. The session is
per-project and reused (find_active_curation_session) — at most one lives at a
time, so reconnecting keeps the user's one tunnel + viewer.

MODEL B (roadmap 10-S1). This panel does not drive the session. "Load into running
session" and "Save picks now" are gone, along with the backend REST calls behind
them: the session's scope is fixed when it is launched, the user saves in ArtiaX
themselves, and crboost's job is to say exactly WHERE (section 3) and to ingest
whatever lands there. What that bought is that a saved `.coords` can no longer be
attributed to the wrong species — the failure the old swap made invisible.

The panel is parented at the page LAYOUT slot, NOT the caller's slot: "curate" is
an async click handler on a button inside a poll-refreshed container, and NiceGUI
(``events.handle_event``) runs an async handler within the sender's
``parent_slot`` — so a dialog created naively lands there and is destroyed the next
time that container refreshes (the "window closes behind me" bug).
``client.layout.default_slot`` lives for the whole page and is never cleared.
"""

from __future__ import annotations

import json
import logging
from contextlib import nullcontext
from pathlib import Path

from nicegui import context, ui

from ui.components.buttons import house_button

logger = logging.getLogger(__name__)

# One copyable-code language across all three sections: a white inset on the light
# section panels, mono, bordered. `_BOX` is a single inline value (address, tunnel,
# folder); `_BLOCK` is the multi-line command paste — it WRAPS (whitespace-pre-wrap +
# break-all) so a long command never spills a horizontal scrollbar off the card.
_BOX = (
    "font-mono text-[11px] bg-white border border-slate-200 text-slate-700 "
    "px-2 py-1 rounded flex-grow min-w-0 break-all leading-tight"
)
_BLOCK = (
    "font-mono text-[11px] bg-white border border-slate-200 text-slate-700 "
    "px-2 py-1.5 rounded w-full break-all leading-snug whitespace-pre-wrap"
)
# Each of the three sections (Connect / Scope / Pick & save) is one of these light
# panels, so they read as parallel cards instead of separator-divided prose.
_SECTION = "w-full gap-1.5 rounded-lg border border-slate-200 bg-slate-50 p-3"

# Per-client: the currently-open curation panel, so re-opening (e.g. switching to
# the next tomogram in an 80–100-tomo session) REPLACES it instead of stacking
# hidden overlays. Keyed by client id so two browser tabs don't delete each other's.
_OPEN_DIALOGS: dict = {}


def _copy_js(text: str) -> str:
    return "navigator.clipboard.writeText(" + json.dumps(text) + ")"


async def open_curation_control_center(backend, project_path: Path | None, *, bundle: dict | None = None) -> None:
    """Open the curation control center.

    ``bundle`` (from ``backend.prepare_curation_bundle``) ties the panel to one
    (species, tomogram): Start launches a session scoped to it, and section 2 states
    that scope. With no bundle (the Picks tab's session chip) it's a status/connect
    panel that reports the scope the RUNNING session was launched on. The panel checks
    liveness itself, so both entry points behave identically.
    """
    b = bundle or {}
    cxc_path = b.get("cxc_path")
    commands = b.get("commands") or []
    manual_coords = b.get("manual_coords") or ""
    # The per-(species,tomo) curation folder: the ONE place a save may land for this
    # scope, and what section 3 tells the user to point ArtiaX's save dialog at.
    save_dir = b.get("curation_dir") or (str(Path(manual_coords).parent) if manual_coords else "")
    tomo_name = b.get("tomo_name") or ""
    species_label = b.get("species_label") or b.get("species_id") or ""
    species_id = b.get("species_id") or ""
    auto_count = b.get("auto_count")
    display_bin = int(b.get("display_bin") or 1)
    display_note = b.get("display_note") or ""
    cmd_block = "\n".join(commands)
    _cur_cfg = getattr(getattr(backend, "config_service", None), "curation", None)
    passwordless = bool(getattr(_cur_cfg, "passwordless_vnc", False))

    # Live session values; copy buttons read from here (closures) so they stay
    # correct as the session transitions off → starting → live without a rebuild.
    sv = {"tunnel": "", "address": "", "password": "", "node": "", "job_id": None, "rest_port": None, "scope": None}
    state = {"session_dir": None, "timer": None, "busy": False, "kind": "off", "closing": False, "restarting": False}

    # Stable page-level host slot (see module docstring). Fall back to the caller's
    # slot if a layout isn't reachable — degraded (may close on refresh) but works.
    try:
        _client = context.client
        host_slot = _client.layout.default_slot
        cid = _client.id
    except Exception:
        host_slot, cid = None, None

    def _stop_timer() -> None:
        if state["timer"] is not None:
            state["timer"].cancel()
            state["timer"] = None

    def _close() -> None:
        """Tear the panel down. Reached from the Close button AND from Esc / the
        dialog hiding itself, so it is re-entrant-safe: `closing` stops the
        `dialog.close()` below from re-firing the value-change handler."""
        if state["closing"]:
            return
        state["closing"] = True
        _stop_timer()
        try:
            dialog.close()
            dialog.delete()
        except Exception:
            pass
        _OPEN_DIALOGS.pop(cid, None)

    def _copy(text_or_getter, tip: str = "Copy") -> None:
        def _do() -> None:
            t = text_or_getter() if callable(text_or_getter) else text_or_getter
            ui.run_javascript(_copy_js(t or ""))

        ui.button(icon="content_copy", on_click=_do).props("flat dense round size=sm").tooltip(tip)

    def _num(n: str) -> None:
        ui.label(n).classes(
            "flex items-center justify-center bg-indigo-600 text-white rounded-full text-[10px] font-bold"
        ).style("width: 18px; height: 18px; min-width: 18px; margin-top: 1px;")

    def _kw(text: str) -> None:
        ui.label(text).classes("text-[11px] text-gray-500").style("width: 64px; min-width: 64px;")

    def _section_head(icon: str, title: str):
        """Consistent header for one of the three section panels: muted icon + bold
        title. Returns the row so the caller can re-enter it (``with hdr:``) to drop
        a trailing ``ui.space()`` + the section's action buttons on the right."""
        row = ui.row().classes("w-full items-center gap-2 no-wrap")
        with row:
            ui.icon(icon, size="16px").classes("text-slate-400")
            ui.label(title).classes("text-[12px] font-semibold text-slate-700")
        return row

    def _troubleshooting_md() -> str:
        addr = sv["address"] or "localhost:<port>"
        port = sv["address"].split(":")[-1] if sv["address"] else "<port>"
        return (
            f"- **Viewer won't connect:** re-run the step-② tunnel and connect to `{addr}` exactly. It backgrounds "
            'itself; an "address already in use" error just means it is already up.\n'
            "- **Black/grey screen for a few seconds:** normal — the desktop + ChimeraX are still starting (~5–10 s).\n"
            '- **"Connection closed/refused":** the tunnel didn\'t come up or the session ended — re-run the tunnel, '
            "or press Stop then Start (Stop also clears any leftover sessions).\n"
            "- **Password rejected:** copy it again above (case-sensitive).\n"
            f"- **Stop the background tunnel:** on your Mac, `lsof -ti tcp:{port} | xargs kill`.\n"
            "- **Wrong tomogram open:** this session was launched on the one named above and crboost does not "
            "switch it. Close this, press `curate` on the tomogram you want, and start again.\n"
            "- **Picks look shifted in ArtiaX:** tell us — the `.cxc` may need an explicit pixel-size line."
        )

    # Replace any panel already open for this client (switching tomos).
    prev = _OPEN_DIALOGS.get(cid)
    if prev is not None:
        try:
            prev.delete()
        except Exception:
            pass

    ctx = host_slot if host_slot is not None else nullcontext()
    with ctx:
        with (
            # `no-backdrop-dismiss` (not `persistent`): Esc closes the panel — the
            # maintainer's peeve — while a stray click on the backdrop, mid copy-paste of a
            # tunnel command, does not. Quasar's own Esc handling does the closing; the
            # value-change hook below runs OUR teardown (poll timer, registry entry).
            ui.dialog().props("no-backdrop-dismiss") as dialog,
            ui.card()
            .classes("w-[56rem] max-w-full")
            .style("max-height: 90vh; display: flex; flex-direction: column; gap: 6px;"),
        ):
            # ── header: title · tomo · status chip ──
            with ui.row().classes("w-full items-center gap-2 no-wrap"):
                ui.icon("view_in_ar", color="indigo").classes("text-xl")
                ui.label("Curate in ChimeraX + ArtiaX").classes("text-base font-bold")
                if tomo_name:
                    ui.label(tomo_name).classes("text-[11px] font-mono text-gray-500 truncate")
                ui.space()
                status_chip = ui.label("○ No session").classes(
                    "text-[11px] font-semibold px-2 py-0.5 rounded-full bg-gray-100 text-gray-500 whitespace-nowrap"
                )
            ui.separator()

            # What the RUNNING session was launched on (its `scope.json`), which may be a
            # different tomogram — or a different project — from the one this panel was
            # opened for. Filled on reconnect; hidden when nothing is running.
            loaded_lbl = ui.label("").classes(
                "text-[11px] text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded self-start whitespace-nowrap"
            )
            loaded_lbl.set_visibility(False)

            body = ui.column().classes("w-full gap-2").style("overflow-y: auto; flex: 1 1 auto; min-height: 0;")
            with body:
                # ── action row (Start/Stop swap in place; spinner while starting) ──
                with ui.row().classes("w-full items-center gap-2"):
                    start_btn = house_button("Start session", lambda: _start(), kind="accent")
                    # The ONLY way to change what a session has open, now that nothing
                    # drives it: stop and relaunch scoped to this tomogram. Shown only when
                    # a live session is on a DIFFERENT scope — otherwise there is nothing to
                    # change, and a permanently-present restart button reads as one.
                    restart_btn = house_button("Restart on this tomogram", lambda: _restart(), kind="accent")
                    stop_btn = house_button("Stop session", lambda: _stop(), kind="danger")
                    busy_box = ui.row().classes("items-center gap-2")
                    with busy_box:
                        ui.spinner(size="sm")
                        busy_lbl = ui.label("Starting…").classes("text-xs text-gray-600")
                    ui.space()
                    job_lbl = ui.label("").classes("text-[11px] text-gray-500 whitespace-nowrap")

                # ── SECTION 1 · CONNECT (always one place; values fill in when live) ──
                with ui.column().classes(_SECTION):
                    hdr = _section_head("vpn_key", "Connect to the session")
                    with hdr:
                        ui.space()
                        ui.label("one-time setup").classes("text-[10px] text-slate-400")
                    with ui.row().classes("items-start gap-2 w-full no-wrap"):
                        _num("1")
                        ui.label(
                            "Get any VNC viewer once — RealVNC / TigerVNC / TurboVNC, or macOS Screen Sharing "
                            "(no install)."
                        ).classes("text-[11px] text-gray-600")
                    with ui.row().classes("items-start gap-2 w-full no-wrap"):
                        _num("2")
                        with ui.column().classes("gap-0.5 flex-grow min-w-0"):
                            ui.label(
                                "SSH tunnel — paste in a Terminal; it backgrounds itself, then you can close "
                                "the window:"
                            ).classes("text-[11px] text-gray-600")
                            with ui.row().classes("items-center gap-1 w-full"):
                                tunnel_lbl = ui.label("— start the session —").classes(_BOX)
                                _copy(lambda: sv["tunnel"], "Copy tunnel command")
                    with ui.row().classes("items-start gap-2 w-full no-wrap"):
                        _num("3")
                        with ui.column().classes("gap-0.5 flex-grow min-w-0"):
                            ui.label(
                                "Open the viewer at this address, then enter the password:"
                                if not passwordless
                                else "Open the viewer at this address — this site runs curation desktops "
                                "without a password:"
                            ).classes("text-[11px] text-gray-600")
                            with ui.row().classes("items-center gap-1 w-full"):
                                _kw("Address")
                                addr_lbl = ui.label("—").classes(_BOX)
                                _copy(lambda: sv["address"], "Copy address")
                            with ui.row().classes("items-center gap-1 w-full"):
                                _kw("Password")
                                pass_lbl = ui.label("—").classes(_BOX)
                                _copy(lambda: sv["password"], "Copy password")
                            if passwordless:
                                ui.label(
                                    "curation.passwordless_vnc is ON — the desktop runs with SecurityTypes None, "
                                    "so anyone who can reach that node's VNC port can drive your session. Turn it "
                                    "off in the config if this cluster is shared."
                                ).classes("text-[10px] text-orange-700")

                # ── SECTION 2 · SCOPE (declared at launch; never changed after) ──
                with ui.column().classes(_SECTION):
                    hdr = _section_head("my_location", "This session's scope")
                    with hdr:
                        if auto_count is not None:
                            ui.label(f"{auto_count} reference picks").classes("text-[10px] text-slate-400")
                        ui.space()
                        if commands:
                            house_button("Copy commands", lambda: ui.run_javascript(_copy_js(cmd_block))).tooltip(
                                "The exact ChimeraX lines this session runs at startup — paste them by hand only if "
                                "the preload didn't happen"
                            )
                    if tomo_name:
                        with ui.row().classes("items-center gap-1 w-full"):
                            _kw("Picking")
                            ui.label(f"{species_label or species_id or '—'} · {tomo_name}").classes(_BOX)
                        ui.label(
                            "Starting from here opens THIS tomogram, and crboost records that this is what the "
                            "session is for. It does not switch tomograms afterwards — for a different one, close "
                            "this and press `curate` on that tomogram."
                        ).classes("text-[11px] text-gray-500")
                        if display_bin > 1:
                            ui.label(
                                f"ArtiaX opens a {display_bin}× downscaled copy so it loads in seconds; the picks "
                                "you place are mapped back onto the full-resolution volume exactly."
                            ).classes("text-[10px] text-slate-400")
                        if display_note:
                            ui.label(
                                f"No downscaled copy for this tomogram ({display_note}) — the full-resolution "
                                "volume opens instead, which takes ~20 s."
                            ).classes("text-[10px] text-orange-700")
                    else:
                        ui.label(
                            "Opened from the session chip, so this panel is not bound to a tomogram. Press "
                            "`curate` on one in the Picks & curation tab to start a session scoped to it."
                        ).classes("text-[11px] text-gray-500")

                # ── SECTION 3 · PICK & SAVE (the contract, stated exactly) ──
                with ui.column().classes(_SECTION):
                    _section_head("save", "Pick & save")
                    ui.label(
                        "In ArtiaX, create a NEW particle list and pick into it (don't add to the reference list "
                        "opened above), then save it — crboost imports it within seconds."
                    ).classes("text-[11px] text-gray-500")
                    with ui.row().classes("items-center gap-1 w-full"):
                        _kw("Folder")
                        save_dir_lbl = ui.label(save_dir or "— open this from a tomogram's `curate` —").classes(_BOX)
                        _copy(
                            lambda: save_dir_lbl.text if (save_dir_lbl.text or "").startswith("/") else "",
                            "Copy folder path",
                        )
                    ui.markdown(
                        "- **Format:** *ArtiaX coordinates* (`.coords`) — **not** RELION star "
                        "(its writer is buggy).\n"
                        "- **Name:** anything **except** `auto.coords` / `*_ref.coords` (crboost's own exports). "
                        "Each file becomes its own pick list, named after it — so save two lists under two names "
                        "and you get two lists. Saving again under the SAME name updates that list.\n"
                        "- **The save dialog already opens in this folder.** A `.coords` saved anywhere else is "
                        "still picked up, but lands in the Picks tab's *unattributed* list for you to assign to a "
                        "species — crboost never guesses which one it was."
                    ).classes("text-[11px] text-gray-500")

                # ── Troubleshooting (collapsed) ──
                with ui.expansion("Troubleshooting", icon="help_outline").classes("w-full text-xs"):
                    ts_md = ui.markdown("").classes("text-[11px]")

            with ui.row().classes("w-full justify-end mt-1"):
                house_button("Close", _close)

    # Esc (and any other Quasar-side dismiss) hides the dialog without going through
    # `_close`, which would leave the 3 s poll timer running and the panel stuck in
    # `_OPEN_DIALOGS`. Route it back through the one teardown.
    dialog.on_value_change(lambda e: None if e.value else _close())

    _OPEN_DIALOGS[cid] = dialog
    dialog.open()

    # ── single state applier (NO body rebuild — targeted updates only) ──────────
    _chip_base = "text-[11px] font-semibold px-2 py-0.5 rounded-full whitespace-nowrap "
    _chip = {
        "live": ("● Live on {node}", _chip_base + "bg-green-100 text-green-700"),
        "starting": ("◌ Starting…", _chip_base + "bg-amber-100 text-amber-700"),
        "error": ("✕ Failed", _chip_base + "bg-red-100 text-red-600"),
        "off": ("○ No session", _chip_base + "bg-gray-100 text-gray-500"),
    }

    def _refresh_scope() -> bool:
        """Reflect what the LIVE session was launched on (its `scope.json`, read back by
        find_active_curation_session). Unlike the pre-10 indicator this survives a crboost
        restart, because the scope is on disk rather than in a process-local dict. Returns
        True when that scope DIFFERS from the one this panel was opened for — the case that
        needs a relaunch, since nothing can re-point a running session."""
        cur = sv.get("scope") or {}
        tn = cur.get("tomo_name") or ""
        loaded_lbl.set_visibility(bool(tn))
        if not tn:
            return False
        sp_txt = cur.get("species_label") or cur.get("species_id") or ""
        same = (not tomo_name) or (tn == tomo_name and cur.get("species_id", species_id) == species_id)
        prefix = "Session is picking" if same else "⚠ Session is picking a DIFFERENT scope:"
        loaded_lbl.set_text(f"{prefix} {sp_txt + ' · ' if sp_txt else ''}{tn}")
        loaded_lbl.classes(
            replace="text-[11px] px-2 py-0.5 rounded self-start whitespace-nowrap "
            + ("text-indigo-700 bg-indigo-50" if same else "text-orange-800 bg-orange-50 font-semibold")
        )
        cd = cur.get("curation_dir")
        if cd:
            save_dir_lbl.set_text(cd)  # the save folder is the RUNNING session's, not this panel's
        return not same

    def _apply(kind: str, busy_msg: str = "") -> None:
        state["kind"] = kind
        text, klass = _chip[kind]
        status_chip.set_text(text.format(node=sv["node"] or "?"))
        status_chip.classes(replace=klass)
        start_btn.set_visibility(kind in ("off", "error"))
        start_btn.set_text("Try again" if kind == "error" else "Start session")
        stop_btn.set_visibility(kind == "live")
        busy_box.set_visibility(kind == "starting")
        if busy_msg:
            busy_lbl.set_text(busy_msg)
        job_lbl.set_text((f"SLURM {sv['job_id']}" + (f" · {sv['node']}" if sv["node"] else "")) if sv["job_id"] else "")
        if kind == "live":
            tunnel_lbl.set_text(sv["tunnel"] or "—")
            addr_lbl.set_text(sv["address"] or "—")
            pass_lbl.set_text(sv["password"] or ("(none — SecurityTypes None)" if passwordless else "—"))
            restart_btn.set_visibility(bool(tomo_name) and _refresh_scope())
        else:
            tunnel_lbl.set_text("— start the session —")
            addr_lbl.set_text("—")
            pass_lbl.set_text("—")
            loaded_lbl.set_visibility(False)
            restart_btn.set_visibility(False)
        ts_md.set_content(_troubleshooting_md())

    # ── actions (re-entry guarded) ──────────────────────────────────────────────
    async def _poll() -> None:
        if state["session_dir"] is None:
            return
        info = await backend.get_curation_session_info(state["session_dir"], sv["job_id"])
        st = info.get("status")
        if st == "ready":
            sv.update(
                node=info.get("node") or "?",
                address=f"localhost:{info.get('port')}",
                password=info.get("password") or "",
                tunnel=info.get("tunnel_cmd") or "",
                rest_port=info.get("rest_port"),
            )
            _stop_timer()
            _apply("live")
        elif st == "pending":
            _apply("starting", f"Waiting for a compute node ({info.get('slurm_state', 'PENDING')})…")
        elif st == "starting":
            _apply("starting", "Node allocated — starting ChimeraX…")
        elif st == "ended":
            _stop_timer()
            _apply("error")
            ui.notify(info.get("detail") or "The session job ended before it came up.", type="negative", timeout=6000)

    async def _start() -> None:
        if state["busy"]:
            return
        state["busy"] = True
        try:
            _stop_timer()
            _apply("starting", "Submitting curation session…")
            # The scope goes WITH the launch: it is what `scope.json` records and what the
            # dir's manifest is stamped with, and it is the only declaration of identity
            # this session will ever get (Model B).
            scope = None
            if tomo_name and project_path is not None and backend is not None:
                scope = backend.curation_scope(project_path, species_id, species_label, tomo_name, save_dir)
            result = await backend.launch_curation_session(project_path, cxc_path=cxc_path, scope=scope)
            if not result.get("success"):
                _apply("error")
                ui.notify(result.get("error") or "launch failed", type="negative", timeout=6000)
                return
            sv["job_id"] = result.get("slurm_job_id")
            sv["scope"] = scope
            state["session_dir"] = result.get("session_dir")
            _apply("starting", f"Submitted SLURM job {sv['job_id']} — waiting for a node…")
            await _poll()
            if state["kind"] not in ("live", "error"):
                state["timer"] = ui.timer(3.0, _poll)
        finally:
            state["busy"] = False

    async def _restart() -> None:
        """Relaunch the session scoped to THIS tomogram — the sanctioned way to change what
        ArtiaX has open, and the only one (10-S1). Confirmed, because stopping the job
        drops whatever is unsaved in the running viewer: crboost cannot save it for the
        user any more, and pretending otherwise is what the old swap did."""
        if state["busy"] or state["restarting"]:
            return
        state["restarting"] = True
        cctx = host_slot if host_slot is not None else nullcontext()
        with cctx, ui.dialog().props("persistent") as confirm, ui.card().classes("w-[28rem] max-w-full gap-2"):
            ui.label("Restart on this tomogram?").classes("text-sm font-bold")
            ui.label(
                f"The running session stops and a new one starts on {tomo_name}. Anything you have picked in "
                "ArtiaX and NOT saved is lost — save your lists there first."
            ).classes("text-[12px] text-gray-600")
            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", lambda: confirm.submit(None))
                house_button("Stop & start here", lambda: confirm.submit(True), kind="accent")
        try:
            go = await confirm
            try:
                confirm.delete()
            except Exception:
                pass
            if not go:
                return
            await _stop()
            await _start()
        finally:
            state["restarting"] = False

    async def _stop() -> None:
        if state["busy"]:
            return
        state["busy"] = True
        try:
            # Pass project_path so Stop sweeps the WHOLE zombie pile, not just the one
            # we reconnected to — otherwise a stale session keeps getting re-found.
            await backend.stop_curation_session(sv["job_id"], project_path=project_path)
            _stop_timer()
            sv.update(job_id=None, node="", address="", password="", tunnel="", scope=None)
            state["session_dir"] = None
            _apply("off")
            ui.notify("Curation session stopped", type="info")
        finally:
            state["busy"] = False

    # ── entry: reconnect a live session (no sbatch) or show the off state ────────
    _apply("starting", "Checking for a running session…")
    active = (await backend.find_active_curation_session(project_path)) if project_path else None
    if not active:
        # Per-user reuse: the user's one live session may live under a DIFFERENT
        # project — find it across all projects so we reconnect instead of relaunch.
        try:
            active = await backend.find_active_curation_session_any()
        except Exception:
            active = None
    if active:
        sv["job_id"] = active.get("slurm_job_id")
        sv["scope"] = active.get("scope")
        state["session_dir"] = active.get("session_dir")
        await _poll()
        if state["kind"] not in ("live", "error"):
            state["timer"] = ui.timer(3.0, _poll)
    else:
        _apply("off")
