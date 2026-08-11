"""Single curation control center for a ChimeraX+ArtiaX manual-picking session.

Option A (no in-crboost proxying): crboost submits the VNC desktop as a SLURM job
and hands the user the exact connection details — one SSH tunnel + a viewer + a
password. ONE panel, opened from the journeys gallery's per-tomo "Curate in
ArtiaX" button (and the sidebar). It is a **one-stop shop with a STABLE layout**:
a status chip (live / starting / off / failed) drives the chrome, Start/Stop swap
in place, and the Connect + Load-this-tomogram guidance is built ONCE — the live
values (tunnel / address / password) fill into fixed slots when the session comes
up rather than the whole panel re-rendering into a different shape. The session is
per-project and reused (find_active_curation_session) — at most one lives at a
time, so reconnecting keeps the user's one tunnel + viewer.

The panel is parented at the page LAYOUT slot, NOT the caller's slot: "Curate in
ArtiaX" is an async click handler on a button inside the dashboard's ``main_area``,
and NiceGUI (``events.handle_event``) runs an async handler within the sender's
``parent_slot`` — so a dialog created naively lands under ``main_area`` and is
destroyed the next time the 4 s live-refresh does ``main_area.clear()`` (the
"window closes behind me" bug). ``client.layout.default_slot`` lives for the whole
page and is never cleared.
"""

from __future__ import annotations

import json
import logging
from contextlib import nullcontext
from pathlib import Path

from nicegui import context, ui

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
# Each of the three sections (Connect / Load / Pick & save) is one of these light
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
    (species, tomogram): Start preloads it, and the Load section shows the paste-in
    commands + the ``.coords`` save target. With no bundle (sidebar) it's a
    project-level start/connect panel. The panel checks liveness itself, so both
    entry points behave identically — there is no separate chooser.
    """
    b = bundle or {}
    cxc_path = b.get("cxc_path")
    commands = b.get("commands") or []
    manual_coords = b.get("manual_coords") or ""
    # The per-(species,tomo) curation folder (= manual_coords' parent): the forefront
    # save target, and where a by-hand ArtiaX save must land. _refresh_loaded repoints it
    # at the LOADED tomogram's folder once a swap records one.
    _initial_save_dir = str(Path(manual_coords).parent) if manual_coords else ""
    tomo_name = b.get("tomo_name") or ""
    auto_count = b.get("auto_count")
    cmd_block = "\n".join(commands)

    # Inputs for the one-click "Load into running session" swap (REST channel). The
    # dashboard handler stamps these onto the bundle; absent (sidebar entry) → no button.
    candidates_star = b.get("candidates_star")
    tomograms_star = b.get("tomograms_star")
    species_id = b.get("species_id") or ""
    species_label = b.get("species_label") or ""
    source_star = b.get("source_star")
    coords_label = b.get("coords_label") or "auto"
    _cur_cfg = getattr(getattr(backend, "config_service", None), "curation", None)
    can_load = bool(getattr(_cur_cfg, "rest_enabled", True)) and bool(candidates_star and tomograms_star)

    # Live session values; copy buttons read from here (closures) so they stay
    # correct as the session transitions off → starting → live without a rebuild.
    sv = {"tunnel": "", "address": "", "password": "", "node": "", "job_id": None, "rest_port": None}
    state = {"session_dir": None, "timer": None, "busy": False, "kind": "off", "load_busy": False, "save_busy": False}

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
            ui.dialog().props("persistent") as dialog,
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

            # "Currently loaded" — what the shared live session has open via the REST
            # swap (backend._curation_loaded). Filled on reconnect + after each Load;
            # hidden until something is loaded. The session is per-user, so this can
            # reflect a tomogram a swap loaded from a different project's dashboard.
            loaded_lbl = ui.label("").classes(
                "text-[11px] text-indigo-700 bg-indigo-50 px-2 py-0.5 rounded self-start whitespace-nowrap"
            )
            loaded_lbl.set_visibility(False)

            body = ui.column().classes("w-full gap-2").style("overflow-y: auto; flex: 1 1 auto; min-height: 0;")
            with body:
                # ── action row (Start/Stop swap in place; spinner while starting) ──
                with ui.row().classes("w-full items-center gap-2"):
                    start_btn = ui.button("Start session", icon="play_arrow", color="indigo", on_click=lambda: _start())
                    start_btn.props("no-caps dense")
                    stop_btn = ui.button("Stop session", icon="stop", color="red", on_click=lambda: _stop())
                    stop_btn.props("flat dense no-caps")
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
                            ui.label("Open the viewer at this address, then enter the password:").classes(
                                "text-[11px] text-gray-600"
                            )
                            with ui.row().classes("items-center gap-1 w-full"):
                                _kw("Address")
                                addr_lbl = ui.label("—").classes(_BOX)
                                _copy(lambda: sv["address"], "Copy address")
                            with ui.row().classes("items-center gap-1 w-full"):
                                _kw("Password")
                                pass_lbl = ui.label("—").classes(_BOX)
                                _copy(lambda: sv["password"], "Copy password")

                # ── SECTION 2 · LOAD THIS TOMOGRAM (auto-preloaded; commands = fallback) ──
                load_btn = None
                with ui.column().classes(_SECTION):
                    hdr = _section_head("layers", "Load this tomogram")
                    with hdr:
                        if auto_count is not None:
                            ui.label(f"{auto_count} auto picks").classes("text-[10px] text-slate-400")
                        ui.space()
                        if commands and can_load:
                            load_btn = ui.button(
                                "Load into running session",
                                icon="swap_horiz",
                                color="indigo",
                                on_click=lambda: _load_into_session(),
                            ).props("dense no-caps size=sm")
                            load_btn.tooltip("Swap the running ArtiaX to this tomogram + picks — no copy-paste")
                        if commands:
                            ui.button(
                                "Copy commands",
                                icon="content_copy",
                                on_click=lambda: ui.run_javascript(_copy_js(cmd_block)),
                            ).props("flat dense no-caps size=sm color=indigo")
                    if commands:
                        ui.label(
                            "crboost preloads this tomogram and its picks for you — use Load into running "
                            "session to swap the live viewer to it. The commands below are a manual fallback "
                            "you only need if that doesn't work."
                            if can_load
                            else "crboost preloads this tomogram and its picks when the session starts. To "
                            "switch the running viewer by hand, paste these into the ChimeraX command line "
                            "(bottom of its window):"
                        ).classes("text-[11px] text-gray-500")
                        ui.label(cmd_block).classes(_BLOCK)
                    else:
                        ui.label(
                            "Open a tomogram from a species gallery's “Curate in ArtiaX” to preload it + its "
                            "picks here."
                        ).classes("text-[11px] text-gray-500")

                # ── SECTION 3 · PICK & SAVE (crboost files your lists for you) ──
                with ui.column().classes(_SECTION):
                    hdr = _section_head("save", "Pick & save")
                    with hdr:
                        ui.space()
                        save_btn = ui.button(
                            "Save picks now", icon="save", color="green", on_click=lambda: _save_picks_now()
                        ).props("dense no-caps size=sm")
                        save_btn.tooltip(
                            "Save the pick lists you made in ArtiaX into the loaded tomogram's folder — no Save dialog"
                        )
                    ui.label(
                        "Make a NEW particle list in ArtiaX and pick into it (don't add to the auto list). "
                        "crboost saves your picks for you — click Save picks now, and it also offers to save "
                        "automatically when you switch tomograms. The steps below are only if you'd rather "
                        "save by hand."
                    ).classes("text-[11px] text-gray-500")
                    with ui.expansion("Prefer to save by hand in ArtiaX?", icon="folder_open").classes(
                        "w-full text-xs"
                    ):
                        with ui.row().classes("items-center gap-1 w-full"):
                            _kw("Folder")
                            save_dir_lbl = ui.label(_initial_save_dir or "— load a tomogram —").classes(_BOX)
                            _copy(
                                lambda: save_dir_lbl.text if (save_dir_lbl.text or "").startswith("/") else "",
                                "Copy folder path",
                            )
                        ui.markdown(
                            "- **Format:** *ArtiaX coordinates* (`.coords`) — **not** RELION star "
                            "(its writer is buggy).\n"
                            "- **Name:** anything (e.g. `picks.coords`), **except** `auto.coords` / `*_ref.coords` "
                            "(crboost's own exports).\n"
                            "- The Save dialog already opens in this folder. crboost auto-imports the **newest** "
                            "`.coords` here within a few seconds."
                        ).classes("text-[11px] text-gray-500")

                # ── Troubleshooting (collapsed) ──
                with ui.expansion("Troubleshooting", icon="help_outline").classes("w-full text-xs"):
                    ts_md = ui.markdown("").classes("text-[11px]")

            with ui.row().classes("w-full justify-end mt-1"):
                ui.button("Close", on_click=_close).props("flat dense")

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

    def _refresh_loaded() -> None:
        """Reflect the shared session's currently-loaded (species, tomo) from the
        backend into the indicator. No-op-safe if the backend predates the getter."""
        cur = None
        try:
            getter = getattr(backend, "get_curation_loaded", None)
            if getter is not None:
                cur = getter({"slurm_job_id": sv["job_id"], "session_dir": state["session_dir"]})
        except Exception:
            cur = None
        tn = (cur or {}).get("tomo_name") or ""
        if tn:
            sp_txt = cur.get("species_label") or cur.get("species_id") or ""
            loaded_lbl.set_text(f"Loaded in session: {sp_txt + ' · ' if sp_txt else ''}{tn}")
            cd = cur.get("curation_dir")
            if cd:
                save_dir_lbl.set_text(cd)  # by-hand save folder follows the loaded tomo
        loaded_lbl.set_visibility(bool(tn))

    def _apply(kind: str, busy_msg: str = "") -> None:
        state["kind"] = kind
        text, klass = _chip[kind]
        status_chip.set_text(text.format(node=sv["node"] or "?"))
        status_chip.classes(replace=klass)
        start_btn.set_visibility(kind in ("off", "error"))
        start_btn.set_text("Try again" if kind == "error" else "Start session")
        stop_btn.set_visibility(kind == "live")
        busy_box.set_visibility(kind == "starting")
        if load_btn is not None:
            load_btn.set_visibility(kind == "live")
        save_btn.set_visibility(kind == "live")
        if busy_msg:
            busy_lbl.set_text(busy_msg)
        job_lbl.set_text((f"SLURM {sv['job_id']}" + (f" · {sv['node']}" if sv["node"] else "")) if sv["job_id"] else "")
        if kind == "live":
            tunnel_lbl.set_text(sv["tunnel"] or "—")
            addr_lbl.set_text(sv["address"] or "—")
            pass_lbl.set_text(sv["password"] or "—")
            _refresh_loaded()
        else:
            tunnel_lbl.set_text("— start the session —")
            addr_lbl.set_text("—")
            pass_lbl.set_text("—")
            loaded_lbl.set_visibility(False)
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
            result = await backend.launch_curation_session(project_path, cxc_path=cxc_path)
            if not result.get("success"):
                _apply("error")
                ui.notify(result.get("error") or "launch failed", type="negative", timeout=6000)
                return
            sv["job_id"] = result.get("slurm_job_id")
            state["session_dir"] = result.get("session_dir")
            _apply("starting", f"Submitted SLURM job {sv['job_id']} — waiting for a node…")
            await _poll()
            if state["kind"] not in ("live", "error"):
                state["timer"] = ui.timer(3.0, _poll)
        finally:
            state["busy"] = False

    async def _stop() -> None:
        if state["busy"]:
            return
        state["busy"] = True
        try:
            # Pass project_path so Stop sweeps the WHOLE zombie pile, not just the one
            # we reconnected to — otherwise a stale session keeps getting re-found.
            await backend.stop_curation_session(sv["job_id"], project_path=project_path)
            _stop_timer()
            sv.update(job_id=None, node="", address="", password="", tunnel="")
            state["session_dir"] = None
            _apply("off")
            ui.notify("Curation session stopped", type="info")
        finally:
            state["busy"] = False

    async def _load_into_session() -> None:
        """One-click swap: clear the running ArtiaX and load THIS tomogram + picks
        over the REST channel — the alternative to copy-pasting the commands."""
        if state["load_busy"]:
            return
        if state["kind"] != "live" or not sv.get("rest_port"):
            ui.notify("Start the session first (and wait for it to go live).", type="warning")
            return

        # Confirm — `close session` is an irreversible wipe of unsaved manual picks.
        cctx = host_slot if host_slot is not None else nullcontext()
        with cctx:
            with ui.dialog().props("persistent") as confirm, ui.card().classes("w-[26rem] max-w-full gap-2"):
                ui.label("Load into running session?").classes("text-sm font-bold")
                ui.label(
                    f"This swaps ArtiaX to {tomo_name or 'this tomogram'} + its picks, clearing what's open now. "
                    "Any manual picks you haven't saved for the current tomogram would be lost."
                ).classes("text-[12px] text-gray-600")
                save_cb = ui.checkbox("Save my current picks first", value=True).props("dense").classes("text-[12px]")
                ui.label("crboost saves your open lists to the current tomogram's folder before switching.").classes(
                    "text-[10px] text-gray-400"
                )
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                    ui.button("Load", color="indigo", on_click=lambda: confirm.submit(True)).props("dense no-caps")
        go = await confirm
        do_save = bool(save_cb.value) if go else False
        try:
            confirm.delete()
        except Exception:
            pass
        if not go:
            return

        state["load_busy"] = True
        if load_btn is not None:
            load_btn.props("loading")
        try:
            session_info = {
                "node": sv["node"],
                "rest_port": sv["rest_port"],
                "slurm_job_id": sv["job_id"],
                "session_dir": state["session_dir"],
            }
            res = await backend.load_into_session(
                session_info,
                project_path,
                Path(candidates_star),
                Path(tomograms_star),
                tomo_name,
                species_label,
                species_id=species_id,
                source_star=Path(source_star) if source_star else None,
                coords_label=coords_label,
                save_first=do_save,
            )
            if res.get("success"):
                n = res.get("auto_count")
                extra = f" ({n} picks)" if n is not None else ""
                saved = res.get("saved") if isinstance(res.get("saved"), dict) else None
                msg = f"Loaded {tomo_name}{extra} into the running session."
                if saved and saved.get("saved"):
                    msg += f" Saved {len(saved['saved'])} list(s) first."
                ui.notify(msg, type="positive")
                _refresh_loaded()
            else:
                ui.notify(f"Load failed: {res.get('error') or 'unknown error'}", type="negative", timeout=7000)
        finally:
            if load_btn is not None:
                load_btn.props(remove="loading")
            state["load_busy"] = False

    async def _save_picks_now() -> None:
        """Forefront save: crboost writes the manual lists open in the session to the
        loaded tomogram's folder over the REST channel (no ArtiaX Save dialog), where
        the dashboard prescan imports them. Non-destructive → no confirm."""
        if state["kind"] != "live" or not sv.get("rest_port"):
            ui.notify("Start the session and load a tomogram first.", type="warning")
            return
        if state["save_busy"]:
            return
        state["save_busy"] = True
        save_btn.props("loading")
        try:
            session_info = {
                "node": sv["node"],
                "rest_port": sv["rest_port"],
                "slurm_job_id": sv["job_id"],
                "session_dir": state["session_dir"],
            }
            res = await backend.save_curation_picks(
                session_info,
                project_path=project_path,
                tomo_name=tomo_name,
                species_id=species_id,
                species_label=species_label,
            )
            if res.get("success"):
                n = res.get("count") or 0
                if n:
                    ui.notify(
                        f"Saved {n} pick list(s) for {res.get('tomo') or 'this tomogram'} — crboost is importing them.",
                        type="positive",
                        timeout=5000,
                    )
                else:
                    ui.notify(
                        "No manual pick lists are open in ArtiaX — make a new list and pick into it first.",
                        type="warning",
                        timeout=6000,
                    )
            else:
                ui.notify(f"Save failed: {res.get('error') or 'unknown error'}", type="negative", timeout=7000)
        finally:
            save_btn.props(remove="loading")
            state["save_busy"] = False

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
        state["session_dir"] = active.get("session_dir")
        await _poll()
        if state["kind"] not in ("live", "error"):
            state["timer"] = ui.timer(3.0, _poll)
    else:
        _apply("off")
