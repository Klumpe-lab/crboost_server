"""
ProjectsOverview -- reusable widget that renders a roster of cryoboost
projects with live, disk-derived status. Used in two places:
  1. The landing page (data_import_panel) sidebar.
  2. The in-workspace "switch project" dialog (pipeline_roster).

Status fields come from backend.scan_for_projects (see _scan_for_projects_sync
+ _derive_live_status in backend.py). The component owns the 15-second
auto-refresh timer and the "Only mine" filter; both mount points get the
same behaviour.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional

from nicegui import ui, app

from services.configs.user_prefs_service import get_prefs_service
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)


# Palette is shared with the rest of data_import_panel / pipeline_roster so
# avatars carry the same per-project identity across all surfaces.
_AVATAR_PALETTE = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ec4899"]

CLR_HEADING = "#0f172a"
CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_GHOST = "#cbd5e1"
CLR_BORDER = "#e2e8f0"
CLR_META = "#64748b"
CLR_RUNNING = "#3b82f6"
CLR_FAILED = "#dc2626"
CLR_DONE = "#0d9488"
CLR_IDLE = "#94a3b8"

CURRENT_USER = getpass.getuser()
DEFAULT_REFRESH_SEC = 15.0


def avatar_color(key: str) -> str:
    return _AVATAR_PALETTE[hash(key) % len(_AVATAR_PALETTE)]


_STATUS_STYLES = {
    "running": {"color": CLR_RUNNING, "bg": "#eff6ff", "border": "#bfdbfe", "label": "live"},
    "failed":  {"color": CLR_FAILED,  "bg": "#fef2f2", "border": "#fecaca", "label": "failed"},
    "done":    {"color": CLR_DONE,    "bg": "#ecfdf5", "border": "#a7f3d0", "label": "done"},
    "idle":    {"color": CLR_IDLE,    "bg": "#f1f5f9", "border": "#e2e8f0", "label": "idle"},
}


class ProjectsOverview:
    """Reusable projects roster.

    Parameters
    ----------
    backend : CryoBoostBackend
    on_open : async callback (path: Path) -> None
        Called when the user clicks a row's open button. The component does
        not navigate or load anything itself -- the caller decides.
    on_delete : optional async callback (path: Path) -> None
        If provided, a delete button is rendered on hover; the callback owns
        the confirm-and-delete dialog. If omitted, the delete button is
        hidden (e.g. inside the in-workspace switcher we don't want users
        nuking projects mid-session).
    base_path_provider : callable () -> str
        Returns the directory to scan. Re-evaluated on every refresh so
        external base-path changes (Browse button, Recent Locations clicks)
        are picked up automatically.
    auto_refresh_sec : float
        Polling interval in seconds. Set to 0 to disable.
    current_path : optional str
        Project directory currently loaded by the caller. Renders that row
        with a "current" highlight, no click handler, and no delete button
        (so the switcher keeps continuity but won't reload the project the
        user is already in).
    show_filter : bool
        Whether to render the "Only mine" toggle in the header.
    height_px : int
        Vertical room for the scroll area.
    """

    def __init__(
        self,
        backend,
        *,
        on_open: Callable[[Path], Awaitable[None]],
        base_path_provider: Callable[[], str],
        on_delete: Optional[Callable[[Path, str], Awaitable[None]]] = None,
        auto_refresh_sec: float = DEFAULT_REFRESH_SEC,
        current_path: Optional[str] = None,
        show_filter: bool = True,
        height_px: int = 380,
        title: str = "Projects Overview",
    ):
        self.backend = backend
        self.on_open = on_open
        self.on_delete = on_delete
        self.base_path_provider = base_path_provider
        self.auto_refresh_sec = auto_refresh_sec
        self.current_path = current_path
        self.show_filter = show_filter
        self.height_px = height_px
        self.title = title
        self.prefs = get_prefs_service()
        try:
            self._current_resolved = (
                str(Path(current_path).resolve()) if current_path else None
            )
        except Exception:
            self._current_resolved = None

        self._projects: List[Dict] = []
        self._outer_container = None
        self._list_container = None
        self._counts_label = None
        self._mine_label = None
        self._timer = None
        self._last_scanned_base: Optional[str] = None
        self._refresh_lock = asyncio.Lock()
        # Set while a delete is in progress -- blocks auto-refresh so the
        # greyed-out row stays visible until rmtree completes.
        self._pause_refresh = False

    # =====================================================================
    # PUBLIC API
    # =====================================================================

    def build(self):
        """Build the UI tree and return the outermost container.
        Triggers the first scan + starts the auto-refresh timer."""
        outer = ui.column().classes("w-full gap-0").style(
            "background: white; border-radius: 8px; "
            f"border: 1px solid {CLR_BORDER}; "
            "box-shadow: 0 1px 3px rgba(15,23,42,0.06);"
        )
        self._outer_container = outer
        with outer:
            self._build_header()
            with ui.scroll_area().classes("w-full").style(
                f"height: {self.height_px}px; padding: 0;"
            ):
                self._list_container = ui.column().classes("w-full").style("gap: 0; padding: 0;")
                with self._list_container:
                    self._render_loading_skeleton()

        # Kick off the first scan; subsequent ones are timer-driven.
        # Hand the coroutines directly to ui.timer -- wrapping them in
        # asyncio.create_task detaches them from the NiceGUI client context,
        # which silently breaks ui.navigate.to and ui.notify in any handler
        # they end up calling.
        ui.timer(0.05, self.refresh, once=True)
        if self.auto_refresh_sec > 0:
            self._timer = ui.timer(self.auto_refresh_sec, self.refresh)
        return outer

    async def refresh(self):
        """Re-scan the base path and re-render. Re-entrant-safe via lock.
        Self-cancels if the underlying container has been destroyed —
        guards against a stale tick firing after the dialog/page is gone."""
        if self._list_container is None:
            self.stop()
            return
        if self._pause_refresh:
            return
        async with self._refresh_lock:
            base = (self.base_path_provider() or "").strip()
            try:
                if not base:
                    self._projects = []
                else:
                    self._projects = await self.backend.scan_for_projects(base)
                self._last_scanned_base = base
                self._render_list()
            except RuntimeError as e:
                # Client gone (tab closed / navigation race) — stop ticking.
                logger.info("ProjectsOverview: client gone, stopping (%s)", e)
                self.stop()
            except Exception as e:
                logger.info("ProjectsOverview refresh failed: %s", e)

    def stop(self):
        """Cancel the auto-refresh timer (e.g. when a dialog closes)."""
        if self._timer is not None:
            try:
                self._timer.cancel()
            except Exception:
                pass
            self._timer = None

    # =====================================================================
    # HEADER
    # =====================================================================

    def _build_header(self):
        with ui.row().classes("w-full items-center px-4 pt-3 pb-2").style("gap: 10px;"):
            ui.label(self.title).style(
                f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING}; "
                "letter-spacing: -0.01em; flex-shrink: 0;"
            )
            self._counts_label = ui.label("").style(
                f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; flex: 1;"
            )

            # Refresh button -- explicit re-scan in addition to the timer.
            ui.button(
                icon="refresh",
                on_click=self.refresh,
            ).props("flat dense round size=xs").classes(
                "text-slate-400 hover:text-blue-600 shrink-0"
            ).tooltip(f"Rescan now (auto every {int(self.auto_refresh_sec)}s)")

            if self.show_filter:
                self._build_mine_toggle()

    def _build_mine_toggle(self):
        def on_toggle(e):
            self.prefs.prefs.show_only_mine = bool(e.value)
            self.prefs.save_to_app_storage(app.storage.user)
            self._render_list()

        switch = (
            ui.switch(value=self.prefs.prefs.show_only_mine, on_change=on_toggle)
            .props("dense color=blue")
            .style("transform: scale(0.7);")
        )
        switch.tooltip(f"Filter to projects created by {CURRENT_USER}")
        self._mine_label = ui.label("Only mine").style(
            f"{FONT} font-size: 10px; color: {CLR_LABEL}; cursor: pointer; flex-shrink: 0;"
        )
        self._mine_label.on("click", lambda: switch.set_value(not switch.value))

    # =====================================================================
    # LIST
    # =====================================================================

    def _render_loading_skeleton(self):
        with ui.row().classes("w-full items-center justify-center").style("padding: 24px 0;"):
            ui.spinner("dots", size="sm").style(f"color: {CLR_SUBLABEL};")
            ui.label("Scanning…").style(
                f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; margin-left: 8px;"
            )

    def _render_list(self):
        if self._list_container is None:
            return
        self._list_container.clear()

        all_projects = self._projects
        show_only_mine = self.prefs.prefs.show_only_mine
        if show_only_mine:
            visible = [p for p in all_projects if (p.get("creator") or "") == CURRENT_USER]
        else:
            visible = all_projects

        # Header counts: live / failed / total visible.
        live_n = sum(1 for p in visible if p.get("live_status") == "running")
        failed_n = sum(1 for p in visible if p.get("live_status") == "failed")
        if self._counts_label is not None:
            base = self._last_scanned_base or ""
            base_short = base.rsplit("/", 1)[-1] if base else ""
            counts = f"{len(visible)} project{'s' if len(visible) != 1 else ''}"
            if live_n:
                counts += f" · {live_n} live"
            if failed_n:
                counts += f" · {failed_n} failed"
            if base_short:
                counts += f"  in  {base_short}/"
            self._counts_label.set_text(counts)

        if self._mine_label is not None:
            mine_count = sum(1 for p in all_projects if (p.get("creator") or "") == CURRENT_USER)
            self._mine_label.set_text(f"Only mine ({mine_count}/{len(all_projects)})")

        with self._list_container:
            if not visible:
                msg = "No projects to show"
                base = self._last_scanned_base or ""
                if not base:
                    msg = "Set a base location to scan for projects"
                elif all_projects and show_only_mine:
                    msg = f"No projects owned by {CURRENT_USER}"
                ui.label(msg).style(
                    f"{FONT} font-size: 11px; color: {CLR_GHOST}; "
                    "font-style: italic; padding: 24px 16px; text-align: center;"
                )
                return

            # Group by owner -- the creator lives in a section header rather
            # than on every row (it used to crowd the name/mnemonic). Current
            # user's section floats to the top, the rest are alphabetical.
            groups: Dict[str, List[Dict]] = {}
            for proj in visible:
                groups.setdefault(proj.get("creator") or "unknown", []).append(proj)

            def _section_order(k: str):
                return (0, "") if k == CURRENT_USER else (1, k.lower())

            for creator in sorted(groups, key=_section_order):
                self._render_section_header(creator, groups[creator])
                for proj in groups[creator]:
                    self._render_row(proj)

    def _render_section_header(self, creator: str, projects: List[Dict]):
        is_me = creator == CURRENT_USER
        known = creator and creator != "unknown"
        dot = avatar_color(creator) if known else CLR_GHOST
        live = sum(1 for p in projects if p.get("live_status") == "running")
        with ui.row().classes("w-full items-center").style(
            f"gap: 6px; padding: 5px 12px; background: #f1f5f9; "
            f"border-bottom: 1px solid {CLR_BORDER};"
        ):
            ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; background: {dot}; flex-shrink: 0;"
            )
            ui.label(creator if known else "unknown owner").style(
                f"{MONO} font-size: 10px; font-weight: 700; color: {CLR_LABEL}; letter-spacing: 0.02em;"
            )
            if is_me:
                ui.label("you").style(
                    f"{FONT} font-size: 8px; color: #1e40af; font-weight: 700; "
                    "background: #dbeafe; border-radius: 3px; padding: 0 5px;"
                )
            ui.label(f"{len(projects)} project{'s' if len(projects) != 1 else ''}").style(
                f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};"
            )
            if live:
                ui.label(f"· {live} live").style(f"{MONO} font-size: 9px; color: {CLR_RUNNING};")

    # =====================================================================
    # ROW
    # =====================================================================

    # Fixed column widths (px). Every row uses the same template so items
    # land at identical x-offsets regardless of name/path length -- the
    # name (line 1) and source path (line 2) are the only flexible cells
    # and absorb all slack, so nothing else ever shifts.
    _W_IDX = 20
    _W_AVATAR = 20
    _W_PILL = 50
    _W_DATE = 76
    _W_DELETE = 18
    _W_TS = 48
    _W_JOBS = 44
    _W_RUNFAIL = 44

    def _render_row(self, proj: Dict):
        path_str = proj["path"]
        name = proj["name"]
        mnemonic = proj.get("mnemonic") or ""
        proj_color = avatar_color(name)
        initials = name[:3].upper()
        stable_index = proj.get("stable_index", 0)
        ts_count = proj.get("ts_count") or 0
        total_planned = proj.get("total_jobs_planned") or 0
        succeeded = proj.get("succeeded") or 0
        failed = proj.get("failed") or 0
        running_live = proj.get("running_live") or 0
        executed = proj.get("executed_jobs") or 0
        live_status = proj.get("live_status") or "idle"
        last_activity = proj.get("last_activity") or proj.get("modified") or ""
        source_dir = proj.get("source_directory") or ""

        is_current = False
        if self._current_resolved:
            try:
                is_current = str(Path(path_str).resolve()) == self._current_resolved
            except Exception:
                is_current = False

        async def _open():
            try:
                await self.on_open(Path(path_str))
            except Exception as e:
                logger.info("Open project failed: %s", e)
                ui.notify(f"Failed to open project: {e}", type="negative")

        # Two stacked lines: identity (left) + status (right) on line 1,
        # source path (left) + counts (right) on line 2.
        base_style = "padding: 6px 12px 7px; gap: 4px; " f"border-bottom: 1px solid {CLR_BORDER};"
        if is_current:
            row_classes = "w-full group"
            row_style = (
                base_style + " background: #eff6ff; cursor: default; "
                "border-left: 3px solid #3b82f6; padding-left: 9px;"
            )
            row_click = None
        else:
            row_classes = "w-full hover:bg-slate-50 transition-colors cursor-pointer group"
            row_style = base_style
            row_click = _open

        row = ui.column().classes(row_classes).style(row_style)
        if row_click is not None:
            row.on("click", row_click)

        fixed = "flex-shrink: 0; white-space: nowrap;"
        with row as row_el:
            # ---- Line 1: idx + avatar + name/mnemonic | status + date + delete ----
            with ui.row().classes("w-full items-center").style("gap: 8px; flex-wrap: nowrap;"):
                ui.label(f"{stable_index:02d}").style(
                    f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; "
                    f"width: {self._W_IDX}px; text-align: right; {fixed}"
                )

                with ui.element("div").style(
                    f"width: {self._W_AVATAR}px; height: {self._W_AVATAR}px; border-radius: 50%; "
                    f"flex-shrink: 0; background: {proj_color}1a; border: 1px solid {proj_color}55; "
                    "display: flex; align-items: center; justify-content: center;"
                ):
                    ui.label(initials).style(
                        f"font-size: 7px; font-weight: 600; color: {proj_color}; "
                        "letter-spacing: 0.03em; line-height: 1; pointer-events: none;"
                    )

                # Name + mnemonic -- the flexible cell. Only the name truncates
                # under pressure; the (short) mnemonic never shrinks, so a long
                # name can't push it around.
                with ui.element("div").style(
                    "flex: 1 1 0; min-width: 0; display: flex; align-items: baseline; "
                    "gap: 6px; overflow: hidden;"
                ):
                    ui.label(name).style(
                        f"{FONT} font-size: 11px; font-weight: 500; color: {CLR_HEADING}; "
                        "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; "
                        "min-width: 0; flex: 0 1 auto;"
                    )
                    if mnemonic:
                        ui.label(mnemonic).style(
                            f"{MONO} font-size: 9px; color: {CLR_META}; font-style: italic; {fixed}"
                        )
                    if is_current:
                        ui.label("CURRENT").style(
                            f"{FONT} font-size: 8px; color: #1e40af; font-weight: 700; "
                            "background: #dbeafe; border: 1px solid #93c5fd; "
                            "border-radius: 3px; padding: 0 5px; flex-shrink: 0; "
                            "letter-spacing: 0.05em;"
                        )

                with ui.element("div").style(f"width: {self._W_PILL}px; {fixed} display: flex;"):
                    self._render_status_pill(live_status)

                ui.label(last_activity).style(
                    f"{MONO} font-size: 9px; color: {CLR_GHOST}; "
                    f"width: {self._W_DATE}px; text-align: right; {fixed}"
                )

                with ui.element("div").style(
                    f"width: {self._W_DELETE}px; {fixed} display: flex; justify-content: flex-end;"
                ):
                    if self.on_delete is not None and not is_current:
                        async def _del(p=path_str, n=name, r=row_el):
                            await self._handle_delete(Path(p), n, r)
                        (
                            ui.button(icon="delete_outline", on_click=_del)
                            .props("flat dense round size=xs")
                            .classes(
                                "text-slate-200 hover:text-red-400 opacity-0 "
                                "group-hover:opacity-100 transition-opacity"
                            )
                            .on("click.stop", lambda: None)
                        )

            # ---- Line 2: source path | TS count + jobs + run/fail + bar ----
            # Indented past the idx + avatar gutter (two 8px gaps between) so
            # the folder icon lines up under the name, not under the idx.
            with ui.row().classes("w-full items-center").style(
                f"gap: 8px; flex-wrap: nowrap; padding-left: {self._W_IDX + self._W_AVATAR + 16}px;"
            ):
                # Source data directory -- the flexible cell; left-aligned so
                # the absolute leading slash stays visible, truncates at the
                # tail. Tooltip carries the full path.
                with ui.element("div").style(
                    "flex: 1 1 0; min-width: 0; display: flex; align-items: center; "
                    "gap: 4px; overflow: hidden;"
                ):
                    ui.icon("folder_open", size="11px").style(f"color: {CLR_GHOST}; flex-shrink: 0;")
                    if source_dir:
                        ui.label(source_dir).style(
                            f"{MONO} font-size: 9px; color: {CLR_LABEL}; min-width: 0; "
                            "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                        ).tooltip(source_dir)
                    else:
                        ui.label("no source recorded").style(
                            f"{MONO} font-size: 9px; color: {CLR_GHOST}; font-style: italic;"
                        )

                ui.label(f"{ts_count} TS" if ts_count else "—").style(
                    f"{MONO} font-size: 9px; color: {CLR_LABEL if ts_count else CLR_GHOST}; "
                    f"width: {self._W_TS}px; text-align: right; {fixed}"
                )

                ui.label(f"{succeeded}/{total_planned}" if total_planned else "—").style(
                    f"{MONO} font-size: 9px; color: {CLR_LABEL if total_planned else CLR_GHOST}; "
                    f"width: {self._W_JOBS}px; text-align: right; {fixed}"
                )

                with ui.element("div").style(
                    f"width: {self._W_RUNFAIL}px; {fixed} display: flex; justify-content: flex-end;"
                ):
                    if running_live:
                        ui.label(f"{running_live} run").style(
                            f"{FONT} font-size: 8px; font-weight: 600; color: {CLR_RUNNING};"
                        )
                    elif failed:
                        ui.label(f"{failed} fail").style(
                            f"{FONT} font-size: 8px; font-weight: 600; color: {CLR_FAILED};"
                        )

                self._render_progress_bar(succeeded, failed, running_live, executed, total_planned)

    def _render_status_pill(self, status: str):
        s = _STATUS_STYLES.get(status, _STATUS_STYLES["idle"])
        # Compact dot+label so the pill stays under ~46 px wide and doesn't
        # bleed past the idx column. The "running" spinner is replaced by a
        # smaller animated dot via CSS-driven opacity to save horizontal real
        # estate; falls back to a solid dot for non-running states.
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 3px; flex-shrink: 0;"
        ):
            ui.element("div").style(
                f"width: 5px; height: 5px; border-radius: 50%; background: {s['color']}; "
                "flex-shrink: 0;"
            )
            ui.label(s["label"]).style(
                f"{FONT} font-size: 8px; color: {s['color']}; font-weight: 700; "
                "letter-spacing: 0.04em; line-height: 1; text-transform: uppercase;"
            )

    def _render_progress_bar(
        self, succeeded: int, failed: int, running_live: int, executed: int, total_planned: int
    ):
        # Width is the planned total. Anything beyond `executed` is shown as
        # the "remaining/scheduled" portion (light grey).
        total = max(total_planned, executed, 1)
        succ_pct = (succeeded / total) * 100
        fail_pct = (failed / total) * 100
        run_pct = (running_live / total) * 100
        # Cap to 100% in case state is briefly inconsistent.
        used = min(100.0, succ_pct + fail_pct + run_pct)
        rest_pct = max(0.0, 100.0 - used)

        with ui.element("div").style(
            "width: 70px; height: 5px; border-radius: 3px; overflow: hidden; "
            "display: flex; background: #f1f5f9; flex-shrink: 0;"
        ):
            if succ_pct > 0:
                ui.element("div").style(f"width: {succ_pct:.1f}%; background: {CLR_DONE};")
            if run_pct > 0:
                ui.element("div").style(f"width: {run_pct:.1f}%; background: {CLR_RUNNING};")
            if fail_pct > 0:
                ui.element("div").style(f"width: {fail_pct:.1f}%; background: {CLR_FAILED};")
            if rest_pct > 0:
                ui.element("div").style(f"width: {rest_pct:.1f}%; background: transparent;")

    async def _handle_delete(self, project_dir: Path, name: str, row_el):
        """Full delete lifecycle owned by the component:
        1. show confirm dialog (anchored to outer container so it's not
           destroyed by row re-renders)
        2. on confirm, pause auto-refresh + grey out the row
        3. await on_delete (just the rmtree)
        4. resume auto-refresh and refresh the list
        on_delete is intentionally only the rmtree -- the consumer doesn't
        need to know about the confirmation flow or the visual state."""
        if self.on_delete is None or self._outer_container is None:
            return
        with self._outer_container:
            confirmed = await self._show_delete_confirm(project_dir, name)
        if not confirmed:
            return

        self._pause_refresh = True
        try:
            self._mark_row_deleting(row_el, name)
            try:
                await self.on_delete(project_dir, name)
                ui.notify(f"Deleted '{name}'", type="positive")
            except Exception as e:
                logger.info("Delete failed for %s: %s", project_dir, e)
                # Try to notify, but the client context may be gone after
                # an error in the rmtree pathway -- swallow if so.
                try:
                    ui.notify(f"Failed to delete: {e}", type="negative")
                except Exception:
                    pass
        finally:
            self._pause_refresh = False
            await self.refresh()

    async def _show_delete_confirm(self, project_dir: Path, name: str) -> bool:
        with ui.dialog() as dialog, ui.card().classes("w-96"):
            ui.label(f"Delete '{name}'?").style(
                f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};"
            )
            ui.label(
                "This will permanently remove the project directory and all its contents."
            ).style(f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;")
            ui.label(str(project_dir)).style(
                f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; margin-top: 6px; "
                "padding: 5px 7px; background: #f8fafc; border-radius: 4px; "
                "word-break: break-all;"
            )
            with ui.row().classes("w-full justify-end mt-3 gap-2"):
                ui.button("Cancel", on_click=lambda: dialog.submit(False)).props(
                    "flat no-caps"
                ).style(f"{FONT} font-size: 12px;")
                ui.button(
                    "Delete permanently", on_click=lambda: dialog.submit(True)
                ).props("no-caps unelevated").style(
                    f"{FONT} font-size: 12px; background: #be4343; color: white; "
                    "border-radius: 6px; padding: 3px 14px;"
                )
        result = await dialog
        return bool(result)

    @staticmethod
    def _mark_row_deleting(row_el, name: str):
        """Replace the row's contents with a spinner + 'Deleting…' label
        and dim it so it's visibly in-flight. Survives until refresh()
        re-renders the list."""
        if row_el is None or getattr(row_el, "is_deleted", False):
            return
        try:
            row_el.clear()
            with row_el:
                ui.spinner("dots", size="xs").style(f"color: {CLR_SUBLABEL};")
                ui.label(f"Deleting {name}...").style(
                    f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; "
                    "font-style: italic; margin-left: 8px;"
                )
            row_el.style(add="opacity: 0.5; pointer-events: none; cursor: default;")
            row_el.is_deleted = True
        except Exception as e:
            logger.debug("Could not grey out row: %s", e)
