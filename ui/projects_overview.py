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
from collections.abc import Awaitable, Callable

from nicegui import ui, app

from services.configs.user_prefs_service import get_prefs_service
from services.project_state import SHARED_OWNER
from ui.components.buttons import house_button
from ui.components.copyable import copy_button
from ui.components.fields import house_select
from ui.components.segmented import render_segmented
from ui.routing import Route, route_to_path
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

# Ordering of the rows inside each owner section. "Recently viewed" is the default
# because it answers the question the list is usually open for ("take me back to the
# one I was in"); it reads prefs.recent_projects, the per-user MRU written by every
# path that lands someone in a workspace (ui/open_project.remember_project_opened).
SORT_MODES = (("recent", "Last opened"), ("created", "Created"))
DEFAULT_SORT = "recent"
ANY_SPECIES = "__any__"


def avatar_color(key: str) -> str:
    return _AVATAR_PALETTE[hash(key) % len(_AVATAR_PALETTE)]


_STATUS_STYLES = {
    "running": {"color": CLR_RUNNING, "bg": "#eff6ff", "border": "#bfdbfe", "label": "live"},
    "failed": {"color": CLR_FAILED, "bg": "#fef2f2", "border": "#fecaca", "label": "failed"},
    "done": {"color": CLR_DONE, "bg": "#ecfdf5", "border": "#a7f3d0", "label": "done"},
    "idle": {"color": CLR_IDLE, "bg": "#f1f5f9", "border": "#e2e8f0", "label": "idle"},
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
    on_transfer : optional async callback (path: Path, new_owner: Optional[str]) -> None
        If provided, a "transfer ownership" button is rendered on hover; the
        component owns the picker dialog and the post-transfer refresh, the
        callback just writes the new owner (SHARED_OWNER for the lab area, or a
        username). No disk move — purely attribution metadata.
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
        on_delete: Callable[[Path, str], Awaitable[None]] | None = None,
        on_transfer: Callable[[Path, str | None], Awaitable[None]] | None = None,
        on_select: Callable[[Path], Awaitable[None]] | None = None,
        auto_refresh_sec: float = DEFAULT_REFRESH_SEC,
        current_path: str | None = None,
        selected_path: str | None = None,
        show_filter: bool = True,
        height_px: int = 380,
        height_css: str | None = None,
        title: str = "Projects Overview",
    ):
        self.backend = backend
        self.on_open = on_open
        self.on_delete = on_delete
        self.on_transfer = on_transfer
        # When set, a row click *previews* the project (on_select) instead of
        # opening it, and an explicit travel arrow (on_open) is rendered per
        # row. The landing page leaves this None → click still opens directly.
        self.on_select = on_select
        self.base_path_provider = base_path_provider
        self.auto_refresh_sec = auto_refresh_sec
        self.current_path = current_path
        self.show_filter = show_filter
        self.height_px = height_px
        # Optional CSS height override for the scroll area (e.g. "calc(88vh - 120px)")
        # so the roster can fill a tall panel instead of a fixed pixel box.
        self.height_css = height_css
        self.title = title
        self.prefs = get_prefs_service()
        try:
            self._current_resolved = str(Path(current_path).resolve()) if current_path else None
        except Exception:
            self._current_resolved = None
        try:
            self._selected_resolved = str(Path(selected_path).resolve()) if selected_path else None
        except Exception:
            self._selected_resolved = None

        self._projects: list[dict] = []
        self._outer_container = None
        self._list_container = None
        self._counts_label = None
        self._mine_label = None
        self._sort_seg = None
        self._species_select = None
        self._sort_mode = DEFAULT_SORT
        self._species_filter = ANY_SPECIES
        self._syncing_species = False
        self._mru_rank: dict[str, int] = {}
        self._timer = None
        self._last_scanned_base: str | None = None
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
        outer = (
            ui.column()
            .classes("w-full gap-0")
            .style(
                "background: white; border-radius: 8px; "
                f"border: 1px solid {CLR_BORDER}; "
                "box-shadow: 0 1px 3px rgba(15,23,42,0.06);"
            )
        )
        self._outer_container = outer
        _scroll_h = self.height_css or f"{self.height_px}px"
        with outer:
            self._build_header()
            with ui.scroll_area().classes("w-full").style(f"height: {_scroll_h}; padding: 0;"):
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

    def set_selected(self, selected_path: str | None):
        """Mark a row as the previewed project (faint outline) and re-render.
        No-op-safe if the list isn't mounted yet."""
        try:
            self._selected_resolved = str(Path(selected_path).resolve()) if selected_path else None
        except Exception:
            self._selected_resolved = None
        self._render_list()

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
        with ui.row().classes("w-full items-center px-3 pt-2 pb-1").style("gap: 8px; flex-wrap: nowrap;"):
            ui.label(self.title).style(
                f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; "
                "letter-spacing: -0.01em; flex-shrink: 0;"
            )
            self._counts_label = ui.label("").style(
                f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; flex: 1 1 0; min-width: 0; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
            )

            # Refresh button -- explicit re-scan in addition to the timer.
            ui.button(icon="refresh", on_click=self.refresh).props("flat dense round size=xs").classes(
                "text-slate-400 hover:text-blue-600 shrink-0"
            ).tooltip(f"Rescan now (auto every {int(self.auto_refresh_sec)}s)")

            if self.show_filter:
                self._build_mine_toggle()

        self._build_sorter_bar()

    def _build_sorter_bar(self):
        """Order + species filter. Sorting happens INSIDE each owner section, so the
        Lab/Shared grouping the roster has always had is untouched by it."""
        with ui.row().classes("w-full items-center px-3 pb-2").style("gap: 8px; flex-wrap: nowrap;"):
            ui.label("ORDER").style(
                f"{FONT} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; "
                "letter-spacing: 0.06em; flex-shrink: 0;"
            )
            self._sort_seg = render_segmented(list(SORT_MODES), self._sort_mode, self._on_sort)

            ui.element("div").style("flex: 1 1 0; min-width: 0;")

            # Species index across the scanned projects. Options are refreshed on every
            # scan (_render_list); a species that no project registers is simply not
            # offered rather than silently matching nothing.
            self._species_select = house_select(
                "SPECIES",
                {ANY_SPECIES: "any"},
                value=ANY_SPECIES,
                width="w-28",
                hint="Show only projects whose registry holds this particle species",
                on_change=self._on_species_filter,
            )

    def _on_sort(self, key: str):
        self._sort_mode = key
        if self._sort_seg is not None:
            self._sort_seg.set_active(key)
        self._render_list()

    def _on_species_filter(self, e):
        # set_options() below re-enters this handler when it has to drop a value that
        # no longer exists; _render_list is mid-clear() at that point, so the flag turns
        # the echo into a no-op instead of a nested rebuild.
        if self._syncing_species:
            return
        self._species_filter = e.value or ANY_SPECIES
        self._render_list()

    def _sync_species_options(self, projects: list[dict]):
        """Rebuild the species dropdown's options from what the scan actually found.
        Keeps the current selection when it still exists; falls back to 'any' (and says
        so by moving the control) when the species it named has gone."""
        if self._species_select is None:
            return
        names: dict[str, str] = {}
        for proj in projects:
            for sp in proj.get("species") or []:
                names.setdefault(sp["id"], sp.get("name") or sp["id"])
        options = {ANY_SPECIES: "any"} | {sid: names[sid] for sid in sorted(names, key=lambda k: names[k].lower())}
        if options == self._species_select.options:
            return
        self._syncing_species = True
        try:
            if self._species_filter not in options:
                self._species_filter = ANY_SPECIES
            self._species_select.set_options(options, value=self._species_filter)
        finally:
            self._syncing_species = False

    def _sort_key(self, proj: dict):
        """Descending order within a section: most recent first for both modes.
        A project the user has never opened has no MRU rank — it sorts after every one
        that does, by activity, rather than being silently treated as 'just opened'."""
        if self._sort_mode == "created":
            return (-(proj.get("created_timestamp") or 0.0), proj.get("name", ""))
        rank = self._mru_rank.get(self._resolved_path(proj))
        return (0 if rank is not None else 1, rank if rank is not None else 0, -(proj.get("last_activity_ts") or 0.0))

    @staticmethod
    def _resolved_path(proj: dict) -> str:
        try:
            return str(Path(proj["path"]).resolve())
        except OSError:
            # Dangling/unreachable mount mid-scan: the unresolved path still keys the
            # MRU correctly for anything opened from this same base.
            return proj.get("path", "")

    def _build_mine_toggle(self):
        def on_toggle(e):
            self.prefs.prefs.show_only_mine = bool(e.value)
            self.prefs.save_to_app_storage(app.storage.user)
            self._render_list()

        switch = (
            ui.switch(value=self.prefs.prefs.show_only_mine, on_change=on_toggle)
            .props("dense color=blue")
            .style("transform: scale(0.65);")
        )
        switch.tooltip(f"Filter to projects created by {CURRENT_USER}")
        self._mine_label = ui.label("Only mine").style(
            f"{FONT} font-size: 9px; color: {CLR_LABEL}; cursor: pointer; flex-shrink: 0;"
        )
        self._mine_label.on("click", lambda: switch.set_value(not switch.value))

    # =====================================================================
    # LIST
    # =====================================================================

    def _render_loading_skeleton(self):
        with ui.row().classes("w-full items-center justify-center").style("padding: 24px 0;"):
            ui.spinner("dots", size="sm").style(f"color: {CLR_SUBLABEL};")
            ui.label("Scanning…").style(f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; margin-left: 8px;")

    def _render_list(self):
        if self._list_container is None:
            return
        self._list_container.clear()

        all_projects = self._projects
        eff = self._eff_owner_of  # mutable owner, falling back to created_by

        self._sync_species_options(all_projects)
        self._mru_rank = self.prefs.prefs.recent_project_rank()

        show_only_mine = self.prefs.prefs.show_only_mine
        if show_only_mine:
            # "Only mine" also surfaces the shared Lab area (under its own
            # header) alongside the current user's own projects.
            visible = [p for p in all_projects if eff(p) in (CURRENT_USER, SHARED_OWNER)]
        else:
            visible = all_projects

        species_filter = self._species_filter
        if species_filter != ANY_SPECIES:
            visible = [p for p in visible if any(s["id"] == species_filter for s in (p.get("species") or []))]

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
            mine_count = sum(1 for p in all_projects if eff(p) == CURRENT_USER)
            self._mine_label.set_text(f"Only mine ({mine_count}/{len(all_projects)})")

        with self._list_container:
            if not visible:
                msg = "No projects to show"
                base = self._last_scanned_base or ""
                if not base:
                    msg = "Set a base location to scan for projects"
                elif all_projects and species_filter != ANY_SPECIES:
                    label = (self._species_select.options if self._species_select else {}).get(
                        species_filter, species_filter
                    )
                    msg = f"No projects with a '{label}' species"
                elif all_projects and show_only_mine:
                    msg = f"No projects owned by {CURRENT_USER}"
                ui.label(msg).style(
                    f"{FONT} font-size: 11px; color: {CLR_GHOST}; "
                    "font-style: italic; padding: 24px 16px; text-align: center;"
                )
                return

            # Group by effective owner -- it lives in a section header rather
            # than on every row (it used to crowd the name/mnemonic). The
            # current user floats to the top, then Lab / Shared, then the rest
            # alphabetically.
            groups: dict[str, list[dict]] = {}
            for proj in visible:
                groups.setdefault(eff(proj), []).append(proj)

            def _section_order(k: str):
                if k == CURRENT_USER:
                    return (0, "")
                if k == SHARED_OWNER:
                    return (1, "")
                return (2, k.lower())

            for owner in sorted(groups, key=_section_order):
                rows = sorted(groups[owner], key=self._sort_key)
                self._render_section_header(owner, rows)
                for proj in rows:
                    self._render_row(proj)

    def _render_section_header(self, creator: str, projects: list[dict]):
        is_me = creator == CURRENT_USER
        is_lab = creator == SHARED_OWNER
        known = creator and creator != "unknown"
        if is_lab:
            label_text = "Lab / Shared"
            dot = CLR_RUNNING
        elif known:
            label_text = creator
            dot = avatar_color(creator)
        else:
            label_text = "unknown owner"
            dot = CLR_GHOST
        live = sum(1 for p in projects if p.get("live_status") == "running")
        with (
            ui.row()
            .classes("w-full items-center")
            .style(f"gap: 6px; padding: 5px 12px; background: #f1f5f9; border-bottom: 1px solid {CLR_BORDER};")
        ):
            ui.element("div").style(f"width: 5px; height: 5px; border-radius: 50%; background: {dot}; flex-shrink: 0;")
            ui.label(label_text).style(
                f"{MONO} font-size: 9px; font-weight: 700; color: {CLR_LABEL}; letter-spacing: 0.02em;"
            )
            if is_me:
                ui.label("you").style(
                    f"{FONT} font-size: 8px; color: #1e40af; font-weight: 700; "
                    "background: #dbeafe; border-radius: 3px; padding: 0 5px;"
                )
            ui.label(f"{len(projects)} project{'s' if len(projects) != 1 else ''}").style(
                f"{MONO} font-size: 8px; color: {CLR_SUBLABEL};"
            )
            if live:
                ui.label(f"· {live} live").style(f"{MONO} font-size: 8px; color: {CLR_RUNNING};")

    # =====================================================================
    # ROW
    # =====================================================================

    # Fixed column widths (px). Every row uses the same template so items land at
    # identical x-offsets regardless of name/path length -- the name (line 1), the
    # project path (line 1) and the source path (line 2) are the only flexible cells
    # and absorb all slack, so nothing else ever shifts.
    _W_AVATAR = 18
    _W_PILL = 46
    _W_CHEVRON = 30
    _W_TS = 44
    _W_JOBS = 40
    _W_RUNFAIL = 40

    def _render_row(self, proj: dict):
        path_str = proj["path"]
        name = proj["name"]
        proj_color = avatar_color(name)
        initials = name[:3].upper()
        ts_count = proj.get("ts_count") or 0
        total_planned = proj.get("total_jobs_planned") or 0
        succeeded = proj.get("succeeded") or 0
        failed = proj.get("failed") or 0
        running_live = proj.get("running_live") or 0
        executed = proj.get("executed_jobs") or 0
        live_status = proj.get("live_status") or "idle"
        last_activity = proj.get("last_activity") or proj.get("modified") or ""
        source_dir = proj.get("source_directory") or ""
        species = proj.get("species") or []
        # The generated mnemonic ("icy-majestic-darwin") no longer takes a column — it
        # competed with the project's real name for the eye and told the user nothing
        # they had asked for. It stays one hover away on the name, where it is findable
        # without being in the way.
        mnemonic = proj.get("mnemonic") or ""

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

        async def _select():
            self.set_selected(path_str)
            if self.on_select is not None:
                try:
                    await self.on_select(Path(path_str))
                except Exception as e:
                    logger.info("Preview project failed: %s", e)

        # Preview mode (on_select set): a click previews the project; the chevron is
        # what travels. Otherwise a click anywhere on the row opens it, and the
        # chevron just makes that obvious (and gives a big target on the right edge).
        preview_mode = self.on_select is not None
        is_selected = False
        if self._selected_resolved:
            try:
                is_selected = str(Path(path_str).resolve()) == self._selected_resolved
            except Exception:
                is_selected = False

        # The row is a two-column flex: the text stack, and a full-height chevron.
        # `overflow: hidden` is load-bearing, not cosmetic: this widget lives inside a
        # QScrollArea, which scrolls BOTH ways — a row wider than the pane used to grow a
        # horizontal scrollbar and take the status pill and the travel affordance off
        # screen at 100 % zoom. Clipping means the roster degrades by dropping meta from
        # the right of line 2 instead of hiding its own controls.
        base_style = (
            "display: flex; flex-direction: row; align-items: stretch; gap: 0; "
            f"overflow: hidden; border-bottom: 1px solid {CLR_BORDER};"
        )
        outline = " box-shadow: inset 0 0 0 1.5px #93c5fd;" if is_selected else ""
        if is_current:
            row_classes = "w-full group"
            cur = " background: #eff6ff; border-left: 3px solid #3b82f6;"
            cur += " cursor: pointer;" if preview_mode else " cursor: default;"
            row_style = base_style + cur + outline
            row_click = _select if preview_mode else None
        else:
            row_classes = "w-full hover:bg-slate-50 transition-colors cursor-pointer group"
            row_style = base_style + outline
            row_click = _select if preview_mode else _open

        row = ui.element("div").classes(row_classes).style(row_style)
        if row_click is not None:
            row.on("click", row_click)

        fixed = "flex-shrink: 0; white-space: nowrap;"
        with row as row_el:
            with ui.element("div").style(
                "flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; gap: 2px; padding: 5px 4px 6px 10px;"
            ):
                # ---- Line 1: avatar + NAME + full path (copyable) | status ----
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 6px; flex-wrap: nowrap; min-width: 0;"
                ):
                    with ui.element("div").style(
                        f"width: {self._W_AVATAR}px; height: {self._W_AVATAR}px; border-radius: 50%; "
                        f"flex-shrink: 0; background: {proj_color}1a; border: 1px solid {proj_color}55; "
                        "display: flex; align-items: center; justify-content: center;"
                    ):
                        ui.label(initials).style(
                            f"font-size: 6px; font-weight: 600; color: {proj_color}; "
                            "letter-spacing: 0.03em; line-height: 1; pointer-events: none;"
                        )

                    # The name is the one thing in this widget with weight. Everything
                    # else on the row is 8-9 px meta, so 12/600 reads as the title
                    # without needing a rule or a background to say so.
                    name_lbl = ui.label(name).style(
                        f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; "
                        "letter-spacing: -0.01em; overflow: hidden; text-overflow: ellipsis; "
                        "white-space: nowrap; min-width: 0; flex: 0 1 auto;"
                    )
                    if mnemonic:
                        name_lbl.tooltip(f"{name}\n{mnemonic}")
                    if is_current:
                        ui.label("CURRENT").style(
                            f"{FONT} font-size: 7px; color: #1e40af; font-weight: 700; "
                            "background: #dbeafe; border: 1px solid #93c5fd; "
                            "border-radius: 3px; padding: 0 4px; flex-shrink: 0; "
                            "letter-spacing: 0.05em;"
                        )

                    # The project's own absolute path, on the title line. Truncates at the
                    # TAIL: the leaf directory is the project name already bolded to its
                    # left, so what this cell is really carrying is which base it lives
                    # under. (No `direction: rtl` head-truncation trick — bidi moves the
                    # leading "/" to the far end and the path reads wrong.) Full path in
                    # the hover and one click away on the copy icon.
                    ui.label(path_str).style(
                        f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; flex: 1 1 0; min-width: 0; "
                        "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                    ).tooltip(path_str)
                    with ui.element("div").style("display: flex; flex-shrink: 0;").on("click.stop", lambda _e: None):
                        copy_button(path_str, tooltip=f"Copy project path\n{path_str}", color=CLR_GHOST)

                    with ui.element("div").style(f"width: {self._W_PILL}px; {fixed} display: flex;"):
                        self._render_status_pill(live_status)

                # ---- Line 2: TS count · date · jobs · progress | species | actions ----
                with ui.element("div").style(
                    f"display: flex; align-items: center; gap: 7px; flex-wrap: nowrap; "
                    f"min-width: 0; padding-left: {self._W_AVATAR + 6}px;"
                ):
                    ui.label(f"{ts_count} TS" if ts_count else "— TS").style(
                        f"{MONO} font-size: 9px; color: {CLR_LABEL if ts_count else CLR_GHOST}; "
                        f"width: {self._W_TS}px; {fixed}"
                    )
                    # The date is the first thing allowed to go when the pane is narrow:
                    # it shrinks and ellipsises rather than pushing the columns after it
                    # (and the chevron) out of the row.
                    ui.label(last_activity).style(
                        f"{MONO} font-size: 9px; color: {CLR_GHOST}; min-width: 0; flex: 0 1 auto; "
                        "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                    )
                    ui.label(f"{succeeded}/{total_planned}" if total_planned else "—").style(
                        f"{MONO} font-size: 9px; color: {CLR_LABEL if total_planned else CLR_GHOST}; "
                        f"width: {self._W_JOBS}px; text-align: right; {fixed}"
                    )
                    self._render_progress_bar(succeeded, failed, running_live, executed, total_planned)

                    with ui.element("div").style(f"width: {self._W_RUNFAIL}px; {fixed} display: flex;"):
                        if running_live:
                            ui.label(f"{running_live} run").style(
                                f"{FONT} font-size: 8px; font-weight: 600; color: {CLR_RUNNING};"
                            )
                        elif failed:
                            ui.label(f"{failed} fail").style(
                                f"{FONT} font-size: 8px; font-weight: 600; color: {CLR_FAILED};"
                            )

                    self._render_species_dots(species)

                    # Absorbs the slack so the hover actions sit on the right edge and
                    # the meta above never stretches.
                    ui.element("div").style("flex: 1 1 0; min-width: 0;")

                    if source_dir:
                        ui.icon("folder_open", size="10px").style(f"color: {CLR_GHOST}; flex-shrink: 0;").tooltip(
                            f"Raw data\n{source_dir}"
                        )

                    if self.on_transfer is not None and not is_current:

                        async def _xfer(p=path_str, n=name):
                            await self._handle_transfer(Path(p), n)

                        (
                            ui.button(icon="swap_horiz", on_click=_xfer)
                            .props("flat dense round size=xs")
                            .classes(
                                "text-slate-200 hover:text-blue-500 opacity-0 "
                                "group-hover:opacity-100 transition-opacity"
                            )
                            .on("click.stop", lambda: None)
                            .tooltip("Transfer ownership")
                        )

                    if self.on_delete is not None and not is_current:

                        async def _del(p=path_str, n=name, r=row_el):
                            await self._handle_delete(Path(p), n, r)

                        (
                            ui.button(icon="delete_outline", on_click=_del)
                            .props("flat dense round size=xs")
                            .classes(
                                "text-slate-200 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity"
                            )
                            .on("click.stop", lambda: None)
                        )

            # ---- Travel chevron: full row height, its own hit area ----
            # The old 18 px round arrow was a pixel-hunt; this is a full-height column
            # on the right edge, so "go there" is the easiest thing on the row to hit.
            self._render_chevron(path_str, is_current)

    def _render_chevron(self, path_str: str, is_current: bool):
        if is_current:
            with ui.element("div").style(
                f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; "
                "align-items: center; justify-content: center;"
            ):
                ui.icon("check", size="14px").style(f"color: {CLR_RUNNING};").tooltip("Currently open")
            return
        # A real <a href> (roadmap 17 S5), not a click handler: the travel arrow now
        # carries the project's addressable URL, so ⌘-click / middle-click opens it in a
        # new browser tab and "copy link address" works. A plain click is the browser's
        # own navigation to the same routed page, which loads the project and lands in
        # the workspace exactly as the old handler did — and reports a missing project
        # itself rather than through a toast on a page the user is leaving.
        chev = (
            ui.link(target=self._project_url(path_str))
            .classes("cb-proj-chevron")
            .style(
                f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; "
                "align-items: center; justify-content: center; cursor: pointer; "
                f"border-left: 1px solid {CLR_BORDER}; text-decoration: none; "
                "align-self: stretch; padding: 0;"
            )
            .tooltip("Open this project")
        )
        with chev:
            ui.icon("chevron_right", size="20px").style(f"color: {CLR_GHOST}; pointer-events: none;")

    @staticmethod
    def _project_url(path_str: str) -> str:
        """`/p/<dirname>?base=<parent>`. The base is always pinned here: the roster lists
        whatever directory the user pointed it at, which need not be one the resolver
        searches, and one stat-free explicit answer beats a per-row search on every
        refresh. The workspace drops the parameter again when it turns out to be
        redundant."""
        p = Path(path_str)
        return route_to_path(Route(project=p.name, base=str(p.parent)))

    def _render_species_dots(self, species: list[dict]):
        """The species this project's registry holds -- the same colour token the
        Species page and the roster chips use. Three at most; the rest is a count, and
        the full list is in the hover. Absent species render nothing (a project with no
        particles is the common case, not a gap to flag)."""
        if not species:
            return
        names = ", ".join(s.get("name") or s["id"] for s in species)
        with (
            ui.element("div")
            .style("display: flex; align-items: center; gap: 2px; flex-shrink: 0;")
            .tooltip(f"Species: {names}")
        ):
            for sp in species[:3]:
                ui.element("div").style(
                    f"width: 6px; height: 6px; border-radius: 50%; background: {sp.get('color') or CLR_RUNNING};"
                )
            if len(species) > 3:
                ui.label(f"+{len(species) - 3}").style(f"{MONO} font-size: 8px; color: {CLR_SUBLABEL};")

    def _render_status_pill(self, status: str):
        s = _STATUS_STYLES.get(status, _STATUS_STYLES["idle"])
        # Compact dot+label so the pill stays under ~46 px wide and doesn't
        # bleed past the idx column. The "running" spinner is replaced by a
        # smaller animated dot via CSS-driven opacity to save horizontal real
        # estate; falls back to a solid dot for non-running states.
        with ui.element("div").style("display: flex; align-items: center; gap: 3px; flex-shrink: 0;"):
            ui.element("div").style(
                f"width: 5px; height: 5px; border-radius: 50%; background: {s['color']}; flex-shrink: 0;"
            )
            ui.label(s["label"]).style(
                f"{FONT} font-size: 8px; color: {s['color']}; font-weight: 700; "
                "letter-spacing: 0.04em; line-height: 1; text-transform: uppercase;"
            )

    def _render_progress_bar(self, succeeded: int, failed: int, running_live: int, executed: int, total_planned: int):
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
            ui.label(f"Delete '{name}'?").style(f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};")
            ui.label("This will permanently remove the project directory and all its contents.").style(
                f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;"
            )
            ui.label(str(project_dir)).style(
                f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; margin-top: 6px; "
                "padding: 5px 7px; background: #f8fafc; border-radius: 4px; "
                "word-break: break-all;"
            )
            with ui.row().classes("w-full justify-end mt-3 gap-2"):
                house_button("Cancel", lambda: dialog.submit(False))
                house_button("Delete permanently", lambda: dialog.submit(True), kind="danger")
        result = await dialog
        return bool(result)

    async def _handle_transfer(self, project_dir: Path, name: str):
        """Transfer lifecycle owned by the component (mirrors _handle_delete):
        show the picker dialog, pause auto-refresh, call on_transfer with the
        chosen owner, then refresh so the row jumps to its new section.
        on_transfer is just the metadata write -- no disk move."""
        if self.on_transfer is None or self._outer_container is None:
            return
        with self._outer_container:
            new_owner = await self._show_transfer_dialog(name)
        if not new_owner:  # cancelled or empty input
            return
        self._pause_refresh = True
        try:
            await self.on_transfer(project_dir, new_owner)
            dest = "Lab / Shared" if new_owner == SHARED_OWNER else new_owner
            ui.notify(f"'{name}' → {dest}", type="positive")
        except Exception as e:
            logger.info("Transfer failed for %s: %s", project_dir, e)
            try:
                ui.notify(f"Transfer failed: {e}", type="negative")
            except Exception:
                pass
        finally:
            self._pause_refresh = False
            await self.refresh()

    async def _show_transfer_dialog(self, name: str) -> str | None:
        """Returns the new owner (SHARED_OWNER or a username) or None on cancel.
        Username candidates come from owners/creators already seen in the scan
        (there's no user directory to enumerate); free text is allowed too."""
        candidates = sorted({self._eff_owner_of(p) for p in self._projects} - {"", "unknown", SHARED_OWNER})
        with ui.dialog() as dialog, ui.card().classes("w-96"):
            ui.label(f"Transfer '{name}'").style(f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};")
            ui.label("Reassigns ownership for grouping only -- the project is not moved on disk.").style(
                f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; margin-top: 2px;"
            )
            username_input = (
                ui.input(label="Transfer to user", placeholder="username")
                .props("dense outlined")
                .classes("w-full")
                .style("margin-top: 10px;")
            )
            if candidates:
                with ui.row().classes("w-full items-center").style("gap: 4px; flex-wrap: wrap; margin-top: 4px;"):
                    ui.label("known:").style(f"{MONO} font-size: 9px; color: {CLR_GHOST};")
                    for cand in candidates:
                        ui.button(cand, on_click=lambda c=cand: username_input.set_value(c)).props(
                            "flat dense no-caps size=sm"
                        ).style(f"{MONO} font-size: 9px; color: {CLR_LABEL}; padding: 0 6px;")
            with ui.row().classes("w-full justify-between items-center mt-3 gap-2"):
                house_button("Move to Lab / Shared", lambda: dialog.submit(SHARED_OWNER))
                with ui.row().classes("items-center gap-2"):
                    house_button("Cancel", lambda: dialog.submit(None))
                    house_button(
                        "Transfer", lambda: dialog.submit((username_input.value or "").strip() or None), kind="accent"
                    )
        result = await dialog
        return result

    @staticmethod
    def _eff_owner_of(p: dict) -> str:
        return p.get("owner") or p.get("creator") or "unknown"

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
                    f"{FONT} font-size: 10px; color: {CLR_SUBLABEL}; font-style: italic; margin-left: 8px;"
                )
            # The row is a stretch-aligned flex row (text stack + chevron); with both
            # gone the spinner would sit flush against the top-left corner.
            row_el.style(
                add="opacity: 0.5; pointer-events: none; cursor: default; align-items: center; padding: 8px 12px;"
            )
            row_el.is_deleted = True
        except Exception as e:
            logger.debug("Could not grey out row: %s", e)
