"""
ProjectsOverview -- reusable widget that renders a roster of cryoboost
projects with live, disk-derived status. Used in two places:
  1. The landing page (data_import_panel) sidebar.
  2. The in-workspace "switch project" dialog (pipeline_roster).

Status fields come from backend.scan_for_projects (see _scan_for_projects_sync
+ _derive_live_status in backend.py). The component owns the 15-second
auto-refresh timer, the "Only mine" filter and project deletion; both mount
points get the same behaviour.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
import itertools
import math
import re
import urllib.parse
from datetime import datetime
from pathlib import Path
from collections.abc import Awaitable, Callable

from nicegui import ui, app

from services.configs.user_prefs_service import get_prefs_service
from services.project_disk_usage import BUCKET_OTHER, BUCKET_PARTICLES, BUCKET_PREPROCESSING, BUCKET_TRASH, DiskUsage
from services.project_state import SHARED_OWNER
from ui.components.buttons import house_button
from ui.components.copyable import copy_on_click
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
CLR_REVIEW = "#d97706"

CURRENT_USER = getpass.getuser()
DEFAULT_REFRESH_SEC = 15.0

# Ordering of the rows inside each owner section. "Recently viewed" is the default
# because it answers the question the list is usually open for ("take me back to the
# one I was in"); it reads prefs.recent_projects, the per-user MRU written by every
# path that lands someone in a workspace (ui/open_project.remember_project_opened).
SORT_MODES = (("recent", "Last opened"), ("created", "Created"))
DEFAULT_SORT = "recent"
ANY_SPECIES = "__any__"
ANY_SPECIES_LABEL = "any species"

# WarpTools preview names end in the reconstruction's pixel size: `<tomo>_6.20Apx.png`.
_PREVIEW_SUFFIX = re.compile(r"_\d+(?:\.\d+)?Apx\.png$")

# Row hover card, run in the browser: no server round-trip to open it, and it stays open
# while the pointer is on it. One shared timer: entering a row or its card cancels a
# pending close, leaving either schedules one, and opening a card closes any other.
_HOVER_STATE = "(window.__cbHoverCard ??= {timer: 0, open: null})"
_HOVER_STAY = f"() => {{ clearTimeout({_HOVER_STATE}.timer); }}"
_HOVER_CLOSE = (
    f"() => {{ const s = {_HOVER_STATE}; clearTimeout(s.timer); s.timer = setTimeout(() => "
    "{ if (s.open !== null) runMethod(s.open, 'hide', []); s.open = null; }, 200); }"
)


def _hover_open_js(menu_id: int) -> str:
    return (
        f"() => {{ const s = {_HOVER_STATE}; clearTimeout(s.timer); s.timer = setTimeout(() => {{ "
        f"if (s.open !== null && s.open !== {menu_id}) runMethod(s.open, 'hide', []); "
        f"s.open = {menu_id}; runMethod({menu_id}, 'show', []); }}, 90); }}"
    )


def _preview_img_html(png: str, stamp: int) -> str:
    """Fixed 260 px box, so stepping between tomograms never resizes the card."""
    url = f"/api/project-preview?path={urllib.parse.quote(png, safe='')}&v={stamp}"
    return (
        f"<img src='{url}' alt='' style='width: 260px; height: 260px; object-fit: contain; "
        "display: block; border-radius: 3px; background: #f1f5f9;' />"
    )


def avatar_color(key: str) -> str:
    return _AVATAR_PALETTE[hash(key) % len(_AVATAR_PALETTE)]


# Only these statuses get a badge; an idle project renders none. "review": the pipeline
# waits for the tilt-filter review (backend._derive_live_status).
_STATUS_STYLES = {
    "running": {"color": CLR_RUNNING, "label": "live"},
    "review": {"color": CLR_REVIEW, "label": "review"},
    "failed": {"color": CLR_FAILED, "label": "failed"},
    "done": {"color": CLR_DONE, "label": "done"},
}

# Disk size: quiet up to _SIZE_QUIET, warming on a log scale to full strength at
# _SIZE_HEAVY. No red anywhere on the ramp: red is FAILED on the same row.
_GIB = 1024**3
_SIZE_QUIET = 50 * _GIB
_SIZE_HEAVY = 1024 * _GIB
_SIZE_RAMP = ((0.0, (0x94, 0xA3, 0xB8)), (0.5, (0xD9, 0x77, 0x06)), (1.0, (0xC2, 0x41, 0x0C)))

_BUCKET_LABELS = (
    (BUCKET_PREPROCESSING, "Preprocessing"),
    (BUCKET_PARTICLES, "Particles/STA"),
    (BUCKET_TRASH, "Trash"),
    (BUCKET_OTHER, "Other"),
)


def format_bytes(n: int) -> str:
    """du-style binary units, written GB/TB: 572 GB, 2.9 TB, 640 MB."""
    if n >= 1024 * _GIB:
        return f"{n / (1024 * _GIB):.1f} TB"
    if n >= 10 * _GIB:
        return f"{n / _GIB:.0f} GB"
    if n >= _GIB:
        return f"{n / _GIB:.1f} GB"
    return f"{n / 1024**2:.0f} MB"


def size_color(n: int) -> str:
    if n <= _SIZE_QUIET:
        return CLR_SUBLABEL
    t = min(1.0, math.log(n / _SIZE_QUIET) / math.log(_SIZE_HEAVY / _SIZE_QUIET))
    for (t0, c0), (t1, c1) in itertools.pairwise(_SIZE_RAMP):
        if t <= t1:
            f = (t - t0) / (t1 - t0)
            r, g, b = (round(a + (z - a) * f) for a, z in zip(c0, c1, strict=True))
            return f"#{r:02x}{g:02x}{b:02x}"
    return "#{:02x}{:02x}{:02x}".format(*_SIZE_RAMP[-1][1])


class ProjectsOverview:
    """Reusable projects roster.

    Parameters
    ----------
    backend : CryoBoostBackend
        Entering a project is only ever the row's chevron: a link to the
        project's routed URL, which loads it. A click on the row itself never
        enters (it previews, when on_select is given).
    base_path_provider : callable () -> str
        Returns the directory to scan. Re-evaluated on every refresh so
        external base-path changes (Browse button, Recent Locations clicks)
        are picked up automatically.
    on_browse : optional async callback () -> None
        If provided, a folder icon in the control row lets the user pick
        another base location (the caller owns the picker).
    auto_refresh_sec : float
        Polling interval in seconds. Set to 0 to disable.
    current_path : optional str
        Project directory currently loaded by the caller. Renders that row
        with a "current" highlight, no click handler, and no delete button
        (so the switcher keeps continuity but won't reload -- or delete --
        the project the user is already in). Every other row can be deleted
        (hover button; the component owns the confirm dialog).
    show_filter : bool
        Whether to render the "Only mine" toggle in the header.
    height_px : int
        Vertical room for the scroll area.
    """

    def __init__(
        self,
        backend,
        *,
        base_path_provider: Callable[[], str],
        on_select: Callable[[Path], Awaitable[None]] | None = None,
        on_browse: Callable[[], Awaitable[None]] | None = None,
        auto_refresh_sec: float = DEFAULT_REFRESH_SEC,
        current_path: str | None = None,
        selected_path: str | None = None,
        show_filter: bool = True,
        height_px: int = 380,
        height_css: str | None = None,
        title: str = "Projects Overview",
    ):
        self.backend = backend
        # When set, a row click *previews* the project (on_select). The landing page
        # leaves this None: there the row is not clickable at all.
        self.on_select = on_select
        self.on_browse = on_browse
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
        self._size_label = None
        self._sort_seg = None
        self._owner_seg = None
        self._view_seg = None
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
        # Set while a row's hover card is open -- also holds auto-refresh, since a
        # rebuild would destroy the card under the pointer. _render_list clears it.
        self._preview_open = False

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
                "box-shadow: 0 1px 3px rgba(15,23,42,0.06); "
                # Never wider than the pane it sits in: a narrow landing page clips
                # here rather than scrolling the whole roster sideways.
                "min-width: 0; max-width: 100%; overflow: hidden;"
            )
        )
        self._outer_container = outer
        _scroll_h = self.height_css or f"{self.height_px}px"
        with outer:
            self._build_header()
            with ui.scroll_area().classes("w-full cb-scroll-noh").style(f"height: {_scroll_h}; padding: 0;"):
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
        if self._pause_refresh or self._preview_open:
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
        """One control row: the title and the total size on the left, every control on
        the right. It wraps on a narrow pane instead of pushing controls off the edge."""
        with (
            ui.row()
            .classes("w-full items-center px-3 py-2")
            .style(f"gap: 6px 10px; flex-wrap: wrap; border-bottom: 1px solid {CLR_BORDER};")
        ):
            ui.label(self.title).style(
                f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; "
                "letter-spacing: -0.01em; flex-shrink: 0;"
            )
            self._size_label = ui.label("").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;")
            ui.element("div").style("flex: 1 1 0; min-width: 0;")

            # Every choice in this row is the same small two-way strip, so the row reads as
            # one set of controls and each state is named rather than implied by a knob.
            # In the full view sorting happens inside each owner section, so the
            # Lab/Shared grouping is unaffected by it.
            self._sort_seg = self._small_segmented(
                SORT_MODES, self._sort_mode, self._on_sort, hint="Order: last opened by you, or created"
            )

            # Species index across the scanned projects. Options are refreshed on every
            # scan (_render_list); a species that no project registers is simply not
            # offered rather than silently matching nothing.
            self._species_select = house_select(
                "",
                {ANY_SPECIES: ANY_SPECIES_LABEL},
                value=ANY_SPECIES,
                width="w-28",
                hint="Show only projects whose registry holds this particle species",
                on_change=self._on_species_filter,
            )

            if self.show_filter:
                self._owner_seg = self._small_segmented(
                    (("all", "All"), ("mine", "Mine")),
                    "mine" if self.prefs.prefs.show_only_mine else "all",
                    self._on_owner_filter,
                    hint=f"Mine: projects created by {CURRENT_USER}, plus Lab / Shared",
                )
            self._view_seg = self._small_segmented(
                (("compact", "Compact"), ("full", "Full")),
                "compact" if self.prefs.prefs.projects_compact_view else "full",
                self._on_view,
                hint="Compact: one line per project. Full: paths, last activity and delete",
            )

            if self.on_browse is not None:
                ui.button(icon="folder_open", on_click=self.on_browse).props("flat dense round size=xs").classes(
                    "text-slate-400 hover:text-blue-600 shrink-0"
                ).tooltip("Browse for another base location")
            # Refresh button -- explicit re-scan in addition to the timer.
            ui.button(icon="refresh", on_click=self.refresh).props("flat dense round size=xs").classes(
                "text-slate-400 hover:text-blue-600 shrink-0"
            ).tooltip(f"Rescan now (auto every {int(self.auto_refresh_sec)}s)")

    @staticmethod
    def _small_segmented(tabs, active: str, on_switch: Callable[[str], None], *, hint: str):
        """A 16 px segmented strip (`cb-seg-sm`) with a hover hint."""
        with ui.element("div").style("display: flex; flex-shrink: 0;").tooltip(hint):
            return render_segmented(list(tabs), active, on_switch, classes="cb-seg-sm")

    def _on_owner_filter(self, key: str):
        self._owner_seg.set_active(key)
        self.prefs.prefs.show_only_mine = key == "mine"
        self.prefs.save_to_app_storage(app.storage.user)
        self._render_list()

    def _on_view(self, key: str):
        self._view_seg.set_active(key)
        self.prefs.prefs.projects_compact_view = key == "compact"
        self.prefs.save_to_app_storage(app.storage.user)
        self._render_list()

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
        options = {ANY_SPECIES: ANY_SPECIES_LABEL} | {
            sid: names[sid] for sid in sorted(names, key=lambda k: names[k].lower())
        }
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
        # Every hover card died with the rows.
        self._preview_open = False

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

        # The header carries one number: what the visible projects weigh on disk.
        if self._size_label is not None:
            measured = [p["disk_usage"] for p in visible if p.get("disk_usage") and not p["disk_usage"].error]
            unmeasured_n = len(visible) - len(measured)
            total = format_bytes(sum(u.total_bytes for u in measured)) if measured else ""
            self._size_label.set_text(f"{'≥ ' if measured and unmeasured_n else ''}{total}")
            # A native title, not .tooltip(): that would add a new tooltip element on every refresh.
            if measured and unmeasured_n:
                s = "s" if unmeasured_n != 1 else ""
                note = f"{unmeasured_n} project{s} not measured yet: the size is a lower bound"
                self._size_label.props(f'title="{note}"')
            else:
                self._size_label.props(remove="title")

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

            if self.prefs.prefs.projects_compact_view:
                # One flat list: the owner moves into each row, so no section breaks.
                for proj in sorted(visible, key=self._sort_key):
                    self._render_row(proj, compact=True)
                return

            # Group by effective owner -- it lives in a section header rather
            # than on every row. The current user floats to the top, then
            # Lab / Shared, then the rest alphabetically.
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

    # Fixed widths (px) for the cells that must line up from row to row. The name and
    # the two paths are the flexible cells and absorb all slack.
    _W_CHEVRON = 30
    _W_DELETE = 20
    # Compact view columns.
    _W_OWNER = 84
    _W_TS = 44
    _W_SIZE = 56
    _W_STATUS = 46

    def _render_row(self, proj: dict, *, compact: bool = False):
        path_str = proj["path"]
        name = proj["name"]
        ts_count = proj.get("ts_count") or 0
        last_activity = proj.get("last_activity") or proj.get("modified") or ""
        source_dir = proj.get("source_directory") or ""
        # The generated mnemonic ("icy-majestic-darwin") gets no column: it would compete
        # with the project's real name for the eye. It is one hover away on the name.
        mnemonic = proj.get("mnemonic") or ""

        is_current = False
        if self._current_resolved:
            try:
                is_current = str(Path(path_str).resolve()) == self._current_resolved
            except Exception:
                is_current = False

        async def _select():
            self.set_selected(path_str)
            if self.on_select is not None:
                try:
                    await self.on_select(Path(path_str))
                except Exception as e:
                    logger.info("Preview project failed: %s", e)

        # Only the chevron enters a project. In preview mode (on_select set) a row click
        # previews it; otherwise the row is not a click target at all, so a stray click
        # never drops the user into a project.
        preview_mode = self.on_select is not None
        is_selected = False
        if self._selected_resolved:
            try:
                is_selected = str(Path(path_str).resolve()) == self._selected_resolved
            except Exception:
                is_selected = False

        # The row is a two-column flex: the text stack, and a full-height chevron.
        # `overflow: hidden` is load-bearing, not cosmetic: this widget lives inside a
        # QScrollArea, which scrolls both ways — a row wider than the pane would grow a
        # horizontal pan and take the status and the travel affordance off screen.
        # Clipping means the roster degrades by ellipsising paths instead.
        base_style = (
            "display: flex; flex-direction: row; align-items: stretch; gap: 0; "
            f"overflow: hidden; border-bottom: 1px solid {CLR_BORDER};"
        )
        outline = " box-shadow: inset 0 0 0 1.5px #93c5fd;" if is_selected else ""
        cursor = " cursor: pointer;" if preview_mode else " cursor: default;"
        if is_current:
            row_classes = "w-full group"
            row_style = base_style + " background: #eff6ff; border-left: 3px solid #3b82f6;" + cursor + outline
        else:
            row_classes = "w-full hover:bg-slate-50 transition-colors group"
            row_style = base_style + cursor + outline

        row = ui.element("div").classes(row_classes).style(row_style)
        if preview_mode:
            row.on("click", _select)

        fixed = "flex-shrink: 0; white-space: nowrap;"
        with row as row_el:
            if proj.get("tomo_preview"):
                self._render_preview_card(proj, row)
            if compact:
                self._render_compact_body(proj, is_current, row_el)
                self._render_chevron(path_str, is_current)
                return
            with ui.element("div").style(
                "flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; gap: 3px; "
                "padding: 9px 8px 10px 14px;"
            ):
                # ---- Line 1: NAME | status ----
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 8px; flex-wrap: nowrap; min-width: 0;"
                ):
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
                    ui.element("div").style("flex: 1 1 0; min-width: 0;")
                    self._render_live_progress(proj)
                    self._render_status(proj)

                # ---- Lines 2-3: where the project lives, where its data came from ----
                self._render_path_line(path_str, color=CLR_LABEL, what="Project directory")
                if source_dir:
                    self._render_path_line(source_dir, color=CLR_SUBLABEL, what="Raw data the project was created from")
                else:
                    ui.label("no raw-data path recorded").style(
                        f"{MONO} font-size: 9px; color: {CLR_GHOST}; font-style: italic;"
                    )

                # ---- Line 4: TS · last activity · size | delete ----
                # Starts flush with the paths above; delete sits at the right edge. Wraps
                # rather than overflows on a narrow pane.
                with ui.element("div").style(
                    "display: flex; align-items: center; flex-wrap: wrap; column-gap: 18px; row-gap: 2px; "
                    "min-width: 0; margin-top: 2px;"
                ):
                    ui.label(f"{ts_count} TS" if ts_count else "— TS").style(
                        f"{MONO} font-size: 9px; color: {CLR_LABEL if ts_count else CLR_GHOST}; {fixed}"
                    )
                    ui.label(last_activity).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; {fixed}").tooltip(
                        "Last activity"
                    )
                    self._render_size(proj)
                    ui.element("div").style("flex: 1 1 0; min-width: 0;")
                    self._render_delete(path_str, name, row_el, is_current)

            # ---- Travel chevron: full row height, its own hit area ----
            # A full-height column on the right edge, so "go there" is the easiest
            # thing on the row to hit.
            self._render_chevron(path_str, is_current)

    def _render_chevron(self, path_str: str, is_current: bool):
        if is_current:
            with ui.element("div").style(
                f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; "
                "align-items: center; justify-content: center;"
            ):
                ui.icon("check", size="14px").style(f"color: {CLR_RUNNING};").tooltip("Currently open")
            return
        # A real <a href>, not a click handler: the travel arrow carries the project's
        # addressable URL, so ⌘-click / middle-click opens it in a new browser tab and
        # "copy link address" works. A plain click is the browser's own navigation to
        # the routed page, which loads the project, lands in the workspace, and reports
        # a missing project itself rather than through a toast on a page the user is leaving.
        # Its own tinted column that lights only on its own hover, never the row's: it
        # is the one control on the row that leaves this page.
        chev = (
            ui.link(target=self._project_url(path_str))
            .classes("cb-proj-chevron")
            .style(
                f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; "
                "align-items: center; justify-content: center; cursor: pointer; "
                f"border-left: 1px solid {CLR_BORDER}; background: #f8fafc; text-decoration: none; "
                "align-self: stretch; padding: 0;"
            )
            .tooltip("Enter this project")
        )
        with chev:
            ui.icon("chevron_right", size="20px").style(f"color: {CLR_SUBLABEL}; pointer-events: none;")

    @staticmethod
    def _project_url(path_str: str) -> str:
        """`/p/<dirname>?base=<parent>`. The base is always pinned here: the roster lists
        whatever directory the user pointed it at, which need not be one the resolver
        searches, and one stat-free explicit answer beats a per-row search on every
        refresh. The workspace drops the parameter again when it turns out to be
        redundant."""
        p = Path(path_str)
        return route_to_path(Route(project=p.name, base=str(p.parent)))

    def _render_delete(self, path_str: str, name: str, row_el, is_current: bool):
        """The hover-revealed delete button in a fixed slot -- empty on the open project's
        row, which can never be deleted from here -- so cells line up from row to row."""
        with ui.element("div").style(f"width: {self._W_DELETE}px; flex-shrink: 0; display: flex;"):
            if is_current:
                return

            async def _del():
                await self._handle_delete(Path(path_str), name, row_el)

            (
                ui.button(icon="delete_outline", on_click=_del)
                .props("flat dense round size=xs")
                .classes("text-slate-300 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity")
                .on("click.stop", lambda: None)
                .tooltip("Delete project")
            )

    def _render_compact_body(self, proj: dict, is_current: bool, row_el):
        """Compact view: one line -- name, live progress, owner, TS, size, status, delete. The
        owner sits in the row because the compact list has no owner sections."""
        name = proj["name"]
        ts_count = proj.get("ts_count") or 0
        mnemonic = proj.get("mnemonic") or ""
        fixed = "flex-shrink: 0; white-space: nowrap;"
        with ui.element("div").style(
            "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 12px; padding: 6px 8px 6px 14px;"
        ):
            name_lbl = ui.label(name).style(
                f"{FONT} font-size: 11px; font-weight: 600; color: {CLR_HEADING}; letter-spacing: -0.01em; "
                "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; flex: 1 1 auto;"
            )
            if mnemonic:
                name_lbl.tooltip(f"{name}\n{mnemonic}")
            self._render_live_progress(proj)
            ui.label(self._owner_label(self._eff_owner_of(proj))).style(
                f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; width: {self._W_OWNER}px; {fixed} "
                "overflow: hidden; text-overflow: ellipsis;"
            )
            ui.label(f"{ts_count} TS" if ts_count else "— TS").style(
                f"{MONO} font-size: 9px; color: {CLR_LABEL if ts_count else CLR_GHOST}; "
                f"width: {self._W_TS}px; text-align: right; {fixed}"
            )
            with ui.element("div").style(f"width: {self._W_SIZE}px; {fixed} display: flex; justify-content: flex-end;"):
                self._render_size(proj)
            with ui.element("div").style(f"width: {self._W_STATUS}px; {fixed} display: flex;"):
                self._render_status(proj)
            self._render_delete(proj["path"], name, row_el, is_current)

    @staticmethod
    def _owner_label(owner: str) -> str:
        if owner == SHARED_OWNER:
            return "Lab / Shared"
        if owner == CURRENT_USER:
            return "you"
        return owner

    def _render_preview_card(self, proj: dict, row) -> None:
        """Row hover: a white card beside the row (to its left, over the page, so it never
        covers the list) with the project's WarpTools tomogram previews, stepped with
        chevrons, and a few facts. It opens and closes in the browser (_hover_open_js) so
        it appears at once and can be moved onto; the roster's auto-refresh holds while
        it is open, because a rebuild would take it away from under the pointer."""
        pv = proj["tomo_preview"]
        pngs = pv["pngs"]
        name = proj["name"]
        # The scan's activity stamp is the cache-buster: a new reconstruction moves it.
        stamp = int(proj.get("last_activity_ts") or 0)
        ts_count = proj.get("ts_count") or 0
        facts = [f"{pv['n_tomos']}/{ts_count} tomograms" if ts_count else f"{pv['n_tomos']} tomograms"]
        if pv["apix"]:
            facts.append(f"{pv['apix']:.2f} Å/px")
        usage = proj.get("disk_usage")
        if usage is not None and not usage.error:
            facts.append(format_bytes(usage.total_bytes))
        species = ", ".join(s.get("name") or s["id"] for s in proj.get("species") or [])
        shown = {"i": 0}

        def caption(i: int) -> str:
            label = _PREVIEW_SUFFIX.sub("", Path(pngs[i]).name).removeprefix(f"{name}_")
            return f"{label} · {i + 1}/{len(pngs)}" if len(pngs) > 1 else label

        def step(delta: int) -> None:
            shown["i"] = (shown["i"] + delta) % len(pngs)
            image.set_content(_preview_img_html(pngs[shown["i"]], stamp))
            caption_label.set_text(caption(shown["i"]))

        menu = (
            ui.menu()
            .props(
                'no-parent-event no-focus no-refocus anchor="center left" self="center right" '
                ':offset="[4, 0]" transition-show="fade" transition-hide="fade" :transition-duration="80"'
            )
            .style(
                f"background: white; border: 1px solid {CLR_BORDER}; border-radius: 6px; "
                "box-shadow: 0 8px 24px rgba(15,23,42,0.16);"
            )
        )
        menu.on_value_change(self._on_preview_toggle)
        with menu:
            card = ui.element("div").style("padding: 8px; width: 276px;")
            card.on("mouseenter", js_handler=_HOVER_STAY)
            card.on("mouseleave", js_handler=_HOVER_CLOSE)
            with card:
                image = ui.html(_preview_img_html(pngs[0], stamp), sanitize=False)
                with ui.element("div").style("display: flex; align-items: center; gap: 2px; margin-top: 6px;"):
                    nav = "flat dense round size=sm"
                    if len(pngs) > 1:
                        ui.button(icon="chevron_left", on_click=lambda: step(-1)).props(nav).style(
                            f"color: {CLR_LABEL};"
                        ).tooltip("Previous tomogram")
                    caption_label = ui.label(caption(0)).style(
                        f"{MONO} font-size: 10px; color: {CLR_HEADING}; flex: 1 1 0; min-width: 0; "
                        "text-align: center; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                    )
                    if len(pngs) > 1:
                        ui.button(icon="chevron_right", on_click=lambda: step(1)).props(nav).style(
                            f"color: {CLR_LABEL};"
                        ).tooltip("Next tomogram")
                ui.label(" · ".join(facts)).style(
                    f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; text-align: center; margin-top: 2px;"
                )
                if species:
                    ui.label(f"species: {species}").style(
                        f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; text-align: center;"
                    )
        row.on("mouseenter", js_handler=_hover_open_js(menu.id))
        row.on("mouseleave", js_handler=_HOVER_CLOSE)

    def _on_preview_toggle(self, e):
        self._preview_open = bool(e.value)

    @staticmethod
    def _render_path_line(path: str, *, color: str, what: str):
        """One absolute path on its own line: tail-truncated, full text in the hover, and
        the text itself copies on click (no icon)."""
        label = ui.label(path).style(
            f"{MONO} font-size: 9px; color: {color}; min-width: 0; max-width: 100%; align-self: flex-start; "
            "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        )
        label.tooltip(f"{what}\n{path}\nClick to copy")
        copy_on_click(label, path)

    @staticmethod
    def _render_live_progress(proj: dict):
        """A live run at a glance (backend._live_progress): the job at work, its tilt-series
        done out of those in scope, and how many the run has failed so far, in red, since each
        is dropped from every job after it. Live projects only; the hover breaks it down."""
        lp = proj.get("live_progress")
        if not lp or proj.get("live_status") != "running":
            return
        if lp["state"] == "queued":
            lines = [f"{lp['stage']}: queued on the cluster"]
        elif lp["total"]:
            lines = [f"{lp['stage']}: {lp['ok']} of {lp['total']} tilt-series done, {lp['running']} running"]
        else:
            lines = [f"{lp['stage']}: running"]
        if lp["failed_by_stage"]:
            lines.append("Failed so far: " + ", ".join(f"{stage} {n}" for stage, n in lp["failed_by_stage"]))
        n_tomos = (proj.get("tomo_preview") or {}).get("n_tomos")
        if n_tomos:
            lines.append(f"{n_tomos} tomograms reconstructed")
        cell = f"{MONO} font-size: 9px; white-space: nowrap;"
        with ui.element("div").style("display: flex; align-items: baseline; gap: 5px; flex-shrink: 0;"):
            ui.label(lp["stage"]).style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_RUNNING}; white-space: nowrap;"
            )
            if lp["state"] == "queued":
                ui.label("queued").style(f"{cell} color: {CLR_SUBLABEL};")
            elif lp["total"]:
                ui.label(f"{lp['ok']}/{lp['total']}").style(f"{cell} color: {CLR_LABEL};")
            if lp["failed"]:
                ui.label(f"· {lp['failed']} failed").style(f"{cell} color: {CLR_FAILED};")
            ui.tooltip("\n".join(lines)).style("white-space: pre-line;")

    @staticmethod
    def _render_status(proj: dict):
        """done / failed / review / live; an idle project gets no badge. The live dot ripples
        (`.cb-live-pulse`, ui/dashboard/css.py). The job counts live in the hover."""
        status = proj.get("live_status") or "idle"
        s = _STATUS_STYLES.get(status)
        if s is None:
            return
        counts = [f"{proj.get('succeeded') or 0}/{proj.get('total_jobs_planned') or 0} jobs succeeded"]
        if status == "review":
            since = f" since {proj['review_since']}" if proj.get("review_since") else ""
            counts.insert(0, f"Waits for the tilt-filter review: {proj['review_parked']} job(s) parked{since}")
        if proj.get("running_live"):
            counts.append(f"{proj['running_live']} running")
        if proj.get("queued"):
            counts.append(f"{proj['queued']} queued")
        if proj.get("failed"):
            counts.append(f"{proj['failed']} failed")
        with (
            ui.element("div")
            .style("display: flex; align-items: center; gap: 4px; flex-shrink: 0;")
            .tooltip(" · ".join(counts))
        ):
            dot = ui.element("div").style(
                f"width: 6px; height: 6px; border-radius: 50%; background: {s['color']}; flex-shrink: 0;"
            )
            if status == "running":
                dot.classes("cb-live-pulse")
            ui.label(s["label"]).style(
                f"{FONT} font-size: 8px; color: {s['color']}; font-weight: 700; "
                "letter-spacing: 0.04em; line-height: 1; text-transform: uppercase;"
            )

    @staticmethod
    def _render_size(proj: dict):
        """On-disk size, coloured on the 50 GB -> 1 TB ramp, with the bucket breakdown in
        the hover. Never measured, failed and partly-unreadable each say so."""
        usage: DiskUsage | None = proj.get("disk_usage")
        cell = f"{MONO} font-size: 9px; flex-shrink: 0; white-space: nowrap;"
        if usage is None:
            ui.label("— GB").style(f"{cell} color: {CLR_GHOST};").tooltip(
                "Not measured yet: queued for a background size scan"
            )
            return
        if usage.error:
            ui.label("? GB").style(f"{cell} color: {CLR_FAILED};").tooltip(f"Size measurement failed: {usage.error}")
            return
        total = usage.total_bytes
        weight = 600 if total >= _SIZE_HEAVY else 400
        label = ui.label(("≥" if usage.unreadable_dirs else "") + format_bytes(total)).style(
            f"{cell} color: {size_color(total)}; font-weight: {weight};"
        )
        measured = datetime.fromtimestamp(usage.measured_at).strftime("%Y-%m-%d %H:%M")
        with label, ui.tooltip().style(f"{MONO} font-size: 10px; padding: 6px 8px;"):
            ui.label(f"{format_bytes(total)} on disk · measured {measured}")
            with ui.element("div").style(
                "display: grid; grid-template-columns: auto auto 1fr; column-gap: 12px; margin-top: 4px;"
            ):
                for bucket, title in _BUCKET_LABELS:
                    n = usage.buckets.get(bucket, 0)
                    if not n:
                        continue
                    top = sorted(usage.parts.get(bucket, {}).items(), key=lambda kv: -kv[1])
                    biggest = " · ".join(f"{lbl} {format_bytes(b)}" for lbl, b in top[:3] if b >= n / 100)
                    ui.label(title)
                    ui.label(format_bytes(n)).style("text-align: right;")
                    ui.label(biggest).style("opacity: 0.75;")
            if usage.unreadable_dirs:
                ui.label(
                    f"{usage.unreadable_dirs} director{'ies' if usage.unreadable_dirs != 1 else 'y'} unreadable "
                    "(permissions): the size is a lower bound"
                ).style("margin-top: 4px;")

    async def _handle_delete(self, project_dir: Path, name: str, row_el):
        """Full delete lifecycle owned by the component:
        1. show confirm dialog (anchored to outer container so it's not
           destroyed by row re-renders)
        2. on confirm, pause auto-refresh + grey out the row
        3. backend.delete_project (the rmtree)
        4. if that was the previewed project, preview the open one again
        5. resume auto-refresh and refresh the list"""
        if self._outer_container is None:
            return
        with self._outer_container:
            confirmed = await self._show_delete_confirm(project_dir, name)
        if not confirmed:
            return

        was_selected = self._selected_resolved is not None and str(project_dir.resolve()) == self._selected_resolved
        self._pause_refresh = True
        try:
            self._mark_row_deleting(row_el, name)
            result = await self.backend.delete_project(project_dir)
            if not result["success"]:
                ui.notify(f"Failed to delete: {result['error']}", type="negative")
                return
            ui.notify(f"Deleted '{name}'", type="positive")
            if was_selected and self.on_select is not None and self.current_path:
                self.set_selected(self.current_path)
                await self.on_select(Path(self.current_path))
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
