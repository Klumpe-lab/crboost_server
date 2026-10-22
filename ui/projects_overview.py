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
import html
import logging
import itertools
import math
import re
import urllib.parse
from datetime import datetime
from pathlib import Path
from collections.abc import Awaitable, Callable

from nicegui import ui, app

from services.background_tasks import get_background_task_registry
from services.configs.user_prefs_service import get_prefs_service
from services.project_disk_usage import BUCKET_OTHER, BUCKET_PARTICLES, BUCKET_PREPROCESSING, BUCKET_TRASH, DiskUsage
from services.project_state import SHARED_OWNER
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button, icon_button
from ui.components.copyable import copy_on_click
from ui.components.dialogs import dialog_host
from ui.components.fields import house_select
from ui.components.reactive import SingleFlight
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
CLR_QUEUED = "#93c5fd"
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
# Column sorts whose first click runs A→Z; every other column starts with the most on top.
_ASCENDING_FIRST = frozenset({"name", "owner", "species"})
# The Status column's order, top first: live work, then what waits on it, then finished, then failed. Idle
# projects have no badge and sort last. The list opens sorted this way (DEFAULT_COLUMN_SORT).
_STATUS_RANK = {"running": 4, "queued": 3, "review": 2, "done": 1, "failed": 0}
DEFAULT_COLUMN_SORT = ("status", True)

# WarpTools preview names end in the reconstruction's pixel size: `<tomo>_6.20Apx.png`.
_PREVIEW_SUFFIX = re.compile(r"_\d+(?:\.\d+)?Apx\.png$")

# Project hover card, run in the browser: no server round-trip to open it. The name column
# opens it; it stays open while the pointer is on its row or on it. One shared timer: entering
# the row or the card cancels a pending close, leaving either schedules one, and opening a
# card closes any other.
_HOVER_STATE = "(window.__cbHoverCard ??= {timer: 0, open: null})"
_HOVER_STAY = f"() => {{ clearTimeout({_HOVER_STATE}.timer); }}"
_HOVER_CLOSE = (
    f"() => {{ const s = {_HOVER_STATE}; clearTimeout(s.timer); s.timer = setTimeout(() => "
    "{ if (s.open !== null) runMethod(s.open, 'hide', []); s.open = null; }, 200); }"
)
# Re-fit the open card (its content may arrive after it shows), then say which side of the
# row Quasar put it on and aim its caret (`.cb-proj-card::before`, ui/dashboard/css.py) at
# the start of the name.
_HOVER_FIT = (
    "(s, id, zone) => { if (s.open !== id) return; runMethod(id, 'updatePosition', []); "
    "requestAnimationFrame(() => { const m = getHtmlElement(id), row = zone && zone.closest('.cb-proj-row'); "
    "if (!m || !row) return; "
    "const r = row.getBoundingClientRect(), z = zone.getBoundingClientRect(), b = m.getBoundingClientRect(); "
    "m.dataset.side = b.top >= r.bottom - 1 ? 'below' : 'above'; "
    "m.style.setProperty('--cb-caret-x', Math.min(Math.max(z.left + 24 - b.left, 14), b.width - 14) + 'px'); "
    "}); }"
)


def _hover_stay_js(menu_id: int) -> str:
    """Back on the row: keep its own card open, leave any other card's pending close alone."""
    return f"() => {{ const s = {_HOVER_STATE}; if (s.open === {menu_id}) clearTimeout(s.timer); }}"


def _hover_open_js(menu_id: int) -> str:
    """Open the name column's card after a short hover. The card is built on the server the first
    time its name column is entered (one emit per element), inside the delay before it shows."""
    return (
        f"(e) => {{ const s = {_HOVER_STATE}; clearTimeout(s.timer); const fit = {_HOVER_FIT}; "
        "const zone = e && e.currentTarget; "
        "if (zone && !zone.dataset.cbCard) { zone.dataset.cbCard = '1'; emit(); } "
        "s.timer = setTimeout(() => { "
        f"if (s.open !== null && s.open !== {menu_id}) runMethod(s.open, 'hide', []); "
        f"s.open = {menu_id}; runMethod({menu_id}, 'show', []); "
        f"for (const ms of [30, 200, 600]) setTimeout(() => fit(s, {menu_id}, zone), ms); }}, 90); }}"
    )


def _set_title(element, text: str):
    """A native hover title. The roster renders hundreds of cells, and a tooltip is one more
    element per cell to build on every render."""
    element.props["title"] = text
    return element


def _short_count(n: int) -> str:
    """842, 33.3k, 1.2M."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _preview_img_html(png: str, stamp: int) -> str:
    """Fixed 260 px box, so stepping between tomograms never resizes the card."""
    url = f"/api/project-preview?path={urllib.parse.quote(png, safe='')}&v={stamp}"
    return (
        f"<img src='{url}' alt='' style='width: 260px; height: 260px; object-fit: contain; "
        "display: block; border-radius: 3px; background: #f1f5f9;' />"
    )


def avatar_color(key: str) -> str:
    return _AVATAR_PALETTE[hash(key) % len(_AVATAR_PALETTE)]


# Background-task keys of the roster's own work, per project path. While one runs, its project is out
# of the list (a deletion) or its trash chip is (a clear), so nobody acts on what is going away.
_DELETE_KEY = "delete-project:"
_CLEAR_TRASH_KEY = "clear-trash:"


def _running_roster_tasks() -> frozenset[str]:
    return frozenset(
        t.dedup_key
        for t in get_background_task_registry().all()
        if t.is_running and t.dedup_key and t.dedup_key.startswith((_DELETE_KEY, _CLEAR_TRASH_KEY))
    )


# Only these statuses get a badge; an idle project renders none. "review": the pipeline
# waits for the tilt-filter review (backend._derive_live_status).
_STATUS_STYLES = {
    "running": {"color": CLR_RUNNING, "label": "live"},
    "queued": {"color": CLR_QUEUED, "label": "in queue"},
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
        with a "current" highlight and no click handler, and it can't be
        deleted (so the switcher keeps continuity but won't reload -- or
        delete -- the project the user is already in). Every other row can be
        deleted in deletion mode: the trash toggle in the header arms it, the
        whole roster outlines red, and a row click opens the confirm dialog
        (owned by the component). One deletion disarms it.
    show_filter : bool
        Whether to render the "Only mine" toggle in the header.
    height_px : int
        Vertical room for the scroll area.
    fill : bool
        Fill the parent's height instead (the parent is a flex column with a height): the
        rows scroll inside and the card ends at the parent's bottom edge.
    title_slot : optional callable () -> None
        Renders the header's title in place of the `title` label (the landing page's view switch).
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
        fill: bool = False,
        title: str = "Projects Overview",
        title_slot: Callable[[], None] | None = None,
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
        self.fill = fill
        self.title = title
        self.title_slot = title_slot
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
        self._column_header = None
        # What the list shows, in order: (key, fingerprint, element) per section header and row. A
        # render whose keys come out the same rebuilds only the entries whose fingerprint moved.
        self._rendered: list[tuple[str, object, ui.element]] = []
        self._size_label = None
        self._scan_note = None
        # True while a scan streams rows in: an empty list then means "not found yet".
        self._scanning = False
        self._sort_seg = None
        self._owner_seg = None
        self._view_seg = None
        self._species_select = None
        # The order the user last chose, in any roster (prefs); one that names a mode or a column this
        # roster no longer has falls back to the default.
        prefs = self.prefs.prefs
        self._sort_mode = prefs.projects_sort_mode if prefs.projects_sort_mode in dict(SORT_MODES) else DEFAULT_SORT
        # A clicked column title: (sort key, descending). Overrides _sort_mode until a mode is clicked.
        self._column_sort: tuple[str, bool] | None = DEFAULT_COLUMN_SORT
        column_sort = prefs.projects_column_sort
        if column_sort is None or column_sort[0] in {"name", *(c[0] for c in self._COMPACT_COLUMNS)}:
            self._column_sort = None if column_sort is None else (column_sort[0], bool(column_sort[1]))
        self._column_labels: dict[str, tuple[ui.label, str, bool]] = {}
        self._species_filter = ANY_SPECIES
        self._syncing_species = False
        self._mru_rank: dict[str, int] = {}
        self._timer = None
        self._last_scanned_base: str | None = None
        self._refresh_lock = asyncio.Lock()
        # Set while a row's hover card is open -- holds auto-refresh, since a
        # rebuild would destroy the card under the pointer. _render_list clears it.
        self._preview_open = False
        # The roster's running deletions and trash clears as of the last render.
        self._running_tasks: frozenset[str] = frozenset()
        # Deletion mode: armed by the header's trash toggle, a row click deletes.
        self._delete_mode = False
        self._delete_btn = None
        self._delete_hint = None
        self._flight = SingleFlight()

    # =====================================================================
    # PUBLIC API
    # =====================================================================

    def build(self):
        """Build the UI tree and return the outermost container.
        Triggers the first scan + starts the auto-refresh timer."""
        outer = (
            ui.column()
            # cb-proj-roster: the container the compact columns' width tiers query (ui/dashboard/css.py).
            .classes("w-full gap-0 cb-proj-roster")
            .style(
                # A pale gray-blue panel, the Journey's section look, holding the list on white. The
                # border is transparent until deletion mode colours it.
                "background: var(--cb-panel, #ffffff); border-radius: var(--cb-radius, 5px); "
                "border: 1px solid transparent; "
                "outline: 1px solid var(--cb-panel-line, #dbe3ec); outline-offset: -1px; "
                "padding: 0 8px 8px; box-sizing: border-box; "
                # Never wider than the pane it sits in: a narrow landing page clips
                # here rather than scrolling the whole roster sideways.
                "min-width: 0; max-width: 100%; overflow: hidden; flex-wrap: nowrap;"
                + (" flex: 1 1 0; min-height: 0;" if self.fill else "")
            )
        )
        self._outer_container = outer
        if self.fill:
            scroll_style = "flex: 1 1 0; min-height: 0;"
        else:
            scroll_style = f"height: {self.height_css or f'{self.height_px}px'};"
        with outer:
            self._build_header()
            self._column_header = self._build_column_header()
            with (
                ui.scroll_area()
                .classes("w-full cb-scroll-noh")
                .style(f"{scroll_style} padding: 0; background: #ffffff; border-radius: 4px;")
            ):
                self._list_container = ui.column().classes("w-full").style("gap: 0; padding: 0;")
            # The last scan of this base, from any roster, shows at once; the scan below brings it up
            # to date and re-renders only the rows that moved.
            base = (self.base_path_provider() or "").strip()
            cached = self.backend.last_project_scan(base) if base else None
            if cached is not None:
                self._projects = cached
                self._last_scanned_base = base
                self._render_list()
            else:
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

    def resume(self):
        """Restart the auto-refresh that `stop` ended (a kept dialog shown again) and rescan now. The
        rows on screen stay until the scan says what moved."""
        if self._outer_container is None or self._outer_container.is_deleted:
            return
        # A hover card does not outlive the dialog it was in; left set, it would hold every refresh.
        self._preview_open = False
        with self._outer_container:
            ui.timer(0.05, self.refresh, once=True)
            if self._timer is None and self.auto_refresh_sec > 0:
                self._timer = ui.timer(self.auto_refresh_sec, self.refresh)

    async def refresh(self, force: bool = False):
        """Re-scan the base path and re-render (a timer tick only when something changed; `force`
        always). Re-entrant-safe via lock.
        Self-cancels if the underlying container has been destroyed —
        guards against a stale tick firing after the dialog/page is gone."""
        if self._list_container is None:
            self.stop()
            return
        if self._preview_open:
            return
        async with self._refresh_lock:
            base = (self.base_path_provider() or "").strip()
            # Nothing of this base on screen yet (first scan, or another base): rows show as the
            # scan finds them instead of after the whole base is read.
            stream = bool(base) and base != self._last_scanned_base
            try:
                if stream:
                    self._scanning = True
                    projects = await self.backend.scan_for_projects(base, on_partial=self._show_partial)
                else:
                    projects = await self.backend.scan_for_projects(base) if base else []
                self._scanning = False
                # The timer rescans every tick; rebuild the rows (and their hover menus) only when
                # the scan, the MRU order or the running deletions moved. Filter, sort and selection
                # changes re-render through their own handlers.
                unchanged = (
                    not force
                    and base == self._last_scanned_base
                    and projects == self._projects
                    and self.prefs.prefs.recent_project_rank() == self._mru_rank
                    and _running_roster_tasks() == self._running_tasks
                )
                self._projects = projects
                self._last_scanned_base = base
                if not unchanged:
                    self._render_list()
            except RuntimeError as e:
                # Client gone (tab closed / navigation race) — stop ticking.
                logger.info("ProjectsOverview: client gone, stopping (%s)", e)
                self.stop()
            except Exception as e:
                logger.info("ProjectsOverview refresh failed: %s", e)
            finally:
                self._scanning = False
                self._set_scan_note(None)

    def set_selected(self, selected_path: str | None):
        """Mark a row as the previewed project (faint outline) and re-render.
        No-op-safe if the list isn't mounted yet."""
        try:
            self._selected_resolved = str(Path(selected_path).resolve()) if selected_path else None
        except Exception:
            self._selected_resolved = None
        self._render_list()

    def _show_partial(self, found: list[dict]) -> None:
        """A streaming scan's projects so far: render them (new rows only, see _patch_list)."""
        if self._list_container is None or self._list_container.is_deleted:
            return
        self._set_scan_note(f"Scanning… {len(found)} found")
        self._projects = found
        self._render_list()

    def _set_scan_note(self, text: str | None) -> None:
        if self._scan_note is None or self._scan_note.is_deleted:
            return
        self._scan_note.set_text(text or "")
        self._scan_note.set_visibility(text is not None)

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
        with ui.row().classes("w-full items-center px-3 py-2").style("gap: 6px 10px; flex-wrap: wrap;"):
            if self.title_slot is not None:
                self.title_slot()
            else:
                ui.label(self.title).style(
                    f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; "
                    "letter-spacing: -0.01em; flex-shrink: 0;"
                )
            self._size_label = ui.label("").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;")
            # Shown while a first scan streams its rows in.
            self._scan_note = ui.label("").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;")
            self._scan_note.set_visibility(False)
            self._delete_hint = ui.label("Deletion mode · click a project to delete it").style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_FAILED}; letter-spacing: 0.02em; "
                "flex-shrink: 0; white-space: nowrap;"
            )
            ui.element("div").style("flex: 1 1 0; min-width: 0;")

            # Every choice in this row is the same small two-way strip, so the row reads as
            # one set of controls and each state is named rather than implied by a knob.
            # In the full view sorting happens inside each owner section, so the
            # Lab/Shared grouping is unaffected by it.
            self._sort_seg = self._small_segmented(
                SORT_MODES,
                "" if self._column_sort else self._sort_mode,
                self._on_sort,
                hint="Order: last opened by you, or created",
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
                hint="Compact: one line per project. Full: paths and last activity",
            )

            if self.on_browse is not None:
                icon_button("folder_open", self.on_browse, tooltip="Browse for another base location")
            self._delete_btn = icon_button(
                "delete_outline",
                self._toggle_delete_mode,
                tooltip="Deletion mode: arm it, then click the project to delete. Click again to disarm.",
                kind="danger",
            )
            self._style_delete_mode()
            # Refresh button -- explicit re-scan in addition to the timer.
            icon_button("refresh", self.refresh, tooltip=f"Rescan now (auto every {int(self.auto_refresh_sec)}s)")

    def _build_column_header(self):
        """Compact view: the column titles, above the scroll area so they stay put. Same widths,
        gap and padding as `_render_compact_body`, plus the chevron's column. A title click
        sorts by its column, a second click reverses it (`_on_column_sort`)."""
        head = ui.element("div").style(
            f"display: flex; align-items: center; gap: 12px; padding: 4px {8 + self._W_CHEVRON}px 4px 14px; "
            "width: 100%;"
        )
        with head:
            columns = [("name", "Project", None, False, ""), *self._COMPACT_COLUMNS]
            for key, title, width, right, tier in columns:
                size = "flex: 1 1 auto; min-width: 0;" if width is None else f"width: {width}px; flex-shrink: 0;"
                label = (
                    ui.label(title)
                    .classes("cb-proj-colhead")
                    .style(f"{size}{' text-align: right;' if right else ''}")
                    .on("click", lambda _e, k=key: self._on_column_sort(k))
                )
                if tier:
                    label.classes(tier)
                self._column_labels[key] = (label, title, right)
        self._paint_column_header()
        head.set_visibility(False)
        return head

    def _paint_column_header(self) -> None:
        """The sorted column's title in the label color with its arrow, on the side that keeps
        the title flush with its column's content."""
        for key, (label, title, right) in self._column_labels.items():
            if self._column_sort is not None and self._column_sort[0] == key:
                arrow = "↓" if self._column_sort[1] else "↑"
                label.set_text(f"{arrow} {title}" if right else f"{title} {arrow}")
                label.classes(add="active")
            else:
                label.set_text(title)
                label.classes(remove="active")

    def _on_column_sort(self, key: str) -> None:
        """First click sorts by the column (text up, numbers and states down), a second reverses
        it. It replaces the Last opened / Created order until one of those is clicked."""
        if self._column_sort is not None and self._column_sort[0] == key:
            self._column_sort = (key, not self._column_sort[1])
        else:
            self._column_sort = (key, key not in _ASCENDING_FIRST)
        if self._sort_seg is not None:
            self._sort_seg.set_active("")
        self._save_sort()
        self._paint_column_header()
        self._render_list()

    def _save_sort(self) -> None:
        """The next roster, here or on another page, opens in this order."""
        self.prefs.prefs.projects_sort_mode = self._sort_mode
        self.prefs.prefs.projects_column_sort = self._column_sort
        self.prefs.save_to_app_storage(app.storage.user)

    @staticmethod
    def _small_segmented(tabs, active: str, on_switch: Callable[[str], None], *, hint: str):
        """A 16 px segmented strip (`cb-seg-sm`) with a hover hint."""
        with ui.element("div").style("display: flex; flex-shrink: 0;").tooltip(hint):
            return render_segmented(list(tabs), active, on_switch, classes="cb-seg-sm")

    def _toggle_delete_mode(self):
        self._set_delete_mode(not self._delete_mode)

    def _set_delete_mode(self, on: bool):
        """Arm or disarm deletion mode. Armed, the rows re-render as delete targets: a click
        opens the confirm dialog, nothing previews, and the travel chevron is a trash glyph,
        so no click on a row leaves the page."""
        self._delete_mode = on
        self._style_delete_mode()
        self._render_list()

    def _style_delete_mode(self):
        """The header toggle, its hint and the roster's outline, for the current mode. The
        armed outline breathes (`.cb-proj-delmode`, ui/dashboard/css.py)."""
        on = self._delete_mode
        if self._delete_btn is not None:
            self._delete_btn.props(f"icon={'delete' if on else 'delete_outline'}")
            if on:
                self._delete_btn.classes(add="cb-icon-btn--armed")
            else:
                self._delete_btn.classes(remove="cb-icon-btn--armed")
        if self._delete_hint is not None:
            self._delete_hint.set_visibility(on)
        if self._outer_container is not None:
            self._outer_container.style(f"border: 1px solid {CLR_FAILED if on else 'transparent'};")
            if on:
                self._outer_container.classes(add="cb-proj-delmode")
            else:
                self._outer_container.classes(remove="cb-proj-delmode")

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
        self._column_sort = None
        if self._sort_seg is not None:
            self._sort_seg.set_active(key)
        self._save_sort()
        self._paint_column_header()
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
        rank = self._mru_rank.get(proj["resolved_path"])
        return (0 if rank is not None else 1, rank if rank is not None else 0, -(proj.get("last_activity_ts") or 0.0))

    def _sorted(self, projects: list[dict]) -> list[dict]:
        """By the clicked column when there is one, else by `_sort_key`. A project the column has
        nothing for always goes last, in `_sort_key` order, whichever way the column runs."""
        base = sorted(projects, key=self._sort_key)
        if self._column_sort is None:
            return base
        key, descending = self._column_sort
        valued = [(self._column_value(key, p), p) for p in base]
        known = sorted(((v, p) for v, p in valued if v is not None), key=lambda vp: vp[0], reverse=descending)
        return [p for _, p in known] + [p for v, p in valued if v is None]

    def _column_value(self, key: str, proj: dict):
        """What a column sorts on; None when the project has nothing there. Text sorts as lowercase,
        states by how much they want attention (failed first)."""
        if key == "name":
            return proj["name"].lower()
        if key == "live":
            lp = proj.get("live_progress")
            if not lp or proj.get("live_status") not in ("running", "queued"):
                return None
            return (lp["state"] != "queued", lp.get("failed") or 0, lp["stage"].lower())
        if key == "owner":
            return self._owner_label(self._eff_owner_of(proj)).lower()
        if key == "furthest":
            return (proj.get("furthest_done") or {}).get("position")
        if key == "jobs":
            total = proj.get("total_jobs_planned") or 0
            return (proj.get("succeeded") or 0, total) if total else None
        if key == "species":
            names = [s.get("name") or s["id"] for s in proj.get("species") or []]
            return " ".join(names).lower() if names else None
        if key == "size":
            usage = proj.get("disk_usage")
            return usage.total_bytes if usage is not None and not usage.error else None
        if key == "status":
            return _STATUS_RANK.get(proj.get("live_status") or "idle")
        value = {
            "ts": proj.get("ts_count"),
            "tomos": (proj.get("tomo_preview") or {}).get("n_tomos"),
            "picks": proj.get("n_picks"),
            "apix": proj.get("apix"),
            "kv": proj.get("voltage_kv"),
            "dose": proj.get("dose_per_tilt"),
            "activity": proj.get("last_activity_ts"),
            "trash": proj.get("trash_count"),
        }[key]
        return value or None

    # =====================================================================
    # LIST
    # =====================================================================

    def _render_loading_skeleton(self):
        with ui.row().classes("w-full items-center justify-center").style("padding: 24px 0;"):
            ui.spinner("dots", size="sm").style(f"color: {CLR_SUBLABEL};")
            ui.label("Scanning…").style(f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; margin-left: 8px;")

    def _render_list(self):
        # Deleted when the tab closed while refresh() awaited the scan.
        if self._list_container is None or self._list_container.is_deleted:
            return

        self._running_tasks = _running_roster_tasks()
        all_projects = [p for p in self._projects if _DELETE_KEY + p["path"] not in self._running_tasks]
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

        compact = self.prefs.prefs.projects_compact_view
        items = self._list_items(visible, compact)
        if self._column_header is not None:
            self._column_header.set_visibility(compact and bool(items))

        if items and self._rendered:
            self._patch_list(items)
            return

        self._list_container.clear()
        self._rendered = []
        # Every hover card died with the rows.
        self._preview_open = False
        with self._list_container:
            if not visible:
                msg = "No projects to show"
                base = self._last_scanned_base or ""
                if self._scanning:
                    msg = "Scanning…"
                elif not base:
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
            for key, fingerprint, build in items:
                self._rendered.append((key, fingerprint, build()))

    def _list_items(self, visible: list[dict], compact: bool) -> list[tuple[str, object, Callable[[], ui.element]]]:
        """What the list shows, in order: (key, fingerprint, builder) per section header and row.
        The fingerprint holds everything the entry's render reads."""
        items: list[tuple[str, object, Callable[[], ui.element]]] = []

        def add_row(proj: dict) -> None:
            path = proj["path"]
            fingerprint = (
                proj,
                compact,
                self._delete_mode,
                proj["resolved_path"] == self._current_resolved,
                proj["resolved_path"] == self._selected_resolved,
                _CLEAR_TRASH_KEY + path in self._running_tasks,
            )
            items.append((f"row:{path}", fingerprint, lambda: self._render_row(proj, compact=compact)))

        if compact:
            # One flat list: the owner moves into each row, so no section breaks.
            for proj in self._sorted(visible):
                add_row(proj)
            return items

        # Group by effective owner -- it lives in a section header rather
        # than on every row. The current user floats to the top, then
        # Lab / Shared, then the rest alphabetically.
        groups: dict[str, list[dict]] = {}
        for proj in visible:
            groups.setdefault(self._eff_owner_of(proj), []).append(proj)

        def _section_order(k: str):
            if k == CURRENT_USER:
                return (0, "")
            if k == SHARED_OWNER:
                return (1, "")
            return (2, k.lower())

        for owner in sorted(groups, key=_section_order):
            rows = self._sorted(groups[owner])
            statuses = tuple(p.get("live_status") for p in rows)
            items.append(
                (f"owner:{owner}", (owner, statuses), lambda o=owner, r=rows: self._render_section_header(o, r))
            )
            for proj in rows:
                add_row(proj)
        return items

    def _patch_list(self, items: list[tuple[str, object, Callable[[], ui.element]]]) -> None:
        """Bring the shown entries to `items` by key: build the new ones and those whose fingerprint
        moved, move the rest into place, delete the ones gone. A timer tick usually changes one live
        project, a streaming scan only adds rows, and a full render of a long roster blocks the event
        loop for a second."""
        wanted = {key for key, _, _ in items}
        old: dict[str, tuple[object, ui.element]] = {}
        for key, fingerprint, element in self._rendered:
            if key in wanted:
                old[key] = (fingerprint, element)
            else:
                element.delete()
                self._preview_open = False
        rendered: list[tuple[str, object, ui.element]] = []
        for i, (key, fingerprint, build) in enumerate(items):
            prev = old.get(key)
            if prev is not None and prev[0] == fingerprint:
                element = prev[1]
            else:
                with self._list_container:
                    element = build()
                if prev is not None:
                    prev[1].delete()
                    # A replaced row takes its hover card with it, open or not.
                    self._preview_open = False
            if self._list_container.default_slot.children.index(element) != i:
                element.move(target_index=i)
            rendered.append((key, fingerprint, element))
        self._rendered = rendered

    def _render_section_header(self, creator: str, projects: list[dict]) -> ui.element:
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
        queued = sum(1 for p in projects if p.get("live_status") == "queued")
        header = (
            ui.row()
            .classes("w-full items-center")
            .style("gap: 6px; padding: 5px 12px; background: var(--cb-panel, #ffffff);")
        )
        with header:
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
            if queued:
                ui.label(f"· {queued} in queue").style(f"{MONO} font-size: 8px; color: {CLR_QUEUED};")
        return header

    # =====================================================================
    # ROW
    # =====================================================================

    # Fixed widths (px) for the cells that must line up from row to row. The name and
    # the two paths are the flexible cells and absorb all slack.
    _W_CHEVRON = 30
    # Compact view columns.
    _W_OWNER = 84
    _W_FURTHEST = 104
    _W_JOBS = 84
    _W_TS = 44
    _W_TOMOS = 44
    _W_PICKS = 48
    _W_SPECIES = 120
    _W_SIZE = 56
    _W_STATUS = 56  # fits "IN QUEUE", the widest badge
    _W_APIX = 44
    _W_KV = 40
    _W_DOSE = 62
    _W_ACTIVITY = 76
    _W_TRASH = 52
    _W_LIVE = 120

    # The compact view's columns after the name, in row order (`_render_compact_body`):
    # (sort key, title, width, right-aligned, tier). A narrow roster hides tier 1 first, then 2,
    # then 3 (`.cb-proj-roster` container queries, ui/dashboard/css.py), so the status and the
    # chevron never leave the row. A title click sorts by its column (`_column_value`).
    _COMPACT_COLUMNS = (
        ("live", "Running", _W_LIVE, False, ""),
        ("owner", "Owner", _W_OWNER, False, "cb-pc-t2"),
        ("furthest", "Furthest job", _W_FURTHEST, False, "cb-pc-t3"),
        ("jobs", "Jobs", _W_JOBS, True, ""),
        ("ts", "TS", _W_TS, True, ""),
        ("tomos", "Tomos", _W_TOMOS, True, "cb-pc-t3"),
        ("picks", "Picks", _W_PICKS, True, "cb-pc-t2"),
        ("species", "Species", _W_SPECIES, False, "cb-pc-t1"),
        ("apix", "Å/px", _W_APIX, True, "cb-pc-t2"),
        ("kv", "kV", _W_KV, True, "cb-pc-t1"),
        ("dose", "Dose", _W_DOSE, True, "cb-pc-t1"),
        ("activity", "Activity", _W_ACTIVITY, True, "cb-pc-t2"),
        ("size", "Size", _W_SIZE, True, ""),
        ("trash", "Trash", _W_TRASH, True, ""),
        ("status", "Status", _W_STATUS, False, ""),
    )

    def _render_row(self, proj: dict, *, compact: bool = False) -> ui.element:
        path_str = proj["path"]
        name = proj["name"]
        ts_count = proj.get("ts_count") or 0
        source_dir = proj.get("source_directory") or ""
        is_current = proj["resolved_path"] == self._current_resolved

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
        is_selected = proj["resolved_path"] == self._selected_resolved

        # The row is a two-column flex: the text stack, and a full-height chevron.
        # `overflow: hidden` is load-bearing, not cosmetic: this widget lives inside a
        # QScrollArea, which scrolls both ways — a row wider than the pane would grow a
        # horizontal pan and take the status and the travel affordance off screen.
        # Clipping means the roster degrades by ellipsising paths instead.
        base_style = (
            "display: flex; flex-direction: row; align-items: stretch; gap: 0; "
            "overflow: hidden; border-bottom: 1px solid #f1f5f9;"
        )
        outline = " box-shadow: inset 0 0 0 1.5px #93c5fd;" if is_selected else ""
        deleting = self._delete_mode
        cursor = " cursor: pointer;" if preview_mode or deleting else " cursor: default;"
        if is_current:
            row_classes = "w-full group"
            row_style = base_style + " background: #eff6ff; border-left: 3px solid #3b82f6;" + cursor + outline
            if deleting:
                row_style += " opacity: 0.5; cursor: not-allowed;"
        elif deleting:
            row_classes = "w-full group cb-proj-delrow"
            row_style = base_style + cursor + outline
        else:
            row_classes = "w-full hover:bg-slate-50 transition-colors group"
            row_style = base_style + cursor + outline

        row = ui.element("div").classes(f"{row_classes} cb-proj-row").style(row_style)
        if deleting:
            # Deletion mode: the row is the trigger and nothing else on it acts.
            if is_current:
                _set_title(row, "The open project can't be deleted from here")
            else:

                async def _delete():
                    await self._handle_delete(proj)

                row.on("click", _delete)
        elif preview_mode:
            row.on("click", _select)

        cell = self._text_cell
        with row:
            if compact:
                self._render_compact_body(proj, row)
                self._render_chevron(path_str, is_current)
                return row
            with ui.element("div").style(
                "flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; gap: 3px; "
                "padding: 9px 8px 10px 14px;"
            ):
                # ---- Line 1: NAME | status ----
                with ui.element("div").style(
                    "display: flex; align-items: center; gap: 8px; flex-wrap: nowrap; min-width: 0;"
                ):
                    with self._name_zone(gap=8) as zone:
                        # The name is the one thing in this widget with weight. Everything
                        # else on the row is 8-9 px meta, so 12/600 reads as the title
                        # without needing a rule or a background to say so.
                        ui.label(name).style(
                            f"{FONT} font-size: 12px; font-weight: 600; color: {CLR_HEADING}; "
                            "letter-spacing: -0.01em; overflow: hidden; text-overflow: ellipsis; "
                            "white-space: nowrap; min-width: 0; flex: 0 1 auto;"
                        )
                        if is_current:
                            ui.label("CURRENT").style(
                                f"{FONT} font-size: 7px; color: #1e40af; font-weight: 700; "
                                "background: #dbeafe; border: 1px solid #93c5fd; "
                                "border-radius: 3px; padding: 0 4px; flex-shrink: 0; "
                                "letter-spacing: 0.05em;"
                            )
                        self._render_hover_card(proj, zone, row)
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

                # ---- Line 4: TS · tomograms · jobs · furthest job · picks · species · acquisition ·
                # last activity · size. Starts flush with the paths above; wraps rather than
                # overflows on a narrow pane. What the project has none of is left out, the TS aside.
                with ui.element("div").style(
                    "display: flex; align-items: center; flex-wrap: wrap; column-gap: 18px; row-gap: 2px; "
                    "min-width: 0; margin-top: 2px;"
                ):
                    cell(f"{ts_count} TS" if ts_count else "— TS", color=CLR_LABEL if ts_count else CLR_GHOST)
                    text, title = self._tomos_text(proj)
                    if text != "—":
                        cell(f"{text} tomos", color=CLR_LABEL, title=title)
                    self._render_jobs(proj)
                    if proj.get("furthest_done"):
                        text, title = self._furthest_text(proj)
                        cell(f"→ {text}", color=CLR_LABEL, title=title, font=FONT)
                    text, title = self._picks_text(proj)
                    if text != "—":
                        cell(f"{text} picks", color=CLR_LABEL, title=title)
                    if proj.get("species"):
                        text, title = self._species_text(proj)
                        cell(text, color=CLR_SUBLABEL, title=title, font=FONT)
                    for text, tip, known in self._acquisition_cells(proj):
                        cell(text, color=CLR_LABEL if known else CLR_GHOST, title=tip)
                    text, title = self._activity_text(proj)
                    cell(text, color=CLR_SUBLABEL, title=title)
                    self._render_size(proj)
                    self._render_trash(proj)

            # ---- Travel chevron: full row height, its own hit area ----
            # A full-height column on the right edge, so "go there" is the easiest
            # thing on the row to hit.
            self._render_chevron(path_str, is_current)
        return row

    def _render_chevron(self, path_str: str, is_current: bool):
        if is_current:
            with ui.element("div").style(
                f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; "
                "align-items: center; justify-content: center;"
            ):
                _set_title(ui.icon("check", size="14px").style(f"color: {CLR_RUNNING};"), "Currently open")
            return
        if self._delete_mode:
            # Armed: the travel column becomes a trash glyph that lights with the row
            # (`.cb-proj-deltrash`), so the one control that leaves the page is gone.
            with (
                ui.element("div")
                .classes("cb-proj-deltrash")
                .style(
                    f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; align-items: center; "
                    "justify-content: center; align-self: stretch;"
                )
            ):
                ui.icon("delete_outline", size="16px")
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
                "background: #f8fafc; text-decoration: none; "
                "align-self: stretch; padding: 0;"
            )
        )
        _set_title(chev, "Enter this project")
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

    def _render_compact_body(self, proj: dict, row: ui.element):
        """Compact view: one line -- the name, then the columns of `_COMPACT_COLUMNS` in that
        order, pushed to the row's right end. The owner sits in the row because the compact list
        has no owner sections."""
        ts_count = proj.get("ts_count") or 0
        fixed = "flex-shrink: 0; white-space: nowrap;"
        cell = self._text_cell
        with ui.element("div").style(
            "flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: 12px; padding: 6px 8px 6px 14px;"
        ):
            with self._name_zone(gap=12) as zone:
                ui.label(proj["name"]).style(
                    f"{FONT} font-size: 11px; font-weight: 600; color: {CLR_HEADING}; letter-spacing: -0.01em; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; flex: 0 1 auto;"
                )
                self._render_hover_card(proj, zone, row)
            with ui.element("div").style(f"width: {self._W_LIVE}px; {fixed} display: flex; overflow: hidden;"):
                self._render_live_progress(proj)
            cell(self._owner_label(self._eff_owner_of(proj)), color=CLR_SUBLABEL, width=self._W_OWNER, tier="cb-pc-t2")
            text, title = self._furthest_text(proj)
            known = proj.get("furthest_done") is not None
            cell(
                text,
                color=CLR_LABEL if known else CLR_GHOST,
                title=title,
                width=self._W_FURTHEST,
                tier="cb-pc-t3",
                font=FONT,
            )
            self._render_jobs(proj, width=self._W_JOBS)
            cell(
                str(ts_count) if ts_count else "—",
                color=CLR_LABEL if ts_count else CLR_GHOST,
                title=f"{ts_count} tilt-series" if ts_count else "No tilt-series recorded",
                width=self._W_TS,
                right=True,
            )
            for (text, title), width, tier in (
                (self._tomos_text(proj), self._W_TOMOS, "cb-pc-t3"),
                (self._picks_text(proj), self._W_PICKS, "cb-pc-t2"),
            ):
                known = text != "—"
                cell(text, color=CLR_LABEL if known else CLR_GHOST, title=title, width=width, right=True, tier=tier)
            text, title = self._species_text(proj)
            known = bool(proj.get("species"))
            cell(
                text,
                color=CLR_SUBLABEL if known else CLR_GHOST,
                title=title,
                width=self._W_SPECIES,
                tier="cb-pc-t1",
                font=FONT,
            )
            acquisition = zip(
                self._acquisition_cells(proj),
                (self._W_APIX, self._W_KV, self._W_DOSE),
                ("cb-pc-t2", "cb-pc-t1", "cb-pc-t1"),
                strict=True,
            )
            for (text, tip, known), width, tier in acquisition:
                cell(text, color=CLR_LABEL if known else CLR_GHOST, title=tip, width=width, right=True, tier=tier)
            text, title = self._activity_text(proj)
            cell(text, color=CLR_SUBLABEL, title=title, width=self._W_ACTIVITY, right=True, tier="cb-pc-t2")
            with ui.element("div").style(f"width: {self._W_SIZE}px; {fixed} display: flex; justify-content: flex-end;"):
                self._render_size(proj)
            with ui.element("div").style(
                f"width: {self._W_TRASH}px; {fixed} display: flex; justify-content: flex-end;"
            ):
                self._render_trash(proj)
            with ui.element("div").style(f"width: {self._W_STATUS}px; {fixed} display: flex;"):
                self._render_status(proj)

    @staticmethod
    def _text_cell(
        text: str,
        *,
        color: str,
        title: str = "",
        width: int | None = None,
        right: bool = False,
        tier: str = "",
        font: str = MONO,
    ) -> ui.label:
        """One 9 px cell. With `width` it is a fixed column that ellipsises; `tier` hides it
        on a narrow roster."""
        style = f"{font} font-size: 9px; color: {color}; flex-shrink: 0; white-space: nowrap;"
        if width is not None:
            style += f" width: {width}px; overflow: hidden; text-overflow: ellipsis;"
            if right:
                style += " text-align: right;"
        label = ui.label(text).style(style)
        if tier:
            label.classes(tier)
        if title:
            _set_title(label, title)
        return label

    @staticmethod
    def _render_jobs(proj: dict, *, width: int | None = None) -> None:
        """Succeeded of planned jobs, the failed ones in red; running and queued in the hover."""
        ok, total, failed = (proj.get(key) or 0 for key in ("succeeded", "total_jobs_planned", "failed"))
        parts = [f"{ok} of {total} jobs succeeded"]
        for key, word in (("running_live", "running"), ("queued", "queued"), ("failed", "failed")):
            if proj.get(key):
                parts.append(f"{proj[key]} {word}")
        style = "display: flex; align-items: baseline; gap: 5px; flex-shrink: 0; white-space: nowrap;"
        if width is not None:
            style += f" width: {width}px; justify-content: flex-end; overflow: hidden;"
        text = (f"{ok}/{total}" if width is not None else f"{ok}/{total} jobs") if total else "—"
        with _set_title(ui.element("div").style(style), " · ".join(parts)):
            ui.label(text).style(f"{MONO} font-size: 9px; color: {CLR_LABEL if total else CLR_GHOST};")
            if failed:
                ui.label(f"{failed} failed").style(f"{MONO} font-size: 9px; color: {CLR_FAILED};")

    @staticmethod
    def _furthest_text(proj: dict) -> tuple[str, str]:
        done = proj.get("furthest_done")
        if not done:
            return "—", "No job has succeeded yet"
        return done["label"], f"Furthest job down the pipeline that succeeded: {done['label']} ({done['instance_id']})"

    @staticmethod
    def _tomos_text(proj: dict) -> tuple[str, str]:
        n = (proj.get("tomo_preview") or {}).get("n_tomos") or 0
        if not n:
            return "—", "No reconstructed tomograms"
        return str(n), f"{n} tomograms in the newest reconstruction job"

    @staticmethod
    def _picks_text(proj: dict) -> tuple[str, str]:
        n, lists = proj.get("n_picks") or 0, proj.get("n_pick_lists") or 0
        if not n:
            return "—", "No picks"
        return _short_count(n), f"{n:,} picks in {lists} pick list{'s' if lists != 1 else ''}"

    @staticmethod
    def _species_text(proj: dict) -> tuple[str, str]:
        names = [s.get("name") or s["id"] for s in proj.get("species") or []]
        if not names:
            return "—", "No particle species registered"
        return " · ".join(names), "Particle species: " + ", ".join(names)

    @staticmethod
    def _activity_text(proj: dict) -> tuple[str, str]:
        ts = proj.get("last_activity_ts") or 0.0
        if not ts:
            return "—", "No activity recorded"
        when = datetime.fromtimestamp(ts)
        short = when.strftime("%b %d %H:%M") if when.year == datetime.now().year else when.strftime("%Y-%m-%d")
        return short, f"Last activity {when.strftime('%Y-%m-%d %H:%M')}"

    @staticmethod
    def _acquisition_cells(proj: dict) -> list[tuple[str, str, bool]]:
        """(text, tooltip, recorded) for pixel size, voltage and dose per tilt, as the project
        recorded them. A project that recorded none says "—"; an estimated dose carries "~"."""
        apix, kv, dose = proj.get("apix"), proj.get("voltage_kv"), proj.get("dose_per_tilt")
        source = proj.get("dose_source") or ""
        dose_tip = {
            "mdoc": "Dose per tilt, read from the mdoc",
            "estimated": "Dose per tilt, estimated from the mdoc DoseRate",
            "user": "Dose per tilt, entered by hand",
        }.get(source, "Dose per tilt")
        approx = "~" if source == "estimated" else ""
        return [
            (f"{apix:.3g} Å" if apix else "— Å", "Pixel size of the raw movies (Å/px)", bool(apix)),
            (f"{kv:.0f} kV" if kv else "— kV", "Acceleration voltage", bool(kv)),
            (f"{approx}{dose:.3g} e⁻/Å²" if dose else "— e⁻/Å²", dose_tip, bool(dose)),
        ]

    @staticmethod
    def _owner_label(owner: str) -> str:
        if owner == SHARED_OWNER:
            return "Lab / Shared"
        if owner == CURRENT_USER:
            return "you"
        return owner

    @staticmethod
    def _name_zone(*, gap: int) -> ui.element:
        """The row's name column: the name and what sits beside it, out to the next column. Only
        this area opens the project's hover card, and the card's caret points back into it."""
        return ui.element("div").style(
            f"flex: 1 1 0; min-width: 0; display: flex; align-items: center; gap: {gap}px; align-self: stretch;"
        )

    def _render_hover_card(self, proj: dict, zone: ui.element, row: ui.element) -> None:
        """Name-column hover: a white card under the project's row (over it when there is no room
        below), never on it, with a caret on its edge pointing at the name. It holds what the row
        does not: the tomogram previews, the paths to copy, and the acquisition and import facts
        without a column. The menu is a child of the row, so the row is its anchor. It opens and
        closes in the browser (_hover_open_js) so it appears at once and can be moved onto; the
        roster's auto-refresh holds while it is open, because a rebuild would take it away from
        under the pointer. Only the empty menu is built with the row; its content is built the
        first time the name column is entered, so a render of the whole roster does not build a
        card per project."""
        with row:
            menu = (
                ui.menu()
                .props(
                    'no-parent-event no-focus no-refocus anchor="bottom left" self="top left" '
                    ':offset="[-8, 8]" transition-show="fade" transition-hide="fade" :transition-duration="80"'
                )
                .classes("cb-proj-card cb-glass")
                # Visible overflow: the caret sits outside the card's box.
                .style("overflow: visible; max-height: none;")
            )
        menu.on_value_change(self._on_preview_toggle)

        def fill() -> None:
            if menu.is_deleted or menu.default_slot.children:
                return
            with menu:
                card = ui.element("div").style("display: flex; align-items: flex-start; gap: 14px; padding: 10px;")
                card.on("mouseenter", js_handler=_HOVER_STAY)
                card.on("mouseleave", js_handler=_HOVER_CLOSE)
                with card:
                    if proj.get("tomo_preview"):
                        self._render_card_previews(proj)
                    self._render_card_facts(proj)

        zone.on("mouseenter", fill, js_handler=_hover_open_js(menu.id))
        row.on("mouseenter", js_handler=_hover_stay_js(menu.id))
        row.on("mouseleave", js_handler=_HOVER_CLOSE)

    @staticmethod
    def _render_card_previews(proj: dict) -> None:
        """The project's WarpTools tomogram previews, stepped with chevrons."""
        pngs = proj["tomo_preview"]["pngs"]
        name = proj["name"]
        # The scan's activity stamp is the cache-buster: a new reconstruction moves it.
        stamp = int(proj.get("last_activity_ts") or 0)
        shown = {"i": 0}

        def caption(i: int) -> str:
            label = _PREVIEW_SUFFIX.sub("", Path(pngs[i]).name).removeprefix(f"{name}_")
            return f"{label} · {i + 1}/{len(pngs)}" if len(pngs) > 1 else label

        def step(delta: int) -> None:
            shown["i"] = (shown["i"] + delta) % len(pngs)
            image.set_content(_preview_img_html(pngs[shown["i"]], stamp))
            caption_label.set_text(caption(shown["i"]))

        with ui.element("div").style("width: 260px; flex-shrink: 0;"):
            image = ui.html(_preview_img_html(pngs[0], stamp), sanitize=False)
            with ui.element("div").style("display: flex; align-items: center; gap: 2px; margin-top: 6px;"):
                if len(pngs) > 1:
                    icon_button("chevron_left", lambda: step(-1), tooltip="Previous")
                caption_label = ui.label(caption(0)).style(
                    f"{MONO} font-size: 10px; color: {CLR_HEADING}; flex: 1 1 0; min-width: 0; "
                    "text-align: center; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                )
                if len(pngs) > 1:
                    icon_button("chevron_right", lambda: step(1), tooltip="Next")

    @staticmethod
    def _render_card_facts(proj: dict) -> None:
        """What the row has no room for: the mnemonic and protocol, every path (click to copy),
        and the acquisition and import facts without a column. What the project did not record
        says so."""
        facts = proj.get("card") or {}
        grid = "display: grid; grid-template-columns: 64px minmax(0, 1fr); column-gap: 8px; row-gap: 2px;"
        key_style = f"{FONT} font-size: 9px; color: {CLR_LABEL};"
        head_style = (
            f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_LABEL}; letter-spacing: 0.06em; "
            "text-transform: uppercase; margin: 8px 0 3px;"
        )

        def value(text: str | None, *, path: bool = False) -> None:
            if not text:
                ui.label("not recorded").style(f"{FONT} font-size: 9px; color: {CLR_META}; font-style: italic;")
                return
            label = ui.label(text).style(f"{MONO} font-size: 9px; color: {CLR_HEADING}; word-break: break-all;")
            if path:
                copy_on_click(label, text)

        def section(title: str, rows: list[tuple[str, str | None]], *, paths: bool = False) -> None:
            ui.label(title).style(head_style)
            with ui.element("div").style(grid):
                for key, text in rows:
                    ui.label(key).style(key_style)
                    value(text, path=paths)

        def num(v, unit: str = "") -> str | None:
            return None if v is None else f"{v:g}{unit}"

        with ui.element("div").style("width: 600px; flex-shrink: 0; min-width: 0;"):
            if proj.get("mnemonic"):
                ui.label(proj["mnemonic"]).style(f"{MONO} font-size: 10px; color: {CLR_HEADING};")
            if proj.get("protocol"):
                ui.label(f"created from the {proj['protocol']} protocol").style(
                    f"{FONT} font-size: 9px; color: {CLR_LABEL};"
                )
            section(
                "Paths · click to copy",
                [
                    ("Project", proj["path"]),
                    ("Raw data", proj.get("source_directory")),
                    ("Movies", facts.get("movies_glob")),
                    ("MDOCs", facts.get("mdocs_glob")),
                ],
                paths=True,
            )
            section(
                "Acquisition",
                [
                    ("Cs", num(facts.get("cs_mm"), " mm")),
                    ("Amp. contrast", num(facts.get("amp_contrast"))),
                    ("Tilt axis", num(facts.get("tilt_axis_deg"), "°")),
                ],
            )
            # Only what was imported: a particle-only project has no frames to describe.
            imported: list[tuple[str, str | None]] = []
            if facts.get("ts_total"):
                imported.append(("Tilt-series", f"{facts.get('ts_selected') or 0} of {facts['ts_total']} selected"))
            if facts.get("frame_extension"):
                imported.append(("Format", facts["frame_extension"]))
            kind = {"stacks": "SerialEM stacks (split)", "movies": "movies"}.get(facts.get("source_kind") or "")
            if kind:
                imported.append(("Source layer", kind))
            if imported:
                section("Import", imported)

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
        _set_title(label, f"{what}\n{path}\nClick to copy")
        copy_on_click(label, path)

    @staticmethod
    def _render_live_progress(proj: dict):
        """A live run at a glance (backend._live_progress): the job at work, its tilt-series
        done out of those in scope (or, without per-TS counts, how far its run.out says it is),
        and a red dot when the run has failed some, since each is dropped from every job after
        it. Live projects only; the hover lays it out as a small key/value card."""
        lp = proj.get("live_progress")
        if not lp or proj.get("live_status") not in ("running", "queued"):
            return
        log = lp.get("log")  # services/log_progress.py, for a job without per-TS counts
        cell = f"{MONO} font-size: 9px; white-space: nowrap; flex-shrink: 0;"
        with ui.element("div").style("display: flex; align-items: baseline; gap: 5px; min-width: 0;"):
            ui.label(lp["stage"]).style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_RUNNING}; white-space: nowrap; "
                "overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 0 1 auto;"
            )
            if lp["state"] == "queued":
                ui.label("queued").style(f"{cell} color: {CLR_SUBLABEL};")
            elif lp["total"]:
                ui.label(f"{lp['ok']}/{lp['total']}").style(f"{cell} color: {CLR_LABEL};")
            elif log is not None:
                # Amber once run.out has gone quiet: a hung tool looks like a running one otherwise.
                ui.label(log.short).style(f"{cell} color: {CLR_REVIEW if log.quiet_min else CLR_LABEL};")
            if lp["failed"]:
                # A superscript dot, the count in the hover.
                ui.element("div").style(
                    f"width: 5px; height: 5px; border-radius: 50%; background: {CLR_FAILED}; "
                    "align-self: flex-start; margin: 1px 0 0 -3px; flex-shrink: 0;"
                )
            ProjectsOverview._live_progress_card(proj, lp, log)

    @staticmethod
    def _live_progress_card(proj: dict, lp: dict, log) -> None:
        """The Running cell's hover: job · where it is · overall bar · time left · warnings, as the
        key/value grid of the app's parameter tooltips (`.cb-params-tt`, ui/dashboard/css.py)."""
        rows: list[tuple[str, str, str | None]] = []  # (key, value, colour)
        fraction = None
        if lp["state"] == "queued":
            rows.append(("state", "queued on the cluster", None))
        elif lp["total"]:
            rows.append(("tilt-series", f"{lp['ok']} of {lp['total']} done · {lp['running']} running", None))
            fraction = lp["ok"] / lp["total"]
        elif log is not None:
            rows.append(("at", log.stage, None))
            fraction = log.fraction
        else:
            rows.append(("state", "running", None))
        if log is not None and log.eta:
            rows.append(("left", f"≈ {log.eta}", None))
        if log is not None and log.quiet_min is not None:
            rows.append(("log", f"no output for {log.quiet_min} min", CLR_REVIEW))
        if lp["failed_by_stage"]:
            failed = " · ".join(f"{stage} {n}" for stage, n in lp["failed_by_stage"])
            rows.append(("failed TS", failed, CLR_FAILED))
        n_tomos = (proj.get("tomo_preview") or {}).get("n_tomos")
        if n_tomos:
            rows.append(("tomograms", f"{n_tomos} reconstructed", None))
        with ui.tooltip().classes("cb-params-tt"):
            ui.label("job").classes("cb-params-k")
            ui.label(lp["stage"]).classes("cb-params-v").style(f"{FONT} font-weight: 600; color: {CLR_RUNNING};")
            for key, value, colour in rows[:1]:
                ui.label(key).classes("cb-params-k")
                ui.label(value).classes("cb-params-v").style(f"color: {colour};" if colour else "")
            if fraction is not None:
                pct = round(fraction * 100)
                ui.label("overall").classes("cb-params-k")
                with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                    with ui.element("div").style(
                        f"width: 90px; height: 4px; border-radius: 2px; background: {CLR_BORDER}; overflow: hidden;"
                    ):
                        ui.element("div").style(f"width: {pct}%; height: 100%; background: {CLR_RUNNING};")
                    ui.label(f"{pct}%").classes("cb-params-v")
            for key, value, colour in rows[1:]:
                ui.label(key).classes("cb-params-k")
                ui.label(value).classes("cb-params-v").style(f"color: {colour};" if colour else "")
            if log is not None:
                ui.label("from the job's run.out; the time left assumes the current pace").style(
                    f"grid-column: 1 / -1; margin-top: 3px; font-size: 9px; color: {CLR_LABEL};"
                )

    @staticmethod
    def _render_status(proj: dict):
        """done / failed / review / in queue / live; an idle project gets no badge. The live dot
        ripples (`.cb-live-pulse`, ui/dashboard/css.py); the in-queue dot stays still. The job
        counts live in the hover."""
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
        badge = ui.element("div").style("display: flex; align-items: center; gap: 4px; flex-shrink: 0;")
        with _set_title(badge, " · ".join(counts)):
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
            _set_title(
                ui.label("— GB").style(f"{cell} color: {CLR_GHOST};"),
                "Not measured yet: queued for a background size scan",
            )
            return
        if usage.error:
            _set_title(
                ui.label("? GB").style(f"{cell} color: {CLR_FAILED};"), f"Size measurement failed: {usage.error}"
            )
            return
        total = usage.total_bytes
        weight = 600 if total >= _SIZE_HEAVY else 400
        label = ui.label(("≥" if usage.unreadable_dirs else "") + format_bytes(total)).style(
            f"{cell} color: {size_color(total)}; font-weight: {weight};"
        )
        measured = datetime.fromtimestamp(usage.measured_at).strftime("%Y-%m-%d %H:%M")
        # The breakdown is one html element, not a label per cell: the roster has a size per row.
        rows = []
        for bucket, title in _BUCKET_LABELS:
            n = usage.buckets.get(bucket, 0)
            if not n:
                continue
            top = sorted(usage.parts.get(bucket, {}).items(), key=lambda kv: -kv[1])
            biggest = " · ".join(f"{lbl} {format_bytes(b)}" for lbl, b in top[:3] if b >= n / 100)
            rows.append(
                f"<span>{html.escape(title)}</span><span style='text-align: right;'>{format_bytes(n)}</span>"
                f"<span style='color: #475569;'>{html.escape(biggest)}</span>"
            )
        body = (
            f"<div>{format_bytes(total)} on disk · measured {measured}</div>"
            "<div style='display: grid; grid-template-columns: auto auto 1fr; column-gap: 12px; margin-top: 4px;'>"
            + "".join(rows)
            + "</div>"
        )
        if usage.unreadable_dirs:
            body += (
                f"<div style='margin-top: 4px;'>{usage.unreadable_dirs} "
                f"director{'ies' if usage.unreadable_dirs != 1 else 'y'} unreadable (permissions): "
                "the size is a lower bound</div>"
            )
        with label, ui.tooltip().style(f"{MONO} font-size: 10px; padding: 6px 8px;"):
            ui.html(body, sanitize=False)

    def _render_trash(self, proj: dict):
        """Jobs removed from the project keep their disk space in its Trash/ until it is cleared.
        Shown only when Trash/ holds something and is not being cleared: its size when the last
        measurement is newer than the trash's last change, else the number of removed jobs. A click
        lists them, sized afresh, and can clear them; in deletion mode the row is the only target."""
        n = proj.get("trash_count") or 0
        if not n or _CLEAR_TRASH_KEY + proj["path"] in self._running_tasks:
            return
        usage: DiskUsage | None = proj.get("disk_usage")
        fresh = usage is not None and not usage.error and usage.measured_at >= (proj.get("trash_mtime") or 0.0)
        size = usage.buckets.get(BUCKET_TRASH, 0) if fresh else None
        jobs = f"{n} removed job{'s' if n != 1 else ''}"
        hint = f"Trash: {jobs}, {format_bytes(size)}" if size else f"Trash: {jobs}, not measured since it changed"
        clickable = not self._delete_mode
        chip = ui.element("div").style(
            "display: flex; align-items: center; gap: 2px; flex-shrink: 0; white-space: nowrap; "
            f"color: {size_color(size or 0)}; cursor: {'pointer' if clickable else 'default'};"
        )
        _set_title(chip, f"{hint}. Click to see them and clear the trash." if clickable else hint)
        with chip:
            ui.icon("delete_outline", size="11px")
            ui.label(format_bytes(size) if size else str(n)).style(f"{MONO} font-size: 9px;")
        if clickable:
            chip.classes("cb-proj-trash")
            chip.on("click.stop", lambda: self._open_trash(Path(proj["path"]), proj["name"]))

    async def _open_trash(self, project_dir: Path, name: str):
        """The project's Trash/: every removed job with its size, measured afresh while the dialog is
        open, and one button that deletes them all. The button does not wait for the sizes: it closes
        the dialog and clears in the background (`_clear_trash`). On the page layout, so a roster
        refresh cannot take the dialog away."""
        async with self._flight("trash") as acquired:
            if not acquired:
                return
            with dialog_host(), ui.dialog() as dialog, ui.card().style("width: 600px; max-width: 95vw;"):
                ui.label(f"Trash · {name}").style(f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};")
                ui.label(
                    "Removing a job moves its directory to Trash/, where it keeps its disk space. Clearing the "
                    "trash deletes these directories permanently."
                ).style(f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;")
                body = ui.column().classes("w-full").style("gap: 0; margin-top: 8px;")
                with body:
                    self._trash_wait("Measuring Trash/ …")
                with ui.row().classes("w-full justify-end mt-3 gap-2"):
                    house_button("Cancel", dialog.close)
                    clear_btn = house_button("Clear trash", lambda: dialog.submit(True), kind="danger")
            listed: dict[str, list] = {}

            async def measure():
                listing = await self.backend.list_project_trash(project_dir)
                # Closed while it measured: cancelled, or cleared without waiting for the sizes.
                if body.is_deleted or not dialog.value:
                    return
                body.clear()
                with body:
                    if not listing["success"]:
                        ui.label(f"Could not read Trash/: {listing['error']}").style(
                            f"{FONT} font-size: 11px; color: {CLR_FAILED};"
                        )
                        clear_btn.disable()
                        return
                    entries = listing["entries"]
                    if not entries:
                        ui.label("Trash/ is empty.").style(f"{FONT} font-size: 11px; color: {CLR_SUBLABEL};")
                        clear_btn.disable()
                        return
                    total = sum(e.size_bytes for e in entries)
                    self._render_trash_entries(entries, total)
                listed["entries"] = entries
                clear_btn.set_text(f"Clear trash · {format_bytes(total)}")

            with dialog:
                ui.timer(0.01, measure, once=True)
            cleared = await dialog
        if cleared:
            self._clear_trash(project_dir, name, listed.get("entries"))

    def _clear_trash(self, project_dir: Path, name: str, entries: list | None):
        """Clear the project's Trash/ in the background: its chip leaves the row at once and the progress
        shows in the tray. `entries` is the dialog's listing when it arrived before the click; without
        it everything in Trash/ goes, and the tray counts jobs instead of bytes."""
        names = [e.name for e in entries] if entries else None
        sizes = {e.name: e.size_bytes for e in entries or []}

        async def work(progress_cb) -> str:
            result = await self.backend.clear_project_trash(project_dir, names, progress_cb)
            removed = result.get("removed") or []
            if sizes:
                done = f"{format_bytes(sum(sizes.get(n, 0) for n in removed))} freed"
            else:
                done = f"{len(removed)} job{'s' if len(removed) != 1 else ''} deleted"
            if not result["success"]:
                raise RuntimeError(f"{done}. {result['error']}")
            # The task no longer hides the chip: a re-render before the next scan must not bring it back.
            path = str(project_dir)
            self._projects = [{**p, "trash_count": 0} if p["path"] == path else p for p in self._projects]
            return done

        subtitle = None
        if entries:
            jobs = f"{len(entries)} removed job{'s' if len(entries) != 1 else ''}"
            subtitle = f"{jobs} · {format_bytes(sum(sizes.values()))}"
        self._run_in_tray(f"Clear trash · {name}", subtitle, _CLEAR_TRASH_KEY + str(project_dir), work)
        self._render_list()

    def _run_in_tray(self, title: str, subtitle: str | None, key: str, work) -> None:
        """Run `work` as a background task with its card in the tray (bottom-right), and rescan when it
        ends. `key` (a `_running_roster_tasks` key) holds the row's state while it runs, and a second
        run of the same key joins the first. The completion timer hangs on the outer container, which
        no re-render clears."""
        with self._outer_container:
            BackgroundTask(title=title, subtitle=subtitle, dedup_key=key).submit(
                work,
                # on_complete is called sync: the one-shot timer awaits the rescan.
                on_complete=lambda _task: ui.timer(0.01, lambda: self.refresh(force=True), once=True),
                show_start_toast=False,
            )

    @staticmethod
    def _trash_wait(text: str):
        with ui.row().classes("items-center").style("gap: 8px; padding: 10px 0;"):
            ui.spinner("dots", size="sm").style(f"color: {CLR_SUBLABEL};")
            ui.label(text).style(f"{FONT} font-size: 11px; color: {CLR_SUBLABEL};")

    @staticmethod
    def _render_trash_entries(entries: list, total: int):
        """One line per removed job: its type and instance, the Trash/ directory, when it last wrote, its size."""
        cell = f"{MONO} font-size: 9px; white-space: nowrap;"
        with ui.element("div").style(
            "display: grid; grid-template-columns: minmax(0, 1fr) auto auto auto; column-gap: 14px; row-gap: 4px; "
            "align-items: baseline; width: 100%; max-height: 50vh; overflow-y: auto;"
        ):
            for title in ("Removed job", "Directory", "Last written", "Size"):
                ui.label(title).style(f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_SUBLABEL};")
            for e in entries:
                with ui.element("div").style("display: flex; align-items: baseline; gap: 6px; min-width: 0;"):
                    ui.label(e.job_label or "unknown job").style(
                        f"{FONT} font-size: 11px; color: {CLR_HEADING if e.job_label else CLR_SUBLABEL}; "
                        "white-space: nowrap;"
                    )
                    if e.instance_id:
                        ui.label(e.instance_id).style(
                            f"{cell} color: {CLR_SUBLABEL}; overflow: hidden; text-overflow: ellipsis;"
                        )
                ui.label(f"Trash/{e.name}").style(f"{cell} color: {CLR_LABEL};")
                ui.label(datetime.fromtimestamp(e.modified).strftime("%Y-%m-%d")).style(
                    f"{cell} color: {CLR_SUBLABEL};"
                )
                size = ui.label(("≥" if e.unreadable_dirs else "") + format_bytes(e.size_bytes)).style(
                    f"{cell} color: {size_color(e.size_bytes)}; text-align: right;"
                )
                if e.unreadable_dirs:
                    size.tooltip(f"{e.unreadable_dirs} unreadable director{'ies' if e.unreadable_dirs != 1 else 'y'}")
        jobs = f"{len(entries)} removed job{'s' if len(entries) != 1 else ''}"
        lower_bound = "≥" if any(e.unreadable_dirs for e in entries) else ""
        ui.label(f"{jobs} · {lower_bound}{format_bytes(total)}").style(
            f"{MONO} font-size: 10px; color: {CLR_LABEL}; margin-top: 8px; align-self: flex-end;"
        )

    async def _handle_delete(self, proj: dict):
        """Confirm (the dialog anchored to the outer container, so no row re-render destroys it), then
        delete in the background: the row leaves the list at once, the progress shows in the tray, and
        a project that could not be deleted comes back to the list. One deletion disarms deletion mode;
        if it was the previewed project, the open one is previewed again."""
        if self._outer_container is None:
            return
        project_dir, name = Path(proj["path"]), proj["name"]
        async with self._flight("delete") as acquired:
            if not acquired:
                return
            with self._outer_container:
                confirmed = await self._show_delete_confirm(project_dir, name)
        if not confirmed:
            return
        usage: DiskUsage | None = proj.get("disk_usage")
        size = format_bytes(usage.total_bytes) if usage is not None and not usage.error else None

        async def work(progress_cb) -> str:
            result = await self.backend.delete_project(project_dir, progress_cb)
            if not result["success"]:
                raise RuntimeError(result["error"])
            # The task no longer hides the row: a re-render before the next scan must not bring it back.
            self._projects = [p for p in self._projects if p["path"] != proj["path"]]
            return f"{size} freed" if size else "deleted"

        was_selected = self._selected_resolved is not None and str(project_dir.resolve()) == self._selected_resolved
        self._run_in_tray(
            f"Delete project · {name}", f"{size} on disk" if size else None, _DELETE_KEY + proj["path"], work
        )
        # One deletion per arming: the next one needs the toggle again. The re-render drops the row.
        self._set_delete_mode(False)
        if was_selected and self.on_select is not None and self.current_path:
            self.set_selected(self.current_path)
            await self.on_select(Path(self.current_path))

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
