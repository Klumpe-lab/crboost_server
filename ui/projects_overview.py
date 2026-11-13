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
import time
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
from ui.components.fields import house_field, house_select
from ui.components.reactive import Debounced, SingleFlight
from ui.components.segmented import render_segmented
from services.routes import Route, route_to_path
from ui.styles import MONO, SANS as FONT, WAIT_SPINNER

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

# The search filters once typing pauses: a render shows and hides every row that comes or goes.
_SEARCH_SETTLE_S = 0.25
# A row shows this many of its tags after its name and counts the rest.
_ROW_TAGS_SHOWN = 2
_TAG_MAX_LEN = 40
_CLR_TAG_MODE = "#4a8fd4"
# A tag on a row: the house chip's capsule, small and colourless (a tag is the user's grouping, not a state).
_ROW_TAG_STYLE = (
    f"{FONT} font-size: 9px; font-weight: 500; line-height: 13px; padding: 0 6px; "
    "border-radius: var(--cb-r-pill, 999px); "
    "border: 1px solid #dbe3ec; background: #f4f8fc; color: #4f6782; white-space: nowrap; flex-shrink: 0;"
)
# A tag chip in the header: the same capsule at the toolbar's 22 px.
_TAG_CHIP_STYLE = (
    f"{FONT} display: inline-flex; align-items: baseline; gap: 5px; height: 22px; box-sizing: border-box; "
    "padding: 0 10px; border-radius: var(--cb-r-pill, 999px); font-size: 10px; line-height: 20px; white-space: nowrap; "
    "cursor: pointer; user-select: none; flex-shrink: 0;"
)
# A section title in the project hover card.
_CARD_HEAD_STYLE = (
    f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_LABEL}; letter-spacing: 0.06em; "
    "text-transform: uppercase; margin: 8px 0 3px;"
)
# The rail avatar's card rescans the project's base at most this often, when it opens.
_OTHERS_RESCAN_S = 30.0
_others_scanned_at: dict[str, float] = {}

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
# Close the open card now: a row a filter hides keeps its menu, which would stay up over the list.
_HOVER_HIDE_NOW = (
    f"(() => {{ const s = {_HOVER_STATE}; clearTimeout(s.timer); "
    "if (s.open !== null) runMethod(s.open, 'hide', []); s.open = null; })()"
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
        "display: block; border-radius: var(--cb-r-sm, 3px); background: #f1f5f9;' />"
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

# The Running cell's segmented progress bar (`_progress_bar`).
_SEG_N = 10
_SEG_W = 3
_SEG_GAP = 1
_SEG_BAR_W = _SEG_N * _SEG_W + (_SEG_N - 1) * _SEG_GAP

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
        # What the list holds: (key, fingerprint, element) per section header and row, the shown ones
        # first and in order, then the rows the filters hide. A render rebuilds only the entries whose
        # fingerprint moved.
        self._rendered: list[tuple[str, object, ui.element]] = []
        self._skeleton = None
        self._empty_note = None
        self._count_label = None
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
        self._mode_hint = None
        # The search: what was typed, and its words lowercased; every word must turn up in a project.
        self._query = ""
        self._query_words: tuple[str, ...] = ()
        self._search_soon = Debounced(self._render_list, _SEARCH_SETTLE_S)
        # Tagging mode: the tag a row click puts on its project or takes off ("" while it has no name
        # yet); None when the mode is off.
        self._tag_mode: str | None = None
        self._tag_strip = None
        self._tag_strip_shown: object = None
        self._tag_btn = None
        self._tag_tools = None
        self._tag_field = None
        self._tag_drop_btn = None
        self._syncing_tag_field = False
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
                "background: var(--cb-section-fill, #ffffff); border-radius: var(--cb-radius, 5px); "
                "border: 1px solid transparent; "
                "outline: 1px solid var(--cb-section-ring, #dbe3ec); outline-offset: -1px; "
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
                .style(f"{scroll_style} padding: 0; background: #ffffff; border-radius: var(--cb-r-md, 4px);")
            ):
                self._list_container = ui.column().classes("w-full").style("gap: 0; padding: 0;")
                # Outside the list, whose children are exactly the rendered entries.
                self._empty_note = ui.label("").style(
                    f"{FONT} font-size: 11px; color: {CLR_GHOST}; font-style: italic; padding: 24px 16px; "
                    "text-align: center; width: 100%;"
                )
                self._empty_note.set_visibility(False)
                # The last scan of this base, from any roster, shows at once; the scan below brings it
                # up to date and re-renders only the rows that moved.
                base = (self.base_path_provider() or "").strip()
                cached = self.backend.last_project_scan(base) if base else None
                if cached is None:
                    self._skeleton = self._render_loading_skeleton()
            if cached is not None:
                self._projects = cached
                self._last_scanned_base = base
                self._render_list()

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
        """Two control rows over the list, each wrapping on a narrow pane instead of pushing controls off
        its edge. The first holds the title, how many projects show and what they weigh, a mode's hint,
        then the order, the view and the actions; the second what shows: the search, the tag chips and
        the tag tools, the species and the owner. The controls are the toolbar's 22 px (`.cb-proj-toolbar`,
        ui/dashboard/css.py), the height of the title switch and the house buttons."""
        with ui.column().classes("w-full cb-proj-toolbar").style("gap: 10px; padding: 12px 14px 10px;"):
            with ui.row().classes("w-full items-center").style("gap: 8px 16px; flex-wrap: wrap;"):
                if self.title_slot is not None:
                    self.title_slot()
                else:
                    ui.label(self.title).style(
                        f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING}; "
                        "letter-spacing: -0.01em; flex-shrink: 0;"
                    )
                with ui.row().classes("items-baseline no-wrap").style("gap: 10px; flex-shrink: 0;"):
                    self._count_label = ui.label("").style(f"{MONO} font-size: 10px; color: {CLR_META};")
                    self._size_label = ui.label("").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL};")
                    # Shown while a first scan streams its rows in.
                    self._scan_note = ui.label("").style(f"{MONO} font-size: 10px; color: {CLR_SUBLABEL};")
                    self._scan_note.set_visibility(False)
                self._mode_hint = ui.label("").style(
                    f"{FONT} font-size: 10px; font-weight: 600; letter-spacing: 0.02em; "
                    "flex-shrink: 0; white-space: nowrap;"
                )
                ui.element("div").style("flex: 1 1 0; min-width: 0;")
                # Every choice here is the same two-way strip, so the row reads as one set of controls
                # and each state is named rather than implied by a knob. In the full view sorting
                # happens inside each owner section, so the Lab/Shared grouping is unaffected by it.
                with ui.row().classes("items-center no-wrap").style("gap: 12px; flex-shrink: 0;"):
                    self._sort_seg = self._segmented(
                        SORT_MODES,
                        "" if self._column_sort else self._sort_mode,
                        self._on_sort,
                        hint="Order: last opened by you, or created",
                    )
                    self._view_seg = self._segmented(
                        (("compact", "Compact"), ("full", "Full")),
                        "compact" if self.prefs.prefs.projects_compact_view else "full",
                        self._on_view,
                        hint="Compact: one line per project. Full: paths and last activity",
                    )
                with ui.row().classes("items-center no-wrap").style("gap: 6px; flex-shrink: 0;"):
                    if self.on_browse is not None:
                        icon_button("folder_open", self.on_browse, tooltip="Browse for another base location")
                    self._delete_btn = icon_button(
                        "delete_outline",
                        self._toggle_delete_mode,
                        tooltip="Deletion mode: arm it, then click the project to delete. Click again to disarm.",
                        kind="danger",
                    )
                    # An explicit rescan on top of the timer's.
                    icon_button(
                        "refresh", self.refresh, tooltip=f"Rescan now (auto every {int(self.auto_refresh_sec)}s)"
                    )

            with ui.row().classes("w-full items-center").style("gap: 8px 16px; flex-wrap: wrap;"):
                self._build_search()
                with ui.row().classes("items-center").style("gap: 6px; flex-wrap: wrap; min-width: 0;"):
                    self._tag_strip = ui.row().classes("items-center").style("gap: 6px; flex-wrap: wrap;")
                    self._build_tag_tools()
                ui.element("div").style("flex: 1 1 0; min-width: 0;")
                with ui.row().classes("items-center no-wrap").style("gap: 12px; flex-shrink: 0;"):
                    # Species index across the scanned projects. Options are refreshed on every scan
                    # (_render_list); a species that no project registers is simply not offered rather
                    # than silently matching nothing.
                    self._species_select = house_select(
                        "",
                        {ANY_SPECIES: ANY_SPECIES_LABEL},
                        value=ANY_SPECIES,
                        width="w-36",
                        hint="Show only projects whose registry holds this particle species",
                        on_change=self._on_species_filter,
                    )
                    if self.show_filter:
                        self._owner_seg = self._segmented(
                            (("all", "All"), ("mine", "Mine")),
                            "mine" if self.prefs.prefs.show_only_mine else "all",
                            self._on_owner_filter,
                            hint=f"Mine: projects created by {CURRENT_USER}, plus Lab / Shared",
                        )
        self._style_modes()

    def _build_search(self) -> None:
        """Every word typed must turn up somewhere in a project (`_search_text`); the list follows once the
        typing pauses. Esc clears it."""

        def build():
            field = ui.input(placeholder="Search name, species, owner, tag, job, path…").props("clearable")
            with field.add_slot("prepend"):
                ui.icon("search")
            field.on_value_change(self._on_search)
            field.on("keydown.esc", lambda: field.set_value(""))
            return field

        house_field("", build, width="w-72")

    def _build_tag_tools(self) -> None:
        """The tag button, and beside it while tagging mode is armed: the tag's name, the mode's hint,
        Remove from all (when projects carry it) and Done."""
        self._tag_btn = icon_button(
            "new_label",
            self._toggle_tag_mode,
            tooltip="Tag projects: name a tag (or click its chip), then click projects to put it on or take it "
            "off. Tags are yours alone: they group your roster and change nothing in the projects.",
        )
        self._tag_tools = ui.row().classes("items-center no-wrap").style("gap: 8px; flex-shrink: 0;")
        with self._tag_tools:

            def build():
                field = ui.input(placeholder="tag name").props(f"maxlength={_TAG_MAX_LEN}")
                field.on_value_change(self._on_tag_name)
                field.on("keydown.esc", lambda: self._set_tag_mode(None))
                field.on("keydown.enter", lambda: field.run_method("blur"))
                return field

            self._tag_field = house_field("Tag", build, width="w-40")
            self._tag_drop_btn = house_button(
                "Remove from all", self._confirm_drop_tag, kind="danger", tooltip="Take this tag off every project"
            )
            house_button("Done", lambda: self._set_tag_mode(None), kind="accent")
        self._tag_tools.set_visibility(False)

    def _build_column_header(self):
        """Compact view: the column titles, above the scroll area so they stay put. Same widths,
        gap and padding as `_render_compact_body`, plus the chevron's column. A title click
        sorts by its column, a second click reverses it (`_on_column_sort`)."""
        head = ui.element("div").style(
            f"display: flex; align-items: center; gap: 12px; padding: 6px {8 + self._W_CHEVRON}px 6px 14px; "
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
    def _segmented(tabs, active: str, on_switch: Callable[[str], None], *, hint: str):
        """A segmented strip at the toolbar's scale, with a hover hint."""
        with ui.element("div").style("display: flex; flex-shrink: 0;").tooltip(hint):
            return render_segmented(list(tabs), active, on_switch)

    def _toggle_delete_mode(self):
        self._set_delete_mode(not self._delete_mode)

    def _set_delete_mode(self, on: bool):
        """Arm or disarm deletion mode. Armed, the rows re-render as delete targets: a click
        opens the confirm dialog, nothing previews, and the travel chevron is a trash glyph,
        so no click on a row leaves the page. Arming it ends tagging mode."""
        self._delete_mode = on
        if on and self._tag_mode is not None:
            self._tag_mode = None
            if self._tag_tools is not None:
                self._tag_tools.set_visibility(False)
        self._style_modes()
        self._render_list()

    def _style_modes(self):
        """The header's mode toggles, the mode's hint and the roster's outline. Armed deletion breathes
        red (`.cb-proj-delmode`, ui/dashboard/css.py); armed tagging is outlined blue."""
        deleting, tagging = self._delete_mode, self._tag_mode is not None
        for btn, on in ((self._delete_btn, deleting), (self._tag_btn, tagging)):
            if btn is None:
                continue
            if on:
                btn.classes(add="cb-icon-btn--armed")
            else:
                btn.classes(remove="cb-icon-btn--armed")
        if self._delete_btn is not None:
            self._delete_btn.props(f"icon={'delete' if deleting else 'delete_outline'}")
        if self._mode_hint is not None:
            if deleting:
                self._mode_hint.set_text("Deletion mode · click a project to delete it")
                self._mode_hint.style(f"color: {CLR_FAILED};")
            elif tagging:
                self._mode_hint.set_text("Tagging · click a project to put the tag on or take it off")
                self._mode_hint.style(f"color: {_CLR_TAG_MODE};")
            self._mode_hint.set_visibility(deleting or tagging)
        if self._outer_container is not None:
            edge = CLR_FAILED if deleting else _CLR_TAG_MODE if tagging else "transparent"
            self._outer_container.style(f"border: 1px solid {edge};")
            if deleting:
                self._outer_container.classes(add="cb-proj-delmode")
            else:
                self._outer_container.classes(remove="cb-proj-delmode")

    # ---- search and tags ----

    def _on_search(self, e) -> None:
        self._query = (e.value or "").strip()
        words = tuple(self._query.lower().split())
        if words == self._query_words:
            return
        self._query_words = words
        self._search_soon()

    def _search_text(self, proj: dict) -> str:
        """Everything the search looks in, lowercased, a line per field so no word matches across two:
        the name and nickname, owner and creator, species, tags, protocol, the status, the furthest job
        and the one at work, the raw-data folder and the project's own path."""
        status = proj.get("live_status") or ""
        parts = [
            proj["name"],
            proj.get("mnemonic") or "",
            self._owner_label(self._eff_owner_of(proj)),
            proj.get("owner") or "",
            proj.get("creator") or "",
            *(s.get("name") or s["id"] for s in proj.get("species") or []),
            *self._tags(proj),
            proj.get("protocol") or "",
            status,
            (_STATUS_STYLES.get(status) or {}).get("label", ""),
            (proj.get("furthest_done") or {}).get("label") or "",
            (proj.get("live_progress") or {}).get("stage") or "",
            proj.get("source_directory") or "",
            proj["path"],
        ]
        return "\n".join(parts).lower()

    def _tags(self, proj: dict) -> list[str]:
        return self.prefs.prefs.tags_of(proj["resolved_path"])

    @staticmethod
    def _carries(tags, tag: str) -> bool:
        return any(t.lower() == tag.lower() for t in tags)

    def _tag_counts(self, projects: list[dict]) -> dict[str, int]:
        """The tags on `projects` -> how many of them carry each, by name."""
        counts: dict[str, int] = {}
        for proj in projects:
            for tag in self._tags(proj):
                counts[tag] = counts.get(tag, 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: kv[0].lower()))

    def _tag_filter(self, counts: dict[str, int]) -> str | None:
        """The tag the list is narrowed to, in its spelling in `counts`; None when there is none, or when
        no project the other filters show carries it any more."""
        tag = self.prefs.prefs.projects_tag_filter
        if not tag:
            return None
        return next((t for t in counts if t.lower() == tag.lower()), None)

    def _paint_tag_strip(self, counts: dict[str, int], active: str | None) -> None:
        """One chip per tag with its count, rebuilt only when the tags, the filter or the mode moved. A
        click narrows the list to the tag's projects, a second shows them all again; in tagging mode it
        arms that tag instead."""
        if self._tag_strip is None or self._tag_strip.is_deleted:
            return
        armed = self._tag_mode
        shown = (tuple(counts.items()), active, armed)
        if shown == self._tag_strip_shown:
            return
        self._tag_strip_shown = shown
        self._tag_strip.clear()
        with self._tag_strip:
            for tag, n in counts.items():
                lit = (armed is not None and armed.lower() == tag.lower()) or (armed is None and tag == active)
                chip = ui.element("div").classes("cb-proj-tagchip" + (" active" if lit else "")).style(_TAG_CHIP_STYLE)
                with chip:
                    ui.label(tag)
                    ui.label(str(n)).classes("cb-proj-tagchip-n")
                if armed is not None:
                    hint = f"Tag projects with {tag}"
                elif lit:
                    hint = "Show every project again"
                else:
                    hint = f"Show only the {n} project{'s' if n != 1 else ''} tagged {tag}"
                _set_title(chip, hint)
                chip.on("click", lambda _e, t=tag: self._on_tag_chip(t))

    def _on_tag_chip(self, tag: str) -> None:
        if self._tag_mode is not None:
            self._set_tag_mode(tag)
            return
        current = self.prefs.prefs.projects_tag_filter
        self._set_tag_filter(None if current is not None and current.lower() == tag.lower() else tag)

    def _set_tag_filter(self, tag: str | None) -> None:
        self.prefs.prefs.projects_tag_filter = tag
        self.prefs.save_to_app_storage(app.storage.user)
        self._render_list()

    def _toggle_tag_mode(self) -> None:
        if self._tag_mode is not None:
            self._set_tag_mode(None)
            return
        # Armed for the tag the list is narrowed to, if any: the usual next step is adding to it.
        self._set_tag_mode(self.prefs.prefs.projects_tag_filter or "")

    def _set_tag_mode(self, tag: str | None) -> None:
        """Arm tagging mode for `tag` ("" = armed, its name still to type) or end it (None). Armed, a row
        click puts the tag on its project or takes it off, and the tag filter rests, so every project can
        be reached. Arming it ends deletion mode."""
        if tag is not None:
            self._delete_mode = False
            tag = self.prefs.prefs.canonical_tag(tag)
        self._tag_mode = tag
        if self._tag_field is not None:
            self._syncing_tag_field = True
            try:
                self._tag_field.set_value(tag or "")
            finally:
                self._syncing_tag_field = False
        if self._tag_tools is not None:
            self._tag_tools.set_visibility(tag is not None)
        self._style_modes()
        self._render_list()
        if tag == "" and self._tag_field is not None:
            self._tag_field.run_method("focus")

    def _on_tag_name(self, e) -> None:
        if self._syncing_tag_field or self._tag_mode is None:
            return
        self._tag_mode = self.prefs.prefs.canonical_tag(e.value or "")
        self._search_soon()

    def _toggle_row_tag(self, proj: dict) -> None:
        tag = self._tag_mode or ""
        if not tag:
            ui.notify("Name the tag first", type="warning", timeout=1500)
            return
        self.prefs.prefs.toggle_tag(proj["resolved_path"], tag)
        self.prefs.save_to_app_storage(app.storage.user)
        self._render_list()

    async def _confirm_drop_tag(self) -> None:
        """Take the armed tag off every project, after a confirm; tagging mode ends with it."""
        tag = self._tag_mode or ""
        n = self.prefs.prefs.tag_counts().get(tag, 0)
        if not n or self._outer_container is None:
            return
        async with self._flight("drop-tag") as acquired:
            if not acquired:
                return
            with self._outer_container, ui.dialog() as dialog, ui.card().classes("w-96"):
                ui.label(f"Remove the tag '{tag}'?").style(
                    f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};"
                )
                ui.label(
                    f"It comes off the {n} project{'s' if n != 1 else ''} that carry it. The projects stay as they are."
                ).style(f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;")
                with ui.row().classes("w-full justify-end mt-3 gap-2"):
                    house_button("Cancel", lambda: dialog.submit(False))
                    house_button("Remove tag", lambda: dialog.submit(True), kind="danger")
            confirmed = await dialog
        if not confirmed:
            return
        self.prefs.prefs.drop_tag(tag)
        self.prefs.save_to_app_storage(app.storage.user)
        self._set_tag_mode(None)

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

    def _render_loading_skeleton(self) -> ui.element:
        with ui.row().classes("w-full items-center justify-center").style("padding: 24px 0;") as skeleton:
            ui.spinner("dots", size="sm").style(f"color: {CLR_SUBLABEL};")
            ui.label("Scanning…").style(f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; margin-left: 8px;")
        return skeleton

    def _render_list(self):
        # Deleted when the tab closed while refresh() awaited the scan.
        if self._list_container is None or self._list_container.is_deleted:
            return

        if self._skeleton is not None:
            self._skeleton.delete()
            self._skeleton = None

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

        # The chips count what the filters above show; the tag filter rests in tagging mode, where every
        # project must be reachable.
        counts = self._tag_counts(visible)
        tag = self._tag_filter(counts)
        self._paint_tag_strip(counts, tag)
        if self._tag_drop_btn is not None:
            self._tag_drop_btn.set_visibility(bool(self._tag_mode) and self._tag_mode in self.prefs.prefs.tag_counts())
        if tag is not None and self._tag_mode is None:
            visible = [p for p in visible if self._carries(self._tags(p), tag)]
        if self._query_words:
            visible = [p for p in visible if all(w in self._search_text(p) for w in self._query_words)]

        self._paint_counts(visible, all_projects)

        compact = self.prefs.prefs.projects_compact_view
        items = self._list_items(visible, compact)
        if self._column_header is not None:
            self._column_header.set_visibility(compact and bool(items))
        self._patch_list(items)
        self._paint_empty_note(visible, all_projects, tag)

    def _paint_counts(self, visible: list[dict], all_projects: list[dict]) -> None:
        """The header's numbers: how many projects show of those here, and what the shown ones weigh."""
        if self._count_label is not None:
            n, total = len(visible), len(all_projects)
            s = "s" if total != 1 else ""
            self._count_label.set_text(f"{n} of {total}" if n != total else f"{total} project{s}")
            # A native title, not .tooltip(): that would add a new tooltip element on every refresh.
            _set_title(self._count_label, f"{n} shown of the {total} project{s} in {self._last_scanned_base or '—'}")
        if self._size_label is not None:
            measured = [p["disk_usage"] for p in visible if p.get("disk_usage") and not p["disk_usage"].error]
            unmeasured_n = len(visible) - len(measured)
            total = format_bytes(sum(u.total_bytes for u in measured)) if measured else ""
            self._size_label.set_text(f"{'≥ ' if measured and unmeasured_n else ''}{total}")
            if measured and unmeasured_n:
                s = "s" if unmeasured_n != 1 else ""
                note = f"{unmeasured_n} project{s} not measured yet: the size is a lower bound"
                self._size_label.props(f'title="{note}"')
            else:
                self._size_label.props(remove="title")

    def _paint_empty_note(self, visible: list[dict], all_projects: list[dict], tag: str | None) -> None:
        """What an empty list says: the filter that left nothing, or that there is nothing here."""
        note = self._empty_note
        if note is None or note.is_deleted:
            return
        note.set_visibility(not visible)
        if visible:
            return
        msg = "No projects to show"
        if self._scanning:
            msg = "Scanning…"
        elif not (self._last_scanned_base or ""):
            msg = "Set a base location to scan for projects"
        elif not all_projects:
            msg = "No projects to show"
        elif self._query_words:
            msg = f"No project matches “{self._query}”"
        elif tag is not None and self._tag_mode is None:
            msg = f"No projects tagged {tag}"
        elif self._species_filter != ANY_SPECIES:
            label = (self._species_select.options if self._species_select else {}).get(
                self._species_filter, self._species_filter
            )
            msg = f"No projects with a '{label}' species"
        elif self.prefs.prefs.show_only_mine:
            msg = f"No projects owned by {CURRENT_USER}"
        note.set_text(msg)

    def _list_items(self, visible: list[dict], compact: bool) -> list[tuple[str, object, Callable[[], ui.element]]]:
        """What the list shows, in order: (key, fingerprint, builder) per section header and row.
        The fingerprint holds everything the entry's render reads."""
        items: list[tuple[str, object, Callable[[], ui.element]]] = []

        tagging = self._tag_mode is not None
        armed = self._tag_mode or ""

        def add_row(proj: dict) -> None:
            path = proj["path"]
            tags = tuple(self._tags(proj))
            fingerprint = (
                proj,
                compact,
                self._delete_mode,
                proj["resolved_path"] == self._current_resolved,
                proj["resolved_path"] == self._selected_resolved,
                _CLEAR_TRASH_KEY + path in self._running_tasks,
                tags,
                tagging,
                bool(armed) and self._carries(tags, armed),
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
        """Bring the list to `items` by key: build the new entries and those whose fingerprint moved,
        show and move the rest into place. A row the filters leave out is hidden rather than deleted
        while its project is in the scan, so a search or a tag chip brings it back without building it
        again; section headers and the rows of projects gone are deleted. A timer tick usually changes
        one live project, a streaming scan only adds rows, and building a long roster's rows blocks the
        event loop for a second."""
        old = {key: (fingerprint, element) for key, fingerprint, element in self._rendered}
        rendered: list[tuple[str, object, ui.element]] = []
        for i, (key, fingerprint, build) in enumerate(items):
            prev = old.pop(key, None)
            if prev is not None and prev[0] == fingerprint:
                element = prev[1]
                if not element.visible:
                    element.set_visibility(True)
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
        scanned = {f"row:{p['path']}" for p in self._projects}
        hid = False
        for key, (fingerprint, element) in old.items():
            if key in scanned:
                if element.visible:
                    element.set_visibility(False)
                    hid = True
                rendered.append((key, fingerprint, element))
            else:
                element.delete()
                self._preview_open = False
        if hid and self._preview_open:
            self._list_container.client.run_javascript(_HOVER_HIDE_NOW)
            self._preview_open = False
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
                    "background: #dbeafe; border-radius: var(--cb-r-sm, 3px); padding: 0 5px;"
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
    _W_LIVE = 250  # bar + count + time left + "Reconstruct Particle", the longest job name

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
        tagging = self._tag_mode is not None and not deleting
        cursor = " cursor: pointer;" if preview_mode or deleting or tagging else " cursor: default;"
        if is_current:
            row_classes = "w-full group" + (" cb-proj-tagrow" if tagging else "")
            row_style = base_style + " background: #eff6ff; border-left: 3px solid #3b82f6;" + cursor + outline
            if deleting:
                row_style += " opacity: 0.5; cursor: not-allowed;"
        elif deleting:
            row_classes = "w-full group cb-proj-delrow"
            row_style = base_style + cursor + outline
        elif tagging:
            row_classes = "w-full group cb-proj-tagrow"
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
        elif tagging:
            # Tagging mode: the row is the trigger, as in deletion mode.
            row.on("click", lambda: self._toggle_row_tag(proj))
        elif preview_mode:
            row.on("click", _select)

        cell = self._text_cell
        with row:
            if compact:
                self._render_compact_body(proj, row)
                self._render_chevron(proj, is_current)
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
                                "border-radius: var(--cb-r-sm, 3px); padding: 0 4px; flex-shrink: 0; "
                                "letter-spacing: 0.05em;"
                            )
                        self._render_row_tags(proj)
                        self._render_hover_card(proj, zone, row)
                    # Fixed slots, filled or not, so the running job and the status line up across rows.
                    with ui.element("div").style(f"width: {self._W_LIVE}px; flex-shrink: 0; display: flex;"):
                        self._render_live_progress(proj)
                    with ui.element("div").style(f"width: {self._W_STATUS}px; flex-shrink: 0; display: flex;"):
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
            self._render_chevron(proj, is_current)
        return row

    def _render_row_tags(self, proj: dict) -> None:
        """The row's tags after its name: the first `_ROW_TAGS_SHOWN`, then how many more. Outside the
        modes a click on one shows only its projects; in them a click anywhere on the row is the mode's."""
        tags = self._tags(proj)
        if not tags:
            return
        clickable = self._tag_mode is None and not self._delete_mode
        with ui.element("div").style("display: flex; align-items: center; gap: 4px; flex-shrink: 0;"):
            for tag in tags[:_ROW_TAGS_SHOWN]:
                chip = ui.label(tag).classes("cb-proj-rowtag").style(_ROW_TAG_STYLE)
                if clickable:
                    chip.classes("cb-proj-rowtag--link").on("click.stop", lambda _e, t=tag: self._set_tag_filter(t))
                    _set_title(chip, f"Show only the projects tagged {tag}")
            if len(tags) > _ROW_TAGS_SHOWN:
                rest = tags[_ROW_TAGS_SHOWN:]
                more = ui.label(f"+{len(rest)}").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")
                _set_title(more, "Also tagged " + ", ".join(rest))

    def _render_chevron(self, proj: dict, is_current: bool):
        path_str = proj["path"]
        if self._tag_mode is not None and not self._delete_mode:
            # Tagging: the travel column says whether the project carries the armed tag, and lights with
            # the row (`.cb-proj-tagcheck`), so the one control that leaves the page is gone.
            on = bool(self._tag_mode) and self._carries(self._tags(proj), self._tag_mode)
            with (
                ui.element("div")
                .classes("cb-proj-tagcheck" + (" on" if on else ""))
                .style(
                    f"width: {self._W_CHEVRON}px; flex-shrink: 0; display: flex; align-items: center; "
                    "justify-content: center; align-self: stretch;"
                )
            ):
                ui.icon("check_box" if on else "check_box_outline_blank", size="16px")
            return
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
                self._render_row_tags(proj)
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
                .classes("cb-proj-card cb-popover")
                # Visible overflow: the caret sits outside the card's box.
                .style("overflow: visible; max-height: none;")
            )
        menu.on_value_change(self._on_preview_toggle)

        def fill() -> None:
            if menu.is_deleted or menu.default_slot.children:
                return
            with menu, _card_box():
                _render_card_content(proj)

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
    def _render_card_facts(proj: dict, after: Callable[[], None] | None = None) -> None:
        """What the row has no room for: the mnemonic and protocol, every path (click to copy),
        and the acquisition and import facts without a column. What the project did not record
        says so. `after` adds a section at the column's end."""
        facts = proj.get("card") or {}
        grid = "display: grid; grid-template-columns: 64px minmax(0, 1fr); column-gap: 8px; row-gap: 2px;"
        key_style = f"{FONT} font-size: 9px; color: {CLR_LABEL};"
        head_style = _CARD_HEAD_STYLE

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
            if after is not None:
                after()

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
        and its time left. Live projects only; the hover lays it out as a small key/value card,
        failed tilt-series included."""
        lp = proj.get("live_progress")
        if not lp or proj.get("live_status") not in ("running", "queued"):
            return
        log = lp.get("log")  # services/log_progress.py, for a job without per-TS counts
        # Fixed tracks (bar | count | time left | job) so each lines up from row to row. The count and time
        # tracks are in `ch` of the cell's mono font: wide enough for "1200/1200" and "≈14h20m", never clipped.
        with ui.element("div").style(
            f"{MONO} font-size: 9px; display: grid; "
            f"grid-template-columns: {_SEG_BAR_W}px 9ch 7ch minmax(0, 1fr); column-gap: 6px; "
            "align-items: center; width: 100%; min-width: 0;"
        ):
            text, fraction, colour = "", None, CLR_LABEL
            if lp["state"] == "queued":
                text, colour = "queued", CLR_SUBLABEL
            elif lp["total"]:
                text, fraction = f"{lp['ok']}/{lp['total']}", lp["ok"] / lp["total"]
            elif log is not None:
                # Amber once run.out has gone quiet: a hung tool looks like a running one otherwise.
                text, fraction = log.short, log.shown_fraction
                colour = CLR_REVIEW if log.quiet_min else CLR_LABEL
            if fraction is not None:
                ProjectsOverview._progress_bar(fraction, CLR_REVIEW if colour == CLR_REVIEW else CLR_RUNNING)
            # Without a bar the text starts where the bar would.
            span = "grid-column: 2;" if fraction is not None else "grid-column: 1 / 3;"
            ui.label(text).style(f"{span} color: {colour}; white-space: nowrap;")
            eta = lp.get("eta")
            # "14 h 20 min" → "≈14h20m"; the hover spells it out.
            short_eta = "≈" + eta.replace(" min", "m").replace(" h", "h").replace(" ", "") if eta else ""
            ui.label(short_eta).style(f"grid-column: 3; color: {CLR_SUBLABEL}; white-space: nowrap;")
            ui.label(lp["stage"]).style(
                f"{FONT} font-size: 9px; font-weight: 600; color: {CLR_RUNNING}; white-space: nowrap; "
                "overflow: hidden; text-overflow: ellipsis; min-width: 0; grid-column: 4;"
            )
            ProjectsOverview._live_progress_card(proj, lp, log)

    @staticmethod
    def _progress_bar(fraction: float, colour: str) -> None:
        """A segmented bar, terminal style: square cells lit in whole cells only."""
        lit = max(0, min(_SEG_N, int(fraction * _SEG_N)))
        with ui.element("div").style(f"display: flex; gap: {_SEG_GAP}px; height: 7px;"):
            for i in range(_SEG_N):
                ui.element("div").style(f"width: {_SEG_W}px; background: {colour if i < lit else CLR_BORDER};")

    @staticmethod
    def _live_progress_card(proj: dict, lp: dict, log) -> None:
        """The Running cell's hover: job · where it is · overall bar · time left · warnings, as the
        key/value grid of the app's parameter tooltips (`.cb-params-tt`, ui/dashboard/css.py)."""
        rows: list[tuple[str, str, str | None]] = []  # (key, value, colour)
        fraction, bar_key = None, "overall"
        if lp["state"] == "queued":
            rows.append(("state", "queued on the cluster", None))
        elif lp["total"]:
            rows.append(("tilt-series", f"{lp['ok']} of {lp['total']} done · {lp['running']} running", None))
            fraction = lp["ok"] / lp["total"]
        elif log is not None:
            rows.append(("at", log.stage, None))
            fraction = log.shown_fraction
            if log.fraction is None:
                bar_key = "this step"  # the log gives the current step's progress only
        else:
            rows.append(("state", "running", None))
        if lp.get("eta"):
            rows.append(("left", f"≈ {lp['eta']}", None))
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
                ui.label(bar_key).classes("cb-params-k")
                with ui.element("div").style("display: flex; align-items: center; gap: 6px;"):
                    ProjectsOverview._progress_bar(fraction, CLR_RUNNING)
                    ui.label(f"{int(fraction * 100)}%").classes("cb-params-v")
            for key, value, colour in rows[1:]:
                ui.label(key).classes("cb-params-k")
                ui.label(value).classes("cb-params-v").style(f"color: {colour};" if colour else "")
            note = None
            if log is not None:
                note = "from the job's run.out; the time left assumes the current pace"
            elif lp.get("eta"):
                note = "the time left assumes the pace the tilt-series have finished at so far"
            if note:
                ui.label(note).style(f"grid-column: 1 / -1; margin-top: 3px; font-size: 9px; color: {CLR_LABEL};")

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
                "padding: 5px 7px; background: #f8fafc; border-radius: var(--cb-r-md, 4px); "
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


def _card_box() -> ui.element:
    """The hover card's box in its menu: moving onto it keeps the card open, leaving it closes it."""
    card = ui.element("div").style("display: flex; align-items: flex-start; gap: 14px; padding: 10px;")
    card.on("mouseenter", js_handler=_HOVER_STAY)
    card.on("mouseleave", js_handler=_HOVER_CLOSE)
    return card


def _render_card_content(proj: dict, after: Callable[[], None] | None = None) -> None:
    if proj.get("tomo_preview"):
        ProjectsOverview._render_card_previews(proj)
    ProjectsOverview._render_card_facts(proj, after)


def project_card_menu(
    anchor: ui.element,
    load: Callable[[], Awaitable[dict | None]],
    *,
    backend=None,
    project_path: Path | str | None = None,
) -> None:
    """The roster's project hover card on another anchor (the workspace rail's project avatar),
    opening to its right with the caret on its left edge at the anchor's middle. `load` reads the
    project's roster row; it runs the first time the anchor is entered, and the card spins until
    it lands.

    With `backend` and `project_path` the card ends in the other projects of the project's base, a
    line each (`_render_other_projects`), so going to another one needs no trip to the landing page.
    The list is drawn from the base's last scan each time the card opens, and the base is rescanned
    then, in the background, when the card last rescanned it more than `_OTHERS_RESCAN_S` ago."""
    with anchor:
        menu = (
            ui.menu()
            .props(
                'no-parent-event no-focus no-refocus anchor="top right" self="top left" '
                ':offset="[10, 0]" transition-show="fade" transition-hide="fade" :transition-duration="80"'
            )
            .classes("cb-proj-card cb-proj-card--right cb-popover")
            .style("overflow: visible; max-height: none;")
        )
    base = str(Path(project_path).parent) if backend is not None and project_path is not None else None
    # The list's box once the card is built, and what it was drawn from: the scan list (a new one per
    # scan), the MRU and the owner filter.
    others: dict = {"box": None, "drawn": None}

    def paint_others() -> None:
        box = others["box"]
        if box is None or box.is_deleted:
            return
        rows = backend.last_project_scan(base)
        prefs = get_prefs_service().prefs
        drawn = (id(rows), tuple(r.path for r in prefs.recent_projects), prefs.show_only_mine)
        if drawn == others["drawn"]:
            return
        others["drawn"] = drawn
        box.clear()
        with box:
            _render_other_projects(rows, str(project_path))

    async def rescan() -> None:
        last = _others_scanned_at.get(base)
        if last is not None and time.monotonic() - last < _OTHERS_RESCAN_S:
            return
        _others_scanned_at[base] = time.monotonic()
        await backend.scan_for_projects(base)
        if not menu.is_deleted and menu.value:
            paint_others()

    def others_section() -> None:
        ui.label("Other projects · click one to go there").style(_CARD_HEAD_STYLE)
        others["box"] = ui.element("div").style("max-height: 280px; overflow-y: auto; margin: 0 -6px;")

    async def fill() -> None:
        if menu.is_deleted or menu.default_slot.children:
            return
        with menu:
            card = _card_box()
        with card:
            ui.html(WAIT_SPINNER, sanitize=False)
        proj = await load()
        if card.is_deleted:
            return
        card.clear()
        with card:
            if proj is None:
                ui.label("This project's parameters could not be read.").style(
                    f"{FONT} font-size: 10px; color: {CLR_META};"
                )
            else:
                _render_card_content(proj, others_section if base is not None else None)
        paint_others()
        menu.run_method("updatePosition")

    async def on_open(e) -> None:
        if not e.value or base is None:
            return
        paint_others()
        await rescan()

    menu.on_value_change(on_open)
    anchor.on("mouseenter", fill, js_handler=_hover_open_js(menu.id))
    anchor.on("mouseleave", js_handler=_HOVER_CLOSE)


def _render_other_projects(rows: list[dict] | None, here: str) -> None:
    """The base's projects but the one at `here`, a line each (`_other_project_line`): the most recently
    opened first, then the most recently active. The roster's Mine holds here too: the user's own and
    the lab's projects only."""
    if rows is None:
        with ui.row().classes("items-center no-wrap").style("gap: 6px; padding: 2px 6px;"):
            ui.spinner("dots", size="xs").style(f"color: {CLR_SUBLABEL};")
            ui.label("Reading the other projects…").style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};")
        return
    prefs = get_prefs_service().prefs
    eff = ProjectsOverview._eff_owner_of
    shown = [p for p in rows if here not in (p["path"], p["resolved_path"])]
    if prefs.show_only_mine:
        shown = [p for p in shown if eff(p) in (CURRENT_USER, SHARED_OWNER)]
    rank = prefs.recent_project_rank()
    shown.sort(key=lambda p: (rank.get(p["resolved_path"], len(rank)), -(p.get("last_activity_ts") or 0.0)))
    if not shown:
        ui.label("No other projects here").style(
            f"{FONT} font-size: 9px; color: {CLR_META}; font-style: italic; padding: 2px 6px;"
        )
        return
    for proj in shown:
        _other_project_line(proj)


def _other_project_line(proj: dict) -> None:
    """One project in the hover card's list: its status dot, its name (and owner, when not the user),
    the job at work or the furthest one done, and its last activity. The line is a link to the project,
    so ⌘-click opens it in a new tab."""
    status = proj.get("live_status") or "idle"
    badge = _STATUS_STYLES.get(status)
    lp = proj.get("live_progress")
    if lp and status in ("running", "queued"):
        stage = "queued" if lp["state"] == "queued" else lp["stage"]
        if lp["state"] != "queued" and lp["total"]:
            stage += f" · {lp['ok']}/{lp['total']}"
        stage_color = CLR_RUNNING
    elif proj.get("furthest_done"):
        stage, stage_color = f"→ {proj['furthest_done']['label']}", CLR_LABEL
    else:
        stage, stage_color = "—", CLR_GHOST
    when, _ = ProjectsOverview._activity_text(proj)
    owner = ProjectsOverview._eff_owner_of(proj)
    link = (
        ui.link(target=ProjectsOverview._project_url(proj["path"]))
        .classes("cb-proj-other")
        .style(
            "display: grid; grid-template-columns: 6px minmax(0, 1fr) 170px 74px; column-gap: 8px; "
            "align-items: center; padding: 2px 6px; "
            "border-radius: var(--cb-r-sm, 3px); text-decoration: none; color: inherit;"
        )
    )
    _set_title(link, f"{proj['name']} · {badge['label'] if badge else 'idle'}\n{proj['path']}\nClick to go there")
    with link:
        dot = ui.element("div").style(
            f"width: 6px; height: 6px; border-radius: 50%; background: {badge['color'] if badge else CLR_BORDER};"
        )
        if status == "running":
            dot.classes("cb-live-pulse")
        with ui.element("div").style("display: flex; align-items: baseline; gap: 6px; min-width: 0;"):
            ui.label(proj["name"]).style(
                f"{FONT} font-size: 10px; font-weight: 600; color: {CLR_HEADING}; overflow: hidden; "
                "text-overflow: ellipsis; white-space: nowrap; min-width: 0;"
            )
            if owner != CURRENT_USER:
                ui.label(ProjectsOverview._owner_label(owner)).style(
                    f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; white-space: nowrap; flex-shrink: 0;"
                )
        ui.label(stage).style(
            f"{FONT} font-size: 9px; color: {stage_color}; overflow: hidden; text-overflow: ellipsis; "
            "white-space: nowrap;"
        )
        ui.label(when).style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; text-align: right; white-space: nowrap;")
