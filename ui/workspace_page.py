import asyncio

from nicegui import ui

from backend import CryoBoostBackend
from ui.background_task_tray import mount_background_task_tray
from ui.components.buttons import house_button
from ui.components.reactive import SingleFlight
from ui.pipeline_builder.pipeline_builder_panel import build_pipeline_builder_panel
from ui.routing import RouteWriter, View, apply_route, route_of_url
from ui.species.page import build_species_page
from ui.ui_state import get_ui_state_manager


# Client-side resize for the job-roster panel: drag the divider, clamp to a
# sane range, and persist the width in localStorage so it survives reloads. A
# MutationObserver mirrors the roster's display onto the handle so the divider
# hides with the roster outside the pipeline view. Idempotent (guarded by data-wired).
_ROSTER_RESIZER_JS = """
(function(){
  const roster = document.getElementById('cb-roster-panel');
  const handle = document.getElementById('cb-roster-resizer');
  if(!roster || !handle || handle.dataset.wired) return;
  handle.dataset.wired = '1';
  const MINW = 240, MAXW = 720;
  const clamp = (w) => Math.min(MAXW, Math.max(MINW, w));
  const apply = (w) => { roster.style.width = w+'px'; roster.style.minWidth = w+'px'; };
  try { const s = localStorage.getItem('cbRosterW'); if(s) apply(clamp(parseInt(s,10))); } catch(e){}
  let dragging=false, startX=0, startW=0;
  handle.addEventListener('mousedown', (e)=>{
    dragging=true; startX=e.clientX; startW=roster.getBoundingClientRect().width;
    handle.classList.add('dragging');
    document.body.style.cursor='col-resize'; document.body.style.userSelect='none';
    e.preventDefault();
  });
  window.addEventListener('mousemove', (e)=>{
    if(!dragging) return;
    apply(clamp(Math.round(startW + (e.clientX - startX))));
  });
  window.addEventListener('mouseup', ()=>{
    if(!dragging) return;
    dragging=false; handle.classList.remove('dragging');
    document.body.style.cursor=''; document.body.style.userSelect='';
    try { localStorage.setItem('cbRosterW', String(Math.round(roster.getBoundingClientRect().width))); } catch(e){}
  });
  try {
    const obs = new MutationObserver(()=>{
      handle.style.display = (getComputedStyle(roster).display === 'none') ? 'none' : 'block';
    });
    obs.observe(roster, {attributes:true, attributeFilter:['style']});
  } catch(e){}
})();
"""

# Client-side resize of the views that sit side by side. A view's share of the main area is its
# weight, `--cb-vw-<name>` on the main area: the server rewrites a frame's whole style on every
# toggle, never the main area's. The splitter on a view's left edge drags the boundary with the
# visible view to its left (by computed `order`); the pair keeps the sum of its weights, so the
# other views stay put, and neither drops below MINW. A release stores both weights in
# localStorage and sends one window resize for Plotly's charts (ECharts follows its own box); a
# double-click splits the pair evenly. Idempotent (guarded by data-wired).
_VIEW_RESIZER_JS = """
(function(){
  const main = document.getElementById('cb-view-main');
  if(!main || main.dataset.wired) return;
  main.dataset.wired = '1';
  const MINW = 260, KEY = 'cbViewW:', PREFIX = 'cb-view-';
  const weight = (n) => {
    const w = parseFloat(main.style.getPropertyValue('--cb-vw-'+n));
    return (Number.isFinite(w) && w > 0) ? w : 1;
  };
  const setW = (n, w) => {
    main.style.setProperty('--cb-vw-'+n, w.toFixed(4));
    let min = 1;
    for (const f of main.children) {
      if (!f.id.startsWith(PREFIX)) continue;
      const v = weight(f.id.slice(PREFIX.length));
      if (v < min) min = v;
    }
    main.style.setProperty('--cb-vw-unit', String(min));
  };
  const store = (n) => { try { localStorage.setItem(KEY+n, String(weight(n))); } catch(e){} };
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      const w = (k && k.startsWith(KEY)) ? parseFloat(localStorage.getItem(k)) : NaN;
      if (Number.isFinite(w) && w > 0) setW(k.slice(KEY.length), w);
    }
  } catch(e){}
  const orderOf = (f) => parseInt(getComputedStyle(f).order, 10) || 0;
  const pairAt = (target) => {
    const sp = target.closest ? target.closest('.cb-view-splitter') : null;
    if (!sp) return null;
    const right = sp.parentElement, o = orderOf(right);
    let left = null, lo = -Infinity;
    for (const f of main.children) {
      if (f === right || !f.id.startsWith(PREFIX) || getComputedStyle(f).display === 'none') continue;
      const fo = orderOf(f);
      if (fo < o && fo > lo) { left = f; lo = fo; }
    }
    if (!left) return null;
    return {sp: sp, left: left, right: right, l: left.id.slice(PREFIX.length), r: right.id.slice(PREFIX.length)};
  };
  let drag = null;
  main.addEventListener('mousedown', (e)=>{
    if (e.button !== 0) return;
    const p = pairAt(e.target);
    if (!p) return;
    const wl = p.left.getBoundingClientRect().width, wr = p.right.getBoundingClientRect().width;
    drag = Object.assign(p, {x0: e.clientX, wl: wl, total: wl + wr, sum: weight(p.l) + weight(p.r)});
    p.sp.classList.add('dragging');
    document.body.style.cursor='col-resize'; document.body.style.userSelect='none';
    e.preventDefault();
  });
  const end = ()=>{
    const d = drag; drag = null;
    d.sp.classList.remove('dragging');
    document.body.style.cursor=''; document.body.style.userSelect='';
    store(d.l); store(d.r);
    window.dispatchEvent(new Event('resize'));
  };
  window.addEventListener('mousemove', (e)=>{
    if (!drag) return;
    // A release over an iframe (the molstar viewer) never reaches this window.
    if (!(e.buttons & 1)) { end(); return; }
    if (drag.total < 2 * MINW) return;
    const wl = Math.min(drag.total - MINW, Math.max(MINW, drag.wl + e.clientX - drag.x0));
    setW(drag.l, drag.sum * wl / drag.total);
    setW(drag.r, drag.sum * (drag.total - wl) / drag.total);
  });
  window.addEventListener('mouseup', ()=>{ if (drag) end(); });
  main.addEventListener('dblclick', (e)=>{
    const p = pairAt(e.target);
    if (!p) return;
    const half = (weight(p.l) + weight(p.r)) / 2;
    setW(p.l, half); setW(p.r, half); store(p.l); store(p.r);
    window.dispatchEvent(new Event('resize'));
  });
})();
"""

# Esc closes the views one at a time, the last turned on first (_on_escape). A capturing
# listener on the window, so it runs before the Escapes that dismiss something smaller (the
# slab lightbox, a pinned tilt popover, a pick selection on screen) and leaves the key to them,
# as it does to an open dialog or menu.
_VIEW_ESCAPE_JS = """
(function(){
  if (window.__cbViewEscape) return;
  window.__cbViewEscape = true;
  const busy = '.q-dialog, .q-menu, .cb-slab-lightbox, .cb-tp-popover[data-pinned], .cb-gallery-tile.selected';
  window.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape' || e.repeat) return;
    // Only what is on screen counts: a hidden view keeps its selections.
    if ([...document.querySelectorAll(busy)].some((el) => el.getClientRects().length)) return;
    // Esc in a field leaves the field, then closes the view like anywhere else.
    const a = document.activeElement;
    const fields = 'input, textarea, select, [contenteditable="true"]';
    if (a && a !== document.body && a.closest && a.closest(fields)) a.blur();
    emitEvent('cb_view_escape');
  }, true);
})();
"""


# workspace view name -> the URL view it addresses. The Species page's internal
# mode string is "workbench"; the URL says `species`.
_MODE_VIEWS: dict[str, View] = {
    "pipeline": View.PIPELINE,
    "workbench": View.SPECIES,
    "journey": View.JOURNEY,
    "gallery": View.TOMOGRAMS,
    "viewer": View.PICKS,
    "protocols": View.PROTOCOLS,
    "table": View.TABLE,
}

# Left-to-right order of the views that are on: the rail's order, the pipeline (job roster
# + job page) first; the pick viewer, which has no icon, and the foot-of-rail Protocols last.
_VIEW_ORDER = ("pipeline", "table", "workbench", "gallery", "journey", "viewer", "protocols")

# Every view's frame: its share of the main area's width when several are on is its weight
# (`--cb-vw-<name>`, 1 until a splitter moves it; _VIEW_RESIZER_JS). It holds the view's title
# line (a stripe: its name, the expand icon and the close cross), the box the view builds into below it
# and the splitter on its left edge.
_VIEW_STYLE = "min-width: 0; height: 100%; overflow: hidden; display: none; flex-direction: row; position: relative;"
_VIEW_COLUMN_STYLE = "flex: 1 1 0; min-width: 0; height: 100%; display: flex; flex-direction: column; overflow: hidden;"
_VIEW_HEAD_STYLE = (
    "flex: 0 0 auto; display: flex; align-items: center; gap: 4px; min-height: 28px; padding: 3px 6px 3px 14px; "
    "margin-bottom: 6px; background: var(--cb-view-head, linear-gradient(180deg, #ffffff, #edf3f9)); "
    "box-shadow: 0 var(--cb-view-head-line-w, 1px) 0 var(--cb-view-head-line, #a9c6e4), "
    "inset 0 1px 0 var(--cb-head-glint, rgba(255, 255, 255, 0.9));"
)
_VIEW_TITLE_STYLE = (
    "font-family: 'IBM Plex Sans', sans-serif; font-size: 12px; font-weight: 600; "
    "color: var(--cb-view-head-ink, #12324f); letter-spacing: var(--cb-head-track, 0); "
    "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0;"
)
_VIEW_BODY_STYLE = "flex: 1 1 0; min-width: 0; min-height: 0; overflow: hidden; display: flex; flex-direction: column;"
# The line between two main panels (views side by side, the roster and the view beside it).
_VIEW_EDGE = "var(--cb-edge-w, 1px) solid var(--cb-edge, #c9d6e3)"
# Each view's name on its title line; the pipeline's names the job its page shows (set_view_title).
_VIEW_TITLES = {
    "pipeline": "Jobs",
    "workbench": "Particles registry",
    "journey": "Journey",
    "gallery": "Tomograms",
    "viewer": "Pick viewer",
    "protocols": "Protocols",
    "table": "Pipeline",
}


def build_workspace_page(backend: CryoBoostBackend, route_writer: RouteWriter | None = None) -> dict:
    """Build the workspace and return its `callbacks` dict, which the routed page handler
    needs to apply the URL's view+target (`ui.routing.apply_route`)."""
    ui_mgr = get_ui_state_manager()
    ui_mgr.prepare_for_page_rebuild()

    if not ui_mgr.is_project_created:
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("error_outline", size="64px").classes("text-red-400")
            ui.label("No project loaded").classes("text-xl text-gray-600")
            house_button("Return to start", lambda: ui.navigate.to("/"))
        return {}

    if not ui_mgr.project_path or not ui_mgr.project_path.exists():
        with ui.column().classes("w-full h-screen items-center justify-center gap-4"):
            ui.icon("folder_off", size="64px").classes("text-orange-400")
            ui.label("Project path is invalid").classes("text-xl text-gray-600")
            ui.label(str(ui_mgr.project_path)).classes("text-sm text-gray-400 font-mono")
            house_button("Return to start", lambda: ui.navigate.to("/"))
        return {}

    callbacks = {}
    # The views showing, in the order they were turned on. The last one is the view the
    # address bar names.
    _on: list[str] = ["pipeline"]
    _refs = {}

    if route_writer is None:
        from ui.open_project import disambiguating_base

        route_writer = RouteWriter(ui_mgr.project_path.name, disambiguating_base(ui_mgr.project_path))
    callbacks["set_url"] = route_writer.write
    callbacks["route_writer"] = route_writer

    def _write_mode_url() -> None:
        """The address bar follows the view turned on last. Every view writes the bare
        route for itself here; the ones with a target (a job, a tilt-series, a species)
        refine it from their own selection handler right after. The pipeline is the one
        exception — its target is the active job, which lives on `ui_mgr`, so there is
        nothing to wait for."""
        if not _on:
            return
        mode = _on[-1]
        view = _MODE_VIEWS.get(mode, View.PIPELINE)
        if view is View.PIPELINE and ui_mgr.active_instance_id:
            iid = ui_mgr.active_instance_id
            route_writer.write(View.JOB, iid, ui_mgr.get_job_ui_state(iid).active_monitor_tab)
            return
        if view is View.PICKS:
            page = _refs.get("viewer_page")
            if page is None or not page.species_id:
                return  # a picks URL without a species names nothing — leave the bar alone
            route_writer.write(View.PICKS, page.species_id, page.tomo_name)
            return
        route_writer.write(view)

    def _apply_views() -> None:
        """Show every view that is on, side by side in the rail's order (_VIEW_ORDER: the
        pipeline, i.e. job roster + job page, leftmost), sharing the width by their weights;
        hide the rest. Every view with a view to its left shows its splitter. The roster comes
        and goes with the pipeline view (the roster's set_active_views), and each view's
        refresh timers run only while it shows."""
        first = True
        for i, name in enumerate(_VIEW_ORDER):
            c = _refs.get(f"{name}_frame")
            if c is None:
                continue
            expand = _refs.get(f"{name}_expand")
            if expand is not None:
                # Alone already: nothing to expand over.
                expand.set_visibility(len(_on) > 1)
            if name in _on:
                # A slate edge and a faint shadow cast onto the view to its left, so two views
                # read as two panels rather than one gray surface.
                border, shadow = (
                    ("none", "none")
                    if first
                    else (_VIEW_EDGE, "var(--cb-edge-shadow, -2px 0 6px rgba(16, 42, 67, 0.06))")
                )
                c.style(f"display: flex; order: {i}; border-left: {border}; box-shadow: {shadow};")
                if first:
                    c.classes(remove="cb-view-split")
                else:
                    c.classes(add="cb-view-split")
                first = False
            else:
                c.style("display: none;")
                c.classes(remove="cb-view-split")
        empty = _refs.get("empty_container")
        if empty is not None:
            empty.style(f"display: {'none' if _on else 'flex'};")

        # Nav-icon highlights + roster hide/restore.
        set_views = callbacks.get("set_active_views")
        if set_views:
            set_views(set(_on))

        # The roster's and the shown job page's timers run only while the pipeline view shows.
        on_pipeline = callbacks.get("on_pipeline_active")
        if on_pipeline:
            on_pipeline("pipeline" in _on)

        # Pause/resume the journey's 4 s live-refresh with its visibility.
        on_journey = callbacks.get("on_journey_active")
        if on_journey:
            on_journey("journey" in _on)

        # Instant registry observe when the Species page is shown (species created
        # from the roster / another tab appear without waiting for its 3 s poll).
        on_wb = callbacks.get("on_workbench_active")
        if on_wb:
            on_wb("workbench" in _on)

        # The pick viewer's coalesced-rebuild timer, the gallery's pending-preview poll, the
        # Protocols view's 15 s poll and the pipeline table's status watch.
        for page_key, name in (
            ("viewer_page", "viewer"),
            ("gallery_page", "gallery"),
            ("protocols_page", "protocols"),
            ("table_page", "table"),
        ):
            page = _refs.get(page_key)
            if page is not None:
                page.set_active(name in _on)

    def _show(name: str) -> None:
        """Turn a view on (a no-op for its layout when it already shows) and make it the
        one the address bar names."""
        was_on = name in _on
        if was_on:
            _on.remove(name)
        _on.append(name)
        if not was_on:
            _apply_views()
            if name == "pipeline":
                invalidate = callbacks.get("invalidate_tm_tabs")
                if invalidate:
                    invalidate()
        # Coarse route first, then each view refines it with its own target — which is also
        # what makes the history entry come out right: the view change pushes, the
        # refinement replaces.
        _write_mode_url()

    def _hide(name: str) -> None:
        """Turn a view off; the address bar then names the view turned on before it."""
        if name not in _on:
            return
        _on.remove(name)
        _apply_views()
        _write_mode_url()

    def _solo(name: str) -> None:
        """The expand icon: this view alone fills the main area; every other view turns off."""
        _on[:] = [name]
        _apply_views()
        _write_mode_url()

    def _set_view_title(name: str, text: str, tooltip: str | None = None) -> None:
        """A view names itself on its title line (the pipeline: the job its page shows); the
        tooltip is a native hover title."""
        title = _refs.get(f"{name}_title")
        if title is None or title.is_deleted:
            return
        title.set_text(text)
        if tooltip:
            title.props["title"] = tooltip
        else:
            title.props.pop("title", None)
        title.update()

    def _view_frame(name: str, style: str = _VIEW_STYLE) -> ui.element:
        """A view's frame in the main area, styled by _apply_views; returns the box the view
        builds into. Its title line names the view and carries, at the right end, the expand
        icon (this view alone) and the cross that turns it off, the same as its rail icon; the
        splitter on its left edge resizes it against the view to its left."""
        frame = (
            ui.element("div")
            .props(f"id=cb-view-{name}")
            .style(f"flex: calc(var(--cb-vw-{name}, 1) / var(--cb-vw-unit, 1)) 1 0; {style}")
        )
        _refs[f"{name}_frame"] = frame
        with frame:
            with ui.element("div").style(_VIEW_COLUMN_STYLE):
                with ui.element("div").style(_VIEW_HEAD_STYLE):
                    _refs[f"{name}_title"] = ui.label(_VIEW_TITLES[name]).style(_VIEW_TITLE_STYLE)
                    ui.element("div").style("flex: 1 1 auto;")
                    _refs[f"{name}_expand"] = (
                        ui.icon("open_in_full", size="10px")
                        .classes("cb-view-close")
                        .props('title="Show only this view"')
                        .on("click", lambda _e: _solo(name))
                    )
                    ui.icon("close", size="12px").classes("cb-view-close").props('title="Close this view"').on(
                        "click", lambda _e: _hide(name)
                    )
                body = ui.element("div").style(_VIEW_BODY_STYLE)
            ui.element("div").classes("cb-view-splitter")
        _refs[f"{name}_container"] = body
        return body

    def _toggle_pipeline():
        """The Jobs icon: the job roster and the job page, on or off together."""
        if "pipeline" in _on:
            _hide("pipeline")
        else:
            _show("pipeline")

    def _ensure_species_page() -> None:
        """Build the Species page on its first showing, not with the workspace: it selects its
        first species and builds that species' Overview, work nobody waits for until they open it.
        Building registers its callbacks (workbench_select_species, species_select_tab, ...)."""
        if _refs.get("species_built"):
            return
        _refs["species_built"] = True
        with _refs["workbench_container"]:
            build_species_page(backend, callbacks)

    def _show_workbench():
        _ensure_species_page()
        _show("workbench")

    def _toggle_workbench():
        if "workbench" in _on:
            _hide("workbench")
        else:
            _show_workbench()

    def _open_species(species_id: str):
        """The 'open in Species' link of a job's Config tab: show the Species page ON
        that species (the view keeps its internal mode name "workbench")."""
        _show_workbench()
        select = callbacks.get("workbench_select_species")
        if select:
            select(species_id)

    def ensure_pipeline_mode():
        if "pipeline" not in _on:
            _show("pipeline")

    def _rail_toggle(name: str, show, flight: SingleFlight):
        """A rail icon's click: turn its view off when it shows, else show it (building it on
        first use). SingleFlight: the icon can be destroyed and recreated mid-click by a
        roster refresh, so several clicks may land; they collapse to one."""

        async def _toggle():
            async with flight("toggle") as acquired:
                if not acquired:
                    return
                if name in _on:
                    _hide(name)
                else:
                    await show()

        return _toggle

    _journey_flight = SingleFlight()
    _journey_built = {"done": False}

    async def _show_journey():
        async with _journey_flight("show") as acquired:
            if not acquired:
                return
            _show("journey")
            if _journey_built["done"]:
                return
            _journey_built["done"] = True
            jc = _refs.get("journey_container")
            if jc is None:
                return
            # Paint a spinner immediately, yield so it flushes to the client,
            # then build. The first per-TS render blocks ~1-2 s; the CSS spinner
            # keeps animating client-side meanwhile. Later switches are an
            # instant display swap (the panel persists).
            with (
                jc,
                ui.element("div").style(
                    "width: 100%; height: 100%; display: flex; align-items: center; justify-content: center; gap: 10px;"
                ),
            ):
                ui.spinner(size="lg", color="#94a3b8")
                ui.label("Loading journey…").classes("text-[10px] text-gray-500")
            await asyncio.sleep(0.03)
            from ui.tomo_dashboard_dialog import build_journey_panel

            build_journey_panel(jc, callbacks)

    _gallery_flight = SingleFlight()

    async def _show_gallery():
        """The Tomograms view: the birds-eye wall of reconstructions. Built lazily on
        first use like the journey (its first pass stats every tomogram's previews)."""
        async with _gallery_flight("show") as acquired:
            if not acquired:
                return
            _show("gallery")
            gc = _refs.get("gallery_container")
            if gc is None:
                return
            page = _refs.get("gallery_page")
            if page is None:
                from ui.tomo_gallery import TomoGalleryPage

                with gc:
                    page = TomoGalleryPage(gc, ui_mgr.project_path, callbacks)
                _refs["gallery_page"] = page
                page.set_active(True)
            await page.show()

    _protocols_flight = SingleFlight()

    async def _show_protocols():
        """The foot-of-rail protocol light: the Protocols view — this project's parameters
        beside the protocol it was created from, and the protocol library. Built lazily like the
        gallery."""
        async with _protocols_flight("show") as acquired:
            if not acquired:
                return
            _show("protocols")
            pc = _refs.get("protocols_container")
            if pc is None:
                return
            page = _refs.get("protocols_page")
            if page is None:
                from ui.protocols_view import ProtocolsPage

                with pc:
                    page = ProtocolsPage(pc, backend, ui_mgr, callbacks)
                _refs["protocols_page"] = page
                page.set_active(True)
            await page.show()

    _table_flight = SingleFlight()

    async def _show_table():
        """The rail's branch glyph: the pipeline table — every job of the roster in dependency
        order. Built lazily like the gallery."""
        async with _table_flight("show") as acquired:
            if not acquired:
                return
            _show("table")
            tc = _refs.get("table_container")
            if tc is None:
                return
            page = _refs.get("table_page")
            if page is None:
                from ui.pipeline_table_view import PipelineTablePage

                with tc:
                    page = PipelineTablePage(tc, ui_mgr.project_path, callbacks)
                _refs["table_page"] = page
                page.set_active(True)
            await page.show()

    _viewer_flight = SingleFlight()

    async def _open_pick_viewer(species_id: str | None, tomo_name: str | None) -> None:
        """The Picks & curation row's `viewer ↗` (and the Journey's `full viewer ↗`):
        show the full-page pick viewer on this (species, tomogram), building it on first
        use like the Journey. It has no rail icon; its header's close link turns it off."""
        async with _viewer_flight("open") as acquired:
            if not acquired:
                return
            vc = _refs.get("viewer_container")
            if vc is None:
                return
            page = _refs.get("viewer_page")
            if page is None:
                from ui.particles.pick_viewer import PickViewerPage

                with vc:
                    page = PickViewerPage(vc, ui_mgr.project_path, callbacks)
                _refs["viewer_page"] = page
            _show("viewer")
            await page.show(species_id, tomo_name)
            # show() keeps whichever of the pair was passed as None, so the URL is
            # written from the page's resolved selection, not from the arguments.
            _write_mode_url()

    def _invalidate_gallery():
        """Something added tomograms to the project — make the wall re-collect on its next
        visit instead of showing what it cached before the import."""
        page = _refs.get("gallery_page")
        if page is not None:
            page.invalidate()

    def _on_popstate(e) -> None:
        """Back/Forward: re-dispatch the popped URL onto the views that
        are already built, instead of letting the browser rebuild the page (which is what
        a real navigation would do, and would throw the Journey/gallery/viewer away)."""
        raw = e.args
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        popped = route_of_url(str(raw or ""))
        if popped is None or popped.project != route_writer.project:
            # Left this project's URL space entirely — let the browser have it.
            ui.navigate.to(str(raw or "/"))
            return
        route_writer.adopt(popped)
        route_writer.suppressed = True

        async def _apply() -> None:
            try:
                await apply_route(callbacks, popped, ui_mgr.project_path)
            finally:
                route_writer.suppressed = False

        ui.timer(0, _apply, once=True)

    def _on_escape(_e) -> None:
        """Esc turns off the view turned on last; the one left showing stays."""
        if len(_on) > 1:
            _hide(_on[-1])

    ui.on("cb_view_escape", _on_escape)
    ui.on("cb_popstate", _on_popstate)
    ui.timer(
        0.1,
        lambda: ui.run_javascript(
            "window.addEventListener('popstate', function(){"
            "  emitEvent('cb_popstate', location.pathname + location.search);"
            "});"
        ),
        once=True,
    )

    # The rail's icons toggle their view; links and routes only ever show one (show_*).
    toggle_journey = _rail_toggle("journey", _show_journey, _journey_flight)
    toggle_gallery = _rail_toggle("gallery", _show_gallery, _gallery_flight)
    toggle_protocols = _rail_toggle("protocols", _show_protocols, _protocols_flight)
    toggle_table = _rail_toggle("table", _show_table, _table_flight)

    callbacks["invalidate_gallery"] = _invalidate_gallery
    callbacks["toggle_pipeline"] = _toggle_pipeline
    callbacks["show_workbench"] = _show_workbench
    callbacks["open_species"] = _open_species
    callbacks["ensure_pipeline_mode"] = ensure_pipeline_mode
    callbacks["show_journey"] = _show_journey
    callbacks["show_gallery"] = _show_gallery
    callbacks["show_protocols"] = _show_protocols
    callbacks["show_table"] = _show_table
    callbacks["open_pick_viewer"] = _open_pick_viewer
    callbacks["set_view_title"] = _set_view_title

    with ui.element("div").style(
        "position: fixed; inset: 0; display: flex; flex-direction: row; "
        "overflow: hidden; gap: 0; margin: 0; padding: 0;"
    ):
        primary_sidebar = ui.element("div").style(
            "width: 60px; min-width: 60px; height: 100%; flex-shrink: 0; "
            "background: var(--cb-rail, #eef3f8); display: flex; flex-direction: column; "
            "align-items: center; gap: 0; overflow: visible; z-index: 20; "
            "border-right: 1px solid var(--cb-rail-line, #d3dde8);"
        )

        roster_panel = (
            ui.element("div")
            .props("id=cb-roster-panel")
            .style(
                "width: 340px; min-width: 340px; height: 100%; flex-shrink: 0; "
                f"background: var(--cb-surface, #ffffff); border-right: {_VIEW_EDGE}; "
                "overflow-y: auto; overflow-x: hidden; "
                "flex-direction: column; gap: 0; display: flex;"
            )
        )

        # Draggable divider between the job roster and the params/main area.
        # Resize + persistence is client-side JS (see _ROSTER_RESIZER_JS) so the
        # drag is smooth and survives reloads via localStorage.
        ui.element("div").props("id=cb-roster-resizer").classes("cb-roster-resizer")

        # Styled once and never again: the view weights the splitters set on it must survive.
        main_area = (
            ui.element("div")
            .props("id=cb-view-main")
            .style("flex: 1; min-width: 0; height: 100%; overflow: hidden; display: flex; flex-direction: row; gap: 0;")
        )

        with main_area:
            pipeline_container = _view_frame("pipeline", _VIEW_STYLE + " display: flex; order: 0;")
            with pipeline_container:
                build_pipeline_builder_panel(
                    backend,
                    callbacks,
                    primary_sidebar=primary_sidebar,
                    roster_panel=roster_panel,
                    toggle_workbench=_toggle_workbench,
                    ensure_pipeline_mode=ensure_pipeline_mode,
                    toggle_journey=toggle_journey,
                    toggle_gallery=toggle_gallery,
                    toggle_protocols=toggle_protocols,
                    toggle_table=toggle_table,
                )

            # Species page: built on its first showing (_ensure_species_page).
            _view_frame("workbench")

            # Journey: built lazily on first switch (see _show_journey) so the
            # heavy per-TS render doesn't tax every workspace load. Starts hidden.
            _view_frame("journey")

            # Tomogram gallery: the birds-eye wall of reconstructions. Lazily built
            # like the journey — its first pass stats every tomogram's previews.
            _view_frame("gallery")

            # Pick viewer: the full-page mount of the slabs + lists +
            # gallery component, opened from a tomogram row of the Particles registry's
            # Picks & curation tab. Lazily built on first use, like the journey; it has
            # no nav icon of its own — its own header carries the routes back, its title line the close cross.
            _view_frame("viewer")

            # Protocols: lazily built on the first click of the foot-of-rail protocol light.
            _view_frame("protocols")

            # Pipeline table: lazily built on the first click of its rail icon.
            _view_frame("table")

            # Every view turned off: say where they are rather than leave a blank page.
            empty_container = ui.element("div").classes("cb-empty").style("flex: 1 1 0; display: none;")
            _refs["empty_container"] = empty_container
            with empty_container:
                ui.icon("view_column", size="40px").classes("text-gray-400")
                ui.label("No view open.").classes("text-sm text-gray-500")
                ui.label("Turn views on and off with the icons on the left; several can show side by side.").classes(
                    "text-[11px] italic text-gray-400"
                )

    # Floating background-tasks tray at workspace scope so spun-off
    # renders/builds remain visible across dialog open/close and view
    # toggles. Survives the entire workspace session; teardown happens
    # when the page rebuilds.
    mount_background_task_tray(project_path_provider=lambda: str(ui_mgr.project_path) if ui_mgr.project_path else None)

    # Wire the roster resizer, the view splitters and Esc once the DOM for this page exists on the client.
    ui.timer(0.1, lambda: ui.run_javascript(_ROSTER_RESIZER_JS + _VIEW_RESIZER_JS + _VIEW_ESCAPE_JS), once=True)

    # The opening layout (the pipeline alone) through the same path every toggle takes, so
    # its rail icon is lit the way a toggled one is.
    _apply_views()

    return callbacks
