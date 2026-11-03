---
name: project_roster_hover_card_tooltips_2026-10-20
description: Projects hub = landing roster in a dialog; name-column hover card with caret; global q-tooltip restyle; Esc pops workspace views — NOT RUN
metadata:
  node_type: memory
  type: project
  originSessionId: 322b9e9a-7ace-4dea-b271-ed3e9f10b519
  modified: 2026-10-20T09:29:01.127Z
---

Built 2026-10-20, NOT RUN in a browser:
- In-project hub (pipeline_roster `_open_project_hub`) lost its BASE bar and left "Project parameters" pane; it is the landing's ProjectsOverview in a 94vw dialog with a folder button (on_browse). `backend.read_project_state_detached` deleted.
- One hover card for both roster mounts (`_render_hover_card`): opens only from the name column (`_name_zone`), card anchored below the row (Quasar flips it above when no room), caret aimed by JS (`data-side`, `--cb-caret-x`); previews + copyable paths + Cs/amp/tilt axis/import facts from the scan's new `card` dict.
- `.q-tooltip.q-tooltip--style` restyled app-wide in ui/dashboard/css.py: slate rgba(51,65,85,0.86) + blur, max-width min(120ch, 92vw).
- Workspace views: slate edge + shadow between side-by-side views; Esc hides the last-opened view (keeps the final one), skipped while a dialog/menu/field/lightbox/pick selection is active. **Superseded 2026-10-27:** Esc closes the lit pane (last pointerdown), see [[project_pane_focus_particles_tools_2026-10-27]].

**Why:** user wanted the two roster surfaces to look the same and tooltips bounded.
**How to apply:** if the caret misaims or the card does not fit, look at `_HOVER_FIT` (updatePosition + getHtmlElement on the q-menu).

Same day, also NOT RUN: logs reworked (ui/components/log_pane.py: dark `.cb-log-well`, error/warn/note line colors; logs tab header = cb-seg-sm stdout/stderr with stderr line-count badge, file path, live mark). Esc bug was LiveLog's `data-pinned` attr matching the Esc guard — renamed to `data-stick`, guard narrowed to `.cb-tp-popover[data-pinned]` and on-screen elements; Esc in a field now blurs and pops. Startup banner: ssh/URL on their own lines; project links bold cyan with "Go to this project: ".
Later 2026-10-20 (NOT RUN): compact column titles sort on click (_column_sort, _column_value; Last opened/Created clears it); new 'Running' column (live progress, red superscript dot for failed TS); roster scroll content padding zeroed (NiceGUI's 1rem padding was the header misalignment); JobSpec display name 'Miss Align' → 'Missalignment' (ids/metadata untouched).
Roster opens sorted by Status (DEFAULT_COLUMN_SORT): running, queued, review, done, failed, idle last — user asked 'running on top, then done, then failed'; queued/review placement was my call.
Tooltips: see-through — rgba(120,120,120,.12) + blur 1.5px, text #111827, 1px rgba(75,85,99,.4) outline, no shadow (light glass rgba(241,245,249,.6)+blur 8px read as "all white/blue", rejected); rich card tips (.cb-chip-tooltip, .cb-params-tt) share it. Rejected on the way: dark slate 0.86 / 0.62 ("black", "looks bad"), white panel in a slate ring ("two-layered cake"). Self-evident tooltips removed (gallery "Click to zoom", view ×, prev/next tomogram, Zoom In/Out).
Later 2026-10-20 (user liked the glass, NOT RUN): glass split into `.cb-glass` (surface only) shared by tooltips and hover popups — roster project card (+ clipped glass caret), landing status-strip popovers, tilt popover; ECharts `_tooltip` uses the same rgba/border/blur; Plotly hover boxes forced by CSS (SVG: no blur, 0.82 tint). Text darkened: base #020617, secondary greys inside glass one step darker (cb-params-k/cb-tt-* via CSS, inline tones edited). Click-opened menus/selects deliberately left opaque.
