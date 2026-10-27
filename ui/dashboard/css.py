"""Journey dashboard CSS.

A single ``<style>`` blob injected once per dashboard mount via ``ensure_assets_loaded()``.
Pure presentation; no logic.
"""

from nicegui import context, ui


_CB_CSS = """
.cb-sidebar {
    width: 300px; min-width: 300px; flex-shrink: 0;
    display: flex; flex-direction: column;
}
.cb-sidebar-header {
    padding: 8px 10px 6px;
    border-bottom: 1px solid #e5e7eb;
    background: #f8fafc;
    flex-shrink: 0;
    font-size: 11px;
    color: #475569;
}
.cb-sidebar-rows { overflow-y: auto; flex: 1; min-height: 0; }
.cb-ts-row {
    display: flex; flex-direction: column; gap: 3px;
    padding: 5px 10px; border-bottom: 1px solid #f1f1f1;
    cursor: pointer;
}
.cb-ts-row:hover { background: #f8fafc; }
.cb-ts-row.selected { background: #eef2ff; border-left: 3px solid #6366f1; padding-left: 7px; }
.cb-ts-titlebar { display: flex; align-items: center; gap: 4px; min-height: 18px; }
.cb-ts-row .cb-ts-pos { font-weight: 600; color: #1f2937; font-size: 11px; }
/* Two tracks per row: a 'prep' bar (the 4 array stages) and one line per
 * species (pick + subtomo segments + pick count). Fixed-width segments keep a
 * stage comparable across tracks; the head column aligns the bars vertically. */
.cb-track { display: flex; align-items: center; gap: 6px; }
.cb-track-head { flex: 0 0 66px; display: flex; align-items: center; gap: 4px; overflow: hidden; }
.cb-track-name { font-size: 9px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.3px; }
.cb-sp-name {
    font-size: 10px; color: #475569; font-family: ui-monospace, monospace;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-roster-sp-dot {
    width: 7px; height: 7px; border-radius: 50%; flex: 0 0 auto;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.18);
}
.cb-pill-strip { display: flex; gap: 2px; }
.cb-pill { height: 4px; width: 13px; flex: 0 0 13px; border-radius: 2px; background: #e5e7eb; }
.cb-pill.ok { background: #10b981; }
.cb-pill.fail { background: #dc2626; }
.cb-pill.running { background: #f59e0b; }
.cb-pill.pending { background: #d1d5db; }
.cb-sp-count {
    margin-left: auto; font-size: 10px; font-family: ui-monospace, monospace;
    color: #475569; min-width: 16px; text-align: right;
}
/* Review column: kept-count after curation (indigo funnel). Present only for
 * reviewed TS, so its presence = "reviewed", absence = "not yet". */
.cb-sp-filtered {
    display: flex; align-items: center; gap: 1px; margin-left: 6px; flex: 0 0 auto;
    font-size: 10px; font-family: ui-monospace, monospace; color: #6366f1;
}
/* zero = stage processed this TS but produced no output (e.g. 0 picks
   above cutoff). Dimmed amber-into-grey so it reads as "ran, yielded
   nothing" — distinct from both "ok" green and "pending" grey. */
.cb-pill.zero {
    background: repeating-linear-gradient(
        45deg, #9ca3af, #9ca3af 2px, #d1d5db 2px, #d1d5db 4px
    );
}
/* skip = supervisor deliberately did not dispatch a task for this TS
   (upstream produced nothing actionable). Soft hatched grey reads as
   "intentionally blank", distinct from "pending" flat grey. */
.cb-pill.skip {
    background: repeating-linear-gradient(
        45deg, #cbd5e1, #cbd5e1 2px, #e5e7eb 2px, #e5e7eb 4px
    );
}
/* Per-row info popover (the ⓘ): full tomo name + metadata + pick counts + file paths,
 * each path copying on click. Click-opened so the values can be clicked. */
.cb-info-card {
    min-width: 300px; max-width: 460px; padding: 6px 4px; display: flex; flex-direction: column; gap: 3px;
    font-family: 'IBM Plex Sans', sans-serif;
}
.cb-info-row { display: flex; align-items: center; gap: 6px; }
.cb-info-key {
    font-size: 9px; color: #8aa0b8; flex: 0 0 88px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-info-val {
    font-size: 10px; font-family: 'IBM Plex Mono', monospace; color: #334155;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    flex: 1 1 auto; min-width: 0;
    /* rtl truncates the LEFT of long paths, keeping the filename visible. */
    direction: rtl; text-align: left;
}
.cb-info-meta { font-size: 10px; color: #64748b; font-family: 'IBM Plex Mono', monospace; padding: 1px 0 3px 88px; }
.cb-info-val.cb-info-count { direction: ltr; font-family: 'IBM Plex Sans', sans-serif; color: #1e293b; }
.cb-main { padding: 12px; }
/* (height/flex/overflow set inline at construction time so the dialog viewport
   chain is self-contained; this rule only carries the padding chrome.) */
.cb-empty {
    flex: 1; display: flex; align-items: center; justify-content: center;
    color: #9ca3af; font-size: 13px; padding: 40px; flex-direction: column; gap: 8px;
}
.cb-section-title {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 12px; font-weight: 600;
    color: #1e293b; letter-spacing: 0;
}
.cb-section-card {
    background: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px;
    padding: 10px 12px 8px; margin-bottom: 8px;
}
/* Title line: title · (params ⓘ) · (status). Real air between it and the content. */
.cb-section-card-header { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
.cb-section-status { font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; color: #b45309; }
/* Job-parameter ⓘ beside a section title: the parameters live in its tooltip. */
.cb-params-icon { color: #cbd5e1; cursor: help; font-size: 13px !important; }
.cb-params-icon:hover { color: #475569; }
.cb-params-tt {
    /* The panel itself is the app tooltip's (below); this only lays out the key/value grid. */
    padding: 6px 9px;
    display: grid; grid-template-columns: max-content max-content; gap: 2px 12px;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px;
}
.cb-params-tt .cb-params-k { color: #94a3b8; }
.cb-params-tt .cb-params-v { font-family: 'IBM Plex Mono', monospace; color: #1e293b; }
/* Collapsible Dataset section: clickable header + rotating caret; the body
 * (chips + key/val grid + pixel-sanity table) hides when collapsed. Default
 * collapsed (pref dashboard_dataset_collapsed). */
.cb-collapsible-header { cursor: pointer; user-select: none; }
.cb-collapse-caret { color: #94a3b8; transition: transform 0.15s ease; margin-left: 2px; }
.cb-collapse-caret.rot { transform: rotate(180deg); }
.cb-collapsible-body.cb-collapsed { display: none; }
.cb-aspect { width: 100%; }
.cb-picks-right { border-left: 1px solid #eef2f7; padding-left: 14px; }
@media (max-width: 900px) {
    .cb-picks-right {
        border-left: none; padding-left: 0;
        border-top: 1px solid #eef2f7; padding-top: 10px;
    }
}
.cb-hover-card {
    background: #f8fafc; border: 1px solid #e5e7eb; border-radius: 4px;
    padding: 8px 10px; font-family: ui-monospace, monospace;
    font-size: 11px; color: #374151;
    display: grid; grid-template-columns: max-content 1fr;
    gap: 4px 12px; align-items: baseline;
}
.cb-hover-card .cb-hover-key {
    color: #6b7280; text-transform: uppercase; font-size: 9px;
    font-weight: 700; letter-spacing: 0.4px;
}
.cb-hover-card .cb-hover-val { color: #1f2937; }
.cb-hover-card.cb-hover-empty { color: #9ca3af; font-style: italic; }
.cb-gallery-grid {
    display: grid; grid-template-columns: repeat(auto-fill, 96px);
    gap: 4px; padding: 0 2px 6px 2px; justify-content: start;
}
.cb-gallery-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    border-radius: 3px; cursor: pointer; overflow: hidden;
    border: 2px solid transparent; transition: transform 0.06s ease;
}
.cb-gallery-tile:hover { transform: scale(1.04); border-color: #c7d2fe; }
/* Selected tile. Amber, matching .cb-pick-ghost.cb-ghost-selected on the slab,
 * so the tile and its dot read as one thing. It stays until another tile is clicked or
 * Esc/click-away clears it, unlike the transient .cb-tile-highlight hover brush. */
.cb-gallery-tile.selected {
    border-color: #f59e0b;
    box-shadow: 0 0 0 1px #f59e0b, 0 4px 10px rgba(245,158,11,0.3);
}
.cb-gallery-tile .cb-tile-score {
    position: absolute; bottom: 0; right: 0;
    padding: 1px 4px; background: rgba(15,23,42,0.72);
    font-family: ui-monospace, monospace; font-size: 9px; color: #f8fafc;
    border-top-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-z {
    position: absolute; bottom: 0; left: 0;
    padding: 1px 4px; background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace; font-size: 9px; color: #cbd5e1;
    border-top-right-radius: 3px;
}
.cb-gallery-tile .cb-tile-idx {
    position: absolute; top: 0; right: 0;
    padding: 0 4px; background: rgba(15,23,42,0.55);
    font-family: ui-monospace, monospace; font-size: 9px; color: #cbd5e1;
    border-bottom-left-radius: 3px;
}
.cb-gallery-tile .cb-tile-rank {
    position: absolute; top: 0; left: 0;
    padding: 0 4px; background: rgba(67,56,202,0.85);
    font-family: ui-monospace, monospace; font-size: 9px; color: white;
    border-bottom-right-radius: 3px;
}
.cb-gallery-empty {
    padding: 18px; text-align: center; font-size: 11px; color: #6b7280;
    background: #f8fafc; border-radius: 4px; border: 1px dashed #cbd5e1;
}
.cb-tomo-preview {
    width: 100%; background: #0f172a; border-radius: 4px;
    overflow: hidden; position: relative;
}
.cb-tomo-preview img { width: 100%; height: 100%; object-fit: cover; display: block; }
.cb-preview-stack { display: flex; flex-direction: column; gap: 6px; width: 100%; }
.cb-pick-marker {
    position: absolute; width: 14px; height: 14px;
    border-radius: 50%; border: 2px solid #fff;
    background: rgba(244, 114, 182, 0.95);
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.55), 0 0 8px rgba(244, 114, 182, 0.55);
    transform: translate(-50%, -50%);
    pointer-events: none; opacity: 0;
    transition: opacity 0.08s ease, left 0.05s linear, top 0.05s linear;
    z-index: 6; left: 0; top: 0;
}
.cb-pick-ghost {
    position: absolute; width: 5px; height: 5px;
    border-radius: 50%; background: rgba(67, 56, 202, 0.55);
    box-shadow: 0 0 0 0.5px rgba(255, 255, 255, 0.25);
    transform: translate(-50%, -50%);
    pointer-events: none; z-index: 4;
}
.cb-overlay-hide .cb-pick-ghost { display: none; }
/* Per-species overlay layer over the shared recon canvas. Inset to the
 * host so child ghost-dots anchor to the same box as the slab image; a
 * single checkbox toggles the whole species layer (X/Y + X/Z together).
 * The layer carries `--sp-color` (set inline per species); the ghost-dot
 * restyle rule below reads it via var() so each species' dots are colored. */
.cb-pick-layer { position: absolute; inset: 0; pointer-events: none; z-index: 4; }
.cb-pick-layer-hidden { display: none; }
.cb-recon-canvas { background: #0f172a; border-radius: 6px; }
/* X/Y + X/Z stack: fills its column (width:100%, left-aligned margin:0). The
 * column is capped per-tomo to the slab's height-limited width (see
 * .cb-particles-canvas-col), so the stack — and the side strip below it — match
 * that width exactly with no trailing whitespace before the gallery. Each child
 * derives its height from its own aspect-ratio. */
.cb-canvas-stack { width: 100%; margin: 0; }
.cb-species-swatch {
    width: 10px; height: 10px; border-radius: 50%;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.3); flex: 0 0 auto;
}
/* Swatch shape mirrors the dot glyph (see .cb-shape-*). */
.cb-swatch-circle { border-radius: 50%; }
.cb-swatch-square { border-radius: 0; }
.cb-swatch-diamond { border-radius: 0; transform: rotate(45deg); }
.cb-swatch-triangle { border-radius: 0; clip-path: polygon(50% 0, 0 100%, 100% 100%); }
/* Particles section: slabs LEFT, galleries RIGHT, side by side so the user
 * can hover a tile and watch its dot light up on the canvas at the same time.
 * Wraps to stacked on narrow viewports. */
.cb-particles-split { display: flex; gap: 12px; align-items: flex-start; flex-wrap: wrap; }
/* Slab column is pinned to the slab's natural height-capped width: its width is set
 * inline per tomo (render_particles_section) to min(1400px, slab_vh·x/y), and it does
 * not flex-grow, so the gallery column (flex-grow 1) claims the remaining width.
 * The inline max-width is the mode's % of the row, not 100%: the vh·aspect width
 * alone lands at ~half the row on a typical monitor, so the % ceiling keeps the slab
 * from starving the gallery (see _SLAB_CAPS: 60vh/34% slim, 74vh/46% full). On a very
 * narrow viewport the gallery wraps below. */
.cb-particles-canvas-col { flex: 0 0 auto; }
.cb-particles-detail-col { flex: 1 1 360px; min-width: 360px; }
.cb-ltable {
    flex: 1 1 auto; min-width: 0;
    display: flex; flex-direction: column; gap: 1px;
    background: #fafbfc; border: 1px solid #eef1f6; border-radius: 8px; padding: 3px;
}
.cb-ltable-row {
    display: grid;
    /* swatch · name · picks · ext · path · delete · eye. The Picks & curation table
     * re-orders these and adds source / origin / job / actions through .cb-ptable-row's
     * own template below. */
    grid-template-columns: 14px minmax(0, 1fr) 48px 26px 22px 22px 20px;
    align-items: center; gap: 8px;
    padding: 3px 7px; border-radius: 5px; cursor: pointer;
    border: 1px solid transparent; transition: background 0.12s, border-color 0.12s;
}
.cb-ltable-row:hover { background: #ffffff; border-color: #e3e8f0; }
.cb-ltable-row.selected { background: #eef2ff; border-color: #c7d2fe; box-shadow: inset 2px 0 0 #6366f1; }
/* Header row: muted column labels, non-interactive, a hairline under it. */
.cb-ltable-head { cursor: default; box-shadow: inset 0 -1px 0 #eef1f6; }
.cb-ltable-head:hover { background: transparent; border-color: transparent; }
.cb-ltable-h-name, .cb-ltable-h-num, .cb-ltable-h-cell {
    font-size: 8px; text-transform: uppercase; font-weight: 700; letter-spacing: 0.04em; color: #b6c0cf;
}
.cb-ltable-h-num { justify-self: end; }
.cb-ltable-h-cell { text-align: center; }
.cb-ltable-cell { display: flex; align-items: center; justify-content: center; min-width: 0; }
.cb-ltable-swatch {
    width: 11px; height: 11px; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25);
    justify-self: center; flex: 0 0 auto;
}
.cb-ltable-name {
    font-size: 11px; font-weight: 600; color: #475569;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cb-ltable-count {
    font-size: 11px; font-family: ui-monospace, monospace; color: #334155; justify-self: end;
}
.cb-ltable-badge { font-size: 12px; font-weight: 700; cursor: help; line-height: 1; }
.cb-badge-ok { color: #059669; }
.cb-badge-todo { color: #94a3b8; }
.cb-badge-stale { color: #d97706; }
/* The per-list extraction job glyph, in the `job` column next to `ext`:
 * queued/failed take the same two hues the roster's status dot uses for those states
 * (ui/status_indicator._DOT_COLORS), so one job state reads the same colour everywhere. */
.cb-badge-queued { color: #60a5fa; }
.cb-badge-err { color: #dc2626; }
/* That mark opens the job's logs, so the whole cell (and the glyph inside it, which
 * .cb-ltable-badge otherwise gives a help cursor) has to read as clickable. */
.cb-ptable-job, .cb-ptable-job .cb-ltable-badge { cursor: pointer; }
/* Species page · Picks & curation: the rail table's chrome (.cb-ltable*), one grid per
 * row: swatch · name · source · origin · kept/total · ext · job · actions (`job` = the live
 * per-list extraction instance; `origin` = the template+mask the picks came out of).
 * Rows are not selectable here (no detail pane), so no pointer cursor / selected state.
 *
 * Every track is fixed or `fr`, and the last one is fixed rather than `auto`: one table is
 * rendered per tomogram group, and a content-sized track (`minmax(a,b)` / `auto`) lets each
 * group settle on its own column widths, staggering the columns between groups.
 * `fr` depends only on the container width, which every group shares, so the columns line
 * up across groups and the row always spans the full line. */
/* `.cb-ltable`'s `flex: 1 1 auto` sizes nothing here: it is a direct child of a ui.column, and
 * NiceGUI's .nicegui-column sets `align-items: flex-start`, so without an explicit width the box
 * shrinks to fit, the `fr` tracks resolve against max-content, and every tomogram group ends at
 * its own x. */
.cb-ptable { width: 100%; }
/* The `ext` track is 60px to fit the spelled-out `Extracted` header; an abbreviation over
 * a one-glyph column doesn't say what the glyph means. */
.cb-ptable-row {
    grid-template-columns: 14px minmax(0, 2fr) 72px minmax(0, 1.4fr) 56px 60px 24px 96px;
    cursor: default;
}
/* Actions hug the right edge, so a row visibly ends AT the end of the line. */
.cb-ptable-actions { justify-self: end; }
/* The import affordance, one per tomogram group and present whether or not the group holds
 * lists (a tomogram with no picks is exactly where a de-novo .coords import starts). It sits
 * inside the table box, in the rows' own padding/radius, but outside the 8-column grid:
 * it is one action, not a row of data. Dashed and dim so it reads as an empty slot until
 * hovered. */
.cb-ptable-import {
    display: flex; align-items: center; gap: 6px;
    padding: 3px 7px; border-radius: 5px; cursor: pointer;
    border: 1px dashed #e1e7f0; background: transparent;
    font-size: 10px; color: #b6c0cf;
    transition: background 0.12s, border-color 0.12s, color 0.12s;
}
.cb-ptable-import:hover { background: #ffffff; border-color: #c7d2fe; color: #6366f1; }
.cb-ptable-group {
    display: flex; align-items: center; gap: 8px; width: 100%;
    padding: 4px 6px 2px; margin-top: 4px;
}
.cb-ptable-group-name { font-size: 11px; font-weight: 700; color: #334155; font-family: ui-monospace, monospace; }
.cb-ptable-source {
    font-size: 10px; color: #64748b; cursor: help;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
/* Shared small metadata text in the per-list detail chrome (provenance, pick
 * counts, sub-labels) — one slate tone + size instead of ad-hoc inline font
 * styles, so the list header / provenance / status rows read as one block. */
.cb-detail-meta { font-size: 10px; color: #94a3b8; line-height: 1.2; }
/* Rich pytom-chip info tooltip, on the app tooltip's pane (its look lives in the shared rule)
 * with an auto-pick-stats section + a template-match section. */
.cb-chip-tooltip {
    padding: 8px 10px; max-width: 280px; line-height: 1.45;
}
.cb-tt-head { font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: #64748b; }
.cb-tt-line { font-family: 'IBM Plex Mono', monospace; color: #334155; }
.cb-tt-sub { font-size: 10px; color: #94a3b8; }
.cb-tt-sep { margin: 5px 0; background: #e2e8f0; }
/* Per-species admin icons in the Particles panel title bar, following the shown
 * list's species; sits left of the canvas-wide Invert switch with a little breathing room. */
.cb-section-admin { margin-right: 4px; }
.cb-preview-toolbar {
    display: flex; align-items: center; gap: 10px;
    font-size: 10px; color: #475569;
    padding: 2px 0 4px 0;
}
.cb-gallery-scroll { overflow-y: auto; max-height: 68vh; padding-right: 4px; position: relative; }
/* A gallery's count and toolbelt, at the end of the tool row over it. Sort, filters,
 * the 3dmod commands, the hovered-pick info and the filtered-set path are toolbelt
 * icons with popovers. */
.cb-gallery-count { font-size: 10px; color: #94a3b8; font-family: ui-monospace, monospace; }
.cb-toolbelt { flex-shrink: 0; }
.cb-tb-btn { color: #6b829b; }
.cb-tb-btn:hover { color: var(--cb-head-ink, #12324f); }
/* An armed stateful toolbelt toggle (curation mode) is loud: while it is on, clicking
 * a cutout changes what downstream consumes, so the icon must not read like the rest
 * of the row. Dark slate, the house accent. */
.cb-tb-btn.cb-tb-on {
    color: #ffffff !important; background: var(--cb-btn-accent, #1f4e79) !important;
    box-shadow: 0 0 0 2px rgba(74, 143, 212, 0.25);
}
.cb-tb-btn.cb-tb-on:hover { color: #ffffff !important; background: var(--cb-btn-accent-hover, #163a5c) !important; }
/* The curation bar appears under the header only while curation mode is armed. */
.cb-curation-bar {
    padding: 3px 0 5px 0; margin-bottom: 2px;
    border-bottom: 1px dashed #e5e7eb;
}
/* Wider popover for the 3dmod commands — they are long absolute paths. */
.cb-3dmod-card { min-width: 460px; max-width: 70vw; }
/* Narrow popover for the sort / display-filter controls — .cb-info-card's 300px floor
 * is sized for path rows, and a lone select rattling around in it reads as broken. */
.cb-tb-card { min-width: auto; padding: 6px 8px; }
/* The full-page viewer: fills the workspace main area and scrolls as one. */
.cb-viewer-page {
    display: flex; flex-direction: column; gap: 6px;
    width: 100%; height: 100%; min-height: 0;
    overflow-y: auto; padding: 8px 10px;
}
.cb-viewer-page-head { padding: 0 10px 4px 2px; }
/* Slab lightbox: a full-viewport overlay holding a CLONE of the clicked slab (image +
 * its pick dots), wheel-zoomed and drag-panned via an inline transform. */
.cb-slab-lightbox {
    position: fixed; inset: 0; z-index: 3000;
    background: rgba(2,6,23,0.92);
    display: flex; align-items: center; justify-content: center;
    overflow: hidden; cursor: grab;
}
.cb-slab-lightbox:active { cursor: grabbing; }
.cb-slab-lightbox-inner {
    width: min(96vw, 1800px) !important;
    max-height: 94vh;
    transform-origin: center center;
    will-change: transform;
}
/* Box-select (lasso) marquee + active state. */
/* While a box-select drag is in progress: crosshair + suppress text selection.
   Tiles keep pointer-events (clicks must work); the post-drag synthetic click is
   swallowed in JS instead. */
.cb-lasso-dragging, .cb-lasso-dragging .cb-gallery-tile { cursor: crosshair; }
.cb-lasso-dragging { user-select: none; }
.cb-lasso-rect {
    position: absolute; z-index: 6; pointer-events: none;
    border: 1px dashed #4f46e5; background: rgba(79,70,229,0.12);
}
.cb-failures-list {
    font-size: 10px; color: #6b7280;
    font-family: ui-monospace, monospace;
    max-height: 90px; overflow-y: auto;
    background: #f8fafc; border: 1px solid #e5e7eb;
    border-radius: 3px; padding: 6px 8px;
}
.cb-failures-list .cb-failure-row {
    display: flex; gap: 8px; padding: 1px 0;
    border-bottom: 1px dashed #e5e7eb;
}
.cb-failures-list .cb-failure-row:last-child { border-bottom: none; }
.cb-failures-list .cb-failure-i { color: #ef4444; min-width: 32px; }
.cb-instance-toolbar {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: #475569;
    padding: 2px 0 4px 0; flex-wrap: wrap;
}
.cb-datadump-grid {
    display: grid; grid-template-columns: max-content 1fr;
    gap: 2px 14px; font-family: ui-monospace, monospace;
    font-size: 11px; padding: 2px 0;
}
.cb-datadump-key {
    color: #6b7280; text-transform: uppercase;
    font-size: 9px; font-weight: 600; letter-spacing: 0.3px;
    align-self: baseline;
}
.cb-datadump-val { color: #1f2937; align-self: baseline; word-break: break-all; }
.cb-section-placeholder {
    font-size: 10px; color: #9ca3af; font-style: italic;
    padding: 4px 0 2px 0;
}
/* The Journey's chart tiles share their row's width and wrap once a tile would get narrower
 * than --cb-plot-min, so the charts always fill the pane; the chart is --cb-plot-h tall (both
 * set on .cb-journey). No border: the tiles sit straight on the section. */
.cb-plot-row { display: flex; flex-wrap: wrap; gap: 12px; margin: 0 0 6px 0; }
.cb-plot-cell {
    flex: 1 1 var(--cb-plot-min, 340px); min-width: 0;
    background: #ffffff; border-radius: 4px; padding: 6px 6px 6px;
}
/* One short regular-weight title per chart (the explainer tooltip hangs on it);
 * the only weighted text in a tile is the axis names. */
.cb-plot-label {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; color: #64748b;
    font-weight: 400; padding: 0 2px 2px; cursor: help;
}
/* Summary statistics as tiles — a small label over a mono value, generous gaps —
 * never a `key val · key val` line crammed into 20 px. */
.cb-stats { display: flex; gap: 26px; flex-wrap: wrap; margin: 0 2px 14px; }
.cb-stat { display: flex; flex-direction: column; gap: 1px; }
.cb-stat-label { font-family: 'IBM Plex Sans', sans-serif; font-size: 9px; color: #94a3b8; white-space: nowrap; }
.cb-stat-value { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: #1e293b; white-space: nowrap; }
/* The Dataset title carries its headline facts inline (it is collapsed by default). */
.cb-stats-inline { margin: 0 0 0 6px; gap: 16px; }
.cb-stats-inline .cb-stat-value { font-size: 10px; color: #475569; }
/* A collapsed section's title line keeps no air below it. */
.cb-section-card-header:has(+ .cb-collapsed) { margin-bottom: 0; }
.cb-drop-list {
    font-family: ui-monospace, monospace; font-size: 10px; color: #6b7280;
    background: #fef3c7; border: 1px solid #fde68a; border-radius: 3px;
    padding: 6px 8px; margin: 4px 0; max-height: 120px; overflow-y: auto;
}
.cb-drop-list .cb-drop-row {
    display: flex; gap: 8px; padding: 1px 0; border-bottom: 1px dashed #fde68a;
}
.cb-drop-list .cb-drop-row:last-child { border-bottom: none; }
.cb-drop-list .cb-drop-tilt { color: #b45309; min-width: 60px; }
.cb-datadump-grid-2col {
    display: grid; grid-template-columns: max-content 1fr max-content 1fr;
    gap: 2px 12px; font-family: ui-monospace, monospace;
    font-size: 11px; padding: 2px 0;
}
.cb-pixel-section-title {
    display: flex; align-items: center; gap: 5px; padding: 8px 0 2px 0;
    font-size: 11px; color: #475569; font-weight: 600;
    border-top: 1px solid #f1f5f9; margin-top: 6px;
}
/* Wrapper allows horizontal scroll on narrow viewports without breaking
 * column alignment. The table itself is one CSS Grid so universal +
 * per-species rows share column widths automatically. */
.cb-pixel-table-wrapper {
    overflow-x: auto;
    padding-bottom: 4px;
}
.cb-pixel-table {
    display: grid;
    grid-template-columns:
        minmax(170px, max-content)
        minmax(60px, max-content)
        minmax(120px, max-content)
        minmax(160px, max-content)
        minmax(150px, max-content)
        minmax(120px, max-content)
        minmax(90px, max-content)
        minmax(180px, 1fr);
    column-gap: 28px;
    font-family: ui-monospace, monospace;
    font-size: 11px;
    padding: 2px 0;
    min-width: max-content;  /* lets the grid grow past the wrapper for x-scroll */
}
.cb-pixel-cell {
    display: flex; align-items: center; gap: 5px;
    padding: 4px 2px;
    color: #1f2937;
    border-bottom: 1px dashed #f1f5f9;
    white-space: nowrap;
}
.cb-pixel-cell.cb-pixel-header {
    color: #6b7280; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
    border-bottom: 1px solid #e2e8f0;
}
.cb-pixel-cell.cb-pixel-warn-error { background: #fef2f2; color: #b91c1c; }
.cb-pixel-cell.cb-pixel-warn-warn  { background: #fff7ed; color: #b45309; }
.cb-pixel-cell.cb-pixel-warn-info  { color: #475569; }
.cb-pixel-cell.cb-pixel-notes { white-space: normal; color: #475569; font-size: 10px; }
.cb-pixel-stage-label    { color: #1e293b; font-weight: 600; }
.cb-pixel-instance-label { color: #94a3b8; font-size: 9px; }
.cb-pixel-warn-icon      { cursor: help; }
/* Species marker = a single full-width row inside the same grid; keeps
 * column alignment perfect. */
.cb-pixel-species-row {
    grid-column: 1 / -1;
    display: flex; align-items: center; gap: 8px;
    padding: 8px 2px 4px 2px; margin-top: 6px;
    border-top: 1px dashed #cbd5e1;
    font-family: ui-monospace, monospace;
    background: #f8fafc;
}
.cb-pixel-species-name {
    color: #334155; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; font-size: 11px;
}
.cb-pixel-species-id {
    color: #94a3b8; font-size: 9px; font-weight: 400;
}
/* Inline status chips used in section-card headers and chip strips. */
.cb-chip-strip {
    display: flex; gap: 6px; flex-wrap: wrap;
    padding: 4px 0 6px 0;
}
.cb-chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 2px 8px; border-radius: 999px;
    font-family: ui-monospace, monospace; font-size: 10px;
    border: 1px solid #dbe3ec; background: #f4f8fc;
    color: #33506e; cursor: help; line-height: 1.4;
    white-space: nowrap;
}
.cb-chip .cb-chip-label {
    color: #7d93ab; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
}
.cb-chip .cb-chip-value { color: #12324f; font-weight: 600; }
.cb-chip-ok      { background: #ecfdf5; border-color: #a7f3d0; }
.cb-chip-ok      .cb-chip-value { color: #047857; }
.cb-chip-warn    { background: #fffbeb; border-color: #fcd34d; }
.cb-chip-warn    .cb-chip-value { color: #b45309; }
.cb-chip-error   { background: #fef2f2; border-color: #fecaca; }
.cb-chip-error   .cb-chip-value { color: #b91c1c; }
.cb-chip-info    { background: #eff6ff; border-color: #bfdbfe; }
.cb-chip-info    .cb-chip-value { color: #1d4ed8; }
.cb-chip-neutral { background: #eef3f8; }
.cb-chip-icon    { font-size: 11px !important; }
/* Recon-section large canvas: viewport-filling WarpTools PNG preview. */
.cb-recon-preview {
    width: 100%;
    background: #0f172a;
    border-radius: 6px;
    overflow: hidden;
    margin: 8px 0 4px 0;
    display: flex; justify-content: center; align-items: center;
}
.cb-recon-preview img {
    width: 100%;
    max-height: 75vh;
    object-fit: contain;
    display: block;
}
.cb-recon-preview-caption {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #6b7280; padding: 2px 4px 6px 4px;
}
/* Side-by-side recon | denoised comparison in the Reconstruct section. Two equal
 * panes, each hosting a .cb-recon-preview canvas. Panes shrink (min-width:0) so the
 * contained images scale down instead of overflowing. When denoise hasn't run, only
 * the recon pane is emitted and it takes the full width. */
.cb-recon-compare {
    display: flex; gap: 8px; width: 100%; align-items: flex-start;
}
.cb-recon-compare-pane {
    flex: 1 1 0; min-width: 0; display: flex; flex-direction: column;
}
.cb-recon-pane-tag {
    font-family: ui-monospace, monospace; font-size: 10px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.04em;
    color: #475569; padding: 0 2px 2px 2px;
}
/* The denoised pane's method: a segmented switch (cryoCARE | IsoNet | …) when >1 method
 * has a result for the TS; a single available method renders as this static label. */
.cb-recon-pane-method {
    font-family: ui-monospace, monospace; font-size: 10px; font-weight: 600;
    color: #475569; padding: 0 2px 2px 2px;
}
/* ----------------------------------------------------------------------
 * Polarity invert: a runtime toggle in the section header flips the
 * apparent intensity of the X/Y slab, X/Z slab, and cutout tiles
 * together — same density convention across all three. Double-invert
 * trick on tile children keeps overlay labels readable.
 * ---------------------------------------------------------------------- */
.cb-invert-polarity .cb-tomo-preview > img,
.cb-invert-polarity .cb-gallery-tile {
    filter: invert(1) hue-rotate(180deg);
}
.cb-invert-polarity .cb-gallery-tile > * {
    filter: invert(1) hue-rotate(180deg);
}
/* Shared recon canvas: `ui.image` renders a q-img that nests the <img> a
 * couple levels down, so the `> img` direct-child rule above can miss it.
 * Use a descendant combinator scoped to the canvas so Invert reliably
 * flips the slab. Same filter value → no double-inversion where both match.
 * Pick-dot layers are excluded, so dots keep their species colors. */
.cb-invert-polarity .cb-recon-canvas img {
    filter: invert(1) hue-rotate(180deg);
}
/* Ghost dots and the pick marker overlay sit on top of the X/Y / X/Z
 * preview img. They aren't part of the inverted set — keep them at
 * their declared color so they don't flicker between hues when toggled. */

/* ----------------------------------------------------------------------
 * Ghost dot restyle: smaller, brighter (cyan), white ring; pointer-events
 * enabled so they're individually hoverable (drives reverse highlight of
 * the matching gallery tile via the JS bridge in _render_gallery_body).
 * ---------------------------------------------------------------------- */
.cb-pick-ghost {
    pointer-events: auto !important;
    cursor: pointer;
    width: 3px !important; height: 3px !important;
    background: var(--sp-color, #00e5ff) !important;
    /* The tomogram gallery's dot (ui/tomo_gallery.py _dots_svg): species color with a hairline
     * white edge, so the picks mark the slab without hiding what they sit on. */
    box-shadow: 0 0 0 0.5px rgba(255,255,255,0.8) !important;
    transition: box-shadow 0.1s ease, width 0.1s ease, height 0.1s ease, filter 0.1s ease;
}
/* Invisible hit-area so the tiny dot is easy to hover AND click (click toggles
 * keep/drop). Inherits pointer-events:auto from the dot. */
.cb-pick-ghost::after { content: ''; position: absolute; inset: -5px; }
/* Active / hover: grow + a bright colored backlight glow so the brushed pick
 * reads at a glance on a busy slab. The clip-path triangle clips box-shadow away,
 * so it gets a drop-shadow instead (below). */
.cb-pick-ghost:hover,
.cb-pick-ghost.cb-ghost-active {
    width: 8px !important; height: 8px !important;
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 2px rgba(255,255,255,1),
                0 0 11px 4px var(--sp-color, #00e5ff) !important;
    z-index: 8;
}
/* Selected pick: a persistent amber ring that survives the pointer leaving,
 * so the dot you clicked stays findable on the slab while you read its tile. Distinct
 * from the transient .cb-ghost-active hover glow, and it wins over it (later rule). */
.cb-pick-ghost.cb-ghost-selected {
    width: 9px !important; height: 9px !important;
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 3px #f59e0b,
                0 0 12px 3px rgba(245,158,11,0.75) !important;
    z-index: 9;
}
/* Dropped/excluded pick: grey the dot so the slab agrees with the gallery
 * (filtered cutouts → greyed dots). Overrides the species color + glow. */
.cb-pick-ghost.cb-pick-ghost-dropped {
    background: #9ca3af !important;
    box-shadow: 0 0 0 1px rgba(0,0,0,0.55), 0 0 0 2px rgba(255,255,255,0.35) !important;
    opacity: 0.5;
}
/* Per-list glyph (set on the layer via .cb-shape-*, cascades to its dots).
 * Color stays the primary distinguisher; shape is secondary reinforcement.
 * Dots are 3px with a hairline edge, so shapes stay
 * simple (triangle trades the ring for a filled glyph — used only by merged). */
.cb-shape-circle .cb-pick-ghost { border-radius: 50%; }
.cb-shape-square .cb-pick-ghost { border-radius: 0; }
.cb-shape-diamond .cb-pick-ghost { border-radius: 0; transform: translate(-50%, -50%) rotate(45deg); }
/* Triangle (merged lists): clip-path clips box-shadow, so the hairline
 * edge is invisible. Give it a little more area and a drop-shadow outline (filter follows
 * the clipped shape), so a merged pick reads on the slab. */
.cb-shape-triangle .cb-pick-ghost {
    border-radius: 0; clip-path: polygon(50% 0, 0 100%, 100% 100%);
    width: 5px !important; height: 5px !important;
    box-shadow: none !important;
    filter: drop-shadow(0 0 0.5px rgba(0,0,0,0.95)) drop-shadow(0 0 1.2px rgba(255,255,255,0.9));
}
/* Triangle brush/hover: grow + an orange drop-shadow halo (box-shadow is clipped,
 * so the generic glow above can't reach it). */
.cb-shape-triangle .cb-pick-ghost:hover,
.cb-shape-triangle .cb-pick-ghost.cb-ghost-active {
    width: 14px !important; height: 14px !important;
    box-shadow: none !important;
    filter: drop-shadow(0 0 1.5px rgba(255,255,255,1))
            drop-shadow(0 0 5px var(--sp-color, #ff6d00))
            drop-shadow(0 0 9px var(--sp-color, #ff6d00));
}

/* ----------------------------------------------------------------------
 * Filter UX: per-tile keep/drop state. Default is keep (no extra class);
 * dropped tiles dim + show diagonal strikethrough so the user can scan a
 * sorted grid and see which were vetoed without losing their position.
 * ---------------------------------------------------------------------- */
.cb-gallery-tile.cb-tile-dropped {
    border-color: #ef4444 !important;
    box-shadow: 0 0 0 1px #ef4444 !important;
}
.cb-gallery-tile.cb-tile-dropped::after {
    content: '';
    position: absolute; inset: 0;
    pointer-events: none;
    background: repeating-linear-gradient(
        135deg,
        rgba(239,68,68,0.0) 0 6px,
        rgba(239,68,68,0.55) 6px 7px
    );
}
/* Degenerate tiles: candidates that produced no cutout (no subtomo match /
 * render failed), shown at the end of the grid for count↔index transparency.
 * Flat slate placeholder, not interactive; the ✕ + index + reason tooltip make
 * it obvious these aren't pickable. When the exclude checkbox is on they read
 * as struck-through (excluded from the saved set). */
.cb-gallery-tile.cb-tile-degenerate {
    background: #1e293b; cursor: default; opacity: 0.7;
    border-color: #334155 !important; box-shadow: none !important;
    display: flex; align-items: center; justify-content: center;
}
.cb-gallery-tile.cb-tile-degenerate .cb-tile-degen-mark {
    font-size: 22px; color: #64748b; line-height: 1;
}
.cb-degen-toggle .q-checkbox__label { font-size: 10px; color: #64748b; }
/* Reverse-hover highlight: when the user mouses a ghost dot in the preview,
 * the matching gallery tile gets this transient ring (distinct from the
 * persistent .selected state so the two don't collide visually). */
.cb-gallery-tile.cb-tile-highlight {
    outline: 2px solid #22d3ee;
    outline-offset: 1px;
    box-shadow: 0 0 0 1px #22d3ee, 0 0 8px rgba(34,211,238,0.6);
}
/* The kept-counter chip, in the gallery's curation bar. */
.cb-filter-counter {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #33506e;
    padding: 1px 6px; border-radius: 3px;
    background: #f4f8fc; border: 1px solid #dbe3ec;
}
.cb-filter-counter.cb-filter-dirty {
    color: #b45309; background: #fef3c7; border-color: #fde68a;
}

/* ── A switcher header line: the Protocols view's (ui/protocols_view.py), and the Journey's when it
   is mounted outside a view frame ───────────────────────────────────────────────
   One thin line: [▾ name · version] and what goes with it; the name block drops down the switcher
   table. When the pane is too narrow for one line, its right part wraps onto a second one. */
.cb-jhead {
    flex: 0 0 auto; display: flex; align-items: center; flex-wrap: wrap; gap: 4px 14px; min-height: 30px;
    padding: 5px 12px 5px 14px;
    font-family: 'IBM Plex Sans', sans-serif; container: cb-jhead / inline-size;
}
.cb-jhead-left { flex: 1 1 auto; min-width: 0; display: flex; align-items: center; gap: 4px 10px; flex-wrap: wrap; }
.cb-jhead-right {
    flex: 0 1 auto; min-width: 0; display: flex; align-items: center; flex-wrap: wrap; row-gap: 4px;
    margin-left: auto;
}
.cb-jhead-empty { font-size: 10px; color: #94a3b8; }
.cb-jsep { width: 1px; height: 14px; background: var(--cb-panel-line, #dbe3ec); flex: 0 0 auto; }
.cb-jswitch {
    display: flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none;
    padding: 3px 6px; margin-left: -6px; border-radius: 4px;
}
.cb-jswitch:hover { background: var(--cb-hover, #e6eff8); }
.cb-jswitch-caret { font-size: 9px; color: #94a3b8; }
.cb-jswitch-pos { font-size: 12px; font-weight: 600; color: var(--cb-head-ink, #12324f); white-space: nowrap; }
.cb-jswitch-pos.excluded { text-decoration: line-through; color: #94a3b8; }
.cb-jswitch-idx { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #64748b; white-space: nowrap; }
/* A species' pick count in the Journey's switcher (ui/dashboard/strip.py), and what curation kept. */
.cb-jsp-dot { width: 7px; height: 7px; border-radius: 50%; flex: 0 0 auto; }
.cb-jsp {
    display: inline-flex; align-items: baseline; gap: 5px; padding: 0 4px;
    border-radius: 3px; white-space: nowrap;
}
.cb-jsp-n { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: #1e293b; }
.cb-jsp-kept { font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #059669; }
.cb-jsp.running { background: rgba(59, 130, 246, 0.14); }
.cb-jsp.running .cb-jsp-n { color: #1e40af; }
.cb-jsp.fail { background: rgba(239, 68, 68, 0.12); }
.cb-jsp.fail .cb-jsp-n { color: #b91c1c; }
.cb-jsp.zero .cb-jsp-n { color: #94a3b8; }
.cb-jsp.pending .cb-jsp-n { color: #cbd5e1; }
/* The Journey's exclude switch, on its title line. */
.cb-jexcl { font-size: 14px; color: #cbd5e1; cursor: pointer; }
.cb-jexcl:hover { color: #dc2626; }
.cb-jexcl.on { color: #b45309; }
/* The switcher dropdown: a full table — as wide and tall as it needs to be (the menu caps it at
   the viewport), one row per entry. */
.cb-jmenu {
    display: flex; flex-direction: column; min-width: 480px;
    padding: 4px 0; background: #ffffff; font-family: 'IBM Plex Sans', sans-serif;
}
.cb-jrow {
    display: grid; grid-template-columns: var(--cb-jcols); align-items: center; column-gap: 8px;
    padding: 4px 12px; cursor: pointer; border-bottom: 1px solid #f8fafc;
}
.cb-jrow:hover { background: #f8fafc; }
.cb-jrow.selected { background: #eef2ff; box-shadow: inset 2px 0 0 #6366f1; }
.cb-jrow-head, .cb-jrow-total { cursor: default; }
.cb-jrow-head:hover, .cb-jrow-total:hover { background: transparent; }
.cb-jrow-head { border-bottom: 1px solid #e5e7eb; padding-bottom: 5px; }
.cb-jrow-total { border-top: 1px solid #e5e7eb; border-bottom: none; margin-top: 2px; padding-top: 6px; }
.cb-jh { font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: #94a3b8; }
.cb-jcell { min-width: 0; }
.cb-jcell-idx { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #94a3b8; }
.cb-jcell-name { display: flex; flex-direction: column; gap: 0; }
.cb-jname { font-size: 11px; font-weight: 600; color: #1e293b; white-space: nowrap; }
.cb-jname-full {
    font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #94a3b8;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
/* ── The Journey's tilt series on its title line (ui/dashboard/strip.py) ─────────
 * After the view's name: the tilt series' position, spelled out, whose click drops the switcher
 * down, and its place among them. The switcher: a search field over one line per tilt series,
 * grouped by stage position, with every species' pick count; the totals at its foot. */
.cb-ts-sep { color: #9fb2c8; margin: 0 -2px; }
.cb-ts-switch {
    display: flex; align-items: center; gap: 1px; min-width: 0; padding: 1px 2px 1px 6px; border-radius: 4px;
    cursor: pointer; user-select: none;
}
.cb-ts-switch:hover { background: var(--cb-hover, #e6eff8); }
.cb-ts-name {
    font-size: 11px; font-weight: 600; color: #24405e; min-width: 0;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cb-ts-name.excluded { text-decoration: line-through; color: #94a3b8; }
.cb-ts-caret { color: #8aa0b8; flex: 0 0 auto; }
.cb-ts-idx { font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #8aa0b8; white-space: nowrap; }
.cb-ts-empty { font-size: 10px; color: #94a3b8; white-space: nowrap; }
.cb-ts-menu {
    display: flex; flex-direction: column; min-width: 380px; padding: 0 0 4px;
    font-family: 'IBM Plex Sans', sans-serif; background: #ffffff;
}
.cb-ts-search {
    position: sticky; top: 0; z-index: 1; display: flex; align-items: center; gap: 6px;
    padding: 6px 12px; background: #ffffff; border-bottom: 1px solid #eef2f6; color: #8aa0b8;
}
.cb-ts-search-input {
    flex: 1 1 auto; min-width: 0; height: 20px; box-sizing: border-box; padding: 0 6px; outline: none;
    border: 1px solid var(--cb-field-line, #d3dde8); border-radius: var(--cb-field-radius, 4px); background: #ffffff;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; color: #1e293b;
}
.cb-ts-search-input:focus { border-color: var(--cb-field-focus, #4a8fd4); box-shadow: var(--cb-field-ring); }
.cb-ts-row {
    display: grid; grid-template-columns: var(--cb-ts-cols); align-items: center; column-gap: 8px;
    min-height: 20px; padding: 1px 12px 1px 22px; cursor: pointer;
}
.cb-ts-row:hover { background: var(--cb-hover, #e6eff8); }
.cb-ts-row.selected { background: var(--cb-head, #edf4fb); box-shadow: inset 2px 0 0 var(--cb-accent, #4a8fd4); }
.cb-ts-row.excluded :is(.cb-ts-beam, .cb-ts-full) { text-decoration: line-through; color: #94a3b8; }
.cb-ts-row.excluded .cb-jsp { opacity: 0.35; }
.cb-ts-head, .cb-ts-total { cursor: default; }
.cb-ts-head:hover, .cb-ts-total:hover { background: transparent; }
.cb-ts-head { padding-top: 5px; }
.cb-ts-total { border-top: 1px solid #eef2f6; margin-top: 4px; padding-top: 4px; }
.cb-ts-group-head { font-size: 10px; font-weight: 600; color: #33506e; padding: 6px 12px 1px; }
.cb-ts-beam { font-size: 11px; color: #1e293b; white-space: nowrap; }
.cb-ts-wide { grid-column: 1 / span 2; overflow: hidden; text-overflow: ellipsis; }
.cb-ts-full {
    font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #94a3b8; min-width: 0;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cb-ts-sp { display: flex; align-items: center; gap: 5px; min-width: 0; }
.cb-ts-sp-name {
    font-size: 10px; font-weight: 600; color: #475569; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-ts-total-name { font-size: 10px; color: #64748b; white-space: nowrap; }
/* ── Species page ──────────────────────────────────────────────────────────── */
/* Segmented control (`ui/components/segmented.py`): the job tab's flat dense
 * switcher as one bordered strip; the active segment is tinted, never a Quasar tab. */
.cb-seg {
    display: inline-flex; align-items: stretch; flex-shrink: 0;
    border: 1px solid var(--cb-field-line, #d3dde8); border-radius: var(--cb-field-radius, 4px); overflow: hidden;
    background: #ffffff; box-shadow: 0 1px 1px rgba(16, 42, 67, 0.04);
}
.cb-seg-btn {
    font-size: 9px; font-weight: 500; line-height: 16px; padding: 2px 8px;
    color: #4f6782; background: #ffffff; cursor: pointer; user-select: none;
    border-right: 1px solid #e3eaf2; white-space: nowrap;
}
.cb-seg-btn { display: inline-flex; align-items: baseline; gap: 5px; }
.cb-seg-btn:last-child { border-right: none; }
.cb-seg-badge { font-size: 10px; font-variant-numeric: tabular-nums; color: #8aa0b8; }
.cb-seg-btn.active .cb-seg-badge { color: #4a6584; }
.cb-seg-btn:hover { background: #f4f8fc; color: var(--cb-head-ink, #12324f); }
/* The active segment: the title stripes' light blue with their blue tick, under it. */
.cb-seg-btn.active {
    background: var(--cb-head, #edf4fb); color: var(--cb-head-ink, #12324f); font-weight: 600;
    box-shadow: inset 0 -2px 0 var(--cb-accent, #4a8fd4);
}
/* Small strip: as tall as a 16 px `.cb-field`, for control rows (projects roster). */
.cb-seg-sm .cb-seg-btn { line-height: 14px; padding: 0 6px; }
/* Projects roster travel chevron (ui/projects_overview.py): a full-row-height tinted
 * column on the right edge, the only control that enters a project. It lights on its own
 * hover only, never the row's, so the row and "enter" read as two different targets. */
.cb-proj-chevron:hover { background: #dbeafe !important; }
.cb-proj-chevron:hover .q-icon { color: #2563eb !important; }
/* A project's trash size in the roster: it opens the trash dialog, so it says so on hover. */
.cb-proj-trash:hover { color: #dc2626 !important; }
/* The roster's compact columns give way on a narrow roster, tier 1 first, so the status and the
 * chevron never leave the row (ui/projects_overview.py `_COMPACT_COLUMNS`). Container queries:
 * the roster is narrower in the project hub than on the landing page at the same window size. */
.cb-proj-roster { container-type: inline-size; }
@container (max-width: 1480px) { .cb-pc-t1 { display: none !important; } }
@container (max-width: 1220px) { .cb-pc-t2 { display: none !important; } }
@container (max-width: 940px) { .cb-pc-t3 { display: none !important; } }
/* The landing page's view switch stands in for its card's title, so it is a notch bigger. */
.cb-seg-title .cb-seg-btn { font-size: 11px; line-height: 18px; padding: 2px 10px; }
/* Click-to-copy text (ui/components/copyable.copy_on_click): the path itself is the
 * control, so it has to say so on hover. */
.cb-copy-path { cursor: pointer; }
.cb-copy-path:hover { color: #2563eb !important; text-decoration: underline; }
/* A live project's status dot in the roster: a ring that ripples out, so a running
 * project is findable in a long list without anything else on the row being loud. */
@keyframes cb-live-pulse {
    0%   { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.6); }
    70%  { box-shadow: 0 0 0 5px rgba(59, 130, 246, 0); }
    100% { box-shadow: 0 0 0 0 rgba(59, 130, 246, 0); }
}
.cb-live-pulse { animation: cb-live-pulse 1.6s ease-out infinite; }
/* Projects roster deletion mode (ui/projects_overview.py): the armed roster breathes a
 * red outline; under the pointer a row tints red and its trash column turns solid. */
@keyframes cb-delmode-breathe {
    0%, 100% { box-shadow: 0 0 0 2px rgba(220, 38, 38, 0.12); }
    50%      { box-shadow: 0 0 0 4px rgba(220, 38, 38, 0.28); }
}
.cb-proj-delmode { animation: cb-delmode-breathe 2.4s ease-in-out infinite; }
.cb-proj-delrow { transition: background-color 0.12s; }
.cb-proj-delrow:hover { background: #fef2f2; }
.cb-proj-deltrash { color: #fca5a5; background: #fffafa; transition: color 0.12s, background-color 0.12s; }
.cb-proj-delrow:hover .cb-proj-deltrash { color: #dc2626; background: #fee2e2; }
/* A project's hover card (ui/projects_overview.py `_render_hover_card`): a caret on the edge
 * facing its row, aimed at the project's name; the open handler sets data-side and --cb-caret-x. */
.cb-proj-card::before {
    content: ''; position: absolute; top: -5px; left: var(--cb-caret-x, 24px);
    width: 9px; height: 9px; background: rgba(120, 120, 120, 0.12); backdrop-filter: blur(1.5px);
    border-left: 1px solid rgba(75, 85, 99, 0.4); border-top: 1px solid rgba(75, 85, 99, 0.4);
    /* Only the half outside the card: the half over it would show the card's edge through the glass. */
    clip-path: polygon(0 0, 100% 0, 0 100%);
    transform: translateX(-50%) rotate(45deg);
}
.cb-proj-card[data-side="above"]::before { top: auto; bottom: -5px; transform: translateX(-50%) rotate(225deg); }
/* The rail avatar's card (`project_card_menu`) opens to the avatar's right: the caret on its left
 * edge, at the 34 px avatar's middle. */
.cb-proj-card.cb-proj-card--right::before { top: 17px; left: -5px; transform: translateY(-50%) rotate(-45deg); }
/* Every tooltip in the app, Quasar's and the native `title` ones (drawn by _TITLE_TIP_JS as
 * .cb-title-tip): see-through, the faintest neutral gray tint and a hairline outline; a touch of blur
 * so the text separates from what it covers. Never wider than about a path and a half; long
 * paths wrap. A Quasar tooltip that sets its own max-width inline keeps it. */
.q-tooltip.q-tooltip--style, .cb-title-tip, .cb-glass {
    background: rgba(120, 120, 120, 0.12); backdrop-filter: blur(1.5px); color: #020617;
    border: 1px solid rgba(75, 85, 99, 0.4); border-radius: 5px; box-shadow: none;
}
.q-tooltip.q-tooltip--style, .cb-title-tip {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px;
    max-width: min(120ch, 92vw); overflow-wrap: anywhere; line-height: 1.4;
}
/* The same glass on the hover popups that are not tooltips (.cb-glass: the projects roster's
 * card, the landing status strip's popovers, the tilt popover) and on the charts' hover boxes:
 * ECharts' through ui/dashboard/figures.py `_tooltip`, Plotly's here (SVG takes no blur, so a
 * little more tint). Secondary text inside them is a step darker than on the page, or it
 * disappears into whatever shows through. */
.js-plotly-plot .hoverlayer .hovertext path {
    fill: rgba(241, 245, 249, 0.82) !important; stroke: rgba(75, 85, 99, 0.4) !important;
}
.js-plotly-plot .hoverlayer .hovertext text { fill: #020617 !important; }
:is(.q-tooltip--style, .cb-glass) .cb-params-k,
:is(.q-tooltip--style, .cb-glass) .cb-tt-sub { color: #475569; }
:is(.q-tooltip--style, .cb-glass) .cb-params-v,
:is(.q-tooltip--style, .cb-glass) .cb-tt-line { color: #020617; }
:is(.q-tooltip--style, .cb-glass) .cb-tt-head { color: #334155; }
:is(.q-tooltip--style, .cb-glass) .cb-tt-sep { background: rgba(75, 85, 99, 0.3); }
.cb-title-tip {
    position: fixed; z-index: 9500; pointer-events: none; padding: 6px 10px;
    white-space: pre-line; display: none;
}
/* Scroll area that must never scroll sideways (projects roster): content is pinned to
 * the viewport width, so an over-wide child clips instead of growing a horizontal pan. */
.cb-scroll-noh .q-scrollarea__container { overflow-x: hidden !important; }
/* No NiceGUI content padding: the rows run edge to edge, under the column titles above them. */
.cb-scroll-noh .q-scrollarea__content { width: 100%; max-width: 100%; padding: 0; gap: 0; }
/* The roster's compact column titles: each sorts its column on a click; the sorted one is darker. */
.cb-proj-colhead {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 9px; font-weight: 600; color: #7d93ab;
    white-space: nowrap; cursor: pointer; user-select: none;
}
.cb-proj-colhead:hover { color: #33506e; }
.cb-proj-colhead.active { color: var(--cb-head-ink, #12324f); }
/* Template workbench lists: table rows, so the per-row facts (apix / box / lp, sigma
 * stats, size, polarity) sit in columns sized to their content. Only the file column flexes.
 * The accent is a custom property set per list on the container (indigo for templates,
 * purple for masks), so one rule serves both. Selection is a class flip, never a
 * rebuild of the list the user just clicked. */
.cb-tw-table { width: 100%; min-width: 0; overflow-x: auto; }
.cb-tw-head, .cb-tw-row {
    display: grid; align-items: center; column-gap: 6px;
    grid-template-columns: var(--cb-tw-cols);
}
.cb-tw-head { padding: 1px 5px 2px 5px; border-bottom: 1px solid #e5e7eb; }
.cb-tw-row {
    padding: 1px 5px; border-bottom: 1px solid #f1f5f9; cursor: pointer; background: #ffffff;
    border-left: 2px solid transparent; min-height: 22px;
}
.cb-tw-row:last-child { border-bottom: none; }
.cb-tw-row:hover { background: #f8fafc; }
.cb-tw-row.selected { background: var(--cb-accent-tint); border-left-color: var(--cb-accent); }
.cb-tw-cell { min-width: 0; display: flex; align-items: center; gap: 3px; }
.cb-tw-cell > * { min-width: 0; }
/* Tight leading everywhere in the table: at 9-10 px the default 1.5 was adding more
 * height per row than the text itself. */
.cb-tw-head > *, .cb-tw-row .cb-tw-cell { line-height: 1.25; }
/* Row action buttons: kept small so the eye and the X don't set the row's height. */
.cb-tw-row .q-btn { min-height: 16px; min-width: 16px; padding: 0 2px; }
.cb-tw-row .q-btn .q-icon { font-size: 13px; }
/* Both radios are rendered; the row's `selected` class decides which one shows, so a
 * selection change costs one class attribute and no server render. */
.cb-tw-sel-on { display: none; color: var(--cb-accent); }
.cb-tw-sel-off { color: #cbd5e1; }
.cb-tw-row.selected .cb-tw-sel-on { display: inline-flex; }
.cb-tw-row.selected .cb-tw-sel-off { display: none; }
/* By-path import at the foot of BOTH lists — the same dim dashed affordance the Picks &
 * curation table ends with (.cb-ptable-import), in this table's tighter scale. It is a
 * single action, so it sits outside the grid the rows share; `--cb-accent` is the table's
 * own (indigo for templates, purple for masks), so each column's row hovers to its colour. */
.cb-tw-import {
    display: flex; align-items: center; gap: 5px;
    margin: 3px 5px; padding: 2px 5px; min-height: 18px;
    border: 1px dashed #e5e7eb; border-radius: 4px; cursor: pointer;
    font-size: 9px; color: #b6c0cf;
    transition: background 0.12s, border-color 0.12s, color 0.12s;
}
.cb-tw-import:hover { background: #f8fafc; border-color: var(--cb-accent); color: var(--cb-accent); }
/* Species header row: active pill left, segmented tabs right. It sits under the view's title line
 * with no rule between them. */
.cb-species-header {
    display: flex; align-items: center; gap: 10px; flex-shrink: 0;
    padding: 5px 10px 5px 14px; min-height: 26px;
}
/* Species rail: 200 px column, one row per species (mark + counts), "+" at the
 * bottom. Selected row = tinted + an indigo left border. */
.cb-srail {
    width: 200px; min-width: 200px; flex-shrink: 0; height: 100%; overflow-y: auto; overflow-x: hidden;
    display: flex; flex-direction: column; border-right: 1px solid var(--cb-panel-line, #dbe3ec);
    background: var(--cb-rail, #eef3f8);
}
.cb-srail-row {
    display: flex; align-items: center; gap: 6px; padding: 6px 8px 6px 7px;
    border-bottom: 1px solid #e3eaf2; border-left: 3px solid transparent; cursor: pointer;
    min-width: 0;
}
.cb-srail-row:hover { background: var(--cb-hover, #e6eff8); }
.cb-srail-row.selected { background: #ffffff; }
.cb-srail-meta {
    margin-left: auto; font-size: 9px; color: #94a3b8; white-space: nowrap; flex-shrink: 0;
    font-family: ui-monospace, monospace;
}
.cb-srail-add {
    display: flex; align-items: center; gap: 4px; padding: 7px 10px; cursor: pointer;
    font-size: 10px; color: #1f4e79; border-bottom: 1px solid #e3eaf2;
}
.cb-srail-add:hover { background: var(--cb-hover, #e6eff8); }

/* ── App-wide control chrome ───────────────────────────────────────────────── */
/* Lives here, not in main_ui's add_head_html: that block is baked into the page
 * shell, which is served stale on this deployment, so new shell CSS may never reach
 * the browser. ensure_assets_loaded() injects per client after connect, over the
 * socket, so these rules always arrive fresh. Structural styling stays inline on the
 * elements (house_button, _stacked_panels); this file only carries what inline styles
 * cannot express. */

/* Native <input type=number> spin arrows OFF: every numeric field is a typed
 * value, not a click-to-increment counter, and the browser arrows were the
 * loudest, least stylable chrome on any form. Pseudo-elements — CSS-only. */
input[type=number]::-webkit-outer-spin-button,
input[type=number]::-webkit-inner-spin-button { -webkit-appearance: none; appearance: none; margin: 0; }
input[type=number] { -moz-appearance: textfield; appearance: textfield; }

/* House button hover states (ui/components/buttons.py — the base look is inline
 * on the element, so hover needs !important to override it). */
.q-btn.cb-btn:hover { background: #f4f8fc !important; border-color: var(--cb-field-line-hover, #a9c6e4) !important; }
.q-btn.cb-btn--accent:hover {
    background: var(--cb-btn-accent-hover, #163a5c) !important;
    border-color: var(--cb-btn-accent-hover, #163a5c) !important;
}
.q-btn.cb-btn--danger:hover { background: #fef2f2 !important; border-color: #f87171 !important; }

/* The .cb-select / .cb-field control chrome; this is its only definition. Every page
 * calls ensure_assets_loaded(). Builders: ui/components/fields.py (house_*) at the
 * .cb-field scale; ui/job_plugins/_field_styles.py at the .cb-select scale. */
/* `height`, not just min-height: Quasar pins dense controls at a FIXED height of
 * 40px (.q-field--dense .q-field__control) — min-height alone never shrank them. */
.cb-select .q-field__control,
.cb-field .q-field__control {
    height: 20px; min-height: 20px; padding: 0 5px;
    border: 1px solid var(--cb-field-line, #d3dde8); border-radius: var(--cb-field-radius, 4px);
    background: var(--cb-field-bg, #fff); transition: border-color .12s ease, box-shadow .12s ease;
}
.cb-select .q-field__control:hover,
.cb-field .q-field__control:hover { border-color: var(--cb-field-line-hover, #a9c6e4); }
.cb-select.q-field--focused .q-field__control,
.cb-field.q-field--focused .q-field__control {
    border-color: var(--cb-field-focus, #4a8fd4); box-shadow: var(--cb-field-ring, 0 0 0 2px rgba(74, 143, 212, 0.16));
}
.cb-select .q-field__control:before,
.cb-select .q-field__control:after,
.cb-field .q-field__control:before,
.cb-field .q-field__control:after { display: none !important; }
.cb-select .q-field__native,
.cb-select .q-field__input,
.cb-field .q-field__native,
.cb-field .q-field__input {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 11px;
    color: #1e293b; padding: 0; line-height: 18px; min-height: 0;
}
/* Number inputs: no browser spin buttons. They ate a third of a narrow box and stepped by 1. */
.cb-select input[type=number]::-webkit-inner-spin-button,
.cb-select input[type=number]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
.cb-select input[type=number] { -moz-appearance: textfield; appearance: textfield; }
.cb-select .q-field__marginal,
.cb-select .q-field__append,
.cb-field .q-field__marginal,
.cb-field .q-field__append { height: 18px; }
.cb-select .q-field__append .q-icon,
.cb-field .q-field__append .q-icon { font-size: 14px; color: #94a3b8; }
.cb-select.q-field--disabled .q-field__control,
.cb-field.q-field--disabled .q-field__control { background: #f8fafc; }
.cb-field .q-field__control { height: 16px; min-height: 16px; padding: 0 4px; }
.cb-field .q-field__native,
.cb-field .q-field__input { font-size: 10px; line-height: 14px; }
.cb-field .q-field__marginal,
.cb-field .q-field__append { height: 14px; }
.cb-field .q-field__append .q-icon { font-size: 12px; }
.cb-field.q-textarea .q-field__control,
.cb-select.q-textarea .q-field__control { height: auto; }
.cb-field textarea.q-field__native { line-height: 1.4; padding: 2px 0; }
.cb-select-popup {
    border: 1px solid #e2e8f0; border-radius: 5px;
    box-shadow: 0 6px 18px rgba(15,23,42,.10);
}
.cb-select-popup .q-item {
    min-height: 26px; padding: 3px 10px;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; color: #334155;
}
.cb-select-popup .q-item:hover { background: #f1f5f9; }
.cb-select-popup .q-item.q-manual-focusable--focused,
.cb-select-popup .q-item--active { background: #eef2f6; color: #1e293b; }
/* I/O source selector (ui/pipeline_builder/io_config_component.py): a .cb-select-sized box
 * opening a menu of status-dot rows. */
.cb-io-src { transition: border-color .12s ease; }
.cb-io-src:hover { border-color: #cbd5e1 !important; }
.cb-io-menu-item { transition: background .1s ease; }
.cb-io-menu-item:hover { background: #f1f5f9; }
/* ── Tomogram gallery (ui/tomo_gallery.py) ─────────────────────────────────── */
/* The birds-eye wall: a grid of reconstruction tiles, each an image box with the
 * species' picks projected onto it as one inline-SVG layer per species. The frame
 * IS the image box (aspect-ratio from the tomogram's own X/Y extent, object-fit
 * fill), which is what lets a dot layer position in plain percentages and stay
 * registered with the slice at every tile size. */
.cb-gal-page { display: flex; flex-direction: column; width: 100%; height: 100%; min-height: 0; }
/* Under the view's title line, with no rule between them. */
.cb-gal-toolbar {
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    padding: 5px 10px 5px 14px; flex-shrink: 0;
}
.cb-gal-title {
    font-size: 10px; text-transform: uppercase; font-weight: 600;
    color: #64748b; letter-spacing: 0.3px;
}
.cb-gal-count {
    font-family: ui-monospace, monospace; font-size: 10px; color: #1f4e79;
    background: var(--cb-head, #edf4fb); border-radius: 3px; padding: 0 5px;
}
.cb-gal-toolbar-label {
    font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em;
    color: #6b829b; margin-left: 4px; padding-left: 10px; border-left: 1px solid var(--cb-panel-line, #dbe3ec);
    line-height: 16px;
}
.cb-gal-toolbar-label:first-child { margin-left: 0; padding-left: 0; border-left: none; }
/* The layers list's rows (ui/components/layer_menu.py), in the wall's menu, the zoom's side column,
 * the Journey's Reconstruct card and the pick viewer. */
.cb-gal-pk-row {
    display: flex; align-items: center; gap: 6px; padding: 2px 10px 2px 16px; cursor: pointer;
    user-select: none; font-size: 10px; color: #334155; white-space: nowrap;
}
.cb-gal-pk-row:hover { background: var(--cb-hover, #e6eff8); }
.cb-gal-pk-row .q-icon { color: var(--cb-accent, #4a8fd4); flex: 0 0 auto; }
.cb-gal-pk-row.off { color: #8aa0b8; }
.cb-gal-pk-row.off .q-icon { color: #b8c7d8; }
.cb-gal-pk-follow { padding-left: 10px; border-bottom: 1px solid #f1f5f9; padding-bottom: 4px; margin-bottom: 2px; }
.cb-gal-pk-marker { display: flex; flex: 0 0 auto; cursor: pointer; border-radius: 50%; padding: 1px; }
.cb-gal-pk-marker:hover { box-shadow: 0 0 0 1px #94a3b8; }
.cb-gal-pk-row.off .cb-gal-pk-marker { opacity: 0.4; }
/* A layer's colours, opened in place under its row by a click on its marker. */
.cb-gal-pk-palette { display: flex; flex-wrap: wrap; gap: 4px; padding: 1px 10px 5px 38px; }
.cb-gal-pk-name { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cb-gal-pk-n { font-family: ui-monospace, monospace; font-size: 9px; color: #94a3b8; }
.cb-gal-pk-row .cb-gal-pk-warn { color: #d97706; }
/* The layers list as two tables, the pick layers' and the segmentations' (ui/components/layer_menu.py):
 * a head per kind whose eye switches all of its layers, a species row that switches all of the
 * species' layers, a row per layer, all on the table's columns (--cb-lyr-cols, set on the table). In a
 * view that can switch its picture or has a gallery the table is wide, with the reconstruction each
 * layer was made on and the button that shows a pick list in the gallery. */
.cb-lyr-table { padding: 0 4px; }
.cb-lyr-table.cb-lyr-wide { min-width: 500px; }
.cb-lyr-table + .cb-lyr-table { margin-top: 4px; }
.cb-lyr-table > .cb-lyr-tr {
    display: grid; grid-template-columns: var(--cb-lyr-cols); column-gap: 8px; align-items: center;
    padding: 2px 6px; border-radius: 3px;
}
.cb-lyr-table > .cb-lyr-head { padding-top: 7px; font-size: 9px; color: #8aa0b8; }
.cb-lyr-title { grid-column: span 2; font-size: 10px; font-weight: 600; color: #33506e; }
.cb-lyr-col { white-space: nowrap; }
.cb-lyr-col.num { text-align: right; }
.cb-lyr-eye { color: var(--cb-accent, #4a8fd4); cursor: pointer; }
.cb-lyr-eye.off { color: #b8c7d8; }
.cb-lyr-head .cb-lyr-eye:hover { color: var(--cb-head-ink, #12324f); }
.cb-lyr-table > .cb-lyr-group {
    cursor: pointer; user-select: none; padding-top: 5px;
    font-size: 10px; font-weight: 600; color: #475569;
}
.cb-lyr-group:hover { background: var(--cb-hover, #e6eff8); }
.cb-lyr-name { display: flex; align-items: center; gap: 4px; min-width: 0; }
.cb-lyr-table .cb-gal-pk-n { text-align: right; }
.cb-lyr-table > .cb-gal-pk-palette { padding-left: 32px; }
.cb-lyr-src {
    justify-self: start; max-width: 100%; box-sizing: border-box; overflow: hidden; text-overflow: ellipsis;
    white-space: nowrap; font-size: 9px; line-height: 14px; padding: 0 5px; border-radius: 3px; color: #4f6782;
    background: #ffffff; border: 1px solid var(--cb-field-line, #d3dde8); cursor: pointer;
}
.cb-lyr-src:hover { border-color: var(--cb-field-line-hover, #a9c6e4); color: var(--cb-head-ink, #12324f); }
.cb-lyr-src.here {
    background: var(--cb-head, #edf4fb); border-color: #c9dbee; color: var(--cb-head-ink, #12324f);
    font-weight: 600; cursor: default;
}
.cb-lyr-src.none { background: transparent; border-color: transparent; color: #94a3b8; cursor: inherit; }
.cb-gal-pk-row .q-icon.cb-lyr-gal { justify-self: center; color: #9fb2c8; cursor: pointer; border-radius: 3px; }
.cb-gal-pk-row .q-icon.cb-lyr-gal:hover { color: var(--cb-head-ink, #12324f); background: #ffffff; }
.cb-gal-pk-row .q-icon.cb-lyr-gal.here { color: var(--cb-accent, #4a8fd4); cursor: default; }
.cb-gal-pk-marker.fixed { cursor: inherit; }
.cb-gal-pk-marker.fixed:hover { box-shadow: none; }
/* Segmentation layers: the opacity row under the list. */
.cb-gal-feat-opacity {
    display: flex; align-items: center; gap: 6px; padding: 6px 10px 2px 16px;
    font-size: 9px; color: #94a3b8; border-top: 1px solid #f1f5f9; margin-top: 4px;
}
.cb-gal-body { flex: 1 1 0; min-height: 0; overflow-y: auto; overflow-x: hidden; }
.cb-gal-grid { display: grid; gap: 10px; padding: 10px; align-items: start; }
.cb-gal-tile {
    display: flex; flex-direction: column; min-width: 0;
    border: 1px solid var(--cb-card-line, #dbe3ec); border-radius: calc(var(--cb-card-radius, 4px) + 1px);
    overflow: hidden; background: #ffffff; box-shadow: var(--cb-card-shadow, 0 1px 2px rgba(16, 42, 67, 0.06));
}
.cb-gal-tile:hover { border-color: #c7d2fe; box-shadow: 0 2px 8px rgba(15,23,42,0.08); }
/* The selected tile (shared with the Journey): an accent ring and a tinted caption. A click on its
 * slice zooms, a click on another tile's selects that one. */
.cb-gal-tile.selected, .cb-gal-tile.selected:hover {
    border-color: var(--cb-accent, #4a8fd4); box-shadow: 0 0 0 1px var(--cb-accent, #4a8fd4);
}
.cb-gal-tile.selected .cb-gal-cap { background: var(--cb-head, #edf4fb); }
.cb-gal-tile.selected .cb-gal-name { color: var(--cb-head-ink, #12324f); font-weight: 600; }
.cb-gal-frame {
    position: relative; width: 100%; min-height: 60px;
    background: #0f172a; cursor: pointer; overflow: hidden;
}
.cb-gal-tile.selected .cb-gal-frame { cursor: zoom-in; }
.cb-gal-img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: fill; display: block; }
/* Unknown tomogram extent: no aspect-ratio to give the frame a height, so the image
 * sizes itself and the tile is as tall as it is. No dots are drawn in this state
 * (nothing to normalize against), so letterboxing can't desynchronize anything. */
.cb-gal-frame-auto { aspect-ratio: auto; max-height: 78vh; }
.cb-gal-frame-auto .cb-gal-img { position: static; height: auto; object-fit: contain; }
.cb-gal-dotlayer { position: absolute; inset: 0; pointer-events: none; }
.cb-gal-dotlayer svg { width: 100%; height: 100%; display: block; }
.cb-gal-note {
    position: absolute; inset: 0; display: flex; flex-direction: column;
    align-items: center; justify-content: center; gap: 6px;
    font-size: 10px; color: #94a3b8; text-align: center; padding: 6px;
}
.cb-gal-badge {
    position: absolute; top: 0; right: 0; padding: 1px 5px;
    background: rgba(15,23,42,0.72); color: #f8fafc;
    font-family: ui-monospace, monospace; font-size: 9px; border-bottom-left-radius: 3px;
}
/* Picks counted but not placeable (unresolved tomogram extent) — amber, so the
 * count can't be read as "these dots are the picks". */
.cb-gal-badge-warn { background: rgba(180, 83, 9, 0.88); }
.cb-gal-cap {
    display: flex; align-items: center; gap: 4px; padding: 3px 6px;
    border-top: 1px solid #f1f5f9; min-width: 0; cursor: pointer;
}
.cb-gal-name {
    font-size: 10px; color: #334155; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1;
}
/* The Journey glyph at the caption's end: opens the Journey on this tilt series, beside the wall. */
.cb-gal-jlink {
    display: flex; flex: 0 0 auto; padding: 1px; margin-right: -2px; border-radius: 3px;
    color: #8aa0b8; cursor: pointer;
}
.cb-gal-jlink:hover { color: var(--cb-head-ink, #12324f); background: var(--cb-hover, #e6eff8); }
/* The top-left switch between a tile's slice and a mosaic of its tilt series. */
.cb-gal-mosaic-toggle { background: rgba(15,23,42,0.62); color: #e2e8f0; }
.cb-gal-mosaic-toggle:hover, .cb-gal-mosaic-toggle.on { background: rgba(37,99,235,0.88); color: #ffffff; }
/* Tilt previews (ui/tilt_previews.py): the Tilts tab's groups and cards, and a tile's
 * mosaic cells. A series is one HTML string; its sizes and layout are inline there. */
/* A series is a pale gray-blue panel, the Journey's section look; its tilt cards stay white. */
.cb-tp-group {
    border-radius: var(--cb-radius, 5px); background: var(--cb-panel, #ffffff); overflow: hidden;
    outline: 1px solid var(--cb-panel-line, #dbe3ec); outline-offset: -1px;
}
.cb-tp-head {
    display: flex; align-items: center; gap: 6px; padding: 4px 8px; min-width: 0;
    cursor: pointer; user-select: none;
    background: var(--cb-group-head, #edf4fb); box-shadow: 0 1px 0 var(--cb-group-head-line, #dbe3ec);
}
.cb-tp-head:hover { background: var(--cb-hover, #e6eff8); }
/* The selected tilt series on the Tilts tab: its group (grouped by position, its position) ringed in
 * the accent, with the accent tick on its header. */
.cb-tp-group.selected { outline-color: var(--cb-accent, #4a8fd4); }
.cb-tp-group.selected > .cb-tp-head {
    box-shadow: inset 2px 0 0 var(--cb-accent, #4a8fd4), 0 1px 0 var(--cb-group-head-line, #dbe3ec);
}
.cb-tp-chev { font-size: 9px; color: #8aa0b8; transition: transform 0.15s; }
.cb-tp-chev.open { transform: rotate(90deg); }
.cb-tp-name { font-size: 10px; font-weight: 600; color: #334155; white-space: nowrap; }
.cb-tp-ts {
    font-family: ui-monospace, monospace; font-size: 9px; color: #94a3b8;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1;
}
.cb-tp-n { font-family: ui-monospace, monospace; font-size: 9px; color: #64748b; }
.cb-tp-card {
    border: 1px solid var(--cb-card-line, #dbe3ec); border-radius: var(--cb-card-radius, 4px); overflow: hidden;
    box-shadow: var(--cb-card-shadow, 0 1px 2px rgba(16, 42, 67, 0.06));
    background: #ffffff; cursor: zoom-in;
}
/* In the review a click on a card toggles the tilt's exclusion; the magnifier opens it. */
.cb-tg-review .cb-tp-card { cursor: pointer; }
.cb-tp-card:hover { box-shadow: 0 0 0 1px #93c5fd; }
.cb-tp-cap {
    display: flex; gap: 6px; padding: 1px 5px; white-space: nowrap; overflow: hidden;
    font-family: ui-monospace, monospace; font-size: 8px; color: #64748b; background: var(--cb-cap, #f4f8fc);
}
.cb-tp-cap b { font-weight: 600; color: #0f172a; }
.cb-tp-cell { cursor: zoom-in; }
.cb-tp-cell:hover > * { outline: 1px solid #93c5fd; outline-offset: -1px; }
/* A tilt's exclusion (services/tilt_series/tilt_state.py `excluded`): one red border, whoever
 * excluded it (a manual label, the confidence score at the threshold, the committed verdict).
 * A mosaic cell outlines what the committed verdict excluded. Information is never red. */
.cb-tp-img { position: relative; }
.cb-tp-card.cb-ts-x { border: 1.5px solid #ef4444; }
.cb-tp-cell.cb-ts-x > .cb-tp-img { outline: 1px solid #ef4444; outline-offset: -1px; }
/* Info icons over a card's image, bottom left, each with its popover: a dark exposure (dim: a
 * crescent, blank: a disc), a tilt alignment's output lacks with no verdict against it (a
 * slashed circle), an outlier (!). The outlier icon carries a cb-to-<metric> class per outlying
 * metric and shows only while the root ticks one of them (cb-tg-m-<metric>). */
.cb-ti-row { position: absolute; left: 3px; bottom: 3px; z-index: 2; display: flex; gap: 2px; }
.cb-ti {
    display: inline-block; width: 12px; height: 12px; border-radius: 50%; cursor: help;
    background: rgba(15,23,42,0.55) center / 8px 8px no-repeat;
}
.cb-ti-dim {
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 10'>\
<path d='M6.6 1.3a3.9 3.9 0 1 0 2.2 6.4A3.2 3.2 0 0 1 6.6 1.3z' fill='%23fbbf24'/></svg>");
}
.cb-ti-blank {
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 10'>\
<circle cx='5' cy='5' r='3.6' fill='%23fbbf24'/></svg>");
}
.cb-ti-out {
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' \
viewBox='0 0 10 10' fill='none' stroke='%23a5b4fc' stroke-width='1.4'>\
<circle cx='5' cy='5' r='3.5'/><path d='M2.6 7.4 7.4 2.6'/></svg>");
}
.cb-ti-outl, .cb-ti-outl-lg {
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' \
viewBox='0 0 10 10' fill='%23c4b5fd'>\
<rect x='4.2' y='1.4' width='1.6' height='4.8' rx='0.8'/><circle cx='5' cy='8.1' r='0.95'/></svg>");
}
.cb-ti-outl { display: none; }
.cb-tg-m-ctf .cb-ti-outl.cb-to-ctf, .cb-tg-m-mot .cb-ti-outl.cb-to-mot, .cb-tg-m-ddf .cb-ti-outl.cb-to-ddf,
.cb-tg-m-ast .cb-ti-outl.cb-to-ast, .cb-tg-m-shift .cb-ti-outl.cb-to-shift { display: inline-block; }
/* In the popovers (inside the root, so the same classes reach them): an outlying value is violet
 * only while its metric is ticked, and the outlier icon's popover lists the ticked metrics only. */
.cb-to-row { display: none; }
.cb-tg-m-ctf .cb-to-row.cb-to-ctf, .cb-tg-m-mot .cb-to-row.cb-to-mot, .cb-tg-m-ddf .cb-to-row.cb-to-ddf,
.cb-tg-m-ast .cb-to-row.cb-to-ast, .cb-tg-m-shift .cb-to-row.cb-to-shift { display: block; }
.cb-tg-m-ctf .cb-to-v-ctf, .cb-tg-m-mot .cb-to-v-mot, .cb-tg-m-ddf .cb-to-v-ddf,
.cb-tg-m-ast .cb-to-v-ast, .cb-tg-m-shift .cb-to-v-shift { color: #6d28d9; font-weight: 600; }
/* The magnifier, top right over a card's image, drawn on hover: the full-size viewer. */
.cb-tp-zoom {
    position: absolute; top: 3px; right: 3px; z-index: 2; width: 16px; height: 16px; border-radius: 3px;
    background: rgba(15,23,42,0.55) url("data:image/svg+xml;utf8,\
<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 10' \
fill='none' stroke='white' stroke-width='1.4' stroke-linecap='round'>\
<circle cx='4.2' cy='4.2' r='2.9'/><path d='M6.4 6.4 8.9 8.9'/></svg>")\
    center / 10px 10px no-repeat;
    display: none; cursor: zoom-in;
}
.cb-tp-card:hover .cb-tp-zoom { display: block; }
.cb-tp-zoom:hover { background-color: rgba(37,99,235,0.88); }
/* A mosaic cell's information: 4 px dots at its bottom left (amber dark, indigo not in alignment's output). */
.cb-tc-dots { position: absolute; left: 2px; bottom: 2px; display: flex; gap: 1px; pointer-events: none; }
.cb-tc-dot { width: 4px; height: 4px; border-radius: 50%; }
.cb-tc-dark { background: #f59e0b; }
.cb-tc-out { background: #818cf8; }
/* The legend's red-border swatch. */
.cb-lg-x {
    width: 12px; height: 12px; box-sizing: border-box; border: 1.5px solid #ef4444; border-radius: 2px;
    background: #f1f5f9;
}
/* A group header's strip: one tick per tilt at its stage tilt, coloured like its tilt. The
 * span is a 4 px hit box; the visible tick is its 2 px ::before. */
.cb-tp-tick { position: absolute; top: 0; bottom: 0; width: 4px; margin-left: -2px; cursor: zoom-in; }
.cb-tp-tick::before {
    content: ''; position: absolute; left: 1px; width: 2px; top: 2px; bottom: 2px; background: #64748b;
}
.cb-tp-tick:hover::before { top: 0; bottom: 0; }
.cb-tk-x::before { background: #ef4444; }
.cb-tk-out::before { background: #818cf8; }
.cb-tk-dark::before { background: #f59e0b; }
/* A card's metric tokens (ui/tilt_previews.py): every card carries every token, and the
 * gallery's metrics menu shows the ticked ones by a class on its root, so a toggle re-renders
 * nothing. Violet: an outlier among the project's tilts at the same stage tilt. */
.cb-tm { display: none; }
.cb-tg-m-num .cb-tm-num, .cb-tg-m-pbad .cb-tm-pbad, .cb-tg-m-exp .cb-tm-exp, .cb-tg-m-ctf .cb-tm-ctf,
.cb-tg-m-mot .cb-tm-mot, .cb-tg-m-ddf .cb-tm-ddf, .cb-tg-m-ast .cb-tm-ast, .cb-tg-m-dose .cb-tm-dose,
.cb-tg-m-shift .cb-tm-shift { display: inline; }
.cb-tm-out { color: #6d28d9; font-weight: 600; }
.cb-tp-card .cb-tp-cap { cursor: help; }
/* Grouped by position: a beam's slim line inside its position's box, flush left. */
.cb-tp-beam { display: flex; align-items: center; gap: 6px; padding: 2px 2px 3px; min-width: 0; }
.cb-tp-beam:not(:first-child) { margin-top: 6px; }
/* On the Tilts tab a beam's line selects its tilt series; the selected one carries the accent tick. */
.cb-tg-select .cb-tp-beam { padding-left: 6px; border-radius: 3px; cursor: pointer; }
.cb-tg-select .cb-tp-beam:hover { background: var(--cb-hover, #e6eff8); }
.cb-tp-beam.selected, .cb-tp-beam.selected:hover {
    background: var(--cb-head, #edf4fb); box-shadow: inset 2px 0 0 var(--cb-accent, #4a8fd4);
}
/* The gallery's metrics menu: one checkbox per metric. */
.cb-tg-menu { padding: 4px 8px 4px 4px; }
.cb-tg-menu .q-checkbox__label { font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; color: #334155; }
/* Zoom: one tile at viewport scale, in a translucent slate card that hugs the slice and its
 * side column (the frame's height and width are set inline from the tomogram's aspect, as tall
 * as the screen). Quasar's minimized-dialog padding and 560 px cap are lifted for this dialog only. */
.cb-gal-zoom-dialog .q-dialog__inner--minimized { padding: 4px; }
.cb-gal-zoom-dialog .q-dialog__inner--minimized > div { max-width: none; max-height: none; }
/* The zoom: the slice on the left, as large as the screen holds, and a white column of controls
 * on its right, top to bottom. */
.cb-gal-zoom {
    background: rgba(51, 65, 85, 0.55); backdrop-filter: blur(3px);
    border-radius: 8px; padding: 6px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.35);
    display: flex; flex-direction: row; align-items: stretch; gap: 8px; width: fit-content; max-width: 99vw;
}
.cb-gal-zoom-side {
    flex: 0 0 auto; display: flex; flex-direction: column; gap: 4px; min-height: 0; overflow-y: auto;
    background: #ffffff; border-radius: 5px; padding: 8px 4px 8px 8px;
}
.cb-gal-zoom-side .cb-gal-pk-row { padding-left: 2px; }
.cb-gal-zoom-side .cb-lyr-table { padding: 0; }
.cb-gal-zoom-side .cb-gal-pk-palette { padding-left: 24px; }
.cb-gal-zoom-side .cb-hmenu-item, .cb-gal-zoom-side .cb-hmenu-group { padding-left: 2px; padding-right: 4px; }
.cb-gal-zoom-side .cb-hmenu-item.nested { padding-left: 8px; }
.cb-gal-zoom-headline { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
.cb-gal-zoom-title { font-size: 12px; font-weight: 600; color: #1e293b; }
.cb-gal-zoom-ts { font-family: ui-monospace, monospace; font-size: 9px; color: #64748b; word-break: break-all; }
.cb-gal-zoom-src { font-family: ui-monospace, monospace; font-size: 9px; color: #94a3b8; word-break: break-all; }
/* A list's head: the glyph of its tool on the wall's title line, and its name. */
.cb-gal-zoom-sec {
    display: flex; align-items: center; gap: 5px; margin-top: 10px; padding-bottom: 1px;
    font-size: 10px; font-weight: 600; color: #33506e;
}
.cb-gal-zoom-sec > :first-child { color: #6b829b; }
.cb-gal-zoom-list { display: flex; flex-direction: column; }
.cb-gal-zoom-actions { margin-top: auto; padding-top: 10px; display: flex; gap: 6px; }
.cb-gal-zoom-img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: fill; display: block; }
.cb-gal-zoom-img.auto {
    position: static; height: calc(100vh - 28px); width: auto; max-width: calc(100vw - 300px); object-fit: contain;
}
.cb-gal-zoom-frame { cursor: default; border-radius: 4px; }
/* The zoom's previous / next chevrons: dim over the slice, solid under the pointer. */
.cb-gal-zoom-chev { color: rgba(255, 255, 255, 0.4); background: rgba(15, 23, 42, 0.12); }
.cb-gal-zoom-chev:hover { color: rgba(255, 255, 255, 0.95); background: rgba(15, 23, 42, 0.5); }
/* The `3dmod` copy chip in a tomogram preview's corner (ui/components/copyable.py). */
.cb-3dmod-chip { background: rgba(15, 23, 42, 0.62); color: #e2e8f0; }
.cb-3dmod-chip:hover { background: rgba(37, 99, 235, 0.88); color: #ffffff; }
/* ── Live-session marker ─────────────────────────────────────────────────────── */
/* The 7 px green dot beside `Curate picks` on the tomogram the running ChimeraX + ArtiaX
 * session has open (Picks & curation tab). It breathes: a CSS animation, never a server
 * tick, and shallow, because a marker that flashes is noise and one that breathes is status. */
@keyframes cb-artiax-breathe {
    0%, 100% { opacity: 0.62; }
    50%      { opacity: 1; }
}
.cb-artiax-live { animation: cb-artiax-breathe 2.6s ease-in-out infinite; }
/* The protocol light breathes the same way while the project's pipeline is running. */
.cb-protocol-live { animation: cb-artiax-breathe 2.6s ease-in-out infinite; }

/* ── Scrollbars: invisible everywhere, scrolling untouched ─────────────────── */
/* Dense panels lost their edges under a 15 px native bar; wheel / trackpad /
 * keyboard scrolling keeps working. Quasar's scroll-area draws its own thumb —
 * hidden too. */
* { scrollbar-width: none; }
*::-webkit-scrollbar { width: 0 !important; height: 0 !important; display: none; }
.q-scrollarea__bar, .q-scrollarea__thumb { display: none !important; }
/* NiceGUI lays a scroll-area's / an expansion's content out as a flex column with
 * `align-items: flex-start` + 1 rem padding + 1 rem gap (nicegui.css), so anything
 * inside without an explicit width shrink-wraps to the left edge. `cb-scroll-tight`
 * makes the content stretch and drops that padding — for dialogs and trackers that
 * own their own spacing. */
.cb-scroll-tight .q-scrollarea__content,
.cb-scroll-tight .q-expansion-item__content { align-items: stretch; padding: 0; gap: 0; }
/* Log wells (ui/components/log_pane.py): every log in the app is a dark slate pane of wrapped
 * mono, set apart from the light UI; error lines red, warnings amber, the app's own notes dim. */
.cb-log-well {
    font-family: 'IBM Plex Mono', monospace; font-size: 10px; line-height: 1.5;
    background: #1e293b; color: #e2e8f0; border-radius: 6px; padding: 8px 10px;
    white-space: pre-wrap; overflow-wrap: anywhere; overflow-y: auto; box-sizing: border-box;
    box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.3);
    scrollbar-width: thin; scrollbar-color: #475569 transparent;
}
.cb-log-well ::selection { background: rgba(147, 197, 253, 0.35); }
.cb-log-err { color: #fca5a5; }
.cb-log-warn { color: #fcd34d; }
.cb-log-note { color: #64748b; font-style: italic; }
/* A live log (LiveLog) fills what its tab leaves, inset from the tab's edges. align-self: its NiceGUI
 * column aligns children flex-start, which shrink-wraps a width-less child to its longest line. */
.cb-livelog { flex: 1 1 0; min-height: 0; width: auto; align-self: stretch; margin: 6px 10px 10px; }
/* ── Species page · Analytics: the per-tomogram table and the set list ───── */
.cb-atable-wrap { max-height: 440px; overflow: auto; width: 100%; border-top: 1px solid #e2e8f0; }
.cb-atable { border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #1e293b; }
.cb-atable th {
    position: sticky; top: 0; background: #ffffff; z-index: 1; cursor: help;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 9px; font-weight: 700; color: #94a3b8;
    text-transform: uppercase; letter-spacing: 0.3px; text-align: left;
    padding: 4px 8px 3px; border-bottom: 1px solid #e2e8f0; white-space: nowrap;
}
.cb-atable td { padding: 2px 8px; border-bottom: 1px solid #f1f5f9; white-space: nowrap; }
.cb-atable tbody tr:hover td { background: #f8fafc; }
.cb-atable .n { text-align: right; }
.cb-atable .nm { font-family: 'IBM Plex Sans', sans-serif; color: #334155; cursor: help; }
.cb-atable .dim { color: #94a3b8; }
.cb-atable .mk { color: #b45309; cursor: help; }
.cb-atable .err { color: #b91c1c; font-weight: 700; cursor: help; }
.cb-atable td:is([title], [data-cb-tip]) { cursor: help; }
.cb-atable tr.sel td { background: #eef2ff; }
.cb-atable .act { color: #6366f1; cursor: pointer; text-decoration: underline dotted; }
/* ── Pipeline table (ui/pipeline_table_view.py): a row is one job and opens it ── */
.cb-pt-row:hover { background: #f1f5f9; }
/* ── Toasts (every ui.notify) ──────────────────────────────────────────────── */
/* A compact white card in the house type, its kind told by a 3 px left bar and the icon's
 * color, instead of Quasar's filled Material banner. `[class*=...]` outranks Quasar's
 * `!important` bg-* / text-* color classes. */
.q-notification, .q-notification[class*="bg-"], .q-notification[class*="text-"] {
    background: #ffffff !important; color: #1e293b !important;
    border: 1px solid #e2e8f0; border-left: 3px solid #94a3b8; border-radius: 4px;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.12); min-height: 0; padding: 3px 10px 3px 8px;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; line-height: 1.4; max-width: 460px;
}
.q-notification .q-notification__wrapper { min-height: 0; }
.q-notification .q-notification__message { font-size: 11px; padding: 3px 0; white-space: pre-line; }
.q-notification .q-notification__caption { font-size: 10px; color: #64748b; opacity: 1; }
.q-notification .q-notification__icon, .q-notification .q-notification__spinner {
    font-size: 13px !important; margin-right: 7px; color: #94a3b8;
}
.q-notification .q-notification__actions .q-btn { font-size: 10px; color: #475569 !important; text-transform: none; }
.q-notification .q-notification__badge {
    font-size: 9px; padding: 0 4px; background: #475569 !important; color: #ffffff !important;
}
.q-notification.bg-positive { border-left-color: #16a34a; }
.q-notification.bg-positive .q-notification__icon { color: #16a34a; }
.q-notification.bg-negative { border-left-color: #dc2626; }
.q-notification.bg-negative .q-notification__icon { color: #dc2626; }
.q-notification.bg-warning { border-left-color: #d97706; }
.q-notification.bg-warning .q-notification__icon { color: #d97706; }
.q-notification.bg-info { border-left-color: #6366f1; }
.q-notification.bg-info .q-notification__icon { color: #6366f1; }
/* A view's expand icon and close cross (ui/workspace_page.py `_view_frame`), at the right end of
 * its title line. */
.cb-view-close { color: #8aa0b8; cursor: pointer; border-radius: 3px; width: 16px; height: 16px; }
.cb-view-close:hover { color: var(--cb-head-ink, #12324f); background: var(--cb-hover, #e6eff8); }
/* The pane Esc closes, the one the pointer last went down in (ui/workspace_page.py `_PANE_FOCUS_JS`):
 * its title line takes the title stripes' light blue over an accent rule. */
[data-cb-focus] {
    --cb-view-head: var(--cb-head, #edf4fb); --cb-view-head-line: var(--cb-accent, #4a8fd4);
    --cb-view-head-line-w: 2px;
}
/* A view's own tools on its title line (ui/components/hover_menu.py `tool_trigger`): a glyph, then
 * a name or nothing, then a caret; muted until hovered, like the icon buttons beside them. The
 * dropdown each opens under the pointer is a white card of rows. */
.cb-head-tools { display: flex; align-items: center; gap: 2px; min-width: 0; }
.cb-htool {
    display: inline-flex; align-items: center; gap: 3px; height: 22px; padding: 0 3px 0 5px; min-width: 0;
    border-radius: var(--cb-field-radius, 4px); color: #4f6782; cursor: pointer; user-select: none;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; font-weight: 500; white-space: nowrap;
}
.cb-htool:hover { background: var(--cb-hover, #e6eff8); color: var(--cb-head-ink, #12324f); }
.cb-htool-text { min-width: 0; max-width: 200px; overflow: hidden; text-overflow: ellipsis; }
.cb-htool-caret { color: #9fb2c8; margin-left: -1px; }
.cb-htool:hover .cb-htool-caret { color: #4f6782; }
/* A tool among a panel's content rather than on a title line (the Particles panel's, over the slabs
 * and the gallery): in a field's box, as tall as the house fields. */
.cb-htool.cb-htool--box {
    height: 20px; box-sizing: border-box; padding: 0 3px 0 6px; gap: 4px; color: #33506e;
    background: var(--cb-field-bg, #fff); border: 1px solid var(--cb-field-line, #d3dde8);
    box-shadow: 0 1px 1px rgba(16, 42, 67, 0.04);
}
.cb-htool.cb-htool--box:hover { background: #f4f8fc; border-color: var(--cb-field-line-hover, #a9c6e4); }
.cb-htool--box .cb-htool-glyph { color: #6b829b; }
.q-menu.cb-hmenu {
    border: 1px solid var(--cb-panel-line, #dbe3ec); border-radius: 5px;
    box-shadow: 0 6px 18px rgba(15, 23, 42, 0.10);
}
.cb-hmenu-body { padding: 4px 0; min-width: 190px; font-family: 'IBM Plex Sans', sans-serif; }
.cb-hmenu-title { font-size: 9px; color: #8aa0b8; padding: 2px 12px 3px; }
/* A sentence over a menu's rows saying what the choice is. */
.cb-hmenu-lede { font-size: 10px; line-height: 1.35; color: #4f6782; padding: 3px 12px 5px; max-width: 280px; }
.cb-hmenu-group { font-size: 10px; font-weight: 600; color: #33506e; padding: 6px 12px 1px; }
.cb-hmenu-item {
    display: flex; align-items: center; gap: 5px; padding: 3px 12px 3px 8px; cursor: pointer; user-select: none;
    font-size: 10px; color: #334155; white-space: nowrap;
}
.cb-hmenu-item.nested { padding-left: 14px; }
.cb-hmenu-item:hover { background: var(--cb-hover, #e6eff8); }
.cb-hmenu-item.on { color: var(--cb-head-ink, #12324f); font-weight: 600; }
.cb-hmenu-check { color: var(--cb-accent, #4a8fd4); visibility: hidden; flex: 0 0 auto; }
.cb-hmenu-item.on .cb-hmenu-check { visibility: visible; }
.cb-hmenu-item.off { color: #b8c7d8; cursor: default; }
.cb-hmenu-item.off:hover { background: transparent; }
.cb-hmenu-text { flex: 1 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.cb-hmenu-note { margin-left: 10px; font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #94a3b8; }
.cb-hmenu-action { color: #33506e; border-top: 1px solid #eef2f6; margin-top: 4px; padding-top: 5px; }
.cb-hmenu-action .q-icon { color: #6b829b; }
.cb-hmenu-empty { font-size: 10px; color: #94a3b8; padding: 4px 12px; max-width: 240px; }
/* A switch standing in for a view's name (the Tomograms / Tilts tabs): the shown tab in the title's
 * face and ink, the other muted, each with its count. */
.cb-seg.cb-seg-head { border: none; box-shadow: none; background: transparent; overflow: visible; gap: 2px; }
.cb-seg-head .cb-seg-btn {
    font-family: 'IBM Plex Sans Condensed', 'IBM Plex Sans', sans-serif; font-size: 13px; font-weight: 600;
    letter-spacing: 0.01em; line-height: 20px; padding: 0 5px; color: #8aa0b8; background: transparent;
    border-right: none; border-radius: 4px;
}
.cb-seg-head .cb-seg-btn:hover { color: var(--cb-head-ink, #12324f); background: var(--cb-hover, #e6eff8); }
.cb-seg-head .cb-seg-btn.active { color: var(--cb-view-head-ink, #12324f); background: transparent; box-shadow: none; }
.cb-seg-head .cb-seg-badge { font-family: 'IBM Plex Mono', monospace; font-size: 9px; font-weight: 500; }
/* A view's splitter (ui/workspace_page.py `_VIEW_RESIZER_JS`): an 8 px strip in the gap on the frame's left
 * edge that drags the boundary with the view to its left; a double-click splits the pair evenly.
 * Only a frame with a view to its left shows it (.cb-view-split). The roster divider's look. */
.cb-view-splitter {
    display: none; position: absolute; left: -8px; top: 0; bottom: 0; width: 8px;
    cursor: col-resize; z-index: 6;
}
.cb-view-split > .cb-view-splitter { display: block; }
.cb-view-splitter::after {
    content: ''; position: absolute; left: 50%; top: 0; width: 1px; height: 100%; transform: translateX(-50%);
    background: transparent; transition: background .12s ease, width .12s ease;
}
.cb-view-splitter:hover::after,
.cb-view-splitter.dragging::after { background: #93c5fd; width: 2px; }
/* ── The Journey (ui/tomo_dashboard_dialog.py, with the Particles section of
 * ui/particles/pick_viewer.py, which the full-page pick viewer shares) ──────── */
/* The root: one chart tile size for every chart; tiles share a row and wrap below the minimum. */
.cb-journey { --cb-plot-h: 220px; --cb-plot-min: 300px; }
/* Text with no face of its own takes Plex Sans, not the page body's. */
:is(.cb-journey, .cb-viewer-page) { font-family: 'IBM Plex Sans', sans-serif; }
/* One font pair on one scale: Plex Sans for text, Plex Mono for numbers, paths and frame
 * names (the shared rules and Tailwind's font-mono use the browser's monospace elsewhere);
 * 12 px titles, 10 px labels and body, 9 px captions and meta, 11 px mono values; no weight
 * above 600. */
:is(.cb-journey, .cb-viewer-page) :is(.font-mono, .cb-hover-card, .cb-tile-score, .cb-tile-z, .cb-tile-idx,
    .cb-tile-rank, .cb-ltable-count, .cb-gallery-count, .cb-failures-list, .cb-datadump-grid,
    .cb-datadump-grid-2col, .cb-drop-list, .cb-pixel-table, .cb-pixel-species-row, .cb-chip,
    .cb-recon-preview-caption, .cb-filter-counter) {
    font-family: 'IBM Plex Mono', monospace;
}
:is(.cb-journey, .cb-viewer-page) :is(.cb-hover-key, .cb-datadump-key, .cb-pixel-header, .cb-pixel-stage-label,
    .cb-pixel-notes, .cb-pixel-species-name, .cb-chip-label, .cb-recon-pane-tag, .cb-recon-pane-method,
    .cb-degen-toggle .q-checkbox__label) {
    font-family: 'IBM Plex Sans', sans-serif;
}
:is(.cb-journey, .cb-viewer-page) :is(.cb-hover-key, .cb-ltable-h-name,
    .cb-ltable-h-num, .cb-ltable-h-cell, .cb-ltable-badge, .cb-pixel-header, .cb-pixel-species-name,
    .cb-chip-label) {
    font-weight: 600;
}
:is(.cb-journey, .cb-viewer-page) :is(.cb-ltable-name, .cb-ltable-count, .cb-instance-toolbar,
    .cb-gallery-empty, .cb-pixel-section-title, .cb-pixel-species-name, .cb-empty) {
    font-size: 10px;
}
:is(.cb-journey, .cb-viewer-page) :is(.cb-ltable-h-name, .cb-ltable-h-num, .cb-ltable-h-cell,
    .cb-detail-meta, .cb-recon-preview-caption) {
    font-size: 9px;
}
/* The tilt filter's dropped-tilts fold: the Dataset section's caret header at label size. */
.cb-drop-head { display: inline-flex; align-items: center; gap: 2px; font-size: 10px; color: #475569; }
/* The CTF-fit scrubber: slate on a light 2 px track. Quasar colours the track and thumb with
 * --q-primary in its normal layer, which these outrank; the slider sets no colour prop. */
.cb-fit-slider .q-slider__track { color: #475569; background: #e2e8f0; }
.cb-fit-slider .q-slider__inner { background: transparent; }
.cb-fit-slider .q-slider__thumb { color: #475569; }
/* The exclude-not-extracted checkbox ticks slate, not --q-primary. */
.cb-degen-toggle .q-checkbox__inner--truthy { color: #475569; }
/* The file browser's rows (ui/local_file_picker.py): hover, and the chosen row. */
.cb-fp-row:hover { background: #eff6ff; }
.cb-fp-row.cb-fp-sel { background: #dbeafe; }
/* A readonly command line (the 3dmod popover): a .cb-field in the mono face. */
.cb-field.cb-field-mono .q-field__native { font-family: 'IBM Plex Mono', monospace; }
/* Sections: a pale gray-blue panel without a border, the charts on white; the titles and the
 * header's active section in gray-blue ink. A card inside a section (the scatter fallback's) is
 * plain. */
.cb-journey .cb-main > .cb-section-card {
    background: var(--cb-panel, #ffffff); border: none; border-radius: var(--cb-radius, 5px); padding: 12px;
    margin-bottom: 10px; outline: 1px solid var(--cb-panel-line, #dbe3ec); outline-offset: -1px;
}
.cb-journey .cb-section-card .cb-section-card { background: transparent; border: none; padding: 0; }
.cb-journey .cb-section-title { color: var(--cb-head-ink, #12324f); }
.cb-journey .cb-plot-label { color: #4a6584; }
/* Species on light chrome at half saturation. The slabs' dots keep their colours; so do the pick
 * lists' swatches, which are the slab dots' legend. */
.cb-journey .cb-jsp-dot { filter: saturate(0.5); }
/* Red for what is wrong: the dropped tilts' angles, out of the amber box. */
.cb-journey .cb-drop-list { background: transparent; border: none; padding: 2px 0; }
.cb-journey .cb-drop-list .cb-drop-row { border-bottom: none; }
.cb-journey .cb-drop-list .cb-drop-tilt { color: #dc2626; }
/* The Particles section's leftover indigo: a cutout's hover ring and rank tag, and the kept counts,
 * in slate. */
.cb-journey .cb-gallery-tile:hover { border-color: #cbd5e1; }
.cb-journey .cb-gallery-tile .cb-tile-rank { background: rgba(51, 65, 85, 0.85); }
.cb-journey .cb-jsp-kept { color: #64748b; }
/* A picture still on its way: its box breathes between two grays, so a tile that is loading
 * reads as loading rather than as a dark, empty one. _IMG_STATE_JS marks every <img> once it
 * has loaded or failed (data-cb-img); a box breathes while it holds an unmarked one, or a
 * waiting note (.cb-gal-note-wait). Cutouts and templates are CSS backgrounds, which fire no
 * events: their container carries a hidden <img> of the same atlas (.cb-img-probe), and its
 * tiles breathe until that one is marked. */
@keyframes cb-img-wait {
    0%, 100% { background-color: #eceff3; }
    50%      { background-color: #c9d1dc; }
}
:is(.cb-gal-frame, .cb-tomo-preview, .cb-recon-preview, .cb-tp-img):has(img:not([data-cb-img]), .cb-gal-note-wait),
:is(.cb-gallery-scroll, .cb-gallery-grid):has(> .cb-img-probe:not([data-cb-img])) > .cb-gallery-tile {
    animation: cb-img-wait 1.4s ease-in-out infinite;
}
/* Every other picture the same way, on its own box: no alt text while it loads (a failed one
 * keeps it), and `ui.image`'s q-img breathes in place of Quasar's spinner. */
img[src]:not([data-cb-img]) {
    color: transparent;
    animation: cb-img-wait 1.4s ease-in-out infinite;
}
.q-img:has(.q-img__loading) { animation: cb-img-wait 1.4s ease-in-out infinite; }
.q-img__loading .q-spinner { display: none; }
.cb-img-probe { display: none; }
.cb-gal-note.cb-gal-note-wait { color: #64748b; }
/* ── Harbor controls ─────────────────────────────────────────────────────────
 * The house icon button (ui/components/buttons.py `icon_button`): muted, a light-blue tint under
 * the pointer, red only on a delete's hover, navy while armed. Quasar's own hover veil is off. */
.q-btn.cb-icon-btn .q-icon { font-size: 15px; }
.q-btn.cb-icon-btn .q-focus-helper { display: none; }
.q-btn.cb-icon-btn:hover { color: var(--cb-head-ink, #12324f) !important; background: var(--cb-hover, #e6eff8); }
.q-btn.cb-icon-btn--danger:hover { color: #b91c1c !important; background: #fef2f2; }
.q-btn.cb-icon-btn.cb-icon-btn--armed { color: #ffffff !important; background: var(--cb-btn-accent, #1f4e79); }
.q-btn.cb-icon-btn.cb-icon-btn--armed:hover { background: var(--cb-btn-accent-hover, #163a5c); }
/* Checkboxes, switches and radios in the accent blue; their labels in the house face. */
.q-checkbox__inner--truthy, .q-checkbox__inner--indet, .q-radio__inner--truthy,
.q-toggle__inner--truthy { color: var(--cb-accent, #4a8fd4) !important; }
.q-checkbox__inner--falsy, .q-radio__inner--falsy { color: #9fb2c8 !important; }
.q-toggle__inner--falsy { color: #b8c7d8 !important; }
.q-checkbox__label, .q-toggle__label, .q-radio__label {
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px; color: #33506e;
}
/* The tool strip under a view's title line (the Tilts tab's, the Particles registry's): white, a
 * hairline under it, butted against the title line so the two read as one header in two tiers. */
.cb-gal-toolbar, .cb-species-header {
    background: var(--cb-surface, #ffffff); border-bottom: 1px solid var(--cb-panel-line, #dbe3ec);
    margin-top: -6px; margin-bottom: 6px; min-height: 31px; box-sizing: border-box;
}
/* A Journey section's title line as the panels' stripe: edge to edge across the card's top. */
.cb-journey .cb-main > .cb-section-card > .cb-section-card-header:first-child {
    margin: -12px -12px 12px; padding: 5px 12px; min-height: 30px; box-sizing: border-box;
    background: var(--cb-head, #edf4fb); border-radius: var(--cb-radius, 5px) var(--cb-radius, 5px) 0 0;
    box-shadow: 0 1px 0 var(--cb-head-line, #c9dbee), inset var(--cb-accent-w, 2px) 0 0 var(--cb-accent, #4a8fd4),
        inset 0 1px 0 var(--cb-head-glint, rgba(255, 255, 255, 0.9));
}
/* Folded, the stripe is the whole card. */
.cb-journey .cb-main > .cb-section-card:has(> .cb-section-card-header:first-child + .cb-collapsed) {
    padding-bottom: 0;
}
.cb-journey .cb-main > .cb-section-card > .cb-section-card-header:first-child:has(+ .cb-collapsed) {
    margin-bottom: 0; border-radius: var(--cb-radius, 5px);
}
.cb-params-icon { color: #8aa0b8; }
.cb-params-icon:hover { color: var(--cb-head-ink, #12324f); }
"""

# The Particles panel (root .cb-particles-root.cb-pv); after _CB_CSS, so it wins its ties.
_PARTICLES_CSS = """
/* One gray-blue for list selection and the hover brush, tile corners quieter (index and z on
 * hover). Selected picks stay amber, dropped ones red, and species keep their colours on the slabs. */
.cb-particles-root.cb-pv > .cb-section-card-header {
    margin-bottom: 10px; flex-wrap: wrap; row-gap: 4px;
}
.cb-particles-root.cb-pv .cb-particles-split { gap: 14px; }
.cb-particles-root.cb-pv .cb-gallery-count { font-size: 9px; }
.cb-particles-root.cb-pv .cb-curation-bar { border-bottom: none; }
.cb-particles-root.cb-pv .cb-gallery-tile .cb-tile-rank { background: rgba(44, 74, 107, 0.85); }
.cb-particles-root.cb-pv .cb-gallery-tile:not(:hover) :is(.cb-tile-idx, .cb-tile-z) {
    opacity: 0;
}
.cb-particles-root.cb-pv .cb-gallery-tile:not(.selected):hover { border-color: #9fb6cf; }
.cb-particles-root.cb-pv .cb-gallery-tile.cb-tile-highlight {
    outline-color: #4e79a7; box-shadow: 0 0 0 1px #4e79a7, 0 0 8px rgba(78, 121, 167, 0.55);
}
.cb-particles-root.cb-pv .cb-lasso-rect {
    border-color: #4e79a7; background: rgba(78, 121, 167, 0.12);
}
/* The locator that follows a hovered tile onto the slab: a hollow white ring, not a pink disc. */
.cb-particles-root.cb-pv .cb-pick-marker {
    width: 12px; height: 12px; background: transparent; border: 2px solid #ffffff;
    box-shadow: 0 0 0 1px rgba(15, 23, 42, 0.8), inset 0 0 0 1px rgba(15, 23, 42, 0.8);
}
/* Beside the list tool over the gallery: the shown list's kept/total, extraction mark, path and delete. */
.cb-particles-root.cb-pv .cb-pv-list-extras { color: #4f6782; }
.cb-pv-list-count { font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #6b829b; margin-right: 2px; }
"""


# The panels' names are set in the condensed cut of the house face (`VIEW_TITLE_STYLE`,
# ui/job_plugins/_field_styles.py); without it they fall back to IBM Plex Sans.
_TITLE_FONT_LINK = (
    '<link rel="stylesheet" '
    'href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Condensed:wght@500;600&display=swap">'
)


def ensure_assets_loaded() -> None:
    """Inject the stylesheet once per client. The Species page (built with the workspace)
    and the Journey / tomogram-import dialog (built later, lazily) share it, so a second
    caller on the same page must not append a second copy to the head."""
    client = context.client
    if getattr(client, "_cb_assets_loaded", False):
        return
    client._cb_assets_loaded = True
    ui.add_head_html(_TITLE_FONT_LINK)
    ui.add_head_html(f"<style>{_THEME_CSS}{_CB_CSS}{_PARTICLES_CSS}</style>")
    client.run_javascript(_TITLE_TIP_JS)
    client.run_javascript(_IMG_STATE_JS)


# Native `title` hovers drawn as the app's tooltip. The browser's own is OS-styled and can't be
# restyled, and the roster and the HTML-string views use `title` because a Quasar tooltip is one
# more element per cell. One delegated listener and one shared box for the whole page: hovering
# an element moves its `title` to `data-cb-tip` (so the native one never shows; a new `title`
# from a later update wins over the stored one) and shows the box under it after a short delay.
_TITLE_TIP_JS = """
(() => {
  if (window.__cbTitleTip) return;
  window.__cbTitleTip = true;
  const tip = document.createElement('div');
  tip.className = 'cb-title-tip';
  document.body.appendChild(tip);
  let host = null, timer = 0, mx = 0, shown = false;
  const hide = () => { clearTimeout(timer); host = null; shown = false; tip.style.display = 'none'; };
  const place = () => {
    const r = host.getBoundingClientRect();
    tip.style.display = 'block';
    const w = tip.offsetWidth, h = tip.offsetHeight, vw = window.innerWidth, vh = window.innerHeight;
    const below = r.bottom + 6 + h <= vh - 4;
    tip.style.left = Math.max(4, Math.min(mx - w / 2, vw - w - 4)) + 'px';
    tip.style.top = (below ? r.bottom + 6 : Math.max(4, r.top - h - 6)) + 'px';
  };
  document.addEventListener('mouseover', (e) => {
    const el = e.target.closest && e.target.closest('[title], [data-cb-tip]');
    if (el === host) return;
    hide();
    if (!el) return;
    const t = el.getAttribute('title');
    if (t !== null) { el.setAttribute('data-cb-tip', t); el.removeAttribute('title'); }
    const text = el.getAttribute('data-cb-tip');
    if (!text || !text.trim()) return;
    host = el; mx = e.clientX;
    timer = setTimeout(() => {
      if (host !== el || !el.isConnected) return;
      tip.textContent = text;
      shown = true;
      place();
    }, 350);
  }, true);
  document.addEventListener('mousemove', (e) => { if (host && !shown) mx = e.clientX; }, true);
  document.addEventListener('mouseout', (e) => {
    if (host && !(e.relatedTarget && host.contains(e.relatedTarget))) hide();
  }, true);
  for (const ev of ['mousedown', 'wheel', 'keydown']) document.addEventListener(ev, hide, true);
  window.addEventListener('scroll', hide, true);
})();
"""

# Marks each <img> data-cb-img="ok" or "err" once it has loaded or failed, for the loading look
# above. load and error don't bubble, so one capturing listener on the document sees them all;
# the pass over document.images catches the ones that finished before it was installed.
_IMG_STATE_JS = """
(() => {
  if (window.__cbImgState) return;
  window.__cbImgState = true;
  const mark = (img, ok) => img.setAttribute('data-cb-img', ok ? 'ok' : 'err');
  const on = (e) => { if (e.target && e.target.tagName === 'IMG') mark(e.target, e.type === 'load'); };
  document.addEventListener('load', on, true);
  document.addEventListener('error', on, true);
  for (const img of document.images) if (img.complete && img.getAttribute('src')) mark(img, img.naturalWidth > 0);
})();
"""

# The app's palette and chrome as `--cb-*` variables, in one place. The panels, title stripes, view title
# lines, the lines between main panels, the cards, fields and buttons read them (inline ones carry the
# same value as their fallback: ui/job_plugins/_field_styles.py, ui/workspace_page.py,
# ui/components/buttons.py). White panels lifted a hair off a cool page, light-blue title stripes with a
# blue tick, a glassy title line on each view ruled off in light blue, blue focus rings, navy actions.
_THEME_CSS = """
:root {
    --cb-page: #f6f8fb; --cb-desk: #e9eef4; --cb-surface: #ffffff; --cb-rail: #eef3f8; --cb-rail-line: #d3dde8;
    --cb-panel: #ffffff; --cb-panel-line: #dbe3ec; --cb-panel-shadow: 0 1px 2px rgba(16, 42, 67, 0.06);
    --cb-radius: 5px; --cb-pad-y: 9px; --cb-pad-x: 11px;
    --cb-head: #edf4fb; --cb-head-line: #c9dbee; --cb-head-ink: #12324f; --cb-head-size: 12px;
    --cb-head-track: 0; --cb-head-pad: 5px; --cb-accent: #4a8fd4; --cb-accent-w: 2px;
    --cb-head-glint: rgba(255, 255, 255, 0.9);
    --cb-view-head: #ffffff; --cb-view-head-line: #dbe3ec;
    --cb-view-head-line-w: 1px; --cb-view-head-ink: #12324f;
    --cb-edge: #c9d6e3; --cb-edge-w: 1px; --cb-edge-shadow: -2px 0 6px rgba(16, 42, 67, 0.06);
    --cb-hover: #e6eff8; --cb-card-line: #dbe3ec; --cb-card-radius: 4px; --cb-cap: #f4f8fc;
    --cb-card-shadow: 0 1px 2px rgba(16, 42, 67, 0.06);
    --cb-group-head: #edf4fb; --cb-group-head-line: #dbe3ec; --cb-box-line: #dbe3ec;
    --cb-field-line: #d3dde8; --cb-field-line-hover: #a9c6e4; --cb-field-focus: #4a8fd4;
    --cb-field-ring: 0 0 0 2px rgba(74, 143, 212, 0.16); --cb-field-radius: 4px; --cb-field-bg: #fff;
    --cb-btn-line: #b8cce0; --cb-btn-accent: #1f4e79; --cb-btn-accent-hover: #163a5c;
}
body { background: var(--cb-page) !important; }
/* A wide job page in two columns: Parameters on the left, Inputs / Outputs over SLURM on the right,
 * the folded sections across both below. Narrower than 860 px it stays one column. */
.cb-jobscroll .q-scrollarea__content { container-type: inline-size; }
@container (min-width: 860px) {
    .cb-jobpage {
        display: grid !important; grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr);
        align-items: start; column-gap: 12px;
    }
    .cb-jsec-params { grid-column: 1; grid-row: 1 / span 2; }
    .cb-jsec-io { grid-column: 2; grid-row: 1; }
    .cb-jsec-slurm { grid-column: 2; grid-row: 2; }
    .cb-jsec-fold { grid-column: 1 / -1; }
}
"""
