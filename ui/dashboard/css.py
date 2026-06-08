"""Journey dashboard CSS.

Extracted verbatim from ``ui/tomo_dashboard_dialog.py`` (R0 refactor). A single
``<style>`` blob injected once per dashboard mount via ``ensure_assets_loaded()``.
Pure presentation; no logic.
"""

from nicegui import ui


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
.cb-ts-info-btn { color: #cbd5e1 !important; min-height: 18px !important; }
.cb-ts-info-btn:hover { color: #6366f1 !important; }
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
/* Per-row info popover (the ⓘ): full tomo name + metadata + file paths, each
 * with a copy-full-path button. Click-opened so the copy buttons are usable. */
.cb-info-card { min-width: 300px; max-width: 460px; padding: 6px 4px; display: flex; flex-direction: column; gap: 3px; }
.cb-info-row { display: flex; align-items: center; gap: 6px; }
.cb-info-key { font-size: 9px; text-transform: uppercase; letter-spacing: 0.3px; color: #94a3b8; flex: 0 0 88px; }
.cb-info-val {
    font-size: 10px; font-family: ui-monospace, monospace; color: #334155;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    flex: 1 1 auto; min-width: 0;
    /* rtl truncates the LEFT of long paths, keeping the filename visible. */
    direction: rtl; text-align: left;
}
.cb-info-copy { color: #94a3b8 !important; }
.cb-info-copy:hover { color: #6366f1 !important; }
.cb-info-meta { font-size: 10px; color: #64748b; font-family: ui-monospace, monospace; padding: 1px 0 3px 88px; }
.cb-main { padding: 12px; }
/* (height/flex/overflow set inline at construction time so the dialog viewport
   chain is self-contained; this rule only carries the padding chrome.) */
.cb-empty {
    flex: 1; display: flex; align-items: center; justify-content: center;
    color: #9ca3af; font-size: 13px; padding: 40px; flex-direction: column; gap: 8px;
}
.cb-section-title {
    font-size: 10px; text-transform: uppercase; font-weight: 600;
    color: #64748b; letter-spacing: 0.3px;
}
.cb-section-card {
    background: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px;
    padding: 6px 9px; margin-bottom: 5px;
}
.cb-section-card-header { display: flex; align-items: center; gap: 6px; margin-bottom: 4px; }
/* R3 — collapsible Dataset section: clickable header + rotating caret; the body
 * (chips + key/val grid + pixel-sanity table) hides when collapsed. Default
 * collapsed (pref dashboard_dataset_collapsed). */
.cb-collapsible-header { cursor: pointer; user-select: none; }
.cb-collapse-caret { color: #94a3b8; transition: transform 0.15s ease; margin-left: 2px; }
.cb-collapse-caret.rot { transform: rotate(180deg); }
.cb-collapsible-body.cb-collapsed { display: none; }
/* R2 — per-panel visibility toggle row: dense checkboxes picking which detail
 * sections render. Sits between the heatmap strip and the detail pane; a
 * user-level pref persisted across projects + TS. */
.cb-panel-toggle-row {
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
    padding: 2px 8px 3px 10px; border-bottom: 1px solid #eef2f7; background: #fafbfc;
}
.cb-panel-toggle-label {
    font-size: 10px; text-transform: uppercase; font-weight: 700;
    color: #94a3b8; letter-spacing: 0.03em;
}
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
/* Horizontal variant: hovered-pick stats as an inline strip at the top of the
 * gallery (key:value pairs in a wrapping flex row). */
.cb-hover-horizontal {
    display: flex; flex-wrap: wrap; gap: 3px 16px; align-items: baseline;
    padding: 5px 9px; margin-bottom: 4px;
}
.cb-hover-horizontal .cb-hover-pair { display: flex; align-items: baseline; gap: 4px; }
.cb-gallery-grid {
    display: grid; grid-template-columns: repeat(auto-fill, 96px);
    gap: 4px; padding: 6px 2px 6px 2px; justify-content: start;
}
.cb-gallery-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    border-radius: 3px; cursor: pointer; overflow: hidden;
    border: 2px solid transparent; transition: transform 0.06s ease;
}
.cb-gallery-tile:hover { transform: scale(1.04); border-color: #c7d2fe; }
.cb-gallery-tile.selected {
    border-color: #4338ca;
    box-shadow: 0 0 0 1px #4338ca, 0 4px 10px rgba(67,56,202,0.25);
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
.cb-reference-strip {
    display: flex; gap: 8px; align-items: stretch;
    padding: 6px 2px; border-bottom: 1px dashed #e5e7eb;
    margin-bottom: 4px;
}
.cb-reference-strip .cb-ref-group {
    display: flex; flex-direction: column; gap: 2px;
}
.cb-reference-strip .cb-ref-label {
    font-size: 9px; color: #6b7280; text-transform: uppercase;
    font-weight: 700; letter-spacing: 0.4px; padding: 0 2px;
}
.cb-reference-strip .cb-ref-tiles {
    display: flex; gap: 4px;
}
.cb-reference-strip .cb-ref-divider {
    width: 1px; background: #e5e7eb; margin: 0 2px;
}
.cb-template-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    background-size: 96px 96px;
    border-radius: 3px; overflow: hidden;
    border: 2px solid #10b981;
    box-shadow: 0 0 0 1px #10b981;
}
.cb-template-tile .cb-tile-rank {
    position: absolute; top: 0; left: 0;
    padding: 0 4px; background: rgba(16,185,129,0.92);
    font-family: ui-monospace, monospace; font-size: 9px; color: white;
    border-bottom-right-radius: 3px;
}
.cb-noise-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    border-radius: 3px; overflow: hidden; cursor: pointer;
    border: 2px solid #fb923c;
    box-shadow: 0 0 0 1px #fb923c;
    transition: transform 0.06s ease;
}
.cb-noise-tile:hover { transform: scale(1.04); }
.cb-noise-tile .cb-tile-rank {
    position: absolute; top: 0; left: 0;
    padding: 0 4px; background: rgba(251,146,60,0.92);
    font-family: ui-monospace, monospace; font-size: 9px; color: white;
    border-bottom-right-radius: 3px;
}
.cb-noise-tile .cb-tile-score {
    position: absolute; bottom: 0; right: 0;
    padding: 1px 4px; background: rgba(15,23,42,0.72);
    font-family: ui-monospace, monospace; font-size: 9px; color: #f8fafc;
    border-top-left-radius: 3px;
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
/* Slab column is the DOMINANT one (R1): flex-grow 3 vs the tabs' 1 so it claims
 * the width and pushes the gallery/tools cluster right. Its max-width is set
 * INLINE per-tomo (_render_particles_section) to min(1080px, 76vh·x/y) — the
 * slab's actual height-capped width — so the column HUGS the previews and leaves
 * no whitespace before the gallery (R1b). */
.cb-particles-canvas-col { flex: 3 1 600px; min-width: 440px; }
.cb-particles-tabs-col { flex: 1 1 360px; min-width: 340px; }
/* List workbench inside a species tab: a horizontal pick-list chip rail ABOVE
 * the selected list's detail/gallery — stacked, not side-by-side, so the short
 * rail doesn't leave dead vertical space beside the tall gallery. */
.cb-workbench-split { display: flex; flex-direction: column; gap: 8px; align-items: stretch; }
.cb-list-rail-host { width: 100%; }
.cb-list-detail-host { width: 100%; min-width: 0; }
.cb-list-rail { display: flex; flex-direction: row; flex-wrap: wrap; align-items: center; gap: 6px; }
.cb-rail-title {
    font-size: 9px; text-transform: uppercase; font-weight: 700; color: #94a3b8;
    letter-spacing: 0.04em; align-self: center;
}
.cb-list-chip {
    border: 1px solid #e5e7eb; border-radius: 6px; padding: 5px 7px; cursor: pointer; background: #fff;
    display: flex; flex-direction: column; gap: 2px; transition: border-color 0.12s, background 0.12s;
    flex: 0 0 auto; min-width: 128px;
}
.cb-list-chip:hover { border-color: #c7d2fe; background: #fafbff; }
.cb-list-chip.selected { border-color: #6366f1; background: #eef2ff; box-shadow: inset 2px 0 0 #6366f1; }
.cb-list-chip-swatch { width: 9px; height: 9px; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25); flex: 0 0 auto; }
.cb-list-chip-label {
    font-size: 11px; font-weight: 600; color: #475569;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cb-list-chip-count { font-size: 11px; font-family: ui-monospace, monospace; color: #334155; }
.cb-list-chip-badge { font-size: 9px; font-weight: 600; }
.cb-badge-ok { color: #059669; }
.cb-badge-todo { color: #94a3b8; }
.cb-badge-stale { color: #d97706; }
/* Chip second line: a small type tag (pytom / manual / imported / merged); the
 * pytom tag is an info handle (cursor:help) for the auto-pick stats tooltip. */
.cb-list-chip-meta { margin-top: 1px; flex-wrap: wrap; row-gap: 2px; }
.cb-list-chip-tag {
    font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.03em;
    color: #64748b; background: #f1f5f9; border-radius: 4px; padding: 0 4px; line-height: 1.55;
}
.cb-list-chip-tag-info { cursor: help; text-decoration: underline dotted #cbd5e1; text-underline-offset: 2px; }
/* Rich pytom-chip info tooltip: a light card (overrides Quasar's dark default)
 * with an auto-pick-stats section + a template-match section. */
.cb-chip-tooltip {
    background: #ffffff !important; color: #334155 !important; border: 1px solid #e2e8f0;
    border-radius: 8px; padding: 8px 10px; max-width: 280px;
    box-shadow: 0 6px 20px rgba(15, 23, 42, 0.16); font-size: 11px; line-height: 1.45;
}
.cb-tt-head { font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; color: #6366f1; }
.cb-tt-line { font-family: ui-monospace, monospace; color: #334155; }
.cb-tt-sub { font-size: 10px; color: #94a3b8; }
.cb-tt-sep { margin: 5px 0; background: #e2e8f0; }
/* Visibility eye — per-list (rail chip) + per-species master (tab); both toggle
 * that scope's canvas dot layers. Shared base + a tab-placement variant. */
.cb-eye { color: #94a3b8; cursor: pointer; flex: 0 0 auto; transition: color 0.12s; }
.cb-eye:hover { color: #4338ca; }
.cb-tab-eye { margin-left: 4px; align-self: center; }
.cb-rail-toolbar {
    display: flex; flex-direction: row; flex-wrap: wrap; gap: 4px;
    margin-left: auto; align-items: center;
}
/* Per-species tabs beside the canvas. Matched to the journey aesthetic:
 * small, slate, no-caps, thin indigo indicator, and a per-species color
 * swatch tying each tab to its overlay color on the shared canvas. */
.cb-species-tabs { min-height: 28px; border-bottom: 1px solid #e5e7eb; }
.cb-species-panels .q-tab-panel { padding: 8px 0 0 0; }
.cb-species-tabs .q-tab {
    min-height: 28px; padding: 0 10px; text-transform: none;
    border-radius: 6px 6px 0 0; transition: background 0.12s;
}
.cb-species-tabs .q-tab:hover { background: #f8fafc; }
.cb-species-tabs .q-tab--active { background: #eef2ff; }
/* Quasar stacks a tab's children in a COLUMN by default — force one inline row
 * so the color dot, label, and master-eye sit side by side at a consistent
 * height (was: dot stacked above the label, and the eye would stack below it). */
.cb-species-tab .q-tab__content { flex-direction: row; align-items: center; flex-wrap: nowrap; }
.cb-species-tab .q-tab__label {
    font-size: 11px; font-weight: 600; color: #64748b; line-height: 1.15; white-space: nowrap;
}
.cb-species-tabs .q-tab--active .q-tab__label { color: #4338ca; }
/* Per-species color dot before the label, tying the tab to its canvas overlay
 * color (--sp-color set inline per tab). Uses Quasar's stable tab internals. */
.cb-species-tab .q-tab__content::before {
    content: ''; width: 8px; height: 8px; border-radius: 50%;
    background: var(--sp-color, #94a3b8); box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.2);
    margin-right: 6px; flex: 0 0 auto; align-self: center;
}
/* Per-species admin icons in the Particles panel title bar, following the active
 * tab; sits left of the canvas-wide Invert switch with a little breathing room. */
.cb-section-admin { margin-right: 4px; }
.cb-preview-toolbar {
    display: flex; align-items: center; gap: 10px;
    font-size: 10px; color: #475569;
    padding: 2px 0 4px 0;
}
.cb-gallery-scroll { overflow-y: auto; max-height: 68vh; padding-right: 4px; position: relative; }
.cb-cutouts-actions { flex-shrink: 0; }
/* Cutouts on top, controls compacted underneath. */
.cb-cutouts-head { padding: 0 0 2px 0; }
.cb-gallery-controls-box {
    display: flex; flex-direction: column; gap: 4px;
    margin-top: 6px; padding-top: 6px; border-top: 1px solid #eef2f6;
    font-size: 11px;
}
.cb-gallery-controls-box .cb-filter-toolbar { padding: 0; }
.cb-gallery-controls-box .cb-hover-card { margin: 0; }
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
.cb-metric-strip {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569;
}
.cb-section-placeholder {
    font-size: 10px; color: #9ca3af; font-style: italic;
    padding: 4px 0 2px 0;
}
.cb-plot-row {
    display: flex; gap: 8px; flex-wrap: wrap; margin: 4px 0 4px 0;
}
.cb-plot-cell {
    flex: 1 1 320px; min-width: 260px;
    background: #ffffff; border: 1px solid #f1f5f9; border-radius: 4px;
    padding: 2px 4px;
}
.cb-plot-cell-wide { flex: 1 1 100%; min-width: 280px; }
.cb-plot-label {
    font-size: 9px; color: #64748b; font-weight: 600;
    padding: 1px 4px 0; text-transform: uppercase; letter-spacing: 0.3px;
}
.cb-stat-strip {
    display: flex; gap: 14px; flex-wrap: wrap;
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569; padding: 2px 0 4px 0;
}
.cb-stat-strip .cb-stat-key { color: #94a3b8; margin-right: 3px; }
.cb-stat-strip .cb-stat-val { color: #1e293b; font-weight: 600; }
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
.cb-pixel-stripe {
    display: inline-block; width: 3px; height: 12px;
    border-radius: 1px; background: #cbd5e1; flex-shrink: 0;
}
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
.cb-pixel-species-row .cb-pixel-stripe { width: 4px; height: 14px; }
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
    border: 1px solid #e5e7eb; background: #f8fafc;
    color: #475569; cursor: help; line-height: 1.4;
    white-space: nowrap;
}
.cb-chip .cb-chip-label {
    color: #94a3b8; text-transform: uppercase;
    font-size: 9px; font-weight: 700; letter-spacing: 0.4px;
}
.cb-chip .cb-chip-value { color: #1e293b; font-weight: 600; }
.cb-chip-ok      { background: #ecfdf5; border-color: #a7f3d0; }
.cb-chip-ok      .cb-chip-value { color: #047857; }
.cb-chip-warn    { background: #fffbeb; border-color: #fcd34d; }
.cb-chip-warn    .cb-chip-value { color: #b45309; }
.cb-chip-error   { background: #fef2f2; border-color: #fecaca; }
.cb-chip-error   .cb-chip-value { color: #b91c1c; }
.cb-chip-info    { background: #eff6ff; border-color: #bfdbfe; }
.cb-chip-info    .cb-chip-value { color: #1d4ed8; }
.cb-chip-neutral { background: #f1f5f9; }
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
/* ----------------------------------------------------------------------
 * Polarity invert: a runtime toggle in the section header flips the
 * apparent intensity of template, X/Y slab, X/Z slab, and cutout tiles
 * together — same density convention across all four. Double-invert
 * trick on tile children keeps overlay labels readable.
 * ---------------------------------------------------------------------- */
.cb-invert-polarity .cb-tomo-preview > img,
.cb-invert-polarity .cb-template-tile,
.cb-invert-polarity .cb-noise-tile,
.cb-invert-polarity .cb-gallery-tile {
    filter: invert(1) hue-rotate(180deg);
}
.cb-invert-polarity .cb-template-tile > *,
.cb-invert-polarity .cb-noise-tile > *,
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
    /* Crisp dual ring: solid dark inner + bright white outer. At this tiny
     * size the high-contrast double ring is what makes the dot legible on
     * any tomogram backdrop — not the dot area itself. */
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 2px rgba(255,255,255,0.9) !important;
    transition: box-shadow 0.08s ease;
}
/* Invisible hit-area so the 3px dot is easy to hover AND click (click toggles
 * keep/drop). Inherits pointer-events:auto from the dot. */
.cb-pick-ghost::after { content: ''; position: absolute; inset: -4px; }
/* Active / hover: NO size change — just a subtle colored backlight glow so the
 * matching pick reads at a glance without ballooning over its neighbours. */
.cb-pick-ghost:hover,
.cb-pick-ghost.cb-ghost-active {
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 2px rgba(255,255,255,1),
                0 0 6px 1.5px var(--sp-color, #00e5ff) !important;
    z-index: 7;
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
 * Dots are 3px and lean on the dual ring for legibility, so shapes stay
 * simple (triangle trades the ring for a filled glyph — used only by merged). */
.cb-shape-circle .cb-pick-ghost { border-radius: 50%; }
.cb-shape-square .cb-pick-ghost { border-radius: 0; }
.cb-shape-diamond .cb-pick-ghost { border-radius: 0; transform: translate(-50%, -50%) rotate(45deg); }
.cb-shape-triangle .cb-pick-ghost { border-radius: 0; clip-path: polygon(50% 0, 0 100%, 100% 100%); }

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
.cb-filter-toolbar {
    display: flex; align-items: center; gap: 8px;
    padding: 4px 0; flex-wrap: wrap;
}
/* Saved filtered-set path line (under the toolbar); reuses .cb-info-* styling. */
.cb-filter-path { gap: 6px; padding: 0 0 4px 0; }
.cb-filter-counter {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569;
    padding: 1px 6px; border-radius: 3px;
    background: #f1f5f9; border: 1px solid #e2e8f0;
}
.cb-filter-counter.cb-filter-dirty {
    color: #b45309; background: #fef3c7; border-color: #fde68a;
}

/* ── Journey heatmap strip (the de-dialoged panel's header) ─────────────────
   Two-block flex: frozen-left row labels + a horizontally-scrolling matrix of
   TS columns. Row heights match across both blocks so the rows line up without
   sticky positioning. box-sizing keeps the 1px row borders out of the height. */
.cb-strip { flex: 0 0 auto; background: #ffffff; border-bottom: 1px solid #e5e7eb; overflow: hidden; }
.cb-strip-wrap { display: flex; flex-direction: row; align-items: stretch; }
.cb-strip-corner, .cb-strip-rowlabel, .cb-strip-colhead, .cb-strip-cell { box-sizing: border-box; }
.cb-strip-left {
    flex: 0 0 auto; min-width: 168px; max-width: 220px;
    display: flex; flex-direction: column; border-right: 1px solid #e5e7eb; background: #fafbfc;
}
.cb-strip-corner {
    height: 26px; display: flex; align-items: center; gap: 4px;
    padding: 0 6px 0 9px; border-bottom: 1px solid #eef2f7;
}
.cb-strip-corner-count { font-size: 10px; font-weight: 700; color: #64748b; font-family: ui-monospace, monospace; }
.cb-strip-rowlabel {
    height: 22px; display: flex; align-items: center; gap: 5px;
    padding: 0 8px 0 9px; border-bottom: 1px solid #f8fafc;
}
.cb-strip-prep { height: 20px; }
.cb-strip-rl-name {
    font-size: 11px; color: #374151; font-weight: 600;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cb-strip-rl-sub { font-size: 8px; color: #94a3b8; letter-spacing: 0.3px; margin-left: auto; }
.cb-strip-rl-sum { font-size: 10px; color: #475569; font-family: ui-monospace, monospace; flex: 0 0 auto; }
.cb-strip-sp-dot { width: 7px; height: 7px; border-radius: 50%; flex: 0 0 auto; }
.cb-strip-scroll { flex: 1 1 0; min-width: 0; overflow-x: auto; overflow-y: hidden; }
.cb-strip-cols { display: flex; flex-direction: row; width: max-content; }
.cb-strip-col {
    flex: 0 0 62px; display: flex; flex-direction: column;
    border-right: 1px solid #f1f5f9; cursor: pointer;
}
.cb-strip-col:hover { background: #f8fafc; }
.cb-strip-col.selected { background: #eef2ff; box-shadow: inset 0 0 0 1.5px #6366f1; }
.cb-strip-colhead {
    height: 26px; display: flex; align-items: center; justify-content: center;
    font-size: 9px; color: #475569; font-weight: 600; white-space: nowrap; overflow: hidden;
    padding: 0 2px; border-bottom: 1px solid #eef2f7;
}
.cb-strip-col.selected .cb-strip-colhead { color: #4338ca; }
.cb-strip-cell { display: flex; align-items: center; justify-content: center; border-bottom: 1px solid #f8fafc; }
.cb-strip-prepcell { height: 20px; gap: 2px; }
.cb-strip-pickcell { height: 22px; gap: 3px; font-family: ui-monospace, monospace; }
.cb-strip-n { font-size: 10px; line-height: 1; }
/* kept-after-curation count — green carries the auto-vs-curated distinction (the
   one place green means a real completed step), so the cell needs no fill. */
.cb-strip-filt { font-size: 9px; color: #059669; line-height: 1; font-weight: 700; }
.cb-strip-dot { width: 6px; height: 6px; border-radius: 50%; background: #d1d5db; }
.cb-strip-dot.ok { background: #10b981; }
.cb-strip-dot.fail { background: #dc2626; }
.cb-strip-dot.running { background: #f59e0b; }
.cb-strip-dot.zero { background: #9ca3af; }
.cb-strip-dot.pending { background: #e5e7eb; }
.cb-strip-pickcell.has { color: #334155; }
.cb-strip-pickcell.running { background: rgba(245, 158, 11, 0.16); color: #92400e; }
.cb-strip-pickcell.zero { background: rgba(148, 163, 184, 0.14); color: #94a3b8; }
.cb-strip-pickcell.fail { background: rgba(220, 38, 38, 0.12); color: #b91c1c; }
.cb-strip-pickcell.pending { color: #cbd5e1; }
"""


def ensure_assets_loaded() -> None:
    ui.add_head_html(f"<style>{_CB_CSS}</style>")
