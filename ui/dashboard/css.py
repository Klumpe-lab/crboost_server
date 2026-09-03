"""Journey dashboard CSS.

Extracted verbatim from ``ui/tomo_dashboard_dialog.py`` (R0 refactor). A single
``<style>`` blob injected once per dashboard mount via ``ensure_assets_loaded()``.
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
    font-family: 'IBM Plex Sans', sans-serif; font-size: 11px; font-weight: 600;
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
.cb-params-icon:hover { color: #6366f1; }
.cb-params-tt {
    background: #ffffff !important; color: #334155 !important; border: 1px solid #e2e8f0;
    border-radius: 6px; padding: 6px 9px; box-shadow: 0 6px 20px rgba(15, 23, 42, 0.14);
    display: grid; grid-template-columns: max-content max-content; gap: 2px 12px;
    font-family: 'IBM Plex Sans', sans-serif; font-size: 10px;
}
.cb-params-tt .cb-params-k { color: #94a3b8; }
.cb-params-tt .cb-params-v { font-family: 'IBM Plex Mono', monospace; color: #1e293b; }
/* R3 — collapsible Dataset section: clickable header + rotating caret; the body
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
    gap: 4px; padding: 6px 2px 6px 2px; justify-content: start;
}
.cb-gallery-tile {
    position: relative; width: 96px; height: 96px;
    background-color: #0f172a; background-repeat: no-repeat;
    border-radius: 3px; cursor: pointer; overflow: hidden;
    border: 2px solid transparent; transition: transform 0.06s ease;
}
.cb-gallery-tile:hover { transform: scale(1.04); border-color: #c7d2fe; }
/* SELECTED tile (11-S4). Amber, matching .cb-pick-ghost.cb-ghost-selected on the slab,
 * so the tile and its dot read as one thing. It stays until another tile is clicked or
 * Esc/click-away clears it — unlike the transient .cb-tile-highlight hover brush. The
 * class existed here since the gallery was written and nothing ever applied it; the
 * click model that finally does is the point of 11-S4. */
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
/* Slab column is pinned to the slab's natural height-capped width: its WIDTH is set
 * INLINE per-tomo (render_particles_section) to min(1400px, slab_vh·x/y), and it does
 * NOT flex-grow — so the gallery column (flex-grow 1) claims ALL the remaining width.
 * The inline max-width is the mode's % of the row (NOT 100%): the vh·aspect width
 * alone lands at ~half the row on a typical monitor, so this % ceiling is what
 * actually keeps the slab from eating half and starving the gallery (see _SLAB_CAPS —
 * 60vh/34% slim, 74vh/46% full). On a very narrow viewport the gallery wraps below. */
.cb-particles-canvas-col { flex: 0 0 auto; }
.cb-particles-detail-col { flex: 1 1 360px; min-width: 360px; }
/* The LISTS STRIP (11-S2) runs full width ACROSS THE TOP, above the slabs/gallery row.
 * It used to sit at the top of the right column, which pushed the gallery a
 * strip's-height below the slabs — the misalignment the maintainer flagged. */
.cb-lists-strip { min-width: 0; margin-bottom: 8px; }
/* The pick-list workbench header sits ABOVE the selected list's gallery: a
 * compact aligned TABLE of lists (one row each) on the left + a vertical action
 * toolbox on the right. The table replaces the old free-floating pills, which
 * wrapped to ragged, inconsistent widths; every row shares one grid template so
 * the swatch / name / count / extract mark / eye line up into columns. */
.cb-list-top { display: flex; flex-direction: row; align-items: stretch; gap: 8px; width: 100%; }
.cb-ltable {
    flex: 1 1 auto; min-width: 0;
    display: flex; flex-direction: column; gap: 1px;
    background: #fafbfc; border: 1px solid #eef1f6; border-radius: 8px; padding: 3px;
}
.cb-ltable-row {
    display: grid;
    /* swatch · name · picks · ext · path · delete · eye. The Journey's merge-tick column went
     * with 11-S3 and the authoritative radio with the concept itself; the Picks & curation
     * table re-orders these and adds source / origin / job / actions through .cb-ptable-row's
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
/* Vertical toolbox beside the table — since 09-S2 the Journey neither starts nor swaps an
 * ArtiaX session, so this holds the `curate ↗` ROUTE to the Particles registry's Picks &
 * curation tab. Kept OUT of the row so the list table stays clean. */
.cb-list-toolbox {
    flex: 0 0 auto;
    display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 2px;
    padding: 3px 6px; background: #fafbfc; border: 1px solid #eef1f6; border-radius: 8px;
}
.cb-toolbox-link {
    font-size: 10px; color: #6366f1; cursor: pointer;
    text-decoration: underline; text-decoration-style: dotted; white-space: nowrap;
}
.cb-toolbox-link:hover { color: #4338ca; }
.cb-badge-ok { color: #059669; }
.cb-badge-todo { color: #94a3b8; }
.cb-badge-stale { color: #d97706; }
/* The per-list extraction JOB glyph (roadmap 07-S4), in the `job` column next to `ext`:
 * queued/failed take the same two hues the roster's status dot uses for those states
 * (ui/status_indicator._DOT_COLORS), so one job state reads the same colour everywhere. */
.cb-badge-queued { color: #a855f7; }
.cb-badge-err { color: #dc2626; }
/* That mark opens the job's logs, so the whole cell (and the glyph inside it, which
 * .cb-ltable-badge otherwise gives a help cursor) has to read as clickable. */
.cb-ptable-job, .cb-ptable-job .cb-ltable-badge { cursor: pointer; }
/* Species page · Picks & curation (roadmap 11-S2, merged surface 09-S1): the rail table's
 * chrome (.cb-ltable*), one grid per row: swatch · name · source · origin · kept/total ·
 * ext · job · actions (`job` = the live per-list extraction instance, roadmap 07-S4;
 * `origin` = the template+mask the picks came out of, 09-S4). The merge tick went with
 * 09-S3 — creating a merge is the Aggregate-candidates flow's job, not this surface's.
 * Rows are not selectable here (no detail pane), so no pointer cursor / selected state.
 *
 * Every track is fixed or `fr`, and the last one is fixed rather than `auto`: one table is
 * rendered PER TOMOGRAM GROUP, and a content-sized track (`minmax(a,b)` / `auto`) lets each
 * group settle on its own column widths — which is exactly the staggering between groups.
 * `fr` depends only on the container width, which every group shares, so the columns line
 * up across groups and the row always spans the full line. */
/* The table CLAIMS THE LINE. `.cb-ltable`'s `flex: 1 1 auto` only sizes it inside the pick
 * viewer's ROW (.cb-list-top); here it is a direct child of a ui.column, and NiceGUI's
 * .nicegui-column sets `align-items: flex-start` — so without an explicit width the box
 * shrinks to fit, the `fr` tracks resolve against max-content, and every tomogram group ends
 * at its own x. That is the staggering; the grid template below cannot fix it alone. */
.cb-ptable { width: 100%; }
/* The `ext` track is 60px because its header spells `Extracted` out — a 3-letter
 * abbreviation over a column whose whole content is one glyph told nobody what the glyph
 * meant. */
.cb-ptable-row {
    grid-template-columns: 14px minmax(0, 2fr) 72px minmax(0, 1.4fr) 56px 60px 24px 96px;
    cursor: default;
}
/* Actions hug the right edge, so a row visibly ends AT the end of the line. */
.cb-ptable-actions { justify-self: end; }
/* The import affordance, one per tomogram group and present whether or not the group holds
 * lists (a tomogram with no picks is exactly where a de-novo .coords import starts). It sits
 * inside the table box, in the rows' own padding/radius, but deliberately OUT of the
 * 8-column grid: it is one action, not a row of data. Dashed and dim so it reads as the
 * empty slot it is until hovered. */
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
/* Gallery header: title · count on the left, the toolbelt on the right. Everything
 * that used to sit UNDER the grid — sort select, filter selects, a per-tomogram 3dmod
 * block, a per-pick 3dmod block, the hovered-pick info row, the filtered-set path —
 * is a toolbelt icon with a popover now (11-S3). */
.cb-gallery-head { padding: 0 0 2px 0; }
.cb-gallery-count { font-size: 10px; color: #94a3b8; font-family: ui-monospace, monospace; }
.cb-toolbelt { flex-shrink: 0; }
.cb-tb-btn { color: #94a3b8; }
.cb-tb-btn:hover { color: #4338ca; }
/* An armed stateful toolbelt toggle (curation mode) is LOUD: while it is on, clicking
 * a cutout changes what downstream consumes, so the icon must not read like the rest
 * of the row. */
.cb-tb-btn.cb-tb-on {
    color: #ffffff; background: #4338ca;
    box-shadow: 0 0 0 2px rgba(67,56,202,0.22);
}
.cb-tb-btn.cb-tb-on:hover { color: #ffffff; background: #3730a3; }
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
/* The reference strip's collapse line (closed by default). */
.cb-ref-toggle {
    font-size: 9px; color: #6b7280; text-transform: uppercase;
    font-weight: 700; letter-spacing: 0.4px;
    cursor: pointer; padding: 3px 2px; user-select: none; width: max-content;
}
.cb-ref-toggle:hover { color: #4338ca; }
/* The full-page viewer (11-S5): fills the workspace main area and scrolls as one. */
.cb-viewer-page {
    display: flex; flex-direction: column; gap: 6px;
    width: 100%; height: 100%; min-height: 0;
    overflow-y: auto; padding: 8px 10px;
}
.cb-viewer-page-head { padding: 2px 2px 4px 2px; }
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
.cb-plot-row {
    display: flex; gap: 12px; flex-wrap: wrap; margin: 0 0 6px 0;
}
.cb-plot-cell {
    flex: 1 1 340px; min-width: 280px;
    background: #ffffff; border: 1px solid #f1f5f9; border-radius: 4px;
    padding: 6px 6px 6px;
}
.cb-plot-cell-wide { flex: 1 1 100%; min-width: 280px; }
/* One short regular-weight title per chart (the explainer tooltip hangs on it);
 * the only bold text in a tile is the axis names. */
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
/* Denoise-method selector in the denoised pane: a compact segmented toggle
 * (cryoCARE | IsoNet | …) shown only when >1 method has a result for the TS;
 * a single available method renders as the static label below instead. */
.cb-denoise-method-toggle .q-btn {
    font-family: ui-monospace, monospace; font-size: 10px;
    min-height: 18px; padding: 0 7px;
}
.cb-recon-pane-method {
    font-family: ui-monospace, monospace; font-size: 10px; font-weight: 600;
    color: #4f46e5; padding: 0 2px 2px 2px;
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
    width: 4px !important; height: 4px !important;
    background: var(--sp-color, #00e5ff) !important;
    /* Crisp dual ring: solid dark inner + bright white outer. At this tiny
     * size the high-contrast double ring is what makes the dot legible on
     * any tomogram backdrop — not the dot area itself. */
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 2px rgba(255,255,255,0.9) !important;
    transition: box-shadow 0.1s ease, width 0.1s ease, height 0.1s ease, filter 0.1s ease;
}
/* Invisible hit-area so the tiny dot is easy to hover AND click (click toggles
 * keep/drop). Inherits pointer-events:auto from the dot. */
.cb-pick-ghost::after { content: ''; position: absolute; inset: -5px; }
/* Active / hover: GROW + a bright colored backlight glow so the brushed pick
 * reads at a glance on a busy slab (the prior subtle glow was near-invisible,
 * especially for the clip-path triangle whose box-shadow is clipped away —
 * handled separately below via drop-shadow). */
.cb-pick-ghost:hover,
.cb-pick-ghost.cb-ghost-active {
    width: 8px !important; height: 8px !important;
    box-shadow: 0 0 0 1px rgba(0,0,0,1), 0 0 0 2px rgba(255,255,255,1),
                0 0 11px 4px var(--sp-color, #00e5ff) !important;
    z-index: 8;
}
/* SELECTED pick (11-S4): a persistent amber ring that survives the pointer leaving,
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
 * Dots are 3px and lean on the dual ring for legibility, so shapes stay
 * simple (triangle trades the ring for a filled glyph — used only by merged). */
.cb-shape-circle .cb-pick-ghost { border-radius: 50%; }
.cb-shape-square .cb-pick-ghost { border-radius: 0; }
.cb-shape-diamond .cb-pick-ghost { border-radius: 0; transform: translate(-50%, -50%) rotate(45deg); }
/* Triangle (merged lists): clip-path CLIPS box-shadow, so the dual ring above
 * is invisible — the dot was a 4px bare fill nobody could see. Give it more area
 * AND swap the ring for a drop-shadow outline (filter follows the clipped shape),
 * so a merged pick reads on the slab. */
.cb-shape-triangle .cb-pick-ghost {
    border-radius: 0; clip-path: polygon(50% 0, 0 100%, 100% 100%);
    width: 9px !important; height: 9px !important;
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
/* The kept-counter chip, in the gallery's curation bar. (The filter toolbar and the
 * filtered-set path line it used to sit between are gone since 11-S3: the toolbar
 * folded into the curation bar, the path into the 3dmod popover's outputs block.) */
.cb-filter-counter {
    font-family: ui-monospace, monospace; font-size: 10px;
    color: #475569;
    padding: 1px 6px; border-radius: 3px;
    background: #f1f5f9; border: 1px solid #e2e8f0;
}
.cb-filter-counter.cb-filter-dirty {
    color: #b45309; background: #fef3c7; border-color: #fde68a;
}

/* ── Journey header line (ui/dashboard/strip.py) ────────────────────────────
   One thin line about the selected tilt series: [▾ position · i/N] · four
   labelled stage dots · per-species pick counts · ⓘ · exclude — and, at the far
   right (mounted by the journey), the section selector. The name block drops
   down the tomogram switcher table; the overview lives there, not on screen. */
.cb-jhead {
    flex: 0 0 auto; display: flex; align-items: center; gap: 10px; min-height: 30px;
    padding: 0 12px; background: #ffffff; border-bottom: 1px solid #e5e7eb;
    font-family: 'IBM Plex Sans', sans-serif;
}
.cb-jhead-left { flex: 1 1 auto; min-width: 0; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.cb-jhead-right { flex: 0 0 auto; display: flex; align-items: center; }
.cb-jhead-empty { font-size: 10px; color: #94a3b8; }
.cb-jsep { width: 1px; height: 14px; background: #e5e7eb; flex: 0 0 auto; }
.cb-jswitch {
    display: flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none;
    padding: 3px 6px; margin-left: -6px; border-radius: 4px;
}
.cb-jswitch:hover { background: #f1f5f9; }
.cb-jswitch-caret { font-size: 9px; color: #94a3b8; }
.cb-jswitch-pos { font-size: 12px; font-weight: 600; color: #0f172a; white-space: nowrap; }
.cb-jswitch-pos.excluded { text-decoration: line-through; color: #94a3b8; }
.cb-jswitch-idx { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #64748b; white-space: nowrap; }
.cb-jstage { display: flex; align-items: center; gap: 4px; }
.cb-jstage-name { font-size: 10px; color: #475569; white-space: nowrap; }
.cb-jdot { width: 7px; height: 7px; border-radius: 50%; background: #e5e7eb; flex: 0 0 auto; }
.cb-jdot.ok { background: #10b981; }
.cb-jdot.fail { background: #dc2626; }
.cb-jdot.running { background: #f59e0b; }
.cb-jdot.zero { background: #9ca3af; }
.cb-jdot.pending { background: #e5e7eb; }
.cb-jdot.skip { background: repeating-linear-gradient(45deg, #cbd5e1, #cbd5e1 2px, #e5e7eb 2px, #e5e7eb 4px); }
.cb-jhead-species { display: flex; align-items: center; gap: 5px; }
.cb-jsp-dot { width: 7px; height: 7px; border-radius: 50%; flex: 0 0 auto; }
.cb-jsp-name { font-size: 10px; color: #475569; white-space: nowrap; }
.cb-jsp {
    display: inline-flex; align-items: baseline; gap: 5px; padding: 0 4px;
    border-radius: 3px; white-space: nowrap;
}
.cb-jsp-n { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: #1e293b; }
.cb-jsp-kept { font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #059669; }
.cb-jsp.running { background: rgba(245, 158, 11, 0.16); }
.cb-jsp.running .cb-jsp-n { color: #92400e; }
.cb-jsp.fail { background: rgba(220, 38, 38, 0.12); }
.cb-jsp.fail .cb-jsp-n { color: #b91c1c; }
.cb-jsp.zero .cb-jsp-n { color: #94a3b8; }
.cb-jsp.pending .cb-jsp-n { color: #cbd5e1; }
.cb-jexcl { font-size: 14px; color: #cbd5e1; cursor: pointer; }
.cb-jexcl:hover { color: #dc2626; }
.cb-jexcl.on { color: #b45309; }
/* The switcher dropdown: a full table of tilt series — as wide and tall as it needs
   to be (the menu caps it at the viewport), one row per tomogram, totals at the foot. */
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
.cb-jrow.excluded .cb-jname { text-decoration: line-through; color: #94a3b8; }
.cb-jrow.excluded .cb-jsp, .cb-jrow.excluded .cb-jstage { opacity: 0.35; }
.cb-jrow-head, .cb-jrow-total { cursor: default; }
.cb-jrow-head:hover, .cb-jrow-total:hover { background: transparent; }
.cb-jrow-head { border-bottom: 1px solid #e5e7eb; padding-bottom: 5px; }
.cb-jrow-total { border-top: 1px solid #e5e7eb; border-bottom: none; margin-top: 2px; padding-top: 6px; }
.cb-jh { font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: #94a3b8; }
.cb-jh-c { text-align: center; }
.cb-jh-sp { display: flex; align-items: center; gap: 5px; min-width: 0; text-transform: none; letter-spacing: 0; }
.cb-jh-spname {
    font-size: 10px; font-weight: 600; color: #475569;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-jcell { min-width: 0; }
.cb-jcell-idx { font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #94a3b8; }
.cb-jcell-name { display: flex; flex-direction: column; gap: 0; }
.cb-jname { font-size: 11px; font-weight: 600; color: #1e293b; white-space: nowrap; }
.cb-jname-full {
    font-family: 'IBM Plex Mono', monospace; font-size: 9px; color: #94a3b8;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.cb-jrow .cb-jstage { justify-content: center; }
.cb-jtotal-name { font-size: 10px; color: #64748b; }
/* ── Species page (roadmap 10) ─────────────────────────────────────────────── */
/* Segmented control (`ui/components/segmented.py`): the job tab's flat dense
 * switcher as one bordered strip; the active segment is tinted, never a Quasar tab. */
.cb-seg {
    display: inline-flex; align-items: stretch; flex-shrink: 0;
    border: 1px solid #e2e8f0; border-radius: 4px; overflow: hidden; background: #ffffff;
}
.cb-seg-btn {
    font-size: 9px; font-weight: 500; line-height: 16px; padding: 2px 8px;
    color: #64748b; background: #ffffff; cursor: pointer; user-select: none;
    border-right: 1px solid #e2e8f0; white-space: nowrap;
}
.cb-seg-btn { display: inline-flex; align-items: baseline; gap: 5px; }
.cb-seg-btn:last-child { border-right: none; }
.cb-seg-badge { font-size: 10px; font-variant-numeric: tabular-nums; color: #94a3b8; }
.cb-seg-btn.active .cb-seg-badge { color: #64748b; }
.cb-seg-btn:hover { background: #f8fafc; }
.cb-seg-btn.active { background: #f1f5f9; color: #1e293b; font-weight: 600; }
/* Template workbench lists (picking-UI roadmap 05): cards became table rows, so the
 * per-row facts (apix / box / lp, sigma stats, size, polarity) sit in columns sized to
 * their content instead of truncating inside a 260 px card. Only the FILE column flexes.
 * The accent is a custom property set per list on the container (indigo for templates,
 * purple for masks), so one rule serves both. Selection is a class flip — never a
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
/* Row action buttons: the eye and the X used to set the row's height on their own. */
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
/* Species header row: active pill left, segmented tabs right. */
.cb-species-header {
    display: flex; align-items: center; gap: 10px; flex-shrink: 0;
    padding: 5px 10px; border-bottom: 1px solid #e5e7eb; background: #fafbfc; min-height: 30px;
}
/* Species rail: 200 px column, one row per species (pill + counts), "+" at the
 * bottom. Selected row = tinted + a left border in the species color. */
.cb-srail {
    width: 200px; min-width: 200px; flex-shrink: 0; height: 100%; overflow-y: auto; overflow-x: hidden;
    display: flex; flex-direction: column; border-right: 1px solid #e5e7eb; background: #fafafa;
}
.cb-srail-row {
    display: flex; align-items: center; gap: 6px; padding: 6px 8px 6px 7px;
    border-bottom: 1px solid #f1f5f9; border-left: 3px solid transparent; cursor: pointer;
    min-width: 0;
}
.cb-srail-row:hover { background: #f8fafc; }
.cb-srail-row.selected { background: #ffffff; }
.cb-srail-meta {
    margin-left: auto; font-size: 9px; color: #94a3b8; white-space: nowrap; flex-shrink: 0;
    font-family: ui-monospace, monospace;
}
.cb-srail-add {
    display: flex; align-items: center; gap: 4px; padding: 7px 10px; cursor: pointer;
    font-size: 10px; color: #6366f1; border-bottom: 1px solid #f1f5f9;
}
.cb-srail-add:hover { background: #eef2ff; }

/* ── App-wide control chrome ───────────────────────────────────────────────── */
/* Lives HERE, not in main_ui's add_head_html: that block is baked into the page
 * shell, which is served stale on this deployment (2026-08-21 incident — new
 * shell CSS never reached the browser). ensure_assets_loaded() injects per
 * client AFTER connect, over the socket, so these rules always arrive fresh.
 * Structural styling stays INLINE on the elements (house_button, _stacked_panels);
 * this file only carries what inline styles cannot express. */

/* Native <input type=number> spin arrows OFF: every numeric field is a typed
 * value, not a click-to-increment counter, and the browser arrows were the
 * loudest, least stylable chrome on any form. Pseudo-elements — CSS-only. */
input[type=number]::-webkit-outer-spin-button,
input[type=number]::-webkit-inner-spin-button { -webkit-appearance: none; appearance: none; margin: 0; }
input[type=number] { -moz-appearance: textfield; appearance: textfield; }

/* House button hover states (ui/components/buttons.py — the base look is inline
 * on the element, so hover needs !important to override it). */
.q-btn.cb-btn:hover { background: #f1f5f9 !important; border-color: #94a3b8 !important; }
.q-btn.cb-btn--accent:hover { background: #1e293b !important; border-color: #1e293b !important; }
.q-btn.cb-btn--danger:hover { background: #fef2f2 !important; border-color: #f87171 !important; }

/* THE .cb-select / .cb-field control chrome — single source since the shell copy
 * in ui/main_ui.py was retired (2026-08-21, picking-UI roadmap 08 S1). Every page
 * calls ensure_assets_loaded(). Builders: ui/components/fields.py (house_*) at the
 * .cb-field scale; ui/job_plugins/_field_styles.py at the .cb-select scale. */
/* `height`, not just min-height: Quasar pins dense controls at a FIXED height of
 * 40px (.q-field--dense .q-field__control) — min-height alone never shrank them. */
.cb-select .q-field__control,
.cb-field .q-field__control {
    height: 20px; min-height: 20px; padding: 0 5px;
    border: 1px solid #e2e8f0; border-radius: 4px;
    background: #fff; transition: border-color .12s ease;
}
.cb-select .q-field__control:hover,
.cb-field .q-field__control:hover { border-color: #cbd5e1; }
.cb-select.q-field--focused .q-field__control,
.cb-field.q-field--focused .q-field__control { border-color: #94a3b8; }
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
/* ── Tomogram gallery (ui/tomo_gallery.py) ─────────────────────────────────── */
/* The birds-eye wall: a grid of reconstruction tiles, each an image box with the
 * species' picks projected onto it as one inline-SVG layer per species. The frame
 * IS the image box (aspect-ratio from the tomogram's own X/Y extent, object-fit
 * fill), which is what lets a dot layer position in plain percentages and stay
 * registered with the slice at every tile size. */
.cb-gal-page { display: flex; flex-direction: column; width: 100%; height: 100%; min-height: 0; }
.cb-gal-toolbar {
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    padding: 6px 10px; border-bottom: 1px solid #e5e7eb; background: #f8fafc; flex-shrink: 0;
}
.cb-gal-title {
    font-size: 10px; text-transform: uppercase; font-weight: 600;
    color: #64748b; letter-spacing: 0.3px;
}
.cb-gal-count {
    font-family: ui-monospace, monospace; font-size: 10px; color: #1e40af;
    background: #eef2ff; border-radius: 3px; padding: 0 5px;
}
.cb-gal-toolbar-label {
    font-size: 9px; text-transform: uppercase; letter-spacing: 0.06em;
    color: #94a3b8; margin-left: 6px;
}
.cb-gal-sp {
    display: inline-flex; align-items: center; gap: 4px; cursor: pointer; user-select: none;
    border: 1px solid #e2e8f0; border-radius: 999px; background: #ffffff; padding: 1px 8px 1px 5px;
}
.cb-gal-sp:hover { background: #f1f5f9; }
.cb-gal-sp.off { opacity: 0.45; }
.cb-gal-sp.off .cb-gal-sp-dot { background: #cbd5e1 !important; }
.cb-gal-sp-dot { width: 8px; height: 8px; border-radius: 50%; flex: 0 0 auto; }
.cb-gal-sp-name { font-size: 10px; color: #475569; white-space: nowrap; }
.cb-gal-body { flex: 1 1 0; min-height: 0; overflow-y: auto; overflow-x: hidden; }
.cb-gal-grid { display: grid; gap: 10px; padding: 10px; align-items: start; }
.cb-gal-tile {
    display: flex; flex-direction: column; min-width: 0;
    border: 1px solid #e5e7eb; border-radius: 5px; overflow: hidden; background: #ffffff;
}
.cb-gal-tile:hover { border-color: #c7d2fe; box-shadow: 0 2px 8px rgba(15,23,42,0.08); }
.cb-gal-frame {
    position: relative; width: 100%; min-height: 60px;
    background: #0f172a; cursor: zoom-in; overflow: hidden;
}
.cb-gal-frame > img { width: 100%; height: 100%; object-fit: fill; display: block; }
/* Unknown tomogram extent: no aspect-ratio to give the frame a height, so the image
 * sizes itself and the tile is as tall as it is. No dots are drawn in this state
 * (nothing to normalize against), so letterboxing can't desynchronize anything. */
.cb-gal-frame-auto { aspect-ratio: auto; max-height: 78vh; }
.cb-gal-frame-auto > img { height: auto; object-fit: contain; }
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
    border-top: 1px solid #f1f5f9; cursor: pointer; min-width: 0;
}
.cb-gal-cap:hover { background: #eef2ff; }
.cb-gal-cap:hover .cb-gal-go { color: #4f46e5; }
.cb-gal-name {
    font-size: 10px; color: #334155; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1;
}
.cb-gal-go { font-size: 10px; color: #cbd5e1; flex: 0 0 auto; }
/* Zoom: one tile at viewport scale. Sized here (not maximized) so the tomogram keeps
 * its aspect and the wall behind stays visible as context. */
.cb-gal-zoom {
    background: #ffffff; border-radius: 6px; padding: 8px;
    display: flex; flex-direction: column; gap: 6px;
    width: 92vw; max-width: 1500px;
}
.cb-gal-zoom-head { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.cb-gal-zoom-title { font-size: 12px; font-weight: 600; color: #0f172a; }
.cb-gal-zoom-ts { font-family: ui-monospace, monospace; font-size: 10px; color: #64748b; }
.cb-gal-zoom-src { font-family: ui-monospace, monospace; font-size: 10px; color: #94a3b8; }
/* Height is bounded by capping the WIDTH against the aspect (inline, where the dims
 * are known) — a max-height would clamp the box out of ratio and object-fit:fill
 * would then squash the slice. */
.cb-gal-zoom-frame { cursor: default; border-radius: 4px; margin: 0 auto; }
.cb-gal-zoom-path {
    font-family: ui-monospace, monospace; font-size: 9px; color: #94a3b8;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
/* ── Curation-session indicator (bottom of the primary sidebar) ─────────────── */
/* Dim by default; when a ChimeraX + ArtiaX session of this user is up it goes green and
 * breathes. The pulse is a CSS animation, never a server tick (CLAUDE.md), and it is
 * deliberately shallow — a rail icon that flashes is noise, one that breathes is status. */
@keyframes cb-artiax-breathe {
    0%, 100% { opacity: 0.62; }
    50%      { opacity: 1; }
}
.cb-artiax-live { animation: cb-artiax-breathe 2.6s ease-in-out infinite; }
/* The protocol light (roadmap 16) breathes the same way while a protocol chain is live. */
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
/* Log views (job Logs tab): wrap long lines instead of scrolling sideways. */
.cb-log .q-scrollarea__content { white-space: pre-wrap; overflow-wrap: anywhere; }
"""


def ensure_assets_loaded() -> None:
    """Inject the stylesheet once per client. The Species page (built with the workspace)
    and the Journey / tomogram-import dialog (built later, lazily) share it, so a second
    caller on the same page must not append a second copy to the head."""
    client = context.client
    if getattr(client, "_cb_assets_loaded", False):
        return
    client._cb_assets_loaded = True
    ui.add_head_html(f"<style>{_CB_CSS}</style>")
