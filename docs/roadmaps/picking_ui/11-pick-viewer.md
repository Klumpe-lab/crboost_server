# 11 — The pick viewer: full page in the registry, slim in the Journey

**Status:** CODE-COMPLETE 2026-08-22, S1–S6, PENDING RUNTIME (§4).
**Risk:** medium-high (extracted ~3.6k lines out of `tomo_dashboard_dialog.py`; a new interaction
model). **Depends on:** 09 for the registry home of the full page. Independent of 10.

**Peeves (maintainer, 2026-08-21):** *"we got a lot of things right about the picks/cutouts gallery,
but as part of the journey it is limited in size and disordered — filters, absolute-path callouts,
3dmod commands and the filtering functionality bolted on top. Rework it as a simpler page with
full-screen blowup… persist in Journey for display and filter only, but the meat moves to the
Particles registry's Picks & curation tab: select a tomogram → the full-page version — tomo slices
plus the cutout gallery IF subtomos were extracted, or just the pick locations if not."* Plus the
five itemized peeves — kitchen sink, click-model, info row, reference strip, lists-above.

## 0. Facts (gathered 2026-08-21; all in `ui/tomo_dashboard_dialog.py` unless noted)

1. **The element is welded into a 5890-line file.** Particles section `_render_particles_section`
   `:3579` → slabs LEFT (`_render_particles_canvas` `:3678`), species tabs + list rail + gallery
   RIGHT (`:3652-3668`, `_render_list_rail` `:4049`, `_render_gallery_body` `:4536`). The rail sits
   at the TOP OF THE RIGHT COLUMN, so the gallery starts a rail's-height below the slabs — the exact
   misalignment the maintainer flags.
2. **3dmod appears twice**: the per-pick "3D peek" block (`_render_peek_skeleton` `:4420-4461`,
   shift-click → `3dmod <subvol>` at `:4528`) and the per-tomogram command section
   (`_render_3dmod_section` `:4310-4342`) at the very bottom. Shift-click hints appear in two more
   places (`:4667`, `:5026`).
3. **The info row** is the horizontal hover card (`_render_hover_card_skeleton` `:5471-5499`, keys
   `idx · px · ang · score · z%-tile · nn`), first thing in the controls box (`:4678`).
4. **Click = keep/drop immediately.** `_on_tile_click` `:4985-4991` → `_toggle_keep` `:4918-4932`;
   ghost-dot clicks and the lasso feed the same toggle (`:4934-4941`, `:5078-5101`). **There is no
   selection concept** — `.cb-gallery-tile.selected` exists in CSS (`css.py:170-173`) and nothing
   ever applies it; `state["selected_idx"]` is only a stale-async guard for the peek.
5. **Reference strip** (`_render_reference_strip` `:4345-4417`) renders template + worst-4 anchors,
   always visible. There is **no best-scored group** yet.
6. **No full-screen affordance exists** anywhere in the gallery; the grid is capped at
   `max-height: 68vh` (`css.py:492`), slabs at `max-width: 34%` (`:3636`), tiles hard-coded 96 px.
7. **Two gallery backends**: auto lists → atlas manifest gallery (Save/Reset dirty model,
   `:4767-4786`); workbench lists → recon contact sheet with auto-commit per click (`:2506`,
   `:2631-2656`). One chrome must front both.
8. **Cross-links already exist** and stay: Journey "manage in Particles registry ↗" (`:3597-3614`),
   registry `journey ↗` (`picks_tab.py:273`), deep-scroll to the particles section (`:693-701`).

## 1. End state

**One component, two mounts.**

- **Full page** (the meat): from the Picks & curation tab, selecting a tomogram opens the viewer
  filling the workspace main area (same swap mechanism as the Journey, not a dialog). Layout:
  - **Lists strip** — species tabs + list table — full-width ACROSS THE TOP.
  - Below it, one row: **slabs LEFT** (bigger than today — no 34% cap here), **gallery RIGHT**,
    top edges aligned.
  - Gallery **toolbelt** (one icon row on the gallery header): sort · display filter · 3dmod ·
    curation-mode toggle · fullscreen · collapse-refs. The bottom kitchen sink is gone.
  - **Reference strip**: thin collapsible line above the grid ("references ▸"), CLOSED by default;
    expanded it shows template + best-4 + worst-4 anchors.
  - **Not-extracted state**: no cutouts yet → slabs with coordinate markers + the list table still
    work; the gallery pane says "no subtomograms extracted for this list" with the per-list extract
    action (09's verb, one place). Score-only filtering still available.
- **Journey slim mount** (display + filter only): same component, `mode="slim"` — slabs + gallery +
  display filters + selection; NO curation mode, NO extraction/import/session actions; a
  `full viewer ↗` link in its header. The Journey keeps its per-TS context (the other sections).
- **Interaction model** (both mounts): click a tile or a slab dot → **SELECT** (persistent highlight
  on tile + dot, Esc or click-away deselects; selection shows score/coords in the tile's tooltip
  region, replacing the deleted info row). **Curation mode** is an explicit toolbelt toggle; only
  while ON does click mean keep/drop and the lasso arm. Shift-click = 3dmod peek in both modes.

## 2. Stages

**S1 — extraction (pure move).** The particles section (`:3579-…`, the gallery bodies, peek, refs,
hover JS) moves to `ui/particles/pick_viewer.py` with a `mode: "slim" | "full"` flag; the Journey
mounts it where the old section was. Zero behavior change; the JS bridges (`fillHover`, lasso,
ghost-dot events) move verbatim. One commit, mechanical, reviewed as a move.

**S2 — layout reshuffle.** Lists strip to the top, full width; slabs/gallery row with aligned tops;
slab width cap becomes mode-dependent (34% slim, ~45% full); reference strip → collapsible closed by
default, add the best-4 group next to worst-4 (`sorted(indices)[:4]` — index IS score rank);
tomogram-wide normalization stays (feedback of record: gallery calibration anchors).

**S3 — kitchen-sink dissolution.** Delete the hover info row (skeleton + `fillHover` + meta
plumbing, fact 3). 3dmod becomes ONE toolbelt icon: its popover holds the per-tomogram command
(from `_render_3dmod_section`, which is deleted) and, when a pick is selected/peeked, the per-pick
subvolume command (from the peek block, also deleted as a standalone element); copy buttons live in
the popover. The filtered-set path row moves into the same popover ("outputs"). Sort + display
filter become toolbelt icons with the existing controls in their popovers.

**S4 — selection model + curation mode.** New `selected` state wired to the dormant
`.cb-gallery-tile.selected` CSS and a dot highlight; Esc/click-away clears. Curation-mode toggle
(default OFF) gates `_toggle_keep`, the ghost-dot toggle, and the lasso; the Save/Reset pair (auto
lists) and auto-commit (manual lists) keep their current per-backend persistence but only apply in
curation mode. Mode state is per-list, visually loud (toolbelt toggle tinted while armed).

**S5 — the full page.** `mode="full"` mount in the workspace main area, opened from the merged tab's
per-tomogram `viewer ↗` (09-S5) with (species, tomo) preselected; back-link to the registry row and
`journey ↗`. Not-extracted state as in §1. Browser-fullscreen button on the toolbelt
(`requestFullscreen` on the viewer root) plus a slab lightbox: click a slab → full-viewport overlay
of that slab with markers, wheel = Z, Esc closes.

**S6 — Journey slim-down.** `mode="slim"` hides curation mode, extraction verbs, import, session
affordances (whatever 09 left); keeps display filters, selection, peek, links. The Journey's
particles section header keeps "manage in Particles registry ↗" and gains `full viewer ↗`.

## 3. Non-goals

- No napari, no browser volume rendering (decisions of record) — slabs and cutout PNGs only.
- No change to atlas/recon-cutout generation, manifest schema, or keep/drop persistence formats.
- The score-histogram/manual-filter expansion (concern (2)'s "manual filters, e.g. score") beyond
  what exists — worth its own slice once this layout is real.

## 4. Runtime checklist (maintainer)

1. Journey: particles section renders as before S1 (move-only parity), then post-S2 with lists on
   top and aligned columns.
2. Click a cutout: it selects (stays highlighted, dot highlighted); Esc deselects; nothing changes
   in the kept/dropped sets. Enable curation mode: click now toggles keep/drop; lasso works; Save
   persists; disable → clicks select again.
3. 3dmod: one icon; per-tomogram command present always; per-pick command after a shift-click; both
   copy. No 3dmod prose anywhere else.
4. References collapsed by default; expanding shows template + best-4 + worst-4.
5. Registry → tomogram → `viewer ↗` → full page: opens on the species whose row you clicked, slabs
   noticeably larger, fullscreen works, slab lightbox opens on a slab click with its pick dots and
   wheel-zooms / drag-pans / Esc-closes. A workbench list with no extraction shows the `○ not
   extracted` line + an Extract button above its contact sheet.
6. Links round-trip: registry row → full page → `← Particles registry` lands back on Picks & curation;
   full page → `journey ↗` selects the TS and scrolls to Particles; Journey → `full viewer ↗` returns.
7. Species switching: flip tabs quickly on a multi-species tomogram — the gallery under the slabs
   must always be the species whose tab is lit (the generation guard), and the roster must be hidden
   in the viewer and back when you leave it.
8. `ruff check .` + `python check_boundaries.py` + `python -c "import main"`.

## 5. Log

- 2026-08-21 — scoped. Root facts: rail-above-gallery is why the tops misalign; no selection concept
  exists (the CSS class is dormant); 3dmod is two elements + two hints; the info row is the
  horizontal hover card; no fullscreen affordance exists; two gallery backends must share one chrome.

- 2026-08-22 — **S1 (move) landed.** `ui/particles/pick_viewer.py`, 4.3k lines;
  `tomo_dashboard_dialog.py` 5775 → 2113. The bulk is byte-identical to what it replaced (verified
  by diffing the extracted range against the original). The cut is bigger than the stage text
  implies because nothing outside the particles section used these: the recon-slab renderer
  (`_recon_slab_paths` / `_auto_kick_recon_slabs`), the polarity Invert switch, the whole
  candidate-extract preview + IMOD generation tail (`_collect_tomo_rows_for_instance`,
  `_auto_kick_preview_generation`, `_auto_kick_imod_generation`, both `_handle_generate_*`), and the
  three read helpers at the top of the file. `reset_auto_kick_state` moved with the four dedup sets it
  clears and is re-imported by name, so the dependency is one-directional: the dashboard imports the
  viewer, never the reverse. Deletions in the same commit, both dead: `_LIST_TYPE_TAG` (the rail became
  a table in 09-S2 and stopped rendering a type tag) and the orphaned "Per-tilt star helpers" banner
  that headed the block being moved out.

- 2026-08-22 — **S2 (layout) landed.** Lists strip full width across the top, then one row with the
  slabs left and the gallery right, tops aligned. **Deviation:** `ui.tab_panels` is gone. It built
  every species' rail AND gallery up front and kept the inactive ones out of the DOM (the reason the
  hover bridge resolves its grid lazily); the strip cannot span full width from inside a panel. The
  species tabs now drive ONE shared `lists_host` + `detail_host` pair via `on_value_change`. That
  introduces a race the panels made impossible — the detail pane renders one tick late off a
  `once`-timer, so two fast species switches could let the first paint into the second's pane — closed
  with a generation counter (`is_current`). `_SLAB_MAX_VH`/`_SLAB_MAX_PCT` became `_SLAB_CAPS`:
  60vh/34% slim (unchanged), 74vh/46% full. Reference strip is collapsible, CLOSED by default, and
  gained the best-4 group beside worst-4 (index IS score rank); its open/closed flag lives in the
  gallery's `state` so a display-filter switch — which re-renders the strip — doesn't slam it shut.

- 2026-08-22 — **S3 (kitchen sink) landed.** The horizontal hover info row, `fillHover` and
  `_build_pick_meta_for_js` are deleted; each tile's own `title` carries idx/score/x/y/z, which is
  what §1 means by "the tile's tooltip region". **Deviation:** the VERTICAL hover card survives for
  the scatter fallback — a scatter point has nowhere else to say what it is — so
  `_render_hover_card_skeleton` lost its `horizontal` parameter rather than the whole function.
  3dmod is one toolbelt icon whose popover holds the per-tomogram command, the per-pick subvolume
  command (auto lists only) and the filtered-set path under `outputs`; `_render_3dmod_section` and
  `_render_peek_skeleton` are gone and no 3dmod prose survives anywhere else. Sort and display filter
  became toolbelt popovers. **Deviation:** "collapse-refs" is NOT a toolbelt icon — the reference
  strip's own inline `references ▸` caret is the affordance, and a second control for it would be two
  places to do one thing. **Deviation (scope up):** the workbench contact sheet got the same toolbelt,
  because fact 7 asks for one chrome fronting both backends and leaving 3dmod off it would have
  deleted the per-tomogram command for manual/imported/merged lists.

- 2026-08-22 — **S4 (selection + curation mode) landed.** `state["selected"]` drives the dormant
  `.cb-gallery-tile.selected` plus a new `.cb-pick-ghost.cb-ghost-selected` on the slab; Esc and a
  click on empty gallery space clear it (one delegated handler per client, in `_MARQUEE_JS`).
  Curation mode is a toolbelt toggle, OFF by default, and gates `_toggle_keep`, the ghost-dot click
  and the marquee — which now refuses to start without `.cb-curating` on the grid, so a drag over a
  look-only gallery behaves normally. `_on_dot_toggle` became `_on_dot_click` and the custom event
  `cbpicktoggle` became `cbpickclick`, since the dot now does whatever a tile does. **Deviation:**
  `.cb-gallery-tile.selected` was recolored indigo → amber to match the slab ring; indigo reads as
  the hover/highlight family the tile already uses. **Deviation (scope up):** the workbench sheet got
  selection AND box-select — its keep/drop was per-tile-per-commit, which is unusable on a
  hand-picked list of any size.

- 2026-08-22 — **S5 (full page) landed.** `PickViewerPage` in `ui/particles/pick_viewer.py`, mounted
  as a 4th workspace container (`ui/workspace_page.py`), opened by `open_pick_viewer(species, tomo)`
  from the Picks & curation group's new `viewer ↗` (which replaced the 09-S5 placeholder comment
  beside `journey ↗`; both links now exist, they answer different questions). The roster hides for
  `viewer` like it does for `journey`. Header carries `← Particles registry` (composed the way the
  Journey composes it: select species AND land on Picks & curation) and `journey ↗`. Refresh is
  coalesced through a flag + 0.2 s timer, like the Journey's, because the auto-kick background
  renders call it with no client context. **Deviation:** the slab lightbox is wheel = ZOOM, drag =
  pan — not "wheel = Z". There is no Z stack to scroll: `render_xy_slab_preview` writes ONE
  central-Z-average PNG per tomogram, and a render per wheel tick is both a background job per tick
  and against the no-browser-volume-rendering decision. The overlay is a DOM clone of the clicked
  slab with every id stripped, so the pick markers come along and the hover bridge never targets the
  copy. **Deviation:** the not-extracted extract verb lands on WORKBENCH lists only. The auto list is
  not extracted per list — that is the registry's own rule (`picks_tab._render_actions`: "the auto
  list follows its subtomo job") — so its scatter fallback keeps saying extraction hasn't run instead
  of growing a button that would contradict the registry.

- 2026-08-22 — **S6 (Journey slim-down) landed.** `mode="slim"` hides the curation toggle in both
  backends (so no Save/Reset, no auto-commit, no lasso) and the per-list extraction verb; display
  filters, selection, peek, the eyes and every cross-link stay. Saved curation is still SHOWN there —
  dropped tiles grey, dropped dots grey — it just can't be changed. The Particles header keeps
  `manage in Particles registry ↗` and gains `full viewer ↗` on the active species.

- **Still owed:** §4 runtime, in one sitting with 08's §4 and 09's §4. `ruff check .` is green;
  `python -c "import main"` and `python check_boundaries.py` could not run in the assistant sandbox
  (no interpreter on the mounted path).
