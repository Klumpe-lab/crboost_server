# Roadmap 35 — Views you can resize, and a quiet Journey

**Status:** scoped 2026-10-17 from reading main (`cb8d419`); §3 decided 2026-10-17; S1–S2 built 2026-10-17 in the
main tree, not run (§7). S1–S2 are the workspace, S3–S5 the Journey.

## Asked (maintainer, 2026-10-17)

- The views turned on from the rail (Jobs, Tomograms, Journey, …) sit side by side: make them resizable
  against each other.
- The close cross in a view's top-right corner sits in a vertical gutter that runs the height of the view and
  pushes everything left. The gutter goes.
- Rework the Journey in the style of the Tilts tab: modest greys, highlights never too much, almost no outlines
  or borders, small icons without much colour, orderly fonts. The Journey today mixes fonts, checkboxes and
  dropdowns; its charts come in different widths and heights, all too large; its dots look like a crypto
  dashboard or Material UI.
- Build three passes of that style with a tiny dropdown at the top of the Journey to flip between them, so the
  maintainer can point at a direction. The dropdown is temporary.

## Before → After

**Before:** the views that are on share the main area equally and nothing changes that. Every view carries a
16 px full-height gutter on its right for its close cross, with a faint rule of its own, so content stops 17 px
short of the edge. The Journey is the busiest page in the app: four font families, Quasar toggles, a switch,
outlined selects, an expansion item and a slider beside the house controls; 240 px charts whose width depends on
how many share a row; Material indigo, amber, Viridis and neon dots.

**After:** the boundary between any two views drags; widths survive a reload and closing a view. The close cross
floats in the frame's corner and content runs to the edge. The Journey speaks the Tilts tab's language: one font
pair on a four-step scale, house controls only, one chart tile size, grey markers, colour only where something
is wrong. It comes in three passes behind a temporary `style` dropdown; the maintainer picks one (or a mix) and
the dropdown and the other two go.

---

## 1. Facts (main `cb8d419`, 2026-10-17, from reading)

### 1.1 Views side by side

- Every view's frame is `flex: 1 1 0`, an equal share (`ui/workspace_page.py:72-75`). `_apply_views` restyles
  each frame on every toggle: `display`, `order`, `border-left` (`:134-152`).
- `_view_frame` builds the view's body and, on its right, `.cb-view-gutter` holding the cross (`:213-226`). The
  gutter is `flex: 0 0 16px` with `border-left: 1px solid #f1f5f9` and a white fill (`ui/dashboard/css.py:1530-1535`):
  a full-height column in every view.
- Precedent for dragging: the roster divider. `_ROSTER_RESIZER_JS` (`ui/workspace_page.py:15-53`) clamps to
  240–720 px and keeps `cbRosterW` in `localStorage`; a 0.1 s one-shot timer wires it (`:541`). Its CSS sits in the
  page-shell block (`ui/main_ui.py:110-124`), which is served stale; new rules go to `ui/dashboard/css.py`
  (CLAUDE.md, "CSS delivery").
- **JS cannot keep a width on a frame.** NiceGUI 3.0.3 sends an element's style as one CSS string
  (`nicegui/static/nicegui.js:156`), so every server `.style()` call replaces the frame's whole inline style in
  the browser: a width written by JS is gone at the next toggle. `main_area` (`ui/workspace_page.py:480-482`) is
  styled once and never again, so custom properties JS sets on it survive, and the frames inside inherit them.
- ECharts follows its own box (a `ResizeObserver`, `nicegui/elements/echart/echart.js:72-79`). Plotly's
  responsive mode (`nicegui/elements/plotly/plotly.vue:20-23`) follows window resizes, so the Particles section's
  Plotly charts need a window `resize` event after a drag.

### 1.2 The Journey's fonts

- The app loads IBM Plex Sans and IBM Plex Mono (`main.py:200-202`); the page body is Plex Sans
  (`ui/main_ui.py:43`).
- `ui/dashboard/css.py` uses three families: `ui-monospace, monospace` (38 rules), Plex Sans (13) and Plex Mono
  (8). In the Journey, stat values and chart ticks are Plex Mono, while datadump values, the drop list and the
  pixel table are `ui-monospace` (the browser's default monospace wherever that generic is unsupported; not
  checked in a browser here).
- The Particles section's Plotly charts set no `font` in their layouts (`ui/dashboard/figures.py`, the scatter and
  histogram builders, e.g. `:97-98`, `:171-172`, `:229-230`): Plotly's default family at 12 px, a fourth family.
- Sizes on one Journey page: 9, 10 and 11 px from the house rules; 12 px (`text-xs`, ×15) and 14 px (`text-sm`)
  from Tailwind in the Particles section, next to `text-[9px]`/`[10px]`/`[11px]` and 8 inline `font-size`s
  (`ui/particles/pick_viewer.py`); Plotly's 12 px; bold 10 px axis names (`figures.py:482`, `:645`).
- `.cb-section-title` titles the sections and also captions the Particles plots (`pick_viewer.py:3758-3775`),
  while the ECharts tiles use `.cb-plot-label` (10 px regular, `css.py:577-580`).

### 1.3 The Journey's controls

The section switch is `Segmented` (`ui/tomo_dashboard_dialog.py:149-162`) and the buttons are `house_button`.
Everything else is Quasar's own look:

| Control | Where | Look |
|---|---|---|
| `ui.toggle` ×2: recon version, denoise method | `tomo_dashboard_dialog.py:2178-2183`, `:2217-2224` | Material button group, `toggle-color=indigo` |
| `ui.switch("Invert")` | `pick_viewer.py:353-360` | Material switch, `color=indigo-6` |
| `ui.select` ×3, `outlined`, `text-xs`: score colouring, sort | `pick_viewer.py:501-512`, `:3221-3226` | outlined Material fields at 12 px |
| `ui.checkbox`: exclude not-extracted | `pick_viewer.py:3170-3172` | default checkbox |
| `ui.expansion`: dropped tilts | `tomo_dashboard_dialog.py:1900` | Material expansion item |
| `ui.slider`: CTF-fit scrubber | `tomo_dashboard_dialog.py:1366-1376` | default (primary) slider |
| `ui.tabs`: species | `pick_viewer.py:1996-2000`; `css.py:437-456` | indigo indicator, `#eef2ff` fill, `#4338ca` label |
| icon buttons: render previews, IMOD | `pick_viewer.py:2181-2208` | `text-purple-600`, `text-blue-600`, `text-gray-500` |
| spinners | `tomo_dashboard_dialog.py:415`, `:475` | `color="indigo"` |

The Tilts tab, the model, uses only: 9 px grey uppercase toolbar labels, `Segmented` strips, one `.cb-field`
select with `cb-select-popup`, `house_button`s, a `house_button` menu of dense `size=xs` checkboxes, and icon-only
legend marks explained on hover (`ui/tilt_previews.py:1210-1233`, `ui/tomo_gallery.py:621-640`; CSS
`css.py:991-1007`, `:1204-1219`).

### 1.4 The Journey's charts

- Every ECharts tile is 240 px tall: the `_plot_cell` default (`tomo_dashboard_dialog.py:1039-1067`) and the
  CTF-fit panel (`:1380`). Width comes from `.cb-plot-row` wrapping `.cb-plot-cell { flex: 1 1 340px;
  min-width: 280px }` (`css.py:566-574`): a chart alone in its row spans the pane, two share it, three take a third
  each. The same kind of chart comes out at different widths in different sections. `wide` and
  `.cb-plot-cell-wide` have no caller.
- Particles: the X/Y and X/Z scatters in aspect-ratio boxes and a 160 px score histogram, all Plotly
  (`pick_viewer.py:3756-3775`).
- Markers: the first series of every per-tilt chart is indigo `#4f46e5`, then `#0f172a` and `#94a3b8`
  (`SERIES_PALETTE`, `figures.py:322`); `symbolSize` is 6 by default (`:582`) and 7 where callers pass it. Motion
  trajectories run indigo `#3730a3` → amber `#b45309` by signed tilt (`:510`). The pick scatter colours by Viridis
  or amber `#fbbf24` (`:53-64`, `:134-138`); the histogram is indigo `#4338ca` (`:220`).
- "Mean frame motion" draws `lines+markers` (`tomo_dashboard_dialog.py:1508`), against the per-tilt rule of
  markers only.
- `.cb-section-card` is a white box with a 1 px `#e5e7eb` border (`css.py:107-110`), and each chart sits in a
  second box with its own 1 px `#f1f5f9` border (`:569-573`): boxes in boxes.

### 1.5 Dots that are not charts

- Species colours default to Material A400 accents (`#ff1744`, `#00e5ff`, `#ffea00`, `#d500f9`, `#76ff03`,
  `#2979ff`; `services/models_base.py:296-302`) and are stored per species. They sit on the dark slabs, where the
  contrast is needed, and on light chrome: the header's species dots (`.cb-jsp-dot`, `css.py:936`) and the species
  tabs' dots (`css.py:454-456`).
- The header's stage dots have colours of their own (`css.py:928-934`): running is amber `#f59e0b`, while the
  roster's running light is blue `#3b82f6` (`ui/status_indicator.py:8-15`).
- The ⓘ beside a section title turns indigo on hover (`css.py:115-116`).

---

## 2. Stages

### S1 — The close cross without a gutter

- Delete `.cb-view-gutter`. The frame gets `position: relative`; the cross is placed absolutely in its top-right
  corner, centred on a 30 px header line, grey `#cbd5e1`, `#475569` on hover, as today.
- Each view's first row keeps the cross's footprint free on its right (`padding-right` about 22 px): the
  Journey's `.cb-jhead` (`css.py:908-912`), Tomograms' `.cb-gal-toolbar` (`css.py:1204-1207`), and the first rows
  of the job page, the Particles registry, Protocols, the pipeline table and the pick viewer (found in this stage).
- *Success:* in every view the content reaches the frame's right edge and nothing sits under the cross.

### S2 — Resizable views

- Each frame gets `id=cb-view-<name>` and `flex: var(--cb-vw-<name>, 1) 1 0` in place of `flex: 1 1 0`
  (`_VIEW_STYLE`). The weights are custom properties on `main_area` (§1.1).
- A splitter: a 6 px hit strip on the left edge of every frame that is not the leftmost one showing.
  `_apply_views` already knows which frame is first (`ui/workspace_page.py:139-147`); it adds or removes a class
  (`.classes()` leaves the style alone). Same look as the roster divider; the CSS goes in `ui/dashboard/css.py`.
- Client JS, wired once like `_ROSTER_RESIZER_JS`. Mousedown on a splitter finds the visible frame to its left by
  computed `order`. Mousemove sets both weights on `main_area` so that the boundary follows the pointer, their
  sum stays put and no frame drops below 260 px. Mouseup stores both weights in `localStorage`
  (`cbViewW:<name>`) and dispatches one window `resize` for Plotly (§1.1). A double-click resets the pair to equal
  shares. Wiring applies every stored weight.
- *Success:* drag the boundary between Tomograms and the Journey, then reload: same widths. Close Tomograms and
  reopen it: same width. The Journey's charts re-fit on release.

### S3 — The Journey's baseline: one vocabulary

Every pass builds on this; none of it is a matter of taste.

- **Fonts.** Plex Sans for text; Plex Mono for numbers, paths and frame names; nothing else inside the Journey.
  Journey-scoped rules replace `ui-monospace`; the Plotly layouts get Plex Sans at 9–10 px; the Particles
  section's Tailwind sizes move onto the scale: 12 px titles, 10 px labels and body, 9 px ticks, captions and
  meta, 11 px Plex Mono stat values. No weight above 600.
- **Controls.** The two `ui.toggle`s and the Invert switch → `Segmented`. The outlined selects → `.cb-field`
  selects with `cb-select-popup`, like the Tilts toolbar's sort. The dropped-tilts `ui.expansion` → the Dataset
  section's caret header (`tomo_dashboard_dialog.py:955-989`). The slider → slate, a 2 px track, a small thumb.
  Icon buttons grey (`#94a3b8`, `#475569` on hover); spinners grey; species tabs with a slate indicator and label
  and no tinted fill; the ⓘ hover slate. The pick viewer page shares this code and gets the same controls.
- **Chart tiles.** `.cb-plot-row` becomes a grid, `repeat(auto-fill, minmax(var(--cb-plot-min), 1fr))`, so every
  chart tile in the Journey has the same width and a chart alone in its row keeps it. The height is
  `var(--cb-plot-h)`, on the `_plot_cell` chart and on the CTF-fit panel. Tiles lose their border. `wide` and
  `.cb-plot-cell-wide` are deleted.
- **Chart tokens.** A `ChartTheme` (a frozen dataclass) in `ui/dashboard/figures.py` holds what is literal today:
  series colours, marker size and opacity, the signed-tilt ramp, axis and tick fonts, grid lines, hover-card
  fonts, the Plotly scatter and histogram colours. The builders take a `theme`. In this stage it carries today's
  values; the passes change them.
- Mean frame motion: markers only. The header's stage dots take the roster's status colours.
- *Success:* on a fully processed project with every section on, a tilt series reads as before, in one font
  pair, with house controls only and every metric chart the same size.

### S4 — Three passes and the switcher

- The choice lives in `app.storage.user["journey_style"]` (`a`, `b` or `c`; `a` by default), not in the prefs
  model, so removing it later migrates nothing. While it lives, the key is a `StrEnum`.
- A tiny `style` select (a 16 px `.cb-field` with a toolbar label) at the left of `.cb-jhead-right`, before the
  section switch (`tomo_dashboard_dialog.py:224`, `:472`). A change swaps the class `cb-jv-a|b|c` on the Journey's
  root (the container `build_journey_panel` fills, `:170`) and re-renders the pane with `render_main(force=True)`,
  because chart options are built on the server.
- CSS: one block, `_JOURNEY_PASSES_CSS` in `ui/dashboard/css.py`, every rule under `.cb-jv-*`, mostly custom
  properties (`--cb-plot-h`, `--cb-plot-min`, section chrome, title style), so it deletes in one cut. Charts:
  `CHART_THEMES = {"a": …, "b": …, "c": …}` in `figures.py`. The Journey and the Particles section read the active
  pass whenever they build a chart, including in later handlers (the CTF-fit scrubber, a species tab switch), so
  a rebuild never falls back to another pass.
- The passes, as starting tokens to tune within their intent:

| | **A · Ledger** — the Tilts tab | **B · Dense** — the most on one screen | **C · Soft** — one muted accent |
|---|---|---|---|
| Section chrome | no card, no border, no fill; 22 px between sections | no card; 14 px between sections | a `#f8fafc` panel without a border, radius 6 px, padding 12 px; charts on white |
| Section title | 12 px, 600, `#1e293b` | 9 px uppercase, tracked, `#94a3b8` (the toolbar label), stats on the title line | 12 px, 600, `#1e293b` |
| Stats | 9 px label over an 11 px mono value | one line, `label value · label value`, 10 px | as A |
| Chart tile | 150 px tall, columns ≥ 260 px | 116 px, columns ≥ 200 px | 170 px, columns ≥ 300 px |
| Chart label | 10 px `#64748b` | 9 px `#94a3b8`, unit folded in (`Defocus · µm`), no axis names | 10 px `#475569` |
| Axes | no axis lines; y grid `#f1f5f9`; 9 px mono ticks `#94a3b8`; 9 px regular axis names | 2–3 y ticks, no grid | as A, plus an x axis line `#e2e8f0` |
| Markers | 4 px `#475569`; 2nd series `#94a3b8`; 3rd hollow `#94a3b8` | 3 px `#64748b` at 70 %; 2nd `#cbd5e1` | 4.5 px blue-grey `#4f6b84`; 2nd sand `#b39b7d`; 3rd `#94a3b8` |
| Fit and reference lines | dashed `#cbd5e1` | dotted `#e2e8f0` | dashed `#cbd5e1` |
| Signed tilt (trajectories) | cool grey for −, warm grey for +, darker at higher \|tilt\| | one grey, darker at higher \|tilt\| | desaturated blue-grey for −, sand for + |
| Hovered or linked tilt | `#0f172a` with a white ring | `#0f172a` | a `#0f172a` ring |
| Pick scatter, histogram | picks `#475569`, a grey ramp by score; bars `#cbd5e1` | as A at 60 % | a blue-grey ramp by score; bars `#c7d2dc` |
| Species on light chrome | a grey glyph named on hover, as in the roster | as A | the species colour at about half saturation |

  Every pass keeps red for what is wrong (a failed stage, an excluded tilt) and keeps the species colours on the
  slabs, which sit on dark images.
- *Success:* on one tilt series, flip A → B → C: header, section chrome, stats, controls, the ECharts and Plotly
  charts and the Particles section all change together, and nothing else does.

### S5 — Pick and fold

When the maintainer has picked (or mixed: "A's tiles with C's colour"), fold those tokens into the base CSS and
the default `ChartTheme`, and delete the switcher, `_JOURNEY_PASSES_CSS`, the other themes and the
`journey_style` key.

## 3. Decisions (maintainer, 2026-10-17)

1. **Where to build: the main tree.** No other session is editing `~/dev/crboost_server`.
2. **Where widths are remembered: per browser, in `localStorage`**, like the roster width.
3. **Species colours on light chrome: per pass, as in the S4 table** (A and B a grey glyph, C desaturated). The
   palette itself (Material A400, stored per species) stays as it is.
4. **One monospace for the app: out of scope.** The Journey moves to Plex Mono in S3; the other 38 rules keep
   `ui-monospace, monospace`.

## 4. Order and dependencies

S1 → S2: the splitter goes into the frame S1 reshapes. S3 → S4 → S5. The two pairs are independent; S1 is the
smallest commit and can land first.

## 5. Runtime checklist

After restarting `python main.py` and a hard reload:

1. Open Jobs, Tomograms and the Journey: no gutter on the right of any view; each cross sits in its corner over
   nothing.
2. Drag the Tomograms | Journey boundary; reload; close and reopen Tomograms: the widths stay. The charts re-fit
   when the drag ends.
3. Open the Journey on a fully processed project with every section on, and flip `style` through A, B and C on the
   same tilt series.
4. In each pass: Reconstruct's version and denoise switches, Invert, score colouring, sort, the dropped-tilts list,
   the CTF-fit scrubber, and chart hover (the fit follows the tilt under the cursor) all still work.

## 6. Not in scope

- The Journey's layout: the slabs left of the gallery, the X/Z slab as wide as the X/Y one, the gallery and canvas
  sizes stay as settled. Only chrome, type, controls and charts change.
- Other pages' styles. The full-page pick viewer gets S3's controls (shared code) but no pass of its own.
- The species palette (§3.3) and the app-wide monospace (§3.4).

## 7. Stage log

- **S1, 2026-10-17 (not run).** `.cb-view-gutter` and its element are gone; `_view_frame` puts the cross straight
  in the frame (`position: relative`), absolutely placed at `top: 8px; right: 4px` with `z-index: 10` (inline,
  `_VIEW_CLOSE_STYLE`; above the pick dots and the lasso), centred on the Journey's 30 px first line; its grey /
  hover look stays in `.cb-view-close`. First lines that keep the corner free: `.cb-jhead` (the Journey and
  Protocols share it) and `.cb-gal-toolbar` get a 22 px right padding, `.cb-species-header` too (CSS only, so
  `ui/species/page.py` is untouched), `.cb-viewer-page-head` 14 px inside the page's 10 px, the job page's header
  row (`job_tab_component.render_job_tab`) 22 px inline, and the pipeline table's title line 4 px inside its
  column's 18 px. The interactive-job header already pads 24 px.
- **S2, 2026-10-17 (not run).** Frames carry `id=cb-view-<name>` and `flex: var(--cb-vw-<name>, 1) 1 0`; the main
  area carries `id=cb-view-main` and is styled once. Each frame holds a `.cb-view-splitter` (6 px, absolute on its
  left edge, the roster divider's hover line; CSS in `ui/dashboard/css.py`), shown by `.cb-view-split`, which
  `_apply_views` adds to every frame that is on and not leftmost and removes from the rest. `_VIEW_RESIZER_JS`
  (wired with the roster resizer by the same 0.1 s one-shot timer) applies every stored `cbViewW:<name>`,
  delegates mousedown / dblclick from the main area, finds the visible frame to the left by computed `order`,
  moves the pair's weights so the boundary follows the pointer with their sum fixed and both frames ≥ 260 px, and
  on release stores both weights and dispatches one window `resize`; a move with no button down ends the drag
  too (a release over the molstar iframe never reaches the window). A double-click sets both to half their sum.
