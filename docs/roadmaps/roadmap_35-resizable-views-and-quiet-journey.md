# Roadmap 35 — Views you can resize, and a quiet Journey

**Status:** scoped 2026-10-17 from reading main (`cb8d419`); §3 decided 2026-10-17; S1–S4 built 2026-10-17 in the
main tree, not run (§7); S5 waits for the maintainer's pick. S1–S2 are the workspace, S3–S5 the Journey.

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

After restarting `python main.py` and a hard reload, on `AGG412_20260311_MyrGFP_Grid2` (two reconstruct versions,
two picking arms). The Plotly charts only draw in the scatter fallback (a species or tilt series with picks but no
cutout atlas); the denoise switch only shows when two denoise methods have a result for the tilt series.

0. **Fonts reach the browser.** DevTools console:
   `[...document.fonts].filter(f => f.family.includes('Plex') && f.status === 'loaded').length`. If it is 0, the
   module-level `ui.add_head_html` blocks (`main.py`, `ui/main_ui.py`) never arrive and every Plex rule renders in
   the fallback faces; the one-pair look then needs the fonts loaded from `ensure_assets_loaded` (app-wide).
1. **S1.** Open Jobs, the pipeline table, the Particles registry, Tomograms, the Journey, Protocols and the pick
   viewer: no gutter on the right of any; each cross sits in its corner, nothing under it on the first line, grey,
   slate on hover; a click closes the view.
2. **S2.** Drag the Tomograms | Journey boundary: it follows the pointer and stops at 260 px a side; on release the
   Particles section's Plotly charts re-fit (ECharts follow live: note any judder with every section on). Reload
   and reopen the same views, then close and reopen Tomograms: the widths stay. A double-click on the boundary
   splits the pair evenly. Release a drag over the molstar viewer (Particles registry · Templates & masks): the
   drag ends on the next move.
3. **S3, in every pass.** Reconstruct's `version` strip (two versions, and `compare`) switches the previews;
   `polarity raw | inverted` flips the slabs and the cutouts; the display filter's TYPE / PARAM (the `tune`
   popover) and the gallery sort (`sort` popover) are 16 px house selects with the house popup; the dropped-tilts
   caret folds the list; the CTF-fit scrubber is a slate 2 px track with a small thumb, and hovering a Motion & CTF
   chart point moves the fit to that tilt (only those tiles drive it); species tabs have a slate indicator and no
   tinted fill; the render-previews / re-render / IMOD icons, the toolbelt and the header ⓘ are grey with a slate
   hover; spinners are grey; the header's stage dots run blue and fail red. Every tile in a row has the same width
   and one alone keeps it; mean frame motion is markers only. Computed fonts on a datadump value, a chip, the pixel
   table, a cutout tile's labels and an axis tick: Plex Sans / Plex Mono only.
4. **S4.** On one tilt series flip `style` A → B → C. Changing together: the header's species mark (grey glyph in
   A and B, half-saturated colour in C), the section chrome (none / none / pale panels), titles (B: grey uppercase,
   stats on the title line), the tile size (150 / 116 / 170 px), chart labels (B: `Defocus · µm`, no axis names),
   markers, reference lines (B dotted), the motion trajectories' ramp, the hovered point, the Plotly picks and
   histogram, the dropped tilts' angles in red. Unchanged: the slabs' pick dots, the pick lists' swatches, the
   Tomograms wall, the full-page pick viewer, the tilt-filter job page, the Species page's Analytics charts. The
   pass survives a reload.
5. **Narrow** the Journey to 260 px in each pass: the charts stay inside the pane and the CTF-fit header line cuts
   its readout short rather than spilling.

Open, not settled by reading:

- Plain `.tooltip()`s and every pop-up (menus, the TS switcher, select popups) sit under `<body>`, outside the
  Journey's root: they keep Quasar's body font and the baseline in every pass.
- ECharts draws text on a canvas: on a cold load before Plex arrives, ticks and names can stay in the fallback until
  the next redraw (`renderer="svg"` in `_plot_cell` and the CTF panel would avoid it).
- The hover card (with its 336 px tilt image) clamps to its tile, so on the smaller tiles it hangs below the chart
  and can run off the window near the bottom edge.
- `static/main.css` styles `.cb-collapsible-header` uppercase at 11 px with `!important`; if that file starts being
  served, the Dataset header and the dropped-tilts fold change with it.
- Toggling views on and off changes their widths without a window `resize`, so Plotly charts keep a stale width
  until the next one (older than S2; S2 dispatches one on release and on double-click only). Stored weights can
  open a view below 260 px when a different set of views is on.
- Not in the S4 table, chosen here: B's third series (hollow `#94a3b8`, as A's), C's gap between panels (10 px),
  the CTF fit's per-pass colours, the 22 / 10 px plot-rect tops.
- A failed job's status word in a section header is amber, like every non-succeeded status (one class).
- Sub-titles (a pick list's header, `Subtomo gallery`, `Pick distribution`) share `.cb-section-title`: 12 px like
  the section titles, and B's toolbar voice reaches them too.

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
- **S3, 2026-10-17 (not run).**
  - *Root.* `build_journey_panel` adds `cb-journey` to the container it fills; `--cb-plot-h: 240px` and
    `--cb-plot-min: 340px` live there.
  - *Fonts.* `:is(.cb-journey, .cb-viewer-page)` rules give Plex Mono to the classes that said `ui-monospace` and
    to Tailwind's `font-mono`, Plex Sans to the labels inside them (keys, headers, chip labels, pane tags, the
    checkbox label), 600 to every 700, and put the off-scale sizes (8 px table heads, 11 px table cells) on
    12 / 10 / 9 px + 11 px mono. `.cb-section-title` is 12 px. The Particles section's Tailwind and inline sizes
    are on the scale (`text-xs` / `text-sm` headlines 12 px, body and hints 10 px; `font-bold` → `font-semibold`).
    The Plotly layouts set Plex Sans (`JOURNEY_THEME.plotly_font`, 10 px), tick and colour-bar numbers in Plex Mono,
    hover labels in the layout font. Both roots (`.cb-journey`, `.cb-viewer-page`) set Plex Sans for text with no
    face of its own. Pop-ups render under `<body>`, outside the roots, so their own classes carry the pair: the TS
    ⓘ popover (`.cb-info-card` Sans, `.cb-info-val` / `-meta` Mono), the chip tooltip (`.cb-chip-tooltip` Sans
    10 px, `.cb-tt-line` Mono, `.cb-tt-head` 600 slate). Words that wore Tailwind `font-mono` (imported-tomogram
    tags, the WIP markers, "N species", the viewer's species name) are Sans. Plain `.tooltip()`s keep Quasar's
    body font: an app-wide `.q-tooltip` rule would be the only reach, and is out of scope.
  - *Controls.* Reconstruct's version and denoise switches are `render_segmented(..., classes="cb-seg-sm")` in a
    tooltip wrapper; Invert is a `polarity` label with a `raw | inverted` strip; the cutout filter TYPE / PARAM and
    the gallery sort are `.cb-field` selects with `cb-select-popup`; the dropped-tilts list folds under a
    Dataset-style caret header (`.cb-drop-head`); the CTF-fit slider is `track-size=2px thumb-size=10px` with
    slate CSS (`.cb-fit-slider`; Quasar colours it with `--q-primary` in its normal layer, which css.py outranks)
    and shrinks before its readout spills; the exclude-not-extracted checkbox is `size=xs` and ticks slate;
    spinners are `color="#94a3b8"` (an inline style); the species tabs drop `indicator-color` (the indicator takes
    the tab's slate) and their tinted fill; the ⓘ, eye, toolbelt, reference-toggle and copy hovers are slate; the
    links are `text-slate-500`; the 3dmod command line is a `.cb-field cb-field-mono`; the Dataset's `rule` icon
    is grey.
  - *Icon buttons.* NiceGUI's `ui.button` defaults to `color='primary'`, and Quasar's `text-primary !important`
    sits in a cascade layer that beats any unlayered rule, `!important` or not, so the old `text-purple-600` /
    `text-blue-600` classes and the `.cb-tb-btn` / `.cb-info-copy` / `.cb-ts-info-btn` greys never showed. Every
    icon button in the Journey and the pick viewer now passes `color=None` (admin ×3 and the toolbelt as
    `.cb-tb-btn`, copies, the header ⓘ, the 3dmod copy). The armed curate toggle, visible for the first time, is
    dark slate; a list's delete is grey with a red hover (`.cb-del-btn`).
  - *Chart tiles.* `.cb-plot-row` is a grid, `repeat(auto-fill, minmax(min(var(--cb-plot-min), 100%), 1fr))`;
    `.cb-plot-cell` has no border and `min-width: 0`; `_plot_cell` and the CTF-fit panel are
    `height: var(--cb-plot-h, 240px)`. `wide`, `height_px` and `.cb-plot-cell-wide` are gone.
  - *Chart tokens.* `ChartTheme` (frozen dataclass, `ui/dashboard/figures.py`) holds series colours, hollow
    series, marker size and opacity, the hovered point, reference lines and the axis pointer, the CTF fit's
    measured / model colours, the signed-tilt ramp, tick and axis-name type, the x axis line, the y grid, y ticks,
    the plot rect's top, legend and hover-card type, and the Plotly font, pick scales (light and over the X/Z
    image) and histogram colour. `DEFAULT_THEME` is today's look and every non-Journey builder keeps it;
    `JOURNEY_THEME` is it with 7 px markers, 600 axis names and Plotly in Plex Sans. The Journey's 15 chart
    builds, the CTF-fit panel and the three Plotly builds take it. `build_per_tilt_chart` builds its axes with
    `_axis` and takes `ref: True` for a fit line (the Through-focus fit) instead of `color` / `dash`; markers take
    the theme's size (the per-series `marker_size` is gone).
  - *Also.* Mean frame motion is markers only; the header's stage dots and running / failed pick counts take the
    roster's colours (`#3b82f6`, `#ef4444`); three hints no longer name colours or dashes; the Plotly captions
    are `.cb-plot-label`.
  - §1.3 is wrong on one row: there is no "score colouring" select; the three selects are the cutout display
    filter's TYPE and PARAM and the gallery sort. The exclude-not-extracted checkbox shows only on the full page.
- **S4, 2026-10-17 (not run).**
  - *Switch.* `ui/dashboard/journey_style.py` (new, deletes whole in S5): `JourneyStyle(StrEnum)` `a | b | c`, read
    from and written to `app.storage.user["journey_style"]` (A when unset or unknown), `journey_theme()` →
    `figures.CHART_THEMES[...]`, and the root's class pair. In the header, `.cb-jhead-right` now holds its own host
    for a `style` toolbar label + a 78 px `.cb-field` select (`A · ledger | B · dense | C · soft`, `cb-select-popup`)
    ahead of the section switch's host (`_build_panel_selector` clears its host, so the select cannot share it). A
    change stores the pass, swaps `cb-jv-a|b|c` on the root and runs `refresh_all(force_main=True)` (collects in a
    thread, then `render_main(force=True)`). `render_main` re-applies the class and `_main_signature` carries the
    pass, so a second tab of the same browser comes round on its next rebuild.
  - *Charts.* Every Journey chart build calls `journey_theme()` (15 sites); the CTF-fit panel reads it once at
    build, so scrubbing and hovering stay in the pass it was built in. The Particles scatter fallback takes
    `theme=journey_theme()` in the Journey (`mode="slim"`) and `JOURNEY_THEME` on the full page.
    `CHART_THEMES = {"a", "b", "c"}` in `figures.py` are `JOURNEY_THEME` with the table's tokens; B also sets
    `axis_names=False` (new token): `_plot_cell` then blanks both axis names and folds the y name's unit into the
    title (`Defocus · µm`; none where the name has none), and the CTF-fit tile titles itself `CTF fit · 1/Å`. The
    CTF fit's measured / model colours are per pass (A slate / light slate, B grey / light grey, C blue-grey /
    sand). Plot-rect top: 22 px in A and C (9 px names), 10 px in B.
  - *CSS.* `_JOURNEY_PASSES_CSS` in `ui/dashboard/css.py`, injected right after `_CB_CSS` by
    `ensure_assets_loaded`, every rule under `.cb-journey.cb-jv-*`: the tile tokens, the section chrome on the
    top-level cards (`.cb-main > .cb-section-card`; a card nested in a section is plain in every pass), B's
    toolbar-label titles and one-line stats beside them (flex on cards `:has(> .cb-stats)`), the chart labels,
    species on light chrome (A and B the roster's `particle.svg` as a grey mask on the header dot, the species tab
    dot and the pixel table's species stripe; C `saturate(0.5)`), and in every pass the dropped tilts' angles in red
    out of the amber box, the selected pick-list row / cutout hover ring / rank tag / kept count in slate and the
    list table without its box.
  - *Not done, by design or open.* There is no linked-tilt highlight to style (only the hovered point, and the
    CTF-fit panel following it). B's third series (Refined shift has three) is not in the table: hollow `#94a3b8`,
    as A's. The pick lists' swatches keep their colours in every pass: they are the slab dots' legend. Pop-ups
    (menus, tooltips, the TS switcher) live under `<body>` and keep the baseline.
