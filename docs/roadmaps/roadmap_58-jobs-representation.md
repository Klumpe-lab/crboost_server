# Roadmap 58: how jobs are shown (roster, magazine, graph, toasts)

The maintainer's list of 2026-11-18:

1. The branch map still doesn't work: back to the drawing board, with options to pick from.
2. A project opens on the Pipeline view, unless the user already had something else open there.
3. The toasts (the copy toast first) should match the rest of the UI.
4. The roster: no clone icons and no drop-down under a job row. A job with tasks opens a popup with its
   description and its magazine (the branch map's idea, bigger), laid out as square as the count allows, coloured by
   state (green, amber, blinking blue, red). Hovering or clicking a mark opens a second popup beside it with that
   task's log and links.
5. The roster drops the per-type header rows ("Denoise Train 2 +"). In their place, a gutter on the left with one
   glyph per job type in canonical order, types set apart by a slight shade; the jobs of each type on the right; a
   type with no job shows one grey template row; a type with jobs gets a small add button by its glyph that keeps
   to the grid.

Items 1, 3, 4 and 5 are prototyped on **`/dev/jobs-ideas`**, using a real project that is only read. Item 2 is
built in the app.

## 1. Built

- **Item 2.** `UIStateManager.load_from_project` (`ui/ui_state.py`) no longer opens the roster's first job. A project
  opens with no job open, so the Pipeline frame shows the map. A tab that already had one of this project's jobs
  open keeps it, which is the "unless they opened something" case: a reload, or a `/p/<project>/job/<id>` link. When
  the tab switches projects the open job is dropped even if the new project has a job with the same instance id
  (every project has a `tsReconstruct`). Every `/p/...` open goes through this function (`load_project_into_tab`,
  `ui/open_project.py`). **Not tried in a running server** (yours on 24326 still runs the old code).
- **The prototypes**, package `ui/jobs_ideas/`, page `/dev/jobs-ideas` (route in `ui/main_ui.py`, also served by
  `scripts/dev_screenshot.py`):
  - `data.py` reads the project with `ProjectState.load(read_only=True)`, refreshes `array_status_cache`, and builds
    the branch map's own model (`project_from_state`). Default project:
    `/groups/klumpe/_crboost_data_api/AGG412_20251113_412_rerun` (151 tilt series, 2 species, so TM, candidates and
    export have two jobs each). Change it with `?project=<path>`; `?sim=1` repaints the last three array jobs as
    running, queued and failed so every state can be seen (the page carries a banner while it is on). `task_detail`
    reads the real `task_<idx>.out` / `.err` of one tilt series.
  - `glyphs.py`: a drawn 12 × 12 single-line glyph for each job type, plus a switch to the Material icons the branch
    map uses today.
  - `roster.py` (A1–A4), `popups.py` (B1–B3), `graphs.py` (C1–C4), `toasts.py` (D), `look.py` (shared lights, marks
    and the page's CSS; in a real build that CSS moves to `ui/dashboard/css.py`).

Open it on your server: `http://localhost:24326/dev/jobs-ideas?token=…` once the server is restarted, or
`http://localhost:24326/dev/jobs-ideas?sim=1&token=…` to see a run in flight.

## 2. The variants

**A · The roster with a type gutter.** Every variant uses the same row: light, type name, job number, a tag that
tells instances apart (the species, otherwise the method), the count, and a × that shows on hover. The name opens
the job page; the count opens the magazine. There are no clone icons, no chevrons and no header rows. A type with
no job shows one grey template row ("+ add" on hover). Subset, which is only ever made from another job's page,
shows only when it has a job.
- A1 Bands: every other type shaded across the full width; the glyph sits on the type's first row with a small `+`
  subscript on its corner.
- A2 Rail: the gutter is a rail in the title-stripe tint with one cell per type and hairlines between types. On
  hover, the type's glyph turns into a `+`.
- A3 Grid: two gutter cells, glyph and `+`, always visible; the phases are stripes across the roster.
- A4 Ledger: A2's gutter, but each row carries its magazine in miniature in place of the count.

**B · The magazine popup** (one switch at the top; it opens from any count in A and any job in C). The card holds a
description line, Reads, Species, the folder (copies on click), the tasks counted by state, and the magazine laid out
as close to square as the count allows (`grid_cols`). Marks: green done, amber waiting in the queue, blue blinking
running, red failed, grey not run, lighter grey skipped. Hovering a mark opens the task card: the tilt series,
the task number, the log path, the last lines of stderr and of the log, and the actions Tasks tab / Journey / Full
log. A click pins it. Esc or scrolling closes both cards.
- B1 Beside: the card next to the row it came from, the task card next to the card.
- B2 Sheet: one sheet in the middle of the screen over a light veil, with the task docked inside on the right.
- B3 Tiles: as B1, with bigger marks labelled `Pos·Beam`, readable without hovering.

**C · The graph** (turned down; the branch map stays). Every layout follows a rule (column = the type's place in the pipeline, line = the job the main
input reads), so nothing is dragged and nothing moves between visits. This is the answer to "back to the drawing
board": the branch map's free cards with sockets and cables are what is replaced.
- C1 Subway: stages as columns, each branch on its own line, stations with a light, a number and a strip; second
  inputs drawn dashed.
- C2 Git log: one row per job in job-number order, branches in a narrow gutter, and columns for tool, species,
  count and strip.
- C3 Icicle: stages as columns, each job a block as tall as the branches below it.
- C4 Tree listing: indented, with box-drawing lines; a chain stays on one level and only a fork indents.

**D · Toasts.** Each one is fired for real, from a path that copies and from a warning.
- Today's toast, for comparison.
- D1 Stamp: "copied" at the pointer, for copies only.
- D2 Ledger line: bottom right, mono, the full path.
- D3 Title stripe: top centre, in the stripe tint.
- D4 Ink tag: bottom centre, dark ink-1 with a mono message.

## 3. Decisions (the maintainer's, 2026-11-18)

- (a) Roster: **A2 Rail** ("amazingly close to what I envisaged"), with two changes asked for when it was taken:
  thin headers back to set Preprocessing apart from Particles, and one hairline under each type that runs across
  the rail and the rows alike (the prototype ruled the rows only, so the rail's tint showed under a type's last
  row; A3 Grid had it right).
- (b) The count opens the magazine; the name keeps opening the job page (the interactions were approved as shown).
- (c) Magazine: **B3 Tiles**, the marks squarer (20 x 17 px, 2 px apart, `Pos·Beam` in 7 px mono) and a muted
  green for done (`#5a9c80` instead of the status green). The other marks stay: amber = waiting in the queue, blue
  blinking = running, red = failed, grey = not run, lighter grey = skipped.
- (d) Graph: **none**. The branch map stays as it is ("all of the options are by far inferior to what we have now").
  C1–C4 are dropped, and S4 with them.
- (e) Glyphs: **drawn**.
- (f) Toasts: the first round (stamp, ledger, stripe, ink tag) was turned down ("some of them I can't even read").
  Second round on the page: T0 copied-in-place for copies, and T1 card / T2 line / T3 status bar for everything
  else, all dark ink on light, in the popup card's and the title stripe's chrome. **T1 Card** taken (2026-11-18).

### Why some toasts were unreadable (and why today's look as they do)

Quasar's colour classes (`.bg-positive { background: var(--q-positive) !important }` and the others) are in
NiceGUI's `quasar_importants` cascade layer (`nicegui/static/quasar.important.css`, imported into that layer by
`nicegui/templates/index.html`). For `!important` declarations a rule in a layer beats any unlayered rule,
whatever the specificity. So the app's own toast rule in `ui/dashboard/css.py` (`.q-notification[class*="bg-"] {
background: #ffffff !important; color: #1e293b !important }`) never gets its white background: a positive toast
is Quasar's green, a negative one Quasar's red, and the dark text the rule does force sits on top of them. The
first-round variants tied with or lost to the same rule. The fix: for `!important` the first-declared layer wins,
and NiceGUI declares `@layer theme, base, quasar, …, quasar_importants`, so the toast's colours go inside
`@layer theme { … }` (as in `ui/jobs_ideas/look.py`). Every other `!important` override of a Quasar colour class
in `ui/dashboard/css.py` has the same problem and needs checking in S5.

## 4. Built (2026-11-18), checked once in a browser on the AGG412 rerun

- S1 `JobSpec` carries `summary` (one line on what the job does) and `glyph` (the 12 x 12 line drawing) for every
  type (`services/jobs/spec.py`); `ui/components/job_glyph.py` draws a glyph.
- S2 The roster (`ui/pipeline_builder/pipeline_roster.py`): a thin stripe per phase (the Particles one keeps its
  four tools), then per type a band: the rail cell in the title-stripe tint with the type's glyph (amber, named on
  hover, while a type it needs is missing; a `+` in its place on hover when the type can be added again), and the
  type's jobs, or one grey template row ("+ add" on hover) that adds the first. One hairline under the band. A row:
  light, name, job folder (copies), the particle set it reads, the species as a small tag, then the count (a click
  opens the magazine), the extra tabs, and a × that shows on hover. Gone: the per-type header rows, the clone
  icons, the chevrons, the per-tilt-series list under a row and the state behind it (`_expanded_instances`,
  `_array_ts_cache`, `_render_ts_sub_rows`). The tilt filter's settings, which folded out under its row, open in a
  card beside it from where the row says the filter is.
- S3 `ui/components/magazine.py` `RosterPopups`: the magazine (Tiles: 20 x 17, `stage·beam` from the registry,
  read in a thread), its facts (summary, Reads from the recorded inputs, species, folder, tasks counted), Open job /
  Tasks / Logs; the task card (log and stderr tails read in a thread; In the Tasks tab, which focuses the task;
  Journey). Parented at the layout slot, repainted when the task states move (3 s, the pipeline view's scope),
  closed by Esc (the workspace's pane Esc steps aside while one is open: `.cb-mag-pop` in `_PANE_FOCUS_JS`'s busy
  list), by a scroll outside it, by the cross, and when its job leaves the roster.
- S5 Toasts: T1 Card for every `ui.notify` (`ui/dashboard/css.py`, colours and the list's alignment in
  `@layer theme`), bottom right. A copy says COPIED over what was copied (`ui/components/copyable.py`, and the two
  copy toasts in `ui/particles/pick_viewer.py`, one in `ui/species/picks_tab.py`).
- S6 `/dev/jobs-ideas` and `ui/jobs_ideas/` removed.

Left: the other `!important` overrides of Quasar colour classes in `ui/dashboard/css.py` likely lose to the
`quasar_importants` layer the same way the toasts did; check them when they are next touched.

## 5. Roster pass (2026-11-18, later), not run in a browser

Asked by the maintainer: the PARTICLES header's Aggregate and Import tomograms out, the particle map out, the roster
resizable against the view beside it, the species tag right-aligned, the job folders in one column, and a species
picker that is not a stock Material select.

- The PARTICLES stripe keeps New species only (Aggregate: roadmap 53 §14.7; Import tomograms now on the Tomograms
  view's title line, same place; the particle map: roadmap 53 §13.3, roadmap 32 §15.9).
- Resizing. The divider was there but could not work: its box (`.cb-roster-resizer`, 6 px wide) was in the page
  shell's stylesheet in `ui/main_ui.py`, which is served stale, so the handle could have no width; and every view
  toggle re-sends the roster's inline style (`set_active_views`), which put `width: 340px` back over a dragged
  width. Now the handle's box is inline (8 px, over the gap to the first view), its hover line is in
  `ui/dashboard/css.py`, and the width is `--cb-roster-w` on the document root, which the roster's inline width
  reads and the server never writes (240–720 px, kept in localStorage as before).
- A row is [light][name][job folder][set it reads] … [filter status / count][extra tabs][species][×]. The name
  column is as wide as the longest job name and the job-folder column 7 ch on every row, so `job011` lines up down
  the roster; the set tag gives way before the name in a narrow roster. The species tag is the last thing before
  the ×, whose 15 px slot is kept on a job in use (no ×), so species tags end at the same place on every row. Before
  a job has a folder, a suffix that only repeats the species id is not shown (the species tag says it).
- The species picker (`ui/pipeline_builder/species_picker.py`): a list of the project's species, nothing chosen
  until a click (the old select pre-chose the first species). A row: colour, name, then what the job takes from the
  species: for template matching, template or no template (amber), Ø (amber "Ø ?" when unset), symmetry; an
  "N already" tag when the species has jobs of this type. Click or Enter adds the job. "New species…" makes one in the
  species prompt and adds the job for it; a project without species gets the same dialog with that as its one action,
  instead of a toast.

Runtime check: drag the divider, toggle the Tomograms view on and off, the width stays; reload, it stays. Rows of a
project with Template matching for two species, a Class 3D reading a set, and the tilt filter: job folders in one
column, species tags flush right. Add a particle job with zero, one and several species.

## 6. Particle cards and class mosaics: prototypes (2026-11-18, round 2)

**Retired 2026-11-19** at the maintainer's word, with every other `/dev/*ideas` page: the prototype is gone from
the tree. Round 2's code is in git (`7e65445`, `ui/particle_card_ideas/`); round 3's changes were never committed
and are lost. What it showed is described below; the open picks (P1–P3, P5, P7) stand on that.

`/dev/particle-card-ideas` (`ui/particle_card_ideas/`), on real jobs of `AGG412_STA`, read and never written: Class 3D
job013 (the 412, K 6, reads s011), job014 (K 3), job005 (ribosome, still iterating), Refine 3D job015 (reads s012),
mask job016, PostProcess job017 (failed), Warp export job003 (reads s011). Switches: `?accent=ochre|verdigris|graphite`,
`?classes=muted|today`, `?strip=S1..S7`. Maps at their own sampling (projections and central slices along z, y, x,
256 px PNG data URLs made in a worker, kept per map and mtime). Large cards at 720 px (the map draws 600 today).

The maintainer on round 1: more organized; the purple particle tint and the colours generally too loud; the fonts hard
to read, mono used at random; small cards good; graphs OK, but in the job page's Results tab, not on the large card.

**Type rule (every piece of the page).** Words (names, captions, project names, sentences) in the sans face, ≥ 10 px,
captions 9.5, never mono. Numbers, ids (job013, s003), counts and values with their units in the mono face with even
figures (`tabular-nums`), right-aligned in columns. Labels (a field's, a column's, a block's name) in the label voice,
which the theme sets (Harbor Mono: mono capitals, 8.5 px), the one place mono carries words. Helpers `words`, `num`,
`lab` (`ui/particle_card_ideas/parts.py`).

**Colour rule.** Flat cards (no shadow), hover and selection as hairline rings, never a lift. A particle job is marked
only by a muted accent: a 2 px left edge, its disc ports (squares stay for tilt series and maps) and its job number;
the header band is the tilt-series card's. Candidates, all `--cb-particle-<name>` with these fallbacks: ochre #9a7631
(default), verdigris #3f7a6e, graphite #46566a. Classes: a proposed muted palette (`--cb-class-1..8`: steel blue
#4f7aa8, rust #b0643a, verdigris #3f8a78, ochre #b08a2e, olive #6f7f3a, oxide red #a14d4a, slate #5f6f86, sand
#a8916a) against today's saturated `CLASS_PALETTE`; figures.py is unchanged.

**Graphs.** Off every large card (no FSC, no stacked shares, no per-class sparklines); the large cards keep pictures,
numbers and bars. The plot primitive moved to `ui/components/plot.py` (`fsc_plot`, `shares_plot`), which the Results
tabs draw; section 3 of the page shows them against today's bare lines.

- **1 Class 3D results.** A *Mosaic* (one tile per class, z projection big with the three axes under it, hover for the
  slice, share, count, Class 3D's own estimate, rotational accuracy, tomograms and the top one, projects; click selects;
  an iteration scrubber). B *Table* (one row per class, numbers in right-aligned columns). C *Small*.
- **2 The particle-job card.** Small cards for export, Refine 3D, mask and the failed PostProcess; large Refine 3D (six
  views, box / pixel / extent, the resolution and what it means, projects) and export (per project, per tomogram as
  median / most / fewest).
- **3 Graphs**, as the Results tab draws them.
- **4 The set strip**, on three jobs at 236 px and on Refine 3D at 580 px:
  - S1 *Ledger*: id, name, particles; a project bar with the projects named inside their segments.
  - S2 *Tomograms*: one column per tomogram (height √ particles), grouped by project, the accent under each group; the
    magazine's analogue. With one crowded tomogram it reads as a texture more than a count.
  - S3 *Chain*: the steps back to the picks with the particles each held (`8 pick lists, ≥ 8.5 σ → s011 1,881 →
    job013 class 2 1,874 → s012 438`); a narrow strip keeps the last two.
  - S4 *Ruler*: this set as a share of what it was cut from (23 % of job013's 1,874); a pool shows S1's named bar.
  - S5 *Status line*: fixed fields split by hairlines, the share as block characters.
  - S6 *Tally*: particles per project as a small table (three and "+ n more" when narrow).
  - S7 *Ruler, status bar* (round 3, the maintainer's pick): S4's ruler drawn as S5's block bar (`█` in the
    accent over a `░` track), the share, "of" the parent's particles and where it was cut. Every strip's ledger line
    now carries the species (its colour and name in the label voice) beside the set's id; the project names inside
    the named bar are in the label voice, inked to read on their segment.


**Round 3 (2026-11-18): fewer colours, no instructions.** The maintainer: too much colour across the panel, the
round-2 scheme disliked, and no prompts that babysit ("click classes to select them", "click to select"). Now:
- A class's colour shows only in the swatch beside its number; tile borders, share bars, selection rings and the
  small card's bars are in ink. The particle accent only on a card's left edge and its set ports; job numbers, set
  ids, the set strips' bars and the page's headings are ink. The species tag is its name alone.
- Five schemes, `?scheme=` (accent and class colours together, everything else the theme's inks): **ink** (no hue:
  the slate ink, classes as eight steps of it; the default), **steel** (the theme's blue and its lightness steps),
  **phosphor** (one muted amber against the slate), **signal** (greys, one signal red for the particle mark),
  **sage** (an instrument-panel grey-green with slate). `?accent=` and `?classes=` are gone.
- No instruction text on the cards: the selection line shows only once something is selected, the table's title is
  its iteration, the picture captions and the "no estimate" sentence are gone (the estimate's cell carries a hover
  title). In the app: the species picker's subtitle and the Refine 3D / PostProcess Results tab's "under the
  pointer" line are gone too.

| # | Decision | Recommended |
|---|---|---|
| P1 | Class 3D's large card: A Mosaic, B Table, or A with B as its "rows" toggle | A, with B for K > 6 |
| P2 | Large card width | 720 px |
| P3 | Projection or central slice as a class's main picture | projection, slice on hover |
| P4 | The colour scheme (round 3): ink, steel, phosphor, signal or sage | **decided 2026-11-18: steel** |
| P5 | The class colours app-wide (figures.py `CLASS_PALETTE`) follow the scheme picked | yes |
| P6 | The set strip | **decided 2026-11-18: S7**, with S1's named bar for pools; S3's chain on the strip's hover |
| P7 | Selecting classes on the card makes the set (the Results tab's Make set) | yes, the same verb |
| — | Graphs on the large card | decided 2026-11-18: no, in the Results tab |

Found while building round 1: job015 refined at 6.2 Å/px, box 128 (job003's bin-4 images), not the bin-2 box-256
exports §12.3 of roadmap 53 lists; its FSC never drops below 0.143, so its 12.6 Å is the Nyquist limit. job017 failed
on that: the mask is 256³ at 3.1 Å, the half maps 128³ at 6.2 Å.

## 7. The map's strips, selection and footer (2026-11-18), not run in a browser

Asked by the maintainer: the Tilt series and Cards strips off the top of the map, the Cards switches as flags on the
Pipeline title line; one click selects a job, in the roster and on the map at once, and opens it only once selected
(or on a double click); the "→ particle set" footer off every card; large cards larger.

- Title line (`BranchMapView._render_head`, ui/branch_map/view.py): after the name, the picked tilt series as a chip
  with its ×; at the right end the tilt-series picker as a menu of the chips (`picker_body`), then flags: all large /
  all small, magazines, the folded shared start (lit while on, `.cb-flag-on`), reset layout once a card was dragged,
  then zoom −, %, +, fit as icons. They hide while a job page shows (`set_visible`). `callbacks["view_head"]` is
  registered before the views are built, so the map can fill the line while it mounts. `MapState.picker_open` and
  `picker_pinned` are gone.
- Selection (`PipelineBuilderPanel.select` / `activate`, `selected` per tab): a click on a roster row's name or a
  card's header selects the job, painted in place in both (`RosterWidget.paint_selected`, `BranchMapView.
  paint_selected`, class `cb-sel`, no redraw); hovered while selected, the row or the header tints and shows
  "OPEN ↗" (`.cb-open-cue`, ui/dashboard/css.py), and a click there opens the page (`switch_tab`, which also
  selects). A double click is two clicks: select, then open. The folded start's rows select the same way. Explicit
  openers stay direct: the card number's menu, the magazine's Open job, a row's extra-tab icons.
- Footer gone: the card ends 6 px under its last row (`FOOT_H`); the "+" that adds a reading job sits in the header
  beside the size toggle; the output port is on the right edge level with the header (`OUT_Y`), what the job makes
  in its hover.
- Large cards 600 px (was 480), pictures 128 px tall (was 104); §6's P2 recommends 720 for the class mosaic.

Runtime check: select in the roster, the card rings; select on the map, the row outlines; hover the selected one,
"OPEN ↗"; second click opens; with a job page open the title-line flags are gone and come back on close; cables
leave from the header-level port and follow a dragged card.

## 8. The graphs go to the job page's Results tab (2026-11-18), not run in a browser

Decided by the maintainer on §6's round 1: keep the readable plots, but in the job page's Results tab, not on the
large card.

- `ui/components/plot.py`: the plot primitive out of the prototype (axes with units, labelled ticks, named reference
  lines, marks, a readout under the pointer in CSS only: `.cb-plot-*` in ui/dashboard/css.py), muted series (accent,
  slate, ochre, verdigris as `--cb-plot-s1…4`), numbers in the mono face with even figures, titles in the label voice.
  `fsc_plot` (a 1/Å axis named in Å, 0.143 and 0.5, the crossing marked, or "> N Å (Nyquist)" when there is none)
  and `shares_plot` (classes stacked to 100 % by iteration).
- Class 3D and Reconstruct particle Results (ui/analytics/readout_panel.py): the convergence and FSC ECharts are these
  plots now; `build_class_convergence_chart` and `build_fsc_chart` deleted from ui/dashboard/figures.py.
- Refine 3D and PostProcess have a Results tab (ui/job_plugins/particle_results.py, `plugins` on their JobSpec rows):
  the resolution in large mono figures with what it measures (gold-standard of the unmasked half maps; masked and
  corrected after PostProcess), converged or which iteration, the B-factor, box, pixel and Nyquist, the FSC curves
  with a legend (PostProcess: corrected, unmasked, masked, phase-randomised). A curve that never drops below 0.143
  says the number is the sampling limit, not a resolution (job015 of AGG412_STA: 12.6 Å at 6.2 Å/px); a failed job
  shows RELION's last error line (job017: the mask's box and pixel do not match the half maps'). Read in a thread by
  `services/particles/fsc_readout.py`.
- The map's large cards for Class 3D, Refine 3D and PostProcess carry no plot any more; Refine 3D's lines say when
  its number is the sampling limit, and point to the Results tab.

Runtime check: open job015 and job017 of AGG412_STA, Results tab; job013's Results tab (the stacked shares); hover
each plot for the readout; a narrow job pane (the plots shrink with their cell).
