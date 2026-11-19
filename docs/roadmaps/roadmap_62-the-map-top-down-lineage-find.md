# Roadmap 62: the map, top-down and fast: no dragging, a job's lineage, find with `f`

**Asked (maintainer, 2026-11-19).** The branched job builder (the branch map, `ui/branch_map/`) has become laggy on a
project like AGG412_STA, cause unknown: find out whether it can be made faster. Its layout stretches the chains into
one long line, so nothing is readable without zooming and the user keeps moving around it. Colleagues found moving
cards around unnecessary and confusing: drop it, and lay the jobs out top-down, CryoSPARC-like, so that the
layout follows by itself when a card is expanded. A colleague asked that selecting a job highlight its whole
lineage. And with the Jobs view focused, `f` should open a find bar in the middle of the view, simple, on frosted
glass: typing "Clas" finds the Class 3D jobs, "ref" the Refine 3D ones, and the whole UI highlights the matches.
Ideas first, as prototypes.

**Status (2026-11-19).** The lag is measured and explained (§1). The ideas are on `/dev/map-ideas` (§2, uncommitted),
over the real project, read-only. One bug found on the way is fixed: H27 in [HOTFIXES.md](HOTFIXES.md) (a job given
a particle set was drawn as reading itself). Nothing else of the real map has changed. The decisions are in §4,
the build in §5.

## 1. Where the lag comes from (measured 2026-11-19)

Measured with a headless Chromium (Playwright) against the running dev server, on AGG412_STA (30 jobs, no tilt
series, two species), the map opened from the roster's map icon. Each click's update is timed and weighed in the
page itself (the socket's `update` messages), the browser's long tasks are counted, and the server's stalls come
from the loop watchdog's lines in `~/.crboost/logs/crboost-24326.log`. The script is `scripts/dev_map_probe.py`.

| action | server's reply | sent to the browser | elements on the page | browser |
|---|---|---|---|---|
| open the map, 30 small cards | 0.52 s | 511 KB | 2,692 | 62 ms of long tasks |
| one card large | 0.17 s | 600 KB | 2,781 | 88 ms |
| that card small again | 0.14 s | 534 KB | 2,692 | 68 ms |
| every card large (results read before) | 0.72 s | 4.1 MB in two updates | 5,549 | 2.7 s until it settles, 457 ms of long tasks |
| every card large, the first time | the event loop blocked 2.5 s, 8.5 s, 3.6 s | | | |
| magazines off, on | 0.17 s, 0.58 s | 531 KB, 538 KB | | |
| select a card | 2 ms | 0.9 KB | | painted in place |
| 30 s with nothing happening | | nothing | | the signature holds |
| pan and wheel-zoom, small or large | | | | 60 frames a second |

What that says:

1. **Every change redraws the whole map.** `BranchMapView.redraw()` marks the view dirty and refreshes it: the
   container is cleared and every card is built again (`ui/branch_map/view.py`, `_render_map`). Toggling one card's
   size, picking a tilt series, opening an input's candidates, and a large card's results arriving each send the whole
   map again (500–600 KB with small cards, megabytes with large ones), and the browser rebuilds about 2,700
   components each time.
2. **A card is about 67 elements, most never seen.** Every card builds two menus at render (the "+" with a row per
   job type it could feed, and the number's Open / Clone / Remove), plus a Quasar tooltip per port, row and button:
   280 `q-tooltip` with small cards, 868 with large, 329 `q-icon`, 62 `q-menu`. Every label carries its whole style
   inline, `var(--cb-…, fallback)` text included, which is most of the bytes.
3. **Large cards carry pictures inside the update.** Class 3D's class slices and the segmentation composites are data
   URLs (`map_slices_url`, `composite_url` in `ui/branch_map/facts.py`), so with every card large each later click
   sends the pictures again: 4.1 MB.
4. **Results arriving redraw everything, once per card.** Each large card's results are read in a thread and then
   `view.refresh` runs (`kick(...)` in `_request_facts`); the facts store's version is in the signature, so N large
   cards can mean N full redraws in a row. The cold "every card large" on AGG412_STA blocked the event loop for
   2.5 s, 8.5 s and 3.6 s (12:34:58–12:35:02, in `blocks.draw_section`, `card.port`, `style.text`,
   `blocks._pictures` and `pathlib.resolve` ×28): every tab of every user stood still for about 15 s. That click
   was the probe's own.
5. **A running array job redraws the map at every tick.** The signature carries every tilt series' status
   (`arr_sig`), so on a preprocessing project in flight the whole map is rebuilt every 3 s whenever a task moves.
6. **Magazines are heavy where there are many tilt series.** One `<rect>` with a `<title>` per tilt series per card
   (`strip_svg`): the 15 cards of the 151-tilt-series AGG412_20251113 rerun are 518 KB of HTML in the prototype,
   which draws them with the same function. Today's map sends them again with every redraw.
7. **The model is not the problem here.** `project_from_state` takes 3–8 ms warm (254 ms the first time) and the
   resolver's edges 2–6 ms. Both still run on the event loop at every render, and they resolve paths (98
   `Path.resolve` calls) that can stall on a cold NFS cache.
8. **Panning and zooming are fine.** They are a transform in the browser. The lag is the redraws.
9. **Not the map, and not this roadmap.** The same log shows stalls of 1.5–4 s in file reads on the event loop today
   (`pathlib` open and stat, 30–79 samples per stall, at 11:05, 11:28 and 12:56–12:59), and 1.3–1.8 s inside
   logging's lock. Since 2026-10-21 this log holds 265 stall lines. By the frame they were caught in: 127 other,
   43 in `dashboard_data`, 27 unattributed (worst 211 s), 25 in logging, 24 in file reads, 19 in the branch map
   (worst 8.5 s). A session run with `CRBOOST_TRACE_STALLS=1` names them.

**Found on the way: H27.** A job given a particle set records the copy it stages in its own folder
(`input_set/optimisation_set.star`), and the map took that path as the job reading itself: 14 of AGG412_STA's 30
jobs were their own parent, piled into the first column, with no lineage. Fixed in `project_from_state`: such a job
reads its set (a row "set …", cabled to the job the set was pooled from when it was). Without that fix no lineage
could be drawn on this project.

## 2. The prototypes: `/dev/map-ideas`

The route is in `ui/main_ui.py`, the page in `ui/map_ideas/` (`data.py`, `cards.py`, `layouts.py`, `assets.py`,
`page.py`). It reads a project with `ProjectState.load(read_only=True)`, refreshes the array status cache, reads the
sets' parents and the branch map's `Project` model, and keeps a facts store of its own (the live map's is left
alone). Nothing is written. URL parameters: `project` (a name or a path; AGG412_STA by default), `layout`,
`lineage`, `find`, `group`, `large=all`.

The page shows a frame like the Jobs view: the roster (grouped by job type as the Rail roster is) beside a Pipeline
view whose title line carries the selected job's lineage count and what each update weighed. Above it, switches for
the choices below.

Every card is one element: an HTML string styled by classes. The browser lays the cards out (flex and grid, nothing
positioned by the server), and a script draws the cables once the browser has laid the cards out, again whenever
a card changes size. Selecting, the lineage and find run in the browser over a graph the page sends once. On
AGG412_STA: 30 cards and 2 set pills in 93–137 elements on the whole page (by layout), built in 2–4 ms, 32 KB. Expanding a card sends that card
only: 74 KB for Class 3D 013 with its class slices, 39 KB for Refine 3D 019 (today: the whole map, 600 KB).
Selecting a job and finding send nothing.

### Layouts (no dragging, no zoom)

- **Tree** (`tree-whole-project.png`): each chain runs down its own column, a parent centred over what reads it. A
  job's kids that end there (its PostProcesses, a Class 3D tried once) stack into one column. CryoSPARC's tree view
  is the reference.
- **Lanes** (`lanes-whole-project.png`): the tree pinned left. The deepest chain runs straight down under its
  parent, every other branch steps right, and the jobs that end at a job hang beside it. AGG412_STA's 412 chain,
  set 412_ge8p5 → Class 3D 013 → Refine 3D 015 → Warp export 018 → Refine 3D 019 → CTF refine 031 → Refine 3D 024,
  is one column, with Class 3D 014, PostProcess 020/030 and 028/029 beside their parents. The whole project is
  1,336 px wide. A preprocessing project reads as its trunk straight down, Import to Reconstruct, then its branches
  (`preprocessing-lanes-lineage.png`).
- **Layers** (`layers.png`): a row per generation. A job sits one row below the deepest job it reads, side inputs
  included, and rows wrap at the view's width.
- **Grid** (`grid.png`): every card in the order the jobs were made, wrapping at the view's width, CryoSPARC's card
  view. No cables at rest; selecting a job draws its lineage's cables across the grid (`grid-lineage.png`).

All four come with **bands per species** (on by default when the project has more than one): AGG412_STA's ribosome
band is Warp export 001, Reconstruct particle 002 and a column of the eight Class 3D jobs, each fed by its own set.
**Sets and files** another job did not make are drawn where they enter. A set read by several jobs is a pill at the
top of their tree (set 412_ge8p5, 1,881 particles, 329 tomograms, read by Warp export 003, Reconstruct particle 004
and Class 3D 013). A set read by one job rides on its card as a tab. **Side inputs** (reference, mask, half maps)
are dashed cables into their row's port, faint; "On select" hides them except on the selected lineage. **Cards**
small or large, each card's expand icon toggles it alone, and the cards around it move (`large-card-class3d-013.png`).
**Width**: natural (scroll down), or fit (shrink to the view's width, never below 45 %).

### Lineage (a click on a card or a roster row)

The lineage is everything the job was made from and everything made from it, through main and side inputs. The
roster's rows light up with the cards, the title line says "Refine 3D 024 · 11 above · 2 below · esc", and ↑ ↓
walk to what the job read and what read it, ← → to its siblings. Esc clears.

- **Dim the rest** (`lineage-dim.png`): the job ringed, its lineage kept, everything else faded.
- **Up and down** (`lineage-updown.png`): what it was made from ringed in ink and tagged ↑ input, what was made from
  it dashed in blue and tagged ↓ made from it; the rest half faded. The roster's rows carry the arrows.
- **Trace** (`lineage-trace.png`): nothing fades; the lineage's cards get a blue edge and its cables march.

### Find (`f` or `/`, while the Jobs view has the focus)

A frosted-glass bar over the view, nothing else on the page changing but the highlights. Matches stay, the rest
fades, in the map and the roster alike. The matched letters are marked where they show, with the browser's own text
highlights (`CSS.highlights`), so the cards' HTML is left alone.

- **Spotlight** (`find-spotlight-ref.png`): the bar alone, in the middle of the view. "ref" finds Refine 3D 015,
  019 and 024, then CTF refine 031: "1 of 4". ↵ and ⇧↵ walk the matches and scroll to each; Esc closes and clears.
- **Palette** (`find-palette-clas.png`): the bar with the matches listed under it, ranked. ↑ ↓ choose, ↵ selects the
  job and shows its lineage.
- **Dock** (`find-dock-post.png`): a small bar in the view's corner that stays while you click around; the matches
  stay lit.

Matching: each word typed must start a word the job is known by. In rank order, the words are its type's name
("Class 3D", "Refine 3D"), its kind and a few aliases (`pp` for PostProcess, `classification`, `tm`), then its
species, status, tool and own parameters (`failed`, `ribosome`, `6 classes`), then the name of the set or file it
reads. A job number matches as 13, 013, #13 or job013, and several words narrow the match ("class 412"). Once
something matches by its name, matches only through a set's name or a run of letters inside a word drop out: "clas"
finds the eleven Class 3D jobs (and the map file named class2_…), not the two masks cut from that file.

## 3. How the real map gets fast

The prototype's way, carried into `ui/branch_map/`:

- **One element per card.** The card and the large card's blocks become HTML builders (`card.py`, `blocks.py`), styled
  by classes in `ui/dashboard/css.py`. Tooltips become `title` attributes (the house title tip already draws them),
  and the "+" and number menus are built when opened: one menu per map, filled for the card that asked.
- **The browser lays out.** The chosen layout is a skeleton of plain divs built once per change of the graph's shape,
  and the cables are drawn by script after layout. `geometry.tidy` and `de_overlap`, the `Box` heights computed
  before drawing, dragging, panning and zooming all go.
- **Keyed updates.** The view keeps its cards by job id. A card's own signature (status, inputs, a digest of its
  tilt-series states, its results' stamp, large or not) redraws that card only, and the shape's signature (the jobs,
  the edges, layout and grouping) rebuilds the skeleton (about 130 elements).
- **Selection, lineage and find in the browser**, over the graph sent once per change of shape; one event tells the
  server which job is selected, for the roster and the job page.
- **Pictures by URL.** Composites and class slices are written to a cache file and served like the rest
  (`/api/vis-asset`), never as data URLs.
- **Compact magazines.** One path per state (or a canvas), with a hover that finds the tilt series from the pointer.
  A running job's magazine updates its own card only.
- **Results arriving** redraw that card only, several landings in one tick together. The model is built in a thread
  and kept per signature.

## 4. Decisions owed (ask first; recommendations given)

- **(a) Layout.** Tree, Lanes, Layers or Grid. Recommended: Lanes (a chain costs height, never width; ends beside
  their parent), and Grid as a second view if a card overview is wanted.
- **(b) Lineage look.** Dim the rest, Up and down, or Trace. Recommended: Up and down, which says which way each
  job lies.
- **(c) Lineage reach.** Through side inputs too (reference, mask, half maps), as prototyped, or main inputs only.
  Recommended: through side inputs.
- **(d) Find look.** Spotlight, Palette or Dock. Recommended: Spotlight, as asked, with ↵ walking the matches.
- **(e) Find scope.** The roster reacts with the map, and `f` works in the Jobs view without the map open (the map
  now opens only from the roster's icon, H25). Recommended: yes.
- **(f) Species bands** when the project has more than one species. Recommended: yes.
- **(g) Side-input cables.** Faint at rest, or only on the selected lineage. Recommended: faint.
- **(h) Sets and files on the map.** A pill when several jobs read it, a tab when one does: the small form of
  roadmap 32 §15.9's "a set as a card of its own". Recommended: yes.
- **(i) Scale.** No zoom; natural size scrolling down, with Fit as the one scale control, off by default.
  Recommended.
- **(j) Today's map until then.** Quick relief first (menus on demand, `title` tooltips, results redrawing their own
  card, pictures by URL: about a day), or straight to the build, whose S1–S3 replace the same code. Recommended:
  straight to the build.

## 5. Stages (after §4)

- **S1 — Cards as HTML.** `card_html` and the large card's blocks as builders over the live `Project`; tooltips as
  `title`; the two menus on demand. The new job's card (a real form, ✓ / ✕) stays NiceGUI elements.
- **S2 — The view.** The skeleton of the chosen layout, keyed card updates, the two signatures, cables drawn by
  script, bands, sets as pills and tabs, Fit. Dragging, panning, zooming and the stored positions go.
- **S3 — Selection and lineage in the browser.** Roster rows carry `data-nid`, `panel.select` hears one event, the
  lineage look from (b), and arrow keys walk the lineage.
- **S4 — Find.** `f` and `/` with the Jobs view focused (`data-cb-focus` on `#cb-roster-panel` or
  `#cb-view-pipeline`; never while typing in a field, nor with a dialog or menu open), the look from (d), the match
  tiers, the roster and the map reacting.
- **S5 — The weight.** Compact magazines, pictures by URL, results landing per card, the model in a thread.
- **S6 — Adding a job on the new map.** The "+" opens the job-type menu as today, and the new card appears where it
  will sit (beside or under its parent). An input's candidates are shown the way find shows matches.
- **S7 — Retire.** `geometry.tidy` and `de_overlap`, the pan and drag script, `MapState.moved`, `zoom`, `tx`, `ty`
  and their prefs, and `/dev/map-ideas`.

**Checks**, on AGG412_STA and on a tilt-series project (an AGG412 rerun) with `scripts/dev_map_probe.py`: about one
element per card; a size toggle sends one card's update; selecting and finding send nothing; no watchdog line while
every card goes large; a running array job's tick redraws only that job's card.

## 6. Pickup

Restart the dev server (the route is in `ui/main_ui.py`) and open `/dev/map-ideas`, e.g.
`/dev/map-ideas?project=AGG412_STA&layout=lanes&lineage=updown&find=spotlight&group=species`. Click inside the
Pipeline frame, then press `f`. The screens are in `notes/screens/map-ideas/`: `today-map-small.png` and
`today-map-all-large.png` (today, fitted to the view), the four layouts, the three lineage looks, the three find
looks, a large card, and a preprocessing project in Lanes. Ask (a)–(j), then build S1–S7 back to back.
