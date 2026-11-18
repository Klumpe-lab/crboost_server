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

- (a) Roster: **A2 Rail** ("amazingly close to what I envisaged").
- (b) The count opens the magazine; the name keeps opening the job page (the interactions were approved as shown).
- (c) Magazine: **B3 Tiles**, the marks squarer (20 x 17 px, 2 px apart, `Pos·Beam` in 7 px mono) and a muted
  green for done (`#5a9c80` instead of the status green). The other marks stay: amber = waiting in the queue, blue
  blinking = running, red = failed, grey = not run, lighter grey = skipped.
- (d) Graph: **none**. The branch map stays as it is ("all of the options are by far inferior to what we have now").
  C1–C4 are dropped, and S4 with them.
- (e) Glyphs: **drawn**.
- (f) Toasts: the first round (stamp, ledger, stripe, ink tag) was turned down ("some of them I can't even read").
  Second round on the page: T0 copied-in-place for copies, and T1 card / T2 line / T3 status bar for everything
  else, all dark ink on light, in the popup card's and the title stripe's chrome. **Open.**

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

## 4. Stages, once decided (each built in the app, then checked once at the end)

- S1 The glyphs and the job-type description move onto `JobSpec` (`services/jobs/spec.py`), and `DESCRIPTION` in
  `ui/jobs_ideas/data.py` goes away.
- S2 The roster (`ui/pipeline_builder/pipeline_roster.py` `render` / `_render_instance_row`): the gutter as chosen.
  Removed: the multi-instance header, the clone icon, the chevron and `_render_ts_sub_rows`,
  `_expanded_instances`, and the `_array_ts_cache` work in `signature()`. The tilt filter's settings, which
  hang under its row today, need a new home (its job page, or its own popup). The signature keeps the counts.
- S3 The magazine popup as a component (`ui/components/magazine.py`): it reads `array_status_cache`, reads the task
  log off the loop (`asyncio.to_thread`), and the task card's buttons go through `panel.switch_to_job_subsection`
  and `callbacks["select_ts"]`. It closes on any roster rebuild that removes its job.
- S4 (dropped: the branch map stays.)
- S5 Toasts: the chosen rule in `ui/dashboard/css.py`, with its colours inside `@layer theme` (see above), replacing
  today's `.q-notification` block; `copy_on_click` / `_write_clipboard` (`ui/components/copyable.py`) switch to T0
  if it is chosen; the default position moves from Quasar's bottom centre.
- S6 Delete `/dev/jobs-ideas` and `ui/jobs_ideas/`.
