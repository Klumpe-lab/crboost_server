# Roadmap 25 — Projects Overview: calmer rows + disk usage per project

**Status:** scoped and **built 2026-10-02 (A + B), NOT RUN**. Verification so far: ruff only (no interpreter in the assistant sandbox); `python -c "import main"`, `python check_boundaries.py` and the user check below are owed. Two parts, one widget:

- **Part A — the row redesign.** The maintainer's walkthrough: the list is "too colorful and motley,
  too much going on in buttons, info and metadata". It also differs between its two mounts for no
  stated reason.
- **Part B — disk usage.** Came out of a storage sizing pass for full reprocessing of nine
  412/MyrGFP datasets. That pass found 9.7 TB in `crboost_data` across 19 projects: 1.6 TB in
  `Trash/` and 2.4 TB in motion-correction `.staging/` copies. None of it is visible in the app.

Build A first: B's size cell is designed into A's footer.

## Before → After

**Before:** each row has a coloured initials avatar, species dots, a green job-progress bar with an
`n/m` count and run/fail counters, a copy icon, and hover-only transfer and delete buttons. The
project path is squeezed onto the title line; the raw-data path is a hover on a folder icon. Inside a
project (the switcher dialog) the same widget loses delete and transfer. On a narrow landing page the
list scrolls sideways. How much disk a project uses means `du -sh` in a shell.

**After:** a calm four-line row: name; project path; raw-data path; one footer with delete at the
left and well-spaced TS · date · size. The only colour is the status (done / failed / pulsing live)
and the size, which warms as a project gets heavy. Both mounts behave identically. No sideways
scrolling at any width. **Gain:** the list reads at a glance, and cleanup decisions happen on the
page where projects get deleted.

---

# Part A — the row

## A.1 One widget, one behaviour on both mounts

Both mounts already use **the same class**, `ProjectsOverview` (`ui/projects_overview.py:74`):

- the landing page (`ui/data_import_panel.py:1497`);
- the in-workspace switcher dialog (`ui/pipeline_builder/pipeline_roster.py:1539`).

The switcher shows no delete or transfer because it passes `on_delete=None` and no `on_transfer`.
That was deliberate: the class docstring (`:83–87`) says "we don't want users nuking projects
mid-session". The open project's row already never shows delete (`:699`, `not is_current`), so that
guard is enough.

**Change:**
- `ProjectsOverview` owns delete itself, so there is no per-mount callback to forget. The rmtree +
  `remove_project_state` body moves out of `data_import_panel._delete_project_on_disk` (`:478`) into
  a `backend.delete_project(path)`. `on_delete` and `on_transfer` go away as parameters.
- In the switcher's preview mode, deleting the *previewed* project resets the left pane to the open
  project.

## A.2 The row, top to bottom

```
┌──────────────────────────────────────────────────────────────────────────────┬───┐
│ agg_SK_20251017_412                                              ● DONE      │   │
│ /groups/klumpe/crboost_data/agg_SK_20251017_412                              │ › │
│ /groups/klumpe/001_Data/412/SK_20251017_412                                  │   │
│ 🗑     137 TS      2026-10-01 14:02      572 GB                                │   │
└──────────────────────────────────────────────────────────────────────────────┴───┘
```

| line | content | type |
|---|---|---|
| 1 | name (12 px / 600, mnemonic in the hover as today) · status at the right | as today |
| 2 | the project's absolute path | `MONO` 9 px, `CLR_LABEL` |
| 3 | the raw-data path the project was created from (`source_directory`, already in the scan dict, `backend.py:1252`) | `MONO` 9 px, `CLR_SUBLABEL`; hover says "Raw data" |
| 4 | delete (hover-revealed, as today) at the far left, then TS count, last activity and size, spaced apart | `MONO` 9 px |

- **Paths are click-to-copy.** On hover they turn blue and underline; a click copies, then shows a
  short "Copied" toast. The copy icon goes. Add a `copy_on_click(element, text)` helper next to
  `copy_button` in `ui/components/copyable.py`, reusing `_write_clipboard`. The hover style goes in
  `ui/dashboard/css.py` (`.cb-copy-path:hover`), per the CSS-delivery rule.
- Both paths truncate at the tail with an ellipsis and carry the full path in the hover.
- **No raw-data path recorded** (old or imported-tomogram projects): line 3 says so in `CLR_GHOST`
  italics. It never borrows another path.
- **Taller:** text-stack padding goes from `5px 4px 6px 10px` (`:590`) to about `9px 8px 10px 14px`,
  and the line gap from 2 to 3 px. The avatar column is gone, so line 2's 24 px indent (`:643`)
  drops to 0.

## A.3 Removed from the row

| element | where | note |
|---|---|---|
| initials avatar circle | `:596–604` | `avatar_color` stays exported: `ui/aggregation/aggregate_dialog.py:52` and `merge_card.py:48` import it |
| species dots | `_render_species_dots`, `:755` | the species **filter** in the sorter bar stays; dots may come back later |
| progress bar, `succeeded/total` count, `n run` / `n fail` labels | `:656–670`, `_render_progress_bar` `:788` | counts move into the status hover |
| copy icon | `:634–635` | replaced by click-to-copy paths |
| transfer-ownership button | `:683–697` | **this is the only UI entry point to ownership transfer** (`backend.transfer_project_ownership` stays; the widget-side `_handle_transfer` / `_show_transfer_dialog` were deleted with the button, as dead code); see A.7 |
| raw-data folder icon | `:678–681` | line 3 shows the path itself |

## A.4 Status: done / failed / live only

- **done**: teal dot + `DONE`. **failed**: red dot + `FAILED`. **live**: blue dot + `LIVE`, the dot
  **pulsing**.
- **idle** renders nothing; a project that is neither running nor finished needs no badge.
- The pulse is a `@keyframes cb-live-pulse` in `ui/dashboard/css.py`, next to the existing
  `cb-artiax-breathe` (`:1320`). It is CSS, never a timer, and never added to `main_ui.py`'s shell
  block (CLAUDE.md).
- Status hover: `16/18 jobs · 1 failed · 1 running` (the counts the bar used to carry).

## A.5 Switcher dialog: full screen

The dialog card is `96vw / max 1440px / 88vh` (`pipeline_roster.py:1461`), and the parameter pane is
480 px capped at 44 % (`:1520`). Make the dialog Quasar `maximized` (full viewport), and widen the
left pane to about 560 px (cap 40 %) so the project parameters stop wrapping. The roster's
`height_css` (`:1548`) follows the new height.

## A.6 No sideways scrolling at narrow widths

The roster sits in a `ui.scroll_area` (QScrollArea), which scrolls both ways. Rows already clip
(`overflow: hidden`, `:567`). The header (`:255`, `flex-wrap: nowrap`) and the sorter bar (`:278`,
`nowrap`, a fixed-width species select) do not wrap. Exactly which element is too wide has to be
confirmed in a browser at the failing width; the fix is the same either way:

- the header and sorter bar wrap (`flex-wrap: wrap`), with the counts label dropping to its own line
  first;
- the footer (line 4) wraps its meta items rather than pushing them past the edge;
- the scroll area's content gets `overflow-x: hidden` and `max-width: 100%`, so a too-wide child
  clips rather than scrolls.

Check at 900, 1200 and 1440 px browser widths on the landing page.

## A.7 Decisions taken, open to veto

1. **Ownership transfer** loses its button and has no other UI entry point. The backend and the
   `owner` field stay. If transfers still happen, it needs a home elsewhere (e.g. the workspace
   Project parameters panel). Not built here.
2. **Delete stays hover-revealed**, as today; it reads as clutter when always visible.
3. **The size ramp avoids red** (§B.1), because red is FAILED in this same row.

## A.8 Control row + compact view (second pass, built 2026-10-02, NOT RUN)

After the first pass ran, the maintainer asked for one control row and a compact mode:

- **One row:** title and total size on the left; on the right, Order (`render_segmented(..., classes="cb-seg-sm")`,
  a 16 px strip matching `.cb-field`), the species select (no caps label; "any species"), `All | Mine`, `Compact | Full`,
  the browse-base folder icon (landing only, via the new `on_browse` callback) and refresh. It wraps on
  a narrow pane. The "30 projects · 2 live · 4 failed · in base/" counts are gone, as is the
  `(mine/total)` count on Only mine; the total size stays.
- **Compact** (`UserPreferences.projects_compact_view`, **default on**, persisted like Mine): one line per project with
  name, owner ("you" / "Lab / Shared" / user), TS, size and status. No owner sections; one flat list
  in the chosen order.
- **No knob switches:** Mine and Compact are the same 16 px two-way strips as Order (`_small_segmented`); the
  scaled Quasar `ui.switch` read as a blob next to them, and a strip names both states.
- **Landing page:** the roster's scroll area is `calc(100vh - 104px)`, so it reaches the bottom of the
  viewport. The offset is an estimate (page padding + status strip + control row); tune it if a gap
  or a page scrollbar shows. The text "Browse for another base location" row under the roster is gone.
- **Switcher:** the roster height offset drops from 148 to 118 px (one control row instead of two),
  another estimate.

## A.9 Third pass (built 2026-10-02, NOT RUN)

- **Only the chevron enters a project.** A row click previews (switcher) or does nothing (landing; cursor
  stays default). `on_open` is gone from `ProjectsOverview`, and with it the landing page's
  `handle_load_project` and the switcher's `_switch_project`, which only that callback reached. Entering
  is the chevron's `<a href>` to the routed URL (`ui/open_project.load_project_into_tab`), which the
  switcher already used. The chevron is a tinted column that lights on its own hover only
  (`.cb-proj-chevron`, `ui/dashboard/css.py`; the row-hover rule is removed).
- **Delete** sits at the right of the full row's footer and at the right of the compact line
  (`_render_delete`, fixed slot, empty on the open project). The full footer (TS · last activity · size)
  now starts flush with the paths above it.
- **Hover preview:** a project with WarpTools reconstruction PNGs shows a white hover card to the left of
  the row (a `ui.menu` opened and closed by client-side JS, `_hover_open_js`: about 90 ms intent delay plus
  one round trip, and it stays open while the pointer is on it). Chevrons step through every tomogram of
  the newest tsReconstruct job; under the image: tomogram name + i/n, tomograms/TS, Å/px, size, species.
  Auto-refresh holds while a card is open. The route serves a cached 512 px JPEG (Pillow) instead of the
  ~1 MB PNG, falling back to the PNG without Pillow. `backend._find_tomo_preview` lists that one directory, cached per project until
  its activity stamp moves. Served by a new `/api/project-preview` route (`main.py`) that only serves
  `.../warp_tiltseries/reconstruction/*.png` inside a tree holding a `project_params.json`; vis-asset's
  loaded-roots guard does not fit a roster of projects nobody has loaded.

---

# Part B — disk usage

## B.1 What the user sees

The **size** sits in the row footer (A.2): `572 GB`, `2.9 TB`.

**Colour ramp (this text only, never the row):**

| size | colour |
|---|---|
| ≤ 50 GB | `CLR_SUBLABEL` (`#94a3b8`), quiet |
| 50 GB → 1 TB | interpolated **on a log scale** (50 → 1000 is 20×; linear would leave everything under ~300 GB looking alike) through amber `#d97706` to orange `#c2410c` |
| ≥ 1 TB | `#c2410c`, weight 600 |

A pure `size_color(bytes) -> str` in `projects_overview.py`.

**Hover on the size:**

```
572 GB on disk · measured 2026-10-02 14:02
Preprocessing   237 GB   Motion & CTF 143 · Reconstruct 81 · Alignment 13
Particles/STA   174 GB   Template matching 161 · Subtomo extraction 13
Trash           161 GB   deleted jobs
Other             1 GB
```

Buckets come from §B.3. Per bucket, the top three jobs are listed by display name.

**States, all visible and none defaulted:**

| state | text | hover |
|---|---|---|
| never measured / queued | `— GB`, `CLR_GHOST` | "Not measured yet — queued" |
| measured | `572 GB`, ramp colour | breakdown above |
| part of the tree unreadable | `≥572 GB` | breakdown + "N directories unreadable (permissions)" |
| measurement failed | `? GB`, `CLR_FAILED` | the error |

**Header counts** (`:402–415`): append `· 9.7 TB` (the sum over visible measured projects). When some
are not measured yet, show `≥ 9.7 TB` and say how many in the label's tooltip.

## B.2 Measurement

New `services/project_disk_usage.py`, with a pure function `measure(project_dir, jobs) -> DiskUsage`:

- A recursive `os.scandir` walk using `lstat` that sums `st_blocks * 512`. That is what `du` reports.
  Checked 2026-10-02: an `st_blocks` sum over `Aaron_Donut_20260205_412_FCIso_unmilled2` gives
  2927.8 GiB, identical to `du -sk`.
- **Never follow symlinks.** `frames/` is all symlinks into `/groups/klumpe/001_Data`. Following them
  would charge the raw data to every project that imports it. (`Pos_28_Donut_Hunt` holds 176 TS of
  symlinks and must read ≈0.)
- Hardlinks are counted once per `(st_dev, st_ino)` when `st_nlink > 1`, as `du` does.
- `FileNotFoundError` mid-walk means a job was re-run or deleted during the walk. Catch it narrowly
  and skip the entry; it is expected and ignorable. `PermissionError` on a subtree is **counted and
  surfaced** as `unreadable_dirs`, never silently undercounted.
- Cost, measured 2026-10-02 with `find -printf %b` on warm NFS: the biggest project (150k entries,
  2.9 TB) takes 5.4 s, and `agg_SK_20251017_412` (18.7k entries) takes 1.6 s. Expect Python to be
  2–4× slower, so the whole of `crboost_data` takes roughly 1–2 min, once. That is too slow for the
  15 s scan (`ui/data_import_panel.py:1503`) and cheap enough for a background pass.

## B.3 Buckets

Each byte is attributed by its top-level path inside the project:

| path | bucket |
|---|---|
| a job dir (`External/jobNNN/`, `Import/jobNNN/`, …) whose `relion_job_name` is in `project_params.json` `jobs` | by that job's `job_type`: **Preprocessing** if `JOB_SPEC_BY_TYPE[t].phase == PHASE_PREPROCESSING` (`services/jobs/spec.py:44`) or `t is TS_IMPORT` (phase `None`, a hidden prerequisite, `spec.py:101`). Every other known type → **Particles/STA** (TM, candidates, extraction, reconstruct-particle, Class3D, `EXTRACT_PICK_LIST`) |
| `Trash/` | **Trash** |
| a job dir with no `jobs` entry | **Other**, labelled "unlisted job dirs" |
| everything else (`Logs/`, `Schemes/`, `templates/`, `registry/`, `Curation/`, dotfiles) | **Other** |

Read the raw `jobs` dict from `project_params.json`, not the scan's `_is_pipeline_job_dict`-filtered
one (`backend.py:1236`). Otherwise pick-list extractions fall into "unlisted".

Sanity check against the 2026-10-02 sizing for `agg_SK_20251017_412`:

| bucket | jobs | size |
|---|---|---|
| Preprocessing | job002–006 | ≈ 237 GB |
| Particles/STA | job011–017 | ≈ 174 GB |
| Trash | | 161 GB |
| Other | | ≈ 1 GB |
| **Total** | | **572 GB** |

## B.4 Cache and freshness

- **Persisted per project** in `<project>/.disk_usage.json`, holding schema, `measured_at`,
  `total_bytes`, per-bucket bytes, per-job bytes and `unreadable_dirs`. Kept next to the data so
  every server sees the same measurement (roadmap 23 runs one server per person) and it survives
  restarts. The 15 s scan already opens `project_params.json` per project, so one more small file
  costs nothing. If the write fails (a read-only project), keep the result in memory and log it once.
- **One background sizer**: a service singleton (`get_disk_usage_service()`) with a single-flight
  queue that measures one project at a time via `asyncio.to_thread`. It touches no UI. Background
  tasks have no client context; the overview picks values up on its next 15 s refresh.
- **When a project is (re)queued:** `_scan_for_projects_sync` (`backend.py:1182`) merges the cached
  usage into each project dict (`"disk_usage"`) and enqueues a project when it has no cache, or when
  `last_activity_ts > measured_at` and the cache is older than 10 min. The 10 min throttle stops a
  project with running jobs from being re-walked every 15 s.

---

## Files

| file | change |
|---|---|
| `ui/projects_overview.py` | A.2–A.4 row, A.6 wrapping, B.1 size + ramp + hover, header total; delete owned here |
| `ui/components/copyable.py` | `copy_on_click` helper |
| `ui/dashboard/css.py` | `.cb-copy-path:hover`, `@keyframes cb-live-pulse` |
| `ui/data_import_panel.py` | drop `_delete_project_on_disk` / `_transfer_project_owner` wiring |
| `ui/pipeline_builder/pipeline_roster.py` | switcher `maximized`, wider left pane, reset preview on delete |
| `backend.py` | `delete_project`; scan merges `disk_usage` and enqueues stale projects |
| `services/project_disk_usage.py` (new) | `measure()`, cache read/write, `DiskUsage` model, sizer service + queue |

Results and exceptions follow the CLAUDE.md idiom: the measurement function raises; the service
catches, calls `logger.exception`, and stores the error, which the cell shows as `? GB`.

## Not in scope

- **Acting on it.** Empty-Trash, delete-staging and "reclaim" buttons belong in a separate roadmap,
  once the numbers have been trusted for a while.
- **The motion-correction `.staging` duplicate itself.** `collect_fs_outputs`
  (`drivers/fs_motion_and_ctf.py:203`) copies each TS's outputs into the job dir and never removes
  the staged originals. That doubles the motion job (2.4 TB across the 412 projects). Only the motion
  job's `.staging` is unreferenced downstream. Alignment, CTF, reconstruct and extraction `.staging`
  dirs are referenced by Warp XML `DataDirectory` entries and particle stars (checked in
  `agg_SK_20251017_412`, 2026-10-02). That fix belongs in the driver and gets its own entry.
- A new home for ownership transfer (A.7).
- Raw data size (it lives outside the project), sorting by size, filesystem free space / quota.
- The `parent slot of the element has been deleted` timer tracebacks in the server log (pasted
  2026-10-02). There is no app frame in the trace, so the timer's owner is unidentified. It is not
  attributed to this widget until a repro says so.

## User check (runtime)

1. **Landing page:** no avatars, species dots or progress bars. Four lines per row. Paths turn blue
   on hover and copy on click. A running project's dot pulses.
2. **Switcher** (from inside a project): full screen; the same rows, with delete on every row except
   the open project; the parameter pane no longer wraps paths.
3. **Narrow:** at 900 / 1200 / 1440 px browser widths the landing roster never scrolls sideways.
4. **Sizes:** within ~2 min every row has one. `agg_SK_20251017_412` reads ≈ 572 GB, with the B.3
   breakdown on hover. `Aaron_Donut_20260205_412_FCIso_unmilled2` reads ≈ 2.9 TB in the strongest
   orange, with Trash ≈ 834 GB. Both match `du -sh`. `Pos_28_Donut_Hunt` reads ≈ 0.
5. Run a job in a small project. Its size updates within ~10 min of the job writing output.
6. The 15 s overview refresh does not get slower (the scan does no walking itself).
