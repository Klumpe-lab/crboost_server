# Roadmap 66: the event loop stays free: reads before draws, dense lists as one element, the gallery's fill in workers

**Asked (maintainer, 2026-11-20).** The UI has become laggy, most of all when the Tomograms overview loads, in the
pipeline viewer and when switching between jobs: find out why, fix the obvious things, and say what larger changes
would help. After the assessment (§1–§5): record it here; it is built later. The site's layout must not change.

**Status (2026-11-20).** Scoped, nothing of this roadmap built. The obvious causes are fixed as H33 in
[HOTFIXES.md](HOTFIXES.md) (`23b8420`; the dev server needs a restart): every child process off the loop
(`services/computing/processes.run_process`), the curation polls' file reads in a thread, the Journey's collect off
the loop at mount, at a pick-list save and at a section switch, and the log file written by a thread. What is left
is below; D1–D4 (§4) are open.

Where the work runs is roadmap 34's frame ([completed/roadmap_34-ui-latency-and-workers.md](completed/roadmap_34-ui-latency-and-workers.md)):
disk reads in a thread, CPU-bound Python in a worker process (`run_in_worker`, `services/workers.py:144`), pictures
as `Render`s. The map's rebuild into HTML cards (roadmap 62 §7, commit `50685a0`) is the pattern for the dense
views here.

## 1. What the stall log says (read 2026-11-20)

From `~/.crboost/logs/crboost-24326.log` (dev) and `crboost-24330.log` (lab install). Only lines after H28's fix
(2026-11-19 13:42) name the server function a stall was spent in; earlier lines place it in the standard library.

| place | stalls | length | cause | now |
|---|---|---|---|---|
| `slurm_service._run_command`, `(of 1 checks)` | 13 | 1.3–4.4 s | uvloop forks the whole server per squeue holding the GIL; worse under the user cgroup's reclaim | fixed, H33 |
| `session_service._find_live_registered`, `get_curation_session_info` | 8 | 1.3–7.2 s | registry, session.json, scope.json read on the loop, on NFS | fixed, H33 |
| `dashboard_data._extracted_ts_cached` | 2 | 2.1, 2.4 s | the Journey's mount collected (a particles.star parse) on the loop | fixed, H33 |
| `logging/handlers.py:shouldRollover` | 1 | 1.2 s | the log file stat'd and written per record, on NFS | fixed, H33 |
| `tomo_gallery._refresh_overlays` | 1 | 1.3 s | dot layers of every tile redrawn | §2.4 |
| `ui/branch_map/*` (draw, ports, labels) | 6 | 1.0–8.5 s | the map built every card as elements | fixed by roadmap 62's HTML cards (`50685a0`, 2026-11-19 14:26); none since |
| `pathlib.py:open` / `stat` without a caller | many | up to 14.3 s | unplaceable before H28 | unknown; S0 and the watchdog's log after H33 say |

**Two more numbers.**
- The gallery logs its own load: on AGG412_STA "wall shown in 0.5–2.1 s, filled in 2.7–7.9 s"; on Kilifish 0.0–0.6 s
  and 0.5–1.5 s. The fill's reads already run in threads (`ui/tomo_gallery.py:749–778`); what is slow is §2.4.
- The stall lines' UI load reads 10,000–29,000 elements and 40–120 polled bindings for one or two tabs. Which views
  hold them is not measured (S0); the counts in §2 are read from the code.

**The headnode.** The user's slice on cbe-login (`/sys/fs/cgroup/user.slice/user-16330.slice`) is capped at 8 CPUs
and 64 GB, page cache included, shared by the server, the user's Claude Code sessions and everything else they run;
`memory.events` counted 19,636 hits on the cap and `memory.peak` was 65 G on 2026-11-20. `~` and `/groups` are
NFS, so `~/.crboost` and the checkout's `.uv/` interpreter and environment are read over the network. That is why
one fork took seconds, and it slows every stall in §2. D3 is about it.

## 2. What is left, by view

### 2.1 The Journey's pane (switching tilt series)

`_render_main_pane_for_ts` (`ui/tomo_dashboard_dialog.py:809`) draws eight analysis sections and the particles
section, synchronously, on every tilt-series switch. The sections' data mostly comes from the in-memory tilt-series
registry, but each reaches disk while building:

- `job_dir_for` (`services/dashboard_data.py:98`): two `is_dir` per section;
- `tilt_thumb_urls` (`services/dashboard_data.py:295`): one `is_file` per tilt, in four sections (Motion & CTF,
  alignment, TS CTF, tilt QC), so about 160–240 stats per switch on a 40–60-tilt series;
- `ensure_tilt_thumbnails` (`services/tilt_series_service.py:266`) in the same four sections;
- the dataset section's `_read_tomohand_from_import_star` (`ui/tomo_dashboard_dialog.py:895`, a `starfile.read`);
- the reconstruct section (`:2291`): the tomograms star, a JSON, `vis_asset_url` (one stat per picture,
  `services/dashboard_data.py:138`).

The particles section (`render_particles_section`, `ui/particles/pick_viewer.py:1801`) reaches about 70 helpers
of that module. Its pictures are `Render`s already; its reads are not: `read_tomograms_table`,
`derive_keep_state_for_list` (`services/particles/picks_filter.py:452`), `read_preview_manifest`
(`services/visualization/preview_orchestrator.py:566`), `subtomo_particles_stars`
(`services/particles/subtomo_link.py:146`), globs and stats. `_render_gallery_body` (`pick_viewer.py:2699`) alone is
778 lines.

### 2.2 Job pages (switching jobs)

A job page is built synchronously when it is not one of the kept pages (`_show_job`,
`ui/pipeline_builder/pipeline_builder_panel.py:211`). Its renderers read little: the files tab lists its directory
(`ui/pipeline_builder/files_tab.py:17`), Subtomo extraction's banner reads about nine files
(`ui/job_plugins/subtomo_extraction.py:96–243`), Subset its rows (`ui/job_plugins/subset.py:56`), Template matching
its template list (`ui/job_plugins/template_match.py:335`), Segment its model note
(`ui/job_plugins/segment.py:158`). The cost is the elements: an array job's task tracker
(`ui/components/array_task_tracker.py:227` `_build_rows`, `:339` `_paint_rows`) builds a row of about five elements
and three click handlers per tilt series, about 750 elements for 151 tilt series. It is in the pre-H28 stall lines.

### 2.3 The Journey's tilt-series switcher

`_build_switcher` (`ui/dashboard/strip.py:207`) builds the switcher menu's tilt series × species table as elements
(a row per tilt series, a cell per species, a tooltip per species head), inside a `ui.menu`, at every strip rebuild.

### 2.4 The Tomograms gallery's fill

`reload` (`ui/tomo_gallery.py:690`) shows the wall, then `_fill_sources`, `_fill_layers` and `_fill_tilts`
(`:749`, `:760`, `:770`) run `source_prep` (`:383`), `sources_by_ts` (`:539`), the pick and segmentation layer
collection and `collect_tilt_groups` (`ui/tilt_previews.py:378`) in threads, then land the results as elements.
The threads parse star files with pandas and hold the GIL most of the time, three at once beside the loop. A tile is
about 8–15 elements (frame, picture, overlay, fills, dot layers, the mosaic toggle with its tooltip, label), so a
wall of 150 tomograms is 1,000–2,000 elements and a source switch or a layer change repaints them
(`_refresh_overlays`, `:1406`).

## 3. Rules: the layout does not move

- **A read/draw split changes no draw.** The draw functions keep their elements, classes and order; they are handed
  what they used to read.
- **A view becomes HTML only when it is a dense list that is read but not edited**: task rows, the switcher's table,
  gallery tiles. Parameter forms, the I/O config, dialogs, the Journey's charts (Plotly) and anything with an input
  stay components.
- **The HTML is the markup the components make now**: the same classes and inline styles; icons as Quasar draws
  them (`<i class="q-icon notranslate material-icons">`); tooltips as `title`, which the app's title-tip script
  shows (`_TITLE_TIP_JS`, `ui/dashboard/css.py:2525`) in the same box as Quasar's (one rule styles
  `.q-tooltip--style` and `.cb-title-tip`, `ui/dashboard/css.py:962–970`).
- **Clicks and hovers go through one script per view**, delegated from the view's root, back to the server as events,
  as the map's do (`ui/branch_map/assets.py`). That is where a conversion can break, not the look.
- **Every conversion is checked by before/after screenshots** of the same project with the headless shooter
  (`~/.crboost/ui_shots/shoot.sh`, or `scripts/dev_screenshot.py`), and by a click on every control it holds.

## 4. Decisions

- **D1, the order.** Proposed: S0, then S1 and S4 (about 2 days, the biggest gain for the effort), then S2, S6, S3,
  S5, S7.
- **D2, gallery tiles as HTML (S7).** The most behaviour to carry over (zoom, mosaic toggle, overlay repaint,
  prefetch) for a gain only in painting. Proposed: decide after S0 and S6 have measured what is left.
- **D3, the server's place.** On the shared, capped slice and on NFS, a fork or a page-in can take seconds whatever
  the code does. Options: the environment and pycache on local disk (`/tmp` on cbe-login is a local xfs, 39 G free;
  roadmap 39 puts the environment in the checkout), or a service node or cgroup of its own from IT. A question for IT,
  not a stage here.
- **D4, two small guards (A1, A2)**: yes or no.

## 5. Stages

Each stage is one commit. The checkout is shared with other sessions: commit from a checked patch
(`git apply --cached`), never `git add <file>`.

| stage | what | effort | layout risk |
|---|---|---|---|
| S0 | measure: elements and build time per view | 1–2 h | none |
| S1 | Journey: the eight sections read in the collect's thread | ~1 day | none |
| S2 | Journey: the particles section the same | 1.5–2 days | none (behaviour: keep/drop, the pick viewer's refresh) |
| S3 | job pages: their reads in a thread | ~0.5 day | none |
| S4 | the array-task tracker as one element | ~0.5 day | low |
| S5 | the Journey's switcher as one element | ~0.5 day | low |
| S6 | the gallery's fill in worker processes | 0.5–1 day | none |
| S7 | gallery tiles as one element each (D2) | 1.5–2 days | moderate (behaviour) |
| A1, A2 | the guards (D4) | 1–2 h each | none |

Everything together is about 6–8 days.

### S0: measure

- Each view's element count in the watchdog's UI-load line (`ui_load`, `services/loop_watchdog.py:148`): the
  workspace's view containers (`ui/workspace_page.py`, `_refs` `journey_container`, `gallery_container`, the pipeline
  frame) and each kept job page (`_job_content_containers`, `ui/pipeline_builder/pipeline_builder_panel.py:74`),
  each counted by its descendants.
- One log line per build: "`<project> · <view> built in X s, N elements`" for the Journey's pane, a job page, the
  gallery's wall and the map.
- **Check:** a session on AGG412_STA (Journey, gallery, map, three job pages including an array job) and the lines it
  leaves. §2's element counts are replaced by measured ones.

### S1: the Journey's sections read before they draw

- A `read_pane(ts, state, project_path)` returns what the eight sections read from disk: job dirs, thumbnail URLs per
  section, `ensure_tilt_thumbnails`' answer, the import star's hand, the reconstruct section's star, JSON and picture
  URLs. It runs with the collect (`_collect` in `build_journey_panel`, `ui/tomo_dashboard_dialog.py:466`, already in a
  thread), for the selected tilt series only.
- Each `_render_*_section` takes its part of it instead of reading. The registry's DataFrames stay where they are
  (in memory).
- `select_ts`, `refresh_all` and `_redraw_soon` pass it to `render_main`; a bare `render_main()` keeps reading
  synchronously as a fallback until nothing calls it bare.
- **Check:** switching tilt series on AGG412_STA and Kilifish leaves no stall line in a section; before/after
  screenshots of every section are identical.

### S2: the particles section the same

- The reads of `render_particles_section` (§2.1) move into one function the collect calls for the selected tilt series
  (or into the pick viewer's own thread pass where a section loads lazily); the draw takes the result.
- The pick viewer's keep/drop and save path (`refresh_roster` → `_refresh_strip_soon`) and the per-list cutouts keep
  their own `Render`s.
- **Check:** as S1, plus keep/drop, save and discard on a pick list, the scatter, a species switch.

### S3: job pages read in a thread

- The reads in §2.2 move into the plugin's own async fill (a skeleton row until it lands, as `_render_lazy_section`
  does in `ui/pipeline_builder/job_tab_component.py:141`).
- **Check:** opening each of those job types leaves no stall line; screenshots identical once filled.

### S4: the array-task tracker as one element

- `_build_rows` emits the rows as one HTML string with the same classes and styles; a row's state is a data attribute
  the stylesheet colours, so `_paint_rows` sends one small update instead of one per row.
- One delegated script: a click on a row selects it (`select`), on the name or the id copies it (`_click_copy`).
- **Check:** an array job of 151 tilt series: the page's element count before and after (S0), switching to it, a
  click on a row, both copies, a status change while running.

### S5: the Journey's switcher as one element

- `_build_switcher` emits the table as HTML inside the existing `ui.menu`; the search filters rows in the browser; a
  click selects the tilt series through an event; the exclude marks stay.
- **Check:** the menu on AGG412_STA's tilt series and a project with three species; search, select, exclude.

### S6: the gallery's fill in worker processes

- `sources_by_ts`, the layer collection and `collect_tilt_groups` run through `run_in_worker` with a new `WorkKind`;
  their arguments become plain data (paths, the job models' fields they read), never the `ProjectState`
  (CLAUDE.md "Nothing heavy on the event loop").
- **Check:** the gallery's own "wall shown / filled" line on AGG412_STA before and after, and no stall line during
  the fill.

### S7: gallery tiles as one element each (after D2)

- `_render_tile` (`ui/tomo_gallery.py:1294`), `_render_picture` (`:1339`), the overlay (`:1381–1418`) and the mosaic
  toggle (`:1448`) emit one HTML string per tile; the dot layers are SVG strings already (`_render_dot_layers`,
  `:1499`). The zoom dialog stays components.
- **Check:** screenshots of the wall in each size, a source switch, layers on and off, a mosaic toggle, zoom.

### A1, A2: guards (D4)

- **A1.** A rule in `check_boundaries.py`: no `asyncio.create_subprocess_exec` / `_shell` outside
  `services/computing/processes.py`, so H33 cannot come back.
- **A2.** The watchdog times full garbage collections (`gc.callbacks`) and logs one over 200 ms. Some 1.1–1.5 s stalls
  on 2026-11-17/18 sat in weakref callbacks (`_weakrefset._remove`, `weakref.remove`), which is what a long
  collection looks like.

## 6. Not here

- The Journey's charts, parameter forms, the I/O config, dialogs: they stay components (§3).
- The map: roadmap 62.
- How often the server asks SLURM: roadmap 46.
- Where the server runs: D3, with IT.

## 7. Pickup

Read §1, §3 and H33. Restart the dev server first so H33 is live, use it for a day, and read the stall lines it
leaves: they may change §2's order. Then S0, then the stages in D1's order, one commit each.
