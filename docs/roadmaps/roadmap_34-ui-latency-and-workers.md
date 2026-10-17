# Roadmap 34 — UI latency: nothing heavy on the event loop, heavy work in worker processes

**Status:** scoped 2026-10-17 from a read-only survey of main (timers and polls, background tasks, the job-tab
edit path); **S0–S6 built the same day, NOT RUN** (ruff clean; no server run here). S7 not built. The decisions
are in §3, what was built and what is left in §7. §1 is the survey as it stood before the build.

## Before → After

**Before:** one Python process serves every browser tab and also does the heavy lifting. A click into a job's
config field can take ~2 s to take focus; switching views is slow. The asyncio loop that answers clicks also runs
the 3 s roster scans, every visited job tab's log, task and tilt-filter timers (hidden or not), the Journey's star
and manifest reads, and the gzip of whole volumes. The work it does hand to threads (preview passes, cutout
filters, pydantic dumps, indented JSON) is mostly Python-level, so it holds the GIL and the loop waits on it anyway.

**After:** the loop only routes events and does in-memory work. Disk reads go to threads. CPU work goes to a
small pool of worker processes. Hidden views do nothing. A stall watchdog names the code that blocked the loop
whenever it does. **Gain:** a click lands in under 100 ms during a full run with several jobs opened, and the
watchdog keeps it that way.

---

## 1. Facts (main, 2026-10-17, from reading)

### 1.1 Hidden job tabs keep working
- Every job opened stays mounted: `_ensure_job_rendered` caches its container, and `switch_tab` only hides the
  others (`ui/pipeline_builder/pipeline_builder_panel.py:194-223, 257-267`). Only `rebuild_pipeline_ui` clears them.
- **Logs:** a 3 s `ui.timer` per job left on its Logs sub-tab while the pipeline runs
  (`ui/pipeline_builder/logs_tab.py:30, 109`). `switch_tab` does not cancel it; only a sub-tab switch does
  (`job_tab_component.py`, `_handle_tab_switch`). Each tick `clear()`s and re-`push()`es up to 500 lines of stdout
  and stderr (`logs_tab.py:202-205`), changed or not. In NiceGUI 3 each line is a Label element, so that is
  ~1000 elements created and deleted every 3 s, per job.
- **Tasks:** `array_task_tracker` scans statuses synchronously every 5 s and re-sends icon, text and three styles
  for every TS row, changed or not; it never stops after the job ends (`ui/components/array_task_tracker.py:127-133,
  230-245`). 114 TS ≈ 500 element updates per tick.
- **Tilt filter page:** `owned_timer(3.0)`, `owned_timer(15.0)` and the DL run line's 3 s timer
  (`ui/tilt_filter_panel.py:178-180, 213`); `owned_timer` stops only when its owner is deleted, never when hidden
  (`ui/components/reactive.py:58-79`). The 15 s poll re-reads the registry on the loop and rebuilds every group.

### 1.2 Disk and parsing on the loop
- **Roster:** `StatusPoller` calls `roster.refresh()` every 3 s per browser tab, shown or not
  (`ui/pipeline_builder/status_poller.py:47, 59, 139`). `signature()` reads each array job's manifest, lists
  `.task_status/` and scans the job dir (`pipeline_roster.py:288-296`, `services/array_tasks.py:99-169`), more for
  expanded rows. `active_instance_id` is in the signature (`pipeline_roster.py:329`), so every job switch rebuilds the
  whole roster.
- **Journey:** `select_ts` / `refresh_all` run `collect_dashboard_journey`, `collect_species_journey`,
  `recon_mrc_map` and the Particles section's per-list `starfile.read` directly on the loop
  (`ui/tomo_dashboard_dialog.py:389-433`; `services/dashboard_data.py:704-790, 909-927, 930`;
  `ui/particles/pick_viewer.py:1289-1320, 1469-1490, 1537, 1868`). It runs on every TS click, and from the 4 s live
  timer whenever a background task starts or ends.
- **Registry:** `get_registry_for` re-syncs on the loop whenever the index's mtime moved — a stat per sidecar plus
  pydantic re-validation of the changed ones (`services/tilt_series/registry.py:346-390, 673-691`). Drivers touch the
  index all through a run.
- **Afterok reconcile** (3 s, one per process, while a pipeline runs): `exists()` ×2 per tracked job and a glob per
  running job on the loop (`services/scheduling_and_orchestration/pipeline_runner.py:103-166, 199`).
- **Render-time reads:** the subtomo tab parses the upstream opt-set and the whole particles.star when it is built
  (`ui/job_plugins/subtomo_extraction.py:178-212`); `load_icon_svg` reads its file on every call
  (`ui/components/svg_icon.py:26`); `current_project_state()` runs `Path.resolve()` (an lstat per path component on
  NFS) on every call, dozens per render (`services/project_state.py:1388`); `template_viewer._load_volume` reads a whole
  MRC (`ui/components/template_viewer.py:42-69`).

### 1.3 Work in threads that still stalls the loop
- **Saves:** `ProjectState.save` runs `model_dump` and then `json.dump(..., indent=2)` — the pure-Python encoder —
  in the default executor (`services/project_state.py:1086-1110, 1499-1511`). Both hold the GIL; `backend.py:269` puts
  a save at "hundreds of ms". Triggers: 1 s after each typing burst (`job_tab_component.py`, `create_save_handler`),
  at once on an I/O change (`ui/pipeline_builder/io_config_component.py:716-731`), every reconcile tick that changes a status
  (`pipeline_runner.py:252, 358`), each curation ingest, each tilt-filter label edit.
- **Preview passes:** candidate previews and list cutouts are pandas + per-pick Python loops + 8 skimage filter
  presets per tile, rewriting an indented manifest after every tomogram (`services/visualization/preview_orchestrator.py:
  211-460, 322`; `services/visualization/cutout_filters.py:191-286`; `services/visualization/recon_cutouts.py:116-137`). Every Journey or pick-viewer mount
  re-submits the "render what is missing" pass (`pick_viewer.py:3998-4060`), although the candidate-extract driver
  already writes these previews on the compute node (`drivers/extract_candidates_pytom.py:293-310`).
- **Tilt thumbnails:** `ImageProcessor` forks the multi-threaded server up to 16 times
  (`filterTilts/image_processor.py:151`, from `services/tilt_series_service.py:218-238`) and pickles every PIL image
  back to a parent that does not use them.
- One unbounded pool: `BackgroundTask` work is a coroutine on the loop that offloads with `asyncio.to_thread`
  (`services/background_tasks.py:95-157`). There is no cap or queue; saves share the default executor
  (min(32, cpu+4) threads) with long renders, so a save can wait behind them.

### 1.4 Bytes and bindings
- **Gzip on the loop:** NiceGUI's `GZipMiddleware` (level 9, `nicegui/ui_run_with.py:71`) compresses every response
  without a `Content-Encoding`. `/api/file` serves template and mask volumes as `application/octet-stream` with none
  set (`ui/template_workbench.py:112-122`), so tens to hundreds of MB are deflated on the loop per load. Image
  routes already opt out (`main.py`, `no_gzip`).
- **Per keystroke:** `ui.input` / `ui.number` send `update:model-value` per key. Each one runs the save debounce and
  every `edit_refreshes` entry: the SLURM signature (`ui/pipeline_builder/slurm_tab.py:90-98`), missAlign's `summary.refresh()` and
  `schedule()` (`ui/job_plugins/miss_align.py:30-45`), the subtomo box-size warning rebuild
  (`subtomo_extraction.py:68-70`). On blur, a changed SLURM field rebuilds the whole section
  (`slurm_tab.py:216-228`), which destroys the field the user clicked into next.
- **Bindings:** every `bind_value` to a model field and every status-dot `bind_content_from` is polled every 0.1 s
  (`nicegui/binding.py:62-74`) and none is removed for hidden tabs. Each `clear()` walks every binding in the process
  (`nicegui/client.py:361`, `binding.py:232-253`), so its cost grows with all mounted elements, across all tabs.

### 1.5 What multiplies it
Per process: the afterok reconcile (3 s) and the curation watcher (5 s). Per workspace browser tab: the roster scan
(3 s), the tray (1.5 s), species observe (3 s), protocol light (6 s), rail counts (15 s) and three tilt-filter row
timers (3 s). Per job tab ever opened in that browser tab: the log, task and tilt-filter timers. Two browser tabs on
one project repeat every scan; nothing is shared between them. A page navigated away from keeps its timers for
`reconnect_timeout=30` s (`main.py:240`). So the loop's disk and DOM work grows roughly as browser tabs × visited
job tabs × tilt series.

---

## 2. Stages

### S0 — A stall watchdog (measure first)
A daemon thread watches a loop heartbeat (a 50 ms `call_later` that stamps a time). When the stamp is more than
250 ms old, the thread logs the loop thread's stack from `sys._current_frames()` once per stall, at WARNING, with
the stall's length when it ends. A minute line logs the number of clients, mounted elements, active bindings and
live timers. This ranks §1 by what actually happens on a real session, and stays on as the regression guard.

### S1 — Hidden views do nothing
- A job container that is hidden pauses its timers (logs, tasks, tilt filter) and resumes them when shown: one
  `set_active(on)` per job tab, called from `switch_tab` the way the workspace calls the views' hooks.
- Logs append only the new tail (an offset per stream) and skip an unchanged tick, instead of clear + push.
- The task tracker scans in a thread, updates only the rows whose status changed, and stops when the job has ended.
- Job tabs beyond the active one and the last two are unmounted (decision 1), so DOM and bindings stop growing
  with every job opened.

### S2 — Disk off the loop, and shared
- One per-process status cache per project: a single `to_thread` scan of the array jobs per tick, read by every
  roster, tracker and table in every browser tab. The roster's signature reads the cache only.
- The roster highlights the active job by toggling a class; a job switch no longer rebuilds it.
- The Journey's `collect_*`, `recon_mrc_map` and Particles-section reads run in one `to_thread` call, then render.
- The registry re-sync, the afterok reconcile's stats and globs, and the subtomo tab's star parse go to threads.
- `load_icon_svg` is cached; `current_project_state()` caches the resolved path per project.

### S3 — Cheap saves
- `model_dump_json(indent=2)` replaces `model_dump` + `json.dump` (pydantic-core writes the JSON in Rust; the file
  stays indented and identical in content).
- Saves get their own single-thread executor, so they never queue behind renders and never overlap.
- The afterok reconcile coalesces its forced saves to one per tick.

### S4 — No compression of what doesn't compress
`/api/file` sets `Content-Encoding: identity` like the image routes; every binary route does the same.

### S5 — No work per keystroke
- Fields keep updating the model on every change (decision 3); what follows an edit waits for the typing to pause.
- The `edit_refreshes` chain (SLURM signature, missAlign summary, subtomo warning) runs 0.3 s after the last keystroke.
- The SLURM section holds its rebuild while one of its fields has focus and rebuilds 0.8 s after the last blur, so it
  never replaces the field the user just clicked into.

### S6 — Worker processes for CPU work
- `services/workers.py`: a `ProcessPoolExecutor` with the `spawn` start method (not fork: the server is
  multi-threaded), 4 workers by default and configurable, and a cap per kind of task (renders, previews,
  analytics). One awaitable `run_in_worker(fn, *args)` is what `BackgroundTask` call sites use instead of
  `asyncio.to_thread` for CPU work.
- Inputs and outputs must pickle. The slab renders, matched-slab registration, recon cutouts and IMOD `.mod`
  generation already take paths and return small results. Candidate previews and the analytics `_compute(state, …)`
  functions are reworked to take `project_path` + `instance_id` and load what they need in the worker, as the
  driver does; progress comes back through a small progress file or a queue.
- Everything that changes `ProjectState` or touches the UI stays in the server: `on_complete`, the state writes at
  the end of a thumbnail pass, the module-level dedup sets.
- The tilt thumbnail pass runs in a worker, with `ImageProcessor`'s fork pool replaced (no forks of the server, no
  images pickled back).
- The server stops re-rendering previews the driver already wrote: a mount re-submits the "missing" pass only for
  tomograms whose manifest entry is absent or stale (decision 4).

### S7 — Fewer polled bindings
Job-model bindings and status dots are updated when the model changes (the status poller already knows when it
does) instead of NiceGUI's 0.1 s polling of every active link in the process.

## 3. Decisions (2026-10-17)
The four questions as they were put, and what was decided.

1. **Hidden job pages: kept or deleted.** What "unload" meant: every job page opened in a browser tab stays
   in that tab's page (hidden) after switching to another job, with every field, polled binding and timer it
   made, on the server and in the browser. Each hidden page slowed every later page operation (a `clear()`
   walks every binding of the process) and its timers kept working. **Built:** the panel keeps the shown page
   and the two shown before it, so switching between recent jobs is instant; older pages are deleted and
   rebuilt when reopened, which costs what a first open costs and loses their scroll position. All hidden
   pages' timers are paused either way. Taken as the default when the build started (the question had come
   back as "what do you mean"); `_KEPT_JOB_PAGES` in `ui/pipeline_builder/pipeline_builder_panel.py` is the one
   number to change if three is wrong.
2. **Where CPU work runs: a process pool owned by the server** (the user's choice). `services/workers.py`:
   spawned processes (the server is multi-threaded; a fork would copy its locks mid-use), 4 by default
   (`CRBOOST_WORKERS`), started on first use, stopped with the server, at a lower CPU priority. A cap per kind
   of work (renders 3, preview passes 1, thumbnail passes 1), so one burst never takes every worker. A worker
   that dies fails its task and the next task gets a fresh pool. Not a separate service: nothing new to start
   or configure at a site. Not SLURM: queue latency on interactive renders.
3. **Fields: update on change** (the user's choice: "think about user experience"). A field writes the model
   on every keystroke, as before. What is derived from it (missAlign's summary and schedule, the SLURM section,
   the subtomo box-size warning) repaints once typing pauses for 0.3 s (`Debounced`); the save stays debounced
   at 1 s. The SLURM section no longer rebuilds while one of its fields has focus, so a click into the next
   field is never swallowed by a rebuild of the section.
4. **Which previews.** The candidate previews: for each template-matching candidate-extract job, the
   per-tomogram images of its picks (the pick overlay, the cutout gallery with its eight display filters, the
   score histograms) that the Journey's Particles section and the pick viewer show, listed in the job's
   `vis/preview/manifest.json`. The candidate-extract driver renders them on the compute node. On every mount
   of the Journey or the pick viewer the server also started a "render missing" pass per job, which read the
   whole candidates.star and every subtomo particles.star only to find each tomogram cached. **Built:** that
   automatic pass now starts only when the manifest was not written by a complete pass or is older than
   candidates.star, tomograms.star or a subtomo particles.star (`previews_up_to_date`, read in a thread); the
   "Render missing" and "Re-render all" buttons always run. A manifest now records whether its pass reached
   every tomogram (`complete`), so an interrupted pass is still picked up. Both passes run in a worker.

## 4. Order and dependencies
- S0 first: an afternoon, and it re-ranks everything below with real stacks.
- S1, S3 and S4 next: small, independent, and they remove the largest suspected stalls (hidden log timers,
  GIL-held saves, gzip of volumes).
- S2 and S5 next.
- S6 is the structural stage; decision 2 first. It removes the GIL contention §1.3 describes, which S1–S5 cannot.
- S7 last, if S0 still shows binding passes after S1 unmounts hidden tabs.
- Per-user servers (roadmap 23 stage 2) give each user their own process: they split load between users but do
  nothing for one user's own tabs; this roadmap is what fixes that.

## 5. Runtime checklist
1. With S0 in place and `CRBOOST_TRACE_STALLS=1`, a normal session (several job tabs, the Journey, the wall, a running pipeline) logs no stall
   over 250 ms; any stall it does log names its code.
2. During a run, with five jobs opened and two left on Logs: a click into a Config field takes focus in under 100 ms.
3. Devtools, websocket frames while idle on one job tab: no traffic from hidden tabs.
4. Typing in a numeric field: no summary or section rebuild per keystroke; the next click lands.
5. Load a template in the workbench: the response carries no gzip; the UI keeps responding while it streams.
6. A candidate-preview pass and a slab render running in workers: the roster and a Config tab stay responsive.
7. Two browser tabs on one project: the array scan runs once per tick, not twice (S0's minute line).

## 6. Not in scope
- Multi-user scaling (roadmap 23).
- Moving the drivers' own compute: they already run on SLURM.
- A different web framework.

## 7. Stage log

**2026-10-17 — S0–S6 built, NOT RUN.** Verified by ruff and reading only; nothing was started here.

- **S0** `services/loop_watchdog.py`, started at app startup (`main.py`): a 50 ms heartbeat on the loop and a
  daemon thread that logs, at WARNING, the loop thread's stack (and any busy thread's) once the loop has not
  ticked for 250 ms, a second sample past 1 s, then the stall's length with the UI load (clients, mounted
  elements, polled bindings).
- **S1** `ViewScope` (`ui/components/reactive.py`), per browser tab through `UIStateManager.view_scope` /
  `job_scope`. The job panel runs only the shown job page's scope, and only while the pipeline view shows
  (`PipelineBuilderPanel._show_job`, `_sync_job_scopes`, `set_view_active`, called from the workspace's
  `_apply_views`); it keeps 3 job pages (§3.1). On those scopes: the Logs tab's poll, the task tracker's,
  the tilt-filter page's three pollers and its review block; on the pipeline scope the roster's tilt-filter
  status and controls. The status poller skips the roster while the view is hidden and the roster catches up
  on return. Logs are a `LiveLog` (`ui/components/log_pane.py`): one text element, written only when the text
  changed, pinned to the end unless scrolled up. The task tracker scans in a thread, updates only the rows
  whose status moved, stops once every task settled and the job ended, and reads a task's log only when its
  row is opened (it read every task's two log files at render).
- **S2** `services/array_status_cache.py`: the roster's signature and render and the pipeline table read
  array statuses from it; the status poller refreshes it in a thread, one scan per project per tick for the
  whole process. A job switch repaints the old and new active rows in place (`RosterWidget.paint_active`)
  instead of rebuilding the roster. The Journey collects in a thread (`_collect`; first paint, TS select,
  refresh, exclude toggle). The afterok reconcile reads its exit sentinels and the started-task glob in a
  thread. The subtomo tab's upstream banner reads its star and sentinel in a thread after the tab is built.
  Icon files are read once per process; `get_project_state_for` resolves each project path once.
- **S3** `ProjectState.save` writes with `model_dump_json(indent=2, fallback=str)` (same content, still
  indented); saves run on their own single thread.
- **S4** `services/http_responses.py` (`NO_GZIP`, `json_headers`): `/api/file` volumes and every image route
  pass through uncompressed; a JSON file over 1 MB too.
- **S5** `Debounced` (`reactive.py`, carries the caller's slot into its task); the job tab's edit refreshes
  and the subtomo warning use it. The SLURM section holds its rebuild while a field has focus and rebuilds
  0.8 s after the last blur.
- **S6** `services/workers.py` (`run_in_worker`, `WorkKind`, progress through a small file read once a second).
  In workers now: the pick viewer's recon slabs (`render_recon_slabs`, moved to `preview_render.py`) and list
  cutouts, the wall's and the Journey's denoised slabs, matched-plane registration, the candidate preview
  passes (`generate_candidate_previews_for_job`, which loads the state from disk) and the tilt thumbnail pass.
  The automatic preview pass is gated (§3.4). `main.py` runs only as `__main__` (a spawned worker imports it
  as `__mp_main__`).

**2026-10-17, first run (port 8012).** The watchdog worked at once: a 506 ms stall at startup, the pipeline
monitor's recovery parsing every project_params.json under the project base on the loop. That scan and the
recovered projects' loads now run in a thread (`_projects_with_active_pipeline`, `preload_project_state`). The
stall was reported against "Busy thread MainThread": the loop thread is now recorded when the watchdog starts,
not at its first heartbeat, which a blocking startup delays.

The second stall (468 ms, first workspace load) was the roster importing `ui.aggregation.aggregate_dialog`
inside its render, and the import compiling the module from source: `main.py` set
`sys.dont_write_bytecode`, so nothing imported after that line was ever cached, and every lazily imported
module compiled on the loop the first time a click reached it, after every restart (the in-tree
`__pycache__` dirs date from 2026-08-16). `pycache_setup.py`, imported first by `main.py`, now sends bytecode
to `~/.cache/crboost/pycache` (the source tree stays clean) and overrides a `-B`.

**2026-10-17, second and third runs (port 8033).** Six stalls, four causes; fixed by reading, NOT RUN:
- **2.8 s, then 1.4 s on the next restart: the first Journey or Tomograms open imported scipy.signal on
  the loop** (`tomo_gallery` / `tomo_dashboard_dialog` → `recon_compare` → `recon_register`, whose
  module-level `from scipy.signal.windows import tukey` pulls scipy.optimize, scipy.stats (about a hundred
  distributions, each formatting its docstring at import), scipy.interpolate and numpy.testing, which runs
  `lscpu` in a subprocess at import). The 2.8 s run also compiled all of that from source: `sys.pycache_prefix`
  makes Python ignore every `__pycache__` beside the sources, the cluster's pre-compiled site-packages
  included, so each third-party module compiles once per user into the cache on its first import (the pyc
  timestamps under `~/.cache/crboost/pycache` date each module's first import). Fixed: `tukey` is imported
  inside `_prepared`, which runs only in the worker; the server never loads scipy.signal.
- **255–303 ms, first workspace build: the roster rendered twice.** `render()` seeded `_expanded_instances`
  for array jobs, the signature includes that dict, so `rebuild_pipeline_ui`'s `roster.refresh()` right
  after `build()`'s found a new signature and repainted. Fixed: `signature()` seeds the defaults. The
  303 ms run also compiled the job plugins and the aggregation modules on first import (one-time).
- **521 ms, Tomograms size switch: the whole wall re-rendered** (5437 elements). Fixed: the switch sets
  the grid's column width and, across the smallest size, the captions in place.
- **834 ms, the Journey's first paint** (`refresh_all` → `render_main`, sampled in a stat under
  `geometry_for_ts` in the Particles section; 3856 elements after it). Not fixed: the main pane's sections
  still read while they render (the item below), and the strip plus pane is ~3000 elements. The watchdog's
  end line now says where a stall went: each 50 ms check while the loop is late records the function of
  the server's own code it is in, and the end line names the most frequent ones. That line decides
  between moving the reads and shrinking the DOM. The loop stack it prints also drops the import
  machinery's frames, so an import stall shows the line that imported.
- The "Busy thread" in the first report was the Projects-overview disk sizer walking AGG412
  (`project_disk_usage.measure`). It releases the GIL at every `scandir` step and `lstat`, so it was
  running, not blocking.

**2026-10-17, quiet by default.** In a normal session the watchdog printed more than anyone reads: every
stall over 250 ms came with the stacks of the loop and of every busy thread. It now logs only the end line,
and only for a stall over 1 s, one a user feels; `CRBOOST_TRACE_STALLS=1` brings back the 250 ms threshold
and the stacks, for capturing a stall to fix (§8.1).

**Left for later, by the same rules:**
- The species tabs' analytics, overview and picks computes still run in threads and take the live
  `ProjectState`; moving them to workers means passing paths and loading the state in the worker.
- The Journey's main pane still reads its Particles section (tomograms.star, manifests, pick lists) while it
  renders; its collect moved, its render did not.
- `get_registry_for` still re-syncs on the loop for callers outside a thread (the tilt-filter page's pollers,
  now paused when hidden).
- `template_viewer._load_volume` reads a whole MRC on the loop.
- S7 (status dots and job-model bindings pushed instead of polled every 0.1 s): only if S0's log still shows
  binding passes now that hidden pages are deleted.

## 8. Open: switching jobs and job sub-tabs still takes 1–2 s (reported 2026-10-17, after S0–S6)

A click on a job in the roster, or on Config / Logs / Files / Tasks in a job's header, still takes a second
or two to show. Not yet looked into. Start here:

1. **Capture first.** Restart with `CRBOOST_TRACE_STALLS=1` (and `pycache_setup.py`, so the first-use compiles are gone after one pass),
   click through a few jobs and sub-tabs twice each, and collect the watchdog lines. A 1–2 s switch is
   well over its 250 ms threshold, so each one should name its code. The second pass separates one-time
   import/compile cost from the real per-switch cost. If no line appears while the click still lags, the
   time is in the browser (DOM size, Quasar components), not the server: devtools Performance on one click.
2. **Suspects from reading:**
   - A job switch to a page that is not among the three kept (`_KEPT_JOB_PAGES`) renders the whole job page:
     Parameters, I/O and SLURM sections of Config at once, 150–300 elements, many of them Quasar inputs, and
     the I/O resolver's `exists()` stats on the shared filesystem (`services/path_resolution_service.py`),
     template header reads (`ui/job_plugins/template_match.py`), all on the loop.
   - A sub-tab switch (`_handle_tab_switch`, `ui/pipeline_builder/job_tab_component.py`) clears the tab
     content and renders the new tab from scratch: back to Config means the whole form again; Files does an
     `iterdir` and a `stat` per entry on the loop (`ui/pipeline_builder/files_tab.py`).
   - Every `clear()` walks every binding in the process (`nicegui/binding.py` `remove`): its cost grows with
     what all tabs have mounted (the watchdog's end line gives the element and binding counts).
   - `roster.refresh()` after a switch is signature-gated, but the signature still walks every job and the
     species and tilt-filter fingerprints on each call.
3. **Likely shape of the fix, by the §2 rules:** sub-tabs built once per job page and switched by visibility
   (like the workspace views), so a switch is a class flip; render-time disk reads (I/O resolution, template
   headers, Files listing) read first in a thread, then build; if the browser side dominates, fewer Quasar
   inputs per form row.
