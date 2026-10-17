# Roadmap 34 — UI latency: nothing heavy on the event loop, heavy work in worker processes

**Status:** scoped 2026-10-17 from a read-only survey of main (timers and polls, background tasks, the job-tab
edit path). **Nothing built, nothing measured**: every cost below is read off the code, so S0 comes first and
re-ranks the rest.

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
- Text and number fields update the model on blur, or after a short debounce (decision 3), not per key.
- The `edit_refreshes` chain (SLURM signature, missAlign summary, subtomo warning) runs on that update only.
- A SLURM field's blur updates the values in place instead of rebuilding the section around the focused field.

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

## 3. Decisions owed
1. **Hidden job tabs:** unmount all but the active one and the last two (recommended; a reopened job is rebuilt,
   which loses scroll position and expanded sections), or keep every one mounted with its timers paused.
2. **Where CPU work runs:** a spawn pool owned by the server (recommended; no new service to run, and it dies with
   the server), a separate long-lived worker process fed by a queue (survives server restarts, one more thing to
   start at every site), or SLURM jobs (queue latency makes interactive renders feel worse).
3. **Fields:** model update on blur (recommended for numbers and paths; nothing changes until the user leaves the
   field) or a 300 ms debounce (updates while typing, still ~10× fewer events).
4. **Server-side preview passes:** only for tomograms the driver's manifest lacks (recommended), or none at all,
   leaving previews to the driver plus an explicit "Render previews" button.

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
1. With S0 in place, a normal session (several job tabs, the Journey, the wall, a running pipeline) logs no stall
   over 250 ms; any stall it does log names its code.
2. During a run, with five jobs opened and two left on Logs: a click into a Config field takes focus in under 100 ms.
3. Devtools, websocket frames while idle on one job tab: no traffic from hidden tabs.
4. Typing in a numeric field: no save until the field is left (or the debounce passes); the next click lands.
5. Load a template in the workbench: the response carries no gzip; the UI keeps responding while it streams.
6. A candidate-preview pass and a slab render running in workers: the roster and a Config tab stay responsive.
7. Two browser tabs on one project: the array scan runs once per tick, not twice (S0's minute line).

## 6. Not in scope
- Multi-user scaling (roadmap 23).
- Moving the drivers' own compute: they already run on SLURM.
- A different web framework.
