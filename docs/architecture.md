# Architecture notes

Brief notes on cross-cutting patterns the codebase relies on. Read this
before touching anything in `services/scheduling_and_orchestration/`,
`ui/pipeline_builder/`, or anything that consumes "is this pipeline
running" state.

## Single-observer pattern: pipeline status

There is exactly **one** server-side observer of pipeline status:
`services/scheduling_and_orchestration/pipeline_monitor.py:PipelineMonitor`.
It runs one async tick (3 s) in the FastAPI event loop, iterates over
every project in `services/project_state.py:_project_states` whose
`pipeline_active` flag is True, and calls
`PipelineRunnerService.sync_all_jobs(project_path)` for each. That call
is the canonical reconciler — it patches `default_pipeline.star`
`Running` rows against on-disk `RELION_JOB_EXIT_{SUCCESS,FAILURE}`
markers, writes the result back into in-memory
`job_model.execution_status`, persists `project_params.json`, and on any
failure calls `stop_and_cleanup` (which clears `pipeline_active`).

**Every UI surface reads from in-memory state.** No UI surface
reconciles. Specifically:

| Surface | Reads | Refresh mechanism |
|---|---|---|
| Workspace pipeline indicator | `state.pipeline_active`, `job_model.execution_status` | `StatusPoller.check_and_update_statuses` (per-tab UI tick, 3 s; no `sync_all_jobs`) |
| Landing-page project list (`ProjectsOverview`) | `_derive_live_status` of `project_params.json` | 15 s timer; reads what the monitor already wrote |
| Project-hub dialog (in-workspace switcher) | same as landing | same |
| Status dots in the roster (`BoundStatusDot`) | `job_model.execution_status` (NiceGUI binding) | NiceGUI client-side polling on the binding |

`StatusPoller` is **not** a reconciler. It is a per-tab UI ticker that:

1. Calls `roster.refresh()` so dots / counters re-render from the
   monitor-updated in-memory state.
2. Watches `state.pipeline_active` transitions to flip the per-tab
   `ui_mgr.is_running` flag and notify the user when the pipeline
   finishes (or another tab finishes it).

If you find yourself wanting to call `sync_all_jobs` from the UI: don't.
Either let the monitor's next tick pick it up, or add the logic to the
monitor.

## Why the monitor exists

Before the monitor, reconciliation lived in the per-tab `StatusPoller`'s
3 s timer. That meant:

- Closing the workspace tab stopped reconciliation. A job that failed
  while the user was on a different page would leave `pipeline_active`
  stuck True forever.
- The landing-page roster never reconciled at all — it read raw
  `project_params.json` and was always stale-by-design.
- The schemer subprocess's own `_monitor_schemer` finally-block could
  clear `pipeline_active`, but only on normal exit; a server restart
  (uvicorn dies → schemer dies as a child) skipped that path entirely.

The monitor moves reconciliation to the server: it runs as long as the
server runs, regardless of whether any browser tabs are open, and
handles startup recovery (see below).

## Startup recovery

When `PipelineMonitor.start()` runs in the FastAPI `startup` event hook
(`main.py`), it first walks the configured project base
(`config_service.default_project_base`) looking for projects whose
persisted `project_params.json` has `pipeline_active: true`. For each:

1. Load the project state into `_project_states`.
2. Call `sync_all_jobs` to patch up any `Running` rows whose jobs
   actually finished while uvicorn was down (`RELION_JOB_EXIT_*` markers
   on disk).
3. If any job is still `RUNNING` (SLURM job still owns it), defer
   re-deploy until that finishes — track the project in
   `_recovered_paths` so the next tick re-checks.
4. If only `SCHEDULED` jobs remain (the schemer died before advancing
   to them), call `deploy_and_run_scheme` with those instance_ids.
   The orchestrator reuses existing `External/jobNNN` dirs, so
   `.task_status/*.ok` markers from the prior run let per-TS retries
   skip already-succeeded tasks.

`_recovered_paths` is the "deferred" set; the tick loop checks it
periodically and resumes re-deploy once every previously-RUNNING job
has settled.

## NiceGUI / async patterns to keep using (and to avoid)

- **Use NiceGUI bindings (`bind_content_from`) for per-job UI elements
  that mirror model state** (e.g. status dots). This is the cheapest
  way to keep many UI surfaces in sync; the binding's internal poll is
  free.
- **One observer per concern.** If two pieces of UI need the same data,
  don't have each one fetch it — read from a single in-memory source
  the observer keeps fresh. This applies beyond pipeline status:
  candidate-preview manifests, template/mask registries, etc.
- **Don't run reconciliation from per-tab timers.** Per-tab timers
  vanish when the tab closes; reconciliation must outlive any one tab.
- **Cancel timers on dismissal paths, not just `hide`.** Dialogs in
  NiceGUI fire `before-hide` and `hide` on dismiss; navigating away can
  skip both. `ProjectsOverview` listens to both events and also
  self-cancels on `RuntimeError` (client gone) in its refresh path.
- **Don't hold module-level in-memory registries you can't rebuild.**
  `PipelineRunnerService._active_processes` (subprocess handles) is
  fundamentally unrecoverable across restart, so anything that depends
  on it must have a separate persisted source of truth (the
  `pipeline_active` flag, in our case).
- **`__setattr__` on `AbstractJobParams` doesn't call `mark_dirty()`
  for `execution_status` writes.** Callers compensate with explicit
  `save_project(force=True)`. If you write `execution_status` from a
  new code path, either call `mark_dirty()` yourself or force-save.

## File map

- `services/scheduling_and_orchestration/pipeline_monitor.py` — the
  monitor.
- `services/scheduling_and_orchestration/pipeline_runner.py:sync_all_jobs`
  — the reconciliation primitive.
- `services/project_state.py:_project_states` — the path-keyed registry
  shared by all tabs in this server process.
- `ui/pipeline_builder/status_poller.py` — per-tab UI ticker (not a
  reconciler).
- `ui/projects_overview.py` — shared project list widget (landing +
  project-hub dialog).
- `main.py:setup_app` — FastAPI startup/shutdown hooks for the monitor.

## Replacing `relion_schemer`

See `docs/scheduler-proposal.md`. Short version: the monitor is the
foundation of a homegrown scheduler. Once we replace
`relion_schemer`-driven node advancement with an in-process loop, the
"restart recovery" branch goes away — restart-safety becomes a property
of the design rather than a patched-on recovery step.
