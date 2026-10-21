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
`PipelineRunnerService.reconcile_afterok(project_path)` for each. That
call is the canonical reconciler — it reads each submitted job's on-disk
`RELION_JOB_EXIT_{SUCCESS,FAILURE}` markers, then `squeue` and `sacct`,
writes the result back into in-memory `job_model.execution_status`,
persists `project_params.json`, cancels the dependents a failure left
waiting forever, and clears `pipeline_active` once no job is queued or
running.

Jobs are submitted by `PipelineOrchestratorService._submit_chain`: one
`sbatch` per job, with `--dependency=afterok:` on the SLURM ids of the
jobs it reads from. The chain runs in SLURM whether or not the server
does.

**Every UI surface reads from in-memory state.** No UI surface
reconciles. Specifically:

| Surface | Reads | Refresh mechanism |
|---|---|---|
| Workspace pipeline indicator | `state.pipeline_active`, `job_model.execution_status` | `StatusPoller.check_and_update_statuses` (per-tab UI tick, 3 s; no reconcile) |
| Landing-page project list (`ProjectsOverview`) | `_derive_live_status` of `project_params.json` | 15 s timer; reads what the monitor already wrote |
| Project-hub dialog (in-workspace switcher) | same as landing | same |
| Status dots in the roster (`BoundStatusDot`) | `job_model.execution_status` (NiceGUI binding) | NiceGUI client-side polling on the binding |

`StatusPoller` is **not** a reconciler. It is a per-tab UI ticker that:

1. Calls `roster.refresh()` so dots / counters re-render from the
   monitor-updated in-memory state.
2. Watches `state.pipeline_active` transitions to flip the per-tab
   `ui_mgr.is_running` flag and notify the user when the pipeline
   finishes (or another tab finishes it).

If you find yourself wanting to call `reconcile_afterok` from the UI: don't.
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

The monitor moves reconciliation to the server: it runs as long as the
server runs, regardless of whether any browser tabs are open, and
handles startup recovery (see below).

## Startup recovery

When `PipelineMonitor.start()` runs in the FastAPI `startup` event hook
(`main.py`), it first walks the configured project base
(`config_service.default_project_base`) looking for projects whose
persisted `project_params.json` has `pipeline_active: true`, and loads
each one into `_project_states`. That is all recovery takes: the chain
went on running in SLURM, every job's SLURM id is persisted, and the
next tick's `reconcile_afterok` picks up whatever finished while uvicorn
was down from the `RELION_JOB_EXIT_*` markers and `sacct`.

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
  Anything a restart must recover is persisted (`pipeline_active`, each
  job's `slurm_job_id`); in-memory state is a cache over it.
- **`__setattr__` on `AbstractJobParams` doesn't call `mark_dirty()`
  for `execution_status` writes.** Callers compensate with explicit
  `save_project(force=True)`. If you write `execution_status` from a
  new code path, either call `mark_dirty()` yourself or force-save.

## Where work runs

The server is one Python process, and one asyncio event loop answers every click of every
browser tab. Anything that holds the loop delays every client's next event by as long, so each
kind of work has one place it runs:

| Work | Where | How |
|---|---|---|
| In-memory reads, building and updating elements | the loop | directly; each callback short |
| Disk reads: stat, glob, star / JSON / manifest parsing, registry re-syncs | a thread | `await asyncio.to_thread(fn, ...)`; a render reads first, then builds (the Journey's `_collect`) |
| CPU-bound Python: slab renders, preview passes, cutout filters, volume registration, tilt thumbnails | a worker process | `await run_in_worker(WorkKind.X, fn, *args, progress_cb=...)` (`services/workers.py`) |
| The array jobs' task statuses (roster, pipeline table) | one scan per tick for the process | `services/array_status_cache.py`: the poller calls `refresh()` in a thread; readers call `get()` |
| Project saves | one thread of their own | `StateService.save_project` → `ProjectState.save` (`model_dump_json`) |

A thread does not protect the loop from Python-level CPU work: the thread holds the GIL and the
loop waits for it. That is what the worker processes are for. Worker work is a module-level
function with arguments and a result that pickle (paths, numbers, small dicts). It never gets a
`ProjectState`, a job model, an element or a callback; it reads what it needs from disk, and every
state change or UI update happens back in the server from its result. Each `WorkKind` has its own
cap, and workers run at a lower CPU priority than the server.

A view that is not on screen does no work. Its pollers are made with `ViewScope.timer`
(`ui/components/reactive.py`): a job page's from `ui_mgr.job_scope(instance_id)`, the roster's from
`ui_mgr.view_scope("pipeline")`; whole views pause through their `set_active` hook from the
workspace's `_apply_views`. The job panel keeps the shown job page and the two before it; older
ones are deleted and rebuilt on reopen.

What follows an edit (a summary, a warning, a section rebuild) runs once the typing pauses
(`Debounced`), while the field itself writes the model on every change. A rebuild never replaces a
field the user may be typing in: the SLURM section holds its rebuild while one of its fields has
focus.

`services/loop_watchdog.py` logs every stall of the loop over 1 s as one line naming the code it was
spent in, to the log file (`~/.crboost/logs/`). With `CRBOOST_TRACE_STALLS=1` it logs every stall over
250 ms, with the stack of the loop thread (and of any busy thread), on the console too. A new one names
the code to move.

## File map

- `services/scheduling_and_orchestration/pipeline_monitor.py` — the
  monitor.
- `services/scheduling_and_orchestration/pipeline_runner.py:reconcile_afterok`
  — the reconciliation primitive.
- `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:_submit_chain`
  — submission.
- `services/project_state.py:_project_states` — the path-keyed registry
  shared by all tabs in this server process.
- `ui/pipeline_builder/status_poller.py` — per-tab UI ticker (not a
  reconciler).
- `ui/projects_overview.py` — shared project list widget (landing +
  project-hub dialog).
- `main.py:setup_app` — FastAPI startup/shutdown hooks for the monitor.
- `services/workers.py` — the worker processes for CPU-bound work.
- `services/array_status_cache.py` — the array jobs' statuses, one scan per tick.
- `services/loop_watchdog.py` — the event-loop stall log.
- `ui/components/reactive.py` — `ViewScope`, `Debounced`, `owned_timer`, `FingerprintedView`.
