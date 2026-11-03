# Replacing `relion_schemer`: scheduler proposal

## Motivation

CryoBoost currently delegates pipeline node advancement to the
`relion_schemer` binary: we write a `Schemes/run_*/scheme.star`
describing the DAG of jobs, then launch `relion_schemer --run ...` as a
subprocess that walks the DAG, calls `sbatch` for each node, watches
for `RELION_JOB_EXIT_*` markers, and updates `scheme.star` /
`default_pipeline.star`. We persist `pipeline_active=True` while it
runs and clear it in `_monitor_schemer`'s `finally:` block on exit.

Problems with this arrangement:

1. **Restart-fragile.** The schemer is a direct child of uvicorn — it
   dies with the server. The currently-running SLURM job survives
   (slurmctld owns it) but the schemer never advances to the next
   node. The pipeline gets stuck and stays stuck until manual
   intervention. We've patched this with startup recovery in
   `PipelineMonitor._discover_and_recover`, but it's a workaround:
   restart-safety should be a property of the design, not a recovery
   step.
2. **Opaque failure semantics.** The schemer's behavior on
   per-node failure is largely "halt and wait for human"; our
   `stop_and_cleanup` mostly does the cleanup the schemer should
   have done. Multiple failure-handling paths live in the codebase
   precisely because the schemer's contract is unclear.
3. **No restart hooks.** The schemer can resume with `--current_node`,
   but we don't use it because we don't trust mid-scheme resume; we
   instead write a sub-scheme of the remaining nodes. Adds complexity.
4. **20-year-old binary baggage.** Container wrapping, env stripping,
   schemer log paths kept in in-memory dicts that die with the server,
   pipeline-control directory conventions — all of this is glue for a
   tool we don't actually want to depend on.

The only reason we use it is to keep on-disk artifacts
(`scheme.star`, `default_pipeline.star`, `Job/External/jobNNN/`)
RELION-compatible. We can keep emitting those without using the
schemer to interpret them.

## What the schemer does that we'd need to replace

Looking at `services/scheduling_and_orchestration/pipeline_runner.py`
and the orchestrator, the schemer's responsibilities are:

| Responsibility | Where in our code today |
|---|---|
| Walk DAG nodes in order | Implicit in `_write_scheme_star` edge list (`pipeline_orchestrator_service.py:328-348`); schemer reads scheme.star and walks. |
| Submit each node via `sbatch` | Schemer calls `sbatch <script>`. We already do this directly for retries (`pipeline_runner.py:708:_sbatch_script`) and for tilt-filter (`backend.py:131-156`). |
| Poll exit markers and advance | Schemer's main loop. Mirrored by `_monitor_retries_and_handoff` (`pipeline_runner.py:591-649`). |
| Update `scheme.star` (`rlnSchemeCurrentNodeName`, `rlnSchemeJobHasStarted`) | Schemer only. We don't touch these columns post-creation. |
| Update `default_pipeline.star` per-job rows | Schemer writes "Running"; we patch Running→Succeeded/Failed in `sync_all_jobs` after reading exit markers. |
| Halt-on-failure | Schemer halts; we additionally do `stop_and_cleanup`. |

## Proposed replacement: in-process node-advancement loop

A `PipelineScheduler` service replaces the schemer subprocess with an
async task per running pipeline. Living in
`services/scheduling_and_orchestration/pipeline_scheduler.py`, it would:

```python
class PipelineScheduler:
    async def run_pipeline(self, project_dir: Path, instance_ids: list[str]):
        # Set pipeline_active, persist.
        # For each node in order:
        #   - sbatch the supervisor script (we already write these)
        #   - poll RELION_JOB_EXIT_{SUCCESS,FAILURE} (~5 s)
        #   - on success: continue to next node
        #   - on failure: stop_and_cleanup and bail
        # After last node: clear pipeline_active.
```

This is essentially `_monitor_retries_and_handoff`
(`pipeline_runner.py:591-649`) generalized: it already does
sbatch-supervisor → poll-exit-marker → advance, just for the retry path.
The v1 implementation can be 80% copy-paste from there.

Two pieces of infrastructure already exist:

- **Script generation**: each job's supervisor script is already
  written (via `_write_job_star` → `generate_job_star`), and the
  script lives at `External/jobNNN/run_submit.script`. We have a
  RELION-flavored sbatch wrapper that the orchestrator emits.
- **Star file emission**: `StarfileService` + `_write_scheme_star`
  produce RELION-compatible scheme.star and pipeline.star files. We
  keep this. The scheduler just doesn't depend on RELION reading them.

### What the scheduler replaces

| Current site | Replacement |
|---|---|
| `pipeline_runner.py:405-507:_run_relion_schemer` | `PipelineScheduler.run_pipeline`. Drops `apptainer exec relion_schemer ...`, drops `_stdout_log_paths` / `_stderr_log_paths` / `_active_processes` for the schemer subprocess. |
| `pipeline_runner.py:728-797:_monitor_schemer` | Folded into the scheduler's main loop. |
| `pipeline_orchestrator_service.py:310-377:_write_scheme_star` | Still emits the file (for RELION compatibility) but the scheduler doesn't read it back; it uses the in-memory DAG that drove the emission. |
| `pipeline_runner.py:799-819:stop_pipeline` | Cancels the scheduler's asyncio task (it stops cleanly, doesn't need SIGTERM); the existing scancel of SLURM jobs in `stop_and_cleanup` is unchanged. |
| `services/scheduling_and_orchestration/pipeline_monitor.py:_discover_and_recover` | Most of it goes away: on startup we just re-launch a `PipelineScheduler.run_pipeline` task for each `pipeline_active=True` project, picking up at the node that hasn't been completed yet. No subprocess to reattach to. The in-memory DAG is rebuilt from the (still-emitted) scheme.star. |

### What stays unchanged

- **Per-job supervisor scripts** (`drivers/array_job_base.py` and friends).
  The scheduler still calls `sbatch run_submit.script`; the driver
  itself is unchanged.
- **Star file emission.** `_write_scheme_star`,
  `default_pipeline.star`, `job.star` generation, optimisation set
  files — all unchanged. The on-disk shape of a project remains
  byte-identical to a RELION-run project so users can open it in
  RELION's own GUI and inspect / re-run from there.
- **`sync_all_jobs` reconciliation primitive.** It already only reads
  state from disk markers — independent of who started the jobs.
  Stays as-is.
- **Retry path.** `_monitor_retries_and_handoff` becomes the prototype
  for the main scheduler loop; the retry path itself folds into the
  main path (a retry is just "first node has an existing job dir,
  re-use it").

## What this gives us

1. **Restart-safe by construction.** The scheduler is an asyncio task,
   not a subprocess. When uvicorn dies, the task dies; on the next
   start, we look at persisted state and start a new task that
   resumes from the current node. No need to track schemer PIDs or
   `--current_node` semantics.
2. **One status owner.** The scheduler IS the monitor — same loop
   writes status and drives advancement. Today the monitor calls
   `sync_all_jobs` (read-only); after this, it directly mutates state
   as it advances nodes.
3. **Cleaner failure semantics.** Halt-vs-continue policy lives in
   one place (the scheduler's per-node-result branch). Today it's
   spread across `sync_all_jobs`'s "STOP-ON-FAIL" block, the
   schemer's own halt behavior, and `stop_and_cleanup`.
4. **Drops a 20-year-old binary dependency.** Containerization of
   `relion_schemer` (the apptainer wrap step in `_run_relion_schemer`)
   goes away. We keep the `relion` container for `relion_import` and
   anything else that actually needs RELION.
5. **Future fork/conditional support is easy.** Today all scheme
   edges are `rlnSchemeEdgeIsFork=0`; future fork-on-condition
   semantics (e.g. "if ts_count > 100 then array-throttle to 32 else
   to 8") are a couple lines in the scheduler.

## Effort estimate

~1.5–2 weeks for one engineer, broken roughly into:

- **DAG model + scheduler skeleton** (`PipelineScheduler.run_pipeline`,
  in-memory node list, sbatch + exit-marker polling): ~3–4 days. The
  retry-monitor pattern is the template.
- **Lifecycle integration** (replace `_run_relion_schemer` call site
  in `deploy_and_run_scheme`; remove schemer-specific dicts from
  `PipelineRunnerService`; replace `_monitor_schemer`): ~2 days.
- **Restart recovery** (rebuild scheduler task from persisted state on
  server boot; the existing `PipelineMonitor` recovery code shrinks
  to "for each pipeline_active project, instantiate scheduler"): ~1
  day.
- **Failure-path consolidation** (one place owns halt-on-fail;
  `stop_and_cleanup` only does scancel + state reset): ~1 day.
- **RELION-side smoke-test** (open a CryoBoost-run project in RELION's
  GUI and confirm it reads the pipeline cleanly; spot-check a project
  resumed via the new scheduler can still be inspected with
  `relion_schemer --pipeline_control ...`): ~1 day.
- **Migration tail** (kill `_active_processes` schemer references,
  remove `apptainer exec relion_schemer` from container config,
  prune the schemer-stderr / sbatch-error special case from the
  monitor): ~1 day.

Total: 9–10 working days, with some slack.

## Risks

1. **RELION-side compatibility regression.** We've never observed a
   project that opened cleanly in RELION's GUI getting broken by our
   star-file writes, but we don't have a regression test. The smoke
   test above is mandatory before claiming we're RELION-compatible.
2. **Sbatch quirks.** We already sbatch supervisors directly (retry
   path, tilt-filter), so most of the env-stripping / cwd-handling
   gotchas are known. But the schemer historically did things like
   set up its own pipeline control dir before sbatch; if any of
   that's load-bearing for a specific driver's behavior, it'll
   surface during migration.
3. **Fork edges (`rlnSchemeEdgeIsFork=1`).** Currently unused; if a
   future use case lands while the scheduler is mid-build, we'd need
   to ship fork support before that use case. Today it's a non-issue.

## Out of scope

- **ccpem-pipeliner adoption.** It brings its own job ontology that
  doesn't map 1:1 onto our `JobType`/`AbstractJobParams` system; the
  mapping layer would be non-trivial. The homegrown scheduler reuses
  our existing param classes wholesale, which is cheaper.
- **Distributed scheduling.** v1 runs one scheduler task per project
  in the uvicorn event loop. If we ever shard pipeline execution
  across multiple processes/machines, the design needs revisiting,
  but that's not on the horizon.
