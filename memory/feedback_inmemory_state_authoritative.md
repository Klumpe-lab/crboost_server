---
name: feedback-inmemory-state-authoritative
description: "In-memory ProjectState is the source of truth while a pipeline is running. Disk re-reads must be load-if-absent only, never unconditional replace."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: eaa44e73-7945-4b79-b61b-612db2a60a2e
---

ProjectState lives in `_project_states` (path-keyed registry, `services/project_state.py:728`). While a schemer is running, `sync_all_jobs` ticks every 3 s and mutates `state.jobs[i].execution_status` / `relion_job_name` / `job_path_mapping` in memory. Saves do happen, but `state.save()` does NOT call `update_modified()` — so `modified_at` is NOT a reliable signal of "was this written recently."

**Rule:** any "load this project" code path that runs inside the server process must use `get_project_state_for(path)` (load-if-absent), never `ProjectState.load(...)` directly followed by `set_project_state_for(...)`. The latter unconditionally replaces the registered instance with a fresh disk read and clobbers any in-flight updates from sync ticks.

**Why:** 2026-05-13 forensic dive on `agg_20260205_412_FCIso_unmilled2`: templatematching/tmextractcand stayed yellow despite RELION_JOB_EXIT_SUCCESS markers because `StateService.load_project` at `project_state.py:842` was doing the unconditional replace. UI flows that re-call `load_existing_project` (landing page, project switcher dialog, browser refresh) clobbered live sync updates. The on-disk forensic fingerprint of this race: `job_path_mapping` entries WITHOUT trailing slash (orchestrator-deploy format, `pipeline_orchestrator_service.py:147`), `relion_job_name=null`, `execution_status=Scheduled`. Sync writes trailing slashes; deploy doesn't.

**Second cause of same fingerprint (2026-05-15, project `GT`):** `run_generated_scheme` was setting `state.pipeline_active=True` and saving BEFORE the long `await asyncio.create_subprocess_shell(...)` that spawns the schemer. The PipelineMonitor's 3-s tick fires during that apptainer-cold-start window, calls `sync_all_jobs`, sees `pipeline_active=True && is_active(project)=False` (because `_active_processes[resolved]` hadn't been written yet), and self-heals pipeline_active back to False at `pipeline_runner.py:161`. The schemer then runs unwatched — SLURM sub-tasks progress, but the monitor skips the project on subsequent ticks (`pipeline_active=False`), so `state.jobs[*].execution_status` is never reconciled from disk. Same race in `launch_retries` between setting `pipeline_active=True` and registering `_retry_monitors[resolved]`. **Fix:** register the resource in `_active_processes` / `_retry_monitors` BEFORE setting `pipeline_active=True` — both ordering bugs corrected in `pipeline_runner.py:run_generated_scheme` and `launch_retries`. **Recovery for an already-broken project:** set `pipeline_active=True` in `project_params.json`, restart the server; `PipelineMonitor._discover_and_recover` will find it, sync from disk, and re-mark active for any still-RUNNING jobs.

**How to apply:** when adding any new "load project" entry point, route it through `get_project_state_for` or `StateService.state_for(path)`. Don't construct fresh `ProjectState` instances mid-session except for genuine new-project creation (`set_project_state_for` is appropriate there). The one legitimate `ProjectState.load(...)` callsite outside the registry is `drivers/driver_base.py:38` — that's fine because SLURM drivers run in separate processes and have no shared memory with the server.

Related: [[reference-hpc-env]] for python env notes when running diagnostic scripts.
