---
name: reference_supervisor_timeout_arrays_survive
description: "On CBE a supervisor killed by its SLURM TIME LIMIT does NOT scancel its array (handler never runs); tasks finish, aggregation is missing; an in-place Run aggregates without recompute"
metadata:
  type: reference
---

Observed 2026-10-05/06 on six AGG412 TM supervisors (`--time=8:00:00`): `run.err` has
`slurmstepd: ... CANCELLED AT ... DUE TO TIME LIMIT`, `run.out` has NO `[SUPERVISOR] Caught signal` line, and
every array task ran on for hours and wrote `.ok` — so `install_cancel_handler` (drivers/array_job_base.py) did
not run on a time-limit kill. Result: all per-item work on disk, but no `aggregate()` output, no exit marker → job
Failed, afterok dependents never start.

**How to apply:** "N/N tasks ok but job Failed" → check `run.err` for TIME LIMIT. Fix = press Run: `_submit_chain`
re-submits a FAILED job into its own dir keeping `.task_status/*.ok`; `submit_array_job` finds nothing to run, the
supervisor aggregates in minutes. Don't edit the job's params first. Prevention ideas (supervisor QOS / stop
before the limit) are roadmap 29 C1; keep arrays alive on supervisor time-out. Related: [[project_agg412_reprocessing_2026-10-04]],
[[reference_cbe_slurm_qos]], [[reference_run_command_watchdog]].

**2026-10-30 (seen again on Tir_Env job008 segment, 63 TS, %20 throttle: supervisor 14:29→22:29, last task 23:03):**
fix built on STA_Kitchen, NOT RUN — `supervisor_slurm.qos` (SupervisorSlurmConfig.qos → `#SBATCH --qos` on array
supervisors via `_render_supervisor_script`); dev conf.yaml = `qos: g_medium`, `time: 2-00:00:00`. Lab conf.yaml
still `time: 4:00:00`, no qos — needs the same two lines.
