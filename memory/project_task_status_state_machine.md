---
name: project_task_status_state_machine
description: "Per-task .ok/.fail/.skip markers encode state as file PRESENCE, so items can hold two at once; containment landed 2026-08-11, JSON-record migration designed not built"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5d9bc108-a26e-4583-a84b-7d29b152fae3
  modified: 2026-08-11T08:52:31.663Z
---

`.task_status/` markers encode state as **file presence, not a value**, so "exactly one status per
item" is a convention 5 writers must each honour — not an invariant. An item can hold `.ok` AND
`.fail` simultaneously.

**Root cause of the persistent red `(!)` badge** (diagnosed 2026-08-11 on
`/groups/klumpe/crboost_data/deadcode_test/External/job002`): re-running a supervisor did **not**
scancel the previous array, so an orphan task from a superseded submission kept running, got
watchdog-killed, and wrote `.fail` over an already-`.ok` item *5 minutes after the job had already
succeeded*. `_get_array_progress` counted marker **files** (6 ok + 1 fail) instead of resolving one
status per item → `6/6` green and `1!` red at once, unclearable by rerun.

**Landed 2026-08-11, PENDING RUNTIME** — `write_status_atomic` clears `.fail`/`.skip` on success (but
never clears `.ok` on failure — that combination means the writer is an orphan);
`is_superseded_task()` rejects writes from a **strictly older** `SLURM_ARRAY_JOB_ID` than the
manifest's (strictly-older only, because `submit_array_job` writes `array_job_id` back *after*
sbatch, so a current task racing that write sees a *newer* id); `cancel_previous_array()` scancels the
prior array; roster resolves per-item via `scan_statuses`.

**Designed, not built** (user steer: land containment, review design first) — one JSON record per
item carrying `state` + `array_job_id` owner, making illegal states unrepresentable. Full map in
`docs/roadmaps/roadmap_task-status-state-machine.md`.

Useful structure fact: the **UI side is already consolidated** — `scan_statuses`
(`ui/components/task_utils.py`) is the single resolver for roster, dashboard, and tracker. Only the 3
driver-side readers (`get_previously_succeeded`, `get_previously_done`, `collect_task_results`) still
glob files, so the migration is smaller than it looks.

See [[project_pipeline_state_desync]], [[reference_cbe_node_mount_rot]], [[project_ts_exclude_from_processing]].
