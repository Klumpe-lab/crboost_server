# Roadmap 33 — Job liveness: one supervisor per job dir, and live means QUEUED or RUNNING

**Status:** scoped 2026-10-15 by merging roadmap 13 (roster staged vs queued, 2026-08-23) and roadmap 15 (job-dir
ownership, 2026-08-28), both re-checked against main the same day and moved to `completed/` as superseded.
**Nothing built.** Stage numbers S1–S4 are 15's, so "15 S1" elsewhere means "33 S1".

What became of each:
- **13's complaint is fixed on the afterok path**, the only one in use. `JobStatus.QUEUED` exists
  (`services/models_base.py:8-14`). `_submit_chain` sets it per job as each sbatch returns
  (`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:689-691`). The roster paints it purple
  (`ui/status_indicator.py:10`) and a staged job amber (`:9`); a parked job gets a ring
  (`ui/pipeline_builder/pipeline_roster.py:244-251`). Its predicate (`pipeline_active and iid in pipeline_order`) is
  dead: `pipeline_order` is roster membership now. What survives is the vocabulary (§1.6): several readers still
  count a staged job as live, and a queued job's form silently drops edits.
- **15's incident is still reachable under afterok, by more routes than it listed.** The run guard moved from
  process-local dicts to persisted job status. That status can say FAILED while a supervisor is still alive, and
  nothing on disk names a job dir's owner.

## Before → After

**Before:** Run resubmits a FAILED job into its own dir (`pipeline_orchestrator_service.py:540-550`) without
asking SLURM whether that job's supervisor still runs, and a live job reads FAILED in four ways (§1.2). Two
supervisors then share one `run.out`, one `tomostar/`, one `.task_status/`; §1.1 lost 62 of 114 tilt series that
way. Delete moves a live job's dir to Trash; removing it from the roster orphans its SLURM job. Staged jobs count
as live in the readers of §1.6, and a queued job looks editable while its edits are dropped.

**After:** a job dir has one owner, named on disk and checked against SLURM. A second supervisor exits without
touching anything or speaking for the job, and the submitter refuses before sbatch, naming the live job id.
Shared inputs are rebuilt atomically. Live means QUEUED or RUNNING everywhere: staged jobs never count as live,
and live jobs cannot be edited, deleted or removed, with the UI saying why. The tilt-series registry fails loudly
on what it cannot read. **Gain:** the double-supervisor class is unreachable, and the two roadmaps that multiply
resubmits and concurrent runs (27 stage 2, 32 stage 2) get their prerequisite.

---

## 1. Facts (main, 2026-10-15)

### 1.1 The incident (2026-08-28, from roadmap 15)
Project `<user>_Aug27`, job `External/job004` (`aligntiltsWarp`, 114 tilt series). 62 array tasks failed with
`registry TS '…' missing from: tomostar dir (producer drift)`. The producer was innocent: `job003/tomostar/` held
all 114 files. The consumer's own copy, `job004/tomostar/`, held 52, and the 62 missing names were exactly the
first 62 in sort order: a directory deleted underneath a sorted rebuild loop. Two supervisors ran in that dir:

| evidence | where |
|---|---|
| Two `SLURM JOB BEGAN` / `SUPERVISOR mode` blocks, interleaved at conflicting offsets (each job reopens `run.out` with `O_TRUNC` while the older one keeps writing) | `job004/run.out` |
| Two array submissions from one dir: `1548806`, then `1548927` | `job004/run.out` |
| The second took the resume path: `52 tilt-series already succeeded; 62 to (re)run` | `job004/run.out` |
| The loser died mid-`rmtree`: `OSError: [Errno 39] Directory not empty: '…/job004/tomostar'` | `job004/run.err` |

That run was on the schemer path (`launch_retries`, now dead code). The class is the same under afterok.

### 1.2 How a live job reads FAILED
- Cancel and Stop mark FAILED even when `scancel` fails: `cancel_job` (`pipeline_runner.py:1429-1454`, a warning
  only) and `stop_and_cleanup` (`:1263-1293`).
- The 90 s absence grace (`_AFTEROK_ABSENT_GRACE_SEC`, `pipeline_runner.py:55`; applied `:493-498`).
- A `RELION_JOB_EXIT_FAILURE` on disk wins over SLURM (`reconcile_afterok` pass 1, `:439-455`), and a duplicate
  supervisor's trailer writes one into the shared dir.
- sacct NODE_FAIL / PREEMPTED map to FAILED (`:36-37`); qsub sets no `--no-requeue`, so a requeued job may come
  back while the app has moved on (unverified).

Once FAILED, a job leaves the tracked set (`:430-434`), and Run's reuse test (FAILED + recorded dir exists,
`pipeline_orchestrator_service.py:541-546`) resubmits into that dir.

### 1.3 What guards exist
- `_submit_lock` (`pipeline_orchestrator_service.py:153-159`): an in-process `asyncio.Lock` per project. It stops
  two Runs in one process; it never looks at job dirs or SLURM. No `O_EXCL`, `flock` or owner file exists anywhere.
- Run refuses while `pipeline_active` (`:175-176`). That protects a FAILED-but-live dir only while a sibling job is
  live, and roadmap 32 S2.2 removes it.
- Driver side, array jobs only: `cancel_previous_array` (`drivers/array_job_base.py:659`, called `:615`) scancels
  the previous submission's array, and `is_superseded_task` (`:89`) mutes status writes from its tasks. Net effect
  today: the second supervisor kills the first one's array; the first then marks the missing items `.fail`
  (`:917-924`) and its trailer writes a marker into the shared dir. Non-array drivers (miss_align, refine3d,
  class3d, reconstruct_particle, denoise_train, ts_import, tilt_filter, extract_pick_list, isonet_star) have
  neither.
- **Crash window:** the job-dir counter is advanced in memory after the prepare pass (`:665-666`); it, the slurm
  ids and `pipeline_active` reach disk only after the whole sbatch loop (`:699-702`). A server that dies mid-loop
  re-allocates the same `jobNNN` dirs on the next Run, over live supervisors (code reading, unverified).

### 1.4 Exit markers
- Written only by the rendered qsub trailer (`QSUB_EXIT_MARKERS`, `services/computing/slurm_service.py:90-98`,
  enforced byte for byte by `write_sbatch_script`, `:101`; template `config/qsub.template.sh:50-58`), plus the
  inline Import (`pipeline_orchestrator_service.py:654`).
- Changing the block means changing every site's `qsub.sh` (the lab install, Munich). A stale one is refused
  loudly, not ignored.

### 1.5 Shared inputs rebuilt in place
- Whole job-local dir, `rmtree` then rebuild: alignment `tomostar/` (`drivers/ts_alignment.py:217-221`), tsCtf
  `tomostar/` (`drivers/ts_ctf.py:248-258`, roadmap 27 §4.4 item 9), miss-alignment `warp_tiltseries/`
  (`drivers/miss_align.py:277-278`).
- Per-TS replace into a shared dir: `ts_alignment.py:131-133`, `warp_export.py:326-328`,
  `subtomo_extraction.py:169-171`.
- In-place overwrites: `ts_alignment.py:224-225` (settings copy), `ts_ctf.py:213-230` (XML copies), the
  manifest writes in `array_job_base.py:66`, `:76-81`.

### 1.6 Staged, queued, parked — the vocabulary
- **Readers that count a staged job as live:** `path_resolution_service.py:667-670` (`source_in_flight` →
  `expect_file_later`: any unrelated live run hides a missing input), `services/dashboard_data.py:460`, `:503`,
  `:688` (all → "running"), the afterok Stop's live set (`pipeline_runner.py:1263-1293`, SCHEDULED jobs that carry
  a slurm id, marked FAILED with the rest). The landing page already gets it right: Queued and Running are live,
  Scheduled is not (`backend.py:1832-1842`).
- **QUEUED is missing** from the candidate ranking used by automatic input selection
  (`path_resolution_service.py:537-542`: it ranks after FAILED) and from `display_name` (`:72-77`: shows `?`).
- **Queued jobs look editable but drop edits.** The model refuses USER_PARAMS writes unless SCHEDULED or FAILED
  (`services/jobs/_base.py:413-423`, an INFO log), while the form freezes only RUNNING and SUCCEEDED
  (`is_job_frozen`, `ui/pipeline_builder/config_tab.py:11-16`). Source and SLURM overrides do reach a queued job:
  drivers re-read the project file at start (`drivers/driver_base.py:83-139`).
- **No liveness gate on Delete or remove.** Job-tab Delete moves the dir to Trash with no scancel
  (`ui/pipeline_builder/job_tab_component.py:196-200` → `pipeline_deletion_service.py:153-163`). Removing a
  non-succeeded job from the roster deletes it from state (`pipeline_builder_panel.py:480-483`) and orphans its
  SLURM job.
- **Per-job Cancel** is offered only for RUNNING (`job_tab_component.py:138`, `:188-191`), not QUEUED. Confirming
  it sets `is_running` False and stops the tab's timers (`:357-361`), while `cancel_job` deliberately keeps
  `pipeline_active` up (`pipeline_runner.py:1429-1435`): the tab stops following the rest of the chain and shows a
  Run the backend refuses.
- **Removed jobs come back.** Removing a succeeded job drops it from `pipeline_order` only
  (`pipeline_builder_panel.py:480-486`); opening the project rebuilds the roster from `state.jobs.keys()`
  (`ui/open_project.py:61-63`, `ui/main_ui.py:267-271`).
- **One deliberate exception:** a confirmed re-extract of a pick list wipes `out/` (`backend.py:628-631`, confirm
  at `ui/particles/list_actions.py:130-155`) without cancelling the earlier extraction (decision §3.1).

---

## 2. Stages

Each stage is one commit. S1 stops the damage; S3 stops the status flip; S4 gives the refusal in the UI. S5 and S6
are independent of the rest.

### S1 — An owner file in every supervisor (the load-bearing stage)
- Before any dir mutation: create `job_dir/.supervisor.owner` with `O_CREAT|O_EXCL`, holding `SLURM_JOB_ID`, host
  and an ISO timestamp. In `ArrayDriver.run_supervisor` (`drivers/array_job_base.py:859`; its first mutations are
  at `:874-895`) and at the entry of each non-array driver. Not in `driver_base.get_driver_context` as it stands:
  array tasks call it too and would contend with their own supervisor (a task has `SLURM_ARRAY_TASK_ID`).
- On `FileExistsError`: read it and ask SLURM (`squeue -h -j <id>`; supervisors already call squeue,
  `array_job_base.py:559`, `:679-689`).
  - **live** → log `another supervisor (slurm <id> on <host>, started <ts>) owns this job dir` and exit without
    touching anything;
  - **dead** → take it over, logging whose it was;
  - **squeue failed** → treat as live.
- Release on normal exit and in the fatal path.
- **Why `O_EXCL`, not `flock`:** job dirs live on the group's shared mount. `O_EXCL` plus an explicit SLURM
  liveness check survives NFS semantics and stale locks, and it names the owner in the error.
- Same pattern as roadmap 23's project lease (§3.2) and roadmap 32 S2.5's registry lock, but those holders are
  server processes with no SLURM id: they need a host/pid rule instead of squeue.
- *Done when:* `sbatch run_submit.script` twice into one dir leaves the first untouched, and the second exits
  naming the owner in `run.out`.

### S2 — Atomic rebuilds; never mutate a shared input in place
- Build into `<dir>.tmp.<slurm_job_id>/`, then `os.replace()` onto the real name (rename the old one aside and
  `rmtree` it after the swap). Atomic writes (temp file + `os.replace`) for the overwrites.
- Sites: §1.5.
- *Done when:* no job-local input dir ever exists half-populated.

### S3 — A marker names the job that wrote it
- The trailer writes `$SLURM_JOB_ID` into the marker instead of `touch`ing it. `reconcile_afterok` pass 1
  (`pipeline_runner.py:439-455`) ignores a marker whose id is not the job's recorded `slurm_job_id`. An empty
  marker, from an old trailer or the inline Import, is accepted as today. RELION only checks that the file exists.
- This changes `QSUB_EXIT_MARKERS`, so every site's `qsub.sh` changes with it (§1.4). Land it before Munich's
  re-install, or ship the `qsub.sh` change with a note.
- *Done when:* a duplicate supervisor's exit, or a crash before its owner check, cannot change the job's status.

### S4 — Submit-side pre-flight, and nothing marked FAILED that SLURM has not confirmed
- In `_submit_chain`, per job and before its sbatch — Run, Run to here (32), Approve (`submit_parked`) and Clone
  all go through it — refuse when:
  - the job's recorded `slurm_job_id` is live per `query_jobs_by_ids` (`slurm_service.py:521`; `None` means squeue
    failed, which counts as live);
  - or the dir's owner file names a live SLURM job.
  - The message names the id: "already running as slurm N".
  - Not `find_slurm_job_for_directory` / `find_all_slurm_jobs_for_directory` (`slurm_service.py:585`, `:616`):
    they return nothing when squeue fails, i.e. they fail open.
- Persist before the first sbatch: the counter and every allocated `relion_job_name`, then each `slurm_job_id` as
  its sbatch returns (closes the crash window, §1.3).
- Cancel and Stop mark FAILED only after SLURM confirms the cancel; a failed `scancel` leaves the job live and says
  so (`pipeline_runner.py:1429-1454`, `:1263-1293`).
- *Done when:*
  - Run refuses rather than tramples, with the live job id in the message;
  - a server killed mid-submit and restarted does not reuse a dir it already submitted into.

### S5 — Live means QUEUED or RUNNING, everywhere (roadmap 13, re-scoped)
- One predicate next to `JobStatus`: live = QUEUED or RUNNING. No new enum member: SCHEDULED is "staged" on the
  afterok path already, and 13 §3's cost of a new member still holds.
- The readers of §1.6 narrow to it. QUEUED joins the candidate ranking (next to RUNNING) and `display_name`.
- The form freezes QUEUED like RUNNING (`is_job_frozen`), matching the model's gate — no more silently dropped
  edits. What a queued job reads at start is roadmap 32 S1's to settle.
- Delete and roster-remove refuse a live job and say why (decision §3.2).
- Per-job Cancel is offered for QUEUED too and leaves `is_running` and the tab's timers alone; the reconciler winds
  `pipeline_active` down. This is the same item as the orchestrator roadmap's P1.D Cancel UI.
- Opening a project builds the roster from `pipeline_order`, not `state.jobs.keys()`.
- The run-slot tooltip's done/total (`pipeline_roster.py:1645-1666`) stops folding staged jobs into the total:
  `3/8 · 5 staged`.
- *Done when:*
  - nothing reads a staged job as live;
  - a queued job cannot be edited, deleted or removed, and the UI says so;
  - Cancel on a queued job works and the roster keeps following the rest of the chain;
  - a removed job stays removed after reopening.

### S6 — The registry fails loudly (roadmap 15's A1–A3)
- **A1, data loss, open.** `TiltSeriesRegistry.load()` drops a sidecar it cannot parse with a warning
  (`services/tilt_series/registry.py:288-298`); `refresh_from_disk` does the same and forgets tilt series missing
  from the index (`:341-345`). `save()` then writes the index from the survivors (`:371-377`). Fix: an unparseable
  sidecar fails the load naming the file, and `save()` never writes an index smaller than the one it loaded
  without an explicit deletion.
- **A2, open.** Only a newer MAJOR schema is noticed, and only warned (`registry.py:279-285`; code schema (1, 5),
  `:48`); with `extra="forbid"` on the models, a newer minor surfaces as A1's silent drop. Fix: refuse when the
  on-disk minor is ahead, naming both versions.
- **A3, mitigated.** Drivers now run from the submitting server's tree (roadmap 20; `services/jobs/spec.py:279-300`),
  so the dev-vs-shared-install skew of the incident needs two servers from two installs. Still worth one line at
  the top of `run.out` naming the install path and the registry schema version.
- *Done when:* a sidecar the code cannot read stops the job with its name and version, and no save shrinks the
  index.

## 3. Decisions owed
1. **Re-extracting a live pick list** (§1.6): cancel the earlier extraction, then wipe `out/` (recommended), or
   refuse while it is live.
2. **Delete / remove on a live job:** refuse with "Cancel first" (recommended), or one confirm that cancels and
   deletes.
3. **Empty markers** (S3): accepted as today (recommended), so old trailers keep working; the alternative breaks
   every site's `qsub.sh` at once.

## 4. Order and dependencies
- S1 → S3 → S4 close the class; S2, S5 and S6 stand alone.
- Roadmap 27 stage 2 (catch-up) needs S1 + S4: a second click on a running top-up is §1.1's incident.
- Roadmap 32 stage 2 (staging while running) needs S1 + S4 — with the project-wide run lock gone, nothing else
  stops a resubmit into a dir whose supervisor still runs — and S5, because staged jobs then sit beside live ones.
- The orchestrator decommission deletes the schemer path, where 13's old conflation (deploy writes SCHEDULED,
  `pipeline_orchestrator_service.py:395-400`) and 15's `launch_retries` still live.

## 5. Runtime checklist
1. `sbatch run_submit.script` twice into one dir: the second exits naming the owner; the first and the job's status
   are unaffected.
2. Cancel a job while `scancel` cannot succeed: the job stays live and says so; Run then refuses with its id.
3. Kill the server in the middle of a 5-job Run; restart; Run again: no dir is reused.
4. Re-run alignment while listing its `tomostar/` in a loop: never a partial set.
5. A queued job: the form is frozen, Delete and remove refuse with a reason, Cancel works and the roster keeps
   following the rest of the chain.
6. A registry sidecar with a field the code does not know: the job fails naming the file and both versions; the
   index on disk is unchanged.

## 6. Not in scope
- Recovering tilt series lost in past incidents: the arrays are idempotent (`task_already_done`,
  `ts_alignment.py:302-312`); one clean re-run after S1 handles them.
- The orchestrator decommission (`roadmap_orchestrator-replacement.md`).
- Adding or removing jobs while a run is live (P-50): roadmap 32 S2.1.

**Modern-Python weave-in:** the owner file as a frozen `dataclass SupervisorOwner(slurm_job_id, host, started)`
with `acquire(job_dir)` returning `ok()`/`err()`; liveness as one function next to `JobStatus` rather than a new
member (13 §3).
