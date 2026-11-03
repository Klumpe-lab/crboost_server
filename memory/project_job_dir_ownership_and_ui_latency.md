---
name: project_job_dir_ownership_and_ui_latency
description: "2026-08-28 incident — two supervisors in one job dir; roadmap 15 merged with 13 into roadmap 33 (job liveness) 2026-10-15, now roadmap 32 §14; L4 + L1 BUILT 2026-10-26 (project_liveness_batch_2026-10-26) + UI latency fixes LANDED (log tail read, incremental registry refresh, scandir task status)"
metadata: 
  node_type: memory
  type: project
  originSessionId: f740f5df-b957-4d55-831a-11b7cee68a60
  modified: 2026-08-28T07:23:25.993Z
---

**2026-10-26:** 33 is now roadmap 32 §14; its L4 + L1 (and 27 2a) were built on `uv_and_UI`, NOT RUN — see [[project_liveness_batch_2026-10-26]].

**2026-10-15:** roadmaps 13 + 15 merged into `docs/roadmaps/roadmap_33-job-liveness.md` (15's S1–S4 numbering kept; 13 is S5, A1–A3 are S6). Re-checked on main: 13's dot complaint is FIXED on afterok (JobStatus.QUEUED, purple); the double-supervisor class is still reachable — a live job can read FAILED (cancel/stop on failed scancel, 90 s grace, foreign FAILURE marker, NODE_FAIL) and Run then reuses its dir in place; plus a crash window (counter/slurm ids saved only after the sbatch loop). Prerequisite of 27 stage 2 and 32 stage 2.

**Incident (2026-08-28, `/groups/klumpe/crboost_data/YakoutRayyan_Aug27`, 114 TS):** 62/114 tsAlignment
tasks failed with `producer drift — missing from: tomostar dir`. The producer (job003) was complete;
**two supervisors ran concurrently in `External/job004`** (two `SUPERVISOR mode` banners interleaved in
one `run.out` at conflicting offsets, two array ids 1548806/1548927, loser died on
`Errno 39 Directory not empty` mid-`rmtree`). `ts_alignment.enumerate_items` `rmtree`s + rebuilds
`job_dir/tomostar` in sorted order, so the lost names were exactly the first 62 in sort order.

Root cause: every liveness guard is **in-memory single-process** (`_active_processes`,
`_retry_monitors`, `ProjectState.pipeline_active`) and `launch_retries` re-`sbatch`es
`run_submit.script` in place after deleting the exit markers. → **docs/roadmaps/roadmap_15-job-dir-ownership.md**
(S1 O_EXCL `.supervisor.owner` lock + squeue liveness = the load-bearing stage; S2 atomic
tmp-dir+`os.replace` rebuilds; S3 only the lock owner may write `RELION_JOB_EXIT_*`; S4 submitter-side
squeue pre-flight). **Scoped, NOT built.**

Two adjacent findings in that roadmap's §3, both real: `registry.load()` **silently drops** sidecars
that fail validation and `save()` then rewrites index.json from the survivors (data loss — a driver
ran on 62 of 114 TS); and the schema check only fires on a **major** bump, so 1.4-vs-1.3 skew was
invisible. Cause of the skew: `/groups/klumpe/software/crboost_server` (the cluster install the
drivers run from) was **9 days stale** vs `/users/artem.kushner/dev/crboost_server` — 22k pydantic
`extra_forbidden` lines, 5.3 MB `run.err`. Redeploying the shared install is an ops step nobody owns.

**Latency fixes LANDED same day (uncommitted, PENDING RUNTIME):**
- `pipeline_runner.get_job_logs` was a blocking full `f.read()` of run.out+run.err on the event loop
  every 3 s per open job tab → now `_read_tail` (256 KB, line-cut) in `asyncio.to_thread`;
  `tail_bytes=0` for the copy button, which reads on demand instead of holding the whole log per tab.
- `get_registry_for` threw the instance away and re-validated all 19 MB / 114 sidecars whenever
  index.json's mtime moved (constant during ingest) → `TiltSeriesRegistry.refresh_from_disk()`
  re-parses only sidecars whose own mtime changed (`_ts_mtime_ns`); `_reindex_ts(replacing=False)`
  drops the quadratic wipe on fresh loads.
- `array_tasks.scan_statuses` did 114 `.exists()` per 5 s tick → one `os.scandir`.

See [[reference_hpc_env]] (no interpreter in the sandbox — ruff only), [[feedback_never_commit]],
[[feedback_never_silent_defaults]], [[project_pipeline_state_desync]].
