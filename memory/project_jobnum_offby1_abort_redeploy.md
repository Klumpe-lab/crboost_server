---
name: project_jobnum_offby1_abort_redeploy
description: "Alignment fails \"Tomostar directory not found\" — TWO distinct causes: (1) job-number off-by-one after abort+redeploy (reuse branch), (2) pending_tiltFilter placeholder from interactive-restore legacy paths key (FIXED 2026-08-12)"
metadata: 
  node_type: memory
  type: project
  originSessionId: ef8456d9-c43d-4928-9224-55270a6ffb7b
  modified: 2026-08-12T10:03:24.689Z
---

**Symptom:** alignment supervisor dies instantly with `[SUPERVISOR] FATAL ERROR: Tomostar
directory not found: .../External/jobNNN/tomostar` (ts_alignment.py:221), and a plain re-run
fails identically (FAILED job → retry path re-sbatches in place with the same stale params).
Seen on /groups/klumpe/crboost_data/20260311_MyrGFP_Grid2 (job005 alignment pointed at the
empty job003 tomostar; real tsImport output was job004).

**Root cause (proven via the redeploy's `Schemes/run_*/resolution_report.txt`):** after a
scheme run **aborts** partway (e.g. RELION_JOB_EXIT_ABORTED) leaving an empty stub job dir,
the redeploy's job-number **prediction goes off-by-one vs RELION's actual assignment**. In
`pipeline_orchestrator_service.py:117-125` the **reuse-on-rerun branch** reuses a stale
`relion_job_name` (the aborted stub, e.g. job003) **without consuming a RELION counter slot**,
while RELION's schemer assigns the next *free* number (job004) when it really runs the job.
Result: every instance's predicted `job_path_mapping` + all resolved input paths sit one
behind RELION's real dirs. Downstream alignment's `tomostar_dir` (InputSlot
preferred_source="tsImport", resolved at deploy into job_model.paths and persisted to
project_params.json) points at the stale empty stub → instant fail. `rlnPipeLineJobCounter`
was already correct (RELION-authoritative); the reuse branch is the sole desync.

The reuse branch is only ever *reached* in the schemer path by aborted/SCHEDULED jobs
(SUCCEEDED is filtered at :47, FAILED-with-rjn goes to the in-place retry path at :69-81), and
RELION never honors the reused number in the schemer path — so the branch is both ineffective
for its stated goal (.task_status preservation) and the cause of the off-by-one.

**STATUS (2026-06-23): treated as HYPOTHESIS-needing-reproduction, NOT fixed in deploy logic.**
User (correctly) hesitant: doesn't happen on most projects, and the failing project is ~10-20
commits behind current main. Confidence: ~certain on the *symptom* (off-by-one → consumer points
at empty stub, proven by resolution_report.txt + on-disk dirs); ONLY ~50% on the *mechanism*
(reuse-branch at orchestrator:117-125 vs stale `_get_current_relion_counter` read — both yield
identical 003/004/005 predictions, surviving artifacts can't distinguish). Full writeup + repro
plan in **services/scheduling_and_orchestration/ORCHESTRATOR_ROADMAP.md** (H1).

**Current-code mitigations found (why most projects work):** (1) `get_driver_context`
(driver_base.py ~99-124) RE-RESOLVES paths at drive time instead of trusting the deploy snapshot
— comment explicitly names "schemer skipped a number … orphan row in default_pipeline.star";
(2) producer ranking (path_resolution_service.py:225-234) prefers SUCCEEDED + higher job number.
BUT both may be defeated by the DUAL-MAPPING: job003(Scheduled stub) + job004(Succeeded) are the
SAME single tsImport instance; the producer index emits ONE candidate from that instance's
relion_job_name (normal slots NOT disk-gated), and in the failing project it's STILL stuck on
job003. So the real fix may be: sync must map an instance to its SUCCEEDED row, not a superseded
stub. Repro (ORCHESTRATOR_ROADMAP step 6) is decisive.

**LANDED 2026-06-23 — transparent guard (code-clean, PENDING runtime):** `driver_base.py`
`require_producer_input(path,label)` + `diagnose_stale_producer()` — on a missing/empty upstream
input, the raised error scans sibling `External/job*/<name>` for a populated producer and names
it + labels the stale-job-number cause. Wired into ts_alignment.py (tomostar+settings),
ts_ctf.py (input_processing+input_star), ts_reconstruct.py (input_star). Makes the failure
legible; does NOT prevent the drift. Deploy-logic fix deferred until reproduction.

**SECOND DISTINCT CAUSE — pending_tiltFilter placeholder (postrefactor project, FIXED 2026-08-12):**
same "Tomostar directory not found" but the path is `External/pending_<instance>/tomostar`, NOT a
jobNNN stub — this is NOT the off-by-one. Chain: interactive tiltFilter committed (finalize sets
`paths["output_tomostar"]` = root `TiltFilter/tomostar`) → user deletes + re-adds the job →
`_restore_interactive_state` (pipeline_builder_panel.py) restored the LEGACY `output_star` key +
unconditionally marked SUCCEEDED → SUCCEEDED interactive producer with no output_tomostar/no
relion_job_name/no job_path_mapping entry → resolver falls to the `pending_` placeholder, and
deploy validation treated pending as "expect later" (only true for dispatched jobs, never
interactive) → doomed path wired, driver crashes with the MISLEADING off-by-one hint. Three fixes
landed 2026-08-12: (1) restore sets `output_tomostar` + SUCCEEDED only if `TiltFilter/tomostar`
non-empty; (2) resolver `_interactive_producer_gap()` — interactive producer resolving to pending_
= hard PathResolutionError at deploy AND drive time (never silent, never "awaiting upstream");
(3) `diagnose_stale_producer` has a pending_-specific hint (producer never deployed / re-commit
panel) instead of blaming job-number drift. Unblock for a hit project: re-commit from the tilt
filter panel (finalize rewrites the key) or delete+re-add the job post-fix, then requeue.

**Immediate per-project unblock (no server restart needed):** symlink the stale stub's
tomostar + settings to the real producer dir, then re-run alignment (retry path picks it up):
`ln -s ../jobREAL/tomostar External/jobSTALE/tomostar` and same for
`warp_tiltseries.settings`. copytree follows the symlink so job005/tomostar gets the real
files. Reversible (rm the symlinks). Don't edit project_params.json on disk — the live
in-memory ProjectState clobbers it on next save (see [[feedback_inmemory_state_authoritative]]).
Related: [[project_pipeline_state_desync]].
