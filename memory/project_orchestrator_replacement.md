---
name: project_orchestrator_replacement
description: "relion_schemer → afterok DAG. SCHEMER DELETED 2026-10-15 on graphical_pipeline (R1+R4+D4+C8, NOT RUN, roadmap 32 §13); left: R2 RELION export (P1.C), rest of P1.D; mark orchestrator §0 done on merge"
metadata: 
  node_type: memory
  type: project
  originSessionId: 6bce5811-b11f-4ab1-ab6e-141fd67999db
---

**UPDATE 2026-10-15 (later): the schemer is DELETED on the graphical_pipeline worktree (roadmap 32 §13, NOT RUN): flag gone from Config + template, ProjectState field always True; sync_all_jobs/is_active/retry path/schemer monitor/scheme writer/relion_pipeliner delete gone; Stop+Cancel now scancel manifest arrays and mark_stopped_tasks_failed (D4); counter seed kept (C8). Kept for R2: project qsub.sh copy, relion --do_projdir, job.star, star pruning on delete, JobTypeResolver. On merge: mark §0 R1/R4/D4/C8 done on main; drop token_auth README use_afterok bullet.**

**STATUS 2026-10-15 (code audit; folded into the doc as §0 R1–R4, order C5+D4 → R1 → R2 → R4):** cutover DONE — every Run/Approve goes through `_submit_chain`, status through
`reconcile_afterok`; the schemer is unreachable under the shipped confs. LEFT: (1) the flag is still a flag —
`Config.use_afterok_orchestrator` defaults False in code (config_service.py:246-249) and `_afterok_global_default`
(project_state.py:55-64) swallows any exception → False, i.e. a config error silently falls back to the schemer;
(2) P1.C not started — nothing writes default_pipeline.star after project init, so "open in RELION" is false and
the delete preview fails then renders green "No downstream jobs will be affected" (job_tab_component.py:403-422,
pipeline_roster.py:773-793; = roadmap 32 S2.6); (3) P1.D half — queue-wait tooltip exists; no staleness indicator;
per-job Cancel UI still schemer-shaped; afterok Stop/Cancel never call mark_stopped_tasks_failed (only runner:1351);
(4) ~1,400 dead schemer lines (runner ~910, orchestrator ~320, monitor ~130). Doc anchors/toggle text are stale.

User wants to tear out `relion_schemer` as the orchestrator. After a design
discussion they agreed **wholeheartedly** with this direction (2026-06-23,
brainstorm — NOT yet implemented):

**Key insight that makes it tractable:** the drivers ALREADY own the hard parts
(per-TS SLURM-array fan-out, partial completion, idempotent restart — e.g.
`drivers/subtomo_extraction.py` supervisor mode). The schemer is only a thin top
layer (sequence job *types*, submit supervisor via `qsub.sh`, track via
`default_pipeline.star`). So replacement is "swap the thin layer," NOT a
from-scratch workflow engine.

**BUG FOUND + FIXED 2026-06-24 — job DELETION was schemer-only (broke for afterok jobs).** Deleting an
afterok job (e.g. a failed `denoisetrain__2`/`denoisepredict__2` 2nd-instance) threw the toast
`Delete failed: Partial failure: External/jobNNN/: Job 'External/jobNNN/' not found in pipeline` and
the job didn't vanish — it re-rendered as a ghost YELLOW/scheduled entry. Root cause: afterok jobs are
NEVER written to `default_pipeline.star` (it lists only Import + the legacy schemer jobs), but the
delete path `project_service.delete → PipelineDeletionService.delete_job` is RELION-pipeline-centric:
it loads default_pipeline.star, can't find the job, and returns an error BEFORE moving the job dir to
Trash. `project_service` then pops the job from `state.jobs` + saves ANYWAY (so it leaves state) but
(a) the External/jobNNN dir is orphaned on disk, and (b) since the result is `success:False`, the
roster's `confirm()` skips `remove_instance_from_pipeline` (pipeline_roster.py:759), so stale UI
tracking re-renders the now-stateless instance as a default SCHEDULED ghost. FIX (code-clean,
SERVER-side → needs main.py restart): `pipeline_deletion_service.delete_job` now, when the job isn't in
default_pipeline.star, moves the dir to Trash via new helper `_move_job_dir_to_trash` and returns
`success=True` (so the roster cleanup runs + the job fully disappears). ⚠️ Remaining minor gap (NOT
fixed, not hit by the user): the no-instance_id branch `project_service.delete` L140 (when
`find_jobs_by_type` returns [] for afterok jobs) does state-only removal without trashing the dir.
Manually moved the already-half-deleted `External/job011` (LORENZ) to Trash. See
[[project_jobnum_offby1_abort_redeploy]] (orphaned dirs → job-number drift).

**Target architecture:**
- `ProjectState` (`project_params.json`) = SOLE runtime truth; nothing reads
  `default_pipeline.star` as input.
- `default_pipeline.star` / `job.star` → demoted to a generated RELION-compat
  EXPORT (the root pain is that file being BOTH live state AND compat artifact,
  forcing `pipeline_runner.sync_all_jobs` reconciliation — source of the status
  desync, see [[project_pipeline_state_desync]]).
- **SLURM job dependencies** (`afterok`) = the DAG engine (SLURM is already the
  real scheduler). Supervisors do internal dynamic fan-out, so unknown-until-
  runtime TS counts aren't a blocker.
- Our code → a thin **stateless reconciler/observer**: submit chain, read
  `sacct`/`squeue` + sentinels → ProjectState, emit WS/SSE events to UI.

**Durability ("dies when login tab closes"):** orthogonal problem; same issue
for any head-node process. Stateless design lets the process die freely. Tiers:
dev = tmux/nohup+setsid; prod sweet spot = **scrontab reconciler** or
self-resubmitting **service-partition SLURM job**; gold = off-cluster host +
`slurmrestd`. User is taking a list of admin questions (logind KillUserProcesses,
enable-linger, scrontab/SLURM version, slurmrestd, service partition/QOS, Open
OnDemand, load-balanced login nodes) to the cluster admin.

**UPDATE 2026-06-23: GREENLIT + PLAN FINALIZED (verified against a full codebase
map — 8 subsystem readers + adversarial critic).** ⚠️ CORRECTION: the off-cluster
VM is **only in talks, NOT secured** (user steer) → build for **on-cluster**:
server + reconciler in ONE process on the headnode, shared-Lustre sentinels, local
sbatch/squeue/sacct. This MOOTS the VM↔Lustre / VM↔SLURM / split-brain forks for
v1 and keeps the reconciler in-process (in-place ProjectState mutation still pushes
to NiceGUI). Future VM move = clean follow-on (design is stateless); only the
SLURM-access seam swaps. Durability v1 = Tier 1 (nohup/tmux); prod = Tier 3
(scrontab/service job).

**UPDATE 2026-06-24: ✅ RUNTIME-VALIDATED (success path) + import-job blocker RESOLVED.**
- **IMPORT-job gate (P1.A #1) RESOLVED via Option B:** import is no longer a relion command — it's our own
  inline registry→star writer (`_write_import_stars_inline`, pipeline_orchestrator_service.py), run inline at
  deploy (no SLURM job/container), output byte-identical to relion's. `_submit_chain` special-cases IMPORT
  (writes inline, marks Succeeded, slurm=None, excluded from `prepared` so fsMotionAndCtf has no afterok
  producer). Full detail: [[project_import_job_necessity]] + doc §6a.
- **AK_new_orchestrator_demo:** a 5-job chain (import→fsMotionAndCtf→tsImport→align→tsCtf) ran end-to-end on
  afterok — schemer never launched (Schemes/ empty), import inline (slurm=None) byte-identical to relion, all
  4 SLURM jobs Succeeded (seq ids), pipeline_active wound to False. **Option B + P1.A + P1.B runtime-proven on
  the SUCCESS path.**
- **Global dev toggle (TEMPORARY):** `conf.yaml use_afterok_orchestrator: true` → every project uses afterok
  without the per-project edit (Config field → `project_state._afterok_global_default`, field default_factory +
  from_dict OR). STICKY (bakes into project_params.json) → keep to scratch projects; REMOVE after validation
  (config field + 2 project_state.py edits + yaml lines).
- **Still unproven at runtime:** the full tail (tiltFilter/tsReconstruct/**denoise**/TM/extract/subtomo/
  reconstruct/class3d), stop-on-fail (DependencyNeverSatisfied→scancel), retries/re-deploy, SLURM-array
  supervisors, and P1.C (RELION-open). **USER PLAN: add denoise/IsoNet in another session, then pick up HERE
  to run the FULL pipeline on afterok.**

**KEY FINDING — the doc's "Phase 0" (stop reading default_pipeline.star) is NOT a
standalone step.** Central read `pipeline_runner.py:62` is the linchpin: on the
schemer path `job_model.slurm_job_id` is None, so in-flight status comes ONLY from
the star; the 4 RMW patches (:866,1097,1131,1214) are load-bearing only BECAUSE :62
reads back; dropping :62 also kills the stop-on-fail trigger. Read-removal is
DISTRIBUTED across P1.A–C; the central read is the **P1.A+P1.B CUTOVER** (replacing
schemer-as-driver AND sync_all_jobs-star-diff-as-status is ONE switch). Gate behind
a `use_afterok_orchestrator` flag (no local runtime; user restarts main.py).

**FINALIZED PHASES (full file-level plan = §6 of the doc):**
- **P1.0 (additive prereqs) — ✅ LANDED 2026-06-23 (code-clean: py_compile + ruff
  check/format; PENDING runtime, needs main.py restart):** (a) `ProjectState.pipeline_order:
  List[str]` added (was ONLY in `UIState.selected_jobs` ui_state.py:64, per-tab);
  SCHEMA_VERSION **(3,0)→(3,1) MINOR** (additive field; NOT 4.0 — major would
  spuriously warn+backup every project); backfilled on load from loaded `jobs` in
  file order (NO PIPELINE_ORDER import into services — submit order = afterok
  toposort, so persisted *membership* is the point); write-through from full
  `selected_instance_ids` in `deploy_and_run_scheme`. (b)
  `PathResolutionService.resolve_edges(instance_ids=None) -> [(producer_iid,
  consumer_iid)]` read-only DAG deriver — mirrors resolve_inputs selection (override
  then `_choose_candidate_for_slot`), reads `producer_instance_id` off chosen
  OutputCandidate (path_resolution_service.py:26), skips manual/mergedSources/SELF/
  unresolved (no dangling or self afterok edge), tolerant of unresolved slots (no
  raise). NO change to resolve_inputs. Pre-existing F401 drift in both files left
  untouched (job_models block is load-bearing). NEXT = P1.A submit_chain.
- **P1.A submit-machinery — ✅ LANDED 2026-06-23 (code-clean: py_compile + ruff
  check/format on touched files; only pre-existing F401 drift; PENDING runtime, needs
  main.py restart).** Dormant behind a NEW per-project `ProjectState.use_afterok_orchestrator`
  flag (default False; schemer path byte-identical when off). `SCHEMA_VERSION (3,1)→(3,2)`:
  +`use_afterok_orchestrator`, +`job_dir_counter` (SOLE job-dir allocator — seeded once from
  `rlnPipeLineJobCounter` which is the NEXT-free number, monotonic via a LOCAL counter committed
  only after a clean prepare+toposort, NO reuse-on-rerun ⇒ off-by-one fixed). New:
  `_submit_chain` + `_render_supervisor_script` (qsub.sh→`run_submit.script`; resources from
  `job_model._get_queue_options()` so array-job *supervisors* stay lightweight via
  supervisor_slurm_defaults, NOT get_effective_slurm_config; RELION marker block KEPT) +
  module-level `_toposort_submit_order` (Kahn + cycle check) in the orchestrator;
  `_sbatch_script(+dependency_after_ids)` injects `--dependency=afterok:` + public
  `submit_supervisor` in pipeline_runner; flag branch at the `deploy_and_run_scheme` launch seam
  (after the retry partition). Adversarially reviewed (5 dims, verified findings); fixes folded in:
  `sync_all_jobs` now early-returns for afterok projects (THE central-read cutover seam — needed
  because default_pipeline.star ALWAYS exists from project init + sync is reachable from UI calls,
  not just the monitor, so it WOULD wipe slurm_job_id/relion_job_name/QUEUED); pipeline_active left
  False (P1.B owns it); partial mid-chain submit failures persist whatever reached SLURM.
  **⚠️ RUNTIME GATES before an afterok run completes end-to-end:** (1) IMPORT job —
  `_build_import_command` reads a nonexistent `paths["mdoc_glob"]` (glob is on `state.mdocs_glob`)
  so `--i` is dropped, and runs relion_import cwd=job_dir with no `--odir` (vs schemer's project-root
  run); dead under schemer (importtomo job.star ignores fn_exe) but LIVE under afterok → must fix the
  cwd/`--odir`/`--i` contract + validate. (2) P1.B reconciler REQUIRED for live status (flag-on chain
  runs but UI shows nothing). (3) retries under the flag still go via `launch_retries` (schemer-coupled).
- **P1.B reconciler — ✅ LANDED 2026-06-23 (code-clean, PENDING runtime).** The afterok flag is now
  the FULL cutover (submit + status). `pipeline_runner.reconcile_afterok` (the monitor dispatches
  afterok projects here; sync_all_jobs stays guarded {}): per NON-terminal tracked job (slurm_job_id),
  authority order = disk `RELION_JOB_EXIT_*` sentinel → targeted `squeue -j` (B1 QUEUED-vs-RUNNING +
  `DependencyNeverSatisfied`→scancel) → `sacct` → a **90s grace window** (`_afterok_absent_since`) that
  FAILs a marker-less vanished job so pipeline_active ALWAYS winds down (the qsub trailer writes the
  marker only on clean exit, so SIGKILL/NODE_FAIL/scancel leave none; this was the verified stuck-forever
  HIGH). New slurm_service: `query_jobs_by_ids` (B2 fix — `None`=CLI error vs `{}`=not-queued, so an
  errored squeue is never read as "done") + `query_terminal_states` (NET-NEW sacct, graceful None).
  `_afterok_state_to_status` maps SLURM states. Monitor `_tick_once` branches afterok→reconcile (schemer
  path byte-identical when off); `_recover_one` registers-and-defers (restart re-observes via persisted
  pipeline_active=True). `submit_chain` now SETS pipeline_active=True + unlinks stale exit markers.
  **Control-path made afterok-aware** (reachable via existing UI buttons): `cancel_job`/`stop_and_cleanup`
  scancel by persisted `slurm_job_id` (NOT squeue-discovery → sidesteps the broad get_user_jobs []-bug);
  `cancel_job` does NOT clear pipeline_active for afterok (would freeze the live DAG); afterok re-runs
  route through `_submit_chain` (fresh dirs + rewired afterok edges), NOT schemer `launch_retries`.
  Adversarially reviewed (4 dims + focused regression pass = SAFE); refuted findings left as-is
  (batch-poison is NOT real on standard SLURM — `squeue -j` returns live rows on mixed valid/gone ids;
  locale; event-loop I/O is the established baseline; proactive dependent-scancel unneeded — SLURM's
  afterok gate physically blocks dependents, the reactive DependencyNeverSatisfied cascade is correct).
  **⚠️ Runtime: the P1.A IMPORT-job command gate still blocks an end-to-end run; one-time verify sacct
  availability/retention + the exact `squeue %r` reason literal for a never-satisfiable afterok dep.**
- **P1.C export_relion_compat()** authors default_pipeline.star
  (processes/nodes/edges from resolve_edges DAG — NEW; _write_scheme_star:310 only
  linear) + job.star (generalize generate_job_star _base.py:225), THEN remove
  remaining reads + flip deletion (pipeline_deletion_service:92) to
  mutate-state-and-regenerate.
- **P1.D UI:** in-place mutation already pushes; add queue-wait banner (T1)+staleness.

**NEW VERIFIED RISKS:** R-OPEN (highest) — does RELION GUI open a project whose
External/jobNNN + rlnPipeLineJobCounter it didn't allocate? must validate export vs
a real open. R-DELETE — no ProjectState mirror of star edge tables; cascade rebuilt
from io_slots graph. R-EXIT — non-uniform supervisor exit codes (ts_alignment exits
0 on partial, fs_motion/ts_reconstruct exit 1). R-EDGE-SNAPSHOT — submit-time edges
vs drive-time re-resolution can differ.

**Verified read-removal targets:** pipeline_runner.py:62,866,1097,1131,1214;
pipeline_orchestrator_service.py:257,417,502; pipeline_deletion_service.py:92.
See [[project_pipeline_state_desync]] (B1/B2 etc.), [[project_jobnum_offby1_abort_redeploy]]
(reuse branch dies with P1.A), [[feedback_inmemory_state_authoritative]] (in-place mutation).

**Buy vs build:** lean BUILD the thin reconciler. NOT Nextflow (batch DSL, fights
RELION layout + GUI status). Snakemake closest "buy" (Python) but still batch/
pull-based. Criterion: buy when the engine owns the hard part; build when the
hard part is what you need to control (our per-TS fan-out / UI status / RELION
layout).

**UI/NiceGUI grievance:** explicitly DEFERRED (lower leverage). The clean status
API from this rework is the seam that later enables an incremental frontend swap.

**Doc:** `services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md`
(phased plan, durability matrix + admin questions, buy-vs-build, risks).

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

GREENLIT + PLAN FINALIZED 2026-06-23 (verified vs full codebase map): rip out relion_schemer → ProjectState sole truth + SLURM afterok DAG + thin stateless reconciler; star → RELION-compat export. VM only IN TALKS, not secured → build ON-CLUSTER (in-process reconciler). KEY: doc's "Phase 0" (stop reading default_pipeline.star) is NOT separable — central read pipeline_runner.py:62 is the P1.A+P1.B cutover. Phases P1.0(prereqs: persist pipeline_order + resolve_edges)→P1.A(submit_chain)→P1.B(reconciler)→P1.C(export+read-removal)→P1.D(UI). Full §6 in services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md.
