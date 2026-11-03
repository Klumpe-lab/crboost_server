---
name: project_pipeline_state_desync
description: "Root-cause analysis of pipeline/SLURM↔UI status desync + redesign tiers, in PIPELINE_AND_ORCHESTRATOR_STATE.md"
metadata: 
  node_type: memory
  type: project
  originSessionId: ef8456d9-c43d-4928-9224-55270a6ffb7b
---

The systematic "UI doesn't reflect the true pipeline state" complaints (spinners not
spinning, orchestrator appears to lag, progress jumps discontinuously) were diagnosed
2026-06-23 after the `20260610_S2_cell_lamellae_LORENZ` incident. Full writeup +
prioritized redesign lives at repo root **`PIPELINE_AND_ORCHESTRATOR_STATE.md`**.

Core finding: there are **three disjoint status layers** never reconciled against each
other — L1 `pipeline_active` bool (schemer-lifetime "running" chrome), L2 one coarse
`rlnPipeLineProcessStatusLabel` row per job → `job_model.execution_status` (the dot/spinner;
written ONLY by `PipelineMonitor.sync_all_jobs` every 3 s), and L3 per-task `.task_status/*.ok`
markers (scanned only by the browser, never by the server reconciler). The architecture
faithfully reflects in-memory state but is **structurally unable to show "work in progress"**
and **collapses very different SLURM realities (queued / running / just-burst-completed) into
coarse statuses**.

Verified symptom root causes:
- **S2 (40-min "running but nothing starts")** = SLURM **queue wait** for the next supervisor
  + its child array (no `--dependency=afterok` chaining anywhere → serial additive waits),
  totally unsurfaced. `wait_sec`=10 s (generated scheme) NOT 180 s (static template) — the
  schemer wait is NOT the cause.
- **S3 (jump to 45/45)** = `.ok` markers written only at task END (after compute+copytree),
  `array_throttle=20` → throttle-sized waves burst together; L2 dot flips in one tick from a
  single exit marker. FingerprintedView gate is NOT the bug (it folds array counts into the
  signature correctly).
- **S4 (spinner lies)** = spinner renders only for `execution_status==RUNNING`; false-RUNNING
  because `.task_manifest.json` is written BEFORE the array is sbatched (manifest-existence
  upgrade); squeue-fail returns `[]` → defaults RUNNING; and the `pipeline_active`-gate freeze
  during schemer handoff (related to [[feedback_inmemory_state_authoritative]]).
- **S1 (transient div-by-zero, fixed by restart)** = Lustre frame-visibility race —
  `stage_fs_environment` WARN-and-continues on not-yet-visible frames without reconciling the
  staged count → WarpTools divides by zero; rerun works once frames are cached; sparse-array
  retry re-runs only the failed indices. One task fail tears down the whole pipeline (no inline
  retry).

Redesign recommended start: Tier 1 = (1) surface SLURM queue/pending state at the pipeline
level with job-id + elapsed-pending timer, (2) drive spinner/progress off fine per-task
pending/running/done counts not the coarse star label. Tiers 2–4 (bug fixes, dependency
chaining, S1 staging guards) scoped in the doc.

**SLICE 1 LANDED 2026-06-23 (code-clean: py_compile + ruff OK; PENDING runtime — needs
`python main.py` restart per [[reference_hpc_env]]):** in pipeline_runner.py +
pipeline_roster.py — (a) `_get_array_progress` returns n_running (started-but-unsettled via
`task_*.out` glob); roster shows blue `▸N` running chip so parallel arrays don't look frozen
at 0/N; (b) `get_pipeline_overview` counts QUEUED + tracks pending time via
`PipelineRunnerService._queued_since` (in-mem, keyed `<path>::<iid>`); run-slot label shows
`⏳ N in SLURM queue` / `Nm for nodes`; `is_complete` now requires queued==0 && scheduled==0;
(c) B1 fixed — `sync_all_jobs` Running-row mapping no longer flips QUEUED→RUNNING on
`.task_manifest.json` existence (written BEFORE array submit); array jobs stay QUEUED until a
child `task_*.out` exists, then RUNNING. STILL TODO: B2 (squeue-fail→[]→false RUNNING), B3
(crashed task stuck "running"), B4/B5 (pipeline_active freeze + staleness banner), Tier 3
(dependency chaining), Tier 4/S1 (staging-count reconcile + auto-retry of failed indices).
Status note at top of §6 in PIPELINE_AND_ORCHESTRATOR_STATE.md.
