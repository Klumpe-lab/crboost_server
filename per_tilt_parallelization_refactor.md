● Summary: Per-Tilt-Series Parallelization Refactor

  The Problem

  CryoBoost dispatched one monolithic SLURM job per pipeline step. With datasets growing from 1 to 18+ tilt series, WarpTools processes
   them sequentially inside a single job, hitting walltime and memory limits. At 100+ TS this becomes unusable.

  The Solution

  Every WarpTools job type now uses a supervisor/array-task pattern: a lightweight supervisor job reads the tilt series manifest,
  submits a SLURM array (one task per TS), polls for completion, then aggregates metadata. Each array task processes exactly one tilt
  series in an isolated staging directory.

  What Was Built

  Core infrastructure (drivers/array_job_base.py — new):
  - Manifest read/write (ts_names, items, per-TS metadata)
  - Atomic status files (.task_status/{ts_name}.ok|fail)
  - Per-TS staging (stage_per_ts_environment — copies settings + tomostar with absolute paths, symlinks single XML)
  - SLURM array dispatch (build sbatch from template, inject --array=0-N%throttle, strip RELION markers from child tasks)
  - Signal handling (SIGTERM → scancel the array)
  - Result collection (ArrayResults dataclass)

  New job type — tsImport (JobType.TS_IMPORT):
  - Separated from alignment driver — runs WarpTools ts_import + create_settings
  - Lightweight (no GPU, no array) — purely metadata assembly
  - Produces tomostar/ dir + warp_tiltseries.settings consumed by alignment/ctf/reconstruct
  - Auto-added as prerequisite when alignment is added to the pipeline

  Converted drivers (all now supervisor/array):

  ┌──────────────────────┬────────────────────────────┬─────────────────────────────────────────┬──────────────────────────────────┐
  │        Driver        │    Supervisor pre-step     │               Per-TS task               │           Aggregation            │
  ├──────────────────────┼────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────┤
  │ fs_motion_and_ctf.py │ Parse import STAR → group  │ Stage TS's frames (symlinked), run      │ Merge per-frame XMLs into output │
  │                      │ frames by TS               │ create_settings + fs_motion_and_ctf     │  STAR                            │
  ├──────────────────────┼────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────┤
  │ ts_alignment.py      │ Copy tomostars + settings  │ Stage single-TS env, run                │ update_ts_alignment_metadata()   │
  │                      │ into job dir               │ ts_aretomo/ts_etomo_patches             │                                  │
  ├──────────────────────┼────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────┤
  │ ts_ctf.py            │ Copy XMLs + run            │ Stage single-TS env, run ts_ctf         │ update_ts_ctf_metadata()         │
  │                      │ ts_defocus_hand globally   │                                         │                                  │
  ├──────────────────────┼────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────────┤
  │                      │ Refactored to use          │                                         │                                  │
  │ ts_reconstruct.py    │ array_job_base (behavior   │ Same as before                          │ Same as before                   │
  │                      │ unchanged)                 │                                         │                                  │
  └──────────────────────┴────────────────────────────┴─────────────────────────────────────────┴──────────────────────────────────┘

  Config/model changes:
  - SupervisorSlurmConfig generalized from TsReconstructSupervisorSlurmConfig (backward-compat alias kept, conf.yaml key auto-migrated)
  - array_throttle field added to all four job param classes
  - All array jobs override _get_queue_options() to use lightweight supervisor resources
  - TOMOSTAR_DIR added to JobFileType; IO slot wiring updated

  UI/pipeline wiring:
  - tsImport in PIPELINE_ORDER between fsMotionAndCtf and alignment
  - Auto-prerequisite logic in pipeline_builder_panel.py — adding alignment auto-adds tsImport
  - Path resolver updated — alignment/ctf/reconstruct get tomostar_dir from IO slots, not hardcoded project root
  - Tasks tab registered for all array job types (reuses existing array_task_tracker component)
  - TS names shortened in tracker display (strips common project prefix → shows Position_11_2 not agg5_20251113_412_Position_11_2)
  - Status fix: jobs with a task manifest show "Running" even if supervisor SLURM state is "Pending"

  Key Files Changed/Created

  NEW:
    drivers/array_job_base.py          — reusable array infrastructure
    drivers/ts_import.py               — tsImport driver
    services/jobs/ts_import.py         — TsImportParams model
    ui/job_plugins/array_tasks.py      — Tasks tab for fs/align/ctf

  REWRITTEN:
    drivers/fs_motion_and_ctf.py       — supervisor/array
    drivers/ts_alignment.py            — supervisor/array
    drivers/ts_ctf.py                  — supervisor/array
    drivers/ts_reconstruct.py          — uses array_job_base

  MODIFIED:
    services/models_base.py            — JobType.TS_IMPORT
    services/io_slots.py               — TOMOSTAR_DIR
    services/jobs/{fs_motion_ctf,ts_alignment,ts_ctf,ts_reconstruct}.py
    services/configs/config_service.py — SupervisorSlurmConfig
    services/path_resolution_service.py
    services/scheduling_and_orchestration/{pipeline_orchestrator,pipeline_runner}.py
    ui/pipeline_builder/pipeline_builder_panel.py
    ui/components/array_task_tracker.py
    ui/ui_state.py
    config/conf.template.yaml

  What's Next (your plan)

  - UI for browsing individual tilt series journeys across the full pipeline (TS identity → fs_motion status → alignment status → ctf →
   reconstruct)
  - fsMotionAndCtf per-TS splitting is live but needs production testing with multi-TS datasets
  - Downstream jobs (denoise, template match) can follow the same pattern when needed
