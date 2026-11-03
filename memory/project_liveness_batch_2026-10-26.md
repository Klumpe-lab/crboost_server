---
name: project_liveness_batch_2026-10-26
description: "Roadmap 32 §14 liveness BUILT 2026-10-26 on uv_and_UI (~/dev/crboost_server_liveness), NOT RUN: §14.7 (L4a-c, L1 owner file, 27's 2a) + §14.8 (L3 via owner file, L6 registry load_problem, L5 is_live); L2 skipped; handed as 10 patches"
metadata:
  type: project
---

Built 2026-10-26 in the worktree `~/dev/crboost_server_liveness`, branch `uv_and_UI` (the same branch is to carry
roadmap 39's uv work and the 32 §15 UI later, only when the user asks). NOT RUN: ruff + reading only. Handed as ten
`git apply --cached` patches in that worktree's `tmpOut/liveness_patches/` (1-L4a, 2-L4b, 3-L4c, 4-L1, 5-2a, 6-docs,
7-L3, 8-L6, 9-L5, 10-docs) because files are shared between commits. Records: roadmap 32 §14.7 and §14.8.

Non-obvious points:
- L4a uses `SlurmService.live_task_counts` (`squeue -r`, rows outside `SLURM_ENDED_STATES`); only "Invalid job id
  specified" means gone (also in query_jobs_by_ids).
- L1: owner file written whole then os.link-ed; takeover renames it aside first; start_supervisor turns SIGTERM
  into sys.exit so atexit releases. A new non-array driver must call `drivers/driver_base.start_supervisor()` first.
- L3 built WITHOUT a qsub.sh change: reconcile pass 1 ignores a FAILURE marker while `.supervisor.owner` names the
  recorded supervisor (`_owner_holds`); non-array sbatch scripts get `--open-mode=append`, and a reused dir's
  run.out/run.err are deleted at submit.
- L6: registry `load_problem` + `raise_if_unreadable()`; project open no longer rebuilds from mdocs over an
  unreadable registry (an all-unreadable registry read as "absent" and the forced rebuild would wipe outputs).
  load_excluded_ts now raises instead of "nothing excluded".
- L5: `JobStatus.is_live`; roster on open = `ProjectState.roster()` (pipeline_order); much of L5 (frozen queued
  form, in_use_notice delete refusal) predated the batch.
- L2 deliberately not built (after L1 no second supervisor races the rebuilds).
- Open: no in-app cancel for a Failed job's live array (L4a can refuse indefinitely); §14.3.1 re-extract decision.

**2026-10-27:** the user committed 1–6 from an EARLIER cut of the patches (before the review fixes), so
9-L5 failed and the fixes were never committed; 7, 8, 10 landed (tip 191eb56). Re-cut against HEAD as
11-review-fixes + 12-L5 (9-L5 is dead). §14.3.1 decided "cancel, then wipe" (user, against my "refuse") → built
as §14.9, with Run's refusal offering the scancel (`still_live`) and the R4 fix (`JobSpec.log_format`): patches
13–16, NOT RUN. Lesson: before handing a patch series, check the reflog for commits made from an earlier cut.

**How to apply:** before building more there (§15 UI, roadmap 39, S4 Fork), confirm from the worktree reflog
(`~/dev/crboost_server/.git/worktrees/crboost_server_liveness/logs/HEAD`) that the patches were committed and ask
for the §14.7/§14.8 runtime-check results. Related: [[project_job_dir_ownership_and_ui_latency]],
[[project_branching_pipeline_survey]], [[reference_supervisor_timeout_arrays_survive]], [[feedback_never_commit]],
[[reference_worktree_per_session]], [[reference_registry_schema_cross_branch]].

**2026-10-27 (later):** everything is on main and pushed: uv_and_UI (liveness §14.7–14.9, roadmap 39 S0–S2, clean_env)
merged with roadmaps (the main checkout's UI work + the main.py → `crboost` launcher rename) as bcdc90d, then
`git fetch . uv_and_UI:main` + push. Lab install rollout (roadmap 39 "Rollout") still the maintainer's next step;
the main checkout (branch roadmaps) still has the old module venv.
