---
name: project_agg412_missalign_rollout
description: "missAlign PAPER rollout to the 7 other AGG412 projects — record in roadmap 29 §6.2; trio done 2026-10-21 (815 tomos); TM on refined tomograms specified in §6.3"
metadata:
  node_type: memory
  type: project
  originSessionId: 9fbf128b-7fa2-43ef-b012-866c3b273ea5
  modified: 2026-10-19T14:45:46.630Z
---

Started 2026-10-18 after the Grid2 A/B (+20 % picks, +8 % top-50 LCC). The campaign record is
`docs/roadmaps/roadmap_29-agg412-sta.md` §6.2: decisions MA1–MA18 (MA10–MA18 proposed, awaiting the
maintainer), full parameter tables for missAlign / tsCtf__2 / tsReconstruct__2, per-project table, run log.
Append decisions and run-log lines there, never overwrite.

Grid2 job022 measured: training 19 h 36 (2.74–3.14 steps/s, early stop at epoch 21–26), alignment 75 s per TS
over the 9-iteration PAPER schedule, 2 min staging. 819 TS to refine in the other seven.

**Trap:** the pilot (`AGG412_20260204_412`) and `AGG412_20260205_412_FCIso_unmilled2` have
`import_selected_tilt_series` = 0, so the missAlign walltime estimate silently falls back to the profile's 2:00:00
(one iteration needs ~2.3 h → endless restarts). Time must be set by hand (proposed 3-00:00:00 everywhere).
Lab install (pulled 2026-10-14) lacks the ts_reconstruct `--task_dir` fix → submit from the dev server.
Related: [[project_warp_export_job]], [[reference-missalign-throughput]], [[project_agg412_reprocessing_2026-10-04]].

**2026-10-19:** 6 of 7 runs + both Grid2 Refine 3D jobs (036/037) ended 14:19–14:24 on g3/g4 nodes: sacct
`FAILED 1:0`, yet nothing in run.out/run.err and no exit marker → the driver exited 1 while /groups writes failed;
likely a shared-filesystem drop (not crboost, not the tools). Checkpoints: SK/1113/Slot6/unmilled2 iter1–3,
Grid3/pilot iter1–5 → Run in place resumes. FCIsolation (job016, SLURM 3527088, clip-g2-3): a patch builder died
at 00:09 on Position_32_4 (3 tilts; the tool's shift generator needs ≥ 4), training hung — cancel before Run.
Maintainer then chose MA19 (roadmap 29): `MIN_TILTS = 10` in drivers/miss_align.py (BUILT 2026-10-19, not run);
a resumed run keeps its staged series (unmilled2 Position_32_3 has 5 tilts and is in its checkpointed run).
FCIsolation's fresh run will leave out 17_2 (4), 29_5 (4), 30_3 (9), 32_4 (3). Run log + MA19 appended in §6.2.

**2026-10-21/22:** all seven missAlign runs + tsCtf__2 + tsReconstruct__2 done (815 refined tomograms, every task ok,
Grid2's parameters incl. halfmap_frames 1). The 2026-10-05 picking jobs are gone from the seven rosters and disks.
Next = template matching on tsReconstruct__2, both species: spec + steps in roadmap 29 §6.3 (ribosome = Grid2 job026,
all code defaults, NO band-pass; 412 = playbook §5). Each TM input slot ties (baseline vs `__2`) → pin both in the IO
tab. Supervisor 8 h (W19) will time out in most projects at throttle 4 → Run again only after the array drains.
