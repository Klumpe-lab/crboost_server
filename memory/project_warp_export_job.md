---
name: project_warp_export_job
description: Warp Export job (roadmap 24 stage 4 = 21 stage 6) in clone ~/dev/crboost_server_warp_export (from species_driven_io); needs a Warp 2.0.0dev41 container; dev36 export bugs fixed upstream in 7ad2dcc
metadata:
  node_type: memory
  type: project
  originSessionId: f2ccfb83-54d7-4366-8c8a-33aa5f0cfec3
  modified: 2026-10-10T21:02:02.665Z
---

Started 2026-10-10 in the clone `~/dev/crboost_server_warp_export` (from `species_driven_io` @ ef4de33; the user names
the branch). Gitignored setup done there: conf.yaml + qsub.sh copied, venv + tilt-filter weights symlinked,
.claude/settings.local.json copied; memory dir symlinked for sessions started in the clone.

**Why:** missAlign's `[3,3]` rounds write GridMovement (local image warps) into the Warp XMLs; RELION extraction
only reads rigid per-tilt geometry, so the gain only reaches particles through `WarpTools ts_export_particles --2d`.
Maintainer order (2026-10-10): build this, then PAPER on all eight AGG412 projects; picks in refined tomograms come
from a TM re-run ([[feedback-no-pick-rehoming]]).

**Facts (read from Warp dev36/dev41 + RELION 5.0.1 sources, 2026-10-10; nothing run):**
- Warp dev36 2D export is wrong for RELION 5: stacks hold every tilt while RELION pairs the n-th visible tilt with the
  n-th slice (silent mispairing); coordinates in output px vs unbinned sizes (depth defocus scale); rlnTomoHand
  hard-coded -1. Fixed upstream in warpem/warp 7ad2dcca (2026-07-22), first in dev40; dev40 also adds
  `--dont_premultiply`. → new container `container_defs/warp_2.0.0dev41_cuda12.9.def` (CUDA 12.9), export job only.
- `relion_tomo_reconstruct_particle` never reads particle stacks (re-extracts from the tilt series; Warp points it at
  a 2x2x2 dummy → empty map). Only `relion_refine --ios` uses Warp particle series. Refine uses per-tilt rotation +
  CTF (+ depth term via rlnTomoHand) + dose; per-tilt shifts are ignored for 2D stacks.
- Hand: crboost writes rlnTomoHand -1 for AreAnglesInverted=True (`services/tilt_series/adapters/ts_ctf.py:266`);
  Warp dev40+ writes +1 for the same flag (with a unit test). Unresolved; also affects the RELION path's depth defocus.
- Warp input: rlnCoordinateX/Y/Z (from the volume corner, × --coords_angpix), particles matched by exact
  rlnMicrographName/rlnTomoName == `<ts>.tomostar`; no centred-coordinate support. ts_export_particles re-saves XMLs
  (and marks failed TS LeaveOut) → stage XML copies, never symlinks. `--max_missing_tilts` default 5 drops particles.

Design defaults proposed to the user: `--dont_premultiply`; XMLs found from each particle's tomogram row
(rlnTomoTiltSeriesStarFile → that tsCtf job), no extra IO slots; output = RELION 5 native set built from tsCtf's
per-TS stars (real rigid geometry + micrographs, crboost's hand) so reconstructParticle works rigidly and
Class3D/Refine3D use the Warp stacks. Issue text for the repo: `q_warp_dev36_export_issue.md` in the clone.

**2026-10-11: built in the clone, NOT RUN.** New: services/jobs/warp_export.py, services/particles/warp_export.py
(pure converters), drivers/warp_export.py (ArrayDriver), ui/job_plugins/warp_export.py; spec row after
SUBTOMO_EXTRACTION; tools.warp_export + job_resource_profiles.warpExport; roadmaps 21/24 premise corrected.
Output uses rlnCenteredCoordinate*Angst (RELION 5 prefers them), tsCtf per-tilt rows reordered into tomostar order,
crboost hand. Def needed `conda config --system --set channel_priority flexible` + `CONDA_OVERRIDE_CUDA=12.9`
(conda 26, GPU-less build host). Gaps: protocols/apply.py still needs subtomoExtraction before reconstructParticle.

**2026-10-11 later: one Warp for everything.** User rejected the separate `warp_export` tool/image. The job now uses
`tools.warp_aretomo`; new def `container_defs/warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def` (CUDA 12.9 devel base,
Miniforge Warp dev41 + AreTomo2 v1.0.0 from source) replaces the dev36 image for every Warp job. CLI of every
WarpTools command crboost calls is unchanged dev36→main; dev40 swaps REST workers for a `tasks/` queue dir under the
output dir, dev41 makes failures exit nonzero. The interim Warp-only sif `warp_2.0.0dev41_cuda12.9.sif` is obsolete.

**2026-10-12: first run PASSED its plumbing.** AGG412_20260311_MyrGFP_Grid2, ribosome: Warp Export job016 (bin 4 →
6.2 Å, box 80, Ø 310) 47/47 TS ok in 6 min, 18,574 particles (candidates 18,579; RELION extraction job015 18,452);
all built-in checks passed; Class 3D job018 read the set + Reconstruct Particle job017 reference (40 min) without
errors. The user cancelled Class 3D in iteration 1 to start missAlign. Not yet shown: GridMovement reaching the
export (baseline alignment has none), particle correctness (map), hand sign. Next: missAlign trio
(missAlign → tsCtf__2 → tsReconstruct__2) then TM → candidates → Warp Export on it; Refine 3D = [[project-refine3d-job-roadmap31]].

**2026-10-14: missAlign PAPER run (job022) crashed on Position_10_4** — a 1-tilt stack in job007's star (31 of 32
frames lost upstream). Driver now leaves out excluded and <2-tilt series (logged, `left_out` in
missalign_changes.json) instead of failing. Rerun started. The warp_export clone is fully merged into the main tree's
species_driven_io (tip faae068) and is being deleted; all further work is in ~/dev/crboost_server.
2026-10-14 17:44: rerun job022 on clip-g4-7 (4× A100, 3 train + 1 recon, g_long, walltime 4-1:23) runs at
6.65 s/step, 10000 steps/iteration → ~19 h/iteration, ~7 d for PAPER; the watchdog (0.9× walltime) will kill it
mid-iteration 5. Resume keeps iter1–4 (GPU layout is not in the resume guard). Suspect the single reconstruction
worker; unconfirmed (asked the user for nvidia-smi). Lab conf.yaml updated the same day (dev41 sif, warpExport, refine3d).

**2026-10-16: missAlign PAPER finished OK** (job022, 11:09 → 07:48 next day ≈ 20.7 h, 9 iterations, early stop at
epochs 18–26; 48/48 refined; local warps 3×3 ~8–44 Å rms; outliers Position_6 122 Å rms residual, 9_2 70 Å with a
193 Å rigid shift, 1_5 62 Å). tsCtf__2 job023 OK. **tsReconstruct__2 job024: 8+ series failed** — dev41 WarpTools
keeps its work queue in `<output_processing>/tasks/` and one manager locks it; concurrent array tasks sharing the
output dir collided ("Another manager is already using queue dir", or 45-min idle hangs). Fixed: ts_reconstruct
passes `--task_dir <.staging/task_X>/tasks`. ts_ctf / ts_alignment / fs_motion stage a per-task output dir, so are
unaffected. Affects every dev41 install (lab too) until pulled.

**2026-10-17 A/B (Grid2, ribosome, bin 2 export, box 192):** A = job009 recon → TM job025 → cand job027 → export
job029 (26,335) → recon job031 → Refine job033; M = job024 (missAlign PAPER) → TM job026 → cand job028 → export job030
(33,252) → recon job032 → Refine job035 (job034 = empty orphan dir, same instance). Same template, FP=1 cutoff, score
noise std identical (ratio 1.000): M picks +20 % median per TS (43/47 up), top-50 LCC +8 % (45/47 up); gain grows with
how much missAlign moved the series (Spearman 0.39; <10 Å rms changed: +5 %, ≥10 Å: +13 %; Position_6 0.110→0.168).
First particle-level evidence that the PAPER run helps. Both Refine 3D OOM-killed at 96G with --preread_images on
(saved state kept preread on, diameter -1, seed -1); M5 used its own job032 reference, not A's. Equal-N subset of M
still needed for the STA verdict.

2026-10-18: the Journey's cutout gallery, subtomo pill and pixel chain now count Warp Export as an extraction job
(`PARTICLE_EXTRACTION_TYPES`), and pair each candidate-extract job with the extraction that read its optimisation
set (`subtomo_link.serves_candidate_job`, for two-branch species like AGG412 Grid2). Still SUBTOMO-only, not
changed: `aggregation/extraction.py` (per-list geometry, Species page auto counts), `analytics_store`, `set_ops`,
`aggregation/discovery`, and Warp Export has no `output_optimisation_filtered` slot, so a gallery keep/drop saved
into its dir is not offered downstream automatically. Not run.

**2026-10-18 Refine 3D rerun (both arms on A's reference job031, preread off):** job036 = A (job029, 26,335),
job037 = M (job030, 33,252), same flags/seed 42. After it001 (halves joined <40 Å) the gold-standard FSC at 23.8 Å
is A 0.54 vs M 0.82 (29.8 Å: 0.78 vs 0.89); est. accuracy for it002: A 2.08°/3.18 Å, M 1.67°/2.63 Å. The SSNR gap
(~4× at 23.8 Å) is far above what +26 % particles explains (~1.26×). M runs ~3.5 h/iteration vs A ~2.5 h.
Particle sets differ (M has its own picks), so an equal-N subset is still owed for the final verdict.
2026-10-19 01:25 update: after it002, FSC at 17 Å A 0.20 vs M 0.51 (20.5 Å: 0.49 vs 0.76); M's it002 beats A's it003
everywhere; est. accuracy for it003 A 1.41°/2.16 Å vs M 1.02°/1.62 Å. M's FSC is still cut off by the current image
size (0.33 at 15.3 Å). M iterations now ~4.7 h, A ~2.7 h.
2026-10-19: both refines died 14:19–14:21 in the same g3/g4 event as the missAlign runs (see
[[project_agg412_missalign_rollout]]). Last: A it009 (16.5 Å, near convergence, 1.875° local), M it004 (14.5 Å).
FSC 0.143 ≈ A-it004 13.8 Å, A-it009 ~11.9 Å, M-it004 ~10.3 Å. drivers/refine3d.py has no --continue, so a rerun
in place restarts from it000.
2026-10-19 later: sacct says FAILED 1:0 (likely a /groups drop). drivers/refine3d.py now CONTINUES a re-run from
the last finished iteration (BUILT, not run): `--continue run[_ctK]_itNNN_optimiser.star --o run_ctNNN` + only
--j/--pool/--gpu/--preread_images/--dont_combine_weights_via_disc; refuses if refinement options differ from the
command recorded in run_it000_optimiser.star; copies run_ctN_* results to run_*; writes run_ctN_optimisation_set.star
itself if relion_refine --continue doesn't (UNVERIFIED which happens). Run on Grid2 → job036 from it009, job037 from it004.
2026-10-20 12:35: the --continue re-runs work. A job036 (from it009 as run_ct9) CONVERGED 03:10: 16.09 Å
(relion_refine's unmasked half-map figure), acc 0.96°/1.56 Å, step 0.47°; 3 iter at 0.94°, 6 at 0.47°. M job037
(from it004 as run_ct4) in it014: 11.45 Å, acc 0.63°/1.07 Å, step 0.94° (4th iter there, still gaining at it013),
~2.1 h/iter vs A's ~0.8 h at the same step. Walltime 5-00 (g_long) from 10-19 16:49, no risk. No postprocess yet.
2026-10-20 final: job036 (baseline, 26,335) converged 03:10 → 11.23 Å; job037 (missAlign, 33,252) converged 20:51 →
8.27 Å (RELION "Final resolution (without masking)"). The missAlign chain = the workflow rolled out to the other seven
(roadmap 29 §6.3).
