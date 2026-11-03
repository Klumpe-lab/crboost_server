---
name: copia-ctf-bimodal-fits
description: Copia test set (28 tilts, 2.07 e/A2/tilt, true defocus 5.3 um on ALL tilts per TiltPS1D scan): Warp fs_ctf/ts_ctf 2D fits diverge on weak-ring tilts; defocus window is NOT the fix (4.0:6.5 gave −18..60 um); band 30:6 vs 30:10 and pre-fit handedness under test (ctf_fit_test.sbatch)
metadata:
  type: project
---

Found 2026-09-08 while running the copia tutorial STA chain ([[copia-tutorial-sta-chain]]). Per-tilt `_rlnDefocusU` in `External/job005/tilt_series/*.star` (Warp ts_ctf, window 512, range 30:6, defocus 1.1:8): the 6 first-acquired tilts (4–19°, ≤10 e/A2) and a few others fit 5.2–5.7 um with CTF resolution 6.1 A; 17 tilts fit 1.3–4.2 um at ~10 A CTF resolution (bimodal, wrong minimum). SerialEM readout is flat (−10.4 um ±0.5, ~0.9 um less at 40–49°), so the true defocus is ~5.3 um (~4.4 um at the highest tilts). WarpTools ts_defocus_hand --check gave correlation −0.035 (meaningless) for the same reason; the protocol forced set_flip anyway. RELION extraction bakes these defoci into the CTF-premultiplied 2D stacks → Refine3D plateaued at 17.4 A, 15.4 A by iteration 7. The v1 scheme used the same ranges, so why the tutorial got 8.5 A is unexplained (Warp dev36 vs v1's dev29/31?).

**Why:** low-dose tilts + a wide defocus window let Warp pick aliased minima; nothing in the pipeline flags per-tilt defocus outliers (violates the never-fail-silently rule).
**How to apply:** re-run tsCtf with `defocus_min_max: "4.0:6.5"` (then subtomoExtraction → reconstructParticle → Class3D → mask → Refine3D; TM/candidates can stay). Consider a tsCtf driver check: flag tilts whose defocus deviates > 1 um from the tilt-series median, and use `rlnTomoNominalDefocus` (mdoc) as a prior for the window. Never rely on `_rlnCtfMaxResolution`/`_rlnCtfFigureOfMerit` there — Warp writes placeholders (see [[warp-relion-star-placeholders]]).

2026-09-08: protocol edited in place (tsCtf.defocus_min_max + fsMotionAndCtf.c_defocus_min_max → "4.0:6.5", UNCOMMITTED) and `docs/known_bugs.md` started (entry 1 = this, entry 2 = healpix description off-by-one). User re-runs the whole pipeline from scratch in a NEW project; the 20260906-1319 project is kept as the broken baseline for comparison. Check the new project's per-tilt defocus first (must be 4.4–5.7 um on all 28 tilts).

2026-09-08 later: WINDOW IS NOT THE FIX. New project copia-empiar12580-20260908-1407 with 4.0:6.5 → ts_ctf per-tilt −18..+60 um, fs_ctf astigmatism 25–64 um (unbounded refinement after the bounded grid search). 1D scan of Warp's own TiltPS1D curves vs CTF² (awk, in session) gives 5.05–5.53 um on ALL 28 tilts, cc 0.5–0.9 in every band → data fine, Warp 2D fit diverges on weak-ring tilts. Candidates: fit band 30:6 (noise-dominated; WarpTools default range_high 4) and/or handedness set BEFORE ts_ctf (driver order = v1 = Warp docs; --check input = fs 2x2 grid = garbage here; ts_ctf --auto_hand exists). Experiment script <newproj>/tutorial_sta/ctf_fit_test.sbatch (ts_ctf hand×band, fs_ctf band 30-10/30-8), PENDING USER RUN. Protocol still carries 4.0:6.5 (to be revised after the experiment); docs/known_bugs.md entry 1 rewritten to this state.
Full log + decision tree: docs/reports/copia/copia-ctf-fit-investigation.md (repo, uncommitted). Next session: read its Handoff section first, then B/tutorial_sta/logs/ctf_fit_test.out.

RESOLVED 2026-09-08 14:45: cause = fsMotionAndCtf c_range_min_max 30:6 (fs_ctf diverges on 2 e/A2 tilts); 30:10 fits all 28 at 5.10–5.53 um. ts_ctf never searches (inherits fs defoci via ts_import, refines locally) → hand/band of ts_ctf irrelevant. Protocol edited: c_range_min_max + range_min_max "30:10", windows back "1.1:8" (UNCOMMITTED). Hand verdict still open → B/tutorial_sta/hand_check.sbatch. Next: project C from the fixed protocol, verify 28 defoci, STA chain.

VERIFIED 2026-09-08 ~15:00 in project C /groups/klumpe/crboost_data/copia-empiar12580-20260908-1447: 28 defoci 5.15–5.58 um, ctf res 6.1–7.6 A, hand check −0.994 = flip = tutorial. Pipeline running (tsReconstruct at 15:00). STA chain copied to C/tutorial_sta/ with paths rewritten; next = mask (01, look at threshold table on the NEW Class3D map) → Refine3D (02, MPI).

Cross-check 2026-09-08: the PI's v1 run /groups/klumpe/user/<pi>/Processing/Copia/try2 (DIFFERENT data: Position_11_2, 39 tilts, 1.55 A/px, 3.2 e/A2, Warp dev31) shows the SAME fs_ctf failure on its 7 last-acquired tilts (1.1–2.2 um), carried by ts_ctf; still 9.8 A (auto) / 8.8 A (manual cleaning). His RELION note.txt commands validated tutorial_sta flags; added --s_vel 0.2 (polish) and --min_frames 1 (extract) to C/tutorial_sta.

2026-09-30: the fix lives ONLY in the copia protocols. Code defaults are still `30:6.0`
(`services/jobs/fs_motion_ctf.py` c_range_min_max, `services/jobs/ts_ctf.py` range_min_max). `bullseye_artifact_two`
(no protocol, 412 data) ran 30:6 and shows the same failure: Position_28_4 10/37 tilts at 1.6–2.3 µm vs ~5.2,
13_2 top tilts at 4.1/1.5 µm; the missAlign branch's tsCtf (job008) carries identical values. The bullseyes are a
separate mechanism (blank-tilt rays, [[project_blank_tilt_rays]]); nothing links the CTF fix to them.

2026-09-30 later: code defaults changed to `30:10` (fsMotionAndCtf c_range_min_max, tsCtf range_min_max); windows stay
`1.1:8`. UNCOMMITTED. Only NEW job instances pick it up; existing projects keep 30:6 in project_params.json.

2026-09-30 A/B on 412 data: `bullseye_artifact_two` (30:6) vs `bullseye_artifact_two_ctfband` (30:10), same dose
2.98 e/A2, 1.55 A/px. Only Position_28_4 was really affected (10/37 in-TS tilts 1.6–2.3 um → all 5.07–5.45; fs median
CTF res 8.8 → 6.3 A; brightest series, mdoc mean ~1430 vs 550–1150 → likely weakest Thon rings). Other 6 TS changed
≤0.1 um. Tilts without signal stay wrong under BOTH bands (13_2 +33/+36° partially beam-blocked; 28_2 +57° at 31
counts survives into the TS) → roadmap 19/22 territory, not the band. Journey hides it: fsMotion tiles are median +
IQR; only the per-tilt scatter / tsCtf min–max tile show it; these projects stop at tsReconstruct (no STA payoff).
