---
name: project_v1_driver_parity_audit_2026-09-15
description: "Pre-Munich-share audit of every driver vs CryoBoost v1 (ground truth) — REPORTED ONLY, nothing edited; drastic items = TM ctf-model phase-flip dropped, TM nominal vs YTilt angles, extract-candidates radius passed as diameter, cryoCARE train hard-coded 10 epochs/lr 1e-5, tilt filter never auto-runs + rule filter gone"
metadata: 
  node_type: memory
  type: project
  originSessionId: 07578eac-4c63-45f0-92fc-11954ec4cd3d
  modified: 2026-09-15T08:23:48.821Z
---

2026-09-15, before the first public share (Munich). User asked: logic must be in the same line/spirit as
v1 (`/users/artem.kushner/dev/CryoBoost`); defaults don't matter; "if drastically different, just tell me,
don't edit anything yet". Four parallel read-only agents; the drastic items below I re-read in both repos.

**Drastic (code-verified):**
- TM: `--tomogram-ctf-model phase-flip` gone (v1 `src/templateMatching/pytomTm.py:52` → `drivers/template_match_pytom.py` ~211-222).
- TM: `.tlt` from `rlnTomoNominalStageTiltAngle` (`template_match_pytom.py:115`) vs v1 `rlnTomoYTilt` (aligned, sign-flipped; nominal commented out in v1 :17). Possible 412 chiral-pick contributor — see [[project_412_divergence_cascade]].
- Candidate extraction: `--particle-diameter int(D/2/apix)*apix` = radius in Å passed as diameter (`drivers/extract_candidates_pytom.py:107`); v1 `-r D/2/apix` px. Exclusion ≈ D/4.
- cryoCARE train: `drivers/denoise_train.py:~406-427` hard-codes epochs 10, lr 1e-5, norm samples ≤2000 (v1 = RELION `relion.denoisetomo` wrapper → cryoCARE defaults ~100 epochs / 4e-4).
- Tilt filter: IS_INTERACTIVE → never dispatched; unattended run drops no tilts (v1 scheme always ran filtertilts DL). v1 rule filter (`librw.py:1390-1432`, defocus/drift windows) has no equivalent. prob_threshold default 0.1 (v1 0.70) never fires on 2-class softmax. Non-square (K3) frames Fourier-cropped to 384² (v1: two square crops, both must be good).

**Moderate:** dose/tilt lost v1's `ExposureDose*1.5` (`librw.py:408`); EER group fixed `or 32` (`jobs/_base.py:357`, v1 derived from dose); AreTomo `--axis_batch min(batch,1)` per-TS (no cross-TS axis consensus); denoise_predict overwrites `rlnTomoReconstructedTomogram` → TM defaults to denoised maps (v1 default uncorrected, user-selectable column); TM `defocus_weight`/`dose_weight` params ignored; TM tilt files keep tilts Warp dropped (v1 dropna); subtomo `--bin int(binning)` truncates; merged particles.star always `rlnTomoSubTomosAre2DStacks=1`.

Same spirit: import stars, fsMotionAndCtf, IMOD patch alignment, tsCtf, tsReconstruct, template/mask prep.

Written up as `docs/reports/v1-parity/driver-audit-2026-09-15.md`. User 2026-09-15: "the protocol test works for me right now, so i'm not going to touch this for now."

**2026-10-04: TM items FIXED (uncommitted)** — `.tlt` now from `rlnTomoYTilt`, rows with NaN YTilt (tilts not in the reconstruction; == Warp UseTilt set on all 927 stars checked) dropped from tlt/defocus/dose, `--tomogram-ctf-model phase-flip` added. Non-legacy relion5-star path untouched (PyTOM reads nominal there). See [[project_agg412_reprocessing_2026-10-04]].

**2026-10-05: extraction radius FIXED (uncommitted)** — `--particle-diameter` gets the full diameter (PyTOM 0.10.0 halves it; verified on May picks: min NN 149 Å = 24 px).

**How to apply:** these are the user's to decide; don't fix unprompted. When they pick one up, the fix is a
quarantined behavior-change commit. Known older items (TomoHand, Warp ts_import filter, I1 angles, TM
symmetry) tracked in their own memories.
