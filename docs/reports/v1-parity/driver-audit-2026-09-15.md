# Driver logic vs CryoBoost v1 — audit 2026-09-15

Read-only comparison of every pipeline driver against the original CryoBoost implementation (v1, the
scientific ground truth), done before the first external share. Scope: *logic* — tool subcommands, flags
with semantic weight, input selection, pre/post-processing. Default parameter values and architecture
(per-tilt-series SLURM arrays, containers, the registry, status files) were out of scope.

**Status: recorded, not acted on.** The copia protocol runs end to end with the current logic; none of these
has been changed. A fix to any of them is a behaviour change and belongs in its own commit.

Paths: `v1:` = the CryoBoost v1 repository; unprefixed = this repository. Line numbers as of this date.

## Large differences

| Job | Difference | Likely effect |
|---|---|---|
| Template matching | `--tomogram-ctf-model phase-flip` is no longer passed (v1 `src/templateMatching/pytomTm.py:52`; `drivers/template_match_pytom.py` ~211–222) | template CTF does not match the phase-flipped Warp tomograms; lower LCC |
| Template matching | per-tilt `.tlt` written from `rlnTomoNominalStageTiltAngle` (`drivers/template_match_pytom.py:115`); v1 used `rlnTomoYTilt`, the aligned and sign-flipped angle (v1 `pytomTm.py:17-18`, nominal commented out) | wedge mirrored and alignment offset lost on asymmetric tilt ranges |
| Candidate extraction | `--particle-diameter int(D/2/apix)*apix` (`drivers/extract_candidates_pytom.py:107`) passes the radius in Å where pytom expects the diameter; v1 passed `-r D/2/apix` in pixels (v1 `pytomExtractCandidates.py:33-35`) | peak-exclusion radius ≈ D/4 instead of D/2; more peaks per particle |
| cryoCARE training | `epochs: 10`, `learning_rate: 1e-5`, normalization samples ≤ 2000 hard-coded (`drivers/denoise_train.py` ~406–427); v1 ran RELION's cryoCARE wrapper (`v1:config/Schemes/warp_tomo_prep/denoisetrain/job.star`), i.e. cryoCARE's own defaults (example config: 100 epochs, 4e-4) | under-trained model, output close to the half-map average |
| Tilt filter | the job is interactive and never submitted (`pipeline_orchestrator_service.py` ~163–167); v1's scheme always ran the DL filter between motion/CTF and alignment. v1's rule-based filter (defocus / drift windows, v1 `src/rw/librw.py:1390-1432`) has no equivalent | an unattended run removes no tilts |

Also in the tilt filter: the uncertainty threshold defaults to 0.1 (v1 scheme: 0.70), which never triggers on a
two-class softmax; non-square (K3) frames are Fourier-cropped straight to 384² (v1: two square crops, a tilt is
good only if both are); the DL model differs (v1: fastai `model.pkl`, ImageNet normalization; now: a
one-channel CNN `.pth`); re-running DL re-applies the stored labels, so a new threshold/model does not change
verdicts; v1's out-of-distribution check and on-failure auto-pass are gone.

## Moderate differences

| Job | Difference | Likely effect |
|---|---|---|
| Import | dose per tilt = raw `ExposureDose`; v1 used `ExposureDose × 1.5` (v1 `librw.py:408`) | ~33 % lower dose → weaker exposure weighting |
| Import | `tilt_series.star` written inline from the registry instead of `relion_import_tomo`; pre-exposure is a running sum in ZValue order (`services/tilt_series/build.py`) | wrong pre-exposure if ZValue order ≠ acquisition order |
| fsMotionAndCtf | EER group size fixed at `eer_fractions_per_frame or 32` (`services/jobs/_base.py:357`); v1 derived it from the EER header and target dose per frame (v1 `src/misc/eerSampling.py`) | group size independent of dose |
| tsAlignment (AreTomo) | `--axis_batch min(axis_batch, 1)` per tilt series (`drivers/ts_alignment.py:84`); v1 refined one consensus axis over `min(batch, nrTomo)` series (v1 `tsAlignment.py:87-93`) | no cross-series tilt-axis consensus |
| Denoise predict | overwrites `rlnTomoReconstructedTomogram` with the denoised map and drops the half-map columns (`drivers/denoise_predict.py` ~130–141); template matching prefers that output. v1 kept a separate `rlnTomoReconstructedTomogramDenoised` column and TM defaulted to the uncorrected map | TM runs on denoised volumes by default |
| Template matching | `defocus_weight` / `dose_weight` parameters are ignored — `--defocus` and `--dose-accumulation` always passed (v1 toggled both, `pytomTm.py:88-91`) | UI switches have no effect |
| Template matching | tilts Warp dropped are not removed from the tilt/defocus/dose files (v1 dropped NaN rows, `librw.py:1317-1318`) | over-filled wedge |
| Subtomo extraction | `--bin int(params.binning)` (`drivers/subtomo_extraction.py:460`, also `drivers/extract_pick_list.py:58`); v1 passed a float | binning 1.5 runs as 1 |
| Subtomo merge | merged particles.star always sets `rlnTomoSubTomosAre2DStacks=1` (`services/subtomo_merge.py:241`) | 3D subtomograms mislabelled when 2D stacks are off |
| tsCtf | settings/tomostar copied only if absent (`drivers/ts_ctf.py` ~238–246); v1 copied fresh every run | a same-directory re-run after re-alignment fits the old tilt set |

## Same approach as v1

Import star columns and conventions (YTilt = −TILT, ZRot = ROT, shift × rescale_angpix), fsMotionAndCtf
command and star overlay, IMOD patch alignment (`ts_etomo_patches`, identical xf/tlt parsing), tsCtf, tsReconstruct
(half maps, `--dont_invert`, output columns), template/mask preparation (relion_image_handler rescale / box /
low-pass, relion_mask_create at mean + 1.85σ), candidate-extraction cutoff methods and `--relion5-compat` output,
subtomogram extraction tool and input.

New jobs with no v1 counterpart: particle reconstruction, Class3D, miss-alignment, de-novo/manual pick-list
extraction.
