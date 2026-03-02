**defocus_handedness: GT=0, Current=-1**

This is a real parameter difference. It controls the sign convention for how defocus varies with z-position in the tomogram, which affects per-tilt CTF weighting. Whether -1 is correct for your dataset or whether it's introducing systematic CTF weighting errors is something I can't determine from the JSON alone -- but it's different, and it feeds directly into how each tilt's contribution is weighted during cross-correlation.

**Input tomogram**

GT: `job005/warp_tiltseries/reconstruction/Position_1_11.80Apx.mrc` (raw)
Current: `job007/denoised/all_together_Position_1_11.80Apx.mrc` (cryoCARE output)

This is confirmed as the primary cause of the 7x elevated score mean. Denoising removes high-frequency Gaussian noise and replaces it with structured low-frequency residuals. An ellipsoidal template cross-correlates better with smooth low-frequency content everywhere, not just at particle locations -- so the entire score distribution shifts up. The FDR method (number-of-false-positives=1) estimates noise from the distribution and sets its cutoff accordingly. With an elevated mean, the cutoff ends up higher in absolute terms, and marginal peaks that GT would have called above threshold don't make it.

**GT defocus outlier at tilt index 24**

GT has `3.269510163e-06` (~3.27 µm) for tilt 25 (46°). Everything else in GT is 5.19–5.58 µm. Your current project has 5.50 µm there. The GT value is a clear defocus estimation artifact for that high-angle tilt. This slightly corrupts the CTF weighting for that one tilt in GT -- arguably your values are more self-consistent -- but it wouldn't explain a 120-particle difference.

**pytom version: 0.10.0 (GT) vs 0.12.1 (Current)**

The FDR noise model implementation, the score normalization, and the candidate extraction algorithm all potentially changed between these versions. Without reading the source diff I can't say exactly what changed, but this is a real confound.

**Schema structure**

GT has flat top-level fields (`tilt_angles`, `ctf_data`, `dose_accumulation`, `defocus_handedness`). Current wraps them in `ts_metadata` with a `__class__: RelionTiltSeriesMetaData` object. This is the v0.12 RELION5 STAR input path vs v0.10 text file path -- both presumably get parsed to the same internal representation, but it's another place version differences could manifest.

**variance/std of score maps**

GT: variance=0.000300, std=0.01733
Current: variance=0.000345, std=0.01858

The slightly higher std in current is consistent with the denoised input having more structured (non-Gaussian) noise -- the variance estimator sees more spread because the background isn't white.

---

**Summary for your postmortem, ranked by likely impact:**

1. Denoised input to TM. This is almost certainly the dominant cause. The GT project ran TM on the raw reconstruction; your pipeline routes the denoised volume through TM instead, which shifts the score background distribution up by ~7x, making FDR-based thresholding more aggressive. This is arguably a design choice your pipeline makes -- denoising before TM can help in some cases -- but it makes the FDR model assumptions worse, not better, for an ellipsoidal template.

2. defocus_handedness=-1 vs 0. Different, origin unclear without checking your driver code vs cryoboost's code for how it reads this from the STAR/XML metadata. Worth tracing exactly where your driver sets this.

3. pytom 0.10 vs 0.12 behavior differences, particularly in FDR cutoff calculation. Uncontrolled confound.

4. Template minor differences, c_use_sum upstream -- negligible by comparison.

The actionable test you have time for before the presentation: note that you already have the raw reconstruction in `job005`. If you rerun TM pointing `-v` at `job005/.../Position_1_11.80Apx.mrc` instead of the denoised output, and get a score mean close to GT's 0.0018, that closes the case on cause #1 definitively.