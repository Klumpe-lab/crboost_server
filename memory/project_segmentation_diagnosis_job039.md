---
name: project-segmentation-diagnosis-job039
description: "2026-10-22 diagnosis of MyrGFP_Grid2 Segment job039 (10 Å, TTA 8, Z smoothing) — what drives quality; easymode nets WERE trained on n2n + iso (IsoNet2) denoised inputs, repo says otherwise"
metadata:
  node_type: memory
  type: project
  originSessionId: 8494ebd3-9ea7-42f1-b92f-da6668077b4f
  modified: 2026-10-22T11:07:18.661Z
---

Brainstorm only (nothing built); maintainer was offered roadmap 39 for it.

Findings on job039 (48 tomos, 1024×1024×512 at 6.2 Å, lamellae; renders + Node MRC scripts were in that session's scratchpad):
- Tilt coverage gates everything: P10_2 kept 10/33 tilts (−36…−9°, tilt filter), P3/P3_2/P3_3/P3_4 22–24/41 one-sided → streaks, void ≈ everywhere.
- Lamella 7 (P7, P7_2–5): full tilts, normal defocus/thickness, but void 96–99.6 %. easymode's void = outside the sample OR poor/thick/badly reconstructed data; its docs use mean void per tomogram as a quality score.
- "Fades at edges": (a) inference rim — cytoplasm p crosses 0.5 ~25 nm in, ≥ 0.9 only 40–60 nm in (Ais 2.5D runs the whole plane at once, mirror-padded); (b) display — wall overlay is p ≥ 0.5 in a flat 6 nm mid-depth slab, lamellae are 70–200 nm thick and tilted 3–7° (P1 ~75 nm, mid-plane moves ~70 nm across Y), so the slab leaves the lamella.
- cytoplasm/nucleus/void are independent sigmoids: mean-p sum > 110 % in 11/47 (P9_4 141 %), < 90 % in 5.
- Z bands gone where confident (P8_2, P9), remain where not (P6_4, P9_4).

**easymode training flavours (docs general_denoisers.md, written Jun/Aug 2026, before our Sep 2026 weights):** "Both n2n and iso type tomograms were used for training of all segmentation models"; their pretrained denoisers at inference "tend to improve segmentation". iso = distilled IsoNet2 (iso_direct.h5 is in our cache), run on RAW Warp 10 Å recons; prefiltered input (low-pass, deconv, prior denoising) is out of distribution. Wedge-filling denoisers occasionally leave locally blurred patches.
**Why:** the tool survey §2.1, the Segment job's IsoNet input warning (ui/job_plugins/segment.py `_render_input_note`) and roadmap 38 "Not here" claim the opposite (no wedge-corrected training data).
**How to apply:** treat easymode n2n and iso as in-distribution Segment inputs (A/B raw vs n2n vs iso on P1, P8_2, P9, P9_4, P6_4, P7); our own IsoNet job (isonet2-n2n, CTF_mode network, mw_weight 200) is a cousin, not the same flavour. Related [[project-segmentation-quality-roadmap38]], [[project-segmentations-roadmap37]].
