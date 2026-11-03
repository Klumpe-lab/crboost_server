---
name: project-denoise-method-redesign
description: "IsoNet denoise fixes 2026-10-02 (method inheritance bypass, CTF_mode/mw_weight/cube/bfactor/multi-GPU, deconvolved-halves guard) NOT RUN; train→predict inheritance redesign still owed"
metadata:
  node_type: memory
  type: project
  originSessionId: 98168433-cd7c-4cba-a0c7-1b236c24a439
  modified: 2026-10-02T08:49:27.876Z
---

2026-10-02: IsoNet predict on a colleague's K3 project (<colleague>_k3_denoisedj, job009) ran cryoCARE_predict against the IsoNet model. Cause: `_apply_inherited_method` in drivers/denoise_predict.py assigned USER_PARAMS on a RUNNING job, and `AbstractJobParams.__setattr__` silently dropped the write. Interim fix: write through `BaseModel.__setattr__`. RUN OK (job011).

The user wants the automatic train→predict method inheritance ("magic") replaced, and the cryoCARE/IsoNet choice made prominent instead of buried in the parameters. Still owed. Scope it first ([[feedback-scope-design-talk-into-roadmap]]).

Why job011 looked bad (half-map FSC + spectra QC; scripts were in the session scratchpad). IsoNet acted as a ~100 Å low-pass and filled only 1–2 % of the missing wedge. Three causes:
1. Refine passed only --epochs 20. IsoNet2's README wants ≥50 epochs, `--mw_weight` 20–200, `--CTF_mode network`.
2. Warp `ts_reconstruct --deconv` ALSO deconvolves the even/odd half-maps (measured: half-map spectrum ∝ deconv map, xc +1 incl. 2nd CTF lobe). Both denoisers trained on Wiener-filtered input.
3. (Corrected the same day.) That K3 data are single-frame tilt images, so the halves are tilt-split (halfmap_tilts=1). The "~55 Å tilt-split ceiling" came from a 512³-box FSC and was wrong. Local FSC on 64–256³ boxes reaches 0.143 at 35–44 Å and drops to ~0 at the first CTF zero (~30 Å at 4–4.5 µm). That zero is the real limit, so n2n on tilt halves is fine and single-map isonet2 isn't needed. There is a narrow band near 15 Å that's correlated between the halves (FSC 0.4–0.6), almost certainly an artifact; n2n would keep it. Recommended: re-run the recon at 11.06 Å/px (2× bin) with deconv 0.

Built 2026-10-02, NOT RUN:
- New train params `isonet_ctf_mode` (default network), `isonet_mw_weight` (200), `isonet_cube_size` (96), `isonet_bfactor` (0).
- `isonet_epochs` default 50; `isonet_max_training_tomograms` default 0. An epoch is a fixed 3000 subtomos, so the cap saved nothing.
- Multi-GPU via `--batch_size 4×GPUs` with `allocated_gpu_count` moved to driver_base; `--with_preview False`; `--isCTFflipped True` in train and predict.
- The cost model now scales by cube³ and GPUs, and is capped by the cached QOS max wall.
- Train refuses deconvolved halves for n2n; tsReconstruct `deconv` default 0; predict default mem 128G.
- Assumption: Warp maps are CTF-corrected (the ctf/ volumes are ≥0). But Warp's deconv map has the opposite sign from the full map in the 2nd lobe, which is unresolved.

**Why:** the inheritance hid the method and failed silently; the IsoNet settings ignored the tool's own recommendations.
**How to apply:** check frame vs tilt halves before recommending n2n; don't reintroduce implicit cross-job overrides.
