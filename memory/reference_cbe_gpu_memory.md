---
name: reference-cbe-gpu-memory
description: CBE g-partition GPUs by feature (g1 P100 12 GB … g4 A100 40 GB); sacct gres/gpumem works but TF jobs report the whole card; easymode segment OOMs on g1
metadata:
  node_type: memory
  type: reference
  originSessionId: 44732dc1-e6a9-4518-b982-0081f27ad7f9
  modified: 2026-10-23T10:11:42.272Z
---

`sinfo -N -p g -o '%N|%G|%f'` (2026-10-23): g1 = `gpu:P100:8` (clip-g1-0..6), g2 = `V100:4`, g3 = `RTX:4` (Quadro RTX 6000, 24 GB), g4 = `A100:4` (40 GB). Gres names the card, so a card → GB table suffices for a pre-submit check.

sacct records `gres/gpumem` in TRESUsageInMax, but TensorFlow grabs the whole card: easymode reads 31852M on V100 (32 GB), 39520M on A100, 11914M on P100 → the g1 P100s are the **12 GB** model. Real need is only measurable with `TF_FORCE_GPU_ALLOW_GROWTH=true`.

easymode segment's microtubule 3D net (160×256×256 tiles) OOMs on g1, runs on g2/g4 → need is >12 GB, ≤32 GB; the conf.yaml "22 GB" note is unverified, so g3 (24 GB) is not proven. MyrGFP_Grid2 job045 failed because its SLURM override set `constraint: ""`, dropping conf's `g3|g4`. Pre-submit GPU check was discussed, NOT built. Related [[reference_cbe_slurm_qos]], [[project-easymode-denoise-and-job-size-chip]].
