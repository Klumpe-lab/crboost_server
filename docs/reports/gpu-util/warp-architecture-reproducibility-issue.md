# Draft issue for the Warp repository: motion and CTF fits differ across GPU architectures on low-signal frames

Status: draft, 2026-11-10, written from P13 (`gpu-types-2026-11-10.md`, roadmap 45 §11). The maintainer files it
after filling the two gaps marked *to fill*. Tone and content are the repository's: an observation with numbers and a
question, not a bug claim. Roles, not names.

---

**Title:** `fs_motion_and_ctf` results are bitwise reproducible within a GPU architecture but differ across
architectures on low-signal frames (B200 / A100 / RTX 6000)

**Summary.** Running the same `WarpTools fs_motion_and_ctf` command on the same EER frames gives bitwise-identical
XMLs on a B200 and on a B200 MIG slice, and bitwise-identical XMLs on an A100 and on an A100 MIG slice, but the B200
and A100 results differ from each other, and both from an RTX 6000's. Most frames differ only at floating-point
level (motion trajectories within 0.01 px, defocus within 0.001 µm). On the highest tilts of a tilt series, and on a
poor tilt series, the differences are material: trajectories off by up to 78 px and a per-frame defocus fit landing
at 1.6 µm on one card and 7.5 µm on another. Downstream, `ts_aretomo` (AreTomo2 1.0.0) is identical across cards on
good tilt series and finds a different solution on the poor one; `ts_ctf`, `ts_reconstruct` and our template matching
(pytom) agree across cards. We would like to know whether this is the expected floating-point behaviour of the
fitting, whether a deterministic or reduced-precision-independent mode exists, and whether any of it points at a
code path worth a look (for example the `sm_100` kernels being JIT-compiled from PTX where the A100 runs native SASS).

**Environment**

- Warp 2.0.0dev41, the conda package (`conda create -n warp warp=2.0.0dev41 -c warpem -c nvidia/label/cuda-12.9.0
  -c conda-forge`, `CONDA_OVERRIDE_CUDA=12.9`), in an Apptainer image on `nvcr.io/nvidia/cuda:12.9.1-devel-ubuntu22.04`,
  run with `apptainer exec --nv`. AreTomo2 v1.0.0 built in the same image with `-gencode` for sm_52 to sm_86 plus
  sm_100 SASS and compute_100 PTX.
- Cards, one Slurm job each on an HPC cluster, 8 CPUs, 48 GB RAM, Warp pinned to 2 CPUs per task as in production:
  NVIDIA B200 (compute capability 10.0, 179 GiB), B200 MIG 1g.23gb (10.0), A100-SXM4-40GB (8.0), A100 MIG 3g.20gb
  (8.0), Quadro RTX 6000 (7.5, 24 GB).
- Host driver version: *to fill* (`nvidia-smi --query-gpu=driver_version --format=csv,noheader` on one node of each
  kind; the g3, g4 and g5 nodes may differ).
- Data: Falcon 4i EER, 1.55 Å/px, 225 EER frames per movie grouped `eer_ngroups = -32` (one frame discarded), dose
  `exposure = 1`; 300 kV, Cs 2.7, amplitude 0.1. Two tilt series of a lamella dataset: 41 tilts (−70° to +50°, 3°
  steps) and a poor one with 27 movies of which 26 tilts (−46° to +29°) survived import.
- Command, identical on every card (the production `create_settings` and `fs_motion_and_ctf` of our pipeline):

```
WarpTools create_settings --output warp_frameseries.settings --folder_processing warp_frameseries \
  --folder_data frames --extension "*.eer" --angpix 1.55 --exposure 1 --eer_ngroups -32
WarpTools fs_motion_and_ctf --settings warp_frameseries.settings \
  --m_range_min 500 --m_range_max 10 --m_bfac -500 --m_grid 1x1x3 \
  --c_window 512 --c_range_min 30 --c_range_max 10 --c_defocus_min 1.1 --c_defocus_max 8 \
  --c_voltage 300 --c_cs 2.7 --c_amplitude 0.1 --c_grid 2x2x1 \
  --out_averages --out_average_halves --perdevice 2
```

**What is identical**

- B200 against B200 1g.23gb: every frame XML (`GridMovementX/Y`, `Defocus`, `DefocusDelta`, `DefocusAngle`) and every
  average, bit for bit; the same for A100-SXM4-40GB against A100 3g.20gb.
- On every card: the frame count, the tilts kept, `ts_ctf`'s per-tilt defocus (max difference 0.005 µm), the
  reconstructed tomogram's centre (correlation 1.000 against the B200's).

**What differs (B200 as the reference, since production ran there; it is not the ground truth)**

Frames whose `Defocus` differs by more than 0.005 µm or whose `GridMovementX/Y` nodes differ by more than 0.5 px.
Everything not listed differs by less than 0.01 px and 0.005 µm.

| Card | Tilt series | Tilt | Δ defocus (µm) | Δ motion X / Y (px) | Defocus, this card / B200 (µm) |
|---|---|---:|---:|---:|---:|
| A100 | 41 tilts | −70° | 0.19 | 6.6 / 65.0 | 3.93 / 4.12 |
| A100 | 41 tilts | −67° | 0.002 | 15.6 / 5.9 | 1.65 / 1.66 |
| A100 | 41 tilts | −64° | 0.03 | 2.6 / 5.8 | 3.88 / 3.91 |
| A100 | 41 tilts | +35°, +41° | 0.002, 0.007 | 1.3 / 3.3, 1.5 / 2.1 | |
| A100 | 26 tilts | −25° | 0.000 | 4.3 / 1.4 | 1.64 / 1.64 |
| RTX 6000 | 41 tilts | −70° | 0.02 | 7.7 / 77.9 | 4.14 / 4.12 |
| RTX 6000 | 41 tilts | −67° | 0.04 | 2.9 / 26.2 | 1.69 / 1.66 |
| RTX 6000 | 41 tilts | −64°, +41°, +50° | 0.08, 0.07, 0.009 | 0.6 / 3.0, 0.9 / 2.0, 4.1 / 1.3 | |
| RTX 6000 | 26 tilts | −46° | 5.90 | 0.00 / 0.00 | 1.58 / 7.47 |
| RTX 6000 | 26 tilts | −37° | 4.69 | 0.03 / 0.06 | 1.59 / 6.27 |
| RTX 6000 | 26 tilts | −25° | 0.008 | 5.1 / 1.5 | 1.63 / 1.64 |

Two readings of this table:

- The material differences sit on the frames with the least signal: the −70° to −61° and +35° to +50° tilts of the
  good series, and the poor series, whose defocus fits scatter between 1.6 and 4.8 µm along the series on every
  card. Frames between −58° and +32° of the good series agree on every card to 0.01 px and 0.01 µm.
- On the poor series' −46° and −37° frames the B200's fits (7.47 and 6.27 µm) are the outliers against their
  neighbours (1.6 to 2.0 µm on every card); the RTX 6000's 1.58 and 1.59 µm agree with the neighbours. So the
  architecture does not make one card right; a fit without signal lands where rounding takes it.

**Downstream.** `ts_aretomo` (AreTomo2 1.0.0, `--alignz 1800`, `--angpix 6.2`), run on the B200's frame averages
on every card: identical `.st.aln` (0 px, 0°) for the three good tilt series on every card; for the poor series, 7
of 26 tilts shift by more than 1 px on the A100 (up to 208 px) and 11 of 26 on the RTX 6000 (up to 704 px). (AreTomo2
is not Warp's code; it is listed because our pipeline runs it through `ts_aretomo` and the pattern is the same: a
different solution only on the ill-conditioned input.) `ts_ctf` and `ts_reconstruct`, staged from the same inputs,
agree on every card; the cascade (motion on an A100, then alignment, CTF and reconstruction of those averages) was
not run: *to fill* if the maintainer wants it in the report (a whole-chain A100 run compared with the B200 run).

**Questions**

1. Is cross-architecture divergence of this size on low-signal frames the expected behaviour of the motion and
   CTF fits (cuFFT plan selection, FMA contraction, reduction order), or does something in the grid search's
   bracketing make it sensitive to the last bits?
2. Does the conda package ship SASS for sm_100, or do the B200 runs JIT-compile PTX? If the latter, the B200 is the
   odd one out here (two of its fits are the outliers), and that would be worth knowing for users on Blackwell.
3. Is there, or could there be, a deterministic mode (fixed cuFFT algorithms, no FMA contraction) for users who need
   the same answer from mixed GPU pools, even at some speed cost?

**Reproduction.** The two tilt series' EER frames and the per-card XMLs can be shared on request (about 6 GB of
frames). The comparison script reads `GridMovementX/Y` and `Defocus` from the frame XMLs and `.st.aln` rows from
AreTomo2; it is in our pipeline's repository (`docs/reports/gpu-util/scripts/probe_gpu_types.py`,
`summarise_gpu_types.py`).
