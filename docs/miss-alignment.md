# miss-alignment — reference

Everything you need to develop with, or run, **miss-alignment** inside crboost.
Companion: [`container_defs/MISS_ALIGNMENT_INTEGRATION.md`](../container_defs/MISS_ALIGNMENT_INTEGRATION.md)
(the crboost wiring roadmap). Grounded in a proven end-to-end run on real data
(412FCIso_Pos28_4, A100-40GB, 2026-07-02).

---

## 1. What it is

warpem's [miss-alignment](https://github.com/warpem/miss-alignment): a **learned
tilt-series alignment refinement** tool. It "learns to improve alignment by making
it worse" — each macro-iteration trains a small 3-D CNN to *score reconstruction
quality* (contrastive: real vs. deliberately-misaligned subtomograms), then freezes
that model and optimizes the tilt-series alignment by gradient descent to maximize
the score. A coarse→fine schedule of macro-iterations refines the geometry.

Two subcommands:
- **`train`** — trains models *and* realigns the dataset. **This is the aligning
  job** — use it when you don't already have a model.
- **`infer`** — reuse a *previous* `train` run's per-iteration models
  (`<run>/iterN/model.ckpt`) to align a new dataset; skips training. An
  optimization for applying one model across many datasets, not the primary path.

It is **Warp-native**: it reads and writes Warp tilt-series XML metadata, so it
slots between crboost's `aligntiltsWarp` (which provides the required initial
coarse alignment) and `tsReconstruct` with **zero format conversion**.

---

## 2. Where it lives (lab)

| thing | path |
|---|---|
| SIF | `/groups/klumpe/software/containers/sifs/miss_alignment_torch2.8.0_cuda12.9.sif` |
| def (lab) | `/groups/klumpe/software/containers/defs/miss_alignment_torch2.8.0_cuda12.9.def` |
| def (repo) | `container_defs/miss_alignment_torch2.8.0_cuda12.9.def` |

**Build notes.** `ubuntu:22.04` + miniforge, Python 3.12, **`cuda-runtime=12.9`**
(NOT the full `cuda-toolkit` — that drags in the Nsight Qt/X11 GUI stack and makes
conda's "Executing transaction" crawl ~40 min; `torch-projectors` ships prebuilt
cu129 wheels so `nvcc` is never needed). Then pip: `torch==2.8.0` →
`torch-projectors` (cu129 index) → `miss-alignment`. `torch` bundles CUDA 12.8 while
`torch-projectors` is `+cu129`; the split is by design and imports/runs cleanly.
The `%test` block emits harmless `source: not found` / conda-activate noise under
apptainer's `sh` — cosmetic, the container is fine.

---

## 3. Runtime / invocation contract

crboost runs tools as `apptainer exec --nv --cleanenv` (see
`services/computing/container_service.py`). For miss-alignment specifically:

- **`--nv`** — required (GPU).
- **`--cleanenv`** wipes host env → any env the tool needs must be set *inside* the
  inner command string or baked into the def `%environment`. The wrapper already sets
  the torch/matplotlib/triton cache dirs (`TORCHINDUCTOR_CACHE_DIR`, `MPLCONFIGDIR`,
  `TRITON_CACHE_DIR`, …) to `${TMPDIR:-/tmp}/crboost-$USER` on the node. (A plain
  `import miss_alignment` is clean; matplotlib/fontconfig noise only appears when it
  *plots* — training thumbnails/tensorboard.)
- **`OMP_NUM_THREADS=1 MKL_NUM_THREADS=1`** recommended (the tool sets torch threads
  to 1 itself).
- **GPU model = one multi-GPU *node* job, not a per-TS SLURM array.**
  `--training-devices` / `--reconstruction-devices` are GPU **indices** (comma-sep;
  repeat an index for multiple recon workers on that GPU, e.g. `0,0,1,1`). The
  alignment phase auto-spreads over all visible GPUs.
- **`--pool-size` constraint:** `pool_size // n_partitions >= 2 * batch_size`, where
  `n_partitions = training devices × dataloaders_per_trainer`. If violated it
  raises at datamodule construction. (Smoke used `--pool-size 128
  --dataloaders-per-trainer 1` with `batch_size 16`.)
- **Several training devices** run DDP: each epoch is split across them
  (`steps_per_epoch / n` steps per device, each with the full batch) and the learning
  rate is multiplied by n. On multi-switch PCIe nodes NCCL's peer-to-peer path can hang;
  the tool's docs prescribe `NCCL_P2P_DISABLE=1` (the crboost driver sets it).
- **`--prepare-stacks <apix>`** — optional; rebuilds tilt stacks at a target pixel
  size from the frame averages before aligning, on every invocation. See §7.
- **`--preprocess`** — optional; runs an XCF coarse alignment + pretilt estimation
  first (backs up XMLs to `pre-iter/`). Only valid with `--start-at-iteration 0`.

---

## 4. Input / output contract

**Input** — a Warp project layout with an **initial coarse alignment already done**
(AreTomo/etomo — crboost's `aligntiltsWarp` provides it):

```
<root>/
  warp_tiltseries.settings         # global: PixelSize, HeaderlessWidth/Height, Tomo Dimensions, …
  warp_tiltseries/                 # ← config general.training_directory
    <name>.xml                     # per-TS Warp metadata (angles, dose, axis, …)
    tiltstack/<name>/<name>.st     # the aligned tilt stack
```

**Output** — the Warp XMLs are **updated in place** (`warpylib.TiltSeries.save_meta`).
Each macro-iteration also writes a snapshot:

```
warp_tiltseries/
  <name>.xml                       # rewritten with refined alignment
  model.ckpt                       # latest trained model
  iterN/
    <name>.xml                     # snapshot for iteration N
    <name>_alignment_loss.json     # per-TS optimization loss trace
    model.ckpt
```

`tsReconstruct` reads the refined `warp_tiltseries/` directly — **no conversion**.

---

## 5. Config schema (`config_template.yaml`, annotated)

The tool ships **no** template in the wheel; the schema is
`yaml.safe_load` into these sections (from the repo `docs/config_template.yaml`):

```yaml
general:
  training_directory: /path/to/warp_tiltseries   # the dir it globs *.xml from (in-place refine target)
  apply_ctf: False                                # leave False; CTF doubles cost, no alignment benefit
  iteration_settings:                             # LIST — its length = number of macro-iterations
    # alignment modes: "global" (single pass) | "anchoring" (iterative) | "spline" (coarse-to-fine)
    #                  | [N, N] (local refinement with an NxN image-warping grid)
    - { downsample: 3, alignment: anchoring }     # coarse → fine schedule
    - { downsample: 2, alignment: anchoring }
    - { downsample: 1, alignment: global }
    - { downsample: 1, alignment: global }
    - { downsample: 1, alignment: [3, 3] }
    - { downsample: 1, alignment: [3, 3] }
    - { downsample: 1, alignment: [3, 3] }
    - { downsample: 1, alignment: [3, 3] }
  seed: 45132

model_training:
  model_architecture: 'default'
  model_checkpoint: null                          # init for iteration 0 only (null = random)
  loss_margin: 0.5
  learning_rate: 1.0e-3
  weight_decay: 1.0e-4                             # 0 disables (plain AdamW)
  max_epochs_per_iteration: 30                     # early-stopping cap
  warmup_steps: 500
  multistep_lr_scheduler: { milestones: [5, 15], gamma: 0.5 }

data_loading:
  batch_size: 32                                  # 32 fits a 24 GB card at reconstruction size 128^3
  patch_size: 96
  steps_per_epoch: 1000                           # defines epoch size

shift_generation:                                 # synthetic misalignments for contrastive training (pixels)
  trajectory_probability: .5
  trajectory_max_shift: 10.0
  jitter_probability: .5
  jitter_max_std: 2.0
  outlier_probability: .5
  outlier_max_shift: 20.0
  fracture_probability: .5
  fracture_max_shift: 20.0

tilt_series_alignment:
  patch_size: 96                                  # match data_loading.patch_size
  patch_overlap: 0.1
  batch_size: 32
```

CLI flags that are NOT in the YAML: `--training-devices`, `--reconstruction-devices`,
`--dataloaders-per-trainer`, `--pool-size`, `--start-at-iteration`,
`--prepare-stacks`, `--preprocess`. The **inference** config is a slimmer
`general` (`data_directory`, `model_run_directory`, `iteration_settings`, `seed`) +
the same `tilt_series_alignment` block.

---

## 6. ⚠️ The crboost gotcha: thin XML → you MUST stamp dimensions

crboost's `aligntiltsWarp` exports a **thin** Warp XML — `DataDirectory`, `Angles`,
`Dose`, `UseTilt`, `AxisAngle` only. It has **no `ImageDimensionsAngstrom`,
`VolumeDimensionsAngstrom`, or `PixelSize`.** miss-alignment loads each XML with
`warpylib.TiltSeries(xml)` and validates the dimensions, dying with:

```
ValueError: XML metadata at <...>.xml has zero values in 'ImageDimensionsAngstrom': [0.0, 0.0]
```

Key facts learned:
- `warpylib.TiltSeries(path)` reads the **XML only** — the `warp_tiltseries.settings`
  sibling is **ignored**. Copying the settings next to the XML does nothing.
- `--prepare-stacks` sets the image dimensions in memory only (`load_images` does,
  `save_meta` is never called), so the stamp is needed with it too.
- `image_dimensions_physical` / `volume_dimensions_physical` are **plain settable
  instance attributes** (not read-only properties), and `save_meta` **persists**
  them to the XML (verified across a fresh reload).

**The fix — a required driver pre-step.** Pixel size and tomogram dimensions come from
`warp_tiltseries.settings`; the **image extent comes from each tilt stack's MRC cell**
(`cella`, Å), the stack the tool actually reads:

```python
import torch
from warpylib import TiltSeries
apix       = 1.55                 # settings: PixelSize
VX, VY, VZ = 4096, 4096, 2048     # settings: Tomo DimensionsX/Y/Z
img_x, img_y = 6348.8, 6348.8     # tiltstack/<ts>/<ts>.st header cella (1024 px × 6.2 Å)
ts = TiltSeries(xml_path)
ts.image_dimensions_physical  = torch.tensor([img_x, img_y], dtype=torch.float32)
ts.volume_dimensions_physical = torch.tensor([VX*apix, VY*apix, VZ*apix], dtype=torch.float32)
ts.ctf.pixel_size = apix
ts.save_meta(xml_path)      # persists on reload
```

⚠️ **Not** the settings' `HeaderlessWidth/Height`: those describe headerless raw formats
and hold Warp's default 7676×7420 for EER/TIFF/MRC data. Stamping them (as every run up to
2026-09-29 did) moves warpylib's image centre by thousands of Å, so every training
subtomogram is mis-projected — the logs then show `aligned == misaligned` scores throughout.

These are physical extents (Å) = pixels × apix, i.e. scale-invariant, so they stay
correct regardless of any later downsampling.

---

## 7. Data requirements & `--prepare-stacks`

- An **initial coarse alignment** must exist (crboost `aligntiltsWarp` = AreTomo).
- The **stamp values** come from `warp_tiltseries.settings` (present in the
  `aligntiltsWarp` staging dir). ⚠️ Beware the microscope-apix default trap
  (`MicroscopeParams.pixel_size_angstrom` defaults to 1.35) — prefer the settings
  file's `PixelSize` over `ProjectState` for the true value.
- **`--prepare-stacks <apix>`** rebuilds tilt stacks at a target pixel size from the
  frame averages. Each tilt resolves to `<DataDirectory>/<MoviePath>` → `<movie
  dir>/average/<movie stem>.mrc` (crboost XMLs carry absolute `MoviePath`s into the
  frame-series job's `warp_frameseries/`); the original pixel size is the first
  average's header value. `stack_tilts` then writes `tiltstack/<ts>/<ts>.st`, the
  `.rawtlt` and PNG thumbnails, opening each for writing — through a symlink that
  would overwrite the alignment job's own files, so the crboost driver stages no
  stack at all when it is set (job param `prepare_stacks_apix`), checks the averages
  first, and passes the flag on a fresh stage only. Not required otherwise:
  reconstruction reads `tilt_stack_path` (the `.st`), not the movies.
- Works fine on a **single** tilt-series.

---

## 8. Resource profile

| run | cost |
|---|---|
| smoke (1 TS, 1 iter, 2 epochs, pool 128) | ~3 min on 1× A100-40GB |
| production (8 iters × 30 epochs, pool 1000, many TS, multi-GPU) | **hours** |

⇒ the crboost job needs **dynamic walltime** (scale by n_TS × iterations, like
`denoise_train`) and a **multi-GPU** SLURM profile.

### 8.1 Spending GPUs and CPUs

Each macro-iteration is a training phase plus an alignment phase. Training is a fixed budget of
`steps_per_epoch × max_epochs_per_iteration × batch_size` cubes, independent of the number of tilt series.
Three kinds of process run during it, each on its own CPU core (miss-alignment 0.2.0,
`data/_reconstruction_worker.py`, `training_dataset.py`, `models.py`):

```
  tilt stacks (the alignment job's, read through the job dir's symlinks)
      │
  ┌───▼──────────────────────────────┐   patch builders (--reconstruction-devices, one entry each):
  │ builder   builder   builder   …  │   load a stack, shrink it on the CPU, reconstruct 2 aligned
  └───┬──────────────────────────────┘   + 2 shifted 96³ cubes on a GPU (~1 GB), next stack after 80
      ▼
  [ pool: $TMPDIR/recon_pool_*, up to pool_size files ]   each cube is used once, then deleted
      │
  ┌───▼─────┐ ┌─────────┐ ┌─────────┐    loaders (--dataloaders-per-trainer): read + normalise
  │ loaders │ │ loaders │ │ loaders │    cubes; each waits until its share of the pool holds
  └───┬─────┘ └───┬─────┘ └───┬─────┘    batch_size files
  ┌───▼─────┐ ┌───▼─────┐ ┌───▼─────┐    trainers (--training-devices): DDP, batch_size cubes per
  │  GPU 0  │ │  GPU 1  │ │  GPU 2  │    step each, gradients synced every step; LR × n trainers
  └─────────┘ └─────────┘ └─────────┘
```

The trainers can only consume what the builders produce and the loaders deliver, and both are
single-threaded Python (`torch.set_num_threads(1)`), mostly CPU work. Measured on AGG412 Grid2 (A100 nodes,
1024² stacks, 2026-10-14/15):

| Layout | Pool | Trainer GPU | Per step | Limit |
|---|---|---|---|---|
| 3 trainers, 1 builder, 4 loaders each | half full (15 × 31) | 0 % | 6.65 s / 96 cubes | builders: ~14 cubes/s per builder |
| 1 trainer, 11 builders, 4 loaders | full (~1000) | bursts, mostly 0 % | 1.45 s / 32 cubes | loaders: a loader builds a whole 32-cube batch, ~5.5 cubes/s each |
| 1 trainer, 5 builders, 10 loaders | ~320 = 10 partitions × 32, the level loaders wait at | ~80 %, short dips | 0.33 s / 32 cubes (~98 cubes/s) | balanced: builders just keep up (~20 cubes/s each), trainer GPU nearly full |

The tool's own examples run three builders per training GPU (`--training-devices 0,1
--reconstruction-devices 2,2,2,3,3,3`) and do not mention loader counts.

In crboost the layout comes from three numbers (`services/jobs/miss_align.py::worker_plan`, used by the
driver and shown in the job tab with both rates):

- `training_gpus` — the first that many GPUs train.
- `patch_builders` and `dataloader_workers` (loaders per trainer) — pinned values are kept; on auto the
  CPUs the trainers leave are split so `builders × 16` and `loaders × 8` cubes/s (the measured rates,
  rounded down) are as high and as even as possible. Loaders are capped by
  `pool_size // (training_gpus × loaders) ≥ 2 × batch_size`. Builders are spread round the non-training GPUs
  first, then the training ones (a builder needs ~1 GB; a trainer ~6.4 GB of 40).

So **CPUs/task is the speed lever**: 2 GPUs (1 training) with 16 CPUs → 1 trainer, 10 loaders, 5 builders
(~80 cubes/s built and loaded by the constants; ~98 measured, iteration 1 training in ~2 h 45). The job tab warns when one side outruns the other by 1.5× (naming the fix,
e.g. raise `pool_size` when it caps the loaders) and when the processes outnumber the CPUs.

- **Queue.** GPUs on the CBE `g` partition are mostly taken one at a time by 1-GPU jobs, so a 4-GPU request
  waits for a whole node to empty (`squeue --start` said 6 days on 2026-10-15) while a 2-GPU request on
  `g2|g3|g4` started within minutes. V100, Quadro RTX 6000 and A100 run the container; P100 (g1, sm_60) does not.
- **Card.** Constraint `g4` keeps the job on A100s at the cost of the queue; a fed trainer's speed per card is not measured yet.
- **Alignment** starts one process per GPU and pulls tilt series from a queue; at 100+ tilt series it
  reaches hours per iteration, and every extra GPU shortens it.
- **Limits.** One node (`num_gpus ≤ 4`). A full node queues longer on `g_long`.
- **Short series.** A series of fewer than 10 tilts (`MIN_TILTS`) is left out at staging, listed under `left_out`
  in `missalign_changes.json`, and absent downstream. The tool's shift generator needs at least 4 tilts; a patch
  builder that draws a shorter series dies, and training waits on it until the walltime. A resumed run keeps the
  series it was staged with.
- **No torch.compile.** Every trainer would compile the same model into one Triton cache, and the ranks race on
  its files (pytorch/pytorch#134384): two 3-trainer runs died a minute into training on a missing `.json`, the
  second with `TORCHINDUCTOR_COMPILE_THREADS=1`. The driver sets `TORCHDYNAMO_DISABLE=1` for every run (a
  compiled model's checkpoint keys differ from an eager one's, so the setting must not change between resumes).
  The tool hangs after a rank dies (the builders keep it alive), and nothing kills it until 5 min before the walltime:
  if `run.out` stops moving for hours, cancel the job.
- **Checkpoints.** Lightning keeps the best three per-epoch checkpoints, but nothing resumes inside a
  macro-iteration (`trainer.fit` gets no `ckpt_path`): a run killed mid-iteration repeats it. Each finished
  iteration writes `iter{N}/` (XMLs, `model.ckpt`), and a re-run resumes after the last one
  (`--start-at-iteration`). The worker layout is not part of the resume check, so it may change between runs.

The wall-time estimate (`_walltime_plan`) trains at the slowest of the builders, the loaders and
trainers × `batch_size` / 1.0 s; an A100 trainer ran 0.33 s eager, slower cards are unmeasured.
The SLURM tab of a missAlign job carries a short form of this as a note (`MissAlignParams.SLURM_NOTE`).

---

## 9. Proven-working setup (smoke test, 2026-07-02)

- Data: `crboost_data/412FCIso_Pos28_4`, `External/job004/warp_tiltseries` (single TS).
- Node: `clip-g4-10`, A100-40GB, apptainer 1.1.9.
- Binds: `-B "$WORK":/work -B /groups/klumpe/crboost_data:/groups/klumpe/crboost_data:ro`
  (source **read-only**, all output to the `$WORK` copy).
- Sequence: copy `warp_tiltseries/` + `.settings` → **stamp dims** → `miss-alignment
  train` (1-entry `iteration_settings`, `max_epochs_per_iteration: 2`,
  `steps_per_epoch: 20`, `batch_size: 16`, `--pool-size 128 --dataloaders-per-trainer
  1 --training-devices 0 --reconstruction-devices 0`).
- Result: full macro-iteration — train loss 0.18, alignment loss 0.102, wrote
  `warp_tiltseries/iter1/{refined .xml, model.ckpt, _alignment_loss.json}`.
- Note: the near-flat alignment-loss trace was the **2-epoch smoke model being
  undertrained**, not a real convergence signal.
