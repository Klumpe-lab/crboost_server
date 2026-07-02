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

crboost runs tools as `apptainer exec --nv --cleanenv --no-home` (see
`services/computing/container_service.py`). For miss-alignment specifically:

- **`--nv`** — required (GPU).
- **`--cleanenv`** wipes host env → any env the tool needs must be set *inside* the
  inner command string or baked into the def `%environment`.
- **`--no-home`** → set a **faux writable HOME** so torch/matplotlib/triton caches
  have somewhere to go: `env HOME=<jobtmp> MPLCONFIGDIR=<jobtmp>/mpl`. (A plain
  `import miss_alignment` is clean; matplotlib/fontconfig noise only appears when it
  *plots* — training thumbnails/tensorboard.)
- **`OMP_NUM_THREADS=1 MKL_NUM_THREADS=1`** recommended (the tool sets torch threads
  to 1 itself).
- **GPU model = one multi-GPU *node* job, not a per-TS SLURM array.**
  `--training-devices` / `--reconstruction-devices` are GPU **indices** (comma-sep;
  repeat an index for multiple recon workers on that GPU, e.g. `0,0,1,1`). The
  alignment phase auto-spreads over all visible GPUs.
- **`--pool-size` constraint:** `pool_size // n_partitions >= 2 * batch_size`, where
  `n_partitions = dataloaders_per_trainer` (× training devices). If violated it
  raises at datamodule construction. (Smoke used `--pool-size 128
  --dataloaders-per-trainer 1` with `batch_size 16`.)
- **`--prepare-stacks <apix>`** — optional; rebuilds tilt stacks at a target pixel
  size from source images before aligning. See §6/§7 for when it's needed.
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
- The thin XML also lacks `tilt_movie_paths`, so `--prepare-stacks` /
  `load_image_dimensions()` **cannot** auto-fill the dims (they need a movie path to
  read an average from).
- `image_dimensions_physical` / `volume_dimensions_physical` are **plain settable
  instance attributes** (not read-only properties), and `save_meta` **persists**
  them to the XML (verified across a fresh reload).

**The fix (proven, ~6 lines) — this is a required driver pre-step**, sourcing values
from `warp_tiltseries.settings`:

```python
import torch
from warpylib import TiltSeries
apix       = 1.55           # settings: PixelSize
W, H       = 7676, 7420     # settings: HeaderlessWidth, HeaderlessHeight
VX, VY, VZ = 4096, 4096, 2048  # settings: Tomo DimensionsX/Y/Z
ts = TiltSeries(xml_path)
ts.image_dimensions_physical  = torch.tensor([W*apix, H*apix], dtype=torch.float32)
ts.volume_dimensions_physical = torch.tensor([VX*apix, VY*apix, VZ*apix], dtype=torch.float32)
ts.ctf.pixel_size = apix
ts.save_meta(xml_path)      # persists on reload
```

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
  *source images* — needs `tilt_movie_paths` (from the tomostar) and the averages on
  disk. It is **not required** if a usable `tiltstack/*.st` already exists and dims
  are stamped: reconstruction reads `tilt_stack_path` (the `.st`), not the movies.
  In the proven smoke run we stamped dims and ran **without** `--prepare-stacks`.
- Works fine on a **single** tilt-series.

---

## 8. Resource profile

| run | cost |
|---|---|
| smoke (1 TS, 1 iter, 2 epochs, pool 128) | ~3 min on 1× A100-40GB |
| production (8 iters × 30 epochs, pool 1000, many TS, multi-GPU) | **hours** |

⇒ the crboost job needs **dynamic walltime** (scale by n_TS × iterations, like
`denoise_train`) and a **multi-GPU** SLURM profile.

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
