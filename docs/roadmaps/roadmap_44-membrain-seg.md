# Roadmap 44 — MemBrain-seg as the third Segment method

**Status:** scoped 2026-11-06. No code. Open: D1–D5 (§6), S0 (a hand run) first.

**In one line.** Add MemBrain-seg (a 3D U-Net trained for membranes only) next to easymode and the Ais model
in the Segment job, so a membrane volume from it can be laid beside easymode's `membrane` on the same
tomograms and, if it is better, used wherever a membrane feature is read (gallery, Journey, TM search region,
ChimeraX surfaces).

**Not in this roadmap:** MemBrain's downstream tools (`membrain components` instances, `skeletonize`, `pick`,
`stats`), fine-tuning the checkpoint, the experimental `FAaug` / `MWaug` checkpoints (D3 asks whether to try
them), a ground-truth benchmark (S1 says what would be needed).

## 1. Goal

**Use case.** On MyrGFP_Grid2's 39 cryoCARE-denoised tomograms, run a Segment job with method MemBrain-seg and
read its membrane volume in the same Tomograms view as job045's easymode `membrane` (10 Å, TTA 8), tomogram
by tomogram, to decide which one the lab keeps for membranes.

## 2. Will it be better? (recon 2026-11-06)

Honest answer: possibly, and nobody has measured it. What is established and what is not:

- **Architecture.** MemBrain-seg is a 3D U-Net (MONAI, nnU-Net style, five levels, deep supervision) on 160³
  patches at 10 Å, loss BCE + Dice + Surface-Dice, with Fourier-amplitude and missing-wedge augmentation,
  sliding window with 8-fold flip TTA. easymode's own text says its 3D networks "mostly follow the residual
  U-Net from MemBrain-seg". The membrane network we run today is an **Ais-engine** network instead: 19.6 M
  parameters, 160 × 160 px window 16 slices deep (2.5D), trained on slice-drawn labels. So the comparison is
  full-3D context and a surface loss against 2.5D and a much larger training set (easymode: about 4 000 tilt
  series; MemBrain: five annotation rounds on Chlamydomonas, spinach and yeast data).
- **Published numbers** (MemBrain v2 preprint, Nature Methods 2026): Dice on the DeePiCt test set 66 % at round
  5, Surface-Dice 55–67 %. The authors say their ground truth was partly made by iterating with MemBrain-seg,
  which favours it. easymode's preprint reports no membrane validation at all.
- **No independent head-to-head against Ais or easymode exists** in what was read. Third-party numbers found by
  search summaries only (UNVERIFIED, none read in full): one paper has MemBrain-seg strong overall but poor on
  membranes lying in the missing wedge (0.30 against 0.88 for its own method); another judges its instance
  coverage too low for quantitative work. Treat both as hints for where to look, not as results.
- **Where it should win:** flat or sheet-like membranes the 2.5D window cannot follow in depth, thin continuous
  surfaces (Surface-Dice), denoised 10 Å input like ours. **Where it may lose:** data far from its training
  domain, wedge-facing membranes, anything easymode saw 4 000 tilt series of.
- **The decision this roadmap serves is empirical**, so S1 is the stage that matters and S2 is only worth its
  cost if S1 does not show easymode clearly ahead.

## 3. What exists (verified 2026-11-06)

**Software on CBE.next.**
- SBGrid `membrain-v2/0.0.1` (`/programs/x86_64-linux/membrain-v2/0.0.1/bin/membrain`): membrain-seg 0.0.10,
  torch 2.10.0, CUDA runtime 12.8, MONAI 1.5.2, Lightning 2.6.1. torch 2.10 + cu128 has sm_100 kernels, so it
  should run on a B200 as it is. Not module-loaded (`source /programs/sbgrid.shrc` or call the path). Not run
  by us yet.
- SBGrid `membrain-seg` 0.0.1 / 0.0.8 / 0.0.9 and a colleague's Aug 2025 image
  (`/groups/klumpe/software/membrain/membrain-seg.sif`, 10.4 GB, unpinned `pip install membrain-seg`, 0.0.9):
  both torch 2.6.0 cu124, **no sm_100 kernels**; do not use on a B200.
- Checkpoint `/groups/klumpe/software/membrain/MemBrain_seg_v10_beta.ckpt` (730 MB). Per the README it is the
  model for denoised data (v9b was for non-denoised); `v10_beta_FAaug` / `v10_beta_MWaug` are experimental.

**`membrain segment` (read from the 0.0.9 source; 0.0.10 not re-read, S0 confirms).**
- Options: `--tomogram-path`, `--ckpt-path`, `--out-folder`, `--in-pixel-size` (header value when omitted; the
  help says the header is often wrong after cryoCARE), `--out-pixel-size` (10), `--rescale-patches`,
  `--store-probabilities`, `--store-connected-components`, `--connected-component-thres`,
  `--test-time-augmentation/--no-…` (8 flips or none), `--segmentation-threshold` (0.0),
  `--sliding-window-size` (160, multiple of 32).
- `--rescale-patches` resizes each window to `160·10/in_px`, runs it at 160 and maps back, so the output stays
  on the input grid. That is what our `services/segmentation/grid.py` spectrum-crop + map-back does for
  easymode; here the tool does it (D2).
- Outputs: `<stem>_<ckpt basename>_segmented.mrc` (binary mask, or component labels) always;
  `<stem>_scores.mrc` with `--store-probabilities`: **raw averaged logits as float32, threshold 0**, not 0–1.
  The `.ckpt` stays in the segmented file's name.
- Resources: the docs say about 8 GB of GPU. The stitched prediction and the normalised tomogram sit in **CPU**
  RAM, two to three times the float32 tomogram. Normalisation is global mean/std, no clipping. Checkpoint loading
  is `strict=False` (a mismatch is silent). tqdm bars and `logging.basicConfig(INFO)` flood logs.

**In crboost (anchors).**
- `services/models_base.py:56` `SegmentMethod` (easymode, ais_model); `services/jobs/segment.py` params, tab
  fields, `get_tool_name()` returns `"easymode"` for both methods today.
- `drivers/segment.py`: method branches at lines 115 (`_settings`), 173 (`resolve_targets`), 258, 369 (manifest
  `method=`), 499, 523; the per-tomogram `.staging/task_<ts>/out/` → `segmentations/` flow and the re-run
  identity (`_identity`, line 124) are reused as they are.
- `services/segmentation/` (`easymode.py` runs the tool, `manifest.py`, `ingest.py`, `volumes.py`, `grid.py`).
- `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:575` treats a Segment producer as
  easymode (`producer.method == SegmentMethod.EASYMODE`) when wiring TM's search-region input.
- `ui/job_plugins/segment.py` lines 71, 90, 110 (method select, easymode-only fields); `ui/job_plugins/
  template_match.py:149` (easymode producers only).
- `config/conf.template.yaml` `tools.easymode` (container, `cache_dir`, `image_libs_first`); cluster probes
  in `tests/test_cluster.py`.
- Registry: `services/tilt_series/registry.py` is at schema 1.7; a new engine name and feature origin mean 1.8
  (1.8 must reach main before projects are opened by other
  servers).
- Baseline already on disk: MyrGFP_Grid2 job045, easymode `membrane`, 10 Å, TTA 8, 39 cryoCARE tomograms.

## 4. Stages

| Stage | What | Check (maintainer) |
|---|---|---|
| S0 | **Hand run, nothing in crboost.** One denoised tomogram, one B200, SBGrid `membrain-v2` binary, `--in-pixel-size` given, `--store-probabilities`, TTA on. Record: starts without a kernel error (and any PTX JIT delay), wall time, GPU and CPU memory, the score range and where 0 sits, output names | the run, the numbers, the volume opened next to job045's |
| S1 | **A/B on the 39 tomograms**, scripted outside crboost, output in a scratch tree: MemBrain scores → sigmoid → p ≥ 0.5 against job045's membrane p ≥ 0.5. Per tomogram: voxel fraction, Dice between the two, one-sided surface distances, connected-component count and size histogram. Choose 5–6 tomograms spanning easy and hard (the job039 diagnosis lists P1, P8_2, P9, P9_4, P6_4, P7). Then look: slabs and ChimeraX surfaces, wedge-facing membranes, vesicles, the lamella edge rim. Variants: with/without `--rescale-patches` against our own 10 Å resample (D2); TTA off | the pictures decide; write the verdict into §7. No ground truth exists, so "better" is the maintainer's reading, plus Surface-Dice only if a few hand labels are made |
| S2 | **Segment method.** `SegmentMethod.MEMBRAIN_SEG`; params `membrain_ckpt` (path, no default: absent is stated, not guessed), `tta` as on/off, `rescale_patches`; the driver runs `membrain segment` per tomogram into the task's `.staging/…/out/`, converts logits to p (D1), re-encodes in the job's `output_format`, writes the manifest with `method="membrain"`; `tools.membrain_seg` in the template and `config_service`; orchestrator and TM plugin accept the new producer; registry 1.8 (engine/origin); `tests/test_cluster.py` probe; `pytest` for the config | a Segment job on 3 tomograms lands in the gallery, Journey and TM search region exactly as an easymode one |
| S3 | **Own image, only if S1 says keep it.** `container_defs/membrain_seg_sm100.def`: torch ≥ 2.7 cu128, membrain-seg pinned, one image per tool, checkpoint beside it like easymode's cache. Until then S2 can point at the SBGrid binary (`exec_mode: binary`), which lab installs outside the VBC will not have | image runs the S0 tomogram to the same volume |
| S4 | **Tab and wording.** Method select entry, a "Needs denoised input" note on the input (README: v10_beta is for denoised data), a marker where the pixel size comes from the header and is 1.0 or 0, the SLURM note (about 8 GB GPU, CPU RAM 2–3× the tomogram) | the tab reads right; a header with voxel size 1.0 shows the marker |

S0 and S1 need nothing from the repo. S2 is where the repo changes; do not start it before S1's verdict.

## 5. Risks

- **The feature name.** easymode's feature is `membrane`; MemBrain's output is also "the membrane". Two
  features of one name on one tomogram, from different tools, must not overwrite each other in
  `segmentations/<tomogram>__<feature>.mrc` or in the registry. D4.
- **Logits.** `_scores.mrc` is not a probability. A threshold of 0 is p = 0.5 only if the head is a plain
  logit; S0 checks the range before S2 assumes a sigmoid (see D1).
- **Header voxel size.** The tool reads it from the MRC header when not given; crboost passes the project's
  value, never the header's, and S4 flags the case it cannot.
- **Domain.** v10_beta may fail on FIB lamellae or defocus ranges unlike its training data; S1 reads that.
- **Checkpoint provenance.** One 730 MB file in a colleague's directory is not a reproducible dependency; S3
  copies it next to the image with its source recorded.

## 6. Decisions

| # | Question | Leaning |
|---|---|---|
| D1 | Store logits through a sigmoid as p ×127/×255, or keep the raw score and threshold at 0? | Sigmoid to p, so every reader (gallery, TM region, surfaces) keeps its p ≥ 0.5 contract; S0 confirms the range first |
| D2 | `--rescale-patches` (tool resizes each window) or our own 10 Å resample + map-back (`grid.py`, shared with easymode)? | Decide from S1's variant; one path for both tools is simpler if the picture is the same |
| D3 | Which checkpoint? v10_beta only, or also `FAaug` / `MWaug` in S1? | v10_beta in S1; add the experimental two only if wedge-facing membranes are the failure |
| D4 | Feature name: `membrane` for both, or a distinct feature (`membrane_membrain`)? | Distinct, with the tool in the label; the A/B needs both on screen at once |
| D5 | Instances (`membrain components`) in this roadmap? | No; the ask is a volume to compare |

## 7. Verdict (S1)

Not run.
