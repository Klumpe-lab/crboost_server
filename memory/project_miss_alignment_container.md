---
name: project_miss_alignment_container
description: miss-alignment (warpem learned TS-alignment) container built + P1 crboost wiring LANDED (code-clean, pending runtime)
metadata: 
  node_type: memory
  type: project
  originSessionId: 5a9f594f-5ebb-4344-9b5c-d1b8e317b007
---

warpem **miss-alignment** ("learns to improve tilt-series alignment by making it
worse"; CLI = `train` + `infer`) is being added to crboost as a new tool.

**Container: BUILT & FUNCTIONAL** (2026-07-02). Def at
`container_defs/miss_alignment_torch2.8.0_cuda12.9.def` → `.sif` builds clean.
ubuntu:22.04 + miniforge, py3.12, `cuda-runtime=12.9` (NOT full `cuda-toolkit` —
that dragged in the Nsight Qt/X11 GUI stack → 40-min conda "Executing transaction";
runtime libs are all torch-projectors needs, confirmed by clean import). Pip:
`torch==2.8.0` → `torch-projectors` (cu129 index) → `miss-alignment`. torch bundles
cu12.8, torch-projectors is +cu129 — mismatch is expected/tolerated, imports fine.
Added `PIP_RESUME_RETRIES=20` for the slow (~300 kB/s) build-host network.

**Build "errors" are all cosmetic %test noise — NO fix needed to use it:**
`source: not found` (apptainer runs %test under sh not bash) → cascades to a
`conda activate` fail, but env python is on PATH via %environment so all imports
still passed. matplotlib/fontconfig "read-only cache" = build-time only BUT WILL
RECUR at runtime under `--cleanenv --no-home` → silence by setting
`MPLCONFIGDIR`/`XDG_CACHE_HOME` (bake in %environment, or prefix the inner driver
command — zero rebuild). **One real unknown: GPU exec unverified (build host is
CPU-only) — first next-session action = GPU smoke test + `train/infer --help`.**

**Integration roadmap doc: `container_defs/MISS_ALIGNMENT_INTEGRATION.md`.** Key
calls: it's a learned TS-alignment REFINEMENT sitting between `aligntiltsWarp`
(TS_ALIGNMENT) and `tsReconstruct`. **Denoise train/predict is the exact code
template** (GPU train/infer pair, `get_tool_name()` container switch, dynamic
walltime); `services/jobs/ts_alignment.py` is the I/O-slot template. Job-type
rec: model it denoise-style (train+infer pair) but SHIP INFER-ONLY first with a
pretrained model (the `zenodo-get` dep implies pretrained models are downloadable)
— rejected shoehorning it into the `AlignmentMethod` one-shot dropdown. **The I/O
contract from `infer --help` gates everything** (input format? output
Warp-consumable or IMOD .xf needing conversion?). Wiring surface: add `tools:
miss_alignment` in conf.yaml; JobType in `services/models_base.py:57`; param class
in `services/jobs/` + register in `services/jobs/__init__.py::jobtype_paramclass()`;
driver in `drivers/`; scheme/job.star; optional `ui/job_plugins/` renderer.
**CONFIRMED at runtime 2026-07-02 (A100-40GB, clip-g4):** GPU works (torch cuda
matmul + torch_projectors ops import); plain `import miss_alignment` is CLEAN —
matplotlib/fontconfig noise appears only when it PLOTS, silence with a faux
`HOME=/tmp/...` (or MPLCONFIGDIR/XDG_CACHE_HOME). **I/O contract fully mapped from
source:** config = plain YAML (`yaml.safe_load`) with sections
general/model_training/data_loading/shift_generation/tilt_series_alignment;
`general.training_directory` = THE warp_tiltseries dir (globs `*.xml`),
`iteration_settings` = list of `{downsample, alignment}` where alignment ∈
{global, anchoring, spline, [N,N]}; `model_architecture:'default'`, patch_size 96,
apply_ctf False. Verbatim templates in repo `docs/config_template.yaml` +
`docs/inference_config_template.yaml`. **OUTPUT = Warp XMLs updated IN PLACE**
(data/io.py → warpylib `TiltSeries.save_meta`; train.py globs *.xml, writes
`iterN/` snapshots + rewrites the XMLs) → `tsReconstruct` reads directly, ZERO
conversion. `train` is the ALIGNING job (trains+realigns); `infer` reuses a prior
run's `iterN/model.ckpt`. Single multi-GPU-node job (`--training-devices` /
`--reconstruction-devices`), NOT a per-TS array. Smoke-test data =
`crboost_data/412FCIso_Pos28_4` (job004 = aligntiltsWarp output = the input;
single TS; warp_tiltseries has xml + tiltstack @~12Å = crboost rescale_angpixs;
41 raw EER frames in frames/). So no `--prepare-stacks` needed — consumes crboost
Warp output as-is. Next: run the 1-iteration smoke test, then wire JobType +
param class + driver. **BLOCKER found 2026-07-02 (smoke test on 412FCIso_Pos28_4):
crboost's aligntiltsWarp exports a THIN Warp XML** (DataDirectory/Angles/Dose/
UseTilt/AxisAngle only) — NO ImageDimensions/VolumeDimensions/PixelSize. warpylib
`TiltSeries(xml_path)` (io.py `_load_metadata_and_stack`) validates
`image_dimensions_physical`/`volume_dimensions_physical` and dies with "zero values
in 'ImageDimensionsAngstrom': [0.0,0.0]". crboost keeps those in
`warp_tiltseries.settings` (PixelSize 1.55, HeaderlessWidth/Height 7676×7420, Tomo
Dimensions 4096³) which warpylib IGNORES (TiltSeries takes the xml path only; the
settings sibling does nothing). `--prepare-stacks` runs fine but does NOT backfill
the dims. FIX = stamp pixel_size + image/volume dims onto the XML via warpylib +
`ts.save_meta()` BEFORE calling miss-alignment — this is a required crboost DRIVER
pre-step, not a one-off. Container itself PROVEN workable (GPU kernels run, config
accepted, training loop starts). Everything else (bind pattern `-B WORK:/work -B
crboost_data:...:ro`, faux HOME=/work/tmphome, pool_size≥n_part*2*batch, single-GPU
`--training-devices 0 --reconstruction-devices 0`) is validated. **SMOKE TEST
PASSED END-TO-END 2026-07-02:** after stamping dims (set
`ts.image_dimensions_physical`=[W*apix,H*apix], `ts.volume_dimensions_physical`=
[VX,VY,VZ]*apix, `ts.ctf.pixel_size`=apix, then `ts.save_meta(xml)` — persists on
reload), a 1-iteration/2-epoch `train` on 412FCIso_Pos28_4 ran the FULL macro-iter
(~3min/A100, loss 0.18) → wrote `warp_tiltseries/iter1/{refined .xml, model.ckpt,
_alignment_loss.json}` + `warp_tiltseries/model.ckpt`. CONTAINER FULLY PROVEN.
Remaining work = crboost wiring ONLY (JobType MISS_ALIGN + param class + driver
that copies upstream warp_tiltseries+settings → stamps dims → writes config.yaml →
runs `miss-alignment train` → exposes refined warp_tiltseries to tsReconstruct).

**P1 BACKEND WIRING LANDED 2026-07-02 (code-clean + ruff, adversarially reviewed
0 real defects, PENDING runtime).** 9 files: `JobType.MISS_ALIGN="missAlign"` +
`MissAlignSchedule` enum (models_base); `MissAlignParams`+`MISS_ALIGN_SCHEDULES`
(services/jobs/miss_align.py, modeled on denoise_train, single GPU-node job, dynamic
walltime = base+12·n_ts·n_iters capped 24h); driver (drivers/miss_align.py);
registry (jobs/__init__.py + job_models.py); orchestrator driver_map +
DRIVER_TO_JOBTYPE; conf.yaml/conf.template.yaml `tools.miss_alignment` +
`job_resource_profiles.missAlign`; UI addability (ui_state PIPELINE_ORDER/DISPLAY +
pipeline_constants PHASE_JOBS/JOB_DEPENDENCIES). **Settings parse (proven format):
`<Param Name="PixelSize"/>`+`HeaderlessWidth/Height` under `<Import>`,
`DimensionsX/Y/Z` under `<Tomo>`; driver raises on missing/blank (never the 1.35
apix trap).** Dim-stamp runs IN-CONTAINER via a generated `stamp_dims.py` (warpylib
only there). Presets fast/default/thorough → iteration_settings; `prepare_stacks_apix`
default 0=off; success gate = ≥1 `warp_tiltseries/iter*/` dir (not tautological xml-glob).
**⚠️ ROUTING = the crux finding: the roadmap's "flip tsCtf.preferred_source→missAlign"
is WRONG — silently regresses no-missAlign pipelines (WARP_TILTSERIES_DIR is
multi-producer + resolver has NO topological guard → tsReconstruct wins by job#).
P1 = MANUAL routing (IO-tab source_override, user chose); P2 = auto per-instance
source_override at deploy time (Choice Z, mirror apply_aggregation_overrides). tiltFilter's
declarative accepts+preferred pattern does NOT transfer (its fallback type is single-producer).**
**2026-07-09 — de-risked from source + hardened + re-reviewed (still PENDING runtime).**
Two runtime unknowns RESOLVED without a cluster round-trip: (#1) pulled the real Typer CLI from
`github.com/warpem/miss-alignment/src/miss_alignment/train.py` → `config_file: Path = typer.Option(...)`
means the flag IS `--config-file` (driver correct; every other flag matches source too; caveat:
`config_file` has `**OPTION_PROMPT_KWARGS`/`prompt=True` → would HANG on stdin if the flag were ever
omitted). (#2) `.def` now checked into `container_defs/miss_alignment_torch2.8.0_cuda12.9.def` (was
missing); its `%environment` front-loads the conda-env bin on PATH and apptainer applies `%environment`
under `exec --cleanenv`, so bare `python` (stamp) + bare `miss-alignment` (train) both resolve — `python`
is CORRECT, no `python3` change. Only device-index-0 → gpu:1 remains genuinely runtime-gated (standard
CUDA_VISIBLE_DEVICES remap). 3-surface adversarial review workflow (6 agents): registry/config 0 findings
(all 8 wiring files consistent vs denoise_train/tsReconstruct baselines); FIX APPLIED = `prepare_stacks_apix
> 0` now raises at driver start instead of running the full multi-hour schedule then failing (binds are P2;
off-by-default so proven path unchanged); 2 minor items left documented (stamp bare-`python` verified safe =
same PATH mechanism as train, fails loud not silent; walltime 2h-floor when import_selected_tilt_series==0 =
attr name CORRECT, floors identically for single-TS target, only under-allocates on large data-less/aggregation
projects → P2 hardening). Roadmap + handoff docs updated. Relates to [[project_denoise_per_ts_split.md]] (denoise
template), [[project_denoise_train_dynamic_walltime.md]] (walltime pattern),
[[feedback_container_separation.md]], [[reference_hpc_env.md]],
[[feedback_never_silent_defaults.md]].

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

warpem learned TS-alignment refinement. Container BUILT + PROVEN END-TO-END on real 412 data (A100, 2026-07-02). cuda-runtime not toolkit. Warp-native: consumes aligntiltsWarp warp_tiltseries/ → refines XMLs in place → tsReconstruct reads directly (zero conversion). crboost's thin XML lacks ImageDimensions/PixelSize → driver MUST stamp dims from warp_tiltseries.settings via warpylib+save_meta (proven). train=aligning job (not infer). Single multi-GPU node job (not per-TS array); denoise_train is code template. Reference: docs/miss-alignment.md. Roadmap: container_defs/MISS_ALIGNMENT_INTEGRATION.md. P1 BACKEND WIRING LANDED 2026-07-02 (9 files, ruff-clean, adversarial review 0 real defects, PENDING runtime): JobType+param class+driver+registry+orchestrator maps+conf+UI-addability. Settings-parse keys proven (PixelSize/Headerless* under Import, Dimensions* under Tomo). ROUTING CRUX: roadmap's "flip tsCtf.preferred_source→missAlign" REGRESSES no-missAlign pipelines (WARP_TILTSERIES_DIR multi-producer + no topo guard); P1=manual IO-tab routing, P2=auto per-instance source_override. Runtime-confirm: train --config-file flag, in-container python, device-idx-0. Next=P2 auto-route+multi-GPU.

**2026-09-29 status (roadmap 21):** no crboost run has ever completed a macro-iteration. Cluster install conf
lacks `tools.miss_alignment` → silent bare-binary fallback → host python without torch (2 runs died). Driver
bugs vs upstream: success check passes on the iter0 baseline; resume off by one. Output consumed by nothing
(tsCtf prefers aligntiltsWarp). RELION extraction reads AreTomo geometry, so it can never see the refinement →
"Warp extraction job" most likely = `ts_export_particles` (+ the dev36→RELION5 converter); asking the
collaborators. ~7 h per macro-iteration at defaults.

**2026-09-29 evening — roadmap 21 stages 1–3 CODE-COMPLETE, NOT RUN (branch `missalignmnet`).** First run
target `bullseye_artifact_two` (7 TS, EER, 412 data) for a presentation the next day; A/B via `tsCtf__2` +
`tsReconstruct__2` next to the finished baseline job005/006. Three findings that explain/avoid past failures:
(1) THE STAMP WAS WRONG ALL ALONG: image dims came from settings `HeaderlessWidth/Height` (7676×7420 = Warp
default, meaningless for EER) instead of the real 4096² (stack cella 6348.8 Å) → image centre off ~2700 Å →
Vermicelli logs show `aligned == misaligned` every batch. Now read from each `.st` MRC cella. (2) roadmap-20
wrapper exports `TQDM_DISABLE=1` → tool silent for hours → 45-min idle watchdog; train cmd uses
`env -u TQDM_DISABLE`. (3) IO-tab override to an UNRUN producer is `type:External/pending_<id>`, dangles once
deploy allocates jobNNN, and resolve_inputs silently falls back to auto — manual routing never worked; stage 3
(`JobSpec.refines` + `_follow_refinements`, pipeline_order tracks roster) replaces it. Generic override bug NOT
fixed. Upstream facts (github src): iteration x writes `iter{x+1}/` (XMLs+loss json+model.ckpt); iter0 = pre-train
snapshot, no ckpt; `--start-at-iteration k` loads `iter{k}/model.ckpt`; `global`/`anchoring` refine
AxisOffsetX/Y only, `[N,N]` writes GridMovementX/Y. XML→star mapping verified on job004 by awk. Driver pre-flight
re-checks mapping + runs full ingest/emit to a scratch dir before GPU time.
**First run LIVE 2026-09-29 20:47** (bullseye_artifact_two job007 → tsCtf__2 job008 → tsReconstruct__2 job009,
afterok; clip-g4-2, 2× A100): stamp 6348.8 Å, pre-flight passed (252 tilts, ZRot ≤0.025°, shifts ≤1.6 Å, emit
7/7), ~1.2 s/step on A100 w/ dedicated recon GPU (vs 2.5–3.4 on g3 single GPU). Review follow-ups listed in the
roadmap. REGISTRY HAZARD: checkouts without the "miss_alignment" Literal (cluster install, dl_filter clone) skip
those TS sidecars on load and drop them from index.json on next save — keep them off missAlign projects.

**First-run readout 2026-09-30 (bullseye_artifact_two job007 vs aligntiltsWarp job004).** Per-tilt offset change
minus a fitted rigid volume shift (a·cosθ+b·sinθ+c per component): Pos19 3.0 Å rms, 6_2 3.8 (≤0.6 px at 6.2 Å —
invisible), 30_5 7.8, 13_2 8.6, 28_4 16.0, 35_2 20.2, 28_2 29.5 (ramp at +32..56°, 161 Å at 56°). The raw change is
dominated by a Z gauge shift (~100–200 Å rms). Training: iter1 loss 0.16→0.037 (aligned/misaligned scores
separated ~2.2); iter2 flat 0.045→0.043 (separation ~0.6–0.9, barely above loss_margin 0.5). apply_ctf=false; the
missAlign CTF branch (job008) has job005's identical defoci. User judged missAlign recons "not better, sometimes
worse" by eye. Proposed definitive test = fixed-particle STA A/B (same particles mapped by the rigid shift, same
CTF, no Polish/CtfRefine), gold-standard FSC with a seed-repeat noise floor.

**2026-09-30 settings + evaluation page:** `docs/reports/miss-alignment/settings-and-evaluation.md` (UNCOMMITTED).
The first run's null result is explained by its config: global-only (paper: STA gains come from `[3,3]` local),
3,750 steps/iter (upstream 30,000; LR cut + early stop never arm at a 15-epoch cap), 7 TS, full 3174 Å Z box
(upstream: fit tightly; empty patches degrade accuracy). Paper works at 10 Å stacks — our 6.2 Å stacks are NOT the
limit. Particle-free eval chosen: tilt-split FSC via `ts_reconstruct --halfmap_tilts` (primary) + K-fold
reprojection FRC (per-tilt); frame-split halves are blind to alignment; across-axis errors only testable below
~1/(D·Δθ). User has 100s-of-TS lamella datasets without particles; copia is 1 TS (too few to train).

**Roadmap 24 drafted 2026-09-30** (`docs/roadmaps/roadmap_24-missalign-regimes-and-evaluation.md`, UNCOMMITTED; user
starts it next session): Å-based schedules + PAPER preset, lr_milestones/seed/training_gpus, z_box auto, un-refused
--prepare-stacks (XMLs DO carry MoviePath; don't symlink the .st), missalign_changes.json; test data =
post_handedness_fix raw data re-processed (20 TS, ribosomes); verdicts = ribosome STA via Warp export + tilt-split
FSC calibrated against it. Binning decision: no 1.55 Å stacks; 3.1 Å as optional arm E.

**2026-09-30 (later) — roadmap 24 stages 1 + 3 CODE-COMPLETE, NOT RUN** (branch `missalignmnet`; details in the
roadmap's Progress block). Å schedules (`30A anchoring; 20A anchoring x2; 10A global; 10A [3,3] x5` text; PAPER +
CUSTOM presets), lr_milestones/seed/training_gpus/reconstruction_devices, z_box full|auto|Å (auto = in-container
warpylib `reconstruct_full` at ~25 Å + `services/analysis/lamella_slab.py`; NOT a reference_tomograms slot — cycle
risk), prepare_stacks un-refused (no tiltstack links: stack_tilts writes through symlinks), run_manifest.json resume
guard, missalign_changes.json, SlurmConfig.qos (+ SLURM-tab row; no crboost job could exceed 8 h before), job-tab
summary plugin `ui/job_plugins/miss_align.py`, local-warp chip on subtomoExtraction, tsReconstruct halfmap_tilts,
resolver: pending_ overrides survive deploy; several refiners → consumer follows the most recently deployed (the duplicate-producer rule), so one arm trio per Run routes itself. Stage 3: `services/analysis/
tilt_split_fsc.py` + `scripts/align_qc.py` (changes / controls / reconstruct-task / score).
Verified WITHOUT python via node ports on real data: rigid-removal fit reproduces job007 numbers exactly; Warp
tomogram Z-std profiles RISE toward the Z ends (so "outer 20 % = background" was wrong) and contrast is only
0.3–0.7 → estimator uses flanking minima. Upstream sources for the container's exact versions were fetched with
node `fetch` (no curl/python in sandbox): miss-alignment 0.2.0, warpylib 1.0.1 (PyPI sdists).
**CORRECTION (2026-09-30, code review):** the first-run "rigid-removed" numbers above (Pos19 3.0 … 28_2 29.5) came
from a 6-parameter per-component fit (a·cosθ+b·sinθ+c on X and Y separately) that also removes 3 REAL modes (constant
across-axis shift = moved tilt axis; along-axis cos/sin). Rigid volume translation = 3 params: across the axis
tx·cosθ − tz·sinθ, along it constant ty. Warp/warpylib image frame: tilt axis along (−sin a, cos a), across along
(cos a, sin a), a = AxisAngle (Rz(−a)·Ry(θ), euler_to_matrix rows). Correct job007 iter0→iter2: 13_2 11.8, 19 4.0,
28_2 34.9, 28_4 35.1, 30_5 14.9, 35_2 22.8, 6_2 4.4 Å rms. Fixed in services/analysis/alignment_changes.py.

**2026-09-30 (job-tab pass, NOT RUN):** presets are now regimes — choosing one sets steps_per_epoch /
max_epochs / lr_milestones / z_box (`PRESET_SETTINGS`: fast+default 250×30, thorough+paper 1000×30, paper z_box
auto); custom keeps them and prefills an empty custom_schedule from the preset left. Job tab CONFIG has an
`on_edit` hub: Parameters and SLURM save through it and it reruns the refreshes they return (plugin returns
`summary.refresh`, `render_slurm_tab` returns a signature-gated per-tab refresh — the old module-level
`@ui.refreshable` refreshed every client's SLURM panel). SLURM blur without a change no longer pins an override
(it used to pin the computed time limit). GPU checks + walltime read a GRES override (`gpu_count()`). Existing jobs
may carry spurious overrides from earlier focus/blur passes → "Reset to profile" once. User's fast-budget choice
open (kept = default's 250×30).

**2026-10-12 GPU layout guidance:** training ≈ 11 h/iter at 1000×30 on one A100 and dominates small projects;
only training GPUs shorten it (pilot PAPER: 1 train + 1 recon ≈ 4.6 d; 3 train + 1 recon ≈ 2 d); constraint `g4`
(A100) is the biggest lever (default g2|g3|g4 can land on RTX, ~2.6× slower). Recorded in docs/miss-alignment.md §8.1;
the missAlign SLURM tab shows it as a muted note + tooltip (`SLURM_NOTE` ClassVar on AbstractJobParams, rendered by
`ui/pipeline_builder/slurm_tab.py::_render_slurm_note`). Multi-trainer DDP never run in crboost.
