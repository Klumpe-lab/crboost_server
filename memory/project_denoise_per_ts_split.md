---
name: project_denoise_per_ts_split
description: "Denoise extension work — (a) predict IS per-TS array-migrated, train stays single global reduction (confirmed vs code 2026-08-13); (b) IsoNet2 as alt denoiser. Both plan docs since deleted from repo."
metadata: 
  node_type: memory
  type: project
  originSessionId: ed48386f-369f-4582-898a-a50df49a6960
  modified: 2026-08-13T07:14:49.702Z
---

**IMPLEMENTED 2026-06-24 (code-clean: py_compile + ruff pass; PENDING runtime + IsoNet CLI).**
After the user landed the **afterok orchestrator** (schemer-free; `use_afterok_orchestrator: true`
global toggle in conf.yaml), I wired both pieces. NO orchestrator changes were needed — afterok
edges derive from INPUT_SCHEMA, and `_render_supervisor_script` routes resources via
`_get_queue_options()` (array→supervisor_slurm_defaults CPU-only, single→per-task).
- **Per-TS split (symmetry):** `drivers/denoise_predict.py` REWRITTEN to supervisor+per-tomogram
  SLURM array mirroring ts_reconstruct (SLURM_ARRAY_TASK_ID dispatch; reuses array_job_base generic
  helpers — NO WarpTools staging; manifest stashes `tomo_basenames` ts_name→recon-MRC-basename map
  since predict keys on filename not rlnTomoName; pre-skip filter via write_skip_status; idempotent
  skip if denoised MRC exists; supervisor aggregates denoised tomograms.star dropping stale Half1/2
  cols). Model `services/jobs/denoise_predict.py`: added array_throttle + `_get_queue_options()`
  supervisor override + denoise_method; kept __init__ gpu:1/64G as PER-TASK. Registered "Tasks"
  tracker tab in `ui/job_plugins/array_tasks.py`. `denoise_train` stays SINGLE (reduction); only
  modernized to read explicit Half1/Half2 cols (fallback to derive).
- **IsoNet option:** `DenoiseMethod` enum (CRYOCARE/ISONET) in models_base.py; `denoise_method`
  field+USER_PARAMS on both models (renders as dropdown via default_renderer is_enum_type, like
  alignment_method); method-aware `get_tool_name()`→"isonet"/"cryocare"; `isonet:` tool in
  conf.yaml + conf.template.yaml (sif `/groups/klumpe/software/IsoNet2/containerized/isonet2.sif`).
  Driver IsoNet branches (`build_isonet_predict_command`, `run_isonet_train`) RAISE
  NotImplementedError — ONLY fill-in left, pending verified `isonet.py <cmd> --help` flags +
  prepare_star/defocus glue. cryoCARE (default) fully wired. container_service execs
  `apptainer exec --nv --cleanenv` so isonet.py is on PATH (no plumbing change).

**UPDATE 2026-06-24 (later): IsoNet command chains WIRED (code-clean; PENDING runtime smoke).**
Got exact IsoNet2 Fire signatures from GitHub HEAD `IsoNet/bin/isonet.py` (installed sif top-level
cmd set matches). Wired: `run_isonet_train` (prepare_star→[deconv]→make_mask→refine→tar to
denoising_model.tar.gz, inner `isonet_maps/`); predict supervisor untars model once + stashes `.pt`
in manifest, each task stages even/odd 1-file dirs → prepare_star → predict into per-task dir → move
single MRC to canonical path (avoids --tomo_idx + output-filename guess). Added params:
`IsoNetRefineMethod` enum (auto/n2n/isonet2/isonet2-n2n) + `isonet_method` (train) + `isonet_deconv`
toggle (both, default False, tooltip: tsReconstruct deconv default=1). User decisions: deconv OFF
default (use rlnTomoName), method=auto (→isonet2-n2n from even/odd), model tar self-IDs by inner
dir (no rename — separate job dirs, io graph wires correctly; path_template is static so a
method-specific filename would need resolver changes). ISONET-ASSUMPTIONS (fail-loud): refine emits
.pt under output_dir; deconv/make_mask mutate star in place; predict writes 1 .mrc per single-tomo
star; gpuID auto. Per-subcommand `--help` NOT yet confirmed (user ran only top-level). NEXT: user
runs --help loop + `check` + a method=IsoNet denoise run.

**UPDATE 2026-06-24 (signatures + prepare_star CONFIRMED against installed sif):** inspect.signature
of all 6 ISONET methods == wired flags (ZERO drift, no container rebuild). `isonet.py check` is
upstream-broken (its `from IsoNet.bin.predict import predict` fails — predict.py exports nothing;
cosmetic, we never call check; the real `predict` subcommand is healthy). **KEY FIX:** prepare_star
with only `--even/--odd` leaves `_rlnTomoName=None` (uses our exact rlnTomoReconstructedTomogram
Half1/Half2 cols) → make_mask/refine/predict `--input_column rlnTomoName` would break. FIXED: both
drivers now ALSO stage + pass `--full` (the real full reconstruction = rlnTomoReconstructedTomogram,
which tsReconstruct emits next to the halves) so rlnTomoName is populated. predict denoises the FULL
volume (even/odd are training-only). All code-clean. Only runtime GPU run left to confirm in-place
star mutation + refine .pt output + predict single-full-mrc output.

**RUNTIME 2026-06-24 (LORENZ IsoNet train = job010/denoisetrain__2, FIRST real GPU run): 2 bugs found
+ FIXED (code-clean).** (1) **refine method=auto FAILS:** `isonet.py refine --method auto` →
`ValueError: Both full and half tomograms are present in the star file. Please specify method as
either 'isonet2' or 'isonet2-n2n'.` Our prep star intentionally carries the full (for
rlnTomoName/make_mask) AND even/odd (n2n data), so `auto` is ambiguous to IsoNet — it never
self-resolves despite the isonet_method field doc claiming "'auto' picks isonet2-n2n". FIX:
`drivers/denoise_train.py` now resolves `AUTO → ISONET2_N2N.value` before the refine cmd (explicit
isonet2 / isonet2-n2n pass through unchanged). NB make_mask (~15 min for 13 tomos) had already
succeeded (mask/ full, isonet_maps/ empty); refine is the only failed step. Driver is invoked by
ABSOLUTE PATH in run_submit.script (`venv/bin/python3 .../drivers/denoise_train.py --instance_id …`)
→ a rerun picks up the driver fix with **NO server restart**. Cleanest repair = rerun the train job
(re-establishes the afterok DAG to denoisepredict__2); resume-from-refine is possible but bypasses
orchestrator/DAG state, not worth saving the 15 min. (2) **Only 13 of 40 tomograms trained:**
`DenoiseTrainParams.tomograms_for_training` DEFAULTED to the dev-leftover string `"Position_1"` — a
SUBSTRING filter (driver L76 `if filter_str not in name: continue`, SILENT, no WARN) → matched
Position_1 + Position_10..19 = 13/40. This is the substring-filter footgun. FIX: default → `""` (=
all) + a Field description warning it's substring. ⚠️ The EXISTING job still has `"Position_1"`
persisted in project_params.json (a default change only affects NEW jobs) — to train on all 40, clear
the field in the job's UI, NOT by hand-editing project_params.json (in-memory ProjectState is
authoritative, see [[feedback_inmemory_state_authoritative]]). For denoising, 13 tomos is plenty
anyway; predict (denoisepredict__2) runs on all 40 since its `denoising_tomo_name` is empty.

**HANDOFF (end of 2026-06-24 session): all wired + code-clean (py_compile + ruff). NEXT SESSION =
runtime only:** restart main.py → (1) cryoCARE denoise to verify per-TS array fanout, (2) IsoNet
denoise to exercise the new chain. Full handoff incl. file list + remaining assumptions at TOP of
repo-root `ISONET_INTEGRATION_PLAN.md` ("▶ NEXT-SESSION HANDOFF").

**★ FOLLOW-UP LANDED 2026-06-24 (code-clean: py_compile + ruff + format; PENDING runtime — needs a
project where denoisepredict finished; AK_new_orchestrator_demo's denoise jobs 006-008 are still
queued).** Denoised preview is now SIDE-BY-SIDE with recon (user re-steered: "side by side", NOT the
toggle the handoff envisioned), in the Journey Reconstruct section — NOT preview_orchestrator/strip.py
and NO MANIFEST_VERSION bump (the handoff's plan was for a manifest-routed toggle; the standalone
side-by-side is simpler and decoupled). Implementation in `ui/tomo_dashboard_dialog.py`:
`_render_recon_big_preview` rewritten to two `.cb-recon-compare` panes — LEFT = recon (unchanged clean
WarpTools PNG, per [[feedback_journey_layout_preferences]]), RIGHT = denoised. Denoised volumes have NO
WarpTools PNG, so the right pane is an X/Y slab rendered from the denoised MRC via the existing
`render_xy_slab_preview`, background-built + cached at `<denoise_job>/vis/slabs/<ts>_xy.png` (new
helpers `_resolve_denoised_mrc_for_ts` / `_denoise_slab_path` / `_render_denoise_slab_sync` /
`_auto_kick_denoise_slab`, all mirroring the recon-slab twins). Resolver reuses
`_resolve_volume_for_3dmod` because the denoised tomograms.star repoints rlnTomoReconstructedTomogram
at the denoised volume. Denoised pane only renders when denoisepredict produced this TS (else recon
shows alone, full width). CSS `.cb-recon-compare{-pane,-pane-tag}` in `ui/dashboard/css.py`.
⚠️ KNOWN ASYMMETRY (flagged to user, offered the swap): recon=WarpTools deconv PNG vs denoised=raw
mean-Z slab are DIFFERENT pipelines → not a strict apples-to-apples denoising comparison. One-line
swap to matched slabs = render the recon pane via `render_xy_slab_preview` too (drop the WarpTools
PNG on the left). See [[project_journey_rework_dashboard_refactor]], [[feedback_journey_layout_preferences]].
- ⚠️ Runtime UNVERIFIED (server no auto-reload — user must restart main.py). IsoNet predict's
  per-tomogram semantics need confirming (its workflow is STAR-centric; may not split cleanly).
---

**IsoNet2 as alternative denoiser (explored 2026-06-24, no code yet — plan at repo root
`ISONET_INTEGRATION_PLAN.md`).** User wants IsoNet2 as a swappable alternative to cryoCARE.
Verdict: low-friction via a `DenoiseMethod` enum INSIDE the existing two denoise job types
(exact `AlignmentMethod` AreTomo/IMOD/Relion precedent in ts_alignment) — no new job types, no
graph edges, no orchestrator/container_service changes. IsoNet2 accepts even/odd halves
(isonet2-n2n mode = same input as cryoCARE) and needs CTF info (voltage/Cs/amp already in
tomograms.star; per-tomo defocus must be threaded from tsCtf). Workflow maps: refine→denoisetrain
(runs prepare_star→deconv→make_mask→refine), predict→denoisepredict. Touch points: DenoiseMethod
enum + `denoise_method` field/USER_PARAMS in both models + `get_tool_name()` branch + driver
branches + an `isonet:` tools entry in conf.yaml (container at
`/groups/klumpe/software/IsoNet2/containerized/isonet2.sif`). Real frictions: deconv double-apply
(tsReconstruct already deconvs by default), output STAR column rename
(rlnDenoisedTomoName→rlnTomoReconstructedTomogram), model-tar slot reuse, confirm exact isonet.py
flags via `--help` in the sif. Orthogonal to + composes with the per-TS split below (predict
array task branches on method). container_service execs `apptainer exec --nv --cleanenv --no-home
bash -c '<cmd>'` and applies the sif %environment → `isonet.py` is on PATH, no plumbing change.

---

The two denoise drivers (`drivers/denoise_train.py`, `drivers/denoise_predict.py`, both dated
Apr 4) are the **last job types never migrated to the per-TS SLURM-array model** that every
other WarpTools job got (the [[project_per_ts_parallelization]] refactor). They "fell behind."

Surfaced 2026-06-24 by `denoise_train` (job007) failing with `Input STAR not found: job006/
tomograms.star` in `…/20260610_S2_cell_lamellae_LORENZ`. **Immediate** cause = orchestration
race (denoise launched ~8 min before tsReconstruct's *supervisor* wrote `tomograms.star`;
project had `use_afterok_orchestrator` unset → legacy schemer). User is handling that in the
schemer-less orchestrator rewrite ([[project_orchestrator_replacement]]) and asked me to instead
plan the per-TS-split gap. **Plan written to repo root `DENOISE_PER_TS_SPLIT_PLAN.md`.**

Key conclusions:
- **`denoise_predict` = the real split work.** Currently one GPU job looping over ALL tomograms
  serially. Migrate to supervisor + array via `drivers/array_job_base.py` (reuse the generic
  manifest/status/submit helpers; it does NOT need the WarpTools-specific
  `stage_per_ts_environment`/tomostar staging — cryoCARE works directly on even/odd MRCs). Model
  needs `array_throttle` + `_get_queue_options()`→`supervisor_slurm_defaults`; existing
  `__init__` gpu:1/64G overrides become per-task. One subtlety: manifest keys on `rlnTomoName`
  but predict keys on the reconstructed-tomogram filename — stash the TomoName→MRC-basename map
  in the manifest.
- **`denoise_train` stays single** (it's a global reduction — one cryoCARE model from patches
  across many even/odd pairs; not splittable). Only modernization: read the new
  `rlnTomoReconstructedTomogramHalf1/Half2` columns instead of re-deriving `reconstruction/
  {even,odd}/` paths (`derive_half_map_path` has a dead identical-branch conditional).
- Tangential pre-existing bug in both: substring TS filter (`"Position_1"` matches `Position_10`…).
- Downstream consumer of denoised `tomograms.star` = template matching
  (`services/jobs/template_match.py` preferred_source="denoisepredict").
- Optional registry alignment: add `DenoisePredictTomogramOutput` to the `TomogramOutput` union +
  a `DenoisePredictIngestAdapter` (mirror `services/tilt_series/adapters/ts_reconstruct.py`).
