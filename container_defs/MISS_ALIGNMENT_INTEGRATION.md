# miss-alignment → crboost integration roadmap

**Status: P1 backend wiring LANDED 2026-07-02 (code-clean + ruff, PENDING runtime).**
Container was already PROVEN end-to-end (full macro-iteration on real 412 data — see
`docs/miss-alignment.md` §9). P1 added, all ruff-clean and adversarially reviewed:
`JobType.MISS_ALIGN` + `MissAlignSchedule` (`services/models_base.py`), `MissAlignParams`
+ `MISS_ALIGN_SCHEDULES` (`services/jobs/miss_align.py`), the driver (`drivers/miss_align.py`),
registry (`services/jobs/__init__.py`, `job_models.py`), orchestrator maps (driver_map +
DRIVER_TO_JOBTYPE), `tools.miss_alignment` + `job_resource_profiles.missAlign` in
conf.yaml/conf.template.yaml, and UI addability (`ui_state.py` order/name +
`pipeline_constants.py` phase/deps). **Runtime-CONFIRM list (couldn't verify in-sandbox):**
(1) `miss-alignment train --config-file` flag name; (2) in-container `python` on PATH for the
stamp step; (3) `--training-devices 0` maps to the gpu:1 allocation; (4) deploy on
`412FCIso_Pos28_4` + manually route tsCtf→missAlign (see routing decision below) → compare recon.

Remaining: **P2** (auto-routing + multi-GPU + UI plugin) and **P3** (`infer`). This doc is
the plan; the tool reference is `docs/miss-alignment.md`.

> Read `docs/miss-alignment.md` first — it has the tool reference, config schema,
> invocation contract, and the **dimension-stamping** gotcha that this driver must
> handle.

---

## Job model (decided)

**`MISS_ALIGN` — a post-alignment refinement.** A single multi-GPU **node** job
(NOT a per-TS SLURM array), code-modeled on `denoise_train`. It consumes
`aligntiltsWarp`'s `warp_tiltseries/`, refines the Warp XMLs in place, and feeds
`tsReconstruct`.

```
… fsMotionAndCtf → aligntiltsWarp → ⟨missAlign⟩ → tsCtf → tsReconstruct → …
                   (AreTomo coarse)  refine XMLs    (reads the refined warp_tiltseries)
```

- **v1 = `train`** (it both trains and aligns — it's the aligning command).
  `infer` (reuse a model across datasets) is a later add-on, not v1.
- **Optional/insertable** job — it augments AreTomo alignment, it doesn't replace it
  (it requires that coarse alignment as input).

Rejected alternative: adding it as an `AlignmentMethod` on the existing
`TS_ALIGNMENT` job — its train/infer + model lifecycle + multi-GPU profile don't fit
that one-shot dropdown.

---

## File-by-file wiring

Templates to copy from: **`services/jobs/denoise_train.py`** + **`drivers/denoise_train.py`**
(global GPU train job, `get_tool_name()` container switch, dynamic walltime) and
**`services/jobs/ts_alignment.py`** (Warp tiltseries I/O slots).

1. **`config/conf.yaml` + `config/conf.template.yaml`** — add under `tools:`:
   ```yaml
   miss_alignment:
     exec_mode: "container"
     container_path: "/groups/klumpe/software/containers/sifs/miss_alignment_torch2.8.0_cuda12.9.sif"
     bin_path: ""
   ```

2. **`services/models_base.py`** `JobType` — add `MISS_ALIGN = "missAlign"`.

3. **`services/jobs/miss_align.py`** — `MissAlignParams(AbstractJobParams)`:
   - `job_type = JobType.MISS_ALIGN`, `JOB_CATEGORY = JobCategory.EXTERNAL`,
     `RELION_JOB_TYPE = "relion.external"`, `IS_TOMO_JOB = True`.
   - `USER_PARAMS`: `prepare_stacks_apix` (float, e.g. 10.0), `n_iterations` (or an
     explicit iteration-schedule preset), `pool_size`, `batch_size`, `patch_size`,
     `max_epochs_per_iteration`, `training_devices`/`reconstruction_devices` (or a
     `n_gpus` that the driver expands), plus the `shift_generation` knobs if exposed.
   - `INPUT_SCHEMA`:
     - `InputSlot(key="warp_tiltseries_dir", accepts=[JobFileType.WARP_TILTSERIES_DIR], preferred_source="aligntiltsWarp")`
     - `InputSlot(key="warp_tiltseries_settings", accepts=[JobFileType.WARP_TILTSERIES_SETTINGS], preferred_source="aligntiltsWarp")`
   - `OUTPUT_SCHEMA`:
     - `OutputSlot(key="output_processing", produces=JobFileType.WARP_TILTSERIES_DIR, path_template="warp_tiltseries/", is_dir=True)`
     - `OutputSlot(key="warp_tiltseries_settings", produces=JobFileType.WARP_TILTSERIES_SETTINGS, path_template="warp_tiltseries.settings")`
   - `get_tool_name() -> "miss_alignment"`, `is_driver_job() -> True`,
     `get_input_requirements() -> {"align": "aligntiltsWarp"}`.
   - Dynamic walltime like `denoise_train._scaled_train_walltime` (scale by
     n_TS × n_iterations × epochs — production is **hours**).
   - Register in **`services/jobs/__init__.py`** (`jobtype_paramclass()` + `__all__`).

4. **`drivers/miss_align.py`** (pattern: `drivers/denoise_train.py`):
   1. `get_driver_context()`.
   2. **Stage inputs** into the job dir (work on a copy — the job writes its own
      output): copy the upstream `warp_tiltseries/` and `warp_tiltseries.settings`.
   3. **Stamp dimensions** onto every `warp_tiltseries/*.xml` — the ~6-line
      `warpylib` snippet from `docs/miss-alignment.md` §6, sourcing `PixelSize` /
      `HeaderlessWidth,Height` / `Tomo Dimensions` from the copied `.settings`.
      (This runs *inside* the container, e.g. a tiny stamped python invoked before
      the train call, since it needs `warpylib`.)
   4. **Write `config.yaml`** from params (`general.training_directory` = the job's
      `warp_tiltseries/`; `iteration_settings`; `model_training`; `data_loading`;
      `shift_generation`; `tilt_series_alignment`). Enforce `pool_size //
      dataloaders >= 2*batch_size`.
   5. **Run** via `get_container_service().wrap_command_for_tool(cmd, cwd, "miss_alignment", additional_binds=[...])`
      where `cmd` = `env HOME=<jobtmp> MPLCONFIGDIR=<jobtmp>/mpl OMP_NUM_THREADS=1
      MKL_NUM_THREADS=1 miss-alignment train --config-file config.yaml
      --training-devices <...> --reconstruction-devices <...> --pool-size <...>
      --dataloaders-per-trainer <...> [--prepare-stacks <apix>] --start-at-iteration 0`.
      `additional_binds` must cover the raw frames/tomostar if `--prepare-stacks` is
      used.
   6. Output = the refined `warp_tiltseries/` in the job dir → the `WARP_TILTSERIES_DIR`
      output slot `tsReconstruct` consumes.

5. **Scheme / `job.star`** — register in `config/Schemes/warp_tomo_prep/scheme.star`
   + provide the `job.star` template, like the other External jobs (keep
   RELION-compat; note the orchestrator replacement is in flight — see
   `services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md`).

6. **UI** — optional `ui/job_plugins/miss_align.py` (else the default renderer
   auto-renders `USER_PARAMS`); register the job in `ui/pipeline_builder/`.

---

## Open decisions

- **⚠️ Downstream routing — CORRECTED (was wrong in the original plan).** The obvious
  approach "edit `tsCtf`'s `preferred_source` from `aligntiltsWarp` → `missAlign`"
  **silently REGRESSES every no-missAlign pipeline** and must NOT be used. Traced the
  resolver (`path_resolution_service._choose_candidate_for_slot`): the score key is
  `(species_match, pref, filtered_pref, relion_job_number, succeeded)` and `pref`
  (matches `preferred_source`) is the ONLY guard above `relion_job_number` — there is
  **no topological guard**. `WARP_TILTSERIES_DIR` is multi-producer (aligntiltsWarp,
  tsCtf, tsReconstruct, missAlign). With `preferred_source="missAlign"` and no missAlign
  job present, `pref=0` for all → `tsReconstruct` (highest job number, non-interactive)
  wins → tsCtf resolves its input to a downstream/pending warp_tiltseries. This is
  DIFFERENT from `tiltFilter` (whose fallback type `TS_CTF_TILT_SERIES_STAR` has a single
  producer, so its declarative `accepts=[filtered, unfiltered]`+`preferred_source` pattern
  is regression-free — that pattern does NOT transfer to this multi-producer slot).
  - **P1 decision (LANDED): MANUAL routing.** `tsCtf.preferred_source` stays
    `"aligntiltsWarp"` (untouched); to verify, point tsCtf's `input_processing` at the
    missAlign job via the IO-tab source dropdown (writes a per-job `source_override`).
  - **P2: auto-wire via per-instance `source_override`** (Choice Z): when missAlign is
    inserted, set `tsCtf.source_overrides["input_processing"] = "missAlign:<live source_key>"`
    at deploy time (after reconciliation, so the instance_path is live) and pop it on
    removal — mirror `apply_aggregation_overrides` (`ui/aggregation_merge_card.py`). A stale
    key degrades safely to raw align, never to a wrong downstream job.
- **Stamp source of truth (DONE):** driver `read_settings_dims` parses
  `warp_tiltseries.settings` (`<Param Name="PixelSize"/>` + `HeaderlessWidth/Height` under
  `<Import>`, `DimensionsX/Y/Z` under `<Tomo>`); raises on missing/blank/non-positive.
  Never trusts `MicroscopeParams.pixel_size_angstrom` (the 1.35 apix trap).
- **`--prepare-stacks` default (DONE):** exposed as `prepare_stacks_apix`, default **0.0 =
  off** (the proven smoke path used the existing aligned `.st`). >0 needs frames+tomostar
  binds (wire those in P2).
- **Multi-GPU mapping:** request N GPUs in SLURM and expand
  `--training-devices`/`--reconstruction-devices` from the allocation; scale walltime
  dynamically.
- **Iteration schedule UX:** expose a small preset (e.g. `fast` / `default` /
  `thorough`) that expands to an `iteration_settings` list, rather than a raw list in
  the UI.

---

## Phasing

- **P1 — make it run in crboost (LANDED, code-clean, PENDING runtime):** param class +
  driver (single-GPU, `train`, `fast/default/thorough` schedule presets, dim-stamp
  pre-step, `--prepare-stacks` off by default). Manual downstream routing. Dynamic
  walltime already scales by n_ts × n_macro_iterations. Enough to deploy on 1–few TS.
- **P2 — production:** auto-routing (per-instance `source_override`, see Open decisions)
  + multi-GPU (expand `--training-devices`/`--reconstruction-devices` + gpu:N) + `--prepare-stacks`
  frame binds + optional `ui/job_plugins/miss_align.py` renderer.
- **P3 — `infer`:** a second job to reuse a trained model across datasets, if wanted.

## Verification path

Deploy on `crboost_data/412FCIso_Pos28_4` (the proven dataset). Confirm the driver
stamps dims, the train run completes, and `tsReconstruct` downstream reads the
refined `warp_tiltseries/`. Compare a tomogram reconstructed with vs. without the
miss-alignment refinement.
