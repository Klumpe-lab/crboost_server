# miss-alignment → crboost integration roadmap

**Status: the container is PROVEN end-to-end** (full macro-iteration on real 412
data — see `docs/miss-alignment.md` §9). Everything unknown has been resolved. What
remains is pure crboost wiring, and every piece of it is now specified. This doc is
the plan for the next session.

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

## Open decisions (small)

- **Stamp source of truth:** read `warp_tiltseries.settings` (authoritative per-run)
  for apix + dims; fall back to `ProjectState` only if absent. Do **not** trust
  `MicroscopeParams.pixel_size_angstrom` (defaults to 1.35 — the apix default trap).
- **`--prepare-stacks` default:** expose `prepare_stacks_apix` (default ~10.0). It
  normalizes stacks to a known alignment resolution but needs frames+tomostar bound;
  if a usable `.st` exists you can skip it. Decide whether it's on-by-default.
- **Multi-GPU mapping:** request N GPUs in SLURM and expand
  `--training-devices`/`--reconstruction-devices` from the allocation; scale walltime
  dynamically.
- **Iteration schedule UX:** expose a small preset (e.g. `fast` / `default` /
  `thorough`) that expands to an `iteration_settings` list, rather than a raw list in
  the UI.

---

## Phasing

- **P1 — make it run in crboost:** param class + driver (single-GPU, `train`, fixed
  sensible `iteration_settings`, dim-stamp pre-step, `--prepare-stacks`). Enough to
  deploy on 1–few TS and feed `tsReconstruct`.
- **P2 — production:** multi-GPU + dynamic walltime + UI plugin + schedule presets.
- **P3 — `infer`:** a second job to reuse a trained model across datasets, if wanted.

## Verification path

Deploy on `crboost_data/412FCIso_Pos28_4` (the proven dataset). Confirm the driver
stamps dims, the train run completes, and `tsReconstruct` downstream reads the
refined `warp_tiltseries/`. Compare a tomogram reconstructed with vs. without the
miss-alignment refinement.
