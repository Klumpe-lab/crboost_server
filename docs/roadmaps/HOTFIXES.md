# Hotfixes

The standing queue of bugs to fix ahead of roadmap work, kept for good. A roadmap plans what to build; this file
holds what is wrong now. [docs/known_bugs.md](../known_bugs.md) records pitfalls by their symptoms; an entry here is a
defect with a fix to make.

**How it works.**
- A bug found along the way lands here, not in a roadmap's "not fixed here" list, once it is confirmed: read in the
  code and reproduced, on a toy case or on real data read without writing.
- Entries are in fix order. Each keeps its id (H1, H2, …), and ids are never reused.
- Each entry gives its status, severity, what is wrong (with anchors), the evidence and how to repeat it, the impact,
  the fix, and the check that shows it fixed.
- A fixed entry moves to **Fixed** with its commit, and stays there.

Anchors are at `c49edcd` (2026-11-05).

## Open

### H1 — The kitchen never sees that two projects show the same acquisition

**Status:** open, found 2026-11-05. **Severity:** high: pooling two projects made from the same raw data puts the same
particles into a set twice, about half of them in opposite halves, and nothing warns.

**What is wrong.**
- The kitchen tells acquisitions apart by `acquisition_key` (`services/particles/tomo_identity.py:99-115`): the first
  frame's file name and mdoc DateTime, "stable across projects and across reprocessing" by its docstring (`:13-17`).
- Import renames every movie `<project name>_<movie>` and writes that name into the project's mdocs
  (`services/scheduling_and_orchestration/project_service.py:110`, `:141-145`, `:595`). The registry is built from
  those mdocs, so the file name in the key carries the project's name.
- So one acquisition imported into two projects has two keys. `source_conflicts`
  (`services/sta_kitchen/pool.py:346-357`), the only check behind Make set's "fed by N sources … Choose one in
  Sources" (`services/sta_kitchen/export.py:145-155`), groups by that key and never fires for them.

**Evidence** (real data, read only).
- `AGG412_20260311_MyrGFP_Grid2` and `agg_20260311_MyrGFP_Grid2` (under `/groups/klumpe/crboost_data`) share 13 tilt
  series. Their frames link to the same files: in both projects, `frames/<project>_Position_1_4_001_12.00_…_EER.eer`
  points at `/groups/klumpe/001_Data/20260311/frames/20260311_MyrGFP_Grid2/Position_1_4_001_12.00_20260311_164515_EER.eer`.
- The first-frame DateTimes agree, and so do the file names once the prefix is dropped. The keys match for 0 of 13.
- To repeat, from the checkout:

  ```bash
  venv/bin/python3 - <<'EOF'
  import json, sys
  from pathlib import Path
  sys.path.insert(0, ".")
  from services.particles.tomo_identity import acquisition_key
  from services.tilt_series.models import TiltSeries
  base, a, b = Path("/groups/klumpe/crboost_data"), "AGG412_20260311_MyrGFP_Grid2", "agg_20260311_MyrGFP_Grid2"
  def key(p, short):
      data = json.loads((base / p / f"registry/tilt_series/{p}_{short}.json").read_text())
      return acquisition_key(TiltSeries.model_validate(data))
  names = lambda p: {f.stem.removeprefix(p + "_") for f in (base / p / "registry/tilt_series").glob("*.json")}
  shared = sorted(names(a) & names(b))
  print(sum(key(a, s) == key(b, s) for s in shared), "of", len(shared), "keys match")
  EOF
  ```

**Impact** (from the code; not run through a Make set).
- Both projects' picks of one tilt series enter a set as picks of different acquisitions.
- A pick's uid hashes its project, frame, member and position (`services/sta_kitchen/table.py:38-47`), and its half
  is a hash of the uid (`services/particles/pick_uids.py:42-44`; set in `export.py:334-349`).
- So the two copies of one particle land in opposite halves about half the time. The half maps are no longer
  independent, the FSC overstates the resolution, and those tilt series count their particles twice.

**Fix.**
- Key on the source movie's own name. The import has it (`project_service.py:141-145`); the registry stores it beside
  `raw_filename`, and `acquisition_key` reads it.
- Existing registries get it from their frame links (`frames/<prefixed name>` links to the original movie) or, in newer
  projects, from `import_sources.json` (`project_service.py:160-162`).

**Check.** The script above prints `13 of 13 keys match`. A pool with one member from each project shows "fed by 2
sources" for those tilt series, and Make set waits for a choice.

### H2 — Neither hand can be set: the tilt-angle hand and the defocus hand are fixed

**Status:** open, found 2026-11-05. **Severity:** high for data that needs the other hand (every structure comes out
mirrored); no effect on the SerialEM data processed so far, which matches what CryoBoost v1 did.

**What is wrong.**
- **Tilt-angle hand.** `invert_tilt_angles` is False for every project (`services/models_base.py:334`), and no code
  writes it, so `ts_import` always gets `--dont_invert` (`drivers/ts_import.py:77-78`).
  - The mdoc parser does work out a value, true unless the software is SerialEM
    (`services/configs/mdoc_service.py:337`), but none of its four consumers takes it: `update_from_mdoc`
    (`services/project_state.py:1595-1621`), `backend.autodetect_parameters` (`backend.py:2262-2276`),
    `services/protocols/apply.py:148-158`, `ui/protocols_dialog.py:175`. Creation copies its own list
    (`project_service.py:522-542`), which leaves it out too.
  - The rule behind it is risky on its own: an mdoc with no software line counts as Tomo5 and so as inverted
    (`mdoc_service.py:179-182`, `:334-337`).
- **Defocus hand.** `invert_defocus_hand` is True for every project, also never written. It reaches the Import star's
  `rlnTomoHand` and tomograms imported in synthesize mode. From TS CTF on, the hand is TS CTF's own `defocus_hand`
  job field (default `set_flip`, `services/jobs/ts_ctf.py:53`; each series' hand from Warp's verdict,
  `services/tilt_series/adapters/ts_ctf.py:157-165`).
- **CryoBoost v1** also always passed `--dont_invert`, but kept both hands per microscope in its conf.yaml, and
  inverting rewrote the mdoc's `TiltAngle` lines at import (`src/rw/librw.py:870-886` there). Its presets said "No"
  for the tilt-angle hand on all four microscopes, and for the defocus hand "Yes" on the three lab Krios and "No" on
  the core facility's K3 Krios. v2 kept the defaults and lost the setting.

**Evidence** (toy). A two-tilt mdoc with the header `[T = Tomography 5.16.0.1234 on TITAN Krios]` and EER
`SubFramePath`s, run through `get_mdoc_service().get_autodetect_params`, then `StateService().update_from_mdoc` on a
fresh `ProjectState`, then `drivers/ts_import.build_ts_import_commands`: the parser says `invert_tilt_angles=True`,
the project holds False, and the command carries `--dont_invert`. The same with a SerialEM header: False, False,
`--dont_invert`.

**Impact.** A dataset that needs the other tilt-angle hand gives mirrored tomograms with no way to fix it short of
editing mdocs. Template matching then searches a mirror image, and every average has the wrong hand.

**Fix.**
- Both hands become project values the landing page asks for, as roadmap 42 already requires through the API
  (§3.5, S4). It shows what the data suggests (the software, for the tilt-angle hand) and fills in neither.
- The parser's guess becomes a hint beside the field. An mdoc with no software line says so instead of counting as
  Tomo5.
- Open for the maintainer: whether conf.yaml keeps per-microscope hands, as v1's presets did, and whether TS CTF's
  `defocus_hand` default follows the project's.

**Check.** The toy above, with the tilt-angle hand set to inverted, builds `ts_import` without `--dont_invert`. A
Create without either hand is refused, naming them.

### H3 — Cs and amplitude contrast are fixed at 2.7 mm and 0.10

**Status:** open, found 2026-11-05. **Severity:** high on other hardware (a Cs-corrected microscope gets the wrong
CTF everywhere); no effect on the lab's Krios, whose Cs is 2.7 mm.

**What is wrong.**
- No mdoc carries either value, and no code in v2 sets them on a project: `backend.autodetect_parameters` returns
  `spherical_aberration_mm: None` (`backend.py:2270`), and nothing writes `spherical_aberration_mm` or
  `amplitude_contrast`. Every project keeps the model defaults (`services/models_base.py:314-341`).
- Both feed motion correction and CTF (`drivers/fs_motion_and_ctf.py:130-131`), TS CTF (`drivers/ts_ctf.py:164-165`),
  the import job.star and Import star (`services/jobs/import_movies.py:58-59`;
  `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:897-898`), template matching
  (`drivers/template_match_pytom.py:239-240`), and imported tomograms (`backend.py:871-872`).

**Evidence** (toy, as in H2). A fresh project from either mdoc holds `spherical_aberration_mm=2.7` and
`amplitude_contrast=0.1`.

**Impact.** A microscope with another Cs, or a dataset that wants another amplitude contrast, runs every CTF step
with the wrong values, silently.

**Fix.** Two more values the landing page asks for, shown empty because no mdoc has them; roadmap 42's `ProjectSpec`
requires them already (§3.5). Made together with H2.

**Check.** A Create without them is refused, naming both; with them, `build_warp_commands` and the TM command carry
the given values.

### H4 — EER movies are always grouped 32 frames per fraction

**Status:** open, found 2026-11-05. **Severity:** medium: the dose per fraction follows from the data's frame count
instead of being chosen, so motion correction runs on fractions anywhere from about half to double what v1 aimed for.

**What is wrong.**
- `eer_fractions_per_frame` is never written. `eer_ngroups` falls back to 32 (`services/jobs/_base.py:375-376`), and
  motion correction gets `--eer_ngroups -32`, i.e. 32 raw EER frames per fraction (`drivers/fs_motion_and_ctf.py:113`).
- The parser's "detected" value is the constant 32 (`services/configs/mdoc_service.py:353-354`), and no field sets
  it.
- CryoBoost v1 computed it: the EER frame count from the first movie's header, the dose per tilt, and a target dose
  per fraction (0.3 e/Å² by default), tuned to lose the fewest frames (`src/misc/eerSampling.py`,
  `src/gui/schemeGui.py:475-495` there).

**Evidence** (toy, as in H2). Both toy projects build `--eer_ngroups -32`.

**Impact.** Dose per fraction = 32 × dose per tilt ÷ EER frames per tilt. At 3 e/Å² per tilt that is 0.16 e/Å² for
600 frames and 0.48 for 200, against v1's 0.3. `docs/known_bugs.md` §1 ruled the grouping out as the cause of the
copia CTF failures, so the effect is on motion-correction quality, not a known failure.

**Fix.** Compute it as v1 did, from the first EER file's frame count, the dose per tilt and a target dose per
fraction. Show the result on the landing page, settable there. Drop the constant from the parser.

**Check.** A toy EER dataset with a known frame count gives the grouping v1's formula gives, and the command carries
it.

### H5 — The tilt-series settings get the gain reference without its flips

**Status:** open, found 2026-11-05. **Severity:** low today, since nothing CryoBoost runs reads that gain; a one-line
fix.

**What is wrong.**
- `drivers/ts_import.py:93` passes `getattr(params, "gain_operations", None)`, but `TsImportParams` has no such field
  (`services/jobs/ts_import.py:43-59`). The flips live only on the motion job (`services/jobs/fs_motion_ctf.py:69`).
- So `warp_tiltseries.settings` gets `--gain_path` without the `--gain_flip_*` / `--gain_transpose` that
  `warp_frameseries.settings` gets.
- CryoBoost v1 passed the same gain options to both (`src/warp/libWarp.py:86-97`, used by `src/warp/tsAlignment.py:31`
  there), as Warp's tilt-series quick start does.

**Evidence** (toy, as in H2, with the gain at `/toy/gain.mrc` and the motion job's `gain_operations="flip_y"`).
`build_warp_commands` carries `--gain_path /toy/gain.mrc --gain_flip_y`. The tilt-series `create_settings` from
`build_ts_import_commands` carries `--gain_path` and no flip.

**Impact** (Warp's source, warpem/warp `main`, read 2026-11-05).
- Every WarpTools tilt-series step reads the frame series' averages, never raw frames (`TiltSeries.LoadMovieData`),
  so none uses this gain. CryoBoost runs no M.
- M takes its gain and flips from the tilt-series settings (`MTools create_source`), and its tilt-movie refinement
  (`DoTiltMovies`) applies them to raw frames. A CryoBoost project taken into M that way gets unflipped gain
  correction, silently.

**Fix.** `gain_operations` describes the gain file, not a job: it moves to the project beside `gain_reference_path`,
and both settings files take it.

**Check.** The toy's tilt-series `create_settings` carries the same `--gain_flip_y` as the frame series'.

### H6 — The cluster strip keeps one GPU type per node: CBE.next's MIG slices go missing

**Status:** open, found 2026-11-05 (roadmap 43 S2). **Severity:** low: display only, nothing submits from it. Since
roadmap 43 gave TM, Segment, Class3D and Refine3D a typed gres, the types a partition offers are what a user picks
between in a job's SLURM section.

**What is wrong.**
- `SlurmService.get_nodes_info` (`services/computing/slurm_service.py:464`, parse at `:496`) and
  `get_partitions_info` (`:330`, parse at `:361`) keep the first `gpu:(\w+):(\d+)` match of a GRES. Only one type
  survives, and `\w` cannot match the `.` in a MIG type (`nvidia_b200_1g.23gb`).
- The landing page's cluster strip groups nodes by that single (count, type) (`ui/landing_status_strip.py:174-176`).

**Evidence** (repeatable on CBE.next). `sinfo -N -p g -o "%N|%P|%T|%c|%m|%G|%f" --noheader | head -1` gives
`cbe-g5-1|g|mixed-|192|1546627|gpu:nvidia_b200:4(S:1-2,5-6),gpu:nvidia_b200_1g.23gb:28(S:0,3-4,7)|...`. Fed to
`get_nodes_info` (its `_run_command` returning that line), it yields `gpus=4, gpu_type='nvidia_b200'`.

**Impact.** The strip shows every `g` node as 4 B200s; its 28 slices (224 on the partition) never appear.

**Fix.** Parse every `gpu[:type]:count` item of a GRES (type `[^:(,]+`) into a list of (type, count) per node and
partition, and let the strip show each type.

**Check.** The line above gives `[("nvidia_b200", 4), ("nvidia_b200_1g.23gb", 28)]`, and the landing page's strip
shows both on `g`.

### H8 — A SerialEM delivery imports some series from stacks and others from raw frames, under one axis and hand

**Status:** fix built 2026-11-10 with roadmap 48 S1–S3, not run; moves to Fixed with its commit. Found 2026-11-10
(anchors at `a22decd`). **Severity:** high: the frames-built series come out
mirrored, with the tilt axis 10° off and no gain correction, and nothing on screen says so.

**What is wrong.**
- The layer is chosen per series: the stack whenever `<mdoc dir>/<ImageFile>` exists with nz == sections, else the
  movies (`services/stack_import.py:57-66`, `services/configs/dataset_parsing_service.py:179-187`).
- A K3's raw frames are stored in the camera's native orientation, and SerialEM's stack and the mdoc's
  `Tilt axis angle` in the rotated/flipped one (`RotationAndFlip = 6`, a vertical mirror; measured, roadmap 48 §3).
- The project has one tilt axis and one hand, both from the stack's view, and no gain unless one is typed.
- The scan only warns "Mixed source layers" in the parse warnings (`dataset_parsing_service.py:209-214`).

**Evidence** (real data, read only). `DatasetParsingService().parse_dataset` on
`/groups/klumpe/001_Data/OLENA_pooled_tomos/experiment2_20260615/*/*/*.mdoc`: 18 series `stack`, 5 `movies`
(`area_4-A_1_ts_002/003`, `area_4-A_2_ts_001`, `area_5-A_ts_002/004`, whose stacks IMOD renamed `_orig.mrc`), one
project tilt axis 85.0. For `area_5-A_ts_002`, the frame sum matches the stack section only after a vertical mirror
(NCC 0.64 against ≤ 0.04).

**Impact.** Any SerialEM K3 share where some stacks were renamed, moved or deleted. CryoBoost v1 had no stack layer,
so this is new with roadmap 18.

**Fix.** One layer per project, chosen by the user, with the axis, hand and gain of that layer (roadmap 48 S2–S3).
Until then: Create refuses a selection with more than one layer.

**Check.** The same scan offers Frames or Stacks for the project, and a Create never holds both.

### H9 — Create after a scan stores the dose per tilt rounded to 0.1

**Status:** open, found 2026-11-05 (roadmap 42 S4). **Severity:** low: a dose off by up to 0.05 e/Å² per tilt, i.e. a
few per cent, in dose weighting and everything that reads the project's dose.

**What is wrong.**
- The mdoc parser keeps the dose to two decimals (`services/configs/mdoc_service.py:215`), as CryoBoost v1 does
  (`src/rw/librw.py:407` there).
- The scan's `AcquisitionSummary` rounds each series' dose to one decimal to tell mixed doses apart
  (`services/tilt_series/preimport.py:165`), and the landing page takes that display value as the project's dose
  (`ui/data_import_panel.py:111`).
- Create without a scan reads the first mdoc and keeps two decimals, so the same data gives two doses. The agent API
  takes the series' own value (`services/dataset_scan.py:133`).

**Evidence** (toy: two EER series whose mdocs say `ExposureDose = 2.04`). Create after a scan stores
`dose_per_tilt: 2.0`; Create without one, and the API, store `2.04`.

**Impact.** 2.04 becomes 2.0 and 2.46 becomes 2.5: every dose-weighted step runs on the rounded dose.

**Fix.** The landing page takes the selected series' own dose, as `dataset_scan.data_values` does; the summary's
rounding stays for telling doses apart and for display.

**Check.** The toy's Create after a scan stores 2.04.

### H10 — Resampling or filtering a black template writes black density into its "white" file

**Status:** open, found 2026-11-05 (roadmap 42 S7). **Severity:** medium: the polarity labels of the made pair are
swapped, and a threshold mask made next is a mask of the inverted density.

**What is wrong.**
- The workbench's Resample and Apply lowpass start from the selected template as it is
  (`services/species_edit.py:439-442`). `process_volume_async` writes its input's contrast to `<name>_white.mrc` and
  the negative to `<name>_black.mrc` (`services/templating/template_service.py:116-123`), and both are registered
  under those names, the white selected.
- When the selected template is black, the made `_white.mrc` holds black density, is registered white and is
  selected. The mask tools start from a `_white.mrc` as it is (`services/species_edit.py:545`), so a threshold mask
  made from it thresholds the inverted density.
- CryoBoost v1 has no resample or filter of a made template: it makes each pair from its source (PDB, EMDB, shape),
  the white as the source and the black inverted (`src/gui/generateTemplate.py:129-134`, `:414-417` there).

**Evidence** (toy, the RELION handler emulated: it keeps contrast, as `relion_image_handler --rescale_angpix
--new_box` does). Select the black of a shape pair, resample: the selected template is
`…_apix10.00_box96_white.mrc`, polarity white, and its density correlates +1.0 with the black source.

**Impact.** TM itself reads only the file, so a run on the selected template matches the contrast the user had
picked. The labels, a flip made from it, and a threshold mask made from it are wrong: the mask covers the
background around the particle.

**Fix.** In `species_edit.make_template`, start a resample or lowpass from the source's white sibling when it is
black and the sibling exists (as the mask tools do); when it has none, swap the two outputs after processing. Then
select the made template of the source's polarity.

**Check.** The toy's resample of a black template selects a black template that correlates +1.0 with its source,
and its registered white correlates −1.0.

### H11 — A basic shape left without a lowpass is filtered at 45 Å and recorded as unfiltered

**Status:** open, found 2026-11-05 (roadmap 42 S7). **Severity:** low: the template's record, not its data.

**What is wrong.** The shape form's lowpass shows "—" when empty (`ui/template_workbench.py:1331`), the service then
filters at 45 Å (`services/templating/template_service.py:188`), and the pair is registered with no lowpass
(`services/species_edit.py`, `make_template`). TM's template row and a protocol export read that record. CryoBoost
v1 filters every shape at 45 Å (`src/gui/generateTemplate.py:452-453` there) and records nothing.

**Evidence** (toy). A shape made with no lowpass is named `…_lp45_white.mrc` and registered with
`lowpass_resolution_ang: None`. The agent API's answer reports `lowpass: 45.0` for it.

**Fix.** The form shows 45 Å as its value, and the pair is registered with the resolution it was filtered at.

**Check.** The toy's shape is registered with `lowpass_resolution_ang: 45.0`.

### H13 — The API's set_params picks a missAlign preset without the fields the preset sets

**Status:** fix built 2026-11-10 in the main checkout, not committed (the test server and roadmap 49's queued chain
run from the `agent_api` worktree, untouched); moves to Fixed with its commit. Found 2026-11-10 (roadmap 49 R1,
through the Agent API at `906170a`). **Severity:** high for an API run: the job runs a different budget and box than
the preset the tab shows, without a word.

**What is wrong.** Choosing `iteration_preset` in the job tab calls `MissAlignParams.apply_preset`
(`ui/job_plugins/miss_align.py:41`), which sets `steps_per_epoch`, `max_epochs_per_iteration`, `lr_milestones` and
`z_box` from `PRESET_SETTINGS` (`services/jobs/miss_align.py:156`). The API's `set_params` sets the field alone,
so the UI and the API leave different jobs behind for the same choice (roadmap 42's parity rule).

**Evidence.** `set_params(missAlign, {iteration_preset: paper})` on `AGG412_20251113_412_rerun` answered
`applied: {iteration_preset: paper}`; `get_job` then showed `steps_per_epoch` 250 and `z_box` full (PAPER: 1000 and
auto), and the walltime estimate followed the 250. Worked around by setting the four fields by hand.

**Fix.** `job_params.set_params` calls `MissAlignParams.apply_preset` when the preset changes, as it already moves
the TM increment with the symmetry (`set_symmetry`): the hook stays on the model, and the tab and the API both call
it. The preset goes first, so a field of it named in the same call overrides it (as the Denoiser does); `applied`
lists the fields the preset moved.

**Check.** Toy run of `set_params` on a fresh `MissAlignParams` (2026-11-10): `paper` → 1000 / 30 / `5,15` / auto,
applied `steps_per_epoch` and `z_box`; `paper` with `z_box: full` → 1000 and full; back to `default` → 250. Owed:
the same through the API on the test server once it runs this code.

### H16 — A tilt series the alignment failed on keeps an older run's alignment and goes downstream

**Status:** open, found 2026-11-16 (anchors at `330c6cc`). **Severity:** high: a series the alignment failed on is
processed downstream with an alignment from a different run, silently; missAlign stops on it.

**What is wrong.**
- The tilt-series registry keeps one alignment output per series per job instance (`ts.outputs["aligntiltsWarp"]`),
  whichever run of that instance wrote it last.
- `TsAlignmentIngestAdapter.emit_star` (`services/tilt_series/adapters/ts_alignment.py:201-205`) writes every series
  of the input star that has *an* output under the instance id; it never checks that the output is this job's
  (`aln_output.job_dir`). `ingest` only writes outputs for the series that aligned now, so a series that aligned in an
  earlier run of the instance and fails in this one keeps the old output, and `emit_star` writes it into this job's
  `aligned_tilt_series.star` and `tilt_series/<ts>.star`, with the old run's values.

**Evidence** (read only). `AGG412_20251031_412_FCIsolation_rerun`: Alignment job020 (2026-11-16) failed on
`Position_8_4` (AreTomo2 exit 139; supervisor "162 ok, 1 fail"), yet its star lists 163 series; the registry's
`aligntiltsWarp` output for `Position_8_4` has `job_dir` job009 (2026-11-15). missAlign job021 then failed twice
in 4 s: `read_stacks` opened `job020/warp_tiltseries/tiltstack/…Position_8_4/…Position_8_4.st`, which does not exist
(SLURM 1697013, 1701634). A scan of the eight `_crboost_data_api` reruns (each alignment star against the registry):
this is the only stale series.

**Impact.** Any job after an alignment rerun reads, for each series that failed this time but aligned before, the
older alignment: TS CTF and Reconstruct would build that tomogram from a different run's geometry without a word;
missAlign stops on the missing stack.

**Fix.** `emit_star` keeps a series only when its output's `job_dir` is this job's (resolved); a stale one joins
`problems` ("aligned by job009, not this job") and is dropped like a failed one. Look for the same pattern in the
other adapters that read `ts.outputs[instance_id]` (TS CTF, Reconstruct, Denoise): the same rerun case applies.

**Check.** Toy: a registry series whose output names another job directory is left out of the emitted star, with
the reason in the log. Real: re-run the alignment's aggregate (or the job) on the project above; the star has 162
series, and missAlign stages 162.

### H17 — A Run that retries a failed job deletes the failed attempt's logs first

**Status:** open, found 2026-11-16 (anchors at `330c6cc`). **Severity:** medium: the reason for a failure is gone
the moment it is retried, so a retry that fails the same way is the only way to read it.

**What is wrong.** Reusing a failed job's directory for an in-place retry removes `run.out` and `run.err` with the exit
markers (`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:786`).

**Evidence.** `AGG412_20251031_412_FCIsolation_rerun` job021: SLURM 1697013 failed after 4 s at 11:00:59; a Run at
11:02:40 reused the directory and its log was gone (only `job.star` and `run_submit.script` left) until the retry
failed in turn.

**Fix.** Keep the last attempt's logs beside the new ones (`run.out.prev`, `run.err.prev`, overwritten by the next
retry); remove only the exit markers. The job page's log view can offer the previous attempt.

**Check.** Fail a toy job, Run again: `run.out.prev` holds the first attempt's lines while the retry writes `run.out`.

### H20 — An array job's supervisor gets 4 h whatever its array: every task done, the job Failed

**Status:** fix in the tree, found 2026-11-18, not yet seen on a run. **Severity:** high: a large project's array
outlives its supervisor, which SLURM kills at the limit; the job reads Failed with every tilt series `.ok`, and
nothing downstream is submitted.

**What is wrong.** Every array job's parent sbatch took `supervisor_slurm` from conf.yaml as it stood
(`_get_queue_options` in ten `services/jobs/*.py`), 4 h on this cluster. The supervisor waits for its whole array,
queue time included; conf.yaml's own comment asked for `ceil(N_ts / array_throttle) × per-task time`, and nothing
computed it. Without `--qos`, CBE.next refuses anything over 8 h (g_short); `--qos=g_medium` takes 2 days.

**Evidence.** The seven Segment jobs of the `_crboost_data_api` reruns (job022, job030, job020, job015, job022,
job019, job018): each `run.err` ends `CANCELLED AT 2026-11-17T23:57:16 DUE TO TIME LIMIT`, sacct says TIMEOUT
04:00:28; `.task_status` holds 23 to 176 `.ok` and nothing else, the last written up to 08:15 the next morning.
The 50-tomogram project's Segment fit in 4 h and Succeeded. `sbatch --test-only -p g … --time=1-03:00:00` is refused
by QOS policy; with `--qos=g_medium` it starts as soon as a 4 h job.

**Impact beside it.** The supervisor's SIGTERM handler (`install_cancel_handler`, `drivers/array_job_base.py`) is
meant to scancel the array on a kill; it did not fire here (no "Caught signal" in any run.out), which is why the
tasks finished. A supervisor that does catch it at its limit throws away the rest of its array.

**Fix.** `AbstractJobParams.supervisor_slurm()` (`services/jobs/_base.py`): the configured supervisor, its time
`ceil(tilt series / array_throttle) × the task's time limit + 1 h`, never below the configured time, capped at the
QOS's probed MaxWall; every supervisor override takes it. Lab conf.yaml: `supervisor_slurm.qos: g_medium`.
**Recovery of a job it hit.** Run it again once its array has drained: every item is `.ok`, so the supervisor submits
nothing and aggregates.
**Check.** A Segment on 150+ tomograms submits its supervisor with about a day (`run_submit.script`, `#SBATCH
--time`) under g_medium and Succeeds.

## Fixed

### H33 — Every squeue stops the whole server for up to 4 s; curation polls and the Journey read NFS on the loop

**Fixed 2026-11-20 (uncommitted).** Every child process goes through `services/computing/processes.run_process`
(`subprocess.run` in a thread): `slurm_service._run_command`, the sbatch calls of `pipeline_runner`, `backend` and
`session_service`, the ChimeraX ssh, the relion project init, cisTEM and `backend.run_shell_command`. The curation
polls (`get_curation_session_info`, `find_active_curation_session[_any]`, `_live_project_curation_job_ids`) read
their registry, job.json, session.json, scope.json and slurm.log in a thread. The Journey no longer collects on the
loop when it mounts (the first paint, collected in a thread, settles the selection), nor when the pick viewer
refreshes the strip or the section switch redraws the pane; the recon map is read with the collect. The log file is
written by a `QueueListener` thread (`services/log_setup.py`).

**Status:** fixed 2026-11-20, found the same day. **Severity:** high: the UI freezes for seconds several times an
hour, for every tab.

**What is wrong.**
- uvicorn runs on uvloop, whose `create_subprocess_exec` forks the server while holding the GIL and waits for the
  child's exec. The fork copies the page tables of the whole process, so the freeze grows with the heap, and with
  the user's memory cgroup reclaiming (`memory.events` max 19636 on cbe-login, 64 GB cap) it reaches seconds. The
  watchdog can only sample once the GIL is back: `(of 1 checks)`, and before H28 at the logging module's after-fork
  hook (`logging/__init__.py:_releaseLock`).
- `_find_live_registered` and `get_curation_session_info` read files under `~/.crboost` (NFS) on the loop, every
  16 s while the Particles registry's Tomograms tab shows; the Journey's builder ran `collect_dashboard_journey`
  (which parses a subtomo job's particles.star) on the loop at every mount.

**Evidence.** `~/.crboost/logs/crboost-24326.log` since H28: `slurm_service.py:_run_command ×1 (of 1 checks)` for
1.3–4.4 s, 13 times from 2026-11-19 13:49 to 2026-11-20 10:11; `_find_live_registered` 1.3–7.2 s (×22 to ×141), six
times; `_extracted_ts_cached` 2.1 and 2.4 s at a Journey open; `logging/handlers.py:shouldRollover ×18` in a 1.2 s
stall. A toy (uvloop, a heap of 2.3 GB of small objects, `squeue --me` six times each): `create_subprocess_exec`
holds the loop 78 ms a spawn (median, worst 104 ms), `subprocess.run` in a thread 0 ms. At an empty heap both are
1 ms.

**Check.** The toy, and `run_process` / `_run_command` against squeue (output, a timeout killed, a missing binary as
`OSError`, stdin); a log record with a traceback reaches the file through the queue in the same format. Owed: the
watchdog's log after a day of use.

### H32 — The companion napari viewer saves into its working directory for a tomogram outside every project

**Fixed 2026-11-19.** `CompanionSession` (`containers/napari/open_tomogram.py`) points the viewer's
three save paths (comments CSV, particle STARs, PNGs) at `_unfiled_dir()` when a request carries no `outputs`: the
session's own folder, `~/.crboost/napari/<id>/`, or `~/.crboost/napari/by-hand/` for a window started by hand. It
does so when it starts as well, since the viewer's own default is the working directory, and the notice names the
folder. **Checked** with a toy: the real methods, napari and Qt stubbed, a stand-in controller whose paths start at
the working directory. A request without `outputs` gives `<session>/tomogram_comments.csv` and
`<session>/particle_stars`; by hand, `~/.crboost/napari/by-hand/…`; a project's `outputs`,
`<project>/Curation/napari/…`; the working directory stays empty. The same toy on HEAD's file gives
`<working directory>/tomogram_comments.csv`. Owed: a real session opening a volume outside every project.

**Where the fix lives now (2026-11-20).** `open_tomogram.py` is gone (roadmap 65 S3): the fix above is `18b31c0`,
and `6f19b19`, the switch to EisGreis, deleted the file. The same rule is EisGreis's link
(`eisgreis_crboost/agent.py`): a request without `outputs` saves into the session folder's `outputs/`, and so do
files given to `eisgreis-crboost` by hand, which makes a session folder of its own. Checked by EisGreis's
`test_outside_every_project_saves_into_the_session_folder` and the end-to-end run of roadmap 65 §10. The
standalone `eisgreis` still saves into its working directory, as the colleague's viewer always did: there the user
chose that folder.

**Status:** fixed 2026-11-19 in `18b31c0`, found the same day (recon HY16, roadmap 63 S1). **Severity:** low in a checkout,
where it leaves files in the tree (the untracked `tomogram_comments.csv` in the dev checkout, 2026-11-19 09:17, is
one); medium once members cannot write the install, where the save fails.

**What is wrong.** `_point_at` used `outputs` when the request had them and `Path.cwd()` otherwise, and the
colleague's viewer starts with all three paths at `Path.cwd()` (its `napari_mrc_lowpass_fast.py`). The server sends
`outputs` only for a volume with a `project_params.json` above it (`_napari_outputs`,
`services/curation/session_service.py`), and a window started by hand (`launch_napari_vnc.sh`) never has them. The
working directory is wherever the server or the shell was, often the checkout or the shared install.

### H31 — "Every card small" on the branch map leaves a blank page

**Fixed 2026-11-19 (uncommitted).** `M.draw` (`ui/branch_map/assets.py`) zeroes the cables' layer before it
measures the world, then sizes the layer to the cards alone. **Checked** in the harness over AGG412_STA: every card
large (world 10,015 px), scrolled to 6,009, every card small: 30 cards in view, scroll height 880, scrolled to 0.
Before the fix the same steps left 0 cards in view, scroll height 10,369, scrolled to 6,009.

**Status:** fixed 2026-11-19, reported by the maintainer the same day. **Severity:** medium: the cards were there,
above, but the view showed empty paper until scrolled back up, and the scroll bar still spanned the large map.

**What is wrong.** The cables' SVG is inside the world, absolutely placed, and `M.draw` set its size to the world's
`scrollWidth` / `scrollHeight`, which count the SVG itself. Once the map had been tall, the layer kept it tall: the
scroll area never shrank, so the browser never pulled the view back, and the view stood below the cards.

### H30 — Starting a particle job from the branch map does nothing: the species picker cannot open

**Fixed 2026-11-19 (uncommitted).** `BranchMapView.on_start` is async and awaits `_start`; the new card's ✓ hands
NiceGUI the `_add_new_job` coroutine instead of a task. NiceGUI's `handle_event` awaits a handler's coroutine inside
the sender's slot (`nicegui/events.py`), so the picker's dialog and the ✓'s notes have a client again. **Checked:**
ruff; NiceGUI's code read; the click in a restarted server is owed.

**Status:** fixed 2026-11-19, found the same day in the log of the dev server (three clicks at 14:33).
**Severity:** high for the map: no particle-phase job (all eleven, Template matching to CTF refine, which ask for a
species first) could be started from it since 50685a0, and nothing on screen said why.

**What is wrong.** `on_start` and the ✓ ran their work in `asyncio.create_task`. NiceGUI keeps a slot stack per task
and a new one starts empty, so `choose_species` (`ui/pipeline_builder/species_picker.py:101`) raised `RuntimeError: The
current slot cannot be determined` at `ui.dialog()`, in the task, to the log only. The ✓ had the same flaw on its
`ui.notify` lines (`_add_new_job`, and `add_instance_to_pipeline`'s refusal): a refused add was lost the same way.

### H29 — Adding a job with parameters from the branch map fails before its card appears

**Fixed 2026-11-19 (uncommitted), in roadmap 62's rebuild of the map.** The new job's form calls
`_attach_input(inp, is_frozen=False, width="M")` (`ui/branch_map/blocks.py`, `_ed_field`), the signature the job
page's fields now have, and sets its own pixel width after. **Checked** on AGG412_STA loaded read only, in a page
that mounts the real map with a stand-in panel (`notes/map-ideas/map_harness.py`): "+" on Class 3D 005 → Refine 3D
→ its card appears with its 25 parameter fields, its reference is chosen in the palette, the ✓ turns ready.

**Status:** fixed 2026-11-19, found the same day. **Severity:** high for the map: no job with parameters could be
added from it since 2026-11-18, and nothing on screen said why.

**What is wrong.** Roadmap 59 (7e65445, 2026-11-18) changed `_attach_input` in `ui/job_plugins/_field_styles.py`
to `(inp, *, is_frozen, width)`. The map's form still passed `narrow=False`, so building the new job's card raised
`TypeError: _attach_input() got an unexpected keyword argument 'narrow'` in the task started by the "+" menu, after
the job type was chosen: the card never appeared, and the error went to the log only.

### H28 — The loop watchdog puts every stall in a standard-library call down to the library, never to its caller

**Fixed 2026-11-19 (uncommitted).** `services/loop_watchdog.py`: `_place` names the first frame of the server's own
code (`_own_code`), and frames under the checkout's `.uv/` and `venv/` (the interpreter and the environment the
launcher builds) no longer count as the server's. **Checked** with a toy (`notes/map-ideas/wd_toy.py`, copied into
`services/` to run): a function of the checkout reading files on a busy loop, sampled by `_place` under the live
server's interpreter (`.uv/env/3.11.18-8febfdb40744/bin/python3`). Before: 8 of 8 samples in
`.uv/python/…/pathlib.py:open`, `read_bytes` or `_scandir`. After: 8 of 8 in the toy's function, under that
interpreter and under `venv/bin/python`. The server needs a restart to pick it up.

**Status:** fixed 2026-11-19, found the same day (roadmap 62 §1.10). **Severity:** medium: no stall in a file read,
a lock or any other standard-library call could be placed, so the worst freezes of the server went unexplained.

**What is wrong.** `_place` walked out from the innermost frame and took the first file under the checkout that is
not in `site-packages`. Since roadmap 39 the launcher's interpreter lives in `.uv/python/` inside the checkout, so
its standard library qualified: the innermost `pathlib.py` frame was reported, and the walk never reached the server
function that called it.

**Evidence.** `~/.crboost/logs/crboost-24326.log`: every stall line places itself in `.uv/python/cpython-3.11.18-…`
(`pathlib.py:open`, `stat`, `resolve`, `logging/__init__.py:_releaseLock`, `asyncio/runners.py:run`), e.g.
2026-11-19 13:31:08, 12.7 s in `pathlib.py:open` ×252, and 2026-11-18 11:44:32, 14.3 s in `pathlib.py:open` ×282.

**Impact.** Stall lines name a place only when it is the server's own Python, which hides the most common kind,
file reads on the event loop. Roadmap 62 §1.9's tally of 265 stall lines by place was made from such lines.

### H26 — RELION's tomo programs place particles without missAlign's local image warps: 8–17 Å RMS off

**Fixed 2026-11-19 (uncommitted): the bridge** (roadmap 53 §15.9). Every driver that hands RELION the tilt images
(Reconstruct particle, CTF refine, Subtomo extraction, pick-list extraction) first calls
`drivers/warp_bridge.carry_warp_geometry`: `services/particles/warp_geometry.py` finds each tilt series' Warp XML
(tsCtf job, Warp export's staging, a particle set's origin), refuses one whose rigid model is not the star's or whose
extents disagree, and when an XML holds more than the rigid model, `containers/miss_alignment/warp_trajectories.py`
(warpylib, in the miss_alignment container) writes `warp_trajectories.star`; the job hands RELION
`warp_optimisation_set.star`, which names it. Particles without names (candidates) get `<tomogram>/<n>`. A set that
already carries the bridge's trajectories for all its particles is used as it is; one naming other trajectories is
refused. Extraction outputs keep the trajectories named; relion_refine and CTF refine carry the line by themselves.
No XML beyond the rigid model: nothing changes. **Checked:** on job019's set the bridge writes the side step's
trajectories to 5e-6 Å in 7 s, and RELION's bin-2 reconstruction from its set is the side step's (9.2 Å; maps equal
to 1e-5 of their SD); on 2,853 unnamed SK candidates over 125 tilt series it names them and writes 2,853 blocks; a
bridged set comes back unchanged; foreign trajectories are refused; the AreTomo XML reads rigid-only, missAlign's on.
Not yet run as a crboost job (the C10 chain again, §15.9).

**Status:** fixed 2026-11-19, found the same day (roadmap 53 §15.7–§15.8). **Severity:**
high for any RELION tomo program on missAligned data: Reconstruct particle, CTF refine, frame alignment, RELION
pseudo-subtomo extraction. A bin-1 reference from them is about 20 Å where Warp's stacks of the same particles give
9 Å. A CTF refine fitted against such a reference, and itself cutting from the tilt images through the rigid star,
moved 275 tilts by more than 1 µm (job031, failed by its own check). Nothing was silently wrong in a result kept so
far: job023 is only the C10 reference, and Refine 3D / Class 3D read Warp's stacks.

**What is wrong.** missAlign writes local image warps into each tilt series' Warp XML (`GridMovementX/Y`, 3×3 per tilt).
Warp's export cuts the 2D stacks with them (`drivers/warp_export.py`: "the tilt series' full Warp model, local
image warps included"). The tilt-series star RELION reads holds only the rigid part (`ts_alignment` /
`adapters/miss_align.py`, which says so in its docstring; the missAlign driver reports the warps' size). RELION
programs that cut from the tilt images (`relion_tomo_reconstruct_particle`, `relion_tomo_refine_ctf`,
`relion_tomo_align`, `relion_tomo_subtomo`) place each particle in each tilt by that rigid star alone.

**Evidence** (AGG412_STA job019's 438 capsids, bin 2, job016's mask, scripts in
`notes/sta_side_steps/412_ctf_reference_diag/`). Warp's full position minus its rigid one at the particles: 7.6 Å
RMS (SK P11_2), 8.3 (FCIso P18_4), 9.2 (Slot6 P10), 17.3 Å (Grid2 P10), up to 64 Å. Tilt-image reconstructions,
half-map FSC 0.143: rigid star 19.4 Å (all 438); with Warp's offsets carried as RELION trajectories 9.2 Å, the
stacks' 9.1 Å (job019); offsets sign-flipped 20.3 Å. Per tomogram: SK 14.4 → 9.9 Å, Slot6 22.0 → 9.2, FCIso 19.8 →
12.0, Grid2 30.5 → 15.0 (flipped: 22.7, 24.0, 29.4, 24.8).

**Fix (D11 taken: per consumer, §15.9).** Carry Warp's local geometry to RELION as per-particle trajectories
(`rlnTomoTrajectoriesFile`, Polish's format): per particle and tilt, the shortest 3-D offset whose projection is
Warp's full position minus its rigid one (`warpylib.get_position_in_all_tilts`), as `warp_trajectories.py` does.
Verified: RELION's tomo programs apply it (above); `relion_refine` on 2D stacks ignores it (maps bit-identical) and
carries it into its output set. **Check.** Reconstruct particle of job019's poses at bin 1 reaches the stacks' resolution; CTF refine's moves stay
within its limit.

### H27 — The branch map shows a job given a particle set as reading itself

**Fixed 2026-11-19 (uncommitted).** `project_from_state` (`ui/branch_map/model.py`) no longer takes a recorded
input path inside the job's own folder as its source. Such a path is the copy of a particle set the job stages in
`input_set/`; the set it was given is its source override (`mergedSources:particle_set__<id>`), which the map
already reads into a "set …" row (cabled to the job the set was pooled from, when it was). A staged copy without an
override shows its file name. **Checked** on AGG412_STA's state loaded read-only, with the sets' parents read as the
map reads them (session scratchpad `dump_graph.py`): no job is its own parent any more; Warp export 001/003/021,
Reconstruct particle 002/004 and Class 3D 005–013 read their sets and start chains of their own; Class 3D 014 and
Refine 3D 015 hang off Class 3D 013 through the class-2 set. The server needs a restart to pick it up.

**Status:** fixed 2026-11-19, found the same day while measuring the map. **Severity:** medium: the lineage of
every job that reads a kitchen set is wrong on the map, and the layout piles those jobs into its first column.

**What is wrong.** A job given a particle set records `<its own job dir>/input_set/optimisation_set.star` in
`paths`. `project_from_state` maps a recorded path to the job whose folder holds it (`model.py`, the `dirs` lookup),
so the job became its own parent: a root nothing reaches (`tidy` never places it), drawn at depth 0, with a cable to
itself.

**Evidence.** AGG412_STA, 2026-11-19: 14 of 30 jobs had `parent` = their own id (warpExport, warpExport__2,
warpExport__4, reconstructParticle, reconstructParticle__2, class3d … class3d__9).

### H18 — add_job's `reading` stores a source no candidate list holds

**Fixed 2026-11-19 (uncommitted).** `pipeline_edit.add_job` checks `reading` before it changes anything (`_checked_reading`: the job added on a `detached_copy`, each source matched against `get_candidates_for_slot` as `set_input` does, `unreadable_reason` applied) and stores the candidate's own source key. A source named the way job views name it, a producer's instance id (`warpExport__4`) or a set without its type (`particle_set__s002_…`), stands for the one candidate it fits; no fit or two refuses, listing the sources. **Seen again** 2026-11-19, plan `p-f775f782`: `add_job(class3d, reading={input_optimisation: "warpExport__4", input_reference: "refine3d__2"})` rehearsed clean and Run refused with the same "set to () belongs to no job now" for both inputs; recovered with `set_input` (plan `p-0811a7dc`). **Checked** on AGG412_STA's state loaded read-only (session scratchpad `h18/check.py`): that call now stores `warpExport:instance:warpExport__4` and `refine3d:instance:refine3d__2`; this entry's evidence stores `mergedSources:particle_set__s002_ribosome_pilot_ge10`; `reading={input_optimisation: "nope"}` is refused naming the sources, the roster unchanged.

**Status:** fixed 2026-11-19, found 2026-11-17. **Severity:** medium: the job's input names nothing, and only Run says so.

**What is wrong.** `pipeline_edit.add_job` checks that each slot of `reading` exists (`services/pipeline_edit.py:222-226`)
and stores the source string as given (`:279-280`). `job_params.set_input` checks the same string against the slot's
candidates and names the ones it could read (`services/job_params.py:385-391`). The API's `add_job(reading=…)` goes
through the first.

**Evidence.** AGG412_STA, 2026-11-17: `add_job(reconstructParticle, reading={input_optimisation:
"particle_set__s002_ribosome_pilot_ge10"})` was taken; the candidate is `mergedSources:particle_set__s002_…`. Run
refused: "the job directory this input was set to () belongs to no job now".

**Fix.** Check `reading` against `get_candidates_for_slot` as `set_input` does, refusing with the sources listed.
**Check.** The call above is refused naming `mergedSources:particle_set__s002_ribosome_pilot_ge10`.

### H19 — plan_run passes a run that Run refuses for an unresolvable input

**Fixed 2026-11-19 (uncommitted).** `plan_run` runs Run's first two passes on a copy (`_unresolved_inputs` in `pipeline_orchestrator_service.py`, in a thread: a provisional dir per job, the failed job's own when it is retried in place, then `resolve_all_paths` for each) and lists "Path resolution failed for …" among `refusals`, so a change plan's rehearsal refuses what Run would. Beside it, Run's "started …" event is written once something was submitted (it was written before resolution, so a refused Run still said "started"). **Checked** with the same state copy: a Class 3D pinned to `warpExport__4` / `refine3d__2` as stored before the H18 fix gives Run's own refusal line; pinned to the candidates' keys, none.

**Status:** fixed 2026-11-19, found 2026-11-17. **Severity:** medium: a change plan's rehearsal says "2 jobs submitted" and the
plan then fails at its run step.

**What is wrong.** `PipelineOrchestrator.plan_run` checks the preflight (`pipeline_orchestrator_service.py:519`) and
describes the inputs; Run also resolves every input of the jobs it submits (`resolve_all_paths`, `:660-688`) and
refuses on a `PathResolutionError`. `plan_run` never calls it, so a required input that resolves to nothing passes.

**Evidence.** Plan `p-cb7f8e82` (AGG412_STA, H18's input) rehearsed clean; its step 5 failed with "Path resolution
failed for warpExport … for reconstructParticle".

**Fix.** In `plan_run`, resolve each job's inputs as Run does (no allocation: the pending job dir) and list a
`PathResolutionError` among `refusals`.
**Check.** H18's input, before H18 is fixed: `plan_run` refuses naming both jobs.

### H24 — Create particle layer fails in the companion napari viewer once crboost's layers are added

**Fixed 2026-11-19 (uncommitted; two of the three fixes are in the colleague's viewer, not this repo).** In
`napari_mrc_lowpass_fast.py`, the File and Particle layer dropdowns now take their choices from callables
(`_file_choice_names`, `_particle_layer_choice_names`), and `_restore_particle_coordinates_for_current_path` clears
each particle layer's selection before swapping in the next tomogram's points. The dev clone is fixed. The lab copy
(`/groups/klumpe/software/Napari/Napari_crboost`) waits for its file to be copied over. The upstream repo has the
same lines, in `napari_mrc_lowpass.py` too. On the crboost side, `containers/napari/open_tomogram.py`
(`log_napari_errors`) now writes every error napari shows into the session's `slurm.log` with its traceback.

**Status:** fixed 2026-11-19. A lab member reported it the same day as an error "regarding dimensions", and the
maintainer hit it in a session. **Severity:** high. Creating a particle layer is the viewer's main job.

**What is wrong.**
1. napari re-reads every docked magicgui widget's choices (`reset_choices`) whenever a layer is added or removed. Both
   dropdowns were made with a fixed list, so every added layer reset them to "No particle layers" / "No files
   loaded". The viewer had no layers of its own after a particle layer, so this never showed. crboost's bundle
   (segmentations, picks) and the 3-D surfaces add layers after it. Create on a name that already exists then sets
   the dropdown to that name and raises `ValueError: 'ribosome' is not a valid choice. must be in ('No particle
   layers',)`. A project with saved STARs always has one: the viewer loads them when it opens.
2. napari 0.10 keeps `Points.selected_data` when `layer.data` is replaced. After a switch to another tomogram, a
   selected point indexes past the new data: `IndexError` in `_apply_particle_layer_style` (`current_border_width`),
   raised out of `load_mrc`. The layers after the failing one keep the previous tomogram's points, and the next save
   can write them under the new tomogram's name.
3. napari's own traceback view fails ("could not get source, probably due to dynamically evaluated source code"),
   and text on the VNC desktop cannot be copied, so nothing reached the user but that line.

**Evidence.** The maintainer's live session (job 1808177, Kilifish_collection1 Position_7_2 with 9 segmentations and
an empty ribosome STAR saved the night before): a screenshot of the display showed the dropdown at "No particle
layers" and the status at "1 particle layer(s)". `~/.crboost/napari_bundletest/drive_kili2.sh` (`STARS=` a copy of
that project's `Curation/napari/particle_stars`) reproduces the ValueError. `drive_create.sh` reproduces the
IndexError on a tomogram switch.

**Check.** Both scripts pass every step against the dev clone. The dropdowns keep their entries through the bundle
and 3-D, and no tomogram reloads on its own. `drive_log.py` shows a button's exception in the log.

### H25 — Closing the branch map closes the job roster too

**Fixed 2026-11-19 (uncommitted).** The Jobs view opens as the roster alone. The branch map is built and shown
only when the roster's map icon is clicked (`PipelineBuilderPanel.show_map`, which mounts it on the first click).
The pipeline frame's cross, and Esc on it, hide only the map again (`_set_map_hidden`, `ui/workspace_page.py`), and
the roster stays. Without the map, the frame comes for a job page and goes again when it closes (`_frame_on`). The
map's pollers stop while it is hidden (`sync_map_visible`). Every workspace opens without it; nothing is kept.

**Status:** fixed 2026-11-19, reported by a lab member the same day. **Severity:** medium. The map could not be put
away without losing the job list, so a user who prefers a fixed list (a CryoSPARC-like layout) had no way to get
one. The maintainer's call: the map never opens unless asked for.

**What is wrong.** Without a job open, the pipeline frame's cross called `_hide("pipeline")`. The roster is that
view's navigation (`RosterWidget.set_active_views`), so it went too. And the frame showed the map whenever no job
was open, so every project opened on it.

**Check.** In a browser: a project opens on the roster alone, with "No job open." beside it. Open a job → its page.
Close it → the roster alone. The map icon → the map. Its cross → the roster alone, the roster unchanged.

### H21 — A plan says "at automatic" for a set_input that only chooses where a set's images come from

**Fixed 2026-11-18 (uncommitted).** `api/plans.py`: a facet-only `set_input` step reads "Take {job}'s {slot} from …" (`_facet_text`: "each project's own warpExport (…)", or "<job> of <project> for …"); a step with a source and facets names both; the overview gets an "<slot> sources" row per job (`_facet_rows` in `_snapshot`). **Checked:** `_snapshot` of AGG412_STA's refine3d (job015) reads "each project's own warpExport (Slot6, FCIsolation, unmilled2, Grid2, SK reruns)"; class3d__7 "warpExport of AGG412_STA for …Grid2_rerun".

**Status:** fixed 2026-11-18, found the same day. **Severity:** low for the result, medium for trust: what the user accepts is not
what the plan does. The job is right; the plan's words and its overview are not.

**What is wrong.** `api/plans.py:158` words a `set_input` step as `a.source or a.path or "automatic"`, so a step that
gives only `facet` (which kitchen-set source each project's images come from) reads "Point …'s input
input_optimisation at automatic". `job_params.set_input` (`services/job_params.py:350`) keeps the source and only
records the choice. The overview's table lists no facet choice at all, so the dialog never shows where a job's
particle images will come from, which is the one choice Run refuses without.

**Evidence.** Plan `p-f6ba241c` (AGG412_STA, nine Class 3D): steps 11–19 read "Point Class 3D's input
input_optimisation at automatic"; the jobs submitted reading their sets, and their `input_set/particles.star` take
every image from the chosen export (job005: 24,100 rows from job001; job013: 1,874 from job003).

**Fix.** Word a facet-only step "Take {job}'s {facet} from {source label} for {project names}" (or "for the whole set"
for `*`), and add a row per job and facet to the overview: "Images · from Warp Export (job001) · 8 projects".
**Check.** The same plan's steps 11–19 and its overview name job001 / job003.

### H22 — An image choice on a class set is accepted, rehearsed and ignored

**Fixed 2026-11-18 (uncommitted).** `job_params.facet_refusal` (blocking; called in a thread by the `set_input` handler, `api/handlers.py`, and by the plan rehearsal, `api/plans.py`) refuses facet choices on an input that reads no set or a set `reads_by_facets` turns down, naming the way out (a Warp Export reading it or the job that refined it). The UI's facet lines only ever showed for kitchen sets, so it was API-only. **Checked** on AGG412_STA read-only: s012 (class set) refused with the message, s011 (kitchen set) passes, a Warp Export source refused as "reads no particle set".

**Status:** fixed 2026-11-18, found the same day. **Severity:** high for the result: the job runs on other images than the plan
named, and the first sign is a later job failing on a size mismatch.

**What is wrong.** `job_params.set_input(facet=…)` (`services/job_params.py:350`) records facet choices on any set.
Only a kitchen set of picks is read through facets (`particle_sets.reads_by_facets`, `services/particles/particle_sets.py:397`:
`op == POOL`, level COORDINATES, a uid column); a class set (`op class_subset`, level extracted) is read as written, so
its images stay the Class 3D's. Neither `set_input` nor the plan's rehearsal says the choice does nothing.

**Evidence.** Plan `p-e1b4bf07` (AGG412_STA): Refine 3D job015 read set s012 (job013 class 2) with the five reruns'
bin-2 Warp exports chosen per project; it ran on job003's bin-4 images (`run_model.star`: 6.2 Å, box 128), RELION
resizing the bin-2 reference and mask down. PostProcess job017 then failed: "The mask is (256, 256, 256) at 3.1 Å, the
half maps (128, 128, 128) at 6.2 Å".

**Fix.** `set_input` refuses `facet` on a set `reads_by_facets` says no to ("a class set takes its Class 3D's images; to
cut them again, a Warp Export reading the class set or its Refine 3D"), and the plan's rehearsal refuses the step.
**Check.** The same plan is refused at its step 7.

### H23 — The kitchen's "in set" filter finds none of a class set's picks when they came from other projects

**Fixed 2026-11-18 (uncommitted).** `setmatch.set_rows` matches a set by its rows' `cbPickUid` (`read_uid_column`; rows without a trustworthy uid are counted, never matched by position) before falling back to positions. **Checked:** all 438 of s012's uids are in s011's `kitchen_uids.txt`, the same uid space `table.uid` holds, so the filter keeps 438; the live kitchen check (plan_kitchen_set with `set s012 in`) waits for the server restart.

**Status:** fixed 2026-11-18, found the same day. **Severity:** medium: a class set cannot become a kitchen set (picks with facets),
so its picks cannot be cut again at another binning from their own projects.

**What is wrong.** `setmatch.set_rows` (`services/sta_kitchen/setmatch.py`) matches a set without `kitchen_uids.txt` by
position, against the home project's own members only. A class set of a Class 3D that read a kitchen set has no uid
list, though its rows carry `cbPickUid`, and its picks lie in the eight reruns, not the home: 0 matched.

**Evidence.** `plan_kitchen_set(AGG412_STA, 412_particle, filters=[set s012 in])`: 0 picks; s012 holds 438 rows with
`cbPickUid`.

**Fix.** Before matching by position, `set_rows` takes the set star's `cbPickUid` column when it has one (as the uid
list). **Check.** The same call keeps 438 picks over 5 projects.

### H14 — missAlign's effective SLURM config drops the profile's GPU type, so the training job lands on any card

**Status:** fixed 2026-11-10 in `e95c07f` (`gres_with_count` keeps the profile's type; the dev conf, `conf.template.yaml` and the lab copy name `gpu:nvidia_b200:1`). Found 2026-11-10 (roadmap 49 R1; anchors at `3c0cc3e`). **Severity:** high on CBE.next: the run's
length follows the card it happens to get, and the profile has no way to choose one.

**What is wrong.**
- `MissAlignParams.get_effective_slurm_config` (`services/jobs/miss_align.py:806-807`) replaces the profile's `gres`
  with `gpu:{num_gpus}` whenever the job carries no `gres` override, so `job_resource_profiles.missAlign.gres` in
  `conf.yaml` can never name a card. Every other GPU profile pins one by a typed GRES (`gpu:nvidia_b200:1` on
  tsReconstruct) or a constraint.
- A `constraint: b200` would not do either: the `b200` feature is on the MIG nodes too (`cbe-g5-1/2/5/6`, features
  `b200,mig`), where `gpu:2` is two 23 GB slices, and SLURM constraints cannot exclude a feature. Only the typed GRES
  (`gpu:nvidia_b200:N`) picks a whole card.
- `gpu_count()` (`:647-657`) already reads a typed override (`gpu:nvidia_b200:2` → 2) and the driver reads
  `SLURM_GPUS_ON_NODE`, so the type only has to survive the effective config.

**Evidence** (real jobs, read only, 2026-11-10). R1's missAlign, SLURM 1526891
(`AGG412_20251113_412_rerun/External/job006`): `ReqTRES gres/gpu=2`, no features, allocated
`gres/gpu:quadro_rtx_6000=2` on `cbe-g3-1` while every full B200 was taken. It trains at 2.29 steps/s with GPU 0
at 100 % in every sample: the RTX 6000 is the limit, and CPUs would not help. The same job on one B200 with half the
CPUs (SLURM 1514330) runs 3.65–4.09 steps/s with its trainer at 24 % of the card. To repeat:
`MissAlignParams().get_effective_slurm_config().gres` with the profile's `gres` set to `gpu:nvidia_b200:1` → `gpu:1`.

**Impact.** A PAPER run over ~140 series: ~2.7 h of training per macro-iteration on the RTX 6000 against ≤ 1.6 h on
a B200, nine times over; R1 is heading for 27–31 h instead of a working day (roadmap 50 §3). A `gres` override by
hand on every job is the only way round it today.

**Fix.** Keep the profile's type and set only the count: read `cfg.gres` as `gpu[:type][:count]` and write back
`gpu[:type]:{num_gpus}`; an override stays untouched. The profile then reads `gres: gpu:nvidia_b200:1` in the dev
conf, `conf.template.yaml` and the lab conf (roadmap 50 S1 sets its CPUs and memory with it).

**Check.** Toy: profile `gpu:nvidia_b200:1` and `num_gpus` 2 → `gpu:nvidia_b200:2`; profile `gpu:1` → `gpu:2`; an
override `gpu:nvidia_a100-sxm4-40gb:2` → unchanged; `gpu_count()` 2 in each case. Real: the next missAlign's `sacct`
shows `gres/gpu:nvidia_b200=2`.

### H15 — missAlign's training pool streams through the shared BeeGFS: tens of terabytes a run for a 5 GB working set

**Status:** fixed 2026-11-10 in `e95c07f` (`pool_root()` in the driver: `/dev/shm/crboost-<user>-<job id>` as the tool's `TMPDIR`, removed after training; `pool_size × 5.3 MB` added to the memory request). Found 2026-11-10 (anchors at `3c0cc3e`). **Severity:** high for the cluster and for the run: every
run writes and reads its training cubes through the site's shared scratch at about 1 GB/s for the whole training
phase, and the builders and loaders wait on it.

**What is wrong.**
- The tool keeps its pool of training cubes under `tempfile.gettempdir()` (in the container,
  `miss_alignment/data/training_datamodule.py:205`: `recon_pool_<hash>`): each patch builder writes every 96³ triplet
  as a 5.3 MB pickle (`_reconstruction_worker.py:232`), each loader lists its partition, reads one file and deletes it,
  per sample. crboost sets no `TMPDIR` for the tool (`drivers/miss_align.py:706-709` sets USER, thread and dynamo
  variables only; `--cleanenv` drops the host's), so the pool goes to the container's `/tmp`.
- On CBE.next a job's `/tmp` is not local disk: it is the job's private directory on BeeGFS (roadmap 45 §2; `df /tmp`
  inside both running jobs → `beegfs_nodev`, 1.3 PB). The node has no local disk (`TmpDisk=0`); its RAM-backed
  `/dev/shm` (756 GB tmpfs, charged to the job's `--mem`) is the only node-local store. The container sees the job's
  private one (probe 2026-11-10 in SLURM 1514330: a directory made host-side was listed inside `apptainer exec`, a
  file made inside appeared outside, the tool's own `sem.mp-*` semaphores sit there), and SLURM's `namespace/tmpfs`
  removes it at job end.

**Evidence** (`sstat`, 2026-11-10 ~21:00). SLURM 1514330 (one B200, 3 builders, 4 loaders): `fs/disk` 26.2 TB after
7 h 22, about 1 GB/s (3.7 steps/s × 32 files × 5.3 MB, written once and read once); SLURM 1526891 (R1, two RTX
6000s): 2.8 TB after 1 h 27. `top` inside both jobs showed loaders and builders in `D` (waiting on I/O); the pools
held 135 files / 0.7 GB and 991 files / 4.9 GB. Roadmap 45 §3 had noted "missAlign's 5 TB is its training cubes in
`/tmp`" without the filesystem behind it.

**Impact.** Every missAlign run puts a sustained ~1 GB/s of churn on the shared scratch for 10–25 h, for a working
set of 5 GB; the producers wait on that churn, part of why a B200 trainer holds 24 % of its card (roadmap 50 §1).
This is the kind of traffic the site asked the lab about (roadmap 45 §1).

**Fix.** The driver makes `/dev/shm/crboost-<user>-<SLURM job id>` before training, passes `TMPDIR=<that>` in the
tool's environment prefix (the tool's `tempfile.gettempdir()` follows it), removes the directory in its `finally`
(the tool deletes its own pool on teardown; the job's namespace goes at job end anyway), and the estimate and the
SLURM tab note add the pool (`pool_size × 5.3 MB`, 16 GB at 3000) to the memory request. The directory must exist
first: `tempfile` falls back to `/tmp` silently when `TMPDIR` does not.

**Check.** Toy (done 2026-11-10 in the container): `TMPDIR=/dev/shm python -c "import tempfile;
print(tempfile.gettempdir())"` → `/dev/shm`; without it → `/tmp`. Real: the next run's `run.out` says `Created pool
directory: /dev/shm/crboost-…/recon_pool_…`, `sstat`'s `fs/disk` stays in the tens of GB (the stacks read), no process
in `D`, and steps/s against the same layout with the pool on BeeGFS.

### H12 — The startup banner tells you to kill a running server's workers

**Status:** fixed 2026-11-10 in `3585c73`. Found
2026-11-10. **Severity:** high: every start prints a `kill -9` line for the workers of a server that is running,
and following it takes that server's render and preview workers away.

**What is wrong.** The port survey knows a server by the script in its command line, `crboost` or `main.py`
(`services/port_survey.py:207`). Since the sh launcher (roadmap 39), a server runs `python3 …/crboost.py`, so no
server started since then is known as one. Its workers then have no server for a parent and are listed as "CryoBoost
worker whose server is gone", with `End them with: kill -9 …`; the server itself is left out of the survey. CryoBoost
v1 has no port survey: this is new with the launcher.

**Evidence** (real processes, read only). 2026-11-10 on cbe-login-0: a test server's banner listed 24292 and 24293
as leftovers; `ps -o ppid` gives 23406 for both, the lab install's server, running on port 24330 since 14:10.

**Fix.** `crboost.py` joins the names the survey knows a server by.

**Check.** `survey()` on that node lists 23406 as a server on 24330, with 24292 and 24293 as its workers and no
leftovers; a second server's banner names neither.

### H7 — Template matching with T, O or I symmetry searches 3 to 15 times the orientations its increment asks for

**Status:** fixed 2026-11-09 in `c27416f` (D9 settled that day; the fix is below the evidence; the tab's count
and note followed the same day), found 2026-11-09 (roadmap 45,
the 412 pilot; anchors at `edebd13`). **Severity:** high for cost, none
for correctness so far: every I1 search runs 3,398 orientations where its 12° needs about 225, and the search is most
of a template-matching task's GPU time. The extra orientations sample finer, so results so far are valid; a fix
changes scores and candidates, so the increment to use afterwards is the scientists' choice.

**What is wrong.**
- `_reduce_to_asymmetric_unit` (`services/templating/angle_lists.py:99-128`) drops a grid orientation only when an
  earlier one has the same orbit key, a rounded quaternion. Two points of the ZXZ grid (`_uniform_so3_grid`, `:74-96`)
  share a key only when a group element maps one exactly onto the other, and on that grid only the rotations about z
  and the 2-folds do. So the cut is the order of that subgroup, not of the group: at 12° the 13,500-point grid becomes
  3,484 (D2), 2,333 (D3), 1,430 (D5), 1,175 (D6), which is right, but 3,448 (T), 3,439 (O) and 3,398 (I1, I2) against
  1,125, 562 and 225.
- The module promises "|G|× faster search" (`:14-18`), and `expected_angle_count` (`:158-166`) has the right number:
  every such job logs both, e.g. "symmetry=I1: wrote 3398 angles (estimate 225)" in the 412 pilot's job007 `run.out`
  (`drivers/template_match_pytom.py:502`).
- Hidden behind it, and live once the cut works: the group is scipy's icosahedral group whatever the template's
  axes (`_scipy_group_name`, `:63-71`, maps I1 and I2 to the same group, and the module calls the orientation moot,
  `:20-27`), and it composes on one fixed side (`sym_group * rot`, `:113`). A list cut to one rotation per orbit
  covers every orientation only for the template's own axes, composed on the side pytom applies a listed rotation;
  RELION's I1 and I2 differ by a 90° turn about z. Today's lists keep so many rotations that it does not show.
- Beside it: an increment that does not parse becomes 12° without a word (`drivers/template_match_pytom.py:494-497`).
- CryoBoost v1 had no such list: it allowed only Cn symmetry for pytom (`src/templateMatching/pytomTm.py:71-73`
  there), so this is new, not inherited.

**Evidence** (the real functions, and the pilot's list read without writing). From the checkout:

```
./crboost python -c "from services.templating.angle_lists import generate_asymmetric_unit_angles as g, expected_angle_count as e; print(len(g('I1', 12)), e('I1', 12))"
```

prints `3398 225`, as the pilot's `angles_I1.txt` has 3,398 lines. For 20,000 random orientations, the angle to the
nearest symmetry image of a listed one: the pilot's list, median 2.3–2.9°, 95th percentile 3.7–4.6°, at most 5.0–6.4°
(the range covers both sides and both 2-2-2 settings). pytom's own sampling for a plain increment
(`pytom_tm.angles.angle_to_angle_list`: HEALPix view directions times in-plane steps), cut to the rotation nearest
the identity in each orbit, keeps 148 of 9,000 at 12° (median 6.0°, 95th percentile 8.6°, against 5.8° and 7.9°
uncut), 578 of 34,560 at 8° (3.8°, 5.2°) and 2,124 of 124,416 at 5° (2.4°, 3.4°). So the pilot's list samples like an
increment of about 5°, which a correct list reaches with 2,124 rotations. The crboost grid cuts unevenly (451 at 12°,
3,512 at 5°): its points crowd at the poles of the Euler angles.

**Impact.** The search took 63.5 of a template-matching process's 112 s in roadmap 45's P7; with 225 orientations it
would take about 4 s. The 412 pilot's template matching held B200s for 8.71 GPU-h, the baseline week's 7.0 (roadmap
45 §3), most of it this search. Projects with T or O templates search about 3 and 6 times what they ask for.

**Fix.** Start from pytom's own sampling at the increment and keep, in each orbit, the rotation nearest the identity:
about a |G|th of the list, with the uncut list's coverage. Use the template's own axes (I1 or I2 as RELION means them)
and the side pytom composes on, as roadmap 45's P9 measures them with pytom's own rotation
(`docs/reports/gpu-util/scripts/tm_angle_lists.py`). Show the count beside the increment in the template-matching
tab, and say there that searches with these groups used to sample finer than their increment, about 5° at a 12°
setting, so a project can keep that sampling by choosing it.

**Check.** The I1 list at 12° has about 150 rotations and covers within half a degree of pytom's uncut 12° list. P9's
tomograms searched with the old list and with correct lists at 12°, 8° and 5°: candidates and scores side by side,
before a project changes over.

**Measured, and what holds the fix** (roadmap 45 §11 P9, §8 D9). On a B200 a 412 tomogram took 119 s with the old
list, 95 s with a correct 5° list (sampling at least as fine), 66 s at 8° and 58 s at 12°. The 412 picks are too
close to pytom's cut-off to tell whether a coarser list loses particles. The maintainer's conditions are in D9:
every increment stays available, nothing narrows the choice unless it is plainly better and loses nothing, and the
account of where lists come from comes before the fix.

**Fix (2026-11-09, D9: the maintainer's answers).** `services/templating/angle_lists.py` now takes pytom's own list
for the increment (`angle_to_angle_list`, ported with its HEALPix RING step: equal to pytom 0.14's to 1e-15 at 3°–90°)
and keeps, per symmetry orbit, the rotation nearest the identity, the symmetry composed on the right; I1 is scipy's
setting, I2 that turned 90° about z. The I1 lists at 12°, 8° and 5° are P9's, rotation for rotation (148, 578,
2,124). An increment coarser than the fundamental region (90° in I or O) keeps the identity alone. An unparsable
increment is refused (`parse_increment`), no longer 12°. New template-matching jobs with I1 or I2 start at 5°
(`DEFAULT_INCREMENT_FOR_SYMMETRY`, pipeline builder and protocol apply): about the sampling the old "12°" lists had,
20 % faster (P9). An existing job keeps its increment, which now means what it says: an I job saved at 12° searches
148 rotations, not 3,398. T and O were also over-sampled before (3,448 and 3,439 at 12°; about 8° and 7° like for
like) and keep the 12° default.

The template-matching tab says under Symmetry about how many rotations the search tries (pytom's list over |G|:
2,074 for I1 at 5°, where the list itself has 2,124) and, for T, O and I, what 12° searched before and the
increment that samples as finely now; an increment that is not a number is flagged there. Switching the symmetry
to or from I1 or I2 moves an increment still at its old default to the new one's (`set_symmetry`,
`services/jobs/template_match.py`); a typed increment stays.
