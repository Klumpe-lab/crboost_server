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

## Fixed

*(none yet)*
