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
