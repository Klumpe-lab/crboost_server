# Roadmap 18 — SerialEM/K3 tilt stacks from the landing page

**Status:** scoped 2026-09-09 (revised the same day: dose is estimated from the data, no protocol bundle);
**S0–S4 CODE-COMPLETE 2026-09-09, PENDING THE RUNTIME PASS (§6).** Static check script for S1:
`notes/k3_ingest/check_s1_scan.py`. Analysis, dry run and decisions of record: `notes/k3_ingest/00-plan.md`
(+ `01-current-data-shape.md`, `02-k3-data-shape.md`). Branch `k3_implementation`.
**Risk:** low-medium. Two new persisted fields, one new utility, no new `JobType`, no format switch.
Two fixes in here change behaviour on every project (S0) and are their own commit.

## 1. The concrete goal

> "Ingest a K3 project from the main page … then this project should be able to be registered,
> reconstructed and its statuses tracked seamlessly just like with the other projects."

One sentence: **point the landing page at a SerialEM delivery folder, press Scan, press Create → a regular
project.** Everything after Create is the pipeline as it is today.

Success, end to end, on the sample `/groups/klumpe/crboost_data/area_5_a_ts_00{2,3,4,7}/`:

1. Type `/groups/klumpe/crboost_data` (the folder holding the four `area_5_a_ts_*` subfolders) into the
   data field, press Enter. The scan finds the four stack mdocs one level down, ignores the 156 per-movie
   `*.tif.mdoc`, and the overview reads: 4 tilt-series × 39 tilts, pixel 1.382, 300 kV, tilt axis 85.0,
   source line `SerialEM 4.1.10 · aligned stacks (.mrc, 39 slices/series) · 5760×4092 · dose: estimated
   4.3 e⁻/Å² (not in mdoc)`. No "unrecognized name" warnings. The dose field is prefilled with `4.3` and
   marked *estimated*; the user can overwrite it.
2. Create runs: the spinner counts `splitting stack 2/4`, then the workspace opens.
3. The project is ordinary: `frames/` holds 156 single-frame `.mrc` (real files), `mdoc/` four copies named
   `<project>_area_5-A_ts_00X.mdoc`, `registry/` four TS, roster Dataset section `Format .mrc · Source
   layer SerialEM stacks (split) · Dose/tilt 4.3 (estimated)`.
4. Run `importmovies → fsMotionAndCtf → tsImport → aligntiltsWarp → tsCtf → tsReconstruct` from the roster.
   Settings show `--extension '*.mrc'`, no `--eer_ngroups`, no gain; `tomo_dimensions` 5760x4092x2048;
   `out_average_halves` and `halfmap_frames` came up as 0 (stacks have no halves); four tomograms; Journey
   and gallery work; statuses track as on any project.
5. The same steps on a Tomo5/Falcon folder behave exactly as today.

Non-goals (decided 2026-09-08, unchanged): raw K3 `.tif` movies, gain/defect files, a detector or format
switch, CryoCARE on stack projects (no even/odd halves exist — stated, not worked around). And, decided
2026-09-09: **no K3 protocol bundle** — protocols stay a pristine registry of flows that are known to
work; defaults the user dislikes they change on the job tab.

## 2. What breaks today, and two new findings

The full integration table is `notes/k3_ingest/00-plan.md` Part 3. In one line each, with the anchor:

| # | symptom on the sample | where |
|---|---|---|
| 1 | mdoc named `area_5-A_ts_002.mrc.mdoc` is dropped ("unrecognized name") | `services/configs/dataset_parsing_service.py:66-70` |
| 2 | tilt axis read as 175.04 (RotationAngle) instead of 85.0 — the `[T = Tilt axis angle = …]` line is split on the first `=` | `dataset_parsing_service.py:278-324`, `services/configs/mdoc_service.py:54-61` |
| 3 | `ExposureDose = 0` → pydantic error at Create (`ge=0.1`); the moment 0 is treated as unknown it would silently become the model default 3.0 | `dataset_parsing_service.py:304-310`, `services/models_base.py:255` |
| 4 | mdocs live one folder down (`<root>/<ts>/`), the widget only globs `<root>/*.mdoc`; frames are only looked for in one flat dir | `ui/data_import_panel.py:496-518`, `dataset_parsing_service.py:216-221`, `services/scheduling_and_orchestration/project_service.py:53,78-85` |
| 5 | the data field appends `*.eer` and Create demands that glob to match — no `.tif`/`.mrc` dataset can be created without editing `conf.yaml` | `ui/data_import_panel.py:250-274, 800-811` |
| 6 | 39 `*.tif.mdoc` (no `[ZValue]`) would each count as a tilt-series | `dataset_parsing_service.py:48-49` |
| 7 | `--eer_ngroups -32` emitted for every input | `drivers/fs_motion_and_ctf.py:114` |
| 8 | `tomo_dimensions` default 4096x4096x2048; `ImageSize` parsed and discarded | `services/jobs/ts_import.py:59`, `ts_alignment.py:64`, `project_service.py:385-405` |
| 9 | `--halfmap_frames`/`--deconv` are Warp *switches*: value 0 still enables them; a stack project dies in Warp with "Can't find half-averages" | `drivers/ts_reconstruct.py:57-58` |
| **10 (new)** | the tilt-series XML key parser strips `_EER.eer`, `.tif`, `.eer` but **not `.mrc`** → every tsCtf frame lookup would miss on a stack project (`frame_by_warp_key` gets `…_-10.0.mrc`) | `services/configs/metadata_service.py:90-96` vs `services/tilt_series/models.py:40-44` |
| **11 (new)** | the protocol create dialog takes dataset facts from `get_autodetect_params`; a missing dose is simply absent from `detected` and the project silently runs on 3.0 | `services/protocols/apply.py:139-151`, `ui/protocols_dialog.py:150-160` |

Cosmetic, noted for the runtime pass: Journey position label falls back to `rsplit("_", 1)[-1]` → "002"
for `<proj>_area_5-A_ts_002` (`services/dashboard_data.py:146-150`); the roster/array helpers already
fall back to the raw name (`services/array_tasks.py:25-66`).

## 3. Decisions (2026-09-09)

**D1 — the source layer is inferred per mdoc; no switch.** The scan classifies each tilt-series:

- *movies*: every `SubFramePath` basename resolves in the frames dir, else beside the mdoc;
- *stack*: `<mdoc dir>/<ImageFile>` exists, is an MRC, and `nz == number of [ZValue] sections`;
- both complete (the K3 delivery ships stack + tifs): **SerialEM dialect → stack; Tomo5 → movies.**
  Tomo5 stacks are unaligned sums nobody processes; SerialEM's are the frame-aligned data its users mean.
  The overview's source line names the chosen layer and says the raw movies were left alone, so a wrong
  call is visible before Create. This is the one judgment call in the roadmap. If it turns out wrong
  for a SerialEM+EER site that saves stacks, the fallback is a segmented `[stacks | movies]` control
  shown only when both layers are complete — not a detector switch.

**D2 — the split happens at Create, inside the existing threaded import.** Not a CLI pre-step (the
user should not leave the landing page), not a background task (a project that is "still importing"
is a new state every view would have to know). `_setup_project_data_sync` already runs in a thread;
it gains a per-mdoc branch and a progress callback that feeds the Create spinner (`splitting stack
2/4`). Cost: one read + one write of each stack (1.8 GB/series here) on the headnode over GPFS —
minutes for the sample, measured in the runtime pass. Escalation if a real delivery makes that
painful: the sbatch prototype in `notes/k3_ingest/dryrun_k3_stacks.sbatch` stage 1 already produces
the flat "frames + mdocs" layout the landing page then imports like any dataset.

**D3 — two durable facts, one schema bump (3.7 → 3.8):** `ProjectState.import_source_kind`
(`""` legacy / `"movies"` / `"stacks"`) and `acquisition.dose_per_tilt_source` (`""` legacy /
`"mdoc"` / `"estimated"` / `"user"`). Both read with defaults so old projects load unchanged. They
drive the roster rows, the job-init stamps of D5 and the tsReconstruct tab hint — nothing else.
Everything else is already recorded (`import_frame_extension`, `acquisition.acquisition_software`,
`acquisition.detector_dimensions`).

**D4 — a missing dose is ESTIMATED from the data, shown as such, and never blocks.** (Revised: the
acquirer's number replaces it when it arrives; until then the estimate is the working value.) The
mdoc carries per-tilt `DoseRate` (e⁻/px/s at the detector) and `ExposureTime`, so the *transmitted*
dose at low tilt is `DoseRate × ExposureTime / PixelSpacing²` ≈ 2.2 e⁻/Å² on the sample. That is the
dose that came *through* the lamella, not the dose the specimen received. Because the lamella
attenuates as `exp(−t / (λ·cos(θ − θ₀)))`, a straight-line fit of `ln DoseRate` against
`1 / cos(θ − θ₀)` over the tilt series extrapolates to zero thickness: the intercept is the incident
dose rate, the slope the thickness, `θ₀` the lamella pretilt. Measured on the four sample series
(2026-09-09, `notes/k3_ingest/00-plan.md` "Dose estimate"):

| series | transmitted @ 1° | incident (fit) | pretilt | t/λ (t at λ = 350 nm) | fit rms (ln) |
|---|---|---|---|---|---|
| ts_002 | 2.28 | 4.32 | −8.5° | 0.63 (221 nm) | 0.006 |
| ts_003 | 2.15 | 4.22 | −8.5° | 0.67 (233 nm) | 0.005 |
| ts_004 | 2.19 | 4.21 | −7.5° | 0.66 (230 nm) | 0.014 |
| ts_007 | 1.90 | 4.30 | −8.5° | 0.81 (283 nm) | 0.005 |

Four lamellae of different thickness (transmitted 1.9–2.3) agree on the incident dose within
4.2–4.3 e⁻/Å²/tilt, the fitted pretilt matches PACEtomo's `pretilt = -10` setting and AreTomo's +7°
tilt offset, and the residual is under 1 %. **Working value: 4.3 e⁻/Å²/tilt (≈168 e⁻/Å² over 39
tilts).** The systematic that remains is SerialEM's counts→electrons calibration (CountsPerElectron),
which the acquirer's number settles.

Mechanics: `estimate_incident_dose(tilts) -> DoseEstimate | None` (pure function in `mdoc_service`:
`transmitted_low_tilt`, `incident`, `pretilt_deg`, `t_over_lambda`, `rms`, `n_tilts`) runs when every
selected section has `DoseRate`, `ExposureTime` and `TiltAngle`, at least 10 tilts spanning ≥ 30°, and
the fit rms is below 0.05; otherwise only the transmitted value exists. The landing field prefills
with `incident` (else `transmitted_low_tilt`), hint `estimated from DoseRate×ExposureTime/px² with a
zero-thickness fit — transmitted 2.2, incident 4.3, pretilt −8.5°, ~230 nm; the acquirer's number
replaces it`; `dose_per_tilt_source` = `"estimated"`, or `"user"` when edited, `"mdoc"` when the mdoc
had it. Tomo5 data (dose in the mdoc) is untouched.

**D5 — no protocol bundle; the data determines what it can, the user owns the rest.** Stamped at job
init (`ensure_job_initialized`, precedent `rescale_angpixs`): `tomo_dimensions` from
`detector_dimensions` (every project; Falcon still yields 4096x4096x2048), and on
`import_source_kind == "stacks"` `fsMotionAndCtf.out_average_halves = False` and
`tsReconstruct.halfmap_frames = 0` — a single-frame input has no even/odd halves, this is a fact, not
a preference. Handedness (`tsCtf.defocus_hand`, default `set_flip`) and the CTF range stay the user's
knobs with the code defaults; for the record Warp's `--check` on the sample said *no flip* and the
high-tilt CTF divergence wanted `30:10` (`notes/k3_ingest/00-plan.md` Results), both to be set on the
tab in the runtime pass. The tomo dashboard's existing handedness cross-check is the safety net.

**D6 — naming.** TS id = `<project>_<mdoc name minus ".mdoc" minus a trailing ".mrc"/".st">`
(`<proj>_area_5-A_ts_002`); frame file = `<project>_<basename(SubFramePath) with ".mrc">`
(`…_area_5-A_ts_002_area_5-A_ts_002_001_000_-10.0.mrc`). `Frame.id` = single-suffix stem, so the
`-10.0` survives and matches Warp's `GetFileNameWithoutExtension` key. TS ids never contain a dot
(the `.task_status/<ts>.ok` stem rule). Original mdoc text is kept verbatim except `SubFramePath`.

## 4. Design, by layer

### 4.1 One mdoc interpretation — `services/configs/mdoc_service.py`

`MdocFacts` (frozen dataclass): `software` (`"SerialEM"`/`"Tomo5"`/`""`), `software_version`,
`pixel_size`, `voltage`, `dose_per_tilt` (**None** when absent or 0), `dose_estimate: DoseEstimate | None`,
`tilt_axis`, `detector_dimensions` (`ImageSize`), `image_file`, `n_sections`.
`acquisition_from_mdoc(parsed) -> MdocFacts` reads:

- header `key = value` lines as today;
- `[T = …]` lines: strip the brackets and the leading `T =`, split on `,`/two-plus spaces into
  `key = value` pairs → `Tilt axis angle = 85.0`; a `[T = SerialEM: …]` or bare `T = SerialEM:` line
  sets `software`; `Version = SerialEM Version 4.1.10 …` gives the version; `[T = Tomography_5.23.0…`
  gives `Tomo5` + `5.23`.
- tilt axis: SerialEM → `Tilt axis angle` (falls back to `abs(RotationAngle)` only if the line is
  missing); Tomo5 → `abs(RotationAngle)` as today (runtime-verified, untouched).
- `estimate_incident_dose(sections)` per D4 when `dose_per_tilt` is None.

Consumers: `dataset_parsing_service._extract_acquisition_params` (deleted, calls this),
`get_autodetect_params` (keeps its dict shape, filled from `MdocFacts`; the dead
`"SerialEM" in header_data.get("", "")` test at `:54` goes), `apply_protocol`'s facts, and the split.
`build_from_mdocs` (`services/tilt_series/build.py:160`) gains `dose_per_tilt_fallback: float | None`
used when the mdoc's `ExposureDose` is absent/0, so the registry's per-frame pre-exposure accumulates
the project's dose instead of 0. `invert_tilt_angles` is left exactly as it is (computed at `:110-113`,
never persisted, model default `False` is what every project runs with).

### 4.2 The scan — `services/configs/dataset_parsing_service.py`, `services/tilt_series/preimport.py`

`TiltSeriesInfo` gains `ts_name: str`, `stage_position: int | None`, `beam_position: int | None`
(decoration from `parse_position` when the name carries it), `source_kind: Literal["movies","stack",
"missing"]`, `stack_path: Path | None`, `stack_nz: int | None`, `acquisition_software`,
`software_version`, `detector_dimensions`, `dose_estimate`. `ts_label` returns `ts_name` (today it is
computed as `Position_{stage}` → every K3 series would be `Position_0` and collide in the panel's
`row_registry`). `missing_frames` counts only for `source_kind == "movies"`.

`parse_dataset`:
- the name gate at `:66-70` goes; `ts_name` from the file name per D6;
- mdocs with no `[ZValue]` section are skipped with one summary warning ("skipped N per-movie mdocs");
- `_build_tilt_info` resolves `frames_dir / name`, then `mdoc_path.parent / name`;
- classification per D1 after the tilts are built; the stack check is a header-only `mrcfile.open`
  (`header_only=True`, permissive) — no data read in the scan;
- `_aggregate_to_positions`: group by stage when every series has one, else one flat group with
  `stage_position=None` rendered as `—`;
- `frame_extension` = the sniffed movie extension for movies, `".mrc"` when the chosen layer is stacks.

`AcquisitionSummary` gains `dose_missing: int` (selected TS with `dose_per_tilt is None`) and
`dose_estimates: list[float]` (incident where available). `DatasetOverview` gains
`source_kinds: dict[str,int]`, `acquisition_software`, `software_version`, `detector_dimensions`,
`unresolved_selected` (selected TS with `source_kind == "missing"`), `dose_estimate()` (median of the
selected series' incident estimates, else transmitted) and `source_line() -> str`.

### 4.3 The landing page — `ui/data_import_panel.py`, `ui/dataset_overview_panel.py`

- **Mdoc discovery one level down.** In `update_mdocs_validation._finish` (`:187-208`): when the derived
  `<dir>/*.mdoc` matches nothing, try `<dir>/*/*.mdoc`; if that matches, adopt it as `mdocs_glob`
  (state + prefs + the hidden mdocs input) and hint `N mdocs found in subfolders`. "mdocs elsewhere"
  mode is untouched.
- **The frames field stops gating on its own glob once a scan has spoken.** `get_missing_requirements`
  (`:250-274`): "Valid Frames" is satisfied when `movies_valid` OR the current overview (same glob as
  `overview_glob`) has `unresolved_selected == 0`. After a successful scan, `_apply_overview_to_fields`
  sets the widget extension from `overview.frame_extension` (`GlobDirectoryInput.set_extension`,
  `ui/glob_directory_input.py:133`), so `movies_glob` records the real extension, clears the red
  "No files match" on the input, and writes the hint: `156 .mrc frames on create (4 stacks)` or
  `164 .eer frames`. The conf default `*.eer` remains the starting extension.
- **Dose field.** A row in the raw-data section, right under the frames row, hidden until an overview
  reports `dose_missing > 0`: label `Dose per tilt`, mono input in the landing style **prefilled with
  the estimate**, hint per D4 (both numbers, pretilt, thickness, "the acquirer's number replaces it").
  `DataImportFormState` gains `dose_per_tilt_override: float | None` (`ui/ui_state.py:29-53`).
  Editing it sets the source to `"user"`. `get_missing_requirements` adds "Dose per tilt" only when the
  field is visible AND empty (no estimate possible, value cleared); `_FIELD_FOR_REQUIREMENT` maps it.
  `handle_create_project` (`:627-648`) puts value + source into `detected_params`, plus
  `acquisition_software` and `detector_dimensions` from the overview.
- **Overview panel.** Dose chip (`_param_chip`, `:313-326`): when `dose_missing > 0` render
  `Dose/tilt 4.3 · estimated` in the amber category colour with the fit details in a tooltip. Under
  the chips, the source line (`overview.source_line()`), 9 px sublabel. POS column shows `—` for `None`.
  Dry-run dialog text (`:516-533`): "symlinks to your frames; SerialEM stacks are split into one file
  per tilt inside the project — the originals are never modified".
- Label/tooltip (`:1162-1168`): `Raw frames / tilt stacks & mdocs` — "frame files (.eer/.tif) or SerialEM
  tilt stacks (.mrc + .mrc.mdoc), flat or one folder per series".

### 4.4 The protocol create dialog — `ui/protocols_dialog.py:130-200`, `services/protocols/apply.py:120-170`

Same D4 behaviour, so the copia and any future protocol never run on a silent 3.0: `apply_protocol`
gains `dose_per_tilt: float | None` and `dose_per_tilt_source`; when the facts have no dose and no
value was passed it uses the estimate (`"estimated"`), and returns `err("dose per tilt: not in the
mdocs and no estimate possible — enter it")` only when there is none. The dialog gets a
`house_number("dose/tilt")` prefilled the same way, with the same hint; `import_summary` in `apply`
stops guessing `frame_extension` from the glob suffix (the import result carries it, 4.5).

### 4.5 Create — `services/stack_import.py` (new), `project_service.py`

`services/stack_import.py` (~80 lines, `mrcfile` imported lazily like `services/tomogram_import.py`):

```
split_stack(mdoc_path, out_dir, *, prefix, progress=None) -> SplitResult(ts_name, n_frames, frame_names, mdoc_lines)
```
- parses the mdoc once, opens `<mdoc dir>/<ImageFile>` with `mrcfile.mmap(permissive=True)`;
- invariant `nz == n_sections`, else `ValueError` (fails Create loud);
- writes slice `z` as `<out>/<prefix><basename(SubFramePath) with .mrc>` (int16 kept, voxel size =
  PixelSpacing), skipping files that exist with the right size (idempotent);
- returns the mdoc text with `SubFramePath` rewritten and every other line verbatim.

`_setup_project_data_sync` (`project_service.py:33-99`) gains `progress_cb` and, per selected mdoc:
classify (same rule as the scan, re-derived from disk — the overview is not passed down) → stack:
`split_stack` into `frames/`; movies: symlink as today, resolving beside the mdoc when the flat dir
misses. The mdoc copy is `mdoc/<prefix><ts_name>.mdoc` (D6). Returns `ok(layer=…, frame_extension=…,
n_frames=…)`. `initialize_new_project` (`:352-470`) persists `import_source_kind`,
`import_frame_extension` from that result (not from the summary), `acquisition_software`,
`detector_dimensions`, `dose_per_tilt` + `dose_per_tilt_source` from `detected_params`, and passes
`state.acquisition.dose_per_tilt` as the registry fallback (`_build_and_persist_registry`,
`:294-350`). Order matters: the state fields are set BEFORE `ensure_job_initialized` so the D5 stamps
see them. The Create handler threads a progress dict into the spinner label the way the scan does
(`data_import_panel.py:966-995`).

`load_project_state` (`:517-535`) needs nothing: the drift check compares registry ids with
`mdoc/*.mdoc` stems, which are the normalised names.

### 4.6 Jobs and drivers

- `drivers/fs_motion_and_ctf.py:114`: `.opt("--eer_ngroups", …)` only when `extension == "*.eer"`.
- `services/project_state.py:1051-1078` `ensure_job_initialized`: when the param class has
  `tomo_dimensions`, stamp `f"{w}x{h}x2048"` from `acquisition.detector_dimensions` (precedent:
  `rescale_angpixs` at `:1069-1075`); when `import_source_kind == "stacks"`, stamp
  `out_average_halves = False` / `halfmap_frames = 0` on the classes that have them (D5).
- `drivers/ts_reconstruct.py:57-58`: `.flag("--halfmap_frames")` only when `params.halfmap_frames == 1`,
  `.flag("--deconv")` only when `params.deconv == 1`. **Behaviour change on every project:** 0 now
  actually disables. Pre-flight in `stage()`: when `halfmap_frames == 1`, resolve the TS's tomostar
  first `wrpMovieName`, check `<its dir>/even/<name>` exists, else raise
  `"no even/odd halves for this tilt-series — set halfmap_frames to 0 (CryoCARE unavailable)"` before
  Warp runs (the safety net when a user turns halves back on on a stack project).
- `services/configs/metadata_service.py:90-96`: key = `frame_id_to_warp_key(Path(basename).stem)`
  (identical result for `_EER.eer`, `.tif`, `.eer`; fixes `.mrc`).
- `ui/job_plugins/ts_reconstruct.py`: on `import_source_kind == "stacks"`, a 9 px hint beside
  `halfmap_frames`: `stack input has no even/odd halves — 0 by construction`. Absent is stated.
- `ui/pipeline_builder/pipeline_roster.py:1170-1176`: Dataset rows `Source layer` from
  `import_source_kind` and `Dose/tilt <v> (estimated|user)` when `dose_per_tilt_source` is not `mdoc`.

## 5. Build order — one commit per stage, back-to-back, one runtime pass at the end

| stage | content | files | size |
|---|---|---|---|
| S0 | switch fix + halves pre-flight; `.mrc` key fix | `drivers/ts_reconstruct.py`, `services/configs/metadata_service.py` | ~30 lines; **its own commit** (every project) |
| S1 | `MdocFacts` + `acquisition_from_mdoc` + `estimate_incident_dose`; scan dialect, classification, names, flat group, skip no-ZValue, resolve beside mdoc, dose estimate, overview fields + `source_line` | `mdoc_service.py`, `dataset_parsing_service.py`, `preimport.py`, `build.py` | ~230 |
| S2 | `stack_import.split_stack`; Create branch + progress; `import_source_kind` + `dose_per_tilt_source` (schema 3.8); registry dose fallback; persisted software/dims; job-init stamps | `services/stack_import.py`, `project_service.py`, `project_state.py`, `models_base.py` | ~200 |
| S3 | landing page (depth-1 mdocs, frames verdict from the overview, extension follows the scan, prefilled dose field, chip, source line, texts); protocol dialog dose | `data_import_panel.py`, `dataset_overview_panel.py`, `ui_state.py`, `protocols_dialog.py`, `apply.py` | ~230 |
| S4 | eer gate; tab hint; roster rows; notes status + index | `fs_motion_and_ctf.py`, `job_plugins/ts_reconstruct.py`, `pipeline_roster.py`, docs | ~40 |

Static gates after each stage: `venv/bin/ruff check . && venv/bin/ruff format --check .` and
`python check_boundaries.py` (R1: `services/stack_import.py` imports nothing from `ui`).

## 6. Verification (runtime pass, in order)

Targets: nested delivery `/groups/klumpe/crboost_data` (four `area_5_a_ts_*` subfolders); flat layout
`/groups/klumpe/crboost_data/_k3_dryrun/split/` (frames + normalised mdocs from the dry run); one Tomo5
folder for the regression line.

1. **Scan, nested.** 4 TS × 39, axis 85.0, pixel 1.382, `.mrc`, source line as in §1, one warning line
   about the 156 skipped per-movie mdocs, dose chip `4.3 · estimated` (tooltip: transmitted 2.2,
   pretilt −8.5°, ~230 nm), POS column `—`, dose field visible and prefilled with 4.3.
2. **Scan, flat split dir.** Same 4 TS, `source_kind = movies`, `.mrc`, same dose estimate (the split
   mdocs keep every DoseRate line).
3. **Scan, Tomo5.** Byte-identical overview to today's (chips, positions, warnings); no dose field.
4. **Create** without touching the dose: spinner counts stacks; time per series noted; `frames/` 156
   real files, `mdoc/` 4 normalised copies, `registry/` 4 TS with pre-exposure accumulating 4.3,
   `project_params.json` has `import_source_kind: stacks`, `import_frame_extension: .mrc`,
   `acquisition_software: SerialEM`, `detector_dimensions: [5760, 4092]`, `dose_per_tilt: 4.3`,
   `dose_per_tilt_source: estimated`, `tilt_axis_degrees: 85.0`; the job tabs show
   `tomo_dimensions 5760x4092x2048`, `out_average_halves` off, `halfmap_frames 0`. Roster Dataset rows
   read `Source layer` and `Dose/tilt 4.3 (estimated)`. Overwrite the dose with 4.0 on a second project
   → `dose_per_tilt_source: user`.
5. **Idempotence / failure.** Delete the project from the roster and recreate: same result. Truncate one
   stack copy in a scratch delivery: Create fails with the nz/sections message, no half project opened.
6. **Protocol path.** Protocols → copia → Create on the nested folder: the dialog's dose field is
   prefilled `4.3 · estimated`; the project records the same source. (The copia protocol is not meant
   for K3 data — this only checks the path cannot run on a silent 3.0.)
7. **Pipeline.** Run the chain: fs settings `--extension '*.mrc'`, no `--eer_ngroups`, no gain, 39 XMLs per
   TS; tsImport 4 tomostars × 39 with `wrpAxisAngle 85.0` and `wrpDose` accumulating 4.3; tsCtf ingest
   resolves every frame (the `.mrc` key fix); set `defocus_hand = set_noflip` on the tsCtf tab; tsReconstruct
   emits no `--halfmap_frames` and writes four reconstructions at 12 Å; flip `halfmap_frames` to 1 on a
   copy → fails BEFORE Warp with the halves message; Journey opens on `area_5-A_ts_002`; gallery shows four
   tomograms; dashboard handedness cross-check consistent.
8. **Regression, EER project.** tsReconstruct with `halfmap_frames = 0` now emits no switch and Warp
   writes no halves; with 1, unchanged. `deconv = 0` likewise. tsCtf ingest unchanged (key identical).
   New EER project: `tomo_dimensions` still 4096x4096x2048, halves still on, dose from the mdoc with
   `dose_per_tilt_source: mdoc`.
9. **Journey label** for the K3 TS reads the series name, not `002` (fix in `dashboard_data.position_label`
   if it does — fall back to `ts_display_name`).

## 7. Out of scope / later

- The raw `.tif` movie path (gain + defects; `create_settings --defects_path` exists in the dev36
  container — noted in `notes/k3_ingest/00-plan.md`).
- A compute-node splitter (the sbatch prototype) or a background-task split — only if D2's cost bites.
- A `[stacks | movies]` chooser — only if D1's rule is wrong for a real site.
- CryoCARE/IsoNet on stack projects (`drivers/denoise_train.py:59,82` need halves) — stated in the tab hint.
- `PriorRecordDose`-style pre-exposure for dose-symmetric SerialEM data (accumulated dose is a
  constant-per-tilt sum; Warp computes its own from `--tilt_exposure`, so only the registry field is approximate).
- Editing `dose_per_tilt` after creation from the UI (today: project parameters / re-create).

## 8. Open items that are not code

- The acquirer's dose per tilt replaces the 4.3 estimate (and, if they confirm ~4.3, validates the
  zero-thickness estimator for future SerialEM deliveries without dose calibration).
- Their handedness answer (Warp: no flip). Their delivery layout (flat vs one folder per series) —
  both are supported, the question decides which line of §6 is the real one.
