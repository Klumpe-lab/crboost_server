# Roadmap 02 — TiltSeries: one source of truth, no regex re-derivation

Companion to `docs/registry-consolidation-roadmap.md` (the strategic direction, already agreed).
This roadmap is the tactical execution of its Phase 2+3 slice as quantified by the 2026-08-10 audit,
with the maintainer's constraint made explicit: **lossy refactors have happened here before; nothing
gets deleted until parity is proven.**

## Before → After

**Before:** five coexisting models of a tilt series (registry entities; `dataset_models` import
parse; the `ProjectState` mirror `tilt_metadata`/`tilt_filter_labels`/`Import*Summary`; the pandas
star view; `TomoFrame`). The Journey dashboard — the biggest TS-fact consumer — uses the registry only
for `excluded_ids()` and re-derives everything else by re-reading emitted STARs and doing filename
surgery. Four independent `Position_*` regexes parse names the registry already stores as fields.
`instance_id` grammar (`{jobtype}__{species}`) is hand-split in 19 places with the canonical decoder
in `ui/ui_state.py:202`.

**After:** the registry is the read path for every TS/Frame/Tomogram fact that exists in it; stars
are produced for RELION/PyTOM interop, not consumed by our own UI; position/species/identity parsing
happens exactly once, behind typed accessors. **Gain:** the "how does a TS accumulate features through
the pipeline" story has one answer; dashboard bugs stop being star-format bugs; the `ProjectState`
mirror (and its drift risk) disappears; robust, predictable mechanisms replace regex heuristics.

## Stage 0 — gather first (this is the caution the history demands)

Build a read-only **parity harness** before changing any consumer:

- `transient` script (or a hidden dashboard debug panel) that, for a given project, renders every
  fact the dashboard currently derives from stars side-by-side with the registry's value:
  per-tilt defocus/CTF-res/motion (star `tilt_series/{ts}.star` vs `FsMotionCtfFrameOutput`),
  alignment per-frame (vs `TsAlignmentPerFrame`), filter verdicts (`tilt_series_labeled/*.star` vs
  `Frame.is_filtered_out`/`filter_probability`), denoised volume path (stem surgery vs
  `DenoisePredictTomogramOutput.denoised_mrc`), TomoHand (import star vs
  `TsCtfTiltSeriesOutput.are_angles_inverted`), position/beam (regex vs
  `TiltSeries.stage_position`/`beam_position`).
- Run it on `projects/try2_after_pixShift` (full chain) and `projects/pos9_10` (has TiltFilter), and
  on at least one real cluster project. **Every divergence gets triaged before migration**: registry
  missing data (→ backfill gap, feeds the consolidation roadmap's Phase 1 backfill), registry wrong
  (→ adapter bug, fix first), or star wrong (→ evidence the registry is better; record it).
- Inventory legacy projects: which lack registry dirs entirely (pre-registry projects) — those need
  the documented fallback story (read-from-stars remains as *explicit* fallback, or a one-shot
  backfill on open; decide here, don't improvise per-panel).

## Stage 0 record (harness built 2026-08-11; runs owed)

`parity_harness.py` at repo root — read-only, run as
`venv/bin/python parity_harness.py <project> [--verbose]`. It imports the dashboard's OWN readers
(`ui.tomo_dashboard_dialog` helpers, `services.dashboard_data`, `frameseries_quality`) so parity is
measured against the real code paths, and diffs them per TS against the loaded
`TiltSeriesRegistry`. Nine facts: position regex, fsMotion per-tilt defocus (star vs
`FsMotionCtfFrameOutput`), fsMotion QC (Warp XML ctf-res/motion vs registry fields), tsCtf per-tilt
defocus, alignment per-frame (XTilt/YTilt/ZRot/shifts), tilt-filter verdicts + DL probability,
TomoHand (import star vs `are_angles_inverted` — different authorities, divergence is a *finding*),
denoised MRC path (star+stem-surgery vs registry), reconstructed MRC path. Statuses per row:
`ok` / `DIFF` / `star-only` (backfill gap) / `reg-only` (registry richer). Also reports: registry
`sanity_check()` problems, star-only TS (in fsMotion stars but not registry), and the
NO-REGISTRY case for pre-registry projects. Tolerances: defocus 1 Å, angles/shifts 0.05,
probability 0.005, QC 0.01. Known limits (fine for stage 0): first-match `find_job_by_type` — one
instance per job type assumed; frame matching prefers movie-stem == `Frame.id`, falls back to
positional Z when a star lacks `rlnMicrographMovieName` (matters for post-filter trimmed stars —
stem matching handles those correctly when the column exists).

### Sandbox runs + triage (2026-08-11, user-executed)

`try2_after_pixShift` (1 TS / 39 frames) and `pos9_10_after_pixShift` (2 TS / 82 frames):

- **Entity layer: FULL PARITY.** position stage/beam 100% ok on both projects; zero sanity
  problems; no star-only TS. The registry's identity model is trustworthy → stages 1-2 are
  unblocked with no caveats.
- **Output layer: EMPTY on both projects — every job fact is star-only** (fsm defocus ×242, fsm
  XML QC ×241, tsctf, alignment, tomohand, recon paths). Triage: **legacy, not a live bug** — all
  four array drivers DO wire their ingest adapter and `registry.save()` today
  (`fs_motion_and_ctf.py:374`, `ts_alignment.py:313`, `ts_ctf.py:306`, `ts_reconstruct.py:185`,
  each refusing to run on an empty registry), but these sandbox jobs ran before that wiring
  landed. Fresh-run verification still owed: one new pipeline run should show ok rows.
- **filter: 79 vacuous ok (keeps) + 3 DIFF `star=dropped, reg=False`** — the three high-tilt
  (69°/70°) frames the filter trimmed. Root cause found by reading the labeled star: pos9_10's
  `TiltFilter/tilt_series_labeled/*.star` is the OLD 26-column format with **no
  `cryoBoostDlLabel`/`cryoBoostDlProbability` columns**, so (a) the prob comparison never emitted,
  (b) current stamping code (which keys off those columns) can never re-stamp legacy filter runs,
  (c) legacy filter-verdict backfill must use the labeled-vs-filtered set diff (what the harness
  computes), not the DL columns. Fresh filter-run stamping check owed.
- Bonus confirmation: the legacy labeled star visibly carries `rlnCtfMaxResolution=0.000001`
  placeholders — the Warp placeholder trap the registry's XML-sourced QC fields exist to fix.

### Cluster run: agg_20251113_412 (18 TS / 728 frames, ran WITH live adapter ingest)

This run settles the triage — the registry works everywhere it was wired:

- **Exact parity on live-ingested facts**: fsm_defocus 1456/1456 ok, tsctf_defocus 1364 ok,
  alignment 3410 ok, tomohand 18/18, recon_path 18/18, position 36/36.
- **STAR WRONG, REGISTRY RIGHT (46 rows)**: the tsctf/alignment "star-only (no registry
  per-frame)" rows are exactly the high-tilt frames the tilt-filter dropped (they coincide with
  the 81 filter DIFFs). The emitted per-tilt stars carry ghost rows for dropped tilts (the known
  `emit_star` ignores `is_filtered_out` gap from the pipeline-reshape work); the registry's
  `per_frame` correctly holds only what Warp processed. First hard evidence the registry is MORE
  correct than the stars — dashboard-on-registry (stage 3) fixes these ghosts for free.
- **fsm_quality 1434 star-only is legacy too, NOT a live gap**: the adapter ALREADY ingests
  `ctf_resolution`/`mean_frame_movement` (`adapters/fs_motion_ctf.py:195`, schema 1.2) — this
  project's fsMotion just ran before that landed. `frameseries_quality.py`'s docstring claiming
  the adapter "doesn't ingest" them was stale and has been corrected (2026-08-11).
- **filter 81 DIFF (star=dropped, reg=False)**: runs predate verdict stamping; stamping code
  exists on both paths today (`drivers/tilt_filter.py:145` DL, `services/jobs/tilt_filter.py:129`
  manual). Fresh-run stamping verification still owed.
- **Bottom line: zero live code gaps found.** Every divergence on every project is a
  pre-adapter/pre-schema/pre-stamping run. A fresh pipeline run today should reach full parity on
  all nine facts.

**Stage-0 decision (maintainer, 2026-08-11): NO backfill, NO backwards compat.** Quote: "don't
worry about legacy stuff... blaze ahead with breaking changes and apologize later." The earlier
one-shot-backfill proposal is DROPPED. Consequences for stage 3: consumers go registry-only;
projects whose registries lack outputs (anything run pre-adapter) show a loud, honest
"not in registry — re-run the job" marker in migrated sections (never a silent star fallback,
per the never-fail-silently rule); the star/XML read helpers are deleted with their consumers.
Legacy projects re-earn dashboard data by re-running jobs, not by backfill.

## Stage 1 record (done 2026-08-11; py_compile + check_boundaries clean, boot owed)

`InstanceId` landed in `services/models_base.py`: `split()` (lenient `(base, suffix)` — the only
place the `__` separator exists), `parse()` (strict, ValueError on unknown JobType),
`matches(raw, jt)`, `__str__` re-encode. `instance_id_to_job_type` moved here from `ui/ui_state.py`
(3 importers re-pointed). `split_species_id` + the ONE `resolve_species` also live here —
models_base is dependency-free, so no circular-import risk (dashboard_data imports
preview_orchestrator top-level, which disqualified it as the canonical home; it keeps `as X`
re-exports for its consumers). Deleted: `template_metadata.resolve_species_from_job`,
`aggregation_authoritative._species_id_for_job`. One deliberate semantic alignment: the latter
used to return a numeric/ghost suffix as a species id (`subtomoExtraction__2` → `"2"`, making
numbered instances unmatchable in `_instance_for_species`); it now falls through the chain like
the other two copies always did. All ~19 split sites converted, including the encoder
(`next_instance_id` builds ids via `str(InstanceId(...))`). `parity_harness.py` left as-is
(transient stage-0 tool). Edge: empty suffix (`"tm__"`) now decodes as None, not `""`.

## Stage 2 record (done 2026-08-11; py_compile + boot owed)

The audit's "four independent Position regexes" were: `dataset_parsing_service.MDOC_FILENAME_RE`,
`array_tasks._POSITION_RE` (the task_utils copy, post roadmap-01 3a), `tilt_filter_panel._POS_RE`,
plus the rsplit-based `_infer_position` in `build.py`. All four now converge on
`services/tilt_series/build.py`: `parse_position(label) -> (stage, beam|None) | None` is THE
grammar (end-anchored `Position_(\d+)(?:_(\d+))?$`, beam None = implicit, None = no suffix);
`infer_position` is its defaults wrapper ((0,1) fallback, made public per plan). Deleted all three
regex copies; `_parse_mdoc_filename`, `ts_display_name`/`ts_pretty_name`/`ts_position_sort_key`,
and `_parse_pos_beam` now delegate. Semantic deltas, all deliberate: (a) mdoc filename acceptance
loosened from `^Position_...` to any name *ending* in `Position_{stage}[_{beam}].mdoc` — prefixed
mdocs (`W7B4_Position_1.mdoc`) now import instead of "Skipped … unrecognized name"; (b) zero-padded
stages would display normalized (`Position_011` → `Position_11`) — cosmetic, unseen in real data;
(c) tilt-filter's unanchored regex is now end-anchored (trailing-junk names no longer half-match).
The "display of a registry-known TS reads `stage_position`/`beam_position`" clause is deferred to
stage 3 deliberately: no current display site holds a TiltSeries entity — they hold raw name
strings from manifests/stars — so the field-read switch lands with each dashboard-on-registry
section, not as a stage-2 mechanical change.

## Stage 3 record (code-complete 2026-08-11; runtime check owed — one sitting, no pipeline runs)

All six sections switched in one pass (user-approved fast path; data equivalence was already
proven by the stage-0 harness runs, so the per-section runtime gate collapsed to one final check).

- **Commit 0 — registry freshness**: drivers ingest on compute nodes, so the server's cached
  `get_registry_for` instance went stale after every run (and a stale server-side mute save could
  clobber driver-written outputs). `TiltSeriesRegistry` now records the index.json mtime at load;
  `get_registry_for` reloads a fresh instance when the on-disk index changed (never discarding
  unsaved in-memory changes). One stat per call.
- **Registry adapters** (`services/dashboard_data.py`): `fsm_registry_df` / `tsctf_registry_df` /
  `alignment_registry_df` build per-tilt DataFrames with the SAME rln column names the stars
  carried, so every plot/hover/stat helper is unchanged; fsm adds `cbCtfResolution`/
  `cbMeanFrameMovement` (real XML-sourced QC — the render path no longer reads Warp XMLs).
  Plus `filter_verdicts_from_registry`, `filter_kept_dropped_from_registry`,
  `denoised_mrc_from_registry`, `warp_hand_from_registry`, `registry_ts_for`.
- **Sections**: fs-motion, alignment, ts-ctf (+ the Tilt-QC composite via `_defocus_source_df`),
  tilt-filter (gate = registry verdict stamps, not star dirs), denoise method selector, TomoHand
  chip (now shows THREE authorities: config intention, Import-star declaration, and the
  registry's Warp-applied `ts_defocus_hand` — disagreement upgrades the chip to error).
  Gap marker: `_render_registry_gap` — "running → lands at completion" vs "predates registry
  ingest → re-run" (amber). Never a silent star fallback.
- **Deleted from the dialog**: `_read_per_tilt_df`, `_per_tilt_star_path`,
  `_resolve_tilt_filter_dir`, `_tilt_filter_verdict_by_frame`, `_read_per_tilt_frame_names`,
  `_read_per_tilt_kept_dropped`, `_resolve_denoised_mrc_for_job`, both `quality_series` XML
  reads. The parity harness now owns its star-reader copies (it stays the independent audit).
  `_read_tomohand_from_import_star` survives (the Import star is that fact's only source).
- **Signature**: `_tilt_filter_sig_for_ts` (star mtimes) → `_registry_sig` (index.json mtime) —
  one stat covers every migrated section's change detection.
- **Known granularity change**: drivers ingest in the supervisor's aggregation step, so per-TS
  results appear at job COMPLETION, not per-array-task as the incremental stars did. The
  running-state marker says so. Mid-run incremental display can return later via per-task ingest.
- **Deliberately NOT migrated**: journey status pills / task manifests (status, not TS facts),
  recon section's tomograms.star read + `recon_mrc_map` (not in the six-section scope),
  tsCtf star's placeholder-ridden "CTF res" strip row (dropped — the registry has no such fact).

## Stage 4 record (code-complete 2026-08-11; runtime check owed)

The `ProjectState` TS mirror is gone: `tilt_metadata` (was write-only — dead),
`import_position_details` (write-only — dead), `import_tilt_series_details`,
`tilt_filter_labels`, and the `ImportPositionSummary`/`ImportTiltSeriesSummary` models.
Migration is forward-only exactly as planned: the hand-rolled loader now ignores the old JSON
keys, and the next save drops them (on-disk JSON untouched until then).

Re-pointed consumers:
- Roster's Dataset TS expansion → `get_registry_for(...).all_tilt_series()`
  (stage/beam/frame_count/mdoc_filename; selected split via `TiltSeries.is_selected`).
  Pre-registry projects just lose the expansion; the header counts still render.
- Tilt-filter panel: `job_model.tilt_labels` is the ONLY label store. Per-click persistence of
  unsaved labels went away with the mirror (clicks mutate the shared in-memory dict; Save
  persists) — acceptable, Save is the explicit action.
- `_restore_interactive_state` (re-creating the TILT_FILTER job) rebuilds labels from the
  registry's per-frame verdict stamps (`Frame.id` == cryoBoostKey) instead of the mirror.

Kept deliberately: the scalar `import_total_*`/`import_selected_*` counts — the registry only
holds *imported* TS, so "12 of 14" totals can't be derived from it; the scalars stay the record
of the import-time selection. `tilt_filter_png_dir` stays (thumbnail cache pointer, not TS data).

## Stage 5 record (code-complete 2026-08-11)

- `job_instance_id` is a REQUIRED keyword on all four ingest adapters. The deleted defaults
  were dead (every driver passes `instance_id` explicitly) and wrong (`"tsCTF"` isn't even the
  enum value — it's `"tsCtf"`; a caller relying on the default would have written outputs under
  a key nothing reads).
- The `_EER` drift is real (Warp keys strip the full extension chain: movie `<stem>_EER.eer` →
  key `<stem>`, while `Frame.id` is `<stem>_EER`), so it is BLESSED into the identity contract:
  rule documented in the `models.py` module docstring, encoded once as `frame_id_to_warp_key()`,
  resolved via `TiltSeries.frame_by_warp_key()`. The ts_ctf adapter's inline three-branch
  `_resolve_frame` fallback chain is deleted (its third branch — raw-filename stem — was
  redundant with `Frame.id` by construction). The other adapters resolve via star movie names
  (`frame_by_filename`), not Warp keys, and needed no change.

## Stage 6 record (code-complete 2026-08-12; stages 0–5 runtime-confirmed same day)

- Runtime checklist for stages 3–5 confirmed by the maintainer 2026-08-12 (Journey ghost rows gone,
  registry label round-trip works, roster/sandbox checks pass). Fresh-run parity harness still owed.
- `services/dataset_models.py` folded verbatim into `services/tilt_series/preimport.py` as the
  explicit pre-import representation; module docstring documents the conversion
  (`build.build_from_dataset_overview`/`_build_one_ts`) and the import-light constraint (pydantic
  only — `build.py` imports it and the parser services import `build`, so importing either from
  `preimport` would cycle). Four import sites re-pointed; old module deleted, no shim.
- `TomoFrame` → `TomogramGeometry` (`services/visualization/coords.py` definition;
  `artiax_bridge.py` call sites; `session_service.py` docstring). Purely internal — nothing on disk
  embeds the class name. Docstring added distinguishing it from the registry entity
  `services.tilt_series.models.Tomogram` (stable id, per-job outputs, no geometry).
- FINDING (deferred, behavior change — quarantined per shared rules):
  `build_from_dataset_overview` has **zero callers**. Live ingest is `build_from_mdocs` re-parsing
  mdocs in `project_service.py:313-330`, while the already-parsed `DatasetOverview` from the import
  panel is thrown away. Wiring the overview path in (and collapsing the two documented divergences,
  `prior_dose` vs `PriorRecordDose` and MinMaxMean ordering) is a separate, explicit commit.

## Stages

1. **`InstanceId` value object** (`services/models_base.py`): frozen dataclass with
   `job_type: JobType`, `species_id: str | None`, `parse()`/`__str__`. Convert the 19 split sites
   and the three duplicated species-resolution chains (`template_metadata.py:148`,
   `ui/dashboard/data.py:77`, `aggregation_authoritative.py:54`) to one function. Pure mechanics,
   no behavior change; move `instance_id_to_job_type` out of `ui/ui_state.py`.
2. **One position parser.** `services/tilt_series/build.py:_infer_position` becomes public
   (`infer_position`); delete the three copies (`dataset_parsing_service.py:22`,
   `ui/components/task_utils.py:13`, `ui/tilt_filter_panel.py:53`) and re-point their display helpers.
   Where the parse is for *display* of a registry-known TS, read `stage_position`/`beam_position`
   fields instead of parsing at all — regex survives only at import time (mdoc→registry) where it is
   genuinely deriving, not re-deriving.
3. **Dashboard-on-registry, one section per commit** (order: fs-motion → alignment → ts-ctf →
   tilt-filter chips → denoise path → TomoHand). Each commit: switch the section's collector
   (`services/dashboard_data.py` after Roadmap 01, else `ui/dashboard/data.py`) to registry reads with
   the Stage-0-decided fallback; user runtime-verifies the panel against the parity harness; only then
   next section. Star-reading helpers are deleted the commit *after* their last consumer switches.
4. **Delete the `ProjectState` TS mirror** (`tilt_metadata`, `tilt_filter_labels`,
   `import_position_details`/`import_tilt_series_details` + `Import*Summary` models) once nothing
   reads them: add a schema migration that drops the keys on load (keep the on-disk JSON untouched
   until save — forward-only, like previous migrations). The import panel's summary counts read the
   registry instead.
5. **Identity contract tightening:** make adapter `job_instance_id` a required kwarg (delete the
   dead-and-wrong defaults `"tsCTF"`/`"tsAlignment"`); either bless or remove the
   `_resolve_frame` string-fallback chain (`adapters/ts_ctf.py:259-274`) — if Warp keys genuinely
   drift from `Frame.id` (`_EER` suffix case), encode that rule *in the identity contract docstring
   and one shared helper*, not as an inline fallback.
6. **(Stretch, after 1-5 are stable)** Fold `dataset_models.py` (`TiltSeriesInfo` et al.) into
   `services/tilt_series/` as the explicit *pre-import* representation with a documented
   `to_tilt_series()` conversion, and rename `TomoFrame` → `TomogramGeometry` to end the Frame-name
   collision. The pandas `TiltSeriesData` view stays (RELION interop needs table form) but is
   documented as a *projection*, never a source.

## Modern-Python weave-in

- `InstanceId` = `@dataclass(frozen=True, slots=True)`; parsing via one `match` on the split result
  is a natural first `match` statement in the codebase.
- `IngestAdapter` Protocol (`ingest(expected_ts_ids)` / `emit_star(...)`) formalizing the four
  adapters — coordinate with Roadmap 04's `BaseIngestAdapter` (Protocol here, shared-code base class
  there; they compose).
- The registry's per-scope outputs already use discriminated unions — extend the same pattern for any
  new output types added during backfill (the consolidation roadmap's Tier-C particle-stage models).
- Typed accessors over raw dict reads on `outputs`: small helpers like
  `ts.output_of(instance_id, TsCtfTiltSeriesOutput) -> TsCtfTiltSeriesOutput | None` beat scattered
  `isinstance` checks.

## Runtime checklist

Per stage-3 commit: open Journey on both sandbox projects, compare the migrated section against the
parity harness output, click through a TS with dropped tilts (filter case) and an excluded TS.
After stage 4: create a fresh project, import, and confirm the import summary + dashboard populate
with the mirror gone; open a *pre-registry* legacy project and confirm the decided fallback engages.
