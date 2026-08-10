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
