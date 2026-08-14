# S3 — Candidate-free extraction + resolver wiring

Depends on S2. ~450 lines. Goal: the full de-novo chain reaches class3d. Locked decision D-7: the
resolver exposes the **extracted** optset; for manual species the roster chain starts at
reconstructParticle.

## Candidate-free extraction (old D3)

- `services/visualization/list_extraction.py`: factor the row-synthesis + `_write_optimisation_set`
  core out of `build_list_optset` (:127-194) and add

  ```python
  def build_list_optset_from_tomograms(list_star, tomo_name, out_dir, tomograms_star) -> dict
  ```

  synthesizing a minimal `data_optics` from the tomograms.star `data_global`
  (`rlnVoltage / rlnSphericalAberration / rlnAmplitudeContrast / rlnTomoTiltSeriesPixelSize` — the
  imported star writer already emits all four, `services/tomogram_import.py:206-232`), and pointing
  the optset at that tomograms.star. Raise on any missing column — no invented optics.
- `drivers/extract_pick_list.py`: `--candidate-optset` (:110) and new `--tomograms-star` become a
  mutually-exclusive required group.
- `backend.extract_pick_list` / `extract_pick_list_and_wait` (`backend.py:223-366`): accept
  `tomograms_star: Path | None`, require exactly one source.
- UI (`_handle_extract_list`, `ui/tomo_dashboard_dialog.py:2957-3014`): drop the hard
  `optimisation_set.star` gate (:2983-2988) — route to the candidate-free builder when the species
  has no CE optset, sourcing the star from `TomoGeometry`.

## Extraction params — kill the silent default

`:2989-2998` silently defaults box/bin/crop to `384/1.0/224` when the species has no
SUBTOMO_EXTRACTION job model (mirrored in `services/aggregation_authoritative.py:247-257`; flagged
hazard in `docs/LIST_EXTRACTION_AND_AGGREGATION.md`). Replace with (D-3):

- prefill from the species' subtomo job model if present, else `species.extraction_params` (S1);
- else a **required** dialog before submit; confirmed values persist to
  `species.extraction_params`;
- applies to the CE path too — the silent default dies everywhere.

## Resolver wiring (old D4, clarified by D-7)

- `_add_pick_list_optset_candidates(index)` in `services/path_resolution_service.py`, sibling to
  `_add_merged_sources_candidates` (:579) / `_add_imported_tomograms_candidates` (:612): for each
  `PickList` with `extraction_state() == EXTRACTED`, inject an `OPTIMISATION_SET_STAR`
  `OutputCandidate` with `producer_instance_id=f"pick_list__{pl.slug}"`,
  `species_id=pl.species_id`, `execution_status=SUCCEEDED`, `relion_job_number=0`.
- Extend the non-job afterok filter (:262-264, currently
  `producer_id in ("mergedSources", "importedTomograms")`) with
  `producer_id.startswith("pick_list__")` — no phantom afterok edges.
- **Audit the override-key branches** (:174-181 in `resolve_inputs`, :399-408 in
  `validate_input_slot`, :475-499 `_resolve_override`): today the
  `MERGED_SOURCES.value + ":"` prefix serves both mergedSources and importedTomograms;
  `pick_list__*` overrides must not route through that branch.
- Tie-break among multiple EXTRACTED lists of one (species, tomo): authoritative slug
  (`get_authoritative_slug`, `services/project_state.py:831`), else newest `extracted_at` — and
  **surfaced in the resolution report**, never silent.
- Orchestrator submit path: consult the authoritative slug to prefill `source_override` only when
  ≠ default; the resolver itself stays dumb.
- Staleness: when a consumed list flips STALE, the candidate disappears and submit raises
  `PathResolutionError` — correct surface, but the message must say
  "pick list '<slug>' went stale — re-extract it", not generic missing-input.
- Re-check S1's `remove_species` purge now that `pick_list__*` producers actually exist in
  `source_overrides`.

---

## S3 CODE-COMPLETE 2026-08-13 — pending runtime

Verified to the sandbox ceiling: `ruff check .` clean repo-wide, and every hunk `ruff format`
produces on a touched file was confirmed pre-existing (the repo has ~67 files of standing format
drift). `py_compile` and `check_boundaries.py` are **owed** — `/software` is unmounted, so the venv's
python symlink is dead and there is no system `python3` this session.

### Landed

**Candidate-free extraction.** `services/visualization/list_extraction.py` grew a shared core —
`_coords_for_tomo`, `_synthesize_particle_rows`, `_write_list_optset` — that `build_list_optset` now
routes through unchanged, plus `build_list_optset_from_tomograms(list_star, tomo_name, out_dir,
tomograms_star)`. The synthesized schema is declared as `SYNTHESIZED_PARTICLE_COLS` (identity,
position, the three Euler angles at explicit 0, optics group) and the one-row `data_optics` is copied
— never derived — from the tomograms.star global block, with a missing OR blank
`REQUIRED_OPTICS_COLS` entry raising by name. Both star producers emit all four: the pipeline import
(`_write_import_stars_inline`, carried forward by every adapter) and `services/tomogram_import.py`.

**Driver + backend.** `--candidate-optset | --tomograms-star` is a mutually-exclusive required group
in `drivers/extract_pick_list.py` (and in `list_extraction`'s own CLI); the container bind set now
follows whichever source was given. `backend.extract_pick_list` takes `candidate_optset: Path | None`
+ `tomograms_star: Path | None` and rejects zero-or-both.

**The silent 384/1.0/224 default is dead in both places.**
`aggregation_authoritative.extraction_params_for_species` is the one resolver: subtomo job model →
`species.extraction_params` → **`None`**. The gate path turns `None` into a blocked entry whose reason
now names the actual gap (`extract_inputs_blocked_reason`, replacing one catch-all string); the
dashboard turns it into a required dialog that persists to `species.extraction_params` and then
submits. `max_dose=-1` / `min_frames=1` / stack2d / float16 keep their defaults deliberately — those
are the tool's own "no limit" sentinels (the driver omits the flags entirely at those values) and
output-representation choices, not sample geometry. Only the geometry was ever the invented part.

**Resolver.** `_add_pick_list_optset_candidates` injects every `EXTRACTED` pick list as an
`OPTIMISATION_SET_STAR` producer, `resolve_edges` drops it from afterok via a new shared
`is_synthetic_producer()`, and the override-key masquerade (risk #3) is closed by
`_synthetic_override_target()`, which discriminates merged / pick_list / imported on the instance
path instead of the shared `mergedSources:` job-type prefix. A dangling pick-list override now gets
its own message, and `PathResolutionService._dangling_pick_list_message` looks the list up in state
so it can say "no longer extracted (its picks changed)" for a STALE list versus "no longer exists"
for a deleted one.

### Deviations from the plan, with reasons

1. **Producer identity is `pick_list__<species>__<tomo>__<slug>`, not `pick_list__<slug>`.** The plan's
   slug-only id is unsafe: `PickList.slug` is unique only within a (species, tomo), and
   `ui/tomo_dashboard_dialog.py` creates every hand-picked list with the literal `slug="manual"`. A
   slug-only id would collapse every species' and every tomogram's manual list onto ONE producer —
   `_resolve_override` would return whichever came first, and the UI dropdown would collapse them into
   one row. It also makes the S1 `remove_species` purge correct: that code matched
   `f"pick_list__{slug}" in value`, so deleting species A would have purged species B's overrides.
   `species_references` now matches `pick_list_producer_prefix_for_species(species_id)`.
2. **Tie-break is spent on `relion_job_number`, and only when there IS a tie.** All EXTRACTED lists are
   injected (so the user can override to any of them); within one (species, tomo) they are ranked
   `0..N-1` by (is-authoritative, `extracted_at`), so `_choose_candidate_for_slot` picks the
   authoritative list deterministically. Ranks start at 0 so a lone pick list ties the other synthetic
   producers exactly as before. "Surfaced, never silent" is the candidate `label`, which the IO
   dropdown renders verbatim — `[auto-selected: authoritative]` / `[auto-selected: most recently
   extracted]`. There is no notes channel on `ResolvedManifest` to put it in, and adding one is out of
   scope here.
3. **No orchestrator auto-prefill of `source_override`.** The plan wanted the submit path to pin the
   authoritative slug. Deviation 2 already makes auto-selection land on that same list, so the prefill
   would only change whether the choice is *pinned* — and an auto-written pin is a UX decision (it
   goes stale loudly rather than following a later re-choice) that belongs with the UI work, not
   ahead of it. The stale-message path is not dead code: it fires for any override the user sets
   through the IO dropdown, which is how overrides normally get set. `apply_aggregation_overrides` is
   the precedent — that mutator is called from the UI, never from the orchestrator.
4. **Existing CE-rich projects are unaffected by the injection**, checked rather than assumed: both
   optset consumers (`ReconstructParticleParams`, `Class3DParams`) declare
   `preferred_source="subtomoExtraction"`, so a real subtomo job scores `pref=1` against a pick list's
   `0` and still wins. The pick list only wins where there is no subtomo job — which is exactly D-7.

### Owed

- Everything under "Verification" below — none of it is reachable in the sandbox.
- `venv/bin/python -m py_compile` + `check_boundaries.py` (blocked on `/software`).
- The `relion_tomo_subtomo`-accepts-synthesized-`data_optics` question (risk #2) is untouched by any
  of this: the first cluster run is still the test, and the fallback is still to mirror the optics
  block shape from a RELION-produced particles.star.

## Verification (user, on cluster — budget shake-out time)

1. Extract a manual list on an **imported** tomogram — the #1 runtime risk is
   `relion_tomo_subtomo` accepting the synthesized `data_optics`. Fallback if rejected: mirror the
   optics block shape from a RELION-produced particles.star (412 projects have them).
2. Add `reconstructParticle` for the manual species → IO tab shows `pick_list__<slug>` as the
   resolved producer; submit has **no** afterok dep on a phantom job; job runs.
3. class3d downstream of that — first full particle-tail run on this path.
4. Re-curate the list (flip a keep) → extraction badge goes STALE → attempt re-submit of
   reconstructParticle → actionable "re-extract" error; re-extract → resolves again.
