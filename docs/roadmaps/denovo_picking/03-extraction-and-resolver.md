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

## Verification (user, on cluster — budget shake-out time)

1. Extract a manual list on an **imported** tomogram — the #1 runtime risk is
   `relion_tomo_subtomo` accepting the synthesized `data_optics`. Fallback if rejected: mirror the
   optics block shape from a RELION-produced particles.star (412 projects have them).
2. Add `reconstructParticle` for the manual species → IO tab shows `pick_list__<slug>` as the
   resolved producer; submit has **no** afterok dep on a phantom job; job runs.
3. class3d downstream of that — first full particle-tail run on this path.
4. Re-curate the list (flip a keep) → extraction badge goes STALE → attempt re-submit of
   reconstructParticle → actionable "re-extract" error; re-extract → resolves again.
