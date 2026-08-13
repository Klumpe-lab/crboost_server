# S2 — TomoGeometry provider + Particles inversion + ArtiaX decoupling
# S4 — Journey unification (trailer)

S2 is the big one (~700 lines). Depends on S1 (color/origin fields). S4 is a small trailer
(~250 lines) that depends only on S2.

## S2 goal

Open the dashboard on an imported-only project with one de-novo species → full Particles section
(canvas slab, rail, empty lists), launch ArtiaX with zero candidates, save picks in ChimeraX, list
auto-ingests, cutouts + keep/discard + merge all work. Regression: a CE-rich TM project renders
identically (except persisted species colors).

## New: `services/visualization/tomo_geometry.py`

Spec in `00-overview.md` §Seam specs. Rules:

- Dims/apix precedence: tomograms.star row (`rlnTomoTiltSeriesPixelSize × rlnTomoTomogramBinning`,
  `rlnTomoSize{X,Y,Z} / binning`) → recon MRC header (dims always; voxel_size only if > 0; read off
  the event loop) → `None` with `apix_provenance="missing"`.
- **Never 1.0, never `[1,1,1]`.** `apix_provenance="missing"` renders a red chip and disables
  overlays. The P1-era `ts_px=1.0` fallback must not survive into the provider.
- Star source order: TS_RECONSTRUCT job's `tomograms.star`, else
  `project_state.imported_tomograms_star_path()` (`services/project_state.py:849`).
- Subsumes `_resolve_recon_mrc_for_ts` (`ui/tomo_dashboard_dialog.py:3271-3295`).

## The inversion

- `_collect_species_data_for_ts` (`ui/tomo_dashboard_dialog.py:3411`): iterate
  `project_state.species_registry`; per species, `ce = ce_instance_for_species(state, sp.id)` (new
  helper in `services/dashboard_data.py`, inverting `resolve_species`
  (`services/models_base.py:152`) over `candidate_extract_instances` (:79)).
  - `iid = ce_iid` or synthetic `f"pick__{sp.id}"` (D-1: key only).
  - Enrichment (manifest, `entry`, `picks`, auto-kicks, `subtomo_jm` match) only when `ce` exists.
  - Geometry/dims: manifest entry first (parity), else `geometry_for_ts(...)`.
  - Color: `sp.color` (replaces `SPECIES_OVERLAY_COLORS[idx]`).
- **Parity contract:** one entry per CE instance exactly as today; one *extra* entry only for
  species with zero CE instances. CE-rich projects execute the identical path with identical
  inputs.
- Same inversion in `collect_species_journey` (`services/dashboard_data.py:566-578`).
- `_render_particles_section` gate: render when the registry is non-empty OR CE instances exist;
  empty registry shows an inline "New species" empty state instead of `return False`.
- Downstream `sp` consumers must tolerate `jm=None` / `job_dir=None` / `manifest={}` — the render
  path already guards most of these (the fallback renderer taught it to); sweep the rest.
- `ui/dashboard/pixel_sanity.py:237`-region species row from geometry when no CE.

## ArtiaX decoupling

- `prepare_curation_bundle` chain gets `candidates_star: Path | None`:
  `services/visualization/artiax_bridge.py:258` (skip `export_tomo_picks_to_coords` when None;
  verify `build_session_cxc` with an absent ref coords), `backend.py:684`,
  `services/curation/session_service.py:708`. `ui/curation_session_dialog.py:90` `can_load` drops
  the `candidates_star` requirement.
- Dashboard call sites (`_handle_curate_in_artiax` :3878, `_handle_load_into_session` :3916,
  `_handle_open_list_in_artiax` :4007) source `tomograms_star` from `TomoGeometry.tomograms_star`,
  not `job_dir/"tomograms.star"`.
- `.coords` auto-ingest (`_auto_kick_coords_ingest` :4142, and
  `backend.import_curation_picks` call at :4181) keys off `curation_dir(...)` + the geometry star,
  not the CE `job_dir`.
- Slab canvas for a no-CE species renders from `TomoGeometry.recon_mrc` via the existing
  `preview_render` slab helpers; the recon-cutout sheet path
  (`services/visualization/recon_cutouts.py`) already takes bare voxel coords.
- Keep `_render_imported_particles_section` (:3574) in place but unreachable behind the new gate —
  belt for one session; deleted in S4.

## S2 verification (user, runtime)

1. **Regression first**: a real TM project — Particles section identical to before (canvas, rail,
   gallery, curation, extraction) except species colors now come from the registry.
2. De-novo loop on imported tomos: create species → Curate in ArtiaX (zero candidates) → place
   picks → save → list auto-ingests → dots on the slab → keep/discard → merge.
3. Import an MRC with no voxel size and no override → red apix-provenance chip, overlays disabled;
   never rendered at a guessed scale.

## S4 — Journey unification

- Delete `_render_imported_particles_section` (:3574-3676) + its dispatch arm (:813-815) and
  `_render_imported_species_row` (:3552).
- `journey_signature` (`services/dashboard_data.py:621`): include pick-list counts/mtimes and the
  imported-tomograms record stamp — **cheap scalars only** (today a new manual list doesn't move
  the strip fingerprint at all).
- Empty-state copy: `_render_no_data_empty_state` (:694) mentions only array jobs — wrong for
  imported-only projects; cover both.
- Forever-spinner failure paths: slab-render auto-kick failure clears the dedup-set entry and shows
  an error chip instead of "Rendering tomogram slices…" until panel re-entry (:3785-3792, dedup set
  :2250, reset :6199; same class at :4765-4772).

## S4 verification (user, runtime)

1. New manual list moves the journey strip without leaving/re-entering the panel.
2. Kill a slab render mid-flight (or point at an unreadable MRC) → error chip, not spinner.
3. Imported-only project: no fallback section anywhere; unified panel handles everything.

---

## S2 CODE-COMPLETE 2026-08-13 — pending runtime

Verified to the sandbox ceiling only: `ruff check .` clean, `ruff format --check` clean on every
touched file. `python -m py_compile` and `check_boundaries.py` are owed (the venv's python symlink is
dead while `/software` is unmounted).

### Landed

**`services/visualization/tomo_geometry.py`** (new, ~215 lines). `TomoGeometry` +
`geometry_for_ts` + `tomogram_star_sources` + `read_tomo_table`, exactly the spec'd shape.
`is_usable` is the render gate. Star table and MRC header are memoized per path by mtime (one entry
each, self-invalidating) — this runs for every selected TS on every dashboard refresh, and the parse
is what costs; recon-MRC *existence* is deliberately re-checked every call so a volume that lands
mid-session appears without a restart.

**The inversion.** `services/dashboard_data.py` gains `ce_instances_by_species` /
`ce_instance_for_species` / `species_render_plan` / `pick_list_counts_for_species`;
`collect_species_journey` and `_collect_species_data_for_ts` both enumerate `species_render_plan`.
`_collect_species_data_for_ts` split into `_ce_species_entry` (the pre-inversion body, unchanged
except color + the dims tail) and `_denovo_species_entry`.

**ArtiaX decoupling.** `candidates_star: Path | None` through
`artiax_bridge.prepare_curation_bundle` → `session_service.prepare_curation_bundle` /
`load_into_session` → `backend`; `auto_coords` is None and `auto_count` 0 when there are no
reference picks, and the `.cxc` simply omits the `open <f>.coords` line (`session_chimerax_commands`
already took `Path | None`). `curation_session_dialog.can_load` now requires only `tomograms_star`.

**`.coords` auto-ingest** takes a `tomograms_star` instead of a CE `job_dir`, and is called once in
the collector loop for BOTH paths — for a de-novo species it is the only way picks enter the project.

**`services/pixel_chain.py`** emits a `Pick` row for every registry species with no CE instance.

### Deviations from the plan, with reasons

1. **Dims come from the recon MRC header first, not from the star.** The plan ordered
   `rlnTomoSize{X,Y,Z} / binning` ahead of the header. For a real reconstruction those columns hold
   the UNBINNED tilt-image size, whose Z is the tilt-image height rather than the tomogram
   thickness — the division is simply wrong there. More decisively, `coords.binned_tomo_size_from_
   tomo_row` (which backs the ArtiaX transform *and* what the preview manifest recorded) reads the
   header, so a star-first provider would make overlays disagree with the picks they round-trip.
   Star dims are kept as the fallback when the volume is absent.
2. **CE species keep sourcing `tomograms_star` from their own job dir.** The plan said all dashboard
   call sites should read `TomoGeometry.tomograms_star`. On a denoised chain the candidate-extract
   job's `tomograms.star` repoints `rlnTomoReconstructedTomogram` at the *denoised* volume, so
   switching to the recon job's star would silently change which volume ArtiaX opens. The geometry
   star is the fallback when the job copy is absent, and the only source on the de-novo path.
3. **`[1, 1, 1]` dims are gone from the CE path too**, not just the no-CE path — it is the same
   invented default the roadmap condemns, and it renders every dot in the corner. Unknown dims now
   disable the overlay (`_render_pick_layer` draws nothing, `_read_pick_list_voxels` returns `[]`)
   and the header chip says why. Only fires where today's render was already wrong.
4. **Pixel-sanity de-novo row uses the recon *job's* geometry, not the provider.**
   `compute_pixel_chain` is a pure state reader with no `project_path` and no TS — pulling per-tomogram
   disk reads into it for one row is out of proportion. On an imported-only project that row reads
   blank; the real per-tomogram geometry (with provenance) is in the Particles header chip.
5. **Parity is enforced by an explicit unclaimed-CE bucket.** A CE instance `resolve_species` cannot
   attribute to a *registered* species (bare iid, no `species_id`, ≥2 species) would have vanished
   under a naive registry-only loop. `ce_instances_by_species` returns it in `unclaimed` and
   `species_render_plan` appends it, so the entry count per CE instance is unchanged. Two CE
   instances resolving to one species also still emit two entries.

### Known visible diffs on a CE-rich project

- Species **order** follows the registry rather than sorted instance id (they usually coincide).
- Species **color** comes from `species.color` (S1) instead of the positional palette index.
- The Particles header carries a new geometry chip (dims · Å/px, red when unset).
- A project with tomograms but no species now renders a Particles card with a "New species" empty
  state instead of no card at all (D-5's second creation entry point).
- `_render_imported_particles_section` is now unreachable: any TS with an imported-star row resolves
  geometry, so `_render_particles_section` handles it. Left in place per the plan; deleted in S4.

### Owed

- The runtime checklist above (regression on a real TM project FIRST).
- `venv/bin/python -m py_compile` + `check_boundaries.py`.
- `_handle_extract_list` on a de-novo species notifies "no optimisation set to extract against yet"
  — S3 replaces that with `build_list_optset_from_tomograms`.
- No radio is pre-checked in the rail's `auth` column for a de-novo species (the default slug is
  `"auto"`, which has no row). Cosmetic; revisit with S3's authoritative-slug work.
