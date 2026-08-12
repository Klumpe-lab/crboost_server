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
