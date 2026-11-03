---
name: project_curation_seed_arc
description: "picking_ui/13 (2026-09-04): one pre-seeded ArtiaX list per (species,tomo) + confirmed REST scope switch + one door (Curate picks) + honest spinner states — S0–S4 CODE-COMPLETE 2026-09-04, uncommitted, ruff green, check_boundaries.py + ONE runtime pass (13 §4) owed"
metadata: 
  node_type: memory
  type: project
  originSessionId: 8dffe8d2-4b7c-4e9b-9985-c10c9a0ba16f
  modified: 2026-09-04T14:59:50.814Z
---

PI feedback 2026-09-04: N candidate lists per (species, tomogram) is over-built; one list is the
norm. Built the same day, S0–S4 back-to-back, in one session (all three forks took the recommended
option). What is on disk now:

- **S1 seed.** `artiax_bridge.ensure_seed_coords` creates `Curation/<sp>/<tomo>/<sp>__<tomo>__picks.coords`
  (0 bytes, `open(p,"x")`, never truncates); `.cxc` opens it LAST; `session_service._ensure_seed_registered`
  registers it at the Curate click as a 0-pick row labelled `picks` (`ingest.DEFAULT_SEED_LABEL`, decided by
  `is_default_slug` on every write); `import_curation_picks(archive=False)` skips the `imports/` copy.
  `_read_pick_list_voxels` → `list | None` (None = unreadable, `[]` = parsed to 0 rows; only None warns).
  `picks_tab._count_user_saves` excludes an untouched 0-byte seed from the `saves` chip.
- **S2 switch.** `swap_chimerax_commands` + `cd_chimerax_command` back in the bridge;
  `CurationSessionService.switch_session_scope` (under `_switch_lock`): chain over REST → `cd` as its OWN call
  (chain fails whole on any UserError) → `_record_scope(how="switch")` rewrites `scope.json` + target
  manifest (`scope_set_by`, new `launched_at` = watcher hot-dir key). `backend.switch_curation_session`.
  Control center: `Switch session to this tomogram` (accent, only when `rest_enabled` + `rest_port`),
  `Restart on this tomogram` becomes `default` kind when REST is on; confirm-first; forces
  `session_status.poll` after success.
- **S3 one door.** Deleted: Picks-tab session chip + `_SESSION_TEXT` + `open_control_center`, roster
  `_build_curation_session_btn`/tick/paint/`_open_control_center` + `_TOMO_DASHBOARD_SVG` (→
  `static/icons/journey.svg`), viewer-rail `curate ↗` + `.cb-list-toolbox`/`.cb-toolbox-link`, the title
  hint label. `house_button(..., icon=)` exists now; `[Curate picks]` = `house_button(icon="view_in_ar")`
  with a 7 px `.cb-artiax-live` dot BEFORE it when `PicksTab.session_live_here(tomo)`; viewer =
  `grid_view` icon button, journey = icon button wrapping `load_icon_svg("journey.svg")`.
  `open_curation_control_center(bundle=)` is required (no unbound entry). `_render_list_rail` /
  `_render_species_tab_body` lost `manage_species`.
- **S4 honest states.** `_auto_kick_recon_slabs` → `"fresh"|"failed"|"in-flight"|"kicked"`;
  `_render_recon_slabs_sync` raises when a renderer returns None; `_RECON_SLAB_ERRORS` + Retry button;
  `PickViewerPage.show` calls `reset_auto_kick_state()`; `_auto_kick_preview_generation` → `str` reason
  (`""` = kicked/in-flight) stored as `sp["preview_note"]` → "No candidate preview — <note>" instead of a
  spinner.
- **Deviations (logged in 13 §5):** CLI `bundle --species-id` added; `ui/tomo_gallery.py` one-line `or []`;
  switch refuses a bundle without `seed_coords`.

**Where it lives:** `docs/roadmaps/completed/picking_ui/roadmap_13-seeded-list-and-session-switch.md` (stage table, §4
runtime checklist, §5 log). Roadmap 10 carries four "[13 note]" paragraphs + a §6 entry. Snapshots
`notes/pu13-stage-snap/s0…s4/`.

**How to apply:** nothing is committed — the maintainer got a 3-commit line-by-line script (S0 docs /
S1–S4 code + snapshots / S5 statuses). Next: maintainer runs `python check_boundaries.py` + the §4 runtime
checklist (12 items; item 4 = note whether ArtiaX's save dialog pre-fills the seed name — if not, reword the
control center's section 3). Never commit ([[feedback_never_commit]]); see [[project_artiax_bridge]],
[[project_picking_ui_wave3]], [[feedback_dont_gate_stages_on_runtime]].
