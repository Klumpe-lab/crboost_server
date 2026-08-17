# Roadmap 11 — Picks tab, Curation tab, Journey declutter

**Status:** S1 + S2 code-complete (S1 2026-08-16, S2 2026-08-17) — both PENDING RUNTIME; S3–S4 next. **Depends on:** 10-S1/S2 (page shell), 09 (services,
watcher, `species_overview`), 08 (rev). **Soft dependency:** roadmap 07 (extract_pick_list becomes
a real job) for *live* extraction status; until then the Picks tab shows the derived
`PickList.extraction_state()` only.
**Risk:** medium — this is the one roadmap that edits the 6605-line Journey module beyond
signatures. Mitigation: S1 is a refactor-only carve with a parity walkthrough before any UI moves.
Four commits: S1 carve · S2 Picks tab · S3 Journey declutter · S4 Curation tab (v1).

**Decision of record (user, 2026-08-16):** Journey = look & curate: canvas + per-list dot layers,
rail table (type · count · auth ◉ · ext ✓ · eye), galleries / cutout sheet with keep/drop, lasso,
brushing, display filter, Save/Reset, ONE ⚡ "load this tomo into ArtiaX", 3dmod handoff, geometry
chip. Species page = manage & act: session control center, import, merge, dedup, extract /
re-extract, extract-all-pending, authoritative selection, cross-tomo tables.

## 0. Facts (Stage 0 — gathered; all in `ui/tomo_dashboard_dialog.py` unless noted)

Particles section = `_render_particles_section` (:3839) → canvas (`_render_particles_canvas` :3937,
per-list layers `_render_pick_layer` :3250) + species tabs (:3906-3934) → `_render_species_tab_body`
(:4510) → `_render_list_rail` (:4661: table :4812-4886 with merge-check · swatch · name · count ·
auth radio (`_set_authoritative` :4787-4803, icons :4846-4853) · ext badge (:4854-4863) · copy path
· eye (`_render_list_eye` :4599); toolbox `.cb-list-toolbox` :4887 = Curate in ArtiaX
(`_handle_curate_in_artiax` :4071) · ⚡ (`_handle_load_into_session` :4109) · Import picks
(`_handle_import_curation_picks` :4394, path dialog `_open_manual_coords_path_dialog` :4423); merge
tick `_toggle_merge` :4727 + inline merge bar :4915 → `_do_inline_merge` :4735-4780 (uses
`_source_for` :4693 → 09-S2 `merge_source_for`), selection in module dicts `_MERGE_SELECT` :136 /
`_SELECTED_LIST_SLUG` :130) → detail `_render_list_detail` (:4932): auto → `_render_species_auto_section`
(:4949, gallery `_render_gallery_body` :5263 with keep/drop, lasso :5805, Save `_on_save` :5494 /
Reset `_on_discard` :5513, filter controls :2365); workbench list → `_render_single_list_cutouts`
(:3038) with the extraction bar `_render_list_extraction_bar` (:2828 → `_handle_extract_list`
:2850-2920 → `_submit_list_extraction` :2923-2961; geometry dialog `_prompt_extraction_geometry`
:2964-3148 persisting `species.extraction_params` :3011), the cutout sheet
`_render_list_cutout_sheet` (:2517, auto-commit `_commit_loop` :2600) + hover bridge (:2678), the
clash/dedup panel `_render_clash_panel` (:3156, `_do_dedup` :3216-3237) for merged lists; 3dmod row
`_render_3dmod_section` (:5037, called :4596); species admin toolbar (render previews / IMOD,
`_render_species_admin_buttons` :4465 — display-support, stays).

Backend/services reused as-is: `backend.extract_pick_list_and_wait` (:341),
`extract_authoritative_pending` (:538, submits per-list extraction for every PENDING list in a
`GateReport`, BackgroundTask-friendly), `get_authoritative_gate_report` (:394),
`compute_gate_report` (`services/aggregation_authoritative.py:371`), `enumerate_authoritative`
(:184), `backend.merge_pick_lists` / `list_clash_stats` / `deduplicate_pick_list` (:762),
`backend.import_curation_picks` (:732), `find_active_curation_session_any` /
`load_into_session` (`services/curation/session_service.py:347/:622`),
`open_curation_control_center` (`ui/curation_session_dialog.py:60`, parented at
`client.layout.default_slot`, per-client replace-on-reopen).

Journey strip: de-novo rows hardcode `subtomo_status: "pending"` (`services/dashboard_data.py:832`)
— per-list extraction never moves the strip for a de-novo species (roadmap 07 §1 item 3).

## 1. Stage S1 — refactor-only carve (Journey behavior unchanged)

- NEW `services/particles/list_ref.py`:
  `@dataclass(frozen=True, slots=True) class ListRef: project_path, species_id, species_label,
  tomo_name, slug, label, star_path, ce_job_dir, subtomo_job_dir, subtomo_iid` (UI-free; the Journey
  builds it from `sp`/`lst` render dicts, the Species page from `PickList` + `ce_instance_for_species`
  / `matching_subtomo_instance`).
- NEW `ui/particles/list_actions.py` (UI helpers shared by Journey and Species page):
  - `async extract_list(backend, ref, *, on_done)` ← `_handle_extract_list` + `_submit_list_extraction`
    (+ `prompt_extraction_geometry(...)` ← `_prompt_extraction_geometry` :2964-3148, which persists
    `species.extraction_params` through `state.mutate_species`); dependencies to break:
    `current_project_state()` → `state_for(ref.project_path)`; module `_curation_flight` → a
    module-level `SingleFlight` in `list_actions`; `refresh` closure → `on_done` (Journey passes
    `refresh`, Species page passes a no-op — the rev drives it).
  - `async merge_lists(backend, refs, name, *, on_done)` ← `_do_inline_merge` minus the selection
    bookkeeping (`_MERGE_SELECT`/`_SELECTED_LIST_SLUG` stay Journey-side).
  - `async dedup_list(backend, ref, radius, *, on_done)` ← `_do_dedup`; backend
    `deduplicate_pick_list` (:762) signature → `(project_path, species_id, tomo, slug, radius)` and it
    updates `pl.count` + saves + bumps the rev itself (today the dashboard patches the count :3230).
  - `async load_tomo_into_session(backend, ref, *, save_first)` ← `_handle_load_into_session`
    (:4109-4125, keeps `find_active_curation_session_any` + confirm-with-save-first).
  - `async import_picks_from_path(backend, ref)` ← `_open_manual_coords_path_dialog` (:4423) +
    `_persist_manual_pick_list` wrapper (09-S2).
- Journey call sites repointed; **no visible change**. Parity walkthrough = the existing runtime
  gates: merge → new chip + selected; dedup → count drops + STALE; extract → badge ✓; ⚡ swaps the
  viewer; import-by-path registers `manual`.

## 2. Stage S2 — Picks tab (`ui/species/picks_tab.py`)

- Rev-gated `FingerprintedView`, sig `(sid, state.registry_rev, tuple(extraction states),
  live-session flag)`; rows from `species_overview(state, project_path, sid).rows` (09-S3), grouped
  by tomogram: `tomo · list (type shape + label) · kept/total · auth ◉ · ext ✓○⚠ · source (hover:
  source_kind/source_ref) · actions`.
- Actions per row: extract / re-extract (`list_actions.extract_list`), dedup (merged lists), ⚡ load
  this tomo, "open in Journey" (`callbacks["toggle_journey"]` + select TS — the journey exposes
  `select_ts` through its callbacks; verify at execution), delete list (`state.remove_pick_list` +
  file cleanup of `<slug>.star`/`_filtered.star`/`<slug>/` — confirm dialog listing what goes).
  Row selection (checkbox) + a merge bar (name → `list_actions.merge_lists`) — same rule as today:
  ≥ 2 lists of one tomo. Authoritative radio per tomo (`state.set_authoritative_slug`, bumps rev).
- Species-level actions (header of the tab): "Extract all pending" = `compute_gate_report` →
  `backend.extract_authoritative_pending` in a BackgroundTask (tray-visible, `project_path`
  explicit) with the count preview ("3 pending · 2 blocked: <reasons>") shown BEFORE submit — the
  gate report is the pre-flight; "Import picks from path…" (`import_picks_from_path`); "Open
  Journey on this species".
- Status until roadmap 07 lands: derived only (badge from `extraction_state()`); after 07, the
  per-list instance's execution status + failure text (07-S3/S4) replace the badge's hover.
- Empty state: "No picks yet — load a tomogram into ArtiaX from the Curation tab, or run Pick
  candidates from the Jobs tab" (two links).

Verification: a TM project shows one `auto` row per tomo with kept/total; a de-novo species shows
its `manual` lists; extract from the table → badge ✓ (and, after 07, a status chip); "Extract all
pending" submits N jobs and refreshes; auth radio persists across reload.

## 3. Stage S3 — Journey declutter (behavior change, own commit)

Remove from the Particles section (each replaced by nothing — the Species page owns it):
- toolbox items Curate-in-ArtiaX + Import picks (`.cb-list-toolbox` :4887); keep ⚡ as the single
  per-tomo action (`list_actions.load_tomo_into_session`; when no session is live it opens the
  control center — today's behavior);
- merge tick-boxes + inline merge bar (:4727, :4915) and `_MERGE_SELECT`;
- extraction bar in the list detail (:2828 call site);
- clash/dedup panel (:3156 call site);
- authoritative radio → display-only ◉ (indicator stays in the table, click removed; set on the
  Species page).
Keep: rail table + eyes, species admin toolbar (render previews / IMOD), galleries + keep/drop +
lasso + brushing + display filter + Save/Reset, cutout sheet auto-commit, hover bridge, 3dmod row,
geometry chip, "New species" empty state (routes to `create_species`).
- Strip: de-novo `subtomo_status` derived from per-list extraction state
  (`dashboard_data.py:832`: any EXTRACTED list for (species, tomo) → "ok"; STALE → "warn"; else
  "pending") — this is roadmap 07 S4's first bullet; do it here if 07 hasn't landed, and note it
  there.
- Delete the now-unused Journey handlers left after S1 (`_render_list_extraction_bar`,
  `_render_clash_panel`, `_toggle_merge`, merge-bar builders, `_handle_curate_in_artiax`'s toolbox
  wiring) — grep for zero callers before deleting.

Verification (parity for CE-rich projects per the denovo S2 contract): canvas/layers/galleries/
keep-drop/3dmod identical; the rail shows the same rows minus the click affordances; ⚡ works; no
`_MERGE_SELECT` references remain.

## 4. Stage S4 — Curation tab v1 (`ui/species/curation_tab.py`)

- Session status chip (live / starting / off) polled ~16 s via `find_active_curation_session_any`
  (reuse the `_CURATION_SESSION_LIVE` cadence rule; move that poll out of the Journey into a small
  shared `ui/particles/session_status.py` observed by both); "Open control center" button →
  `open_curation_control_center` (existing dialog; v2 later = inline panel split of
  `curation_session_dialog.py`).
- Per-tomo rows for this species: tomo · has recon? · lists (n) · ⚡ load · "import from path".
- Save-dir contract, spelled out once per species: `Curation/<species_id>/<tomo>/`, format `.coords`
  (positions only, corner-Å), any filename except `auto.coords` / `*_ref.coords`, newest wins,
  auto-detected by the watcher within ≤ 35 s; copy button per tomo dir.
- Watcher ingest log (`CurationWatcher.events(project_path)` filtered by species: time · tomo ·
  file · count · result) and **unattributed dirs** (`watcher.unattributed(project_path)`) with the
  reason ("no species matches slug X" / "no tomogram matches slug Y" / "no geometry") — the
  never-silent rule for saves that could not be attributed.

Verification: save in ArtiaX with the Species page open → the row's list count and the log line
appear ≤ 35 s; a hand-dropped file in a wrong dir shows under "unattributed".

## 5. Risks

- Journey carve regressions (no tests): S1 is refactor-only with the parity walkthrough; S3 is the
  only behavior change and is one commit.
- Selection state (`_SELECTED_LIST_SLUG`) stays module-global in the Journey — unchanged here
  (peeve candidate, not a blocker).
- Two places to set the authoritative list would drift → it is set only on the Species page (S3
  makes the Journey radio display-only).
- `extract_authoritative_pending` runs many SLURM jobs at once — keep the count preview + confirm.

## 6. Modern-Python weave-in

`ListRef` frozen slotted dataclass; `match ref.list_type` for the shape/label; `Protocol` for the
`on_done` callback signature.

## Stage log (append-only)

- 2026-08-16 — scoped. No code.
- 2026-08-16 — **S1 CODE-COMPLETE** (branch `denovo_picking`; sandbox ceiling was `ruff check .` again
  — no python, so `py_compile` + `check_boundaries.py` are owed with the runtime pass; R1–R4 grep-
  approximated clean). Refactor-only carve, Journey behavior unchanged. NEW
  `services/particles/list_ref.py`: `ListRef` (frozen, slotted) = `project_path · species_id ·
  species_label · tomo_name · slug · label · list_type · star_path · ce_job_dir · subtomo_job_dir ·
  subtomo_iid · tomograms_star` (+ `is_auto`, `candidates_star`, `merge_source_dict()`), and `fs_slug`
  (the Journey's `_fs_slug`, moved — it names merged-list slugs and the cutout cache). Deviation from
  §1: `list_type` + `tomograms_star` added (the ArtiaX round trip needs the tomogram's star even for a
  job-less species; the merge source needs the type), `subtomo_iid` kept as written (the geometry job
  is resolved through `state.jobs[iid]`, not carried as a model). NEW `ui/particles/list_actions.py`
  (module `SingleFlight`, same keys as before): `extract_list(backend, ref, *, on_done)` (+
  `_submit_list_extraction`, `prompt_extraction_geometry` — now persists through `state.mutate_species`
  so the rev moves and the Overview's "extraction" chip repaints; state by
  `get_project_state_for(ref.project_path)`, subtomo model = `state.jobs.get(ref.subtomo_iid)`),
  `merge_source_for(ref)` (→ `picks_filter.merge_source_for` with `ref.merge_source_dict()`),
  `merge_lists(backend, refs, name) -> slug | None` (deviation: RETURNS the new slug and takes no
  `on_done` — the Journey must land `_SELECTED_LIST_SLUG` / clear `_MERGE_SELECT` before its rebuild),
  `dedup_list(backend, ref, radius, *, on_done)`, `curate_in_artiax(backend, ref)` (carved too — S3's
  ⚡ falls back to it when no session is live), `load_tomo_into_session(backend, ref)`,
  `register_imported_picks(backend, ref, result, *, on_done)` (= `_persist_manual_pick_list` +
  `_register_manual_pick_list` folded), `import_picks_from_path(backend, ref, *, on_done)`, and
  `extraction_badge(state)` (the `_EXTRACTION_BADGE` table, moved). `backend.deduplicate_pick_list`
  → `(project_path, species_id, tomo_name, slug, radius_ang)`: resolves the list's star from the
  registry, rewrites it, updates `pl.count`, persists by path and bumps the rev (only caller was the
  Journey). Journey (`ui/tomo_dashboard_dialog.py`, 6425 → 6045 lines): NEW `_list_ref(sp, lst | None,
  project_path)` (None = the tomo's `auto` slot for the per-tomo actions); `subtomo_iid` added to both
  species entries; call sites repointed (extraction bar button, clash panel `_do_dedup`, rail
  `_do_inline_merge` + toolbox Curate / ⚡, `_handle_import_curation_picks` fallback + registration);
  DELETED `_handle_extract_list`, `_submit_list_extraction`, `_prompt_extraction_geometry`,
  `_handle_curate_in_artiax`, `_handle_load_into_session`, `_persist_manual_pick_list`,
  `_register_manual_pick_list`, `_open_manual_coords_path_dialog`, `_source_for`, `_fs_slug`,
  `_EXTRACTION_BADGE`; kept: `_artiax_inputs` + the caller-less `_handle_open_list_in_artiax` (W1
  foundation, per its docstring), `_curation_flight` (new-species prompt, auto-discover import).
  Verification = §1 parity walkthrough (merge → new chip + selected; dedup → count drops + STALE;
  extract → badge ✓; ⚡ swaps the viewer; import-by-path registers `manual`).
- 2026-08-16 — **S2 IN PROGRESS (checkpoint, session ended on usage limit; `ruff check .` clean, all
  new helpers unused-but-importable — safe to leave in the tree, NOT to commit yet).** Landed so far:
  `services/particles/list_ref.py` + `SpeciesAnchors` / `species_anchors(state, project_path, sid)`
  · `tomograms_star_for(state, project_path, sid, tomo)` (the watcher's private
  `_tomograms_star_for` moved here and the watcher repointed — one policy) · `list_ref_for(...)` (a
  ref from `ListRow` facts + anchors) · `known_tomograms(state, project_path)` (import-picker
  universe); NEW `services/particles/list_admin.py`: `pick_list_files(pl)` (stars · `<slug>/` out
  dir · a `manual` list's `.coords` saves — the watcher would re-register them after a restart) +
  `async delete_pick_list(project_path, sid, tomo, slug)` (files → `remove_pick_list` → forced save;
  the authoritative choice is left DANGLING on purpose, the gate surfaces it); `list_actions`:
  `geometry_inputs()` + `commit_extraction_geometry(...)` factored out of the per-list prompt (for the
  extract-all pre-flight), `import_picks_from_path(..., tomo_options={tomo: ref}, intro=)` (tomogram
  picker for the species-level entry — the tomo may have no picks yet), `open_dedup_dialog(backend,
  ref, *, default_radius_ang, on_done)` (the clash panel as a dialog); CSS `.cb-ptable-row` /
  `.cb-ptable-group*` / `.cb-ptable-source` / `.cb-ptable-tick` in `ui/dashboard/css.py`.
  **Still to write:** `ui/species/picks_tab.py` (`PicksTab` + rev-gated `_PicksView`, off-loop
  `_compute` = `species_overview` rows → refs via `species_anchors` + `tomograms_star_for` per tomo +
  `known_tomograms`, recompute on `(registry_rev, job statuses)` else ≤ 15 s while shown — the
  Overview pattern; per-tomo groups: header = tomo · ⚡ `load_tomo_into_session` · "journey ↗"; rows
  = tick · auth radio (`set_authoritative_slug` + forced save + in-place icon flip) · swatch · label ·
  kept/total · ext badge · source (hover kind/ref) · actions extract / dedup (merged) / delete
  (confirm dialog listing `pick_list_files`); per-tomo merge bar when ≥ 2 ticked; header actions
  "Extract all pending" (pre-flight dialog = `backend.get_authoritative_gate_report` counts + blocked
  notes + `geometry_inputs()` when `extraction_params_for_species` is None → BackgroundTask
  `extract_authoritative_pending`, string summary) · "Import picks from path…" (`tomo_options`) ·
  "Open in Journey ↗"; empty state with links via a NEW page hook `callbacks["species_select_tab"]`);
  `ui/species/page.py` (`_make_tab("picks")` → `PicksTab(ctx)`, register `species_select_tab`);
  `ui/tomo_dashboard_dialog.py` (register `callbacks["journey_show_ts"](ts, section=None)` =
  `select_ts` + `_scroll_section_into_view`, next to `on_journey_active`); README row; S2 snapshot
  under `notes/11-stage-snap/s2/`. Deviation planned: no live-session flag in the S2 signature (S4's
  shared `session_status` adds it).
- 2026-08-17 — **S2 CODE-COMPLETE** (branch `denovo_picking`; `ruff check .` clean + `ruff format`
  clean on the touched files — the sandbox has ruff but `venv/bin/python3` is a dangling symlink to an
  unmounted `/software` tree, so `py_compile` + `check_boundaries.py` are STILL owed with the runtime
  pass). NEW `ui/species/picks_tab.py` (~470 lines): `PicksTab` + rev-gated `_PicksView` over an
  off-loop `_Computed` (`species_overview` rows + one `ListRef` per row + one per-tomo reference ref +
  species color), recomputed when `(registry_rev, job statuses)` moves / on show / at most every 15 s —
  the Overview tab's cadence, verbatim. Layout: per-tomo group header (name · n lists · ⚡
  `load_tomo_into_session` · "journey ↗") over a `.cb-ltable` whose rows use the landed
  `.cb-ptable-row` grid (tick · auth radio · swatch by `glyph_for` in the species color · label ·
  kept/total · ext badge · source with `kind · ref` hover · actions), then a merge bar that appears at
  ≥ 2 ticks of ONE tomogram. Per-row actions: extract / re-extract, dedup (merged lists only, via
  `open_dedup_dialog`, default radius Ø/2 from the SPECIES — the Journey reads it off the CE job,
  which a de-novo species has not got), delete (confirm dialog listing `pick_list_files` and naming
  the dangling-authoritative consequence). Header actions: "Extract all pending" (gate report as the
  pre-flight — counts, per-list lines, blocked reasons from `notes`, and `geometry_inputs()` inline
  when the species has no committed geometry — then a tray-visible BackgroundTask), "Import picks from
  path…" (`tomo_options` = rows ∪ `known_tomograms`, so a tomogram with no picks yet is a valid
  target), "Open in Journey". Empty state links to the Curation / Jobs tabs through the new
  `callbacks["species_select_tab"]`. Wiring: `ui/species/page.py` (`_make_tab("picks")` → `PicksTab`,
  hook registered); `ui/tomo_dashboard_dialog.py` `callbacks["journey_show_ts"](ts, section=None)` =
  membership check → `select_ts` → `_scroll_section_into_view` (a tomogram with no strip column is
  toasted, not silently ignored).
  **Deviations / findings:**
  (1) NEW `list_actions.dialog_host()` and every dialog of that module (geometry prompt, dedup,
  import) plus the two new Picks dialogs now open inside the page LAYOUT slot. `nicegui/events.py:406`
  is explicit that "the handler is called within the context of the parent slot of the sender", so a
  dialog opened from a row of a `FingerprintedView` table dies the moment a rev bump clears it —
  `load_tomo_into_session` had already hand-rolled this capture; it now shares the helper. This also
  hardens the Journey's copies of those dialogs against its own rail rebuild.
  (2) Row model = the checkpoint spec (per-row action icons AND merge ticks), NOT the one-toolbar-
  per-tomo variant floated in the session before it: the `.cb-ptable-row` grid landed in the tree
  already encodes 8 columns ending in `auto` for actions. Switching to a single per-tomo action bar is
  a contained change (drop `_render_actions`, widen the merge bar into a selection toolbar) if the
  walkthrough prefers it.
  (3) `build()` only paints "computing…"; the first compute is kicked by the page's
  `_refresh_visible_tab` — `asyncio.create_task` during page construction is not the tab's business
  (matches `OverviewTab`).
  (4) The merge name is held on the tab (`merge_names`) so ticking one more list — which is part of
  the view signature and therefore rebuilds — cannot eat a half-typed name.
  (5) As planned: no live-session flag in the signature; the ⚡ button does not know whether a session
  is up until S4's shared `session_status` lands (today it reports that through
  `load_tomo_into_session`'s own toast).
