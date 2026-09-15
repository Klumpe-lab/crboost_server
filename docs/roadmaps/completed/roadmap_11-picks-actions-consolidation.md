# Roadmap 11 — Picks tab, Curation tab, Journey declutter

**Status:** ALL FOUR STAGES CODE-COMPLETE (S1 2026-08-16; S2, S4, S3 2026-08-17) — PENDING RUNTIME,
to be verified in ONE pass with the rest of the arc. S3 was held for one session on the grounds that
it DELETES working affordances (merge ticks, extraction bar, clash panel, import, Curate) while the
replacing tabs had never rendered; the maintainer lifted that gate on 2026-08-17 ("no worries about
unverified things… I'll take a double look after we are done with all the relevant roadmaps"), so it
landed on code-review confidence — see the S3 stage record for what that review caught, and for the
list of things reported but deliberately NOT fixed inside this stage.
**Depends on:** 10-S1/S2 (page shell), 09 (services,
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
**[FIXED by S3 — `dashboard_data.pick_list_subtomo_status`.]**

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

*Line numbers below are pre-S1 and are stale by ~600 lines after S1's carve — go by symbol name.
§3 was amended on landing (2026-08-17) with two additions the original spec did not call for; both
are marked ADDED and argued in the stage log.*

Remove from the Particles section (each replaced by nothing — the Species page owns it):
- toolbox items Curate-in-ArtiaX + Import picks (`.cb-list-toolbox` :4887); keep ⚡ as the single
  per-tomo action. ADDED: ⚡ is not a bare `list_actions.load_tomo_into_session` — with a live
  session it swaps, and with `off`/`unknown` it falls through to `list_actions.curate_in_artiax`,
  i.e. the control center (which checks liveness itself, so `unknown` cannot cause a second
  ChimeraX). Without that branch, removing the Curate button leaves `load_tomo_into_session`'s
  no-session toast pointing at a button that no longer exists;
- merge tick-boxes + inline merge bar (:4727, :4915) and `_MERGE_SELECT`;
- extraction bar in the list detail (:2828 call site);
- clash/dedup panel (:3156 call site);
- authoritative radio → display-only ◉ (indicator stays in the table, click removed; set on the
  Species page). It applies `species_overview`'s rule, legacy `filtered` included, so the two
  surfaces cannot light different rows;
- ADDED: a "manage in Species ↗" link in the Particles section header, following the active species
  tab (`callbacks["open_species"]`, threaded down — never stashed module-level, it closes over ONE
  client's page). Species → Journey already existed (11-S2); without the reverse route a user
  standing in the rail has no in-app path to any removed action, and the stage reads as a loss
  rather than a move.
Keep: rail table + eyes, species admin toolbar (render previews / IMOD), galleries + keep/drop +
lasso + brushing + display filter + Save/Reset, cutout sheet auto-commit, hover bridge, 3dmod row,
geometry chip, "New species" empty state (routes to `create_species`).
- Strip: de-novo `subtomo_status` derived from per-list extraction state
  (`dashboard_data.py:832`) — this is roadmap 07 S4's first bullet; do it here if 07 hasn't landed,
  and note it there. AMENDED on landing: the spec said "any EXTRACTED → ok; STALE → warn; else
  pending", but `warn` was dropped and the strip is TWO-state (`ok` / `pending`). Telling STALE from
  EXTRACTED requires `PickList.filtered_count` synced from disk (`picks_filter.sync_filtered_count`,
  a pandas read per list — which is why both headless readers call it); the strip derives this for
  EVERY tomogram on a render path, so it can afford neither the reads nor the wrong-but-plausible
  amber cell an unsynced cache produces for a list filtered in a prior session. Freshness stays on
  the Picks tab's per-list badge, which does sync. Also amended: the collector now takes `only_ts`,
  because it runs inside the main pane's signature. See the stage log.
- Delete the now-unused Journey handlers left after S1 (`_render_list_extraction_bar`,
  `_render_clash_panel`, `_toggle_merge`, merge-bar builders, `_handle_curate_in_artiax`'s toolbox
  wiring) — grep for zero callers before deleting.

Verification (parity for CE-rich projects per the denovo S2 contract): canvas/layers/galleries/
keep-drop/3dmod identical; the rail shows the same rows minus the click affordances; ⚡ works; no
`_MERGE_SELECT` references remain. Plus: with no ChimeraX up, ⚡ opens the control center (it must
NOT toast "click Curate in ArtiaX"); "manage in Species ↗" lands on the active species' PICKS tab;
setting the authoritative list there repaints the Journey's ◉ within a tick; a de-novo species' strip
cell moves off "not started" once any of its lists is extracted.

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
  Journey). Journey (`ui/tomo_dashboard_dialog.py`, 6007 lines after the carve — this
  entry first recorded "6425 → 6045", which matches no artifact; the S1 snapshot is 6007): NEW `_list_ref(sp, lst | None,
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
- 2026-08-17 — **S3 CODE-COMPLETE** (`ruff check .` clean, `ruff format` clean on every touched file;
  `py_compile` + `check_boundaries.py` still owed — `venv/bin/python3` is a dangling symlink into an
  unmounted `/software`). Landed under the lifted per-stage runtime gate, so CODE REVIEW WAS THE
  VERIFICATION: a 5-agent preflight audit before editing, then a 4-dimension adversarial review of the
  diff against `notes/11-stage-snap/s4/` with an independent refuter per finding. Four defects the
  review caught are fixed in this same stage and are called out below — none would have been caught by
  ruff.
  `ui/tomo_dashboard_dialog.py` 6010 → 5861 lines, and 5854 once the two `ruff format` reflows this
  stage's own edits left behind (the `_render_list_rail` call that lost its `refresh` argument, and
  the rewritten ⚡ tooltip) were folded in — the "`ruff format` clean on every touched file" claim
  above was false for this one file until then; the other 3 format hunks in it are inherited. DELETED: `_render_list_extraction_bar`,
  `_render_clash_panel`, `_dedup_default_radius` (the Journey's copy — `picks_tab`'s same-named helper
  is a different function and stays), `_handle_import_curation_picks`, `_MERGE_SELECT` +
  `_box_style`/`_update_merge_bar`/`_clear_merge`/`_toggle_merge`/`_do_inline_merge` + the merge bar,
  `_set_authoritative` + `auth_icons`, the toolbox Curate + Import buttons, the `ErrorCode` import.
  `_list_ref(sp, lst, project_path)` → `_tomo_ref(sp, project_path)`: after S3 the only surviving
  caller passed `lst=None`, so the per-list branch was dead. `_render_list_rail` lost its `refresh`
  parameter (nothing in it refreshed any more). KEPT and verified still-used: `_SELECTED_LIST_SLUG`
  (sticky rail selection), `_curation_flight` (new-species prompt + `_handle_open_list_in_artiax`),
  `_list_count_text`, `_artiax_inputs` + the deliberately caller-less `_handle_open_list_in_artiax`
  (W1 foundation), and the rail's per-list extraction BADGE (read-only, unlike the bar).
  CSS (`ui/dashboard/css.py`): `.cb-ltable-row` 8 → 7 grid tracks (the merge-tick column); NEW
  `.cb-auth-static` (the read-only radio must not offer a pointer or a hover) — and NO
  `.cb-strip-pickcell.warn`, since deviation (1) below dropped `warn` (this entry first claimed the
  rule; it was never added); `.cb-merge-bar` KEPT — the Picks tab reuses it; `.cb-ptable-row` still
  overrides the grid for the Picks tab's 8 columns (later rule, same specificity — checked).
  Strip (`services/dashboard_data.py` + `ui/dashboard/strip.py`): NEW
  `pick_list_subtomo_status(state, species_id, tomo_name)` — `ok` once ANY list of the (species,
  tomogram) has an extraction output on disk, else `pending`, via `PickList.extraction_state()` (the
  one authority, never re-derived); the de-novo branch of `collect_species_journey` uses it instead of
  the hardcoded `"pending"`.
  **Deviations / findings:**
  (1) **`warn` DROPPED — the spec's three-state rule was not implementable honestly here.** Two
  problems, found in that order. First, `"warn"` was not a legal strip token at all: `_STATUS_WORD` had
  no entry (the tooltip would print the raw word) and `_cell_class` had no branch (the cell would be
  styled `has` — identical to a healthy one — because a de-novo row's `pick_status` is `ok` whenever it
  has picks). Adding the token was easy. The second problem was not: STALE-vs-EXTRACTED turns on
  `PickList.filtered_count`, a CACHE that only self-heals when the Journey renders that tomogram's
  species tab (`_collect_pick_lists_for_species`) — which is exactly why `aggregation_authoritative`
  and `species_overview` each call `picks_filter.sync_filtered_count` first, and why that call's
  docstring names the failure: a list filtered in a PRIOR session reads falsely STALE right after a
  correct extraction. The strip derives this for EVERY tomogram on a render path, so syncing (a pandas
  read per list) is unaffordable and not syncing paints a wrong-but-plausible amber cell — the thing
  CLAUDE.md's surfacing-uncertainty rule exists to prevent. So the strip answers the question it can
  answer correctly and cheaply ("has extraction happened here"), and freshness stays on the Picks tab's
  per-list badge, which syncs. §3 amended above. `_STATUS_WORD` still gained `"skip"` —
  `services/array_tasks.scan_statuses` has always been able to emit it and it was rendering as the raw
  token (opportunistic, per the never-silent policy).
  (2) NEW `only_ts` parameter on `collect_species_journey`, passed by `_main_signature`. That signature
  fingerprints ONE tilt series but was collecting the whole project, so the new per-list stats would
  have been paid ~40× over for rows it never reads. The strip's own collect stays unfiltered — it draws
  every column.
  (3) ADDED, not in §3: the ⚡ fallback to `curate_in_artiax` and the "manage in Species ↗" link. §3 is
  amended above with the argument for each.
  (4) **Caught by review — the display-only radio would have gone stale.** `_main_signature`
  deliberately EXCLUDED the authoritative choice, which was correct only while the Journey repainted it
  in place through `auth_icons`. With the click gone, setting it on the Picks tab moved nothing the
  main pane fingerprints, so the Journey would have kept lighting the old row indefinitely — the exact
  drift §5 lists as the reason to have one setter. Fixed: `auth_sig` (in-memory dict lookups per
  species) folded into the signature, and the stale comment corrected.
  (5) **Caught by review — the ⚡ tooltip lied on first paint.** `session_status` starts `unknown` with
  an EMPTY error (never polled ≠ poll failed), and the Journey paints before the first poll, so the
  tooltip read "Could not ask SLURM whether a session is running () — …". Now three-way: live / off /
  errored-with-reason / not-yet-polled.
  (6) **Caught by review — the link landed on the wrong tab.** `open_species` reuses the page's last
  tab (`DEFAULT_TAB = "overview"` on a fresh workspace), so the link advertising merge/extract/dedup
  arrived where none of them are. It now composes `open_species` + `species_select_tab("picks")`.
  (7) **Caught by review — legacy `filtered` auth slug.** `species_overview` lights the auto row when
  the stored slug is `auto` OR `filtered`; a strict equality here would have disagreed on legacy
  projects. The Journey now mirrors the tuple.
  (8) Stale user-facing copy that S3 falsified, all fixed: the new-species toast ("pick into it with
  'Curate in ArtiaX'"), `load_tomo_into_session`'s no-session toast (named a button the Journey no
  longer has — now surface-neutral, and the ⚡ pre-empts it anyway), the control center's "open from a
  species gallery's Curate in ArtiaX" line, `pipeline_builder_panel`'s comment, and three docstrings
  (`_render_list_header`, `_handle_open_list_in_artiax`, `.cb-detail-meta`'s CSS comment).
  **Owed / reported, NOT fixed here (the maintainer's call during the verification pass):**
  *(a)–(g) and (i) were all fixed 2026-08-18 — see the last two entries of this log. Only (h),
  the capability delta, still stands as written.)*
  `refresh_roster` calls on every keep/drop Save. The new derivation costs 0 stats for a never-extracted
  list and ~4 for an extracted one, so a fully-extracted de-novo project with 40 tomograms × 2 lists
  pays ~320 stats per Save. Nothing else in that function is memoized either (see (b), (c)), so a memo
  here alone would be inconsistent; the fix that pays for all of it at once is hoisting the collectors
  so `refresh_all` computes `journey`/`species_journey` ONCE instead of twice (`_main_signature` and
  `render_strip` each call both, and `render_main` computes the signature before its `force`
  short-circuit).
  (b) `_candidate_extract_status_per_ts` re-scans every `tmResults/*_particles.star` on EVERY call
  because `if not zero_picks:` treats a manifest's `"zero_picks": []` — what real manifests on disk
  carry — as a cache miss. One-line fix: `if "zero_picks" not in summary:`.
  (c) `read_preview_manifest(jd)` is parsed twice per CE species per call (once in
  `collect_species_journey`, once inside `_candidate_extract_status_per_ts`).
  (d) `PickList.extraction_state()` does one redundant `src.exists()` before `src.stat()` inside a
  `try/except OSError` that already covers the missing file (4 stats → 3 in the EXTRACTED case).
  (e) An UNCLAIMED candidate-extract instance draws a full species tab in the Journey but has no
  Species-page presence at all (the page's universe is `state.species_registry`), so after S3 it has no
  home for the moved actions. Either suppress it or mark it, per the surfacing-uncertainty rule.
  (f) `curation_tab`'s `species_tomo_map` is called with no `extra_tomos` (unlike `picks_tab`), so a
  tomogram described only by a CE job's own `tomograms.star` gets a Journey rail row but no Curation-tab
  row; related, `tomograms_star_for` returns a CE species' star for every tomogram without checking the
  tomogram is a row in it, so `has_geometry` can read True and fail later inside
  `prepare_curation_bundle`.
  (g) `picks_tab` renders a merge tick on the AUTO row unconditionally; merging an auto row of a species
  with a subtomo job but no candidate-extract job reaches `picks_filter`'s `raise ValueError` uncaught.
  (h) Capability delta, roadmap-sanctioned: the removed Import button auto-discovered the newest
  `.coords`; both Species-page entries are path-only. The `CurationWatcher` ingests the same file
  automatically and the Curation tab shows its log + unattributed dirs, so nothing is silent — but there
  is no longer a "scan for saves now" button anywhere.
  (i) `PlaceholderTab` (`ui/species/tab.py`) still has zero callers (noted at S4).
- 2026-08-17 — **S4 CODE-COMPLETE** (`ruff check .` clean, `ruff format` clean; `py_compile` +
  `check_boundaries.py` still owed — no python in the sandbox). Stage order deviates from the doc: S4
  landed BEFORE S3 on purpose (see the Status note — S3 is the only destructive stage and is held for
  the runtime pass; S4 is additive and is what makes S3 safe, since it gives "Curate in ArtiaX" and
  "Import picks" the home S3 removes them from).
  NEW `ui/particles/session_status.py`: the Journey's `_CURATION_SESSION_LIVE` dict + its `% 4` tick
  gate replaced by a process-wide cache with a `POLL_S = 16 s` self-throttle, so N observers cost at
  most one `squeue`. **Three states, not two** — a poll that RAISES now reports `unknown` with the
  error text instead of `off`; reading a squeue failure as "no session" would invite the user to
  launch a second ChimeraX. The Journey reads `session_status.status()` in both its signatures and
  `is_live()` for the Curate-button color; `_CURATION_SESSION_LIVE` and `_curation_tick` deleted.
  NEW `ui/species/curation_tab.py`: session chip + "Open control center"
  (`open_curation_control_center`, project-level — the inline split stays v2 as scoped) · the save
  contract stated once per species (`Curation/<species-slug>/<tomogram>/`, `.coords` positions-only
  corner-Å, any filename except `auto.coords` / `*_ref.coords`, newest wins) with the pickup times
  DERIVED from the watcher's own `TICK_SEC` / `FULL_SWEEP_EVERY` / `SETTLE_SEC` (~7 s for a tomogram
  loaded in the session, ~32 s otherwise) rather than the retyped "≤ 35 s" · per-tomogram rows
  (name · geometry-missing chip · lists · saves-on-disk · curate · ⚡ · import-by-path · copy the save
  dir) · the watcher log filtered to this species (newest 12) · and the project-wide **unattributed
  saves** block with each dir's reason.
  Supporting changes: `CurationWatcher.unattributed()` now returns `{"dir", "reason"}` dicts — the
  09-S4 signature dropped the reason on the floor, which is exactly the thing this block exists to
  show (no callers existed, so the change is free). NEW `list_ref.species_tomo_map()` (anchors + one
  `{tomo: tomograms.star | None}` map in a single disk pass) and `list_ref.auto_ref()` (the per-tomo
  reference slot); both Species-page tabs now build refs from them and `picks_tab._compute` was cut
  over — one policy for "which tomograms could this species have picks on", not two.
  **Findings:** (a) `PlaceholderTab` (`ui/species/tab.py`) now has zero callers — all five tabs are
  real. Left in place (surgical-diff rule); it is a dead-code candidate for a later sweep.
  (b) The Curation tab needs NO timer of its own: the page only ticks the visible tab, so
  `refresh()` kicks the shared session poll when stale and the 15-s disk recompute otherwise — polls
  observe, they do not rebuild.
  **Not done, by design:** the inline control-center panel (v2), and any per-tomogram "start session
  here" beyond what `curate_in_artiax` already does.
- 2026-08-18 — **(e) (f) (g) from S3's reported-not-fixed list are FIXED** (`ruff check .` clean;
  `ruff format --check` clean on all six touched files; the maintainer ran `python -c "import main"`,
  `check_boundaries.py` and `ruff check .` green on the arc immediately before). Reviewed with the
  maintainer, who re-scoped (e) on the spot: an unattributable particle job is not a rendering
  problem to mark, it is a state that must not be creatable.
  **(e) → an invariant, not a badge.** `add_instance_to_pipeline` (`ui/pipeline_builder/
  pipeline_builder_panel.py`) now refuses a NEW `PHASE_PARTICLES` instance with no `species_id`.
  The chooser in `prompt_species_and_add` already asked, but it is one caller of four: the two
  registered callbacks (`add_job_to_pipeline` drops `species_id` entirely, `add_instance_to_pipeline`
  leaves it optional — both currently unconsumed, both open) plus any future direct call. Re-selecting
  an EXISTING instance is untouched. `_ensure_prerequisites` does NOT route through the gate; it
  cannot produce a particle job today because every `JobSpec.prerequisite` in the table points at
  `tsImport`, and the comment at the gate says so, so giving a particle job a particle prerequisite
  is a change that has to notice this. The legacy unclaimed-instance RENDER path is deliberately
  left alone — `species_render_plan` keeps emitting those entries (parity contract) and the
  maintainer does not care about legacy jobs; what changed is that no new one can be minted.
  **(f) → two defects, one call pair** (`services/particles/list_ref.py`). `tomograms_star_for`
  returned the CE job's `tomograms.star` for ANY tomogram as long as the species had a CE job and the
  file existed, so `has_geometry` (`curation_tab.py:106`) read True for tomograms that star does not
  describe and the failure landed later inside `prepare_curation_bundle` — a late crash where the
  surfacing-uncertainty rule wants a disabled control. It now checks the row is actually there
  (`_star_tomograms`, memoized by `read_tomo_table`'s mtime cache, so per-tomogram costs one parse per
  file) and falls through to the geometry provider otherwise. Second: `species_tomo_map`'s universe was
  `extra_tomos ∪ known_tomograms`, and `known_tomograms` reads only the reconstruct and imported stars
  (`tomogram_star_sources`) — so the Curation tab, which passes no `extra_tomos`, dropped every
  tomogram described only by a CE job's own star or arriving through a merge. On a merged project with
  no local reconstruct job that tab listed NOTHING while the Journey listed everything. Fixed at the
  source rather than at the caller (`_species_tomograms`: the species' pick-list tomograms ∪ its CE
  star's rows), so a future third caller cannot get it wrong by forgetting an argument.
  **(g) → the raise is now unreachable from the UI** rather than caught. `picks_filter.auto_source_for`
  is split out of `merge_source_for` (same two branches, returns None instead of raising), so
  `list_actions.can_merge_source` can ask BEFORE the control is drawn; `picks_tab` renders the merge
  tick disabled (`.cb-ptable-tick.off`, dashed + not-allowed) with the reason in its tooltip. The
  reachable combination was never exotic: `auto_counts` is populated from the subtomo job's per-tomo
  curation records, which outlive any committed `particles_filtered.star`, so a species with a subtomo
  job, no committed filter and no CE job drew a mergeable-looking auto row whose merge could only
  answer by raising out of the click handler. `list_actions.merge_source_for` re-checks and notifies
  rather than propagating, because disk can change between render and click.
- 2026-08-18 — **(a)–(d) and (i) fixed** in the same session, closing S3's reported-not-fixed list
  except (h) (`ruff check .` clean; `ruff format --check` clean on `dashboard_data.py`,
  `project_state.py`, `species/tab.py`; `tomo_dashboard_dialog.py` still reports three format hunks at
  lines 1369 / 1691 / 2040 — all PRE-EXISTING and nowhere near these edits, left alone rather than
  reformatting a 5.8k-line file for unrelated reasons).
  **(b) was the biggest single win and a one-liner.** `_candidate_extract_status_per_ts` tested
  `if not zero_picks:` to decide whether the manifest's fast path had missed — but a v10+ manifest that
  found no zero-pick tomograms records `"zero_picks": []`, which is what real manifests on disk carry.
  Every call therefore fell through and re-scanned every `tmResults/*_particles.star`. Now keyed on the
  KEY's presence (`if "zero_picks" not in summary`), so absence means pre-v10 and an empty list means
  an answer.
  **(c)** `_candidate_extract_status_per_ts` takes an optional pre-read `manifest`.
  `read_preview_manifest` is an uncached `read_text` + `json.loads` (no mtime memo, unlike
  `read_tomo_table`), and `collect_species_journey` had already read the same document one line
  earlier — so every CE species parsed it twice per collect. The other caller passes nothing.
  **(d)** `PickList.extraction_state` dropped the `src.exists()` guard before `src.stat()`: the
  enclosing `try/except OSError` already covers a missing file (`FileNotFoundError` IS an `OSError`),
  and the fall-through is the same EXTRACTED either way. 4 stats → 3 in the EXTRACTED case.
  **(a) is the hoist the stage record asked for, and it got easier once (b)/(c) landed.** `refresh_all`
  now collects `collect_dashboard_journey` + `collect_species_journey` ONCE and passes both down;
  `render_strip` and `_main_signature`/`render_main` take them as optional params and collect for
  themselves only when called bare (selection change, exclude toggle, the pane's `refresh_roster`
  callback — which is always invoked with no arguments, checked at :4779/:4809). `_main_signature`
  slices its TS's rows out of the strip's UNFILTERED result rather than paying a second filtered pass,
  which is sound because `only_ts` is a pure row filter — two `continue`s in `collect_species_journey`,
  nothing else. Explicit parameters rather than a pass-scoped memo dict: no ordering hazard, and a
  future caller cannot accidentally pay the unfiltered cost where it used to pay the filtered one.
  `render_main`'s new params are keyword-only so its `_build_panel_toggle_row` callback use is
  unaffected.
  **(i)** `PlaceholderTab` deleted (`ui/species/tab.py`, 50 → 36 lines). It scaffolded tabs that had
  not landed yet; all five are real now, and it had zero callers.
