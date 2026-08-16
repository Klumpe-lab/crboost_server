# Roadmap 10 — the Species page (replaces the "Template Workbench" view)

**Status:** scoped 2026-08-16; no code. **Depends on:** 08 (identity/rev gates, `species_pill`),
09-S2/S3 (ingest service, `species_overview`). **Unblocks:** 11 (Picks / Curation tabs live here).
**Risk:** medium (new page shell; the workbench module itself is mounted, not changed).
Four commits: S1 shell + rail + tabs · S2 Templates & masks tab (mounted workbench) · S3 Overview
tab (identity editor moved out of the workbench, delete cascade → service) · S4 Jobs tab.

**Decisions of record (user, 2026-08-16):** the Species page replaces the Template Workbench view;
`ui/template_workbench.py` (2347 L: molstar bridge, ready gate, dedup, polarity pairs) is
**mounted as a tab, not rewritten**. Journey = look & curate + one ⚡; Species = manage & act (see
11). Cross-project catalog = hook now (`catalog_id`, 09-S2), build last (12).

## 0. Facts (Stage 0 — gathered)

- Mounting today: `ui/workspace_page.py:209-214` builds `build_species_workbench_panel(backend)`
  once into `workbench_container`; `_switch_to("workbench")` flips CSS display (:77-115); nav button
  `pipeline_roster.py:878` (`vial.svg`, "Template Workbench", `panel.toggle_workbench`).
- `ui/species_workbench_panel.py` (196 L): `_prompt_species_name` (:8-33, also used by the roster
  :1437 and the Journey empty state `tomo_dashboard_dialog.py:3808`); a horizontal strip of species
  buttons (:49-79); one `TemplateWorkbench` per species built **lazily on first switch**
  (:105-133: mkdir `templates/<sid>`, container `w-full overflow-auto; flex:1 1 0%; min-height:0`,
  visibility toggled, constructed `with container:`); `_add_species` (:146-154).
- `TemplateWorkbench.__init__(backend, project_path, species_id, *, on_species_deleted=None)`
  (`template_workbench.py:139`) renders immediately into the *current* slot (`_render` :1002-1012)
  — there is no `self.container`; per-species iframe id + event name (:150-157); registers a window
  `message` listener per instance and never removes it (:1016-1031); layout = species header
  (:1077-1162: swatch `_render_color_swatch` :1037-1075, name, Ø, sym, notes, delete) → templates
  cards + source tabs → masks cards + tabs → viewer (molstar/slice) → activity log. Every header
  edit goes through `_mutate_species` (:257-263 → after 08-S0 `state.mutate_species`) +
  `_save_state` (:265-266, non-debounced `backend.save_project(path)`; notes fire per keystroke
  :1143-1152). Delete cascade `_request_delete_species` (:509) → `_do_delete_species` (:555-596:
  `backend.delete_job` per bound job, `_delete_file_with_sidecar` (:598) per template/mask,
  `os.rmdir(output_folder)` :582, `remove_species` + save :586-589, `on_species_deleted`).
- Compact segmented control precedent (NOT Quasar tabs, per `feedback_ui_chrome_conventions`):
  `_render_tab_switcher` (`ui/pipeline_builder/job_tab_component.py:282-312` — flat dense 9px
  buttons, right borders, active `#f1f5f9`); also `_origin_toggle` (`ui/aggregation_merge_card.py:507-519`).
  The Journey's `.cb-species-tabs` (`ui/dashboard/css.py:419-446`) is Quasar `ui.tabs` styled compact
  — do not copy that one.
- Species pill: one component after 08-S1 (`ui/components/species_pill.py`).
- Species↔job attribution: `resolve_species` (`services/models_base.py:152-182`; suffix → `species_id`
  → single-species fallback); inversion helpers `ce_instances_by_species` (`services/dashboard_data.py:720`),
  `ce_instance_for_species` (:741), `matching_subtomo_instance` (:711); job creation
  `PipelineBuilderPanel.add_instance_to_pipeline(job_type, species_id=...)`
  (`pipeline_builder_panel.py:256-345`; TM copies template/mask/symmetry :322-327, CE copies
  diameter :331-334; snapshot-at-creation — kept, D-5) and `prompt_species_and_add` (:108-167).
- Three create-species paths: roster (`pipeline_roster.py:1441-1457`, origin=manual, no mkdir),
  panel (`species_workbench_panel.py:146-154`, origin=workbench, mkdir), Journey empty state
  (`tomo_dashboard_dialog.py:3805-3822`, origin=manual, force save).

## 1. Stage S1 — shell, rail, tabs, prompt (behavior-preserving swap of the view)

- NEW `ui/species/page.py: build_species_page(backend, callbacks) -> None`, mounted at
  `workspace_page.py:214` in place of `build_species_workbench_panel(backend)`; keep the container /
  mode name `"workbench"` internally (rename to `"species"` in the same commit only if trivial — the
  roster's `set_active_mode` :815-824 and `_switch_to` compare the string); nav label
  `pipeline_roster.py:878` → "Species" (icon can stay `vial.svg`).
  Layout: `[rail 200px | detail]`; detail = header row (pill + segmented tabs) + one container per
  tab (`Overview | Templates & masks | Picks | Curation | Jobs`), visibility-flipped; **per-species
  content built lazily on first selection** and cached (`{(species_id, tab): container}`).
- NEW `ui/species/rail.py: SpeciesRail(FingerprintedView)` — vertical list, one row per species
  (`species_pill` + counts from a cheap in-memory read: n pick lists, n templates), "+" at the bottom;
  `signature() = (active_id, state.species_identity(), tuple(n_lists per species))`; the strip
  logic of 08-S0's `_StripView` moves here (delete `_StripView`/`_observe` from the old panel — the
  page owns a 3-s observe + `callbacks["on_workbench_active"]`).
- NEW `ui/components/segmented.py: segmented(container, tabs, active, on_switch)` hoisted from
  `_render_tab_switcher` (leave the job tab's copy in place; repoint later, optional) + `.cb-seg` /
  `.cb-seg-btn` / `.cb-seg-btn.active` in `ui/dashboard/css.py` (`ensure_assets_loaded()` at page
  build).
- NEW `ui/species/prompt.py`: move `_prompt_species_name` here and add
  `async def create_species(backend, project_path, *, origin) -> ParticleSpecies | None`
  (prompt → `state.add_species(name, origin=origin)` → `save_project(project_path, force=True)`);
  repoint the three paths (roster, page "+", Journey empty state). The workbench-only mkdir of
  `templates/<sid>` moves into the Templates tab's first mount (`TemplateWorkbench.__init__` :149
  already mkdirs).
- Delete `ui/species_workbench_panel.py` at the end of S1 (its content is split across
  `page.py`/`rail.py`/`prompt.py`).
- Tabs Picks/Curation/Jobs render a one-line "coming in roadmap 11 / this roadmap S4" placeholder
  in S1 (no dead affordances — just the label).

Verification: view opens as before; species list = registry; "+" creates and selects; switching
species toggles cached content; label reads "Species".

## 2. Stage S2 — Templates & masks tab (mounted workbench)

- `ui/species/templates_tab.py`: on first selection of (species, this tab) construct
  `TemplateWorkbench(backend, str(project_path), species_id=sid, on_species_deleted=page._on_deleted)`
  **inside the tab container** (`with container:`), exactly like `_ensure_species_rendered` did
  (:105-133); cache the container; on species switch only flip visibility — **never `clear()` and
  rebuild a workbench** (iframe reload, orphaned window listener → double `emitEvent`).
- Nothing changes inside `template_workbench.py` in S2 (header still rendered by the workbench until
  S3 moves it).

Verification: molstar loads a template on the eye click; switching species and back does not
re-init the iframe; activity log per species stays separate.

## 3. Stage S3 — Overview tab (identity editor moved out; delete cascade → service)

- NEW `services/species_admin.py` (headless): `async def delete_species(backend, project_path,
  species_id) -> dict` = the cascade from `_do_delete_species` (:555-596): bound jobs via
  `state.species_references(sid)["jobs"]` → `backend.delete_job(...)` each (errors collected, not
  fatal), template/mask files + sidecars (`_delete_file_with_sidecar` :598 → move here), `os.rmdir`
  of `templates/<sid>` (empty-only), `state.remove_species(sid)` (now dirty+rev), forced save.
  Returns `ok(deleted_jobs=[...], deleted_files=n, errors=[...])`. The workbench's
  `_do_delete_species` becomes a call into it (keeps its confirm dialog).
- NEW `ui/species/overview_tab.py`:
  - **Identity editor** = `_render_species_header` (:1077-1162) + `_render_color_swatch`
    (:1037-1075) moved here as `render_identity_editor(backend, project_path, species_id)`; writes
    via `state.mutate_species(sid, fn)`; persistence `backend.save_project(path, debounce_s=1.0)` and
    `.props("debounce=400")` on text inputs (kills the per-keystroke full save, peeve P-02). Then
    delete `self._render_species_header()` from `TemplateWorkbench._render` (:1007) and the two
    helpers. **The editor is built once per species in its own container — NOT inside a rev-gated
    view** (the rev moves on every keystroke it makes).
  - **Status block** (rev-gated `FingerprintedView`, sig `(sid, state.registry_rev)`): from
    `species_overview` (09-S3): tomos with picks / picks / kept / extracted lists / gate roll-up;
    bound CE + subtomo instance ids; extraction geometry (`species.extraction_params` or "not set —
    asked on first extract"); templates/masks counts + selected names.
  - **Provenance line**: `origin` (first reader of the field), `created_at`, `catalog_id` ("—" for
    now), template/mask sources as hover.
  - **Sanity row**: this species' rows from `services/pixel_chain.compute_pixel_chain(state)` +
    `apply_sanity_rules` (already species-aware, `pixel_chain.py:282-297`) rendered with the
    dashboard's compact table (`ui/dashboard/pixel_sanity.render_pixel_sanity_table` filtered).
  - Delete button → confirm dialog (lists `species_references`) → `species_admin.delete_species`.
- Verification: header gone from the workbench, present on Overview; edits persist (one save per
  pause, not per keystroke); delete from Overview removes jobs/files/registry and re-selects.

## 4. Stage S4 — Jobs tab

- NEW `services/particles/species_jobs.py: jobs_for_species(state, species_id) -> list[(iid, jm)]`
  — generalise `ce_instances_by_species` (`dashboard_data.py:720`) to all particle-phase job types
  (`PHASE_JOBS[PHASE_PARTICLES]`) via `resolve_species`, honoring the unclaimed bucket.
- NEW `ui/species/jobs_tab.py` (rev-gated + job-status-gated `FingerprintedView`, sig
  `(sid, registry_rev, tuple((iid, execution_status) …))`): rows = job type · iid · status dot ·
  drift chips (species `symmetry`/`diameter_ang` vs the job's snapshot `symmetry` /
  `particle_diameter_ang`; TM template/mask ≠ species selected) · "open" (→
  `callbacks["ensure_pipeline_mode"]` + select the instance — the pipeline panel exposes the active
  instance via `ui_mgr.active_instance_id`; check `PipelineBuilderPanel` API at execution).
  One-click add row: "Add Template Match / Pick candidates / Subtomo / Reconstruct for this species"
  → `callbacks["add_instance_for_species"](job_type, species_id)` registered by
  `PipelineBuilderPanel` over `add_instance_to_pipeline` (SingleFlight-guarded).
- No auto-propagation of species values into existing jobs (D-5): drift is displayed only.

Verification: a species with TM+CE+subtomo shows three rows with correct status; changing species
symmetry shows a drift chip on the TM row; "Add Reconstruct" adds a bound instance.

## 5. Risks

- Per-species molstar iframes: lazy + cached; only ever one instance per species per page.
- Rev-gated tabs must not contain inputs (identity editor is outside the gate).
- `workspace_page` mode string / roster `set_active_mode` coupling: rename in one commit or not at all.
- Journey empty-state + roster "+" repointed to `create_species` — keep `origin` semantics.

## 6. Modern-Python weave-in

Frozen slotted dataclasses for rail rows; `Protocol` for the tab objects (`build(container)`,
`refresh()`), so the page treats tabs uniformly; `match` on job type for the drift rules.

## Stage log (append-only)

- 2026-08-16 — scoped. No code.
