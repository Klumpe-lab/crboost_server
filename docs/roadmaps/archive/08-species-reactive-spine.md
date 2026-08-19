# Roadmap 08 — Species reactive spine + naming ("stop the bleeding")

**Status:** S0 + S1 CODE-COMPLETE 2026-08-16 (both PENDING RUNTIME — see stage log). Scoped 2026-08-16 (code-read root cause + design review).
**Depends on:** nothing. **Unblocks:** 09 (watcher needs the rev), 10/11 (page gates on identity/rev).
**Risk:** low. Two commits (S0 spine, S1 naming); each leaves the app runnable.
**Arc:** first of the species-registry roadmaps 08 → 09 → 10 → 11 → 12 (see `README.md`). Peeves that
fall out of the user's walkthrough are triaged in `particles-ux-peeves.md`.

## 0. Trigger and facts (Stage 0 — gathered)

Trigger: a de-novo species ("copia") created via the roster PARTICLES "+" does not appear in the
Template Workbench ("No species registered yet / Add first species"). Root cause, by code reading:

1. `ui/species_workbench_panel.py` is built **once** at workspace load
   (`ui/workspace_page.py:209-214`); its empty state (`species_workbench_panel.py:183-196`) is a
   static element toggled only by the panel's own `_switch_species` (:138-140) /
   `_handle_species_deleted` (:100-102); `_refresh_tab_strip` (:49-79) reads `species_registry` live
   and unfiltered but is only invoked by panel-internal events (:144, :102).
2. Switching views is a pure CSS `display` flip (`workspace_page.py:77-115`); the journey gets an
   `on_journey_active` hook (:111-114), the workbench gets nothing.
3. The roster add (`ui/pipeline_builder/pipeline_roster.py:1428-1471`) mutates the shared registry
   correctly and then only calls `rebuild_pipeline_ui()` (`pipeline_builder_panel.py:463-506`), which
   never reaches the workbench container. There is no event bus / `on_species_added` anywhere.
4. `ProjectState` identity is NOT the cause: both sides resolve
   `get_project_state_for(ui_mgr.project_path)` (`services/project_state.py:1192-1210`, load-if-absent,
   `.resolve()`-keyed) → same object. A browser reload shows the species.

Two adjacent hazards surfaced by the same read:

- **H1 — persistence.** `add_species` (`project_state.py:777-796`) and `remove_species` (:839-867)
  call `update_modified()` but never `mark_dirty()`; the roster (`pipeline_roster.py:1455`), the
  workbench "+" (`species_workbench_panel.py:153`) and the workbench delete
  (`ui/template_workbench.py:586-589`) all `save_project(path)` WITHOUT `force=True`, and
  `StateService.save_project` writes only `if force or state.is_dirty` (`project_state.py:1293`).
  So a created species is **not written to disk** unless something else happened to be dirty (and a
  deleted one can come back) — lost/resurrected on server restart. Only the Journey empty-state path
  forces (`ui/tomo_dashboard_dialog.py:3818`).
- **H2 — no refresh gate sees the registry.** `RosterWidget.signature()`
  (`pipeline_roster.py:183-242`) folds per-job `species_id` but no species id/name/color;
  `journey_signature` (`services/dashboard_data.py:887-901`) folds label but not color and only sees
  species that already have rows; the journey's 4-s outer gate `_maybe_refresh`
  (`tomo_dashboard_dialog.py:577-637`, sig at :617) has no registry input; the workbench swatch editor
  `_pick` (`template_workbench.py:1056-1065`) repaints only its own dot. Hence peeve P-01: recoloring
  a species in the workbench does not update the roster chip / journey tabs / strip.

Also verified: `ProjectState` has no `model_config` (default extra="ignore", no validate_assignment);
`_dirty` is a `PrivateAttr` (:695); `load()` builds via `cls(...)` then assigns fields
(:1054-1073) so private attrs are initialised; `save()` uses `model_dump` (:981) which never emits
them; the in-memory object is replaced only by `set_project_state_for` at project creation
(`project_service.py:388`) and dropped on delete (`ui/data_import_panel.py:512`). A second
`PrivateAttr` is safe and cannot reset under a live view.

## 1. Design rule — rev for wake-up gates, precise tuples for DOM gates

Two cheap primitives, used for different things:

- `ProjectState.registry_rev` — an in-memory (non-persisted) counter bumped by every species /
  template / mask / pick-list / authoritative mutation. **Coarse**: it also moves on a notes
  keystroke, a template append, an authoritative click. Use it only as a **wake-up input for outer /
  poll gates** (the journey's 4-s `_maybe_refresh` sig, the workbench panel's 3-s observe).
- `ProjectState.species_identity()` = `tuple((s.id, s.name, s.color) for s in species_registry)` —
  **precise** for renders that draw only id/name/color (roster pill, workbench strip, journey species
  tabs, strip dot). Use it (and other precise tuples) as the input of **DOM gates**
  (`FingerprintedView.signature()`), so a rebuild happens only when something the render draws moved.

Never fold the bump into `mark_dirty()` — `services/jobs/_base.py:195-232` marks dirty on every
job-param `__setattr__` (every keystroke in a job form) and would turn the rev into a rebuild storm.
Never bump from render-time writes or in-pane cache updates: the keep/drop `_commit_loop`
(`tomo_dashboard_dialog.py:2600-2640`, writes `pl.filtered_count` at :2631) and the render-time
filtered_count sync (:3396-3398) must NOT bump — with the rev in the outer gate they would
`refresh_all()` after every settled keep/drop burst. `add_pick_list` calls `remove_pick_list`
internally (:885) → an upsert bumps twice; harmless (equality-compared, not counted).

## 2. Stage S0 — the spine (~65 lines, one commit)

### S0.1 `services/project_state.py` — model

```python
# next to `_dirty` (:695)
_registry_rev: int = PrivateAttr(default=0)

# after is_dirty (:709-711)
@property
def registry_rev(self) -> int:
    return self._registry_rev

def bump_registry_rev(self) -> None:
    """Non-persisted change counter for species / templates / masks / pick lists /
    authoritative choices. Wake-up input for poll gates; DOM gates use precise
    tuples (species_identity)."""
    self._registry_rev += 1

def species_identity(self) -> tuple:
    return tuple((s.id, s.name, s.color) for s in self.species_registry)

def mutate_species(self, species_id: str, fn) -> bool:
    """The one way to edit a species in place (workbench header, swatch, appends,
    selects). Marks dirty + bumps the rev; caller persists."""
    sp = self.get_species(species_id)
    if sp is None:
        return False
    fn(sp)
    self.mark_dirty()
    self.bump_registry_rev()
    return True
```

Mutators:
- `add_species` (:795, after `append`) → `self.mark_dirty(); self.bump_registry_rev()` (keep
  `update_modified()`).
- `remove_species` (:866, before `return removed`) → same.
- `add_pick_list` (:887), `remove_pick_list` (:898), `set_authoritative_slug` (:915) → add
  `self.bump_registry_rev()` right after the existing `mark_dirty()`.
- `backend.py:378` and `:621` (the two `PickList.mark_extracted` callers — both already
  `mark_dirty()` + `save_project(force=True)`) → add `state.bump_registry_rev()`.

### S0.2 `ui/template_workbench.py` — route all species edits through the model

`_mutate_species` (:257-263) body becomes
`get_project_state_for(Path(self.project_path)).mutate_species(self.species_id, fn)` (keep the
method; every header edit, swatch pick, `_append_template`/`_append_mask`, select, delete already
calls it — :363, :381, :394, :402, :477-482, :1060, :1112, :1131, :1149, :1967). No other change.

### S0.3 Signatures

- **Roster** `pipeline_roster.py:234-242`: append `current_project_state().species_identity()` to the
  returned tuple. Precise for what :400-408 renders (pill = name + color). The roster's 3-s poll runs
  in every mode and the roster stays visible in workbench mode (`set_active_mode` :815-824 hides it
  only for journey) → recolor lands ≤ 3 s.
- **`services/dashboard_data.py:897`** (`journey_signature`): add `s["color"]` to the per-species
  tuple. This is the *precise* fix for `render_strip` (`tomo_dashboard_dialog.py:423-438`), which
  draws `sp['color']` (`ui/dashboard/strip.py:132`) and nothing else registry-wide. Do NOT put the rev
  in the strip sig (heavy `build_strip` over all TS + column click targets).
- **`_main_signature`** (`tomo_dashboard_dialog.py:377-406`): add `state.species_identity()` (covers
  a fresh species with no rows → species tabs at :3906-3934) and a per-TS pick-list tuple:

  ```python
  def _pick_lists_sig(ts: str) -> tuple:
      # exclude filtered_count (in-pane keep/drop) and authoritative (in-place radio)
      return tuple(sorted(
          (pl.species_id, pl.slug, pl.count, str(pl.path), str(pl.extracted_at))
          for pl in (state.pick_lists or []) if pl.tomo_name == ts))
  ```

  Do NOT put the rev here: `_set_authoritative` (:4787-4803) updates icons in place and never calls
  `refresh()`; a rev in the main sig would tear the pane down 4 s after each radio click.
- **Outer 4-s gate** (:617): `sig = (running, finished, curation, _CURATION_SESSION_LIVE["on"],
  state.registry_rev)`; extend the `moved` diagnostics (:623-632) with `"registry"`. This is the only
  place the rev belongs in the journey. `_set_journey_active(True)` (:657-670) needs no change — the
  next tick recomputes.

### S0.4 `ui/species_workbench_panel.py` — observe the registry

Make the strip a `FingerprintedView` (RosterWidget precedent) so every strip repaint goes through
one gate (today `_switch_species` calls `_refresh_tab_strip()` directly at :144; a timer beside it
would repaint the strip a second time 0–3 s later because it never saw the sig move).

```python
class _StripView(FingerprintedView):            # replaces _refresh_tab_strip's body
    def _get_container(self): return _refs.get("strip")
    def signature(self):
        st = get_project_state_for(project_path)
        return (_active["species_id"], st.species_identity())
    def render(self): ...                        # the current :55-79 body, minus strip.clear()

def _observe():                                  # 3-s poll + on-show
    st = get_project_state_for(project_path)
    ids = [s.id for s in st.species_registry]
    if not ids:
        if _active["species_id"] is not None:    # deleted elsewhere → empty state
            _active["species_id"] = None; _refs["empty"].set_visibility(True)
    elif _active["species_id"] not in ids:       # created elsewhere / active deleted → pick first
        _switch_species(ids[0])
    elif _refs["empty"].visible:                 # was empty at build, now populated
        _switch_species(_active["species_id"] or ids[0])
    strip_view.refresh()

ui.timer(3.0, _observe)                          # in-memory tuple only → fine while hidden
def _set_active(on: bool):
    if on: _observe()                            # instant on show
if callbacks is not None:
    callbacks["on_workbench_active"] = _set_active
```

- Signature: `build_species_workbench_panel(backend, callbacks=None)`; `ui/workspace_page.py:214`
  passes `callbacks`; `_switch_to` gets, after the journey hook (:111-114):
  `on_wb = callbacks.get("on_workbench_active"); if on_wb: on_wb(_mode["current"] == "workbench")`.
- `_switch_species` (:136-144) and `_handle_species_deleted` (:83-103) call `strip_view.refresh()`
  instead of `_refresh_tab_strip()`; `_observe` handles the multi-tab delete case.
- Swatch `_pick` (`template_workbench.py:1056-1065`) needs no new wiring — the panel's `_observe`
  sees `species_identity()` move within 3 s while the view is visible. (An instant path would be an
  optional `on_species_changed` kwarg on `TemplateWorkbench.__init__` :139 next to
  `on_species_deleted`; not needed.)

### S0.5 Forced saves (optional belt)

The model-level `mark_dirty()` is the systemic fix; the two non-forced sites (`pipeline_roster.py:1455`,
`species_workbench_panel.py:153`) and the workbench delete (`template_workbench.py:589`) then write
without changes. Adding `force=True` at the two creation sites is harmless and matches the journey
path; optional. The `:152` mkdir-before-save is irrelevant to persistence.

### S0 verification (user, after `python main.py` restart)

1. Roster PARTICLES "+" → open the workbench view → the species is listed, no browser reload.
2. Workbench swatch → new color: roster job-row chip repaints ≤ 3 s; journey species tab dot +
   strip dot ≤ 4 s (journey visible).
3. Restart the server → the species is still in `project_params.json`; delete a species → still gone
   after restart.
4. Keep/drop 20 tiles quickly on a manual list → NO full-pane rebuild (log has no
   "journey live-refresh rebuild (changed: registry)" line from the burst).

## 3. Stage S1 — naming + one species pill + job-tab slimming (one commit)

Facts: the particle-phase job catalog is `services/jobs/spec.py:170-212` (single source; roster row,
tab header and add dialog render `JobSpec.display_name` via `ui/ui_state.py:152`,
`pipeline_roster.py:304,325,352`, `pipeline_builder_panel.py:145`). `JobType.TEMPLATE_EXTRACT_PYTOM =
"tmextractcand"` runs `pytom_extract_candidates.py` (`drivers/extract_candidates_pytom.py:103-121`):
peak picking on the TM score volumes → `candidates.star` coordinates; **no volume is cut** (the box
cut is `relion_tomo_subtomo`, `drivers/subtomo_extraction.py:454-471`). Internally the code says
"candidate extract" everywhere (`ui/job_plugins/candidate_extract.py:1`, `dashboard_data.py:742`);
only the display name says "Template Extract" (`spec.py:182`) — the source of the "what is template
extraction" confusion.

The species pill exists three times with identical style strings: roster `pipeline_roster.py:400-408`,
`ui/job_plugins/default_renderer.render_species_badge` (:85-113), job-tab header
`ui/pipeline_builder/job_tab_component.py:164-176`. All three particle plugins prepend the same
read-only `render_template_summary_card` (`ui/components/template_summary_card.py`, 283 L: template
row · mask row · `diameter • symmetry` row · provenance row) at `template_match.py:47`,
`candidate_extract.py:48`, `subtomo_extraction.py:54` — species facts, not job inputs. Genuine
per-job content: TM's template/mask/symmetry override dropdowns (`template_match.py:69-80,
83-128, 131-169, 182-218`); CE's threshold/max-picks/diameter/advanced (:69-169); subtomo's params
plus two sanity checks — empty-upstream banner (:51, :159-194) and box-vs-diameter (:69, :293-344) —
and a collapsed box/crop explainer (:79, :83-156) that duplicates the dashboard tooltips
(`ui/dashboard/pixel_sanity.py`).

Changes:
1. `spec.py:182` `"Template Extract"` → `"Pick candidates"` (JobType value unchanged; nothing else
   keys on the label).
2. NEW `ui/components/species_pill.py`: `render_species_pill(species, *, compact=False, tooltip=None)`
   — one style; repoint the three sites above (+ the Species rail in roadmap 10 later).
3. `ui/job_plugins/template_match.py`: drop `render_species_badge` (:39) and the summary card (:47-57);
   render the pill + a "open in Species" link line (link = `callbacks["toggle_workbench"]` today,
   Species page in 10); keep the default form and the override dropdown card.
4. `ui/job_plugins/candidate_extract.py`: drop the summary card (:47-58); pill line; keep the rest.
5. `ui/job_plugins/subtomo_extraction.py`: drop the summary card (:53-64); pill line; the two sanity
   checks stay but as one-line chips (same rule + text, less chrome); delete `_render_box_crop_help`
   (:79, :83-156) — the explainer lives in the dashboard tooltips (link to it in the chip tooltip).
6. Delete `ui/components/template_summary_card.py` once grep shows no importer.

Verification: roster/add dialog/tab header read "Pick candidates"; TM/CE/subtomo Config tabs open
with one pill line on top; TM dropdowns still default from the species; subtomo banner/warning still
fire on a zero-candidate upstream / a 1.2× box.

## 4. Risks and pitfalls

- Rebuild storms: covered by §1 (rev only in outer gates; no bump from render-time writes).
- Multi-tab: tab A typing notes bumps the rev → tab B's journey outer gate re-runs `refresh_all()` at
  most every 4 s; the precise sigs don't move → no DOM change. Acceptable.
- Two server processes on one shared project share nothing in memory (rev/mtime logic is
  process-local). Existing limitation.
- `TemplateWorkbench._render` installs a `window` message listener per instance and never removes it
  (`template_workbench.py:1016-1031`); a species deleted then re-created with the same id yields two
  listeners → double `emitEvent`. Existing hazard; keep one instance per species per page (matters
  for roadmap 10's cached containers).
- Peeve for `particles-ux-peeves.md` (not S0): notes/Ø/sym inputs mutate per keystroke and each
  keystroke triggers a full non-debounced save (`_save_state` :265-266; `backend.save_project`
  supports `debounce_s`, `backend.py:88-97`) → `debounce_s=1.0` + `.props("debounce=400")`.

## 5. Modern-Python weave-in

`species_identity()` returns a plain tuple (hashable, no allocation beyond the tuple); if a typed
row is wanted later use `@dataclass(frozen=True, slots=True)`. `origin` stays a `str` here;
roadmap 09 turns `origin`/`source_kind` into `StrEnum`s.

## 6. Runtime checklist (consolidated)

After S0 + S1: (1) create species from roster → visible in workbench without reload; (2) recolor →
roster + journey ≤ 4 s; (3) restart → persisted; (4) keep/drop burst → no pane rebuild; (5) labels
read "Pick candidates"; (6) job tabs show one pill line, dropdowns/sanity chips intact; (7) "open in
Species ↗" on a TM / pick / subtomo / reconstruct Config tab → workbench view opens ON that species;
(8) reconstruct / class3d Config tabs now show the species line at all (was dead code); a denoise-predict
Config tab shows the resolved denoiser (cryoCARE / IsoNet) instead of "set in denoise-train" when a
train job exists.

## Stage log (append-only)

- 2026-08-16 — scoped from a 3-agent code read + design review; no code. Species-registry arc
  ordering agreed with the user: 08 → 09 → 10 → 11 → 12; roadmap 07 remains the dependency for live
  extraction status (11-S2). User is switching branches before the code deep-dive.
- 2026-08-16 — **S0 CODE-COMPLETE** (branch `denovo_picking`; PENDING RUNTIME — sandbox ceiling was
  `ruff check` only, no python: `py_compile` + `check_boundaries.py` owed). Landed exactly as §2:
  `_registry_rev` PrivateAttr + `registry_rev` / `bump_registry_rev()` / `species_identity()` /
  `mutate_species()` on `ProjectState`; `add_species` + `remove_species` now `mark_dirty()` +
  bump (H1 closed); `add_pick_list` / `remove_pick_list` / `set_authoritative_slug` + the two
  `mark_extracted` sites in `backend.py` bump; `TemplateWorkbench._mutate_species` delegates to
  `mutate_species`; roster sig + `journey_signature` (color) + `_main_signature`
  (`species_identity` + per-TS pick-list tuple, no rev) + outer 4-s gate (rev, `"registry"` in the
  moved-diagnostics) per S0.3; `species_workbench_panel.py` strip is a `FingerprintedView`
  (`_StripView`), 3-s `_observe` + `on_workbench_active` hook wired through `workspace_page._switch_to`.
  One addition beyond the text: `_observe` also drops the containers of species deleted in another
  tab (`_drop_container`, shared with `_handle_species_deleted`) so a same-id re-create can't reuse a
  stale `TemplateWorkbench`; the unreachable third `_observe` branch from §S0.4 was omitted. S0.5
  forced saves NOT added (model-level `mark_dirty` is the fix). Runtime checklist = §"S0 verification".
- 2026-08-16 — **S1 CODE-COMPLETE** (same branch/session; PENDING RUNTIME, `ruff check .` clean).
  `spec.py` "Template Extract" → "Pick candidates" (P-03). NEW `ui/components/species_pill.py`:
  `render_species_pill(species, *, compact, tooltip)` (the one capsule; roster = compact 8 px, header /
  Config = 9 px), `render_species_line(species, on_open)` (Config-tab header line: SPECIES caps label +
  pill + "open in Species ↗", or an amber one-liner when no species resolves — never silent) and
  `species_opener(callbacks, sid)`. Repointed: roster row, job-tab header, `default_renderer.render_species_badge`
  (now the shared line; its `try/except Exception: return` swallow around `get_project_state_for` dropped
  per the exception policy). TM / pick-candidates / subtomo plugins render the line themselves via
  `resolve_species` and call `render_config_preamble` + `render_default_params` directly (TM used to CALL the
  badge twice — itself + inside `render_default_params_card`); the summary card is
  gone from all three and `ui/components/template_summary_card.py` is DELETED (P-04). Subtomo: both sanity
  checks are one-line chips (`_draw_banner` = icon · title · state pill, explanation in the tooltip;
  box-vs-Ø = icon · "Box vs Ø" · message, arithmetic + rules + pointer to Journey → Dataset pixel-table
  tooltips in the chip tooltip); `_render_box_crop_help` deleted.
  **Deviations, deliberate:** (a) the "open in Species" link does NOT reuse `toggle_workbench` (which
  toggles and never selects) — new `callbacks["open_species"](sid)` in `workspace_page.py` shows the
  workbench view and selects the species through `callbacks["workbench_select_species"]` (registered by
  the panel; roadmap 10 re-targets both to the Species page); `callbacks` are threaded into the Config
  renderer ctx (`job_tab_component._render_tab_content` → `config_tab.render_config_tab(callbacks=)` →
  renderer / `render_default_params_card(**_ctx)`); (b) TM + subtomo now honor the ctx `exclude`
  (`array_throttle` lives in the SLURM section) — they used to render it twice; the pick-candidates plugin
  renders it explicitly in Advanced and is logged as P-09, not touched. Verification = §3 "Verification".
  **Bug surfaced by the swallow removal (fixed in the same commit):** `render_default_params_card` passed
  `str(ui_mgr.project_path)` to `render_species_badge` / `render_denoise_inheritance`, and
  `get_project_state_for()` calls `.resolve()` on its argument → `AttributeError`, eaten by both helpers'
  `except Exception` → the default-path species badge (reconstruct / class3d) NEVER rendered and the
  denoise-predict "Denoiser" row always read "set in denoise-train". Now a `Path` is passed and both
  swallows are gone; the denoiser row shows the resolved method for the first time — check it on a
  project with a denoise-train job (runtime checklist item 7).
