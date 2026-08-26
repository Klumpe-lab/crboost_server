# 01 — Species entry point and creation

**Status:** CODE-COMPLETE 2026-08-18 (all stages), PENDING RUNTIME — checklist in §4. **Depends on:** nothing. **Unblocks:** S2 needs S1.
**Risk:** low (S0 is one file + one asset; S1 is a mechanical move; S2 adds fields to one dialog).
Three commits: S0 nav icon · S1 template/mask registration → service · S2 the creation form.

**Peeve (maintainer, 2026-08-18):** *"when i 'create new species' via the button on Particles or the
species registry I get exactly one fucking input field and that's it… it would be nice to have the
interface to for example choose a template and mask and bind it to the given particle upfront at its
creation. So when the user goes to queue jobs it's already usable and she doesn't need to come back to
the species registry and select all this stuff again."* Plus: *"let's change the species registry icon
from the flask to something more biologically plausible."*

## 0. Facts (Stage 0 — gathered)

- **One create path, three entry points.** `ui/species/prompt.py:46-57` `create_species(backend,
  project_path, *, origin)` → `prompt_species_name()` (`:19-43`, a single `ui.input` in a `w-80` card)
  → `ProjectState.add_species` → forced save. Callers: the Species page rail "+"
  (`ui/species/page.py:254`, `origin=workbench`), the roster PARTICLES-header "+"
  (`ui/pipeline_builder/pipeline_roster.py:1441`, `origin=manual`, the button built at `:1420-1458`),
  and the Journey empty state (`ui/tomo_dashboard_dialog.py:3522`, `origin=manual`). Widening the
  dialog upgrades all three at once — no call-site churn.
- **What the model already carries** (`services/project_state.py:203-260`): `name`, `color`,
  `origin`, `created_at`, `diameter_ang`, `symmetry` (default `"C1"`), `notes`, `templates[]`,
  `masks[]`, `selected_template_id`, `selected_mask_id`, `extraction_params` (None = undecided,
  `:189-200`), `catalog_id` / `catalog_version`. `add_species` (`:888-914`) sets id (slugified,
  collision-suffixed), name, palette color, origin, `created_at` — and nothing else.
- **Registration of a template / mask is UI-private today.** `TemplateWorkbench._append_template`
  (`ui/template_workbench.py:319-359`) and `._append_mask` (`:361-381`) are pure state mutations
  (`sidecar_ensure` → build the model → replace-in-place by path or append → auto-select when it is
  the first) wrapped in `_mutate_species` + a fire-and-forget save; `._select_template` / `._select_mask`
  (`:383-397`) likewise. Nothing outside a mounted 2111-line workbench can register a template.
- **The import path exists and is standalone.** `open_template_import_dialog(project_path, species,
  initial_path=None) -> ParticleTemplate | None` (`ui/template_import_dialog.py:31-39`) already reads
  the MRC header, asks the user to confirm polarity / apix / lowpass, copies the file into the project
  and hands back a populated model; the workbench just appends the result (`:1714-1737`). Mask import
  is a bare file picker + `TemplateMask(method="imported")` (`:2101-2111`).
- **The nav icon** is `static/icons/vial.svg` — a laboratory flask — rendered by
  `pipeline_roster.py:876` via `_sb_svg_btn` (`:1610-1631`: loads the file, string-replaces
  `currentColor` with the active/muted color, draws it at 18 px inside a 30 px hit box). Icons are
  15×15 viewBox and must use the literal token `currentColor`. The Species page's *empty state* uses
  the Material `biotech` glyph (`ui/species/page.py:118`) — also a lab instrument, same peeve.
- **Colour picking already exists**, but is welded to a saved species:
  `overview_tab._render_color_swatch(backend, project_path, species_id, current)`
  (`ui/species/overview_tab.py:60-89`) mutates + saves on pick. A pre-registration dialog needs the
  same swatch with a plain callback.

## 1. Stage S0 — the nav icon (one commit, cosmetic)

Replace the flask with a **particle**: a hexagonal capsid outline with three subunit dots. It reads
at 18 px (a literal ribosome silhouette does not) and it is what the page is actually about — the
species registry lists macromolecular complexes, not glassware.

- NEW `static/icons/particle.svg`, matching the house style (15×15 viewBox, `currentColor`,
  1.1 stroke):

  ```svg
  <svg width="15" height="15" viewBox="0 0 15 15" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M7.5 1.4 L12.78 4.45 V10.55 L7.5 13.6 L2.22 10.55 V4.45 Z"
          stroke="currentColor" stroke-width="1.1" stroke-linejoin="round"/>
    <circle cx="7.5" cy="5.0" r="1.15" fill="currentColor"/>
    <circle cx="5.34" cy="8.75" r="1.15" fill="currentColor"/>
    <circle cx="9.66" cy="8.75" r="1.15" fill="currentColor"/>
  </svg>
  ```

- `pipeline_roster.py:876` → `self._sb_svg_btn("particle.svg", "Species", ...)`.
- `ui/species/page.py:118` empty state → the same glyph rather than `biotech` (`ui.html` of the file
  at 48 px, or the nearest honest Material name — `blur_on` / `scatter_plot` read as particles;
  `hexagon` is the shape without the subunits). Pick one and use it in both places.
- Keep `vial.svg` on disk only if something else still loads it (`grep -rn "vial.svg"` — today only
  `:876`); otherwise delete it in the same commit.

**ASK (aesthetic, one line):** if the maintainer prefers a node-and-bond *molecule* glyph over the
capsid, say so before this lands — everything else in the stage is identical.

## 2. Stage S1 — template / mask registration becomes a service

Enabling move for S2, and the end of "you must mount a workbench to register a template". Pure
mechanical extraction; no behavior change.

- In `services/species_admin.py` (already the home of the delete cascade and
  `delete_file_with_sidecar`), add:
  - `register_template(state, species_id, template_path, *, polarity, source, lowpass=None,
    imported_from=None, notes="") -> dict` — the body of `_append_template`
    (`ui/template_workbench.py:319-359`) verbatim: `sidecar_ensure`, build `ParticleTemplate`,
    replace-in-place when a registered entry already has that path (preserving its id so dropdowns
    stay stable), else append, auto-select only when it is the first. Returns
    `ok(template_id=...)` / `err(...)` when the species is unknown.
  - `register_mask(state, species_id, mask: TemplateMask) -> dict` — the body of `_append_mask`.
  - `select_template(state, species_id, template_id) -> dict` / `select_mask(...)` — the bodies of
    `_select_template` / `_select_mask`.
- **These do not save.** They mutate through `state.mutate_species` (dirty + rev) and return; the
  caller owns persistence, exactly as today (the workbench's `_after_register` saves + refreshes;
  the creation dialog force-saves once at the end).
- `TemplateWorkbench._append_template` / `_append_mask` / `_select_template` / `_select_mask` become
  three-line wrappers that call the service and then `asyncio.create_task(self._after_register())`.
  Every existing call site inside the workbench keeps working untouched.
- `python check_boundaries.py` must stay clean (services must not import ui; these do not).

## 3. Stage S2 — the creation form

`prompt_species_name` becomes `prompt_species_draft() -> SpeciesDraft | None`, and `create_species`
applies the draft. Same signature, same three callers, richer dialog.

- NEW `ui/components/color_swatch.py: render_color_swatch(current: str, on_pick: Callable[[str], None])`
  — `overview_tab._render_color_swatch` (`ui/species/overview_tab.py:60-89`) with the
  `mutate_species` + save closure lifted out into `on_pick`. Repoint the Overview tab to it in the
  same commit (it passes a closure that mutates + `save_project(debounce_s=1.0)`).
- `ui/species/prompt.py`:
  - `@dataclass(frozen=True, slots=True) SpeciesDraft` — `name`, `color`, `diameter_ang: float | None`,
    `symmetry: str`, `notes: str`, `template_path: str`, `mask_path: str`.
  - `prompt_species_draft()` — one `ui.dialog` / `ui.card` at `w-[30rem]`, two blocks separated by a
    `section_rule()`-equivalent, all inputs `dense outlined`, labels in the Species-page scale
    (`text-[10px] font-bold uppercase` for block headers, `text-xs` for field labels — the Overview
    tab's `_LABEL_CLS` / `_BODY_CLS`, so the dialog looks like the page it creates into):
    - **Identity** — colour swatch · name (autofocus, Enter submits, required) · Ø (Å) · Symmetry ·
      Notes (`ui.textarea`, 2 rows, free-form).
    - **Templates & masks — optional** — two rows, each a read-only path field + a "choose…" button
      (`ui/local_file_picker.py`, `.mrc/.map/.rec/.ccp4`) + a clear "×". Hint under the block:
      *"Leave empty and add them later on the Species page. The template's header is inspected and
      confirmed after you create."*
  - `create_species(backend, project_path, *, origin)` — unchanged signature. Runs the draft prompt,
    `add_species(name, origin=origin, color=draft.color)`, then applies Ø / symmetry / notes via one
    `mutate_species`; then, when set:
    - template → `open_template_import_dialog(project_path, species, initial_path=draft.template_path)`
      (the inspector confirms polarity / apix / lowpass and copies the file in) →
      `species_admin.register_template(...)` with the returned model's fields;
    - mask → `species_admin.register_mask(state, sid, TemplateMask(mask_path=..., method="imported"))`.
    Each registration auto-selects when it is the first, so `selected_template_id` /
    `selected_mask_id` are set and the TM job tab is immediately usable.
  - One forced `backend.save_project(project_path, force=True)` at the end (as today), after the
    optional imports — not one per step.
  - Failure of an optional import is reported (`ui.notify` + the species still exists), never silent:
    a species with no template is a legitimate state (de-novo picking), a species that silently lost
    the template the user picked is not.

**Symmetry in this dialog** uses the display convention roadmap 03 introduces: the option list is
`SymmetryGroup` with `C1` labelled **"None (C1)"** and preselected. Land whichever stage comes first
and make the other match; do not ship two spellings.

**Deliberately NOT in the creation dialog:** extraction geometry. It is not a property of the
particle (the maintainer's own conclusion, 2026-08-18 — see
[07-scoping-particle-registry-edges.md](07-scoping-particle-registry-edges.md)); it is asked once per
species per project at first extract (D-3) and gets its own panel in
[04-overview-declutter.md](04-overview-declutter.md). Also not here: import-from-catalog — the rail
already has that entry point (roadmap 12).

## 4. Runtime checklist (maintainer)

1. Sidebar: the Species button is a particle, not a flask; active/muted colouring still tracks the
   selected view; tooltip still reads "Species". The Species page's empty state matches.
2. Create a species from **each** of the three "+" buttons (rail, PARTICLES header, Journey empty
   state). Each opens the wide dialog; Enter on the name still submits; Cancel still returns None and
   creates nothing.
3. Fill everything: colour, name, Ø, symmetry, notes, a template `.mrc`, a mask `.mrc`. On Create the
   import inspector opens for the template, and after confirm the Species page shows the species with
   one template + one mask, both selected, and the identity fields populated. Reload the project —
   all of it persists.
4. Create one with only a name (the de-novo path): species exists, no template, no mask, no
   `extraction_params`, nothing invented.
5. Open a TM job tab bound to the new species: the template and mask dropdowns default to the ones
   bound at creation with no visit to the Templates & masks tab.
6. The Templates & masks tab still registers templates the old way (shape / PDB / EMDB / import /
   resample / lowpass / flip) — S1 touched every one of those paths.
7. `venv/bin/python3 -m compileall -q services ui` + `python check_boundaries.py` + `ruff check .`.

## 5. Log

- 2026-08-18 — scoped from the walkthrough. No code.
- 2026-08-18 — **all three stages implemented the same day.** S0: `static/icons/particle.svg` +
  `ui/components/svg_icon.py` (`load_icon_svg` hoisted out of `pipeline_roster._load_svg` so the
  Species page's empty state can use the same glyph); `vial.svg` deleted, no callers left. The capsid
  ASK was answered by shipping the capsid — swap it if you prefer a molecule. S1: `register_template`
  / `register_mask` / `select_template` / `select_mask` in `services/species_admin.py`, non-saving as
  specified; the workbench's four methods are wrappers. S2: `SpeciesDraft` + `prompt_species_draft`
  (identity block + optional template/mask block), `ui/components/color_swatch.py` with the Overview
  repointed to it, one forced save after the optional imports.
