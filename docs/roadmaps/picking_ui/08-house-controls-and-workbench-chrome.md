# 08 — One control vocabulary: inputs, buttons, borders, and the rename

**Status:** CODE-COMPLETE 2026-08-21 (S1–S6 all landed, uncommitted). PENDING RUNTIME — checklist §4.
S0's recheck folds into that runtime pass (restart + hard-reload first).
**Risk:** low–medium (chrome + CSS delivery; S2/S3 are wide but mechanical). Every stage is its own commit.
**Depends on:** nothing. Independent of 09–12.

**Peeves (maintainer, 2026-08-21):** *"too much gray border lines everywhere… why are [the input boxes]
so fucking huge, ugly and each one has these lame ass useless up and down arrows? This is a complaint
I have ACROSS our codebase… Does nicegui confine us to some lame ass set of components or can we pull
in a regular simple input box…?"* · *"buttons like 'Create mask', 'generate', 'fetch & resample'…
just some blue background and white lowercase text… all over the place in terms of length, height and
positioning… standardize on something that fits the projects overview or the job roster — austere,
standardized, dim and formal. Prefer operational fonts."* · *"Species should be renamed to 'Particles
registry'."* · *"when I switch between shape/pdb+emdb/import/edit current everything downstairs of it
(the molstar viewer) jumps around like crazy."* · *"in 'masks' the 'from template' should be the
default first option (not 'sphere')."*

## 0. Stage S0 — restart first: three of these peeves are already fixed in the tree

The complaint describes the build the maintainer last ran, not the working tree. Before any code:
restart `python main.py`, hard-reload the browser, and re-check. Expected to already be fixed:

1. **Mask default.** `_MASK_TABS` is `(("relion", "From template"), ("sphere", "Sphere"),
   ("import", "Import"))` with `"relion"` default-selected — `ui/template_workbench.py:151-154`,
   `:1426`, `:1441`. "From template" is first AND default.
2. **The blue buttons.** Every text button in the workbench already goes through `house_button`
   (`ui/components/buttons.py:40-50`): white ground, 1 px slate outline, 10 px IBM Plex Sans, 22 px
   tall, `min-width: 84px` — exactly the "austere, dim, formal" roster look. The quoted labels are
   also stale: it is now "Import…" / "Fetch & simulate" / "Pick mask file…", and "Edit current" is a
   segment, not a button. The blue-pill look was `color=primary`; `house_button` passes `color=None`,
   which is load-bearing (`buttons.py:45`).
3. **Input size + spinners.** The 16 px `.cb-field` chrome and the spinner suppression
   (`input[type=number]::-webkit-…-spin-button` + `-moz-appearance: textfield`) are both delivered
   via the trustworthy socket channel — `ui/dashboard/css.py:1041-1095` — since the 2026-08-21
   stale-shell incident (recorded in `buttons.py:10-15`). All 19 workbench `ui.number`s are wrapped
   by `_field()` (`template_workbench.py:214-226`) and wear `.cb-field`.

**If size/arrows persist after restart**, diagnose in devtools before writing code: (a) does a
workbench input's `.q-field__control` compute to `height: 16px`? (b) is the
`input[type=number]::-webkit-inner-spin-button` rule present in a `<style>` injected after connect?
(c) is the element receiving `display: flex` from `/static/main.css` (see S1 fact 3)?

## 1. Facts (gathered 2026-08-21)

1. **NiceGUI does not confine us.** Every element is a Quasar component that takes `.props()` /
   `.classes()` / `.style()`; `.cb-field` already tames QInput/QNumber/QSelect to a 16 px box with
   10 px Plex text (`css.py:1087-1095`). Raw `<input>` via `ui.element("input")` is possible but
   loses v-model plumbing and buys nothing `.cb-field` doesn't already deliver. The problem was never
   the component set — it is (a) delivery channels and (b) surfaces that never adopted the vocabulary.
2. **Three CSS channels exist, and two of them lie.** Socket per-client
   (`css.py ensure_assets_loaded`, `:1110-1118`) — trustworthy. The `main_ui.py` head-html shell —
   served stale on this deployment; it still carries a full duplicate of the `.cb-select`/`.cb-field`
   block (`ui/main_ui.py:94-138`). And `/static/main.css` (mtime cache-busted, DOES arrive fresh,
   `main.py:99-106`) defines a **conflicting legacy `.cb-field`** as a flex *container* —
   `static/main.css:204-209` (`display: flex; align-items: baseline; gap: 8px`), plus
   `.cb-field-grid/-label/-path/-input` at `:194-260`. Every modern `.cb-field` input also picks up
   `display: flex` from it.
3. **`ensure_assets_loaded` covers `/workspace` only.** Call sites: `ui/species/page.py:92` (eager at
   workspace build, covers the whole client), `ui/tomo_dashboard_dialog.py:316`,
   `ui/tomogram_import_dialog.py:55`. The landing page (`main_ui.py:183-224`) and `/cluster-info`
   (`:248-252`) never call it — no spinner kill, no `.cb-btn` hover, no `.cb-field` there. The config
   editor opens from the landing page.
4. **Raw-control census.** Label-first `ui.button(`: **108 sites / 22 files** (top:
   `particles/list_actions.py` 12, `tomo_dashboard_dialog.py` 11, `species/picks_tab.py` 11,
   `pipeline_builder/job_tab_component.py` 9, `curation_session_dialog.py` 8, `data_import_panel.py`
   7). `ui.number(` without `.cb-field`: `particles/list_actions.py` ×4, `tomogram_import_dialog.py`
   ×2, `tilt_filter_panel.py` ×1, `species/prompt.py` ×1. `ui/tilt_filter_panel.py:73,84` still
   defines its own `_btn_primary` / `_btn_flat`. One live `color="primary"` remains
   (`ui/species/prompt.py`).
5. **The viewer jump has two mechanisms, neither is a missing `_stacked_panels`.** Both source
   switchers ARE grid-stacked (`template_workbench.py:1235`, `:1429`). (a) The stack is always as
   tall as its tallest panel, and the **Edit current** panel swings ~200 px: one line when nothing is
   selected (`:1319-1325`) vs chip row + three tool groups when a template is selected (`:1327-1361`,
   rebuilt by `_refresh_edit_current` `:513-518` on every row click / register / delete). The whole
   left column — and the viewer below the two-column grid (`:1009-1020`) — moves with it. (b) The
   **viewer-mode** switcher (molstar ↔ slice, `:1576-1584`) uses bare `set_visibility` — the card
   collapses from the fixed 380 px molstar row to the slice panel's variable plotly height and the
   Activity log jumps.
6. **Border census (workbench).** 17 chrome sources; the load-bearing ones are `.cb-field` boxes
   (~20 — the input affordance, keep), the two `.cb-seg` strips, and the table head hairline. The
   noise: per-session-item nested `ui.card`s inside the viewer card (`:872`, `:964`), the tray
   `border-r` (`:1591`), the Quasar `ui.expansion("Activity log")` chrome (`:1641`), the
   `_TOOL_GROUP_STYLE` tinted boxes (`:167`, applied `:1375`), per-row `border-bottom` hairlines
   (`css.py:981-987`).
7. **The rename is half-done.** Sidebar tooltip is already "Particles registry"
   (`ui/pipeline_builder/pipeline_roster.py:878`); so are six other strings. Remaining *page-naming*
   copy: `ui/tomo_dashboard_dialog.py:3608` ("manage in Species ↗") + tooltips/notifies at
   `tomo_dashboard_dialog.py:3610-3614`, `:4128-4131`, `ui/particles/list_actions.py:666-671`,
   `ui/curation_session_dialog.py:284-287`, `ui/species/prompt.py:172-175`. Entity copy ("New
   species", "Delete species…") names the *thing*, not the page, and stays.

## 2. Stages

**S1 — one field vocabulary, one delivery channel.**
- New `ui/components/fields.py`: promote the workbench's `_field()` into `house_field(label,
  build, *, width, hint)` plus thin `house_number` / `house_text` / `house_select` builders.
  `house_number` takes the `numeric_forward` semantics from `_field_styles.py:187-211` when given a
  model+attr (the int-corruption trap — a float written into an int field drops the job instance on
  next load — must not be reintroducible by construction). `house_select` always sets
  `popup-content-class="cb-select-popup"`.
- Purge the legacy `.cb-field*` family from `static/main.css:194-260` (grep first for anything still
  wearing the container spelling; rename those call sites, then delete the rules).
- Delete the duplicate `.cb-select`/`.cb-field` block from the `main_ui.py` shell (`:94-138`) —
  `ui/dashboard/css.py` is the single source; the shell copy only exists to drift.
- Add unprefixed `appearance: none` / `appearance: textfield` beside the prefixed spinner rules
  (`css.py:1041-1043`).
- Call `ensure_assets_loaded()` on `/` and `/cluster-info` page builds.
- Record the vocabulary in CLAUDE.md (one paragraph replacing the current scatter).

**S2 — convert the input stragglers.** The 8 raw `ui.number` sites (fact 4) plus the bare
`ui.input`/`ui.select` in the same dialogs (`list_actions`, `tomogram_import_dialog`,
`curation_session_dialog`, `config_settings_dialog`, `tilt_filter_panel`, `species/prompt`) move to
`house_*`. Job tabs stay on `_field_styles` (`.cb-select` scale) — the two scales differ one notch by
design (`css.py:1051-1057`); do not collapse them.

**S3 — one button vocabulary, app-wide.** Convert the 108 label-button sites to `house_button`,
batched one commit per surface cluster: (a) particles/species (`list_actions`, `picks_tab`,
`curation_tab`, `catalog`, `merge_card`), (b) pipeline_builder (`job_tab_component`, `files_tab`,
`logs_tab`, `pipeline_builder_panel`, `slurm_tab`), (c) dialogs (`curation_session_dialog`,
`tomogram_import_dialog`, `config_settings_dialog`, `local_file_picker`, `tilt_filter_panel` — delete
`_btn_primary`/`_btn_flat`), (d) landing + misc (`data_import_panel`, `projects_overview`,
`main_ui`, `workspace_page`, `tomo_dashboard_dialog`). Placement rule (from picking_ui/06(c)):
actions sit WITH their inputs — `[button] [hint]`, never a `flex-1` spacer to the page edge; dialog
footers are a right-aligned `[Cancel] [accent action]` pair. Icon-only toolbar buttons are NOT in
scope — the 30×30 `_sb_svg_btn` pattern (`pipeline_roster.py:1609-1630`) is its own vocabulary.

**S4 — workbench border diet.** Keep: `.cb-field` boxes, the two `.cb-seg` strips, table head
hairline, the outer viewer-card border. Kill or soften: session-item nested cards → flat 22 px rows
(swatch · name · eye · close) with hover background only; pending-load card → inline row; tool-group
tinted boxes → whitespace + TOOL-rank header (the P-30 three-rank rule); `ui.expansion("Activity
log")` → house section header with a plain disclosure caret; tray `border-r` → 12 px gap. Row
hairlines drop from `#f1f5f9` to hover-only if still noisy after the above.

**S5 — kill the vertical jump.** (a) `_render_edit_current_form` renders the chip row + all three
tool groups ALWAYS; with no selection they are disabled with one 9 px hint ("select a template
first") — constant height, and "absent is stated" instead of a collapsed panel. Drop the
clear+rebuild in `_refresh_edit_current` in favor of enabling/updating in place where cheap. (b) The
viewer-mode switcher adopts `_stacked_panels` (or the card gets `min-height` = the 380 px molstar
row) so molstar ↔ slice doesn't move the Activity log.

**S6 — finish the rename.** The six page-naming sites in fact 7 → "Particles registry". Entity copy
stays "species". Grep `"Species page"` / `"in Species"` across `ui/` afterward to catch strays.

## 3. Non-goals

- No behavior change anywhere — S1–S6 change chrome and copy only.
- No new components beyond `fields.py`; no TypeScript/NextJS detour (noted by the maintainer as a
  someday, not this arc).
- Job-tab field scale (`.cb-select`, `LABEL_W=110`) untouched.

## 4. Runtime checklist (maintainer)

1. After restart: mask source reads From template · Sphere · Import with From template active;
   workbench buttons are white/slate 22 px; inputs are 16 px with NO spin arrows (Chrome + Firefox).
2. Flip Shape → PDB / EMDB → Import → Edit current with and without a template selected: the molstar
   viewer's top edge never moves. Toggle molstar ↔ slice: the Activity log never moves.
3. Landing page: config editor fields/buttons render in house chrome; no native spinners anywhere.
4. One pass through each converted dialog (import tomograms, config editor, tilt filter, species
   creation): buttons uniform height/width discipline, inputs 16 px, no blue.
5. Journey link reads "manage in Particles registry ↗".
6. `ruff check .` + `python check_boundaries.py`.

## 5. Log

- 2026-08-21 — scoped. Census: 108 raw label-buttons / 22 files; 8 raw `ui.number` sites; three CSS
  channels with a conflicting legacy `.cb-field` in `static/main.css`; jump root-caused to the
  Edit-current height swing + the unstacked viewer-mode switcher (NOT a missing `_stacked_panels` on
  the source tabs). Mask default, house buttons, and the 16 px field chrome found already landed in
  the tree — S0 is a restart-and-recheck gate so we don't re-fix fixed things.
- 2026-08-21 — S1–S6 built back-to-back (verify once at the end, per standing practice):
  - **S1**: `ui/components/fields.py` (`house_field/house_number/house_text/house_select`;
    `house_number` with `model=`/`attr=` binds through `numeric_forward` so the int-corruption trap
    is unreachable by construction; a `w-full` width stretches the row, box takes the remainder).
    Workbench `_field` now imports it. Legacy `.cb-field-grid/-label/-path/-input/-suffix` + the
    conflicting flex-container `.cb-field` DELETED from `static/main.css` (zero Python callers).
    Shell duplicate `.cb-select`/`.cb-field` block DELETED from `main_ui.py` — `ui/dashboard/css.py`
    is the single source; unprefixed `appearance` rules added beside the prefixed spinner kill;
    `ensure_assets_loaded()` now called on `/` and `/cluster-info` (both after `client.connected()`).
    CLAUDE.md scatter replaced by one "One control vocabulary" bullet.
  - **S2**: all 8 raw `ui.number` sites + sibling bare inputs/selects converted — `list_actions`
    (geometry ×3, dedup radius, tomo select, coords path), `tomogram_import_dialog` (apix, binning,
    reference-star input → `.cb-field` in place), `tilt_filter_panel` (model/threshold/action),
    `species/prompt` (name, Ø, symmetry, notes, path rows, the last `color=primary`),
    `config_settings_dialog` (path/grid/mini inputs → `.cb-field` in place, keeping its aligned
    label columns). Job tabs untouched (`.cb-select` scale, by design).
  - **S3**: ~75 label-buttons converted to `house_button` across all four clusters (4 parallel
    passes): (a) particles/species 28, (b) pipeline_builder 15, (c) dialogs 30 incl.
    `_btn_primary`/`_btn_flat` deleted from tilt_filter_panel and `_ACT` from
    tomogram_import_dialog, (d) landing+misc 18 incl. the Create-project enable/disable repaint
    that used to reapply blue chrome. Deliberately NOT converted: icon-only buttons, `_sb_svg_btn`,
    the logs stdout/stderr and job-tab segmented switchers, the 512/1K/2K/Full zoom selector, the
    username-suggestion chips, one container-style labeled-children button in data_import_panel.
  - **S4**: session items → flat 22 px hover rows; pending loads → inline rows; tool-group tinted
    boxes → whitespace + TOOL-rank header (`_TOOL_GROUP_STYLE` deleted); Activity log →
    house section header + plain disclosure caret (starts closed); tray `border-r` → `gap-3`.
    Row hairlines left as-is pending a look at runtime.
  - **S5**: (a) Edit-current renders chip row + all three tools ALWAYS; no selection = disabled
    (`opacity-50` + `pointer-events: none`) behind one hint — constant height. The clear+rebuild in
    `_refresh_edit_current` stays (harmless now; height no longer swings). (b) viewer panels
    grid-stacked; `_apply_viewer_mode_visibility` flips `visibility`, not `set_visibility`
    (display:none would stop sizing the grid cell and the card would still collapse).
  - **S6**: "manage in Particles registry ↗" (Journey), authoritative-list tooltip,
    curation "no session" notify, session-dialog empty-state, species-prompt hint. Entity copy
    ("New species", "Delete species…") kept. Grep confirms no user-facing "Species page" strings
    remain.
  - Gates run: `ruff check .` GREEN. `python check_boundaries.py` + `import main` owed to the
    maintainer (no runnable python in the sandbox). Repo-wide `ruff format --check` drift is
    pre-existing at HEAD in files we touched; not churned.
