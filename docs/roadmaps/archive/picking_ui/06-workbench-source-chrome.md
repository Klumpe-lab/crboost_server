# 06 — Templates & masks: one font scale in the creation forms

**Status:** CODE-COMPLETE 2026-08-18, PENDING RUNTIME — checklist in §3. **Depends on:** 05 for (d) — the masks column needs its own SOURCE
header, which only exists once the columns are split. (a)–(c), (e), (f) are independent.
**Risk:** low (chrome only; no handler, no state, no file I/O touched). One commit, or two if 05 is
not landing first.

**Peeves (maintainer, 2026-08-18):** *"Again, super heterogenous in terms of UI. Ugly and confusing
fonts (different in terms of fonts and sizes). The tabs on the 'source' — basic shape, pdb/emdb/
import/edit current — are again very stylistically non-uniform and fucking bigger than the title of
the section they are in. So are the input elements inside them. Please fix that. Furthermore 'Masks'
that follow have no notion of 'source' stated even though there is subscript that says in tiny gray
that they are derived from the currently selected template. … Let's definitely make the tools/inputs
in each individual tab more compact too though, for example in the 'import' the fucking button is at
the very rightmost edge of the page, ugly. In the case of 'edit current' they are just visually
inseparable right now from the rest of the page."*

## 0. Facts (Stage 0 — gathered) — the peeves are all one root cause plus three layout slips

- **(root) The workbench declares a three-size scale and then hands two of the three surfaces to
  Quasar.** `ui/template_workbench.py:117-133` sets `_TITLE_CLS` 14 px / `_LABEL_CLS` 10 px /
  `_BODY_CLS` 12 px / `_MONO_CLS` 10 px and the module docstring (`:15-17`) states "font scale
  collapsed to three". But:
  - `_TAB_PROPS = "dense align=left indicator-color=indigo inline-label"` (`:130`) has **no
    `no-caps`**, so Quasar's `QTab` renders its labels at its own 14 px **uppercase** with
    letter-spacing — larger and louder than the 10 px `_LABEL_CLS` "SOURCE" header directly above
    them (`:1125`). That is literally "bigger than the title of the section they are in".
  - Every control in the forms is a bare `ui.input` / `ui.number` / `ui.select` / `ui.checkbox` with
    `props("dense outlined")` (`:1146-1202`, `:1284-1302`, `:1377-1430`) — Quasar's default ~14 px
    field text and floating labels, i.e. a fourth and fifth size in a "three-size" module.
  - This is the same root cause as the Overview tab's (roadmap 03 fact (a)); fixing both makes the
    Species page one visual system.
- **(a) The project already has the replacement for Quasar tabs.** `Segmented` /
  `render_segmented` (`ui/components/segmented.py`) — flat 9 px buttons on one bordered strip,
  active tinted `#f1f5f9`, switching flips a class instead of rebuilding
  (`ui/dashboard/css.py:954-965`). `feedback_ui_chrome_conventions` says as much: *no material
  `ui.tabs()` CAPS tabs*. The workbench predates the component.
- **(b) The project already has the replacement for bare Quasar fields.** `.cb-select`
  (`ui/main_ui.py:86-115`) is a `q-field__control` rule — 24 px min-height, 1 px `#e2e8f0` box,
  11 px text, themed popup — and therefore applies to **any** QField (`ui.input`, `ui.number`,
  `ui.select`), not only selects, despite the name. Paired with a 10 px `_LABEL_CLS` label to the
  left, it is exactly the job-tab row idiom (`ui/job_plugins/_field_styles.py:33-41`).
- **(c) The import button is pushed to the page edge by a spacer, not by a layout system:**
  `_render_import_form` (`:1204-1212`) puts the explanatory label in a `flex-1` and the button after
  it; same in `_render_mask_import_form` (`:1421-1430`). Nothing else on the page has an
  edge-anchored action.
- **(d) The masks column has no SOURCE header.** `_render_masks_section` (`:1304-1324`) writes
  "MASKS" + `_mask_source_label` (`_update_mask_source_label` `:1367-1375`: *"derive new masks from
  <file>"* / *"(select a template first — masks derive from it)"*) and then drops straight into the
  purple Quasar tabs with no header of their own. The information is present, in 10 px gray, in the
  wrong place — it is a *source* statement filed as a section subtitle.
- **(e) Edit Current's three actions are separated by a hairline only.**
  `_render_action_section` (`:1261-1282`) = `ui.separator().classes("my-1 opacity-40")` + title +
  hint + inputs + button. `_field_styles.GROUP_MUTED_STYLE` (`:60-61`: 1 px `#eef2f6` box, `#fafbfc`
  fill, used by `slurm_tab.py:220-222` for "Supervisor") is the house treatment for exactly this.
- **(f) Tab labels are in three cases:** "Basic Shape" / "PDB / EMDB" / "Import" / "Edit Current" vs
  "Sphere" / "RELION Mask" / "Import" (`:1136-1140`, `:1319-1322`).

## 1. Stage S1 — one commit

**(a) Quasar tabs → `Segmented`.** In both `_render_source_panel` (`:1122-1144`) and the mask tab
block (`:1319-1324`): `render_segmented([(key, label), …], active, on_switch)` plus one container per
panel with `set_visibility` flipped on switch — the Species page's own tab pattern
(`ui/species/page.py:195-201`), minus the lazy build (these panels are cheap). Delete `_TAB_PROPS` /
`_TAB_PROPS_PURPLE` (`:129-131`) and both `ui.tabs()` / `ui.tab_panels()` blocks. The active-segment
tint is neutral for both columns; the indigo/purple accent stays where it already carries meaning
(the row selection of roadmap 05).

**(b) One control treatment.** A module-local helper —

```python
def _field(label: str, control_builder, *, width: str = "w-24", hint: str | None = None): ...
```

— that renders a 10 px `_LABEL_CLS` label followed by the control with `.classes("cb-field " + width)`
and `.props("dense")` (drop `outlined`; the border comes from CSS). Widen the CSS rule in
`ui/main_ui.py:86-103` from `.cb-select` to `.cb-select, .cb-field` (one selector list change, no new
rules) and note in the comment that non-select fields use the `cb-field` spelling. Apply to every
control in `_render_basic_shape_form` (`:1146-1166`), `_render_pdb_emdb_form` (`:1168-1202`),
`_render_resample_inputs` (`:1284-1293`), `_render_lowpass_inputs` (`:1295-1302`),
`_render_spherical_mask_form` (`:1377-1398`) and `_render_relion_mask_form` (`:1400-1419`). The
`auto box` checkbox gets `props("dense size=xs")` + the 10 px sans style `toggle_field` uses
(`_field_styles.py:207-218`). In-field floating labels go away — the label is now to the left, which
is what makes the rows scannable at 10 px.

**(c) Actions sit with their inputs.** Drop the `flex-1` spacers in `_render_import_form` and
`_render_mask_import_form`; the row becomes `[button] [hint]` (or button first, hint wrapping
beneath at `_HINT_CLS`). Every "generate / fetch / create" button gets the same size —
`props("unelevated dense no-caps size=sm")` — so the six of them stop competing.

**(d) Masks get a stated source.** With the columns split (roadmap 05), the masks column reads
`MASKS` + row list, then `SOURCE` + the hint that `_update_mask_source_label` already produces:
*"derived from `ribo_lp30_white.mrc`"* / *"select a template first — masks derive from it"*. Same
two-line header shape as the templates column ("SOURCE · generate, fetch, import; new entries append
above", `:1125-1127`). The label element and its updater stay; only where it is rendered changes.

**(e) Edit Current's actions become groups.** `_render_action_section` (`:1261-1282`) wraps its body
in `field_group(muted=True)` (import from `ui/job_plugins/_field_styles.py` — a UI→UI import, which
`check_boundaries.py` permits) and drops the `ui.separator()`. Three visibly discrete tools:
Resample · Apply lowpass · Flip polarity.

**(f) Sentence case everywhere**, matching the Species page's own tab labels ("Templates & masks",
not "Templates & Masks"): `Shape` · `PDB / EMDB` · `Import` · `Edit current`; `Sphere` ·
`From template` (today "RELION Mask" — the label should say what it does, and the tool used is
already named in the panel's hint at `:1416-1418`) · `Import`.

## 2. Non-goals

- No change to what any form *does* — no new parameters, no changed defaults, no changed canonical
  output paths, no touching the polarity-pair / dedup logic.
- The activity log, the viewer panel and the molstar bridge are untouched.
- Not a rewrite of `ui/template_workbench.py`. Roadmap 10's decision of record stands: this file is
  mounted and adjusted, not rebuilt.

## 3. Runtime checklist (maintainer)

1. Source strip: 9 px sentence-case segments on one strip, smaller than the "SOURCE" header above
   them; switching panels does not flash the list above; the previously active panel's typed values
   survive the switch.
2. Every input in every panel renders at 11 px in a 1 px box with a 10 px label to its left; no
   floating labels, no 14 px text anywhere in the two columns.
3. Import panel: the button sits next to its explanation, not at the window edge. Same in the mask
   import panel.
4. Edit Current: Resample / Apply lowpass / Flip polarity are three boxed groups; each still acts on
   the selected template and still skips the write when the canonical output already exists.
5. Masks column shows `SOURCE` with the derived-from line, and it updates when the selected template
   changes.
6. Generate a shape, fetch a PDB, fetch an EMDB, import a template, create a sphere mask, create a
   threshold mask, import a mask — all seven still work and still log.
7. `ruff check .`.

## 4. Log

- 2026-08-18 — scoped from the walkthrough. No code. Root cause recorded: the module documents a
  three-size scale but hands the tabs and every field to Quasar's defaults; both replacements
  (`Segmented`, `.cb-select`) already exist in the codebase.
- 2026-08-18 — **implemented.** `render_segmented` replaces both Quasar tab blocks (`_TAB_PROPS` gone),
  `_field` + the widened `.cb-select, .cb-field` rule in `ui/main_ui.py` cover every control in the six
  forms, `_render_action_section` wraps in `field_group(muted=True)`, the masks column has its own
  `SOURCE` header, and the labels are sentence case ("Edit current", "From template").
