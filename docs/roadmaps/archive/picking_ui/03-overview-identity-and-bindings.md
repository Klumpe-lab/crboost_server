# 03 — Species Overview: identity block and template / mask bindings

**Status:** CODE-COMPLETE 2026-08-18, PENDING RUNTIME — checklist in §3. **Depends on:** nothing (04 touches the same file — land 03 first or
rebase). **Unblocks:** nothing. **Risk:** low (one tab file + one new small component). One commit.

**Peeves (maintainer, 2026-08-18):** *"ui wise it's super duper heterogenous and for no good reason.
The fields like name, diameter, symmetry and notes are huge and inside this long ass panel with a blue
sidebar. why? The radius field cuts its value off because it's so short. Then 'sym' should be
'symmetry' and actually enable-able to see the dropdown but default to 'none'. Most particles are not
symmetric (ex. ribosome/proteasome etc.) — we've just happened to work with viruses a lot. Notes
should be free-form. Current template and masks associated with this particle should be listed here
next (with links to the full paths and copy clipboard icons next to them) — have we made any
decisions that would prevent that? hopefully not — whether or not a template/mask is associated with
the species those fields should be there and link to the templates & masks page of the interface."*

**Answer to the question asked: no, nothing blocks it.** All three ingredients already exist —
see Facts (c), (d), (e).

## 0. Facts (Stage 0 — gathered)

- **(a) The blue sidebar and the oversized fields.** `render_identity_editor`
  (`ui/species/overview_tab.py:92-189`) wraps everything in `ui.card().tight()` with
  `border-left: 4px solid #6366f1` (`:108`). The inputs are bare Quasar (`ui.input` / `ui.number` /
  `ui.select` + `props("dense outlined")`) so they render at Quasar's default ~14 px while every label
  around them is 10–12 px (`_LABEL_CLS` / `_BODY_CLS` / `_HINT_CLS`, `:43-47`). That mismatch is the
  whole "heterogenous / huge" complaint, and it is the same root cause as the workbench's
  (roadmap 06).
- **(b) The clipped Ø.** `ui.number(... suffix="Å").classes("w-24")` (`:130-134`) — 96 px has to hold
  the value, the "Å" suffix and Quasar's spinner affordance. Also: the model field is
  **`diameter_ang`** and the label is the bare glyph "Ø" (`:129`), which is exactly why it got read as
  "the radius field". Symmetry is `.classes("w-20")` (`:151-155`) — 80 px, narrower than its own
  longest option, and visually inert. Notes is a single-line `ui.input` (`:168-172`), not free-form.
- **(c) Templates and masks are already first-class on the species.**
  `species.templates` / `species.masks` / `selected_template_id` / `selected_mask_id`
  (`services/project_state.py:233-240`) with `get_selected_template()` / `get_selected_mask()` /
  `get_template_by_id()` / `get_mask_by_id()` (`:246-260`). Nothing in the v3 decoupled-collections
  design prevents rendering the selected pair anywhere.
- **(d) Copy-to-clipboard exists** — `TemplateWorkbench._copy_to_clipboard`
  (`ui/template_workbench.py:1001-1003`: `navigator.clipboard.writeText` + a 1.2 s toast), plus the
  card header's file-icon-copies-path affordance (`:979-999`). UI-private today; hoisting it is the
  one new component this stage adds.
- **(e) The deep link exists** — the page registers `callbacks["species_select_tab"] = self.select_tab`
  precisely so a tab can jump to a sibling tab (`ui/species/page.py:136-138`; the Picks tab's empty
  state already uses it). `"templates"` is the tab key (`page.py:42`).
- **(f) Reactivity constraint.** The identity editor is built **once** per species, deliberately
  outside every rev-gated view, because each edit bumps the rev and a gated rebuild would destroy the
  input mid-keystroke (`overview_tab.py:1-11`, and P-02's debounce fix). Anything that must repaint
  when the *selection* changes on another tab therefore needs its own `FingerprintedView` — the
  status block already fingerprints exactly these terms in `_StatusView._species_terms`
  (`:237-248`: `len(templates)`, `selected_template_id`, `len(masks)`, `selected_mask_id`, geometry).
- **(g) Symmetry options** come from `SymmetryGroup` (`services/jobs/_base.py:19-36`): C1…C6, D2…D6,
  T, O, I1, I2. `C1` *is* "no symmetry" — the display name is the only thing that needs to change.

## 1. Stage S1 — one commit

**(a) New `ui/components/path_link.py`:**

```python
def copy_to_clipboard(value: str) -> None: ...
def render_path_link(path: str, *, on_open: Callable[[], None] | None = None,
                     open_label: str = "open ↗", empty_text: str | None = None) -> None:
    """basename (11 px mono, bold) · full path (10 px mono, truncated, full value on hover) ·
    copy icon · optional open link. With `path` empty, renders `empty_text` in the hint style."""
```

Hoisted from `template_workbench._copy_to_clipboard` (`:1001-1003`); the workbench keeps working via
a one-line delegate and roadmap 05 repoints its rows to `render_path_link`.

**(b) Identity block** (`render_identity_editor`, `ui/species/overview_tab.py:92-189`):

- Drop `border-left: 4px solid #6366f1` and the card entirely; the block is a plain column with the
  page's own spacing. The species colour is already carried by the swatch and the header pill —
  it does not need a third representation as a stripe.
- Row 1 (identity line): swatch · name input, sized to read as the title of the page section
  (13 px, `w-64`), nothing else.
- Row 2 (`field_grid()`-style two/three column pack, labels 10 px, inputs sized to their content):
  - **Diameter** — `ui.number`, `w-32`, suffix "Å" rendered as a sibling label rather than Quasar's
    in-field suffix so the number never competes with it. Spelled out; the "Ø" glyph goes in the
    tooltip, not the label.
  - **Symmetry** — `ui.select`, `w-40`, options `{g.value: label}` with **`C1` → "None (C1)"** and
    the rest as-is; value defaults to `sp.symmetry or "C1"`. Tooltip: *"Point-group symmetry of the
    particle. Most complexes are C1; this is the default for new TM jobs, which can still override
    it."* Same display convention as roadmap 01-S2's creation dialog and roadmap 02's TM row — one
    spelling across all three.
- Row 3: **Notes** — `ui.textarea` (`autogrow`, 2 rows min), full width, free-form.
- Keep the existing `debounce=400` on every input and the single
  `save_project(project_path, debounce_s=1.0)` per edit (peeve P-02 — do not regress it); the textarea
  needs the same `debounce` prop.
- Keep the hint line (`:184-188`: "Defaults for new TM / pick-candidates jobs; existing jobs aren't
  auto-updated") and `_render_provenance` (`:192-209`) — both are already in the right register.

**(c) NEW bindings block** — its own `FingerprintedView` (`_BindingsView`) directly under the identity
block, in-memory reads only:

- `signature() = (selected_template_id, selected_mask_id, len(templates), len(masks),
  selected template path, selected mask path)`.
- Renders **two rows that are always present**, whether or not anything is bound:
  - `TEMPLATE` — `render_path_link(selected.template_path, on_open=…)`; polarity chip
    (`template_workbench._polarity_chip`, worth hoisting alongside `path_link` if it is one line);
    when unset: *"not set"* + the open link.
  - `MASK` — same, with the method chip; when unset: *"not set"*.
  - `on_open` = `ctx.callbacks["species_select_tab"]("templates")`; when the hook is absent the row
    renders without the link (mirror `species_opener`'s None handling,
    `ui/components/species_pill.py:38-46`).
- One muted trailing line: `"N templates · M masks registered"`, itself the link to the tab.
- **No disk reads here** — apix / box / σ come from `read_template_header` and stay on the Templates
  & masks tab (and in the status compute). This block must be safe to re-render on every 3 s tick.

## 2. Non-goals

- Nothing about *which* template a job uses changes; this block is a read-only view of the species'
  selection with a route to where it is changed.
- Selecting a different template from the Overview is **not** in scope — selection lives with the
  thing that shows headers, stats and the viewer (roadmap 05's table). One selection affordance, not
  two.

## 3. Runtime checklist (maintainer)

1. Overview tab: no blue stripe, no card; name / Diameter / Symmetry / Notes read at the same scale
   as their labels; the diameter value is fully visible with the Å beside it; the symmetry dropdown
   is obviously a dropdown and opens on click.
2. A fresh species shows Symmetry = "None (C1)".
3. Notes accepts multiple lines and grows; edits still save once per pause (watch the log — no
   per-keystroke saves).
4. Bindings block: with a template and a mask selected, both rows show basename + full path; the copy
   icon puts the full path on the clipboard (paste into a terminal); "open ↗" switches to Templates &
   masks with the species still selected.
5. With nothing bound (a de-novo species): both rows still render, say "not set", and still link.
6. Change the selected template on the Templates & masks tab, come back — the row updated (within the
   3 s tick or instantly on show) without the identity inputs losing focus or content.
7. `ruff check .`.

## 4. Log

- 2026-08-18 — scoped from the walkthrough. No code. Recorded the answer to the maintainer's
  question: the v3 decoupled-collections model, the clipboard helper and the cross-tab hook all
  already exist, so the bindings block needs no model change.
- 2026-08-18 — **implemented.** `ui/components/path_link.py` (+ `ui/components/chip.py` for the
  polarity / method chips hoisted alongside it); the blue-stripe card is gone, `SYMMETRY_OPTIONS`
  renders C1 as "None (C1)", notes is a `ui.textarea`, and `_BindingsView` is an in-memory
  `FingerprintedView` over the six terms in §1(c).
