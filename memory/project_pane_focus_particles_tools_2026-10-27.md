---
name: project-pane-focus-particles-tools-2026-10-27
description: "2026-10-27 UI pass on branch roadmaps: Esc closes the lit pane (last pointerdown), Particles tools moved over slabs/gallery as boxed dropdowns, layers list as two tables, references strip deleted; missAlign Journey view scoped as roadmap 21 §7; NOT RUN"
metadata:
  node_type: memory
  type: project
  originSessionId: f51f1bbc-3c19-4e0f-9c4f-a13e5590b2a0
  modified: 2026-10-27T09:07:00.249Z
---

Built 2026-10-27 on branch `roadmaps`, ruff-clean, NOT RUN (no python in the sandbox).

- **Pane focus + Esc** (`ui/workspace_page.py` `_PANE_FOCUS_JS`): pointerdown in a pane (view frame or
  `#cb-roster-panel`, named "roster") sets `data-cb-focus`; css.py `[data-cb-focus]` overrides
  `--cb-view-head*` (light-blue title line + 2 px accent rule). Esc emits the lit pane's name: job page →
  `close_job`, roster → hide Jobs view, view → `_hide`. No "keep the last view" guard any more. Server pushes:
  `_show`/`_solo` light the view (`_panes_of`: Jobs = job page if open else roster); `_apply_views` calls
  `cbPaneKeep(shown, next)` so a vanished lit pane hands over to the view turned on last. Nothing lit at load.
- **Particles panel** (`ui/particles/pick_viewer.py`): header keeps title, geometry chip, links, admin, invert.
  Slab column: `_TOOL_ROW_STYLE` row (22 px, fixed) with version + layers; gallery column: same row with the
  list tool, extras, then `head_host`, a slot the galleries fill with "N cutouts" + toolbelt (+ Extract in full
  mode). Gallery grid top padding 0 so slab and first cutouts start level. "Subtomo gallery" title, references
  strip (`_render_reference_strip`/`_render_anchor_tiles` + CSS) and the sheet's hint line deleted (hint → title
  on "N cutouts"); sheet counter moved into its curation bar. `tool_trigger(boxed=True)` = field-box look
  (`.cb-htool--box`); version menu opens with a `.cb-hmenu-lede` sentence.
- **Layers list** (`ui/components/layer_menu.py`): `_table` per kind, head row = eye (toggle all) + name (+ column
  names when wide); species rows without the icon; rows on `--cb-lyr-cols` fixed columns (subgrid rejected:
  padding/gap quirks). Optional columns: `sources` (recon → switch call; "made on" chip, lit when shown) and
  `LayerRow.on_gallery/in_gallery` (grid icon). Wide table min 500 px. Segmentation % fractions dropped
  everywhere (`fraction_text` deleted; zoom tooltip edited).
- **missAlign in the Journey**: design only, roadmap 21 §7 + stage 7 (change-per-tilt chart, 3×3 warp strip,
  blink in Reconstruct card, iteration scrubber); decisions owed there.

**Why:** user peeves: hunting for little crosses; Particles dropdowns looked like bare text+chevron and sat
away from what they control; layers list too narrow and icon-repetitive.
**How to apply:** runtime check owed (alignment of slab vs cutouts, menu widths, Esc on job page vs roster).
Related: [[project_title_line_tools_2026-10-26]], [[project_roster_hover_card_tooltips_2026-10-20]],
[[project_style_themes_2026-10-22]].
