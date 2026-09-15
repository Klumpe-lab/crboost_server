# 05 — Templates & masks: two columns, and cards become table rows

**Status:** CODE-COMPLETE 2026-08-18 (S1 + S2), PENDING RUNTIME — checklist in §4. **Depends on:** 03 (`ui/components/path_link.py`).
**Unblocks:** nothing. **Risk:** medium — the largest visual change in this directory, but confined
to the render half of `ui/template_workbench.py`; no state, no file I/O, no molstar bridge change.
Two commits: S1 the two-column shell · S2 the tables.

**Peeves (maintainer, 2026-08-18):** *"Templates themselves when one creates them are ugly cards with
a bunch of text, some of which is truncated — can we rather make it a table with cards becoming rows
and the currently selected template being the selected row with a clear and more obvious selection
mechanism. We can put 'Masks' section on the right of the 'Templates' section to utilize all of that
unused space. The viewer can continue hanging underneath."*

## 0. Facts (Stage 0 — gathered)

- **Current layout** (`ui/template_workbench.py:898-909`, and the docstring at `:3-9`): one column,
  `gap-5`, stacked — templates cards → SOURCE tabs → masks cards → mask tabs → viewer → activity log.
  The cards are `_CARD_W = 260` px wide (`:133`) in a `flex-wrap` row (`:936`), so on a wide screen
  two or three cards sit in the top-left and the rest of the row is empty — the "unused space".
- **A card is five stacked lines** (`_render_template_card` `:953-977`): header row (file icon that
  copies the path, basename `truncate flex-1`, eye, delete — `_render_card_header_row` `:979-999`) ·
  polarity chip + `_format_header_summary` (apix · box · lp) `truncate` + size badge · stats row
  (min/max/σ, colour-coded, `_render_stats_row` `:1026-1038`) · the free-text `source` line, also
  `truncate` (`:976-977`). Three of those five lines truncate inside 260 px. Masks are the same shape
  with a method chip and a knobs line (thr / ext / soft) (`_render_mask_card` `:1338-1365`).
- **Selection today** is a 3 px indigo (templates) / purple (masks) left border on the card
  (`:955-960`, `:1340-1345`) — the same visual weight as the 1 px gray border on every other card,
  which is why it does not read as "selected".
- **Every select rebuilds both lists.** `_select_template` / `_select_mask` (`:383-397`) →
  `_after_register` → `_refresh_after_change` (`:399-414`), which `clear()`s and rebuilds the
  templates container, the masks container and the Edit Current form. Event-driven, so it does not
  violate the polling rule in CLAUDE.md — but it does mean the click that selects a row destroys that
  row, and it makes a 12-template list flash on every click.
- **The viewer** (`_render_viewer_panel` `:1432-1482`) is already full-width and fixed-height (380 px:
  a `w-44` "in viewer" list + the molstar iframe, with a slice-fallback panel behind a mode select).
  It stays where it is.
- **Shared cell pieces already exist** and are reusable as table cells unchanged:
  `_polarity_chip` (`:1087-1096`), `_method_chip` (`:1098-1111`), `_render_size_badge` (`:1073-1085`),
  `_render_stats_row` (`:1026-1038`), `_format_header_summary` (`:1040-1048`), `_render_empty_state`
  (`:1113-1120`).
- **`read_template_header` is disk I/O** and is called once per card per render (`:954`, `:1339`).
  A table does not change that; it must not make it worse (no extra reads per cell).

## 1. Stage S1 — the two-column shell (one commit, layout only)

`_render` (`:898-909`) becomes:

```
column gap-4:
  grid  (grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 16px; align-items: start)
    ├─ column A: TEMPLATES  — section header · list · its creation tabs
    └─ column B: MASKS      — section header · list · its creation tabs
  viewer panel      (full width, unchanged)
  activity log      (unchanged)
```

- `auto-fit` + `minmax(380px, 1fr)` is the same idiom `field_grid()` uses
  (`ui/job_plugins/_field_styles.py:85-88`): two columns on a wide window, one column when the
  workspace is narrow or the roster is open — no breakpoint to maintain.
- The SOURCE tabs move **inside the Templates column** and the mask creation tabs **inside the Masks
  column**. That is also what makes roadmap 06's "masks have no stated source" fixable: each column
  becomes `<things> + <where new ones come from>`.
- `align-items: start` so a long template list does not stretch the mask column.
- Nothing else moves. `_render_source_panel`, `_render_masks_section`'s tab block, the viewer and the
  log keep their current bodies in this commit — this is a pure re-parenting so the diff stays
  readable.

## 2. Stage S2 — cards become rows (one commit)

**(a) The row grid.** A `div` grid, not `ui.table` — Quasar's QTable brings material chrome the
project has explicitly rejected (`feedback_ui_chrome_conventions`). One `grid-template-columns` per
list, a 10 px uppercase header row (`_LABEL_CLS`), rows as grid children:

- Templates: `[20px sel] [1fr file] [56px polarity] [minmax(120px,auto) header] [minmax(110px,auto) stats] [64px size] [52px actions]`
- Masks: `[20px sel] [1fr file] [64px method] [minmax(120px,auto) header] [minmax(110px,auto) knobs] [64px size] [52px actions]`

Only the **file** column is allowed to be flexible and therefore to ellipsis; it carries
`render_path_link` (roadmap 03) so the full path is one hover and one click away. Every other column
is sized to its content, which is what ends the "a bunch of text, some of which is truncated"
complaint. The `source` / `imported_from` free text moves out of the row body into the file cell's
tooltip.

**(b) A selection that looks like one.** Leading cell = `radio_button_checked` /
`radio_button_unchecked` (13 px, accent colour when selected, `#cbd5e1` otherwise); the selected row
additionally gets `background: <accent>0f` and a 2 px left accent bar. Class-driven:
`.cb-tw-row` / `.cb-tw-row.selected` in `ui/dashboard/css.py`, accent set per list via a CSS custom
property (`--cb-accent: #6366f1` on the templates grid, `#a855f7` on the masks grid) so one rule
serves both.

**(c) Selecting stops rebuilding.** `_select_template` / `_select_mask` keep the state write, but the
DOM update becomes a class flip across the kept row handles — `Segmented.set_active`
(`ui/components/segmented.py:40-48`) is the precedent. Keep a `{entry_id: row_element}` dict per list,
built by the render. `_refresh_after_change` (`:399-414`) then only rebuilds a list when its
*membership* changed (register / delete); selection alone never rebuilds. The Edit Current form and
`_update_mask_source_label` still refresh on selection — they genuinely depend on it.

**(d) Rows keep every affordance the cards had**: click the row to select (row handler), eye to load
into the viewer, `close` to delete — the action buttons keep `click.stop` (`:993-999`) so they never
select as a side effect.

**(e) Empty state** stays `_render_empty_state` (`:1113-1120`), rendered in place of the rows with
the header row still visible, so the columns' shapes are legible before anything exists.

**(f) Deletions:** `_CARD_W` (`:133`), `_render_template_card` (`:953-977`), `_render_mask_card`
(`:1338-1365`), `_render_card_header_row` (`:979-999`) — its copy affordance is now
`render_path_link`'s — and `_copy_to_clipboard` (`:1001-1003`), delegated to
`ui/components/path_link.py`.

## 3. Non-goals

- No sorting, no filtering, no column resize. If a lab ends up with 30 templates on one species that
  is a different conversation.
- No change to what a row *means*, to selection semantics (`selected_template_id` /
  `selected_mask_id` stay the single selection per category), or to the polarity-pair / dedup logic.
- The viewer keeps its position, size and mode toggle.

## 4. Runtime checklist (maintainer)

1. Wide window: Templates left, Masks right, each with its own creation tabs beneath; viewer full
   width below; activity log last. Narrow the window (or open the roster) → one column, same order,
   nothing clipped.
2. Rows: filenames ellipsis only when genuinely long, full path on hover, copy icon puts it on the
   clipboard; apix / box / lp, σ stats, size and polarity/method are never truncated.
3. Click a row: the radio fills, the row tints, the left bar appears — and the list does **not**
   flash or rebuild. Click a second row: the first deselects. Edit Current updates to the new
   selection; the mask column's "derived from …" line updates.
4. Generate a shape / fetch a PDB / import: the new rows appear (membership changed → rebuild), the
   first one auto-selects when the list was empty, and the activity log records it.
5. Delete a template with the row's ×: the confirm dialog still names the file and the sidecar; the
   row disappears; the viewer's session list is unaffected.
6. Eye icon still loads into molstar; switching to slice mode still shows the selected template/mask.
7. Switch species and back: molstar iframe survives (the tab container is never cleared — roadmap
   10's rule), the tables render for the right species.
8. `ruff check .`.

## 5. Log

- 2026-08-18 — scoped from the walkthrough. No code. Flagged that select-rebuilds-everything is a
  latent click-eater and folded the fix into S2 rather than leaving it as a separate peeve.
- 2026-08-18 — **both stages implemented.** The `auto-fit`/`minmax(380px, 1fr)` grid is in place with
  each column carrying its own creation tabs; `.cb-tw-*` rows replace the cards. (c) landed as
  specified and is the part worth remembering: `_select_*` calls `_flip_selection` over kept row
  handles and `_refresh_after_change` rebuilds a list only when `_membership_moved`, so the click that
  selects a row no longer destroys it.
