# Journey dashboard — refactor + UX-rework handoff

**Audience:** the next session (cold start, no prior conversation context).
**Date:** 2026-06-08. **Author:** prior session (Claude).
**Companion memory:** `project_journey_rework_dashboard_refactor.md`. **Related:**
`services/visualization/ARTIAX_BRIDGE_PLAN.md`, `project_ux_jank_refactor`,
`feedback_reactive_ui_patterns`, `reference_hpc_env`.

---

## TL;DR / status

| Phase | What | Status |
|---|---|---|
| **R0** | Decompose the 6,930-line `ui/tomo_dashboard_dialog.py` into a `ui/dashboard/` package (behavior-preserving) | ✅ **DONE** — original **6,930 → 4,590**; smoke-tested (opens, doesn't break) |
| **P1** | **The UX rework** — de-dialog + heatmap top-strip + hide-roster + active-icon highlight | 🔨 **C1 (de-dialog) VERIFIED by user · C2 (heatmap strip) LANDED, pending runtime verify** — see Progress |
| **P2** | Carve the remaining ~2,900-line particles/gallery/pick-lists/curation region into modules, *as the ArtiaX workbench slices touch them* | ⬜ deferred (do opportunistically) |

R0 was the "safe extractions first" prelude; P1 (the real goal) is being done as
two checkpoints — see **Progress** below. P2 is still untouched.

## Progress (2026-06-08, P1)

**C1 — de-dialog → embedded swappable panel. VERIFIED by user.**
- `open_tomo_dashboard()` → `build_journey_panel(container, callbacks)` (`tomo_dashboard_dialog.py`):
  no dialog/card/close-button; mounts into a new `journey_container`. The 4 s live timer is gated by
  `_active` + `on_journey_active(bool)` so it pauses when the journey is hidden.
- `workspace_page.py`: `journey_container` (lazy-built on first open behind a spinner; SingleFlight-
  guarded `_show_journey`); `_switch_to` handles `"journey"`; registers `toggle_journey`.
- `pipeline_roster.py`: dashboard icon → `panel.toggle_journey`; `set_active_mode` = exactly-one-lit
  nav highlight (pipeline/workbench/journey) + hide/restore the 300px roster; `_on_pipeline_icon`
  (layers returns to pipeline when elsewhere, else toggles roster). `pipeline_builder_panel.py`
  threads `toggle_journey` + registers `set_active_mode`.
- Behavior notes: nav icons are now mode-driven (one lit); auto-kick dedup resets at build, not
  per-open (the panel persists now).

**C2 — heatmap top-strip + column layout + dedup. LANDED, pending user runtime verify.**
- New `ui/dashboard/strip.py` (`build_strip`) — pure presentation over `data.py`; imports ONLY
  `data.py` (DAG one-way), the ⓘ popover arrives as a callback (no back-import into the shell).
  Frozen-left prep row + per-species rows (name + Σ pick/filt) × horizontally-scrolling TS columns;
  prep cell = 4 status dots, species cell = `n_picks` shaded by furthest stage (subtomo>pick),
  filtered count surfaces in the selected column. CSS added to `css.py` (`.cb-strip-*`).
- `build_journey_panel` flipped row→column (strip on top, detail pane below); `render_sidebar` →
  `render_strip`, gated on `(_journey_signature, selected_ts)` so selection forces a rebuild (the
  select-ordering pitfall is preserved: render main pane first, rebuild strip last).
- Deleted orphaned `_render_ts_row` / `_pill_tooltip` / `_PILL_TOOLTIP_LABEL`; dropped the redundant
  per-TS pick count from the particles species tabs (now lives in the strip). `tomo` 4590 → 4501.
- **Deferred from P1's original step 4:** deep async-on-TS-switch. C1's spinner covers the worst case
  (first open); TS-switching stays sync, matching the C1 feel the user already validated. Add a
  switch-spinner later only if the user asks.

**To verify C2:** strip renders as a matrix header; clicking a column selects that TS + drives the
detail pane; selected column highlighted + shows `▸filtered`; rows align across the frozen-left
labels and the columns; horizontal scroll appears for many TS while the left labels stay; the ⓘ in
the corner pops the selected TS's file paths.

**C2b — post-feedback polish (landed same session, after user tried C2).** Three asks:
- **Instant TS-switch.** `select_ts` is now `async`: it moves the column highlight + paints a
  spinner and flushes to the client, THEN runs the (still-synchronous) detail render — the CSS
  spinner animates client-side so the click feels instant. Generation-guarded (`_sel_gen`) against
  rapid clicks (latest wins, no double render). `build_strip` now returns `{ts: col element}` so the
  highlight can move without a full rebuild; `render_strip` stores it in the `col_els` closure.
- **Filtered/kept count on ALL cells, always** (was selected-column-only — defeated the overview).
  Columns widened 52 → 62 px to fit `auto ▸kept`.
- **De-greened the species cells.** No more success-green fill (picks existing isn't a "success").
  Only running (amber) / fail (red) / zero (gray) fill; "has picks" is neutral. Green now lives ONLY
  on the kept-count number (= curated, a real step) and the prep dots (= stage done). `_cell_class`
  rewritten; `.cb-strip-pickcell.{done,ok}` removed, `.has` added, `.cb-strip-filt` → green/bold.

## Next UX asks (roadmap — user-listed 2026-06-08, after C2)

Recorded for later — do NOT start without re-confirming priority. User's stated order: these come
BEFORE the big picks-gallery rework (the differentiated manual/auto/merge picking + selection layout,
which is the ArtiaX-aligned work in `services/visualization/ARTIAX_BRIDGE_PLAN.md`).

**R1 — Bigger tomo preview. ✅ LANDED (pending user runtime verify).** Now the journey reclaimed the
side, the recon **xy + xz slabs render ≥2× larger** and the gallery/tools cluster is pushed right.
Three levers (all in `css.py` + the inline cap in `_render_particles_canvas`):
- **Flipped column proportions** so the slab column dominates and the gallery yields width:
  `.cb-particles-canvas-col` `flex: 1 1 360px / max-width 540` → `flex: 3 1 600px / max-width 1080`
  (basis up, grow 1→3, **width cap doubled 540→1080**); `.cb-particles-tabs-col` `flex: 2 1 440` →
  `flex: 1 1 360` (grow 2→1, yields the width).
- **Raised the XY height cap** `52vh → 76vh` (inline `stack.style(... calc(76vh * x/y) ...)` in
  `_render_particles_canvas`, ~line 2506) so near-square tomos grow toward the doubled width before
  the height cap binds. No-picks preview cap bumped `70vh → 76vh` to match.
- **R1b — column hugs the slab (no gutter).** First pass left the column growing to its flex share
  (≤1080px) while the height-capped near-square slab was only ~820px wide → ~200px whitespace before
  the gallery (user-reported). Fix: the column's max-width is now set **inline per-tomo** in
  `_render_particles_section` to `min(1080px, calc(76vh * x/y))` — the slab's *actual* height-limited
  width — so the column hugs the previews; the inner stack's own `max-width` was dropped (it just
  `width:100%`-fills the column now). The gallery column absorbs all reclaimed space. Fixes the
  no-picks path for free (same column). Dims taken from the first species with real `dims` (≠[1,1,1]).
- **Net (1080p display):** landscape tomos hit a clean **2× linear** (column-bound at 1080px);
  near-square tomos hit **~1.5× linear / ~2.3× area** (height-bound at 76vh ≈ 820px) — the viewport
  height is the physical limit; full 2×-linear on a square tomo would need ~100vh and break XY/XZ
  co-visibility. **The dial is the `76vh`** (appears twice now — inline col cap in
  `_render_particles_section` AND the no-picks preview `max-height`): drop to ~62vh if the user can't
  see XY+XZ together; the width cap (`1080px`) is the other dial. Pick overlays scale automatically
  (fractional `inset:0`).

**Persistence layer (R2/R3) — ALREADY EXISTED, no new layer built.** `services/configs/
user_prefs_service.py`: `UserPreferences` (Pydantic) + `get_prefs_service()` singleton, dual-stored
in `app.storage.user` AND `~/.crboost/prefs.json`; `storage_secret` is wired in `main.py`; load/save
used across `main_ui`/`data_import_panel`/`pipeline_roster`/`projects_overview`. R2/R3 just added two
fields: `dashboard_hidden_panels: List[str]` (absence ⇒ visible) + `dashboard_dataset_collapsed: bool
= True`. New panels added later default to shown (hidden-list semantics). Backward-compatible (old
stored prefs lack the fields → Pydantic defaults).

**R2 — Per-panel visibility toggles. ✅ LANDED (pending user runtime verify).** A dense-checkbox row
(`.cb-panel-toggle-row`, mirrors `.cb-species-toggle-row`) sits between the heatmap strip and the
detail pane in `build_journey_panel` (`panel_toggle_container`, built once via `_build_panel_toggle_row`
after `render_main` exists). One checkbox per panel from `_DASHBOARD_PANEL_KEYS` (dataset / fs_ctf /
tilt_filter / ts_align / ts_ctf / reconstruct / particles). Toggling → `_toggle_panel` updates
`dashboard_hidden_panels`, persists (`_save_dashboard_prefs` → `app.storage.user`), and calls
`render_main()` (strip untouched — visibility doesn't affect it). `_render_main_pane_for_ts` gates
each emitter on `_hidden_dashboard_panels()` (keys match the registry); all-hidden shows a "re-enable
above" hint instead of the no-data state. Toggle row is chrome — NOT rebuilt by the 4 s live timer.

**R3 — Collapsible Dataset section. ✅ LANDED (pending user runtime verify).** `_render_dataset_section`:
header is now `.cb-collapsible-header` (clickable) with a rotating `.cb-collapse-caret`; the body
(stage-0 chips + 2-col key/val grid + `_render_pixel_sanity_table`) is wrapped in
`.cb-collapsible-body`, hidden via `.cb-collapsed` when collapsed. Collapsed (header + metric strip
only) is the default (`_dataset_collapsed()` reads the pref, default True). Click toggles CSS classes
directly + flips/persists `dashboard_dataset_collapsed` — no re-render, instant. Orthogonal to R2
(R2 hides the whole panel; R3 collapses within it). Compute (`_compute_pixel_chain`) still runs when
collapsed (cheap, in-memory) so expand is instant.

**Then (the big one):** picks-gallery rework — sane/differentiated manual/auto/merge picking +
selection layout. See `ARTIAX_BRIDGE_PLAN.md`.

---

## Hard constraint: NO runtime test in this venv

`venv/bin/python3` + `venv/bin/ruff` work directly (LD_LIBRARY_PATH is set in
`.claude/settings.local.json` — see `reference_hpc_env`). The venv has
`nicegui/fastapi/starfile/mrcfile` but **NOT pandas/numpy**, so anything that imports
`services/project_state.py` (→ pandas) cannot be imported/run here. Verification you CAN do:

```bash
venv/bin/python3 -m py_compile <file>     # syntax
venv/bin/ruff check <files>               # F821 undefined / F401 unused = the oracle
venv/bin/ruff format <files>
venv/bin/ruff check --fix <file>          # prune unused imports + sort
```

The **only** real proof of runtime correctness is the **user opening the app**
(`python main.py --port 8081`) and exercising the Journey panel. Plan for a build →
user-tests loop on P1 (unlike R0, P1 is *new logic*, not behavior-preserving).

---

## R0 result — the `ui/dashboard/` package (current state)

```
ui/dashboard/
  __init__.py        (7)    package marker
  css.py           (666)    _CB_CSS blob + ensure_assets_loaded()
  data.py          (702)    ★ status/lookup layer — the API P1's strip consumes
  figures.py       (419)    Plotly dict builders + _safe_floats/_stats/_is_meaningful_series
  pixel_sanity.py  (630)    _compute_pixel_chain → _apply_sanity_rules → _render_pixel_sanity_table
```

`ui/tomo_dashboard_dialog.py` (now 4,590) still holds: the **shell**
(`open_tomo_dashboard`), the per-TS **sidebar rows** (`_render_ts_row`), the **section
renderers** (dataset / prep / recon / particles), the **gallery**, **pick-lists**,
**curation handlers**, **auto-kick**. Public API unchanged: `open_tomo_dashboard` +
`has_any_previews_rendered` (the latter now lives in `data.py`; the one caller,
`pipeline_roster.py`, was repointed).

### `data.py` — the API P1 builds on

```python
_collect_dashboard_journey(state, project_path) -> (journey, ts_names)
    # journey: {ts_name: {stage_key: status}}  stage_key ∈ fs_ctf/align/ctf/recon/pick/subtomo
    #          status  ∈ ok/fail/running/zero/pending
    # ts_names: ordered union of all TS in the project
_collect_species_journey(state, project_path) -> {ts_name: [species_dict, ...]}
    # species_dict = {idx, label, color, species_id, pick_status, subtomo_status,
    #                 n_picks, filtered_count, ce_star, subtomo_star, pixel_size_ang, tomo_dims}
_journey_signature(journey, species_journey, ts_names) -> tuple   # fingerprint for signature-gating
_recon_mrc_map(state, project_path) -> {ts_name: mrc_path}
_PREP_STAGES   # the 4 prep stages [(key,label,JobType)] — fs_ctf/align/ctf/recon
```

The heatmap strip is a **pure presentation transpose** of this data — no new collectors needed.

---

## P1 — the UX rework (design rationale — IMPLEMENTED in C1/C2/C2b; see Progress above)

### Why it's janky today (root causes, all confirmed)

1. **Journey opens as `ui.dialog().props("maximized")`** (`tomo_dashboard_dialog.py:181`,
   inside `open_tomo_dashboard` @155) — a z≈6000 overlay that **covers the always-present
   60px sidebar**. Pipeline/Workbench don't, because they swap *inside* `main_area`
   (`workspace_page.py`).
2. **2–3 s to open:** `open_tomo_dashboard` does all disk I/O + DOM build *synchronously
   before* `dlg.open()` (@325). The killer is the eager main-pane render for the selected TS
   → `_render_particles_section` (@2450) → `_collect_species_data_for_ts` →
   `_read_pick_list_voxels` → `starfile.read()`, none off the event loop.
3. **Per-TS status** lives in the journey's **own 300px internal sidebar**
   (`render_sidebar` closure @247 → `_render_ts_row` @407), duplicating species name+count
   across the sidebar row + the particles header + the particles tabs.

### Decided design (locked with the user via Q&A — DO NOT re-litigate)

- **Strip layout = HEATMAP MATRIX.** Species are frozen-left **rows** (name + project-wide
  Σ pick/filtered totals) + a `prep` row (FS·Al·CT·Re); tilt-series are
  **horizontally-scrolling columns**; each cell = pick count shaded by status; filtered
  counts surface for the **selected** column; the selected TS column is highlighted and
  **drives the detail view below**. Species name/count written **once on the left**, not per TS.

  ```
   species      Σ pk/filt │ ts01  ts02 ▸ts03  ts04  ts05  →
  ────────────────────────┼──────────────────────────────────
   prep  FS·Al·CT·Re      │ ●●●●  ●●●○  ●●●●  ●●●●  ●○○○
   ● ribosome    639/402  │  142   98  [176]  203    —
   ● ferritin     90/ 60  │   18   12  [ 22]   31    7
                          │   (filtered shown for ▸ts03)
  ```

- **Strip scope = JOURNEY-PANEL HEADER.** It sits at the top of the Journey content and
  **replaces** the 300px internal sidebar. Pipeline/Workbench are untouched. (Not a global page bar.)
- **Roster in Journey mode = HIDE for full width.** Drop the 300px pipeline job roster while
  Journey is active → preview/picks/graphs get the full width right of the 60px icon strip.
- **Gallery visual-density redesign stays DEFERRED** (user-flagged in `ARTIAX_BRIDGE_PLAN.md`).
  P1 modularizes/repositions; it does NOT redesign the gallery internals.

### Implementation plan (step by step, with anchors)

**1. `workspace_page.py` — add a journey container + extend the swap.** (This file was NOT
touched by R0; anchors are current.)
- `main_area` @90 currently holds `pipeline_container` (@95) and `workbench_container` (@109),
  swapped by `_switch_to(mode)` @32 via CSS `display`. Add a **`journey_container`** sibling
  (`display:none` default), `_refs["journey_container"] = ...`, and build the journey panel into it.
- Extend `_switch_to` to handle `"journey"`: show journey_container, hide the others, **and
  hide `roster_panel`** (the 300px job list) for full width. On leaving journey, restore the
  roster to its prior `_roster_visible` state. Because journey_container is a flex sibling of
  the 60px `primary_sidebar` (@76, z-index 20), it **physically cannot cover the sidebar** —
  that's the whole fix for root-cause #1.

**2. De-dialog the shell** (`open_tomo_dashboard` @155 → an embedded `build_journey_panel(container)`).
- Today: `ui.dialog().props("maximized")` (@181) → card → flex-row of `sidebar`(300px) + `main_area`.
  Closures: `render_main` (@217), `select_ts` (@227), `render_sidebar` (@247), `refresh_all`
  (@279), `_maybe_refresh` (@295), `live_timer = ui.timer(4.0, …)` (@321), `dlg.open()` (@325).
- Change: mount the content into `journey_container` (a **column**: strip on top, `main_area`
  below — NOT a row with a 300px sidebar). Drop the dialog/card wrapper + the floating close button.
- The **4 s `live_timer`**: tie it to the journey mode lifecycle (start/stop in `_switch_to`,
  or gate `_maybe_refresh` on "journey is active") instead of `dlg.on("hide")`.

**3. New `ui/dashboard/strip.py` — the heatmap strip** (replaces `render_sidebar`/`_render_ts_row`).
- Consumes `_collect_dashboard_journey` + `_collect_species_journey` from `data.py` (already the
  exact shape needed). Compute per-species Σ totals across `ts_names` for the frozen-left column.
- Frozen-left labels (sticky `position: sticky; left: 0`) so they stay during horizontal scroll;
  TS columns in a horizontally-scrollable flex/grid. Cell = `n_picks` with status-shaded bg;
  prep row = 4 status dots (reuse the `.cb-pill {status}` CSS already in `css.py`).
- Selected TS column highlighted; clicking a column calls `select_ts(ts)`.
- **Signature-gate it** (reuse `_journey_signature`) so the 4 s timer only rebuilds on real change
  (FingerprintedView discipline — see `feedback_reactive_ui_patterns` / CLAUDE.md). Kill the
  name/count duplication: the particles-section per-species tabs can drop their redundant counts.

**4. Async load** (root-cause #2). In `render_main`: paint the strip + a spinner *immediately*,
then run the heavy `_render_main_pane_for_ts` (@476) — esp. `_render_particles_section` (@2450)
— off the event loop via `run.io_bound`/`asyncio.to_thread`, then populate. Precedent: the
existing recon-slab "Rendering…" spinner.

**5. `pipeline_roster.py` — wire the button + active highlight.**
- `_build_dashboard_btn` @1486 currently does `open_tomo_dashboard()` on click (@1508). Repoint to
  a `panel.toggle_journey()` that calls `_switch_to("journey")` (mirror `panel.toggle_workbench` @854).
- **Active-icon highlight:** the Pipeline button already styles off `self._roster_visible` via
  `_update_pipeline_btn_style` @813 (uses `SB_ABG/SB_ACT/SB_MUTE` @817). Workbench (@854) and
  Journey (@862) have **no** active state — add one driven by the current `_switch_to` mode.
  `_sb_svg_btn` @1542 already takes an `active=` flag; thread the mode through.

### Pitfalls (each one bit before — heed them)

- **`select_ts` ordering crash.** See the comment in `select_ts` @227: render the **main pane
  first** (it may `run_javascript` via `_scroll_section_into_view`) while the clicked element is
  still alive, **then** refresh the strip (which tears down/rebuilds). Reversing it = "parent
  element this slot belongs to has been deleted" crash. Preserve this ordering in the strip.
- **Disk I/O off the event loop** (CLAUDE.md "UI reactivity"). `signature()` should read
  mtimes/in-memory, not do heavy disk reads on every 4 s tick.
- **SingleFlight** the toggle/open handler (`ui/components/reactive.py`) — the icon button can be
  destroyed+recreated mid-click by a refresh.
- **Can't runtime-test here** — build compile+ruff-clean, hand to the user to verify in the app.

---

## P2 — deferred carve (do opportunistically, ArtiaX-aligned)

The remaining ~2,900-line particles/gallery/pick-lists/curation region is where the ArtiaX
workbench slices (3b→6, see `ARTIAX_BRIDGE_PLAN.md`) keep churning — carve it **as those slices
touch it**, not speculatively. Target modules:

| Module | Source (names) | Note |
|---|---|---|
| `sections/prep.py` | dataset/FS-CTF/align/CTF/tilt-filter renderers + the data readers `_read_picks_json`/`_read_per_tilt_df`/`_per_tilt_star_path`/`_read_atlas_index` (could move to `data.py`) | |
| `sections/reconstruct.py` | recon section + slabs + polarity | |
| `particles.py` | `_render_particles_section`/`_render_particles_canvas`/`_collect_species_data_for_ts` + `_read_tm_job_json` | |
| `gallery.py` | `_render_gallery_body` (~800 lines) + peek + hover + scatter | biggest single fn |
| `pick_lists.py` | list cutouts/clash/merge/pick-layers + `_PICK_LIST_DEFAULT_COLOR` const | **grows w/ ArtiaX** |
| `curation.py` | `_handle_*_artiax`/import/register + `_curation_flight` SingleFlight | **grows w/ ArtiaX** |
| `auto_kick.py` | preview/imod/cutout background-gen glue | |

---

## The refactor playbook (use this for P2 — it worked cleanly for all of R0)

1. **Re-grep current line numbers before EVERY cut** — each deletion shifts everything below it
   (`grep -nE "^(async )?def |^class " <file>`). Stale anchors = wrong cuts.
2. **Map shared helpers first.** `grep -n "\b<fn>\b"` — if a function is used by both the moving
   cluster and staying code, it's foundational → put it in `data.py` (one-way dep), never leave it
   where a cycle forms. (`_template_match_instances` was the textbook case.)
3. **Build the new module, ruff-check it in ISOLATION.** A clean `ruff check` on the new file
   proves self-containment (no dangling refs). Add `from ui.dashboard.data import …` for anything
   it pulls from the foundation.
4. **Delete the moved ranges from the original** (`sed -i -e 'A,Bd' -e 'C,Dd'` — one pass, original
   line numbers), then `ruff check` → **F821 "Undefined name" lists exactly what to re-import**;
   **F401 lists exactly what went dead** (`ruff check --fix` prunes). `ruff format` normalizes the
   blank-line seams the deletes leave.
5. **DAG only:** dashboard modules import from `data.py` / services; they must **never** import
   back into `ui.tomo_dashboard_dialog` (module-level cycle = import failure). Original → package
   is the only direction.
6. Data readers / pure builders move freely (no cycles). Renderers depend on `css.py` + `data.py`.

---

## Gotchas / leftovers

- **Pre-existing** `asyncio` F401 at `pipeline_roster.py:1` — NOT from this refactor; left untouched
  per the surgical rule. Clean it (or not) independently.
- Net lines went 6,930 → 7,014 across 6 files (+84 = per-module docstrings/imports). Expected.
- `_read_tm_job_json`, `_read_picks_json`, `_read_per_tilt_df`, `_per_tilt_star_path`,
  `_read_atlas_index` were left in the original (used by staying sections) — fold into `data.py`
  during P2 if convenient.
