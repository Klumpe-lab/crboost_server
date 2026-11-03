---
name: picking-ui-wave3
description: "2026-08-24 packaging pass: authoritative-list model DELETED everywhere (schema 3.6), rail curation-session indicator, roster hides outside pipeline view, Journey green dot gone, Picks-table grid de-staggered. CODE-COMPLETE, uncommitted, PENDING RUNTIME"
metadata: 
  node_type: memory
  type: project
  originSessionId: f24d7bcf-121d-4bcc-aae6-c585ff709a1b
  modified: 2026-08-24T09:24:58.693Z
---

Maintainer's wave-3 asks (2026-08-24), all landed in one session, ruff green, **no runtime pass yet**:

1. **Rail curation-session indicator.** `RosterWidget._build_curation_session_btn` pins a
   `view_in_ar` icon at the FOOT of the 60px primary sidebar (after the `flex: 1` spacer). Dim
   `SB_MUTE` normally, `#16a34a` + `.cb-artiax-live` CSS breathe when a session is up; hover names
   the species + tomogram. It never launches — click opens the same control center the Picks &
   curation chip does, so 09-S2's "exactly one launch affordance" still holds.
   `ui/particles/session_status.py` now caches the live session's `scope.json` (`scope()`,
   `scope_text()`); a 6 s `ui.timer` asks it, the module's own ~16 s throttle decides when a
   `squeue` actually happens. FOUR paint states, not three: `unknown`-with-error is amber,
   `unknown`-never-polled is just dim (else every page load cries wolf).

2. **Roster hides outside the pipeline view.** `set_active_mode` was `mode in (journey, gallery,
   viewer)`; now `mode != "pipeline"`. Particles/Species no longer coexists with the job roster.
   Collapse/expand is now purely "how much room does the open job page get".
   ALSO FIXED THERE: `_update_pipeline_btn_style` used to `container.clear()` + rebuild, which
   destroyed the Quasar tooltip parented to the container — the Pipeline button silently lost its
   hover text after the first mode switch. It now sets `ui.html.content` in place, and
   `_sb_svg_btn` stores the icon under `<ref_key>_icon` for exactly that.

3. **Journey's green dot removed** (`has_any_previews_rendered` was true forever after the first
   reconstruction, so it read as a permanent "something is new" badge pointing at nothing).
   `_build_dashboard_btn` is now a plain `_sb_svg_btn`. The service helper is left in place.

4. **Authoritative-list model DELETED.** Rationale from the maintainer: what feeds a refinement is
   the source set chosen in the Aggregate-candidates / merge flow, in front of the merge it feeds,
   so a stored per-(species, tomogram) nomination decides nothing. Gone: `ProjectState.
   authoritative_pick_lists` + `_auth_key`/`get_`/`set_authoritative_slug` (SCHEMA_VERSION → 3.6;
   old project_params.json keys are simply not read), `ListRow.is_authoritative`,
   `SpeciesOverview.gate`, the `auth` column in BOTH tables (picks_tab + pick_viewer strip),
   `.cb-auth*` CSS, the Journey's `auth_sig`, `species_references()["authoritative_pick_lists"]`,
   and `backend.get_authoritative_extraction_status` (had zero callers).
   `services/aggregation/authoritative.py` → **`services/aggregation/extraction.py`**, keeping the
   extraction-inputs half + `apply_aggregation_overrides` and gaining `pending_extractions()`
   (→ `PendingExtraction`: every list NOT_EXTRACTED/STALE, with `extracted_path` and a
   `blocked_reason`). `backend.extract_authoritative_pending` → `extract_pending_lists` (dropped
   the never-used `only_tomos`), `get_authoritative_gate_report` → `get_pending_extractions`.
   The header `gate` chip became an `extracted n/m` chip whose denominator excludes `n/a` rows.
   `merge_card.preflight_blockers` no longer asks a gate: it matches SELECTED source paths against
   STALE lists' `extracted_path`, so it warns only about sources whose optset is behind its picks
   (a never-extracted list has no optset and cannot be a source — warning about it was crying wolf).

5. **Picks-table de-staggered.** `.cb-ptable-row` had `minmax(48px,72px) minmax(70px,150px) … auto`
   — content-sized tracks, and ONE TABLE IS RENDERED PER TOMOGRAM GROUP, so every group settled on
   its own column widths. Now every track is fixed or `fr`
   (`14px minmax(0,2fr) 72px minmax(0,1.4fr) 56px 26px 24px 96px`), last one fixed with
   `.cb-ptable-actions { justify-self: end }` so rows end AT the line's end. `_render_actions` also
   holds an empty 28px box where `dedup` would go on non-merged rows, so extract/dedup/delete never
   slide. `.cb-ltable-row` (pick-viewer strip) lost its 30px auth track: 6 tracks, 6 children.

**How to apply:** the user still owes ONE runtime pass — this stacks on the 08–12 wall in
[[project_picking_ui_wave2]], which is also uncommitted and PENDING RUNTIME. Verification ceiling
reached here was `ruff check .` + `ruff format`; hand the user `python -c "import main"` and
`python check_boundaries.py` ([[reference_hpc_env]] — Claude's venv python is a dangling symlink).
Docs under `docs/roadmaps/completed/roadmap_list-extraction-and-aggregation.md` §8.x still describe the removed gate; left
alone deliberately (historical design record), and `merge_card` still cites §8.9 as a pointer.
Related: [[project_denovo_picking]], [[project_artiax_bridge]], [[feedback_ui_chrome_conventions]].
