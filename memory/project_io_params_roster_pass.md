---
name: project_io_params_roster_pass
description: "IO-tab species tags + abs-path tooltips, duplicate-source-candidate bug fix, param 2-col compaction, resizable roster (2026-07-25)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 758c059d-5fc7-4b18-a7aa-fafc38761084
---

Follow-up UI pass on the project workspace (2026-07-25, code-clean py_compile+ruff, PENDING RUNTIME — restart `python main.py`). Five items from user feedback:

**Duplicate source-candidate bug (root-caused + fixed).** In `agg_SK_20251017_412`, reconstructParticle's `input_optimisation` dropdown showed TWO identical "Subtomo Extraction #2 (job017)". ROOT CAUSE: a job can expose several outputs of the SAME `JobFileType` — subtomoExtraction emits both `output_optimisation` (optimisation_set.star) and `output_optimisation_filtered` (optimisation_set_filtered.star, `prefer_if_exists=True`), see `services/jobs/subtomo_extraction.py:57-69`. Both become `OutputCandidate`s and **`OutputCandidate.source_key` is `jobtype:instance_path` — it does NOT include the output key** (`path_resolution_service.py:44-47`), so the two collide on source_key AND on `_candidate_name` label. Worse: `_resolve_override` returned the FIRST index match (raw) while the auto-scorer `_choose_candidate_for_slot` prefers `prefer_if_exists` (curated) — inconsistent. FIX: (a) `get_candidates_for_slot` dedups by source_key keeping the `prefer_if_exists` sibling (it has a SINGLE caller, io_config_component.py:325 — UI-only, safe); (b) `_resolve_override` now returns the `prefer_if_exists` match among ties. NET BEHAVIOR CHANGE (flagged to user): reconstruct now consumes the curated `_filtered` optimisation set, not the raw one — matches the picks-curation workflow + the design intent. Note: instance `subtomoExtraction`=job016/species 412_blob, `subtomoExtraction__2`=job017/species 412 (unsuffixed≠first here).

**Species tags (#1).** Roster already had a species badge (`pipeline_roster.py:438`). ADDED one to the job-tab header (`job_tab_component.py`, after the tab switcher, uses `state.get_species(job_model.species_id)`) and a `· <species_id>` suffix in IO candidate labels (`io_config_component._candidate_name`) to disambiguate same-type producers per species.

**Abs-path tooltips (#4).** `io_config_component._input_name_label(slot, validation)` now appends `validation.resolved_path` (+ awaiting/not-on-disk notes). Threaded the already-computed `validation` in at the call site.

**Param compaction (#3).** `ui/job_plugins/_field_styles.py` `field_grid()` was a 1-col flexbox (`gap:1px`); now `display:grid; grid-template-columns: repeat(auto-fill, minmax(300px,1fr)); row-gap:2px`. Short numeric/enum/text fields flow 2-4 across; paths stay full-width (rendered outside field_grid), section headers sit above each grid. Single caller path unaffected.

**Resizable roster (#5).** `ui/workspace_page.py`: roster default 300→340px + a `cb-roster-resizer` drag handle between roster and main; client-side JS (`_ROSTER_RESIZER_JS`, injected via one-shot `ui.timer`+`run_javascript`) resizes+clamps 240-720px, persists to `localStorage.cbRosterW`, MutationObserver mirrors roster display so the handle hides in journey mode. CSS `.cb-roster-resizer` in `main_ui.py`. Job-name label is `flex:1`+ellipsis (no fixed cap) so widening directly relieves truncation.

**Env corrections:** `/groups/klumpe/crboost_data` IS accessible now (mount back — corrects [[reference_crboost_data_access]]). Sandbox `venv/bin/python3` runs py_compile + non-nicegui code, but **`pandas` is NOT importable** (project_state import chain needs it) → the resolver/ProjectState can't be exercised in-sandbox; hand resolver runtime checks to the user. Pre-existing dead import `TemplateMatchPytomParams` at `path_resolution_service.py:9` (not mine, left per surgical rule).

---

**Follow-up polish batch (2026-07-25, code-clean PENDING RUNTIME):**
- New reusable `ui/components/copyable.py`: `copy_button(text)` + `copyable_path(path)` (full path, wraps, never truncates, + copy icon). APP-WIDE RULE: every path gets a copy icon.
- **IO tab STANDARDIZED across job states.** `IOConfigComponent` gained `read_only` param; `io_tab.py` now always calls `render_io_config(read_only=is_frozen)` and the old flat `_render_readonly_paths` dump is GONE (deleted `_compact_template`, dead `re` import; `kv_row` in `_field_styles.py` now unused-but-left). Completed jobs get the SAME Inputs/Outputs sectioned view, read-only (static source face via `_render_static_source`, no manual/reset/gain-browse). Input + output paths now render as a full-width copyable line BENEATH each slot (was truncated ellipsis). Removed the "Input — wired from an upstream job" operational line + the path block from the name hovers (path is inline now, so hovers are pure type-explanation).
- Dropdown hover darkened (`.cb-io-menu-item:hover` #f1f5f9→#e2e8f0, main_ui.py).
- **Landing strip** (`landing_status_strip.py`): SLURM + host dots are now interactive dark popovers (dotted-underline hint; open on mouseenter OR click, click-away to close, `ui.menu` `no-parent-event`) — host shows the `ssh -f -N -L port:localhost:port user@host` command (copyable), SLURM shows partition chips (from `get_partitions_info` → `slurm_part_details`) + the `config/qsub.sh` full path (copyable). ⚠️ hover-open QMenu is the one bit I could NOT runtime-verify — if janky, switch to click-only.
- **Settings dialog** (`config_settings_dialog.py`): widened 760px→`92vw/max 1060px`; inputs were truncating at an intrinsic mid-width (same Quasar shrink-wrap bug as BASE bar / ArtiaX) → added `width:100%` to all path/tool/slurm inputs so full paths are always visible.
- Species chip already added to job-tab header in the prior batch (this batch didn't touch it).

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

2026-07-25: duplicate source-candidate bug root-caused (job with raw+_filtered same-JobFileType outputs collide on source_key=jobtype:instance_path, no output_key) → dedup in get_candidates_for_slot + _resolve_override prefers prefer_if_exists (NET: reconstruct now uses curated _filtered set); species chip in job-tab header + IO candidate labels; abs-path input tooltips; field_grid → multi-col grid; resizable roster (client JS + localStorage). /groups/klumpe/crboost_data accessible again; venv python has NO pandas.
