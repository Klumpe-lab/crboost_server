# S6 — Aggregation factor-out + `is_aggregation` removal

Independent trailer; last because it is value-neutral for de-novo picking. ~600 lines, mostly
moves. The aggregation tool is **kept** (user directive) — this factors it into its own module,
fixes its known state bugs, and dissolves the "aggregation project" type.

## Current coupling (verified 2026-08-11)

`is_aggregation` (persisted, `services/project_state.py:623`) gates exactly **three** live sites:

1. `apply_aggregation_overrides` early-return (`services/aggregation_authoritative.py:396`);
2. merge dialog silent no-op (`ui/aggregation_merge_card.py:590`);
3. merge button visibility (`ui/pipeline_builder/pipeline_roster.py:927` →
   `_build_aggregation_merge_btn` :1496).

Everything else is already type-agnostic: the resolver's merged-sources candidate injection is
unconditional (`services/path_resolution_service.py:571`, whose own comment calls the flag "a
single point of failure"), `services/aggregation_discovery.py` is pure, `drivers/subtomo_merge.py`
is pure with strict optics validation, and the `ProjectState.active_merge*()` accessors are
type-free.

Real coupling is to the **UI layer**: `ui/aggregation_merge_card.py` (1007 lines) reaches
`current_project_state()` in 12+ places, has module-global cross-tab leaks (`_DIALOG_REFS` :669,
`_registry_expanded` :869, `_pending_save_task` in `_persist_state` :89-105 — documented bug in
`docs/PICKS_FILTER_AGGREGATION_ROADMAP.md:196-199`), no `SingleFlight` on the open handler or async
expanders, and full-rebuild rendering.

## Two-commit discipline

**Commit 1 — behavior-preserving move (zero logic change; py_compile is real verification here):**

- New `services/aggregation/` package ← `services/aggregation_discovery.py`,
  `services/aggregation_authoritative.py` (keep-and-relocate per D-6 — the authoritative-handle /
  GateReport half is built and CLI-verified, not dead weight).
- `ui/aggregation/merge_card.py` ← `ui/aggregation_merge_card.py`.
- `drivers/subtomo_merge.py` **stays in `drivers/`** — it is node-run code.
- Update call sites (`ui/pipeline_builder/pipeline_builder_panel.py:341,594`, roster, backend) —
  mind the `as X` re-export rule from the F401 boot-break lesson if shims are used.

**Commit 2 — de-global + de-flag:**

- Replace the module globals with a per-dialog state object (fixes the cross-tab leak);
  `SingleFlight` on merge submit and the open handler; thread `ProjectState` explicitly instead of
  the 12+ `current_project_state()` reaches.
- Wire `GateReport` / `extract_authoritative_pending` (`backend.py:368-380, ~529`) into the merge
  card as the pre-flight — this absorbs `docs/LIST_EXTRACTION_AND_AGGREGATION.md` §8.9 steps 4–7.
- **P5 — delete `is_aggregation`**: remove the field (:623; tolerate-and-ignore on load for old
  projects), the `apply_aggregation_overrides` gate (`aggregation_authoritative.py:396` — the
  function then runs only when explicitly invoked from the merge card / `_set_active_merge`, no
  longer on every workspace render), the dialog no-op (:590), and open the merge button to all
  projects — moved from the sidebar to its natural home on the PARTICLES phase header
  (`pipeline_roster.py:308-310`), next to import-tomograms and S1's new-species button.
- Landing page: delete the aggregation toggle (`ui/data_import_panel.py:1229-1268`) — with S1
  having removed the particle-only toggle, project creation has **no** type switches left.
- Cleanup: `docs/PICKS_FILTER_AGGREGATION_ROADMAP.md:215,255` reference the already-deleted
  `merge_panel_component.py` — fix the stale citations while in there.

## Note on `apply_aggregation_overrides` semantics after de-flag

Today it self-heals on every workspace render (`pipeline_builder_panel.py:596`) but only in
flagged projects. After the flag dies, keep it **invocation-scoped** (merge card actions), not
render-scoped — a regular project with an active merge opted into the wiring by creating the merge;
running a mutator on every render for all projects would be new, riskier behavior.

## Verification (user, runtime)

1. Regression: reproduce an old aggregation-project merge on a regular project — same
   `MergedSources/<slug>/` output, same overrides written.
2. Two browser tabs on the same project: selector/registry state no longer cross-talks.
3. GateReport pre-flight shows BLOCKED/stale lists before merge.
4. `grep -rn is_aggregation` → 0 hits (modulo tolerate-on-load shim); old aggregation projects
   still open.
