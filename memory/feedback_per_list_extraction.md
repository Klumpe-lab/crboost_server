---
name: feedback_per_list_extraction
description: "Merge/downstream for manual+auto picks = per-list extraction, status-flagged, user-triggered — not auto, not tedious"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7f3f4214-f86c-4b6e-ac49-4c9ce013753d
---

When asked how the curation workbench's `_combined`/merge should feed downstream (manual picks are raw
coordinates, never subtomo-extracted, so they can't reach refinement without extraction), the user rejected
BOTH a fully-automatic merge→re-extract AND a manual/tedious flow. The decided model (2026-06-08):

- **Per-list extraction, status-flagged, USER-triggered per list.** Each pick list shows a badge —
  "Extracted" vs "needs (re-)extraction / picks added" — and the user clicks to (re-)extract THAT list. No
  throwaway re-extraction fires on every merge; no hunting for a CLI.
- **Subtomo extraction is scoped PER LIST** (by path/dir/filename), so auto/manual/merged extract
  independently and don't overwrite each other. The authoritative list's extraction output is the canonical
  optimisation_set the downstream resolver forwards (zero driver changes).
- **Dovetails with the co-located per-(species,tomo) lists directory** (`Curation/<species>/<tomo>/<slug>/`)
  so the separate lists can be inspected separately on one tomogram "without polluting the vision" (the
  canvas). That dir + the badges are the LATER UI.

**Why:** the user wants control over when expensive extraction runs (it's not free), while keeping the common
path one-click. Status flags make staleness legible instead of silently re-running or silently going stale.

**How to apply:** model the extraction state as DERIVED, never a stored boolean (`PickList.extraction_state()`
from `extracted_path`/`extracted_count`/`extracted_at` — stale-flag-trap discipline, see
[[feedback_inmemory_state_authoritative]]). Build order the user set: state/flags → merge + radius dedup →
per-list (re)extraction wiring → badge + lists-dir UI. Full context in
`services/visualization/ARTIAX_BRIDGE_PLAN.md` "## SESSION 2026-06-08"; project arc in [[project_artiax_bridge]].
