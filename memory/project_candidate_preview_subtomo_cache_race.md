---
name: candidate-preview-subtomo-cache-race
description: "Mass 'no subtomo match' on candidate cutouts is a stale-cache RACE (preview auto-kicks before SUBTOMO_EXTRACTION finishes), NOT a coord-matching bug — fixed 2026-05-20 by making the cutout cache subtomo-aware"
metadata: 
  node_type: memory
  type: project
  originSessionId: b08ecc18-77c5-4c49-9829-171c437affca
---

When a candidate-extract gallery shows most cutouts failing with **"no subtomo
match"** (e.g. 19/20 on a tomogram), the coordinate join is almost never the
problem. Diagnosed on `/groups/klumpe/crboost_data/agg_20260311_412_Grid3`
(species 412: candidate `External/job021` → subtomo `External/job022`):

- The join (`services/visualization/subtomo_link.py::_coord_key` on
  `rlnCenteredCoordinate{X,Y,Z}Angst`, keyed `(rlnTomoName, rounded-0.1Å)`) is
  **correct** — replaying it on the live star files matched **317/340 (93%)**,
  every tomogram 17–20/20.
- The failures were a **race + cache blind spot**: the candidate preview
  auto-kicks the moment `candidates.star` lands, but the SUBTOMO_EXTRACTION job
  is usually still running. Tomograms rendered before their `.mrcs` exist froze
  `cutout_index.json` at `n_ok=1` / 19× "no subtomo match". `candidates.star`
  is unchanged once the subtomo job finishes, and `is_output_stale` only
  watched `[candidates.star, tomograms.star]`, so nothing ever rebuilt them.
  Mtimes proved it: candidates 18:32 → stale cutouts 18:33 → subtomos 18:42 →
  healthy cutouts (rendered after 18:42) all 20/20.

**Fix (2026-05-20, `preview_orchestrator.py` + `subtomo_link.py`):** added
`subtomo_particles_stars()` and a `cache_cutout_stale` check — a cached atlas
older than any subtomo `particles.star` is treated as stale and rebuilt. It
self-heals on the next auto-kick / "Render missing" (force not required).
Validated on the 412 project: flags exactly the 3 race-frozen tomograms,
leaves the 14 healthy ones untouched.

**Triage rule:** before suspecting coord math on mass "no subtomo match",
compare `cutout_index.json` mtime vs the subtomo `particles.star` mtime. Note
**legitimately-partial** tomograms (17–19/20) are normal — those picks were
dropped by the subtomo extraction at the tomo edge / missing wedge, and
correctly do NOT rebuild.

Related: [[project_per_tomo_dashboard_arc]], [[project_per_ts_parallelization]],
[[project_pixel_arithmetic_pain_point]].

**2026-10-05, a second race:** `FileNotFoundError ... vis/preview/manifest.json.tmp -> manifest.json` failing an
"Auto-render previews" task = two concurrent `generate_candidate_previews` passes over one job sharing the fixed tmp
name. The auto-kick's dedup key (`render-previews:…:no-force`) never matched the "Render missing" button's
(`preview-render:…:force=False`), so a click during an auto-render ran a second pass. Fixed: per-write tmp name
(`.manifest.json.tmp<8hex>`) + the auto-kick uses the button's key. Force vs non-force can still overlap (by design).
