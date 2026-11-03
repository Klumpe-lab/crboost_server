---
name: Template & Mask First-Class Objects refactor — v2 plan
description: Reframed schema-cleanup as making templates/masks first-class objects with metadata visible at every UI surface, plus replacing the broken molstar workbench viewer
type: project
originSessionId: 465fa000-162c-4ae7-abde-5f7927a77ab6
---
The v1 plan (`TEMPLATE_REGISTRY_REFACTOR_PLAN.md`) framed this as schema
cleanup. v2 (`TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md`, drafted 2026-05-10)
supersedes it and reframes around two concrete pains:

1. Templates dissolve into bare filepaths once the workbench closes —
   no metadata visible at TM/extract job-config tabs, pixel-sanity
   panel, or per-tomo dashboard. Box-vs-binning mismatches go undetected
   pre-flight and are forensically blind post-hoc.
2. The molstar viewer in `ui/template_workbench.py:73` is broken and
   visually outdated; first-class metadata UI needs a working viewer
   anyway.

**Why:** User stated (2026-05-10) they don't want to overengineer schema
cleanup — they want concrete UX wins. v1's schema work is the *enabler*
for first-class template UI, not the goal.

**Decisions locked in v2 (don't relitigate):**
- Singular `species.template: Optional[ParticleTemplate]`, NOT a list.
  Defer multi-template until a concrete second-template case appears.
- No persisted MRC header cache (`pixel_size_ang`, `box_px`). Read
  on demand; per-process LRU keyed by `(path, mtime)` in the viz layer.
- Mask is a sub-object of template (`template.mask`), not a sibling
  field on species.
- Polarity becomes a `Literal["white","black"]` field; filename
  convention becomes convenience, not state.
- Migration is staged across 3 PRs (additive land → UI flip → viewer
  + cleanup), NOT big-bang.
- Viewer replacement is in scope: NiceGUI + plotly heatmap triplet
  (XY/XZ/YZ with mask overlay), not three.js. Templates are 64–128 px
  so the "don't render volumes in browser" memory rule doesn't apply
  (that's about tomograms with missing-wedge streaks).
- Workbench becomes a pure editor; picker + sanity panel + species
  panel read its outputs.

**How to apply:**
- Read `TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md` for full scope, schema,
  UI surface map, and PR phasing. Don't re-derive from v1.
- When adding new dashboard analytics that touch template metadata,
  read the proposed v2 schema — don't add workarounds against the
  current ad-hoc layout.
- Land before adding more sanity rules to the §11 pixel panel —
  several rules become one-liners after PR 2.
- Estimated 3–4 days total across the 3 PRs.
