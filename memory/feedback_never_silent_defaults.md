---
name: never-silent-defaults-surface-in-ui
description: "Never invent \"reasonable defaults\" or fail silently for missing/ambiguous/multi-option params; surface them as subtle UI tooltips/red markers/WIP badges so the user can supply or override"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: a3c9f6ea-79b0-4b08-bc86-86a034def341
---

When a parameter is missing, ambiguous, or admits several viable interpretations, do NOT pick a "reasonable default" and do NOT swallow it in a silent fallback. Surface it in the UI where the value is used — a subtle tooltip, a red marker, or a "WIP / unverified" badge — so the user sees the open decision while running the app and can supply or override it.

**Why:** Inventing a value hides a decision the domain expert needs to make; silent fallbacks (e.g. apix → 1.0 when an MRC header has no voxel_size) produce wrong-but-plausible results that are painful to trace. The user exercises the UI specifically to discover which decisions remain open — making them visible turns the app into a checklist of open questions, which is the point.

**How to apply:** Subtle, not noisy — only where a real gap exists, never litter the UI. Tooltip for "unverified/WIP"; red marker or warning for a missing-or-guessed load-bearing value, always overridable. Never `except: return <default>` for a semantically load-bearing value — raise or visibly flag. Recorded project-wide in CLAUDE.md ("Surfacing uncertainty") and in PARTICLE_PROJECT_ROADMAP.md. Related: [[project_pixel_arithmetic_pain_point.md]] (pixel/binning is the #1 bad-picks source), [[project_particle_project_roadmap.md]].
