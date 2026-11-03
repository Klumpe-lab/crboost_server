---
name: Visualization stack patterns and traps
description: Hard-won patterns for the per-tomogram dashboard — Plotly via dict, aspect-ratio containers, manifest versioning, the numpy mmap view trap.
type: feedback
originSessionId: 8bb3142b-c215-446c-a10e-3493578913ec
---
Patterns that have settled out of multiple iterations on the candidate-preview / per-tomogram dashboard. Don't relitigate; do follow.

**1. `ui.plotly()` accepts a dict.**
Pass `{"data": [...], "layout": {...}, "config": {...}}` directly — no `import plotly.graph_objects` needed and no plotly Python package dependency. NiceGUI's bundled JS handles rendering.
- Why: keeps Python deps small, avoids version drift, lets us serialize/cache figures as plain JSON.
- How to apply: build figure dicts in helper functions like `_build_xy_scatter_fig(...)`. Use `"type": "scattergl"` for >100 points to keep things smooth.

**2. Aspect-ratio CSS on the wrapper, not Plotly `scaleanchor`.**
`scaleanchor: "x"` forces a square *data area* inside whatever container, leaving 60% empty horizontal space when the container is wide. Instead set `aspect-ratio: <x_dim>/<y_dim>` on the wrapping `<div>` and let Plotly fill it.
- Why: Plotly's data-area sizing is the wrong primitive for "make this plot the right shape"; CSS is.
- How to apply: cap the ratio (`min(2.5, max(0.5, x/y))`) so a flat tomogram doesn't produce a 41:1 strip; add `min-height` only if the aspect-ratio result would otherwise produce a too-short box.

**3. Manifest version is the cache key.**
Each schema bump (`MANIFEST_VERSION = N` in `preview_orchestrator.py`) automatically invalidates prior caches because `prior_entries` only loads entries whose version matches.
- Why: lets us evolve the schema without writing migrations.
- How to apply: bump the version on any add/remove/rename of fields; old artifacts on disk are ignored, then garbage-collected naturally on next regen.

**4. mmap view trap (numpy).**
`np.asarray(m.data, dtype=np.float32)` returns a *view* of the mmap when the dtype already matches. The view is invalid the moment the `with mrcfile.mmap(...)` block closes — silently produces all-zero outputs downstream.
- Why: this trap cost most of one debugging session — `_f32.mrc` files are float32 so they hit the view path.
- How to apply: always `np.array(m.data, dtype=..., copy=True)` (or do all work inside the `with` block). This rule applies to anyone reading mrcfile data into a numpy array that outlives the `with`.

**5. NiceGUI 3.x: `ui.html()` requires `sanitize=False` for raw SVG/canvas.**
Without it, all HTML is sanitized → `<svg>`, `<canvas>`, `<button>` get stripped.
- How to apply: `ui.html(html_str, sanitize=False)`. Default-required keyword in NiceGUI 3.x.

**6. NiceGUI re-renders can wipe JS-set DOM attributes.**
If you set `img.src` or `svg.viewBox` from a `ui.run_javascript()` block, a Vue v-html re-render of the parent will wipe it back to the static HTML. Solution: bake dynamic attributes (img src, svg viewBox, etc.) into the HTML string passed to `ui.html(..., sanitize=False)` server-side, so they survive re-renders.

**7. Path-validator: tolerate string-typed project root keys.**
`_project_states.keys()` may be Path or str. `_is_under` in `main.py` accepts either; do not assume Path. AttributeError there → 500 (not 404), which is invisible in many dev-tools panels.
