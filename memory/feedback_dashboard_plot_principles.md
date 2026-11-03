---
name: Dashboard plot principles (cryo-ET diagnostics)
description: Plot conventions for the per-TS dashboard analytics — when to use markers vs lines, data validation, hover content, tooltip explainers
type: feedback
originSessionId: 90bd9c4a-9d17-41fe-875c-80baba426451
---
When building per-tilt or per-stage diagnostic plots in the dashboard:

- **Don't connect discrete datapoints with lines — including for "smoothly varying" per-tilt metrics.** Per-tilt CTF metrics (defocus U/V, astigmatism, defocus angle) AND per-tilt alignment outputs (refined X/Y shift, tilt-axis residuals, Z-rot) are all per-tilt estimates and should be `mode: markers` only. The argument "but shifts vary smoothly across tilt order" is wrong — outliers turn into ugly zigzag and the trend is already visible from marker density. The user explicitly corrected this on the alignment refined-shift plot; don't re-introduce lines anywhere on per-tilt scatter (only accumulated/cumulative quantities are candidates for lines, and even those default to markers unless there's a strong reason).
- **Validate data before plotting.** WarpTools-exported RELION stars contain placeholder `1e-6` values for `rlnAccumMotion{Total,Early,Late}`, `rlnCtfMaxResolution`, and sometimes `None` for `rlnCtfFigureOfMerit`. A plot that's all-zeros or all-None is worse than no plot — detect these and skip with a note explaining "not exported by WarpTools." Use a magnitude threshold (~1e-3) rather than checking literal zero.
- **Hover should include tilt identity** — frame basename (or at minimum tilt index #N) in addition to the metric value. The angle alone is ambiguous when tilts share angles or there are interleaved acquisitions.
- **Tooltip-explain non-obvious metrics** on the plot label itself. Domain newcomers won't know what Defocus U vs V means, what astigmatism implies, or why CTF max-resolution matters. One-line explainer per metric.
- **Don't overengineer summaries.** A compact "39/41 kept, 2 dropped: angle X° / Y°" stat strip + drop list is often better than a per-tilt scatter that just visualizes "1 or 0".

**Why:** User explicitly called out tangled-line plots, flat/empty plots, and missing hover context as the dashboard's main pain points after the first analytics pass.

**How to apply:** Apply at every per-tilt plot site. Before plotting, gate on `_is_meaningful_series(values)`. Always include `customdata=[[idx, frame_basename], ...]` in the trace. For each new metric label, add a `.tooltip(...)` with a one-line explainer.
