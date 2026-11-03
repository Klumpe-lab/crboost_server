---
name: project-tilt-views-roadmap26
description: "Roadmap 26 (tilt views + Journey) CLOSED 2026-10-15 — maintainer: works well enough; F + G on main; G runtime pass and V2 dropped; doc in docs/roadmaps/completed/"
metadata:
  node_type: memory
  type: project
  originSessionId: 48130d81-fbd6-4012-9b27-042cf00ab1c6
  modified: 2026-10-04T12:27:26.643Z
---

**CLOSED 2026-10-15** (maintainer: the tilt views work well enough). Doc moved to `docs/roadmaps/completed/roadmap_26-tilt-views-and-journey.md`; G's runtime pass and V2 are dropped, not deferred.

2026-10-02: roadmap 06 §13.1–13.8 moved verbatim to `docs/roadmaps/roadmap_26-tilt-views-and-journey.md` (appendix A,
A.n = 06 §13.n); 06 keeps a pointer + §13.9.

§10 decided 2026-10-02: tilt N = tilt_index + 1; "in the tomogram" = in alignment's per_frame output; dark = mdoc mean
counts < 10 % (blank < 1 %) of series median, display-only; V4 (review moves into Tilts view) = yes, after V2.
Maintainer wary of rule-based drops → see [[feedback-rules-mark-not-drop]].

V1 built 2026-10-02 (tilt_state.py, tilt_previews.py, tomo_gallery caption/strips). Stage F built 2026-10-02 as F1–F5
(Approve reads the registry, DL auto, row dropdown + parked look, TiltGallery, job page rewrite). F2–F5 landed as ONE
whole-file commit after a parallel session's whole-file adds broke per-hunk patches — hand whole-file commits.

**2026-10-04 — F in use** (DL review on 8 projects of one dataset; page on a 126-series, 5007-tilt project).
Confirmed from code for the user: Approve stamps model-flagged and hand-labelled tilts identically (effective_label →
stamp_verdict, no source kept); alignment's snapshot drops them, tsCtf/recon inherit, star writers skip them.

**Stage G scoped 2026-10-04** (roadmap 26 §9 G1–G5, §10.15–10.23, §8.9–8.10): red border = excluded
(any source, pending or approved); dark / not-in-alignment / outlier = translucent icons bottom-left with hover;
click toggles, magnifier zooms; popover = table, no shorthands; "P(bad)" → "Confidence score" (tooltip "Confidence that
this particular tilt should be excluded"); no "you/your" ("manual label"); metrics menu of checkboxes; icons-only legend;
group by position = one box per position, beams as slim lines, no indent; page = stats first, small uniform linear
charts collapsed; row = method + when DL always shown, threshold editable only for Apply automatically, no Review →;
"Approve labels"; DL review auto-predicts only when the filter is in a Run (hold names it). Outlier rule marks 31 %
of tilts (1555/5007) → outliers mark only ticked metrics until G5's per-metric breakdown names the culprit.
**G BUILT 2026-10-04 (same session, user said "just build")**, ruff-clean, NOT RUN; build records under each G subsection.
Deviation: "not in alignment's output" is INDIGO everywhere (grey fails the dataviz chroma floor); chart palette
#e34948 excluded / #4a3aa7 out / #eda100 dark / #2a78d6 kept. Handed as 3 commits: A = G1+G2+owned_timer (row, reactive,
driver, jobs/tilt_filter, backend, orchestrator, runner, image_processor); B = G3–G5 (tilt_state, tilt_previews, css,
panel, figures, dashboard_data, tomo_dashboard_dialog); C = docs. Next: G runtime pass, then V2. See [[reference-nicegui-timer-parent-slot]].
Related: [[project-dl-tilt-filter]], [[project-blank-tilt-rays]], [[project-tomo-gallery-view]].
