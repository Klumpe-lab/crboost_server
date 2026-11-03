---
name: feedback-rules-mark-not-drop
description: "Rule-based tilt classifications (dark/blank exposure etc.) are markers, not drops; every surface that marks one explains the rule and says it drops nothing"
metadata:
  node_type: memory
  type: feedback
  originSessionId: daa852b6-9f30-4629-9a4e-056dffc1acbc
  modified: 2026-10-02T08:40:55.678Z
---

The maintainer is hesitant about dropping data because of a fixed rule (e.g. "mdoc counts < 10 % of the series
median = dark"). Such rules may classify and mark, but the drop decision stays with a human (the review / Approve).

**Why:** said 2026-10-02 when deciding roadmap 26 §10.3: "a little hesitant about dropping things based on some
'rule' … please mark/annotate with tooltips which explain this decision or flag amply in the interface".

**How to apply:** wherever a rule-derived marker appears (card token, border, header count, caption, chart), give it a
tooltip stating the rule, the measured value against the cut, and that nothing is dropped by it; add a legend line
where several markers share a view. One shared explanation text, built from the constants. Applies to roadmap 22's
blank-exposure exclusion too: surface it as a pre-label the human confirms rather than a silent drop. Related:
[[never-silent-defaults-surface-in-ui]], [[project-tilt-views-roadmap26]].
