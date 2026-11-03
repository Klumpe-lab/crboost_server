---
name: feedback-journey-dont-overcorrect
description: "Roadmap 35's quiet-Journey passes overshot; the user wants big filling charts, distinct series colours and some gray-blue, not grey minimalism"
metadata:
  node_type: memory
  type: feedback
  originSessionId: f87114c8-26aa-4367-a79e-3ecdaa2c1f46
  modified: 2026-10-17T21:13:03.672Z
---

2026-10-17, after roadmap 35 S3/S4 (passes A/B/C): "the last session went WAAAAY overboard on each one of these
points ... the plots/graphs are waaay too small now, the colors are too much dark and gray and the dots are pretty
hard to tell apart". Their earlier ask (standardize, remove colour, compact plots) was, in their words, "a bit too
strongly" put. They also wanted charts to fill the pane's width, a header that wraps instead of stacking into a
column when the Journey shares the screen, and "a little bit at least of gray blue" so the page is not discoloured.
Colour may go where it carries nothing (the Dataset pixel table's stage stripes).

**Why:** a style direction stated strongly gets applied to every token at once; the user judges the result by
readability (chart size, telling series apart), not by consistency.

**How to apply:** when asked to quiet or standardize a page, move each token part of the way and keep what aids
reading: charts at ~220 px that fill their row, two clearly different series hues (gray-blue + warm), 6 px markers.
Cut colour only where it encodes nothing. Related: [[feedback_ui_chrome_conventions]],
[[feedback_journey_layout_preferences]], [[feedback_asap_style_pass_minimal]].

2026-10-18: the maintainer kept the pulled-back C ("looks wonderful") and it is folded into the base
(`JOURNEY_THEME`, `.cb-journey` CSS; no style select). Next ask: Particles panel colours/spacing/layout/fonts
"too messy" — built as roadmap 35 S6, a temporary `layout` select (now / A toolbar / B sidebar / C ledger,
`ui/particles/layout_pass.py`). They kept A · toolbar (same day); folded as S7 (`_PARTICLES_CSS`, no select).
