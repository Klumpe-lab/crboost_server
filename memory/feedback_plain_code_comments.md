---
name: feedback-plain-code-comments
description: "Code comments/docstrings must be plain present-tense rationale — no roadmap/stage/census/date/maintainer-decision refs, no history narration or monologue"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: d3b9dbd7-7531-47cf-902c-ab31c3db3e67
  modified: 2026-09-15T10:52:51.848Z
---

Never write process references into code comments or docstrings: no "roadmap NN", "S3", "11-S4", "D-8", "§2", "census #38", "(maintainer decision 2026-08-14)", "(deliberate)", incident dates, "finding H", pointers to docs/roadmaps/ or notes/. No history narration ("used to", "no longer", "the old X was replaced") and no internal monologue / spoonfeeding / stacked ALL-CAPS emphasis. A comment states plainly, in present tense, what is true now; rationale only where the decision needs explaining beyond the code's mechanics. Pointers to reference docs (non-roadmap) are fine.

**Why:** 2026-09-15 the user had the whole codebase scrubbed of this ("pollutes the comments and makes them unreliable, unreadable"), especially services/ and drivers/. Roadmap/stage labels rot and mean nothing to a reader of the code.

**How to apply:** planning vocabulary lives in docs/roadmaps/ only. When writing or editing any comment, write it as a senior engineer's note about the current code. Legacy handling is stated in present tense ("older project files carry X; ignored on load").
