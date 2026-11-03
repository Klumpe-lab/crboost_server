---
name: feedback-asap-style-pass-minimal
description: "A quick UI style request (e.g. Journey look for slides) means visible styling only — no refactors or guideline sweeps of older code, even when a pasted roadmap lists them"
metadata:
  node_type: memory
  type: feedback
  originSessionId: 9ce94ce2-d485-43cc-92f4-992c105c92c0
  modified: 2026-10-17T12:32:35.155Z
---

When the user wants a look fixed fast ("asap", "for slides"), build only the visible styling and
stop. Don't extend into refactors (theme dataclasses threaded through builders, `color=None`
sweeps, font/size sweeps of pop-ups and shared classes) or apply code guidelines to older code, even
when a roadmap pasted from an earlier session lists them as part of the stage. The user often hasn't
read those roadmaps; the pasted brief is a handoff, not a reviewed spec.

**Why:** 2026-10-17, roadmap 35 (resizable views + quiet Journey): built S1–S4 with mapper/review
workflows; the user cut in: "please wrap it up, i don't want to refactor the whole … app right
now, i just needed the stylistic fixes in the journey to make some slides … if there is like real
refactoring suggestions and code guidelines in there that you are implementing over the older code
now -- that's too much."

**How to apply:** before a multi-stage build from a pasted roadmap, name the visible deliverable in
one line and ask whether the structural items are wanted now. Prefer CSS-scoped changes over Python
plumbing; skip review fan-outs for a style pass; give progress in the final message, not mid-turn.
Related: [[feedback_write_roadmaps_directly]], [[feedback_engage_concrete_goal]].
