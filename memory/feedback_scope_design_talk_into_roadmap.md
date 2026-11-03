---
name: feedback-scope-design-talk-into-roadmap
description: When the user lays out a feature vision ("the way I see it…", "let's work on this first"), scope it into the roadmap — build only on an explicit build/implement
metadata:
  type: feedback
---

A user's design description is a request to scope, not to build, unless they explicitly say build/implement. On
2026-10-01 (roadmap 06, the Tilts tab + per-tomogram tilt mosaic) I started reading code to implement after "Let's work
on this first"; the user stopped it: "dont build anything, just scope this all into the roadmap please". They had said
earlier in the session that they would pick the design up next session.

**Why:** the user designs in conversation and implements in fresh sessions (context budget, commit-per-chunk flow);
starting a build mid-design burns context and preempts their decisions.
**How to apply:** turn the description into a concrete spec in the active roadmap (where, data, rendering, files,
not-in-scope, user check), with code anchors verified; ask before building. Related: [[feedback-write-roadmaps-directly]].
