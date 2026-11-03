---
name: feedback-fresh-projects-not-backfills
description: "Test new metadata/code on fresh projects re-run through the current code (copia protocol), not by migrating old ones; backfills are throwaway; never make users run CLI steps"
metadata:
  type: feedback
---

To try new registry fields or views, the maintainer re-runs test data through the server with the current code (the
copia protocol makes a fresh project). Backfill/migration tooling for old projects is a prototyping stopgap at most,
deleted as soon as fresh data exists; it is not a feature.

**Why:** said 2026-10-02 about `crboost_reingest.py --mdoc`: "fine as an intermediary solution for prototyping but we
will ditch it right away since it's really no problem for me to run new projects … That's what the copia protocol is
for", and earlier: "worried that we are starting to rely on some cli actions here".

**How to apply:** don't design features around backfilling old projects; don't add UI text that tells users to run a
shell command or stop the server. If old data lacks a field, the UI says "not recorded". Related:
[[project-tilt-views-roadmap26]], [[feedback-protocols-scope-no-harness]].
