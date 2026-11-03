---
name: star-files-untrusted
description: "Treat every star file RELION or Warp writes as untrusted, mutating input: check what you rely on at every read, report, never guess"
metadata:
  type: feedback
---

The maintainer (2026-10-29, accepting roadmap 40 D21 = a pick-uid column carried through extraction, Class 3D,
Refine 3D): be very careful with RELION / Warp / starfile outputs, treat them "like a mutating biohazard".

**Why:** tools rename, reorder, drop and regenerate columns and rows (Warp export regenerates rlnTomoParticleName,
relion_tomo_subtomo drops invisible particles and the drivers re-sort by tomogram; placeholders and skew already bit
us: [[warp-relion-star-placeholders]], [[warp-dev36-relion5-star-skew]]).

**How to apply:** any code that relies on a column or row correspondence in a tool-written star checks it on every
read (present, one value per row, unique where it is a key, values known), reports anything else in the UI or the
job log, and never falls back silently; a stand-in (e.g. the position join) says on screen that it stands in.
Related: [[project-sta-kitchen]], [[feedback-never-silent-defaults]].
