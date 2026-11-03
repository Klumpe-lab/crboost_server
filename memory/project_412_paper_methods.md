---
name: project_412_paper_methods
description: "DAV paper (2026-10-22): methods provenance in docs/reports/412-capsid/412-methods-provenance.md; draft methods named wrong tools; 412_aggregation dir missing from crboost_data"
metadata:
  node_type: memory
  type: project
  originSessionId: e5935b42-937b-4a4d-85bd-17c9317dc1cb
  modified: 2026-10-22T07:20:23.219Z
---

The lab is writing the paper (2026-10-22); the user said the mystery particle is DAV, but nothing in the repo says which capsid (412 ~50 nm or the ~30 nm one) that is — unconfirmed. Provenance of the May–July
run (Warp 2.0.0dev36, AreTomo2 via Warp, pytom-match-pick 0.10.0, RELION 5.0.1; 112 → 94 particles, ~35 Å
FSC cliff) is in `docs/reports/412-capsid/412-methods-provenance.md`, with a rewritten methods paragraph.

**Why:** the draft methods paragraph cited MotionCor2/CTFFIND4/Warp 1.0.9 and sphere templates, none of which
processed this particle.

**How to apply:** for any further paper question, start from that doc. Still open: `412_aggregation` is no
longer under `/groups/klumpe/crboost_data` (needed for per-source counts), AreTomo2 version is from the image
name only, ArtiaX use is outside the record. Related: [[project_412_sta_refinement_playbook]],
[[project_agg412_reprocessing_2026-10-04]].

**2026-10-22 decision:** paper uses DAV = the ~50 nm capsid = the PI's v1 `412/try1/Class3D/job121` (23 particles,
21 tomograms, RELION 5.0.0); copia = `crboost_data/copia-tutorial-20261015-1551` (1,016 particles, Warp
2.0.0dev41, RELION 5.0.1); the ~30 nm capsid (galbut virus) is dropped. Table in §7 of the provenance doc.
Later the same day: the PI recalled "something like 50" DAV particles. His whole `Processing/412/` has averages in only
`try1` (~50 nm, 23 final, max 32) and `second_particle/old_version` (~33–41 nm, 58 picks → 210, one tomogram,
19.84 Å gold-standard). §8 of the provenance doc. Which map is DAV is still the PI's call.
**Correction (same day):** copia for the paper = the PI's `Processing/Copia/try2` (76 particles, one tomogram
Position_11_2, same 20251017 session as try1), NOT copia-tutorial-20261015-1551 (EMPIAR tutorial, 2.95 Å/px,
wrong pick). EER = 225 fractions/tilt, 32 per frame → 7 frames, ~0.46 e⁻/Å² per frame (mdoc 3.2/tilt). Before
answering "which project", check acquisition site/pixel size against the paper's data, not "most processed".
