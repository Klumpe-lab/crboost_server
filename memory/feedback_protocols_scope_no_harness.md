---
name: feedback-protocols-scope-no-harness
description: Protocols are parameter shapes only — no evaluation/harness machinery, no invented vocabulary (blessed/banded/record/baseline/evaluate/verdict/reference project); spawn a regular project and run it the regular way
metadata:
  type: feedback
---

A "protocol" is just a way to record a bunch of parameters spread over jobs — the shape of a
pipeline — untethered from any particular project. From it the user spawns a REGULAR project and
runs it through the REGULAR pipeline (roster Run button). Nothing about results or evaluation
belongs in the feature; the user looks at the class3d resolution in the spawned project themselves.

**Why:** on 2026-09-04 the maintainer found roadmaps 14 + 16 had grown a regression harness
(frozen inputs, run.json verdicts, records, bless, bands, evaluate, baseline, INFRA, snapshots, two
CLIs, a six-state light) around a feature nobody asked for that shape — "you are going ballistic on
features and inventing your own terminology". Everything harness was deleted the same day; the
Protocols view UI (liked as a place to store protocols and read per-stage parameters) stayed.

**How to apply:** when asked for a feature, build the literal ask and stop; do not add
measurement, verdict or baseline machinery "so the first run is judged against something"; do not
coin vocabulary the user did not use (they will hear it as scope creep); keep the running of anything
as close to the regular pipeline as possible, because the regular pipeline is what is being tested.
Related: [[feedback-engage-concrete-goal]], [[feedback-dont-gate-stages-on-runtime]].

**2026-09-09 addendum — protocols are not a per-dataset parameter grid.** I proposed a `k3-serialem-stacks`
bundle to carry K3-specific job params (set_noflip, halfmap_frames 0, CTF range). Maintainer: "NO. protocols are
just for testing and should remain a pristine registry of what we already know … If the user doesn't like our
defaults — they change them themselves… protocols is not [a] place to multiply cartesian grids of parameters on
every dataset."
**How to apply:** a protocol is written only for a flow that has been run and is known good (copia). For a new
data source, what the DATA determines gets stamped at job init from persisted facts (e.g. `tomo_dimensions`
from ImageSize, halves off when the input has none); everything else keeps the code default and the user
edits the tab. Never author a protocol while prototyping.
