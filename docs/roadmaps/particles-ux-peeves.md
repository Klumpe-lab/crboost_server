# Particles / Journey / Species UX peeves — living triage

**What this is:** the intake + triage sheet for the maintainer's walkthrough of the Journey panels,
the dashboards, the Species/Template Workbench view and the job roster (queue). Raw peeves are
written by the maintainer in `q_denovo_picking_interface.md` (repo root, "Small peeves" and below);
each one gets a row here with an id, a category and a home. Cosmetic items are executed **after** the
surface they target has been restructured by roadmaps 08–11, so we don't polish things that move.

## Rubric

| Category | Meaning | Where it goes |
|---|---|---|
| **bug** | supposed to work today and doesn't | fix immediately (usually 08-S0 or a one-off commit) |
| **structural** | changes the model, where a concept lives (page/module), or a data flow | an 08–12 stage (cross-referenced) |
| **cosmetic** | chrome, wording, spacing, sizes, colors, ordering — no model/flow change | this sheet's per-surface checklist, batched per surface |

Conventions from memory that every cosmetic fix must respect: compact `cb-*` chrome, no Quasar/material
tabs or chunky buttons, slabs LEFT of galleries, tiny high-contrast pick dots, CSS animations not
server timers, FingerprintedView/SingleFlight for anything timer- or dialog-driven.

## Triage table

| id | surface | peeve (short) | category | home | status |
|---|---|---|---|---|---|
| P-01 | Journey / roster / workbench | changing a species' overlay color must change it everywhere (job-row chip, journey tabs, strip, workbench strip) | bug | 08-S0 (identity in signatures) | code-complete 2026-08-16, PENDING RUNTIME |
| P-02 | Workbench header | notes / Ø / symmetry inputs trigger a full project save on every keystroke | cosmetic (perf) | 10-S3 (identity editor: `debounce_s` + input debounce) | scoped |
| P-03 | Job roster / add dialog | "Template Extract" label — it picks candidates from TM scores, cuts nothing | bug (naming) | 08-S1 | code-complete 2026-08-16 ("Pick candidates") |
| P-04 | Job tabs (TM / pick / subtomo) | the same read-only species card ×3 clogs the parameter form | structural | 08-S1 | code-complete 2026-08-16 (one species line; card deleted) |
| P-05 | Workbench | de-novo species created from the roster "+" not shown until reload | bug | 08-S0 | code-complete 2026-08-16, PENDING RUNTIME |
| P-06 | Any | species created from roster/workbench may not persist across restart (no dirty mark) | bug | 08-S0 | code-complete 2026-08-16, PENDING RUNTIME |
| P-07 | Journey › Particles | ArtiaX saves only auto-ingest while the Journey is open on that tomo | structural | 09-S4 (watcher) | scoped |
| P-08 | Journey › Particles | curation control center / import / merge / extraction / dedup / auth radio crowd the per-tomo view | structural | 11-S3 (declutter) after 11-S2 | scoped |
| P-09 | Job tabs (pick candidates) | `array_throttle` rendered twice — in the plugin's Advanced group AND the SLURM Resources section (TM / subtomo had the same duplicate; fixed in 08-S1 by honoring the ctx `exclude`) | cosmetic | Job tabs checklist (one-line delete in `candidate_extract.py` Advanced) | open |

Add rows as the walkthrough produces them; keep the peeve text short and put the long form in
`q_denovo_picking_interface.md`.

## Per-surface cosmetic checklists (filled from the table)

### Journey (per-TS dashboard)
- (none yet beyond P-08's structural move)

### Species page / Template Workbench
- P-02 debounce

### Job tabs
- P-09 drop the explicit `array_throttle` field from the pick-candidates Advanced group (SLURM section owns it)

### Roster / queue
- (pending walkthrough)

## Log

- 2026-08-16 — sheet created with the peeves surfaced by the species-registry scoping (P-01…P-08).
- 2026-08-16 — 08-S0 + 08-S1 code-complete: P-01/P-05/P-06 (S0, pending runtime), P-03/P-04 (S1) moved to
  code-complete; P-09 added (pick-candidates `array_throttle` duplicate — TM/subtomo variants fixed in S1).
