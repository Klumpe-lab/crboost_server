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
| P-02 | Workbench header | notes / Ø / symmetry inputs trigger a full project save on every keystroke | cosmetic (perf) | 10-S3 (identity editor: `debounce_s` + input debounce) | code-complete 2026-08-16 (Overview identity editor: `debounce=400` inputs + `save_project(debounce_s=1.0)`), PENDING RUNTIME |
| P-03 | Job roster / add dialog | "Template Extract" label — it picks candidates from TM scores, cuts nothing | bug (naming) | 08-S1 | code-complete 2026-08-16 ("Pick candidates") |
| P-04 | Job tabs (TM / pick / subtomo) | the same read-only species card ×3 clogs the parameter form | structural | 08-S1 | code-complete 2026-08-16 (one species line; card deleted) |
| P-05 | Workbench | de-novo species created from the roster "+" not shown until reload | bug | 08-S0 | code-complete 2026-08-16, PENDING RUNTIME |
| P-06 | Any | species created from roster/workbench may not persist across restart (no dirty mark) | bug | 08-S0 | code-complete 2026-08-16, PENDING RUNTIME |
| P-07 | Journey › Particles | ArtiaX saves only auto-ingest while the Journey is open on that tomo | structural | 09-S4 (watcher) | code-complete 2026-08-16 (`CurationWatcher` replaces the Journey prescan), PENDING RUNTIME |
| P-08 | Journey › Particles | curation control center / import / merge / extraction / dedup / auth radio crowd the per-tomo view | structural | 11-S3 (declutter) after 11-S2 | unblocked 2026-08-17 — 11-S2 landed the Species-page home for all of them |
| P-09 | Job tabs (pick candidates) | `array_throttle` rendered twice — in the plugin's Advanced group AND the SLURM Resources section (TM / subtomo had the same duplicate; fixed in 08-S1 by honoring the ctx `exclude`) | cosmetic | Job tabs checklist (one-line delete in `candidate_extract.py` Advanced) | code-complete 2026-08-18 |
| P-10 | Species creation | one text field only — no Ø / symmetry / notes, and no way to bind a template or mask upfront | structural | picking_ui/01-S2 | code-complete 2026-08-18, PENDING RUNTIME |
| P-11 | Nav sidebar | the species registry icon is a laboratory flask (`vial.svg`) | cosmetic | picking_ui/01-S0 | code-complete 2026-08-18, PENDING RUNTIME |
| P-12 | Job tabs (TM) | the boxed "Template, mask & search symmetry" card clashes with the parameter form; needs a guideline for every species-derived field | cosmetic (guideline) | picking_ui/02 + `picking_ui/00-overview.md` §guideline | code-complete 2026-08-18, PENDING RUNTIME |
| P-13 | Species › Overview | identity fields oversized inside a blue-stripe card; Ø clipped and mislabelled; "sym" not obviously a dropdown; notes single-line | cosmetic | picking_ui/03-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-14 | Species › Overview | the bound template / mask are not shown — no full paths, no copy, no link to Templates & masks | structural | picking_ui/03-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-15 | Species › Overview | STATUS is a kitchen sink; the counts that matter belong on the tab strip | structural | picking_ui/04-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-16 | Species › Overview | the "gate" chip is confusing (the concept stays; the chip goes) | cosmetic (deletion) | picking_ui/04-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-17 | Species › Overview | raw instance-id pills (`tmExtractCand`, `subtomoExtraction`) duplicate the Jobs tab, worse | cosmetic (deletion) | picking_ui/04-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-18 | Species › Overview | extraction geometry is a chip; needs its own panel that also prefills new subtomo jobs | structural | picking_ui/04-S2 | code-complete 2026-08-18, PENDING RUNTIME |
| P-19 | Species › Overview | pixel/binning sanity is a project × binning fact, not a species fact | structural (deletion) | picking_ui/04-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-20 | Templates & masks | one stacked wall of Templates then Masks; the right half of the page is empty | structural | picking_ui/05-S1 | code-complete 2026-08-18, PENDING RUNTIME |
| P-21 | Templates & masks | cards truncate three of their five lines; selection is a 3 px border nobody sees | structural | picking_ui/05-S2 | code-complete 2026-08-18, PENDING RUNTIME |
| P-22 | Templates & masks | Quasar tabs + bare Quasar fields = five font sizes in a module that documents three; the tabs are bigger than the section title above them | cosmetic | picking_ui/06 (a)(b) | code-complete 2026-08-18, PENDING RUNTIME |
| P-23 | Templates & masks | Masks have no stated SOURCE — the derived-from fact is filed as a subtitle | cosmetic | picking_ui/06 (d) | code-complete 2026-08-18, PENDING RUNTIME |
| P-24 | Templates & masks | import button pinned to the page edge; Edit Current's three actions are visually inseparable | cosmetic | picking_ui/06 (c)(e) | code-complete 2026-08-18, PENDING RUNTIME |
| P-25 | Cross-cutting | cross-project particle registry: deferred by the maintainer mid-scoping; tier split + edge rules recorded instead | structural (deferred) | picking_ui/07 | scoped 2026-08-18 |

Add rows as the walkthrough produces them; keep the peeve text short and put the long form in
`q_denovo_picking_interface.md`.

## Per-surface cosmetic checklists (filled from the table)

The 2026-08-18 walkthrough produced enough per-surface work that the checklists below are now
**pointers into `docs/roadmaps/picking_ui/`** rather than loose bullets — each of those roadmaps is a
committable chunk with its own runtime checklist, which is what the maintainer asked for.

### Journey (per-TS dashboard)
- (none yet beyond P-08's structural move)

### Species page — Overview
- P-13 identity block · P-14 template/mask bindings → **picking_ui/03**
- P-15 counts to the tab strip · P-16 gate chip · P-17 instance-id pills · P-19 pixel sanity ·
  P-18 extraction geometry panel → **picking_ui/04**

### Species page — Templates & masks (the mounted workbench)
- P-20 two columns · P-21 cards → table rows → **picking_ui/05**
- P-22 font scale / Quasar tabs · P-23 mask SOURCE · P-24 import button + Edit Current groups →
  **picking_ui/06**
- P-02 debounce — done in 10-S3 (identity editor)

### Species creation / nav
- P-11 nav icon · P-10 the creation form → **picking_ui/01**

### Job tabs
- P-09 drop the explicit `array_throttle` field from the pick-candidates Advanced group (SLURM section
  owns it) — **done 2026-08-18**
- P-12 species-derived fields follow the guideline in `picking_ui/00-overview.md` → **picking_ui/02**

### Roster / queue
- (still pending — the 2026-08-18 walkthrough covered Particles / Species / job tabs, not the roster)

## Log

- 2026-08-16 — sheet created with the peeves surfaced by the species-registry scoping (P-01…P-08).
- 2026-08-16 — 08-S0 + 08-S1 code-complete: P-01/P-05/P-06 (S0, pending runtime), P-03/P-04 (S1) moved to
  code-complete; P-09 added (pick-candidates `array_throttle` duplicate — TM/subtomo variants fixed in S1).
- 2026-08-16 — 10-S3 code-complete: P-02 (per-keystroke saves) closed by the Overview tab's identity
  editor (pending runtime).
- 2026-08-17 — row correction: P-07 had been left at "scoped" although 09-S4 landed the watcher the
  same day; it is code-complete (pending runtime). P-08 marked unblocked: 11-S2 gives every crowding
  affordance a home on the Species page, so 11-S3 can delete them from the Journey.
- 2026-08-17 — still ONE raw peeve intake so far. The walkthrough (Journey / Species / roster →
  `q_denovo_picking_interface.md`) has not happened; the cosmetic checklists stay near-empty until it
  does, and the cosmetics batch waits for 11-S3/S4 to stop moving the surfaces.
- 2026-08-18 — P-09 closed (the explicit field is gone from `ui/job_plugins/candidate_extract.py`; a
  comment there names the SLURM section as the owner so it does not come back). This plugin renders
  every field by hand rather than through `render_default_params`, which is why the ctx `exclude` that
  fixed the TM/subtomo variants in 08-S1 never reached it. The intake is still the ONE original peeve
  — the walkthrough remains owed, and it is the only thing gating the cosmetic batches.
- 2026-08-18 — **the walkthrough happened** (Particles button / Species registry / Overview /
  Templates & masks / the TM job tab). P-10…P-25 added. It is large enough that the cosmetic batches
  are no longer a bullet list on this sheet: they became `docs/roadmaps/picking_ui/` (00 index +
  guideline, 01 entry & creation, 02 job-tab bindings, 03 Overview identity & bindings, 04 Overview
  declutter, 05 workbench two columns + tables, 06 workbench form chrome, 07 the deferred
  cross-project registry scoping). The old "cosmetics wait for 11-S3/S4 to stop moving the surfaces"
  hold is lifted — those stages are code-complete, so the surfaces these peeves target are stable.
  Roster / queue peeves remain un-collected.
- 2026-08-18 (later, same session) — **P-10…P-24 are code-complete**: picking_ui 01–06 were all built
  the same day the walkthrough scoped them (stage records in each roadmap file). P-25 stays deferred by
  the maintainer's own decision. Every one of them is PENDING RUNTIME. Still uncollected: the roster /
  queue peeves, the Picks / Curation / Jobs tab peeves, "how de-novo picking actually feels" once the
  maintainer has run the jobs, and the three items in `q_important_ui_fixes.md` (beam-induced-motion
  connected graph, bring back the tomo-recon gallery with Journey links, landing-page data-identification
  jank) — none of those three has a row here yet.
