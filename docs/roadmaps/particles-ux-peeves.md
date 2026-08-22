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
| P-26 | Species page | a species with ANY mask crashed the whole page — `TemplateMask.imported_from` does not exist (it is a `ParticleTemplate` field); and the rail refreshed AFTER the first tab built, so the crash presented as "the registry is empty" | bug | fixed in place 2026-08-19 (`overview_tab._mask_source`, `template_workbench._mask_note`, rail painted before any tab) | FIXED, needs a re-check |
| P-27 | Species › Overview | one undifferentiated wall of oversized fields — no named blocks, Quasar's 14 px inputs against 9 px labels | cosmetic | three `section_header` blocks (Species info · Template matching files · Extraction geometry) + `.cb-field` on every input | fixed 2026-08-19, needs a re-check |
| P-28 | Species › Overview, Templates & masks | the absolute path is drawn in full and eats the row; the filename never truncates | cosmetic | `render_path_link` draws the FILENAME only (ellipsised); the directory is a hover fact on the name and the copy button | fixed 2026-08-19, needs a re-check |
| P-29 | Templates & masks | the tables are crowded — text too large, rows too tall | cosmetic | 9 px row body / 10 px filename, tighter `--cb-tw-cols`, 22 px rows, row buttons no longer set row height | fixed 2026-08-19, needs a re-check |
| P-30 | Templates & masks | section titles ("SOURCE", "TEMPLATES") share a style with tool names and field labels, so nothing reads as containing anything | cosmetic | three ranks enforced: SECTION `section_header` 11 px mixed case + rule · TOOL `_TOOL_CLS` 10 px · FIELD `_LABEL_CLS` 9 px uppercase | fixed 2026-08-19, needs a re-check |
| P-31 | Species › Overview | the extraction-geometry panel is three orphan numbers — it never says which job it feeds | cosmetic (wording) | panel states it feeds the Subtomo extraction job and what the box is | fixed 2026-08-19, needs a re-check |
| P-32 | Nav / page copy | "Species" → "Particles registry" (page-naming copy only; entity copy stays "species") | cosmetic | picking_ui/08-S6 | scoped 2026-08-21 (6 sites remain; sidebar tooltip etc. already renamed) |
| P-33 | All inputs, app-wide | boxes huge + spinner arrows; conflicting legacy `.cb-field` in `static/main.css`; shell CSS channel stale; 8 raw `ui.number` sites | structural (CSS delivery) + cosmetic | picking_ui/08-S1/S2 | scoped 2026-08-21 — partly stale-build, 08-S0 rechecks first |
| P-34 | Templates & masks | gray-border overload (17 chrome sources censused) | cosmetic | picking_ui/08-S4 | scoped 2026-08-21 |
| P-35 | Templates & masks | source-mode switch jumps the molstar viewer — Edit-current height swing + unstacked viewer-mode switcher (NOT missing `_stacked_panels`) | bug (layout) | picking_ui/08-S5 | scoped 2026-08-21 |
| P-36 | Templates & masks | masks default should be "From template", not "Sphere" | already landed (`_MASK_TABS`, `template_workbench.py:151-154`) | picking_ui/08-S0 recheck | verify at runtime |
| P-37 | All buttons, app-wide | one button vocabulary — 108 raw label-buttons in 22 files → `house_button`; placement rule | cosmetic (sweep) | picking_ui/08-S3 | scoped 2026-08-21 (workbench text buttons already converted) |
| P-38 | Species › Picks + Curation | two overlapping tabs; 9 ArtiaX entry points; 2 import buttons; half-duplicated in Journey → ONE "Picks & curation" surface | structural | picking_ui/09 | scoped 2026-08-21 |
| P-39 | Picks surface | aggregation/merge controls leave the surface; roster icon → "Aggregate candidates" spawning a prepopulated job | structural (deferred) | picking_ui/12 | scoped 2026-08-22 (D1–D5, stages S0–S6) |
| P-40 | ChimeraX bridge | remote-driving controls ("save picks now", "load into session") = wrong route; scoped-launch + staging-inbox contract; napari pilot gated | structural (decision) | picking_ui/10 | scoped 2026-08-21, Model B adopted |
| P-41 | Journey gallery | 3dmod duplicated (peek block + bottom section + 2 hints) → one toolbelt icon with a popover | cosmetic | picking_ui/11-S3 | scoped 2026-08-21 |
| P-42 | Journey gallery | click = keep/drop by default → click = SELECT (Esc/click-away deselects); curation mode is an explicit toggle | structural (interaction) | picking_ui/11-S4 | scoped 2026-08-21 |
| P-43 | Journey gallery | hover info row (IDX · PX · …) superfluous — delete | cosmetic (deletion) | picking_ui/11-S3 | scoped 2026-08-21 |
| P-44 | Journey gallery | reference strip → thin collapsible, CLOSED by default; add best-4 anchors beside worst-4 | cosmetic | picking_ui/11-S2 | scoped 2026-08-21 |
| P-45 | Journey gallery | lists strip must sit ABOVE slabs + gallery so their tops align | structural (layout) | picking_ui/11-S2 | scoped 2026-08-21 |
| P-46 | Registry + Journey | full-page pick viewer (slabs + gallery, or markers-only when not extracted) opened from the merged tab; Journey keeps a slim display+filter mount; links both ways | structural | picking_ui/11-S1/S5/S6 (+09-S5) | scoped 2026-08-21 |
| P-47 | Curation control center | doesn't close on Esc (page-layout-parented dialog) | bug | picking_ui/10-S1 | scoped 2026-08-21 |
| P-48 | Curation session | passwordless VNC option requested | structural (security tradeoff) | picking_ui/10-S1 (config flag, default off) | scoped 2026-08-21 |

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
- 2026-08-19 — **first runtime finding, P-26.** Two call sites read `TemplateMask.imported_from`, which
  has never existed — masks carry `derived_from_template_id` + `method`, `imported_from` is a
  `ParticleTemplate` field. One is PRE-EXISTING (`overview_tab._render_provenance`, shipped in 10-S3 and
  already committed); the other was the same mistake copied into picking_ui 05-S2's mask row. It only
  fires once a species has at least one mask, which is why the arc's static gates and every earlier
  session missed it. Both now resolve provenance through the fields that exist and say so when the
  source template is gone. The amplifier is fixed separately: `SpeciesPage.build` refreshed the rail
  *after* selecting the first species, so any tab that raised during build took the species list with
  it — the rail is painted first now. **Lesson for the arc: `ruff` + `python -c "import main"` cannot
  see an attribute that does not exist on a pydantic model.** Sweeping every new `<model>.<attr>` read
  against the model definition found no others (ParticleSpecies, SpeciesOverview, ExtractionParams,
  ListRef, ParticleTemplate all clean).
- 2026-08-19 — **second walkthrough pass on the Species page (P-27…P-31), all fixed in place.** The
  one structural idea behind all five: **three type ranks, and a thing may only use the rank it is.**
  SECTION (`section_header`, 11 px semibold MIXED case + `section_rule`) names a block of the page;
  TOOL (10 px semibold, `_TOOL_CLS` in the workbench) names one tool that owns inputs; FIELD (9 px
  uppercase muted, `_LABEL_CLS`) labels exactly one input and heads a table column. They had all
  collapsed onto one 10 px uppercase style, which is why "SOURCE" and "apix" shouted equally.
  Two other decisions worth not re-deriving:
  **(a) `render_path_link` no longer draws the directory at all.** It draws the ellipsised filename;
  the absolute path lives in the hover on the name and on the copy button. The old row had the
  basename at `flex-shrink: 0` followed by a flexing path label, which is exactly why a long mask
  filename ran the width of the table.
  **(b) `.cb-field` diverged from `.cb-select` by one notch** (20 px box / 10 px text vs 24 / 11) as
  overrides after the shared block, so the job tabs' selects are untouched. `.cb-field` is used only
  by the Species page (workbench forms + the Overview's inputs, which until now were bare Quasar at
  ~14 px — the actual "everything is too big" complaint). If a third surface adopts it, check that
  the tighter scale suits it before adding the class.
- 2026-08-21 — **the promised second intake happened** (Picks/Curation tabs, ChimeraX control,
  gallery rework, chrome round 2 — `q_important_ui_fixes.md:9-55`). P-32…P-48 added; homed in the
  new wave-2 roadmaps `picking_ui/08–12`. Key finding while triaging: P-36 and the button/input
  halves of P-33/P-37 describe the pre-restart build — the fixes are already in the tree, so
  `picking_ui/08-S0` is a restart-and-recheck gate before any re-fixing. Entry-point inventory of
  record (9 ArtiaX launch points, 2 imports, 4 extract triggers) lives in `picking_ui/09` §0.
  Still deliberately uncollected (maintainer's "don't touch this yet", `q_important_ui_fixes.md:60-66`):
  beam-induced-motion graph, tomo-recon gallery revival, landing-page data-identification jank,
  pre-populating per-TS task rows before jobs run, and the logs-tab width + copy-to-clipboard — no
  rows yet.
- 2026-08-22 — P-39 scoped with the maintainer → `picking_ui/12` rewritten from stub to a six-stage
  plan, plus `docs/particle-data-flow.md` as its reference figure. Two defects surfaced during
  scoping and are now stages rather than rows here: pixel-grade merges accept mismatched
  `rlnImageSize` / `rlnTomoSubtomogramBinning` with only a log warning (→ 12-S0), and
  `list_actions.merge_lists` has had zero call sites since 09-S3, so the app currently cannot create
  a merged pick list at all (→ 12-S6). A successor roadmap is named in 12 §4: fine-grained
  collate-filters (per-source score bands, per-tomogram caps, orientation filters).
