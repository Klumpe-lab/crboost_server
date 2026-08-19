# 04 — Species Overview: declutter the status block, give extraction geometry a panel

**Status:** CODE-COMPLETE 2026-08-18 (S1 + S2), PENDING RUNTIME — checklist in §4. **Depends on:** 03 (same file; land 03 first or rebase).
**Unblocks:** nothing. **Risk:** low-medium — mostly deletion, plus one new panel and one prefill
branch. Two commits: S1 declutter (counts → tabs, chips deleted, sanity dropped) · S2 the extraction
geometry panel.

**Peeves (maintainer, 2026-08-18):** *"'Status'. 'picks · extraction · bound jobs'. Why the fuck is
this such kitchen sink? Can we just display some of these that are relevant as plain numbers on the
tabs above like 'picks' and 'jobs' etc. 'Gate' is confusing. Remove it. 'pick candidates
tmextractcand subtomo subtomoExtraction' pills are confusing and I'm not sure what the fuck are the
alternatives? Remove them. Extraction geometry — it should be its own panel that the user can fill
that persists between jobs and possibly between projects (with relevant info filling in the
tm/extraction jobs etc.). … I don't think pixel/binning sanity belongs here since it also refers to a
particular tomogram set in this project, whereas in other projects or even within the same project
this particle may be applied to differently dimensioned tomos, same tomos with different binning
etc."*

## 0. Facts (Stage 0 — gathered)

- **The block.** `_StatusView.render` (`ui/species/overview_tab.py:255-325`): a "STATUS" label with
  the hint "picks · extraction · bound jobs" (`:261-263`), then **two** chip strips —
  strip 1 (`:274-286`): tomos with picks · picks · kept · extracted lists · **gate**;
  strip 2 (`:287-319`): **pick-candidates instance id** · **subtomo instance id** · extraction
  geometry · templates count · masks count — then the **pixel sanity table** (`:320-325`).
- **Gate** is `compute_gate_report`'s roll-up (`services/particles/species_overview.py:149-151`:
  BLOCKED / PENDING / READY). It is genuinely load-bearing **elsewhere**: the Picks tab's "Extract
  all pending" runs a gate-report pre-flight (`ui/species/picks_tab.py:218`, roadmap 11-S2). Only the
  *chip* is confusing — per-list state is already a column in that table. Delete the chip, keep the
  service and the pre-flight.
- **The two instance-id pills** render `ov.ce_iid` / `ov.subtomo_iid` — raw instance ids
  (`tmExtractCand`, `subtomoExtraction`, possibly `…__<species>`). The Jobs tab
  (`ui/species/jobs_tab.py`) lists exactly these jobs with type, status dot, drift chips and an
  "open ↗". The pills are a worse duplicate of a better surface.
- **Pixel sanity already has a home:** `ui/tomo_dashboard_dialog.py:1033` + `:1061` render
  `compute_pixel_chain(project_state)` / `render_pixel_sanity_table` for the whole project. The
  Overview copy (`overview_tab.py:226-228`) recomputes the *entire* project chain and then filters to
  one species — and the maintainer's objection is correct on the merits: the chain is a property of
  (this project's tomograms × this binning), not of the particle. Deleting the copy loses no surface.
  It also removes the only MRC-header read from this tab's compute.
- **The tab strip** is `Segmented` (`ui/components/segmented.py:17-49`), built once from
  `TABS` (`ui/species/page.py:40-46`); each segment is a `ui.label` with `.cb-seg-btn`
  (CSS `ui/dashboard/css.py:954-965`). There is no API to change a segment's text after construction.
- **Cheap in-memory counts already exist**: the rail computes `(n_templates, n_pick_lists)` per
  species from `state.species_registry` + `state.pick_lists` with no disk (`ui/species/rail.py:26-29`);
  `jobs_for_species` (`services/particles/species_jobs.py`) reads `state.jobs`. So tab badges need
  **no** disk work and can ride the page's existing 3-s observe.
- **Extraction geometry today:** `species.extraction_params` (`services/project_state.py:189-200`,
  no defaults, None = undecided). Resolution precedence lives in
  `extraction_params_for_species` (`services/aggregation/authoritative.py:229-254`): the species'
  SUBTOMO_EXTRACTION job model → `species.extraction_params` → **None**, deliberately with no third
  branch (the old `384/1.0/224` fallback was killed by de-novo D-3). It is asked once, in a modal, at
  the first per-list extract (`prompt_extraction_geometry`, `ui/particles/list_actions.py:392-...`,
  committed by `commit_extraction_geometry` `:~370-389`).
- **Job prefill from species today** (`ui/pipeline_builder/pipeline_builder_panel.py:371-381`): TM
  gets template / mask / symmetry; TEMPLATE_EXTRACT gets `particle_diameter_ang`. **SUBTOMO_EXTRACTION
  gets nothing** — it falls back to its own model defaults `box_size=384`, `binning=1.0`,
  `crop_size=224` (`services/jobs/subtomo_extraction.py:85-110`), i.e. exactly the numbers D-3
  removed from the species-level resolver.

## 1. Stage S1 — declutter (one commit)

**(a) Counts move to the tab strip.**
- `ui/components/segmented.py`: add `set_badge(key: str, text: str) -> None` — appends/updates a
  `<span class="cb-seg-badge">` inside the segment (built once at construction, text set later so no
  DOM rebuild ever happens on a count change). CSS in `ui/dashboard/css.py` next to `.cb-seg-btn`:
  10 px, tabular-nums, muted, `margin-left: 5px`; nothing on an empty string.
- `ui/species/page.py`: in `observe()` (already a 3-s in-memory tick, `:230-246`), push badges for the
  active species: `templates` → `len(sp.templates)`, `picks` → number of the species' pick lists
  (same read as `rail._counts`), `jobs` → `len(jobs_for_species(state, sid))`. Overview and Curation
  get no badge. Guard with the same "signature moved" discipline the rail uses — set the text only
  when it changed.

**(b) The status block shrinks to one line.** Replace both chip strips with a single muted sentence
built from the same `SpeciesOverview`:
`"12 picks on 4 tomograms · 9 kept · 2 lists extracted"` — plain numbers, no chips, no gate, no
instance ids. Keep the "computing…" and the error branch (`:264-271`) exactly as they are: a failed
compute must stay visible (CLAUDE.md "never fail silently").

**(c) Deletions**, all from `ui/species/overview_tab.py`:
- the `gate` chip (`:279-286`) and the `gate_status` map (`:273`);
- the `pick candidates` / `subtomo` instance-id chips (`:288-299`) — with a one-line comment naming
  the Jobs tab as the owner, so they do not come back (same convention as P-09's fix);
- the `templates` / `masks` count chips (`:308-319`) — roadmap 03's bindings block supersedes them;
- the `extraction` chip (`:300-307`) — S2 supersedes it;
- `render_pixel_sanity_table` + `compute_pixel_chain` + `apply_sanity_rules` imports (`:30, 37`), the
  rows in `_Computed` (`:216-220`), their computation (`:226-229`) and their render (`:320-325`).
  `_Computed` keeps `overview` + `error`; the `sanity_key` term disappears from the signature.
- `SpeciesOverview.gate`, `ce_iid`, `subtomo_iid` stay on the dataclass — the Picks tab and
  `list_actions` read them. This stage deletes *renderers*, not the read model.

## 2. Stage S2 — the extraction-geometry panel (one commit)

Its own panel on the Overview, below the bindings block, titled **"Extraction geometry"** with the
subtitle **"this project"**.

- Three inputs in the roadmap-03 field vocabulary: **Box** (binned voxels) · **Binning** ·
  **Crop** (binned voxels), each with the description already written on the job model
  (`services/jobs/subtomo_extraction.py:85-110` — reuse that text as the tooltip; do not write a
  second version of it).
- **Empty is a real state.** Unset renders as empty inputs plus a muted
  *"not set — asked once at the first extraction, never guessed"*; it never shows `384 / 1.0 / 224`
  as if the user had chosen them. Writing requires all three; a partial edit saves nothing and says
  so (the same rule `prompt_extraction_geometry` already enforces).
- **Effective-value line** under the inputs, mirroring `extraction_params_for_species`' precedence in
  words: *"In use: from the subtomoExtraction job (box 384 · bin 1 · crop 224). Values here apply to
  hand-picked lists that have no such job."* — or *"In use: these values."* — or *"Nothing set."* One
  read of `extraction_params_for_species(state, sid)` plus a check of which branch answered.
- Commit path: reuse `list_actions.commit_extraction_geometry` (already `mutate_species` + forced
  save) rather than a second writer. `prompt_extraction_geometry` stays as-is for the
  never-visited-the-panel case, and gains a one-line pointer to the panel.
- **The prefill the peeve asks for:** `ui/pipeline_builder/pipeline_builder_panel.py:371-381` gains a
  `elif job_type == JobType.SUBTOMO_EXTRACTION:` branch that copies `species.extraction_params` into
  `box_size` / `binning` / `crop_size` **when set** (leave the model defaults alone otherwise — a job
  parameter legitimately has a default; a species answer does not). Same shape and same
  snapshot-at-creation semantics as the TM and pick-candidates branches above it (D-5: no
  auto-propagation to existing jobs).
- **Panel copy states the known limitation**, because the maintainer named it and it would otherwise
  be a silent lie: *"One geometry per species per project. A species extracted from tomogram sets at
  different binnings needs one per set — see docs/roadmaps/picking_ui/07."* Keep it to a tooltip or a
  single muted line; do not litter.

## 3. Non-goals

- **No cross-project persistence.** "possibly between projects" is exactly the deferred piece; the
  panel is project-scoped and says so. [07](07-scoping-particle-registry-edges.md) records why and
  what would have to change.
- **No per-tomogram geometry.** Same reason. The panel is written so that adding a set-scoped
  override later means adding rows, not rewriting the writer.
- **The gate concept is not removed** — only its chip.

## 4. Runtime checklist (maintainer)

1. Tab strip shows counts next to Templates & masks, Picks and Jobs; they track reality (add a pick
   list, add a job, register a template — each moves within 3 s) and never cause a visible relayout.
2. Overview: one summary line where two chip strips were; no gate, no instance ids, no sanity table.
3. The Picks tab's "Extract all pending" still runs its gate pre-flight and still refuses when the
   report says BLOCKED (the chip is gone, the behaviour is not).
4. The Tomogram Dashboard still shows the full pixel/binning sanity table (the surface that
   *keeps* it).
5. Extraction geometry panel on a de-novo species with nothing set: empty, with the "asked once…"
   line; fill all three, reload the project, values persist; the effective-value line reads "these
   values".
6. On a species that has a subtomoExtraction job: the line names the job as the source and says the
   panel's values apply to hand-picked lists.
7. Add a new Subtomo extraction job for a species that has geometry set → the new job's box / binning
   / crop are prefilled from it. Add one for a species with none → the job's own defaults, unchanged.
8. First per-list extract on a species with the panel filled no longer opens the geometry modal.
9. `ruff check .` + `python check_boundaries.py`.

## 5. Log

- 2026-08-18 — scoped from the walkthrough. No code. Recorded that `gate` / `ce_iid` / `subtomo_iid`
  stay on the read model (other callers), and that dropping the sanity table also removes the tab's
  only MRC-header read.
- 2026-08-18 — **both stages implemented.** S1: `Segmented.set_badge` + `.cb-seg-badge`, badges pushed
  from `page.observe()` only when the number moved; `_StatusView.render` is one muted sentence and its
  docstring names where each deleted chip went, so they do not come back. S2: `ExtractionGeometryPanel`
  (built once — it holds inputs), `_effective_text` mirrors `extraction_params_for_species`' precedence
  in words, commit goes through `list_actions.commit_extraction_geometry`, and
  `pipeline_builder_panel` gained the `SUBTOMO_EXTRACTION` prefill branch (only when set).
