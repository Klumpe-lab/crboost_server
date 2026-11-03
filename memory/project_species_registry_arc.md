---
name: project_species_registry_arc
description: Species-registry arc 08-12 + 07 + de-novo S5/S6 COMMITTED 872bdbc 2026-08-18; picking_ui 01-06 BUILT but uncommitted and its docs still say "no code"; ~40-step runtime pass still owed
metadata:
  type: project
---

The whole arc is **code-complete and unverified**. Roadmaps 08/09/10 were committed 2026-08-16;
everything after that landed in commit `872bdbc` "Finish the species-registry arc" on 2026-08-18. Nothing in
the arc has been exercised at runtime.

State by piece:
- **07** (per-list extraction = a real job) — S0–S4 plus an S5 "review follow-up" stage that closed
  the 8 open findings from the dead adversarial review: `backend.reconcile_pick_list_extractions`
  asks SLURM instead of trusting a stored status (fixes the stranded instance that made "Extract all
  pending" a permanent no-op AND recovers a finished-but-unrecorded extraction),
  `cancel_pick_list_extraction` on the delete path, per-tomogram auth hint, logs-dialog leak/truncation
  /gone-instance fixes, rmtree off the loop.
- **11** — all 4 stages (Picks tab, Curation tab, Journey declutter, list_actions carve).
- **de-novo S5** — tomogram import: `JobType.IMPORTED_TOMOGRAMS` (ends the MERGED_SOURCES masquerade),
  multi-import BATCHES with an explicit rename/skip report, `.rec`, chunked probe + capped preview.
- **de-novo S6** — `services/aggregation/` + `ui/aggregation/` packages (no shims), `_MergeDialog`
  replaces the cross-tab module globals, SingleFlight, GateReport pre-flight, **`is_aggregation`
  deleted** → merging is a PARTICLES-header capability of EVERY project and project creation has no
  type switches left.
- **12** (lab species catalog) — all 4 stages: `species_catalog_root` (empty = feature off),
  `services/particles/catalog.py` with immutable `v<N>` version dirs, rail "From catalog" + Overview
  "Publish to catalog", `⌗ catalog_id` chip in the merge card that selects every project holding that
  species. Schema 3.5.
- **peeves** — P-09 closed; the walkthrough intake landed 2026-08-18 (P-10…P-25 → `docs/roadmaps/picking_ui/`, scoped 2026-08-18, then BUILT the same day — see the bottom of this file).

Owed, in order: `python -m py_compile`, `python check_boundaries.py`, then the single consolidated
runtime pass in `docs/roadmaps/roadmap_arc-runtime-checklist.md` (35 numbered steps, riskiest first).
`ruff check .` is clean. See [[feedback_dont_gate_stages_on_runtime]] and [[feedback_never_commit]].

**2026-08-18 — static gates are GREEN.** The maintainer ran `python -c "import main"` (import graph
OK), `python check_boundaries.py` (clean, R1–R4) and `ruff check .` on the cluster. `import main` is
the right substitute for `py_compile`: ruff already parses every file, so syntax was proven, and the
no-shim module move's real hazard was import-time. Section 0 of ARC-RUNTIME-CHECKLIST.md is marked
done; sections 1–5 (the ~35 UI steps) are still owed.

**Maintainer's standing position (2026-08-18):** does NOT want to run the 35-step pass before more
building. Polish of picking + the species register is the priority; deferred items (half-map import
D-9, imported tomograms as registry entities D-10, TM-on-imported, roadmaps 05/06) all confirmed
out of scope. The walkthrough intake — the one thing that could not be substituted — landed later the SAME day (see bottom).


**11-S3's (e)/(f)/(g) fixed 2026-08-18** (see that roadmap's stage log for the full record):
(e) was re-scoped by the maintainer from "mark the unattributable instance" to an INVARIANT — a new
PARTICLES-phase job cannot be created without a species, gated in `add_instance_to_pipeline` (the
choke point) rather than only in `prompt_species_and_add`'s dialog; the legacy render path is left
alone deliberately. (f) `tomograms_star_for` now checks the tomogram is actually a row in the CE
star, and `species_tomo_map`'s universe gained `_species_tomograms` (fixed at the source, not at the
caller). (g) `picks_filter.auto_source_for` split out so the Picks table can disable an unmergeable
`auto` tick instead of raising out of the click handler.

**11-S3's (a)–(d) and (i) also fixed 2026-08-18** — the reported-not-fixed list is now empty except
(h) (the removed "scan for saves now" button; the `CurationWatcher` covers it, so it is a product
question, not a defect). (b) was the biggest win and a one-liner: `_candidate_extract_status_per_ts`
treated a manifest's `"zero_picks": []` — what real manifests carry — as a cache miss and re-scanned
every `tmResults/*_particles.star` on EVERY call; keyed on the key's presence now. (c) threads the
already-read manifest in (`read_preview_manifest` has NO mtime memo, unlike `read_tomo_table`).
(a) `refresh_all` hoists both collectors and passes them to `render_strip` + `_main_signature` as
explicit optional params (not a memo dict — no ordering hazard); the signature slices its TS out of
the strip's unfiltered result, which is sound because `only_ts` is a pure row filter.
GOTCHA for future edits: `ui/tomo_dashboard_dialog.py` is NOT `ruff format`-clean and never has been
— three pre-existing hunks at lines ~1369 / ~1691 / ~2040. Check `ruff format --diff` hunk locations
before assuming your edit caused it; do not reformat the whole 5.8k-line file.

**2026-08-18 — THE WALKTHROUGH INTAKE HAPPENED, and it is scoped, not built.** The maintainer walked
Particles → Species registry → Overview → Templates & masks → the TM job tab and dictated ~16 peeves.
They became `docs/roadmaps/picking_ui/` (00 index + the guideline of record, 01 entry & creation,
02 job-tab bindings, 03 Overview identity + bindings, 04 Overview declutter, 05 workbench two
columns + tables, 06 workbench form chrome, 07 the DEFERRED cross-project registry scoping) and rows
P-10…P-25 in `particles-ux-peeves.md`. The ask was explicitly "scope this
into a few roadmaps, I'll run the picking jobs meanwhile and tell you more about the picks/curation
and jobs tabs and then the de-novo picking itself". So the next intake (picks / curation / jobs tabs,
de-novo feel) is still coming and picking_ui will grow.

Two things decided in that session worth not re-deriving:
- **Cross-project particle registry: DEFERRED by the maintainer mid-sentence** ("bigger endeavour than
  I initially thought now that I talked myself through it"). The tier split they arrived at —
  definition (portable, = roadmap 12, shipped) / application = particle × tomogram-set (extraction
  geometry + pixel chain, NOT modelled) / results (project-bound) — is recorded in picking_ui/07 with
  the edge rules 01–06 must not violate. See [[feedback_species_field_chrome]].
- **Extraction geometry and pixel/binning sanity do not belong to a particle.** Both are counted in
  voxels of a particular reconstruction regime; the same species can be extracted from several
  tomogram sets at several binnings. 04 pulls the sanity table off the Overview (the Tomogram
  Dashboard keeps it) and gives geometry its own project-scoped panel that says so.

**2026-08-18 (later) — picking_ui 01–06 were BUILT, same session, and the docs were never updated.**
Every stage of `docs/roadmaps/picking_ui/` 01–06 is implemented in the working tree (verified
2026-08-19 by reading the code against each spec): 01-S0 particle nav icon + `ui/components/svg_icon.py`
(`vial.svg` deleted) · 01-S1 `species_admin.register_template/register_mask/select_*` · 01-S2
`SpeciesDraft` + `prompt_species_draft` + `ui/components/color_swatch.py` · 02 `choice_row` +
`SPECIES_SECTION_TITLE` + the TM restyle + the CLAUDE.md guideline · 03 identity block + `_BindingsView`
+ `ui/components/path_link.py` + `ui/components/chip.py` · 04-S1 `Segmented.set_badge` + the one-line
STATUS + five deletions · 04-S2 `ExtractionGeometryPanel` + the SUBTOMO_EXTRACTION prefill · 05
two-column grid + `.cb-tw-row` tables with class-flip selection · 06 `Segmented` + `.cb-field` + muted
Edit-current groups + mask SOURCE header.
**Documentation debt, do not re-derive:** every 01–06 Log still says "No code", `picking_ui/00-overview.md`
still says "Nothing implemented yet", the `README.md` row still says "scoped 2026-08-18, no code", and
`particles-ux-peeves.md` rows P-10…P-24 still say "scoped". Flip those and write the stage records
before/with the commit.

**Also uncommitted and NOT in any roadmap (2026-08-18):** the tilt-filter reshape — the filter no
longer produces a tomostar dir at all; the verdict is per-frame `is_filtered_out` in the registry and
`drivers/ts_alignment.py` applies it via `drop_tilts_from_tomostar` while snapshotting. That is what
fixes the maintainer's "Cannot finalize: tsImport tomostar not found" report and finally kills the
`pending_tiltFilter` phantom producer ([[project_jobnum_offby1_abort_redeploy]]). Plus the int-param
load-repair (`ProjectState._build_job_params`) — see [[reference_int_job_param_corruption]].

**Still uncollected, next intake:** the Picks / Curation / Jobs tab peeves and "the de-novo picking
itself and how it feels" (maintainer said they'd run picking jobs and report back), roster/queue
peeves, and the three items in `q_important_ui_fixes.md` (beam-induced-motion connected graph — see
[[feedback_dashboard_plot_principles]] "don't connect discrete datapoints" — bring back the tomo-recon
gallery with Journey links, landing-page data-identification jank). None are triaged into the peeves
sheet yet.

**2026-08-19 — first runtime finding, and the lesson.** A species with any mask crashed the whole
Species page: two sites read `TemplateMask.imported_from`, a field that has never existed (masks carry
`derived_from_template_id` + `method`; `imported_from` belongs to `ParticleTemplate`). One was
PRE-EXISTING and already committed (10-S3's `overview_tab._render_provenance`), the other the same
mistake copied into picking_ui 05-S2's mask row. It only fires once a mask is registered, which is how
it survived every static gate. **`ruff check` + `python -c "import main"` cannot see an attribute that
does not exist on a pydantic model** — before calling UI code-complete, sweep every new
`<model>.<attr>` read against the model definition (doing that found no others: ParticleSpecies,
SpeciesOverview, ExtractionParams, ListRef, ParticleTemplate all clean). Second fix, separate cause:
`SpeciesPage.build` refreshed the rail AFTER selecting the first species, so any tab raising during
build took the species list with it and the bug presented as "my species is not in the registry" —
the rail is painted first now. Tracked as P-26 in `docs/roadmaps/roadmap_particles-ux-peeves.md`.
