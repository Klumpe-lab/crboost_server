# Picking / Particles UI pass — overview

**Intake:** the maintainer's walkthrough of the Particles surfaces, 2026-08-18. This is the
walkthrough `particles-ux-peeves.md` had been waiting for since 2026-08-16 ("still ONE raw peeve
intake so far"); its rows P-10…P-24 are the triage of what follows, and this directory is where the
work lives.

**Scope:** the species *creation* path, the species-derived fields inside particle job tabs, the
Species page's Overview tab, and the Templates & masks tab (the mounted Template Workbench). Every
stage is a self-contained commit that leaves the app runnable. No stage changes what a job computes.

**Not in scope, deliberately:** the cross-project particle registry. The maintainer talked through it
during this walkthrough and concluded mid-sentence that it is a bigger endeavour than it first looked
— so it is *scoped*, not built, in [07-scoping-particle-registry-edges.md](07-scoping-particle-registry-edges.md).
Stages 03 and 04 are written so the seams that expansion needs are the ones they cut along.

## Index

| # | Roadmap | Theme | Risk | Status |
|---|---|---|---|---|
| 01 | [species-entry-and-creation](../archive/picking_ui/01-species-entry-and-creation.md) | nav icon; template/mask registration → service; the creation dialog stops being one text field | low | **code-complete 2026-08-18**, pending runtime |
| 02 | [species-bindings-in-job-tabs](../archive/picking_ui/02-species-bindings-in-job-tabs.md) | the TM "Template, mask & search symmetry" card → an in-form section in the project's field vocabulary; the guideline below | low | **code-complete 2026-08-18**, pending runtime |
| 03 | [overview-identity-and-bindings](../archive/picking_ui/03-overview-identity-and-bindings.md) | Overview identity block restyled; bound template + mask listed with full paths, copy, and a deep link | low | **code-complete 2026-08-18**, pending runtime |
| 04 | [overview-declutter](../archive/picking_ui/04-overview-declutter.md) | counts move to the tab strip; gate + job-instance pills deleted; pixel sanity dropped; extraction geometry gets its own panel | low-medium | **code-complete 2026-08-18**, pending runtime |
| 05 | [workbench-two-column-and-tables](../archive/picking_ui/05-workbench-two-column-and-tables.md) | Templates ▏ Masks side by side; cards → table rows with a real selection affordance; viewer stays below | medium | **code-complete 2026-08-18**, pending runtime |
| 06 | [workbench-source-chrome](../archive/picking_ui/06-workbench-source-chrome.md) | one font scale in the source tabs; masks get a stated SOURCE; import button off the page edge; Edit Current visually separated | low | **code-complete 2026-08-18**, pending runtime |
| 07 | [scoping-particle-registry-edges](07-scoping-particle-registry-edges.md) | scoping only — where extraction geometry, pixel sanity and a cross-project catalog actually belong | — | scoping doc, no code |

Suggested order: 01 → 02 → 03 → 04 → 05 → 06. Only 01-S1 blocks anything (01-S2 needs it); the rest
are independent and can land in any order.

## Guideline of record — species-derived fields in a job's parameter tab

Recorded here at the maintainer's request (2026-08-18), because the peeve is not about one panel: it
is about every place a species value shows up in a form.

A particle-phase job tab (TM, Pick candidates, Subtomo extraction, Reconstruct, Class3D) shows three
kinds of field: the job's own parameters, its SLURM resources, and the handful of values it
**inherited from the species when the instance was created** — template, mask, symmetry, Ø,
extraction geometry. Those inherited values are still job parameters (snapshot-at-creation, D-5 of
roadmap 10: nothing auto-propagates), they just have a species-shaped default. So:

- **Weakly separated, not boxed.** One `section_header("From species — override for this run")` +
  `section_rule()` (`ui/job_plugins/_field_styles.py:66-73`) inside the *same* Parameters card the
  other fields live in. Never a nested `ui.card()` with its own tinted header bar, its own icon and
  its own font scale — that reads as a second application pasted into the form.
- **Same vocabulary as every other row.** `LABEL_W = 110` label column, 10 px sans label, 11 px mono
  value, `field_grid()` packing, `enum_field` / `path_row` where they fit
  (`ui/job_plugins/_field_styles.py:33-41, 77-90`). No per-panel `w-28 text-right uppercase` label
  column invented locally.
- **The species line stays on top, once.** `render_species_line`
  (`ui/components/species_pill.py:49-70`) is the header of the Config tab and the only place the
  species is *named*; the override section does not repeat the pill.
- **Say what the default was.** Each overridden row's tooltip names the species value it defaulted
  from, so "why is this not what the species says" is answerable without leaving the tab. The Jobs
  tab's drift chips (`ui/species/jobs_tab.py`) are the other half of the same story and must keep
  agreeing with it.
- **Absent ≠ defaulted.** No template registered means the row says so and points at the Species page
  (CLAUDE.md "Surfacing uncertainty"); it never silently picks the first file it finds.

Applies to `ui/job_plugins/template_match.py` (the offender, roadmap 02),
`ui/job_plugins/candidate_extract.py` and `ui/job_plugins/subtomo_extraction.py` (already close —
they render only the species line today), and to anything added later that inherits a species value.

## Conventions every stage inherits

From `feedback_ui_chrome_conventions` + CLAUDE.md's "UI reactivity patterns":

- compact `cb-*` chrome; no Quasar/material `ui.tabs()` CAPS tabs, no chunky buttons;
- `FingerprintedView` for anything a timer touches, `SingleFlight` for anything that opens a dialog;
- animations are CSS, never server ticks; disk I/O off the event loop;
- `ok()` / `err()` from `services/result.py` at every new service boundary;
- never invent a default for a load-bearing value — flag the gap in the UI instead.

## Log

- 2026-08-18 — directory created; stages 01–06 scoped from the walkthrough, 07 written as the
  deferred-scope record.
- 2026-08-18 (later, same session) — **01–06 were all implemented**; each has its stage record in its
  own file. 07 remains scoping only. All six are PENDING RUNTIME — nothing here has been exercised in
  the app, and the arc-wide script is `../ARC-RUNTIME-CHECKLIST.md` plus the per-roadmap checklists.
