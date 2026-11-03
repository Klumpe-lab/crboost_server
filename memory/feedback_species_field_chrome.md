---
name: feedback_species_field_chrome
description: Species-derived fields in a job's parameter tab are a weakly-separated section in the house field vocabulary — never a nested boxed card with its own font scale (user 2026-08-18)
metadata:
  type: feedback
---

When a particle-phase job (TM, Pick candidates, Subtomo extraction, Reconstruct, Class3D) shows values
it inherited from the species — template, mask, symmetry, Ø, extraction geometry — render them as
`section_header("From species — override for this run")` + `field_grid()` rows **inside the same
Parameters card**, using `ui/job_plugins/_field_styles.py` (LABEL_W=110, 10 px label, 11 px mono
value). Never a nested `ui.card()` with a tinted header bar, an icon and its own font sizes — that is
the "ugly gaping panel" the user called out on the TM tab. `ui/job_plugins/candidate_extract.py`
already does it right; `template_match.py` is the offender.

**Why:** the inherited values ARE job parameters (snapshot-at-creation, roadmap 10 D-5 — nothing
auto-propagates); they just have a species-shaped default. Boxing them implies they are a different
kind of thing, and it puts a second visual system inside one form.

**How to apply:** one `section_header` + rows in the shared vocabulary; the row tooltip names the
species value it defaulted from; absent (no template registered) is *stated* and links to the Species
page, never silently defaulted ([[feedback_never_silent_defaults]]). Same rule anywhere else a species
value surfaces. Full text: `docs/roadmaps/completed/picking_ui/roadmap_00-overview.md` § guideline; implementation is
`docs/roadmaps/picking_ui/02-species-bindings-in-job-tabs.md`, which also lands it in CLAUDE.md.

The same session's root-cause finding for the Species page and the Template Workbench is one sentence:
both declare a compact 10/11/12 px scale and then hand their tabs and every input to Quasar's ~14 px
uppercase defaults. The replacements already exist in-repo — `ui/components/segmented.py` for tabs,
the `.cb-select` q-field rule in `ui/main_ui.py` for fields. See [[feedback_ui_chrome_conventions]]
and [[project_species_registry_arc]].
