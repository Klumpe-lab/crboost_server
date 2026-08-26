# 02 — Species bindings inside job parameter tabs

**Status:** CODE-COMPLETE 2026-08-18, PENDING RUNTIME — checklist in §3. **Depends on:** nothing. **Unblocks:** nothing (but it fixes the
guideline the rest of the arc is measured against). **Risk:** low — one plugin file, one new field
primitive, no model or driver change. One commit.

**Peeve (maintainer, 2026-08-18):** *"in the particle jobs (TM, pick candidates, subtomo extraction)
especially the TM job — there is currently an ugly gaping panel that doesn't fit into the aesthetics
of the parameter tab: 'Template, mask & search symmetry (all three default to the species; you can
override here for a one-off run)'. This should indeed follow the species and be weakly separated from
the rest of the parameters but be in line with them stylistically. Let's record this as a guideline
for all the other places the species get used."*

The guideline is recorded in [00-overview.md](00-overview.md) § "Species-derived fields in a job's
parameter tab". This roadmap makes the one offender obey it.

## 0. Facts (Stage 0 — gathered)

- **The offender:** `ui/job_plugins/template_match.py:57-69` — a full `ui.card()` with
  `border border-gray-200 shadow-sm bg-white mt-2`, a `bg-gray-50` header bar carrying a `category`
  icon, a `text-sm font-bold` title and an italic `text-[11px]` parenthetical. Inside it, three rows
  built by hand (`:72-118`, `:120-158`, `:171-207`) with their own label column
  (`text-[10px] font-bold text-gray-400 uppercase w-28 shrink-0 text-right`) and their own value
  styling (`flex-1 text-xs font-mono`).
- **The house pattern, already in this directory:** `ui/job_plugins/candidate_extract.py:60, 122, 130`
  — `section_header("Pick threshold", first=True)` / `section_header("Particle geometry")` /
  `section_header("Advanced")`, each followed by a `field_grid()` of `numeric_field` / `enum_field` /
  `text_field` rows. Same in `ui/pipeline_builder/slurm_tab.py:132-133, 220-222` (`field_group()` +
  `section_header(..., first=True)`).
- **The vocabulary** (`ui/job_plugins/_field_styles.py`): `LABEL_W = 110` shared label column
  (`:26-31` explains why it is shared — three per-section widths used to make one form look like
  three), `ROW_STYLE` `:43`, `SECTION_HEADER_STYLE` `:50-57` (mixed case at 11 px, **not** uppercase —
  the maintainer pushed back on caps once already), `section_rule()` `:71`, `field_grid()` `:77-90`,
  `field_group(muted=True)` `:93-97`.
- **Why no existing primitive fits the two path dropdowns:** `enum_field` (`:188-206`) derives its
  options from an `Enum` type bound to the attr. `template_path` / `mask_path` are `str` fields whose
  options are a *runtime* dict (path → rich label with apix / box / polarity / lowpass, built at
  `template_match.py:86-99` and `:186-190`). Hence the one new primitive below.
- **`symmetry` is already an enum-shaped choice** but with annotated labels
  (`TM_SYMMETRY_CHOICES` → `"{s}  ·  {suffix}"`, `template_match.py:126-135`); the annotation is
  genuinely useful (it says how PyTOM will honour the group) and must survive the restyle — so it
  goes through the same new primitive rather than `enum_field`.
- **The job's value is the operative one, at execution too.** `drivers/template_match_pytom.py` reads
  `params.symmetry` — the dropdown — and never consults `ParticleSpecies.symmetry`. That is the
  strongest argument for the guideline (these rows *are* job parameters), and also the reason the
  species-default tooltip matters: the species card can say I1 while the run is C1, and nothing but
  this tooltip and the Jobs tab's drift chip will say so.
- The other two particle plugins already comply: `candidate_extract.py:49` and
  `subtomo_extraction.py:55` render only `render_species_line` and then house-style sections. Nothing
  to change there beyond the shared header wording (below).

## 1. Stage S1 — `choice_row` + the TM restyle (one commit)

**(a) New primitive** in `ui/job_plugins/_field_styles.py`, next to `enum_field`:

```python
def choice_row(label, job_model, attr, options: dict[str, str], *, is_frozen, save_handler,
               hint=None, placeholder: str | None = None):
    """A select over a RUNTIME options dict (value → display label), in the same row chrome as
    every other field. `enum_field` covers the static-Enum case; this covers the case where the
    choices come from project state (a species' templates and masks, an annotated symmetry list)."""
```

Body mirrors `enum_field` exactly — `ROW_STYLE` element, `_label(label, hint)`, `ui.select(options=…,
value=…)` with `_INPUT_PROPS_BASE`, `cb-select` + `cb-select-popup`, `flex: 1 1 0; min-width: 0`,
`disable()` when frozen, `save_handler()` on change. It writes through an explicit `on_value_change`
(not `bind_value`) so the caller controls the empty-string case (`mask_path` has an explicit
`"(none)"` option today, `template_match.py:186`).

**(b) `ui/job_plugins/template_match.py`** — delete the card at `:58-69`; render, in the same flow as
the default params (i.e. after `render_default_params`, no wrapper element):

```
section_header("From species — override for this run")
with field_grid():
    choice_row("Template", …)      # options as built today (:86-99)
    choice_row("Mask", …)          # incl. the "(none)" entry (:186-190)
    choice_row("Symmetry", …)      # annotated labels as built today (:126-135)
```

- Each row's `hint` (the shared `_label` tooltip) names **what the species says**, so the tab answers
  "why is this not the species value" without a trip to the Species page:
  *"Species default: `ribo_lp30_white.mrc`. Changing it here affects only this job instance —
  the species is not modified."* Reuse `species.get_selected_template()` / `get_selected_mask()` /
  `species.symmetry`, which the file already resolves at `:75-76` and `:174-175`.
- The symmetry tooltip keeps the PyTOM explanation currently at `:143-148` (Cn → `--z-axis-…`,
  D/T/O/I → generated angle list).
- The empty states keep their meaning and stop being bespoke rows: when `species.templates` is empty,
  render the row with a single disabled option reading *"no templates registered"* plus the existing
  amber pointer to the Species page — absent must not read as defaulted (CLAUDE.md "Surfacing
  uncertainty"). Same for masks.
- `exclude` at `:50` is unchanged: `{"template_path", "mask_path", "symmetry"} | ctx exclude`.
- `render_species_line` stays exactly where it is (`:45`) — the pill is not repeated inside the
  section.

**(c) Header wording, shared.** "From species — override for this run" is the one phrasing; put it in
`ui/job_plugins/_field_styles.py` as a module constant (`SPECIES_SECTION_TITLE`) so the next plugin
that inherits a species value does not invent a fourth spelling.

**(d) CLAUDE.md.** Add three lines under "UI reactivity patterns" (or a sibling "Job parameter tabs"
subsection) recording the guideline: species-derived fields are a `section_header` + `field_grid`
inside the same card, never a nested card; the tooltip names the species default; absent is stated,
never defaulted. Cross-reference `docs/roadmaps/picking_ui/00-overview.md`.

## 2. Non-goals

- No change to *which* value a job runs with. Snapshot-at-creation (roadmap 10 D-5) stands: the
  species does not push into existing jobs, and this section does not write back to the species.
- No new drift detection — the Jobs tab (`ui/species/jobs_tab.py`) already renders drift chips for
  exactly these three fields and remains the place that flags divergence.

## 3. Runtime checklist (maintainer)

1. Open a TM job's Config tab: the three rows sit in the same form as the other parameters, same
   label column, same 11 px mono values; no boxed card, no second font scale, no icon bar.
2. Hover each of the three labels: the tooltip names the species default and says the override is
   per-instance.
3. Change the template; save; reopen the tab — the choice persisted. The Species page's Jobs tab
   shows a drift chip for that instance.
4. On a species with no templates and no masks: both rows state the absence and point at the Species
   page; nothing is silently preselected.
5. A frozen (already-run) job renders all three disabled.
6. Pick candidates and Subtomo extraction tabs are unchanged and now visually indistinguishable in
   chrome from the TM tab.
7. `ruff check .`.

## 4. Log

- 2026-08-18 — scoped from the walkthrough. No code.
- 2026-08-18 — **implemented.** `SPECIES_SECTION_TITLE` + `choice_row` in `_field_styles.py`; the
  boxed card is gone from `template_match.py` (no `ui.card` left in the file) and the three rows sit
  in one `section_header` + `field_grid` after the default params, each tooltip naming the species
  default. The guideline is recorded in CLAUDE.md under "Job parameter tabs — species-derived fields".
