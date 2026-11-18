# Roadmap 59: the job page's upper half (Parameters, Inputs & Outputs, SLURM)

The maintainer's list of 2026-11-18:

1. Job pages have drifted apart: fonts, path display, sizes and above all layout differ from job to job, and almost
   every job has an element of its own.
2. On every job page the on/off options are an unordered pile, not aligned with anything.
3. Some boxes are wider than their neighbours for no reason (Symmetry).
4. Inputs is a mess of project names, copyable paths and analytics lines; reuse the job builder's look.
5. Outputs needs no section of its own: one "Inputs & Outputs", the outputs set slightly apart.
6. The round status dots are out; Harbor Mono's status lights instead.
7. The right column (Inputs, Outputs, SLURM, the supervisor) is taller than Parameters on almost every job, which
   leaves a hole under the parameters.

Prototyped on **`/dev/job-page-ideas`**; nothing in the job page itself is changed.

## 1. Built

- Package `ui/job_page_ideas/`, route in `ui/main_ui.py`, also served by `scripts/dev_screenshot.py`:
  - `data.py` reads four real jobs with `ProjectState.load(read_only=True)`: Motion & CTF, Template match and
    Reconstruct of `AGG412_20251113_412_rerun`, Class 3D job013 of `AGG412_STA`. Inputs come from the branch map's
    model (`project_from_state`), a particle set's lineage and facets from `particle_sets` and the job's
    `submitted_inputs`, outputs from the I/O schema, SLURM from `get_effective_slurm_config()` and the supervisor's
    defaults. The parameter groups, units and width classes are a table in this file (`LAYOUT`): today they live in
    each plugin, or nowhere.
  - `parts.py` draws the pieces: the header line, a parameter group in each toggle style, the I/O block in each
    style, SLURM as a strip or a grid. Status marks are the roster's light (`ui/status_indicator._dot_html`), square
    in Harbor Mono.
  - `page.py` lays them out as the options below. Switches at the top apply to every option:
    `?job=motion|tm|class3d|recon`, `?io=sockets|table|chain`, `?tog=grid|rows|flags`.
- Open it on your server after a restart: `http://localhost:24326/dev/job-page-ideas?token=…`.

## 2. What every option shares

- **One field geometry (C).** A 132 px label column in the builder's slot voice (mono, caps, muted); three box
  widths, S 64 px for a number, M 150 px for a choice or a short code (`500:10`, `2x2x1`, Symmetry), L to the
  column's edge for a path or a list; a 30 px unit column after S and M, so their boxes line up down a group whatever
  the unit. Group titles are sans 10.5 px semibold.
- **Toggles stay in their group** (CTF correction with CTF, Pre-read images with Compute), never one pile at the end.
- **Inputs & Outputs is one section.** Outputs sit under the inputs on the builder card's footer band. A path is never
  printed whole: the source names it, the copy icon holds it, hovering shows it; the job folder and outputs are
  written relative to the project.
- **The header line** carries status light, name, tool pill, number, species and the folder (relative, copy icon).
- **SLURM is short.** Nodes and tasks show only when not 1; the supervisor is one folded line (partition · GRES ·
  memory · time · QOS) that opens to its fields and a sentence on what it is.

## 3. The options

**A · The page** (each at the job pane's real width, 1180 px):
- A1 Strips: one column in reading order: Inputs & Outputs, Parameters (groups in balanced CSS columns, two or three
  across), SLURM as one wrapping strip of label-over-value cells. Nothing sits beside anything, so nothing leaves a
  hole.
- A2 Rail: today's two columns, the right one a 360 px rail made short (I/O in the builder's rows, SLURM as a
  two-column grid, the supervisor folded); the parameters flow into two columns on the left.
- A3 Flow: every block (I/O, each parameter group as a section of its own, SLURM) flows into balanced columns; the
  browser evens their heights.

**B · On/off options:**
- B1 Grid: checkboxes in fixed 160 px cells from the value column; they line up across groups and jobs.
- B2 Rows: one row each, like any field: the label in the label column, a yes | no switch where a box would be.
- B3 Flags: chips from the value column, filled when on; the densest.

**D · Inputs & Outputs:**
- D1 Sockets: the large card's rows: socket, slot word, the source's light, name and number; a set's lineage and
  facets indented under it; a missing optional input as a dashed socket and "optional, none".
- D2 Table: one grid for in and out rows, the file's last two parts in a column of their own.
- D3 Chain: the job in its place: its sources as small cards cabled into its card, its outputs off its right side.

## 4. What a build would be

- Field metadata on the param classes: a group, a unit and a width class per field (`json_schema_extra` or a
  `FieldLayout` table beside `USER_PARAMS`), so one renderer lays out every job's Parameters. The per-type plugins
  keep only what is theirs: choice rows fed from project state (template, mask, Denoiser), warnings, the
  template-matching search region.
- `io_config_component.py` re-drawn in the chosen D style, its behaviour (source menus, facet menus, set checks,
  Reset inputs, gain row) kept; Outputs folded into it.
- `slurm_tab.py` re-drawn as the chosen strip or grid; the supervisor folded.
- The job page's grid (`cb-jsec-*` classes in `ui/dashboard/css.py`) re-placed per the chosen A.
- Check: every job type's page opened once (the 22 types), against today's for missing fields.

## 5. Decisions owed

| # | Decision | Recommended |
|---|---|---|
| D1 | Page layout: A1, A2 or A3 | A1: no column ever outgrows another, and it reads in the order a job runs |
| D2 | Toggle look: B1, B2 or B3 | B1: lined up, and still reads as checkboxes |
| D3 | I/O look: D1, D2 or D3 | D1: the builder's own rows, room for a set's facets |
| D4 | Field metadata: on the param classes, or a layout table per job type | On the param classes: one place a field is declared |
