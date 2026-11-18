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

## 5. Decisions

| # | Decision | Taken |
|---|---|---|
| D1 | Page layout: A1, A2 or A3 | **A2 Rail** (maintainer, 2026-11-18) |
| D2 | Toggle look: B1, B2 or B3 | **B2 Rows** |
| D3 | I/O look: D1, D2 or D3 | **D2 Table** |
| D4 | Field metadata: on the param classes, or a layout table per job type | **On the param classes** (maintainer, 2026-11-18) |

## 6. Round 2 (2026-11-18): headers, long paths, checkboxes

The maintainer, after round 1: Harbor Mono's header (the blue-grey band, the accent on its left, the rounded top that
reads as a shadow) is everywhere and everywhere the same. Wanted: a header language more like the rest of the look
(Oxide's console as the reference), with more than one level: two or three for the important things, and a quieter one
for the groups of one section (Angles & offsets, Reference & mask), subtly different from the section's. Also: long
paths are cut too often, and checkbox labels sit above the box's bottom edge.

On the same page, round 2 first; switches `?head=today|rule|tab|band` and `?path=cut|wrap|open`; the defaults are
round 1's picks (`io=table`, `tog=rows`). Code: `ui/job_page_ideas/heads.py`.

**H · Header ladders.** Each look has four levels: page (the job's title line), section (Parameters, Inputs &
Outputs, SLURM, Tasks), accented section (one that asks to be looked at: a failed job's Log, Results), group (inside a
section). Each panel shows the ladder, then the picked page (Rail, Rows, Table) drawn in it.
- Today: the band, its left accent, the hairline; a group is bold text.
- H1 Rule (Oxide's console): section title in mono caps, a hairline running on from it to the edge, meta at the far
  end; no fill, no corners. Accent: a square pip, title and rule in blue. Group: sans semibold, no line.
- H2 Tab: the title in a flat inverted tab (ink, white mono caps) on a full-width ink rule. Accent: tab and rule in
  blue. Group: mono caps behind a small hollow square.
- H3 Band: a flat square band ruled in ink along its top instead of its left. Accent: a 2 px blue top rule on a bluer
  band. Group: sans semibold over a dotted hairline.

**E · Long paths.** Inside the project a path is written from the project folder, outside it whole; the hover shows it
whole, the copy icon copies it. The folder gives way before the file name (`flex-shrink` 1000 against 1), so the name
is cut only once the folder is gone.
- E1 Cut: one line.
- E2 Wrap: every part, breaking only after a slash (a zero-width space after each).
- E3 Open: E1, and a click opens it to E2 in place.
In Table (D2) an input's file now goes on its own line under its source, the block's full width, instead of a narrow
column; a path-valued parameter (Template, Mask) uses the same display inside its box.

**F · Checkboxes.** Measured on the running server (Template match's tab): Quasar's `dense size=xs` box runs 0.75–14.25
px in its row, the 10 px label is centred on it, so its baseline sits ~2 px above the box's bottom edge: the "elevated"
look. Ours: a 10 px square box with no text in it (the tick is an out-of-flow pseudo-element), in an `inline-flex`
row aligned on baselines, so the box's bottom edge is the label's baseline. The panel draws a red line along each
box's bottom.

| # | Decision | Recommended |
|---|---|---|
| D5 | Header look: H1, H2 or H3 | **H1 Rule** (maintainer, 2026-11-18). It amends CLAUDE.md's "sections separate by whitespace, never underline rules": the hairline is the header's own, not a rule under a title |
| D6 | Long paths | **Middle cut** (§7), the maintainer's design, replacing E1–E3 |
| D7 | Checkboxes: ours everywhere, or Quasar's restyled | **Ours** (`house_check`, maintainer, 2026-11-18) |

## 7. Long paths, as decided (2026-11-18)

The maintainer's design, built in the prototype (`heads.mpath`, `MPATH_JS`, `MPATH_CSS`); E1–E3 are gone from the page.

- **Cut in the middle.** As much of the path's start as fits, an ellipsis, then its last part (`/name`) whole. When the
  name alone does not fit, the name is cut in its middle with its end (extension) kept. Inside the project the path is
  written from the project folder, outside it whole. The cut is made in the browser in characters (the face is
  monospaced; one character's width is measured in the element itself, and again once the fonts are ready), and made
  again whenever the element's width changes (`ResizeObserver`), so a path only loses characters when its space does.
- **Click copies** the whole absolute path (`copy_on_click`, the house toast). No copy icon beside a path any more.
- **Hover unrolls it.** A lit copy (ink, a blue glow) is laid over the cut path, anchored at the ellipsis; the hidden
  middle comes back a few characters a frame from both sides of it, then the project folder comes in on the left,
  until the absolute path shows (~230 ms whatever its length). It is kept inside the window. It closes when the
  pointer leaves, or when a scroll moves the path (a log scrolling elsewhere, or a scroll event that moved nothing,
  does not close it).
- **Checked** in headless Chromium on `/dev/job-page-ideas`: at 360 px `External/job013/in…/optimisation_set.star`,
  `412_shell_rotavg…b004_apix6.20_box128.mrc`; a box resized 520 → 300 → 180 px re-cut live (whole,
  `External…/optimisation_set.star`, `opti…t.star`); the clipboard held the absolute path after a click; the overlay's
  edges per frame grew from the ellipsis outwards to the full path.
- **Build:** the script as a static asset loaded once per client beside `ui/dashboard/css.py`'s styles (a `<script>`
  injected after connect, as the CSS is), the component in `ui/components/` (`path_text(path, project)`), and every
  `copyable_path` and path label in the job page, the roster's hover card and the dialogs moved onto it.

## 8. Build plan (every decision taken, 2026-11-18)

Taken: A2 Rail, B2 Rows, D2 Table, H1 Rule, middle-cut paths (§7), field metadata on the param classes (D4), our own
checkbox (D7). The prototype is the reference for every look: `ui/job_page_ideas/` (`heads.py` headers, paths and
checkbox; `parts.py` field geometry, Rows, Table, SLURM grid; `page.py` the Rail). Build in the main checkout, on the
maintainer's word, in this order; each stage leaves the app working.

**S1 · Primitives (nothing on a page changes yet).**
- `ui/components/path_text.py`: `path_text(path, project=None)`, the prototype's `heads.mpath`; its script
  (`MPATH_JS`) as a constant run per client from `ensure_assets_loaded` (`ui/dashboard/css.py`, beside `_TITLE_TIP_JS`),
  its CSS (`MPATH_CSS`) into `_CB_CSS`. Class names `cb-path` / `cb-unfurl` (not `cbjp-`).
- `ui/components/check.py`: `house_check(label, *, model=None, attr=None, value=False, on_change=None, disabled=False,
  tooltip=None)`, the prototype's box with the label's baseline on its bottom edge; binds like `bind_value`; its CSS
  (`CHECK_CSS` minus the guide) into `_CB_CSS`.
- Header ladder (H1 Rule): `section_head(title, *, right="", accent=False)` and `group_title(text)` in
  `ui/job_plugins/_field_styles.py`. The Rule's hairline is structural: an element after the title (flex: 1, 1 px,
  `--cb-head-rule`), never a pseudo-element in the stale head shell. Variables in `_HARBOR_MONO_CSS`: `--cb-head-rule`,
  `--cb-head-accent`; `--cb-head` transparent, `--cb-accent-w` 0, the head's radius 0.

**S2 · Field metadata (D4).**
- Every user field declares `group`, `unit`, `width` (`S` / `M` / `L`) and a `title`, in `json_schema_extra` on its
  `Field(...)` in `services/job_models.py` (and the param classes beside it). The group order is the order of first
  appearance in the class.
- `services/job_layout.py` (ui-free, boundary R1): `layout_of(job_model) -> list[Group]`, groups of `(name, title,
  width, unit, is_toggle)`; a field with no group lands in "Other", logged once per class so gaps get filled.
- Port the prototype's `LAYOUT` table (`ui/job_page_ideas/data.py`: Motion & CTF, Template match, Class 3D,
  Reconstruct) first, then the other 19 job types (`services/jobs/spec.py` JOB_SPECS: importmovies, tsImport,
  tiltFilter, aligntiltsWarp, missAlign, tsCtf, denoisetrain, denoisepredict, segment, subset, tmextractcand,
  filamentPick, subtomoExtraction, warpExport, extractPickList, reconstructParticle, refine3d, maskCreate,
  postprocess). The group names follow today's plugins where they have them.

**S3 · One parameter renderer (B2 Rows, the field geometry).**
- `ui/job_plugins/params_renderer.py`: draws `layout_of(job_model)`: `group_title` per group; a row per field: 132 px
  label column in the slot voice (`--cb-label-*`), the box at S 64 / M 150 px / L to the edge, a 30 px unit column; a
  toggle as a yes | no switch in the box's place; path values through `path_text`. Binding and saving as today
  (`numeric_forward`, `enum_forward`, the save handler, `FocusHold`).
- Hooks for what is a plugin's own: `custom={field: builder}` replaces one field's row (choice rows from project
  state: template, mask, Denoiser, species), `after={group: builder}` adds rows under a group (template matching's
  search region, warnings). Each plugin in `ui/job_plugins/` (fs_motion_and_ctf, miss_align, subset,
  template_match, segment, candidate_extract, warp_export, ts_reconstruct, subtomo_extraction, default_renderer)
  shrinks to its hooks; its layout code goes. missAlign keeps its refresh coupling with SLURM (`edit_refreshes`).
- The species-derived fields keep "From species — override for this run" as a group, its tooltips naming the
  species default (CLAUDE.md, "Job parameter tabs — species-derived fields").

**S4 · Inputs & Outputs (D2 Table).** `ui/pipeline_builder/io_config_component.py`: one section "Inputs & Outputs"
(meta: "fixed at submit" / "chosen at Run"). Grid `24px 100px 1fr`: `in`, slot word, source (the roster's square
light, type name, number; the source menu opens from it as today); the file on the line under it through `path_text`;
a set's lineage, facet lines and Run-check lines under that, as today. A hairline, then `out` rows: slot word and file.
Keep all behaviour: source and facet menus, Reset inputs, gain row, manual paths, `observe`, the signature. Job folder
moves to the page header (S6).

**S5 · SLURM (Rail).** `ui/pipeline_builder/slurm_tab.py`: header meta "profile <name>", Reset to profile on it; a
two-column grid of label / box (66 px labels); "At once" (array throttle) first; Nodes and Tasks/node only when not
1, behind a "more" switch otherwise; the supervisor as one folded line (partition · GRES · memory · time · QOS) that
opens to its fields and a sentence. Writes through `set_slurm` as today; the focus hold stays.

**S6 · The page (A2 Rail).** `ui/pipeline_builder/job_tab_component.py` and the `cb-jsec-*` grid in
`ui/dashboard/css.py` (`@container (min-width: 860px)`): `grid-template-columns: minmax(0, 1fr) 360px`; Parameters
left (its groups in CSS columns of 330 px when wide); Inputs & Outputs over SLURM in the rail; the folded sections
across both. Under 860 px one column. A header line: light, name, tool pill, number, species, the folder as
`path_text`.

**S7 · Across the app.**
- Headers: every `PANEL_HEAD_STYLE` / `panel_head` / `config_section` / fold head (17 files) draws through
  `section_head`; `section_header` (27 calls) becomes `group_title`; the Journey's `.cb-section-card-header` rule in
  `css.py` the same look. Accent where a section asks to be looked at (a failed job's Log, Results).
- Paths: `copyable_path` (6 calls) and path labels that ellipsize at the end move to `path_text`.
- Checkboxes: `ui.checkbox` (20 calls) and `toggle_field` (7) to `house_check`, or to Rows where they are job
  parameters.
- CLAUDE.md: "Sections separate by whitespace, never underline rules" becomes the Rule ladder; "Job-tab parameter
  forms stay on _field_styles.py (.cb-select scale, 110 px label column)" becomes the 132 px field geometry; add
  `path_text` and `house_check` to "One control vocabulary".
- Retire the prototype: `ui/job_page_ideas/`, its route in `ui/main_ui.py`, its entry in `scripts/dev_screenshot.py`
  (as `ui/jobs_ideas/` went in 152df75).

**Checks.**
- Every job type's page (23) opened in the running server, each field of today's page found on the new one (a
  scripted list: `USER_PARAMS` against what the renderer drew); a draft job, a frozen one, a failed one with Reset
  inputs; a pane under 860 px.
- Edits still save and still reach SLURM's refresh (missAlign's time limit and GRES follow its parameters).
- Paths: hover unroll and click copy on a job page; a page with a running log (the scroll rule of §7).
- `./crboost python -m ruff check .`, `./crboost python check_boundaries.py`; screenshots with
  `scripts/dev_screenshot.py --url` (token in the query; rename the PNGs, their names carry it).

## 9. Built (2026-11-18): S1–S7

All seven stages, in the main checkout, uncommitted; not yet seen in the running server (it needs a restart).

- **S1.** `ui/components/path_text.py` (`path_text`, `set_path`; `PATH_CSS`, `PATH_JS` run per client from
  `ensure_assets_loaded`), `ui/components/check.py` (`house_check`, `yes_no`; `CHECK_CSS`), the Rule ladder in
  `_field_styles.py` (`section_head`, `section`, `group_title`; `config_section` and `panel_head` draw through it).
  Variables `--cb-head-rule`, `--cb-head-accent`, `--cb-head-accent-soft` in `_HARBOR_MONO_CSS`.
- **S2.** Every user field of the 22 param classes carries `title` and `json_schema_extra=form(group, width, unit)`
  (`services/jobs/_base.py`, put on by a codemod; an AST comparison against HEAD shows no other argument changed);
  each class orders its groups in `PARAM_GROUPS`; the species-derived fields are in `SPECIES_GROUP`.
  `services/job_layout.py` reads them. `shows_param` on the class hides a row that means nothing now (Segment's
  method fields, Pick candidates' two thresholds).
- **S3.** `ui/job_plugins/params_renderer.py`: one renderer, hooks `before` / `custom` / `replace` / `after`; rows
  are built once and hidden or shown, never rebuilt. `fs_motion_and_ctf.py` and `warp_export.py` are gone (the
  renderer draws them); template_match, candidate_extract, segment, miss_align, subtomo_extraction and
  ts_reconstruct keep only their hooks. Field helpers in `_field_styles.py` draw the 132 px geometry; `toggle_field`
  is a yes | no row; a frozen job's path parameter is a `path_text` in the box.
- **S4.** `io_config_component.py`: one "Inputs & Outputs" section (meta "fixed at submit" / "chosen at Run", Reset
  inputs on its head), the `24px 100px 1fr` table with the builder's slot words, the roster's square light, type name
  and number; each file under its row through `path_text`; `out` rows after a hairline. Menus, facets, set checks,
  gain row, manual paths and `observe` unchanged.
- **S5.** `slurm_tab.py`: the section's own head (profile as meta, Reset to profile on it), the two-column grid with
  "At once" first, Nodes and Tasks/node behind a "nodes & tasks" check while they are 1, the supervisor one folded
  line.
- **S6.** The Rail grid (`minmax(0, 1fr) 360px`, `.cb-jsec-rail` holding I/O over SLURM so the rail never leaves a
  gap between them), groups in 330 px CSS columns, folds with Rule heads (accent on a failed job's Log and on
  Results). The header line: light, tool pill, number, named species, the folder as `path_text`.
- **S7.** Every `PANEL_HEAD_STYLE` stripe and fold head moved onto `section_head` (13 files; `PANEL_HEAD_STYLE` and
  `fold_panel` deleted); the Journey's `.cb-section-card-header` in the Rule look from `css.py`; `section_header`
  gone (`group_title`); `copyable_path` gone, the end-cut path labels (protocols dialog, the roster's hover card,
  the projects overview, the merge card) on `path_text`; all 19 `ui.checkbox` on `house_check` (it sends the event
  `ui.checkbox` sends, so the handlers stand). CLAUDE.md amended. The prototype retired.

Departures from the plan:
- `--cb-head` keeps its value: selected rows (tilt series, layers, the branch map) use it. The Rule heads never read it.
- The header line does not repeat the job's name: the view's title line above it names the job.
- Subset keeps its own renderer (its one parameter is its table); denoise-predict's Denoiser block draws its group.

Checks run:
- In-process (a NiceGUI client with no socket, three projects of `_crboost_data_api` loaded read-only, nothing
  saved): all 23 job types' pages built, a draft of each plus every real job of the three projects (succeeded,
  running, queued) and two failed copies; every user parameter's label found on its page (denoise-predict's method
  fields wait for a Denoiser, as before). Failed copies: Reset inputs on the head, the Log accented.
- Behaviour, same way: a yes | no click writes the model and saves, a model change reaches the switch; Segment's
  method swaps its rows after the edit refresh; missAlign's GPU count reaches SLURM's GRES (gpu:2 -> gpu:1);
  `house_check` sends `on_change` on a click and on a set from code.
- Headless Chromium on the shipped CSS and script: middle cut (`External…/optimisation_set.star` at 180 px), re-cut
  on resize, the hover copy grows each frame to the absolute path, a stray scroll leaves it open, leaving closes it;
  the checkbox's bottom edge on its label's baseline (0 px).
- Owed: the real app after a restart: a pane under 860 px, a running log beside a hovered path, screenshots.
