# Unified Per-Tomogram Dashboard — Roadmap

**This doc supersedes everything that was in this file before.** It's the plan
for the *unified per-tilt-series dashboard* that replaces three currently-
separate widgets:

- `ui/candidate_preview_dialog.py` — the per-tomo, per-candidate-extract-job
  megawidget. **Almost all of its content carries forward**, but the dialog
  is re-anchored on the tilt-series instead of on the candidate-extract job,
  and renamed.
- `ui/components/ts_journey_view.py` — TS × pipeline-stage status matrix.
  Folds into the new dashboard's sidebar (one journey-pill row per TS).
- `ui/pipeline_builder/pipeline_roster.py::_open_tomo_previews` — the project-
  wide WarpTools tomogram preview grid. Removed; the new dashboard's sidebar
  becomes the discovery surface, and per-TS Reconstruct sections show the
  WarpTools PNG.

`HANDOFF.md` next to this file is **stale** (describes a sprite-cache issue
that's been fixed and a layout that's been overhauled). Disregard it; start
from this ROADMAP.

---

## 1. Vision

A single per-TS dashboard. As pipeline jobs complete, more sections light up.
Each section captures both the *input parameters* and the *output results* of
one job for the selected tilt-series, so the user can mentally debug a TS's
pipeline state from one page.

The user values **density**: monospace, terse, dense per-stage metric chips
in card headers; full visualization below. Don't add chrome unless it's
load-bearing. **First cuts of new sections may be primitive key/value
datadumps — that's fine, the structure is what matters.**

---

## 2. Architecture

### 2.1 Anchor entity

Tilt-series name (string). The dashboard takes a TS, walks
`project_state.jobs`, and asks each section "do you have anything to render
for this TS?"

Section pseudocode contract:
```python
def render_section(ts_name: str, project_state) -> bool:
    """Render the section card for this TS or skip silently.
    Return True if a card was rendered."""
```

### 2.2 Render rules per section

| Job in pipeline? | Job has data for this TS? | Result                                 |
|------------------|---------------------------|----------------------------------------|
| no               | —                         | skip                                   |
| yes              | no (running/pending)      | placeholder card with status pill      |
| yes              | yes                       | full card                              |
| yes              | errored                   | error card with diagnostic + log link  |

Sections never throw — a missing/corrupt input renders an "errored" card
with the exception text. **Never explode the dialog because one section's
job hasn't run.**

### 2.3 Layout

Maximized dialog. Two columns.

- **Sidebar (~300px):** scrollable list of all TS in the project. Each row:
  - tomo name (mono, ellipsized)
  - position label
  - 6-pill journey strip — one pill per pipeline stage, color-coded
    (emerald=ok, amber=running, red=fail, gray=pending). Sidebar mirror of
    the old `ts_journey_view` matrix, condensed to one line per TS.
  - tiny metric line if relevant (e.g., `N=134`).
  Click → load main pane at that TS.

- **Main pane:** vertical scroll of section cards (`cb-section-card`s). One
  per stage. Card structure:
  - **Header:** stage icon + label + 1-line monospace metric strip
    (`bin=4 · 10.0 Å/px · 4096³ · job003`)
  - **Body:** outputs (visualizations, tables, datadumps). Optional
    collapsible "params" details below the metric strip if there's more.
  - **Footer (optional):** "regen this TS" button when a regen action exists
    for the section.

Section cards stack in **pipeline order** (Dataset → FS/CTF → Filter →
Align → CTF → Reconstruct → Template Match → Candidate Extract → Subtomo
Extract → Class3D). User can scroll top-to-bottom and read the TS's pipeline
state in chronological order.

### 2.4 Entry points

- New "Tomogram Dashboard" workspace-sidebar button replacing both
  *Tomogram Previews* and *Tilt Series Journey* buttons. Opens at the first
  TS.
- Per-job *Candidate preview* button (currently in `pipeline_roster.py`):
  deep-links into the dashboard at the first TS the job has data for, with
  the Candidate Extract section scrolled into view (`focus_section=...`).

---

## 3. Section catalog

For each pipeline stage: data source, params metric strip, what we have
today, what to add later. **First-cut sections may be primitive datadumps**
(key/value table from `job_model.*` + the relevant star); structured
analytics are a follow-up.

### 3.1 Dataset / Import
- Source: `project_state.dataset_overview`, `MicroscopeParams`, `AcquisitionParams`
- Metric strip: `10.0 Å/px · 300 kV · 3 e/Å²/tilt · ±60° / 41 tilts`
- Outputs (current): 2-column key/val grid + primitive binning-flow chip strip
- Outputs (planned): consolidated **Pixel / binning sanity panel** (see §11) —
  unifies pixel size, tomo dims (px + Å), and template/extract/subtomo box +
  padding into one table with sanity-check warnings. Replaces the current
  blocky chip+arrow flow.

### 3.2 FS Motion / CTF — `JobType.FS_MOTION_CTF`
- Source: `job_model.*` + per-tilt motion star
- Metric strip: `voxel=4 · dose-weighted · refine`
- **Outputs (current): primitive datadump** — key/value table of params, raw
  star contents
- Outputs (planned): per-tilt motion-trajectory thumbnails, defocus-vs-tilt

### 3.3 Tilt Filter — if job exists
- Outputs (current): per-tilt PNG strip from filter job (already cached on
  disk by the filter job)
- Outputs (planned): kept-vs-dropped band, range histogram

### 3.4 TS Alignment — `JobType.TS_ALIGNMENT`
- Metric strip: `alignment=AreTomo · iter=3`
- Outputs (current): primitive datadump
- Outputs (planned): per-tilt residual plot, alignment heatmap

### 3.5 TS CTF — `JobType.TS_CTF`
- Metric strip: `defocus=2.5–4.0 µm · astig=0.1 · res=8 Å`
- Outputs (current): primitive datadump
- Outputs (planned): defocus-vs-tilt plot, resolution-cutoff histogram

### 3.6 TS Reconstruct — `JobType.TS_RECONSTRUCT` (Reconstruction card)
- Metric strip: `bin=4 · vox=10 Å · 4096×4096×500`
- **Outputs (current): WarpTools tomogram PNG (XY top-down) + X/Z slab. These
  move here from the current Candidate Extract section** (where they live
  today as the "preview pair"). Also: 3dmod copy command for the volume.
- Outputs (planned): per-Z slice strip

### 3.7 Template Workbench / Match — `JobType.TEMPLATEMATCHING`
- Metric strip: `box=64 · pad=128 · θ=15° · symm=C1`
- Outputs (current): primitive datadump of template params
- Outputs (planned): score-volume max-projection PNG (small, downsampled),
  threshold histogram with the extraction threshold marked

### 3.8 Candidate Extract — `JobType.CANDIDATE_EXTRACT` (Picks card)
- Metric strip: `thresh=0.45 · NMS=160 Å · diam=200 Å · N=134 picks`
- **Outputs (current): existing flagship section is preserved unchanged**:
  - Pick gallery (CSS-sprite atlas, sort best/worst/Z, click-to-select)
  - Per-tile metadata (rank, idx, z, score) + browser tooltip
  - **NB**: the preview pair (XY+XZ slices with hover marker, ghost overlay,
    "show all picks" toggle) **moves to Reconstruction (§3.6)** in this
    refactor. The hover marker is still gallery-driven, but now the gallery
    and the preview pair live in *different* section cards. JS delegation
    needs to find both markers across the document — the existing
    document-scoped `getElementById(host_id)` lookup works fine, just don't
    scope it to the gallery's parent.
  - Picks scatter fallback (XY+XZ Plotly + score histogram) when no subtomo
    atlas is available.
  - Hover-pick details card.
- Outputs (planned): cross-link to subtomo_extract section

### 3.9 Subtomo Extract — `JobType.SUBTOMO_EXTRACTION`
- Metric strip: `box=96 · pad=2 · normalize=True`
- Outputs (current): the cutout atlas — already feeds the gallery in
  Candidate Extract via `services/visualization/subtomo_link.py`. Surface
  the atlas + per-pick failure list here too as a self-contained card.
- Outputs (planned): per-class average, box visualization

### 3.10 Class3D / Refine — defer
- Pure datadump first.

---

## 4. Architectural principles (LOCKED)

These were debated and settled across several sessions. Don't relitigate
without a strong reason. The first six are carried verbatim from the old
ROADMAP §3; §§4.7–4.9 are new contracts earned in the session that wrote
this file.

### 4.1 No browser-side volumetric rendering for picks
**Don't** ship Z-MIP / orthoslab / per-pick stamp pipelines from the *raw
tomogram volume*. Cryo-ET reconstructions have missing-wedge streak
artifacts that dominate any 2D projection — particle SNR can't survive.
Plus reading 2 GB MRC files from Lustre is ~30 s per tomogram, unscalable
to projects with 100s of tomograms. Use star-file metadata + the
WarpTools-rendered PNG (already on disk from ts_reconstruct) + 3dmod
handoff for actual 3D inspection.

### 4.2 Plotly via dict, not the Python package
`ui.plotly()` accepts a dict in Plotly's JSON spec. Construct dicts
directly — no `import plotly.graph_objects` needed. Keeps the Python
dependency surface small and lets us serialize figures to JSON for caching.

### 4.3 Aspect-ratio containers, not Plotly scaleanchor
`scaleanchor: "x"` forces the *data area* to be square inside whatever
container Plotly is given, producing 60% empty horizontal space when the
container is wide. Set `aspect-ratio: x/y` on the wrapper div instead and
let Plotly fill it. Cap the aspect (e.g., `min(2.5, max(0.5, x/y))`) so
pathologically flat tomograms don't produce 41:1 strips.

### 4.4 Manifest versions are cache invalidators
Each manifest schema bump (`MANIFEST_VERSION = N`) automatically invalidates
prior caches because `prior_entries` only loads if the version matches.
Bump the version on any field add/remove/rename.

### 4.5 Mmap view trap (numpy)
`np.asarray(m.data, dtype=np.float32)` returns a *view* of the mmap when
the dtype already matches; the view is invalid the moment the
`with mrcfile.mmap(...)` block closes. Always `np.array(..., copy=True)`
or do all work inside the with-block. This bug cost an entire session —
silently all-zero panels on `_f32.mrc` files.

### 4.6 NiceGUI 3.x requires `sanitize=False` on `ui.html()`
Without it, SVG / canvas / button tags get stripped.

### 4.7 Asset URLs cache-busted by mtime
The `/api/vis-asset` endpoint serves PNG with `Cache-Control: public,
max-age=300` and JSON with `Cache-Control: no-cache`. The UI builds asset
URLs as `/api/vis-asset?path=...&v={mtime}` so regen of an asset
invalidates client cache automatically. See `_vis_asset_url` in the
current `candidate_preview_dialog.py`. Don't break this.

### 4.8 IMOD-up Y/Z convention
WarpTools-emitted XY tomogram PNG and the X/Z slab preview both place
y=0 / z=0 at the *bottom* of the image (IMOD-up convention). For
DOM-top positioning of pick markers / ghosts, invert:
`top_frac = 1 - y/y_dim` (and likewise for z). Confirmed visually by the
user in the session that wrote this doc.

### 4.9 Aspect-ratio matched preview boxes (no letterbox)
For preview slices that need pick overlays, set the wrapper's
`aspect-ratio: x_dim/y_dim` (or `x/z`) inline so the image fills the box
exactly — no `object-fit: contain` letterbox. This makes percentage-based
marker / ghost positioning pixel-accurate without JS measurement.

---

## 5. Implementation slices

Each slice is shippable on its own.

### Slice A — Scaffold & entry-point unification
- Rename `ui/candidate_preview_dialog.py` → `ui/tomo_dashboard_dialog.py`.
- Public entry: `open_tomo_dashboard(project_state, ts_name=None,
  focus_section=None)`.
- Sidebar: per-TS list with journey pill strip (port from
  `ts_journey_view._collect_journey_data`).
- Main pane: re-anchor the section stack on the selected TS rather than
  on the candidate-extract job. **Carry forward all current sections.**
- Keep both old entry points working as deep-links into the new dashboard:
  - "Tomogram Previews" sidebar button → opens dashboard at first TS.
  - Per-job "Candidate preview" button → opens dashboard, focuses the
    Candidate Extract section.
- Remove `_open_tomo_previews` body and `ts_journey_view` standalone wiring.

### Slice B — Reconstruction section split
- Move the XY+XZ preview pair, ghost overlay, hover marker, "show all
  picks" toggle, and the 3dmod block out of Candidate Extract into a new
  Reconstruction section card.
- Candidate Extract retains: gallery + scatter fallback + tile metadata +
  hover-pick details.
- Hover marker JS still finds both markers; query is now document-scoped
  rather than scoped to the gallery's parent (already document-scoped via
  `getElementById`, so no real change).

### Slice C — Datadump sections
For each of: Dataset, FS Motion/CTF, Tilt Filter, TS Alignment, TS CTF,
Template Match, Subtomo Extract — add a section emitter that renders a
key/value table and (where cheap) a small primitive plot. **No new
manifests required for these — read directly from `job_model.*` + the
job's relevant star file.** This is the "structure first" pass.

### Slice D — Articulated analytics
Replace datadumps with proper visualizations stage-by-stage. Priority order:
Reconstruct (per-Z slice strip) → CTF (defocus-vs-tilt) → Template Match
(score MIP) → Tilt Filter (kept/dropped band) → Alignment (residuals).

### Slice E — Per-section regen
Each section grows a "regen this TS" button. Server-side: rebuild that
one tomo entry in the manifest, write back. UI: refresh just the section
in place. (Today's `Render new` / `Force` buttons regen the whole job —
keep them as bulk affordances.)

---

## 6. File / module layout (target after Slice C)

```
services/visualization/
  preview_render.py            -- render emitters (atlas, xz_slab, picks.json)
  preview_orchestrator.py      -- per-job manifest writer
  subtomo_link.py              -- pick ↔ subtomo cross-job join
  imod_vis.py                  -- IMOD .mod model gen
  manifest_schema.py           -- shared MANIFEST_VERSION + helpers
  section_dataset.py
  section_fs_motion_ctf.py
  section_tilt_filter.py
  section_ts_alignment.py
  section_ts_ctf.py
  section_reconstruct.py
  section_template_match.py
  section_candidate_extract.py
  section_subtomo_extract.py
  ROADMAP.md
ui/
  tomo_dashboard_dialog.py     -- the unified per-TS dashboard
  sections/                    -- UI renderers per section
    candidate_extract.py
    reconstruct.py
    fs_motion_ctf.py
    ...
```

---

## 7. Data sources cheat-sheet

| Stage              | Params source                                       | Outputs source                                                          |
|--------------------|-----------------------------------------------------|-------------------------------------------------------------------------|
| Dataset            | `project_state.dataset_overview`, MicroscopeParams, AcquisitionParams | dataset star                                                |
| FS Motion/CTF      | `job_model.*`                                       | `<job>/Motion/*.star`                                                   |
| Tilt Filter        | `job_model.*`                                       | `<job>/filtered/*.star`, `<job>/filtered/png/*.png`                     |
| TS Alignment       | `job_model.alignment_method`                        | `<job>/aligned_tilt_series.star`, IMOD `.xf`                            |
| TS CTF             | `job_model.*`                                       | `<job>/ts_ctf.star`                                                     |
| TS Reconstruct     | `job_model.binning`, `job_model.box_size`           | `<job>/warp_tiltseries/reconstruction/<tomo>.png`, `<tomo>.mrc`         |
| Template Match     | `job_model.box_px`, `padding`, `angle_step`         | `<job>/score_<tomo>.mrc`, `match_metadata.star`                         |
| Candidate Extract  | `job_model.threshold`, `nms_distance_ang`, `particle_diameter_ang` | `<job>/candidates.star`, `<job>/vis/preview/<tomo>/*` |
| Subtomo Extract    | `job_model.box_px`, `normalize`                     | `<job>/<tomo>/particles.star`, `<tomo>/*.mrcs`                          |

(Confirm exact field names against `services/job_models.py` when wiring;
the table above is approximate.)

---

## 8. State-of-the-world snapshot

What works in `ui/candidate_preview_dialog.py` as of the session that
wrote this roadmap. **All of these carry forward** into the new dashboard
through Slices A and B:

- ✅ Sidebar tomo list (per-row N picks + score range, status badges)
- ✅ Compact sidebar toolbar: `Render new` / `Force` / `IMOD` buttons with
  tooltips that explain the difference (incremental vs cache-busting; see
  §5/Slice E for plans to add per-TS regen alongside these)
- ✅ Stacked XY + X/Z preview slices, aspect-ratio matched (§4.9)
- ✅ Faint always-on pick ghost overlay on both slices (server-rendered as
  absolute-positioned divs at percent coords)
- ✅ Hover marker driven by gallery tile mouseover, synchronized across
  both slices, IMOD-up Y/Z convention (§4.8)
- ✅ "Show all picks" checkbox toggles ghost visibility via single class
  on the left column
- ✅ Subtomo cutout gallery (CSS sprite atlas, sort best/worst/Z,
  click-to-select). Atlas is rendered server-side via
  `render_pick_cutouts_atlas` (192-px source tiles, 96-px display)
- ✅ Per-tile metadata in corners (rank TL, idx TR, z BL, score BR) +
  native browser title tooltip with full pick info
- ✅ Picks scatter fallback (XY + XZ + score histogram) when no subtomo
  atlas exists for the tomo
- ✅ Hover-pick details card (idx / px / Å / score / z%-tile / NN distance)
- ✅ 3dmod copy command per tomo
- ✅ `/api/vis-asset` mtime cache-bust + per-mime cache headers (§4.7)
- ✅ Floating top-right close button (no header bar chrome)

Manifest version: `MANIFEST_VERSION = 9` in
`services/visualization/preview_orchestrator.py`.

---

## 9. Open design questions for next session

- Sidebar journey pill strip: which 6 stages get pills? Best guess:
  FS/CTF, Align, CTF, Recon, Pick, Subtomo. Confirm with user.
- "Compare TS side-by-side" mode? Defer.
- Per-section regen vs. global regen — keep both? (Plan: yes, see §5/E.)
- Project-level aggregate KPIs (Phase 4 of old roadmap) — defer; user
  prefers density and per-TS detail over project rollups for now.
- `ts_journey_view`'s click-to-expand-logs interaction: where does it go
  in the new UI? Probably as a "show logs" link inside each section
  card's footer, scoped to that stage.

---

## 10. Implementation log

### Slice A — done
- `ui/tomo_dashboard_dialog.py` created. Public entry
  `open_tomo_dashboard(ts_name=None, focus_section=None)`. Sidebar with
  6-pill journey strip (FS/CTF · Align · CTF · Recon · Pick · Subtomo);
  main pane re-anchored on the selected TS; Candidate Extract carries
  forward as one stacked card per matching candidate-extract instance
  (multi-species → multiple cards).
- `pipeline_roster.py`: three sidebar buttons (Tomogram Previews, Tilt
  Series Journey, Candidate Previews) collapsed into a single **Tomogram
  Dashboard** entry point with green-dot indicator when previews exist.
- `workspace_page.py`: stripped journey-mode wiring; only pipeline /
  workbench modes remain.
- `ui/candidate_preview_dialog.py` and `ui/components/ts_journey_view.py`
  deleted (their content rolled into the new file).
- **Journey-pill bug fix.** Legacy projects without `.task_manifest.json`
  per array job now fall back to the stage's primary output star
  (`fs_motion_and_ctf.star`, `aligned_tilt_series.star`,
  `ts_ctf_tilt_series.star`, `tomograms.star`) plus the job's
  `execution_status` to populate the pills. So pre-array-tracker projects
  show real ok/fail status across all 4 array stages.

### Slice C — partial
Done:
- **Dataset section** — 2-column key/val grid + binning-flow chip strip.
  *To be reworked per §11.*
- **FS Motion / CTF** — scatter plots for Defocus U/V + astigmatism with
  frame-name hover and tooltip explainers. Motion / CTF max-res / FOM are
  gated on `_is_meaningful_series` (skipped with a note when WarpTools
  wrote `1e-6` placeholders — see `project_warp_relion_star_placeholders`
  memory).
- **Tilt Filter** — kept/dropped stat strip + drop list (frame name + nominal
  angle). Auto-detects standalone-tool output at `<project>/TiltFilter/` vs
  pipeline-job output at `<job_dir>/filtered/`.
- **TS Alignment** — refined-shift plot (markers only — even smoothly-
  varying per-tilt metrics get zigzag from outliers; user corrected this
  preference) + refined alignment angles, with hover identity and
  validation.
- **TS CTF (post-alignment)** — same defocus / astig scatter as FS Motion /
  CTF, post-alignment refit values.

Carried-over plot principles (see `feedback_dashboard_plot_principles.md`):
- All per-tilt scatter → markers only, never lines (including alignment
  shifts/angles — user corrected the earlier "smooth so lines are fine"
  exception). Only cumulative/accumulated quantities are even candidates
  for lines, and even then default to markers.
- Validate every column with `_is_meaningful_series` before plotting
  (~1e-3 magnitude threshold filters WarpTools placeholder writes).
- Hover always carries `[tilt_index, frame_basename]` customdata so the
  user can identify the specific tilt — angle alone is ambiguous.
- Tooltip-explain non-obvious metrics on the plot title (`info_outline`
  icon + Quasar tooltip).

Not yet done (Slice C original scope):
- §3.7 Template Workbench / Match section card (datadump only).
- §3.9 Subtomo Extract dedicated section (currently surfaces only via the
  Candidate Extract gallery cross-link).

### §11 Pixel / binning sanity panel — done

- `_compute_pixel_chain(project_state)` walks Camera → FS-CTF → Filter →
  Align → TS CTF → Recon → TM → Pick → Subtomo. Multi-instance fan-out for
  TM / Pick / Subtomo: one row per species with a colored stripe in the
  stage column.
- `_apply_sanity_rules(rows)` flags violations as per-cell warnings:
  - **Box vs particle Ø** outside 1.5–3× (error if < 1.5×, warn if > 3×)
    on TM and Subtomo rows. Uses each species' candidate-extract
    `particle_diameter_ang` (joined by species_id) as the reference.
  - **Particle Ø consistency** across multiple candidate-extract instances
    for the same species — flags both rows.
  - **Template px ≠ recon px** (>5% mismatch) — error on the TM row;
    silently mismatched template px is exactly the failure mode the user
    flagged in the §11 brief.
  - **Subtomo crop > box** — invalid padding, error on the Subtomo row.
  - **Subtomo crop (Å) < particle Ø (Å)** — particle clipped out of the
    cropped output cube; error on the Subtomo row.
- `_render_pixel_sanity_table(rows)` renders a 7-column CSS Grid (stage ·
  Å/px · tomo (px) · tomo (Å) · box/pad/Ø · box (Å) · notes). Warnings
  inline as info-icons next to the offending cell with a tooltip
  explaining the violation.

#### Deviations from §11 spec

These are recorded so the spec table in §11 doesn't get cargo-culted in a
later refactor:

- **TemplateMatchPytomParams has no `box_px` / `padding` / `angle_step`
  fields.** PyTOM's template box is implicit in the template volume; we
  read it from `ParticleSpecies.workbench.box_size` (px) and `pixel_size`
  (Å) instead. `angular_search` is a string-typed field used for the
  notes column. The §11 spec's "box=64 · pad=128" example doesn't map to
  any real field — TM rows show `box=<workbench.box_size>` only.
- **CandidateExtractPytomParams has no `nms_distance_ang`.** NMS distance
  isn't a user param; it's derived inside PyTOM. The Pick rows show
  `Ø=<particle_diameter_ang>`, `<cutoff_method>=<cutoff_value>`, and
  `max N=<max_num_particles>` instead of the §11-spec NMS field.
- **SubtomoExtractionParams has no `padding` field.** It has `box_size`
  and `crop_size`; the per-side pad rim is derived as `(box-crop)/2`. The
  Subtomo row's `pad_px` reflects this rim, not a separate user param.
- **Layout: full-width below the keyval grid, not in the right column.**
  The 7-column table doesn't fit at the dataset card's prior 320px
  right-column width. Replaced the `cb-dataset-row` flex split: the
  microscope/acquisition keyval grid runs above, the sanity table runs
  full-width below.

---

## 11. Next session priority — Pixel/binning sanity panel

**Why this is urgent.** The user's most common pipeline failure is
pixel-arithmetic mistakes — under/overestimating template box size or
extraction padding because the binning math at each stage is hard to track
in your head. Concretely the user said: *"this is a crucial point of failure
for me so far i.e. that i sometimes over/underestimate my templates size
and paddings etc. and the picks are shit (sometimes because im genuinely
not sure, but often just because i do the binning/pixel arithmetic
incorrectly...)."*

The current Dataset binning-flow chip strip is *directionally right* but
too primitive: blocky chip + arrow chrome eats vertical space, only carries
pixel size, and isn't co-located with the box/padding params for template
matching, candidate extract, and subtomo extract — which is precisely
where the arithmetic matters.

### What to build

A **Pixel / binning sanity panel** that lives where the chip-flow lives now
(inside the Dataset card, right column). One compact monospace table with
columns:

| Stage | px size (Å) | Tomo dim (px) | Tomo dim (Å) | Box / pad | Box (Å) | Notes |
|---|---|---|---|---|---|---|
| Camera | 1.55 | 4096 × 4096 | 6349 × 6349 | — | — | |
| FS-CTF | 1.55 | (frame avg) | — | — | — | |
| Filter | 1.55 | — | — | — | — | |
| Align | 12.0 | (rescaled) | — | — | — | rescale ÷7.7 |
| TS CTF | 12.0 | — | — | — | — | |
| Recon | 12.0 | 512×512×256 | 6144×6144×3072 | — | — | |
| Template Match | 12.0 | — | — | box=64 · pad=128 | 768 · 1536 | θ=15° symm=C1 |
| Candidate Extract | 12.0 | — | — | NMS=160 Å · diam=200 Å · thresh=0.45 | — | particle 200 Å ≈ 17 px |
| Subtomo Extract | 6.0 | — | — | box=96 · pad=2 | 576 · 12 | re-extracted at bin 4 |

Sanity-check rules to flag inline (with colored icon next to the offending
cell):
- **Box-size vs particle diameter.** Template / extract box should be
  ~1.5–3× particle diameter in Å. Smaller → particle won't fit; larger →
  wasted compute.
- **Padding vs box.** Padding < 0 is impossible; padding > box is unusual.
- **Particle diameter consistency.** Particle diameter (Å) declared at
  template-match, candidate-extract, subtomo-extract should agree. If they
  diverge, the user almost certainly made a binning mistake.
- **Pixel-size match across template ↔ recon.** Template-match operates on
  the reconstructed tomogram; if `template_match.pixel_size != recon.pixel_size`
  silently, picks are garbage.
- **Subtomo box vs candidate diameter.** Subtomo box should encompass the
  particle with reasonable margin.

### Implementation sketch

1. Build `_compute_pixel_chain(project_state)` returning a list of
   `{stage, px_size_ang, tomo_px, tomo_ang, box_px, box_ang, pad_px, pad_ang,
   particle_diameter_ang, warnings: [...]}` dicts. Pull from job param
   classes:
   - `FsMotionCtfParams`, `TiltFilterParams` — no own px change
   - `TsAlignmentParams.rescale_angpixs` + `tomo_dimensions`
   - `TsReconstructParams.rescale_angpixs`
   - `TemplateMatchPytomParams` — verify field names (`box_px`, `padding`,
     `angle_step`, `particle_diameter_ang`, `symmetry`)
   - `CandidateExtractPytomParams.threshold`, `nms_distance_ang`,
     `particle_diameter_ang`
   - `SubtomoExtractionParams.box_px`, `padding`, `normalize`
2. Replace `_render_binning_chain` with `_render_pixel_sanity_table` —
   monospace table, dense, no chip/arrow chrome. Use the existing
   `cb-datadump-grid` styling extended to N columns.
3. Sanity rules as small functions returning warning strings; render as
   inline `info_outline` (green) / `warning_amber` / `error` icons next to
   the offending cell with a tooltip explaining the violation.
4. Per-instance variants: when there are multiple template-match /
   candidate-extract / subtomo-extract instances (multi-species), the table
   gets one row per instance for those stages. Group visually by species.

This subsumes the "Template Match params datadump" and "Candidate Extract
metric strip" content from §3.7 / §3.8 — the param info moves to the
pixel-chain table at the top of the dashboard, and the per-instance
section cards focus on outputs (picks, atlas, score volumes).

---

## 12. Other items deferred from this session

- **WarpTools XML metadata.** Real per-tilt motion + CTF max-res + FOM
  values live in WarpTools' own XML files (`warp_frameseries/*.xml`,
  `warp_tiltseries/*.xml`), not in the RELION star export. User pointed
  at the container directory:
  `/groups/klumpe/software/containers_and_definitions/`
  with `ais.sif`, `easymode.sif`, `pom_cryoet.sif` — likely relevant
  for parsing those XMLs (or for finding the WarpTools binary that emits
  them). Worth exploring: parse `warp_frameseries/*.xml` directly to
  surface real motion + CTF curves. If yes, the FS Motion / CTF and TS
  CTF sections grow back the plots they currently skip.
- **Slice B (Reconstruction card).** Hoist WarpTools PNG + X/Z slab +
  ghost overlay + "show all picks" toggle + 3dmod block out of Candidate
  Extract into its own Reconstruct section card. Still useful when the
  user has a reconstruct job but no candidate-extract yet.
- **Slice D analytics.** Once the pixel-sanity panel and WarpTools-XML
  story are settled: per-Z slice strip for Reconstruct, defocus-vs-tilt
  theoretical-fit overlay, alignment residual heatmap, score-volume MIP
  for Template Match.
- **Slice E per-section regen.** Currently regen buttons in Candidate
  Extract scope to the whole job. Add a per-TS regen that just rebuilds
  that one tomo entry in the manifest.
- **Project-level rollups in the sidebar.** Mark sidebar TS rows red when
  their median CTF-res is in the project's worst 10%, or their max
  alignment shift exceeds project median + 2σ — outlier-detection at the
  list level so the user finds bad TS without scrolling each one.

---

*Last updated 2026-05-08 — Slice A complete, Slice C partial, §11 Pixel /
binning sanity panel done (deviations from spec captured at the bottom of
§10). Next session opens with the remaining Slice C scope (§3.7 Template
Match section card, §3.9 Subtomo Extract section card) and §12 — WarpTools
XML metadata parsing for real motion + CTF curves. Update this doc in any
PR that overrides anything in §4 or changes the section catalog.*
