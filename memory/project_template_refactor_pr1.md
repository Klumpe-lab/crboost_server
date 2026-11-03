---
name: Template refactor — schema v3 (multi-template, decoupled masks) landed (2026-05-11)
description: Tracks template-first-class refactor progress — v2 landed 2026-05-10 then re-cut as v3 on 2026-05-11. v3 decouples masks from templates (sibling collections on species), adds UUID identity with sidecar .meta.json, drops seeds entirely (relion_mask_create threshold path is enough), append-not-replace semantics for all generation flows. TM job plugin gets template + mask dropdowns. Hard cut from v2 fields. Molstar Slice A (Python bridge: ready-gate queue + optimistic visibility + dedup + hard-reset iframe remount) landed 2026-05-12; Slices B (embed.js) + C (UI affordances) still pending.
type: project
originSessionId: 465fa000-162c-4ae7-abde-5f7927a77ab6
---
Per `TEMPLATE_REGISTRY_REFACTOR_PLAN_V2.md`. User explicitly asked to do
this without artificial PR boundaries — kept changes additive throughout.

**Landed 2026-05-10:**

Schema + migration (`services/project_state.py`):
- SCHEMA_VERSION (1,0) → (2,0).
- New models: `TemplateMask`, `ParticleTemplate`, `TemplateWorkbenchUIState`.
- `TemplateMask` carries the `relion_mask_create` knobs as first-class
  fields: `threshold`, `extend_pixels`, `soft_edge_pixels`, `lowpass_ang`,
  plus `method` (Literal: spherical/cylindrical/relion/manual/imported).
- `ParticleTemplate` includes `imported_from` (original path retained
  when user drops in a third-party .mrc), `source` (PDB/EMDB id or
  free-form), `created_at`, `notes`, `polarity` (typed Literal),
  `lowpass_resolution_ang` (None = unfiltered).
- `ParticleSpecies` extended additively with `diameter_ang`, `symmetry`,
  `notes`, `template`, `workbench_ui` (v1 fields kept).
- `_migrate_v1_to_v2(data)` — idempotent, version-guarded; populates
  `species.template` from v1 fields, lifts particle_diameter_ang +
  symmetry off jobs onto species, polarity inferred from filename.
- `ProjectState.load()` writes `<file>.v1.bak` once before migration.
- Templates' apix/box are deliberately NOT cached on the model — read
  on demand from MRC header.

Path resolution (`services/path_resolution_service.py`):
- `get_context_paths` for TEMPLATE_MATCH_PYTOM dual-reads with v1-semantic
  preservation: per-job `tm_jm.template_path`/`mask_path` win when
  explicitly set, otherwise fall back to `species.template`.

Shared utilities:
- `services/templating/template_metadata.py`: `read_template_header(path)`
  with mtime-keyed cache returning `TemplateHeader(apix_ang, box_px, nx,
  ny, nz)`. Plus `get_effective_template_path` / `get_effective_mask_path`
  v2-prefer / v1-fallback resolvers.
- `services/templating/mrc_inspection.py`: `inspect_mrc_for_import(path)`
  returns rich `MrcInspection` dataclass — header geometry, mode/dtype,
  full-volume statistics, polarity inference (central vs overall mean),
  mask-likeness (binary distribution test), free-text MRC labels, and
  best-effort regex-pulled provenance hints (PDB id, EMDB id, tool
  name). Building block for the workbench's "the PI handed me an .mrc"
  import flow.

UI component (`ui/components/template_summary_card.py`):
- `render_template_summary_card(species, *, on_edit=None, compact=False)`.
- Shows polarity chip (white/black), template filename, apix + box from
  MRC header, lowpass-applied status, mask path + method chip + relion
  knobs (threshold/extend/soft/lowpass), particle diameter + symmetry,
  source + imported_from + notes. Compact variant for inlining.

UI surfaces flipped:
- `ui/job_plugins/template_match.py`: summary card embedded above the
  legacy path pickers. Pickers relabeled "Per-job override (legacy)";
  semantic preserved by path_resolution v1-priority.
- `ui/job_plugins/candidate_extract.py`: summary card embedded.
- `ui/tomo_dashboard_dialog.py` pixel-sanity panel: TM block reads
  template path via `get_effective_template_path(species)` + MRC header.
  Symmetry prefers `species.symmetry`. Candidate-extract block prefers
  `species.diameter_ang` over `ce_jm.particle_diameter_ang`. Local
  `_TEMPLATE_HEADER_CACHE` removed; thin wrapper around shared util.

**Verified (Claude sandbox):**
- py_compile clean across all touched files.
- `ruff check` on all new files: 0 violations. Repo baseline of 177
  unchanged on touched files.
- Standalone tests on _migrate_v1_to_v2 (6 cases), template_metadata
  helpers (5 cases), and `_infer_provenance` regex (PDB/EMDB/tool
  extraction) all pass.

**NOT verified (blocked on user's `loadpython` env):**
- Live UI render of the summary card.
- End-to-end project load → migrate → render.
- `inspect_mrc_for_import` on a real .mrc (needs numpy + mrcfile in env;
  can run from interactive shell).

**Also landed 2026-05-10 (the user-facing pieces from the workbench redesign sketch):**

`ui/template_import_dialog.py` — `await open_template_import_dialog(project_path, species, initial_path=None)`.
File-picker → `inspect_mrc_for_import` → editable form (polarity, source
auto-suggested from inferred PDB/EMDB id, lowpass, notes) with banners for:
mask-likeness ("looks like a mask, not a template — confirm?"),
non-cube volumes, replace-existing-template warnings. Copies the file
into `templates/<species_id>/` (collision-safe with `_2`, `_3` suffix),
returns a populated `ParticleTemplate` with `imported_from = original
path`. Returns None on cancel. Caller writes to `species.template` and
saves project state.

`ui/components/template_viewer.py` — `render_template_viewer(template_path,
mask_path=None, *, height_px=280, show_mask_default=True)`. Orthoslice
triplet (XY/XZ/YZ at center) using NiceGUI + plotly heatmap-via-dict.
Per-axis slice sliders, mask overlay toggle (overlay drawn as plotly
contour at value=0.5 in amber), per-process mtime-keyed volume cache so
slider drags don't re-mmap. Aspect-ratio CSS, NOT scaleanchor (per the
visualization-stack-patterns memory). Returns a controller with
`update_paths(template_path, mask_path)` for swap-without-remount.
Replaces molstar entirely.

**Hand-off doc for next session:**
`TEMPLATE_WORKBENCH_RESTRUCTURE_HANDOFF.md` at the repo root contains
the comprehensive plan for the workbench restructure plus all the
remaining integration points. Read that first; the doc lists exact
files, the per-flow rewrite recipe, gotchas (HPC env, no NiceGUI
hot-reload, plugin loader behavior, dual-read v1-priority semantics),
and a green-light UI checklist for testing. Don't re-derive from
scratch.

**Also landed 2026-05-10 (workbench restructure session):**

`ui/template_workbench.py` rewritten (1062 → ~720 lines). Five-panel
layout: summary card → particle metadata editor (inline below summary,
per user choice over right-sidebar/own-tab) → source flow tabs
(Basic Shape / PDB-EMDB / Import / Edit Current) → mask editor
(RELION + Import) → orthoslice viewer. Public constructor signature
preserved.

Write-through pattern landed: every generation flow's success path
calls `_register_template(...)` which builds a `ParticleTemplate` and
assigns it to `species.template` directly, plus mirrors to v1
`template_path`/`mask_path` for one cycle. After-register hook
re-renders the summary card and calls `viewer_controller.update_paths`
so the user sees the new template instantly without a reload (#1
visible pain point eliminated).

Generation flows wired:
- Basic shape: dual `_white.mrc`/`_black.mrc` written (per user choice
  to keep both side-by-side on disk); polarity selector picks which
  one is registered. Source tagged `basic_shape:<x:y:z>`.
- PDB: combined Fetch + Simulate button (no click-to-select dance).
- EMDB: combined Fetch + Resample. Source tagged `PDB:<id>` /
  `EMDB-<id>`.
- Import: delegates to `open_template_import_dialog`; result is
  written via `_set_template_object`.
- Edit Current: source / lowpass / notes editing; **flip-polarity
  button** (per user choice) prefers a `_white`/`_black` sibling on
  disk, falls back to writing a negated MRC via numpy + mrcfile if
  no sibling exists (handles imported files with no sibling).

Mask editor wired via `_attach_mask`: RELION mask flow records
TemplateMask with `method="relion"` + four knobs; Import flow records
`method="imported"`. Spherical / cylindrical generators not yet
implemented (Literal in schema supports them; UI tabs deferred).

Molstar deletion: `MOLSTAR_EMBED_HTML`, `/molstar-workbench` route,
`/api/file` route, `_post_to_viewer`, `_handle_viewer_event`, color
palette, session-tray, file-list, click-to-select selection labels —
all gone. /api/file confirmed unused elsewhere via grep before
deletion.

**REVERTED 2026-05-11 — molstar restored, layout reworked:** User
pushed back on the post-restructure UX: too blocky, empty right halves
on every panel, inconsistent icon/font sizing, particle metadata
visually divorced from the species, slice viewer slider drag taking ~2s
per re-render (unusable). Most importantly: **slice viewer is not a
replacement for molstar** — user explicitly asked to bring molstar back
as the primary viewer. Reverted the molstar deletion and reorganized
the workbench layout in the same session:

- `MOLSTAR_EMBED_HTML`, `/molstar-workbench`, `/api/file`, color
  palette, `_post_to_viewer`/`_handle_viewer_event`, and the
  session-tray are restored at module load.
- Viewer panel now has a mode toggle (molstar / slice fallback). Slice
  viewer kept behind the toggle for cases where molstar fails.
- Generation flow success → clears molstar session + posts
  `load_volume` for the new template/mask. Same trigger on viewer-mode
  switch.
- Layout reworked: (a) thin species header strip with diameter /
  symmetry / notes inline at the top (replacing the standalone v2
  summary card); (b) registered-templates card row designed for
  multi-template growth even though schema is singular today;
  (c) per-flow geometry inputs (apix / box / lowpass) live INSIDE
  each tab that actually resamples — they used to share a bar at the
  top that misleadingly suggested they applied to Import / Edit too
  (which they didn't).
- Style constants (`_HDR_ICON_SIZE`, `_HDR_LABEL_CLS`, etc.) at module
  top so all panels match. Removed per-panel ad-hoc sizing.
- Mask panel header now shows `derived from <template-name>` so the
  source relationship is visible (was just "records knobs onto
  species.template.mask" operational text before).

**Species drives jobs (creation-time defaulting):** Per user choice,
`pipeline_builder_panel.py:add_instance_to_pipeline` now copies
`species.symmetry` → `job_model.symmetry` for new TM jobs and
`species.diameter_ang` → `job_model.particle_diameter_ang` for new
candidate-extract jobs. Existing jobs don't auto-update — user can
still override per-job. Driver code untouched (reads from params as
before). This was the user's preferred semantics for v2.

**Import dialog UX:** Added `ui.spinner("dots", size="sm")` during
the synchronous MRC inspection (which can take seconds for large
volumes — was previously a silent freeze). Inspection moved to
`asyncio.to_thread` so the spinner can paint. Renamed
"Metadata (editable)" section to "Template record (your project
bookkeeping)" with a hint that these are free-form fields stored
alongside the template, NOT MRC header values, and don't affect
job execution or modify the .mrc file. Header values live in the
"Read from header" block above.

**Open: how species values flow to jobs** is currently snapshot-at-
creation. If user wants live "species edit auto-updates existing
jobs" semantics, would need either (a) propagate-on-species-edit
hook in the workbench, or (b) read species at driver time
(`get_driver_context` override). Punted for now per user's choice
in original AskUserQuestion.

Subtomo job-config (`ui/job_plugins/subtomo_extraction.py`) wired:
v2 summary card embedded; inline box-vs-diameter sanity warning that
mirrors the dashboard's rule (`box_ang/diameter_ang ratio` in
[1.5, 3.0]). Per user choice both inline + dashboard rules are kept.
Save handler wraps to re-render the warning when box_size / binning
changes.

Verified clean via py_compile + ruff (with HPC env override) — 0
violations on both touched files. Live UI render still requires
user's interactive `loadpython` env.

**Re-cut as v3 on 2026-05-11 (this session):** User pushed back on the
v2 design — couplings and visual issues. Key changes vs v2:

- **Masks decoupled from templates.** `species.template.mask` removed.
  Now `species.templates: List[ParticleTemplate]` and
  `species.masks: List[TemplateMask]` are sibling collections; each
  entry carries a UUID4. `species.selected_template_id` and
  `species.selected_mask_id` pick the active one.
- **UUID + sidecar `.meta.json`** for stable identity across renames.
  `sidecar_ensure(path, kind)` reads existing UUID or creates one and
  writes `<file>.meta.json` ({"id": uuid, "kind": "template"|"mask"}).
- **Seeds dropped entirely.** `*_seed.mrc` files are no longer
  first-class. The seed pathway was a precision-vs-fidelity tradeoff
  (exact ellipsoid mask vs masking the smoothed white density);
  matching the smoothed density is actually more correct since that's
  what TM sees. `auto_infer_seed` removed from workbench_ui.
  `template_service.generate_basic_shape_async` still writes the seed
  intermediate to disk; the workbench just ignores it.
- **APPEND semantics.** Generation flows (basic shape, PDB simulate,
  EMDB resample, import, resample-current, flip-polarity, create-mask)
  all ADD a new entry instead of replacing the current one. Selection
  is preserved unless this is the first registered entry. Basic shape
  + PDB + EMDB register BOTH polarities as separate entries since
  template_service writes both _white and _black. User picks via card
  click.
- **Workbench layout reworked.** Species header (thin strip) →
  Templates section (compact selectable cards, indigo border when
  selected) → Source tabs (text-only labels: shape / pdb / emdb /
  import / edit current; each owns its own apix/box/lowpass) → Masks
  section (parallel card list, purple border when selected; mask
  derivation source shown in the header: "new masks will be derived
  from <selected template name>") → Mask creation tabs (relion /
  import) → Viewer (molstar / slice fallback) → Activity log.
- **Edit-current Resample action.** Major missing piece in v2.
  Resamples the currently-selected template to a user-supplied target
  apix + box (+ lowpass) via `process_volume_async`, writes new
  _white/_black files, appends both as new template entries. Crucial
  for the "imported template at 1.55 Å/px but tomos at 6.2" use case.
- **Tab labels shrunk** — text-only, lowercase, no icons; smaller
  font via `_TAB_LABEL_STYLE`. The "Basic shape" / "Edit current" /
  etc. labels no longer stand out as oversized chrome.

**TM job plugin (v3):** Replaced the "Per-job override (legacy)"
single picker with TWO dropdowns: Template (lists species.templates
with apix / box / polarity in option labels) and Mask (lists
species.masks with method chip; "(none)" option for skip). Defaults
to the species's selected entries; user can still override per-job.
`symmetry` field removed from the renderer (lives in species header).

**Schema migration v2 → v3 (hard cut):**
- SCHEMA_VERSION (2,0) → (3,0).
- `_migrate_v2_to_v3(data, project_root)` in `services/project_state.py`.
- For each species: pull v2 `template` and `template.mask` into the
  new collections; walk `templates/<sid>/` for orphan MRCs and
  register them as templates (or masks if filename ends `_mask.mrc`).
  Skip `_seed.mrc` files. Generate UUIDs via `sidecar_ensure`.
- Hard cut: pop v2 fields (`template`, `template_path`, `mask_path`,
  `workbench`) from the data dict. Pop `auto_infer_seed` from
  `workbench_ui`.
- `.v2.bak` snapshot written once on first migration.
- v1→v2 migration kept (chained: v1→v2→v3 for legacy projects).

**Files touched in v3 rework:**
- `services/project_state.py` — schema models + sidecar helpers +
  v2→v3 migration.
- `services/templating/template_metadata.py` — helpers resolve via
  `species.get_selected_template()` / `get_selected_mask()`.
- `services/path_resolution_service.py` — TM template/mask resolution
  reads from species's selected entries (v3); per-job override still wins.
- `ui/components/template_summary_card.py` — reads via helpers.
- `ui/template_workbench.py` — full rewrite for v3 layout.
- `ui/template_import_dialog.py` — appends instead of replacing;
  sidecar written on ingest.
- `ui/job_plugins/template_match.py` — two dropdowns (template + mask).
- `ui/pipeline_builder/pipeline_builder_panel.py` — species-driven
  defaulting uses v3 helpers (unchanged surface, just the helper
  semantics changed).

**Session 5 polish (2026-05-11):** Delete dialog now uses
`.on('click.stop', ...)` on the X and file-icon inside cards so the
outer card click handler (which calls `_select_template`) doesn't
fire when the user clicks delete — that bubbling re-rendered the card
container and tore down the dialog mid-display. Added a per-card
eye icon (`visibility`) that triggers an explicit `load_volume`;
`_handle_viewer_event` no longer auto-loads selected entries on
`ready` events. Viewer mode toggle no longer auto-loads either.
Card filenames are constrained with `min-w-0 + truncate + flex-1`
to guarantee the eye / X stay visible no matter how long the
filename gets. Tab labels switched to Title Case ("Basic Shape",
"PDB / EMDB", "Edit Current", "RELION Mask", etc.) using Quasar
default tab chrome (dropped custom `_TAB_LABEL_STYLE` font/padding
overrides). Action buttons dropped `size=sm` so they match the
height of `dense outlined` inputs in the same row. Outer column
uses `gap-5` between major groups; Templates+Source and Masks+tabs
are wrapped in inner columns with `gap-2` so paired sections read
as one visual group. Comprehensive cross-session handoff written
to `TEMPLATE_REFACTOR_HANDOFF.md` at the repo root — read first.

**Molstar Slice A landed 2026-05-12** (Python-only bridge robustness;
no embed.js edits). In `ui/template_workbench.py`:
- READY GATE: outgoing posts buffer in `_pending_commands` until iframe
  emits `ready`, then flush in arrival order. Eliminates the silent
  post-before-ready drop. `getItems` bypasses the gate.
- IN-FLIGHT DEDUP: per-(itemId, action) timestamp; identical sends
  within `DEDUP_WINDOW_S = 0.2` are dropped. Applied to
  `setVisibility`, `setColor`, `setIsoValue`, `deleteItem`.
- OPTIMISTIC VISIBILITY: `_optimistic_visibility` map flips icons
  immediately on click; reconciled on `itemsChanged`. Toggle handler
  only updates optimistic state on successful post (returns bool now)
  so dedup-dropped clicks don't create UI/state divergence.
- HARD RESET: `_hard_reset_viewer()` clears bridge state + remounts
  iframe via `src='about:blank'` → original src. Wired to a
  `restart_alt` button next to the existing `delete_sweep` in the
  viewer sidebar header. Use when molstar's internal load queue jams
  on a partially-loaded large volume.
- ERROR ECHO: `error` event handler enriched with `action`/`itemId`
  context if embed.js emits them.

**Slice C landed 2026-05-12** (Python only): file-size badges on
template/mask cards (gray/orange/red by threshold), optimistic
per-load spinners above the session tray with FIFO reconciliation on
itemsChanged, 60s→error→90s evict pruning timer. Sweep + hard reset
also clear pending. Also added: σ-normalize template outputs in
`services/templating/template_service.process_volume_async` (default
flipped to True; shared helper `normalize_white_and_negate_to_black`
used from pdb_service too); auto-select new white after
Resample/Apply-lowpass via `_register_polarity_pair(select_new_white=True)`
default; min/max/σ stats row on every template/mask card (red if
constant, orange if σ outside [0.1, 5]); species delete from the
species header strip with confirmation modal cascading through
template+mask file removal and `os.rmdir` of the species folder
(empty-only — preserves manual drops). Wired via `on_species_deleted`
callback through `species_workbench_panel` → switches to next species
or empty state.

**Molstar source repo discovered 2026-05-12.**
`/users/artem.kushner/dev/template-workbench/` contains the
TypeScript source for our deployed `static/molstar/embed.js`
(diff-q confirmed dist/embed.js is byte-identical). Vite build,
molstar ^4.0.0 dep. Edit `src/embed/bridge.ts` +
`src/lib/core/AlignmentViewer.ts` +
`src/lib/core/representations.ts`, then
`npm run build && cp dist/embed.js ../crboost_server/static/molstar/embed.js`,
then restart main.py. The previous "embed.js is opaque" framing in
the old plan was wrong. `MOLSTAR_VIEWER_PLAN.md` rewritten v2 with
concrete file targets.

**Likely root cause for "resampled black doesn't render"** (not yet
fixed): `AlignmentViewer.loadLocalVolume:282` heuristic
`isInverted = stats.max < |stats.min| * 0.5` evaluates FALSE for our
normalized black template (min≈-2.7, max≈+4.4 — outlier asymmetry).
So iso stays positive when it should be negative. Slice B2 fix:
plumb `polarity` from Python into the load_volume command, kill the
heuristic.

**Slice B still pending** (per `MOLSTAR_VIEWER_PLAN.md` v2):
B1 diagnostic events (loadProgress + volumeStats), B2 polarity
plumbing, B3 slicing API, B4 size guard + memory cleanup. Start with
B1 — it tells us which step silently fails without committing to a
particular fix yet. Slice B addresses the user's biggest pain
(large templates silently fail to render) but needs JS work.

**P2 UX list from `TEMPLATE_REFACTOR_HANDOFF.md` already-landed before
2026-05-12 session.** Handoff doc lists 6 P2 items as pending but on
audit all were already implemented in `ui/template_workbench.py`:
delete-dialog `click.stop`, lazy-load + eye icons, card truncation
with `min-w-0`, button sizing without `size=sm`, Title Case tab
labels using Quasar defaults, gap-5/gap-2 visual grouping. The
workbench module docstring (lines 11-28) describes the final state.

**Still pending (next chunks after molstar):**
1. **Future: per-TM-row template thumbnail** in tomo dashboard
   (forensic context).
2. **Spherical/cylindrical mask generators** — schema supports them
   but workbench mask editor only exposes RELION + Import for now.
3. **Eventual v1 field deletion** — once a couple of user-iteration
   cycles confirm v2 is stable. Specific fields:
   - `ParticleSpecies.template_path`, `mask_path`, `workbench`
   - `TemplateWorkbenchState` model in `services/jobs/_base.py`
   - `TemplateMatchPytomParams.template_path`, `mask_path`, `symmetry`
   - `CandidateExtractPytomParams.particle_diameter_ang`
   - Dual-read v1-priority in `path_resolution_service.py`
   - "Per-job override (legacy)" UI in `template_match.py`
   - v1 mirror writes inside workbench `_register_template` /
     `_attach_mask` / `_set_template_object` / `_save_workbench_ui`
