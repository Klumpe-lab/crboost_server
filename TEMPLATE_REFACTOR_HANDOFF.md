# Template & Mask Refactor — Handoff (2026-05-11)

This doc chronicles the template / mask / particle-metadata refactor
across the workbench → schema → driver pipeline, captures the
overarching ambition, and lists what is **done** vs **next** so future
sessions can resume without re-deriving.

The companion plan for the molstar viewer lives in
`MOLSTAR_VIEWER_PLAN.md` — it's listed below in the roadmap section
under "Outstanding" but the detailed engineering plan is in that doc.

---

## Overarching ambition

> Templates, masks, and the particle metadata that describes a species
> (diameter, symmetry) should be **first-class infrastructure objects**
> visible at every surface that references them, with provenance, not
> opaque path strings that "dissolve into bare filepaths the moment the
> workbench closes."

Concretely:

1. **One species, many registered templates and many registered masks.**
   The workbench is a *registry editor*. Users acquire templates (basic
   shape, PDB simulate, EMDB resample, file import, resample-existing,
   apply-lowpass, flip-polarity) and they all *append* to a per-species
   collection. Same for masks (RELION create, manual import). One
   template / one mask is the species's *selected* one — that's the
   default used by jobs.

2. **Decoupled.** A mask is NOT owned by a template. A user can attach
   different masks to different templates over time. Both lists are
   independently selectable.

3. **Jobs read from the species at submission time.** TM and
   candidate-extract jobs get their template / mask / symmetry /
   diameter defaults from the species's selected entries at job
   creation. The user can still override per-job via dropdowns in the
   job config tab.

4. **Provenance survives.** Each registered entry carries source
   (PDB:6Z6J, EMDB-1234, basic_shape:550:550:550, imported, flipped
   from <other>, resampled from <other>), polarity (typed not
   filename-encoded), lowpass resolution claim, mask creation knobs
   (threshold/extend/soft/lowpass), created_at timestamp. UUID4
   identity persisted in a sidecar `.meta.json` next to the .mrc so
   renames within the project keep the entry connected.

5. **Visible everywhere.** Every surface that references a template
   (TM job-config, candidate-extract job-config, subtomo job-config,
   pixel-sanity panel, per-tomo dashboard) shows the species's metadata
   via the same `template_summary_card` component.

6. **Auditable in the dashboard.** The pixel-sanity panel reads
   template apix/box from the MRC header (not from a stale persisted
   field), and warns on mismatches (box-vs-particle-diameter ratio,
   apix mismatches across species's TM instances). Diameter from
   `species.diameter_ang`, symmetry from `species.symmetry`.

---

## What landed (chronological)

### Session 1 — schema v2 + UI surface flip (2026-05-10)

- `SCHEMA_VERSION` (1,0) → (2,0). `_migrate_v1_to_v2` (idempotent,
  version-guarded).
- New models: `TemplateMask` (with relion_mask_create knobs as fields),
  `ParticleTemplate` (with `imported_from`, `created_at`, etc.),
  `TemplateWorkbenchUIState`.
- `ParticleSpecies` extended with `diameter_ang`, `symmetry`, `notes`,
  `template`, `workbench_ui`.
- `services/templating/template_metadata.py` — shared helpers
  (`read_template_header` with mtime cache, `get_effective_*_path`
  resolvers, `resolve_species_from_job` with three-fallback chain).
- `services/templating/mrc_inspection.py` — `inspect_mrc_for_import`:
  header geometry, statistics, polarity inference, mask-likeness test,
  PDB/EMDB id regex pulls, MRC labels.
- `ui/components/template_summary_card.py` — reusable indigo card.
- `ui/template_import_dialog.py` — file picker → inspection →
  editable metadata form → copy-into-project flow.
- `ui/components/template_viewer.py` — orthoslice triplet (built but
  later relegated to fallback behind a toggle).
- UI surfaces flipped to embed the summary card: TM plugin,
  candidate-extract plugin, pixel-sanity panel in the dashboard.

### Session 2 — workbench restructure + v2 write-through (2026-05-10)

- Full rewrite of `ui/template_workbench.py` (1062 → ~720 lines).
- Five-panel layout: summary card → particle metadata editor → source
  flow tabs (shape / pdb-emdb / import / edit current) → mask editor →
  orthoslice viewer.
- Every generation flow's success calls `_register_template(...)`
  writing to `species.template` directly. `viewer_controller.update_paths`
  refresh after register.
- Molstar viewer deleted (later reverted — see session 4).
- Subtomo job-config wired with summary card + inline box-vs-diameter
  warning mirroring the dashboard rule.

### Session 3 — molstar restore + layout pass (2026-05-10)

User pushed back on the post-restructure UX: too blocky, empty right
halves on every panel, slice viewer sliders 2 s per drag, and most
importantly "bring back molstar — it's crucial." Reverted the molstar
deletion same session.

- `MOLSTAR_EMBED_HTML`, `/molstar-workbench` route, `/api/file` route,
  the JS bridge methods, color palette, session-tray all restored.
- Viewer panel grew a mode toggle (molstar / slice fallback). Slice
  viewer kept as escape hatch.
- Species header moved to a thin strip with diameter / symmetry /
  notes inline.
- Per-flow geometry inputs (apix / box / lowpass) moved inside each
  generation tab — they used to live in a shared bar at the top that
  misleadingly applied to Import / Edit too.
- Style constants (`_HDR_ICON_SIZE` etc.) at module top.
- Import dialog UX: `ui.spinner("dots", size="sm")` during the
  synchronous MRC inspection; inspection moved to `asyncio.to_thread`.
- Pipeline builder: `pipeline_builder_panel.add_instance_to_pipeline`
  copies `species.symmetry` → new TM jobs and `species.diameter_ang` →
  new candidate-extract jobs. Existing jobs aren't auto-updated.

### Session 4 — schema v3 (decouple + UUIDs + drop seeds) (2026-05-11)

User asked to break the mask→template coupling: each species should
register N templates and N masks independently. Plus drop seeds entirely
(`relion_mask_create` threshold path is enough; seeds were a fidelity
optimization that didn't pay off).

- `SCHEMA_VERSION` (2,0) → (3,0). Hard cut migration; `.v2.bak` snapshot
  before migrating.
- `services/project_state.py`:
  - `sidecar_ensure(path, kind)` — reads or creates a
    `<file>.meta.json` UUID sidecar.
  - `TemplateMask` gains `id: str` (uuid4) and
    `derived_from_template_id: Optional[str]`.
  - `ParticleTemplate` gains `id: str`. `mask` field **removed**.
  - `ParticleSpecies` gains
    `templates: List[ParticleTemplate]`,
    `masks: List[TemplateMask]`,
    `selected_template_id: str`,
    `selected_mask_id: str`.
  - v2 fields (`template`, `template_path`, `mask_path`, `workbench`)
    **removed**.
  - `_migrate_v2_to_v3(data, project_root)` — walks
    `templates/<sid>/` for orphan MRCs and registers them
    (`*_seed.mrc` skipped, `*_mask.mrc` → masks, others → templates
    with polarity inferred from filename suffix).
- `services/templating/template_metadata.py` — helpers resolve via
  `species.get_selected_template()` / `get_selected_mask()`.
- `services/path_resolution_service.py` — TM template/mask resolution
  reads species's selected entries; per-job override still wins.
- `ui/components/template_summary_card.py` — reads via helpers.
- `ui/job_plugins/template_match.py` — **two dropdowns**: Template
  (lists species.templates with apix/box/polarity in option labels)
  and Mask (lists species.masks with method chip, includes "(none)"
  option). Defaults from species's selected entries; user can override.
  `symmetry` excluded from the renderer (lives in species header).
- `ui/template_import_dialog.py` — appends instead of replacing;
  sidecar written on ingest. Replace warning becomes informational
  ("this will be added alongside the N existing").

### Session 5 — UX polish + dedup + delete (2026-05-11, this session)

- `MOLSTAR_VIEWER_PLAN.md` written: bridge robustness slice (queue +
  optimistic UI + error echo + hard-reset), `embed.js` patches slice
  (slicing API + size guard + progress events + memory cleanup), UI
  affordances slice (per-item status badges, workbench reset button,
  file-size on cards).
- Flip-polarity now dedup-aware: canonical-path check → existing
  registry entry select → on-disk register + select → else write.
- `_negate_volume_to_disk_at` refuses to overwrite.
- All `_append_template` / `_append_mask` calls replace-in-place when
  the path is already registered (instead of growing the list).
- Mask filename includes threshold/extend/soft knobs so identical-knob
  runs are path-idempotent.
- Delete with confirmation modal: clicks `ui.dialog` with filename and
  Cancel/Delete buttons; removes file + sidecar from disk, unregisters
  from species, cascades through masks derived from a deleted template.
- Card visual: file icon with hover-tooltip showing full path, click
  copies to clipboard; row-2 = chip + apix/box/lowpass; row-3 = source.
- Edit Current restructured as **discrete action sections**: Resample
  (target apix + box + lowpass), Apply lowpass (target lp at same
  apix/box), Flip polarity. No more in-place source/lowpass/notes
  editing — these are write-once at creation.
- Color palette collapsed to gray + indigo (templates) + purple
  (masks). Dropped blue/emerald form tints, amber warnings, orange
  hints.
- Font scale collapsed to `_TITLE_CLS` / `_LABEL_CLS` / `_BODY_CLS` /
  `_MONO_CLS` / `_HINT_CLS`.
- Section panels: header row + spacing, no outer card wrappers
  (cards reserved for items in a list).

---

## Schema v3 — at-a-glance

```python
class ParticleSpecies(BaseModel):
    id: str
    name: str
    color: str = "#3b82f6"

    # Particle-intrinsic
    diameter_ang: Optional[float] = None
    symmetry: str = "C1"
    notes: str = ""

    # Decoupled collections
    templates: List[ParticleTemplate] = []
    masks: List[TemplateMask] = []
    selected_template_id: str = ""
    selected_mask_id: str = ""

    workbench_ui: TemplateWorkbenchUIState

    # Helper methods on the model:
    get_selected_template() -> Optional[ParticleTemplate]
    get_selected_mask() -> Optional[TemplateMask]
    get_template_by_id(id) -> Optional[ParticleTemplate]
    get_mask_by_id(id) -> Optional[TemplateMask]

class ParticleTemplate(BaseModel):
    id: str  # uuid4
    template_path: str
    polarity: Literal["white", "black"]
    lowpass_resolution_ang: Optional[float] = None
    source: Optional[str] = None
    imported_from: Optional[str] = None
    created_at: Optional[datetime] = None
    notes: str = ""
    # mask field DOES NOT EXIST — masks are siblings on species

class TemplateMask(BaseModel):
    id: str  # uuid4
    mask_path: str
    method: Optional[Literal["spherical","cylindrical","relion","manual","imported"]]
    threshold: Optional[float] = None
    extend_pixels: Optional[float] = None
    soft_edge_pixels: Optional[float] = None
    lowpass_ang: Optional[float] = None
    derived_from_template_id: Optional[str] = None  # soft audit link
    created_at: Optional[datetime] = None
    notes: str = ""
```

Disk layout per project: `templates/<species_id>/*.mrc` with
`*.mrc.meta.json` sidecars pinning the UUID. Migration walks the
folder to register orphans.

---

## Where things are read in the pipeline

| Surface | What it reads | Helper |
|---|---|---|
| TM driver (compute node) | `params.template_path`, `params.mask_path`, `params.symmetry` | path_resolution_service stamps resolved paths via `get_selected_template/mask` |
| Candidate-extract driver | `params.particle_diameter_ang` | defaulted at job creation from `species.diameter_ang` |
| Job-config tabs (TM) | `species.templates` + `species.masks` for dropdowns; defaults from selected | `species.get_selected_*` |
| Pixel-sanity panel | `species.diameter_ang`, `species.symmetry`, MRC header from selected template | `get_effective_template_path` + `read_template_header` |
| Summary card (everywhere) | selected template + mask + species fields | `get_selected_template / get_selected_mask` |
| Pipeline builder (job creation defaults) | selected template/mask/symmetry/diameter | `get_effective_*_path` |

Job models still carry `template_path` / `mask_path` / `symmetry` /
`particle_diameter_ang` as per-job override fields. The values flow:

1. Job creation in `pipeline_builder_panel.add_instance_to_pipeline`:
   stamps species's selected values into the new job's params.
2. Path resolution at submission: prefer per-job non-empty value, else
   re-read species's currently selected entry.
3. Driver reads `params.*` as before; this code is untouched.

---

## Files touched (cumulative across v2 + v3 + polish)

```
services/project_state.py
  - SCHEMA_VERSION (3,0); _migrate_v1_to_v2 + _migrate_v2_to_v3;
    sidecar helpers; v3 models; .v1.bak + .v2.bak; ParticleSpecies
    convenience methods

services/templating/template_metadata.py
  - read_template_header (mtime cache); get_effective_*_path;
    get_selected_template/mask helpers; resolve_species_from_job

services/templating/mrc_inspection.py
  - inspect_mrc_for_import: rich .mrc inspector for import flow

services/path_resolution_service.py
  - get_context_paths for TEMPLATE_MATCH_PYTOM reads via v3 helpers;
    per-job override wins

ui/components/template_summary_card.py
  - Reusable read-only card; reads via v3 helpers

ui/components/template_viewer.py
  - Orthoslice triplet (slice fallback behind toggle)

ui/template_import_dialog.py
  - Inspection dialog with spinner during MRC read; appends new entry;
    writes UUID sidecar

ui/template_workbench.py
  - Full v3 layout: species header / templates cards / source tabs /
    masks cards / mask tabs / viewer / log. Append semantics. Discrete
    Edit-Current actions. Confirmation-modal delete with disk + sidecar
    removal. Dedup-aware flip polarity. Color palette gray + indigo +
    purple. Three font sizes total.

ui/job_plugins/template_match.py
  - Two species-scoped dropdowns (Template / Mask); excludes
    template_path / mask_path / symmetry from default-renderer

ui/job_plugins/candidate_extract.py
  - Summary card embedded

ui/job_plugins/subtomo_extraction.py
  - Summary card embedded; inline box-vs-diameter sanity warning

ui/tomo_dashboard_dialog.py (pixel-sanity panel)
  - Reads via v3 helpers; species fields preferred over job fields

ui/pipeline_builder/pipeline_builder_panel.py
  - Species-driven defaulting at job creation for TM (template/mask/
    symmetry) and candidate-extract (diameter)
```

---

## Roadmap — what's still pending

### Priority 1 — molstar viewer (separate doc)

See `MOLSTAR_VIEWER_PLAN.md` for the full plan. Three slices:

A. **Python bridge robustness** (no JS edits): ready-gate command queue,
   optimistic UI for visibility toggles, in-flight action dedup,
   hard reset button (iframe re-mount escape hatch), error echo.

B. **embed.js modifications**: per-volume load progress events,
   per-volume load error events, size guard with `too_large` rejection,
   slicing API (`setSliceAxis`/`setSlicePosition`/`setSliceMode`),
   memory cleanup on `clear`. Likely requires rebuilding the bundle
   from molstar's npm package vs. patching the compiled artifact.

C. **UI affordances**: per-item status badges in the session tray
   (spinner / red dot / green dot), workbench "Reset viewer" button,
   file-size badge on template/mask cards.

The user-reported symptoms in priority order:
1. **Large templates silently fail to render** (1.55 Å/px ~345 MB
   template — sidebar entry appears, ISO slider present, no 3D object).
   This is the blocker.
2. Visibility toggles flaky.
3. No way to "reset everything" from the client.
4. No progress feedback during large loads.

### Priority 2 — UX polish (open user feedback from session 5)

1. **Cards still get the X pushed off-screen by long filenames.** The
   `flex-1 truncate` row inside a flex container needs `min-w-0` so
   the ellipsis kicks in instead of expanding the row past the close
   button. Also consider making the cards a touch wider (260+ px) or
   restructuring so the X is always at a fixed-pixel position.

2. **Laggy tab-switch between species + auto-load of large files.**
   Today the molstar `ready` handler auto-loads the selected template
   and mask. When the second species has 1.55 Å/px (300+ MB) entries,
   loading them silently chews bandwidth on every species switch. Plan:
   - Don't auto-load on `ready`. Show an empty viewer until the user
     explicitly requests a load.
   - Add a per-card "load into viewer" icon (eye / cloud-up icon).
     Clicking it posts `load_volume` for that specific entry. Selection
     (which template is "active" for jobs) is decoupled from loading
     (which template is being looked at).
   - On species switch, the new species's workbench renders cleanly
     with no in-viewer items.

3. **Delete dialog jumps and closes immediately.** Likely cause: the X
   button click inside a card with a `.on("click", select)` handler at
   the card level — the click bubbles up, the card calls
   `_select_template` → `_after_register` → re-renders the cards,
   which re-renders the X button inside a now-different DOM node.
   The dialog might be torn down by the re-render or the bubbled
   click is interpreted as a click-outside-to-close on the modal. Fix:
   add `event.stop` (or `@click.stop` on the button) to prevent
   propagation from the card's click handler.

4. **Buttons should be sized to match adjacent input boxes.** Inputs
   use `props("dense outlined")` which gives one height; buttons with
   `props("... size=sm")` look smaller. Drop `size=sm` from the action
   buttons (generate / fetch & simulate / create mask / etc.) so they
   match the inputs in the same row.

5. **Tab labels should be proper tabs, not lowercase bold text.**
   The custom `_TAB_LABEL_STYLE` (font-size 10 px, padding 0 8) makes
   them feel like flat text. Drop the override, use Quasar's default
   tab styling, and revert to Title Case labels: "Basic Shape", "PDB
   / EMDB", "Import", "Edit Current"; "RELION", "Import".

6. **Visual grouping.** Group Templates (cards) + Source (tabs) as one
   visual unit since Source is the producer for Templates. Same for
   Masks (cards) + Mask creation tabs. Increase the gap between
   groups vs. within groups so the grouping reads.

### Priority 3 — wiring tightenings

1. **Species edits don't propagate to existing jobs.** Today we
   snapshot at job creation. If user edits `species.diameter_ang`
   after the candidate-extract job has been created, the existing job
   still uses the old value. Either (a) propagate-on-species-edit hook
   in the workbench with a confirmation, or (b) read species at driver
   time in `get_driver_context`. Punted from earlier sessions.

2. **Per-TM-row template thumbnail in tomo dashboard.** Forensic
   context for failed picks — show the template that was registered
   at submission time next to the TM result row. Cheap with
   `read_template_header` + a thin numpy→png helper.

3. **Spherical / cylindrical mask generators.** Schema's
   `TemplateMask.method` Literal supports them; the workbench mask
   editor only exposes RELION + Import for now.

### Priority 4 — schema cleanups

- `TemplateWorkbenchState` v1 model in `services/jobs/_base.py:39` is
  dead code after the v3 hard cut. Safe to delete.
- Some pre-existing baseline lint in `services/project_state.py` and
  `services/path_resolution_service.py` (unused imports).

---

## Gotchas for the next session

1. **HPC env (Lmod, no `module` command in Claude shells)** —
   reference `memory/reference_hpc_env.md` for the `PYROOT` /
   `SSLROOT` / `LD_LIBRARY_PATH` override that gives a working
   `venv/bin/python3` and `venv/bin/ruff`. The venv doesn't have
   `numpy` / `pandas` so full-app imports require the user's
   interactive `loadpython` env. Lint and `py_compile` work without.

2. **NiceGUI doesn't hot-reload.** `ui.run_with` in `main.py:115` —
   code changes require restarting `python main.py`. Mention this if
   the user says "I don't see your changes."

3. **`_seed.mrc` files still get written** by
   `template_service.generate_basic_shape_async` as an intermediate
   step. The workbench just doesn't register them anymore. To clean
   up the disk artifacts, the future session can teach the service to
   write to a temp path and clean up after RELION smoothing — out of
   scope for v3.

4. **Sidecar `.meta.json` files**: when a registered .mrc is deleted
   via the workbench's confirmation modal, the sidecar is also
   removed. If a user manually `rm`'s an .mrc without the sidecar, the
   sidecar will orphan; the next migration walk would re-register
   nothing (the file is gone) but the orphan sidecar would sit there
   until cleaned manually. Not a real problem but worth knowing.

5. **Schema migration is one-way (v1 → v2 → v3).** A user who saves
   then downgrades the code won't be able to read the new fields. The
   `.v1.bak` and `.v2.bak` snapshots are the recovery path.

6. **TM job's `template_path` / `mask_path` / `symmetry` are still
   on the param class** as USER_PARAMS. The plan was to delete these
   at "v1 deletion time" — that's now misnamed; effectively means
   "after species drives jobs at driver-time (not creation-time)".
   Still pending under Priority 3.

---

## Suggested execution order for the next session

1. **Read this doc + `MOLSTAR_VIEWER_PLAN.md`.** Don't re-derive.
2. **Address the Priority 2 UX list** (most user-visible). Order:
   delete-dialog fix (Priority 2 #3) → lazy-load + per-card load
   icon (Priority 2 #2) → card layout fix (Priority 2 #1) → button
   sizing + tab styling + grouping (Priority 2 #4 / #5 / #6).
3. **Pick a molstar slice from `MOLSTAR_VIEWER_PLAN.md`.** Slice A
   (bridge robustness) is pure Python and unblocks the user's biggest
   pain (no hard reset, no error echo). Slice B (embed.js) is the
   bigger commitment.
4. **Loop with user** before doing Priority 3 schema work — these are
   not as urgent as the UX fixes.

That's the state of the world. Good luck.
