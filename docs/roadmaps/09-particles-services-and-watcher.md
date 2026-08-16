# Roadmap 09 — `services/particles/` + server-side curation watcher

**Status:** S1 + S2 CODE-COMPLETE 2026-08-16 (`py_compile` + `check_boundaries.py` owed — no python in the sandbox); scoped 2026-08-16. **Depends on:** 08-S0 (`registry_rev`, `species_identity`).
**Unblocks:** 10 (Species page reads `species_overview`, ingest service), 11 (Picks/Curation tabs).
**Risk:** low-medium (one behavior change — ingest moves server-side — quarantined in its own commit).
Four commits: S1 move-only · S2 ingest service (+ small model additions) · S3 overview reader ·
S4 watcher (deletes the UI prescan in the same commit).

## 0. Facts (Stage 0 — gathered)

- Architecture assessment §9.4 already names the split: `services/visualization/` → `services/particles/`
  (coords, pick_merge, picks_filter, list_extraction, subtomo_link — the load-bearing half) +
  rendering stays (preview_render, recon_cutouts, cutout_filters, preview_orchestrator, imod_vis,
  artiax_bridge, tomo_geometry).
- The manual-list upsert lives in the UI: `ui/tomo_dashboard_dialog.py:4247-4277`
  `_persist_manual_pick_list(result, species_id, tomo_name, project_path)` — builds a
  `PickList(slug="manual", label=<coords stem>, list_type=MANUAL, path=result["out_star"], count,
  color=_PICK_LIST_DEFAULT_COLOR[...], created_by)` and `save_project(project_path, force=True)`.
  `_PICK_LIST_DEFAULT_COLOR` is a dashboard module constant (:105).
- `.coords` detection is UI-bound: `_auto_kick_coords_ingest` (:4335-4391) is called from
  `_collect_species_data_for_ts` (:3557) — i.e. only when the Particles section of the *selected TS*
  is being rendered while the Journey view is visible (`_set_journey_active` :657-670 pauses the
  timers otherwise; the journey is built lazily on first switch, `ui/workspace_page.py:126-156`).
  Its helpers: `_pending_save_for_tomo` (:4306-4332, newest non-export `.coords` in the curation
  dir), dedup set `_AUTO_INGESTED_COORDS` (:4297) keyed `f"{species_id}:{tomo}:{mtime}"` + a
  `pl.created_at >= mtime` guard (:4358-4366); the poll folds `_curation_bundles_sig` (:550-575,
  glob over ALL `Curation/*/*/*.coords`) into the 4-s outer gate (:604, :617) and
  `_curation_sig_for_ts` (:342-363, selected TS) into `_main_signature` (:405). There is **no
  server-side watcher**; the only server loop is `PipelineMonitor`
  (`services/scheduling_and_orchestration/pipeline_monitor.py:202-266`, started/stopped in
  `main.py:117-123`, constructed in `backend.py:56-63`).
- Save-location contract (already enforced twice): `swap_chimerax_commands(..., cwd=out_dir)`
  appends `cd <curation_dir>` on every REST swap (`services/visualization/artiax_bridge.py:186,
  205-210`; used at `services/curation/session_service.py:668-676`) and `save_curation_picks`
  (:577-607) writes straight into `_curation_loaded[job_id]["curation_dir"]`.
  `_curation_loaded[job_id] = {project_path, species_id, species_label, tomo_name, curation_dir}`
  (:681-687), in-memory, per live session (per-user, may point at another project).
- `import_curation_picks(project_path, tomograms_star, tomo_name, species_label, species_id, *,
  coords_path=None)` (`session_service.py:780`; facade `backend.py:732`) owns the file I/O
  (`.coords` → `Curation/<sp>/<tomo>/manual.star`, raw archived to `imports/<stamp>.coords`
  :838-847) and returns `{count, out_star, coords_source, raw_import, discovered, created_by}`; with
  `coords_path=None` it auto-discovers via `_discover_manual_coords` (:753 — non-recursive
  `*.coords`, excludes `auto.coords` / `*_ref.coords`, newest first).
- Geometry for the import: CE-rich species use the CE job's `tomograms.star`
  (denoised chains repoint the volume there — S2 deviation 2 in
  `denovo_picking/02-geometry-and-inversion.md`), de-novo species use
  `geometry_for_ts(...).tomograms_star` (`services/visualization/tomo_geometry.py:229`); the
  dashboard resolves this at `:3503-3504` / `:2888-2897`.
- `curation_dir` slugs are lossy (`_safe_slug`, `artiax_bridge.py:153-171`:
  `Curation/<slug(species_id or label or tomo)>/<slug(tomo)>`), so a directory name must be mapped
  back to (species_id, rlnTomoName) by matching slugs of the known ids/names, never by parsing.
- Built-but-unwired status machinery to compose (not rebuild): `services/aggregation_authoritative.py`
  `enumerate_authoritative` (:184), `compute_gate_report` (:371), `extraction_params_for_species`
  (:222-260), `extract_inputs_for_list` (:264); `services/dashboard_data.py`
  `pick_list_counts_for_species` (:777), `ce_instance_for_species` (:741),
  `matching_subtomo_instance` (:711), `species_render_plan` (:747-774);
  `PickList.extraction_state()` (`services/project_state.py:547`).
- `PickList` (`project_state.py:505-580`) has its own `color` field (default `#3b82f6`) — a second
  source of truth beside `species.color`; `origin` on `ParticleSpecies` (`services/models_base.py:211`)
  has zero readers.
- `ProjectState.load()` is field-by-field; `PickList(**p)` / `ParticleSpecies(**s)` construction
  means new fields WITH defaults need no migration, but any new top-level `ProjectState` field must be
  added to `load()` explicitly (known trap).

## 1. Stage S1 — move-only (`services/particles/`)

- `git mv services/visualization/{coords,pick_merge,picks_filter,list_extraction,subtomo_link}.py
  services/particles/` + `__init__.py`; update every importer (grep `services.visualization.(coords|
  pick_merge|picks_filter|list_extraction|subtomo_link)` — UI, backend, drivers, services, docs).
  Zero logic change; `python check_boundaries.py` + `py_compile` are the real verification.
- Leave a one-line note in `services/visualization/__init__.py` docstring pointing at the split.

## 2. Stage S2 — ingest service + small model additions

- NEW `services/particles/ingest.py`:
  `register_manual_pick_list(state, result: dict, species_id: str, tomo_name: str) -> PickList` =
  the body of `_persist_manual_pick_list` minus the save (pure state mutation via
  `state.add_pick_list`, which now bumps the rev). Dashboard's `_persist_manual_pick_list` becomes
  a 3-line wrapper (resolve `state_for(project_path)`, call, `save_project(project_path, force=True)`).
- `_PICK_LIST_DEFAULT_COLOR` (`tomo_dashboard_dialog.py:105`) → `services/models_base.py`
  `PICK_LIST_DEFAULT_COLOR` next to `PickListType` (services may not import ui — R1). Dashboard
  imports it from there.
- `picks_filter.merge_source_for(...)` ← `_source_for` (`tomo_dashboard_dialog.py:4693-4714`, "kept
  subset if `<slug>_filtered.star` exists, else base; auto → subtomo `particles_filtered.star` when
  `has_filtered_set`"). Dashboard calls it.
- Model (additive, defaults, no migration):
  - `PickList.source_kind: str = ""` (`"tm" | "artiax" | "import" | "merge"`) and
    `source_ref: str = ""` (CE iid · coords stem [+ session id] · imported path · `"a+b+c"` parent
    slugs). Populate at the four creation sites: `register_manual_pick_list` (artiax/import — the
    result's `coords_source`), merge (`session_service.merge_pick_lists` caller in the dashboard
    `_do_inline_merge` :4735-4780), the synthesized auto/filtered entries
    (`_collect_pick_lists_for_species` :3327 — render dicts, `source_ref = ce iid`).
  - `ParticleSpecies.catalog_id: str | None = None` — the roadmap-12 hook (D-3, user 2026-08-16);
    written by nothing yet.
  - `PickList.color`: stop writing it; render sites derive from `species.color` (+ type shape). Keep
    the field for old JSON (ignored). Grep `pl.color` / `lst["color"]` / `"color":` in the dashboard
    at execution and switch each to the species color.
  - `origin` reader: nothing renders it yet — roadmap 10 Overview does; here only make sure the
    Journey empty-state + roster + workbench "+" all set it (they do: manual/manual/workbench).
- `StrEnum`s: `SpeciesOrigin` (`workbench|manual|imported`, `""` tolerated on load) and
  `PickSourceKind`; fields stay `str` for JSON, validated by the enum at the write sites.

Verification: `py_compile` + `check_boundaries`; runtime: explicit "Import picks" click still
registers the `manual` list (same slug), merge still creates `merged__<name>` with `source_ref` set.

## 3. Stage S3 — `species_overview` reader (headless, ~80 lines)

NEW `services/particles/species_overview.py`:

```python
@dataclass(frozen=True, slots=True)
class ListRow:      # one pick list on one tomogram
    species_id: str; tomo_name: str; slug: str; list_type: str; label: str
    count: int; kept: int | None; is_authoritative: bool
    extraction_state: str          # NOT_EXTRACTED | EXTRACTED | STALE | "n/a" (auto w/o subtomo)
    star_path: str | None; extracted_path: str | None; source_kind: str; source_ref: str

@dataclass(frozen=True, slots=True)
class SpeciesOverview:
    species_id: str; n_tomos_with_picks: int; n_picks: int; n_kept: int
    n_extracted_lists: int; gate: str          # READY | PENDING | BLOCKED (compute_gate_report roll-up)
    ce_iid: str | None; subtomo_iid: str | None
    rows: tuple[ListRow, ...]

def species_overview(state, project_path, species_id) -> SpeciesOverview: ...
def all_species_overview(state, project_path) -> list[SpeciesOverview]: ...
```

Composition only: `enumerate_authoritative` + `compute_gate_report` (aggregation_authoritative),
`pick_list_counts_for_species` / `ce_instance_for_species` / `matching_subtomo_instance`
(dashboard_data), `PickList.extraction_state()`, `picks_filter.read_reviewed_counts` for auto kept
counts. `__main__` CLI: `python -m services.particles.species_overview --project P [--species S]`
prints the rows (verify on `/groups/klumpe/crboost_data/agg_20260311_412_Grid3 --species 412` when the
mount is up, like `aggregation_authoritative`'s CLI). No UI import. Disk work stays in the callee
helpers (they already stat/parse); callers run it off-loop.

## 4. Stage S4 — server-side curation watcher (behavior change, own commit)

NEW `services/curation/watcher.py`:

```python
class CurationWatcher:                      # PipelineMonitor skeleton (start/stop/_stopping/_loop/_tick_once)
    TICK_SEC = 5; FULL_SWEEP_EVERY = 6      # hot dirs every tick, full Curation/*/*/ every ~30 s
    SETTLE_SEC = 2                          # ArtiaX writes are not atomic
    def __init__(self, backend): self._seen: set[tuple[str,str,str,int]] = set(); self._events = defaultdict(deque(maxlen=200))
    def events(self, project_path) -> list[dict]: ...          # for the Curation tab / peeves triage
    def unattributed(self, project_path) -> list[Path]: ...    # dirs whose slugs match no species/tomo
```

Tick, per open project (`_project_states` snapshot, like the monitor):
1. `Curation/` absent → skip (one stat).
2. Dirs to scan = hot dirs from `curation_service.loaded_dirs()` (new accessor over
   `_curation_loaded` values filtered to this project) every tick, plus a full `Curation/*/*/`
   sweep every 6th tick.
3. Off-loop scan → for each dir: newest non-export `*.coords` (reuse `_discover_manual_coords`'s
   rule; move it to the bridge module or import from session_service), skip if `now - mtime <
   SETTLE_SEC`.
4. Attribute: build once per tick `{_safe_slug(s.id): s for s in species_registry}` and
   `{_safe_slug(name): name for name in tomogram names}` (from `tomogram_star_sources` +
   `read_tomo_table`, `tomo_geometry.py`; memoized by star mtime); unresolvable → record in
   `unattributed`, log once per dir, continue.
5. Dedup: `(project, sid, tomo, int(mtime)) in self._seen` → skip; existing `manual` PickList with
   `created_at >= mtime` → skip (restart-safe; mirrors `:4358-4366`).
6. `tomograms_star` = CE job's `tomograms.star` if that species has a CE instance and the file exists,
   else `geometry_for_ts(...).tomograms_star`; none → log + `unattributed`-style event ("no geometry
   for <tomo>"), skip.
7. `res = await backend.import_curation_picks(project_path, tomograms_star, tomo, label, sid,
   coords_path=coords)`; on error → event + skip.
8. `register_manual_pick_list(get_state_service().state_for(project_path), res, sid, tomo)`;
   `await get_state_service().save_project(project_path=project_path, force=True)` (explicit path —
   no client context here); rev bumped by `add_pick_list`; append an event.
- Lifecycle: construct in `backend.py` next to `pipeline_monitor` (:56-63); start/stop in
  `main.py:117-123` beside the monitor. Sequential ingest per tick (no overlap); one lock per project
  if parallelised later.
- **Same commit deletes** the UI prescan: `_auto_kick_coords_ingest` (:4335-4391),
  `_pending_save_for_tomo` (:4306-4332), `_AUTO_INGESTED_COORDS` (:4297), `_curation_bundles_sig`
  (:550-575) and its `curation` term in the outer gate (:604, :617, :623-632 diagnostics),
  `_curation_sig_for_ts` (:342-363) and its term in `_main_signature` (:405). Otherwise two ingesters
  with different dedup keys race. Keep the explicit "Import picks" click (:4394-4420, idempotent
  upsert of the same `manual` slug) and the path dialog (:4423).
- How the UI learns: `add_pick_list` → rev++ → journey outer gate (08-S0) → `_pick_lists_sig` moves
  → pane rebuild shows the diamond layer; strip via `journey_signature` (n_picks); Species page tabs
  (10/11) gate on the rev; roster untouched.

Verification (user, runtime): (1) with the Journey NOT open, save a `.coords` in ArtiaX (or "Save
picks now") → within ≤ 35 s the strip shows the pick count for that tomo; open the Journey → the
`manual` list is there, one entry; (2) save again for the same tomo → the list is replaced (count
updates), no duplicate; (3) restart the server → no re-ingest of already-registered saves; (4) a
`.coords` dropped by hand into `Curation/<sid>/<tomo>/` (any name except `auto.coords`/`*_ref.coords`)
is picked up on the next full sweep; (5) a dir whose slug matches nothing shows up in
`watcher.unattributed(project)` (surfaced in 11-S4's Curation tab).

## 5. Risks

- `check_boundaries.py` R1/R2: services must not import ui; the watcher may call `StateService`
  directly (only `ui/` is barred from bare `get_project_state()`).
- Watcher cost: proportional to tomos ever opened for curation (dirs are created by `curation_dir`
  on load/prepare), scanned off-loop; the journey did the same full glob every 4 s — net reduction.
- Slug collisions: two species/tomos whose `_safe_slug` coincide → attribute to neither, report
  unattributed (never guess).
- Per-user shared session across projects: `_curation_loaded` may point at project B while the
  watcher runs for project A — hot dirs are filtered per project; the full sweep covers the rest.
- Rendering the events: 11-S4; until then they are only in the log.

## 6. Modern-Python weave-in

`StrEnum` for `SpeciesOrigin` / `PickSourceKind`; frozen slotted dataclasses for `ListRow` /
`SpeciesOverview`; `match` on `source_kind` where the UI picks a shape/label.

## Stage log (append-only)

- 2026-08-16 — scoped (facts from a 3-agent code read + design review). No code.
- 2026-08-16 — **S1 CODE-COMPLETE** (branch `denovo_picking`; sandbox ceiling was `ruff check .` only —
  no python this session, so `py_compile` + `check_boundaries.py` are owed with the runtime pass).
  Plain `mv` (no git in the sandbox — git detects the renames at commit): `coords`, `pick_merge`,
  `picks_filter`, `list_extraction`, `subtomo_link` → `services/particles/` (+ package docstring);
  `services/visualization/__init__.py` docstring points at the split. Importers repointed (13 files:
  `ui/tomo_dashboard_dialog.py`, `ui/aggregation_merge_card.py`, `drivers/extract_pick_list.py`,
  `services/aggregation_{discovery,authoritative}.py`, `services/dashboard_data.py`,
  `services/curation/session_service.py` (the `artiax_bridge, pick_merge` line split in two),
  `services/visualization/{imod_vis,preview_orchestrator,tomo_geometry,artiax_bridge}.py`, and the
  two intra-package imports in `pick_merge` / `picks_filter`); path comments in
  `services/subtomo_merge.py`, `services/jobs/subtomo_extraction.py`; live docs
  (`ARTIAX_BRIDGE_PLAN`, `PICKS_FILTER_AGGREGATION_ROADMAP`, `denovo_picking/00-overview`,
  `denovo_picking/03-extraction-and-resolver`) — dated records (architecture assessment, roadmap
  02/04 + census) keep the old paths on purpose. `imod_vis.py`'s import collapsed to one line under
  `ruff format` (shorter path). Zero logic change.
- 2026-08-16 — **S2 CODE-COMPLETE** (same session; `ruff check .` clean, `py_compile` +
  `check_boundaries.py` owed). Landed per §2 with one deviation: NEW
  `services/particles/ingest.py::register_manual_pick_list(state, result, species_id, tomo_name) -> PickList`
  (upsert via `add_pick_list`, stamps `source_kind` = `artiax` when the .coords sits in the
  tomogram's own curation dir — same parent as `out_star` — else `import`; `source_ref` = stem resp.
  path); dashboard `_persist_manual_pick_list` is the 3-line wrapper. `picks_filter.merge_source_for(slug,
  lists, *, ce_job_dir, subtomo_job_dir)` replaces the dashboard's `_source_for` body (kept as a
  one-line delegating local; `auto` without a CE job dir now raises `ValueError` instead of a
  `KeyError`). Models: `PickList.source_kind/source_ref` (defaults `""`), `ParticleSpecies.catalog_id:
  str | None = None`, `SCHEMA_VERSION` 3.2 → 3.3 (minor, info-log only), `PickList.color` marked
  LEGACY (kept for old JSON, no writer, no reader — both render dicts in
  `_collect_pick_lists_for_species` now carry `sp["color"]`, glyph distinguishes types); the merge
  stamps `source_kind="merge"`, `source_ref="a+b+c"`; the synthesized `auto` render dict carries
  `source_kind="tm"`, `source_ref=<CE iid>`. `StrEnum`s `SpeciesOrigin` / `PickSourceKind` in
  `models_base.py`; `add_species` validates `origin` through `SpeciesOrigin(...)` (callers pass
  `manual` / default `workbench`). **Deviation:** `_PICK_LIST_DEFAULT_COLOR` was DELETED, not moved
  to `models_base.PICK_LIST_DEFAULT_COLOR` — with `PickList.color` no longer written its only two
  users vanished, so the move would have installed a dead constant.
