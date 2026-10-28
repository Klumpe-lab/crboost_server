# Roadmap 40 — STA kitchen: one species' picks across tomograms and projects

**Pending (maintainer, 2026-10-28):** build next, on branch `STA_Kitchen`, stages S0 → S5 back to back (S6 as far as
it goes). The Journey's Particles panel, the Analytics tab's score and sets panels and the merge / aggregate dialogs are
expected to be deleted once the kitchen covers them (§9); nothing is removed by this roadmap.

**Status:** S0–S5 built 2026-10-28 on `STA_Kitchen`, checked in the sandbox (record in §11), not run in a browser:
the §10 pass is owed. Decisions in §7: D1–D11 accepted by the handoff of 2026-10-28; D12 (from the S0 facts) waits for
a nod.

**In one line.** A new tab of the Particles registry where one species' picks from every source (template matching,
hand-picked, imported, later other pickers), on any reconstruction of any tomogram of any project, sit in one pool;
filters you drag update a large cutout gallery and a large tomogram view live; a by-tomogram mode streams slabs linked
by lines to their cutouts; a sources tree, a ledger and a doctor keep track of what is in the pool, what came out of it
and what looks wrong; Make set turns the selection into an ordinary particle set that downstream jobs read.

**Not in this roadmap:** new pickers, learned scorers, condition tags, 3D rendering in the browser, moving picks
between reconstructions (never: §2), writing into other projects, changes to the Journey. S7 lists what follows.

## 1. Goal

**Use case 1 (cross-project cut).** In one AGG412 project, a user opens Particles registry → `412_particle` → STA
kitchen, adds the other seven projects' 412 candidate extractions as sources, drags a σ range and a depth band while
the cutouts and the slab dots follow, drops the tomograms with poor CTF fits, drops junk tiles by hand, and makes one
set spanning the eight projects that Class 3D / Refine 3D in this project reads. No dialog per project, no script.
Every count on screen says how many particles from which projects and tomograms, and a tilt series reconstructed in two
projects is flagged before the set is made.

**Use case 2 (one project, many picks).** The ribosome candidates of AGG412 MyrGFP_Grid2 (59,628 rows from two
extractions on two reconstructions of the same 47 tilt series; S0) reviewed tomogram by tomogram in the by-tomogram
stream, worst CTF fits first, with hand verdicts that persist. The pilot project has no ribosome extraction of its own,
so its ribosome pool takes them as sources from MyrGFP_Grid2.

## 2. Principles

- **One row per physical pick.** Everything that happens to it later is a column: its extraction, class, refinement
  numbers, the hand verdict, tags. A candidate and its extracted subtomogram are one row, not two.
- **The table is the data layer.** Every later feature is a new column producer (a picker, a segmentation, a classifier,
  a tag) or a new view of the same table. The UI renders the table and never reads job files itself (roadmap 30 §4).
- **Rules mark, they never drop.** Filters define what is shown; only Make set writes, and it writes a new set with its
  recipe, leaving every parent untouched.
- **Picks never move between reconstructions.** A tilt series reconstructed twice is two frames, each with its own
  picks (no re-homing). A set takes one frame per acquisition (D3).
- **Other projects are read, never written.** No ProjectState of theirs enters this server's registry, no analytics
  record is stored in them, no picture is rendered into them. Everything the kitchen writes lands in the home project.
- **Nothing silent.** Every filter states its coverage (picks without a score, without a value), an unknown pixel size is
  red and never 1.0, a pick without a cutout says why.
- **One element per surface.** The gallery, the slab dots and the filter strip are one custom element fed a column
  table; no NiceGUI element per pick (§3, last block). CLAUDE.md's UI rules hold: hidden views do nothing, nothing heavy
  on the loop, house controls, colour only for state, species colour for dots.

## 3. What exists (anchors, 2026-10-28)

**Sets and rules.** `services/particles/particle_sets.py`: `ParticleSet`, `Recipe`, `ParentRef`, `SetOp`, `SetLevel`,
`publish_set`, `operations.jsonl`, `jobs_reading_set`; the resolver offers every set as an optimisation-set producer,
explicit choice only (`services/path_resolution_service.py:1281`). `services/particles/set_ops.py`: `read_table` (:75),
`greedy_keep` (:100), `apply_rules` (:144: σ, LCC, top-N, depth band, faces, NN thinning, include/exclude tomograms,
away-from-species), splits, A − B / A ∩ B, replay (:743–903).

**σ and per-pick geometry.** `services/particles/analytics_store.py`: `sources_for_species` (:128), `candidates_record`
(:444, per tomogram: n, σ, cutoff, ladder, cap, every score), `tomo_sigma_map` (:535), `tomogram_quality` (:736: CTF fit,
tilts used, blank/dim), `_store` (:583, writes backfills into the project it reads). `services/particles/analytics.py`:
`particle_sigma_scores` (:366), `tomogram_extents` (:459), `fit_pick_plane` (:477), `plane_depth` (:496), `min_distances`
(:565). A candidate-extract job writes one `candidates.star` for all tomograms plus `tmResults/*_job.json` (σ per tomogram).

**Identity across projects.** `services/particles/tomo_identity.py`: `distinguishing_key` (a reconstruction),
`acquisition_key` (a tilt series, from the registry's first frame: raw filename + mdoc time), `check_transferable` (same
geometry, so centred-Å coordinates name the same place), `read_tomo_hand`.

**Merges.** `services/particles/coord_merge.py`: `CoordSource` (:74), `plan_merge` (:192: equates acquisitions, blocks
non-transferable pairs, renames colliding `rlnTomoName`s), `merge_coordinate_sources` (:325, ragged score columns go to a
sidecar, never filled). `services/subtomo_merge.py:358` `merge_optimisation_sets_into_jobdir` (pixel grade, critical
optics columns guarded). `docs/particle-data-flow.md` §4–§6 is the reference.

**Joins.** `services/particles/subtomo_link.py:155` `build_pick_to_mrcs_index` (candidate ↔ extracted particle by tomogram
+ centred Å rounded to 0.1 Å). `services/particles/class_readout.py:178` joins by `rlnTomoParticleName`. Merges reassign
`rlnTomoParticleName` (coord_merge.py:386), so pick identity is coordinates, not names.

**Sources of picks.** Candidate-extract jobs per species (`sources_for_species`); `PickList` rows for hand-picked,
imported and merged lists (`services/project_state.py:541`, star in `path`, curation in `<slug>_filtered.star`);
`services/particles/list_ref.py:183` `species_tomo_map` (which tomograms.star frames a list). Projects to offer:
`services/aggregation/discovery.py:296` `discover_pick_list_projects` over `prefs.recent_project_roots`
(`ui/aggregation/aggregate_dialog.py:234`). Species across projects: `ParticleSpecies.catalog_id`
(`services/project_state.py:213`); the lab catalog root is set on the dev and lab installs.

**Geometry and pictures.** `services/visualization/tomo_geometry.py`: `TomoGeometry` (:52; binned dims, apix and its
provenance), `geometry_for_ts` (:261), `all_geometries` (:233). `services/renders.py` (`Render`, `ensure`, `restart`;
`RenderSlot` in `ui/components/render_slot.py`). Slabs: `render_recon_slabs` (`services/visualization/preview_render.py:565`),
cached at `<recon or denoise job>/vis/slabs/<tomo>_{xy,xz}.png` (`ui/particles/pick_viewer.py:379`, Render key
`slabs:<xy png>` at :401). Cutouts: `_crop_slab` (`services/visualization/recon_cutouts.py:34`, mean of a Z slab around
the pick), normalization over the whole tomogram's frames (`services/visualization/cutout_filters.py:245`), box 2 × Ø in
binned px (`ui/particles/pick_viewer.py:432`), tiles rendered at 192 px and shown at 96. TM candidates have cutouts
today only once a subtomo extraction ran (the preview pass joins picks to `.mrcs`).

**Workers, routes, views.** `services/workers.py`: `WorkKind` (:45), caps (:57), `run_in_worker` (:136).
`server_app.py:75` `/api/vis-asset`, guarded by `is_in_loaded_project` (`services/project_state.py:1554`: roots of
`_project_states` only). `services/http_responses.py` `NO_GZIP`. Routes can also be declared with nicegui's `app`
(`ui/template_workbench.py:112`). `ui/components/reactive.py:130` `ViewScope`; `ui/ui_state.py:415` `view_scope`.

**Species page.** `ui/species/page.py`: `TABS` (:45), rail (:105), `_ensure_tab` (:206, container
`w-full overflow-auto`), `_make_tab` (:217), `_apply_visibility` (:238); tab contract `ui/species/tab.py`. Routing
mirror `ui/routing.py:57` `SPECIES_TABS` lacks `"analytics"` today, so a deep link to the Analytics tab is refused.

**Charts and doors.** `ui/dashboard/figures.py` `build_sigma_histogram` (:1226), `build_survival_chart` (:1268).
`ui/curation_session_dialog.py:68` `open_curation_control_center` (ArtiaX; writes into the project).

**NiceGUI 3.0.3 custom elements.** `class X(Element, component="x.js")` (`nicegui/element.py:89`, `dependencies=` and
`esm=` also exist); the JS is a Vue component object; `this.$emit("name", payload)` reaches Python through
`.on("name", handler)`; Python calls JS methods with `run_method`. Component JS loads with the element
(`loadDependencies` in `nicegui/static/nicegui.js`), not through the page shell.

**What does not scale.** Each slab dot (`ui/particles/pick_viewer.py:1160`) and each gallery tile (:3190, with three
children and a handler) is its own NiceGUI element: about six per pick, built and diffed on the server. The Journey's
particle side lags at 500–1,000 picks (roadmap 30 §4); a pool of eight projects' candidates is tens of thousands.

**Hazard found while scoping (from reading, not observed).** `ui/aggregation/aggregate_dialog.py:159` loads other
projects with `get_project_state_for`, which installs them in `_project_states`. The pipeline monitor reconciles every
installed state whose persisted `pipeline_active` is set (`services/scheduling_and_orchestration/pipeline_monitor.py:157`)
and saves it with `force=True` (`pipeline_runner.py:286`; it can also scancel dependents); the curation watcher saves
installed states too (`services/curation/watcher.py:239`). A project whose pipeline runs under another server gets a
second reconciler writing its `project_params.json`. Also: `ProjectState.load` may write schema backups next to the file
it reads (`services/project_state.py` ~1289–1307), and analytics backfills are stored into the project they describe
(`analytics_store._store`). S1 fixes the first and avoids the other two.

## 4. Data model

### 4.1 Where it lives

`<home project>/Analytics/kitchen/<species_id>/`:

| Path | What |
|---|---|
| `pool.json` | `KitchenPool`: species, members, the draft recipe, tags, settings (box Å, cutout slab Å). Written atomically (`services/particles/records.py` `write_json_atomic`), debounced. |
| `verdicts.jsonl` | Append-only hand verdicts and tag events: `{at, user, uid, op, value, source}`. The latest event per uid and key wins. |
| `table/<member_key>.npz` + `.json` | Each member's columns and the fingerprint (source paths + mtimes) they were built from. |
| `tiles/<frame_key>/<uid>.<ext>` + `tiles/<frame_key>/norm.json` | Cutouts rendered on demand; the frame's normalization bounds. |
| `slabs/<frame_key>/<tomo>_{xy,xz}.png` | Slabs of other projects' reconstructions that have none on disk. |

One pool per species per home project (D8). View preferences (mode, size, sort, group) live per browser tab in
`ui_state`, not in `pool.json`.

### 4.2 Members

`PoolMember`: `key` (stable: project token + kind + instance or list id), `project_path` (absolute), `project_label`,
`species_id` (in that project), `kind`, `ref` (instance id or `species/tomo/slug`), `tomos` (include list; None = all),
`added_at`, `added_by`.

Kinds in v1: `tm` (a candidate-extract job: `candidates.star`; its subtomo extractions join as columns) and `list` (a
`PickList`: hand-picked, imported, merged; its per-list extraction joins as columns). Particle sets and merges are not
members (D9): they become filters.

Reading a member's project: if it is installed in this server (the home project, or one open in another browser tab),
its live state is used (in-memory state is authoritative); otherwise a read-only snapshot (§6.1).

### 4.3 The particle table

One row per (member, pick). Numeric columns are float32 with NaN for "no value"; categorical columns are int32 codes
into a per-column category list. Each column carries a label, a unit and its coverage.

| Group | Columns |
|---|---|
| Identity (categorical) | `uid`, `member`, `project`, `frame` (a reconstruction: `distinguishing_key`), `acq` (acquisition key, or "unknown"), `tomo`, `source` (tm / hand / imported / merged) |
| Position | `x_a, y_a, z_a` (centred Å); `x_px, y_px, z_px` (binned voxels of its frame, for slabs and tiles) |
| Score | `sigma` (LCC ÷ the tomogram's σ; NaN when unscored), `lcc`, `rank` (in its tomogram), `margin` (σ − the tomogram's FP=1) |
| Geometry | `depth_a` (from the tomogram's pick plane), `face_a` (to the nearest volume face), `nn_a` (to the nearest pick of the same member and tomogram), `z_pct` |
| Extraction | `extracted` (categorical: the job, or none), `vis_tilts` (count of `rlnTomoVisibleFrames`) |
| Tomogram facts | `ctf_res`, `n_tilts`, `n_blank`, `slab_a` (pick-plane thickness), `tm_marker` (σ outlier / no peak / cap bound) |
| Hand | `verdict` (keep / drop / none), `tags` (int32 bitmask, ≤ 32 tags) |

Later columns (S6, S7): `agreement`, `class`, refine numbers, `p_<feature>` / `d_<feature>` from segmentations,
`other_a` (to the nearest pick of another species).

Built per member in a worker, from files only, cached on disk by fingerprint; the pool table is their concatenation plus
the hand columns. Its version = hash of the member fingerprints + the verdict log length.

### 4.4 Identity and duplicates

- `uid` = first 12 hex of sha1(project path, frame key, member key, centred Å rounded to 0.1 Å). Stable across re-reads.
  A re-run of the source makes new picks and new uids; verdicts on vanished uids are orphans the Doctor counts.
- Frames of one acquisition: poolable only when they are one alignment reconstructed alike: the same tilt-series star
  (`rlnTomoTiltSeriesStarFile`) and `check_transferable` on their tomograms.star rows (+ hand) (D12). Poolable frames
  (a raw and a denoised volume of one alignment) are pooled at Make set and deduplicated by proximity (Ø/2, `pick_merge`
  priority: a hand pick wins a clash), as `plan_merge` does. Any other frame of the acquisition blocks Make set until
  the user picks one (D3); the choice is stored in `pool.json` (`frame_choice`) and sets the other frames' rows aside.
- Several members on one frame (TM + hand + imported): clashes within Ø/2 are deduplicated at Make set with the same
  priority; the `agreement` column (S6) shows them before that.
- Acquisition unknown (no raw filename or mdoc time): never equated with anything; the Doctor says so.

### 4.5 Recipe semantics (one definition, implemented in Python and JS)

1. **Range filter** `{col, lo, hi, keep_missing}`: a row passes when `lo ≤ v ≤ hi`, or when `v` is NaN and
   `keep_missing` (default true for scores, so hand picks are not dropped by a σ cut without saying so).
2. **Category filter** `{col, allowed}`: passes when the row's code is in `allowed`.
3. **Set filter** `{set_id, mode: in | out}` (S6): membership by uid, matched through the set's parent member.
4. All filters are a conjunction. **Post-ops** then run in order on what passes, per (frame, tomogram), ranked by σ
   (NaN last): NN thinning at r Å (`greedy_keep`), top N.
5. **Hand verdicts** last: `final = (passes and verdict ≠ drop) or verdict = keep` (D10).
6. "Removes N" for a filter = rows that pass every other filter and fail this one.

The browser applies 1, 2 and 6 on every drag frame, 4 and 5 on release; the server applies all of it for the
authoritative count and for Make set. A count mismatch after release is a bug, logged with both numbers.

### 4.6 Verdicts and tags

Keep / drop / clear per uid; tags defined in `pool.json` (`{id, name, key}`, keys 1–9). Seeding (D6): when a member is
added, its existing curation (`particles_filtered.star` of a subtomo job, `<slug>_filtered.star` of a list) is read once
and appended as drop events with `source: "seed:<path>"`. Verdicts are written by the server only, off the loop, batched.

### 4.7 Make set

- Writes an ordinary `ParticleSet` into the home project's set store with `SetOp.POOL` (new) and a recipe holding the
  members as `ParentRef`s (gaining `project_path`; `SET_SCHEMA` 2), the filters, the post-ops, the verdict counts, and
  the frozen uid list (`kitchen_uids.txt` in the set dir) so the set can be compared and replayed exactly.
- Level: every selected row extracted, with agreeing optics → an extracted set (per-member row subsets of the extracted
  particles, merged with `merge_optimisation_sets_into_jobdir(..., allow_no_primary=True)`); otherwise a coordinates set
  (per-member subset stars merged with `merge_coordinate_sources`, `registry_lookup` from the snapshots). Mixed: the
  dialog offers "coordinates set (extract later)" or "only the N extracted rows".
- One member only: a plain row subset (the `set_ops` path), no merge.
- A cross-project set carries its merged `tomograms.star` in its own directory; `publish_set` gains a writer for it, and
  the optimisation set names it. The resolver label adds "(N projects)".
- Preflight: `plan_merge` blockers and unverified lines (handedness unstated) and the pixel-grade optics guards, shown
  before anything is written; confirm, then publish.

## 5. UI

### 5.1 Placement

- `("kitchen", "STA kitchen")` joins `TABS` (`ui/species/page.py:45`) and `SPECIES_TABS` (`ui/routing.py:57`);
  `_make_tab` builds `KitchenTab(ctx)` (`ui/kitchen/tab.py`). `"analytics"` joins `SPECIES_TABS` in its own commit.
- The kitchen container is `height: 100%; overflow: hidden` (its scroll areas are inside the component), not the default
  `overflow-auto` column of `_ensure_tab`.
- While the kitchen tab shows, the species rail is hidden; a rail toggle in the header brings it back (D7).
- The tab contract gains an optional `set_visible(on)`; the page calls it from `_apply_visibility` and `_set_active`, and
  the kitchen's timers are `ViewScope` timers that run only while it is on screen.
- Pane width, not window width, decides the layout (container queries, as the registry's `_REGISTRY_CSS` does): below
  ~1,100 px the two halves stack.

### 5.2 Layout

```
STA kitchen · 412     3,950 of 13,120 · 211/744 tomograms · 6/8 projects   ▕▓▓▓▒▒░░▏ by project
Pool | By tomogram    S M L    sort σ ▾    group: none ▾          Sources   Doctor ⚑3   Make set…
┌ σ ───────────────┐ ┌ depth ──────────┐ ┌ NN ────────────┐ ┌ CTF fit ───────┐ ┌ source ─────┐
│▁▂▃▅█▇▅▃│▓▓▓▓▓▓▓▓│ │ ░░│▓▓▓▓▓▓▓▓│░░ │ │░│▓▓▓▓▓▓▓▓▓▓▓│ │▓▓▓▓▓▓▓▓▓│░░░░│ │ TM ▮ hand ▮ │ + filter
└ removes 8,910 ───┘ └ removes 702 ────┘ └ removes 96 ────┘ └ removes 1,120 ─┘ └─────────────┘
 then: NN ≥ ___ Å · top ___ per tomogram        edge of the cut:  ▢ ▢ ▢ ▢ ▢ ▢ │ ▢ ▢ ▢ ▢ ▢ ▢
┌ Position 11_2 · project 3 ───────────────────┐ ┌ 3,950 cutouts ─────────────────────────────┐
│   X/Y   ● passes   ○ filtered out   ◆ hand   │ │ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢     virtualized grid   │
├──────────────────────────────────────────────┤ │ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢                        │
│   X/Z                                        │ │ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢ ▢                        │
└──────────────────────────────────────────────┘ └────────────────────────────────────────────┘

By tomogram:
┌ Position 11_2 · project 3 · 38/112 · ⧉2 ┐  ┌──────────────────────────────────┐
│  ●┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄▶ ▢  ▢  ▢  ▢  ▢  ▢             │
│        ●┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄▶ ▢  ▢  ▢  ▢  ▢  ▢   ↕ scrolls │
└──────────────────────────────────────────┘  └──────────────────────────────────┘
```

The header line holds the count line and a thin composition bar (selection by project; hover for numbers), the mode
switch, size, sort and group, then Sources, Doctor (with its marker count) and Make set (the one accent button).

### 5.3 The custom element

One element, `KitchenView(Element, component="kitchen.js")` (`ui/kitchen/view.py`, `ui/kitchen/kitchen.js`), renders the
filter strip, the slab panel and the gallery (pool mode) or the stream (by-tomogram mode) from one in-browser store. One
element keeps one store; there is no cross-element synchronisation to get wrong.

- **Props:** `table_url` (versioned), `tile_url` (template with `{uid}`), `draft` (the recipe), `verdicts` (uid → state,
  initial), `tags`, `mode`, `size`, `sort`, `group`, `species_color`, `slabs` (frame/tomogram → `{xy, xz, dims}`).
- **Store:** a typed array per column, the pass bitmask, per-filter removal counts, the sort index, the visible window.
  The store functions touch no DOM, so `node` (`/usr/local/bin/node`) can exercise them in the sandbox.
- **Events to Python:** `draft` (on release, debounced 300 ms), `focus` (a frame and tomogram to show on the left),
  `need_slabs` (stream rows coming into view), `verdict` (uids, keep / drop / clear), `tag` (uids, tag, on / off).
- **Methods from Python:** `set_slab(key, urls, dims)`, `reload_table(url)`, `apply_verdicts(map)`, `set_view(...)`.
- **Filter strip:** one canvas per filter: 40 bins over p0.5–p99.5 (σ: 0.25σ bins, as the Analytics tab), pale = all
  rows, dark = rows passing every other filter, two drag handles, drag across to set a range, double-click to reset; the
  "removes N" chip flips the gallery to only those rows. Coverage line under it when below 100 %: "applies to 12,990 of
  13,120 · 130 have no value: keep | drop". Categorical filters are chips with counts. "+ filter" lists every column with
  its coverage. Post-ops line under the strip (`house_number`-style inputs, applied on release).
- **Edge strip:** the six cutouts just above and the six just below the σ filter's low edge, recomputed while dragging.
- **Gallery:** absolutely positioned virtualized grid (visible rows plus one screen of overscan; a few hundred DOM nodes
  at most). Tiles are `<img>` at S / M / L = 96 / 160 / 256 px from 192-px renders (the data has no finer detail: the
  box is 2 × Ø at the reconstruction's pixel size). Sort: σ, depth, NN, tomogram, random (seeded, for unbiased review),
  distance to the σ cut. Group: none / tomogram / project / σ bin / tag, with sticky group headers. A tile shows only
  a source glyph in a corner, tag dots, and the verdict (dropped = the existing red border and stripes); one glass
  tooltip carries project · tomogram · source · σ · depth · NN.
- **Slab panel:** XY and XZ `<img>` with one SVG of dots: passes = species colour with the house dual ring, filtered out
  = hollow ring, hand picks = diamonds. Dots follow every drag frame. Hover a tile → its dot lights (one active dot);
  hover a dot → its tile lights if it is in view. Click a tile → its tomogram moves to the left.
- **Curation:** click toggles keep / drop (when curating), a drag over tiles applies one direction to the box (majority
  rule, as the Journey's lasso), keys: arrows move focus, `k` keep, `x` drop, `u` clear, `1`–`9` tags, Enter focuses
  the tile's tomogram.
- **Budget:** ≤ 10 ms of script per pointermove on 100k rows (requestAnimationFrame-throttled); no Python per drag frame.

### 5.4 Pool mode

Left half: the focused tomogram, XY large over a thin XZ, all pool rows on its frame as dots; its title line: tomogram ·
project · ⧉ when its acquisition sits in another frame · passing / total · CTF fit and tilt count · "Curate picks"
(ArtiaX, home-project tomograms only: it writes into the project). Right half: the gallery. The first focus is the
tomogram of the first tile.

### 5.5 By-tomogram mode

One row per tomogram with at least one row in the pool, virtualized: slab card (XY with dots, thin XZ) · an SVG of
leaders · a cutout box as tall as the card that scrolls. Tiles in the box are ordered by their dot's Y (then X), so
leaders rarely cross; leaders are drawn for the tiles in view, faint (species colour, low alpha), the hovered pair solid,
and they follow the box's scroll (D5). Row title as in pool mode. Row order: passing count, CTF fit, project, name.
Tomograms with nothing passing fold into one line ("N tomograms with nothing passing"), expandable. Slabs are asked for
as rows come into view; a skeleton holds the place until the render lands.

### 5.6 Sources

A dialog. Default grouping by acquisition (one line per tilt series, frames under it, members under each frame), toggle
to group by project. Every level has a checkbox and in-pool / passing counts; members also show coordinates / extracted,
and stale (fingerprint moved since the table was built; the table rebuilds that member). "Add sources…" lists the
discoverable projects (`discover_pick_list_projects` over the recent project roots); for each, the species with the same
`catalog_id` is pre-ticked, otherwise a species select with "not in this project" as the default (no name matching,
D2); then its members (candidate extractions, lists) with their tomogram counts.

### 5.7 Doctor (S5)

Markers, never drops. Each has a tooltip with the rule, the measured value and "drops nothing", built from one
constants table:

- an acquisition in two or more frames: transferable ("pooled and deduplicated at Make set") or not ("pick one frame",
  with "prefer project X" and per-acquisition actions);
- acquisition unknown (no raw filename or mdoc time): duplicates cannot be checked;
- pixel size, box or handedness disagreeing between extracted members (the merge's critical columns);
- coordinates and extracted rows mixed;
- unscored rows (hand-picked, imported);
- members matched with different template-matching settings (template, mask, angular search, band-pass, CTF and
  phase-flip, whitening, random-phase correction; fields from `TemplateMatchPytomParams`,
  `services/jobs/template_match.py:35`): a σ cut does not mean the same on both;
- stale members; orphaned verdicts; frames with an unknown pixel size (red); picks without a cutout (outside the volume).

### 5.8 Ledger (S5)

The sets made from this pool (`SetOp.POOL`): name, date, level, counts per project and tomogram, the recipe in one line,
the jobs reading it (`jobs_reading_set`), and their headline results where a reader has finished (Class 3D shares and
Reconstruct / Refine 3D resolution from the roadmap 30 readouts). Two sets selected → "compare": the gallery shows the
uids only in A, then only in B.

## 6. Performance and robustness

### 6.1 Reading other projects

`services/kitchen/snapshots.py`: `project_view(path)` returns the installed state when the project is in
`_project_states`, else a read-only snapshot: `ProjectState.load(path, write_backups=False)` (new parameter) parsed off
the loop and never installed, plus a `TiltSeriesRegistry` loaded without entering `_registries`. A snapshot refreshes
when `project_params.json` or the registry index changes (checked on the kitchen's 15-s disk tick, never on the 3-s
observe), has no save path, and registers its root as readable for the asset routes (`is_in_loaded_project` also accepts
roots from a `register_readable_root` set in `services/project_state.py`). Analytics records of members in other projects
are computed without storing (`store=False` on the record functions) and cached in the home kitchen's `table/`.

The aggregate dialog's foreign-project read (`ui/aggregation/aggregate_dialog.py:159`) moves to `project_view` in its own
commit, flagged as a behaviour change.

### 6.2 The table

Built per member in a worker (`WorkKind.KITCHEN`, cap 2) from `candidates.star`, `tmResults/*_job.json`,
`tomograms.star`, the subtomo `particles.star`, list stars, and the registry facts; cached on disk by fingerprint;
opening the tab reads caches and rebuilds only stale members. Served at `GET /api/kitchen/table?home=…&species=…&v=…`
(`ui/kitchen/routes.py`): a JSON header (columns, dtypes, categories, coverage, units, row count) and the columns as
little-endian arrays, `NO_GZIP`, immutable per version. Worst case on AGG412 (ribosome, eight projects, ~30 columns):
a few × 10 MB, fetched once per version.

### 6.3 Tiles

`GET /api/kitchen/tile?home=…&species=…&uid=…&v=…`: from the disk cache, else rendered in a worker (`WorkKind.KITCHEN_TILE`,
cap 3): `_crop_slab` on the frame's reconstruction (mmap; workers keep a small LRU of open volumes), box 2 × Ø Å and a
slab of fixed Å thickness (S0 fixes the value from today's 5 px at the commonest pixel size), normalized with the
frame's bounds (computed once from a sample of ≤ 64 of its picks and cached in `norm.json`: never per tile, so noise
stays grey), resized to 192 px, encoded (format chosen in S0 by size and look: PNG or JPEG q90). Immutable caching keyed
by the reconstruction's mtime. A stream row may prefetch its whole tomogram in one worker call. Both kitchen routes
answer only for a home project installed in this server and for uids in its pool table, so a URL cannot name an
arbitrary volume.

Without a species diameter there is no common physical scale: tiles fall back to a 48-px window per frame and the header
says scales differ (red marker), as `_list_cutout_box_px` does today for one tomogram.

### 6.4 Slabs

Home-project frames use the existing slab paths and Render keys (one render shared with the Journey and the wall). Other
projects' frames read an existing current slab PNG in place; a missing one renders into the home kitchen's `slabs/`.

### 6.5 Loop budget

No per-row Python element; no Python per drag frame; verdict batches appended off the loop; the 3-s tab refresh reads
memory only; disk checks (member fingerprints, snapshots) on a 15-s `ViewScope` timer while the tab is on screen.

## 7. Decisions (2026-10-28)

Accepted ("Ok, write it" to the design message):

- **D1 Home and writes.** The pool belongs to one species of the project the tab is opened in. Other projects are read,
  never written. Sets, merges, verdicts and caches land in the home project.
- **D2 Same species elsewhere.** Matched by `catalog_id` or chosen per project; never by name.
- **D3 One frame per acquisition per set.** Several only when transferable (pooled, deduplicated). Non-transferable
  duplicates block Make set until a frame is chosen. No override in v1.
- **D4 Pictures.** Every row's cutout is cut from its reconstruction on demand (works for coordinates and extracted rows
  alike), on one physical scale. The extracted-stack view is a later toggle (S6).
- **D5 Lines** go from each dot to its tile, ordered so they rarely cross, drawn for tiles in view.
- **D6 Verdicts and tags** belong to the particle (per species, in the home project) and are the curation store from
  now on; existing `_filtered.star` selections seed them once.
- **D7 Width.** Half of the kitchen pane each; the species rail hides while the kitchen shows; narrow panes stack.

Proposed while writing, waiting for a nod:

- **D8** One pool per species per home project; named pools only if they turn out to be needed.
- **D9** Existing particle sets and merges enter as filters ("in set X" / "not in set X"), never as members, so no
  particle appears twice.
- **D10** A hand keep rescues a pick the filters remove; a hand drop always wins; the count line shows both.
- **D11** Roadmap 30 decision 6 ("no multi-project page") is superseded by this roadmap.

Accepted by the handoff of 2026-10-28 (D1–D11). Added in S0, waiting for a nod:

- **D12 One alignment per frame group.** Two frames of one acquisition share picks only when they name the same
  tilt-series star (one alignment) and pass `check_transferable`. `check_transferable` alone passes MyrGFP_Grid2's
  job009 and job024 reconstructions (same pixel size, binning, size and hand) although they come from different
  alignment branches (job008 vs job023 tilt-series stars), so pooling them would move picks between alignments. Every
  other second frame blocks Make set until one is chosen.

## 8. Stages

Each stage is one commit or a short series, leaves the app runnable, and ends with `ruff check .`, `ruff format`,
`python check_boundaries.py`, `python -c "import server_app"` (the whole app's import graph; the venv's python runs in
the sandbox since 2026-10-28), `node` over the store functions of `kitchen.js` once it exists, and a
stage record in §11. Snapshots of each stage's files go to `notes/40-stage-snap/<stage>/`.

### S0 — Facts (read-only)

On the eight AGG412 projects and the copia tutorial project, without writing anything into them:

1. Per project: species ids and `catalog_id`s; candidate-extract jobs per species with `candidates.star` rows; whether
   `<job>/analytics/candidates.json` sidecars exist; subtomo jobs and their `particles.star` rows; pick lists by type.
2. Registry: share of tilt series with an acquisition key; acquisitions present in more than one project; whether
   `rlnTomoHand` is stated; tomograms.star pixel size, binning and size per project.
3. Existing `particles_filtered.star` / `<slug>_filtered.star` files (seed volume for D6).
4. Timings with the venv's python: one member's table build (the largest job), 50 tiles from one reconstruction (cold and
   warm), one slab render; PNG vs JPEG tile size and look.

Done when §11 holds the table of facts and timings, and any surprise has re-scoped the stages below.

### S1 — Data layer (services only)

- `services/kitchen/`: `models.py` (`KitchenPool`, `PoolMember`, `MemberKind`, filters as a discriminated union, post-ops,
  tags, verdicts), `store.py` (paths, atomic `pool.json`, `verdicts.jsonl` append and compaction), `snapshots.py` (§6.1),
  `members.py` (members a project offers for a species; catalog matching), `table.py` (worker-safe per-member builders
  and the npz cache), `pool.py` (assembly, uids, categories, coverage, version, and `select(table, draft, verdicts)`,
  the authoritative recipe of §4.5), `payload.py` (the binary encoding).
- Edits: `ProjectState.load(..., write_backups=True)`; `register_readable_root` and the guard in
  `services/project_state.py`; `store=` on the analytics record functions; `WorkKind.KITCHEN` and `KITCHEN_TILE`.
- Separate commit, behaviour change: the aggregate dialog reads other projects through `project_view`.

Done when a sandbox script builds the AGG412 412 pool read-only, its rows equal the members' `candidates.star` totals,
its σ ladder matches the Analytics tab's records (the sidecars on 2026-10-28: ≥ 10σ, 1,642 picks in 348 tomograms
across the eight extractions; roadmap 30 §12's 929 in 255 counted seven, before the re-runs), nothing outside the home
project's `Analytics/kitchen/` changed (mtimes checked before and after), and a second build from the cache takes
under a second.

### S2 — The tab, Sources, and the pool-mode gallery

- `ui/kitchen/`: `tab.py` (`KitchenTab`: tab contract, `set_visible`, events, the 15-s disk tick), `view.py` and
  `kitchen.js` (store, virtualized grid, S / M / L, sort, group, slab panel with dots and focus), `routes.py` (table and
  tile routes), `sources.py` (§5.6). `services/kitchen/tiles.py` (§6.3) and `slabs.py` (§6.4).
- Edits: `ui/species/page.py` (tab, rail toggle, container style, `set_visible` calls), `ui/routing.py` (`kitchen`, and
  `analytics` in its own commit), `ui/dashboard/css.py` (`_KITCHEN_CSS`: hover and polish only; structure inline).

Done when, on a copia tutorial project (about 1,000 candidates) and the AGG412 pilot (412; ribosome from MyrGFP_Grid2), the tab
opens, the first screen of tiles shows in under 2 s warm, 15k tiles scroll smoothly with a few hundred DOM nodes,
S / M / L switches at once, a click on a tile focuses its tomogram with the dot lit, and Sources adds another
project's member read-only.

### S3 — Filters, live dots, edge strip, verdicts

- `kitchen.js`: filter strip, coverage lines, categorical chips, "+ filter", post-ops line, "removes N" inverted view,
  edge strip, live dots, click / drag / keys curation. `tab.py`: draft persistence, verdict and tag batches, the
  authoritative count after release.

Done when dragging σ on the ribosome pool keeps up with the pointer and moves gallery, dots and counts together; release
persists the draft (a reload restores it); a hand list's rows show in the σ filter's coverage line and stay unless
dropped on purpose; verdicts and tags survive a restart; JS and server counts agree after every release.

### S4 — By-tomogram stream

- `kitchen.js`: stream layout, row virtualization, leaders, `need_slabs`; `tab.py`: slab renders per row.

Done when a 255-tomogram pool scrolls smoothly, leaders follow the box's scroll and the hovered pair lights, empty
tomograms fold, and switching modes keeps the filters, verdicts and scroll target.

### S5 — Doctor, Make set, Ledger

- `services/kitchen/doctor.py` (markers, one constants table of rule texts), `services/kitchen/export.py` (§4.7);
  `particle_sets.py` (`SetOp.POOL`, `ParentRef.project_path`, `SET_SCHEMA` 2, `publish_set` tomograms writer); the
  resolver label; `ui/kitchen/doctor.py`, `make_set.py`, `ledger.py`.

Done when, on AGG412, a ≥ 9σ coordinates set over the eight projects is made from the kitchen, shows in an Extract or
Class 3D job's I/O dropdown of the home project as "Pool set — <name> (8 projects)", the Ledger lists it with counts per
project and its readers, and the pilot's ribosome pool (MyrGFP_Grid2's job027 and job028: 47 acquisitions in two
frames each, D12) blocks Make set until a frame is chosen.

### S6 — Extras (each its own commit, in this order)

1. Kept vs removed averages (unaligned means of the tiles, in a worker, on release).
2. σ × tomogram heat strip (rows = tomograms sorted by CTF fit, x = σ); a row brush becomes a tomogram filter.
3. Pair-distance histogram g(r) within tomograms; the spike below Ø selects those pairs in the gallery.
4. `agreement` column (sources within Ø/2 on one frame) and its filter.
5. Set filters (D9) and the extracted-stack picture toggle (subtomo atlases where they exist).
6. Review sample: 50 random kept and 50 random removed rows to label good / bad; precision and lost-recall estimates at
   the cut with Wilson intervals.
7. Loupe (space on a tile): larger cutout, X/Z and Y/Z slices, and the template rotated to the TM angles beside it.
8. Fate chart in the Ledger: candidates → kept → extracted → class → refined, by project.

### S7 — Later (scoped here, not scheduled)

Segmentation columns (`p_<feature>` at the pick, distance to the feature surface from one distance transform per
tomogram and feature on a coarse grid); downstream columns (Class 3D class, Refine 3D numbers) joined by particle name
within a job chain; other pickers as member kinds; a two-component fit over σ (P(signal) per pick) beside the decoy FDR;
TM-angle coverage; named pools; display filters on tiles; the deletions of §9.

## 9. What the kitchen covers (for a later deletion pass)

| Surface | Covered by |
|---|---|
| Journey Particles panel, pick viewer (`ui/particles/pick_viewer.py`) | pool mode + by-tomogram stream |
| Analytics score panel and sets panel (`ui/analytics/score_panel.py`, `sets_panel.py`) | σ filter + its expanded view, Ledger |
| Merge card and aggregate dialog (`ui/aggregation/`) | Sources + Make set |
| Picks & curation tab's list table and keep / drop sheets | Sources, verdicts |

The Analytics tab's TM health, controls and results stay until the Ledger and Doctor show what they show.

## 10. Runtime checklist (once, after S5)

1. Restart the server; `python check_boundaries.py`; hard-reload the browser (new CSS and a new JS component).
2. AGG412 pilot → Particles registry → `412_particle` → STA kitchen: the tab opens with the project's own 412
   candidates (158; a new pool starts with the home project's members); the species rail hides (the sidebar icon
   brings it back); the URL ends in `/species/412_particle/kitchen`, and `/species/412_particle/analytics` opens too.
3. Scroll the gallery to the end; switch S / M / L; click tiles from three tomograms: the left half follows, the
   clicked tile's dot is ringed; hover a dot and its tile lights.
4. "+ filter" → σ; drag the low handle to 9: the count line says 13 for the pilot alone (the Analytics tab's 9σ
   cut); dots below the cut go hollow; the edge strip shows six tiles each side of 9σ; after release the kept and
   removed means appear beside it within a minute.
5. Sources → Add sources: the other seven projects offer `412_particle` (catalog id); "Tick every source shown", Add;
   the state line says it builds the members, then the count line reads ~2,922 of 16,533 at ≥ 9σ across 8 projects
   and the composition bar splits them. Doctor shows no frame conflict for 412.
6. Curate: drop a few tiles by `x` and by a drag box, keep one removed tile from a "removes N" view; restart the
   server; the verdicts and the draft are still there. Tags: add one, press 1 on a tile.
7. By tomogram: the rows scroll, leaders follow the box's scroll, the hovered pair is solid; "N tomograms with
   nothing passing" folds; "rows" → worst CTF fit reorders.
8. Make set… at ≥ 9σ: the plan says 8 projects and no blocker; Make set; a Class 3D or Extract job's I/O dropdown
   lists "Pool set — … (8 projects)"; the Ledger lists it with per-project counts.
9. `dm_ovary_ribosome` → STA kitchen → Sources → add MyrGFP_Grid2's two ribosome extractions: Doctor ⚑ lists 47
   acquisitions in two frames, Make set is off; "prefer AGG412_20260311_MyrGFP_Grid2 · job024" settles them.
10. Check that no file in the other projects changed (the maintainer's `find -newer` on a marker file).

## 11. Build record

### S0 — facts (2026-10-28, read-only)

Read with the venv's python (`ProjectState.load` of schema-3 files writes nothing; registries loaded uninstalled).
Scripts and outputs: `notes/40-stage-snap/s0/`.

| Project | 412 extraction (rows / tomograms) | Ribosome extractions | TS (acq. key) |
|---|---|---|---|
| AGG412_20240204_412_Slot6 | job017: 1,779 / 121 | none | 126 (126) |
| AGG412_20251031_412_FCIsolation | job020: 1,672 / 132 | none | 163 (163) |
| AGG412_20251113_412 | job017: 4,336 / 136 | none | 151 (151) |
| AGG412_20260204_412 (pilot) | job020: 158 / 23 | none | 23 (23) |
| AGG412_20260205_412_FCIso_unmilled2 | job023: 2,770 / 159 | none | 176 (176) |
| AGG412_20260311_412_Grid3 | job019: 1,036 / 91 | none | 96 (96) |
| AGG412_20260311_MyrGFP_Grid2 | job043: 2,891 / 47 | job027: 26,345 / 47 (recon job009); job028: 33,283 / 47 (recon job024) | 50 (50) |
| AGG412_SK_20251017_412 | job020: 1,891 / 129 | none | 137 (137) |
| copia-tutorial-20261028-1036 | copia job008: 1,028 / 1; subtomo job009: 1,016 | — | 1 (0) |

- Species: every AGG412 project has `412_particle` (catalog `412_particle`, Ø 575 Å) and `dm_ovary_ribosome` (catalog
  `dm_ovary_ribosome`, Ø 310 Å); copia has `copia` with no catalog id (Ø 550 Å).
- Every candidate extraction has its `analytics/candidates.json` sidecar; every TM job its `analytics/tm_tomo/`.
- No AGG412 project has a subtomo extraction, a pick list or a `_filtered.star`; copia has one subtomo job. So the
  extracted level and D6 seeding run only on copia today, and list members on no real data.
- No acquisition is in two projects. MyrGFP_Grid2 reconstructs all 47 of its tilt series twice, on two alignment
  branches (tilt-series stars of job008 vs job023): 47 acquisitions in two frames, which `check_transferable` alone
  would call poolable (D12).
- tomograms.star: 1.55 Å × bin 4 = 6.2 Å, 4096 × 4096 × 2048 unbinned, `rlnTomoHand` −1 stated in every row (copia
  2.95 Å × 4 = 11.8 Å). Reconstructions: float16, 1024 × 1024 × 512, 1.07 GB.
- Timings: state load < 0.3 s, registry load 0.2–1.1 s per project; the largest member (job028, 33,283 rows) reads in
  0.18 s and its σ, plane and NN columns take 2.7 s; 34 tiles from one reconstruction 0.46 s cold, 0.02 s warm; X/Y
  slab 0.15 s, X/Z 0.77 s.
- Tile format: PNG 33 KB and 3.6 ms per 192-px tile, JPEG q90 22.5 KB and 0.2 ms, WebP 18.5 KB and 15.5 ms; no visible
  difference at 96–256 px on noisy cutouts → JPEG q90. Cutout slab: 30 Å (today's 5 px at 6.2 Å/px).
- A recursive glob over one project for `*_filtered.star` took minutes on NFS: the kitchen reads named paths only.

Re-scoped from these facts: use case 2 and the S2/S5 done lines (ribosome = MyrGFP_Grid2's extractions as sources of
the pilot's pool); §4.4 and D12; the S1 ladder numbers.

### S1 — data layer (2026-10-28)

Built: `services/kitchen/` `models.py`, `store.py`, `snapshots.py`, `members.py`, `table.py` (worker builder),
`pool.py` (assembly, frame conflicts, `select`), `payload.py`, `service.py` (the loaded pools: open, rebuild,
verdict and tag writes, selection, payload). Edits: `ProjectState.load(..., read_only=False)`; `WorkKind.KITCHEN`
(cap 2) and `KITCHEN_TILE` (cap 3); `tomogram_quality(project_path, registry=None)`. Separate commit (behaviour
change): the aggregate dialog reads other projects through `project_view`, so they no longer enter `_project_states`.

Checked (`notes/40-stage-snap/s1/build_pool.py`, home = the pilot, kitchen files redirected to scratch): the 412 pool
has 8 members, 16,533 rows = the sidecars' totals, 838 frames, no frame conflict; ≥ 10σ 1,642 picks in 348 tomograms
and ≥ 9σ 2,922, both equal to the sidecar ladders once σ is rounded to 0.001 as the records store it; first build
10 s (members 0.9–2.8 s each, two workers), second build from the cache 0.11 s; payload 2.6 MB. The ribosome pool:
59,628 rows, 94 frames, 47 conflicted acquisitions, payload 7.0 MB, first build 5 s. Directory and file mtimes in all
eight projects unchanged; `_project_states` and `_registries` empty afterwards.

Deviations: `read_only` instead of `write_backups` (it also refuses a file that would need the v1/v2 migration, which
writes template sidecars); no `store=` on the analytics record functions (the builder reads sidecars and job.json
itself and stores nothing); no `register_readable_root` (S2 serves slabs and tiles through kitchen routes that answer
only for frames in a loaded pool); verdicts are not in the table or its version (they travel apart, so a key press does
not refetch the table).

### S2 — the tab, Sources, the pool-mode gallery (2026-10-28)

Built: `ui/kitchen/` `tab.py` (`KitchenTab`: header rows of house controls, a `ViewScope` with the 3-s observe and the
15-s disk tick, lazy open on first show), `view.py` + `kitchen.js` (store, count line and composition bar,
virtualized grid with S / M / L, six sorts, five groupings with a pinned group header, slab panel with canvas dots and
focus), `routes.py` (table, tile and slab routes), `sources.py` (members with state and remove; Add sources over the
recent project roots plus the projects the pool already reads, species matched by catalog id or chosen, never by
name). `services/kitchen/tiles.py` (worker cut + `TileBatcher`: requests of one frame within 20 ms share one worker
call; a tile asked for twice is cut once) and `slabs.py`. Edits: `ui/species/page.py` (tab, `FULL_PANE_TABS`, rail
hidden while the kitchen shows with a toggle, `set_visible` calls), `ui/species/tab.py` (contract note),
`ui/routing.py` (`kitchen`, and `analytics` in the same edit: one line of one file, so not a commit of its own),
`ui/dashboard/css.py` (`_KITCHEN_CSS`: hover and state polish, the narrow-pane stack as a container query).

Checked in the sandbox: `node` runs the store functions against `services/kitchen/pool.select` on the AGG412 412 and
ribosome tables, 14 drafts each (σ, depth, CTF and category filters, keep_missing off, NN thinning, top N, 400 random
hand verdicts): identical kept rows, removals, coverage and frame counts in all 28. Filters alone take ≤ 6.3 ms per
drag frame on 59,628 rows; a release with NN thinning ≤ 70 ms. Service level (home = a scratch copy of the pilot's
params, so nothing renders into /groups): 120 tiles of the ≥ 9σ screen across 77 frames 1.5 s cold, 0.02 s warm;
22 KB each; a foreign frame's slabs render into the home kitchen's `slabs/` (1.3 s), no mtime under /groups moved. A
NiceGUI simulation builds the tab, opens the pool, serves table (2.6 MB, immutable), tile and slab, refuses a pool
not open in the server, and builds Sources and Add sources. The JavaScript drawing (grid, dots, focus) has not run
in a browser yet: §10.

Decisions in the build: tiles are flipped to the slabs' Y-up convention (the Journey's recon cutouts are not), so a
tile and its dot agree; a box that leaves the volume is padded with mid-grey, not shrunk and stretched; the norm sample
is 8 cutouts (the first batch's own, then the frame's picks), since one cutout read costs ~25 ms on NFS and 64 of them
made a cold screen take 25 s; slabs and tiles go through kitchen routes, not `/api/vis-asset`; view preferences live
in the tab object (per browser tab, not kept across a reload). Sources groups by project; the by-acquisition view is
the Doctor's (S5).

### S3 — filters, live dots, edge strip, verdicts (2026-10-28)

Built in `kitchen.js`: the filter strip (one card per filter: a √-scaled histogram over p0.5–p99.5, 0.25σ bins for σ,
pale = every row with a value, dark = rows passing every other filter; two handles, drag across to set a range, an
end dragged off the histogram opens it, double-click clears; the coverage line with keep | drop for rows without a
value; categorical chips with counts, the twelve largest then "+N more"; "removes N" flips the gallery to exactly
those rows and back), "+ filter" listing every column with its coverage, the post-ops line, the edge strip (six
cutouts each side of the σ cut among rows passing the other filters, found in one pass, no sort per frame), live
counts and dots on every drag frame (the grid re-sorts at most every 120 ms while dragging, fully on release), and
curation: a Browse | Curate switch (curating, a click toggles drop on a kept tile and keep on a removed one; a drag
draws a box and applies one direction by majority), keys arrows / k / x / u / 1–9 / Enter; verdicts batch for 250 ms
before they go to the server. `tab.py`: draft persistence on release with the server's count as the authority (a
disagreement is logged with both numbers), verdict and tag events, a Tags dialog (up to 32, keys 1–9).
`services/kitchen/seeds.py` + `service.seed_verdicts`: D6, a member's `particles_filtered.star` /
`<stem>_filtered.star` read once into drops named `seed:<path>`, never over a hand verdict (`PoolMember.seeded_at`).

Checked: the 28 node cases still agree; a simulated tab takes drop / keep / tag / draft events, and a reopened pool
(the server's memory cleared) has the draft, the four verdicts and both tags; a cleared verdict stays cleared after
another reopen; the seeding rule on a synthetic list (two of five missing from `_filtered.star` → two drops). No real
project has a `_filtered.star` (S0), so seeding has not met real data.

### S4 — by-tomogram stream (2026-10-28)

Built in `kitchen.js`: `streamFrames` (store: one entry per frame with a shown row, its rows ordered by the dot's Y,
top first, then X; frames with nothing shown apart) and the stream: virtualized rows of variable height (slab card
280 px wide, 340 at L: X/Y over a half-height X/Z, canvas dots), a cutout box as tall as the card with its own
virtualized scroll, SVG leaders from each dot to its tile for the tiles in view (faint, species colour; the hovered
pair solid, white over a dark under-stroke), following the box's scroll; one folded line "N tomograms with nothing
passing", expandable. `tab.py`: a rows select (most passing, worst CTF fit, project, name) and the element's `mode`
event (a click on a stream tile while browsing opens its tomogram in pool mode). Switching modes keeps the draft,
verdicts and focused tomogram; the stream scrolls to the focused tomogram's row.

Checked in node: on the ≥ 9σ draft the 412 pool streams 464 rows + 374 folded tomograms, the rows' kept counts sum to
the 2,922 kept, every row is ordered by Y; the ribosome pool 84 rows + 10 folded; 5–36 ms per regrouping. The drawing
(rows, leaders, scrolling) waits for a browser (§10).

Deviation: no `need_slabs` event. A row's slab `<img>` asks the slab route when the row is built; the route waits on
the one shared Render, so the browser's own request is the "need". Tiles of a row in view go to the server as one
`need_tiles` batch.

### S5 — Doctor, Make set, Ledger (2026-10-28)

Built: `services/kitchen/doctor.py` (markers from one rule table, each "drops nothing": frames that cannot share picks,
poolable frames, unknown acquisitions, unknown pixel size, picks with no cutout, unscored rows, mixed levels,
template-match settings that differ between members, member errors, orphaned verdicts), `services/kitchen/export.py`
(`plan`: blockers, unverified lines, level, clashes within Ø/2 deduplicated per pooled tomogram with a hand pick first,
then the higher σ; `write_set` in a worker: one member → a row subset, several → `merge_coordinate_sources` or
`merge_optimisation_sets_into_jobdir`; parents, recipe, verdict counts and `kitchen_uids.txt` recorded),
`service.py` (diagnose, choose a frame, prefer a project's reconstruction job for every conflicted acquisition,
plan, make, ledger with readers' headlines from their readouts), `ui/kitchen/sets.py` (Doctor with the frame choices,
Make set with the plan shown first and the button off while anything blocks, Ledger with Compare: the gallery shows the
particles only in A, then only in B, until "back"). `particle_sets.py`: `SetOp.POOL`, `ParentRef.project_path` and kind
`list`, `SET_SCHEMA` 2, `publish_set(..., write_tomograms=)` so a set carries its own tomograms.star;
`path_resolution_service.py`: "Pool set — <name> (N projects)" in the I/O dropdowns. The Doctor button counts the
markers that block or warn.

Checked on scratch homes (copies of the pilot's and copia's params; nothing under /groups moved): the 412 pool at
≥ 9σ makes `s001` with 2,922 coordinates in 464 tomograms from 8 projects in 21 s (the merge reads each member star
once per tomogram), its own tomograms.star (464 rows), provenance, summary and uid list; the resolver labels it
"Pool set — 412 ge9 all (8 projects) · 2,922 coordinates"; the Ledger lists it with the per-project counts. The
ribosome pool (MyrGFP_Grid2 job027 + job028) blocks with 47 acquisitions in two frames; "prefer … job024" settles all
47, and ≥ 10σ then makes 7,729 picks in 41 tomograms. Copia (one member, 1,007 of 1,018 kept rows extracted) plans a
coordinates set by default and makes an extracted one with "only the extracted rows"; its Doctor says acquisition
unknown and mixed levels. The three dialogs build in a NiceGUI simulation.

Not done in S5: the Doctor's by-acquisition listing shows 30 conflicts and leaves the rest to "prefer"; the Ledger's
headlines read only readouts a driver already wrote (no backfill from the kitchen).

### S6 — extras (2026-10-28)

1. **Kept vs removed averages: built.** After each release the server cuts a fixed random sample of up to 200 kept
   and 200 removed cutouts and makes their unaligned means in a worker (`tiles.mean_image`,
   `service.averages`, route `/api/kitchen/average`); the strip shows them beside the edge strip. On the 412 pool
   at ≥ 9σ the kept mean is a sharp ring and the removed mean a fainter one (real particles below the cut); 36 s
   cold, cached after.
2–8. Not built in this session (heat strip, g(r), agreement, set filters and the extracted-stack toggle, review
   sample, loupe, fate chart).

**Modern-Python weave-in.** Filters as a pydantic discriminated union (`kind: Literal[...]`); `StrEnum` for member kinds,
verdicts and sources; frozen dataclasses for per-member column chunks; a `Protocol` for member builders
(`build(member, view) -> Columns`) dispatched with `match`; numpy arrays end to end, pandas only where stars are read.
