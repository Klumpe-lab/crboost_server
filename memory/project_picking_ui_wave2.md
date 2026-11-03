---
name: picking-ui-wave2
description: "picking_ui/08 + 09 + 10(S1-S3) + 11(S1-S6) ALL CODE-COMPLETE PENDING ONE RUNTIME PASS (chrome vocabulary; Picks&curation merged; Model B external-picker contract; pick viewer extracted to ui/particles/pick_viewer.py); 12 CODE-COMPLETE 2026-08-22 (all S0–S6, PENDING RUNTIME), 10-S4 napari gated"
metadata: 
  node_type: memory
  type: project
  originSessionId: b41fe27c-100e-4454-b4a0-d880e515ff79
  modified: 2026-08-22T00:00:00.000Z
---

2026-08-21 intake (`q_important_ui_fixes.md:9-55`, rows P-32…P-48 in particles-ux-peeves.md) scoped
into `docs/roadmaps/picking_ui/`: **08** one input+button vocabulary app-wide (`ui/components/fields.py`
promotion of workbench `_field()`, purge conflicting legacy `.cb-field` in `static/main.css:194-260`,
delete `main_ui.py:94-138` duplicate block, 108 raw label-buttons → house_button, border diet,
viewer-jump fix = Edit-current height swing + unstacked viewer-mode switcher, "Particles registry"
rename 6 remaining sites); **09** Picks+Curation → one "Picks & curation" tab (9 ArtiaX entry points
→ 1 per-tomogram curate action, one import, merge bar out, origin template+mask column); **10**
external-picker contract — Model B ADOPTED: no post-launch REST driving (delete control-center
Load/Save), scoped launch writes manifest.json, watcher ingests EVERY .coords per-stem (kills
newest-only + slug="manual" collapse), unattributed inbox gains explicit species assignment,
display-binned recon for load speed, napari pilot GATED (PyQt5 container, same contract); **11**
gallery → `ui/particles/pick_viewer.py` shared component (full page in registry / slim in Journey),
click=SELECT + explicit curation mode, 3dmod one toolbelt icon, hover info row deleted, ref strip
collapsible + best-4 added, lists strip above slabs+gallery; **12** aggregate-candidates stub
(roster merge icon rename; open edge: Pick candidates has no list-input mode — ties to archived #68).

**Why:** the maintainer's wave-2 walkthrough; organizing principle = separate (1) display info /
(2) filter tools / (3) de-novo picking.

**2026-08-21 (later session): 08 is CODE-COMPLETE, all S1–S6, uncommitted, PENDING RUNTIME**
(checklist in the roadmap doc §4 + full landing log in §5). Landed: `ui/components/fields.py`
(house_field/number/text/select; house_number(model=,attr=) routes numeric_forward so int corruption
is unreachable); legacy `.cb-field*` purged from static/main.css; main_ui shell duplicate block
deleted — ui/dashboard/css.py is the single CSS source and `ensure_assets_loaded()` now runs on
`/` and `/cluster-info` too; ~75 buttons → house_button (4 parallel subagent passes; segmented
switchers/icon-only/zoom-res selector deliberately kept); workbench border diet (flat session rows,
no tool-group tint, Activity log = house header+caret, tray gap not border); viewer jump killed
(Edit-current constant height via always-render+disable, viewer modes grid-stacked with
visibility toggling — NOT set_visibility, display:none would collapse the grid cell); rename done.
`ruff check .` green; user owes `import main` + `check_boundaries.py` + the §4 visual pass.

**2026-08-22: 09 is CODE-COMPLETE, all S1–S5, uncommitted, PENDING RUNTIME** (§4 checklist +
per-stage log in the roadmap doc). `ui/species/curation_tab.py` DELETED; `page.TABS` is
Overview · Templates & masks · Picks & curation · Jobs (tab KEY stays `picks`, so every existing
deep-link keeps working). Three deliberate deviations from the scoped §2, all recorded in §5:
(a) groups render over the whole tomogram UNIVERSE, not just tomograms with rows — a pick-less
tomogram is where de-novo picking starts and is an import target, and the species-level import
button was deleted; (b) per-tomogram `curate` carries the Journey ⚡'s branch (live session → swap
via `load_tomo_into_session`, else bundle + control center) rather than always opening the control
center — that also keeps `load_tomo_into_session` from becoming dead code; (c) column order is
source · origin adjacent (one provenance story), not split by `picks`. S4 origin chain =
CE instance → `paths["input_tm_job"]` → parent dir == TM job's `job_dir` → its template/mask; a
recorded SNAPSHOT, in-memory only, verifiable headlessly via
`python -m services.particles.species_overview --project <proj>`. Fallout worth knowing: the Journey
no longer polls `squeue` or folds `session_status` into `_main_signature` (it shows no session state
now), and `_tomo_ref` + the `ListRef`/`list_actions` imports are gone from `tomo_dashboard_dialog.py`.

**How to apply:** build 08 → 09 → 10 → 11; 12 unscheduled. 08, 09 AND 10 owe ONE runtime pass, together. GOTCHA: 08-S0 is a restart-and-recheck
gate — mask "From template" default, house buttons, and 16px `.cb-field` chrome were ALREADY in the
tree on 2026-08-21 (the complaint described the pre-restart build). ChimeraX identity fragility
evidence (5 points: .coords carries nothing, dir = last swap, in-memory `_curation_loaded` amnesia,
newest-only ingest, "manual" slug collapse) is in 10 §1. Related: [[project_denovo_picking]],
[[project_artiax_bridge]], [[feedback_ui_chrome_conventions]], [[project_species_registry_arc]].

**2026-08-22: 10 is CODE-COMPLETE, S1+S2+S3, uncommitted, PENDING RUNTIME** (S4 napari still gated —
untouched). Model B is now the code: crboost sends a curation session NOTHING after launch. Gone —
`load_into_session` / `save_curation_picks` / `save_session_particle_lists` / `get_curation_loaded` /
`loaded_curation_dirs` (service + backend), `artiax_bridge.swap_chimerax_commands`,
`list_actions.load_tomo_into_session`, the control center's Load + Save buttons, and
`tomo_dashboard_dialog._handle_open_list_in_artiax`/`_artiax_inputs` (the "W1 foundation" — W1 closed
another way). `send_chimerax_command` survives quarantined (launch-time health checks only);
`curation.rest_enabled` now gates only that. `picks_tab.curate` lost its liveness branch — this
REVERSES 09's deviation (b), and deliberately.

What replaced the swap: identity is DECLARED once. `prepare_curation_bundle` writes
`Curation/<sp>/<tomo>/manifest.json`; `launch_curation_session(scope=…)` stamps `launched_at` there
and writes `<session_dir>/scope.json`, which both `find_active_curation_session*` merge into their
result — so the panel names the running session's scope even after a crboost restart. The watcher
attributes from the manifest (slug matching only for pre-10 dirs), ingests EVERY settled `.coords`
per dir, and derives its hot set from manifest `launched_at`. Slugs are per-file:
`ingest.manual_slug_for` → `manual__<stem>`, star `<slug>.star`; same name = same list = W1 closed.
`migrate_legacy_manual_slugs` runs inside `ProjectState.load` (BEFORE pipeline_order is derived) and
re-keys list + authoritative choice + extraction instance + `source_overrides` naming the old
synthetic producer; files are NOT renamed. Unattributed rows carry their files and gained
`assign ↗` → `assign_unattributed_coords` (moves + writes the manifest), with NO pre-selected
species. Peeves: panel is `no-backdrop-dismiss` (Esc closes; `on_value_change` runs the teardown)
and `curation.passwordless_vnc` → `CX_VNC_NOPASS` → `-SecurityTypes None`.

S3 = `curation.display_bin` (default 2) → `ensure_display_recon` caches `<recon>_cbdisp<N>.mrc`.
THE THING TO REMEMBER: block-binning shifts the corner origin by exactly `(N-1)/2 * px_full`
(binned voxel v sits at full-res v*N + (N-1)/2), so export subtracts it and import adds it, recorded
per dir as `corner_offset_angst`. N=1 ⇒ 0.0 ⇒ byte-identical to pre-S3. Residual, stated in the
import dialog rather than hidden: a `.coords` imported by explicit PATH from outside a curation dir
is read as full-res, so a session save moved out of its folder lands ~3.1 Å off at N=2.
Added beyond scope: **"Restart on this tomogram"** in the control center (confirmed), shown only when
a live session's scope differs — without the swap there was otherwise no way forward from
`curate` on a second tomogram. That is the answer to 10 §4's open question 1.

**2026-08-22: 11 is CODE-COMPLETE, all S1–S6, uncommitted, PENDING RUNTIME** (§4 checklist + a
per-stage log with every deviation in the roadmap doc §5). `ui/particles/pick_viewer.py` (4.3k lines)
is the component; `tomo_dashboard_dialog.py` went 5775 → 2110 and now IMPORTS it — the dependency is
one-directional (the move took the recon-slab renderer, the Invert switch and the whole
candidate-extract preview/IMOD generation tail with it, because nothing else used them).
`reset_auto_kick_state` moved with its four dedup sets. Deleted as dead: `_LIST_TYPE_TAG`.

Structure to remember: `ui.tab_panels` is GONE. The species tabs drive ONE shared `lists_host` +
`detail_host` pair (that is what lets the lists strip run full width across the top, which is what
fixed the slabs/gallery top misalignment). The shared hosts reintroduce a race the panels made
impossible — the detail pane renders one tick late off a `once`-timer — closed by a generation
counter (`is_current`). Slab caps are `_SLAB_CAPS`: 60vh/34% slim, 74vh/46% full.

Click model: click = SELECT (amber tile border + `.cb-ghost-selected` amber ring on the slab dot;
Esc / click-away clears, both via one delegated handler in `_MARQUEE_JS`). Curation mode is a
toolbelt toggle, OFF by default and FULL-MODE ONLY, and it gates keep/drop, the ghost-dot click and
the marquee — which refuses to start without `.cb-curating` on the grid. `cbpicktoggle` →
`cbpickclick`. Both gallery backends (auto subtomo atlas, workbench recon contact sheet) share the
toolbelt now: sort · display filter · 3dmod · curate · fullscreen, each a popover.

Deviations worth not re-litigating: the vertical hover card SURVIVES for the scatter fallback (only
the horizontal gallery info row + `fillHover` + `_build_pick_meta_for_js` died); "collapse-refs" is
the reference strip's own inline caret, not a second toolbelt icon; the slab lightbox is wheel =
ZOOM / drag = pan, NOT "wheel = Z" — there is no Z stack, `render_xy_slab_preview` writes one
central-Z-average PNG; the not-extracted Extract verb is on WORKBENCH lists only, because the auto
list follows its subtomo job (the registry's own rule in `picks_tab._render_actions`).

Full page = `PickViewerPage`, a 4th workspace container next to pipeline/workbench/journey, opened
by `callbacks["open_pick_viewer"](species, tomo)` from the Picks & curation group's new `viewer ↗`;
the roster hides for `"viewer"` like it does for `"journey"`. Its `refresh` is flag + 0.2 s timer
(the auto-kick background renders call it with NO client context).

**2026-08-22: 12 is SCOPED (not built) — the blocking decision DISSOLVED, not resolved.** ② Pick
candidates is a peak-finder over `tmResults/*.mrc` volumes; it cannot consume a coordinate list at
all, so "spawn a Pick candidates job prepopulated with the combined list" has no referent. A union
of coordinate lists IS what ② would have produced, so the terminal action skips ② and spawns per
payload GRADE: L1 coordinate merge → the merged PickList's per-list Extract; L2/L3 coordinate merge
→ a `subtomoExtraction` job; pixel grade → wire as `mergedSources`, offer Reconstruct/Class3D. No new
job type, `#68` stays archived. Maintainer decisions D1–D5 + stages S0–S6 are in the rewritten
`docs/roadmaps/completed/picking_ui/roadmap_12-aggregate-candidates.md`; the reference figure is the new
`docs/particle-data-flow.md` (see [[reference_particle_data_flow]]). One species per merge, ALWAYS —
that is what killed the geometry ambiguity that made the stub hard. Two defects found while scoping,
now stages: 12-S0 (pixel-grade merges accept mismatched `rlnImageSize`/`rlnTomoSubtomogramBinning`
with only a `[MERGE WARN]` print) and 12-S6 (`list_actions.merge_lists` has had ZERO call sites since
09-S3 — the app currently cannot create a merged pick list at all). Successor roadmap named in 12 §4:
fine-grained collate-filters.

**2026-08-22 (same session): 12-S0 + S1 + S2 + S3's service half are CODE-COMPLETE**, all pure
service layer, all committable AHEAD of the 08–11 wall (nothing in `ui/` imports them yet).
`services/subtomo_merge.py` gained a third optics tier `GEOMETRY_OPTICS_COLS` — absence tolerated,
divergence FATAL (not promoted into CRITICAL, because `strict` treats a missing critical column as a
hard KeyError and an omitted box is not a mismatch). New `services/particles/tomo_identity.py`
(distinguishing vs acquisition key, `read_tomo_hand` — `rlnTomoHand` is NOT a tomograms.star column,
it lives in the Import job's tilt_series.star via `rlnTomoTiltSeriesStarFile` — and a
blocking/unverified split). New `services/particles/coord_merge.py` (515 lines): `CoordSource` →
`plan_merge` → `merge_coordinate_sources` + `clash_report`, with a headless CLI. DEFAULT IS
CONSERVATIVE: with no `registry_lookup`, two same-named tomograms in different projects are kept
APART under `<name>__<project>` and reported as unverified — pooling requires a matching
acquisition key AND the transferability gate. Two more silent-failure defects guarded while writing
it: foreign `tomograms.star` paths may be project-relative (→ `_absolutize_paths`), and stacking
different tomograms.star schemas NaN-fills gaps that extraction then skips per-TS without
complaining (→ raises). NEXT = S4 (the dialog, first UI stage), S5 terminal toggle, S6 the
merge-creation regression.

**2026-08-22 (same session, later): ROADMAP 12 IS CODE-COMPLETE, all S0–S6, PENDING RUNTIME.**
S4–S6 = new `ui/aggregation/aggregate_dialog.py` (648 lines) + `discover_pick_list_projects` in
`services/aggregation/discovery.py` + a 3-line change in `pipeline_roster._build_aggregation_merge_btn`
(icon now opens it, tooltip "Aggregate candidates"). THOSE THREE TOUCH `ui/`, so from S4 onward this
folds into the 08–11 commit wall; S0–S3 still commit clean on their own. KEY DEVIATION: S4 is a NEW
module, NOT a grade switch inside `merge_card` — that dialog is built entirely around
`AggregationSource` (one entry per optset) and a coordinate source is a LIST (several per tomogram,
usually no optset at all). `merge_card` is UNTOUCHED; the two are linked by an "extracted particles
instead ↗" line. Cross-project needed a second discovery path because
`discover_subtomo_optimisation_sets` only finds projects that have EXTRACTED something — wrong filter
one stage earlier; the new one keys on `project_params.json` and reads foreign
`get_project_state_for(p).pick_lists`. S5's verb follows SHAPE: single-tomogram-this-project →
`backend.extract_pick_list` for real; wider → points an existing subtomoExtraction's
`input_optimisation` at the aggregate and STOPS (never submits — box/crop/binning are the species'
decision). Refuses outright when the species has no committed `ExtractionParams` rather than guessing
a box. GOTCHA FOR NEXT SESSION: a `perl -0pi` with a WIDE-CHAR replacement (`\x{2014}`) double-encodes
every non-ASCII byte in the whole file — it hit `pipeline_roster.py`, was caught by ruff E902/RUF001
and repaired via `iconv -f UTF-8 -t ISO-8859-1`; use raw byte escapes or sed for non-ASCII, and
spot-check the roster chips (—, Å, ▸, ·, °) at runtime.
