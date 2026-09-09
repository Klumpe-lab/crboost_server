# 13 — One pre-seeded ArtiaX list per (species, tomogram), a confirmed in-session switch, one door

**Status:** scoped **2026-09-04** from the maintainer's session with the boss; **S0–S4 CODE-COMPLETE
2026-09-04** (see §5). `ruff check .` green on the whole repo; `python check_boundaries.py` owed
(no python in the agent sandbox). ONE runtime pass owed (§4).
**Risk:** medium — S1 changes the *advertised* ingest contract; S2 reverses a decision of record
(10 §1-2, Model B "nothing drives the viewer after launch"). **Depends on:** 10-S1…S3 (scoped
launch, manifest, display-binned recon), 09-S2 (per-tomogram `curate`), 11-S5 (viewer route).

## 0. The decision

The feedback (2026-09-04) was that we over-built support for **N arbitrary candidate lists per
particle species per tomogram**. In practice one list per (species, tomogram) is the norm and two
is exceptional: a person adds or deletes picks in the list that exists, they do not create another
one and then merge/stitch in a labyrinthine interface. Today the user also has to decide *where*
a new list is created and *what* it is called when hand-picking a new species or tomogram.

**Modus operandi going forward.** For a curation job on a given species and tomogram there is a
**soft-default file** where crboost expects the user's picks. crboost pre-creates it, empty,
named and placed correctly, opens it in the ArtiaX session so the list already exists when the
user arrives, and registers it. The user picks into it and saves it back. Extra lists are still
allowed and still work (the N-lists mechanism stays) — but then the naming and placing burden is
theirs, and the UI stops advertising them.

Decisions locked with the maintainer:

| | Decision |
|---|---|
| Seed name | `<species_id>__<tomo_name>__picks.coords` in `Curation/<species_id>/<tomo>/`; slug `manual__<species_id>__<tomo>__picks`; ~~the table shows the row as `picks`~~ — reversed 2026-09-06: the table shows the file stem like every manual list, with a ⧉ that copies the `.coords` path ("enough indirection"). `species_id` is the token — a stable slug and already the dir name — never the display label, which can change. `__` is the house component separator (`manual__`, `merged__`, `templatematching__ribosome`); tomogram names carry only single underscores. |
| Registration | At *Curate picks* click, as a 0-pick default row: "pick into it in ArtiaX". |
| Rail light | Removed with the other doors. The in-session tomogram's *Curate picks* button carries the live marker. |
| In-session switch | Restored over the quarantined REST channel: one caller, scope rewritten on disk in the same call, confirm-first, `Restart on this tomogram` kept as the fallback. Reverses 10 §1-2; 10 gets re-scoped by appending, not rewriting. |

### Why the seed is cheap

Three pieces already exist and were never joined: `prepare_curation_bundle` reserves
`manual.coords` (`services/visualization/artiax_bridge.py:507` — a path only, never written); the
generated `.cxc` already tells the user "create a NEW particle list and save it here" (:437-442);
the watcher already treats a same-name re-save as an update of that list (`add_pick_list` upserts,
`services/project_state.py:1010`). ArtiaX's `.coords` reader
(`ArtiaX/src/io/Coords/CoordsParticleData.py`, checked 2026-09-04) is a `csv.reader` row loop:
a 0-byte file opens as an empty particle list named after the file, no error.

### Pitfalls, named

- **Saving stays a manual act.** crboost cannot make ArtiaX save into the seed; the user clicks
  *Save particle list* and the dialog opens in the curation dir (`curation_session.sh:201` `cd`)
  with the seed listed — "click the file, confirm overwrite", not "invent a name". Whether ArtiaX
  pre-fills the seed's name from the opened file is a runtime check (§4 item 4).
- **The watcher would ingest the empty seed** as a 0-pick list, and the pick viewer's P4 guard
  (`ui/particles/pick_viewer.py:1311-1324`) would WARN on every render. S1 registers the seed
  deliberately and distinguishes "parsed to 0 rows" from "unreadable".
- **Re-seeding must be idempotent.** A second *Curate picks* on the same tomogram never touches a
  seed that exists, empty or not.
- **`close session` drops unsaved ArtiaX lists.** The switch is confirm-first and does NOT try to
  save for the user — that path (`save … partlist #N`) was never verified and is what the old swap
  pretended to do.
- **`_safe_slug("")` is `"tomo"`** (`artiax_bridge.py:186`): with an empty `species_id` (only the
  CLI `bundle` path) the seed is skipped and the CLI says so.
- **The `saves N` chip** counts every user `.coords`; an untouched 0-byte seed is not a save.

## 1. Stages

| # | Stage | Theme | Risk | Status |
|---|---|---|---|---|
| S0 | this doc + re-scope 10 | index rows; append "[13 note]"s to roadmap 10 where it now lies | — | DONE 2026-09-04 (notes at 10 :3, :59, :83, :99 + §6 log) |
| S1 | pre-seeded default list | seed file + `.cxc` opens it last + registered as a 0-pick `picks` row; idempotent; no false 0-pick warnings; prose | low-medium | CODE-COMPLETE 2026-09-04 |
| S2 | confirmed in-session switch | `swap_chimerax_commands` restored; `switch_session_scope` rewrites `scope.json` + manifest; Switch button beside Restart | medium | CODE-COMPLETE 2026-09-04 |
| S3 | one door | session chip, rail light, viewer-rail `curate ↗` deleted; `[Curate picks]` icon+text button with live dot; viewer/journey icon buttons; hint label gone | low | CODE-COMPLETE 2026-09-04 |
| S4 | honest canvas states | slab render fails loudly + Retry; full-page viewer resets the dedup set; "no candidate preview" empty state instead of a spinner | low | CODE-COMPLETE 2026-09-04 |
| S5 | close-out | statuses, git script | — | statuses written 2026-09-04; git script handed to the maintainer |

Each stage: `notes/pu13-stage-snap/<stage>/` holds a verbatim copy of every file the stage edits,
taken before the edit (`pu13` because a top-level `13-roster-staged-vs-queued.md` exists). Then
`./venv/bin/ruff check .` · `./venv/bin/ruff format --check .` · `python check_boundaries.py`.
S1 is the only hard dependency (S2/S3 name the seed). Build S1→S4 back-to-back; ONE runtime pass
at the end (§4).

## 2. Stage detail

### S0 — docs

Owed: append to `10-external-picker-contract.md` without rewriting history — status line (:3)
"re-scoped in part 2026-09-04 by 13"; a footnote under the model table (:54): 13-S2 reintroduces
exactly ONE outbound command after launch, the confirmed scope switch, and closes fragility #2 by
rewriting `scope.json` + the manifest in the same call; "[13 note]" under S1 (:64-69) and S2
(:74-84): "every `.coords` becomes its own list" stays the *mechanism*, the *advertised* contract
is one pre-seeded list per (species, tomo); a §6 log entry.

### S1 — pre-seeded default list

`services/visualization/artiax_bridge.py`
- After `user_coords_saves` (:223): `SEED_SUFFIX = "__picks.coords"`;
  `default_seed_name(species_id, tomo_name)` = `f"{_safe_slug(species_id)}__{_safe_slug(tomo_name)}{SEED_SUFFIX}"`;
  `seed_coords_path(curation_dir, species_id, tomo_name)`;
  `ensure_seed_coords(curation_dir, species_id, tomo_name) -> tuple[Path, bool]` — `mkdir`, then
  `open(p, "x")` (atomic 0-byte create) → `(p, True)`; `FileExistsError` → `(p, False)`. Never
  truncates. Docstring: 0 bytes is a valid ArtiaX list.
- `session_chimerax_commands(recon_mrc, auto_coords=None, seed_coords=None)` (:382-394): `open <seed>`
  after `open <auto>`, before `lighting simple` — the seed is the LAST model opened, so it is the
  one selected when the user arrives.
- `build_session_cxc` (:406-443): kwarg `manual_coords` → `seed_coords`; comment block (:437-442)
  → "pick into the list opened last (`<seed>`), save it back to the SAME file; crboost updates that
  row within seconds; a save under another name in this folder still becomes its own list".
- `prepare_curation_bundle` (:446-556): delete `manual_coords` (:507); after the ref export
  `seed, seed_created = ensure_seed_coords(...) if species_id else (None, False)`; pass to the cxc;
  `write_manifest(..., seed_coords=seed.name)`; result drops `manual_coords`, adds `seed_coords`,
  `seed_created`; `commands` includes the seed. CLI `bundle` (:613-621) prints the seed or
  "none (no species id)". Module docstring: one sentence.

`services/particles/ingest.py`
- `from services.visualization.artiax_bridge import default_seed_name` (services→services, no cycle).
  `DEFAULT_SEED_LABEL = "picks"`; `default_slug_for(species_id, tomo_name)` =
  `manual_slug_for(default_seed_name(...))` (`fs_slug` is identity on a `_safe_slug`ed ASCII stem);
  `is_default_slug(slug, species_id, tomo_name)`.
- `register_manual_pick_list` (:37-67): `label = DEFAULT_SEED_LABEL if is_default_slug(...) else (src.stem or "Manual (ArtiaX)")`.
  The one writer — every re-ingest keeps the short label. Module docstring (:9-15): seed paragraph.

`services/curation/session_service.py`
- `import_curation_picks` (:697-797): kwarg `archive: bool = True`; skip the `imports/` copy when False.
- `prepare_curation_bundle` wrapper (:605-654): when `info["seed_coords"]`, call new
  `_ensure_seed_registered(project_path, tomograms_star, tomo_name, species_label, species_id, seed, created=)`;
  merge `seed_slug` / `seed_registered` / `seed_error` into the result. The bundle stays `ok` on a
  registration failure — the `.cxc` opens the seed regardless and the watcher registers on the
  first save.
- `_ensure_seed_registered`: `slug = default_slug_for(...)`; `not created` and already registered →
  `ok(slug, registered=False)` (idempotent: count/label untouched); else
  `import_curation_picks(..., coords_path=seed, archive=False)` (0 rows → header-only star,
  count 0) → `register_manual_pick_list` → `save_project(project_path=..., force=True)` (explicit
  path, cf. `list_admin.py:109`). A just-created seed is always registered from the file; an existing
  seed with no registered list is registered from whatever it holds. `logger.exception` + `err`.

Watcher — no code change. Registration at T1 > seed mtime T0 → `watcher.py:185` guard skips it;
first save at T2 > T1 → ingest → upsert same slug with count N, label `picks`.
`list_admin.pick_list_files` (:63-64) already unlinks the seed with the list; the next *Curate
picks* re-seeds.

`ui/particles/pick_viewer.py`
- `_read_pick_list_voxels` (:1243-1271) → `list[dict] | None`: `None` for missing path / unresolved
  geometry / no columns / exception (keep that warning); `[]` only for a table that parsed to 0 rows.
- `_collect_pick_lists_for_species` (:1309-1335): the "read back 0 picks" WARNING only when `picks is None`.

`ui/curation_session_dialog.py` (:80-83): drop `manual_coords`; `save_dir = curation_dir`;
`seed_name` from `seed_coords`. Section 3 (:313-333): "Pick into the list opened last — `<seed>` —
and save it back to that file"; other names still become lists; outside these folders →
UNATTRIBUTED SAVES. `ui/particles/list_actions.py` `curate_in_artiax` (:657-707): warn-toast on
`seed_error`; docstring.

`ui/species/picks_tab.py`: `_save_contract` (:147-158) rewritten to the seed contract; `saves`
chip tooltip (:348-349); `_compute` (:238) `n_saves` excludes the untouched 0-byte seed;
`_render_row` (:428) tooltip on the default row ("the pre-seeded ArtiaX list — save it back to
`<file>`"). `services/particles/list_admin.py` docstring: deleting the seed list deletes the seed.

### S2 — confirmed in-session switch

`services/visualization/artiax_bridge.py`: replace the tombstone (:397-403) with
`swap_chimerax_commands(open_recon, auto_coords=None, seed_coords=None) -> str` =
`"close session ; " + " ; ".join(session_chimerax_commands(...))` — the chain runtime-verified
2026-06-10 (`docs/ARTIAX_BRIDGE_PLAN.md:391-399`) — and `cd_chimerax_command(curation_dir)`. The
`cd` is sent as its OWN call because `send_chimerax_command` fails the whole `;`-chain on any
UserError (`session_service.py:585-600`). Fix the :276-279 comment.

`services/curation/session_service.py`
- `self._switch_lock = asyncio.Lock()`; rewrite :46-49 ("nothing drives a running session") and the
  module docstring (:5-14).
- `_record_scope(..., *, how="launch")` (:191-217): payload + manifest gain `scope_set_by`;
  docstring: `launched_at` now means "became this session's scope at" — it is the watcher's hot-dir
  key (`watcher.py:229-248`), which is exactly what a switch must move.
- New `switch_session_scope(session_info, *, open_recon, auto_coords, seed_coords, scope) -> dict`:
  1. `rest_enabled` off → `err("curation.rest_enabled is off — use Restart on this tomogram")`;
     missing seed/recon → `err`.
  2. under the lock: `send_chimerax_command(session_info, swap_chimerax_commands(...), timeout=120)`;
     failure → `err(f"ArtiaX did not switch: …", raw=)`.
  3. `send_chimerax_command(..., cd_chimerax_command(scope["curation_dir"]), timeout=15)` best-effort → `cd_error`.
  4. `session_dir` missing → `ok(switched=True, scope_recorded=False, warning=…)` (loud, never
     silent); else `_record_scope(Path(sdir), slurm_job_id, scope, how="switch")`.
  5. `ok(switched=True, scope_recorded=True, cd_error=, commands=[chain, cd])`.
- `send_chimerax_command` docstring (:503-509): the channel carries launch-time health checks AND
  the scope switch; any caller must record scope in the same call.

`backend.py` (:1020-1022): tombstone → `switch_curation_session(...)` facade.
`services/configs/config_service.py` (:198-203): `rest_enabled` gates the switch; off ⇒ Restart only.

`ui/curation_session_dialog.py`
- `rest_enabled` read beside `passwordless` (:91-92); import `ui.particles.session_status`.
- Action row (:212-225): `switch_btn = house_button("Switch session to this tomogram", …, kind="accent")`
  before `restart_btn` (restart becomes `default` kind when REST is on — one accent per state).
  Tooltips: switch = re-points the RUNNING ArtiaX over its command channel (close session → open
  tomogram + picks + seed → cd), asks first because unsaved lists are lost; restart = the fallback.
- `_apply` (:383-406): `mismatch = bool(tomo_name) and _refresh_scope()`; `restart_btn` visible on
  mismatch; `switch_btn` visible on `mismatch and rest_enabled and sv["rest_port"]`.
- New `_switch()` mirroring `_restart` (:463-491): confirm dialog under `host_slot` ("ArtiaX closes
  what it has open — anything picked and NOT saved is lost; save your lists there first"); busy box
  "Switching ArtiaX to <tomo>…"; `scope = backend.curation_scope(...)`;
  `backend.switch_curation_session({node, rest_port, session_dir, slurm_job_id}, open_recon=b["open_recon"],
  auto_coords=b["auto_coords"], seed_coords=b["seed_coords"], scope=scope)`; success →
  `sv["scope"] = scope`, `_apply("live")`, notify "ArtiaX now has <tomo> open — pick into <seed>",
  warn on `cd_error` / `scope_recorded=False`, `await session_status.poll(backend, force=True)`;
  failure → negative notify "… — use Restart on this tomogram", Restart stays.
- Prose: Section 2 (:289-293), troubleshooting (:164-165), `_refresh_scope` docstring (:360-364),
  module docstring (:14-19); `list_actions.curate_in_artiax` (:663-666);
  `picks_tab.PicksTab.curate` (:741-749) and `_render_group_actions` (:396-401).

### S3 — one door

- `ui/components/buttons.py` `house_button` (:40-50): optional `icon: str | None = None` →
  `ui.button(label, icon=icon, on_click=…, color=None)`.
- `static/icons/journey.svg` (new): the `_TOMO_DASHBOARD_SVG` markup with `width="18" height="18"`
  on the root (`load_icon_svg`'s `size=` only rewrites existing attributes, `ui/components/svg_icon.py:31-33`).
  `ui/pipeline_builder/pipeline_roster.py`: delete `_TOMO_DASHBOARD_SVG` (:52-62);
  `_build_dashboard_btn` (:1796-1801) → `_sb_svg_btn("journey.svg", …)`. Keep `_load_svg`'s inline
  branch (:1629-1632) — `pipeline_constants.PHASE_META` still passes inline SVGs.
- `ui/species/picks_tab.py`
  - Delete the hint label (:270); give the `PICKS & CURATION` title the `_save_contract` tooltip.
  - Delete the session chip: `_render_header` (:294-312) keeps only `Extract all pending` (fold into
    the title row); delete `_SESSION_TEXT` (:101-106), `PicksTab.open_control_center` (:731-738),
    the `open_curation_control_center` import (:80). `_render_no_picks_hint` (:316) → "'Curate picks'".
  - `_render_group_actions` (:395-420): `house_button("Curate picks", …, icon="view_in_ar", tooltip=…)`
    — tooltip: prepares the bundle, pre-seeds `<sp>__<tomo>__picks.coords` as a 0-pick list, never
    overwrites an existing seed, opens the control center; with a session on another tomogram it
    offers Switch or Restart. Live marker: `span.cb-artiax-live` (7 px green dot, tooltip "the live
    session has this tomogram open") when `self._tab.session_live_here(tomo)`. Viewer →
    `ui.button(icon="grid_view").props("flat dense round size=sm color=indigo")`; journey →
    `ui.button(...)` wrapping `ui.html(load_icon_svg("journey.svg", "#4f46e5", size=16), sanitize=False)`.
    Tooltips carry the old link text.
  - `PicksTab.session_live_here(tomo)`: `session_status.is_live()` and `scope()` species_id/tomo_name
    match (no I/O; the tab already polls `session_status`, :634-640). `_PicksView.signature`
    (:254-262) adds the scope's (species_id, tomo_name).
- `ui/pipeline_builder/pipeline_roster.py`: delete `_build_curation_session_btn`,
  `_tick_curation_session`, `_paint_curation_session`, `_open_control_center` (:1853-1940), the call
  (:1033), `_curation_paint` (:214-216), now-unused imports (:16-17); fix comments (:1803-1808,
  :1027-1031); `_CURATION_TICK_S` (:41-44) → `_RAIL_LIGHT_TICK_S` (still the protocol light's timer, :1831).
- `ui/dashboard/css.py`: keep `.cb-artiax-live` (the marker); delete `.cb-list-toolbox` /
  `.cb-toolbox-link` (:364-376); fix the :1278-1286 comment.
- `ui/particles/pick_viewer.py`: delete the toolbox `curate ↗` (:2443-2456); drop the now-dead
  `manage_species` parameter from `_render_list_rail` (:2328, call :2248) and
  `_render_species_tab_body` (:2162, positional call :1944). Keep `manage in Particles registry ↗`
  (:1866) and `← Particles registry` (:4267) — navigation, not a launch.
- `ui/curation_session_dialog.py`: `bundle` becomes required (opened only from Curate picks);
  delete the no-bundle prose (:74-76) and branch (:304-308). `ui/particles/session_status.py`
  docstring: one observer.
- Grep gate: `open_control_center`, `_SESSION_TEXT`, `curation_btn|curation_icon|curation_tip`,
  `_TOMO_DASHBOARD_SVG`, `cb-toolbox-link`, `cb-list-toolbox`, `manual_coords` → zero hits.

### S4 — honest canvas + preview states (`ui/particles/pick_viewer.py`)

Root causes (2026-09-04, "Rendering tomogram slices… spins forever in a project with no picks"):
(a) `_render_recon_slabs_sync` (:348-352) ignores `None` from `render_xy_slab_preview` /
`render_xz_slab_preview` (`services/visualization/preview_render.py:122-160, :193-240` return
`None` on missing deps / non-3D MRC / read error), so the BackgroundTask settles *succeeded* with
no PNG; `on_complete` fires once, the canvas (:2006-2013) re-renders as a spinner, and
`_AUTO_KICKED_RECON_SLABS` (:340) blocks any re-kick for the process lifetime —
`reset_auto_kick_state` (:3948) is called only by the Journey mount
(`ui/tomo_dashboard_dialog.py:208`), never by `PickViewerPage`. (b) the sibling "Generating
preview…" placeholder (:2501-2508) spins even when `_auto_kick_preview_generation` (:3958-4003)
submitted nothing (candidate-extract not SUCCEEDED, or no `candidates.star`/`tomograms.star` — the
no-picks project); that function also returns `False` for an in-flight kick, contradicting its
docstring (:3962-3963).

- `_RECON_SLAB_ERRORS: dict[str, str]` beside :340. `_render_recon_slabs_sync` raises
  `RuntimeError` naming the renderer, MRC and PNG when a renderer returns `None`.
- `_auto_kick_recon_slabs` (:354-387) returns `"fresh" | "failed" | "in-flight" | "kicked"`;
  `on_complete` records `task.error or task.status` in `_RECON_SLAB_ERRORS` when
  `task.status != "succeeded"` (`services/background_tasks.py:63-70`), then `refresh()`.
- `_render_particles_canvas` (:2004-2013): spinner only for `kicked` / `in-flight`; `failed` →
  `cb-empty` with red `error_outline`, "Slab render failed — <reason>" and a `house_button("Retry")`
  that pops the key from both sets and refreshes; any other state with a missing PNG → "Slab PNG
  missing after a successful render — <paths>". Never a spinner without in-flight work.
- `reset_auto_kick_state` (:3949) also clears `_RECON_SLAB_ERRORS`; `PickViewerPage.show` (:4186)
  calls it before rendering.
- `_auto_kick_preview_generation` (:3958-4004) returns `str`: `""` = kicked or already in flight,
  else the reason ("candidate-extract has not finished for this species" / "… has not produced
  candidates.star / tomograms.star for this tilt-series"). `_ce_species_entry` (:1431) stores it as
  `sp["preview_note"]`; `_render_species_auto_section` (:2501-2508) renders "No candidate preview —
  <note>; run Pick candidates from the Jobs tab" instead of the spinner when the note is non-empty.

### S5 — close-out

Statuses here, in `00-overview.md` and `README.md`; hand the maintainer the git script (one
command per line, no `&&`).

## 3. Non-goals

No auto-save before a switch. The N-lists machinery (watcher ingests every `.coords`, per-list
extraction, merge) stays — only its advertising goes. No change to the napari pilot (10-S4). No
new `PickList` field: "is the default" is derived from the slug.

## 4. Runtime checklist (one pass, maintainer)

1. Picks & curation: no session chip, no hint label, no rail ArtiaX light, no `curate ↗` in the
   viewer rail; each tomogram row shows `[Curate picks]` + two icon buttons (grid, journey bars).
2. Curate picks on tomogram A → `Curation/<sp>/<A>/<sp>__<A>__picks.coords` exists, 0 bytes;
   `open.cxc` opens it last; the table shows a `picks` row with 0; `manifest.json` names `seed_coords`.
3. Curate picks on A again → seed mtime unchanged, no second row, no watcher event.
4. Start session; ArtiaX shows the empty list; pick 3; *Save particle list* → same file (note
   whether the dialog pre-fills the name) → within ~7 s the `picks` row reads 3; the green dot
   appears on A's row.
5. Save under another name → a second row appears (mechanism intact).
6. Delete the `picks` list → seed gone; Curate picks again → re-seeded 0-pick row.
7. Session live on A; Curate picks on B → control center shows "DIFFERENT scope", Switch (accent)
   and Restart; Switch → confirm → ArtiaX closes A, opens B + B's seed; `scope.json` names B; B's
   manifest gains `launched_at` / `scope_set_by: switch`; the green dot moves to B within ~20 s;
   a save in ArtiaX lands in B's folder (cd worked) and ingests as B's list.
8. `curation.rest_enabled: false`, restart crboost, repeat 7 → only Restart shows; it lands B.
9. Restart crboost mid-session after a switch → control center still names B (scope on disk).
10. Pick viewer on a tomogram whose recon MRC is unreadable → "Slab render failed — …" with Retry,
    not a spinner; restore the file → Retry renders.
11. Pick viewer on a species with no candidate-extract output → "No candidate preview — …" instead
    of a spinner.
12. Server log: no "read back 0 picks" WARNING for the seeded list.
13. (2026-09-06 fix) Species A then species B on the SAME tomogram, via Switch: the toast names B's
    seed model id and says the dialog now opens in B's folder; section 3 shows a `Command` row with
    `save <B seed> partlist #id`; the row in Picks & curation reads `<B>__<tomo>__picks` (no
    `picks` alias) with a ⧉ that copies the path. **The check that matters:** open *Save particle
    list* WITHOUT navigating — it must open in B's folder listing B's seed (the `runscript` did its
    job; ChimeraX's log shows `crboost: save dialog now opens in …`). Save → B's row reads N within
    ~7 s (hot dir moved at the switch), A untouched. If the dialog still opens in A's folder, the
    Qt lever failed: note the toast, paste B's path or run the command instead — both still land B.
14. Deliberately save into A's folder while scoped to B → the WATCHER footer shows a red `off-scope`
    event naming B; A's row updates (mechanism intact) and `imports/` holds A's previous content.
15. Load a project with rows labelled `picks` → server log `Relabelled seeded pick list …` and the
    table shows the stems.

## 5. Log

- 2026-09-04 — scoped. Forks settled: namespaced seed name, register at Curate click, rail light
  removed. Facts verified: ArtiaX empty-`.coords` behaviour (source), old swap chain runtime record
  (`ARTIAX_BRIDGE_PLAN.md:391-399`), `send_chimerax_command` chain-failure semantics, both spinner
  root causes.

- 2026-09-04 — **S0–S4 CODE-COMPLETE**, built back-to-back in one session; snapshots under
  `notes/pu13-stage-snap/s0…s4/`. Deviations from the stage text, all small and named:
  - **S1** the CLI `bundle` gained `--species-id` (the seed needs the registry id, which the CLI
    had no way to pass; without it the CLI prints `seed: none (no species id …)` as scoped).
    `ui/tomo_gallery.py` — not in the stage's file list — took a one-line `or []` because it
    `extend`s what `_read_pick_list_voxels` returns and that is now `None` for unreadable.
  - **S2** `switch_session_scope` requires the target's `seed_coords` (a bundle without one is a
    pre-13 bundle; the switch refuses rather than open a session with no default list). The `cd`
    error is reported through a separate warning toast, never folded into "switched".
  - **S3** the live dot is rendered BEFORE the `Curate picks` button (a row that gains it must not
    shift the button under a cursor already on it). `Extract all pending` moved into the title row
    when the session chip left it (the stage text said "fold into the title row").
  - **S4** `_auto_kick_recon_slabs` reports four states (`fresh` / `failed` / `in-flight` /
    `kicked`); "Slab PNG missing after a successful render" is the fifth, unreachable-by-design
    branch kept for honesty. `reset_auto_kick_state` also clears `_RECON_SLAB_ERRORS`.
  - Grep gate (§2 S3) is zero-hit across `*.py` and `*.md` outside `docs/roadmaps` and `notes/`.

- 2026-09-06 — **first runtime contact, one real bug, fixed.** Maintainer picked two species on the
  same tomogram (`somespecies_denovo` then `newspecies`, Position_1 of `denovo_picking_testdrive`);
  the second save overwrote the first species' seed. Disk timeline: switch recorded 13:55:22 → save
  landed in the PREVIOUS scope's seed 13:55:46 → watcher re-ingested it as the previous species.
  Root cause, from the ChimeraX + ArtiaX sources: ArtiaX's *Save particle list* is ChimeraX's
  `MainSaveDialog`, a QFileDialog that opens in Qt's **last-visited directory**; only the very first
  dialog of the process falls back to the cwd. So `cd` steers the first save after launch and nothing
  after — after a switch the dialog opens in the previous folder listing the previous seed, and the
  list chooser (which defaults to ArtiaX's `selected_partlist`, i.e. the newly opened seed) writes
  the NEW picks into the OLD file. Fixed, without crboost driving a save:
  - `switch_session_scope` now VERIFIES via `info models` that the seed's model is open (its name is
    the file's basename) and refuses the switch otherwise; the model id it finds becomes a
    pasteable `save <seed path> partlist #id` (`artiax_bridge.save_chimerax_command`) shown in the
    control center's section 3 for the USER to run — the dialog-proof save.
  - Prose everywhere (`.cxc` comment, section 3, the Picks tab contract) now states the dialog
    truth; section 3's *File* row copies the FULL seed path (was: the name).
  - The seeded row is labelled by its file stem like every other manual list (no `picks` alias —
    "enough indirection"); `ingest.relabel_seed_rows` fixes rows written while it was aliased. Every
    hand-picked row has a ⧉ that copies its `.coords` path.
  - The watcher records an **off-scope** event (red, in the WATCHER footer) for a save newer than
    the hot dir's `launched_at` that landed in another dir — the mechanism still files it where it
    landed; the event is the never-silent half. Its previous content is always in `imports/`.
  Not built (needs a maintainer decision): a hard guard that refuses to ingest an off-scope save
  and offers "move to the scoped seed + restore the previous content from imports/".
  Snapshots: `notes/pu13-stage-snap/s6-save-folder/`.

- 2026-09-06 (later) — **"confirm TOMO, LIST and DIRECTORY"** + ingest latency. TOMO and LIST were
  already right (chain order + `info models` verification). DIRECTORY was not, and `cd` cannot
  make it so: Qt opens every fresh QFileDialog in its process-wide *last visited* directory, and
  ArtiaX's save button passes no directory (`ArtiaXSaveDialog.display(initial_directory=None)`,
  verbatim). The one lever: `QFileDialog.setDirectory()` on a hidden dialog SETS that global. So
  `prepare_curation_bundle` now writes `<curation_dir>/set_save_dir.py`
  (`artiax_bridge.write_save_dir_script`), the `.cxc` ends with `runscript <it>`, and the switch
  sends the same `runscript` as its own best-effort call (`save_dir_error` → warning toast; the
  pasteable `save … partlist #id` stays as the fallback). Runtime-unverified — §4 item 13 checks it.
  **Latency:** the hot dir moved only on the 30 s full sweep, so the first save after a launch or
  switch waited up to ~32 s. `CurationWatcher.mark_hot` is now called by the backend facades on
  launch and switch (~7 s to a row update), and the Picks & curation title row has **Refresh**
  (`watcher.sweep_now` under a new tick lock + immediate recompute) for everything else.
  Snapshots: `notes/pu13-stage-snap/s7-save-dir-refresh/`.
