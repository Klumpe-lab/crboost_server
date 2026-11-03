---
name: project_artiax_bridge
description: "ArtiaX manual-picking bridge at the gallery stage — agreed plan, locked decisions, cluster data-access facts"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7696e382-def9-4276-8905-0b9780e39224
---

**S18-build (2026-06-12) — aggregation foundation steps 1-2 BUILT (code-clean: py_compile+ruff+format; PENDING RUNTIME).** Per
docs/roadmaps/completed/roadmap_list-extraction-and-aggregation.md §8.9: (1) `picks_filter.sync_filtered_count(pl)` — headless filtered_count sync so
`PickList.extraction_state()` is correct OFF the render path (generalizes the badge fix so aggregation reads don't false-STALE).
(2) NEW `services/aggregation_authoritative.py` — PURE/HEADLESS (takes explicit ProjectState, NO UI import, resolves job dirs from
`state.relion_job_name`/`job_path_mapping` NOT the client-context `ui.dashboard.data._job_dir_for` which reads get_project_state()):
`AuthoritativeHandle`, `resolve_authoritative_optset` (auto/filtered→subtomo `resolve_canonical_optset`; workbench→`PickList.extracted_path`
+`extraction_state()`), `enumerate_authoritative` (per-species (tomo,slug,kind,state,kept/total) rows; tomo universe = `load_tomo_curation`
∪ pick_list tomos), + a `__main__` CLI + `backend.get_authoritative_extraction_status` passthrough. READ-ONLY, NO merge, no UI table yet
(placement deferred to user). VERIFY in the module env: `python -m services.aggregation_authoritative --project
/groups/klumpe/crboost_data/agg_20260311_412_Grid3 --species 412` (now prints per-tomo rows + a `ready/pending/blocked → roll-up
READY|BLOCKED` summary). **§8.3 GATE DONE (S18, code-clean): `compute_gate_report`/`GateReport` (ready=EXTRACTED; pending=workbench
NOT_EXTRACTED|STALE → extractable; blocked=auto/filtered-missing-optset or `kind='dangling'` slug-with-no-PickList) +
`extract_inputs_for_list` (resolves candidate_optset/list_star/box-bin from the PyTOM + subtomo jobs, headless) + `backend.get_authoritative_gate_report`
(read-only) + `backend.extract_authoritative_pending` (submits per-list `extract_pick_list` for pending lists, polls out-dirs via
`_await_extraction_outdirs`, `mark_extracted`+persist; idempotent; never touches auto/filtered/ready; RUN IN A BackgroundTask). Read-only
gate CLI-verifiable; the ACTION awaits UI wiring for its runtime test (no caller yet → can't fire by accident).** NEXT = §8.4-8.6
namespacing + box/apix hard-blocks + rollup via subtomo_merge → species-level optimisation_set, then §8.7 forwarding. (Can't import-test
in Claude's venv — no pandas; py_compile+ruff only.)

**S18-runtime (2026-06-12) — Slice C extraction CONFIRMED WORKING + stale-badge bug ROOT-CAUSED & FIXED + new aggregation doc.**
User ran Extract on a manual list (agg_20260311_412_Grid3 / Position_13 / `manual`): the SLURM job ran in ~6s (missed in a single squeue
→ NOT "no job"), correctly extracted the 4 KEPT picks (out/optimisation_set.star + result.json{ok,count:4}), but the badge read ⚠ STALE.
ROOT CAUSE = the `filtered_count`-cache hazard (pre-runtime watch-item #2): persisted PickList had count=7, **filtered_count=None**,
extracted_count=4 → extraction_state compared 4 vs the fallback count(7) → false STALE. FIX (PENDING RESTART): `_collect_pick_lists_for_species`
syncs `pl.filtered_count` from disk truth (`_memoized_keep_state`) + `mark_dirty()` (tomo_dashboard_dialog.py:2744-2756) → badge reads ✓.
Wrote **services/visualization/LIST_EXTRACTION_AND_AGGREGATION.md** (workflow-mapped, citations verified against code): today's list
extraction/merge/forwarding + a STAGED design for SEAMLESS cross-tomo/cross-project aggregation of AUTHORITATIVE lists → one species-level
optimisation_set → Class3D, with a GATE (auto-extract or one explicit list; never silently ship un-extracted/stale picks). KEY GAPS named:
`PickList.extracted_path` is read by NOTHING downstream; the aggregator (aggregation_discovery + ui/aggregation_merge_card) is blind to the
pick-list model / `authoritative_pick_lists`; `'auto'` authoritative has no per-tomo extracted_path (needs job-level slice); extraction is
never gated on authoritative; cross-project rlnTomoName HARD-ABORTS in subtomo_merge (no namespacing). Reuse story: subtomo_merge +
extract_pick_list + picks_filter + the IO-slot scorer (path_resolution_service.py:590 `(species_match,pref,filtered_pref,job#,succeeded)`)
all reused as-is; NEW = authoritative→optset resolver + enumerate/gate/rollup orchestrator + namespacing pre-flight + species-level
synthetic producer. Build order in doc §8.9; smallest/highest-leverage first = move the filtered_count sync off the render path. This doc
supersedes the old "downstream FORWARDING capstone" with a fuller plan. See [[feedback_per_list_extraction]].

**S18 (2026-06-12) — Slice C PRE-RUNTIME adversarial review + 3 surgical fixes (code-clean, PENDING RUNTIME; no auto-reload →
restart main.py).** A 5-lens workflow review of the per-list extraction path found 1 silent-correctness bug + 3 should-fixes;
3 applied: (1) **[MUST]** Re-extract reused STALE subtomograms — `backend.extract_pick_list` cleared the exit markers but NOT
`out_dir/out/`, so the driver idempotency skip (`out/particles.star`+`out/Subtomograms/`) skipped `relion_tomo_subtomo` on
Re-extract → OLD uncurated particles kept while NEW count recorded + badge ✓ (wrong results downstream). Fix: `import shutil`
+ `shutil.rmtree(out_dir/"out", ignore_errors=True)` after marker cleanup; driver skip kept (guards SLURM requeue only).
(2) **[should]** Same-count re-curation read false-fresh — `extraction_state()` stat'd the BASE star not the consumed
`<stem>_filtered.star`; now stats the filtered star when present (project_state.py, INLINED, NO picks_filter import → stays
pandas-free). (3) **[should]** Driver `result.json{error}` was dropped (poller hit FAILURE marker first); `_poll` now reads it
on FAILURE + handler surfaces it. DEFERRED (report-only, needs render-path design): 60-min watcher orphans a queued-then-
successful extraction (badge stuck ○ on contended gpu partition) → reconcile from result.json ON RENDER, not a longer loop;
+5 watch-items (stale `pl.filtered_count` cache, raw `slurm_defaults` vs subtomo profile walltime, `subtomo_jm=None` geometry,
OSError→EXTRACTED). REFUTED (no edit): happy-path count gate (list stars single-tomo → n==kept), input/output optset confusion.
⚠️ STILL PENDING RUNTIME. Detail in docs/roadmaps/completed/roadmap_artiax-bridge.md "S18 PRE-RUNTIME REVIEW DONE". See [[reference_hpc_env]] (no auto-reload).

**S17 (2026-06-12):** P1–P7 USER-CONFIRMED at runtime ("yeah it works"). Then built **SLICE C — per-list extraction
CORE** (code-clean, PENDING RUNTIME). Files: `services/visualization/list_extraction.py` (+`write_extracted_optset`),
`drivers/extract_pick_list.py` (NEW standalone driver), `backend.py` (+`extract_pick_list`), `services/project_state.py`
(extraction_state staleness fix), `ui/tomo_dashboard_dialog.py`, `ui/dashboard/css.py`. Flow: select a workbench list →
detail pane shows an **Extraction bar** (derived ○/✓/⚠ badge + Extract/Re-extract) → click submits a one-off SLURM job
(`backend.extract_pick_list` = exact `submit_tilt_filter_dl` qsub.sh pattern, resources=`state.slurm_defaults`) running
`drivers/extract_pick_list.py`: `build_list_optset` (prefers `<slug>_filtered.star` = KEPT subset) → container-wrapped
`relion_tomo_subtomo` (mirrors `drivers/subtomo_extraction.py`) → `Curation/<sp>/<tomo>/<slug>/out/` + `result.json`. A
BackgroundTask watches the out dir (`RELION_JOB_EXIT_*` + result.json) → `PickList.mark_extracted` (explicit project_path,
W2). Box/bin from `sp["subtomo_jm"]`. **Staleness fix:** `extraction_state()` compares `extracted_count` vs `filtered_count`
(kept) when filtered, else `count` → extracting 4-of-7 ≠ instant-stale. **DEFERRED capstone = downstream FORWARDING**: the
mechanism exists (subtomo `output_optimisation_filtered` slot has `prefer_if_exists=True` → resolver already prefers
`<subtomo_job>/optimisation_set_filtered.star`); remaining = MERGE per-(species,tomo) AUTHORITATIVE extractions into that
species-level file (per-tomo→per-species, high blast radius, NOT wired blind). P7: list-detail ad-hoc font/color → shared
`.cb-detail-meta`; broad audit still co-owned. Detail in docs/roadmaps/completed/roadmap_artiax-bridge.md → "SLICE C — CORE LANDED". See
[[feedback_per_list_extraction]].

**S16-cont (2026-06-12):** P7 styling — the user's NAMED targets landed (broad Particles-panel audit still open); code-clean,
PENDING RUNTIME. Files: `ui/curation_session_dialog.py`, `ui/tomo_dashboard_dialog.py`, `ui/dashboard/css.py`. The **curation
control center** ("Curate in ChimeraX+ArtiaX" dialog) was rebuilt: widened 44→56rem (+90vh) and the command block WRAPS
(`whitespace-pre-wrap break-all`, was `whitespace-pre`) → no more horizontal scrollbar; `_BOX`/`_BLOCK` unified into one
white-inset/mono/bordered code language; the three sections (Connect / Load / Pick & save) are now uniform light `_SECTION`
panels with a shared `_section_head` helper (icon+title+right actions); copy reworded to say crboost AUTO-preloads the tomo +
its picks and AUTO-offers to save on tomo-switch, with the commands/by-hand steps framed as fallback "only if necessary."
Load-into-session **icon `bolt → swap_horiz`** in both the dialog and the rail toolbox. The **'Curate' toolbox button is now
session-aware**: gray `.cb-curate-off` when no ChimeraX session, soft-green `.cb-curate-live` pill when live — liveness polled
into module `_CURATION_SESSION_LIVE` by `_maybe_refresh` (~16s, squeue-derived, in the refresh signature → repaints on
start/stop anywhere). NOTE the `with hdr:` re-entry pattern (build header row, return it, re-enter to append right-side actions)
is used 3× — valid NiceGUI. Detail in docs/roadmaps/completed/roadmap_artiax-bridge.md → "P7".

**S16 (2026-06-11):** P1–P6 of the SESSION 16 polish list LANDED (py_compile + `ruff check`/`ruff format` clean on the two
touched files; PENDING RUNTIME — restart `python main.py`). Files: `ui/tomo_dashboard_dialog.py` + `ui/dashboard/css.py`.
P7 (broad styling audit) DEFERRED to the user (open-ended, unverifiable without runtime). **P1** jitter: trailing-edge refresh
coalescing — auto-kick on_completes + the 4s tick now set `_refresh_req` instead of rebuilding; a 0.2s `refresh_coalesce_timer`
flushes ONE `refresh_all()` once quiet (first paint + tab-select stay immediate). **P2** count out-of-sync: table `filtered_count`
now derived from `<slug>_filtered.star` via new mtime-memoized `_memoized_keep_state` (CONFIRMED on disk: GT `agg_20260311_412_Grid3`
Pos_13 manual had `filtered_count=None` but `manual_filtered.star`=4/7). **P3** cutout re-render: `_read_atlas_index` memoized by
(path,mtime) `_ATLAS_INDEX_MEMO`; spinner skipped on warm revisit (logs `atlas cache HIT`); `_vis_asset_url` is mtime-keyed so the
PNG URL is stable (NOT a cache-bust — the spinner was the perceived re-render). **P4** merged-list vanishing: ON-DISK PROOF it's
ALREADY FIXED — `merged__newmergeee` (14 rows, valid CENTERED_COLS) is persisted in project_params.json + on disk + readable; S15's
persistence already cured it. Landed two GUARDS so it can't silently vanish again: `_collect_pick_lists_for_species` renders a
persisted-but-0-pick list (logs a warning) instead of `continue`-dropping it; `_do_inline_merge` saves with explicit
`project_path=` (was client-context default). **P5** label: manual list now labelled the `.coords` STEM (`coords_source` stem,
backend.py:985), slug stays `manual`. **P6** copy column: new `path` table column = `content_copy` button (reuses `.cb-info-copy`,
`click.stop`) copying the backing star's full path (auto→candidates.star, workbench→pl.path); CSS grid 7→8 cols. Full per-item
detail + RUNTIME GATES in docs/roadmaps/completed/roadmap_artiax-bridge.md → "SESSION 16". See [[feedback_background_task_no_client_context]].

**S15 (2026-06-11):** pick-list panel UI rework (user peeves; code-clean, PENDING RUNTIME — no auto-reload).
Pills → one aligned `.cb-ltable` table (row=merge-check·swatch·name·count·extract-symbol·eye, shared grid → columns
line up; click row→gallery); the 3 curation icons (Curate/Load/Import) pulled OUT into a vertical `.cb-list-toolbox`
side panel. Markers fixed: merged TRIANGLE was a 3px bare fill — clip-path CLIPS box-shadow so its ring/glow never
showed → now 9px + `drop-shadow` outline, 14px orange `drop-shadow` halo on brush; generic brush backlight grows 4→8px
+ a bright `0 0 11px 4px` glow (was near-invisible). Keep/drop filter now propagates: `_source_for` prefers
`<slug>_filtered.star` → merge consumes the KEPT subset (extraction/Slice C already reads the same star). Duplicate
"Open in ArtiaX" removed from the gallery header (`_handle_open_list_in_artiax` kept for W1). Files:
`ui/tomo_dashboard_dialog.py` + `ui/dashboard/css.py`. Broad panel-wide styling audit (Slice B item 8) STILL OPEN.

**S15-cont:** keep/drop now AUTO-COMMITS (no "Save picks" button — it's deleted; serialized `_commit_loop` writes
`<slug>_filtered.star` on every toggle) → selection PERSISTS across navigation + feeds merge (kept-only union: 10-of-20 ∪
3-of-7 = 13). Table shows kept/total via new `PickList.filtered_count` (live-updated cell + persisted). NEW authoritative
selector: `ProjectState.authoritative_pick_lists` (per-(species,tomo)→slug, default "auto") + `get/set_authoritative_slug`
+ a table "auth" radio column (one-on, persists) + header row + the existing subtomo "ext" ○/✓/⚠ column. Downstream
CONSUMPTION of the authoritative choice is still Slice C (resolver not wired to read it; workbench lists have no
`extracted_path` until extracted). ⚠️ `services/project_state.py` has PRE-EXISTING ruff debt (unused-import F401s + format
drift) NOT from this change — left untouched. Detail: docs/roadmaps/completed/roadmap_artiax-bridge.md → "SESSION 15" + "SESSION 15-cont".

**S15-cont2:** merge was STILL unioning the full AUTO set (`_source_for("auto")` returned `candidates.star`) → 10/20 auto
+ 4/7 manual = ~21 not 14. FIX: auto source is now `subtomo_job_dir/particles_filtered.star` (curated subtomo set, has
centered-Å + rlnTomoName → `pick_merge.read_centered_coords` yields the KEPT auto picks) when `picks_filter.has_filtered_set`,
else full candidates. Auto kept/total now in the table via `read_reviewed_counts(subtomo_job_dir)[ts]` → `sp["auto_kept_count"]`
→ auto lst `filtered_count` (refresh-time, not live — auto's filter lives in the subtomo gallery, not the cutout sheet).
"Unrecognizable cutouts after merge" = NOT a location bug: auto gallery shows EXTRACTED-subtomo cutouts, manual/merged show
RECON cutouts (5-vox Z-slab mean of a 2×-diameter box from the raw binned recon — missing wedge, no CTF, loose framing);
same `centered_angst_to_voxel` transform so location is right. Real subtomo cutouts for a list only exist post-extraction
(Slice C). Tuning levers: `_list_cutout_box_px` (box) + `slab_px` in `recon_cutouts.render_recon_cutouts_atlas`.

**S16 QUEUE (user-prioritized 2026-06-11, OVER Slice C; user drives next session) — recorded in docs/roadmaps/completed/roadmap_artiax-bridge.md
"▶▶▶ SESSION 16 — START HERE":** P1 upfront page jitter (initial `_auto_kick_*`→`refresh()` burst, incl. the manual-list
ingest landing); P2 table count out of sync — table shows 7 but sheet preloads 3/7 (table trusts cached `pl.filtered_count`,
sheet DERIVES from `<slug>_filtered.star`; fix = collect filtered_count from the star, not the cache); P3 cutouts re-render
every visit (cache exists — spinner/`_probe` on every open or `is_output_stale` churn; memoize `_read_atlas_index`); P4 HIGH
merged list doesn't survive restart — merge auto-persists (`add_pick_list`+`save_project`) so it's a broken ROUND-TRIP; prime
suspect `_collect_pick_lists_for_species`'s `if not picks: continue` silently drops a persisted merged list whose star is
unreadable on reload; P5 manual list label hardcoded "Manual (ArtiaX)" (~3023) → use the `.coords` filename stem; P6 add a
copyable full-path column per list (reuse `cb-info-copy`); P7 styling pass (Slice B item 8). Core flows confirmed working.

Bidirectional manual-picking workflow at the PyTOM gallery: export our picks for a tomogram
into a bundle that opens in ChimeraX+ArtiaX on the user's Mac, manually pick, import back,
browse/merge/commit a combined manual+auto set that downstream consumes. Full plan in-repo at
`services/visualization/ARTIAX_BRIDGE_PLAN.md` (status 2026-06-03: coord bridge + self-contained
ChimeraX/ArtiaX VNC appliance BUILT — SIF at /groups/klumpe/software/containers/sifs/chimerax_artiax.sif;
**crboost UI one-click launch (Option A) BUILT**). **Authoritative handoff + ordered next steps live in
that plan doc's "Session log & next-session handoff" section.** NEXT: runtime-test the launch path (UI
button OR manual srun → tunnel → eyeball ChimeraX+ArtiaX), then build the `.cxc` generator + import +
`_combined` wiring.

**UI launch landed (2026-06-03, session 2) — Option A only.** User locked **A** (crboost hands over
the `ssh -L` tunnel + viewer + password; NO in-crboost noVNC/proxy — rejected as a redundant proxy chain
on top of browser→login→crboost). SIF path is **config-driven, never hardcoded**: `conf.yaml`
`curation.sif_path` (env `CX_SIF` overrides) → `config_service.curation` (`CurationConfig`). One worker
`containers/chimerax_artiax/curation_session.sh` (writes `session.json`; `_session_inside.sh` deleted).
Backend: `launch_curation_session` / `get_curation_session_info` / `stop_curation_session`. UI: sidebar
button (`RosterWidget._build_curation_btn`) → `PipelineBuilderPanel.launch_curation_session` (SingleFlight)
→ `ui/curation_session_dialog.py` (submit → poll 3 s → 3-step connect card + Stop).

**WORKS END-TO-END (2026-06-03, session 3):** ChimeraX+ArtiaX renders over VNC, interactive. Getting
a working GUI on CBE took a chain of non-obvious container fixes (all in `chimerax_artiax.def`):
x11vnc for `-storepasswd` (no vncpasswd/python in image); writable XDG config (baked /opt/cx is RO);
bundled Qt6 registered via ld.so.conf+ldconfig; Qt6 xcb/X11/GL libs; **and the big one — strip
`.note.ABI-tag` from all `/usr/lib/ucsf-chimerax/*.so` because CBE's el7 kernel 3.10 < Qt6's required
ABI tag 4.11** (see [[reference_cbe_kernel_abi]]). Next = the curation *workflow*, fully designed in
the plan doc's **"## Post-launch curation workflow roadmap"**: fullscreen polish → per-tomo copyable
info panels → `.cxc` generator + one-click "curate this tomo" → ingest `.coords`→ManualPicks star +
registry → `_combined` optimisation_set sibling (resolver tier `_combined > _filtered > original`) →
Tier-3 `crboost` ChimeraX bundle + session manifest for the 40–80-tomo blitz.

**Session model DECIDED (2026-06-03):** ONE live curation session per project, reused — never
one-per-tomogram (a fresh SLURM job per tomo = new node/port/password = re-tunnel every tomo, killing
the blitz). Track the active session in `ProjectState` (mirror `aggregation_merges`); squeue/`session.json`
poll flips it live→ended. Launch button = `ensure-session` (reuse if live → no 2nd sbatch → also dodges
the display-`:1` collision; else launch), idempotent. Tier-2 "curate this tomo" enqueues into the live
session's manifest → **merges Tier-2 + Tier-3** (manifest is the in-session swap mechanism, not a separate
endgame). Sub-decisions (recommended, not locked): swap driven hands-in-ChimeraX via baked `next/prev/save`
bundle over the manifest (REST `remotecontrol` is the gallery-driven escape hatch); scope = per-project.

**Curation = a MULTI-LIST WORKBENCH (DECIDED 2026-06-03; contract = plan doc "## Curation workbench — the
multi-list model").** NOT "auto vs manual + diff/combine-policy" (that framing is superseded). Per (species,
tomo) there are N named `PickList`s — `auto` (PyTOM candidates.star, circle), `filtered` (CC/top-N, dimmed
circle), `manual` (ArtiaX, diamond), `imported` (square), `merged` (triangle) — each a visibility-toggleable
layer (shape+color) on the recon slab canvas with linked cutouts; the eye toggle governs dots AND cutouts
together. Exactly ONE list is authoritative = whatever `picks_filter.resolve_canonical_optset`
(`picks_filter.py:269-277`) returns; merge writes `_combined` sibling + one resolver tier (`_combined >
_filtered > original`) → downstream + cross-project aggregation forward it with ZERO driver changes. KEY
INSIGHT: this is mostly a GENERALIZATION of the existing per-species overlay in `ui/tomo_dashboard_dialog.py`
— `_render_pick_layer()` (~3976, per-set toggleable `.cb-pick-layer`, uniform-circle = shape seam), per-species
checkbox toggle (4391-4406), linked dot↔cutout brushing (5554-5681), `_render_gallery_body()` (4898) — widen
"per species" → "per list". Two NEW pieces only: (1) shape encoding on dots, (2) on-the-fly cutout atlas from
the BINNED RECON (today's atlas needs a subtomo-extraction job, so manual/imported lists have nothing →
reuse `render_pick_cutouts_atlas()` sourced from recon). Registry = `pick_lists` on ProjectState mirroring
`AggregationMerge` (`project_state.py:432-511`); dedup merge via `subtomo_link._coord_key` (`subtomo_link.py:41`).
"Get rid of stats" (z-pct/nn/CC-histogram); CC survives as per-dot hover + filter handle. Build order (6
slices) in the plan doc; user wants to INVEST here (journey subtomo-preview refactor in scope).

**IMPLEMENTATION IN PROGRESS (2026-06-03, user said "rip" through 2→3→4):** Slices 1+2+3-enabler LANDED,
all compile+ruff clean (full status + NEXT steps in plan doc "## Curation workbench → Progress").
S1: `PickListType` (models_base) + `PickList`/`pick_lists`/accessors (project_state) — additive, no schema
bump. S2: per-list overlay in `ui/tomo_dashboard_dialog.py` (`_collect_pick_lists_for_species` returns just
`auto` so far; `_render_pick_layer(...,shape)` + `.cb-shape-*`/`.cb-swatch-*` CSS; canvas toggle+layers
widened per-list, id `cb-pl-{nonce}-{idx}-{slug}-{axis}`, per-list `_lid_xy/_lid_xz`; bridge still targets
`auto`) — backward-compatible, 1 auto list renders identically. S3-enabler:
`services/visualization/recon_cutouts.py::render_recon_cutouts_atlas` — same atlas PNG+index schema as
`preview_render.render_pick_cutouts_atlas` but cropped from the binned recon (central-Z slab), so any list
gets cutouts; NOT yet wired. NEXT = wire recon atlas as gallery fallback (`_render_species_tab_body` ~4682,
watch `_render_gallery_body` subtomo_job_dir curation-save coupling) → per-list contact sheet → ingest
(.coords→PickList+registry via `artiax_bridge`) + "Open in ArtiaX" loads chosen list → filter → merge→
`_combined`+`resolve_canonical_optset` tier. **HARD CONSTRAINT:** Claude's venv has NO numpy/mrcfile/
starfile/pandas → cutouts/star-IO/UI are py_compile+ruff only; user must runtime-test in module-loaded app.

**REPRIORITIZED after first real use (2026-06-03):** two things block ACTUAL use and come BEFORE the
list-panel/contact-sheet/merge slices. (1) **Blank session** — launching the curation session drops the
user into a bare ChimeraX with NO tomo/picks loaded; crboost knows both (it resolves the recon for the
slabs + has the picks) but doesn't pass them. The session dialog's `open /path/to/tomogram.mrc` is a FAKE
placeholder. FIX = generate a `.cxc` (`artiax open tomo <real recon>` + `open <real picks>.coords` +
`lighting simple` baked in) and launch `chimerax that.cxc` so it opens preloaded (Tier-2); for an
already-running session, show the REAL copyable `open` commands in the journey card (Tier-1 guidance). To
have a `.coords`, crboost first exports the tomo's picks (export half of `artiax_bridge` exists). This is
the NEXT build, ahead of list UI. (2) **Perf is unusable** — 3-5s/camera-move; it's software GL, real fix
is a GPU node — see [[reference_chimerax_vnc_perf]] (don't bother with VNC viewer swaps or ChimeraX quality
knobs). User pausing here; resumes next session to build .cxc-preload + GPU/VirtualGL + then the list slices.

**Session 4 (2026-06-03) — `.cxc`-preload MECHANISM landed (blocker #1 half-done); GPU scaffolding already
present.** Built + verified-to-ceiling (py_compile/ruff/bash-n clean; `.cxc` string-gen + worker
CB_CXC-composition exec-verified; NOT ChimeraX-tested). In `artiax_bridge.py`: `build_session_cxc()`
(pure-stdlib `.cxc` text), `session_chimerax_commands()` (single source of the CONFIRMED load backbone
`artiax start`/`artiax open tomo <recon>`/`open <auto.coords>`/`lighting simple` — shared so the `.cxc` and
the future Tier-1 copyable-commands UI never drift), `prepare_curation_bundle()` (export picks→`__auto.coords`
+ write `open_<tomo>.cxc`; hard-errors if recon MRC absent), `bundle` CLI subcmd. UNCERTAIN ArtiaX subcommands
(particle-list px, empty manual list) are emitted as COMMENTS not commands, so a wrong guess can't halt the
script — user confirms exact syntax on first runtime test. Worker `curation_session.sh`: new `CB_CXC` env
appends the `.cxc` (`printf %q`) to `CX_LAUNCH`, composes with BOTH software-GL and the already-built `vglrun`
path; missing file ⇒ warn + blank fallback. `backend.launch_curation_session(project_path, cxc_path=None)`
exports `CB_CXC`. Runtime-test path (no UI yet): `python -m services.visualization.artiax_bridge bundle
--candidates <ce>/candidates.star --tomograms <ce>/tomograms.star --tomo <T> --out-dir <d> --species <s>`
(user's module env) → `CX_SIF=… CB_CXC=<d>/open_<T>.cxc containers/chimerax_artiax/launch_curation_vnc.sh`.
**NOTED:** the worker + `launch_curation_vnc.sh` ALREADY carry the GPU/VirtualGL scaffolding (`CX_VGL` →
`vglrun -d egl chimerax` under `--nv`, `_GL.sif`, partition `g`) — blocker #2's plumbing is partly built;
remaining is building/validating `chimerax_artiax_GL.sif` + the runtime perf check. ALSO landed this session
(backend session-reuse substrate, the ensure-session prerequisites, all py_compile/ruff clean + the liveness
fn unit-tested off-cluster): `backend.launch_curation_session` now persists `<session_dir>/job.json`
(slurm_job_id/session_id/cxc) at submit; `backend.find_active_curation_session(project_path)->dict|None` scans
`.curation_sessions/*/job.json` + ONE `get_user_jobs()` → first live (`RUNNING/PENDING/CONFIGURING/SCHEDULED/
COMPLETING`) `-J cb-curation` job, merged w/ session.json (squeue-derived liveness, NEVER a stored bool — dodges
the stuck-yellow trap); `backend.prepare_curation_bundle(project_path, candidates_star, tomograms_star,
tomo_name, species_label)` = thin `asyncio.to_thread` wrapper over the bridge, writes under
`<project>/.curation_sessions/bundles/<species_slug>/`. NOTE the UI already holds `job_dir`→candidates/tomograms
star, so NO species→job-dir resolver is needed in the backend (earlier plan's `prepare_curation_bundle_for_tomo`
is unnecessary). **UI HALF of blocker #1 also LANDED this session (compile/ruff clean, NOT UI-eyeballed
off-cluster):** `backend.get_backend()` process singleton (set in `__init__`, idiom like `get_config_service`)
so the dashboard function-module can reach the backend. `ui/tomo_dashboard_dialog.py`: `view_in_ar` button in
`_render_species_tab_header` (gated on `row.get("vol_path")` = recon present) → `_handle_curate_in_artiax(sp,
project_path)` (module-level `_curation_flight = SingleFlight()`, key `species_id:tomo`): calls
`backend.prepare_curation_bundle(...)` then branches on `find_active_curation_session` — live →
`open_curation_commands_dialog(commands, info, manual_coords)` (Tier-1 paste-in, no 2nd sbatch); none →
`open_curation_session_dialog(..., cxc_path=…)` (Tier-2 preload). `ui/curation_session_dialog.py`:
`open_curation_session_dialog` gained `cxc_path` + a preload-aware "once it opens" step; new
`open_curation_commands_dialog`. **Ensure-session sidebar button ALSO landed:** `launch_curation_session` now
calls `find_active_curation_session` first → live shows `open_curation_chooser_dialog` (Reconnect / Stop &
relaunch); Reconnect = `open_curation_session_dialog(existing=…)` new `_reconnect()` path (seed job_id/dir, poll
session.json, NO 2nd sbatch); relaunch = stop+fresh. Click-time chooser (keeps squeue OUT of the timer render),
idempotent. **Ingest CONVERTER also landed (return half of the bridge):**
`artiax_bridge.import_coords_to_centered_star(coords, tomograms_star, tomo, out_star, project_root)` = inverse of
export via the SAME TomoFrame (round trip parity-exact; `selftest` already covers the `artiax_to_centered_angst`
leg <1e-6); writes minimal RELION-5 star (`rlnTomoName` + 3 `rlnCenteredCoordinate*Angst`) at
`ManualPicks/<species>/<tomo>.star`; new `import` CLI subcmd → full round trip CLI-testable
(`export …` → `import …` → compare). Converter-only — NOT wired to UI Import button / PickList registry / manual
watch path yet. **STILL NOT done:** ingest UI+registry; bundle mtime staleness. GATE: if auto picks land shifted
in ArtiaX, add a pixel-size cmd to `session_chimerax_commands` (one place) — buttons need no change. Full detail
in plan doc "Progress" + "NEXT".

**Session 5 (2026-06-07) — button WORKS end-to-end but UX is raw; strategic direction set for next session (all
written to plan doc: "## Curation UX overhaul", "## Dispatch commands to ChimeraX from crboost (REST)", "##
Switch the GL container to DEFAULT").** User tested the per-tomo "Curate in ArtiaX" button on a real 412 project:
it correctly resolved recon + picks and showed the live-session paste-in dialog (Tier-1) — proving the resolution
works. Feedback: (a) button is LOST in the tab-header among gray stats + "sizes & 3dmod" expansion — promote to a
primary action with inline session-status dot; (b) the popup is a command-dump that assumes a session is already
live — make it a SESSION-AWARE CONTROL CENTER (status + a "Start session" button when off + framed "①switch to
VNC ②click ChimeraX cmd line ③paste" steps, since the "these go into ChimeraX" context is missing); (c) want a
per-(species,tomo) FILE OVERVIEW + ONE co-located curation dir (`Curation/<species>/<tomo>/` holding auto/manual/
imported `.coords` + `.cxc`) instead of bundle in `.curation_sessions/bundles/` and ManualPicks elsewhere; (d)
gallery-panel UX overhaul DEFERRED by user. **BIGGEST UNLOCK = REST dispatch:** ChimeraX `remotecontrol rest
start port N` lets crboost POST `artiax open tomo …; open ….coords` to the RUNNING ChimeraX → "Load this tomo"
becomes one click, NO paste-in. Reachability is promising (headnode reaches compute-node ports already, same as
VNC; session.json has `node`); caveat = REST may bind 127.0.0.1 so may need `ssh <node> curl localhost:<port>`.
SPIKE THIS FIRST next session. **USER LANDED the GL/VirtualGL container 2026-06-06** (`chimerax_artiax_GL.sif`,
fluid on P100) — make it the DEFAULT sif (conf.yaml partition g + gres + vgl); see [[reference_chimerax_vnc_perf]].
GPU sif's tomo→pick→save loop still un-validated (only `open 4ug0` rendered).

**Session 6 (2026-06-07) — WORKS END TO END on GPU; the bridge is real.** Proven on a live 412 session:
GPU one-click launch (partition g + gres + vgl wired into `CurationConfig` + `backend.launch_curation_session`)
→ `.cxc` PRELOADS the tomogram + our auto picks in ArtiaX (no typing) → user places manual picks → saves a
`.coords` → crboost reads it back in-register (5 real human picks confirmed, well-formed corner-Å). **THE
UNLOCK was a scipy fix, not UX:** ChimeraX 1.9's bundled scipy fails to load on el7 ("ELF load command
address/offset not page-aligned" — a page-align SIBLING of the ABI-tag trap, [[reference_cbe_kernel_abi]]);
since ArtiaX imports scipy lazily, EVERY `artiax` command silently died into the broken Log panel while
`open 4ug0` worked. Fixed by pinning manylinux2014 `scipy==1.11.4` in the def (NOT objcopy), placed after the
ABI strip + a build-time `import scipy.linalg` assertion. **UX consolidated:** the three curation dialogs
(open_curation_session/commands/chooser) collapsed into ONE status-first `open_curation_control_center`
(live → full connect block incl. tunnel+password + load-this-tomo steps; off → Start; Stop≠relaunch +
busy-guard killed the double-submit) — both the gallery button and sidebar route to it. **Container RELOCATED**
to `/groups/klumpe/software/containers/{defs,sifs}` (`chimerax_artiax_GL.{def,sif}`); CPU def retired; conf.yaml
`curation.sif_path` → the /groups GL sif. **EDIT THE DEF AT /groups NOW**, not the repo (worker scripts stay in
repo — they're crboost code). **Remaining peeves (non-blocking):** (a) ChimeraX Log panel still gray —
`libgbm1`/`libasound2`/`libxshmfence1` were NOT enough for QtWebEngine on el7 (likely needs `--no-sandbox`/more
libs/`/dev/shm`); non-essential, errors still go to the session `slurm.log`. (b) VNC fidelity "like 480p video,"
worst on the tomogram — Xvnc is `-depth 24` (correct) so it's CLIENT-side: real viewer (TigerVNC/TurboVNC) at
full colour + max/lossless + 1:1, NOT macOS Screen Sharing; durable fix = TurboVNC server in the container.
(c) ingest UI/registry + auto-watch (ANY `.coords` name — user saved `particles.coords`, not `__manual`) is the
next build, then the multi-list workbench. Plan doc "## SESSION 2026-06-07 — END TO END WORKING" has the detail.

**Session 7 (2026-06-08) — round-trip + workbench landed in the UI; merge model DECIDED.** User chose to build
items 1+2 ("so I can prototype and give feedback"; UI polish = later). Landed (all py_compile+ruff clean, NOT
runtime-tested — Claude venv lacks numpy/starfile/pandas/mrcfile): **CP1 ingest round-trip** —
`backend.import_curation_picks` (+ `_discover_manual_coords`: ANY non-`__auto`/non-`_ref` `.coords` by
extension+recency, species-bundle-dir-first, so `particles.coords` works) → `import_coords_to_centered_star` →
`ManualPicks/<species>/<tomo>.star` + raw archived; per-tab **"Import picks"** button registers/upserts a `manual`
PickList → overlays as diamond dots+toggle (`_collect_pick_lists_for_species` reads the registry via
`_read_pick_list_voxels`). **CP2 per-list recon cutouts** — `_render_registry_list_cutouts` +
`_auto_kick_list_cutouts` (mirrors `_auto_kick_recon_slabs`, cached `.curation_sessions/cutouts/`, star-mtime
dedup key) → CSS sprite contact sheet from `render_recon_cutouts_atlas`, below the auto section regardless of auto
status (split `_render_species_tab_body`→`_render_species_auto_section`). **CP3a per-list "Open in ArtiaX"** —
generalized `prepare_curation_bundle` (`source_star`+`coords_label`; list exports to `<tomo>__<slug>_ref.coords`,
discovery excludes `*_ref.coords`); `_render_list_header` "Open in ArtiaX" per list. **MERGE MODEL DECIDED (user):
per-list extraction, status-flagged, user-triggered — NOT auto (no throwaway re-extraction), NOT tedious.**
Supersedes the A/B fork. Foundation landed (model only): `ListExtractionState` (NOT_EXTRACTED/EXTRACTED/STALE,
`models_base`); `PickList.extracted_path/extracted_count/extracted_at` + `extraction_state()` (DERIVED, never a
stored bool — stale-flag-trap discipline) + `mark_extracted()` (`project_state`). Subtomo extraction scoped PER
LIST (`extracted_path` = the list's optset; co-locate under future `Curation/<species>/<tomo>/<slug>/`).
**MERGE + RADIUS DEDUP LANDED (same session, decomposed per user: merge=union, dedup=SEPARATE user action, never
auto).** `services/visualization/pick_merge.py` (numpy/starfile, compile-only here): `merge_lists_to_star` unions
2+ lists' centered-Å coords in TYPE-PRIORITY order (manual/imported/merged before auto — row order is the ONLY
manual-wins encoding, greedy dedup keeps earlier); `clash_stats_*` (n_clashing/n_removed/n_after at a chosen
radius); `deduplicate_star` greedy keep-first in place. Backend `merge_pick_lists`/`list_clash_stats`/
`deduplicate_pick_list`. UI: **"Merge lists"** button (tab header, ≥2 lists) → `_open_merge_dialog` (checkbox select
→ union → register `merged` PickList, orange triangle, NOT_EXTRACTED); merged contact sheet gets `_render_clash_panel`
— radius input (default Ø/2) + live clash note + **Deduplicate** (terse tooltip, rewrites star→count drops→STALE).
**NEXT = per-list (re)extraction wiring** (Extract action scoped to one list's coords → per-list output →
`mark_extracted` → badge flips EXTRACTED; authoritative list's `extracted_path` = canonical optset, zero driver
changes), THEN badge + co-located `Curation/<species>/<tomo>/<slug>/` lists-dir UI. Filter (CC/top-N) deferred.
See [[feedback_per_list_extraction]]. Pre-existing F401/format drift in models_base.py + project_state.py left
untouched (surgical-changes rule).

**Session 8 (2026-06-08) — GALLERY UX REWORK started; Slice A LANDED (compile+ruff clean, pending runtime
verify).** This is the deferred "dedicated gallery pass" (the gallery was "sooo messy"). Decided with user via
Q&A: **left list-rail + detail** layout, **layout-first** (per-list extraction wiring after). Slice A reworked
the species tab body in `ui/tomo_dashboard_dialog.py` into a `[list rail | detail]` workbench — ONE coherent
mental model replacing the old "auto gallery + bolted-on workbench-cutout sprites + crammed 5-button header."
`_render_species_tab_body` now builds `_render_list_rail` (chips) + `_render_list_detail` (router). Rail = one
CHIP per pick list (auto + manual/imported/merged): swatch · label · count · (non-auto) extraction-state BADGE
— **first UI surface of `PickList.extraction_state()`/`ListExtractionState`** (✓ extracted / ○ not extracted /
⚠ stale) — + a curation toolbar (Curate in ArtiaX · Import picks · Merge lists) MOVED OUT of the now-trimmed
`_render_species_tab_header`. Clicking a chip drives the detail: `auto` → `_render_species_auto_section`
(existing subtomo gallery/scatter, still canvas-cross-linked via `layer_ids`); workbench list →
`_render_single_list_cutouts` — **renamed/carved from the old `_render_registry_list_cutouts` (now deleted)**,
takes ONE list. Selection is sticky per (species, tomo) via module dict `_SELECTED_LIST_SLUG` so a
background-render `refresh()` rebuild doesn't bounce back to auto (key UX: cutouts finishing → refresh → stays
on the selected list). An `auto` chip is always synthesized even at 0 picks so the zero-picks/errored/generating
states stay reachable. CSS `.cb-workbench-split`/`.cb-list-rail`/`.cb-list-chip{.selected}`/`.cb-list-chip-badge`
+ `.cb-badge-{ok,todo,stale}` in `dashboard/css.py`. **Deliberately NOT in Slice A:** (1) per-list **eye**
(visibility still in the canvas "Show picks" row — consolidating onto the chip + retiring that row is the next
micro-step; mind it loses the cross-species at-a-glance toggle); (2) the 599-line `_render_gallery_body`
monolith reused untouched (carve opportunistically). **NEXT (reprioritized 2026-06-08 after Slice A) = Slice B — gallery DECLUTTER + interaction parity + styling
(full 8-item checklist in plan doc "### Slice B"): retire the "Show picks" row → species-tab master-eye +
per-chip eye; prettify the ugly species tabs; move the gray auto-stats + TM line into a tooltip on the pytom
chip + tag chips by type (pytom/manual/imported/merged); delete the duplicate size pills; 3dmod block to the
bottom; consolidate render/imod/re-render buttons into a panel toolbar next to Invert; REUSE the gallery
hover-brushing infra for non-auto lists (carve `_render_gallery_body`); global styling pass (the Curate/Import/
Merge buttons look dated). THEN Slice C: per-list Extract wiring onto the chip**
(subtomo extraction scoped to one list → `mark_extracted` → badge flips → that list's `extracted_path` is the
canonical optset downstream, zero driver changes) — the roadmap's stated functional next-step, now with a home.
This rework is also logged in [[project_journey_rework_dashboard_refactor]] lineage (R1/R2/R3 dashboard polish
preceded it). See [[feedback_per_list_extraction]], [[feedback_journey_layout_preferences]].

**Sessions 9–10 (2026-06-08/09) — Slice B 1–6 + rail subpanel restructure ACCEPTED; 4 issues UNRESOLVED
(user: "really bad", "very laggy").** Slice B items 1–6 (visibility eyes, prettier tabs, stats→tooltip,
deleted size pills, 3dmod→bottom, admin icons→title bar, rail-above-gallery) verified by the user. The
pick-list rail then became a compact subpanel header (single-line pills + far-right icon buttons) — accepted.
UNRESOLVED, recorded at the TOP of the plan doc ("## NEXT SESSION", ⛔ 2026-06-09 banner): **(1) WIDTH** —
`.cb-gallery-grid`/`.cb-list-rail` still ~half the viewport; two flex/cap fixes FAILED (`_SLAB_MAX_VH` 76→60vh;
canvas-col `flex:3`→`flex:0 0 auto`+inline `width`). The slab sits BESIDE the gallery so it eats half BY LAYOUT
— **next session MUST use browser devtools to find the element stuck at 50%; I worked blind and kept guessing
wrong.** **(2) Prescan** still doesn't auto-surface the saved list. Detection logic CONFIRMED correct vs the
real bundle (`…/agg_20260311_412_Grid3/.curation_sessions/bundles/412/` → claims `particles.coords`).
Persistence ROOT CAUSE found + FIXED: `pick_lists: []` was a GC'd fire-and-forget `create_task(save_project())`
— now `_persist_manual_pick_list` is `async` + `await save_project(force=True)`. UNVERIFIED whether
`_auto_kick_coords_ingest` actually FIRES (couldn't run app). **(3) Lag** on list switch = synchronous Lustre
I/O in `_render_single_list_cutouts` — move off-loop. **(4) Feedback** must be the bottom-right BackgroundTask
tray post (`_BackgroundTaskTray`), NOT a bottom-center `ui.notify` (reverted my `show_start_toast=True`). KEEP
the persistence async-await fix; merge/dedup saves (`tomo_dashboard_dialog.py:1993, 2091`) have the SAME GC bug.

**Session 11 (2026-06-09) — all 4 issues CONFIRMED FIXED by the user at runtime ("full width, no freezes";
auto-ingest seen in logs). ⚠️ THE BIG META-LESSON: the server has NO auto-reload (`main.py` `uvicorn.run(app)`,
no `reload=True`; CSS injected from Python) — edits do NOTHING until the user restarts `python main.py` +
hard-reloads. This silently masked ~3 sessions of working fixes as "didn't work" and drove repeated wrong
re-guessing. ALWAYS confirm code is live (fresh log line / changed computed style) before assuming a fix failed.
Now in [[reference_hpc_env]]. (1) WIDTH FIXED —
full static trace proved there is NO inner 50% cap (inside `.cb-particles-tabs-col` it's `w-full`/`width:100%`/
`align-items:stretch` down to `.cb-gallery-grid`); the gallery/rail were "half" only because the slab column's
inline `width: min(1080px, _SLAB_MAX_VH·aspect)` lands at ≈half the row, and tabs-col `flex:1` got the rest. The
two prior fixes never bounded the horizontal split — that's why nothing moved. Fix = new `_SLAB_MAX_PCT = 34`,
slab inline `max-width:100%`→`max-width:34%` (slab ≤34% of row → gallery/rail ≥~64%); slabs stay LEFT;
`_SLAB_MAX_PCT` is the tuning knob. CORRECTION: the slab cap alone did NOTHING visible — the REAL cause (found
via the user's devtools width-chain measurement, NOT the top-down CSS read) was `.cb-workbench-split` having no
width → shrink-wrapping to its content (~half) inside the q-tab-panel; fixed with `width:100%; min-width:0` in
css.py. Both needed (slab cap widens the column, width:100% fills it). (2) Manual-pick auto-ingest REAL ROOT CAUSE found (supersedes "unverified
whether it fires"): `_auto_kick_coords_ingest` is only called from `_collect_species_data_for_ts`, which only
runs on a dashboard REBUILD; the sole auto-rebuild = the 4 s `_maybe_refresh` timer whose signature is built
ENTIRELY from the background-task registry (no knowledge of `.coords`), so an EXTERNAL ArtiaX save never moves it
→ prescan never runs → "needs Import click." GC saves at 1993/2091 fixed (`await save_project(force=True)`);
`logger.info` added at every gate of `_auto_kick_coords_ingest`. Fix-TODO (needs user nod + runtime): make a
fresh save trigger a rebuild — recommended = "Curate in ArtiaX" spawns a background WATCHER task that polls the
bundle dir off-loop, ingests, and (being a registry task) bumps the refresh signature. (3) LAG FIXED —
`_render_single_list_cutouts` now `async`: spinner → `asyncio.to_thread` for all read-only probes → build tiles
(cold-path build-kick stays on-loop); `_render_list_detail`/`_render_detail`/`_select` async; initial render via
`ui.timer(0.05, _render_detail, once=True)`. Highest runtime-verification need. (4) Feedback already correct (no
change): cutout+ingest tasks use `show_start_toast=False` + `project_path`, and the tray is mounted
(workspace_page.py:177, main_ui.py:127) → they surface bottom-right already.**

**Pick-list SCOPE BUG + standardization (2026-06-09, session 11 — user hit it live).** Curation bundles are
per-SPECIES (`.curation_sessions/bundles/<species>/`), NOT per-(species, tomo). A generic ArtiaX save
(`particles.coords`) can't be tomo-attributed, so the Import button (`_discover_manual_coords`, newest-`.coords`-
wins) + the prescan import the SAME save onto whatever tomo is in view — clicking Import on tomo 13-3 imported
tomo 13-1's coords + rendered cutouts. WORSE than a mislabel: `.coords` are physical-Å positions for a SPECIFIC
volume, so cross-tomo = geometric garbage → would extract garbage subtomos. FIX = now **Slice C.0** in the plan
doc, a PREREQUISITE before the extraction launch: one per-(species, tomo) dir `Curation/<species>/<tomo>/{open.cxc,
auto.coords, manual.star, merged.star, <slug>/}`; the "Curate in ArtiaX" launch points ArtiaX's SAVE TARGET at
that dir; import/prescan scan ONLY that dir; delete the recency/single-tomo heuristics (the bleed). OPEN Q (ArtiaX
domain): can the `.cxc`/session force ArtiaX to save into a chosen dir? If not, the live session tracks the active
(species, tomo) and stamps each new save. The step-1 optset-builder (`list_extraction.py`) is unaffected (explicit
list-star + tomo). See [[feedback_per_list_extraction]].

**Session 12 (2026-06-09) — user RE-STEERED away from extraction; Slice C.0 scope fix LANDED in code.** User
pushed back on the session-11 handoff's "run extraction validation first" — extraction is the TERMINAL step, and
validating star formats for lists that are mis-scoped (geometric garbage) + merged through a raw UX is premature.
Slice C (per-list extraction) PARKED. New order: (1) scope fix [done, below], (2) brushing parity (Slice B item 7
— carve `_render_gallery_body` so manual/imported/merged lists get tile↔dot brushing, the keystone for trusting a
merge), (3) merge UX polish (overlap preview before commit; "what drops" preview on dedup; make manual-wins-over-
auto priority visible — today only row-order encodes it), (4) styling pass DEFERRED ("ui polish a little later").
**Slice C.0 scope fix LANDED (compile+ruff+format+bash clean; PENDING runtime verify — server has NO auto-reload,
[[reference_hpc_env]]).** One shared helper `artiax_bridge.curation_dir(project, tomo, *, species_id, species_label)`
→ `Curation/<species_id_slug>/<tomo_slug>/` (species_id-first slug, so all sites agree). Repointed: backend
`prepare_curation_bundle` (out_dir) + bridge filenames (`auto.coords`/`open.cxc`/`<label>_ref.coords`, tomo-prefix
dropped since the dir IS the tomo), `import_curation_picks` (→`manual.star` + `imports/<stamp>.coords`),
`merge_pick_lists` (→`<slug>.star`), `_discover_manual_coords` (now scans ONLY the per-tomo dir — the per-species
newest-wins bleed is GONE), `_pending_save_for_tomo` (single-tomo/name heuristic deleted), `_curation_bundles_sig`
(walks `Curation/*/*/`), worker `curation_session.sh` (`cd`s ChimeraX to `dirname(CB_CXC)` so saves DEFAULT there).
**KEY INSIGHT (resolves the old open-Q): correctness does NOT depend on where ArtiaX saves — it depends on scoping
the SCANS to the per-tomo dir. The worker cd is a best-effort UX nudge; a save steered elsewhere still works via
explicit-path Import.** NO file-migration: old `ManualPicks/`+`.curation_sessions/bundles/` data still loads via
PickList's stored absolute paths, and since the bleed means some old imports may be wrong-tomo garbage, re-curate
rather than launder. One-time cost: an un-imported save in an OLD bundle dir now needs explicit-path Import.
RUNTIME GATE: fresh session → save `.coords` → confirm it lands in `Curation/<sp>/<tomo>/` + auto-ingests, no click.
**Session 12 cont. — curation control center REWRITTEN + VNC lifecycle fixed (clean, PENDING runtime verify).** (a)
`ui/curation_session_dialog.py` rebuilt as a STABLE-layout one-stop-shop (status chip live/starting/off/failed +
Start/Stop swap-in-place + Connect/Load guidance built ONCE, values fill via `set_text`/`set_visibility`, no
`body.clear()` shape-shift; smaller fonts; dark mono command block). **It was closing on every dashboard refresh** —
root cause is framework-level (NiceGUI 3.0.3 `events.handle_event` runs an async click handler inside the SENDER's
`parent_slot`, so the dialog parented under `main_area`, which the 4 s live-refresh `clear()`s; my scope-fix's
curation-mtime-in-signature made it fire on every Curate click). Fix = parent at `context.client.layout.default_slot`
(never cleared) + `persistent` + per-client replace-on-reopen. Same latent bug in `_open_merge_dialog`/import/dedup
dialogs — see [[feedback_reactive_ui_patterns]]. (b) **VNC session lifecycle** (`backend.py` + `curation_session.sh`):
RealVNC "connection closed unexpectedly" was THREE compounding bugs — port was ALWAYS 5901 (`DISPLAY_NUM=${CX_DISPLAY:-1}`
never set), no zombie cleanup (Start spawns a job, find_active reconnects to first live, Stop killed only that one → 7
tries = 7 live-but-wedged zombies), and a silent-failure tunnel (`ssh -L 5901` with a stale local forward binds nothing
but still connects → dead forward). Fixes: backend sets `CX_DISPLAY=2+(hash%88)` (unique-ish port 5902–5989), worker
free-scans `/tmp/.X<n>-lock` for an actually-free display + writes the real port to session.json; launch scancels all
live project cb-curation jobs + `stop_curation_session(job, project_path=)` sweeps the whole pile in one Stop
(`_live_project_curation_job_ids`); tunnel → `ssh -f -N -o ExitOnForwardFailure=yes -L …` (backgrounds + frees the
terminal + fails LOUD on local-port collision). EXISTING mess needs a one-time manual clear: `scancel -u $USER -n
cb-curation` + `lsof -ti tcp:5901 | xargs kill` (Mac). Modal dialog → next-tomo needs a Close first (non-modal dockable
panel = possible future ask).

**Session 13 (2026-06-10) — REST command-channel SPIKE is GREEN (runtime-verified on CBE); per-user reuse chosen.**
The biggest-unlock-since-S5 REST dispatch path is PROVEN. Worker `curation_session.sh` now auto-starts ChimeraX's
built-in REST server via `--cmd "remotecontrol rest start port $REST_PORT json true"` (REST_PORT = `CX_REST_PORT`
override else `46000+DISPLAY_NUM`); `session.json` + the banner gained `rest_port`. **VERIFIED at runtime:** from the
CBE headnode (clip-login-1), `ssh clip-g3-0 "curl -s 'http://127.0.0.1:46061/run?command=set+bgColor+red'"` returned
`{… "error": null}` — i.e. passwordless headnode→compute-node ssh WORKS, the REST server is up, and ChimeraX ran the
command. **REST binds 127.0.0.1 only** (no auth) so a DIRECT headnode→node:port GET is refused — the **ssh-hop curl is
the required transport** (`ssh <node> curl localhost:<rest_port>/run`). To confirm without VNC, use a state query:
`…/run?command=info+models`. **CORRECTION to the S5/handoff assumption:** the backend does NOT ssh anywhere today —
`sbatch`/`scancel` run LOCALLY on the headnode (`slurm_service._run_command`, no ssh wrapper); the REST dispatcher will
be the FIRST backend ssh-to-node. **Decision: per-user shared session** (was per-project) — one long-lived ChimeraX
per USER, reused across tomos/species/projects; the session is just a viewer, every file sent is an absolute path on
the shared FS, so cross-project = just different paths, no relaunch/re-tunnel. **Verified ArtiaX command facts** (web +
source, [[project_artiax_bridge]] workflow): NO `artiax close` → use ChimeraX `close session` (then re-`artiax start`,
since close session resets camera/lighting and may drop ArtiaX); NO `artiax open particles` → bare `open <f.coords|.star>`
is suffix-routed into ArtiaX once started; NO `artiax save` → `save <f> [format relion] partlist #N`; RELION-5 writer
still buggy (#40) → keep `.coords`. **VNC port-churn robustness:** the daily "connection refused" pain is a SYMPTOM of
one-SLURM-job-per-tomo (every launch = new port → stale viewer bookmarks → re-tunnel every time). Shipped a
self-cleaning tunnel (worker `TUNNEL_CMD` now prefixes `kill $(lsof -ti tcp:<port>) 2>/dev/null;` — `\$( )` escaped so
it runs on the user's Mac, not the node) so a stale local forward can't trip `ExitOnForwardFailure`. The REAL fix =
the per-user persistent session (tunnel once, swap via REST, port never churns); a fixed LOCAL viewer port (bookmark
`localhost:5901` forever, remote varies) is deferred into that build because it touches worker+backend+dialog together
+ needs a `python main.py` restart. **NEXT BUILD (the agreed reuse feature):** generic per-user launch (omit `CB_CXC` →
empty-but-ready session, `--cmd` does REST + `artiax start`); `backend.send_chimerax_command(session, cmd)` over ssh+curl
+ `backend.load_into_session(project, tomo, species, save_first=)` building `close session ; artiax start ; artiax open
tomo <recon> ; open <picks>` (reuse `artiax_bridge.session_chimerax_commands`); `find_active_curation_session` widened
to per-USER find-or-reuse (stops spawning duplicate jobs); dialog → non-modal dockable + "Currently loaded: <sp/tomo>"
indicator + a "Load into running session" button (SingleFlight + confirm-with-save-first, since `close session` is an
irreversible wipe of unsaved manual picks); paste-in stays as the graceful fallback when a POST fails. Gate behind a
`rest_enabled` config flag until the full path is runtime-verified. **Transport VERIFIED end-to-end (2026-06-10,
user-confirmed at runtime):** over the ssh-hop, `info models` returns full ArtiaX state, `set bgColor` is visibly
applied, `artiax open tomo <path>` loads a 2nd tomogram, and the full `close session ; artiax start ; artiax open
tomo … ; open <coords>` swap runs as ONE semicolon-joined command. **GOTCHA the dispatcher MUST honor:** send the
command as a GET QUERY — `ssh <node> curl -s -G --data-urlencode 'command=…' http://127.0.0.1:<rest_port>/run`. A
plain urlencoded POST body (`--data-urlencode` WITHOUT `-G`) returns `"command" parameter missing` because `/run`
reads `command` from the query string, not the body (multipart `-F` also works; urlencoded POST does NOT). Live model
tree: ArtiaX `#1`, tomogram `#1.1.1`, particle lists `#1.2.1`/`#1.2.2` → usable for targeted `close #1.1.1` /
`save <path> partlist #1.2.2`. `rest_port` derives from `46000+DISPLAY_NUM` (e.g. node clip-g3-1 → rest 46019).

**Session 13 cont — per-user REUSE feature IMPLEMENTED + adversarially reviewed (code-clean, PENDING runtime verify;
user said "rip until 45% context").** Backend (`backend.py`, all additive): `send_chimerax_command(session_info, cmd)`
= ssh-hop `curl -s -G --data-urlencode 'command=…'` (BatchMode/StrictHostKeyChecking=accept-new/ConnectTimeout/
ServerAlive; reaps the ssh child on timeout), parses json-true `error`+`log messages.error`; `swap_chimerax_commands`
(artiax_bridge) = `close session ; artiax start ; artiax open tomo <recon> ; open <coords> ; lighting simple` joined
with " ; "; `load_into_session(...)` re-preps the bundle + sends the swap + tracks `_curation_loaded[job_id]`;
`save_session_particle_lists` (best-effort save of open ParticleLists before a destructive swap, parses `info models`
→ json_values[0] double-decode, skips `auto.coords`/`*_ref.coords`); per-user registry
`~/.crboost/curation/registry.jsonl` + `find_active_curation_session_any()` (cross-project find-or-reuse). UI: control
center dialog gained a "Load into running session" button (confirm + save-first checkbox) + cross-project reconnect
(`find_active_curation_session_any` fallback) + `rest_port` in `sv`; dashboard rail gained a per-tile ⚡ button
(`_handle_load_into_session`) — the keystone reuse UX (swap any tomo's tile into the live viewer, no dialog).
`CurationConfig.rest_enabled` (default on) gates it. **Worker kept REST-ONLY** (`remotecontrol rest start …`, NO
`ui tool show ArtiaX ; artiax start` in --cmd) to protect the verified preload path — the swap self-arms ArtiaX.
**5 adversarial-review findings FIXED before declaring done** (workflow: 4 dim reviewers → verify): (1) HIGH —
transient squeue `[]` (get_user_jobs returns [] on error, not raise, force_refresh uncached) was wiping the registry
via the prune; now prune gated on `jobs` non-empty (an empty result ≠ confirmed-dead). (2) registry lost-update race
(read snapshot → await squeue → rewrite-from-stale drops a concurrent append); now append + prune both under
`_curation_registry_lock`, prune RE-READS fresh and removes only confirmed-dead session_dirs. (3) `find_…_any` picked
the OLDEST live entry (append-order, first-wins) → cross-project wrong-viewer `close session`; now picks NEWEST
(last-wins). (4) ssh child orphaned on `wait_for` timeout → `proc.kill()` + ServerAlive bounds post-connect hang.
(5) overlapping different-tomo loads into one session raced on `_curation_loaded` + the single REST endpoint → per-
session `_curation_swap_locks`. **NOT enforced (accepted):** one-live-session user-wide at LAUNCH (housekeeping stays
per-project) — find-or-reuse + newest-first make the acute bug unreachable in normal use. **TRANSPORT GOTCHA (re-confirmed,
must keep):** GET query only (`-G`); urlencoded POST body → `"command" parameter missing`. **Deferred:** dialog
"Currently loaded: <sp/tomo>" indicator (backend tracks `_curation_loaded`, UI unwired); fixed LOCAL viewer port.
**RUNTIME FIX #1 (2026-06-10, user hit it on every tomo, code-clean, PENDING re-verify):** the ⚡ button returned
`Load failed: ssh/curl to clip-g3-1 failed: command-line line 0: unsupported option "accept-new"`. The CBE HEADNODE
runs OpenSSH **7.4** (el7/CentOS 7), which predates `StrictHostKeyChecking=accept-new` (added in OpenSSH 7.6). Fixed
`send_chimerax_command` ssh opts → `StrictHostKeyChecking=no` (el7-safe auto-accept; the manual `ssh <node> curl…`
test worked only because interactive ssh prompts/auto-adds, which `BatchMode=yes` forbids). **el7 ssh-version gotcha
= sibling of the [[reference_cbe_kernel_abi]] ABI/page-align traps** — any new ssh option must exist in OpenSSH 7.4.
Lesson: the adversarial code-review verified LOGIC but could not know the headnode's ssh version — env/runtime bugs
still need the user's restart-and-test. NEXT SESSION starts here: user restarts `python main.py`, retries ⚡ → expect
the swap to work; then verify cross-project reuse + save-on-swap.

**Session 14 (2026-06-10) — W2 (auto-import display) ROOT-CAUSED + FIXED in code (compile/ruff/format clean,
PENDING runtime verify; server has NO auto-reload → user must restart `python main.py`, [[reference_hpc_env]]).**
The S13-handoff prime suspect ("builder's captured `project_state` ref ≠ background's mutation, replaced-singleton,
same as 2026-05-13 stuck-yellow") was WRONG. REAL root cause: **a `BackgroundTask` work coroutine has NO NiceGUI
client/tab context** (`background_tasks.py` runs it via bare `asyncio.create_task`), and `get_project_state()`
resolves the project *from the current tab's `app.storage.tab`* (`project_state.py:926`) — with no client it
hits the `except RuntimeError` and **returns a blank throwaway `ProjectState()`**. So in `_persist_manual_pick_list`,
the prescan/auto-ingest path's bare `get_project_state().add_pick_list(...)` mutated a discarded object and the
path-less `await save_project(force=True)` no-opped (blank state's `project_path is None` → early `return` at
`project_state.py:1042`). The `manual.star` file still landed (it's written by `backend.import_curation_picks` from
explicit paths), but the registry + `project_params.json` never got the entry → chip never appeared. The explicit
**Import button** worked only because it runs in client context (real singleton). NOT a coord/voxel-read bug, NOT keys.
**FIX** (`ui/tomo_dashboard_dialog.py`): `_persist_manual_pick_list` now takes `project_path` and resolves the REAL
registry context-independently via `get_state_service().state_for(project_path).add_pick_list(...)` +
`save_project(project_path=project_path, force=True)`; `project_path` threaded through `_register_manual_pick_list`
and all 4 callers (2 click, 1 prescan-bg). Builder's captured `state` (dashboard line 257, resolved in client
context) IS the same registry singleton, so the bg add is now visible → `refresh()` rebuild shows the chip.
**SIBLING bug found + fixed** (`ui/tilt_filter_panel.py` thumbnail `_run`): identical anti-pattern (bare
`get_project_state()` + path-less `save_project()` INSIDE a BackgroundTask) → `tilt_filter_png_dir` never persisted
→ panel forgot its thumbnails on reload; fixed the same way (resolve by `project_path`). **AUDITED clean:** the
dashboard preview/IMOD bg tasks (`_auto_kick_preview_generation`/`_handle_generate_for_instance` etc.) are SAFE —
they capture `state = get_project_state()` BEFORE the coroutine (client context) and pass it as an arg, and never
call a path-less `save_project()` inside the detached work. General rule now in [[feedback_background_task_no_client_context]].
RUNTIME GATE: restart server → save a `.coords` in ArtiaX → the `manual` chip should auto-surface within ~4 s (no
Import click) AND `pick_lists` should now contain the `manual` entry in `project_params.json`. NEXT (unchanged):
W4 = generalize `picks_filter` to any list star+slug — but it has ONE open fork the user hasn't answered (manual
lists are scoreless: keep/discard tiles vs spatial cull vs pass-through). Get that answer before building W4.

**Session 14-rt (2026-06-11) — W2 USER-CONFIRMED WORKING at runtime** ("the auto-pick up of manually saved things
seems to work now"). The background-context fix landed for real. **New issue + FIX (jitter, compile/ruff/format
clean, PENDING runtime):** user reported "the whole page is jittery, rebuilds every ~10 s." Root cause = the 4 s
`_maybe_refresh` signature included each running task's `(id, progress_current, progress_total)`, so every PROGRESS
tick of a long preview/cutout generation job moved the signature → `refresh_all()` → full `main_area.clear()`
rebuild. Fix = signature now observes task MEMBERSHIP only (`tuple(sorted(t.id for t in active))`) + finished-set +
curation mtimes, NOT progress (progress is a tray concern). Start/finish still trigger one rebuild each; per-tick
progress doesn't. Added `logger.info("journey live-refresh rebuild (changed: tasks|tasks-done|curation-save)")` so
any lingering loop is diagnosable from the log. Lesson in [[feedback_reactive_ui_patterns]]. **W4 fork — user is
leaning (a) manual keep/discard tiles** ("reuse the dot↔tile sync; manual lists just won't have a score filter;
own tab; why complicate this"). RECOMMENDED framing to lock: filtering = "select a subset of picks to keep";
score-threshold is just the AUTO-only FAST path for that selection, manual keep/discard is the same selection by
hand — so (a) is the base case, not a special mode; output = `<slug>_filtered.star` (kept rows) uniformly; per-list
extraction consumes the filtered variant if present. **DEPENDENCY to flag: (a) leans on dot↔tile brushing for
non-auto lists, which is Slice B7 (carve `_render_gallery_body`) — still PENDING — so brushing parity is effectively
a W4(a) prerequisite, not free.** Awaiting user's lock before building.

**Session 14-cont (2026-06-11) — user LOCKED (a) ("let's go ahead"); W4(a) interactive-filter slice LANDED**
(compile/ruff/format clean, PENDING runtime — restart `python main.py`). **Decision on the carve:** did NOT carve
the 599-line `_render_gallery_body` auto monolith (too risky blind, no runtime) — instead made the workbench-list
sheet interactive on its own + reused the SMALL proven `_sync_dropped_dots` JS pattern (≈8 lines) for the dot-grey
sync, NOT the 150-line hover-highlight bridge. Auto gallery UNTOUCHED (zero regression risk). **Backend
(`services/visualization/picks_filter.py`, additive):** `save_filtered_list(source_star, dropped_indices)` —
**DROP-based** (keeps every row EXCEPT dropped, incl. tile-less/out-of-bounds picks the curator never saw → a save
never silently loses a placed pick; this was a self-caught bug in the first keep-based draft), `derive_keep_state_for_list`
(centered-Å match against `<stem>_filtered.star`), `discard_filtered_list`, `filtered_list_path`
(`manual.star`→`manual_filtered.star`), `_list_particle_block`. **UI (`_render_list_cutout_sheet` rewritten
interactive):** click a tile → keep/drop; dropped tile greys + greys its slab ghost-dot via the list's own
`_lid_xy/_lid_xz` layers; "Save picks"→`<slug>_filtered.star` (off-loop), "Reset"→delete filter; kept-counter +
dirty Save gating; existing filter loads on open (derived in `_render_single_list_cutouts._probe`, off-loop). KEY
invariant verified: pick index == star row order == atlas index key == dot `data-pick-idx` (so keep_set indices map
straight to `df.iloc`). run_javascript wrapped (slot-deletion gotcha [[feedback_reactive_ui_patterns]]).
**DEFERRED (noted to user):** full hover-HIGHLIGHT brushing (tile↔dot hover, the big bridge) — only the keep/drop
dot-grey sync landed; lasso box-select for lists; carving the auto bridge into a shared one (dedup cleanup);
downstream extraction consuming `<slug>_filtered.star` (Slice C, parked). RUNTIME GATE: open a manual/merged list
tab → click tiles to drop → dots grey on slab → Save → `<slug>_filtered.star` written → reopen shows drops persisted.

**Session 14-cont2 (2026-06-11) — HOVER-BRUSHING for workbench lists LANDED** (compile/ruff/format clean, PENDING
runtime; user: "Proceed with brushing and then we'll tackle merging and dedups"). Added `_install_hover_bridge()` in
`_render_list_cutout_sheet`: hover a tile → glow its slab ghost-dot(s) (`cb-ghost-active`, in the list's `--sp-color`);
hover a ghost-dot → glow it + `cb-tile-highlight` its tile (+ scrollIntoView if off-screen). Mirrors the AUTO bridge
pattern — delegate `mouseover/mouseout` on the Particles `.cb-section-card` (stable ancestor of canvas + sheet),
re-resolve grid lazily via `getGrid()`, `clearActive()` each mouseover (tile→tile doesn't stick), tear down only on
full exit via `relatedTarget` — but IMPROVES on it: the grid gets a fresh per-render `cb-list-grid-<uuid>` id and the
handler pair is stashed on `root._cbListBridge` + the prior pair removed before re-add, so re-renders DON'T stack
duplicate listeners (the auto bridge's latent leak). Wrapped in `setTimeout(…,60)` so the freshly-built grid exists;
no-op when the list has no canvas layers. NO ghost-click-to-toggle (auto has it via `cbpicktoggle`) — brushing only,
tiles still toggle via their Python `.on("click")`. **NEXT = W3 (merge + dedup UI)**: named merges (today one `merged`
slug overwrites), co-located lists panel (vs separate dialog), merge provenance on the chip — `_open_merge_dialog` +
`backend.merge_pick_lists`/`list_clash_stats`/`deduplicate_pick_list` already exist ("minimal trigger"). STILL deferred
after W3: lasso box-select for lists; the shared-bridge dedup carve; per-list extraction consuming `<slug>_filtered.star`
(Slice C).

**Session 14-cont3 (2026-06-11) — W3 NAMED MERGES LANDED** (user picked "#1 named merges in the existing dialog";
compile/ruff/format clean, PENDING runtime). Was mostly UI — backend `merge_pick_lists(..., out_slug="merged")`
ALREADY took a slug + wrote `<slug>.star`, and `PickList.slug` is unique-per-(species,tomo) with `add_pick_list`
upserting by slug. Changes (all `ui/tomo_dashboard_dialog.py`): `_open_merge_dialog` gained a **"Merge name"** input
(default `merge N+1` from the existing MERGED count); `_do_merge` derives `slug = f"merged__{_fs_slug(name)}"` →
passes `out_slug=slug` + registers `PickList(slug=slug, label=name)` → **re-using a name replaces that merge (upsert),
a new name creates a distinct merged list** (own slug→own star/chip/curation; no more single-`merged` clobber); sets
`_SELECTED_LIST_SLUG` so the user lands on the new merge. PROVENANCE: `_collect_pick_lists_for_species` now carries
`parent_slugs` into the render dict; `_render_list_header` shows a muted `⋃ <parents>` line for merged lists. Verified
distinct names → distinct slugs → distinct `_safe_slug` filenames (no disk collision); old single-`merged` lists still
load (no migration). The rail already renders one chip per slug + routes detail by `slug=="auto"` vs list-sheet + gates
the clash/dedup panel on `list_type==MERGED`, so named merges get full keep/discard + brushing + dedup for free.
**W3 remaining:** co-located lists panel (vs separate dialog) — the only unbuilt W3 gap; provenance + named merges done.
RUNTIME GATE: merge → name it → new orange-triangle chip appears + selected; merge again under a new name → second
chip coexists; re-use a name → that merge updates in place.

**Session 14-cont4 (2026-06-11) — W3 (b) CO-LOCATED MERGE PANEL LANDED → W3 COMPLETE** (user: "Colocated panel.
Let's go"; compile/ruff/format clean, PENDING runtime). Removed `_open_merge_dialog` (the popup) entirely; merging is
now INLINE in `_render_list_rail`: each pill gained a small **merge-checkbox** (13px box, `click.stop` so it doesn't
also view-select or toggle the eye — three click zones per chip now: body=select, eye=visibility, box=merge-pick), and
an **inline merge bar** (name input + "Merge N lists" + clear ✕) sits as a full-width SIBLING BELOW the rail row,
hidden until 2+ boxes are ticked. Selection persists in module `_MERGE_SELECT[(species,tomo)]` (survives the refresh
rebuild like `_SELECTED_LIST_SLUG`); merge reuses the named-merge slug logic (`merged__<name>`), clears the selection,
lands on the new merge. Layout gotcha handled: `.cb-list-rail` is `flex-direction:row; flex-wrap:nowrap`, so the bar
had to be a sibling of the rail (both children of the full-width `.cb-list-rail-host` block) to stack below rather
than squeeze into the row. NO css.py edits (inline styles + manual `display` toggle via `_update_merge_bar`, dodging
`set_visibility`/display conflicts). Dedup stays per-merge in the detail panel (`_render_clash_panel` on `lst["path"]`).
**W3 done (named merges + provenance + co-located panel).** NEXT options (user's call): per-list EXTRACTION wiring
(Slice C — the actual path downstream, consumes `<slug>_filtered.star`); or polish (lasso box-select, shared-bridge
carve, styling). RUNTIME GATE: tick 2+ pills → merge bar appears below the rail → name + Merge → new merge chip,
selection cleared; <2 ticked → bar hidden.

**Locked decisions (2026-06-02):**
- **Positions only** — manual picks carry no orientation (refinement derives angles).
- **Coords-only exchange (`.coords`)** — we do NOT use ArtiaX's RELION-5 writer (open shift bug
  FrangakisLab/ArtiaX#40; reporter confirms `.em`/`.coords` save fine, RELION-5 doesn't). We own
  the centered-Å ↔ voxel conversion both ways; same `N/2` cancels the relion#1280 half-voxel
  parity in the round trip. Display flip/landing is the only thing needing Phase-0 calibration.
- **Data path: rsync/scp bundles primary.** Cluster = CLIP/CBE @ VBC (`clip-login-1.cbe.vbc.ac.at`).
  Project+recons on `/users` (NFS `clip-cbe.imp.ac.at:/clip_homes`). `/groups`,`/scratch` on
  `storage.vbc.ac.at:/ifs/...` = Isilon (can do SMB). Direct NFS-to-Mac = no. SMB only *maybe*
  for the Isilon `/groups` (needs IT enabling SMB + VPN + account; would mean relocating curation
  data off `/users`). One email to IT decides it.
- **Plan-doc-first** — user wants to approve the doc before any implementation.
- ArtiaX = display+placement surface only; NO browser volume picker ([[feedback_no_browser_volume_rendering]]).

**Progress (2026-06-02):**
- **Coord bridge built + VALIDATED.** `.coords` format decoded from a real ArtiaX 0.6 save: plain
  `X Y Z`, physical Å from the volume **corner** (= `voxel × pixel_size`), no header, positions only;
  ChimeraX reads pixel size from the MRC header (6.2 OK). So `centered_Å = artiax_Å − (N/2)×px`.
  Exported our PyTOM picks → opened in ArtiaX → **they land on the density correctly, NO flip/axis
  swap needed.** Shipped: `services/visualization/coords.py` (canonical centered-Å↔voxel math;
  gallery `imod_vis.py` deduped onto it) + `services/visualization/artiax_bridge.py` (export/import
  + CLI; ruff + round-trip verified). ArtiaX cmds: `open f.coords` loads a particle list,
  `artiax open tomo <path>`, UI mouse-mode "mark point" places markers.
- **Data-path PIVOT → run ChimeraX REMOTELY on the cluster.** Local-mount + per-tomo download is a
  non-starter (datasets 100s of GB; you only add 1–2 picks/tomo; sshfs *writes* fail + macOS `._`
  AppleDouble junk). Running ChimeraX on the cluster reads tomos in place AND saves picks straight
  to the cluster FS — kills both problems; mount becomes optional. Mechanism = **TurboVNC +
  VirtualGL** (both installed: `/software/2020/software/{turbovnc,virtualgl}`, + `mesa`), tunneled
  over SSH — NOT `ssh -X` (OpenGL-over-X11 is the laggy case the user hates). No Open OnDemand at
  CBE. ChimeraX = self-install Linux tarball in `$HOME` (not a module). ArtiaX picking is
  slice-based (light GL) → CPU node + Mesa software GL likely fine; GPU optional. This **moots** the
  SMB/rsync data-access question above.

**How to apply:** Build against the phased plan (Phase 0 calibration de-risks coords before any
UI). Reuse existing seams: factor coords math out of `imod_vis.py:46-112` into one helper;
materialize a `_combined` optimisation_set sibling preferred over `_filtered` via the same
`prefer_if_exists` tiering subtomo_extraction already uses → zero driver changes downstream.
Related: [[project_pixel_arithmetic_pain_point]], [[project_412_tomohand_discrepancy]],
[[project_picks_filter_aggregation]], [[feedback_reactive_ui_patterns]].

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

Export/import picks to ChimeraX+ArtiaX; positions-only + .coords exchange; coord bridge + VNC appliance + crboost one-click launch WORK (renders over VNC); S7 landed ingest round-trip + per-list cutouts + "Open in ArtiaX" + merge/dedup; S8 Slice A + S9 Slice B + S10 rail→compact subpanel landed.

S11 (2026-06-09) FIXED all 4 S10 issues, USER-CONFIRMED at runtime: WIDTH real cause = .cb-workbench-split shrink-wrapped to its content (~half) inside the q-tab-panel — NOT the slab — fixed with width:100% (+ slab cap _SLAB_MAX_PCT=34); LAG fixed (_render_single_list_cutouts async + asyncio.to_thread probes); auto-ingest fixed (newest .coords mtime per bundle folded off-loop into the 4s _maybe_refresh signature → external save triggers rebuild → prescan ingests, no click); feedback already correct (tray mounted, show_start_toast=False). META-LESSON: server has NO auto-reload — edits did nothing until user restarted python main.py, masking ~3 sessions of working fixes (see reference_hpc_env).

S12 (2026-06-09): user RE-STEERED off extraction (terminal step) → Slice C parked; scope-bleed fix LANDED = one per-(species,tomo) Curation/<species_id>/<tomo>/ dir via artiax_bridge.curation_dir(), all scans scoped to it (bleed dies by scoping, NOT by ArtiaX save behavior; worker cd best-effort; NO migration).

S13 (2026-06-10): per-user REUSABLE REST session — "Load into running session" swaps ONE ChimeraX/ArtiaX viewer across tomos/species/PROJECTS via ssh-hop curl -G to the node's loopback REST server; el7-ssh fix = StrictHostKeyChecking=no, NOT accept-new (CBE headnode is OpenSSH 7.4). S13-rt (user tested — WORKS): intermittent error toast fixed: send_chimerax_command fails ONLY on a ChimeraX UserError — other exception types are benign noise. ROOT CAUSE was ArtiaX/ChimeraX command-history save() raising AttributeError "'ParticleList' object has no attribute 'string'" on EVERY command once a particle list is open (upstream bug; command still runs); lands in REST top-level error as {type,message}, so neither log messages.error NOR top-level error alone is a reliable failure signal — discriminate by exception TYPE. Also: parent-slot-deleted crash (~20s load outlives main_area.clear() → _handle_load_into_session captures layout slot + _notify helper); save-location (swap appends ChimeraX cd <curation_dir> so ArtiaX Save follows the loaded tomo). SAVE-UX REBUILT (user-approved): backend.save_curation_picks → green "Save picks now" button files open ArtiaX lists into Curation/<sp>/<tomo>/ (prescan auto-imports) + "Prefer to save by hand?" expansion stating exact contract (folder / format=.coords NOT star / name≠auto.coords|*_ref.coords / newest wins). 20s swap = 1 GB float32 recon — NOT fixing (user: downsampling tomos is folly). ONE GATE: save_session_particle_lists (info models json-values parse) runtime-UNVERIFIED → "Save picks now" no-ops if ChimeraX doesn't surface model tree over REST; raw-response diag log added — if "nothing open" though picks exist, grab that line.

W2 auto-import-on-save ROOT-CAUSED + FIXED S14: captured-ref/replaced-singleton hypothesis was WRONG — real cause = BackgroundTask work coroutine has NO NiceGUI client context, so bare get_project_state() returns blank throwaway → add_pick_list + path-less save_project() silently no-op (manual.star still lands via explicit-path backend → star-on-disk but no registry entry → chip never appeared). Fix = _persist_manual_pick_list(...,project_path) resolves via state_for(project_path) + save_project(project_path=,force=True), threaded through all 4 callers; identical sibling fixed in tilt_filter_panel.py thumbnail task; see feedback_background_task_no_client_context.

S14 (2026-06-11) DONE: W2 FIXED + USER-CONFIRMED; page-jitter FIXED (4s refresh signature dropped per-tick task progress); W4(a) interactive keep/discard filter for manual/merged lists (picks_filter.save_filtered_list DROP-based → <slug>_filtered.star); hover-brushing; W3 COMPLETE = named merges (merged__<name>) + provenance + co-located inline merge panel (popup _open_merge_dialog REMOVED). W-order was W2→W4→W3→W1; W1 manual-list edit loop still partial. Status banner at TOP of services/visualization/ARTIAX_BRIDGE_PLAN.md. NEXT = per-list EXTRACTION (Slice C) — consume <slug>_filtered.star downstream via mark_extracted/extracted_path resolver.
