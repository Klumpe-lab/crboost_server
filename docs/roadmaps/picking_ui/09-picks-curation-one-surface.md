# 09 — Picks & curation: one surface, one entry point per action

**Status:** CODE-COMPLETE 2026-08-22, all stages S1–S5 (scoped 2026-08-21 from
`q_important_ui_fixes.md:19-36`). Static gates: `ruff check .` green; `check_boundaries.py` +
`import main` owed to the maintainer (no runnable python in the agent sandbox). Runtime
checklist §4 PENDING.
**Risk:** medium (tab merge + deletions across four files; no service-layer change except one
provenance field). **Depends on:** nothing to build; the launch action's *semantics* follow
picking_ui/10's decision, and the row deep-link lands with picking_ui/11.

**Peeves (maintainer, 2026-08-21):** *"the pages we made here look nice, but they are frankly
somewhat useless… too many ways to open an artiax session… a thousand places where one can 'import a
.coords file'… some of these functionalities are duplicated across both 'picks' [and 'curation'],
and a bunch is already half-duplicated in the journey. Let's unite these into a single page that
lets us actually CURATE the PICKS."*

**Organizing principle (maintainer's, verbatim intent):** separate (1) *comprehensively displaying
picks information* — this roadmap's surface — from (2) *the tools for filtering picks* — the pick
viewer, picking_ui/11 — from (3) *de-novo picking a species* — the external picker, picking_ui/10.
The two load-bearing dimensions are **which tomogram** and **which species**; everything else is an
auxiliary fact of a pick set: source, count, filter status, extraction status, template+mask used.

## 0. Facts (gathered 2026-08-21)

1. **Two tabs, heavily overlapping.** Picks (`ui/species/picks_tab.py`): per-tomogram groups, list
   table (tick · auth · swatch · list · picks · ext · job · source · actions), merge bar, header
   buttons "Extract all pending" / "Import picks from path…" / "Open in Journey", per-group ⚡ and
   `journey ↗`. Curation (`ui/species/curation_tab.py`): session chip + "Open control center", the
   WHERE-TO-SAVE contract block, per-tomogram rows with `view_in_ar` curate / ⚡ / import / copy-dir,
   watcher log + unattributed saves.
2. **Nine live ArtiaX entry points** across the app: sidebar launcher
   (`pipeline_roster.py:1588-1607`), Curation tab "Open control center" (`curation_tab.py:160-162`),
   Curation tab per-row curate (`:217-219`) and ⚡ (`:220-222`), Picks tab per-group ⚡
   (`picks_tab.py:266-272`), Journey ⚡ (`tomo_dashboard_dialog.py:4192-4200`), and inside the
   control center: Start/Stop (`curation_session_dialog.py:202-204`), "Load into running session"
   (`:259-265`), "Save picks now" (`:294-299`).
3. **Two `.coords` import buttons**, one implementation (`list_actions.import_picks_from_path`
   `:743-810`): species-level with tomogram picker (`picks_tab.py:224-226`), per-tomogram
   (`curation_tab.py:223-225`). The watcher is the third, automatic path.
4. **Extraction affordances are already per-list and status-flagged** (the agreed model): row
   `science` action + ext badge + job chip (`picks_tab.py:344-391`), "Extract all pending" with a
   gate-report preflight (`:693-748`).
5. **Template/mask provenance is not on the row.** `ListRow`
   (`services/particles/species_overview.py:90-106`) carries source kind/ref but not which template
   and mask produced an auto list; that lives in the TM job's param snapshot.
6. **Journey duplication is already half-resolved** by archived roadmap 11 (Journey = look & curate,
   one ⚡, read-only auth radio; Species page = manage & act). This roadmap finishes the thought on
   the Species-page side.

## 1. End state

One tab, **"Picks & curation"** (replaces both), on the Particles registry. Top to bottom:

- **Status line** (one row, chips only): session state chip (click → control center) · lists ·
  picks · kept · gate. The WHERE-TO-SAVE contract block collapses into a tooltip on the session chip
  and a copy-dir icon per tomogram row — the prose paragraph goes.
- **Per-tomogram groups** (tomogram = the first dimension), each with the list table. Columns:
  swatch · list · source · picks (kept/total) · **origin** (template+mask tooltip, fact 5) · auth ·
  ext badge + job chip · actions.
- **Row actions** (the only place each verb exists): extract/re-extract · delete · set
  authoritative. **Group actions:** curate (the ONE launch affordance — semantics per
  picking_ui/10) · import `.coords` · copy curation dir · `viewer ↗` (per picking_ui/11; `journey ↗`
  until then).
- **Footer:** watcher log + unattributed saves (from the old Curation tab, unchanged — it is the
  staging inbox picking_ui/10 builds on).

Deleted outright: the merge tick column + merge bar (aggregation leaves this surface —
picking_ui/12), the species-level "Import picks from path…" header button, the Picks-tab ⚡, the
sidebar launcher, and "Open in Journey" (the per-group `viewer ↗` replaces it).

## 2. Stages

**S1 — merge the tabs.** `ui/species/page.py:43-49` drops the `curation` entry; `picks` relabels to
"Picks & curation". `picks_tab.py` absorbs the session chip, per-row copy-dir, and the watcher-log
footer from `curation_tab.py`; `curation_tab.py` is deleted. The contract prose becomes the session
chip's tooltip. Tab badge keeps the pick count (`page._push_tab_badges`). No handler logic changes —
this is a re-homing commit.

**S2 — one entry point per verb.** Delete: sidebar launcher `_build_curation_btn`
(`pipeline_roster.py:1588-1607`) and its wiring (`pipeline_builder_panel.py:95-107`); the Picks-tab
per-group ⚡; the species-level import header button. The merged tab's per-tomogram "curate" action
(`view_in_ar`) is the single launch/scope affordance; the control center is reachable only via the
session chip. The Journey ⚡ becomes a navigation link to this tab's row (`curate ↗`) — Journey
looks, the registry acts (consistent with archived roadmap 11). The control center's "Load into
running session" / "Save picks now" buttons are torn out in picking_ui/10 (they are the bridge, not
this surface).

**S3 — drop aggregation from the surface.** Remove the tick column and `_render_merge_bar`
(`picks_tab.py:301-312`, `:393-419`). `list_actions.merge_lists` and the dedup dialog stay at the
service/dialog layer (merged lists still exist and still render with their `join_inner` inspect
action); creating NEW merges moves to the Aggregate-candidates flow (picking_ui/12). Flag in the
commit message as a deliberate capability re-home, not a loss.

**S4 — origin column.** `ListRow` gains `template_name` / `mask_name` (auto lists: resolved from the
producing TM job's param snapshot via `source_ref`; manual/import: "—"). Rendered as a compact
`origin` cell with the full paths in the tooltip. This answers "exactly which template and mask were
used for picking" without leaving the row. If the snapshot cannot be resolved, the cell says so —
never guessed (CLAUDE.md "Surfacing uncertainty").

**S5 — links.** Per-tomogram `viewer ↗` → the full-page pick viewer once picking_ui/11 lands
(`journey ↗` fallback until then); the viewer links back here. Journey's "manage in Particles
registry ↗" keeps deep-linking to this tab.

## 3. Non-goals

- No change to extraction, merge, ingest, or watcher *behavior* — only where their controls live.
- The control center dialog's internals (tunnel command, VNC info) — picking_ui/10.
- The pick viewer itself — picking_ui/11.
- Cross-project aggregation (`merge_card`) — untouched here; renamed in picking_ui/12.

## 4. Runtime checklist (maintainer)

1. Particles registry shows Overview · Templates & masks · Picks & curation · Jobs. No Curation tab.
2. Exactly ONE way to launch/scope an ArtiaX session in the whole app (the merged tab's per-tomogram
   curate action); the sidebar icon is gone; Journey's ⚡ is now a `curate ↗` link that navigates
   here. With NO session up, curate opens the control center preloaded with that tomogram; with one
   already up, it swaps the live viewer to it (confirm dialog) instead of opening a second ChimeraX.
3. Exactly ONE import affordance (per-tomogram row); it still registers a list correctly — including
   into a tomogram that holds no picks yet, which now has a group of its own ("no lists").
4. Merge bar gone; an existing merged list still renders and its dedup inspect still opens.
5. Origin column: an auto list names its template+mask (hover shows full paths); a manual list and a
   de-novo species' auto row show "—"; a list whose TM job was deleted says "TM job gone", never a
   guess. `python -m services.particles.species_overview --project <proj>` prints the same answer
   headlessly.
6. Watcher: save a `.coords` from ArtiaX → appears in the merged tab's log and as a list row without
   reopening the page.
7. Journey still paints correctly with no ArtiaX session anywhere (it no longer polls `squeue`), and
   the section header's "manage in Particles registry ↗" still lands on Picks & curation.
8. Every tomogram of the project gets a group, ordered by name; a 100-tomogram project is still
   readable (if it is not, that is the cue for a "tomograms with picks only" filter — do NOT add one
   pre-emptively).
9. `ruff check .` + `python check_boundaries.py` + `python -c "import main"`.

## 5. Log

- 2026-08-21 — scoped. Inventory: 9 ArtiaX entry points, 2 import buttons, extraction affordances
  confirmed per-list + status-flagged (kept — extraction status is one of the maintainer's own five
  auxiliary facts, and the viewer needs cutouts). Merge controls leave the surface per the
  maintainer's "none of the aggregation controls belong here".

- 2026-08-22 — **S1 merged the tabs.** `page.TABS` drops `curation`; `picks` relabels to
  "Picks & curation" (the tab KEY stays `picks`, so every existing deep-link — the Journey's
  `manage_species`, `species_select_tab` — keeps working untouched). `picks_tab.py` absorbed the
  session chip, the per-tomogram copy-dir and the watcher-log footer; `curation_tab.py` is
  deleted. The contract prose is now `_save_contract()`, the session chip's tooltip.

  **Deviation from §2, deliberate:** the groups are built over the species' whole tomogram
  UNIVERSE (`species_tomo_map`), not just tomograms that already hold rows. The old Curation tab
  listed every tomogram for a reason — a tomogram with no picks is exactly where de-novo picking
  starts and is a legitimate `.coords` import target — and with the species-level import button
  deleted in S2, rows-only groups would have made those tomograms unreachable. A group with no
  lists renders its header (name · "no lists" · chips · actions) and no table.

- 2026-08-22 — **S2 one entry point per verb.** Deleted: the sidebar launcher
  (`pipeline_roster._build_curation_btn` + `pipeline_builder_panel.launch_curation_session`), the
  Picks-tab per-group ⚡, the species-level "Import picks from path…" header button, and "Open in
  Journey". 9 entry points → 3 (per-tomogram `curate`; the session chip → control center; the
  control center's own Start/Load/Save, which picking_ui/10 owns).

  **Deviation, deliberate:** per-tomogram `curate` is not plain `curate_in_artiax` — it is the
  Journey ⚡'s branch, moved here: live session → `load_tomo_into_session` (swap in place), else →
  `curate_in_artiax` (bundle + control center). That is the honest merge of the two buttons the
  old Curation tab had side by side, and it keeps `load_tomo_into_session` reachable (it would
  otherwise have become dead code — its only three callers were all deleted by this roadmap).

  The Journey's ⚡ became `curate ↗` in the list toolbox, routing through the same
  `manage_species` the section header uses (threaded down through `_render_species_tab_body` →
  `_render_list_rail`). Follow-on cleanups that fell out: `_tomo_ref` and the `ListRef` /
  `list_actions` imports are gone from `tomo_dashboard_dialog.py`, and the Journey no longer
  polls `session_status` or folds it into `_main_signature` — it shows no session state now, so
  the `squeue` and the spurious full rebuild on every session start/stop both go with it.

- 2026-08-22 — **S3 aggregation off the surface.** Tick column, `_render_merge_bar`,
  `toggle_tick` / `clear_ticks` / `set_merge_name` / `merge` and the `ticks` / `merge_names` state
  all deleted, with `.cb-ptable-tick` / `.cb-merge-bar` CSS. `list_actions.merge_lists` /
  `can_merge_source` / `open_dedup_dialog` are untouched — merged lists still render and their
  `join_inner` dedup inspect still opens. **Capability re-homed, not lost:** creating a NEW merge
  is the Aggregate-candidates flow (picking_ui/12).

- 2026-08-22 — **S4 origin column.** `ListRow` gains `template_path` / `mask_path` /
  `origin_note` (full paths, not names — the cell shows `Path(...).name`, the tooltip the whole
  path). Resolver `species_overview.tm_origin_for_ce`: candidate-extract instance →
  `paths["input_tm_job"]` → its parent dir == the template-match job's `job_dir` → that job's
  `template_path` / `mask_path`. A recorded SNAPSHOT, so it stays right for a species that later
  acquires a second TM instance; in-memory `state.jobs` only, so it is free on the render path.
  Four answers, no fifth: the template name · "—" (manual/imported/merged, and the de-novo auto
  row — no template was involved) · a short note ("TM job gone", "no TM input recorded") · never a
  guess. The module CLI prints the resolved origin so this is verifiable headlessly.

  **Deviation from §2, deliberate:** column order is swatch · list · **source · origin** · picks ·
  auth · ext · job · actions. §1 had source and origin flanking `picks`; they are one story
  (where did these coordinates come from) and belong adjacent.

- 2026-08-22 — **S5 links.** Per-tomogram `journey ↗` stays the viewer fallback until
  picking_ui/11 (marked in-code at its call site); the Journey's "manage in Particles registry ↗"
  and the new `curate ↗` both deep-link to this tab. Copy sweep: every user-facing "Curation tab"
  / "Picks tab" string across `curation_session_dialog`, `list_actions`, `overview_tab`,
  `tomo_dashboard_dialog`, `session_status`, `list_ref` and `watcher` now names
  "Picks & curation"; the no-live-session toast no longer points at a tab that does not exist.
