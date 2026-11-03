---
name: project-sta-kitchen
description: "STA kitchen = roadmap 40 on STA_Kitchen: S0–S5 + S6.1 built 2026-10-28, R1 the same day after the maintainer's first look (Tags/Doctor/Ledger out), S6.2–S6.7 that evening (second session), speed pass that night (third); NOT RUN in a browser (§10 items 1–16); record = roadmap §11"
metadata:
  node_type: memory
  type: project
  originSessionId: d12f21a2-9c3b-4349-92a1-53a707cf00d9
  modified: 2026-10-28T12:54:41.575Z
---

`docs/roadmaps/roadmap_40-sta-kitchen.md`, scoped and written 2026-10-28 after a design chat; README Pending row
at the top; roadmap 30 decision 6 marked superseded. The maintainer plans to build it in the next session, S0 → S5
back to back, on branch `STA_Kitchen`.

Shape: one pool per species in the home project (`Analytics/kitchen/<species>/`), members = candidate-extract jobs and
pick lists from any project (other projects read-only via snapshots), one row per pick with columns, one custom NiceGUI
element (`kitchen.js`) doing filters/gallery/slab dots in the browser, tiles cut on demand from reconstructions,
verdicts in `verdicts.jsonl`, Make set → ordinary ParticleSet (`SetOp.POOL`). D1–D7 accepted ("Ok, write it");
D8–D11 (one pool per species, sets as filters, hand keep rescues, roadmap 30 D6 superseded) proposed in writing.

**Why:** the user calls it the scaffolding for future STA work; the Journey Particles panel, Analytics score/sets
panels and merge/aggregate dialogs are to be deleted once it covers them.

**How to apply:** start from the roadmap's Status, §7 and §8; S0 may read the AGG412 and copia projects read-only (the
handoff authorizes it). Found while scoping: per-pick NiceGUI elements in pick_viewer can't scale; foreign projects
loaded via `get_project_state_for` get reconciled and saved by this server (roadmap §3 hazard, fixed in S1).
Related: [[project-particle-analytics-roadmap30]], [[picks-filter-per-species-and-aggregation]],
[[feedback-scope-design-talk-into-roadmap]].

**Build session 2026-10-28:** S0–S5 and S6.1 built and checked in the sandbox (record = roadmap §11, snapshots in
`notes/40-stage-snap/`). Sandbox checks use scratch homes (copies of a project's params) and redirect
`store.kitchen_dir` to the scratchpad, so nothing is written under /groups; worker-using scripts must be files with a
`__main__` guard (spawned workers re-import the main script). JS parity: `node` over kitchen.js's store functions vs
`services/kitchen/pool.select` (`notes/40-stage-snap/s2/check.mjs`). Facts that shaped it: the pilot has no ribosome
extraction (ribosome = MyrGFP_Grid2 job027/job028, two alignment branches → D12, "prefer … job024"); no AGG412
subtomo/pick lists/_filtered stars; one cutout read ≈ 25 ms on NFS (norm sample cut to 8). Next: §10 pass, then
S6.2–S6.8.

**R1, same day (after the maintainer ran it on MyrGFP_Grid2, 412 pool of 7 members):** they asked for speed and
caching of every picture, the registry's tabs on its title line, a metric per tile, the Journey's cutout scale, one tile
everywhere, a readable "+ filter" list that stays open, no grey rebuild, Tags/Doctor/Ledger OUT ("didn't ask for this,
later"), the means on request only, pool mode without tomograms, nothing lost on a mode switch. Built and checked in the
sandbox (jsdom harness `notes/40-stage-snap/r1/harness.mjs`, jsdom installed in the session scratchpad only); record =
roadmap §11 R1. NFS fact: one cutout reads ~1.3 MB (~19 ms); threads and processes both cap at ~80 MB/s, so bytes
(the norm sample, 8 → 3) not parallelism bound a cold screen. Box now 1.4 × Ø (`CUTOUT_BOX_FACTOR`), matching the
Journey's Warp-export subtomos (794 Å for 412). Orphaned caches in MyrGFP_Grid2's `Analytics/kitchen/412_particle/`:
old-spec `tiles/` (~187 MB) and kitchen-dir slab PNGs (~337 MB) — told the user, not deleted.

**S6.2–S6.7, 2026-10-28 evening (second session, asked as "continue with the roadmap"):** tomograms filter kind +
σ-by-tomogram strip, pairs chart (raw counts to 2Ø, pair view), `agreement` column (Ø/2, in `pool.assemble`), set
filters (`setmatch.py`: uid list or position ≤ 0.5 Å), "pictures" select (extracted subtomograms via the Journey's join,
`pictures.py`), Review mode (blind 50+50, Wilson), loupe (space; `loupe.py`). All opt-in (R1 lesson: nothing extra on
screen unasked). One commit (shared files). Checks in `notes/40-stage-snap/s6b/` (notes/ is git-ignored); fixtures
copied from session 05396fd4's scratchpad (home_myr, kitchen, jsdom). Open points told to the user: the table's
`extracted` column joins Subtomo Extraction only (Warp export not), so Make set treats Warp-exported picks as
coordinates; Warp-export projections don't line up with the reconstruction's (transpose best, weak). Template turn
convention: [[reference-pytom-angle-convention]].


**Speed pass, 2026-10-28 night (third session, asked as "roadmap 40. speed tips. let's vectorize and hone in"):**
built, uncommitted at the end of the session; record = roadmap §11 "Speed pass" (table of before/after). Old-vs-new
harness in `notes/40-stage-snap/speed/`: HEAD's objects are PACKED (the node loose-object reader fails on the HEAD
commit), so `make_oldtree.py` rebuilds the HEAD files from the `s6b/` snapshots plus this pass's edits reversed, as a
symlink tree. Found, not built (told the user): a registry load costs 0.2–2.2 s per project, ~all of it pydantic
validating `Path` fields (207k Path parses for 759 tilt series); the aggregate dialog runs `merge_coordinate_sources`
in `run.io_bound` (a thread, GIL) rather than a worker.

**R2, 2026-10-29 (the maintainer ran it in a browser):** paths renamed to `services/sta_kitchen/`, `ui/sta_kitchen/`
(`sta_kitchen.js`), pool folder `Analytics/sta_kitchen/` (old `Analytics/kitchen/` renamed on first open); leaders
gone; sizes S/M/L/XL = 64/96/160/256, S default; no tooltips on cutouts; right-click tile menu (keep/drop/clear/loupe)
= the vehicle for subset and tag actions. jsdom harness lives in session 882716d6's scratchpad `jsdom/`
(node_modules there). Open design talk: subsets (live labels in the home pool, frozen by Make set; class/refine results
mapped back by the coordinate join), segmentation layers in By tomogram. Review mode confused them.
Roadmap 40 §12 (2026-10-29) = the subsets proposal D13–D18, awaiting nods: one source per acquisition (generalises
D3/D12), overlapping subsets + disjoint groups (tags dropped), jobs read frozen sets only, a job-start set preflight,
rlnRandomSubset from the uid, Class3D/Refine3D results back as columns + By class mode. The maintainer said they don't
fully understand it yet: explain with scenarios, not jargon.

2026-10-29 later: D13, D14, D16 accepted (§12.8; D13 hand-list question open). Next = S8 prototype in §13
(selection gestures, subsets + groups, the rail + scope line, Sources tree project → tomogram → job with the
per-tomogram choice); downstream jobs (D15/D17/D18) after. Three pins in §13.7 before building. Not built.

2026-10-29 S8 BUILT (roadmap 40 §13.8, NOT RUN in a browser): Draft.scope (subset ids), subsets in
subsets/<id>.txt + meta in pool.json, groups (disjoint, add = move), source_choice per acq_key (D13 blocker
only when kept picks clash), selection gestures + floating bar, rail left of gallery/cards, Select/Paint toggle,
Sources tree (ticks edit ordinary filters), filter cards 54 px with keeps/removes, By tomogram tiled cards +
spare cards + Dots menu. S8 test scripts in this session's scratchpad s8/ (s8_cases.py, s8_check.mjs,
s8_unit.py, h8.mjs, page_sim_s8.py).

2026-10-29 evening: maintainer happy with S8 ("looking pretty good"). Next = roadmap 40 §14: subsets as inputs of
particle jobs (D19 Make set from rail / per group, D20 offer sets by level, D21 extraction keeps lineage,
D22 lineage on screen, D23 job-start checks). Today: Make set → Pool set in every job's input_optimisation dropdown
(path_resolution_service._add_particle_set_candidates), coordinates sets offered to Class3D too. Open fact:
can Subtomo extraction / Warp export extract tomograms living in another project?

2026-10-29 night: §14.3 step 1 facts recorded as roadmap 40 §14.4 (code + RELION 5.0 source on GitHub,
nothing run, no project data read). Key facts: Reconstruct particle cuts from the tilt series (coordinates sets
OK); Subtomo extraction / Warp export stop at the HOME registry preflight (array_job_base.preflight_registry) for
foreign tomograms; RELION reads per-tilt stars by block data_<rlnTomoName>, so the coordinates merge's
`<name>__<project>` renames break extraction/Reconstruct; extracted merge raises on name clashes; row order not
kept through extraction; RELION + Warp carry unknown columns. §14.2 rewritten: D20 by job, D21 = uid column
`cbPickUid` (no second set per extraction), D24 new (origin columns, set-owned renamed per-tilt stars, per-project
registry, exclusions marked not applied). All D19–D24 await the maintainer's nod; order D19+D20, D24+D21, D22, D23.

2026-10-29 later: D19–D24 ACCEPTED (roadmap 40 §14.5): D21 with the rule "treat RELION/Warp stars as untrusted"
([[star-files-untrusted]]); D24 without exclusions (per-project concept; a small "excluded in <project>" marker in
the kitchen; jobs reading a set apply none). Level check BUILT (§14.6, not run in a browser): InputSlot.needs_extracted
(Class3D/Refine3D), OutputSlot/OutputCandidate.coordinates_only (CE optset, coordinates sets); check script in
session 91526d05's scratchpad level/level_check.py. Next session: build §14 in order D19+D20, D24+D21, D22, D23.

2026-10-29 evening (session 91fd6b69, took over from d4961087 after the user committed its half): rest of §14
BUILT, record = roadmap 40 §14.8, NOT RUN in a browser (§10 items 17–24). Warp exports joined by position
(TABLE_SCHEMA 4; the 412 draft makes 2,697 of 2,708 picks with images, box 256 / 3.1 Å); set extractions joined by
uid (`services/sta_kitchen/uid_join.py`); Make set from a subset / each subset of a group / the selection; plain-word
dialog ("What the set holds"); members renamed "template-match picks jobNNN" on open; rail "→ n sets · k changed
since sNNN"; drivers check each tomogram against its own project's registry, no exclusions on set inputs, Warp files
staged under the set's name; Run checks = `services/particles/set_preflight.py` (ErrorCode.SET_CHECKS). RELION 5
keeps given halves (verified in exp_model.cpp). Checks in that session's scratchpad `c14/`.
Found: the R2 rename was half committed (HEAD still has services/kitchen/ + ui/kitchen/, page.py importing
ui.kitchen, recon_cutouts without CUTOUT_BOX_FACTOR); handed a "finish the rename" commit. Next: D18.

2026-10-30: first real run of a kitchen set: Reconstruct job048 (MyrGFP_Grid2) on s001 → 412 capsid shell (peak
~160 Å radius), no facets (TM angles disagree); FSC 37 Å is the round shell only. Roadmap 40 §15 written: one set,
many facets (set = uids, no level; job picks each facet's source on its I/O, written + frozen at Run; per-project
sources combine only with identical params; never combine columns of two jobs; "Run one Warp export on this set"
when they differ). Open: I/O layout (mock page /dev/io-layouts = ui/io_layouts_preview.py, delete after the pick;
route added in ui/main_ui.py) and in-place retry rule (failed jobs retry in place comparing inputs only, params
editable → mixed boxes possible; maintainer leans "forbid", I proposed params+inputs equal). Found: s001
particles.star rlnTomoTiltSeriesPixelSize 6.2 copied from TM vs 1.55 in tomograms.star.
2026-10-30 later: layout A TAKEN for the §15 build; build handoff = roadmap 40 §15.8 (next session; ask the in-place
rule before step 5). Six "outside the box" designs for the future graphical builder on /dev/io-ideas
(ui/io_ideas_preview.py; sockets, pantry, river, questions, matrix, project map) = §15.9; both preview pages + routes
in ui/main_ui.py stay until the maintainer picks.
