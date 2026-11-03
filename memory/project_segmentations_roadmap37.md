---
name: project-segmentations-roadmap37
description: "Roadmap 37 segmentations v1 — S1–S7 BUILT 2026-10-20, on main by 2026-10-24 (tree clean at 7ea82cb): Segment job, import, fills in wall/Journey/pick viewer, TM search region, ChimeraX surfaces; next = maintainer's one runtime pass (§5, §6.4, §7.4)"
metadata:
  node_type: memory
  type: project
  originSessionId: a83e89c4-2457-44cc-b98e-794f585a6664
  modified: 2026-10-20T15:40:40.151Z
---

`docs/roadmaps/roadmap_37-segmentations-v1.md` — S0 record, §6 (S1–S3 build record), §7 (S4–S7 build record).
Survey `docs/reports/tool-survey/segmentation-and-annotation-models-2026-10.md`. Image
`/groups/klumpe/software/containers/sifs/easymode_1.2.5.sif`, weights `cache_and_artifacts/easymode_1.2.5/`
(config: `tools.easymode.cache_dir`). Cluster scripts in `/groups/klumpe/software/containers/tests/`.

Commit state (2026-10-20): NOTHING of S1–S7 committed (HEAD = "Glass look…"); S3's css.py and S2's
io_config_component.py hunks went into earlier commits. Patches for a two-commit split:
`notes/37-stage-snap/commit_s1s3/*.patch` (HEAD→S3 state of the files S4–S7 also changed, job_lineage.py included),
`pipeline_runner_segmentation.patch`, `orchestrator_segmentation.patch` (those two files also hold another
session's uncommitted "quiet logs" hunks — never `git add` them whole). `base4/` = every S4–S7 file before S4,
`s4-s7/` = as built. Two review subagents ran on S4–S7 the same day; their findings are fixed (roadmap §7).

Non-obvious facts:
- HF_HUB_OFFLINE does NOT make easymode offline (it GETs huggingface.co); offline = `https_proxy=…127.0.0.1:9`.
- easymode/Ais rewrite settings at every start → per-task EASYMODE_CACHE_DIR / AIS_CONFIG_DIR.
- Ais output name = `<stem>__<.scnm title>.mrc`.
- An optional input slot auto-wires (and adds an afterok edge) whenever a producer exists → TM's segmentations
  input uses `InputSlot.when="restricts_search"` instead of required=False.
- Perl `"…@property…"` interpolates `@property` away — write replacement text via single-quoted heredoc files.

**Why:** maintainer trimmed scope to basic produce/ingest/show/navigate + TM search region; meshes, copick, Z
scrubbing out of v1.
**How to apply:** registry 1.6 must reach main before Segment runs on projects other servers open
([[reference_registry_schema_cross_branch]]); features are registered by the SERVER (job landing, import),
never by a driver. Related [[project-tool-survey-2026-10]], [[feedback-container-separation]].

First run (2026-10-20): MyrGFP_Grid2 job038 (Segment on tsReconstruct__2 = missAlign, job024) succeeded 18:03 —
manifest, registry entries (48 TS), overlays and server landing (feature_registry) all OK. The wall still showed
nothing: with 2 reconstruct jobs it starts on the lowest job dir (job009, aligntiltsWarp) and hides layers made on
another reconstruction; only cue = "0 of 3 layers" + an eye-off icon inside the menu, next to the always-present
"Import segmentations…" button. Proposed (not built): start on the version with layers, say "N on <version>" in
the trigger, move Import into the menu, invalidate the wall when a Segment job lands.

Quality recon (2026-10-20, MyrGFP job038 at 6.2 Å; sources easymode v1.2.5 / Ais v1.2.38 on GitHub):
- Ais engine (all .scnm: cytoplasm/nucleus/void/mitochondrion 2.5D at 50/30 Å, membrane 3D at 10 Å) resamples XY
  ONLY; Z stays native, and Ais training also took native Z → models expect 10 Å Z spacing (16 slices = 160 Å);
  at 6.2 Å they get 99 Å. Native .h5 path (ribosome, microtubule, tric) resamples XYZ to 10 Å — fine.
- Both engines' global_mad = MAD of native voxels (32 Z-slices, no low-pass) → on 6.2 Å the ~50 Å picture arrives
  at 0.72× the normalized contrast of a 12.4 Å bin (measured, Position_1 and _10).
- Ais TTA lists have 8 entries: tta 9–16 → IndexError → placeholder → our task fails; SegmentParams allows 16.
- Output: per-plane banding in Z (cytoplasm 11→57→8 % of a plane 100 Å apart), blocky 50 Å-grid regions,
  cytoplasm frame along volume edges; overlay orientation vs WarpTools PNG verified (r 0.98).
- Built as roadmap 38 (2026-10-20, NOT RUN): see [[project-segmentation-quality-roadmap38]].

**2026-10-24:** committed and on main (segmentations fast-forwarded into main, tree clean at 7ea82cb); the patch-split notes above are moot.
