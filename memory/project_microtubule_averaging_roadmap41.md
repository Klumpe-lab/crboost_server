---
name: project-microtubule-averaging-roadmap41
description: "Roadmap 41 (S2 Filament pick job BUILT 2026-11-01, not run): MT average via easymode seg → ais pick -filament → warpExport → helical Refine3D; 2026-10-29 D1 = 20260820 Tir_Env B1G4 + B2G2, D3/D5 agreed, D4/D6/D7 deferred, S0 hand-run owed"
metadata:
  node_type: memory
  type: project
  originSessionId: 4c7597ee-595c-45e0-9fb8-909cfae746fd
  modified: 2026-10-29T12:57:39.232Z
---

`docs/roadmaps/roadmap_41-microtubule-averaging.md`, README Open row. Scoped 2026-10-28 on MyrGFP_Grid2.

2026-10-29 answers: D1 source = 20260820_B1G4_Tir_Env_mutant (63 TS, recon done) + 20260820_B2G2_Tir_Env_Mut (146 TS,
recon running); neither had Denoise/Segment yet. Iso denoising before Segment is a recommendation only (never
compared); the maintainer skipped it and segments tsReconstruct job007 directly (microtubule + actin, TTA 8; tsCtf
job006; missAlign precedes tsCtf there). D3 (82 Å,
bin 2, box 40) and D5 (no helical symmetry; Ais tilt/psi are the priors) agreed; D4 EMDB pick, D6 polarity vote,
D7 filamentPick job type deferred until the hand-run shows tracing works. Tracing trial = `ais pick -filament` on
MyrGFP job045 (IsoNet2-n2n input job041 — project_params says cryoCARE but run.out shows it inherited IsoNet at runtime; job046/047 deleted 2026-10-29); aim = workflow robustness, not yield. Command in roadmap §6 S0 item 3.

2026-10-30: Tir_Env Segment job008 done in both (raw tsReconstruct job007, int8, p0.5 = 64). MT fraction >0.05 %: B1G4 12/63, B2G2 32/139. Best clean tomo = B1G4 Position_11_5 (3 in-plane tubes 260–518 nm, visible in raw); busiest = B2G2 Position_22_7 (~10 tubes + fragments). False positives seen: B2G2 Position_6_2 = all along the tomogram edge; Position_3_3/3_5 have 40–60 nm-thick merged components. Estimated ~20–25 µm MT usable ≈ 2500–3000 boxes @82 Å (roadmap §6 "Tir_Env segmentation"). Viewer: stay on ChimeraX+ArtiaX, no napari. Trace 2026-10-30: B1G4 470 + B2G2 1529 pts (~16 µm); recall-limited (missed MTs score ~0, lower threshold barely helps) → denoise + re-segment. B2G2 iso done 2026-10-30 (job009 denoise → job010 segment): fraction_pct >0.05 % 57 vs 32 tomos, but top risers are diffuse low-p, not tubes; trace of job010 = 1358 pts/48 fil/26 tomos vs raw 1529/67/28, same tubes (97 % within 19 nm), fewer fragments → denoising does not lift recall; Position_22_7 tubes segmented in short pieces that -size/-length drop. B1G4 n2n vs iso not submitted (low value now).

Ais 1.2.38 (in easymode_1.2.5.sif) facts from its source are in roadmap §3: int8 threshold halved (128 = p 0.5),
no hole filling, full-volume skeletonize per component (use -b 2 and -size), junctions split filaments, loops dropped,
angles already RELION convention (A·e_z = tangent), parse_known_args swallows misspelt flags. Half-sets must split
per tube (boxes overlap ~80 %) → rlnRandomSubset per tube in the conversion.

2026-11-01 S2 BUILT, NOT RUN: JobType.FILAMENT_PICK "filamentPick" (services/jobs/filament_pick.py, drivers/filament_pick.py,
services/particles/filament_picks.py); directions computed from neighbouring points (agree with Ais to 0.0° median);
conf.template.yaml filamentPick CPU profile — user's conf.yaml (dev + lab) lacks it → job asks for a GPU until added.
Warp Export binds to it with no code; B2G2 has two Segment jobs → choose job010 in the I/O section. Fine-tuning
(`easymode train --weights microtubule.h5`, Ais .scnt labels) recorded as the recall route (roadmap §9).

**Why:** first filament STA; actin is a later roadmap (TARDIS likely, helical symmetry imposed).
**How to apply:** build only on an explicit "build" ([[feedback-scope-design-talk-into-roadmap]]); next is the
maintainer's hand-run result (axis vs wall, IDs per tube). Related [[project-segmentations-roadmap37]],
[[project-sta-kitchen]].
