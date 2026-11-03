---
name: project_agg412_reprocessing_2026-10-04
description: "AGG412_* (8 projects, 922 TS, created 2026-10-02) vs May agg_* subsets: 30:10 cut per-tilt defocus outliers 148→62 on 51 matched TS; 55 TS without tomograms; 25 tomograms keep blank tilts the DL filter scored good; TM driver still nominal angles + no phase-flip"
metadata:
  node_type: memory
  type: project
  originSessionId: aae66ed4-23d6-4dcb-9f45-a1f2bb80ec9f
  modified: 2026-10-04T16:52:43.471Z
---

Read-only comparison run 2026-10-04 (node scripts over registry + Warp XMLs + AreTomo logs; no python in sandbox).
New: `AGG412_{20240204_412_Slot6, 20251031_412_FCIsolation, 20251113_412, 20260204_412, 20260205_412_FCIso_unmilled2,
20260311_412_Grid3, 20260311_MyrGFP_Grid2, SK_20251017_412}` — chain import→fsMotionAndCtf→tsImport→tiltFilter(DL,
before alignment)→AreTomo→tsCtf→tsReconstruct, 30:10, deconv 0. Old May subsets with counterparts: agg_ Slot6 (11),
FCIsolation (11), 20251113 (18), MyrGFP_Grid2 (13); TS names differ only by prefix, frame names identical.

**Results:** matched fs outlier tilts (>1 µm off series median) 148→62; 183/2141 tilts moved >0.5 µm, 141 toward
consensus; tsCtf res median 6.2–6.9 → 6.1–6.3 Å; AreTomo identical unless the tilt set changed (pre-filter removed junk:
26_3 residual 359→3 px, May bullseyes from a blank +69.9° tilt gone). Motion/astig/hand unchanged (same frames, same params).

**Open (reported, not fixed):** 52 align fails (7 = 0–1 tilts left after DL filter → Warp abort; 45 = AreTomo dies
mid-run, all nodes ~5 %, not node rot; driver message wrongly cites the long-name .tlt bug for short names); 3 recon
fails = 1-tilt TS passed through align/tsCtf as ok. 25 tomograms keep blank tilts (mdoc mean 0–9) with DL p_bad
0.01–0.44 → bullseyes (seen on 20251113 Position_19_3). 78 TS <25 tilts (34 FCIsolation, thr 0.301). AreTomo
tilt-offset still noise (CC ≤0.02, 75 pinned ±15°), applied. TM driver fixed same day (YTilt + aligned-tilts-only + phase-flip, uncommitted) — see [[project_v1_driver_parity_audit_2026-09-15]].

Samples are Drosophila follicle cells (user-confirmed); ribosome template suggested: PDB 6XU8 (D. mel ovary 80S, 3.0 Å),
fallback 4V6W. Related: [[reference-412-dataset-locations]], [[project_blank_tilt_rays]], [[copia-ctf-bimodal-fits]].

**TM plan given 2026-10-04:** 412 = `agg_20251113_412/templates/412/run_it015_class001_apix6.20_box128_black.mrc`
+ `_sphere_d575_s5` (shell r 125–265 Å), I1 @12°; black because Warp runs `--dont_invert` and PyTOM phase-flip
uses abs(ctf) (template contrast must equal tomogram contrast). Ribosome = 6XU8 via workbench (6.2 Å, box 64,
black), sphere Ø310 Å s3 (99 % atoms ≤152 Å, max dim ~318 Å), C1 7–10°. Extraction radius FIXED 2026-10-05 (uncommitted): --particle-diameter now the full diameter; May picks
proved the bug (closest pair = 149 Å = 24 px in every project, i.e. D/4 exclusion for D 575).

**Playbook (2026-10-05):** `notes/agg412_picking/PLAYBOOK.md` (git-ignored notes/) is the living record for the
412 + ribosome picking campaign — decisions D1–D12, watch-list W1–W13, run log, TS lists; analysis scripts in
`notes/agg412_picking/scripts/`. Pilot project = `AGG412_20260204_412` (species `412_particle`, `md_ovary_ribosome`).
Read it first in any session about AGG412 picking and append to its run log.

**2026-10-05 verification (playbook D13–D18, W14–W16):** §5 re-checked against PyTOM 0.10.0 source + May score
maps. Wrong and corrected: 412 subtomo bin 1/786/600 → bin 2/384/224; FP=1 does NOT give ~1 FP per tomogram (May 412
maps: 788/1026 picks above 7.1σ, ~15/tomo, the cap sets the count; the PI manual 1–3); ribosome 40 Å low-pass rationale was
copied from PyTOM's 13.79 Å/px tutorial → A/B 25 Å at the same 7.6°; ribosome box 64 too tight (CTF weighting happens
in the template box) → 96, ~+4 % cost. Ribosome hand check (mirrored template) promoted to a pilot step. Rollout of the
two species = the lab catalog (roadmap 12), currently off (species_catalog_root empty) — see [[reference_settings_dialog_drops_override_keys]].

**2026-10-05 ~11:20:** user recreated the pilot ribosome species as `dm_ovary_ribosome` (was `md_ovary_ribosome`), box-96
templates; all five picking jobs deleted (to be recreated from playbook §5). Polarity rule recorded in playbook §4: template
contrast = tomogram contrast → BLACK here (Warp `--dont_invert` + PyTOM phase-flip keeps template contrast); masks have no polarity.

**2026-10-05 12:43 pilot TM read (playbook run log, D19, W19):** 412 TM healthy (phase-flip on, 3398 rot, σ 0.0073–0.0094);
MEASURED 17 min/tomo on g3 RTX, 7 min on A100 — the "95 s" in older notes was wrong (May was the same 13–17 min). Real 412
peaks stand at 10.4–16.8σ vs noise ≤9.6σ (~1 per tomogram ≥10σ; FP=1 = 7.1σ gives ~13). Limits that bite: TM task walltime
1:30 (watchdog 81 min), supervisor 4 h (cancels children) → ribosome 7.6° (~3 h/tomo) impossible; pilot switched to 12°.
Rollout needs supervisor_slurm.time 8:00:00 + TM throttle ≥ N/12. Script: notes/agg412_picking/scripts/tmpeek.js.

**2026-10-05 15:30 ribosome pilot read (playbook run log):** job013/014/015 clean. 12° TM = 11–15 min/tomo (box 96 ~3× cheaper per rotation than the box-128 412; D19's 45 min was wrong). 15,672 picks at FP=1 (7.24σ), continuum 7–12σ, no gap; signal real (scores peak mid-lamella). Rollout blockers: supervisor 4 h (W19), species extraction_params null. Script: scripts/ribohealth.js.

**2026-10-08 failure survey + STA scope:** rollout (2026-10-05 15:53–16:06, supervisor 8 h, TM throttle 4) left 19
Failed jobs in 5 projects, none needing recompute: 6 TM supervisors (5 ribosome + Slot6 412) killed at 8 h with all
tasks `.ok` (arrays survived), 12 dependents never started, SK 412 subtomo job014 raced the NFS cache. Remedy = one
Run per project (in-place re-submit; TM supervisor finds nothing to submit and aggregates). Plan from the picks on:
`docs/roadmaps/roadmap_29-agg412-sta.md` (ribosome R1/R2 incl. hand check, 412 V1–V4 local capsomer route,
fixed-particle missAlign A/B on the pilot, tools C1–C5 unbuilt). Playbook W19 updated, W20 added. See
[[reference_supervisor_timeout_arrays_survive]].
