---
name: project_412_sta_refinement_playbook
description: "412 capsid post-aggregation STA refinement — RELION/M playbook doc, EXTERNAL to crboost, run from Apptainer containers; decisions + what it's gated on"
metadata: 
  node_type: memory
  type: project
  originSessionId: 14d7c7a0-bddf-4ac7-bd2f-6dca270ad89b
---

Post-aggregation refinement of the 412 capsid (94 clean particles @ 16.3 Å from Class3D job003,
I1, box 600 @ 1.55 Å/px). Sequel to the aggregation handoff.

**WORKING AGREEMENT (user, 2026-07-02):** this whole effort is **external to crboost** — run in
**RELION-5 tomo + M and whatever else helps, straight from Apptainer containers**. Do **NOT** add
code/job-types to crboost for it; crboost's role ended at aggregation. User has spare containers for
RELION/M/etc. beyond conf.yaml — ASK if a step wants a specific build.

**Artifact:** `docs/reports/412-capsid/412-sta-refinement-playbook.md` (job-by-job teaching doc). Continues
`docs/reports/412-capsid/412-capsid-aggregation-handoff.md`. §1 = run prelude (`rel()` alias); §11 = session bring-up.

**Sequence:** Step0 measure radial profile → Step1 cut 94-ptcl optset → Step2 shell mask(s) → Step3
consensus Refine3D (I1, masked, self-ref low-passed) → Step5 geometry loop (Polish⇄CtfRefine⇄Refine3D,
Polish-first) → Step6 symmetry-expand→focused capsomer refine (94×60×13≈73k sub-ptcls) → Step7
PostProcess (manual B −75) → Step8 M.

**HOW TOOLS RUN ON THIS CLUSTER (verified via conf.yaml + container_service.py, 2026-07-02):** NOT
`module load` — everything is Apptainer. Alias baked into doc §1:
`rel(){ apptainer exec --nv --cleanenv -B /groups -B /scratch -B /tmp "$RELION5" "$@"; }`.
Containers (full paths):
- RELION 5.0-tomo: `/groups/klumpe/software/Setup/cryoboost_containers/relion5.0_tomo.sif`
- Warp 2.0.0dev36 (+M, UNVERIFIED): `/groups/klumpe/software/Setup/cryoboost_containers/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.sif`
- ChimeraX+ArtiaX GL: `/groups/klumpe/software/containers/sifs/chimerax_artiax_GL.sif` (GPU node + VNC, not headnode)
GUI = drop `--cleanenv`, launch in a VNC session; RELION Running-tab reuses crboost `config/qsub.sh`
(XXXextra1..8XXX = Partition/Constraint/Nodes/Tasks/CPUs/GRES/Memory/Walltime).

**EMAN2 IS NOT INSTALLED** (no container, no conf.yaml tool). Doc's old `e2proc3d mask.sharp` carves
REPLACED (2026-07-02) by a numpy shell-carve run inside the RELION image (ships python+mrcfile) →
`relion_mask_create` softening. Step-0 radial profile is likewise a numpy snippet, not e2proc3d.

**Key discipline baked into doc:** self-ref low-passed (NOT the PI's high-res template) + gold-standard
FSC = anti-model-bias; 94 ptcls thin for per-tilt Polish/CtfRefine (symmetry does NOT multiply
geometry-fit leverage) → noisy Polish, reliable CtfRefine, manual B-factor; mask must match refine
box/apix; verify center+I1 before sym-expand or it symmetrizes to mush. Grounded in the PI's and a colleague's
CryoBoost tutorial `/users/artem.kushner/dev/CryoBoost/docs/extdocs/tutorial/tutorial.rst` (Copia recipe).

**SANDBOX LIMITS:** Claude sandbox has NO /groups mount, NO python, NO container runtime. On restart the
412 project dir is bind-mounted → Claude can READ star files + CUT the 94-ptcl subset directly, but
STILL can't run RELION/numpy/apptainer → `--stats`, radial profile, RELION version, M check are all
user-run-and-paste. See [[reference_crboost_data_access]] [[feedback_no_cluster_lib_snooping]].

**STEP 1 CUT — ✅ DONE 2026-07-02** (mount worked this session: /groups WAS bind-mounted, contra the old
"no mount" note — that's session-specific, but the files now persist on disk). Cut from
`External/job003/run_it015_data.star` filtered `rlnClassNumber==1` via awk (byte-exact, streamed). Output:
`/groups/klumpe/crboost_data/412_aggregation/sta_refine/{optimisation_set,particles}.star`. VERIFIED: 94
rows all class-1 all 24 fields; general/optics/loop headers byte-identical to RELION; optics row confirms
300kV·Cs2.7·ampl0.1·**1.55 Å/px·box600·bin1** (§1 conventions now runtime-verified). Absolute paths inside
→ relocatable. **Provenance finding: 94 ptcls span 57 tilt-series, ~1.6 each (one has 16, most have 1); all
57 present in shared tomograms.star.** → materially weakens per-series geometry fits: even CtfRefine is
thinly constrained, Polish likely unfittable, and M (Step 8) may not beat consensus Refine3D until more
ptcls/series. (Baked into doc §3 DONE-callout + §6 Step-5 caveat.) Aside: job003 `run.err` shows the crboost
class3d.py driver FATAL'd post-run looking for unnumbered `run_optimisation_set.star`, but RELION itself
finished all 15 iters fine (run_it015_* all present) — cosmetic driver filename-expectation bug, not a data problem.

**DECISIONS LANDED (2026-07-02 — all 3 gather items back, doc §1/§2/§5/§7/§9/§11 finalized):**
(1) Radial profile → **ONE-SHELL branch**: one *positive* protein shell (peak +8.9e-4 @ ~183 Å, inner
zero-cross ~125 Å, outer plateau ~240-250 Å) over a **NEGATIVE/disordered lumen** (r<125 Å, −3e-4→−8e-4);
a 2nd ordered layer would read positive — it doesn't → no refinable inner shell. **R_in≈120 Å(77px),
R_out≈250 Å(161px), map self-masked ~285 Å(184px)** @1.55. Weak sub-features (outer bump ~220 Å, interior
shoulder ~71 Å) <10% shell → revisit two-layer split ONLY if they sharpen post-Step3/5. **Consensus mask =
THRESHOLD-ONLY** (lumen negative → no radial carve); doc's old `--ini_threshold 0.15` was an ABSOLUTE-value
bug (map peak ~9e-4 → selects nothing) → use **~0.0005**, verify enclosed frac ~0.1-0.2.
(2) RELION **5.0.1**-commit-b16a35 → Step-6 capsomer focus **ROUTED THROUGH M** (5.0.1 subtomo
shifted-recenter isn't a clean one-job flow).
(3) M **PRESENT**: MTools/MCore/WarpTools in Warp image `/opt/conda/envs/warp/bin` (bare `M`=GUI only).
**Refine pass-1 = BIN 1** box600@1.55 (existing extraction; Class3D proved it; ref+mask already 600/1.55 →
NO rescale), I1, init-lowpass 30 Å, global auto-refine from 3.7°, mask_shell, mask-diam 575.
**NEXT (open):** `mask_shell.mrc` **BUILT ✓ 2026-07-02** (avg 0.0871, min0/max1 = clean hollow shell;
0.07–0.09 is the RIGHT band for a hollow capsid shell, NOT 0.10–0.20 which is a solid blob — geometry:
hard shell [120..250 Å]≈0.072 +soft→0.087; green-lit). Now: **Refine3D HEADLESS CLI, NO GUI** — script written at `sta_refine/run_refine3d.sh` (sbatch, partition g,
gpu:1, 64G, 24h). **KEY tomo flag = `--ios <optimisation_set.star>`** (NOT --i) — mined from the REAL working
command in `External/job003/run.out` (the one that made the 16.3 Å map). Command = known-good Class3D invocation
minus `--K/--iter/--firstiter_cc`, plus `--auto_refine --split_random_halves --low_resol_join_halves 40` +
`--solvent_mask mask_shell.mrc --solvent_correct_fsc`; I1, ref=job003 class001, --ini_high 40, --healpix_order 2
--auto_local_healpix_order 4, --particle_diameter 550, --pad 2 --trust_ref_size --zero_mask --preread_images --gpu.
**MPI IS MANDATORY (settled the hard way 2026-07-02 — I flip-flopped twice):** serial `relion_refine` ABORTS in
~1min with `ERROR: Cannot split data into random halves without using MPI!`. Gold-standard auto-refine REQUIRES
`relion_refine_mpi` with ≥3 ranks (1 leader + 1/half-set). Class3D ran serial ONLY because classification never
splits half-sets. Script uses `mpirun -np 3 relion_refine_mpi` launched INSIDE the container
(`apptainer exec … bash -lc "mpirun -np 3 relion_refine_mpi …"`), `#SBATCH --ntasks=3 --cpus-per-task=4`, 3 ranks
share the 1 GPU. Fallback if image has no mpirun: `srun --mpi=pmix apptainer exec … relion_refine_mpi …`.
(Container mpirun CONFIRMED present 2026-07-02; MPI 3-rank setup came up fine.)
**OOM WALL (2026-07-02, attempt 2 died in ~1min):** MPI worked but `slurmstepd oom_kill` — 2 followers EACH build a
`--pad 2` box-600 reconstruction (~20 GB) → blew past `--mem 64G` (Class3D survived only bc single-process = 1 recon
at a time). RELION also warned box-600 too big for a SHARED GPU ("usable image-size 518 < 600"). → box 600 hits TWO
memory walls under MPI (host RAM + GPU). FIX = RELION's own advice: **1 follower/GPU** → script now `--gres=gpu:2
--mem=128G` (needs g nodes w/ ≥2 GPU + ≥128G — user checking `sinfo -p g -N -o "%N %G %m"`). **If g nodes are 1-GPU →
DO THE DOC'S ORIGINAL BIN-2 PLAN: re-extract 94 ptcls at bin2 (box384/crop224, 3.1 Å/px) → ~4× less mem, fits 1 GPU**
(the box-600/bin-1 "simplicity" override is what caused this). Reserve knobs: `--pad 2→1` (8× less recon RAM, minor
quality, recoverable via final pad-2 reconstruct), `--mem 192G`.
**ROOT-CAUSED via node map (`sinfo -p g`): the failed run landed on clip-g3 = RTX ~11GB VRAM (too small for box600).
CBE g-partition GPUs: g1=P100×8/174G, g2=V100×4/169G, g3=RTX×4/169G, g4=A100×4/355–506G. Class3D succeeded only
bc it happened to land on clip-g4-0 (A100).** FIX (user rejected A100-pin to avoid queue): script now `--gres=gpu:2 --constraint=g2|g4 --mem=128G` — NOT model-pinned,
runs on V100(g2) OR A100(g4)=16 nodes, drops ONLY the 11GB RTX (g3, too small for box600 regardless of --mem). Two
memories: host RAM (--mem, was the crash) vs GPU VRAM (--mem can't touch; RTX too small); `gpu:2`=1 worker/GPU (no
sharing) is what makes box600 fit the GPU. 128G fits g2's 169G total. Escape hatches if it still fights: `--pad 1`
(8× less recon RAM, recover via final pad-2 reconstruct) or **bin-2 re-extract (box384/crop224) = runs on ANY card incl RTX, shortest queue, the doc's original plan**.
**RESOURCE FIX WORKED 2026-07-02: both submits landed on A100 (g4-3, g4-8) — gpu:2+g2|g4 is good.** BUT user
double-submitted → BOTH jobs wrote same `--o Refine3D/run` → COLLISION: 96864 crashed `rwMRC error reading
run_it011_half2_unfil` (read the other's half-written file), 96661 kept limping but CORRUPTED. **TELL: resolution
REGRESSING/oscillating (40→31→29→then 32-36 bounce, never past 29, worse than 16.3 input) = cross-contamination
signature (clean auto-refine never goes backwards).** Nothing completed (no run_class001/model.star). Fix = scancel
dupes, `mv Refine3D Refine3D_collision_bak` (non-destructive), resubmit ONE, verify single job in squeue. **NEVER
submit 2 jobs to same --o.** OPEN QUESTION for the clean run: if it ALSO stalls ~30 Å (not past 16.3), that's the
94-ptcl 412 data-quality issue (picking history [[project_412_picking_quantification]]), not technical.

**CLEAN RUN RESULT (98819, solo on A100 g4-3, 2026-07-02→03): CAPPED at gold-standard 35.8 Å — CONFIRMED data-limited,
NOT technical.** Trajectory 40→38.75→35.77 then FLAT for 15 iters, "converged". Then OOM'd at the FINAL half-combine
(full-data recon = peak memory, exceeded 128G) → no run_class001.mrc (to complete: `--continue run_it017_optimiser.star`
+ `--pad 1` or mem192G). **KEY DIAGNOSIS: accuracy 1.17° (≈ Class3D's 1.00°) but gold-std res 35.8 vs Class3D's 16.3 →
the 16.3 was OPTIMISTIC/OVERFIT (Class3D EstRes = single-map SSNR, counts overfit noise; Refine3D = independent-halves
FSC = honest).** FSC ~1.0 to 38Å then CLIFF to 0.03@34Å, dead past 22Å. Low gold-std FSC **rules OUT wrong-I1 artifact**
(wrong sym → halves spuriously AGREE not disagree). → these 94 ptcls have reproducible signal only to ~35Å. Refinement
tweaks (Polish/CtfRefine/M) CANNOT help — they fit geometry on signal that isn't reproducibly there. **REAL levers =
the particle set: (a) picking noise (~8× FP [[project_412_picking_quantification]]); (b) CROSS-DATASET AGGREGATION
inconsistency — 94 ptcls from ~10 datasets/57 tomos; if handedness/apix not harmonized ([[project_412_tomohand_discrepancy]])
the mix caps res regardless — SPECIFICALLY FIXABLE.** User (2026-07-03) REFUSES to accept 35Å as floor — "something wrong in data OR our setup; the approach Claude came up
with was never verified." → **VERIFICATION ROADMAP opened = doc §12** (job-by-job ledger + exact commands + ordered tests).
**METADATA AUDIT DONE (§12.0): "datasets differ" RULED OUT** — all 125 tomos in merged tomograms.star uniform: TomoHand=−1,
origApix=tiltApix=1.55, 300kV/Cs2.7; particles.star = single optics group. So NOT mixed handedness/apix. → suspicion now on
(A) CLAUDE'S UNVERIFIED REFINE SETUP [§12.1 flags the 5 suspect params: `--low_resol_join_halves 40`, `--sym I1` (ref never
verified on-axis), `--solvent_mask`+`--solvent_correct_fsc` (mask never overlaid on density), `--ini_high 40` (FSC cliff sits
right at ~38-40Å=the low-pass!), dropped `--firstiter_cc`], (B) particles/reference quality. **ORDERED TESTS (§12.2): Tier0
eyeball ref+mask+map in ChimeraX; Tier1 one-var diagnostic refines (1a maskless, 1b drop join+ini_high25, 1c firstiter_cc,
1d relion_align_symmetry I1-on-axis); Tier2 CTF sanity + single-source subset refine + compare the PI try1.** Complete-map cmd
(dodge OOM) = `--continue run_it017_optimiser.star --pad 1`. Ledger table in §12.4. NOT a dead end until Tier0-2 clean. → Reconstruct(I1, box384/crop224) → PostProcess(mask_shell) = first
gold-std FSC vs 16.3 Å. User pastes `Final resolution` from `sta_refine/refine3d-*.out`.
**SLURM WALLTIME GOTCHA (2026-07-02): --time=24:00:00 REJECTED with `QOSMaxWallDurationPerJobLimit`; Class3D's
8h is proven under the cap → scripts pinned to `08:00:00`.** Auto-refine is CONTINUABLE, so 8h is a floor not a
risk: `sta_refine/continue_refine3d.sh` (written) resumes via `relion_refine --continue <newest run_it*_optimiser.star>`
if it walls before converging. My sandbox has NO SLURM client (sbatch/scontrol/sacctmgr absent) → walltime/QOS
checks are user-run.

**MASK LEVER — first fixable cause found (2026-07-06, PI-steered).** The PI flagged our solvent mask
`sta_refine/mask_shell.mrc` as too thick inside (inner scoop radius too small → masks-in the disordered
lumen boundary + inner wall) and built a better one at `412_aggregation/maskgen/MaskCreate/job001/mask.mrc`.
Verified by reading MRC data directly (no numpy: `od`+`dd` line/central-slice azimuthal radial profile).
The class map `External/job003/run_it015_class001.mrc` is a **thick ~140 Å bilobed wall** — inner sublayer
peak 180 Å, outer sublayer peak 223 Å, valley ~205 Å, NEGATIVE lumen < 124 Å. **Ours** (relion_mask_create
only, lowpass18/thr0.0005, NO carve) enclosed 0.087, R_in 127 Å, captures **99.3%** of positive protein
density = the WHOLE wall (both sublayers); its soft inner edge even dips into the sign-flipped lumen.
**His** (EMAN2 `e2proc3d --process=mask.sharp:inner_radius=136` [=211 Å] carve THEN relion_mask_create
lowpass25/thr0.000106) enclosed 0.057, R_in 203 Å, captures **45%** = outer sublayer only. Same outer edge
(~265 Å); difference entirely inside. = the playbook's outer-shell FOCUSED mask / the PI's "don't confuse the
alignment" — isolates the one rigid outer shell, excludes variable inner sublayer + lumen. Recorded in
`sta_refine/mask_fix.md`. RERUN = single-variable A/B: `sta_refine/run_refine3d_<pi>mask.sh` (written; ==
run_refine3d.sh but --solvent_mask=his mask, --o Refine3D_<pi>mask, all else fixed) → continue --pad1 combine
→ masked FSC vs our 23.85. Both masks box600/apix1.55 = drop-in. Tiny cosmetic artifact: his thr picks up a
~2vox FFT-origin blob (harmless). This REOPENS Step-0's tentative "one-shell" call — it's two-sublayer.

Related: [[project_412_divergence_cascade]] [[project_412_tomohand_discrepancy]]
[[project_412_picking_quantification]] [[project_picks_filter_aggregation]]
[[project_i1_angle_list_underperforms]] [[reference_412_dataset_locations]] [[reference_hpc_env]]

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

docs/reports/412-capsid/412-sta-refinement-playbook.md: job-by-job RELION-5/M refinement of 94 aggregated 412 ptcls (measure→mask→Refine3D→geometry loop→sym-expand→focused capsomer→M). EXTERNAL to crboost (user 2026-07-02: don't add job-types, run from Apptainer containers, ask for spare builds); NOT module load — rel() alias §1; EMAN2 absent→numpy carve; M-in-Warp UNVERIFIED. Sandbox has no python/apptainer but restart DID bind-mount PROJ.

STEP 1 CUT DONE 2026-07-02: /groups/klumpe/crboost_data/412_aggregation/sta_refine/{optimisation_set,particles}.star — 94 class-1 rows verified, headers byte-identical, apix1.55/box600/bin1 confirmed, 94 ptcls span 57 tilt-series (~1.6 each) all present in tomograms.star → per-series geometry fits (CtfRefine/Polish/M) will be thin.

3 GATHER ITEMS DECIDED 2026-07-02: radial profile → ONE-SHELL (positive protein shell over NEGATIVE/disordered lumen; R_in≈120 Å/R_out≈250 Å; consensus mask=threshold-only ~0.0005, NOT the doc's old absolute-value 0.15 bug); RELION 5.0.1 → Step-6 capsomer focus via M; M present (MTools/MCore in Warp img). Refine pass-1=BIN1 box600@1.55 I1 lowpass30 global-3.7°. mask_shell BUILT (avg0.087=correct hollow shell; 0.07-0.09 is the right band NOT 0.10-0.20=solid blob).

Refine3D HEADLESS CLI no-GUI = sta_refine/run_refine3d.sh (sbatch). KEY flag --ios <optset> (NOT --i), mined from job003/run.out's real working cmd; = Class3D cmd −K/−iter/−firstiter_cc +auto_refine/split_random_halves/solvent_mask/solvent_correct_fsc, ini_high40.

MPI MANDATORY: serial relion_refine ABORTS "Cannot split data into random halves without using MPI" → use mpirun -np 3 relion_refine_mpi inside container, --ntasks=3, 3 ranks share 1 GPU (Class3D ran serial only bc classification doesn't split halves). Fallback: srun --mpi=pmix. mpirun CONFIRMED in container.

OOM WALL: 2 followers each build pad-2 box-600 recon (~20GB)→exceeded --mem 64G (Class3D survived bc single-proc); box-600 also too big for SHARED GPU. ROOT CAUSE (sinfo -p g): failed run landed on clip-g3=RTX ~11GB VRAM (too small for box600); Class3D worked only bc it hit clip-g4-0=A100. CBE g-GPUs: g1=P100/g2=V100/g3=RTX/g4=A100(40-80GB,355-506G RAM). FIX (user rejected A100-pin re queue): script now --gres=gpu:2 --constraint=g2|g4 --mem=128G = V100 or A100 (16 nodes), drops only 11GB RTX. gpu:2=no-share fits box600 on GPU; --mem fixes host OOM. Reserve: --pad 2→1; bin-2 re-extract(box384/crop224)=runs on ANY card, shortest queue.

RESOURCE FIX WORKED (both submits hit A100 g4) BUT user double-submitted → 2 jobs same --o = COLLISION (96864 crashed rwMRC-reading the other's half-written file; 96661 corrupted: resolution REGRESSED 40→29→bounce 32-36, never past 29 = cross-contamination tell). Fix: scancel dupes, mv Refine3D Refine3D_collision_bak, resubmit ONE. NEVER 2 jobs→same --o.

CLEAN RUN (98819 solo A100) CAPPED gold-std 35.8Å = DATA-LIMITED not technical: accuracy 1.17°≈Class3D's 1.00° but res 35.8 vs 16.3 → 16.3 was OVERFIT (Class3D=single-map SSNR; Refine3D=honest independent-halves FSC, cliff 0.97→0.03 at 35Å). Low gold-FSC RULES OUT wrong-I1. Refinement tweaks can't help (no reproducible signal past 35Å). REAL levers=particle set: picking noise + cross-dataset aggregation handedness/apix inconsistency (94 ptcls/~10 datasets, see project_412_tomohand_discrepancy, fixable). OOM'd at final combine→no final map (--continue+--pad1 to complete).

User REFUSES 35Å as floor (2026-07-03) → VERIFICATION ROADMAP = doc §12 (handoff + exact commands + ordered tests + ledger). METADATA AUDIT DONE: "datasets differ" RULED OUT (all 125 tomos uniform TomoHand=−1/apix1.55/300kV). Suspect now = the UNVERIFIED refine setup (§12.1 flags 5 params: low_resol_join_halves40, sym-I1-not-verified-on-axis, solvent_mask+solvent_correct_fsc, ini_high40=FSC-cliff-sits-here, dropped firstiter_cc) OR particle/ref quality. TESTS §12.2: Tier0 ChimeraX eyeball ref/mask/map; Tier1 one-var diagnostic refines (maskless / drop-join+ini_high25 / firstiter_cc / relion_align_symmetry I1-on-axis); Tier2 CTF+single-source-subset+the PI-try1-compare. Complete-map=--continue run_it017_optimiser.star --pad1. → Reconstruct(384/224) → PostProcess = first gold-std FSC vs 16.3 Å.
