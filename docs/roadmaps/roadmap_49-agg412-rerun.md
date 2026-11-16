# Roadmap 49 — AGG412 rerun: the eight projects again, lean, from import to picks

**Status:** scoped 2026-11-10 on the maintainer's direction. Stage R1 (the first project) started the same day
through the Agent API on the test server (`/groups/klumpe/_crboost_data_api`). Run log in §7.

**In one line.** Process every AGG412 dataset again from the movies to picked 412 particles and ribosomes, along the
shortest chain that gets there, on CBE.next with today's settings (pytom 0.14, the B200 resource tuning of roadmaps
43/45/47, H7's angle lists); compare each rerun with its old project on speed, disk and results; then delete the old
project. One project at a time, because the old ones are what we compare against and both don't fit.

## 1. Why, and why in stages

The eight `AGG412_*` projects in `/groups/klumpe/crboost_data` (made 2026-10-02, roadmap 29) grew by trial: a
baseline chain, then missAlign and a second CTF and reconstruction, TM runs on both chains, segmentations,
denoising, extraction and refinement here and there. They hold **15.0 TB** (`.disk_usage.json`, 2026-11-05/06):

| Stage | TB | |
|---|---|---|
| Reconstruct | 5.81 | two full sets per project, each with even/odd half-tomograms (⅔ of it) |
| Template matching | 4.26 | float32 scores + angles maps, 2.15 GB each per tomogram; mostly 412 only |
| Motion & CTF | 3.68 | frame averages + even/odd half averages (the halves ⅔ of it) |
| Segment, denoise | 0.77 | |
| Alignment, CTF, missAlign, export | 0.45 | |

Quota on 2026-11-10: the user holds 25.1 of 32.8 TB on `/groups` (7.7 TB free); the filesystem 25 TB free, shared.
A lean rerun of all eight is ~10.5 TB (§4), so the old projects have to go first, but they are also the reference
for what changed. Hence: one project rerun, compared, its old project deleted, the next.

## 2. The chain

Import → Motion & CTF → tsImport → Tilt filter → Alignment (AreTomo) → missAlign → TS CTF → Reconstruct →
Template matching (412) + Template matching (ribosome) → Pick candidates (each).

What the old projects did that the rerun does not:

- **No baseline reconstruction.** missAlign reads the alignment job's star, Warp folder and settings and rebuilds
  its stacks from the frame averages; nothing reads the pre-missAlign tomograms but the A/B of roadmap 29 §6,
  which is settled (MA9).
- **No half-sets.** `out_average_halves` off in Motion & CTF, `halfmap_frames` 0 in Reconstruct (MA15's reasoning:
  the halves feed only denoising). Denoising later stays possible with `halfmap_tilts` on a reconstruction rerun,
  which needs no frame halves.
- **No segmentation, no denoising, no second TM arm.**
- Warp Export, Reconstruct Particle, Class 3D, Refine 3D: not in this roadmap (D5).

## 3. Settings

Parameters as the old project's jobs, which are roadmap 29 §6.2.3 / §6.3.3 everywhere; SLURM resources are today's
profile defaults (roadmap 47: `constraint: b200` on the Warp stages, the CPU steps on GPU nodes), not the old
projects' overrides, which name legacy-cluster QOS and constraints.

| Job | From the old project | Changed on purpose |
|---|---|---|
| Import | as-is | |
| Motion & CTF | m 500:10, bfac −500, grid 1x1x3; c 30:10, 1.1:8 µm, grid 2x2x1, window 512 | `out_average_halves` false |
| tsImport | tomo dimensions 4096x4096x2048 | |
| Tilt filter | DL, `tiltnet_resnet18_o1034`, threshold per project (0.301 or 0.5, roadmap 29 §6.3.2) | `dl_auto` instead of `dl_review` (D2) |
| Alignment | AreTomo, 6.2 Å, sample 180 nm | |
| missAlign | PAPER preset (1000 steps × 30 epochs, `5,15`, `z_box` auto), seed 45132, `num_gpus` 2, 16 CPUs, 64G | QOS `g_medium`, `2-00:00:00` (the estimate asks 5½ d with its ~5× long constants, roadmap 29 §6.2.6; a timed-out run resumes) |
| TS CTF | 512, 30:10, `set_flip`, 1.1:8 | input = missAlign |
| Reconstruct | 6.2 Å, deconv 0 | `halfmap_frames` 0 |
| TM 412 | `412_particle` (catalog v1): box-128 black template, sphere d575 s5 mask, I1, no band-pass | the angle list after H7 (D3); pytom 0.14 |
| TM ribosome | `dm_ovary_ribosome` (catalog v1): 6XU8 box-96 black, sphere d350 s5, C1, 12°, no band-pass (Grid2 job026) | new for this project |
| Pick candidates | FP = 1, max 1500, diameter from the species | |

Acquisition values are the old project's (mdoc where it has them; Cs 2.7, AC 0.1, defocus hand inverted, tilt
angles not inverted, no gain reference, the EER grouping as the old project ran it).

## 4. Disk

Per tomogram: frame averages ~1.4 GB (41 × 33.5 MB), tomogram 1.1 GB, TM 4.3 GB per species. Lean, all eight:
averages ~1.3 TB, alignment/missAlign/CTF ~0.35 TB, one reconstruction ~1.0 TB, TM both species ~7.5 TB, picks
negligible: **~10.5 TB**, of which TM is 70 %. One project of ~150 series: ~1.7 TB.

## 5. Comparison, per project

Read from the exit markers, task files, `run.out`s and one accounting query per project (the old ones ran on the
legacy cluster; their times come from file times and `task_N.out`):

1. **Speed**: wall time Run → picks; per stage, the wait before its first task and its length; GPU-h.
2. **Disk**: per stage, old vs new (`.disk_usage.json`).
3. **Preprocessing**: tilts kept by the filter; alignment failures; tsCtf fit resolution per series; missAlign's
   per-tilt change (`missalign_changes.json`); series lost at any stage.
4. **412**: picks per tomogram at FP = 1, top-peak σ, overlap of the new picks with the old ones (same tomogram
   names, within the particle radius), the PI's manual picks where the project has them.
5. **Ribosome**: the same read-outs against Grid2 job026's distributions (roadmap 29 §6.3.3): no AGG412 project
   still holds a ribosome TM.
6. A verdict line, then the old project is deleted (by the maintainer, from the roster; Trash cleared).

## 6. Decisions

| # | Decision | Why | Who, when |
|---|---|---|---|
| D1 | First project: `AGG412_20251113_412` (151 series, 2.53 TB) | the bulkiest record to compare with: 412 TM on the legacy cluster (job016) and on CBE.next with pytom 0.10 (job019, roadmap 43/45's reference), extraction and Refine 3D (job024) | proposed 2026-11-10 |
| D2 | Tilt filter `dl_auto` with the old project's model and threshold | the old ones ran `dl_review` and committed the model's verdict without a hand label (20251113: 5195 kept, 792 dropped); `dl_auto` gives the same verdict without holding the run. Slot6, unmilled2, SK carry hand labels (roadmap 29 §6.3.2): there the run waits for review | proposed 2026-11-10 |
| D3 | The 412 at 5° (2,124 rotations), the I-group default since H7 | roadmap 45 D9: 5° samples at least as finely as the old "12°" lists (3,398 rotations, the H7 bug); 12° would now be ~225. Scores are not comparable one to one with the old runs' | maintainer, 2026-11-09 (D9) |
| D4 | Projects without hand labels first, one at a time, old project deleted after its comparison | quota (§1) | maintainer, 2026-11-10 |
| D5 | Stop at Pick candidates | the request: "from import to picked 412 particles and ribosomes" | maintainer, 2026-11-10 |
| D6 | Resources from today's profiles, no hand overrides except where a profile is known short (missAlign time) | the old overrides name the legacy cluster | proposed 2026-11-10 |

## 7. Run log (append; newest last)

- **2026-11-10** — Roadmap scoped. Test server started from the `agent_api` worktree (port 24302, base
  `/groups/klumpe/_crboost_data_api`).
- **2026-11-10** — R1: `AGG412_20251113_412_rerun` made through the API: 151 series, the old project's values
  (dose 2.9 against the mdoc's 2.88, axis 84.4, EER 32 fractions as the old job002 ran `--eer_ngroups -32`).
  Roster: import, motion & CTF (halves off), tsImport, tilt filter (`dl_auto`, 0.301), AreTomo, missAlign (PAPER),
  tsCtf, tsReconstruct (`halfmap_frames` 0), TM + candidates for `412_particle` (5°, I1) and `dm_ovary_ribosome`
  (12°, C1); species from catalog v1. `plan_run`: 12 jobs, no refusal, no need. Found HOTFIXES H13 (the API's
  preset choice leaves the preset's fields) and set the four PAPER fields by hand. Differences from the old run
  besides §2: Warp 2.0.0dev41 + AreTomo2 (old: dev36 + AreTomo 1) on B200s, pytom 0.14 with lanes and whole
  tomograms, throttles 40.
- **2026-11-10** — R1 submitted (maintainer's go, `dl_auto`): SLURM 1524926 (motion) … 1524936, one afterok
  chain: motion 1524926, tsImport 27, tilt filter 28, alignment 29, missAlign 30, tsCtf 31, tsReconstruct 32,
  TM 412 33, TM ribosome 34, candidates 35 / 36.
- **2026-11-10 ~19:00** — The tilt filter failed at start: `filterTilts/tiltnet_resnet18_o1034.pth` (git-ignored) was
  missing from the re-made `agent_api` worktree; its dependents were cancelled unstarted. Motion & CTF 150/151:
  Position_11_4 tilt 029 (+52°) died in Warp dev41's per-movie CTF fit (`System.OverflowException ... Decimal`,
  `Movie.ProcessCTF.cs:427`); dev36 processed it in the old project. The maintainer linked the weights from the main
  checkout and pressed Run: tilt filter through, alignment 1526890 on, missAlign 1526891 … candidates 1526897.
  Motion was not resubmitted, so the rerun carries 150 series.
- **2026-11-10 ~21:00** — missAlign 1526891 landed on two Quadro RTX 6000s (`cbe-g3-1`): the profile cannot name a
  card (HOTFIXES H14) and every B200 and A100 was taken. 2.29 steps/s with GPU 0 at 100 % → 27–31 h for the nine
  iterations, inside its 2-day limit; left to run (roadmap 50 D1). Its training pool streams through BeeGFS (H15).
  Speed levers and the arithmetic: roadmap 50; R2 runs on its S1 profile (two B200s, 48 CPUs, pool in RAM), and D4
  there asks whether R2–R8 `infer` from R1's models instead of training.
- **2026-11-13** — R1's first TM + candidates (job009–012, all 139/139) and a second ribosome candidates run
  (job013, stopped) removed by hand to Trash: candidate extraction then deleted the TM angles maps (fixed in
  a0abec9), so TM runs again.
- **2026-11-14** — `made_by_api` had been dropped on 2026-11-10 18:47 when an older main server (schema < 3.13)
  saved the project (the newer file kept as `project_params.json.schema-3.14.bak`); restored by hand with the
  server stopped (pre-edit copy `project_params.json.before-made-by-api.bak`). Through the API: TM 412 (5°, I1),
  TM ribosome (12°, C1, 6XU8 black, d350 mask), candidates for each (FP 1, max 1500, Ø 575 / 310); `plan_run`
  clean. Submitted: TM 412 1673848, TM ribosome 1673849, candidates 1673850 (afterok 48) / 1673851 (afterok 49).
- **2026-11-15** — R1 through to picks (TM 1673848/49 and candidates 1673850/51 ran 14:01–16:23 on 2026-11-14).
  §5 comparison against `AGG412_20251113_412`: old job020 (TM job019: CBE.next, pytom 0.10, whole tomograms,
  3,398 rotations) is the like-for-like 412 reference; old job017 (TM job016, legacy cluster, `-s 2 2 1`) the
  older one. Scripts and numbers: session scratchpad `compare.py`, `movies.py`, `bysigma.py`, `timing.py`.
  - **Preprocessing: same.** 139 vs 141 tomograms. Lost: Position_11_4 (Warp dev41 CTF overflow, above) and
    Position_20_2 (AreTomo2 exit 139 at alignment; AreTomo 1 aligned it, but missAlign then moved it by 109 Å
    RMS, the worst series in the old run). The other 10 alignment failures are the same series in both. Per
    movie (5,946 paired): CTF fit 7.7 Å median on both, 4,296 equal, 802 better / 848 worse; motion 2.46 vs 2.48 Å.
    Tilts kept: 127/139 series identical. missAlign residual RMS 11.26 vs 11.48 Å median (80 lower / 59 higher).
  - **412: the same particles.** 2,932 candidates (139 tomos) vs 2,933 (job020) vs 4,336 (legacy job017); per-tomo
    counts track job020 (Spearman 0.90, 60 up / 63 down). Agreement with job020 rises with score: 38 % of picks
    < 7.25σ, 56 % at 7.25–7.5σ, 80 % at 7.5–8σ, 94 % at 8–9σ, 95 % ≥ 9σ. The near-cutoff picks are noise either
    way; above 8σ both runs find the same 412s. Peak heights slightly lower (top peak per tomo 7.88 vs 7.96σ median,
    49 up / 72 down), expected from 2,124 vs 3,398 rotations (D3). The legacy run's σ is inflated by its split
    (top peaks 8.75σ): 95 of its 105 picks ≥ 10σ are in the rerun, at 8.8σ median. Roadmap 29's "real VLPs at
    10.4–16.8σ" is a split-tomogram scale; on whole tomograms the same particles sit around 8.5–9σ.
  - **Ribosome (new; no old ribosome TM to compare):** 49,553 candidates, 311 per tomo median, none at the 1,500 cap;
    31,268 ≥ 8σ in 130 tomos (225 per tomo), top peak 13.5σ median. In range of roadmap 29's complete projects
    (unmilled2 258 ≥ 8σ per tomo, Grid2 235).
  - **Speed** (CBE.next, rerun): motion 20 min, alignment 18 min, tsCtf 15 min, reconstruct 17 min, missAlign
    14.6 h (4 × RTX 6000; old 25 h on the legacy cluster), TM + picks 2 h 22 min. TM 412: 3.6 GPU-h, 37 min (24
    lane tasks on B200) vs job019's 6.9 GPU-h, 20 min (141 single-tomogram tasks): half the GPU-h, 16 % less
    per rotation. TM ribosome 21.4 GPU-h, 78 min: 18 of 24 tasks started on A100-40GB (~66 min each vs ~18 on a
    B200) because the 412 held the B200s. Ribosome candidates 59 min, 20.9 GPU-h on RTX 6000s (412: 5.5 min): the
    biggest GPU cost after missAlign.
  - **Disk:** 1.61 TB vs 2.53 TB. Motion 205 vs 609 GB (no halves), one reconstruction 150 GB vs 459 GB each (no
    halves), TM 599 GB per species (scores + angles; old job016 608 GB, job019 304 GB after the angles deletion).
    TM is 74 % of the rerun.
  - **Verdict:** no regression in picks or preprocessing quality; −2 series; disk −37 %, 412 TM GPU-h −48 %.
    The old project's only extras are those two series and the 412 STA (job021–024, Refine 3D at 41.8 Å).
- **2026-11-15** — R2–R8 at once, on the maintainer's go (R1 showed no regression; the per-project comparisons give way
  to an archive of each old project's small records: `/groups/klumpe/_crboost_archive/agg412_old_records/`,
  project_params, registry with hand tilt labels, stars/json/xml/logs, templates, STA maps, ~3 GB; to be deleted once
  the 412 and the ribosome are averaged on the reruns). Seven projects made through the API in
  `/groups/klumpe/_crboost_data_api/<old name>_rerun`, values as the old projects (dose 3.0 / 2.98 / 4.8 / 2.9,
  axis 84.4 / 84.36, EER 32), roster as R1 to Reconstruct (halves off, missAlign paper with 4 GPUs / 2 training on
  the profile's cards, `halfmap_frames` 0). Tilt filter: `dl_auto` 0.301 for FCIsolation, pilot, Grid3, Grid2;
  `dl_review` 0.5 for Slot6, unmilled2, SK (they wait for the maintainer's review). Submitted SLURM 1686722–1686755.
  TM + candidates held back: quota 28.4 of 32.8 TB, the seven reruns need ~8 TB (~6 TB of it TM) — they go on the
  roster once the old projects are deleted.
- **2026-11-15 ~17:30** — The seven sat 20+ min with every supervisor PENDING (Priority): the supervisor ticket
  (`supervisor_slurm.constraint: rtx6000|a100`) had nowhere to go, every RTX 6000 and A100 allocated or planned for a
  higher-priority A100 job, while 112 B200 MIG slices idled. `conf.yaml`: the ticket and the six ticket-only job
  profiles (tsImport, tmextractcand, filamentPick, subtomoExtraction, extractPickList, reconstructParticle) take
  `rtx6000|a100|mig` (B200 1g.23gb slices, ~3.6 billing an hour; whole B200s stay out). Server restarted, the seven
  stopped (nothing had started) and run again with tsImport set per job to the same constraint: SLURM
  1686782–1686815. All seven motion supervisors started within a minute, on cbe-g5-2 slices.
- **2026-11-15 ~19:30** — Alignment losses vs the old projects: Slot6 81 vs 123, Grid3 60 vs 95, unmilled2 154 vs
  164, SK 132 vs 133, R1 139 vs 141; FCIsolation 142 = 142, pilot 23 = 23, Grid2 49 = 49. Two causes.
  (1) Known bug #3 moved: AreTomo2 (dev41 container) mangles the IMOD `.tlt` at 44 characters (AreTomo 1.0 at 45),
  and the alias only took names over 44 — every 44-character name failed (`Position_<1–9>_<k>` under the
  41-character `…Grid3_rerun` / `…Slot6_rerun` prefixes: Grid3 36/36, Slot6 42/42), every other length aligned.
  `ARETOMO_TS_NAME_MAX` 44 → 43 (known_bugs #3 updated). (2) AreTomo2 segfaults (exit 139) on some series even under
  the alias: unmilled2 22 (12 of them aligned by AreTomo 1.0 in the old project), SK 5, Slot6 ~3, R1 1; the crashed
  unmilled2 series kept a median ~21 tilts after the filter against ~38 for the aligned ones. Not ours (no tool
  patches). On the maintainer's word everything after tsCtf was cancelled in the seven (Reconstruct; Slot6's TM and
  candidates); the TM + candidates jobs added to the other six were never submitted.
- **2026-11-15 ~20:30** — The AreTomo2 crash (exit 139) is the tilt filter's holes: across 752 series of six
  reruns, among those with ≥ 20 tilts left, 0/546 crashed with no tilt missing at the 3° step (largest gap ≤ 4°),
  1/57 with one missing, 15/64 (23 %) with 2–3 in a row missing, 22/25 (88 %) with ≥ 4 in a row; all 4 series
  with no tilt within 6° of 0 crashed. ≤ 12-tilt series die at AreTomo2's start. AreTomo 1.0 survived the same
  holes (12 unmilled2 series). unmilled2 stopped for the maintainer to look at its filter verdict (1378 dropped at
  0.5 vs 1011 in the old project). Slot6 and Grid3: missAlign/tsCtf cancelled, `aligntiltsWarp__2` (clone, now
  under the 43-character alias limit) wired into missAlign → tsCtf; not run yet — the submitted-then-cancelled
  Reconstruct (both) and TM/candidates (Slot6) must leave the roster first (UI delete), or Run resubmits them.
- **2026-11-15 ~21:00** — After the limit fix (committed by the maintainer, server restarted), Slot6, Grid3 and
  unmilled2 re-aligned from scratch (SLURM 1688656/59/62, 4–7 min each): Slot6 123 = old 123, Grid3 96 vs old 95
  (+Position_2_2), unmilled2 154 vs 164 (the same 12 AreTomo2 crashes on filter holes, same 0.301 verdict as
  before: 1378 dropped). missAlign → tsCtf chained (1688657–58, 1688660–61, 1688663–64); nothing after tsCtf.
- **2026-11-16 ~10:00** — unmilled2 job008 (154/176): all 22 drops are AreTomo2 exit 139 on what the 0.301 verdict
  left. 5 had no tilt left at all (Position_5_6, 8, 18_2, 22_3, 32_3) and were still handed to AreTomo: they should
  be dropped before alignment with that reason. 16 had a gap of 18–84°, one 12°. Pooled over five reruns (783
  series), crash rate by largest gap: ≤ 3.5° 8/559 (5 = the empty ones), 3.5–9.5° 7/127, 9.5–15.5° 15/33, > 15.5°
  32/33. The holes are mostly the model being unsure, not confident: unmilled2 has 11.1 % of tilts at 0.301 < p ≤
  0.9 (FCIsolation 7.5 %, R1 3.3 %, the rest ≤ 2.8 %) and those two lose the most series (22, 21). Off-line estimate
  for unmilled2: threshold 0.9 alone ~160 aligned; 0.9 with the verdict trimmed to the contiguous run around 0°
  (cut at the first gap > 6.5°) ~165, the old project 164. Trimming is not in crboost (proposed: the filter's
  verdict trims instead of punching holes, and a series left with too few tilts is dropped before alignment with
  that reason). unmilled2's missAlign/tsCtf (1688663–64) were cancelled before starting (2026-11-15 20:09);
  unmilled2 left alone. The other seven queued to Warp Export with R1's settings (missAlign paper/4 GPUs/2
  training, halfmap_frames 0; Warp Export 412 binning 2 box 256 as in the old projects, ribosome binning 2 box 200,
  new): Slot6 1695140–46, Grid3 1695150–56, FCIsolation 1695157–65 and SK 1695166–74 (both from missAlign: their
  rosters had lost everything after the overnight re-alignments 1689586/1689591), Grid2 1695175–76, pilot
  1695177–78, R1 1695179–80 (export only).
- **2026-11-16 ~11:00** — Keep rule built (known_bugs #4, maintainer's choice of the proposed variant): a commit
  keeps a bad-called tilt where excluding it leaves a gap > 6.5° or under 15 tilts; blank exposures and manual
  "bad" labels are never put back, and a gap they hold open cuts the series there. Off-line on the live
  registries: unmilled2 at 0.9 → 121 tilts kept despite the filter, 15 cut, largest gap left 6°, no empty
  series (rule off: 604 excluded; on: 498); FCIsolation at 0.9 → 155 kept, 16 cut. Preview (open review) and
  commit agree tilt for tilt. Not run on the cluster yet: needs the server restarted. Then unmilled2 and
  FCIsolation (21 series dropped; the maintainer wants it redone from the filter too, its missAlign chain
  1695157–65 was cancelled before starting) re-run from the filter at 0.9 with the rule.
