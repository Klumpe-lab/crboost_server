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
