# Roadmap 47 — The whole pipeline on what the cluster has: every card, nothing waits on `c`

**Status:** scoped 2026-11-10 on the maintainer's direction; S1 built the same day (§7), not yet run on a pipeline.
Next: S3, the balance pilot. Roadmaps 45 and 46 began from the new B200s and
over-corrected for them: card and slice comparisons, MPS lanes, chunk sizes, each worth minutes on a stage that lasts
minutes, while the pipeline's wall time is set elsewhere, by its CPU jobs queueing on partition `c` between every two
GPU stages, by `constraint: b200` keeping 63 idle cards out of reach, and by `%20` throttles. P13 (every card type,
roadmap 45 §11, `docs/reports/gpu-util/gpu-types-2026-11-10.md`) ran the same morning and settled the card question:
every card runs every stage; Warp's fits differ across GPU architectures on frames without signal, which the
maintainer weighed against 63 idle cards and accepted (D4): every GPU stage but reconstruct and template matching
takes any card, and the CPU steps ride the RTX 6000s and A100s as a ticket. S1 built the same day (§7).

**In one line.** A pipeline is as fast as its slowest queue, not its fastest card. Every GPU stage runs on whichever
card fits its memory and is free now; every CPU step rides a GPU node rather than queueing on `c`; the throttle is the
number of cards we are willing to hold, not 20; and the tuning stops where a stage is already minutes long.

**Not in this roadmap:** how a card performs (roadmap 45: measurements, levers, the meter), how jobs are tracked and
who submits them (roadmap 46: Part B removes the supervisors). This roadmap orders those by pipeline wall time, names
what stops, and carries the one pilot that checks the balance. No tool is patched, no kernel written.

## 1. Goal

**Use case.** The maintainer presses Run on a 412-sized project (160 tilt series) at a busy hour. tsImport and the
first stage start within minutes; every later stage's tasks start within a couple of minutes of the stage before
ending, on whichever cards are free; nothing waits for partition `c`; the pipeline reaches extraction in about an
hour of wall time at a billing within 1.5x of the anglestest pilot's 205.6 billing-h; the outputs are the same ones.

**Measures**, per pipeline, from the exit markers and one accounting query (no new code; `sacct_gpu_report.py`'s
window plus the markers' mtimes):

1. wait before the first task (Run to the first task's start);
2. the sum of the stage-boundary waits (the last task of stage N ending to the first task of stage N+1 starting);
3. wall time Run to extraction done;
4. billing-h;
5. outputs identical to the anglestest pilot's within its own spread (same five alignment failures, 158 of 158 from
   CTF on, candidates within the usual overlap).

## 2. What exists (checked 2026-11-10, 10:40–10:55)

- **Partition `g` has 319 GPUs, five kinds** (`scontrol show partition g`): 32 B200 (billing 24 an hour), 224 B200
  1g.23gb slices (3.43), 16 A100 SXM4 40 GB (6), 8 A100 3g.20gb (3), 39 Quadro RTX 6000 24 GB (2). CPUs bill 1 each,
  memory 0.134 per GB; a job bills the largest. At 10:45: 8 RTX 6000 nodes and 3 A100 nodes idle, the B200 nodes
  mixed, 4 jobs pending on `g` against 132 running. The RTX and A100 nodes have 4 cards each (the 3g node 8), so
  roadmap 45's crowding (5x slower motion above about 7 tasks on one node) cannot happen there.
- **Partition `c` is the stall.** 12 nodes; at 10:45 eleven were drained or draining (a CDU issue in rack C-4 and two
  kill-task failures), 34 jobs running and 550 pending. `sbatch --test-only` for a supervisor-shaped job (1 CPU, 4 GB,
  4 h) said 2026-11-12 03:43 at 10:43 and "now" at 10:50 after one node returned: the estimate swings, the shape of
  the problem does not. Roadmap 45 §11 has the steady form: on 2026-11-09, with `c` merely full, the anglestest
  pilot's first supervisor waited 26 minutes and P11 three hours, while `g` had five idle nodes. Partition `m` is no
  escape: it requires 200 GB a job and had 4 of 6 nodes drained.
- **`g` refuses a job without a GPU**, also as `-p c,g` (dry run: "Partition g requires 1 or more GPUs"). So the
  ticket onto `g` for CPU work is a GPU, and the cheapest ticket is an RTX 6000 at 2 an hour (a 4 GB, 1-CPU job bills
  2 there; a slice would bill 3.43). Dry runs at 10:48: every GPU shape started at once (untyped `gpu:1`, an RTX 6000
  typed, an A100 typed, a full B200, `--constraint='rtx6000|a100'`).
- **Where `c` sits on a GPU pipeline's path today** (dev conf): tsImport (8 CPUs, `c`), then one supervisor per
  array stage (1 CPU, 4 GB, 4 h, `c`), eligible only when the stage before ends, so it enters the queue afresh with no
  age: six of them to extraction (motion, alignment, CTF, reconstruct, template matching, extraction). After
  extraction: subtomoExtraction, extractPickList, reconstructParticle and filamentPick on `c`. Fair-share is not ours
  to move (roadmap 45 §11, 2026-11-09 13:07: the lab's use barely moves the factor).
- **Cards per stage today** (dev conf): motion, alignment, CTF and export `gpu:1` with `constraint: b200` (a B200 or
  a slice; the RTX 6000 and A100 nodes were untested, which P13 ends); reconstruct, template matching, segment, Class3D
  and Refine3D a full `nvidia_b200`; candidate extraction a slice. The lab conf lags all of it (45 §11).
- **Throttles**: 20 for motion, alignment, CTF, reconstruct and denoise predict; 16 for template matching, extraction,
  export and subtomogram extraction; 8 for segment (`array_throttle`, a user parameter of each job, at most 64). QOS
  `g_short` (8 h) has no per-user cap; `g_medium` caps a user at 16 B200s, 112 slices, 128 GPUs.
- **Parallelism already in place**, nothing more asked for here: Warp `--perdevice` (motion 2; reconstruct chunks of
  8 with 2 workers), template matching in 3 MPS lanes x 2 with `gpu_split`, tsImport in 4 calls, Refine3D under MPI.
- **P13** (roadmap 45 §11; seconds an item, B200 / B200 slice / A100 40 GB / A100 3g / RTX 6000): motion 40 / 44 /
  68 / 72 / 103; alignment 21 / 26 / 26 / 29 / 23; CTF 11 / 14 / 23 / 22 / 23; reconstruct 43 / 169 / 136 / 144 /
  349; a tomogram's template matching 104 / 554 / 260 / 510 / 736. Alignment and CTF do not care about the card;
  motion is 2.5x slower on an RTX 6000 at a third of the billing; reconstruct is 3–8x slower off a B200. **Outputs:**
  the same architecture gives the same bits (B200 against its slice, A100 against its slice); across architectures
  tilt-series CTF, reconstruct and pytom agree, while Warp's motion and frame-CTF fits and AreTomo2 differ materially
  on frames and series without signal (the highest tilts; the pilot's poorest series), with no card right or wrong.
  So mixing architectures in a project costs reproducibility: a failed item re-run elsewhere gets a different answer.

## 3. Principles

- **Wall time first, billing second, utilisation third.** A card held idle for an hour at 2 an hour that saves a
  three-hour queue is the right trade; the trade is written down (D1), not hidden.
- **Any card that fits, reproducibility at the margins accepted.** Warp's and AreTomo2's fits are bitwise
  reproducible within a GPU architecture and not across them, on frames and tilt series without signal (P13). The
  maintainer's call (2026-11-10): the 63 non-B200 cards are worth more than that, so every stage takes any card that
  fits its memory, and a marginal item may come out differently between runs. Only speed and memory type a request
  (reconstruct, template matching). The finding is recorded (roadmap 45 §11, the issue draft of D7).
- **No `c` on a GPU pipeline's path.** CPU work rides a GPU node on the cheapest card until the server does the glue
  itself (roadmap 46 Part B), after which only tsImport needs a ticket.
- **The throttle is the number of cards we hold at once**, chosen against what exists (319) and what others need,
  not a constant.
- **Stop where a stage is minutes long.** 160 motion tasks take 7 minutes on 39 RTX 6000s or 5 on 20 uncrowded B200s;
  neither decides a pipeline whose boundaries cost 30. No more slice-against-card comparisons for such stages.
- **Reversible first.** Conf where conf can say it; code only for what conf cannot (throttle defaults). Both installs
  get the same conf, with the reason in its comments.
- **Same outputs** (roadmap 45 §7): every card change is checked against the pilot's outputs before it is a default.

## 4. Decisions (the maintainer's)

- **D1 — CPU steps onto `g` on an RTX 6000 now, conf only.** (The card is a ticket, never a compute choice, so the
  architecture finding does not touch it.) The supervisors, tsImport and the four post-extraction
  CPU jobs take `partition: g`, `gres: gpu:quadro_rtx_6000:1`, their CPUs and memory as now. Cost: about 1.5 card-h
  of a 0 %-busy GPU job per pipeline (about 3 billing-h on 206), and IT's accounting shows six idle-GPU jobs of ours
  per pipeline, on cards that stand idle anyway. The alternative is to wait for roadmap 46 Part B. Recommended: yes
  now, D2 behind it; the roadmap 46 conversation with IT (its D1) mentions it.
- **D2 — Roadmap 46 D8: may a pipeline depend on the server being up?** Recommended: yes for the lab's deployment,
  where the server is already what every user reads results from; `orchestration: supervisor` stays the template
  default for sites that cannot keep a server up. This unlocks 46's S6–S8, which remove the supervisors for good.
- **D3 — Throttle defaults.** Proposed: 40 for motion, alignment, CTF and candidate extraction (any card, 319 exist,
  no `g_short` cap); reconstruct 16 (B200 only: 32 exist, 16 is the `g_medium` cap and half the cards); template
  matching stays 16 (lanes of 6 tomograms); segment 8. Or leave them at 20 and raise per job in the tab.
- **D4 — The Warp stages take any card** (the maintainer, 2026-11-10, after the P13 finding was put to them: "throwing
  away 60-something GPUs for good just to keep some marginal tilts reproducible is not worth it"): `constraint: b200`
  is gone from motion, alignment, CTF and export. Reconstruct keeps its full B200 (43 s a tilt series against 136–349
  elsewhere, and dearer on an RTX 6000) and template matching its B200 lanes (D5); both for speed, not architecture.
- **D5 — Template matching stays in B200 lanes** (about 40 billing-h a hundred tomograms). An A100 40 GB does a
  tomogram in one piece in 260 s at 43 billing-h a hundred with the same score map (3e-8): a second pool at the same
  price when the B200s are full, offered as an alternative profile the SLURM tab can pick, not a default. The 20–24 GB
  cards take 8–12 minutes a tomogram.
- **D7 — The reproducibility finding goes to Warp's author** as an issue (drafted 2026-11-10:
  `docs/reports/gpu-util/warp-architecture-reproducibility-issue.md`; the maintainer files it). A whole-chain pilot on
  A100s against the B200 pilot (`compare_projects.py`, about 135 billing-h) would show whether the frame-level
  differences reach good tilt series' tomograms and picks; optional, after S3.
- **D6 — What stops.** Slice-against-card comparisons for the short stages; MPS lane tuning beyond what is built; the
  crowding probe (unless the balance pilot shows motion crowded on a slice node again); roadmap 45's meter (S2) and
  its UI chip (S5) wait until after S3 here, because `sacct`'s busy figure is enough to judge a stage; item flow
  (46 L11) after Part B, not before.

## 5. Stages

**S1 — Conf: every card, nothing on `c`** (both installs; `conf.template.yaml` with the reasons in its comments).

```yaml
supervisor_slurm:
  partition: g                       # c queues for hours between stages; g starts at once with a GPU ticket
  gres: gpu:1                        # the ticket; the supervisor never touches it
  constraint: rtx6000|a100           # any RTX 6000 or A100 card or slice (2 to 6 an hour), never a B200 (24)
  # cpus_per_task 1, mem 4G, time 4:00:00 as now; qos '' = g_short

job_resource_profiles:
  # fsMotionAndCtf, aligntiltsWarp, tsCtf, warpExport: gpu:1, the constraint b200 line removed (any card, D4)
  tsImport:
    partition: g
    gres: 'gpu:1'
    constraint: 'rtx6000|a100'
    mem: 16G
    cpus_per_task: 8
    time: '0:30:00'
  subtomoExtraction:
    partition: g
    gres: 'gpu:1'
    constraint: 'rtx6000|a100'
    mem: 16G
    cpus_per_task: 4
    time: '0:30:00'
  extractPickList:
    partition: g
    gres: 'gpu:1'
    constraint: 'rtx6000|a100'
    mem: 16G
    cpus_per_task: 4
    time: '0:30:00'
  reconstructParticle:
    partition: g
    gres: 'gpu:1'
    constraint: 'rtx6000|a100'
    mem: 32G
    cpus_per_task: 4
    time: '2:00:00'
  filamentPick:
    partition: g
    gres: 'gpu:1'
    constraint: 'rtx6000|a100'
    mem: 32G
    cpus_per_task: 8
    time: '1:00:00'
  # unchanged: tsReconstruct, templatematching, segment, class3d, refine3d on nvidia_b200; tmextractcand on a slice.
```

The CPU steps move, and the Warp stages lose their constraint (D4); reconstruct and template matching keep their
B200 type (D5). The dev conf first (the server reads it on restart), the
lab conf with its pull (45 §11 lists what else it lacks). The six dry runs of §2 accepted every shape.

**S2 — Throttle defaults** (code, small; after D3): the `array_throttle` `Field` defaults in `services/jobs/*.py`
for the stages D3 names. `USER_PARAMS` and the tab are unchanged; a project's saved values are untouched.

**S3 — The balance pilot.** One 412 pipeline to extraction on the dev server with S1 (and S2), started at a busy
hour, measured by §1's five numbers against the anglestest pilot (first supervisor waited 26 minutes; 79 minutes from
there to the end of template matching; 205.6 billing-h). Per stage: which cards the tasks landed on and the per-item
time by card against P13's. The numbers go into §7 and roadmap 45 §11; D3–D5 are settled from them.

**S4 — Roadmap 46 Part B, S6–S8** (after D2): the server runs `plan` and `finish` and submits each array to `g`
itself; no supervisor jobs; the RTX 6000 ticket is then only tsImport's. Item flow (46 S10) only if S3's boundary
waits are still visible after that.

**S5 — The lab**, the maintainer's step: pull, conf, restart; a week later roadmap 45's P0 over the window (no job)
gives the per-type figures for both installs.

## 6. Runtime checklist (after S1)

1. Run on a 412 project while `c` shows a queue: tsImport and the first supervisor start on a `cbe-g3-*` node within
   minutes (`cb-*-sup` jobs carry `gres/gpu:quadro_rtx_6000:1` in accounting).
2. Motion, alignment and CTF tasks land on `g3`, `g4` and `g5` nodes mixed, per-task times by card near P13's; the
   CPU jobs on `g3` or `g4` nodes; the outputs as before but for marginal items (D4).
3. Each stage's supervisor starts within two minutes of the stage before ending.
4. A supervisor on an RTX 6000 node submits its array, polls and lands outputs exactly as from `c` (same code; sbatch
   exports only `SCRATCHDIR`, so the tasks inherit nothing from the node).
5. Billing per pipeline within 1.5x of 205.6 billing-h; §1's five numbers recorded.

## 7. Build record

- **2026-11-10, scoped.** Dry runs and the cluster state of §2 are from this morning; nothing built, no conf changed.
  P13 ran the same morning (roadmap 45 §11, `docs/reports/gpu-util/gpu-types-2026-11-10.md`): the first draft's
  "any card for the short stages" became "one architecture per project" (§3, D4), and D7 was added.
- **2026-11-10, S1 built on the maintainer's go** ("supervisors on the GPUs"). `config/conf.yaml` (dev):
  `supervisor_slurm` and the tsImport, filamentPick, subtomoExtraction, extractPickList and reconstructParticle
  profiles take `partition: g` and `gres: gpu:quadro_rtx_6000:1`, CPUs, memory and time as before; no GPU stage
  changed (D4, D5) in this first form, revised the same afternoon (next entry).
  `config/conf.template.yaml`: the generic defaults stay, with the ticket option and CBE.next's values in the
  comments at `supervisor_slurm`, tsImport and the RELION CPU steps. The lab's patched conf, the same edits on
  yesterday's: `/scratch/users/artem.kushner/crboost_lab_conf/conf.yaml-20261110` (the maintainer installs it: back
  up `/groups/klumpe/software/crboost_server/config/conf.yaml`, copy, restart). `sbatch --test-only` for the three
  shapes (1 CPU 4G 4 h; 8 CPUs 16G; 4 CPUs 32G 2 h), all `gpu:quadro_rtx_6000:1` on `g`: each to start at once on
  `cbe-g3-1`. The dev server reads the conf on restart. Not run: S3, the balance pilot, is the check (§6).
- **2026-11-10, S1 revised on the maintainer's second call** (D4): `constraint: b200` removed from fsMotionAndCtf,
  aligntiltsWarp, tsCtf and warpExport in the dev conf and the lab's patched conf; the ticket widened from
  `gpu:quadro_rtx_6000:1` to `gpu:1` + `constraint: rtx6000|a100` for `supervisor_slurm` and the five CPU profiles
  (whichever RTX 6000 or A100 is free first; `check_constraint` accepts it on `g`; the dry run of the same shape
  started at once this morning). The template's comments follow. The issue draft for Warp's author written (D7).
