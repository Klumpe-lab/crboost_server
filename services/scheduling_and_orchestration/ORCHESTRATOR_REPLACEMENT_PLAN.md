# Orchestrator Replacement — Plan

**Status:** PROPOSAL → GREENLIT → **PLAN FINALIZED 2026-06-23** (verified against a
full codebase map; see §6 for the file-level phases). We are building this and
excising `relion_schemer`. Companion to `../../PIPELINE_AND_ORCHESTRATOR_STATE.md`
(repo root; the status-desync diagnosis), which is the concrete motivation.

**Location (2026-06-23, corrected):** build for **on-cluster** — the off-cluster
VM is only *in talks*, NOT secured, so do not design around it. The reconciler +
server run where they run today: on the headnode, with shared-Lustre access to
sentinels and **local** `sbatch`/`squeue`/`sacct`. This keeps the reconciler
**in-process** (so in-place `ProjectState` mutation still pushes to the NiceGUI
client) and makes the VM↔FS / VM↔SLURM / split-brain questions in §4 moot for v1.
A later VM move stays clean because the design is stateless — keep SLURM access
behind a thin seam so the only swap is local-CLI → ssh-hop/`slurmrestd`.

**One-line thesis:** Stop using `relion_schemer` to *drive* execution. Make
`ProjectState` the sole runtime truth, let **SLURM job dependencies** be the DAG
engine (SLURM is already our real scheduler), reduce our own code to a thin
**stateless reconciler/observer**, and demote `default_pipeline.star` / `job.star`
from live state to a generated **RELION-compatibility export**.

---

## ▶ NEXT SESSION — pick up here (status 2026-06-24)

**✅ RUNTIME-VALIDATED 2026-06-24 (success path) on `/groups/klumpe/crboost_data/AK_new_orchestrator_demo`:**
a 5-job chain (importmovies → fsMotionAndCtf → tsImport → aligntiltsWarp → tsCtf) ran end-to-end via the
afterok DAG with the global toggle on. Confirmed: `Schemes/` empty (schemer never launched); `importmovies`
Succeeded with `slurm=None` and `Import/job001/tilt_series.star` written by our inline writer — **byte-identical
to the relion schemer's output** on the same dataset (same 8+6 columns, acquisition-order dose `0,3,6,9`,
`rlnTomoHand=-1`, nominal defocus `-5.5`); `fsMotionAndCtf` consumed it and Succeeded; all 4 SLURM jobs
Succeeded (sequential ids 30863491–94, sentinels present); `pipeline_active` wound down to False. So Option B
(inline import), P1.A (submit_chain), and P1.B (reconciler) are all runtime-proven on the success path.

**Still unexercised at runtime (the next-session checklist):**
1. **Full pipeline** — this chain stopped at `tsCtf`. Not yet run through afterok: `tiltFilter`, `tsReconstruct`,
   the **denoise** jobs (`denoise_train`/`denoise_predict`), template matching, and the **particle-stage tail**
   (`extractCandidates` → `subtomoExtraction` → `reconstructParticle` → `class3d` — the RELION-binary jobs).
2. **Stop-on-fail** — a job FAILs → `DependencyNeverSatisfied` → the scancel path (this run was all-green).
3. **Retries / re-deploy** under the flag (the afterok re-run branch in `_submit_chain`).
4. **P1.C** — under afterok nobody maintains `default_pipeline.star`; "open in RELION" not yet validated.
5. **Array-job tail** — `tsReconstruct`/`subtomoExtraction` run as supervisor+SLURM-array; only single-supervisor
   jobs have been exercised on afterok so far.

**▶ USER PLAN (stated 2026-06-24):** add a **denoising / IsoNet** job in a separate session, then pick up HERE
to run the **full pipeline** (through the particle tail) end-to-end on afterok — covering checklist #1 + #5,
and ideally #2 (one deliberate failure). New job types must declare correct io-slot `INPUT_SCHEMA`/`OUTPUT_SCHEMA`
so `resolve_edges` wires them into the afterok DAG automatically (no orchestrator change needed).

**Done (code-clean: `py_compile` + `ruff check`/`format` on touched files; only the documented
pre-existing F401 drift):**
**P1.0 + P1.A (submit_chain) + P1.B (reconcile_afterok + control-path) all landed.** The per-project
flag `ProjectState.use_afterok_orchestrator` (default False) is now the **full submit+status cutover**:
on, the pipeline is submitted as a SLURM `--dependency=afterok` DAG and status is reconciled from
`sacct`/`squeue` + sentinels — `relion_schemer` is never launched. Schemer path is byte-identical when
the flag is off. Full detail + the per-file change list is in §6's **Implementation log** below.

**The IMPORT-job blocker — RESOLVED BY DECISION (2026-06-24): replace it, don't fix it (Option B, inline).**
Investigation (two grounded workflow passes against the real succeeded project
`/groups/klumpe/crboost_data/post_handedness_fix/Import/job001`) showed `_build_import_command` is wrong at
*every* level, not just `--i`/`--odir`: the relion5 schemer never runs `relion_import` — it runs the **native**
`relion_python_tomo_import SerialEM` (hyphenated flags, from the user's `~/.local/bin`, NOT the container),
bare, from project root. Rather than chase that binary onto a compute node (PATH/exec-env headaches), we
**own the import**: it becomes OUR registry→star writer emitting `Import/jobNNN/tilt_series.star` + per-TS
`tilt_series/<TS>.star` byte-matching the oracle's columns — **no relion binary, no container, no afterok
SLURM job**. Grounded basis: the import star is a tiny, fully registry-derived seed; only `fsMotionAndCtf`
consumes it; the particle jobs never read it; handedness is overwritten by Warp `ts_defocus_hand` anyway;
deepwiki confirmed the RELION GUI does NOT hard-fail without an Import node. Full record in §6a; memory
`project_import_job_necessity.md` mirrors it.

**LANDED 2026-06-24 (code-clean: `py_compile` + `ruff check`/`format` on the touched file; PENDING runtime —
restart `main.py`).** `pipeline_orchestrator_service.py`: new `_write_import_stars_inline(job_model, job_dir,
project_dir)` builds the master `tilt_series.star` (8-col `global`) + per-TS `tilt_series/<TS>.star` (6-col,
acquisition order) from `get_registry_for(project_dir).all_tilt_series()` + ImportMoviesParams, sourcing
`rlnTomoNominalDefocus` from the mdoc (TargetDefocus by movie basename, defaults 0.0). `_submit_chain`'s
prep-loop special-cases `JobType.IMPORT_MOVIES`: runs the writer inline, drops `RELION_JOB_EXIT_SUCCESS`,
sets the job `SUCCEEDED` + `slurm_job_id=None`, and **excludes it from `prepared`**; the submit DAG now
toposorts over `list(prepared)`, so `fsMotionAndCtf`'s afterok edge to import drops out and it submits with
no producer (star already on disk). `_build_import_command` reduced to a harmless no-op (the relion command
is dead on both paths). Schemer path (flag off) behavior byte-identical (importtomo job.star ignores fn_exe).
**Runtime gate:** on a scratch project with the flag on, deploy and diff `Import/jobNNN/{tilt_series.star,
tilt_series/*.star}` column-for-column against the `post_handedness_fix` oracle (float decimal padding may
differ — values, columns, ordering, and `rlnTomoHand` sign are what matter). Then the P1.A+B smoke-test below.

**⚠️ TEMP DEV TOGGLE (added 2026-06-24, REMOVE after validation):** `conf.yaml` now has a global
`use_afterok_orchestrator: true` so EVERY project opened/created uses the afterok path without the
per-project `project_params.json` edit. It's `true` in the live `conf.yaml` and `false` in
`conf.template.yaml`. Wiring: `Config.use_afterok_orchestrator` (config_service.py) → `project_state.py`
`_afterok_global_default()`, applied as the field `default_factory` (fresh projects) and OR'd into the
load path (existing projects); a per-project `True` still wins. **Stickiness:** opening/saving any project
while this is on bakes `true` into that project's `project_params.json` — so keep it to scratch projects,
and turning the toggle off later won't revert already-saved ones. Remove this toggle (config field + 2
`project_state.py` edits + the yaml lines) once the afterok path is validated.

**Then smoke-test P1.A+B (no P1.C needed):** with the toggle on (above), just create a *scratch* project,
restart `main.py`, deploy (no per-project edit needed). Expect: chain submitted with correct `afterok`
deps (verify `squeue`/`scontrol show job <id>`), status reconciled live in the UI, `pipeline_active`
winds down when done. **One-time cluster checks:** `sacct` availability + accounting retention window;
the exact `squeue %r` reason string for a never-satisfiable afterok dependent (the stop-on-fail scancel
matches the literal `DependencyNeverSatisfied`).

**Next phase = P1.C (RELION-compat export).** First concrete step: write
`export_relion_compat(ProjectState, project_path)` authoring `default_pipeline.star`
(general/processes/nodes/edges from the `resolve_edges` DAG) + per-job `job.star` (generalize
`generate_job_star`, `services/jobs/_base.py`), then **validate the RELION GUI opens the result**
(risk **R-OPEN**, §7) before converting the remaining reads + the deletion-cascade
(`pipeline_deletion_service.py`) to the edge graph. See §6 P1.C + the read-removal map.

---

## 1. Motivation — three annoyances wearing one trenchcoat

The frustration with the schemer is really three separate problems. Only the
first is worth a rewrite; conflating them makes the job look bigger than it is.

1. **Dual source of truth (the crucial one).** `ProjectState`
   (`project_params.json`) is our truth, but the schemer's `default_pipeline.star`
   is *also* a truth we must continuously chase (`pipeline_runner.sync_all_jobs`).
   Every reconciliation is an opportunity to desync. This is the root of a whole
   class of status bugs (see `../../PIPELINE_AND_ORCHESTRATOR_STATE.md`).
2. **Process durability.** The schemer (and the web server) die when the SSH
   session that launched them gets `SIGHUP`'d ("closed the login tab"). This is
   **orthogonal** to the schemer being old — a homecooked orchestrator launched
   the same way dies identically. See §4.
3. **Opaque, unshapeable status.** We want per-TS / per-array-task transitions
   pushed to the UI; the schemer gives us a star file to poll and diff. We can't
   shape its output to the UI's needs.

Naming them separately matters: #2 has a cheap fix independent of everything
else, and #1 + #3 are the *same* refactor.

---

## 2. Key insight — we already own most of the orchestrator

The genuinely hard parts of orchestration are **already ours**, per job type, in
the drivers. Example: `drivers/subtomo_extraction.py` (supervisor mode) slices
per-TS inputs, submits a SLURM array, waits, collects `.ok/.fail/.skip`, and
merges. That's dynamic fan-out + partial completion + idempotent restart — the
hard stuff.

So what is the schemer *still* doing?

- Sequencing the job **types** in a mostly-linear DAG
  (import → motion/ctf → align → recon → TM → pick → extract).
- Submitting each supervisor as a SLURM job via `qsub.sh`.
- Tracking completion via sentinels + `default_pipeline.star`.

That is a **thin top layer** over machinery we own. Replacing it is *not*
"write a workflow engine from scratch." That reframing is decisive for the
effort estimate (§6).

---

## 3. Target architecture

1. **`ProjectState` is the sole runtime truth.** Nothing reads
   `default_pipeline.star` as *input* anymore.
2. **`default_pipeline.star` / `job.star` become a generated export.** We already
   own the writers (`StarfileService`). Serialize on demand / after state
   changes. RELION-openability becomes an *output artifact*, not live state.
   This single demotion removes the reconciliation tax — and it's exactly the
   "serialize our metadata to RELION star format" idea that motivated this.
3. **SLURM job dependencies are the DAG engine.** Submit the top-level chain up
   front with `--dependency=afterok:<prev>` between supervisors (a dependency on
   an array job waits for *all* its tasks; `aftercorr` for per-task
   correspondence). Each supervisor still does its own internal dynamic fan-out,
   so "we don't know the TS count until import runs" is not a blocker — that
   lives *inside* the supervisor, not in the top DAG.
4. **Our code becomes a thin, stateless reconciler.** It:
   - (a) submits the chain from `ProjectState`,
   - (b) periodically reads `sacct`/`squeue` + sentinel files → writes status
     into `ProjectState`,
   - (c) emits status events to the UI (WS/SSE).

   Because all durable state lives in SLURM + on-disk sentinels + the json, this
   loop can crash and resume by **re-observing**. No babysitting.

```
ProjectState (sole truth, project_params.json)
      │  submit
      ▼
SLURM dependency chain  ── supervisors do internal per-TS array fan-out
      │  observe (sacct/squeue + .ok/.fail/.skip sentinels)
      ▼
Reconciler (stateless) ──► writes status back into ProjectState
      │                    └─► emits events to UI (WS/SSE)
      ▼
RELION export (default_pipeline.star / job.star)  ── generated on demand
```

**Why this is the high-leverage version:** it fixes #1 (one truth), #3 (we emit
exactly the events the UI wants instead of diffing a star file), **and** #2 —
because if execution lives in the SLURM dependency chain, the pipeline keeps
running even when the head-node process dies; the reconciler just reconnects and
re-observes on restart.

**State-store rule (Lustre):** keep orchestrator state in SLURM (`sacct`) +
sentinel files + json. Do **not** put a SQLite/embedded DB on Lustre — POSIX
locking over networked filesystems is unreliable.

---

## 4. Durability — "where does the brain live"

A process started in an interactive SSH session dies on `SIGHUP` when the
connection drops. Site policy may also reap long-running login-node processes,
kill user processes on logout, or reboot head nodes. "Survives tab close",
"survives logout", and "survives reboot" are three different bars.

Critically: our reconciler is **stateless**, so we are not asking for a pet
process that must never die — we are asking for *a place to run a restartable
observer*. That makes the cheap options viable.

### DECISION (2026-06-23, corrected): on-cluster for v1 (the VM is only in talks)

The off-cluster VM is **not secured** — it is in discussion only. **Do not design
around it.** v1 runs on-cluster exactly as the server runs today: server +
reconciler **in one process on the headnode**, with shared-Lustre access to
sentinels / `project_params.json` and **local** `sbatch`/`squeue`/`sacct`. This
moots the VM↔cluster integration questions for now (no FS-mount fork, no
ssh-hop/`slurmrestd` choice, no split-brain) and keeps the reconciler in-process,
so its in-place `ProjectState` mutations still push to the NiceGUI client (a
remote reconciler would push *nothing* to the headnode browser).

Durability for v1 is the cheap **Tier-1** quick-win (`setsid nohup` / `tmux`), same
as the server today; **Tier 3** (scrontab or a self-resubmitting service-partition
job) is the prod hardening when wanted. Because the reconciler is stateless, a
later move to an off-cluster VM (Tier 4) is a clean follow-on — the only code that
changes is the SLURM-access seam (local-CLI → ssh-hop/`slurmrestd`) plus, if the
VM does not mount Lustre, a sentinel-read channel. The tier menu + admin-question
list below is retained as rationale **for that future move; none of it gates v1.**

### Tiers

- **Tier 1 — zero-admin, dev-grade** (tab close ✅; logout ⚠️; reboot ❌):
  `setsid nohup …`, or `tmux`/`screen`. Fine while iterating; not for users.
- **Tier 2 — single-node durable** (logout ✅, reboot ✅, pinned to one node):
  `systemd --user` unit + **lingering** (`loginctl enable-linger`). Proper
  no-root daemon. Caveat: pinned to one login node; load-balanced SSH may not
  return you to it.
- **Tier 3 — cluster-native durable (best fit for this design):**
  - **Long-running SLURM job** on a *service/long* partition/QOS,
    **self-resubmitting near walltime** (submits its successor with a dependency
    before hitting the wall). The resubmit seam is a non-event for a stateless
    reconciler.
  - **`scrontab`** (SLURM's cron, ≥ 20.11): run the reconciler every N minutes
    and exit. No long-lived process to die — it's *meant* to terminate and
    re-run. Cleanest option for a stateless reconciler.
- **Tier 4 — off-cluster brain (most robust):** always-on gateway VM / institute
  host running server + reconciler, talking to the cluster via SSH or
  **`slurmrestd`** (REST + token). Nothing long-lived on a node that
  reboots/reaps. If the site runs **Open OnDemand**, that's a sanctioned way to
  host an interactive app backed by a batch job + proxy.

### Survives-what matrix

| Approach | tab close | logout | node reboot | admin ask |
|---|:---:|:---:|:---:|---|
| nohup + setsid / tmux | ✅ | ⚠️ policy | ❌ | confirm logout-kill policy |
| systemd --user + linger | ✅ | ✅ | ✅ (1 node) | enable lingering |
| long SLURM job (self-resubmit) | ✅ | ✅ | ✅ | service partition/QOS + walltime |
| scrontab reconciler | n/a | ✅ | ✅ | scrontab enabled (Slurm ≥20.11) |
| off-cluster host + slurmrestd | ✅ | ✅ | ✅ | slurmrestd/token or gateway VM |

### Questions for the cluster admin

1. On logout, are user processes killed? (`logind KillUserProcesses`, or a site
   reaper.) Decides whether tmux/nohup are viable at all.
2. Can you enable **lingering** for my account (`loginctl enable-linger`)?
3. Any **cgroup limits / reaper crons** on login nodes killing long-running
   processes? What runtime is tolerated?
4. Is there a **service/long partition or QOS** for a persistent driver job, and
   the max walltime?
5. Is **`scrontab`** enabled? What **SLURM version** are we on?
6. Is **`slurmrestd`** available with token auth?
7. Is there an **always-on host / VM / Open OnDemand** for science-gateway
   services that can reach the scheduler?
8. If SSH is **load-balanced across login nodes**, is there a stable hostname, or
   will a node-pinned service be unreachable next session?

**Steer:** dev → Tier 1 now. Production → Tier 3 (scrontab or self-resubmitting
service job) is the sweet spot because it makes "the process died" a non-event;
Tier 4 is gold if a gateway host / slurmrestd is available, and it's also the
right home for the always-on UI server. NB: the orchestrator and the web server
are the *same* durability problem — wherever the server lives is where the brain
lives, unless we deliberately split them.

---

## 5. Buy vs build

**Criterion:** adopt a heavyweight engine when it owns the hard part *for* you;
build when the hard part is the part you need to *control*. Our hard parts —
per-TS fan-out, UI-grade status granularity, RELION-compatible on-disk layout —
are exactly what a generic engine fights.

- **Nextflow** — own DSL (Groovy); hashed `work/` + symlink staging that fights
  the RELION layout; resumability is file-hash based; you'd drive it by shelling
  out to `nextflow run` and parsing weblog for status. Great for shareable,
  reproducible *batch* pipelines (nf-core); poor fit for an interactive GUI
  orchestrator. **Lean no.**
- **Snakemake** — strongest "buy" candidate because it's Python and embeddable.
  But pull-based (target-file driven) and batch-oriented; dynamic per-TS fan-out
  needs checkpoints and gets fiddly; live GUI status is still a mismatch. Closer
  to home, still fighting the model.
- **Prefect / Dagster** — modern, nice observability, embeddable. Cloud/
  container-native; you'd bolt SLURM submission on and run extra infra. Heavy.
- **Parsl / Covalent** — actually HPC/SLURM-native and Python (futures / "apps").
  Worth a look *if* we decide we don't want to own a control loop at all.
  Smaller communities.
- **Homecooked thin reconciler over SLURM deps** — least new tech, keeps
  control, reuses drivers + `qsub.sh`. **Recommended.**

**Trap to avoid:** bundling this with "and adopt Nextflow and rewrite the UI."
That bundle *is* prohibitive. The scoped version is not.

---

## 6. Phased plan (FINALIZED 2026-06-23 — verified against a full codebase map)

> **Implementation log.**
> - **P1.0 — LANDED 2026-06-23** (code-clean: `py_compile` + `ruff check`/`format`
>   pass; **PENDING runtime** — needs a `main.py` restart, no auto-reload).
>   `ProjectState.pipeline_order: List[str]` added (`project_state.py`);
>   `SCHEMA_VERSION (3,0)→(3,1)` — additive ⇒ MINOR, not the 4.0 first sketched (a
>   backward-compatible field needs no major bump, and major would spuriously warn +
>   back up every project). Backfilled on load from the loaded `jobs` in file order;
>   written through from the full `selected_instance_ids` at deploy
>   (`pipeline_orchestrator_service.deploy_and_run_scheme`). `resolve_edges()` added
>   to `PathResolutionService` (read-only; mirrors `resolve_inputs` selection; skips
>   manual / `mergedSources` / self / unresolved → no dangling or self `afterok`
>   edges). No change to `resolve_inputs`; no live-path behavior change.
> - **P1.A — submit-machinery LANDED 2026-06-23** (code-clean: `py_compile` + `ruff
>   check`/`format` pass on the touched files; only output is the documented pre-existing
>   F401 drift; **PENDING runtime** — needs a `main.py` restart). Dormant behind a new
>   per-project `ProjectState.use_afterok_orchestrator` flag (default False; the schemer path
>   is byte-identical when off). `SCHEMA_VERSION (3,1)→(3,2)`: +`use_afterok_orchestrator`,
>   +`job_dir_counter` (sole job-dir allocator — seeded once from `rlnPipeLineJobCounter`,
>   monotonic, no reuse-on-rerun ⇒ off-by-one fixed). New in the orchestrator: `_submit_chain`
>   + `_render_supervisor_script` (qsub.sh→`run_submit.script`; resources from
>   `_get_queue_options()` so array-job *supervisors* stay lightweight; RELION marker block
>   kept) + module-level `_toposort_submit_order` (Kahn + cycle check). In `pipeline_runner`:
>   `_sbatch_script(+dependency_after_ids)` injects `--dependency=afterok:` + public
>   `submit_supervisor`. The flag branch sits at the `deploy_and_run_scheme` launch seam.
>   Adversarially reviewed (5 dimensions, verified findings); fixes folded in: (a) `sync_all_jobs`
>   early-returns for afterok projects — the central-read cutover seam — so the schemer reconcile
>   can't wipe `slurm_job_id`/`relion_job_name`/QUEUED (`default_pipeline.star` always exists from
>   project init, so the clobber *would* fire; it's also reachable from UI calls, not just the
>   monitor); (b) `pipeline_active` left False (P1.B owns it); (c) local job-dir counter committed
>   only after a clean prepare+toposort (no counter leak on failure); (d) partial mid-chain submit
>   failures persist whatever reached SLURM.
> - **⚠️ P1.A runtime GATES (verify/fix before an afterok run completes end-to-end):**
>   (1) **IMPORT job command** — `_build_import_command`'s `relion_import` string is wrong at every level
>   (wrong binary: the real schemer cmd is native `relion_python_tomo_import SerialEM`; dropped `--i`;
>   no `--odir`; wrong cwd). **RESOLVED BY DECISION 2026-06-24 (§6a): Option B — replace with our own
>   inline registry→star writer, no relion binary.** Was dead under the schemer (importtomo job.star
>   ignores fn_exe) but LIVE under afterok. (2) **P1.B reconciler required**
>   for live status: with the flag on, the SLURM chain runs but the UI shows no progress until P1.B.
>   (3) retries under the flag still route through `launch_retries` (schemer-coupled), not afterok.
> - **P1.B — reconciler + cutover LANDED 2026-06-23** (code-clean: `py_compile` + `ruff
>   check`/`format` on touched files; only the documented pre-existing F401 drift; **PENDING
>   runtime**). The afterok flag is now the FULL cutover (P1.A submit + P1.B status). New:
>   `slurm_service.query_jobs_by_ids` (targeted `squeue -j`, **B2-safe**: `None`=CLI error vs `{}`=
>   not-queued) + `query_terminal_states` (**net-new `sacct`**, graceful `None` if unavailable);
>   `pipeline_runner.reconcile_afterok` + `_afterok_state_to_status` — per non-terminal tracked job:
>   disk `RELION_JOB_EXIT_*` sentinel → `squeue` (QUEUED-vs-RUNNING B1 + `DependencyNeverSatisfied`→
>   scancel, defensive-matched) → `sacct` → a **grace window** (`_afterok_absent_since`, 90 s) that
>   FAILs a marker-less vanished job so `pipeline_active` always winds down; mutates `execution_status`
>   in place, drives `pipeline_active` off live-job presence (NOT `is_active`, always False here). The
>   monitor `_tick_once` dispatches afterok projects to `reconcile_afterok` (schemer path byte-identical
>   when off); `_recover_one` registers-and-defers them (restart re-observes). `submit_chain` now SETS
>   `pipeline_active=True` + unlinks stale exit markers on its job dir. **Control-path made afterok-aware**
>   (reachable via existing UI): `cancel_job`/`stop_and_cleanup` scancel by persisted `slurm_job_id`
>   (not squeue discovery, sidestepping the broad `get_user_jobs` `[]`-bug); `cancel_job` no longer
>   clears `pipeline_active` for afterok; afterok re-runs route through `_submit_chain` (fresh dirs +
>   rewired afterok edges), not the schemer `launch_retries`. Adversarially reviewed (4 dims + focused
>   regression pass); HIGH/MEDIUM findings fixed, refuted ones (batch-poison on standard SLURM, locale,
>   event-loop I/O, proactive dependent-scancel vs SLURM's afterok gate) left as-is.
> - **⚠️ P1.B runtime checks:** the IMPORT-job command gate (P1.A) still blocks an end-to-end run.
>   One-time cluster verify: `sacct` availability/retention + the exact `squeue %r` reason string for a
>   never-satisfiable afterok dependent (the scancel path matches `DependencyNeverSatisfied`).
> - **P1.C / P1.D — NOT STARTED.** P1.C = `export_relion_compat` (default_pipeline.star/job.star as a
>   generated export; validate the RELION GUI still opens it — R-OPEN) + the remaining read-removal +
>   deletion-graph from `resolve_edges`. P1.D = UI status events (queue-wait banner, staleness) + wire
>   the afterok stop/cancel UI.
>
> _Pre-existing lint drift (not introduced here): `ruff check` reports F401 unused
> imports in `project_state.py` / `path_resolution_service.py` — left untouched
> (the `job_models` block is load-bearing for Pydantic class resolution; `--fix`
> would strip it)._

### §6a — Import-job replacement (Option B, inline) — DECIDED + IMPLEMENTED 2026-06-24

> **STATUS: implemented 2026-06-24** (code-clean, PENDING runtime). Per-file landed detail + the runtime
> validation gate are in the ▶ NEXT SESSION banner at the top of this doc. The "Implementation" steps below
> are the as-built design.

**Decision:** the afterok IMPORT job stops being a relion invocation and becomes **our own
registry→star writer, run inline at deploy**. This both unblocks P1.A's broken import command and
advances the thesis (relion = "just another tool", not an owner of our metadata/orchestration).

**Why the original "fix the command" plan was wrong.** Grounded against the real succeeded project
`/groups/klumpe/crboost_data/post_handedness_fix/` (structural successor to the renamed 412 `GT`
project), the relion5 schemer's import job does **not** run `relion_import`. It runs the **native**
`relion_python_tomo_import SerialEM` (hyphenated flags, installed at `~/.local/lib/python3.10/.../
tomography_python_programs/`, NOT in `relion5.0_tomo.sif`), **bare**, from **project root**:
`relion_python_tomo_import SerialEM --tilt-image-movie-pattern "./frames/*.eer" --mdoc-file-pattern
"./mdoc/*.mdoc" --nominal-tilt-axis-angle <axis> --nominal-pixel-size <apix> --voltage <kV>
--spherical-aberration <Cs> --amplitude-contrast <Q0> --optics-group-name <name> --dose-per-tilt-image
<dose> [--invert-defocus-handedness] --output-directory Import/jobNNN/ --pipeline_control Import/jobNNN/`.
So `_build_import_command`'s `relion_import --do_movies --i …` is wrong on binary, flags, cwd, and odir.

**What import actually writes (tiny, 100% registry/ProjectState-derived):**
- master `Import/jobNNN/tilt_series.star` `data_global`, 8 cols, 1 row/TS: `rlnTomoName`,
  `rlnTomoTiltSeriesStarFile` (project-rel pointer), `rlnVoltage`, `rlnSphericalAberration`,
  `rlnAmplitudeContrast`, `rlnMicrographOriginalPixelSize`, `rlnTomoHand` (±1), `rlnOpticsGroupName`.
- per-TS `tilt_series/<TS>.star` block `data_<TomoName>`, 6 cols, 1 row/tilt in **acquisition order**:
  `rlnMicrographMovieName` (`frames/<movie>.eer`), `rlnTomoTiltMovieFrameCount`,
  `rlnTomoNominalStageTiltAngle`, `rlnTomoNominalTiltAxisAngle` (the **param** e.g. 84.4, NOT mdoc's
  84.36), `rlnMicrographPreExposure` (**accumulated** dose), `rlnTomoNominalDefocus` (mdoc TargetDefocus).
- Two reproduction subtleties: dose must accumulate in **acquisition order** (registry `tilt_index` /
  `pre_exposure_e_per_a2` matches the star verbatim); tilt-axis is a **persisted import param**, not
  back-derived from the mdoc. **USER DIRECTIVE: mirror the oracle's columns verbatim, no trimming.**

**Why dropping the relion binary is safe (grounded):**
- Only `fsMotionAndCtf` consumes `tilt_series.star`; it re-keys movies to **our** TiltSeriesRegistry by
  basename anyway. The particle jobs (`subtomo_extraction`/`reconstruct_particle`/`class3d`) **never read
  it** — their optimisation_set→tomograms→per-TS metadata is produced by our Warp/AreTomo adapters.
- Handedness: import's `rlnTomoHand` **dies at fsMotionAndCtf**; Warp `ts_defocus_hand` recomputes the
  value that reaches averaging (Warp XML `AreAnglesInverted`). So import's hand is moot for particles.
- Openability (deepwiki 3dem/relion, 2026-06-24): the RELION-5 GUI does **NOT** hard-fail without an
  Import node — `gui_mainwindow.cpp:1483-1505` dispatches tomo nodes by **type label**, not by import
  origin. And mandatory `tilt_series.star` global cols are only `rlnTomoName` + `rlnTomoTiltSeriesStarFile`
  (we still write the full oracle set per the user directive). RELION-as-just-a-tool at the particle stage
  is already shipping: we drive `relion_tomo_subtomo`/`reconstruct_particle`/`refine` from OUR-authored
  `optimisation_set.star` + `tomograms.star` with no relion import/align/ctf in that lineage.

**Implementation (inline-at-deploy):**
1. Writer: registry + ImportMoviesParams → `tilt_series.star` + per-TS stars via `StarfileService`,
   columns byte-matched to the oracle.
2. Run it **inline at deploy** (server process, headnode, before chain submission; `asyncio.to_thread`
   for the write). Pure metadata, sub-second, **no SLURM job, no container, no compute-node PATH question**
   (this is what dissolves the P1.A exec-env knot entirely).
3. Reroute `_build_fn_exe`/`_build_import_command` so IMPORT_MOVIES no longer emits the relion command.
4. Bookkeeping: allocate the `Import/jobNNN` slot, write the dir + `RELION_JOB_EXIT_SUCCESS` sentinel,
   set the job model **Succeeded** with its `relion_job_name`/dir; exclude it from the afterok submit set
   so `fsMotionAndCtf` submits with no afterok producer (the star is already on disk). P1.C's
   `export_relion_compat` emits the `relion.importtomo` node for GUI-openability.
5. **Runtime gate:** diff the writer's output against `post_handedness_fix/Import/job001/{tilt_series.star,
   tilt_series/*.star}` column-for-column before trusting it. (Sub-decision rejected: keeping import as a
   trivial SLURM supervisor job — buys only DAG uniformity at the cost of queue latency + a pointless
   compute round-trip for a metadata write.)

**Option C (fast-follow, not now):** drop the Import node entirely and rewire `fsMotionAndCtf` to read
the registry directly — purest "we own it", but touches io_slots + DAG + the consumer input contract.
Deferred until B proves the registry→star authoring is faithful.

**Correction to the original Phase 0.** "Stop *reading* `default_pipeline.star`"
is **not** a standalone first step. The map proves the central read
(`pipeline_runner.py:62`) is the linchpin: on the schemer path
`job_model.slurm_job_id` is `None`, so in-flight RUNNING/QUEUED status comes *only*
from reading the schemer's star; the four read-modify-write patches
(`:866,1097,1131,1214`) are load-bearing *only because* `:62` reads back; and
dropping `:62` also deletes the stop-on-fail trigger (`already_failed_in_star` —
the 2026-05-18 spurious-scancel foot-gun). So read-removal is **distributed across
P1.A–P1.C, each site dying only when its replacement lands**, and the central read
specifically is the **P1.A+P1.B cutover** — replacing the schemer as *driver* and
`sync_all_jobs`' star-diff as *status source* are one switch, not separable. The
phases below supersede the old Phase 0/1/2.

**Rollout safety:** gate the cutover behind a `use_afterok_orchestrator` flag so
P1.A+B can be built and exercised on a test project while the schemer stays the
default for existing projects. (There is no local runtime — every change is
verified by the user restarting `main.py`; a flag makes that safe.)

### P1.0 — Prerequisites (additive; no live-path behavior change)
- **Persist pipeline membership + order into `ProjectState`.** Today the
  participating-job set + submit order lives *only* in `UIState.selected_jobs`
  (`ui/ui_state.py:64`, per-browser-tab NiceGUI storage); a stateless reconciler /
  `submit_chain` has no tab. Added `ProjectState.pipeline_order: List[str]`; bumped
  `SCHEMA_VERSION (3,0)→(3,1)` (`project_state.py:58`) — additive ⇒ MINOR. Backfilled
  on load from the loaded `jobs` in file order (no `PIPELINE_ORDER` import into
  services: submit order is the `afterok` toposort, so persisted *membership* is the
  point; order is a UI nicety). Write-through from the full `selected_instance_ids`
  at deploy. _(Deferred: mirroring UI add/remove into `pipeline_order` live — it is
  refreshed at each deploy, which is when submission reads it.)_
- **`PathResolutionService.resolve_edges()`** — a read-only DAG deriver returning
  `[(producer_instance_id, consumer_instance_id)]`, reusing
  `_choose_candidate_for_slot` and reading `producer_instance_id` straight off the
  chosen `OutputCandidate` (`path_resolution_service.py:26`), filtering `manual` /
  `mergedSources` / interactive producers (no SLURM job → would be a dangling
  `afterok`). **No change to `resolve_inputs`.**

### P1.A — Direct submission (`submit_chain`), replacing the schemer
Generalize the *existing* schemer-free template `launch_retries` / `_sbatch_script`
(`pipeline_runner.py:685-756,879`) — it already sbatches a supervisor directly,
captures its id, and watches `RELION_JOB_EXIT_*`. For fresh jobs: render
`config/qsub.sh`→`run_submit.script` (today only the retry path reuses a
pre-existing one), capture `slurm_job_id` from `"Submitted batch job <N>"` and
store it on `job_model` **before** submitting dependents, and inject
`#SBATCH --dependency=afterok:<producer ids>` (from a `resolve_edges()` toposort;
normalize array ids via `normalize_slurm_ids`). crboost becomes the **sole job-dir
allocator** — move `rlnPipeLineJobCounter` into `ProjectState`, seed once at
migration, and **drop the reuse-on-rerun branch**
(`pipeline_orchestrator_service.py:117-125`, the documented off-by-one root) in the
*same* change. Replace the `_run_relion_schemer` launch at the single seam
(`pipeline_orchestrator_service.py:190`).

### P1.B — Stateless in-process reconciler, replacing `sync_all_jobs`' star-diff
Enumerate jobs from `ProjectState.pipeline_order` → read `RELION_JOB_EXIT_*` +
`.task_status` sentinels (kept verbatim; supervisors write them regardless of the
schemer) + query **`sacct`** (net-new) / `squeue` keyed on `slurm_job_id` → mutate
`execution_status` **in place** (in-memory-authoritative; never replace the bound
object). QUEUED-vs-RUNNING from `.task_manifest.json` + `task_*.out` (the existing
B1 logic). Re-root stop-on-fail on sentinel-FAILURE + `sacct` FAILED, and
**actively `scancel`** `DependencyNeverSatisfied` dependents so a hard upstream
fail can't leave the chain pending forever. **Fix the `squeue` `[]`-on-error bug
first** (B2, `slurm_service.py:299-301`) — a reconciler that reads `[]` as "no
jobs" will mark live chained jobs done on a transient CLI blip. Keep SLURM access
behind a thin method seam; default = local-CLI shell-out, exactly as
`_sbatch_script` does today.

> **P1.A and P1.B cut over together.** The instant the schemer stops launching
> (P1.A), the reconciler must own status (P1.B) — nothing maintains
> `default_pipeline.star` anymore, so `sync_all_jobs`' read at `:62` would observe
> a dead file. Develop as two modules; land as one switch behind the flag above.

### P1.C — RELION-compat export, then the remaining read-removal
`export_relion_compat(ProjectState, project_path)` authors `default_pipeline.star`
(general / processes / nodes / input+output edges from the `resolve_edges` DAG —
*new* authoring; `_write_scheme_star:310` is the closest template but only emits a
linear chain) + per-job `job.star` (generalize `generate_job_star`,
`services/jobs/_base.py:225`, redirect its target to the real `External/jobNNN`).
**Validate** by diffing against a schemer-produced star and opening the result in
the RELION GUI (risk **R-OPEN**, §7). **Then** convert the remaining reads:
`pipeline_deletion_service.load_pipeline_graph` (`:92`) mutates `ProjectState` +
regenerates the export (riskiest — cascade/orphan rebuilt from the edge graph;
feeds the UI delete-preview), and the orchestrator's counter/type reads switch to
the model.

> **Read-site removal mapped to phase** (the doc's original "Phase 0", distributed):
> **P1.A** `pipeline_orchestrator_service.py:257` (counter → `ProjectState`).
> **P1.A+B cutover** `pipeline_runner.py:62` (central read → reconciler) +
> `:866,1097,1131,1214` (RMW patches → mutate `execution_status` in place, drop the
> read). **P1.C** `pipeline_orchestrator_service.py:417,502` (job-number / type
> from the model) + `pipeline_deletion_service.py:92` (deletion graph from the edge
> DAG).

### P1.D — UI status events
In-process reconciler ⇒ in-place mutation already pushes through the existing
binds / `FingerprintedView`. Add the SLURM **queue-wait banner** (T1: "waiting for
a node — job N, pending Ym") + a **staleness** indicator (B5). The clean status
this produces is the seam for a later frontend swap (§9) — still out of scope.

### Locked decisions
- **`afterok` target = the supervisor** (it blocks on its own child array, then
  writes the markers). Exit semantics differ per driver — `fs_motion` /
  `ts_reconstruct` exit 1 if any TS fails (→ `afterok` correctly blocks downstream),
  `ts_alignment` exits 0 on partial (→ soft-pass, intended). Keep the per-driver
  behavior; the reconciler's `DependencyNeverSatisfied` cleanup ensures a hard-fail
  never hangs the chain.
- **Submit the whole chain up front** (was §8 Q1) — maximizes durability, fits the
  supervisor pattern; mid-chain failure is handled by reconciler cancel.
- **Supervisors keep blocking-waiting on their own arrays** (was §8 Q2) — `afterok`
  only orders supervisor→supervisor; the supervisor→child wait stays.
- **No SQLite/embedded DB on Lustre** (unchanged, §3) — state stays SLURM +
  sentinels + `project_params.json`.

### Rough effort & order
Land **P1.0 → P1.A+P1.B (one cutover) → P1.C → P1.D**. P1.0 is small (additive + a
schema migration). P1.A/P1.B are the core. P1.C is medium-large (the star
authoring is net-new; the deletion-cascade rebuild is the sharp edge). P1.D is
small. At the end of the P1.A+B cutover the system runs **schemer-free** with
status from the reconciler; the star is simply unmaintained until P1.C makes it a
generated export.

---

## 7. Risks & non-goals

- **We own the reconciler's edge cases now** (races, partial array completion,
  SLURM-state-vs-sentinel skew) — distributed-systems-hard. *But we already pay
  this tax as desync bugs;* doing it deliberately with one source of truth is a
  net win, not new debt.
- **Lustre visibility races (S1)** persist regardless of orchestrator — this
  rework does not fix filesystem-consistency issues. Do not expect it to.
- **Losing "open in RELION GUI mid-run"** unless we keep exporting star files
  live (cheap; keep doing it).
- **Non-goal:** a general workflow engine. We only need our DAG, our status, our
  layout.

**Verified risks from the codebase map (2026-06-23):**
- **R-OPEN (highest unknown):** with crboost as the sole `External/jobNNN`
  allocator, it is unverified whether the RELION GUI opens a project whose job dirs
  + `rlnPipeLineJobCounter` it did not allocate. Validate the export against a real
  RELION open before trusting it (gates P1.C).
- **R-DELETE:** there is **no `ProjectState` mirror** of the star's
  `pipeline_nodes`/`input_edges`/`output_edges`. Cascade/orphan deletion must be
  rebuilt from the io_slots edge graph; a wrong edge graph silently mis-cascades
  (deletes a still-consumed output, or orphans a live one) and it drives the UI
  delete-preview users act on directly.
- **R-EXIT:** supervisor exit semantics are **not uniform** (`ts_alignment` exits 0
  on partial failure; `fs_motion`/`ts_reconstruct` exit 1) — a single `afterok`
  policy is wrong; handled by per-driver behavior + reconciler cancel (§6 locked).
- **R-EDGE-SNAPSHOT:** the `afterok` edge set is snapshotted at submit, but
  `get_driver_context` re-resolves inputs at drive time; a curated/interactive
  output changing between can make the dependency and the actually-consumed path
  name different producers. Keep drive-time re-resolution as the safety net.
- **R-SACCT:** `sacct` is net-new and the only post-`squeue` terminal-state source;
  subject to the cluster's accounting purge window (§8).

---

## 8. Open questions

**Resolved into §6 (no longer open):** submit the whole chain up front (locked);
supervisors keep blocking-waiting on their own arrays (locked); `afterok`
dependents that would hang on upstream failure are `scancel`ed by the reconciler
(P1.B). VM↔cluster questions are moot for v1 (on-cluster, §4 DECISION).

**Still open:**
- **R-OPEN validation (highest):** does the RELION GUI still open the exported
  project once crboost owns `External/jobNNN` allocation (counter consistency, no
  numbering gaps)? Confirm against a real open before trusting the export. Gates
  P1.C.
- **`sacct` retention window:** the reconciler relies on `sacct` for terminal state
  after a job leaves `squeue`; if accounting purge is short, catch terminal state
  before purge or fall back to `RELION_JOB_EXIT_*`. Affects poll cadence + the
  squeue→sacct fallback. (Answerable by the admin / running `sacct` in the user's
  interactive env.)
- **Export cadence:** write the RELION-compat export live after every state change,
  or only on demand (and on an "open in RELION" action)? Lean on-demand + after
  terminal transitions; cheap either way.

---

## 9. UI (out of scope here, but enabled by this)

The NiceGUI grievance is lower-leverage and *should not* be bundled with this.
But the clean status/control API produced by Phase 2 (FastAPI REST + WS/SSE) is
the **seam** that later makes a frontend swap *incremental* (strangler-fig: new
pages in a JS/TS SPA against the same API, migrate page by page) instead of a
big-bang rewrite. Also: a chunk of current UI jank is "live status is hard when
state is server-side and reactivity is coarse" — a cleaner event-driven status
model relieves some of it *even if we stay on NiceGUI*. So this rework pays UI
dividends either way.
