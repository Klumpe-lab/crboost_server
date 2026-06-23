# Orchestrator Replacement — Design Proposal

**Status:** PROPOSAL → GREENLIT. Authored 2026-06-23 from a design discussion;
decision now made — we are building this and excising `relion_schemer`. Companion
to `../../PIPELINE_AND_ORCHESTRATOR_STATE.md` (repo root; the status-desync
diagnosis), which is the concrete motivation for this rework.

**Update 2026-06-23:** Durability resolved — an off-cluster **VM (Tier 4)** was
requested as the brain's home. The generic admin-question menu in §4 is
superseded by the VM↔cluster integration questions now recorded there (see the
DECISION block at the top of §4 — the project-FS-mount question is the first
thing to nail next session, before any reconciler code is written).

**One-line thesis:** Stop using `relion_schemer` to *drive* execution. Make
`ProjectState` the sole runtime truth, let **SLURM job dependencies** be the DAG
engine (SLURM is already our real scheduler), reduce our own code to a thin
**stateless reconciler/observer**, and demote `default_pipeline.star` / `job.star`
from live state to a generated **RELION-compatibility export**.

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

### DECISION (2026-06-23): off-cluster VM (Tier 4)

A VM has been requested as the brain's home: server + reconciler live on an
always-on VM, not on a login node. The tier menu below is retained as rationale,
but the choice is made; the generic admin questions further down are mostly moot.
The questions that now matter are **VM↔cluster integration**:

- **Does the VM mount the cluster shared filesystem (Lustre / the project dir)?**
  RW, RO, or not at all? **This is the single biggest fork.** The reconciler
  reads on-disk sentinels (`.ok/.fail/.skip`) + `project_params.json`, and the UI
  serves visualization artifacts (PNGs, star files, cutouts) straight from the
  project tree. If the VM mounts the project FS → simplest (reconciler + UI read
  it directly). If NOT → we must split the brain (hybrid fallback below). Resolve
  this *before* writing reconciler code — it dictates where the reconciler runs.
- **How does the VM reach SLURM?** SSH-hop to a submit host (we already have an
  ssh-hop pattern from the ArtiaX REST bridge — `StrictHostKeyChecking=no`, el7
  OpenSSH 7.4) vs **`slurmrestd`** + token.
- **Network / identity:** is a submit host (or the REST port) reachable from the
  VM? Does the VM submit *as* the user (ssh key / token), and under whose SLURM
  account does job accounting land?
- **Hybrid fallback (if no project-FS mount on the VM):** keep the reconciler
  ON-cluster (scrontab or a service-partition job, Tier 3) where it can read the
  sentinels, and run only the UI/API on the VM against an on-cluster state/API
  endpoint. Brain splits into "on-cluster observer" + "off-cluster UI."

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

## 6. Phased plan

- **Phase 0 (days):** durability quick-win (Tier 1/2 per §4); stop *reading*
  `default_pipeline.star` (only ever write it). De-risks with no commitment.
- **Phase 1 (~1–2 wks):** submit the linear chain from `ProjectState` via SLURM
  `afterok`; reconciler reads `sacct` + sentinels → `ProjectState`; keep
  generating star files as export. Run one real project schemer-free.
- **Phase 2 (~1–2 wks):** status events → UI over WS/SSE; restart/continue/cancel
  semantics; retries.
- **Phase 3 (optional, later):** incremental frontend migration behind the new
  API (see §9).

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

---

## 8. Open questions

- Submit the **whole chain up front** (max durability) vs **step-by-step**
  (more flexible, needs durable loop)? Current lean: up-front chain of
  supervisors, each doing internal fan-out, since it maximizes durability and
  fits the existing supervisor pattern.
- Do supervisors keep **blocking-waiting** on their arrays (holding a light CPU
  node), or do we restructure so the merge step is a separate
  dependency-gated job? Optimization, not required for v1.
- Failure handling for `afterok` chains: dependents stay pending forever on
  upstream failure — need explicit cleanup/cancel.
- VM↔cluster integration questions in §4 (durability itself is decided: VM).
  The project-FS-mount-on-VM question gates the reconciler's location — answer
  first.

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
