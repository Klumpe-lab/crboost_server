# Platform Roadmap — From Lab Tool to Multi-Cluster Platform

*"Playing with the big dogs": the long-term arc from a per-user login-node REST process to a
persistent, multi-tenant, CryoSPARC-like control plane that can drive any SLURM cluster.*

> **How to read this.** This is a north-star document, not a sprint plan. It is organized as
> chapters — each a coherent milestone that ships value on its own and de-risks the next. Read the
> thesis and the two reference sections first; they explain *why* the chapters are ordered the way
> they are. Status markers: ☐ not started · ◐ in flight · ☑ done. Chapters are roughly sequential,
> but 0/1 overlap and 2 can start early.

---

## The thesis: one process today is secretly two planes

Stop thinking of CryoBoost as "a server that talks to SLURM." It is **two planes fused into one
process and pinned to one user's SSH session**:

- **Control plane** — the UI, `ProjectState`, orchestration/reconciliation, the job graph. This is
  *our* software. It wants to be **persistent, single, and session-independent**.
- **Execution plane** — "submit this job script to *this* cluster, tell me its status, move these
  files." This is **cluster-specific** and belongs behind a narrow interface.

Nearly every limitation we have — port-forwarding, per-user processes, jobs that die when an SSH
connection drops, the inability to point at a second cluster — is a symptom of the fusion. **Unfuse
the planes and the rest is mechanics.** The whole roadmap is the staged unfusing.

We are already ~60% of the way to the right seams: `services/computing/slurm_service.py`,
`services/computing/container_service.py`, `services/path_resolution_service.py`, and especially the
`config/qsub.sh` placeholder-templating (`XXXextra1XXX`…`XXXextra8XXX`) substituted from
`SlurmConfig` at submit time. That template-render-and-submit pattern is *exactly* how the big dogs
integrate with clusters (see CryoSPARC mapping below). The gap is persistence, multi-tenancy, and
turning "which cluster" into data rather than a hardcode.

---

## North-star architecture

```
                         ┌──────────────────────────────────────────────┐
        browsers ──SSO──▶│            CONTROL PLANE  (persistent)         │
        (no port-fwd)    │   reverse proxy ──▶ NiceGUI / FastAPI          │
                         │   ProjectState · reconciler loop · job graph   │
                         │   identity · project registry · audit          │
                         └───────────────────────┬──────────────────────-┘
                                                 │  ClusterBackend interface
                                                 │  submit · status · cancel · stage · resolve_path
                          ┌──────────────────────┼──────────────────────┐
                          ▼                       ▼                      ▼
                   ┌─────────────┐         ┌─────────────┐        ┌─────────────┐
                   │  CLIP / IMP │         │  cluster B  │        │  cluster C  │
                   │ submit-host │         │  SSH + rsync│        │ slurmrestd  │
                   │  + Lustre   │         │  (off-clstr)│        │  + JWT      │
                   └─────────────┘         └─────────────┘        └─────────────┘
                        EXECUTION PLANE — one connector profile per cluster
```

The control plane is one persistent service. Each cluster is a **connector** (a `ClusterBackend`
implementation + a declarative profile). Today we have exactly one connector, hardwired and inlined.
The arc below pulls it out and makes it plural.

---

## Reference A — the three (and only three) ways to talk to SLURM from a container/VM

SLURM does not let arbitrary hosts submit; the submitter must authenticate into the cluster's trust
domain. From a VM or container there are exactly three patterns. This is the whole decision space for
"how do you containerize something that talks to SLURM."

| # | Pattern | What the host needs | Pros | Cons | Use when |
|---|---------|--------------------|------|------|----------|
| 1 | **Be a submit host (munge)** | SLURM client binaries, `slurm.conf`, `/run/munge` socket, route to `slurmctld`, shared FS mounted | `sbatch` "just works"; zero change to submit code | VM enters cluster trust domain; SLURM version must match; admin-gated | Home cluster, VM on-cluster |
| 2 | **slurmrestd + JWT** | HTTP reachability to `slurmrestd`; a per-user JWT | clean HTTP boundary; **per-user tokens also solve attribution**; no munge in app | site must enable it; JWT lifecycle; still need FS for I/O | Home cluster if slurmrestd is available |
| 3 | **SSH to a real submit host** | SSH client + credentials; SFTP/rsync for files | works against **any** cluster you can SSH to; sidesteps munge; trivially containerizable | credential handling; SSH overhead; cluster FS is remote | Off-cluster / multi-cluster portability |

**Recommendation:** pattern **1 or 2 for the home cluster now**, architect the seam so **3 drops in
later**. Pattern 3 is the one that eventually gets us to "connect to any SLURM cluster."

## Reference B — the filesystem is the real constraint, not SLURM

RELION/Warp projects are directory trees that compute nodes read and write on Lustre. Wherever the
control plane runs, it must *also* see those dirs (write `job.star`, read `default_pipeline.star`,
resolve I/O slots). Three scenarios, in order of difficulty:

1. **Service on-cluster, Lustre mounted** → app reads/writes project dirs directly, like today.
   Easiest. **This is what the admin's VM offer gives us.** Target for Chapters 0–2.
2. **Service off-cluster** → must stage data over SSH/SFTP/rsync, or keep bulk data on each cluster's
   FS and sync only metadata. This is the true cost of going multi-cluster. Chapter 3–4.
3. **Hybrid** → control metadata lives in the service DB; bulk data stays per-cluster and is never
   moved between clusters unless required. This is what actually scales. Chapter 4.

The FS scenario, more than the SLURM pattern, dictates how far the control plane can physically sit
from the cluster.

## Reference C — CryoSPARC is the proven map (use it with the admins)

We called this "CryoSPARC for cryo-ET"; CryoSPARC's architecture is the de-risked reference for
exactly this problem:

- A persistent **master** (web UI + DB + scheduler) on a management node/VM — **not per-user, not on
  a login node.**
- **Lanes** = clusters/partitions chosen at submit time.
- Cluster integration via a templated `cluster_script.sh` + `cluster_info.json` of `sbatch`/`squeue`
  /`scancel` command templates the master renders and runs.

That last bullet is **our `config/qsub.sh` + `config/conf.yaml` already.** The distance between us and
the CryoSPARC topology is *not* the SLURM integration — we have that. It is (a) the master is
persistent and shared instead of per-user, and (b) "lane" becomes a cluster-profile abstraction.
Framing the admin conversation as "I want CryoSPARC's master/lane topology" makes them evaluate a
known-safe pattern instead of a novel one-off.

---

## The two decisions that are policy, not code

These gate the whole project and must be settled **with the cluster admins**, not in our repo.

**D1 — Run-as / job attribution (the big one).** If one service account submits everyone's jobs, then
on Lustre every file is owned by `svc_crboost` and SLURM fairshare/accounting bills all compute to one
account. Admins hate this; it breaks quotas and fairshare and may get the whole thing vetoed. Jobs
must run **as the actual user**. Mechanisms, best-first:
1. **slurmrestd per-user JWT** — user authenticates to our UI; we submit with *their* token. Cleanest;
   also gives us identity for free (ties to Chapter 2).
2. **`sudo -u <user> sbatch`** — service account granted scoped sudo by admins.
3. **Per-user SSH / delegated credential** — works but storing user keys is unpleasant.

**D2 — Where the VM sits.** Three yes/no answers pick our submission pattern automatically:
- Routable to `slurmctld` / `slurmrestd`?
- Is **Lustre mounted** on it?
- Is it in the **munge** trust domain, or is **slurmrestd** available?

Everything downstream — which `ClusterBackend` we build first, whether FS is local or staged — falls
out of D1 and D2. **Settle these first.**

---

# The chapters

## Chapter 0 — A persistent, containerized control plane  ☐

*The admin's literal ask ("pack it in a container, we'll spin up a VM"). The single biggest
quality-of-life jump for the least architectural risk.*

**Goal.** One persistent instance of the server, reachable by the whole lab without port-forwarding,
that does not die with anyone's SSH session.

**Unlocks.** Kills port-forwarding and per-user processes immediately. Establishes the host where the
session-independent reconciler (Ch. 1) will live.

**Work.**
- Dockerfile for the FastAPI/NiceGUI app (the *control plane* — see the boundary note below).
- Run it under **systemd** on the admin's VM (auto-restart, boot persistence, journald logs).
- Front it with a **reverse proxy** (Caddy/nginx) terminating TLS, **passing WebSockets** (NiceGUI is
  WS-driven), and — Chapter 2 — doing SSO.
- Keep submission as-is if the VM is a submit host with Lustre mounted (pattern 1, scenario 1).

**Critical boundary note.** *Containerizing the control-plane service ≠ jobs running in containers.*
The tools (RELION/Warp) **already** run in Apptainer on compute nodes — that does not change. Ch. 0 is
about wrapping *our long-running service*, a different concern. Keep this distinction crisp; admins
routinely conflate the two and it makes us sound sharp to separate them.

**Decisions/obstacles.** D2 (VM placement). VM sizing — a few cores + 8–16 GB RAM is plenty for a
NiceGUI master serving a lab; heavy lifting stays on compute nodes. NiceGUI is single-process,
server-side state → keep **one replica** (file-based `ProjectState` + in-memory registry assume a
single writer).

**Login-node stopgap (bridge only).** If the VM is weeks out: `systemd --user` with **lingering**
survives logout on some sites; `tmux`/`nohup` are fragile. Persistent daemons on login nodes are an
anti-pattern admins reap — this is exactly what the VM offer is steering us away from. Treat as a
bridge, not a destination.

**Exit criteria.** Lab members open one URL (SSO or temporary basic-auth), no port-forwarding; the
server survives a reboot via systemd; a closed laptop does not kill the web service.

---

## Chapter 1 — Service-owned orchestration (session-independent)  ◐

*This is the in-flight orchestrator replacement, re-framed as the technical heart of persistence.
See `services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md` (greenlit 2026-06-23).*

**Goal.** Job orchestration runs as a **service-owned singleton background loop**, reconciling
`ProjectState` ↔ SLURM ↔ FS, independent of any browser/SSH session.

**Unlocks.** "My jobs don't die when I close my laptop" — literally. Removes `relion_schemer` as a
per-session child process; makes `ProjectState` the sole source of truth (stops reading
`default_pipeline.star` as truth); enables a thin stateless reconciler over a SLURM `afterok` DAG.

**Work (per the existing plan).** P1.0 persist `pipeline_order` + `resolve_edges` → P1.A
`submit_chain` → P1.B reconciler → P1.C export RELION-compat star + remove the central
`default_pipeline.star` read (`pipeline_runner.py:62`) → P1.D UI.

**The one constraint this roadmap adds to that plan.** Design the reconciler assuming it lives in a
**persistent, multi-user** service from day one. Do **not** bake in single-user or
"owned-by-the-client-that-launched-it" assumptions. The reconciler is a process-level singleton, not a
per-tab object. (Cross-check against the known failure mode in
`feedback_inmemory_state_authoritative.md`: in-memory state is authoritative; disk re-reads must be
load-if-absent so the loop's updates aren't clobbered.)

**Depends on.** Best sequenced *with* Ch. 0 (the reconciler wants the persistent host to live in). It
technically runs today inside the per-user process, but its value is only realized once the host is
persistent.

**Exit criteria.** No `relion_schemer` child process; a job DAG submitted, then the submitting browser
closed, still advances to completion and is correctly reflected when any user reopens the project.

---

## Chapter 2 — Identity & multi-tenancy  ☐ (data model partially seeded)

*Going from "one process per user" to "one service for the lab" makes identity load-bearing.*

**Goal.** Real authenticated identity; jobs and projects attributed to actual users; per-user views
plus shared lab projects.

**Unlocks.** Honest accounting/attribution (prerequisite for D1's run-as-user); a shared service the
admins will actually sanction; the foundation for any audit trail.

**Work.**
- **Authn** via the reverse proxy → institutional **SSO/OIDC** (Keycloak or the institute IdP). The
  authenticated principal replaces today's honor-system `owner`.
- Wire that identity into **D1's run-as submission** (JWT/`sudo -u`).
- Make project listing/registry **user-aware**. The data model already has a head start: mutable
  `owner` vs immutable `created_by`, `SHARED_OWNER="@lab"`, create-as-shared toggle + roster transfer
  (landed 2026-06-23; see `project_ownership_sharing.md`). What's missing is *real* auth behind it.

**Decisions/obstacles.** No auth exists today. NiceGUI server-side state is fine for tens of users,
not hundreds. Reverse proxy must keep WS + sticky sessions if we ever add replicas (we shouldn't yet).
Security posture: a shared service that can submit arbitrary cluster jobs is a higher-value target —
the templated submit path must stay **injection-safe** (placeholder substitution, never shelling
unsanitized user strings), and job params need validation.

**Exit criteria.** A user logs in via SSO; the projects they see and the jobs they submit are bound to
their real cluster identity; lab-shared projects are visible to the lab; nothing runs as a generic
service account on the compute side.

---

## Chapter 3 — The `ClusterBackend` abstraction + cluster profiles  ☐

*The refactor that turns "which cluster" from a hardcode into data. This is what "runs on many SLURM
clusters" actually means in code.*

**Goal.** A narrow `ClusterBackend` interface with multiple implementations, plus a declarative
per-cluster **profile**, so adding a cluster = writing a profile + testing, touching no core logic.

**Interface (sketch).**
```
submit(job_spec)        -> job_id
status(job_id)          -> JobStatus
cancel(job_id)
stage_in / stage_out    -> move files (no-op when FS is local; rsync/SFTP when remote)
resolve_path(...)       -> map logical project paths to cluster-physical paths
fs.read / write / glob  -> abstract the project FS
```
Implementations: `SubmitHostBackend` (pattern 1, local FS), `SlurmRestBackend` (pattern 2),
`SshBackend` (pattern 3, remote FS). Today's behavior becomes `SubmitHostBackend`.

**Cluster profile schema.** A declarative description of one cluster, the seed of which is already
`conf.yaml`. Promote every hardcode into it:
- submit mechanism (which backend) + endpoint/credentials,
- module system (Lmod/tmod/none) and **module names**,
- container runtime (Apptainer/Singularity/Docker), `--nv` GPU flags, bind mounts,
- partition / QOS / account / GRES syntax, walltime caps, constraints,
- FS layout (scratch vs project vs home, writable roots, quota),
- the `qsub.sh`-style submit template (already templated — generalize the placeholders).

**Kickoff task (the code-grounding I offered).** Audit where SLURM calls and path assumptions
currently **leak past** `slurm_service.py` / `path_resolution_service.py` / `container_service.py`
into drivers, UI, and orchestration. Every leak is a place that silently assumes "SLURM is local" or
"the FS is local." The audit output *is* the `ClusterBackend` method list.

**Decisions/obstacles.** The drivers (`drivers/`) run on compute nodes and load `project_params.json`
via `driver_base.get_driver_context()` — they assume the shared FS. That assumption is fine *inside* a
cluster but is the boundary the `stage_*`/`resolve_path` methods must honor when the control plane is
remote. Network reachability to `slurmctld`/`slurmrestd` is often firewalled off-site → SSH (pattern
3) is the universal fallback.

**Exit criteria.** The home cluster runs as one profile through `SubmitHostBackend` with zero
behavior change; a second profile (even a throwaway test SLURM, or CLIP via SSH) submits a trivial job
end-to-end with **no changes to core orchestration code**.

---

## Chapter 4 — Multi-cluster control plane (the "big dogs" end state)  ☐ aspirational

*Only build when a second real cluster needs serving. Named here so Chapters 0–3 don't accidentally
foreclose it.*

**Goal.** A central control plane (possibly off-cluster) driving N clusters via per-cluster
connectors, with project metadata in a real DB and bulk data staying per-cluster.

**Work (when the time comes).**
- **State store**: migrate the authoritative registry from per-project JSON + in-memory map to
  **Postgres** (file-based single-writer state breaks across replicas / off-cluster). Keep `job.star`
  /RELION-compat dirs as an *export*, not the source of truth (Ch. 1 already moves us here).
- **Per-cluster connectors** = Ch. 3 backends, now several live at once; "lane" selection at submit.
- **Data staging / locality**: scenario-3 hybrid — metadata central, bulk data per-cluster, explicit
  cross-cluster transfer only when asked.
- Optional: horizontal scale (multiple control-plane replicas) → requires the DB + sticky-session/WS
  work flagged in Ch. 2. Kubernetes only if the institute already runs it; **not** needed for a lab.

**Exit criteria.** One URL; a user picks a cluster lane per project/job; two clusters are driven
concurrently from a single control plane; losing the control-plane host loses no project state
(it's in Postgres).

---

## What "playing with the big dogs" actually means — definition-of-done rubric

A platform-grade cryo-ET pipeline manager, checklist form:

- [ ] **Persistent** — survives reboots and logouts (systemd; Ch. 0).
- [ ] **Session-independent orchestration** — jobs advance with no browser open (Ch. 1).
- [ ] **Authenticated, multi-tenant** — real identity, per-user attribution, shared projects (Ch. 2).
- [ ] **Honest accounting** — jobs run as the real user; fairshare/quota intact (D1; Ch. 2).
- [ ] **Cluster-agnostic** — "which cluster" is a profile, not a hardcode (Ch. 3).
- [ ] **Portable submission** — at least one of submit-host / slurmrestd / SSH backends per cluster (Ch. 3).
- [ ] **Durable state** — authoritative store survives host loss (Postgres; Ch. 4).
- [ ] **Safe** — injection-safe submit path, validated job params, no shared submission account.
- [ ] **Observable** — centralized logs (journald → log aggregation), per-job audit trail.

We hit "lab-grade platform" at Ch. 2, and "multi-cluster product" at Ch. 4.

---

## Agenda for the cluster admins (the meeting Alex offered)

1. **Where will the VM live** — routable to `slurmctld`/`slurmrestd`? **Lustre mounted**? **In the
   munge domain**, or is **slurmrestd available**? (Picks our submission pattern — D2.)
2. **How are jobs attributed to real users** — slurmrestd per-user JWT, `sudo -u` for a service
   account, or another sanctioned mechanism? Flag the **fairshare/quota** stakes so they know we get
   why it matters. (D1 — the likely gate.)
3. **Institutional SSO/OIDC** endpoint we can put a reverse proxy in front of? (Ch. 2.)
4. **VM sizing** — a few cores + 8–16 GB RAM; heavy lifting stays on compute nodes.
5. **Framing** — "CryoSPARC master + cluster-lane topology" so they're evaluating a known pattern.
6. **Distinction** — we're containerizing the *control-plane service*; the *jobs* already run in
   Apptainer on compute nodes. Different concern.

---

## Appendix — current-state inventory (the seams we already have)

Honest read of what's reusable vs. what blocks us, as of this writing:

**Already pointing the right way:**
- `services/computing/slurm_service.py` — `SlurmConfig`, presets, submission/query. The submit seam.
- `services/computing/container_service.py` — `apptainer exec` wrapping per tool. Good per-tool config
  model to generalize into cluster profiles.
- `services/path_resolution_service.py` + `io_slots.py` — typed path resolution; the natural home for
  `resolve_path` / `stage_*`.
- `config/qsub.sh` + `SlurmConfig` placeholder substitution — *is* the CryoSPARC `cluster_info.json`
  pattern. Generalize, don't replace.
- `config/conf.yaml` + `configs/config_service.py` — typed `Config`, singleton, per-tool config with
  legacy aliases. The seed of the cluster-profile schema.
- `ProjectState` + `StateService` (asyncio-locked persistence) — single source of truth, already the
  direction Ch. 1 formalizes.
- Ownership model — mutable `owner` / immutable `created_by` / `@lab` shared projects (2026-06-23).
  Data model for Ch. 2 minus the auth.

**Blocks / assumptions to retire:**
- `relion_schemer` as a per-session child process → Ch. 1 removes it.
- Reading `default_pipeline.star` as truth (`pipeline_runner.py:62`) → Ch. 1 cutover.
- No auth — honor-system attribution → Ch. 2.
- File-based state + in-memory registry assume a **single writer** → fine until Ch. 4, then Postgres.
- SLURM/path assumptions that leak past the service seams into `drivers/` and UI → Ch. 3 audit.
- Drivers assume the shared FS via `driver_base.get_driver_context()` → fine on-cluster; the boundary
  `stage_*`/`resolve_path` must honor when remote.

**Related plans this umbrella points at:**
- `services/scheduling_and_orchestration/ORCHESTRATOR_REPLACEMENT_PLAN.md` — Chapter 1, in flight.
- `PIPELINE_AND_ORCHESTRATOR_STATE.md` — the status-layer diagnosis behind Chapter 1.
- `PARTICLE_PROJECT_ROADMAP.md` — orthogonal feature track; unaffected by this arc.
