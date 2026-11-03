---
name: project_platform_roadmap
description: "North-star roadmap for productionizing crboost into a persistent, multi-tenant, multi-cluster CryoSPARC-like platform"
metadata: 
  node_type: memory
  type: project
  originSessionId: 85428891-9a84-4944-a3b0-8180157a0d75
---

**SUPERSEDED 2026-09-29:** user chose per-user servers with Jupyter-style tokens (docs/roadmaps/roadmap_23-per-user-servers.md); CryoSPARC-style master rejected; VM / IT / front door parked indefinitely ("not going there anytime soon"). A short "Parked" paragraph in roadmap 23 keeps the connect-helper / OnDemand idea. See [[project_release_prep_roadmaps_2026-09-29]].

Umbrella platform-evolution roadmap at repo root `PLATFORM_ROADMAP.md` (created 2026-06-25),
written as a chapter-by-chapter arc from today's per-user login-node REST process to a
"CryoSPARC-like" multi-cluster control plane. Prompted by the cluster admin offering to
containerize + spin up a VM.

**Core thesis:** the app is secretly two planes fused into one session-pinned process — a
*control plane* (UI, ProjectState, orchestration; wants to be persistent/single/session-independent)
and an *execution plane* (submit/status/files; cluster-specific, belongs behind a narrow
`ClusterBackend` seam). Unfusing them is the whole roadmap.

**Three (only three) ways to talk to SLURM from a container/VM:** (1) be a submit host (munge socket +
binaries + slurm.conf + Lustre), (2) slurmrestd + per-user JWT, (3) SSH to a real submit host + rsync.
1/2 for the home cluster now, architect so 3 (the portable one) drops in later. The **filesystem**
(Lustre visibility), not SLURM, is the real constraint on how far the control plane can sit from the
cluster. Our `config/qsub.sh` + `SlurmConfig` placeholder templating already *is* CryoSPARC's
`cluster_info.json` pattern — generalize, don't replace.

**Two gating decisions are policy, not code:** D1 run-as/attribution (one service account submitting
all jobs breaks fairshare/quota — admins may veto; fix = per-user JWT / `sudo -u`), D2 VM placement
(routable to slurmctld? Lustre mounted? in munge domain or slurmrestd available?). Settle with admins
first.

**Chapters:** 0 persistent containerized control plane (admin's ask; systemd+reverse-proxy+VM;
control-plane container ≠ jobs-in-containers, which already happens via Apptainer) → 1 service-owned
session-independent reconciler (= the in-flight [[project_orchestrator_replacement]]; this roadmap
adds the constraint: design the reconciler as a persistent multi-user singleton from day one) →
2 identity/multi-tenancy (SSO/OIDC; ownership data model already seeded per
[[project_ownership_sharing]]) → 3 `ClusterBackend` interface + declarative cluster-profile schema
(the "runs on any SLURM cluster" refactor; kickoff = audit where SLURM/path assumptions leak past
slurm_service/path_resolution_service into drivers+UI) → 4 multi-cluster control plane w/ Postgres
(aspirational, only when a 2nd real cluster needs serving).

Doc also has: definition-of-done rubric ("playing with the big dogs"), an admin-meeting agenda, and a
current-state seam inventory (reusable: slurm_service, container_service, path_resolution_service,
qsub.sh templating, conf.yaml, ProjectState; blockers: relion_schemer child process, reading
default_pipeline.star as truth, no auth, single-writer file state).

## Index snapshot (detail moved out of MEMORY.md, 2026-08-10)

North-star arc PLATFORM_ROADMAP.md (root, 2026-06-25): unfuse control-plane (persistent UI+state+reconciler) from execution-plane (cluster-specific, behind a ClusterBackend seam). 3 ways to reach SLURM from a container/VM (submit-host+munge / slurmrestd+JWT / SSH+rsync); FS-visibility not SLURM is the real constraint; our qsub.sh templating already = CryoSPARC cluster_info.json. 2 policy gates (D1 run-as/attribution, D2 VM placement). Chapters: 0 containerize+VM+systemd → 1 service-owned reconciler (=orchestrator replacement) → 2 SSO/multi-tenancy → 3 ClusterBackend+cluster-profiles → 4 multi-cluster+Postgres. Prompted by admin offering container+VM.
