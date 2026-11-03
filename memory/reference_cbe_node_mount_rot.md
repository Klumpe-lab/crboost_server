---
name: cbe-node-apptainer-mount-rot
description: "CBE compute nodes can have broken node-local mounts that kill apptainer at container creation — check task_*.out CONTAINER OUTPUT for \"FATAL: container creation failed\" before blaming code"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2720cff5-beea-4bae-8cd4-e5838c3e694d
  modified: 2026-08-14T12:24:54.122Z
---

Seen 2026-08-10 (deadcode_test fsMotionAndCtf, array job 941918): all 6 tasks failed with exit 255 at **apptainer container creation**, two distinct node-level causes:

- **clip-g3-8**: `mount /eos->/eos error: ... transport endpoint is not connected` = dead FUSE/network mount on the node. `/eos` is bound by the cluster's system apptainer.conf, NOT by crboost (grep-verified: no /eos anywhere in config/ or container_service).
- **clip-g3-4**: `mount /scratch-cbe/jobtmp/clip-g3-4/<OLD-jobid> ... doesn't exist` = stale per-job tmpdir bind entries left over from long-dead SLURM jobs (925533-935) being applied to new jobs.

Diagnosis discipline: when an array job fails 100% right after submission, read `task_*.out` "CONTAINER OUTPUT" section FIRST. If apptainer FATALs before the tool runs, it's node/cluster infra — python-side driver logs ([DRIVER_BASE]/[SUPERVISOR] lines in run.out) will show the whole crboost stack ran fine.

Remedies: resubmit (different nodes); steer away from rotten nodes via slurm_overrides constraint (g2|g4 trick from the 412 work, see [[project_412_sta_refinement_playbook]]) or node exclude; report node names to CBE admins for remount/cleanup.

Update 2026-08-12: clip-g3-8 STILL rotten (same /eos ENOTCONN) — node rot persists for days if unreported; deacode_stage006 job002 lost 3/6 tasks to it. Full-fleet scan method: `grep -l "transport endpoint is not connected"` over `task_*.out -mtime -4` under /groups/klumpe/crboost_data, then read the `Node:` line — 7/7 hits were clip-g3-8, so "random failures" collapsed to one node. Canonical stopgap: extend the existing `#SBATCH --exclude=` line in config/qsub.sh:11 (precedent: clip-g4-4 already excluded there; propagates to run_array.sh + run_submit.script). NOTE: /groups/klumpe/crboost_data WAS readable from this sandbox (contradicts [[crboost-data-not-accessible]] — check before assuming).

Update 2026-08-14: clip-g3-4 STILL rotten (same stale 925532–925535 jobtmp binds, now 4+ days) — killed all 5 ts_alignment tasks of after_refactor_test job004 in one shot because SLURM packed the whole `--array=0-4%5` onto that single node. One rotten node = 100% job failure is the norm, not the exception. Fix applied same day: `config/qsub.sh:11` exclude extended to `clip-g4-4,clip-g3-4,clip-g3-8`. Supervisor auto-retry-with-node-exclusion was proposed and REJECTED by user ("cluster-crawling abomination") — the policy is: static exclude stopgap + report rotten nodes to CBE admins, no self-healing infra workarounds in crboost. See [[feedback-no-cluster-workaround-automation]].
