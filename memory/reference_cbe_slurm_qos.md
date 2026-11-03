---
name: reference-cbe-slurm-qos
description: CBE SLURM per-job wall-time is capped by QOS (not the partition); how to select a longer one with --qos
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2936b400-4c3d-4974-839f-b74f083553e9
---

On CBE, the per-job wall-time ceiling comes from the **QOS**, not the partition. `sacctmgr -n show qos format=Name,MaxWall` (2026-07-07):

| QOS tier | MaxWall |
| --- | --- |
| `short` / `c_short` / `m_short` / `g_short` | **08:00:00** |
| `medium` / `c_medium` / `m_medium` / `g_medium` | **2-00:00:00** (2 days) |
| `long` / `c_long` / `m_long` / `g_long` | **14-00:00:00** (14 days) |
| `rapid` / `c_rapid` | 01:00:00 |
| `grid_gene+` | 14 days |

Prefix = partition family: `g_*` = GPU (`-p g`), `c_*` = CPU, `m_*` = high-mem, bare = default.

**The default QOS is `*_short` = 8 h.** A job with `#SBATCH --time` > 8h and no explicit `--qos` is rejected at
submit with `sbatch: error: QOSMaxWallDurationPerJobLimit`. To run longer on the GPU partition add
`#SBATCH --qos=g_medium` (≤ 2 days) or `#SBATCH --qos=g_long` (≤ 14 days). `rapid` (1 h) is the fast-turnaround
tier. Tradeoff: the longer tiers generally queue behind `*_short`, so only reach for `g_medium` when the job
genuinely needs > 8 h — e.g. the healpix-order-4 (1.8° **global**) 412 refines, where the exhaustive fine global
search blows past 8 h. Jobs that fit in 8 h should stay on the (faster-scheduling) default. See [[reference-hpc-env]].

**crboost (2026-09-30):** a job asks for a QOS via the SLURM tab's QOS row (`SlurmConfig.qos`, or `job_resource_profiles.<type>.qos` in conf.yaml); `write_sbatch_script` injects `#SBATCH --qos=`. Before that no crboost job could leave the 8 h default. missAlign's walltime estimate caps at the chosen QOS's probed MaxWall.

**QOS priority (sacctmgr, 2026-10-30):** g_short 1000, g_medium 500; neither has MaxJobsPU/MaxSubmitPU. Array
supervisors use g_medium (`supervisor_slurm.qos`) — accepted tradeoff: half the QOS priority for a 1-CPU job.
