---
name: project_missalign_perf_walltime_qos
description: "missAlign (miss-alignment) perf tuning + walltime/QOS saga on for_<pi>_Vermicelli — dataloaders/num_gpus/steps_per_epoch params, walltime estimator rewrite, live QOS integration"
metadata: 
  node_type: memory
  type: project
  originSessionId: 62ec17db-127f-4b70-a702-c91ee591da78
---

2026-07-30, Vermicelli project (`/groups/klumpe/crboost_data/for_<pi>_20260121_Vermicelli`, External/job012-014 = missAlign on 4 TS). All code-clean (py_compile+ruff), **PENDING RUNTIME** (needs server restart — no auto-reload; sandbox has no numpy/sacctmgr).

**Root failures found & fixed (drivers/miss_align.py + services/jobs/miss_align.py):**
1. **getpwuid crash** → see [[reference_container_getpwuid_cleanenv]] (USER/LOGNAME + TORCHINDUCTOR_CACHE_DIR in the in-container env prefix).
2. **Data-loading NOT the bottleneck** — the Lightning "num_workers may be a bottleneck" line is a generic nag; raising dataloaders 1→6 barely moved per-step (2.78→3.1s). The real wall = **GPU compute per step on RTX** (recon shares the training GPU) × 30000 steps/iter.
3. **Per-step ~3.3s on RTX (g3)**; A100 (g4) ≈ ~1s. Jobs kept landing on g3 (constraint g2|g3|g4, num_gpus=1).

**New USER_PARAMS added (all render via default_renderer, persist in project_params.json):**
- `num_gpus` (1-4, default 1): drives `--gres=gpu:N` + driver train/recon split (GPU0=training, rest=recon so they stop contending). Driver reads ACTUAL allocation (SLURM_GPUS_ON_NODE→CUDA_VISIBLE_DEVICES→param) so a manual gres override still maps right.
- `dataloader_workers` (0=auto=min(cpus-2, pool//(2*batch)); pinned value clamped to pool cap, warns if >cpus).
- `steps_per_epoch` (default **250**, was hardcoded 1000): the #1 walltime lever. steps_per_epoch × max_epochs = steps/iter.

**Walltime estimator REWRITTEN** (`_scaled_walltime`): old `12min × n_ts × n_iters` was ~8× too low AND wrongly scaled TRAINING by n_ts (training is a fixed pooled step budget, n_ts-INDEPENDENT; only the alignment phase is per-TS). New model = `base + n_iters × (steps_per_epoch×max_epochs×_WALLTIME_PER_STEP_SEC(3.5, RTX-conservative) + 5min×n_ts)`. Warns loudly if ONE iteration > QOS cap (the stuck-loop condition).

**THE STUCK-LOOP INSIGHT:** job checkpoints ONCE PER MACRO-ITERATION. If requested walltime < one iteration's time, it dies mid-iter-1 → no checkpoint → resume restarts iter 1 forever = zero progress. job014 requested 3:42 but one iter = ~6.9h → doomed. `_WALLTIME_CAP_MIN`=24h.

**QOS was the real ceiling:** `_qos_safe_cap_minutes()` read `supervisor_slurm_defaults.time` = **4h default** (SupervisorSlurmConfig.time="4:00:00"; that field is shared by ~10 job supervisors — DON'T repurpose/bump it). One RTX iter (6.9h) > 4h cap → can't fit even with correct estimate. But the partition really allows ≥8h (**job012 ran at pinned 8:00:00, not rejected**) — 4h was just an over-conservative crboost default.

**Live QOS integration (LANDED, PENDING RUNTIME):**
- `services/computing/slurm_service.py`: `QosLimit` dataclass, `get_user_qos_limits()` (sacctmgr `show assoc user=$USER format=QOS,DefaultQOS` → names+default; `show qos format=Name,MaxWall,MaxTRESPerJob,MaxTRESPerUser` queried ALL + filtered in Python since positional name-filter syntax varies). Module cache `_qos_maxwall_cache_min` + sync getter `get_cached_qos_maxwall_minutes()` (populated by the landing probe; `_QOS_UNLIMITED`=100000; 0=unprobed).
- `_qos_safe_cap_minutes()` now PREFERS the cached real default-QOS MaxWall, falls back to the 4h config default. So once the landing page has probed, missAlign auto-requests up to the real QOS (8h) → covers one 6.9h iteration. Depends on the landing probe running before submit (normal flow); manual fallback = pin time=8:00:00 in the SLURM tab.
- `ui/landing_status_strip.py`: SLURM popover REDESIGNED (2nd pass after user rejected v1). **User feedback on v1: (a) popover was night-owl DARK (#0f172a) — jarring vs the gray/blue UI; (b) lingered until click-away/esc; (c) QOS lines unaligned free-text, "160 gpu/job" nonsense; (d) missing the actual cluster GPU inventory.** Fixes: `_POPOVER_STYLE` now LIGHT (white/#e2e8f0 border/slate text/#2563eb accent) matching CLR_* palette; QMenu kept transparent, light card is inner div. Hover-card interaction (open on mouseenter, close on BOTH trigger+menu mouseleave, menu.mouseenter re-opens so copy stays reachable) — PENDING RUNTIME flicker check on trigger→menu gap. **Cluster layout** built from get_partitions_info + get_nodes_info: per partition, GPU-type groups aggregated from per-node gres (`4× A100 · 2 nodes · [g4]`) so CBE's heterogeneous `g` partition reads clearly; feat chip tooltip = `--constraint=gN`. **QOS as aligned 3-col grid** (QOS ★default | MAX WALL/JOB | GPUS/JOB); gpus/job shown only when 0<n<=16 (bigger = "—", not a real per-job cap — kills the "160" noise). Every section has a `_section_header(title, cmd)` with a terminal-icon tooltip showing the exact sinfo/sacctmgr command. (UNVERIFIED visually — no render/sacctmgr access.)

**Recommended recipe for 4 TS:** A100 (g4 constraint) + num_gpus=2 + steps_per_epoch=250 → ~2h/iter fits. On RTX must drop max_epochs to ~14 to fit one iter under 4h, OR raise the QOS cap.
