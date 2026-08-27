# 04 · How work reaches the cluster

## The two-level dispatch

Every stage submits **one** SLURM job. That job is a *supervisor* — small, CPU-only — and its
whole purpose is to fan the stage out into a **SLURM array with one task per tilt-series**.

@figure. This figure can be much better. Let us portray here indeed the one job type (say with 20 tilt series) being dispatched to slurm. let's show that the headnode dispathces the supervisor and then the supervisor dispathces to multiple nodes various nodes each individaul ts. 
@ui . let us produce a small liltle set of "running per-tilt tasks" (i dont care what you name them they should just be in a runnign state under the given job type)
```
                 headnode                    │              SLURM
                                             │
   orchestrator                              │
      │                                      │
      │ sbatch config/qsub.sh                │
      └──────────────────────────────────────┼──►  SUPERVISOR JOB
                                             │     1 CPU · 4 GB · no GPU
                                             │        │
                                             │        │ 1. enumerate tilt-series
                                             │        │    (from the TS registry)
                                             │        │ 2. write .task_manifest.json
                                             │        │       {items: [TS_01, TS_02, …]}
                                             │        │ 3. sbatch --array=0-N%throttle
                                             │        ▼
                                             │   ┌──────────────────────────────┐
                                             │   │  ARRAY JOB                    │
                                             │   │  task 0 → TS_01  ┐            │
                                             │   │  task 1 → TS_02  │ GPU each   │
                                             │   │  task 2 → TS_03  │ per-task   │
                                             │   │     …            │ resources  │
                                             │   │  task N → TS_NN  ┘            │
                                             │   └───────────┬──────────────────┘
                                             │        │      │ each task:
                                             │        │      │  stage an isolated dir
                                             │        │      │  apptainer exec <tool>.sif …
                                             │        │      │  touch .task_status/TS_xx.ok
                                             │        │      ▼
                                             │        │ 4. poll squeue until the array drains
                                             │        │ 5. aggregate per-TS metadata → output star
                                             │        │ 6. touch RELION_JOB_EXIT_SUCCESS | _FAILURE
                                             │        ▼
                                             │     supervisor exits
```