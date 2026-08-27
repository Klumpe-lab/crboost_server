# 01 · Architecture, one slide

## The whole thing

@figure. create a figure where the headnode actually contains our containers in a static state but actually get used on the cluster node when the job is dispatched. reduce the amount of text drastically. there shoul be multiple slurm compute nodes in the figure to signfiy multiplicity, but the one where the job is done being active (we want to portray the connections but also the dispatch process).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  YOUR LAPTOP                                                                │
│      browser ──────────► http://localhost:8081                              │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  SSH tunnel  (-L 8081:localhost:8081)
┌──────────────────────────────────▼──────────────────────────────────────────┐
│  CLUSTER HEADNODE — ONE long-lived python process  (main.py)                │
│                                                                             │
│   ┌───────────────┐      ┌────────────────────┐      ┌──────────────────┐   │
│   │  NiceGUI UI   │◄────►│  CryoBoostBackend  │◄────►│    services/     │   │
│   │   ui/*.py     │      │   (the facade)     │      │  state · paths   │   │
│   │  FastAPI      │      │    backend.py      │      │  orchestrator    │   │
│   │  /  /workspace│      └────────────────────┘      │  slurm · sif     │   │
│   └───────────────┘                                  └──────────────────┘   │
│                                                                             │
│   IN MEMORY : ProjectState (one per project, shared by every browser tab)   │
│               TiltSeriesRegistry (per-TS / per-frame / per-tomogram facts)  │
│   ON DISK   : project_params.json  +  a RELION-compatible project tree      │
│                                                                             │
│   Computes: NOTHING heavy.  It writes state and calls sbatch.               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  sbatch  (config/qsub.sh)
┌──────────────────────────────────▼──────────────────────────────────────────┐
│  SLURM                                                                      │
│                                                                             │
│    supervisor job (1 CPU, 4 GB)                                             │
│         │  enumerates tilt-series, writes a manifest                        │
│         └──► ARRAY JOB  [ task 0 │ task 1 │ task 2 │ … │ task N ]           │
│                            one tilt-series each, GPU each                   │
│                                     │                                       │
│                                     ▼  apptainer exec <tool>.sif …          │
│         RELION 5 · WarpTools · AreTomo · IMOD · PyTOM ·                     │
│         cryoCARE · IsoNet · miss-alignment · ChimeraX+ArtiaX                │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                   SHARED FILESYSTEM — the project directory
              (headnode and every compute node see the same paths)
```
