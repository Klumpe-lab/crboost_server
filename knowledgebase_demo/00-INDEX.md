# CryoBoost — lab tutorial knowledge base

Scratch material for the lab talk. Each file is a **section**, meant to be read on its own
and turned into a handful of slides. Nothing here is code documentation — it is the story
of what the thing does and why.

| # | Section | What it answers | Slides it feeds |
|---|---|---|---|
| [01](01-architecture.md) | **Architecture, one slide** | Where does the code live, what talks to what | 1 big diagram + 2 detail diagrams |
| [02](02-tomogram-lifecycle.md) | **What happens to a tomogram** | The physical story: movies → tilt-series → tomogram → particles → map | 1 flow diagram + per-stage "what changed" table |
| [03](03-jobs-reference.md) | **The jobs, one by one** | 16 job types: tool, in/out, knobs, gotchas | 1 slide per job (or 1 table + zoom-ins) |
| [04](04-cluster-dispatch.md) | **How work reaches the cluster** | supervisor → SLURM array → containers; who watches it | 2 diagrams + the status model |
| [05](05-ui-tour.md) | **UI tour / screenshot checklist** | Every surface in the order you'd demo it | your screenshot shopping list |
| [06](06-particles-species-picking.md) | **Species, templates, picking, curation** | The particle half of the app | 4–6 slides |
| [07](07-protocols-reproducibility.md) | **Protocols & the regression harness** | Making a run portable and re-runnable | 2–3 slides |
| [08](08-demo-runsheet.md) | **Live-demo run sheet** | Literal click order, with fallbacks | speaker notes |
| [09](09-gaps-and-limits.md) | **Honest limits** | What's WIP, what to not promise | 1 closing slide |

---

## The 30-second version (use this as slide 2)

> CryoBoost is a **web UI that runs on the cluster headnode** and turns cryo-ET tomography
> processing into a pipeline you assemble, launch, and watch — instead of a directory of
> shell scripts. It orchestrates **RELION, Warp/AreTomo, IMOD, PyTOM, cryoCARE/IsoNet** inside
> Apptainer containers, dispatches everything to **SLURM one tilt-series at a time**, and keeps
> a single JSON file as the source of truth so a project stays openable in RELION too.

Three claims worth defending on stage:

1. **Per-tilt-series parallelism everywhere.** Almost every stage fans out into a SLURM array —
   one task per tilt-series — so a 60-TS dataset does not run 60× longer than a 1-TS dataset.
2. **One state file, no hidden state.** `project_params.json` is what the UI reads, what the
   drivers on the compute node read, and what a protocol snapshot is cut from.
3. **The server computes nothing.** It writes state and submits jobs. Every heavy operation
   happens on a compute node, inside a container.

## Vocabulary to establish early (before any diagram)

| Term | Means |
|---|---|
| **Job type** | One of the 16 pipeline stages (`Motion & CTF`, `Alignment`, …) |
| **Instance** | One *placed* job in *this* project. Keyed by `instance_id`. |
| **`instance_id`** | `tsReconstruct`, or `templatematching__ribosome` — the `__` suffix names a species |
| **Supervisor** | The tiny CPU job that fans a stage out into a SLURM array |
| **Task** | One array element = one tilt-series (or one tomogram) |
| **Driver** | The Python script that runs *inside* a SLURM job (`drivers/*.py`) |
| **Species** | A particle you're after (ribosome, capsid…) — carries templates, masks, Ø, symmetry |
| **Pick list** | A named set of coordinates on one tomogram for one species |
| **Protocol** | A portable snapshot of a workflow that worked |

## Suggested talk shape (45 min)

```
 5 min   What problem — the shell-script tomography pipeline (no slides needed, just talk)
 5 min   Architecture, one slide            → 01
 8 min   What happens to a tomogram         → 02   (this is the cryo-ET content; spend time here)
 7 min   How it reaches the cluster         → 04   (the "why is this not just a GUI" part)
12 min   LIVE DEMO                          → 08   (fall back to 05's screenshots if the cluster sulks)
 5 min   Particles & picking                → 06
 3 min   Protocols / reproducibility        → 07
   +Q&A  Limits                             → 09
```

Job-by-job detail (03) is **reference material** — don't present it linearly. Put the big
table on one slide and zoom into 3 jobs you actually care about.
