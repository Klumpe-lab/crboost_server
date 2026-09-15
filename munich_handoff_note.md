# Handoff notes for the Munich installation

What will differ on your cluster from ours, and what to edit. The general install steps are in
[README.md](README.md). `venv/bin/python3 preflight.py` creates the two config files and then checks most of
what is below.

## 1. Python environment — build your own

**Ours is not portable.** Our `venv/` was created from a module-loaded interpreter
(`module load python/3.11.5-gcccore-13.2.0`), and several scientific packages — numpy, pandas, scipy,
requests — are *not* installed in the venv at all: they come from `module load arrow/16.1.0-gfbf-2023b`
(an EasyBuild 2023b stack). The venv only works while those modules are loaded, which is why our
`qsub.sh` loads them on every compute node.

What you need:

- A Python **3.11** venv at exactly **`<repo>/venv`**, with `venv/bin/pip install -r requirements.txt`.
  `requirements.txt` now lists everything the code imports, including numpy/pandas/scipy/requests and
  torch/torchvision (for the DL tilt filter). Only some versions are pinned; numpy/pandas/scipy are not —
  if your module stack already provides them, use those and load the same modules in `qsub.sh`.
- The repository and the venv on a filesystem the **compute nodes** can see.

Why the location matters: every pipeline job runs its driver on the compute node as

```
export PYTHONPATH=<repo>; <repo>/venv/bin/python3 <repo>/drivers/<job>.py ...
```

(`services/jobs/spec.py`, `driver_launch_prefix`). If `<repo>/venv/bin/python3` does not exist it falls back
to whatever `python3` is on the node's PATH. The `crboost_python` key in `conf.yaml` is informational only.
The heavy tools (WarpTools, AreTomo, RELION, PyTOM, cryoCARE, IsoNet) run inside Apptainer, but the drivers
themselves — star-file handling, the DL tilt filter (torch) — run in this venv on the node, so it must start
there with all its packages.

## 2. `config/qsub.sh`

Created by preflight from `config/qsub.template.sh` (which is our production script with the cluster
specifics commented out). Edit:

1. **SLURM HEADER** — replace our commented Lmod lines with whatever your cluster needs so that
   `<repo>/venv/bin/python3` starts (same Python module the venv was built with, plus any module providing
   packages you did not pip-install) and `apptainer` is on PATH.
2. **Account / QOS** — add `#SBATCH --account=…` and/or `#SBATCH --qos=…` if your cluster requires them.
   `--exclude` is there commented out if you ever need to keep jobs off broken nodes.
3. **GPU request format** — jobs request `--gres=gpu:1` (from `conf.yaml`). If your cluster wants typed
   gres (`gpu:a100:1`) or `--gpus`, change the `gres` values in `conf.yaml` (section 3), not the template.

Do **not** change: the `XXX…XXX` placeholders, the `RELION_JOB_EXIT_*` if/else block (matched byte-for-byte by
`drivers/array_job_base.py`), or the final `exit $EXIT_CODE` (job dependencies chain on it). Preflight
checks all three.

## 3. `config/conf.yaml`

Created by preflight from `config/conf.template.yaml` with `crboost_root` filled in. Everything cluster- or
site-specific needs your values:

- `local.DefaultProjectBase` — where projects are written (optionally the two data globs).
- `slurm_defaults` — GPU `partition`, `constraint` (usually `""`), `gres`, `mem`, `time`.
- `supervisor_slurm` — the lightweight supervisor job of per-tilt-series array jobs; point it at a
  partition you have (a CPU partition is fine). **Careful:** if you delete a partition/constraint key, the code
  silently falls back to *our* names — partition `g`, constraint `g2|g3|g4`
  (`services/configs/config_service.py`). Set them explicitly; preflight checks every partition against `sinfo`.
- `job_resource_profiles` — per-job mem/time/gres. Our times are tuned to our QOS wall-time limits.
- `use_afterok_orchestrator: true` — keep it: this is what we run (jobs chained with SLURM `afterok`
  dependencies). `false` selects the older `relion_schemer` path.
- `tools:` — the `.sif` path of every container you built (see the table in README section 4). `cistem`
  is a native binary, only needed to simulate a template from a PDB model; it will be something like
  `/groups/<name_of_group>/software/cisTEM/bin/simulate`.
- `curation:` — ChimeraX/ArtiaX manual-picking sessions (optional): `sif_path`, and a `partition` you have.
- `species_catalog_root` — optional shared species catalog; leave `""` to switch it off.

Settings changed in the UI's config dialog are written to `~/.crboost/conf.yaml`, which overlays
`config/conf.yaml` for that user.

## 4. Container bind mounts (in code, not config)

Containers only see the paths `services/computing/container_service.py` binds (around line 190–201):
`/tmp`, `/scratch`, `$HOME`, the job directory, and — if they exist — `/groups`, `/programs`, `/software`,
plus the SLURM/munge/passwd paths. **If your raw data or project base lives anywhere else** (e.g. `/ptmp/…`,
`/u/…`), tools inside the containers will not find the files: add those roots to the `hpc_paths` list at
line 201.

## 5. Protocol templates and masks

The two protocol bundles ship without their volumes (`*.mrc` is gitignored). Before creating a project
from a protocol, place these files — applying a protocol reports `asset missing` otherwise, and it checks
that each template's MRC header voxel size is **11.80 Å** (the protocols reconstruct at
`rescale_angpixs: 11.8`):

| File | What it is |
|---|---|
| `config/protocols/copia-empiar12580/assets/copia_template.mrc` | sphere 550:550:550 Å, black (dark) density, 11.80 Å/px, box 96, low-pass 45 Å |
| `config/protocols/copia-empiar12580/assets/copia_mask.mrc` | spherical mask, diameter 575 Å, soft edge 5 px, 11.80 Å/px, box 96 |
| `config/protocols/copia-tutorial/assets/copia_template.mrc` | the CryoBoost v1 repo's `data/vols/copiaBlack_11.8A.mrc` |
| `config/protocols/copia-tutorial/assets/copia_mask.mrc` | the CryoBoost v1 repo's `data/vols/mask_copia_11.8A.mrc` |

The first two can be made in the app (Species page → Templates & masks: basic shape, 11.80 Å/px, box 96)
and copied into place. What each bundle expects is spelled out in its `protocol.yaml` under `species:`.

## 6. Not included

- **DL tilt-filter model weights** (`filterTilts/deepLearning/data/models/…/model.pth`, see
  `filterTilts/deepLearning/model_loader.py`). The DL mode of the tilt filter fails without them; manual
  tilt filtering works.
- **Data** — the copia protocols are written for EMPIAR-12580.

## 7. Known differences from CryoBoost v1

Where the job logic deliberately or not-yet-deliberately differs from the original CryoBoost pipeline:
[docs/reports/v1-parity/driver-audit-2026-09-15.md](docs/reports/v1-parity/driver-audit-2026-09-15.md).
