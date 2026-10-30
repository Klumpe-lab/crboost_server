# Installing CryoBoost

Done once per cluster. Each user then runs their own server from the same install
([starting it](../README.md#start-cryoboost)).

**You need:** a headnode with SLURM and Apptainer (or Singularity), GPU compute nodes, and a shared
filesystem that the headnode and all compute nodes see under the same paths. Put everything — the
install, the containers, your data and your projects — on that shared filesystem. About 6 GB for the
install, 1–12 GB per container.

## 1. Download and install

```bash
cd /groups/<your_group>/software            # any folder on the shared filesystem
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.11
uv venv -p 3.11 venv
uv pip sync -p venv/bin/python3 requirements.txt
```

<details>
<summary>If other people will use this install</summary>

uv puts its Python in your home folder, which others may not be able to read. Run these two lines
**before** `uv python install` to put it somewhere shared instead:

```bash
export UV_PYTHON_INSTALL_DIR=/groups/<your_group>/software/uv/python
export UV_CACHE_DIR=/groups/<your_group>/software/uv/cache
```

</details>

<details>
<summary>Using conda or pip instead of uv</summary>

Any Python 3.11 environment works, as long as it is on the shared filesystem and starts on a compute node.
With pip: `python3 -m venv venv && venv/bin/pip install -r requirements.txt`. If that Python comes from a
cluster module, add its `module load` lines to `config/qsub.sh` (step 4) so it also starts on the compute
nodes. A cluster module that puts packages on `PYTHONPATH` can shadow the venv's; preflight warns about it.

`requirements.txt` is generated: to change a dependency, edit `requirements.in` and run the
`uv pip compile` line at the top of `requirements.txt`.

</details>

## 2. Get the containers

Each tool runs from an Apptainer image (`.sif` file). Build the ones you want from `container_defs/` and put
them in a folder on the shared filesystem, for example:

```bash
apptainer build --fakeroot --nv /groups/<your_group>/software/containers/warp_aretomo.sif \
    container_defs/warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def
```

You need at least `warp_aretomo` and `relion`; the rest only for the jobs that use them.

<details>
<summary>Which tool is which</summary>

| Name in `conf.yaml` | Used for | Build from `container_defs/` |
|---|---|---|
| `warp_aretomo` | **required:** motion/CTF, alignment, reconstruction, particle export | `warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def` |
| `relion` | **required:** extraction, averaging, Class3D, Refine3D, templates/masks | `relion5.0_tomo.def` |
| `pytom` | template matching | `pytom_match_pick_0.10.0.def` |
| `cryocare` | cryoCARE denoising | `cryocare.def` |
| `isonet` | IsoNet2 denoising | `isonet2.def` |
| `easymode` | segmentation | not in the repo yet |
| `imod` | pick viewer utilities | `imod.def` |
| `pymol` | templates from a PDB/mmCIF model | `pymol.def` |
| `miss_alignment` | alignment refinement | `miss_alignment_torch2.8.0_cuda12.9.def` |
| `cistem` | template simulation; a normal cisTEM install, not a container | — |

`--fakeroot` must be allowed on your machine; if it isn't, build where you have root and copy the `.sif`
over. ChimeraX/ArtiaX manual picking (`chimerax_artiax_GL.def`) is set up separately under `curation:`.

</details>

## 3. Fill in `config/conf.yaml`

```bash
venv/bin/python3 preflight.py
```

The first run creates `config/conf.yaml` and `config/qsub.sh` and stops. Open `config/conf.yaml` in any text
editor (e.g. `nano config/conf.yaml`) and change these — leave everything else as it is:

- **`DefaultProjectBase`** — the folder where new projects go. It must exist.
- **`partition`** under `slurm_defaults` and under `supervisor_slurm` — your GPU partition name. Run
  `sinfo -s` to see the names.
- **`container_binds`** — the top folders your data and software live in, e.g. `[/groups, /scratch]`.
- **`tools:`** — for each tool, the full path to its `.sif` file from step 2. **Delete the whole entry of
  every tool you don't have** (all its lines), or preflight will complain.
- **`curation:`** — not using ChimeraX picking: set `sif_path: ""`.
- **`tilt_filter:`** — the tilt classifier's weights file isn't in git. Without it, replace the whole
  `tilt_filter:` block with `tilt_filter: {models: {}}`.

<details>
<summary>Example: a site with only Warp, RELION and PyTOM</summary>

Only the changed parts are shown; everything else stays as in the file. The `tools:` list is complete: the
entries of the other tools were deleted.

```yaml
local:
  DefaultProjectBase: "/groups/mylab/cryoboost_projects"

slurm_defaults:
  partition: "gpu"

supervisor_slurm:
  partition: "gpu"

curation:
  sif_path: ""

tilt_filter: {models: {}}

container_binds: [/groups, /scratch]

tools:
  warp_aretomo:
    exec_mode: "container"
    container_path: "/groups/mylab/software/containers/warp_aretomo.sif"
    bin_path: ""
  relion:
    exec_mode: "container"
    container_path: "/groups/mylab/software/containers/relion5.sif"
    bin_path: ""
  pytom:
    exec_mode: "container"
    container_path: "/groups/mylab/software/containers/pytom.sif"
    bin_path: ""
```

</details>

<details>
<summary>All settings explained</summary>

`config/conf.template.yaml` explains every setting in its comments. In short:

- `crboost_python` — leave empty.
- `local.DefaultMoviesGlob` / `DefaultMdocsGlob` — optional pre-fill for the import form.
- `slurm_defaults` — resources for every job. `gres: "gpu:1"` works on most clusters (some write
  `"gpu:a100:1"`; see `sinfo -o '%P %G'`). `time` must stay within your cluster's limit
  (`sinfo -o '%P %l'`). `constraint` limits jobs to certain node types; usually leave it empty.
- `supervisor_slurm` — a small job that starts and watches the per-tilt-series job arrays. Needs no GPU.
  It runs until its whole array is done, queue waits included, so give it a long `time` and, if your
  cluster caps wall time per QOS, a long `qos` (`sacctmgr show qos format=Name,MaxWall`).
- `job_resource_profiles` — per-job-type defaults that override `slurm_defaults`. Lower any `time` above
  your cluster's limit. Users can change all resources per job in the browser.
- `curation` — ChimeraX/ArtiaX sessions: `sif_path` and `partition`.
- `species_catalog_root` — optional shared folder for a lab-wide particle catalog.
- `tilt_filter` — each model names a weights file (`*.pth`, not in git: ask the maintainers) in
  `filterTilts/`.
- `container_runtime` — `apptainer` or `singularity`.
- `container_binds` — folders available inside every container. The project, the raw data and the gain
  reference are added automatically.
- `tools` — `exec_mode: "container"` with `container_path`, or `exec_mode: "binary"` with `bin_path` (the
  folder holding the program, e.g. cisTEM's `simulate`). A tool without an entry only fails the jobs that
  need it.

A user can override settings for themselves only in `~/.crboost/conf.yaml`.

</details>

## 4. Check `config/qsub.sh`

Every job is submitted with this script. Usually you change nothing. Only if needed:

- Apptainer isn't available on the compute nodes by default → add your `module load apptainer` line in the
  `SLURM HEADER` part.
- Your cluster requires an account or QOS → add `#SBATCH --account=…` / `#SBATCH --qos=…` next to the other
  `#SBATCH` lines.

Don't touch the `XXX…XXX` words, the `RELION_JOB_EXIT` block or the last line: CryoBoost needs them.

## 5. Check that everything works

```bash
venv/bin/python3 preflight.py
```

It lists anything that is wrong, one line each. Fix, run again, until there are no failures. Then:

```bash
venv/bin/python3 -m pytest -m cluster
```

This submits one short test job per tool and takes a few minutes. When it passes,
[start the server](../README.md#start-cryoboost).

<details>
<summary>What preflight and the self-test check</summary>

Preflight checks: Python and its packages; that `conf.yaml` loads, `DefaultProjectBase` is writable, every
tool path and bind folder exists; that SLURM and Apptainer are on PATH and every partition exists; that
`qsub.sh` still has its placeholders, exit block and `exit $EXIT_CODE`. Settings nothing reads (usually a
typo) are warnings. It doesn't check the tilt-filter weights or the ChimeraX image.

`venv/bin/python3 -m pytest` runs the same checks in seconds. `-m cluster` runs every configured tool in its
container on a compute node, the way real jobs do, including the GPU. Test jobs go to
`~/.crboost/selftest/`; a failure prints the command and the log files.

</details>

## Updating

```bash
cd <install>
git pull
uv pip sync -p venv/bin/python3 requirements.txt
venv/bin/python3 preflight.py
```

Then restart the server. `git pull` never changes your `config/conf.yaml` or `config/qsub.sh`.

<details>
<summary>Upgrading an install with older config files</summary>

The easiest way: rename your old `config/conf.yaml` and `config/qsub.sh`, run `preflight.py` to get fresh
ones, and copy your values over. If you edit the old files instead, they must have:

**`config/conf.yaml`**
- `container_runtime` and `container_binds`.
- `slurm_defaults.partition` and `supervisor_slurm.partition`, and `curation.partition` if
  `curation.sif_path` is set.
- Tool entries with the template's names (`warp_aretomo`, `relion`, …). `tools.cistem.bin_path` is the folder
  holding `simulate`, not the program itself.
- No `crboost_root:` and no `use_afterok_orchestrator:`. Preflight warns about every setting nothing reads.
- `tilt_filter` pointing at a weights file that exists, or `tilt_filter: {models: {}}`.

**`config/qsub.sh`**
- The `RELION_JOB_EXIT` block exactly as in `config/qsub.template.sh`, and the final `exit $EXIT_CODE`.
  Without them CryoBoost refuses to submit, or failed jobs look successful. Preflight checks both.
- The old `ENV PATHS` block (`CRBOOST_SERVER_DIR`, `CRBOOST_PYTHON`) is no longer used; delete it.

**A venv built from a cluster module's Python:** stop the server, let running jobs finish, then `rm -rf venv`
and redo the uv lines from step 1. Delete the Python `module load` lines from `config/qsub.sh`.

Then run `preflight.py` and `pytest -m cluster`, and start the server with `./crboost` (it used to be
`main.py`). Every browser needs the new `?token=` link once. The `.nicegui/` folder in the install is no
longer used; you can delete it.

</details>
