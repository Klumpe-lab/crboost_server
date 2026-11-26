# Installing CryoBoost

Done once per cluster. Each user then runs their own server from the same install
([starting it](../README.md#start-cryoboost)).

**You need:** a headnode with SLURM and Apptainer (or Singularity), GPU compute nodes, and a shared
filesystem that the headnode and all compute nodes see under the same paths. Put everything — the
install, the containers, your data and your projects — on that shared filesystem. About 2 GB for the
install, and 0.5–14 GB per container: 26 GB for the two every site needs (Warp/AreTomo and RELION).

## 1. Download and install

```bash
cd /groups/<your_group>/software            # any folder on the shared filesystem
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
./install.sh
```

`install.sh` puts everything the server runs on inside the folder: uv (`.uv/bin`), its own CPython
(`.uv/python`) and the Python environment built from `requirements.txt` (`.uv/env/<python>-<hash>`). Nothing
comes from `~/.local`, a cluster module or your shell, and `./crboost` starts the server with that Python only
(it drops `PYTHONPATH` and the like, and so do the jobs it submits). It takes seconds to a minute.

The environment is named by the dependency list and is never changed once built. When `requirements.txt`
changes, `./crboost` (or `./install.sh`) builds a new one beside the old, and servers still running keep theirs:
there is nothing to activate, repair or stop first. `./crboost clean` lists the ones no longer needed. In an
install the users cannot write to, the maintainer runs `./install.sh` after each `git pull`; everywhere else
the first `./crboost` after an update does it.

<details>
<summary>Using conda or pip instead of uv</summary>

Not supported: the lock and `install.sh` assume uv's Python. To run the server on an environment you built
yourself, start `crboost.py` with that Python directly; the jobs it submits follow it.

`requirements.txt` is generated: to change a dependency, edit `requirements.in` and run `./crboost lock`.

</details>

<details>
<summary>No internet on the headnode</summary>

The build reaches these hosts, and nothing else:

- `github.com` and `release-assets.githubusercontent.com`: uv itself, an archive checked against the sha256 in
  `uv.toml`;
- `releases.astral.sh`: uv's Python (python-build-standalone);
- `pypi.org` and `files.pythonhosted.org`: the wheels.

Behind a proxy, `https_proxy` is enough. Behind a mirror, set uv's own variables before `./install.sh`:
`UV_INDEX_URL` for the wheels and `UV_PYTHON_INSTALL_MIRROR` for the Python. For uv itself, put a binary of the
version named in `uv.toml` (`required-version`) at `.uv/bin/uv`, or point `CRBOOST_UV` at one.

</details>

## 2. Get the containers

Each tool runs from an Apptainer image (`.sif` file), which needs no root to run: x86-64, Apptainer 1.1 or newer, and
for the GPU tools an NVIDIA driver for CUDA 12.9 (575 or newer; `./crboost test -m cluster` in step 5 shows whether
yours runs them). Put the images in a folder on the shared filesystem and name each in `config/conf.yaml` (step 3).

**Get the images, do not build them.** They are built once from `container_defs/` and handed out as files, so every
site runs the same image; ask the maintainers for the current set until they are published. You need at least
`warp_aretomo` and `relion`; the rest only for the jobs that use them.

<details>
<summary>Which tool is which</summary>

| Name in `conf.yaml` | Used for | Image (`container_defs/<name>.def`) |
|---|---|---|
| `warp_aretomo` | **required:** motion/CTF, alignment, reconstruction, particle export, M | `warp-2.0.0dev41_aretomo2-1.0.0_torch-2.13_cuda12.9_ubuntu24.04` |
| `relion` | **required:** extraction, averaging, Class3D, Refine3D, templates/masks | `relion-5.0.1-b16a35b_torch-2.8.0_cuda12.9_ubuntu24.04` |
| `pytom` | template matching | `pytom-match-pick-0.14.0_cupy-13.6.0_cuda12.9_ubuntu24.04` |
| `cryocare` | cryoCARE denoising | `cryocare-0.3.0_tensorflow-2.4_cuda11.0_ubuntu24.04` |
| `isonet` | IsoNet2 denoising | `isonet2-a266ff4_torch-2.8.0_cuda12.9_ubuntu24.04` |
| `easymode` | segmentation | `easymode-1.2.5_tensorflow-2.11_cuda11.2_ubuntu24.04` |
| `miss_alignment` | alignment refinement; the Warp-geometry bridge | `miss_alignment-0.2.0_torch-2.8.0_cuda12.9_ubuntu24.04` |
| `tilt_filter` | the DL tilt filter's network (GPU) | `tilt_filter-torch-2.8.0_cuda12.8_ubuntu24.04` |
| `cistem` | template simulation; a normal cisTEM install, not a container | — |

ChimeraX/ArtiaX manual picking (`chimerax_artiax_GL.def`, under `curation:`) and the napari viewer
(`napari-0.10.0_pyqt6_virtualgl-3.1.4_ubuntu24.04`) are set up separately. ChimeraX's licence does not allow us to
hand out its image: build it yourself.

</details>

<details>
<summary>If you must build an image</summary>

```bash
container_defs/build.sh <name> /groups/<your_group>/software/containers
```

No root needed where the host lets users make user namespaces; `build.sh` adds the fakeroot flags that work with or
without a subuid range for you, and checks every download against your host's CA bundle. Behind a proxy that
inspects TLS, name the bundle that holds the proxy's CA with `CRBOOST_BUILD_CA=/etc/pki/tls/certs/ca-bundle.crt`
(never turn certificate checks off). Builds take minutes to a
quarter of an hour and up to 40 GB in `APPTAINER_TMPDIR`. Details: `container_defs/README.md`.

</details>

## 3. Fill in `config/conf.yaml`

```bash
./crboost preflight
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
- **`tilt_filter:`** — the tilt classifier's weights file isn't in git, and it runs in the `tilt_filter` image
  (its entry under `tools:`). Without the weights or the image, replace the whole `tilt_filter:` block with
  `tilt_filter: {models: {}}` and delete the `tilt_filter` entry under `tools:`.

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
  `filterTilts/`. Running one needs the `tilt_filter` tool (an image) under `tools`.
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
./crboost preflight
```

It lists anything that is wrong, one line each. Fix, run again, until there are no failures. Then:

```bash
./crboost test -m cluster
```

This submits one short test job per tool and takes a few minutes. When it passes,
[start the server](../README.md#start-cryoboost).

<details>
<summary>What preflight and the self-test check</summary>

Preflight checks: Python and its packages; that `conf.yaml` loads, `DefaultProjectBase` is writable, every
tool path and bind folder exists; that SLURM and Apptainer are on PATH and every partition exists; that
`qsub.sh` still has its placeholders, exit block and `exit $EXIT_CODE`. Settings nothing reads (usually a
typo) are warnings. It doesn't check the tilt-filter weights or the ChimeraX image.

`./crboost test` runs the same checks in seconds. `-m cluster` runs every configured tool in its
container on a compute node, the way real jobs do, including a kernel launch on the GPU for the tools that use one. Test jobs go to
`~/.crboost/selftest/`; a failure prints the command and the log files.

</details>

## Updating

```bash
cd <install>
git pull
./install.sh                # builds the new Python environment now, if the dependencies changed
./crboost preflight
```

Then restart the servers; each keeps running on the environment it started with until then. (`./install.sh`
is optional where users can write to the folder: the next `./crboost` builds it.) `git pull` never changes your
`config/conf.yaml` or `config/qsub.sh`, so a setting the template gained since you installed has to be copied
over by hand.

<details>
<summary>Upgrading an install with older config files</summary>

The easiest way: rename your old `config/conf.yaml` and `config/qsub.sh`, run `./crboost preflight` to get
fresh ones, and copy your values over. If you edit the old files instead, they must have:

**`config/conf.yaml`**
- `container_runtime` and `container_binds`.
- `slurm_defaults.partition` and `supervisor_slurm.partition`, and `curation.partition` if
  `curation.sif_path` is set.
- Tool entries with the template's names (`warp_aretomo`, `relion`, …). `tools.cistem.bin_path` is the folder
  holding `simulate`, not the program itself.
- No `crboost_root:` and no `use_afterok_orchestrator:`, and no `crboost_python:` line (the drivers run the server's own
  interpreter). Preflight warns about every setting nothing reads.
- `tilt_filter` pointing at a weights file that exists (and a `tilt_filter` entry under `tools:`), or
  `tilt_filter: {models: {}}`.

**`config/qsub.sh`**
- The `RELION_JOB_EXIT` block exactly as in `config/qsub.template.sh`, and the final `exit $EXIT_CODE`.
  Without them CryoBoost refuses to submit, or failed jobs look successful. Preflight checks both.
- The old `ENV PATHS` block (`CRBOOST_SERVER_DIR`, `CRBOOST_PYTHON`) is no longer used; delete it.

**A venv built from a cluster module's Python, or any `venv/` from before environments were named:** run
`./install.sh` (it leaves the old `venv/` alone), delete the Python `module load` lines from `config/qsub.sh`,
and delete `venv/` once no server runs from it.

Then run `./crboost preflight` and `./crboost test -m cluster`, and start the server with `./crboost` (it used
to be `main.py`). Every browser needs the new `?token=` link once. The `.nicegui/` folder in the install is no
longer used; you can delete it.

</details>
