[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Klumpe-lab/crboost_server)

# CryoBoost Server

CryoBoost Server is a web-based cryo-electron tomography pipeline manager. It runs as a single
long-lived process on your cluster's **headnode**, serves a browser UI (NiceGUI/FastAPI), and
orchestrates RELION-based tomography pipelines as SLURM jobs on your compute nodes. External tools
— RELION, Warp/AreTomo, PyTOM, CryoCARE, IsoNet, IMOD, ChimeraX/ArtiaX — run inside Apptainer
containers (or as native binaries), configured per tool in `config/conf.yaml`.

You point it at your movies and mdocs, build a pipeline in the UI (or start from a protocol bundle),
and it submits, monitors and reconciles the jobs for you. Everything below assumes you are working on
the headnode; you reach the UI from your laptop through an SSH tunnel.

> **Requirements:** Python 3.11+ (with dev headers), SLURM, Apptainer/Singularity, CUDA GPUs, SSH
> access to compute nodes, and a shared filesystem visible from the compute nodes.

## Quick start

```bash
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
python3 -m venv venv && venv/bin/pip install -r requirements.txt
venv/bin/python3 preflight.py        # 1st run writes config/conf.yaml + config/qsub.sh, then stops
$EDITOR config/conf.yaml config/qsub.sh
venv/bin/python3 preflight.py        # must exit 0
venv/bin/python3 main.py --port 8081 --host 0.0.0.0
```

Then from your laptop: `ssh -f -N -L 8081:localhost:8081 $USER@$HEADNODE` and open
<http://localhost:8081>.

---

<details>
<summary><b>Python environment — why it must be <code>&lt;repo&gt;/venv</code></b></summary>

Pipeline drivers running inside SLURM jobs are launched as exactly `<repo>/venv/bin/python3`. So the
venv must live in the repository root, the repo must be on a shared filesystem, and that interpreter
must start on a compute node with the same modules loaded in `config/qsub.sh`.

If your cluster's Python ships its own packages and versions conflict, prefer the cluster's modules.

</details>

<details>
<summary><b>Preflight checks</b></summary>

`venv/bin/python3 preflight.py` creates `config/conf.yaml` and `config/qsub.sh` from their templates on
the first run and stops. Every later run only verifies (it never edits):

- Python version, and that every package in `requirements.txt` imports
- `config/conf.yaml` loads, `DefaultProjectBase` is writable, every `tools:` path exists
- `sbatch`/`squeue`/`sacct`/`sinfo` and `apptainer` are on PATH, and every configured partition exists
- `config/qsub.sh` still has its placeholders, exit-marker block and `exit $EXIT_CODE`

It exits non-zero and lists each failed check.

</details>

<details>
<summary><b>Configuring <code>config/conf.yaml</code> (paths, SLURM, tools)</b></summary>

### Paths

```yaml
local:
  DefaultProjectBase: "/path/to/your/projects"      # where all project output goes
  DefaultMoviesGlob : "/path/to/your/movies/*.eer"  # optional, also settable in the UI
  DefaultMdocsGlob  : "/path/to/your/mdocs/*.mdoc"  # optional, also settable in the UI
```

### SLURM defaults

```yaml
slurm_defaults:
  partition      : "gpu"        # your GPU partition, see `sinfo`
  constraint     : "gpu|v100"   # usually safe to leave blank
  nodes          : 1
  ntasks_per_node: 1
  cpus_per_task  : 4
  gres           : "gpu:4"
  mem            : "64G"
  time           : "3:30:00"
```

`sinfo` lists your partitions (ours is called `example-g`). `constraint` only matters if you want to
confine jobs to nodes with particular hardware.

### Tools

Each external tool is either an Apptainer image (`exec_mode: "container"` + `container_path`) or a
native executable (`exec_mode: "binary"` + `bin_path`):

```yaml
tools:
  warp_aretomo:
    exec_mode: "container"
    container_path: "/path/to/containers/warp_aretomo.sif"
    bin_path: ""
  relion:
    exec_mode: "container"
    container_path: "/path/to/containers/relion5.0_tomo.sif"
    bin_path: ""
  # ... likewise cryocare, isonet, pytom, imod, pymol, miss_alignment, cistem
```

| `tools:` key | used for | definition file in `container_defs/` |
|---|---|---|
| `warp_aretomo` | WarpTools motion/CTF, tilt-series alignment (AreTomo / IMOD patch), CTF, reconstruction | `warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.def` |
| `relion` | subtomogram extraction, particle reconstruction, Class3D, template/mask preparation | `relion5.0_tomo.def` |
| `pytom` | template matching + candidate extraction | `pytom_match_pick_0.10.0.def` |
| `cryocare` | cryoCARE denoising | `cryocare.def` |
| `isonet` | IsoNet2 denoising (alternative to cryoCARE) | `isonet2.def` |
| `imod` | IMOD utilities used by the pick viewer | `imod.def` |
| `pymol` | template generation from a PDB/mmCIF model | `pymol.def` |
| `miss_alignment` | learned tilt-series alignment refinement | `miss_alignment_torch2.8.0_cuda12.9.def` |
| `cistem` | template simulation (`simulate`), native binary | — |

The ChimeraX/ArtiaX curation image (`chimerax_artiax_GL.def`) is configured separately under
`curation.sif_path`. The legacy top-level `containers:` map is still read, but `tools:` is the format
to use.

</details>

<details>
<summary><b>Building containers (optional)</b></summary>

Definition files live in `container_defs/`. The names and locations of the built `.sif` files don't
matter as long as `conf.yaml` points at them.

```bash
apptainer build --fakeroot --nv relion5.0_tomo.sif container_defs/relion5.0_tomo.def
apptainer build --fakeroot --nv cryocare.sif       container_defs/cryocare.def
apptainer build --fakeroot --nv warp_aretomo.sif   container_defs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.def
```

</details>

<details>
<summary><b>Adapting the SLURM template <code>config/qsub.sh</code></b></summary>

Every job CryoBoost queues is built from this one script (created from `config/qsub.template.sh`).
Per-job resources come from the UI via RELION-style placeholders (`XXXextra1XXX` … `XXXextra8XXX` =
partition, constraint, nodes, ntasks-per-node, cpus-per-task, gres, mem, time), so you only define
the cluster environment once:

- **SLURM HEADER** — the module loads (or equivalent) that make `<repo>/venv/bin/python3` start on a
  compute node and put `apptainer` on PATH. Our Lmod lines are in the template as a commented example.
- **Optional** — `#SBATCH --account` / `--qos` / `--exclude` lines your cluster needs.
- **Leave untouched** — every `XXX...XXX` placeholder, the `RELION_JOB_EXIT_*` marker block (matched
  verbatim by `drivers/array_job_base.py`), and the final `exit $EXIT_CODE` (job dependencies chain on
  it). `preflight.py` checks all three.

If you already have a working RELION/Warp SLURM script, its module lines are the right starting point.

</details>

<details>
<summary><b>Starting the server and tunnelling in</b></summary>

```bash
venv/bin/python3 main.py --port 8081 --host 0.0.0.0
```

```
CryoBoost Server Starting
Access URLs:
  Local:    http://localhost:8081
  Network:  http://111.11.11.11:8081
INFO:     Uvicorn running on http://0.0.0.0:8081 (Press CTRL+C to quit)
```

From your local machine (still on the cluster's network, physically or via VPN):

```bash
ssh -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# background:
ssh -f -N -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# kill when done:
pkill -f "ssh.*${LOCAL_PORT}:localhost:${HEADNODE_PORT}"
```

`LOCAL_PORT` is any free port on your computer, `HEADNODE_PORT` is the port the server listens on.
This assumes your public key is already on the cluster. Saving the tunnel in your `~/.ssh/config` is
handy:

```
Host cryoboost-tunnel
    HostName [YOUR CLUSTER NODE]
    User [YOUR USERNAME]
    LocalForward 8080 localhost:42
    LocalForward 8081 localhost:42
    LocalForward 8082 localhost:42
```

Then `ssh cryoboost-tunnel` is enough.

```
Your Laptop          SSH Tunnel               Head Node
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Browser    │────►│ Port 8080       │────►│ Port 42      │
│ localhost:  │     │       ↓         │     │ CryoBoost    │
│   8080      │     │ SSH Connection  │     │ Server       │
└─────────────┘     └─────────────────┘     └──────────────┘
```

</details>

<details>
<summary><b>Notes &amp; gotchas</b></summary>

- The server needs write access to project directories.
- Container files must be reachable from the compute nodes.
- GPU memory requirements vary with dataset size and processing type.
- Use a shared filesystem for large datasets (and for the repo itself).

</details>

## Further documentation

`docs/` — [architecture](docs/architecture.md), [protocols](docs/protocols.md),
[particle data flow](docs/particle-data-flow.md), [binning and resolution](docs/binning-and-resolution.md),
[known bugs](docs/known_bugs.md).
