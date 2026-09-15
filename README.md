[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Klumpe-lab/crboost_server)


# CryoBoost Server Installation Guide

This software is supposed to run in a dedicate process on your computational cluster's _headnode_ (as opposed to your local machine or the compute node). Hence all further instructions assume we are in the _headnode_ environmnet.

## Prerequisites

Ensure your cluster has:
- **Python 3.11+** with development headers
- **SLURM** scheduler for job submission (we may eventually add PBS).
- **Apptainer/Singularity** for container execution
- **CUDA-capable GPUs** (for GPU-accelerated processing)
- **SSH access** to compute nodes

## 1. Clone Repository

```bash
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
```





## 2. Create Python Environment

Create the environment as a venv named `venv` **in the repository root**. The pipeline drivers that run
inside SLURM jobs are launched with exactly `<repo>/venv/bin/python3`, so the venv must live there and
must start on the compute nodes (repo on a shared filesystem, and the same Python modules loaded in
`config/qsub.sh`):

```bash
python3 -m venv venv          # python3 = a 3.11 interpreter that compute nodes can also run
venv/bin/pip install -r requirements.txt
```

It is not unlikely that your cluster's Python comes pre-bundled with a bunch of its own packages. Here, if version conflicts arise -- default to your cluster's modules.


## 3. Run Preflight Check

```bash
venv/bin/python3 preflight.py
```

On the first run it creates `config/conf.yaml` and `config/qsub.sh` from their templates and stops so you can
edit them. Every later run checks, without editing anything:
- the Python version and that every package from `requirements.txt` imports
- that `config/conf.yaml` loads, `DefaultProjectBase` is writable, and every `tools:` path exists
- that `sbatch`/`squeue`/`sacct`/`sinfo` and `apptainer` are on PATH, and every configured partition exists
- that `config/qsub.sh` still has its placeholders, exit-marker block and `exit $EXIT_CODE`

It exits non-zero and lists each failed check.


## 4. Configure Paths

Edit `config/conf.yaml` to match your cluster's paths and resources:

### Local Paths
Update the local paths section to point to your data directories:

```yaml
local:
  DefaultProjectBase: "/path/to/your/projects" # <--- This is where the output for all projects will go

  # The following two options can be easily configured in the UI, but for convenience can be set here.
  DefaultMoviesGlob : "/path/to/your/movies/*.eer" # <--- A particular set of data
  DefaultMdocsGlob  : "/path/to/your/mdocs/*.mdoc" # <--- A particular set of mdocs 
```

### SLURM Configuration
Adjust SLURM defaults to match your cluster's partition names and constraints



```yaml
slurm_defaults:
  partition      : "gpu"  # Change to your GPU partition
  constraint     : "gpu|v100"  # Update to your GPU types
  nodes          : 1
  ntasks_per_node: 1
  cpus_per_task  : 4
  gres           : "gpu:4"
  mem            : "64G"
  time           : "3:30:00"
```

Your cluster probably has its own particular name for the GPU `partition`. You can find that out easily via `sinfo` (for example, ours is `example-g`):
```bash
[dev/crboost_server] sinfo
PARTITION           AVAIL  TIMELIMIT  NODES  STATE NODELIST
exc*                 up   infinite      1   drng [REDACTED]
exc*                 up   infinite      3  drain [REDACTED]
exc*                 up   infinite    121    mix [REDACTED]
exm                  up   infinite      5    mix [REDACTED]
example-g            up   infinite     25    mix [REDACTED]
example-g            up   infinite      1  alloc [REDACTED]
example-g            up   infinite      7   idle [REDACTED]
```
`constraint` likely can be left blank unless you want to confine your jobs to nodes that contain particular hardware. Consult your cluster docs to find out more.



### Tools (containers)
Every external tool has an entry under `tools:` — either an Apptainer image (`exec_mode: "container"` +
`container_path`) or a native executable (`exec_mode: "binary"` + `bin_path`):

```yaml
#config/conf.yaml
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

The ChimeraX/ArtiaX curation image (`chimerax_artiax_GL.def`) is configured separately under `curation.sif_path`.
The legacy top-level `containers:` map is still read, but `tools:` is the format to use.



## 5. Build Containers (Optional)

The definition files are in `container_defs`. Your cluster must provide `apptainer` (formerly Singularity). The names with which you build `.sif` files and their locations do not matter insofar as you specify correct locations in `conf.yaml` post-build.

```bash
# Build RELION container
apptainer build --fakeroot --nv relion5.0_tomo.sif container_defs/relion5.0_tomo.def

# Build CryoCARE container
apptainer build --fakeroot --nv cryocare.sif container_defs/cryocare.def

# Build Warp+AreTomo container
apptainer build --fakeroot --nv warp_aretomo.sif container_defs/warp_2.0.0dev36_aretomo1.0.0_cuda11.8_glibc2.31.def
```

## 6. Update SLURM Template

`config/qsub.sh` is the one SLURM script every job CryoBoost queues is built from; `preflight.py` creates it
from `config/qsub.template.sh`. Per-job resources come from the UI through the RELION-style placeholders
(`XXXextra1XXX` … `XXXextra8XXX` = partition, constraint, nodes, ntasks-per-node, cpus-per-task, gres, mem,
time), so you only define the cluster environment once:

- **SLURM HEADER** — the module loads (or equivalent) that make `<repo>/venv/bin/python3` start on a compute
  node and put `apptainer` on PATH. Our own Lmod lines are in the template as a commented example.
- **Optional** — `#SBATCH --account` / `--qos` / `--exclude` lines your cluster needs.
- **Leave untouched** — every `XXX…XXX` placeholder, the `RELION_JOB_EXIT_*` marker block (matched verbatim
  by `drivers/array_job_base.py`), and the final `exit $EXIT_CODE` (job dependencies chain on it).
  `preflight.py` checks all three.

If you have a RELION/Warp SLURM script that already works on your cluster, its module lines are the right
starting point for the SLURM HEADER.

## 7. Start the Server

Launch CryoBoost Server:

```bash
venv/bin/python3 main.py --port 8081 --host 0.0.0.0
```

The server will display access URLs:
```
[dev/crboost_server] python3 main.py
CryoBoost Server Starting
Access URLs:
  Local:    http://localhost:8081
  Network: http://111.11.11.11:8081

To access from another machine, use an SSH tunnel:
ssh -L 8081:[login_node]:8081 [YOUR_USERNAME]@[login_node]
------------------------------
INFO:     Started server process [20015]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8081 (Press CTRL+C to quit)

```

## 8. Access via SSH Tunnel

On your local machine (it must still be on the cluster's institutional network whether physically or via a VPN), open a porward forwarding tunnel:
```
ssh -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# or in the background
ssh -f -N -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# (kill when done:  `pkill -f "ssh.*${LOCAL_PORT}:localhost:${HEADNODE_PORT}"`)
```

- `LOCAL_PORT` is any free port of choosing on your computer
- `HEADNODE_PORT` is the port on which this software (crboost_server) is running on the headnode
- `USERNAME` and `HEADNODE_URL` are the credentials for your local cluster setup

This, of course, assumes that `USERNAME` has previously added their public key to the cluster's ssh folder (usually done for you by Slurm's admins). You may also want to save this configuration to your local sshconfig (example):
```
Host cryoboost-tunnel
    HostName [YOUR CLUSTR NODE]
    User [YOUR USERNAME]
    LocalForward 8080 localhost:42
    LocalForward 8081 localhost:42
    LocalForward 8082 localhost:42
```

Then, `ssh cryoboost-tunnel` suffices on local.

```
Your Laptop          SSH Tunnel               Head Node
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Browser    │────►│ Port 8080       │────►│ Port 42      │
│ localhost:  │     │       ↓         │     │ CryoBoost    │
│   8080      │     │ SSH Connection  │     │ Server       │ 
└─────────────┘     └─────────────────┘     └──────────────┘
```

## Notes

- The server requires write access to project directories
- Container files must be accessible from compute nodes
- GPU memory requirements vary by dataset size and processing type
- Consider using a shared filesystem for large datasets


