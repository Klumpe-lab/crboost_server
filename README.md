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

Create a dedicated Python environment for CryoBoost Server:

```bash
# Using conda (recommended)
conda create -n crboost python=3.11 -y
conda activate crboost

# Or using venv
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

It is not unlikely that your cluster's Python comes pre-bundled with a bunch of its own packages. Here, if version conflicts arise -- default to your cluster's modules. We will eventually serve conda configs that should circumvent this.


## 3. Run Preflight Check

CryoBoost includes a setup validation script that helps configure your installation and checks that everything is in place:
```bash
python preflight.py
```

The script will:
- Create `config/conf.yaml` from the template (interactive prompts for paths)
- Create `config/qsub.sh` from the template
- Validate your Python environment and required modules
- Check that container .sif files exist and are functional
- Verify SLURM connectivity and partition names
- Check directory permissions

Run it after cloning to set up your config files, then re-run after making changes to verify everything is correct:
```
$ python preflight.py
CryoBoost Setup - /groups/group/software/crboost_server

1. Configuration: config/conf.yaml
  [OK] conf.yaml exists

2. Python (conf.yaml -> crboost_python)
  [OK] /path/to/venv/bin/python3
  [OK] Version: Python 3.11.5
  [OK] Modules: pydantic, yaml, nicegui

3. Containers (conf.yaml -> containers)
  [OK] relion: 10.3GB - RELION version: 5.0.1
  [OK] warp_aretomo: 9.5GB - WarpTools
  ...

Summary
  crboost_root: /groups/group/software/crboost_server
  crboost_python: /path/to/venv/bin/python3
  DefaultProjectBase: /groups/group/crboost_data

  Containers:
    relion: OK
    warp_aretomo: OK
    ...

  Ready to go!
  Start server: /path/to/venv/bin/python3 main.py
```

If anything is missing or misconfigured, the script will list what needs to be fixed.


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



### Container Paths
Update container paths to point to your Apptainer images:

```yaml
#config/conf.yaml
containers:
  warp_aretomo: /path/to/containers/warp_aretomo.sif
  cryocare    : /path/to/containers/cryocare.sif
  pytom       : /path/to/containers/pytom_match_pick.sif
  relion      : /path/to/containers/relion5.0_tomo.sif
```
These containers are for Relion and other external tools that are used in the pipeline (Warp, AreTomo2, PyTOM, CryoCare). We will shortly provide a way to mix locally (or cluster-specific module-loaded) tools with containerized tools freely, but currently to circumvent compatibility issues everything is assumed to run in a container.



## 5. Build Containers (Optional)

The definition files are in `container_defs`. Your cluster must provide `apptainer` (formerly Singularity). The names with which you build `.sif` files and their locations do not matter insofar as you specify correct locations in `conf.yaml` post-build.

```bash
# Build RELION container
apptainer build --fakeroot --nv relion5.0_tomo.sif container_defs/relion5.0_tomo.def

# Build CryoCARE container
apptainer build fakeroot --nv cryocare.sif container_defs/cryocare.def

# Build Warp+AreTomo container
apptainer build --fakeroot --nv warp_aretomo.sif container_defs/warp_aretomo1.0.0_cuda11.8_glibc2.31.def
```

## 6. Update SLURM Template

TLDR: if you have a Relion/Warp slurm script that already works for you -- adapt that as a basis. Set `ENV PATHS`, delete `SLURM HEADER` section. If your modules don't load -- contact us.


`qsub.sh` is the template slurm script that is shared between all jobs CryoBoost queues on your cluster. You need to define it once per cluster environment -- job parameters will configurable in the UI (via relion template vars ex. `XXXextra1XXX`).

`ENV PATHS`: particular environment paths to your CryoboostServer installation and the python environment in which it runs (on the _headnode_). **YOU MUST SET THESE**.



```bash
#!/bin/bash
#SBATCH --job-name=CryoBoost
#SBATCH --partition=XXXextra1XXX
#SBATCH --constraint="XXXextra2XXX"
#SBATCH --nodes=XXXextra3XXX
#SBATCH --ntasks-per-node=XXXextra4XXX
#SBATCH --cpus-per-task=XXXextra5XXX
#SBATCH --gres=XXXextra6XXX
#SBATCH --mem=XXXextra7XXX
#SBATCH --time=XXXextra8XXX
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

# ------------ SLURM HEADER  -----------
export MODULEPATH=/software/system/REDACTED
. /opt/ohpc/REDACTED/init/bash

module load build-env/f2022
module load miniconda3/24.7.1-0
module load python/3.11.5-gcccore-13.2.0 
module load gcccore/13.2.0 
module load arrow/16.1.0-gfbf-2023b
which python3
python3 --version
# ------------ ------------  -----------


# ------------ ENV PATHS  --------------
export CRBOOST_SERVER_DIR="/users/cryoboost_user/dev/crboost_server/"
export CRBOOST_PYTHON="/users/cryoboost_user/dev/crboost_server/venv/bin/python3"
export PYTHONPATH="${CRBOOST_SERVER_DIR}:${PYTHONPATH}"
# ------------ ---------  --------------
```

## 7. Start the Server

Launch CryoBoost Server:

```bash
# Activate environment first
conda activate crboost  # or: source venv/bin/activate

# Start the server
python main.py --port 8081 --host 0.0.0.0
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

## 7. Access via SSH Tunnel

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

## Troubleshooting

### Environment Variables

You can use environment variables in `conf.yaml` for dynamic paths:

```yaml
local:
  DefaultProjectBase: "${HOME}/crboost_projects"
  DefaultMoviesGlob : "${DATA_DIR}/movies/*.eer"
```

## Notes

- The server requires write access to project directories
- Container files must be accessible from compute nodes
- GPU memory requirements vary by dataset size and processing type
- Consider using a shared filesystem for large datasets


