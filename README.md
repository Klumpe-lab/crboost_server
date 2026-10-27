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

> **Requirements:** [uv](https://docs.astral.sh/uv/) (one static binary; it brings its own Python 3.11),
> SLURM, Apptainer/Singularity, CUDA GPUs, SSH access to compute nodes, and a shared filesystem visible
> from the compute nodes.

## Quick start

```bash
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
curl -LsSf https://astral.sh/uv/install.sh | sh   # once per user: uv in ~/.local/bin
uv python install 3.11
uv venv -p 3.11 venv
uv pip sync -p venv/bin/python3 requirements.txt
venv/bin/python3 preflight.py        # 1st run writes config/conf.yaml + config/qsub.sh, then stops
$EDITOR config/conf.yaml config/qsub.sh
venv/bin/python3 preflight.py        # must exit 0
venv/bin/python3 -m pytest -m cluster   # one short SLURM job per tool (minutes); must pass
venv/bin/python3 main.py
```

The server picks its own port. On your laptop, run the `ssh …` tunnel command it printed and open the
`http://localhost:<port>/?token=…` link it printed. Without the token every page answers 401
(see *Starting the server* below). Updating an install whose `config/` came from older templates: see
*Upgrading an existing install* below.

`requirements.txt` is a generated lock: to change a dependency, edit `requirements.in` and recompile with the
`uv pip compile` line at the top of that file.

---

<details>
<summary><b>Python environment — venv, conda or uv</b></summary>

Pipeline drivers running inside SLURM jobs are launched with the interpreter that runs `main.py`
(or `crboost_python` in `config/conf.yaml`, if you set it). Any environment works — a venv, a
conda/mamba env, uv — as long as it and the repo are on a shared filesystem and that interpreter
starts on a compute node with the modules loaded in `config/qsub.sh`.

The quick start's uv venv runs on uv's own Python, so a compute node needs no module for it. uv keeps
that Python in `~/.local/share/uv/python` and its cache in `~/.cache/uv`; both must be on a filesystem
the compute nodes see (a shared home is). For an install several people use, set `UV_PYTHON_INSTALL_DIR`
and `UV_CACHE_DIR` to a shared place before `uv python install`, e.g.
`/groups/<lab>/software/cache_and_artifacts/uv/{python,cache}`.
A module loaded in your shell does no harm to such a venv: `main.py` and `preflight.py` start themselves
again without the shell's `PYTHONPATH` when the venv has its own packages (`clean_env.py`), so the server and
every job it submits (sbatch passes the server's environment on) see only the venv.

pip works on the same lock: `python3 -m venv venv && venv/bin/pip install -r requirements.txt`. The
interpreter is then whichever `python3` built the venv, and `config/qsub.sh` must make it start on a
compute node. A cluster module that puts packages on `PYTHONPATH` shadows the venv's; preflight warns
about each one.

</details>

<details>
<summary><b>Preflight checks and self-test</b></summary>

`venv/bin/python3 preflight.py` creates `config/conf.yaml` and `config/qsub.sh` from their templates on
the first run and stops. Every later run only verifies (it never edits):

- Python version, and that every package in `requirements.txt` imports; one imported from outside the venv (a
  cluster module on `PYTHONPATH`) is a warning naming its path
- `config/conf.yaml` loads, `DefaultProjectBase` is writable, every `tools:` path and `container_binds`
  entry exists, the partitions are set; a key no setting reads (a misspelling) is a warning
- `sbatch`/`squeue`/`sacct`/`sinfo` and `apptainer` are on PATH, and every configured partition exists
- `config/qsub.sh` still has its placeholders, exit-marker block and `exit $EXIT_CODE`

It exits non-zero and lists each failed check.

**Self-test.** `venv/bin/python3 -m pytest` runs the same checks as tests, plus a render of
`config/qsub.sh` (headnode, seconds). `venv/bin/python3 -m pytest -m cluster` submits one short job
per configured tool, run the way a pipeline driver runs it, and waits for all of them (minutes,
mostly queue): each proves the tool starts in its container on a compute node with the site's binds
and sees the GPU where it needs one. Job directories are under `~/.crboost/selftest/`; a failure
prints the command and the job's log paths.

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
native executable (`exec_mode: "binary"` + `bin_path`, the directory holding it — crboost puts it first
on `PATH`). A tool without an entry fails the job that needs it. Container calls bind `/tmp`, the
project directory, the raw-data directories and the gain reference; list the other places your data
and software live under `container_binds`:

```yaml
container_runtime: apptainer        # or singularity
container_binds: [/groups, /scratch]
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
| `warp_aretomo` | WarpTools motion/CTF, tilt-series alignment (AreTomo / IMOD patch), CTF, reconstruction, particle export (needs Warp 2.0.0dev40+) | `warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def` |
| `relion` | subtomogram extraction, particle reconstruction, Class3D, template/mask preparation | `relion5.0_tomo.def` |
| `pytom` | template matching + candidate extraction | `pytom_match_pick_0.10.0.def` |
| `cryocare` | cryoCARE denoising | `cryocare.def` |
| `isonet` | IsoNet2 denoising (alternative to cryoCARE) | `isonet2.def` |
| `imod` | IMOD utilities used by the pick viewer | `imod.def` |
| `pymol` | template generation from a PDB/mmCIF model | `pymol.def` |
| `miss_alignment` | learned tilt-series alignment refinement | `miss_alignment_torch2.8.0_cuda12.9.def` |
| `cistem` | template simulation (`simulate`), native binary | — |

The ChimeraX/ArtiaX curation image (`chimerax_artiax_GL.def`) is configured separately under
`curation.sif_path`.

</details>

<details>
<summary><b>Building containers (optional)</b></summary>

Definition files live in `container_defs/`. The names and locations of the built `.sif` files don't
matter as long as `conf.yaml` points at them.

```bash
apptainer build --fakeroot --nv relion5.0_tomo.sif container_defs/relion5.0_tomo.def
apptainer build --fakeroot --nv cryocare.sif       container_defs/cryocare.def
apptainer build --fakeroot --nv warp_aretomo.sif   container_defs/warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def
```

</details>

<details>
<summary><b>Adapting the SLURM template <code>config/qsub.sh</code></b></summary>

Every job CryoBoost queues is built from this one script (created from `config/qsub.template.sh`).
Per-job resources come from the UI via RELION-style placeholders (`XXXextra1XXX` … `XXXextra8XXX` =
partition, constraint, nodes, ntasks-per-node, cpus-per-task, gres, mem, time; an empty constraint
drops the `--constraint` line), so you only define the cluster environment once:

- **SLURM HEADER** — the module loads (or equivalent) that make the server's Python start on a
  compute node and put `apptainer` on PATH. Our Lmod lines are in the template as a commented example.
- **Optional** — `#SBATCH --account` / `--qos` / `--exclude` lines your cluster needs.
- **Leave untouched** — every `XXX...XXX` placeholder, the `RELION_JOB_EXIT_*` marker block (matched
  verbatim; CryoBoost refuses to submit without it), and the final `exit $EXIT_CODE` (job dependencies
  chain on it). `preflight.py` checks all three.

If you already have a working RELION/Warp SLURM script, its module lines are the right starting point.

</details>

<details>
<summary><b>Upgrading an existing install (older <code>conf.yaml</code> / <code>qsub.sh</code>)</b></summary>

`git pull` never touches `config/conf.yaml` or `config/qsub.sh`, so an install set up from older templates
keeps its old files. The safest route is to start both again from the current templates and carry your
values across. If you edit the old files instead, compare them with `config/conf.template.yaml` and
`config/qsub.template.sh`; they must have:

**`config/conf.yaml`**
- `container_runtime` and `container_binds`. A container call binds `/tmp`, the project, the raw-data
  directories and the gain reference by itself; every other place your data, references or software live
  goes in `container_binds`.
- `slurm_defaults.partition` and `supervisor_slurm.partition` (there are no built-in partitions), and
  `curation.partition` if `curation.sif_path` is set.
- Tool entries under `tools:` with the template's key names (`warp_aretomo`, `relion`, …).
  `tools.cistem.bin_path` is the directory holding `simulate`, not the binary.
- No `crboost_root:` and no `use_afterok_orchestrator:` (jobs are always submitted as SLURM afterok
  chains); `crboost_python:` is the optional interpreter override. Preflight warns about every key nothing
  reads (a misspelling, or a key from an older template): act on those warnings.
- `tilt_filter.models.<name>.path` pointing at a weights file that exists. Weights (`*.pth`) are not in
  git: copy `filterTilts/tiltnet_resnet18_o1034.pth` into your checkout by hand (ask the maintainers), or
  leave `tilt_filter.models` empty to review tilts by hand only. Preflight does not check this file.

**`config/qsub.sh`**
- The exit-marker block, byte for byte as in the template (`if [ $EXIT_CODE -eq 0 ]; then` … `fi`), and
  the final `exit $EXIT_CODE`. Without the block crboost refuses to submit; without the exit line a
  failed job exits 0 and the jobs after it still run. Preflight checks both.
- The old `ENV PATHS` block (`CRBOOST_SERVER_DIR`, `CRBOOST_PYTHON`) is no longer read; delete it.

**Rebuilding `venv/` on uv's Python** (an install whose venv was made from a cluster module's Python):
stop the server and let running jobs finish (they run the venv's interpreter), then `rm -rf venv`,
`uv python install 3.11`, `uv venv -p 3.11 venv`, `uv pip sync -p venv/bin/python3 requirements.txt`, and
delete the Python module lines from `config/qsub.sh`.

**Then:** `uv pip sync -p venv/bin/python3 requirements.txt` (or `venv/bin/pip install -r requirements.txt`),
`venv/bin/python3 preflight.py` (must exit 0),
`venv/bin/python3 -m pytest -m cluster`, and restart the server. Every browser needs the newly printed
`?token=` link once (see *Starting the server* below). The `.nicegui/` folder in the checkout no longer
holds sessions or browser storage; you can delete it.

</details>

<details>
<summary><b>Starting the server and tunnelling in</b></summary>

```bash
venv/bin/python3 main.py
```

```
On your own computer (not here), open the tunnel:
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L 24305:127.0.0.1:24305 $USER@<headnode>
Then open in your browser:
http://localhost:24305/?token=Xq3…
```

**The port.** CryoBoost servers take their ports from one small block, 24300–24331, and each checkout
gets the same port every time it is free (remembered in `~/.crboost/ports.json`), so the tunnel
command and bookmarks stay the same. Before it binds, the server looks at what holds the ports on this
host: if this checkout is already running and answering, it prints that server's links and stops instead
of starting a second one. A taken port is skipped, and a port given with `--port` that is taken is
reported with whoever holds it. `--host` binds one address instead of every interface.

**Leftovers.** Every start also lists what of yours on this host is left over from earlier sessions, with
the `kill -9 …` command that ends them: an `ssh -L` tunnel to the headnode itself (the tunnel command
pasted here instead of on your computer: it holds the port, and a browser tunnel that lands on it hangs),
a CryoBoost server that is stopped (Ctrl-Z) or no longer answers, and CryoBoost worker processes whose
server is gone. Nothing is killed for you.

The tunnel command ends with an error rather than lingering when it cannot take its port: on your
computer an old tunnel may still hold it (`pkill -f 'ssh.*-L <port>:'` ends that one), and on the
headnode the server holds it. `ServerAliveInterval` ends a tunnel whose connection died (laptop asleep,
VPN switched) instead of leaving every page hanging.

Below the two pinned lines comes a faint startup snapshot: user, cluster, node/GPU totals, your SLURM
jobs grouped by job directory, and your five most recently active projects with their status and a
tokened link each. Already on the cluster network? Replace `localhost` with the headnode's name.

**The token.** Like Jupyter, the server only answers browsers that carry its token: anyone who reaches
the port could otherwise submit jobs and read files as you. Open one of the printed links; the server
moves the token into a cookie and drops it from the address bar, and links within the app (deep links
included) then work as usual. A browser without it gets 401.

- The token is in `~/.crboost/token` (mode 600) of the user running the server. It is reused across
  restarts, so bookmarks and open tabs keep working.
- **Rotate it** by deleting `~/.crboost/token` and restarting the server; every browser then needs the
  newly printed link.
- Treat a tokened link like a password: it is in your browser history, and whoever has it controls
  the server as you. Don't paste it into chats, screenshots or bug reports.
- Plain HTTP carries the token unencrypted on the internal network; the SSH tunnel is encrypted.

Browser-side state (session, preferences) lives in `~/.crboost/nicegui/` and its session cookie is
named per port, so two servers on one host — say a development checkout and the shared install — can be
open side by side in one browser.

From your local machine (still on the cluster's network, physically or via VPN):

```bash
ssh -L ${LOCAL_PORT}:127.0.0.1:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# background:
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L ${LOCAL_PORT}:127.0.0.1:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# kill when done:
pkill -f "ssh.*${LOCAL_PORT}:127.0.0.1:${HEADNODE_PORT}"
```

`LOCAL_PORT` is any free port on your computer, `HEADNODE_PORT` is the port the server listens on; open
`http://localhost:${LOCAL_PORT}/?token=…` with the printed token.
This assumes your public key is already on the cluster. Saving the tunnel in your `~/.ssh/config` is
handy:

```
Host cryoboost-tunnel
    HostName [YOUR CLUSTER NODE]
    User [YOUR USERNAME]
    LocalForward 24305 127.0.0.1:24305
    ExitOnForwardFailure yes
    ServerAliveInterval 30
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
