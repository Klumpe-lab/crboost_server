[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Klumpe-lab/crboost_server)

# CryoBoost Server

CryoBoost Server is a web-based cryo-electron tomography pipeline manager. It runs as a single
long-lived process on your cluster's **headnode**, serves a browser UI (NiceGUI/FastAPI), and
orchestrates RELION-based tomography pipelines as SLURM jobs on your compute nodes. External tools
— RELION, Warp/AreTomo, PyTOM, CryoCARE, IsoNet, IMOD, easymode, ChimeraX/ArtiaX — run inside
Apptainer containers (or as native binaries), configured per tool in `config/conf.yaml`.

You point it at your movies and mdocs, build a pipeline in the UI (or start from a protocol bundle),
and it submits, monitors and reconciles the jobs for you. The server runs on the headnode; you use it
from the browser on your own computer through an SSH tunnel.

- **[Quick start](#quick-start)** — CryoBoost is already installed on your cluster; you want to start it
  and open it in your browser.
- **[Installation](#installation)** — setting it up from scratch on a headnode.

## Quick start

This assumes someone has already installed CryoBoost on your cluster (see [Installation](#installation)
otherwise) and you know the directory it lives in, called `<install>` below. Every user runs their own
server from the same install: it submits jobs as you and only you can open it.

**1. Log in to the headnode and start a `tmux` session.** The server runs in the foreground and stops
when the terminal that started it closes; inside `tmux` (or `screen`) it survives a dropped connection
or a closed laptop.

```bash
ssh <your-username>@<headnode>
tmux new -s crboost          # later, to come back to it: tmux attach -t crboost
```

**2. Start the server.**

```bash
cd <install>
./crboost
```

It picks a free port by itself (the same one every time it is free) and, after a few seconds, pins
three lines to the top of the terminal — the port and token below are examples:

```
On your own computer (not here), open the tunnel:
ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L 24305:127.0.0.1:24305 $USER@headnode01
Then open in your browser:
http://localhost:24305/?token=Xq3…
```

If your server is already running (you started it earlier and forgot), `./crboost` prints that server's
links and stops instead of starting a second one.

**3. Open the tunnel — on your own computer, not on the headnode.** Copy the `ssh -f -N …` line into a
terminal on your laptop/desktop and run it. It returns at once and keeps forwarding in the background.

- `$USER` is filled in by *your* computer's shell. If your username there differs from your cluster
  username, replace `$USER` with your cluster username.
- `headnode01` is the headnode's name as the headnode knows it. If that name doesn't resolve from your
  computer, use whatever you put after `@` when you log in with `ssh` (and connect to the VPN first if
  you need one for that).
- Already on a machine inside the cluster network? Skip the tunnel and replace `localhost` in the link
  with the headnode's name.

**4. Open the link** (`http://localhost:<port>/?token=…`) in your browser. The token is what lets your
browser in: after the first visit it is kept in a cookie and drops out of the address bar, so later you
can just open `http://localhost:<port>/` or a bookmark. A page that says **401** means the browser has no
token yet — open the full printed link once.

**Stopping.** `Ctrl-C` in the server's terminal. Running SLURM jobs are not affected; they keep running,
and the server picks up their status again the next time it starts.

**When something doesn't work**

| Symptom | Cause and fix |
|---|---|
| The tunnel command says `Address already in use` | An old tunnel on your computer still holds the port. End it with `pkill -f 'ssh.*-L <port>:'` and run the tunnel command again. |
| The browser page loads forever | Usually the tunnel command was pasted on the headnode instead of your computer. `./crboost` lists such a leftover tunnel at startup with the `kill` command that ends it; then open the tunnel on your own computer. Otherwise the tunnel died (laptop asleep, VPN switched): run the tunnel command again. |
| `401` | Open the full `?token=` link once (see step 4). |
| The page was fine, then everything stops responding | The server stopped (its terminal closed, or `Ctrl-C`). `tmux attach -t crboost` to look; start it again with `./crboost`. |
| `./crboost` lists *leftovers* | Processes of yours from earlier sessions (a stopped server, orphaned workers, a tunnel pasted on the headnode), each with the `kill -9 …` that ends it. Nothing is killed for you. |

More about ports, tokens and tunnels: [Server, port, token and tunnel in detail](#server-port-token-and-tunnel-in-detail).

---

## Installation

Done once per cluster, by whoever maintains the install. Plan for an afternoon, most of it building or
collecting containers and waiting for the self-test jobs in the queue.

### What you need

- A Linux headnode with **SLURM** (`sbatch`, `squeue`, `sacct`, `sinfo` on PATH) and **Apptainer** or
  Singularity, from which you can submit jobs.
- Compute nodes with **NVIDIA GPUs** and Apptainer available (directly, or via a `module load` you add to
  `config/qsub.sh`).
- A **shared filesystem** that the headnode and every compute node see at the same path. The checkout,
  the Python environment, the containers, your raw data and your projects all have to live on it: jobs on
  the compute nodes run the same Python and the same files the server uses.
- [**uv**](https://docs.astral.sh/uv/), one static binary that brings its own Python 3.11 (step 2 installs it).
- Disk: about 6 GB for the checkout and Python environment (mostly PyTorch), 1–12 GB per container.

### 1. Clone the repository on the shared filesystem

```bash
cd /path/on/shared/filesystem          # e.g. /groups/<your_group>/software
git clone https://github.com/Klumpe-lab/crboost_server.git
cd crboost_server
```

This directory is `<install>` everywhere else in this README.

### 2. Create the Python environment

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh     # once per user; puts uv in ~/.local/bin
uv python install 3.11
uv venv -p 3.11 venv
uv pip sync -p venv/bin/python3 requirements.txt
```

This makes `<install>/venv`, built on uv's own Python 3.11, so a compute node needs no Python module.
uv keeps that Python in `~/.local/share/uv/python` and its download cache in `~/.cache/uv`.

**If other people will use this install,** that Python must be somewhere they can read, not in your home.
Before `uv python install`, point uv at a shared directory:

```bash
export UV_PYTHON_INSTALL_DIR=/groups/<your_group>/software/uv/python
export UV_CACHE_DIR=/groups/<your_group>/software/uv/cache
```

Prefer conda or pip? See [Python environment: venv, conda or uv](#python-environment-venv-conda-or-uv).

### 3. Get the containers

Each tool CryoBoost drives runs from an Apptainer image (`.sif`). Definition files for most of them are in
`container_defs/`; build the ones you need on a machine where you may run `apptainer build --fakeroot`,
and put the `.sif` files somewhere on the shared filesystem:

```bash
mkdir -p /groups/<your_group>/software/containers
cd /groups/<your_group>/software/containers
apptainer build --fakeroot --nv warp_aretomo.sif <install>/container_defs/warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def
apptainer build --fakeroot --nv relion5.sif      <install>/container_defs/relion5.0_tomo.def
apptainer build --fakeroot --nv pytom.sif        <install>/container_defs/pytom_match_pick_0.10.0.def
# ... and the others you want, see the table below
```

Which tool does what, and which ones you need:

| `tools:` key in `conf.yaml` | used for | definition in `container_defs/` |
|---|---|---|
| `warp_aretomo` | **core:** motion/CTF, tilt-series alignment (AreTomo / IMOD patch), CTF, reconstruction, particle export (Warp 2.0.0dev40+) | `warp_2.0.0dev41_aretomo1.0.0_cuda12.9.def` |
| `relion` | **core:** subtomogram extraction, particle reconstruction, Class3D, Refine3D, template/mask preparation | `relion5.0_tomo.def` |
| `pytom` | template matching + candidate extraction | `pytom_match_pick_0.10.0.def` |
| `cryocare` | cryoCARE denoising | `cryocare.def` |
| `isonet` | IsoNet2 denoising (alternative to cryoCARE) | `isonet2.def` |
| `easymode` | Segment job (easymode + Ais + Pom); needs its weights downloaded once into `cache_dir` | not in the repo yet |
| `imod` | IMOD utilities used by the pick viewer | `imod.def` |
| `pymol` | template generation from a PDB/mmCIF model | `pymol.def` |
| `miss_alignment` | learned tilt-series alignment refinement | `miss_alignment_torch2.8.0_cuda12.9.def` |
| `cistem` | template simulation (`simulate`); a native cisTEM install, not a container | — |

You don't need all of them. A tool you leave out only breaks the jobs that use it.

### 4. Create the two config files

```bash
cd <install>
venv/bin/python3 preflight.py
```

The first run copies the templates to `config/conf.yaml` and `config/qsub.sh` and stops. These two files
are yours: git ignores them and `git pull` never touches them. Steps 5 and 6 edit them in any text editor
(`nano config/conf.yaml` if you have no favourite).

### 5. Edit `config/conf.yaml`

The file is long, but most of it is per-job resource defaults that work as they are. What you have to
change, top to bottom:

| Key | What to put there | Find it with |
|---|---|---|
| `crboost_python` | Leave `""`. | — |
| `local.DefaultProjectBase` | An existing, writable directory on the shared filesystem where new projects are created. Users can pick another one per project. | — |
| `local.DefaultMoviesGlob`, `DefaultMdocsGlob` | Optional pre-fill for the import form, e.g. `"/data/session1/*.eer"`. Leave `""` if unsure. | — |
| `slurm_defaults.partition` | Your GPU partition. | `sinfo -s` |
| `slurm_defaults.gres` | `"gpu:1"` works on most clusters; some name the card type (`"gpu:a100:1"`). | `sinfo -o '%P %G'` |
| `slurm_defaults.time` | Must not exceed your partition's/QOS's time limit. | `sinfo -o '%P %l'` |
| `supervisor_slurm.partition` | A partition for small CPU-only jobs (1 CPU, 4 GB, several hours) that watch the array jobs. Your GPU partition works too. | `sinfo -s` |
| `job_resource_profiles` | Per-job memory/GPU/time defaults; leave as they are. If a `time:` there exceeds your limits, lower it. Users can change resources per job in the UI. | — |
| `curation` | ChimeraX/ArtiaX manual-picking sessions. Not using them: set `sif_path: ""`. Using them: point `sif_path` at the `.sif` built from `container_defs/chimerax_artiax_GL.def` and set `partition`. | — |
| `species_catalog_root` | Optional shared directory for a lab-wide catalog of particle species. Leave `""`. | — |
| `tilt_filter` | The deep-learning tilt classifier. Its weights (`*.pth`) are not in git — ask the maintainers for `tiltnet_resnet18_o1034.pth` and copy it to `<install>/filterTilts/`. Without it, replace the whole block with `tilt_filter: {models: {}}`; tilts are then reviewed by hand. | — |
| `container_runtime` | `apptainer` or `singularity`. | `which apptainer singularity` |
| `container_binds` | Top-level directories your data and software live under, e.g. `[/groups, /scratch]`. Every container call binds these at the same path, so a container can read anything a user points the UI at. | `df -h` |
| `tools.<name>.container_path` | The full path to each `.sif` from step 3. **Delete the whole entry** of any tool you don't have — an entry still pointing at `/path/to/…` fails preflight. | — |
| `tools.cistem.bin_path` | The directory that holds cisTEM's `simulate` (not the binary itself), or delete the entry. | `which simulate` |
| `tools.easymode.cache_dir` | Where the easymode weights live, or delete the entry. | — |

For example, a minimal site that only has Warp, RELION and PyTOM:

```yaml
crboost_python: ""
local:
  DefaultProjectBase: "/groups/mylab/cryoboost_projects"
  DefaultMoviesGlob: ""
  DefaultMdocsGlob: ""
slurm_defaults:
  partition: "gpu"
  # ... rest as in the template
supervisor_slurm:
  partition: "cpu"
  # ... rest as in the template
# job_resource_profiles, processing_defaults: as in the template
curation:
  sif_path: ""
species_catalog_root: ""
tilt_filter: {models: {}}
container_runtime: apptainer
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

All keys, and what preflight checks about them: [Configuration reference](#configuration-reference-configconfyaml).

### 6. Edit `config/qsub.sh`

Every job CryoBoost submits is this script with the `XXX…XXX` placeholders filled in. You usually change
at most three things:

1. **Make `apptainer` available on a compute node.** Log in to a compute node (`srun --pty bash`) and run
   `which apptainer`. Found: nothing to do. Not found: add the line that makes it available (e.g.
   `module load apptainer`) in the `SLURM HEADER` section, below the commented example.
2. **Account / QOS.** If your cluster requires `--account` or `--qos` on every job, add
   `#SBATCH --account=<yours>` / `#SBATCH --qos=<yours>` next to the other `#SBATCH` lines.
3. **Broken nodes.** To keep jobs off particular nodes, remove one `#` from the `##SBATCH --exclude=` line
   and list them.

Leave everything else as it is: every `XXX…XXX` placeholder, the `RELION_JOB_EXIT_*` block and the final
`exit $EXIT_CODE`. CryoBoost refuses to submit without them; preflight checks all three. Details:
[The SLURM template](#the-slurm-template-configqsubsh).

### 7. Run preflight until it passes

```bash
venv/bin/python3 preflight.py
```

It checks the Python packages, the config, every tool path and bind directory, SLURM and Apptainer, and
the partitions you named. Each failed check is one line saying what to fix; edit, run again, until it
exits without failures. Warnings (e.g. a key in `conf.yaml` that nothing reads, usually a misspelling)
don't stop it but are worth reading. What it checks: [Preflight and self-test](#preflight-and-self-test).

### 8. Run the self-test

```bash
venv/bin/python3 -m pytest              # headnode checks, seconds
venv/bin/python3 -m pytest -m cluster   # one short SLURM job per configured tool, minutes (mostly queue)
```

The cluster test proves each tool starts in its container on a compute node, sees the bound directories
and, where it needs one, the GPU. A failure prints the command it ran and the job's log files (under
`~/.crboost/selftest/`).

### 9. Start it

Continue with [Quick start](#quick-start), step 1. Tell your users the `<install>` path; they need nothing
else.

### Updating

```bash
cd <install>
git pull
uv pip sync -p venv/bin/python3 requirements.txt
venv/bin/python3 preflight.py           # a new required key shows up here
```

Then restart the server(s). `git pull` never changes your `config/conf.yaml` or `config/qsub.sh`; when the
templates gained something you need, preflight says so. Coming from a much older install:
[Upgrading an existing install](#upgrading-an-existing-install).

---

## Reference

### Server, port, token and tunnel in detail

**The port.** CryoBoost servers take their ports from one small block, 24300–24331. Each checkout gets the
same port every time it is free (remembered per user in `~/.crboost/ports.json`), so the tunnel command
and bookmarks stay the same. A taken port is skipped. `./crboost --port N` asks for a specific port (a
taken one is reported with whoever holds it); `--host` binds one address instead of every interface;
`--debug` logs at DEBUG.

**The token.** Like Jupyter, the server only answers browsers that carry its token: anyone who reaches the
port could otherwise submit jobs and read files as you.

- The token is in `~/.crboost/token` (mode 600) of the user running the server. It is reused across
  restarts, so bookmarks and open tabs keep working.
- **Rotate it** by deleting `~/.crboost/token` and restarting the server; every browser then needs the
  newly printed link.
- Treat a tokened link like a password: it is in your browser history, and whoever has it controls the
  server as you. Don't paste it into chats, screenshots or bug reports.
- Plain HTTP carries the token unencrypted on the internal network; the SSH tunnel is encrypted.

Browser-side state (session, preferences) lives in `~/.crboost/nicegui/`, and its session cookie is named
per port, so two servers on one host — say a development checkout and the shared install — can be open
side by side in one browser.

**The tunnel.** The printed command forwards port `<port>` on your computer to the same port on the
headnode. `ExitOnForwardFailure` makes it fail loudly instead of going to the background without the port;
`ServerAliveInterval` ends a tunnel whose connection died instead of leaving every page hanging. To use a
different local port, change the first number: `-L 8080:127.0.0.1:24305` serves the server on
`http://localhost:8080/`. To end a background tunnel: `pkill -f 'ssh.*-L <port>:'`. A tunnel you use daily
can live in `~/.ssh/config` on your computer:

```
Host cryoboost
    HostName <headnode>
    User <your-cluster-username>
    LocalForward 24305 127.0.0.1:24305
    ExitOnForwardFailure yes
    ServerAliveInterval 30
```

Then `ssh -f -N cryoboost` opens it.

**The startup screen.** Below the pinned links comes a faint snapshot: user, cluster, node/GPU totals, your
SLURM jobs grouped by job directory, and your five most recently active projects with a link each. Logs go
to `~/.crboost/logs/crboost-<port>.log`; the terminal only shows events and warnings.

### Python environment: venv, conda or uv

Pipeline drivers running inside SLURM jobs are launched with the interpreter that runs `crboost` (or
`crboost_python` in `config/conf.yaml`, if you set it). Any environment works — a venv, a conda/mamba env,
uv — as long as it and the repo are on a shared filesystem and that interpreter starts on a compute node
with whatever `config/qsub.sh` loads.

The uv venv from the installation runs on uv's own Python, so a compute node needs no module for it. A
module loaded in your shell does no harm to such a venv: `crboost` and `preflight.py` start themselves
again without the shell's `PYTHONPATH` when the venv has its own packages (`clean_env.py`), so the server
and every job it submits (sbatch passes the server's environment on) see only the venv. `./crboost` started
with an older system `python3` hands itself to `venv/bin/python3` the same way.

pip works on the same lock: `python3 -m venv venv && venv/bin/pip install -r requirements.txt`. The
interpreter is then whichever `python3` built the venv, and `config/qsub.sh` must make it start on a
compute node (load its module in the `SLURM HEADER`). A cluster module that puts packages on `PYTHONPATH`
shadows the venv's; preflight warns about each one.

`requirements.txt` is a generated lock: to change a dependency, edit `requirements.in` and recompile with
the `uv pip compile` line at the top of that file.

### Preflight and self-test

`venv/bin/python3 preflight.py` creates `config/conf.yaml` and `config/qsub.sh` from their templates on the
first run and stops. Every later run only verifies (it never edits):

- Python version, and that every package in `requirements.txt` imports; one imported from outside the venv
  (a cluster module on `PYTHONPATH`) is a warning naming its path
- `config/conf.yaml` loads, `DefaultProjectBase` is writable, every `tools:` path and `container_binds`
  entry exists, the partitions are set; a key no setting reads (a misspelling) is a warning
- `sbatch`/`squeue`/`sacct`/`sinfo` and the container runtime are on PATH, and every configured partition
  exists
- `config/qsub.sh` still has its placeholders, exit-marker block and `exit $EXIT_CODE`

It exits non-zero and lists each failed check. It does not check the tilt-filter weights file or the
curation image.

`venv/bin/python3 -m pytest` runs the same checks as tests, plus a render of `config/qsub.sh` (headnode,
seconds). `venv/bin/python3 -m pytest -m cluster` submits one short job per configured tool, run the way a
pipeline driver runs it, and waits for all of them: each proves the tool starts in its container on a
compute node with the site's binds and sees the GPU where it needs one. Job directories are under
`~/.crboost/selftest/`; a failure prints the command and the job's log paths.

### Configuration reference (`config/conf.yaml`)

`config/conf.template.yaml` documents every key in its comments; this is the overview. A per-user file
`~/.crboost/conf.yaml`, if present, is laid over the install's `config/conf.yaml` for that user only.

**Paths.**

```yaml
local:
  DefaultProjectBase: "/path/to/your/projects"      # where new projects go
  DefaultMoviesGlob : "/path/to/your/movies/*.eer"  # optional, also settable in the UI
  DefaultMdocsGlob  : "/path/to/your/mdocs/*.mdoc"  # optional, also settable in the UI
```

**SLURM.** Three layers, later ones winning: `slurm_defaults` (every job) ← `job_resource_profiles.<job
type>` (only the fields given there) ← what the user sets on the job in the UI. `supervisor_slurm` is the
small CPU job that submits a per-tilt-series SLURM array, waits for it and collects its results.
`constraint` restricts jobs to nodes with given features (`sinfo -o '%N %f'`); usually leave it blank.

**Tools.** Each external tool is either an Apptainer image (`exec_mode: "container"` + `container_path`) or
a native install (`exec_mode: "binary"` + `bin_path`, the directory holding it — crboost puts it first on
`PATH`). A tool without an entry fails the job that needs it; nothing falls back to a guessed binary.
Container calls bind `/tmp`, the project directory, the raw-data directories and the gain reference by
themselves; everything else your data and software need goes under `container_binds`. The tool keys are in
the table under [Installation step 3](#3-get-the-containers). The ChimeraX/ArtiaX curation image is
configured separately under `curation.sif_path`.

**Building containers.** The names and locations of the `.sif` files don't matter as long as `conf.yaml`
points at them. `--fakeroot` needs your site to allow it; otherwise build on a machine where you have root
and copy the `.sif` over.

### The SLURM template (`config/qsub.sh`)

Every job CryoBoost queues is built from this one script (created from `config/qsub.template.sh`). Per-job
resources come from the UI via RELION-style placeholders (`XXXextra1XXX` … `XXXextra8XXX` = partition,
constraint, nodes, ntasks-per-node, cpus-per-task, gres, mem, time; an empty constraint drops the
`--constraint` line), so you only define the cluster environment once:

- **SLURM HEADER** — the module loads (or equivalent) that put `apptainer` on PATH on a compute node, and,
  if your Python environment is not the uv venv, make its interpreter start there. A commented Lmod
  example is in the template.
- **Optional** — `#SBATCH --account` / `--qos` / `--exclude` lines your cluster needs.
- **Leave untouched** — every `XXX...XXX` placeholder, the `RELION_JOB_EXIT_*` marker block (matched
  verbatim; CryoBoost refuses to submit without it), and the final `exit $EXIT_CODE` (job dependencies
  chain on it).

If you already have a working RELION/Warp SLURM script, its module lines are the right starting point.

### Upgrading an existing install

`git pull` never touches `config/conf.yaml` or `config/qsub.sh`, so an install set up from older templates
keeps its old files. The safest route is to start both again from the current templates and carry your
values across. If you edit the old files instead, compare them with `config/conf.template.yaml` and
`config/qsub.template.sh`; they must have:

**`config/conf.yaml`**
- `container_runtime` and `container_binds` (see the configuration reference).
- `slurm_defaults.partition` and `supervisor_slurm.partition` (there are no built-in partitions), and
  `curation.partition` if `curation.sif_path` is set.
- Tool entries under `tools:` with the template's key names (`warp_aretomo`, `relion`, …).
  `tools.cistem.bin_path` is the directory holding `simulate`, not the binary.
- No `crboost_root:` and no `use_afterok_orchestrator:` (jobs are always submitted as SLURM afterok
  chains); `crboost_python:` is the optional interpreter override. Preflight warns about every key nothing
  reads (a misspelling, or a key from an older template): act on those warnings.
- `tilt_filter.models.<name>.path` pointing at a weights file that exists, or `tilt_filter: {models: {}}`.

**`config/qsub.sh`**
- The exit-marker block, byte for byte as in the template (`if [ $EXIT_CODE -eq 0 ]; then` … `fi`), and
  the final `exit $EXIT_CODE`. Without the block crboost refuses to submit; without the exit line a failed
  job exits 0 and the jobs after it still run. Preflight checks both.
- The old `ENV PATHS` block (`CRBOOST_SERVER_DIR`, `CRBOOST_PYTHON`) is no longer read; delete it.

**Rebuilding `venv/` on uv's Python** (an install whose venv was made from a cluster module's Python): stop
the server and let running jobs finish (they run the venv's interpreter), then `rm -rf venv`,
`uv python install 3.11`, `uv venv -p 3.11 venv`, `uv pip sync -p venv/bin/python3 requirements.txt`, and
delete the Python module lines from `config/qsub.sh`.

**Then:** `uv pip sync -p venv/bin/python3 requirements.txt`, `venv/bin/python3 preflight.py` (must exit
0), `venv/bin/python3 -m pytest -m cluster`, and restart the server with `./crboost` (it was `main.py`
before). Every browser needs the newly printed `?token=` link once. The `.nicegui/` folder in the checkout
no longer holds sessions or browser storage; you can delete it.

## Further documentation

`docs/` — [architecture](docs/architecture.md), [protocols](docs/protocols.md),
[particle data flow](docs/particle-data-flow.md), [binning and resolution](docs/binning-and-resolution.md),
[known bugs](docs/known_bugs.md).
