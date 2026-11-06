# Roadmap 39 — Python environment and setup: a locked, self-contained environment built with uv

**Pending (maintainer, 2026-10-26):** wanted; move to uv, plus §6 (start-up cost of every array task first).
**Status:** S0–S2 done (lab on uv since 2026-10-28; records in §3). 2026-11-06: the install became self-contained
(`install.sh`, `.uv/` inside the checkout, `crboost` an sh launcher; `f49ad87`) and torch moved to 2.8.0/cu128 for the
B200 nodes (`d12f371`); §2 notes what that revised. The same evening S3 and S6 were built on the working tree, not yet
committed and NOT RUN on a project or the lab: S6 makes the environment a build product named by its inputs (no `venv/`
to repair, no rebuild in place, `./crboost` finds or builds it), S3 moves the tilt filter's torch into the
`tilt_filter` container, so an environment is 0.8 GB instead of 7.3. The records are in §3; next, the maintainer's
steps in S6 ("By hand"), then S4, S5 and §6.

**In one line.** Today the server's environment is a venv built on the cluster's Python module. Some of its packages
are probably not in the venv at all, and most of its versions are not pinned. This roadmap makes that environment
one that installs the same everywhere and runs on a compute node without module loads. It also takes the 5 GB of
PyTorch out of it, and adds the checks that keep it that way: CI, a pinned ruff, labelled containers and
checksummed weights.

## 1. What is there now

*(The state of 2026-10-22, before S0–S2; §3 records what changed since.)*

- **The interpreter is the cluster's.** `venv/pyvenv.cfg` points at
  `/software/f2022/software/python/3.11.5-gcccore-13.2.0`. A compute node can start the venv only after the same
  `module load python/...`, which is why `config/qsub.sh` carries module lines. Every other site has to rebuild that
  module stack before a driver starts.
- **numpy, pandas, scipy and requests are not in the venv.** `venv/lib/python3.11/site-packages` has no
  `numpy*`, `pandas*`, `scipy*` or `requests*` entry. `~/.profile` loads
  `build-env/f2022 python/3.11.5-gcccore-13.2.0 gcccore/13.2.0 arrow/16.1.0-gfbf-2023b`. An EasyBuild `arrow` module
  normally brings SciPy-bundle (numpy, pandas, scipy) on `PYTHONPATH`. So the server most likely imports these from
  the module tree, and so do the drivers on the compute nodes, which load the same modules. Not verified, because
  `/software` is not mounted where this was written; stage 0 settles it.
- **Most versions float.** `requirements.txt` pins nicegui, fastapi, uvicorn, pydantic, biopython, scikit-image,
  Pillow, starfile, mrcfile, tifffile, torch and torchvision. It leaves numpy, pandas, scipy, matplotlib, pyyaml,
  requests and every transitive dependency to whatever is newest on the day of the install. A new install (Munich,
  a fresh lab checkout) resolves a different set from the one the code was last run on.
- **PyTorch is 5.0 of the venv's 5.6 GB** (`torch` 1.6 GB, `nvidia` CUDA libraries 2.7 GB, `triton` 0.7 GB). The
  only host-side user is the DL tilt filter (`drivers/tilt_filter.py` → `filterTilts/deepLearning/model_loader.py`).
  Miss-alignment imports torch inside its own container. The torch 2.6.0 pin exists only because the host venv must
  start on the CentOS 7 compute nodes (the comment in `requirements.txt`).
- **No CI.** There is no `.github/`. `ruff check .` currently fails with 6 E501 errors in
  `services/curation/session_service.py:290` and `services/jobs/fs_motion_ctf.py:54–64`.
- **ruff is unpinned.** It is not in any requirements file (the venv has 0.15.12). Formatter output changes between
  ruff versions, so two checkouts can reformat each other's lines.
- **Containers carry no version.** The `.sif` files are built by hand from `container_defs/`. Nothing records which
  definition and version an image came from, so preflight cannot tell an image that is out of date.
- **The tilt-filter weights travel by hand.** `filterTilts/*.pth` is git-ignored and copied between installs; nothing
  checks that the file is the one `conf.yaml` describes.

## 2. Decisions owed

**Answered 2026-10-27 (maintainer):** D1, D2, D3, D5, D6 as recommended. D4: uv's Python and cache go under
`/groups/klumpe/software/cache_and_artifacts/uv/{python,cache}`. D7: the weights are not published yet; Hugging
Face is the likely place, licence still to check.

**Revised 2026-11-06:** D4 — uv's Python and cache now live inside each checkout (`.uv/python`, `.uv/cache`; built by
`install.sh`), so an install depends on nothing outside its folder. D6 — the tilt filter's torch is 2.8.0 / CUDA 12.8,
as the venv has had it since `d12f371`: 2.6.0/cu124 has no sm_100 kernels and died on the B200 nodes. 2.8.0 needs
glibc >= 2.28 (`requirements.in`); the maintainer chose to leave older nodes out, so the old-glibc reason for 2.6.0
(§1) is gone.

**Answered 2026-11-06, evening (maintainer):** the environment is a build product named by its inputs (S6, D8–D13), and
the tilt filter's torch lives in a container (S3), not in the environment and not as CPU torch. D3 (keep `venv/`) now
holds only as a convenience link; D4's shared uv directory is gone with the rest of the outside-the-checkout state.

| # | Question | Recommendation |
|---|---|---|
| D1 | Lock format: `requirements.in` compiled to a pinned `requirements.txt` (`uv pip compile`), or `pyproject.toml` + `uv.lock` + `uv sync` | The compiled `requirements.txt`. pip keeps working on it unchanged, and it is the smaller step; `pyproject.toml` can follow if the repo ever becomes an installable package |
| D2 | Hashes in the lock (`--generate-hashes`) | Not yet. pip then requires a hash for every package, which turns any hand-added package into a failed install |
| D3 | The venv's directory: keep `venv/`, or move to `.venv/` (uv finds `.venv` by itself) | Keep `venv/`; every README command and the habit of everyone who runs it point there. Pass `-p venv` to uv |
| D4 | Where uv's Python and cache live for the shared lab install (`/groups/klumpe/software/crboost_server`) | On the same filesystem as that checkout (e.g. `/groups/klumpe/software/uv/{python,cache}`). It must be visible from the compute nodes and must not sit in one person's home. Same filesystem as the venv keeps the cache's hardlinks working |
| D5 | Whether the README keeps a pip route | Yes, in a dropdown: the compiled `requirements.txt` installs with pip too, but the interpreter is then the module's again |
| D6 | The tilt filter's torch inside its container | torch 2.6.0 / CUDA 12.4 as now: no behaviour change. Upgrading is a separate, later choice that the container makes free (the compute nodes' glibc no longer matters) |
| D7 | Where the weights are published, and whether the model author's licence allows it in a public place | Owed by the maintainers |

## 3. Stages

### S0 — Inventory: where each import comes from (one command, headnode)

```bash
venv/bin/python3 - <<'EOF'
import importlib
for m in ["numpy", "pandas", "scipy", "requests", "yaml", "matplotlib", "PIL", "skimage", "torch", "nicegui"]:
    mod = importlib.import_module(m)
    print(f"{m:11} {getattr(mod, '__version__', '?'):14} {mod.__file__}")
EOF
echo "PYTHONPATH=$PYTHONPATH"
module show arrow/16.1.0-gfbf-2023b 2>&1 | grep -i -E "pythonpath|scipy|depends"
```

The versions this prints are the known-good set: the lock in S1 starts from them, not from whatever is newest. Any
path outside `venv/` confirms the mixed environment described in §1.

#### S0 record (2026-10-27, `tmpOut/uv_s0_probe.sh` on clip-login-1 and compute node clip-c2-28)

- Confirmed: the venv (`include-system-site-packages = false`) is bypassed by `PYTHONPATH`. The `arrow` module
  prepends seven module site-packages, so numpy 1.26.2, pandas 2.1.3, scipy 1.11.4 (scipy-bundle/2023.11) and
  requests 2.31.0 (python-bundle-pypi/2023.10) come from `/software`. pyarrow (arrow 16.1.0) loads with pandas.
- The venv holds 79 distributions, 5.6 GB, of which torch 2.6.0+cu124, nvidia-* and triton are 5.0 GB.
  The rest of the server's own packages are in the venv: nicegui 3.0.3, fastapi 0.118.0, uvicorn 0.37.0,
  pydantic 2.12.3, matplotlib 3.10.9, Pillow 12.0.0, scikit-image 0.26.0, mrcfile 1.5.4, biopython 1.86, PyYAML 6.0.3.
- `pip freeze` through the job's environment lists 298 packages (venv plus the module bundles), so the lock in
  S1 starts from the venv's 79 plus the four module packages above, not from that freeze.
- Same on both hosts. Start-up (§6 item 1): `import drivers.driver_base` takes 1.3 s warm, 1.85 s cold on the
  compute node; pandas is 0.58 s of it, pulled in by `services/jobs/_base.py`. Loading and validating a 636 KB
  `project_params.json` (9 jobs) adds 0.31 s. So a task starts working ~1.6–2 s after it starts.
- Not on `sys.path` at all: `tqdm` (filterTilts imports it optionally). pyarrow 16.1.0 is the arrow module's;
  pandas 2.1 does not need it, so the lock leaves it out (it is 0.06 s of the import).

### S1 — Lock the dependencies

- `requirements.in`: the current `requirements.txt` list, same pins and comments. numpy, pandas, scipy, matplotlib,
  pyyaml and requests are pinned to the S0 versions, so the lock reproduces what runs today.
- `requirements.txt` is generated:
  `uv pip compile requirements.in -o requirements.txt --python-version 3.11 --python-platform x86_64-unknown-linux-gnu`.
  The platform flags make a compile on a laptop resolve the cluster's set, torch's `nvidia-*` dependencies
  included. The file's header says it is generated and from what.
- `requirements-dev.in` → `requirements-dev.txt`: `-c requirements.txt` plus `ruff==0.15.12`.
- preflight `check_python` also reports each required module that imports from outside `sys.prefix`, as a warning
  naming the path. That is the mixed environment of §1, made visible on every install.
- README: the quick start installs with `uv pip sync requirements.txt`. One line says how to change a dependency:
  edit `requirements.in`, then recompile.

#### S1 record (2026-10-27, compile owed)

- `requirements.in`: the old `requirements.txt` with numpy 1.26.2, pandas 2.1.3, scipy 1.11.4, requests 2.31.0,
  matplotlib 3.10.9 and PyYAML 6.0.3 pinned to the S0 versions; the compile command in its header.
- `requirements-dev.in`: `-c requirements.txt` plus ruff 0.15.12.
- First compile (uv 0.13.6, 97 packages): 48 packages not pinned in `requirements.in` resolved newer than what
  runs today (python-dateutil 3.0.0 under pandas 2.1.3, pyarrow 26 via starfile, pytest 9 among them). Pinning
  them all to the S0 versions does not resolve: today's set only works because the modules shadow the venv
  (nicegui 3.0.3 needs jinja2 >= 3.1.6 and runs on the module's 3.1.2). The maintainer keeps the newer ones and
  pins back whatever breaks (2026-10-27); python-dateutil is the first suspect.
- `uv.toml` (`[pip]` python-version 3.11, python-platform x86_64-unknown-linux-gnu), so the compile line needs
  no flags.
- preflight `check_python` warns for each required module imported from outside `sys.prefix`, naming the path.
- README: one line on editing `requirements.in` and recompiling; the preflight bullet mentions the warning.
- Compiled by the maintainer 2026-10-27 (uv 0.13.6).

### S2 — The environment's own Python (uv-managed)

- `uv python install 3.11`, then `uv venv -p 3.11 venv`, then `uv pip sync -p venv requirements.txt`. uv's Linux
  Pythons (python-build-standalone) need glibc 2.17, which is CentOS 7's. The venv then starts on any node that sees
  the shared filesystem, with no module loaded.
- Per-user installs keep uv's defaults (`~/.local/share/uv/python`, `~/.cache/uv`); the home directory is shared
  with the compute nodes. The lab install sets `UV_PYTHON_INSTALL_DIR` and `UV_CACHE_DIR` per D4. The README says
  both must be on a filesystem the compute nodes see.
- `config/qsub.template.sh`: the example header drops the Python, gcccore and arrow module lines. What is left is
  what puts `apptainer` on PATH. `preflight` and the cluster self-test (`pytest -m cluster`) already prove that the
  server's interpreter starts on a compute node, so they are the check.
- README: the quick start becomes `uv` from clone to `crboost`. The requirement "Python 3.11+ with dev headers"
  goes, replaced by "uv" (one static binary, `curl -LsSf https://astral.sh/uv/install.sh | sh`). The
  *Python environment* dropdown keeps venv/conda via `pip install -r requirements.txt` for sites that prefer them.
  The *Upgrading an existing install* section gets the three commands that rebuild `venv/` in place.
- Rebuilding an existing venv changes numpy/pandas/scipy from the module's builds to PyPI wheels of the same
  versions. That is the one behavioural risk; the end-of-arc runtime pass (§4) covers it.

#### S2 record (2026-10-27, docs built; the venv rebuild is the maintainer's)

- README: the requirement is uv instead of "Python 3.11+ with dev headers"; the quick start installs uv, its
  Python 3.11, `uv venv -p 3.11 venv` and `uv pip sync -p venv/bin/python3 requirements.txt`. The *Python
  environment* dropdown says where uv keeps its Python and cache, the shared-install variables (D4), and the pip
  route on the same lock. *Upgrading an existing install* gains the in-place rebuild.
- `config/qsub.template.sh`: the header comment says a uv venv needs no module, only `apptainer` on PATH; ours
  is kept as the example for a venv on a module Python.
- Trial in the liveness worktree, whose `venv` was a symlink to the main checkout's (so jobs there keep running
  on the old one): rebuild it there, point `crboost_python` at it and drop the module lines from that tree's
  `config/qsub.sh`, then preflight, `pytest -m cluster` and one real chain. Every tool except cisTEM
  (`exec_mode: binary`) runs in a container, so no driver needs a module library.
- The server must start from a shell without the modules: sbatch exports the submitting environment
  (`--export=ALL`), so a leftover `PYTHONPATH` reaches every job. README says so; preflight's warning shows it.
- Done 2026-10-27: venv rebuilt on uv's CPython 3.11.18 (98 packages); this tree's `crboost_python` points at
  it and its `config/qsub.sh` module lines are commented out (originals in `tmpOut/*.before-uv`).
- Same day, from a shell after `module purge` + `unset PYTHONPATH`: preflight all OK (no import from outside
  the venv), `pytest -m cluster` 12 passed in 10:52. `main.py` then stopped at an ImportError unrelated to uv
  (main has UI code whose definitions were never committed; the three were copied into this branch).
- Then the server started (modules 3.3 s) and a fresh 8-TS project ran its chain on the uv venv: fsMotion, the DL
  tilt filter (torch from the venv on a GPU), alignment, CTF, reconstruction. Seen on the way: the server's and the
  driver's PNG conversions shared one `.png.part` name (one rename failed); the part file now carries the pid.
  This tree's `config/qsub.sh` also unsets `PYTHONPATH` in its header, so a server started from a module shell
  cannot leak the modules into jobs.
- `clean_env.py`, imported first by `crboost` and `preflight.py`: when the interpreter's own site-packages holds
  numpy and `PYTHONPATH` is set, the process re-executes itself without it (one console line says so). Nobody
  needs `module purge` before starting the server; a venv on a module Python (no numpy of its own) is untouched.

#### Rollout (2026-10-27, the maintainer's steps)

1. Commit what is left on `uv_and_UI`; check the committed tree imports cleanly (every `from <repo module>
   import name` resolves), since main has shipped partial commits before.
2. `main` is an ancestor of `uv_and_UI`: `git fetch . uv_and_UI:main`, then `git push origin main`. `roadmaps`
   (another session's work) is not part of it.
3. Lab install (`/groups/klumpe/software/crboost_server`), with no server or job running from it:
   `git pull origin main`; `UV_PYTHON_INSTALL_DIR` / `UV_CACHE_DIR` per D4 and `umask 002`;
   `uv python install 3.11`; `mv venv venv.modules` (the way back); `uv venv -p 3.11 venv`;
   `uv pip sync -p venv/bin/python3 requirements.txt`; `chmod -R a+rX` the uv Python; in `config/qsub.sh`
   comment out the module lines and add `unset PYTHONPATH`; then preflight and `pytest -m cluster` (pytest has no
   `clean_env`, so that one from a shell after `module purge`).
4. Way back: `rm -rf venv`, `mv venv.modules venv`, restore `config/qsub.sh` from its backup.

Done 2026-10-28 on the lab install at `44375a0`: uv's CPython 3.11.18 under
`/groups/klumpe/software/cache_and_artifacts/uv/python`, 97 packages, 10313 files byte-compiled at sync. The
old venv went outside the checkout instead, to `/groups/klumpe/software/crboost_venv.modules` (`.gitignore` covers
`venv/`, not `venv.modules/`); `config/qsub.sh` module lines commented out, `unset PYTHONPATH` added, original in
`config/qsub.sh.bak-modules`. Preflight all OK, `pytest -m cluster` 11 passed + 1 skipped in 16:43, `./crboost`
started (modules 17.1 s on that first start, compiling into the per-user cache).

### S3 — The tilt filter in its own container

**Why a container (maintainer, 2026-11-06).** Of the environment's 7.3 GB, 6.6 GB was the CUDA build of torch
(measured the same day: the nvidia-* libraries 4.2 GB, torch itself 1.7, triton 0.55, sympy, networkx, mpmath and
torchvision 0.14; everything the server needs 0.9), and the DL tilt filter is the only host-side user: a ResNet-18 at
224 px with 45 MB of weights, run with the 8-fold rotate/flip averaging its checkpoint asks for (`tta`). A CPU-only
torch in the environment (`2.8.0+cpu`, under 2 GB) was weighed and not taken. Timed on a login node with 4 threads
(the profile's `cpus_per_task`), the network does 116 tilts/s per forward pass and about 15 with the averaging, so
minutes per project where a GPU takes seconds. It would also have moved the numerics the 0.301 cut was set on, and
`ModelLoader`'s "no CPU mode" is a deliberate rule (a fallback would hide a broken node). The image keeps the GPU
path, the numbers and the code as they were.

**Built 2026-11-06.**

- `container_defs/tilt_filter_torch2.8.0_cuda12.8.def`: `python:3.11-slim-bookworm`, then `pip install` of torch 2.8.0,
  torchvision 0.23.0 and the lock's Pillow and numpy, nothing else. No CUDA base and no apt: the torch wheel carries its
  own CUDA libraries, and apt is what the other CBE.next recipes spend their fakeroot workarounds on. The check that
  `sm_100` is among torch's compiled-in architectures runs at the end of `%post`, since a failing `%test` does not fail
  an Apptainer build. `PYTHONNOUSERSITE=1` in `%environment`, because the home directory is bound in.
- `filterTilts/deepLearning/predict.py`, run as `python -m filterTilts.deepLearning.predict` in the container: a PNG
  list file in, one JSON file out (P(bad) per PNG in that order, and the model facts the driver prints: architecture,
  input size, temperature, TTA, GPU name, torch version), written whole or not at all. GPU only, as `ModelLoader`.
- `filterTilts/gallery.py` holds `GALLERY_PNG_SIZE`, the one fact the host (which makes the thumbnails) and the
  container (which reads them) share, so neither imports the other's packages.
- `drivers/tilt_filter.py`: the PNGs are found or converted on the host as before (`ImageProcessor` needs no torch);
  the inference is one `run_tool(..., tool_name="tilt_filter")` call that binds the checkout's `filterTilts/`, the
  weights' directory and the PNG directories; the registry write and the liveness check read the JSON back. The
  order changed in one place: the registry entry (weights file, `tools.tilt_filter`) is checked before any conversion,
  and a missing GPU shows at the container call instead of before the conversion.
- `resolve_model` (`services/jobs/tilt_filter.py`) also raises when `tools.tilt_filter` is not configured, so the job
  panel's red marker, Run's refusal and the driver all say so before anything is queued or converted.
- `config/conf.template.yaml`: a `tools.tilt_filter` entry. `tests/test_cluster.py`: a `tilt_filter` probe that launches a
  kernel (`torch.ones(1, device="cuda") + 1`) and not only `is_available()`, which a torch without the node's GPU
  architecture passes. Preflight's tool loop covers the new entry; torch and torchvision left its import list.
- `requirements.in`: torch and torchvision out, with the 20 packages only they needed; ruff in (S6, D13). 99 pins
  became 78 and no other pin moved.

**Not in S3.** A CPU mode. Image labels and the weights checksum (S5).

#### S3 record (2026-11-06; the driver has NOT been run on a project)

- Image: built from the `.def` as SLURM job 1432979 on a compute node, 3 min 14 s, 3.8 GB, into
  `container_verify/tilt_filter_torch2.8.0_cuda12.8.sif` in the maintainer's scratch (the lab's `sifs/` untouched).
  The `%post` assertion that `sm_100` is among torch's compiled-in architectures passed.
- On a B200 (job 1433011, a `1g.23gb` slice of `cbe-g5-4`): the kernel launch passes with torch 2.8.0+cu128, and
  `predict.py`, run as the driver runs it (the checkout's `filterTilts/` bound, `PYTHONPATH` set), did 48 synthetic
  384 px PNGs with the real weights in 3.5 s wall, start-up included. The same PNGs through the loader on 4 CPU
  threads (the old dev venv, torch 2.8.0): 2.9 s for the network alone, max |ΔP(bad)| 1.6e-4, mean 1.8e-5. All 48
  values lay between 0.01 and 0.13, far from the 0.301 cut, so this shows the two paths agree numerically, not that
  no tilt flips next to the cut.
- Lock: 99 pins became 78 (22 out: torch, torchvision, triton, 14 nvidia-*, sympy, mpmath, filelock, fsspec,
  setuptools; ruff in), and `./crboost lock` gives the same file back.
- Not tried: the driver on a real project (registry write, liveness, the red marker in the job panel), `./crboost
  test -m cluster` with the new probe, the image in the lab's `sifs/`.

### S4 — CI and a pinned ruff

- Fix the 6 E501 lines first.
- `.github/workflows/lint.yml` (public repo, so GitHub Actions costs nothing): install `requirements.txt` (ruff
  0.15.12 is in it since S6), then run `ruff check .`, `ruff format --check .`, `python check_boundaries.py` and
  `python -m compileall -q .`. No pytest: `tests/` is the install self-test and needs a site's `conf.yaml`.
- `CLAUDE.md` *Commands* (done 2026-11-06): ruff from the environment, `./crboost python -m ruff check .`. The stale
  line about `sys.dont_write_bytecode` in `main.py` went 2026-10-28.

### S5 — Labelled containers and checksummed weights

- Every `.def` gets `%labels`: `crboost.tool <key>` and `crboost.def <file name>`. preflight reads
  `apptainer inspect --labels` for each configured `container_path` and warns when the image's definition differs
  from the one the code expects for that tool, or when it has no labels (an image built before this stage). It
  warns and never blocks: a hand-built image may be right.
- `tilt_filter.models.<name>.sha256` in `conf.yaml` (optional). When it is set, preflight checks the weights file
  against it. `scripts/fetch_weights.py` downloads the published file (D7) into `filterTilts/` and checks it.

### S6 — The environment is a build product, named by its inputs (2026-11-06)

**Why.** Four things went wrong on 2026-11-06, and a check at the right moment would have caught each.

- *The venv's torch was never run on the GPUs it is for.* The lock pinned torch 2.6.0/cu124, which has no sm_100
  kernels. Preflight only imports torch, and every `pytest -m cluster` probe was a container tool, so the DL tilt
  filter, the one GPU workload that ran in the venv, died on the B200 nodes with "no kernel image is available".
- *A pulled lock did not reach the venv.* `d12f371` fixed the pin and `git pull` carried it to the lab install, whose
  venv stayed on 2.6.0 until someone rebuilt it. Nothing said so in between.
- *The rebuild in place failed.* `uv venv --clear` stopped with "Device or resource busy": a server on another login
  node still had the old venv open, and NFS keeps such files as `.nfs…` entries that block removing their directory.
  No `venv/bin` was left. Each member runs a server from the lab install, so whoever installs cannot list every
  process that uses it.
- *Several interpreters.* The dev venv sat on the shared uv Python and was built by the system uv 0.11.19; the lab venv
  on its own `.uv/python`, built by uv 0.13.7 (the installer fetched whatever was newest). Both `conf.yaml` files set
  `crboost_python` to an absolute path, where the template says empty. About 40 GB: two 7.3 GB venvs and three uv caches.

Outside Python, the same day: the lab `conf.yaml` lacked the `qos: "g_medium"` lines of `missAlign` and `denoisetrain`
that `89f3fe4` put in the template. `conf.yaml` is untracked, so `git pull` never carries such a line (not in S6).

**Idea.** Nobody maintains the environment. It is a build product, a directory named by a hash of what goes into it (a
Nix store path, a Docker tag), found or built by `./crboost` and never modified afterwards. A lock change builds a new
one beside the old; a server still running keeps its own; going back to the old lock finds the old one again. There is
no `--clear`, no switch, no "stop every server first", and no stale environment, because a stale one would have a
different name. (This replaces the first S6 of the same day: generations switched by a link, a lock stamp, refusing
checks at start. It was heavier and still rebuilt in place.)

**The name.** `<python>-<12 hex of a sha256>` over the Python version and the lock's pin lines (comments, blank lines and
order ignored, so recompiling to the same pins changes nothing). uv's own version is not in it: it does not change what
is installed. One home per fact, and none of them in a script:

| Fact | Lives in | To try something without editing it |
|---|---|---|
| what is installed | `requirements.txt`, compiled from `requirements.in` by `./crboost lock` | `CRBOOST_LOCK=<file>` |
| which Python | `.python-version`, an exact x.y.z (uv's own convention) | `CRBOOST_PYTHON=x.y.z` |
| which uv, and how it behaves (wheels only, bytecode, platform, own Pythons only) | `uv.toml`: `required-version`, `[pip]` | `CRBOOST_UV=<binary>` |
| where environments live | `.uv/env` in the checkout | `CRBOOST_ENV_ROOT=<dir>`: checkouts with the same key share one |
| which interpreter runs the jobs | the server's own (`sys.executable`) | none |
| how anything is called | `./crboost preflight\|test\|python\|lock\|env\|clean` | none |

```
.uv/bin/uv                       the builder, exactly the version uv.toml requires
.uv/python/                      uv's CPython
.uv/cache/                       wheel cache
.uv/env/3.11.18-a1b2c3d4e5f6/    one environment; its BUILD file records the inputs
venv -> .uv/env/...              a convenience link for editors, refreshed at every start; nothing depends on it
```

**What runs.**

- `./crboost` asks `install.sh` for the environment's path, then execs `<env>/bin/python3 crboost.py` with `PYTHONPATH`
  and the like removed. It execs the resolved path, never the `venv` link, so a lazy import cannot come from a newer
  environment than the server started on.
- `install.sh` prints only the path on stdout (progress goes to stderr). If `<env>/bin/python3` exists it is done.
  Otherwise it fetches the pinned uv, runs `uv python install`, builds with `uv venv --relocatable` and
  `uv pip sync` under `<name>.tmp-<pid>`, checks the Python, writes `BUILD` and renames the directory to `<name>`,
  one `mv -T`. Wheels only, bytecode compiled, `UV_NO_CONFIG=1` plus an explicit `--config-file uv.toml`: a person's
  own `~/.config/uv` is ignored and the checkout's policy is not. `uv python install --no-bin`, because uv would
  otherwise put a `python3.11` shim in `~/.local/bin`, outside the checkout.
- Two starts at once both build; the second rename fails and that one throws its copy away. Wasted work, no lock
  file, no corrupt result. An interrupted build leaves only a `*.tmp-*` directory, which `clean` removes.
- A checkout the user cannot write to, with no environment for its lock yet: `./crboost` says "ask whoever installed
  CryoBoost to run `<checkout>/install.sh`" and stops. Nothing else installs by itself.
- `./crboost clean` lists, and with `--yes` removes, environments that are neither this lock's nor the newest other
  (the way back) and that no process on this node runs. It cannot see servers on other login nodes and says so. It
  refuses under `CRBOOST_ENV_ROOT` (a shared root may hold other checkouts' environments). Removal renames first,
  then deletes, so a name that exists is never half-deleted; an NFS-busy remainder is retried by the next clean.
- A `venv/` that is a real directory (an install from before this) is neither used nor touched. Preflight says it can
  go once no server runs from it.
- Preflight warns when `crboost_python` is set: drivers then run something other than the server's interpreter, and a
  `conf.yaml` copied between installs carries the old path. The key itself goes later.

**Tried 2026-11-06 (uv 0.13.7, scratch).** An ordinary venv built under a temporary name and then renamed breaks: its
console scripts keep the temporary path. `uv venv --relocatable` survives the rename, and a second one.
`UV_NO_CONFIG=1` plus `--config-file` applies the checkout's `uv.toml` (`required-version` is enforced through it) and
ignores a person's own `~/.config/uv`.

**At the first real build.** uv picks the link mode itself (the first `install.sh` forced `UV_LINK_MODE=copy`, and nobody
remembers why). On the XFS scratch it cloned (link count 1, no warning); `--link-mode hardlink` gave 2. What `/groups`
does is not known, and uv copies, with a warning, where it can do neither. A full copy of an environment is 0.8 GB now,
so nothing depends on the answer.

**Decisions.**

| # | Question | Answer |
|---|---|---|
| D8 | Generations switched by a link, or rebuild in place with "stop every server first" | Neither (2026-11-06): named environments, built beside and never changed. A link survives only as the `venv` convenience |
| D9 | `./crboost` refuses, or warns, when the environment is not the lock's | The question goes away: the name is the lock, so start-up finds or builds the right one. It refuses (naming the command) only in a checkout the user cannot write to |
| D10 | `.uv/cache` after a successful install: keep or delete | Keep: it is 0.7 GB, and a lock change then downloads only what changed |
| D11 | Keep one previous generation | Yes: `clean` keeps this lock's environment and the newest other |
| D12 | Where torch lives | In the `tilt_filter` container (S3), not in the environment, and not as CPU torch |
| D13 | A dev lock for ruff, or one lock | One lock: `ruff==0.15.12` is in `requirements.in`, `requirements-dev.*` are gone, so there is no `--dev` and the formatter is pinned for everyone |

**Not in S6.** Hashes in the lock (D2 stands). Finding servers on other login nodes. Conda and pip routes (run
`crboost.py` with your own Python; the jobs follow it through `sys.executable`). Removing the `crboost_python` key
from the config model, the settings dialog and `spec.py`. A check for `conf.yaml` keys the template has and the site's
file lacks (the `qos` incident). The per-task start-up cost and the other items of §6.

**By hand, after it lands.**

1. Delete the `crboost_python:` line from both `conf.yaml` files. Harmless now: empty means the server's own
   interpreter, which is what both point at. Preflight warns until it is gone.
2. Put the tilt-filter image where the others are (S3) and add `tools.tilt_filter` to both `conf.yaml` files.
3. Dev: `./crboost`. The first start builds. Delete the old `venv/` once no server runs from it; the `venv` link then
   appears.
4. Lab, at a quiet time: `git pull`, then `./install.sh` once to watch the build, then each member's `./crboost`.
   The old `venv/` stays until nobody runs from it.
5. Then delete what nothing uses: each member's `~/.cache/uv` and `~/.local/share/uv`; the shared
   `/groups/klumpe/software/cache_and_artifacts/uv` (12 GB), once the old dev `venv/` and the `agent_api` worktree's
   link to it are gone, since both run on its interpreter; the `config/*.bak-*` files, once diffed.

**User check.**

1. A fresh clone, a shell with the EESSI modules loaded and a poisoned `PYTHONPATH`: `./crboost preflight` builds,
   prints the environment's path, and is clean; no import comes from outside it, and there is no torch.
2. With a server up, change one pin in `requirements.txt`. `./crboost` builds a second environment while the first
   server keeps serving; `ls .uv/env` shows both; putting the pin back starts at once; `./crboost clean` lists the
   unused one.
3. Kill `./install.sh` during the sync: no half environment, only a `*.tmp-*` directory; the next run builds.
4. Two starts at once in a fresh clone: both finish, one environment.
5. In a checkout the user cannot write to, with no environment for its lock: the message names `install.sh`.
6. On a B200, `./crboost test -m cluster` passes `tilt_filter` (a kernel launch).

#### S6 record (2026-11-06, a scratch copy of the working tree on local XFS; NOT RUN on dev or the lab)

- First start from nothing (fetch uv 0.13.7, install Python 3.11.18, download and install 78 wheels, run): 6.6 s;
  a second start 22 ms. The environment is 0.8 GB, the wheel cache 0.7, the Python 0.09 and uv 0.04: 1.6 GB per
  checkout, where the lab's was 14.5.
- Found by the first start and fixed: `uv python install` tried to put a `python3.11` shim in `~/.local/bin`
  (`--no-bin` now). A second fresh build then touched nothing under `~/.local`, `~/.config`, `~/.cache/uv` or
  `~/.crboost`.
- The name: unchanged by a comment-only edit and by a reordered lock; changed by a pin and by another Python.
  `./crboost lock` gives `requirements.txt` back unchanged.
- Two starts at once: both exit 0, one environment, one "built" and one "another start finished it first". SIGTERM
  mid-sync: nothing usable and nothing left behind, and the next run builds. A root the user cannot write to: the
  message names `install.sh`, exit 1. `clean` keeps this lock's environment and the newest other, lists the rest, and
  `--yes` removes it.
- `ruff check .` finds only the 6 E501 of §1; `check_boundaries.py` is clean; `./crboost test` collects the 3 headnode
  tests; `./crboost preflight` shows the Python section with no import from outside the environment.
- Not tried: `/groups` (NFS: the link mode, group permissions, a remainder from `rm` on a busy file), the real dev and
  lab trees, two members on two login nodes, the shell of user check 1 (EESSI modules and a poisoned `PYTHONPATH`).

## 4. End-of-arc runtime pass

One pass after S1–S5, on a fresh clone in a fresh directory (not a rebuilt old venv):

1. The quick start runs as written, `./install.sh` from clone to `./crboost`, in a shell with **no** modules loaded.
2. `./crboost preflight` exits 0 and lists no import from outside the environment.
3. `pytest -m cluster` passes, `tilt_filter` included, with the slimmed `qsub.sh` header.
4. One run of the copia protocol through Tilt Filter → Reconstruct. The tilt filter's P(bad) values match an
   earlier run's on the same tilts (same torch, same weights, now in a container), and the motion/CTF and
   alignment outputs look as before, since numpy, pandas and scipy are now PyPI's builds.
5. `du -sh $(./crboost env)` is under 1 GB (0.8 GB measured 2026-11-06 in a scratch clone).

## 5. uv itself: the risk and the way out

OpenAI announced on 2026-03-19 that it would acquire Astral, uv's maker. uv stays open source (MIT/Apache-2.0),
and the commentary sees the risk as drift in its direction, not its disappearance. What this roadmap leaves behind
is a plain `requirements.txt` and an ordinary venv. Leaving uv means `python -m venv` plus
`pip install -r requirements.txt` on a Python 3.11 of the site's choosing; only S2's self-contained interpreter
would be lost.

## 6. Speed and robustness beyond the lock (added 2026-10-26)

**Pending (maintainer, 2026-10-26):** wanted, with item 1 first.

1. **What every array task pays before it does any work.** Each task starts a driver, and
   `drivers/driver_base.py:18` imports `services.project_state`, which pulls `services.job_models` (every job's
   param class), `services/jobs/_base.py:8` (`import pandas`) and the rest of that graph, from a venv on the
   shared filesystem; then it parses and validates the whole `project_params.json` for its one job. A 114-TS job
   pays that 114 times, and a campaign tens of thousands of times.
   - Measure first, on a compute node: `python -X importtime drivers/ts_ctf.py --help 2> imp.txt`, plus the time
     of `get_driver_context` alone. pandas is not free to drop everywhere: starfile I/O needs it, and most drivers
     read a star. The likelier wins are the import graph (lazy imports of what a driver does not touch) and
     validating only the job the task runs.
   - `uv pip sync --compile-bytecode` ships the `.pyc` files with the venv. Only the server compiles into
     `~/.cache/crboost/pycache` (`pycache_setup.py`, imported by `crboost`); a driver uses the plain
     `__pycache__` next to each file, and writes none where it cannot write or `PYTHONDONTWRITEBYTECODE` is set
     (sbatch passes the server's environment on). Built 2026-10-28 as `compile-bytecode = true` in `uv.toml`. The
     uv venv held 1 `.pyc`; `import drivers.driver_base` on clip-login-1 took 2.24–2.39 s without the venv's
     bytecode and 1.43–1.84 s with it (6 interleaved runs each), so ~0.6 s off every task.
   - A smaller venv (S3) means fewer files to stat on NFS at every import.
2. **Queued jobs run the code that is on disk when they start, not when they were submitted.**
   `config/qsub.sh` points the driver interpreter and `PYTHONPATH` at the checkout, so an edit (or a `git pull`)
   mid-campaign changes jobs already in the queue. At least: write `code_version()` (`services/code_version.py`)
   into `run_submit.script` at submit and have the driver log a warning when the checkout moved since. Stronger:
   production jobs from the release install only (roadmap 23 stage 6).
3. **Version skew between installs.** Built 2026-10-26 for projects (roadmap 23 stage log): `saved_by` on every
   save, a warning and a one-time backup when older code saves a newer file; jobs are never blocked (the
   maintainer's rule). Still open: the drivers print `code_version()` in their run.out header, and the registry
   index records it too.
4. **CI** — S4 above; it also runs `python -c "import main"`, which catches the import error at startup that
   the assistant sandbox cannot.
5. **A shorter cluster self-test.** `pytest -m cluster` took 16:43 on the lab install (2026-10-28) for 4–26 s of
   tool calls: every probe asks for `slurm_defaults` (partition `g`, `gpu:1`, 32 G), and they started in batches
   of one to three about two minutes apart. Wanted (maintainer, 2026-10-28): the probes that do not check a GPU
   (relion, isonet, imod, pymol, cistem, chimerax) run on `supervisor_slurm`'s resources (no gres, 4 G), so only
   the GPU probes wait for a GPU and each log stays one tool's. Not built.
