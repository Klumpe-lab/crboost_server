# Roadmap 39 — Python environment and setup: a locked, self-contained environment built with uv

**Pending (maintainer, 2026-10-26):** wanted; move to uv, plus §6 (start-up cost of every array task first).
**Status:** scoped 2026-10-22; nothing built. S0 inventory run and §2 answered 2026-10-27 (records in §2, §3).

**In one line.** Today the server's environment is a venv built on the cluster's Python module. Some of its packages
are probably not in the venv at all, and most of its versions are not pinned. This roadmap makes that environment
one that installs the same everywhere and runs on a compute node without module loads. It also takes the 5 GB of
PyTorch out of it, and adds the checks that keep it that way: CI, a pinned ruff, labelled containers and
checksummed weights.

## 1. What is there now

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
  compute node; pandas is 0.58 s of it, pulled in by `services/jobs/_base.py`.

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

### S3 — The tilt filter in its own container

- `container_defs/tilt_filter_torch2.6.0_cu124.def`: torch 2.6.0 + torchvision 0.21.0 + Pillow + numpy, nothing
  else (one image per tool envelope, as for the other tools). New `tools:` key `tilt_filter`, in
  `config/conf.template.yaml`, the README's tool table, the preflight tool checks, and a `PROBES` entry in
  `tests/test_cluster.py` that imports torch and sees the GPU.
- `filterTilts/deepLearning/predict.py`: a command-line entry run inside the container. Its inputs are the PNG list,
  the weights path, arch and normalisation. It writes one JSON file holding P(bad) per tilt plus the facts the
  driver prints today: input size, temperature, TTA, device.
- `drivers/tilt_filter.py`: the PNG conversion stays on the host (`filterTilts/image_processor.py` needs no torch).
  Inference becomes one `run_tool(..., tool_name="tilt_filter")` call, and the driver reads the JSON back into the
  registry as now. `GALLERY_PNG_SIZE` moves out of `model_loader.py` into a torch-free module, so the driver no
  longer imports torch. The weights file's directory is bound into the call, since the checkout is not among the
  default binds.
- `requirements.in` drops torch and torchvision; the venv goes from 5.6 GB to well under 1 GB.
  `filterTilts/deepLearning/convert_checkpoint.py` and the scripts under `docs/reports/dl-tilt-filter/scripts/` are
  run in the container from then on; their docstrings say how.

### S4 — CI and a pinned ruff

- Fix the 6 E501 lines first.
- `.github/workflows/lint.yml` (public repo, so GitHub Actions costs nothing): install `requirements-dev.txt`'s
  ruff, then run `ruff check .`, `ruff format --check .`, `python check_boundaries.py` and
  `python -m compileall -q .`. No pytest: `tests/` is the install self-test and needs a site's `conf.yaml`.
- `CLAUDE.md` *Commands*: ruff from `requirements-dev.txt`. Also fix the stale line that says
  `sys.dont_write_bytecode` is set in `main.py`; `pycache_setup.py` handles bytecode now.

### S5 — Labelled containers and checksummed weights

- Every `.def` gets `%labels`: `crboost.tool <key>` and `crboost.def <file name>`. preflight reads
  `apptainer inspect --labels` for each configured `container_path` and warns when the image's definition differs
  from the one the code expects for that tool, or when it has no labels (an image built before this stage). It
  warns and never blocks: a hand-built image may be right.
- `tilt_filter.models.<name>.sha256` in `conf.yaml` (optional). When it is set, preflight checks the weights file
  against it. `scripts/fetch_weights.py` downloads the published file (D7) into `filterTilts/` and checks it.

## 4. End-of-arc runtime pass

One pass after S1–S5, on a fresh clone in a fresh directory (not a rebuilt old venv):

1. The quick start runs as written, `uv` from clone to `./crboost`, in a shell with **no** modules loaded.
2. `preflight.py` exits 0 and lists no import from outside the venv.
3. `pytest -m cluster` passes, `tilt_filter` included, with the slimmed `qsub.sh` header.
4. One run of the copia protocol through Tilt Filter → Reconstruct. The tilt filter's P(bad) values match an
   earlier run's on the same tilts (same torch, same weights, now in a container), and the motion/CTF and
   alignment outputs look as before, since numpy, pandas and scipy are now PyPI's builds.
5. `du -sh venv` is under 1 GB. A second checkout's venv built from the same cache adds little (hardlinks).

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
   - `uv pip sync --compile-bytecode` ships the `.pyc` files with the venv. Today each user compiles every module
     on first import into their own `~/.cache/crboost/pycache` (`pycache_setup.py`), so the first job of each
     user pays the compile on a compute node.
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
