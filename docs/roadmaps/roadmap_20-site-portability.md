# Roadmap 20 — Site portability: the Munich install review, one container wrapper, site values only in config

**Status:** approved in full 2026-09-29 (maintainer: "long overdue cleanup and consolidation"). Stage 0 papercut +
stage 1 code-complete 2026-09-29, not run; stage 0 facts owed (see the stage log at the end). Sources: the Munich collaborator's install review
(September 2026: everything ran, template generation included, after a handful of local patches) and a
repo-wide audit of how paths and flags flow config → container → driver (2026-09-29).

**Success for the whole roadmap:** Munich runs an **unmodified checkout**. Each local patch the reviewer
had to make becomes unnecessary, and a fresh install with only their own `conf.yaml` values passes the
self-test (stage 4).

## The review, item by item (verbatim substance, paraphrased)

| # | Review item | Verdict | Where |
|---|---|---|---|
| R1 | preflight hard-codes `ROOT/venv`; support a conda/mamba env | Right, and worse than preflight: drivers run `<repo>/venv/bin/python3` and **silently fall back to bare `python3` from PATH** when it is missing (`services/jobs/spec.py:257-259`, `backend.py:201-203`) | S2 |
| R2 | turn preflight into pytest functional tests of the containers — saves time on every install and update | Yes — as a separate self-test tier that exercises the production wrapper on a compute node | S4 |
| R3 | is there a config flag to add container binds? | There is none. `additional_binds` exists per job but is always `[]` (326 job entries across 47 projects). The wrapper binds a fixed CBE list (`/groups`, `/programs`, `/software`, `/scratch`) — **the project dir and raw data are never bound explicitly; pipelines work at CBE only because everything lives under `/groups`** | S1 |
| R4 | had to remove the `/usr/bin` bind for relion — what is it for? | Leftover from the `relion_schemer` era (the schemer ran `sbatch` from inside the container, so it needed host SLURM binaries). The afterok orchestrator is forced on for every project (`project_state.py:1335`), so it is dead — and binding host `/usr/bin` over an Ubuntu image hides the image's own binaries (`mpirun` among them). Delete | S1 |
| R5 | pytom needed `CUPY_CACHE_DIR` / `CUDA_CACHE_PATH` (better under `$TMPDIR`) | Right, and general: under `--cleanenv` every tool's JIT/kernel cache lands wherever its default points. One cache block for all tools, not a pytom branch | S1 |
| R6 | Create with no project name should say the name is missing | `_mark_missing` sets Quasar `error-message='required'` (`ui/data_import_panel.py:359-368`), which the 16 px house fields clip to nothing. Say it in words | S0 |
| R7 | anyone on the network can open the GUI — secure it, token like Jupyter | Yes | roadmap 23 |

## Before → After

**Before** (`services/computing/container_service.py:182-268`): every container call is
`unset <27 vars>; apptainer exec --nv --cleanenv --no-home -B $HOME -B /tmp -B /scratch -B <cwd>
-B /usr/lib64/slurm -B /run/munge -B /etc/passwd:ro -B /etc/group:ro -B /groups -B /programs -B /software
[-B /usr/bin for relion] <sif> bash -c '[RELION_QSUB_EXTRA exports;] <cmd>'`, followed by a
pretty-printed command that is not the one that runs. Site values are also baked into code defaults
(partition `g`, constraint `g2|g3|g4`, curation partition `c`: `config_service.py:98,117-118,176`), the
ChimeraX worker (`containers/chimerax_artiax/curation_session.sh:106`: `-B /groups -B /software
-B /scratch`, fatal on any host lacking one), and silent fallbacks (unconfigured tool → guessed binary,
`config_service.py:363`; empty `container_path` → native, `container_service.py:185-187`; bad SLURM
config → CBE defaults, `slurm_service.py:73-80`; misspelled conf keys ignored, `config_service.py:231`).

**After:** one wrapper renders every container and binary call from `conf.yaml`; the only site paths
anywhere are `container_binds` and the tool entries; per-job binds (project root, raw-data dirs, gain
reference) are computed once; tool caches go to node-local scratch; the driver interpreter is whatever
runs the server; a missing or misspelled setting fails loudly; and a self-test proves a site works before
anyone creates a project.

---

## 1. Design

### 1.1 One wrapper — `wrap(tool, cmd, cwd, binds)`
- **Unconfigured tool → raise** with the conf key to add. No guessed binaries, no "empty path → native".
- **Binary tools:** `export PATH=<bin_path>:$PATH; <cmd>` (`bin_path` is read today only for cisTEM).
- **Container tools:** `<runtime> exec --nv --cleanenv` + one `-B` per existing path in
  {`/tmp`, cwd, per-job binds (§1.2), `container_binds` (§1.3)}, deduped, + `<sif> bash -c '<env>; <cmd>'`.
  `runtime` = `apptainer` or `singularity` from conf (preflight already accepts both, `preflight.py:156`).
- **Home:** one mechanism instead of `--no-home` contradicted by `-B $HOME` — keep today's effective
  behaviour (home mounted).
- **Env block, same for every tool:** caches under `${TMPDIR:-/tmp}/crboost-$USER`:
  `XDG_CACHE_HOME`, `CUPY_CACHE_DIR`, `CUDA_CACHE_PATH`, `MPLCONFIGDIR`, `TORCHINDUCTOR_CACHE_DIR`,
  `TRITON_CACHE_DIR`, `NUMBA_CACHE_DIR`; plus the variables drivers set today that `--cleanenv` silently
  drops (`TQDM_DISABLE` at `template_match_pytom.py:436`, cisTEM's `OMP_NUM_THREADS` at
  `pdb_service.py:463`). Replaces the miss-align-only block (`drivers/miss_align.py:310-312`).
- **Deleted:** host `/usr/bin`, `/usr/lib64/slurm`, `/run/munge`, `/etc/passwd`, `/etc/group` binds;
  `RELION_QSUB_EXTRA*` exports; the 27-variable `unset` (redundant under `--cleanenv`, and it discards a
  site's own `APPTAINER_BIND`); the `Colors` print (drivers already echo the exact command,
  `driver_base.py:297`). If the dormant schemer path survives until the orchestrator roadmap deletes it,
  it passes its SLURM binds from its single call site (`pipeline_runner.py:770`).
- The host `/etc/passwd` bind hides the user entry apptainer injects on LDAP clusters — probably the real
  cause of the `getpwuid` crash that `drivers/miss_align.py:267-312` works around (verify in S0; if so,
  delete the `USER`/`LOGNAME` workaround, keep the cache dirs).

### 1.2 Per-job binds, computed once
In `driver_base` (`:131`), replacing the always-empty `additional_binds`: project root, parent dirs of
`movies_glob` / `mdocs_glob`, `import_source_directory`, gain-reference dir. The five per-driver bind hacks
go (`class3d.py:107-108`, `reconstruct_particle.py:103`, `subtomo_extraction.py:471-474`,
`extract_pick_list.py:105-106`, `pdb_service.py:55-56`). The twelve `run_tool` callers do not change.

### 1.3 Site values live in config only
```yaml
container_runtime: apptainer        # or singularity
container_binds: [/groups]          # site data/software roots; Munich lists theirs
tools:
  relion: {exec_mode: container, container_path: /…/relion5.0_tomo.sif}
  cistem: {exec_mode: binary, bin_path: /…/cisTEM/bin}
```
- Code defaults for partition / constraint / curation partition become empty; preflight names the
  missing key. `SlurmConfig.from_config_defaults` raises instead of falling back.
- Unknown / misspelled conf keys → a load warning shown in the UI (the `load_warnings` idiom).
- The ChimeraX worker takes the computed binds from the server (`session_service.py:147-157`); the
  `CX_SIF` env override goes (`~/.crboost/conf.yaml` already does this).

### 1.4 Interpreter and root are derived (R1)
- Repo root from `__file__`, not `Path.cwd()` (`main.py:137,150`, `project_service.py:336`,
  `config_service.py:30`).
- **Driver interpreter = `sys.executable` of the running server** — works unchanged for venv, conda,
  mamba and uv. `crboost_python` in conf remains only as an explicit override; a missing interpreter is
  a loud error, never bare `python3`.
- PYTHONPATH set once (qsub.sh); the `sys.path.insert` in drivers and the `spec.py`/`backend.py`
  exports collapse to it.
- Deleted: `crboost_root` key (display-only), `venv_path`/`venv_python`/`get_tool_path` (no callers),
  `.env` (nothing reads it), the duplicate tilt-filter launch code (`backend.py:201-207`).

### 1.5 One sbatch renderer
Four renderers (`backend.py:213-231`, `backend.py:385-399`, `array_job_base.py:489-535`,
`pipeline_orchestrator_service.py:520-541`) → one function. Fix the `'''g2|g3|g4'''` quoting in conf so
nobody strips quotes. Exit markers written once (qsub.sh and drivers both touch `RELION_JOB_EXIT_*` today).

### 1.6 Names
`warptools` → `warp_aretomo` in the five `get_tool_name` implementations; both alias maps, the
`containers:` fallback and the stale `tsreconstruct_supervisor_slurm` key go (rename it in the deployed
confs first — CBE's live `conf.yaml:28` still uses it).

### 1.7 Container definitions (only when images are next rebuilt)
Pin every "latest"/HEAD/branch install (Miniconda/Miniforge, relion `ver5.0`, IsoNet2, cryoCARE,
miss-alignment, torch-projectors, ArtiaX, pymol) — a release other sites build from must be reproducible.
Drop `%runscript` blocks that repeat `%environment` (crboost only uses `exec`), the RHEL openmpi paths in
the Ubuntu relion def, and `TZ=Europe/Vienna`. Add the two ChimeraX defs the docs reference but the repo
lacks (`chimerax_artiax.def`, `chimerax_artiax_patch.def`).

### 1.8 Self-test (R2)
`tests/` with pytest, two tiers, both through the **production** wrapper so binds and flags are tested too:
- **static** (headnode, seconds): conf valid with no unknown keys; every tool's SIF/bin exists;
  `container_binds` exist; partitions exist (`sinfo`); qsub renders.
- **containers** (`-m cluster`, one short `sbatch --wait` per tool on the right partition, minutes):
  `WarpTools --help` + `nvidia-smi`; `relion_refine --version`; pytom + a one-line CuPy kernel (exercises
  the cache dirs); cryoCARE / TF sees the GPU; IsoNet import; `miss-alignment --help` +
  `torch.cuda.is_available()`; DL tilt-filter checkpoint loads; IMOD `header`; ChimeraX `--version`.
  Every failure prints the exact command and the log path.
`preflight.py` keeps config creation and the static tier; the container tier is new.

## 2. Stages

### Stage 0 — Papercut + facts
- R6: Create with missing fields also says so in words (`ui.notify` listing them), next to the red outline.
- On a GPU node, verify: does `--cleanenv` drop `CUDA_VISIBLE_DEVICES`
  (`apptainer exec --nv --cleanenv <sif> env | grep CUDA`)? Does removing the passwd bind fix
  `getpwuid` (`apptainer exec <sif> getent passwd $(id -u)`)? Does `sbatch --constraint=""` work?
- *Success:* Create without a name says "Missing: Project Name"; three answers recorded here.

### Stage 1 — The wrapper (R3, R4, R5)
- §1.1 + §1.2 + `container_binds`.
- *Success:* at CBE every job type still runs (copia chain to tsReconstruct + one TM + one extraction);
  at Munich the `/usr/bin` and CuPy patches are unnecessary and their binds come from `container_binds`.

### Stage 2 — Interpreter, root, config hygiene (R1)
- §1.3 minus ChimeraX, §1.4, §1.5, §1.6.
- *Success:* a conda-env install runs drivers with the env's interpreter with no preflight edits; a
  misspelled conf key shows a warning; an absent partition default stops preflight with the key name.

### Stage 3 — ChimeraX worker + container defs
- §1.3 ChimeraX part; §1.7 with the next image rebuild.
- *Success:* curation launches on a host without `/software`; defs build reproducibly from pinned versions.

### Stage 4 — Self-test (R2)
- §1.8.
- *Success:* at both sites `pytest tests -m "not cluster"` passes in seconds and `pytest tests -m cluster`
  submits one job per tool and passes; a deliberately wrong SIF path fails with the path in the message.

### Stage 5 — Munich re-install from the README only
- *Success:* unmodified checkout + their `conf.yaml` → self-test green → the copia protocol runs.

## 3. Runtime checklist
1. CBE: copia chain end to end after stage 1; one job's `run.out` shows the new bind list with the project
   root and raw-data dir and none of the SLURM/passwd binds.
2. Munich: stages 1–2 remove all three local patches.
3. Both: self-test tiers green.

**Modern-Python weave-in:** `ToolConfig` as a discriminated union on `exec_mode` (container vs binary)
so a container entry without a path cannot validate; `container_runtime` as a `StrEnum`.

## 4. Stage log

**S0 (2026-09-29).** R6 built: a Create click with gaps also toasts "Missing: Project Name, …".
The three runtime facts are still owed — run on a GPU node inside an allocation:
1. `apptainer exec --nv --cleanenv <any sif> env | grep CUDA` — does `CUDA_VISIBLE_DEVICES` survive?
2. `apptainer exec <any sif> getent passwd $(id -u)` (no passwd bind) — does it print your entry?
3. `sbatch --constraint="" --wrap hostname` — accepted?

**S1 (2026-09-29), code-complete, not run.** `wrap_command_for_tool` renders from conf only:
`container_runtime` + `container_binds` keys; `--nv --cleanenv`, apptainer's default home mount (no
`--no-home` / `-B $HOME` pair); binds = `/tmp`, the cache root, cwd, per-job binds, `container_binds`,
existing paths only, a path inside another bound path dropped; cache env block + `TQDM_DISABLE=1`;
unconfigured tool / empty path → raise; binary tools get `bin_path` (now a directory) first on PATH.
Per-job binds (`driver_base.project_binds`): project root, the wildcard-free root of both globs,
`import_source_directory`, the gain reference's dir; the always-empty `additional_binds` job field is
gone. cisTEM's `OMP_NUM_THREADS` rides on the command. The schemer passes its own SLURM binds and
`RELION_QSUB_EXTRA*` exports. Deviations, each deliberate:
- **`CUDA_VISIBLE_DEVICES` is not forwarded.** The pytom driver passes the host's ids as `-g`, which
  is right only while the container sees every GPU; forwarding would renumber them. Decide after fact 1.
- **Input-parent binds kept** in class3d / reconstruct_particle / subtomo `task_binds`: a reference,
  mask or manual optimisation set can live outside the project and outside `container_binds`. The
  wrapper drops them when an ancestor is bound, so at CBE they cost nothing. extract_pick_list's
  project/job-dir binds went (the per-job set has them); pdb_service's hand-rolled resolve/dedup went.
- **miss_align keeps `USER`/`LOGNAME`** until fact 2 confirms the passwd bind was the cause; its
  `HOME`/`MPLCONFIGDIR`/`TORCHINDUCTOR_CACHE_DIR` went (home is mounted, caches come from the wrapper).

Deployed confs need, before running this code: `container_binds: [/groups, /scratch, /software,
/programs]` (today's bind set at CBE) and `tools.cistem.bin_path: /groups/klumpe/software/cisTEM/bin`.

**S2a (2026-09-29), code-complete, not run — interpreter and root (R1).** Drivers run `crboost_python`
when set, else `sys.executable` of the running server; a non-executable interpreter raises, and both
backend launchers (tilt-filter DL, pick-list extraction) build the command before touching job state or
old output. `config_service.REPO_ROOT` from `__file__` replaces the cwd walk; `main.py` (static mount,
CSS, backend root), the project qsub copy and `ui/components/svg_icon.py` (a fifth cwd-relative path —
icons silently blank when started elsewhere) use it. `crboost_root`, `venv_path`/`venv_python` and the
duplicate tilt-filter launch code are gone; preflight checks the driver interpreter, not `<repo>/venv`.
Deviation: **PYTHONPATH lives in the driver command** (`spec.driver_launch_prefix`, built by the running
server from its own root), not in qsub.sh — qsub.sh gets copied between checkouts and worktrees, so a
path baked into it would run a worktree's jobs against the main checkout. qsub.template.sh's ENV PATHS
block (`CRBOOST_SERVER_DIR`, the unused `CRBOOST_PYTHON`) is gone. `.env` is untracked and unread;
delete it locally.

**S2b (2026-09-29), code-complete, not run.** The seventeen per-driver `sys.path` inserts are gone;
every launch goes through `driver_invocation`, whose command carries the one PYTHONPATH.
