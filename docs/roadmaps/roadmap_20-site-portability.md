# Roadmap 20 — Site portability: the Munich install review, one container wrapper, site values only in config

**Status:** approved in full 2026-09-29 (maintainer: "long overdue cleanup and consolidation"). R6 and
S1–S4 code-complete and committed on `bindmounts_and_auth` 2026-09-29; none of it has run on the cluster.
**Next: the runtime pass** (owed below), then S5 — see the handoff below and the stage log at the end. Sources: the Munich collaborator's install review
(September 2026: everything ran, template generation included, after a handful of local patches) and a
repo-wide audit of how paths and flags flow config → container → driver (2026-09-29).

**Success for the whole roadmap:** Munich runs an **unmodified checkout**. Each local patch the reviewer
had to make becomes unnecessary, and a fresh install with only their own `conf.yaml` values passes the
self-test (stage 4).

## Handoff (2026-09-29) — what is left, in order

Each item is its own commit; hand the maintainer `git add`/`git commit` lines (one per line, no
attribution trailer, no names) and let them commit. Items touching the same file go in separate turns.

1. **S2c — one sbatch renderer (§1.5).** Done; see the stage log.
2. **S2d — names (§1.6).** Done; see the stage log.
3. **S2e — config hygiene (§1.3).** Done; see the stage log.
4. **S3 — ChimeraX worker (§1.3) + container defs (§1.7).** Worker done; see the stage log. §1.7
   (pinned container defs) is image-rebuild work, not code: do it with the next rebuild.
5. **S4 — self-test (§1.8).** Done; see the stage log.
6. **S5** — the Munich re-install; the maintainer's step.

**Owed by the maintainer (runtime):**
1. `venv/bin/pip install 'pytest>=7'`, then `venv/bin/python -m pytest` and `venv/bin/python -m pytest -m cluster`
   at CBE. The cluster tier's probe commands have never run; fix whichever ones the images disagree with.
2. Stage-0 facts 1–2 (commands in the stage log; fact 3 is moot since S2c drops an empty `--constraint`).
3. §3 item 1: the copia chain end to end at CBE.
4. The shared install (`/groups/klumpe/software/crboost_server`): its `conf.yaml` already has
   `container_runtime`, `container_binds` and `supervisor_slurm:` (2026-09-29). When it pulls this branch:
   `tools.cistem.bin_path: /groups/klumpe/software/cisTEM/bin` (a directory now; its current code wants the
   `simulate` binary), delete the `crboost_root:` line, and install pytest into its venv.

**Working notes.** No Python in the assistant sandbox: the ceiling is `venv/bin/ruff check` + reading. Six
ruff errors predate this work (`services/curation/session_service.py:289`, `services/jobs/fs_motion_ctf.py:54-63`),
and several files carry older format drift — format only the lines you touch. Roadmap 06 is being built in
a separate worktree on `dl_filter`; the tilt-filter launch in `backend.py` and the header and exit-marker
lines of `drivers/tilt_filter.py` changed here, so expect a small conflict when the two merge. After this roadmap,
roadmap 23 (per-user servers) is next; its "repo-relative paths from `__file__`" item is already done (S2a).

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

**S2c (2026-09-29), code-complete, not run.** `slurm_service.write_sbatch_script` renders every qsub.sh
submission (tilt-filter DL, pick-list extraction, array tasks, afterok supervisors): logs `run.*` /
`task_%a.*` beside the script; `--array` straight under the shebang (the old insert was anchored on the
`--output` line, so a reworded one silently gave a non-array child that ran as another supervisor); the
`--constraint` line dropped when empty; a qsub.sh without the exit-marker block refused. The block is one
constant, `QSUB_EXIT_MARKERS`, which preflight imports. The rendered trailer is the only writer of
`RELION_JOB_EXIT_*`: class3d, ts_import, reconstruct_particle, tilt_filter, denoise_train, miss_align and
ArrayDriver only exit 0/1. The tilt-filter launch renders before it flips the job to RUNNING; a render
failure in the afterok chain returns an error. Deviations, each deliberate:
- **Constraint quotes are stripped by a `SlurmConfig` validator, not fixed in the conf alone.** Every
  project file stores a quoted constraint (39× `'g2|g3|g4'`, 15× `'g4'`, 1× `'g2|g4'` in the 55 newest; the
  last two typed into overrides), so dropping the renderers' strips would have broken every existing
  project. The local dev conf now says `"g2|g3|g4"`; the shared install's triple quotes are harmless.
- **A killed array supervisor writes no marker.** Its SIGTERM handler still scancels the array, but the
  batch shell that writes the marker dies with the same signal; `reconcile_afterok` concludes FAILED from
  sacct or its absent-grace window, as for any SIGKILL or OOM death.

Render smoke check passed on the headnode 2026-09-29: a quoted, an empty-constraint and an array script
rendered from the live `config/qsub.sh`.

**S2d (2026-09-29), code-complete, not run.** Tool names are the conf keys: the five WarpTools jobs say
`warp_aretomo`, ImportMovies and the schemer say `relion`. `get_tool_config` / `is_tool_configured` look up
`tools:` only; the alias maps, the `containers:` fallback and field, and every trace of
`tsreconstruct_supervisor_slurm` (field, migration, class alias, property) are gone. The local dev conf's
key is renamed. Deviation: **ImportMovies was a second alias caller** (`relion_import`, missed in the
handoff); protocol validation asks `is_tool_configured` for every stage's tool, so without the rename every
protocol with an import stage would have reported relion as unconfigured. Until S2e warns on unknown keys, a
conf still saying `tsreconstruct_supervisor_slurm:` is silently ignored and the supervisor gets the code
defaults (the local dev conf's values equal them; S2e empties them).

**S2e (2026-09-29), code-complete, not run.** No site values in code defaults: `slurm_defaults.partition`,
`supervisor_slurm.partition`/`constraint`, `curation.partition` and `SlurmConfig`'s own
partition/constraint default to empty. Preflight fails on an unset `slurm_defaults.partition` or
`supervisor_slurm.partition` by key name, and warns on an unset `curation.partition` when curation has a
`sif_path`. `SlurmConfig.from_config_defaults` no longer falls back to built-ins; a config that does not
load raises. `ConfigService.load_warnings` lists, per conf file, every key the models do not read (dotted
path) and every `job_resource_profiles` entry named after no job type; the list is logged, printed by
preflight, and shown on the landing status strip as an amber "config" dot with a popover, only when
non-empty. The local dev conf lost `crboost_root:`.

**S3 (2026-09-29), code-complete, not run — ChimeraX worker.** The worker binds `/tmp` and `$HOME` itself
and everything else from `CX_BINDS` (colon-separated; a path the node lacks is skipped); the server
passes `container_binds` + the project. The server-side `CX_SIF` override is gone: the SIF is
`curation.sif_path` only, and `CX_SIF` stays as the transport to the worker. A launch with no
`curation.partition` now says so instead of submitting `#SBATCH -p ` with nothing after it. The manual
launcher (`launch_curation_vnc.sh`) no longer gets `/groups` & co. for free: set `CX_BINDS` by hand.
§1.7 waits for the next image rebuild.

**S4 (2026-09-29), code-complete, not run — self-test.** `pytest.ini` + `tests/`. `pytest` (the default
selection) runs preflight's checks as a test, asserts no conf key goes unread and renders `qsub.sh` as
an array script. `pytest -m cluster` submits one `sbatch --wait` job per configured tool (slurm_defaults'
resources, 15 min) from `~/.crboost/selftest/<name>/`, all before waiting on any. Each job starts
`tests/cluster_probe.py` the way a driver starts (the driver interpreter, the PYTHONPATH export) and
calls `run_tool`, so the container command is built on the compute node as in production; built on the
headnode it would bind the headnode's `$TMPDIR` cache path. Probes: `nvidia-smi` + `WarpTools --help`;
`relion_refine --version`; `pytom_match_template.py --help` + a CuPy reduction; cryoCARE import + TF sees
the GPU; `isonet.py --help`; `miss-alignment --help` + `torch.cuda.is_available()`; IMOD `point2model` on
one point (what crboost runs); `import pymol`; `simulate` on PATH (cisTEM prompts on stdin, so it is not
run); ChimeraX `--version` from `curation.sif_path` via a bare `<runtime> exec`, as the worker starts it.
A configured tool without a probe fails `test_every_tool_has_a_probe`. `pytest>=7` is in
`requirements.txt`. Deviations: no DL tilt-filter checkpoint probe (roadmap 06 is replacing the weights);
the probe commands come from the container defs and the drivers but have never run against the images,
so the first `-m cluster` run at CBE settles them.
