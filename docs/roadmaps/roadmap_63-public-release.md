# Roadmap 63 — Public release: CI, releases, containers served, a second site installs from the README

Scoped 2026-11-19 from an eight-lens read-only recon, checked once adversarially the same day; nothing built.
Target: `v2026.12.0` in the week of 14 December 2026, the Munich site installing it and updating to the next one
without help. Raw recon, every claim anchored:
[docs/reports/public-release/recon-2026-11-19.md](../reports/public-release/recon-2026-11-19.md). Its finding ids
(`CI-3`, `CIR-1`, `ONB-2`, `HY4`, `C02`, `CR1`, `OS5`, `PP16`, …) are cited below; `HOTFIXES Hn` means an entry of
[HOTFIXES.md](HOTFIXES.md).

**The ask (maintainer, 2026-11-19).** Of everything a real deployed software has, what is missing: free CI/CD on
GitHub, a release and tag schedule, what leaves the repo, quality-of-life, and above all how the tool containers are
built and served, and where uv stands. Deployment, not scientific correctness.

**Folds in** roadmap 39 S4 (CI) and S5 (image identity, weights checksums), roadmap 20 §1.7 (pin every latest/HEAD in
the defs) and S5 (Munich re-installs from the README), and the local half of roadmap 23 stage 6 (the update banner).
Those roadmaps point here for them.

**Pickup.** The decisions are taken (§3, 2026-11-19) but one: whether D3's notes are also erased from history.
Send Munich §5's questions in the week of 23 Nov, then build from S1 in order. §2 is not a stage: it is wrong today.

## 1. What there is (read 2026-11-19, at `80a874b`)

**The repo.** Public, Apache-2.0, 913 commits, ~36 MiB clone; 0 tags, releases, workflows or packages. 816 tracked
files, of which 331 (41 %) are notes: `memory/` (174 assistant notes, a personal profile among them), `docs/roadmaps/`
(93) and `docs/reports/` (64), carrying the maintainer's home path ~200 times, internal hostnames, a private IP and
unpublished AGG412 results (HY1, HY2, OS2). The reference docs that stay public carry some too: AGG412 sample names
and group paths in `docs/known_bugs.md`, `docs/miss-alignment.md` and `CLAUDE.md`. The front page is the lab's user
guide with the lab's install path (`README.md:12-16`); community health 50 %; no CHANGELOG, CITATION, CONTRIBUTING,
SECURITY or NOTICE, and the `.gitignore` line `/*.md` would silently ignore them at the root (HY4).
`static/molstar/embed.js` (2.6 MB, Mol*, MIT) ships with no licence notice and its source is gone (HY5). No live secret
anywhere in history (HY14, OS7: 924 commits scanned, the real token absent); history keeps pre-scrub names, old
profanity and a screenshot, and a rewrite would not purge them (24 PR refs pin the old commits).

**Python and uv (roadmap 39).** Done and sound. A fresh clone's `./install.sh` takes 6–7 s on local disk (uv 0.13.7,
CPython 3.11.18, 78 wheels), asks nothing, writes nothing to `HOME`; `.uv/` is 1.5 GB, half of it uv's cache
(CI-6, ONB-15). The lock recompiles byte for byte in under a second (CI-7). Loose ends: uv arrives by `curl … | sh`
with only a version check (UV-1, OS9); uv's cache and CPython are fixed inside the checkout (`install.sh:44`), only the
environment root can move (`CRBOOST_ENV_ROOT`); `install.sh` and `crboost` have never run under dash (Ubuntu's
`/bin/sh`).

**Checks.** Ready today: `ruff check` (clean once §2's two errors are fixed), `check_boundaries.py`, compileall, an
import of 392 of 396 modules (the 4 are the torch modules that run in the `tilt_filter` image), the conf template
parsing into `Config`, the qsub exit markers, the copia protocol validating, the lock in sync, `setup_app` building
75 routes on the template conf with no SLURM (CI-0). Failing on committed code: `ruff format --check` (48 files, 22 of
them the hand-aligned `services/jobs/*` Field blocks, CI-3) and `check_look_literals.py` (11 ui files, 76 literals over
its 6-day-old allow-list, CI-2). Nothing runs any of it: no workflow, hook or single entry point (CI-1). The static
tier passes 3 of 5 on a template conf; the other two need a real site (CI-4).

**Security.**
- *The lab install is world-writable*, files and directories (`drivers/`, `crboost`, `config/conf.yaml`,
  `config/qsub.sh`, `.uv/env`, `.git`), under group directories that are themselves `drwxrwxrwx` with no sticky bit
  (OS1, checked again 2026-11-19). Every member's server and every job runs that code, and the conf names the binaries
  jobs execute, so write access there is code execution as every lab member.
- The server binds every interface over plain HTTP when `--host` is not given (OS3). The token gate itself is sound:
  one ASGI gate before every route, raw-file routes limited to loaded projects, agent writes need the Bearer header.
- The pinned web stack carries ~85 known advisories (pillow 12.0.0, nicegui 3.0.3, starlette 0.48.0 through fastapi
  0.118.0, requests 2.31.0, biopython 1.86), nearly all behind the gate (SEC-1, OS4). GitHub's dependency graph sees
  no manifest, so Dependabot shows 0; secret scanning, push protection, private vulnerability reporting and branch
  protection are off (CI-10, OS8).
- On a compute node, other users can reach a curation session: ChimeraX's REST port has no auth, and the VNC password
  sits in a group-readable `session.json` (OS6). Every page fetches IBM Plex from Google (OS11). The token cookie is
  the token itself and never rotates (OS12).

**Versions and updates.** `code_version()` is `<branch>@<sha8>` read from `.git`, `None` outside a checkout
(`services/code_version.py:46-61`), stamped into project saves, driver headers, the log and the API (REL-1, CI-13).
Sites clone `main` and `git pull`; the lab install pulled 17 times this month. A pull changes code under running
servers (475 function-local imports, re-importing workers) and under queued jobs (OS5, roadmap 39 §6.2). Side-by-side
release folders nearly work already: both launchers resolve `pwd -P`, and each job carries the resolved interpreter
and repo root of the server that submitted it (OS5).

**Site config.** `conf.yaml` is untracked by design and the template changed 39 times since 15 September. A conf
filled at the Munich handoff (`0e1d708`) loads under HEAD with 0 warnings while missing 61 of today's keys, whole job
profiles included, and keeps old memory values (tsCtf 8G against today's 32G) (ONB-2). The template ships CBE.next
values as live settings, `gpu:nvidia_b200:1` and `qos: g_medium` for missAlign, `g_medium` for denoisetrain,
partition `c` for curation (`config/conf.template.yaml:92,97,139,265`), and preflight checks partitions only
(`preflight.py:207-223`), so Munich would pass preflight and fail at sbatch (ONB-3, HY3, PP21). Requirements nobody
documents or probes: MPS on GPU nodes, a driver new enough for CUDA 12.9, ssh from headnode to nodes, SLURM
accounting, the hosts builds reach, fakeroot (ONB-7).

**Containers.** 15 defs for 11 tools, all built by hand.
- *Which recipe.* `docs/installation.md:57-78` points a new site at four legacy defs (warp + AreTomo1,
  `relion5.0_tomo`, pytom 0.10, `isonet2.def`) whose images the lab deleted on 17 November; the lab runs the
  `*_sm100` ones (C01, PP19). easymode's def exists only on the lab share; the repo's `napari_GL.def` is the lab's
  `napari_GL_v2.def` under the old name (C03). The `miss_alignment` image was not built from the repo's def
  (`cuda-toolkit`, nvcc included, in the image; `cuda-runtime` in the def), and since HOTFIXES H26 it is also the
  runtime of the Warp-geometry bridge, whose `warpylib` and `starfile` arrive only as unpinned dependencies (C02).
- *Reproducible?* No image is: Miniforge/Miniconda "latest", base images by moving tag, unpinned pip and conda
  (C05). But every SIF embeds the def that built it (`apptainer inspect --deffile`, ~20 ms), and full conda/pip locks
  export read-only from every running image; 8 were exported during the recon (C04, C05). Of the 12 images in use,
  7 embed exactly the repo's def; chimerax and imod differ in comments, cryocare in a label, miss_alignment for real,
  easymode has no repo def.
- *Identity.* Preflight checks only that each image path is a file: a 4-byte fake `.sif` passed (ONB-4).
- *Size.* The 12 images in use total 82.4 GB; the two every site needs (warp_aretomo 12.7, relion 13.4) 26.1 GB.
  Mostly duplicated CUDA: a 7.3 GB devel toolkit plus 1.2 GB nsight in each of warp, relion, pytom and isonet next to
  a conda or pip CUDA; pymol is 8.9 GB for an unused cryodrgn/torch (C08, ONB-13).
- *Licences.* ChimeraX may not be redistributed (C06, CR9). The `nvidia/cuda` images fall under NVIDIA's Deep
  Learning Container Licence, which allows distributing a derived container that adds real functionality; the CUDA
  toolkit EULA keeps nvcc and the other developer tools for internal use. The conservative published image therefore
  carries no nvcc: it ends on a runtime or base stage, and its conda CUDA is runtime packages only (CR10, C07). Four of
  the six images on devel bases compile nothing. The GPL tools (RELION, Warp, IsoNet2, pytom, easymode, IMOD) need
  their source offered from the same place as the image (CR11). cryocare and imod still pull Anaconda's defaults
  channel and accept its terms in the build; imod fetches with `--no-check-certificate` (C10, CR12).
- *Building.* No def needs a GPU to build; builds take 2–16 min on a 16-core CBE.next CPU node. The working build and
  verify scripts are in the maintainer's scratch, not the repo, and the six older defs lack the block CBE.next builds
  need (C09).
- *By hand.* The DL tilt-filter weights (45 MB, roadmap 39 D7 open), the easymode weights (4.0 GB), cisTEM (4.3 GB of
  which only `simulate` is used) (ONB-6, C11, C13).

**Serving, the facts that decide it (CR1–CR18, CT-1).**
- GHCR is free for public packages, storage and bandwidth, with a month's notice before that changes. It caps a layer
  at 10 GB and an upload at 10 min, and `apptainer push oras://` stores a SIF as one layer: relion (13.4 GB),
  warp_aretomo (12.7) and isonet2 (12.4) cannot be pushed as they are.
- Free public-repo runners: 4 CPU, 16 GB RAM, 6 h a job, 14 GB SSD documented (third parties observe 25–31 GB free and
  ~60 GB after removing preinstalled software; not guaranteed), no apptainer (`eWaterCycle/setup-apptainer` installs
  it), no GPU.
- A comparable project serves RELION 5.1 this way: a Dockerfile, devel to runtime, built with docker buildx and a
  layer cache on GHCR, 15–34 min warm and under 2 h cold, converted to a 6.9 GB SIF pushed over ORAS. That is our
  13.4 GB RELION's content without the toolchain. An `apptainer build` from a `.def` has no layer cache on a runner:
  every build is the cold one.
- Release assets are capped at 2 GiB per file (only napari, chimerax and imod fit). Docker Hub limits anonymous pulls
  to 100 per 6 h per IP. Sylabs' cloud library is gone (its hosts no longer resolve, 2026-11-19). Zenodo takes 50 GB a
  record by default, with a DOI per version.
- A self-hosted runner on a public repo is what GitHub says "should almost never" be done (CR13); GPU checks stay on
  the cluster with `./crboost test -m cluster`.

**Promised and owed.** Munich was promised a re-install from an unmodified checkout with only their own conf
(roadmap 20 S5); nothing records it happened, and their conda request (R1) was later answered "not supported" (PP16,
PP17). Nobody has walked a fresh clone through install, preflight, the cluster tier and the copia protocol (roadmap
39 §4, PP25). The DL tilt filter has never run on a project from its image (PP26). HOTFIXES H3 (Cs 2.7 mm and
amplitude contrast 0.10 fixed in code) is wrong for any other microscope (PP30). Many NOT RUN labels sit on paths
production has used since, others are true (PP24). Legacy CBE shuts down on 18 December.

**Settled, not reopened here:** one container per tool when pinnings differ; a newer version upgrades its container
in place; `requirements.txt` lock, not pyproject, and no hashes in it (roadmap 39 D1, D2); per-user servers, no
central VM; no automation around cluster-infrastructure failures. History holds nothing secret (HY14, OS7); whether
it is rewritten anyway is D3's open question.

## 2. Now, outside the stages

- **The lab install's permissions** (OS1). It should not be writable by anyone but its owner, and the group
  directories above it should not be world-writable (they belong to the group's owner, so that is the PI's or IT's
  to change). Inside it, ~150 files belong to other members: their drivers' `__pycache__`, which regenerates, and a
  `.nicegui/` of 40 browser-storage files from before the token gate, which servers no longer write there (storage is
  `~/.crboost/nicegui` now). Those cannot be chowned without root; they are deleted. The copy's `.claude/` folder goes
  too. The commands are in this session's hand-off, not here.
- **`ruff check` fails on HEAD** in the recon dump script (E401, E741); fixed in the working tree with this
  roadmap's commit.

## 3. Decisions (taken by the maintainer 2026-11-19)

| D | Decided | Gates |
|---|---|---|
| D1 | CalVer `vYYYY.M.PATCH` (`v2026.12.0`), candidates `v2026.12.0rc1`, a one-line `VERSION` changed only in release commits. Git's `--sort=v:refname` puts `rc1` after the final release, so the update command picks the newest with `packaging.version` | S8 |
| D2 | Release folders behind a `current` link. `<root>/repo.git`, a bare mirror the update command fetches into; `<root>/releases/<tag>/`, a local clone of it at the tag, owned by the site maintainer, read-only to users, bytecode compiled at install; `<root>/current`; `<root>/site/` (conf, qsub, weights); one uv home the releases share. A running server and its queued jobs keep their release's code; a code rollback is one link; `site/` and the images are per site, so a release that changed conf keys or images is rolled back with them. The lab install moves to it at `rc1`; `main` stays the dev line. **Plus** `./crboost updates`, one command to see and manage it all (S8) | S5, S8 |
| D3 | The notes leave the repo: `memory/`, `docs/roadmaps/`, `docs/reports/`, `scripts/file_roadmap_issues.py`. They stay as local, untracked files on the maintainer's machine (the maintainer's call, not the PI's). Open: whether they are also erased from history, below | S9 |
| D4 | One `ruff format .` commit, its sha in `.git-blame-ignore-revs`, then the format gate | S2 |
| D5 | The 11 offending ui files converted to `--cb-*` variables and voices, then `check_look_literals.py` gates | S2 |
| D6 | The `.def` stays the recipe; every published image two-stage (compile on CUDA devel, end on runtime, base or plain Ubuntu with runtime-only conda CUDA), so no nvcc in it and every SIF under GHCR's 10 GB layer cap. CI on the free runner builds **every** def (cold, up to the 6 h job limit) on a change to it, on a tag or by hand, a few times a year: a recipe that no longer builds shows up there. Images under 9.5 GB are pushed to `oras://ghcr.io/<org>/crboost-<tool>`, pinned per release by digest. Best effort for all images in December, warp and relion included. Munich gets the lab's images by Dropbox for `rc1`, so the release does not wait on this | S7 |
| D7 | Everything but ChimeraX is published under the org, by the rule above (no nvcc, Miniforge, licences and GPL source commits in the image). PyMOL goes altogether: the three things `services/templating/pdb_service.py` asks of it (a structure's extent and residue count; rotating its coordinates onto an alignment; centring it and saving CIF for cisTEM) run in-process with biopython, which the lock already has | S7 |
| D8 | The hash of each image's embedded def, normalised (comments, blank lines, `%labels` stripped), against the hash `manifest.yaml` pins per tool; works on every existing image without a rebuild, and editing a def file warns nothing until its image is rebuilt. `%labels crboost.tool/crboost.def` on the next rebuild. Warns, never blocks | S6 |
| D9 | Split the resource profiles: memory, CPUs, time and GPU count ship as tracked defaults beside the job types; partition, GRES type, QOS, constraint, prefer/avoid stay in the site conf and overlay them. An old conf that still carries the code-owned keys keeps overriding, so the update command lists them for removal. In December | S5 |
| D10 | NiceGUI 3.19.x (the last 3.x) with FastAPI on `starlette>=1.3.1`, on a branch from the week of 23 Nov, with one browser pass over every view. Fallback: 3.0.3 for December, said in the release notes (five starlette advisories stay open) | S3 |
| D11 | Local only: the server reads the releases `./crboost updates fetch` brought into `<root>/repo.git` and shows "v2027.1.0 available". No outbound check of any kind | S8 |
| D12 | Copyright "CryoBoost contributors" (NOTICE); `CITATION.cff` drafted, the author list filled in by the maintainer; a Zenodo DOI per release, tried once on sandbox.zenodo.org first (a record can be deleted by its owner for 30 days, after that only on justified request) | S9, S10 |
| D13 | No triage of NOT RUN labels: they leave with the notes (D3), and the release ships what is built | S10 |
| D14 | `127.0.0.1` unless `local.SshLoginHost` is set, then the headnode's cluster interface, not `::` | S4 |
| D15 | Curation runs the GL image the lab uses; the CPU variant goes, and the viewer chips hide when no image is set. ChimeraX itself may go later | S6 |

**Open: D3's history.** Untracking takes the notes out of the tree GitHub shows and out of every release from now
on; the commits since September still hold them, as they hold the pre-scrub names, old profanity, a screenshot and
a 19 MB EMDB map (HY14, OS7). Erasing them means a history rewrite (`git filter-repo` of those paths, a force-push).
It costs less now than the recon assumed, because Munich re-clones for `rc1` and the lab install moves to release
folders anyway. It still breaks: local branches and worktrees (rewritten in place, but every other clone must
re-clone), commit SHAs quoted in issues and docs, and GitHub keeps the 24 PRs' refs and cached views until GitHub
Support purges them (the same request the contributor cache already needs). If chosen: once, just before `rc1`.

The Claude Code plugin (roadmap 42 D6) waits for `v2027.1.0`; the forwarder's dependency lock is in S2.

## 4. Stages

Each stage is one or a few commits, in this order. Items marked **(January)** are past December's cut line (below)
and go into `v2027.1.0`.

### S1 — Tidy, no decisions needed

- `.gitignore` rewritten (HY4's list: `/*.md`, `**env`, `--git-common-dir`, `!munich_handoff_note.md` and the tool
  entries out); `.gitattributes` with `*.mrc binary` and `static/molstar/embed.js linguist-vendored -diff`.
- `static/molstar/LICENSE` with Mol*'s MIT text (HY5, first half). `static/icons/gear.svg` (0 bytes, unused) deleted.
- Broken links in user docs: `docs/README.md`, `docs/miss-alignment.md`, `CLAUDE.md:156`; `docs/known_bugs.md`
  names a protocol folder that does not exist (HY12, CI-8, PP23).
- HOTFIXES H12, H14, H15 move to Fixed with their commits (`3585c73`, `e95c07f`) (PP22).
- Strings that name the old way or one cluster: the no-conf error says `python preflight.py`
  (`services/configs/config_service.py:383`), the upgrade lines in `docs/installation.md`, "1–12 GB"; job help that
  advises "on CBE: g3 = RTX 6000…" and QOS names (`services/jobs/segment.py:65`, `services/jobs/miss_align.py:455,
  499, 816`), a tooltip naming the lab (`ui/tomo_dashboard_dialog.py:951`) (ONB-10, HY9, HY10). AGG412 in code
  comments becomes a neutral example (8 files, `scripts/dev_map_probe.py` included).
- `config/qsub.template.sh:42`: `cd "${JOB_DIR}" || exit 1`; the driver launch shell-quotes the interpreter,
  `PYTHONPATH`, the driver script and the project path (`services/jobs/spec.py:573, 589-593`), so an install path
  with a space works (OS14).
- Install output: `uv venv -q`; a failed `uv python install` names `UV_PYTHON_INSTALL_MIRROR` (ONB-10).
- The napari companion writes comments, picks and PNGs into its working directory when a session has no project
  outputs (`containers/napari/open_tomogram.py:296-299`; today's untracked `tomogram_comments.csv` in the checkout
  came from it). It writes into the session's own folder under `~/.crboost/napari/<id>/` instead: HOTFIXES H32 (HY16).

### S2 — CI on

Needs D4, D5.

- The format commit and `.git-blame-ignore-revs`.
- The 11 ui files over the look-literals allow-list converted to `--cb-*` variables and `voice()` (`ui/agent_plan_page.py`,
  `ui/analytics/readout_panel.py`, `ui/components/check.py`, `ui/components/path_text.py`,
  `ui/job_plugins/params_renderer.py`, `ui/pipeline_builder/io_config_component.py`,
  `ui/pipeline_builder/job_tab_component.py`, `ui/species/overview_tab.py`, `ui/species/rail.py`,
  `ui/species/tomograms_tab.py`, `ui/tomo_gallery.py`; 76 literals, CI-2), the allow-list lowered to the new counts.
- `scripts/ci.py` behind `./crboost ci [--fast]`: every step a subprocess with its time, all run even after one fails,
  a table at the end, exit 1 on any failure. Steps: ruff check, ruff format --check, `check_boundaries.py`,
  `check_look_literals.py`, compile of the tracked `.py` files, the import-every-module smoke (torch modules
  excluded), `pytest tests/test_repo.py`, the lock in sync (`--fast` skips it), `bash -n`, and shellcheck when present
  (CI-1, CI-7).
- `tests/test_repo.py`, needing no site: `ConfigService(config/conf.template.yaml)` with no load warnings, the qsub
  template's markers and placeholders, every protocol bundle validates, every job's tool is a template tool, the agent
  verb table, `setup_app` builds. `./crboost test` stays the install self-test (CI-4).
- `.github/workflows/ci.yml` on push and PR: `ubuntu-24.04` pinned, `permissions: contents: read`, a concurrency
  group, actions pinned by SHA. Jobs: `./crboost ci` (which runs `install.sh`, so under dash); the serve smoke of CI-5
  (temp `HOME`, template conf, start on `127.0.0.1`, poll for 401, mask the token, GET `/` and `/api/v1/tools` with the
  Bearer header, the `?token=` redirect sets a cookie, stop, no `Traceback` in the log); shellcheck and actionlint.
- `.github/workflows/weekly.yml` (Monday and on demand, non-blocking, opens an issue on failure): uncached
  `install.sh` on Ubuntu and in an EL container, `pip-audit -r requirements.txt --no-deps --disable-pip`, a link check
  of `README.md` and `docs/*.md`, gitleaks.
- `.github/dependabot.yml`: the `uv` ecosystem monthly, grouped into one PR, 7-day cooldown, NiceGUI majors ignored;
  `github-actions` monthly. Dependabot compiles without `uv.toml`, so some of its PRs will fail the lock gate;
  `./crboost lock` on the branch fixes them (DEP-1). If every month's fails, the platform and wheel flags move into the
  compile command. Not Renovate: it rejects our lock header.
- `install.sh` downloads the uv archive for x86_64 and checks a sha256 committed beside `required-version`, instead of
  `curl | sh` (UV-1, OS9). `docs/installation.md` lists every host the install reaches (uv's archive, CPython's,
  PyPI) for sites behind a proxy.
- The MCP forwarder's dependencies locked (`uv lock --script agent/crboost_mcp.py`, checked against uv 0.13.7) and a
  smoke run in CI (CI-9).
- Repo settings, by hand (a list in the stage record): dependency graph on (find why it sees no manifest), Dependabot
  alerts and security updates, secret scanning and push protection, private vulnerability reporting, CodeQL default
  setup, Actions limited to SHA-pinned actions, wiki off, Discussions on.

### S3 — Dependencies

Needs D10.

- `requests>=2.33`, `pillow>=13`, `biopython>=1.87`; the `requirements.in` rule tying numpy, pandas, scipy and
  requests to the old cluster modules goes (the environment has had its own Python since roadmap 39 S2) (SEC-1, OS4).
- NiceGUI 3.19.x with FastAPI and `starlette>=1.3.1` (D10), and one browser pass over every view. A CI grep lists
  `ui.html(..., sanitize=False)` calls whose argument is neither a constant nor escaped (93 calls today, OS4).

### S4 — Security defaults

Needs D14.

- Bind per D14; the banner says which interface (OS3).
- `session.json` of a curation session mode 600 in a 700 folder (OS6). **(January)** ChimeraX's REST channel
  replaced by a command file in that folder or given a per-session secret; Xvnc bound to localhost and reached through
  the ssh tunnel the OnDemand route already uses; `passwordless_vnc` out of the template.
- The cookie holds an HMAC of the token and the port, not the token; `./crboost token --rotate`; `~/.crboost` created
  0700 and logs 0600; `/api/snapshot/plan` a POST (OS12).
- IBM Plex vendored under `static/fonts/` (SIL OFL) and served from `/static`: no request to Google, and offline sites
  keep the look (OS11).
- Preflight warns when code the server or jobs execute is world-writable or owned by an account other than the
  install's (OS1).
- On the first save by another version, the old `project_params.json` is kept as `project_params.json.<version>.bak`
  (the release, or the short sha on a dev line), the last three kept, so a rollback has a file to go back to (OS13).

### S5 — Site config that survives an update

Needs D2 (the site folder).

- Template values neutral (`gpu:1`, `qos: ""`, curation partition `""`), the CBE.next values as comments, as roadmap
  43 S2 meant (ONB-3, PP21); `tilt_filter.models: {}` until the weights are published.
- Preflight checks every GRES type against `sinfo -h -o '%P %G'` and every QOS against the user's
  `sacctmgr show assoc`, naming the key; checks the napari and curation images when set; warns when a registered
  model's weights file is missing or its sha256 differs (ONB-3, ONB-6).
- Preflight's "template against this conf": keys the running release's template has and the conf lacks, and values
  that differ from the template's, each with the template's comment. It announces, never merges (ONB-2, PP6). What
  changed between two releases' templates is S8's update command's job.
- `CRBOOST_SITE_DIR` for `conf.yaml`, `qsub.sh` and weights, default `config/` (ONB-5, OS5); the port and the "already
  running" check keyed on the unresolved install path, so moving `current` does not change a user's port (OS5).
- A "Site requirements" table in `docs/installation.md`: host binaries, the driver floor for CUDA 12.9, MPS, ssh to
  nodes (or `curation.rest_enabled: false`), accounting, the hosts builds reach, fakeroot (ONB-7). **(January)**
  `./crboost site-survey`, read-only, printing the same as an answer sheet.
- The cluster tier: probes that need no GPU run on `supervisor_slurm` resources (16 min today, roadmap 39 §6.5); every
  GPU probe launches a kernel (cryocare and easymode only list devices); an MPS start/stop probe; the bridge's imports
  in the miss_alignment probe; the pymol probe goes with PyMOL (ONB-14, PP8, C02, D7).
- `code_version()` written into `run_submit.script` at submit; the driver warns when it starts under another version.
  Under D2 that never fires; it is there for a site that still pulls (PP5).
- D9's split: memory, CPUs, time and GPU count as tracked defaults beside the job types that own them; the conf's
  profiles keep placement only and overlay them; preflight lists the code-owned keys an older conf still sets, since
  they would keep overriding (ONB-2).

### S6 — Containers I: one recipe per tool, in the repo, checked

Needs D8, D15.

- The four legacy defs deleted; `easymode_1.2.5.def` in verbatim (it is the image's recipe); `napari_GL.def` renamed
  for the image it builds; D15 (C01, C03, HY11).
- `container_defs/manifest.yaml`: tool key → def, tool version, image name, the pinned def hash (D8), published
  reference and digest, the repo scripts the image runs and the imports they need, licence, published or build-yourself
  (C01, C12). The install doc's image table is generated from it; `tests/test_repo.py` keeps the two in step.
- `container_defs/README.md`, `build.sh` and `verify.sh` from the maintainer's scratch scripts, site-neutral, the
  CBE.next flags (`--fakeroot --ignore-fakeroot-command`, `APPTAINER_TMPDIR` off BeeGFS) as options; every check moved
  from `%test` (which never fails a build) into `%post` (C09).
- Identity per D8, in preflight and the landing dot (C04).
- Reproducible: a conda and pip lock per image exported from the image that runs and committed next to its def; the
  def installs from it; `From:` by digest; Miniforge by version and sha256; git sources by commit; `TZ=Europe/Vienna`
  out; cryocare and imod on Miniforge (conda-forge only); IMOD over TLS with a sha256; ArtiaX by version; personal and
  placeholder `%labels` replaced by project labels (C05, C10, C14, CR12, roadmap 20 §1.7).
- miss_alignment: the def restored to what built the running image (`cuda-toolkit`), with miss-alignment,
  torch-projectors, warpylib and starfile pinned at the versions in it and every import check hard (C02). Its move to
  `cuda-runtime` is S7's.
- `PYTHONNOUSERSITE=1` for every tool in the container wrapper, so a user's `~/.local` cannot shadow an image's
  packages (C16).

### S7 — Containers II: slim, two-stage, built in CI, published

Needs D6, D7. Best effort for December; the release does not wait on it (Munich gets the lab's images by Dropbox).

- `.github/workflows/images.yml`, the proof that every recipe still builds: one matrix job per def, on a change to
  it, on a tag, or by hand (a few times a year); a `df` check that fails clearly, preinstalled software removed,
  `setup-apptainer` at a pinned version, the build (cold, within the 6 h job limit; RELION's is the long one), a
  `%post` that fails on a broken import. Images under 9.5 GB are pushed to
  `oras://ghcr.io/<org>/crboost-<tool>:<version>` and `:sha-<commit>` with an artifact attestation on the digest;
  bigger ones are built and not pushed until they are two-stage. ChimeraX is built (the workflow sets the licence
  build argument for the build only) and never pushed (CR4, CR9, CR14).
- `container_defs/images.lock`: tool → reference and digest, per release; `./crboost images pull [tool]` pulls by
  digest into the site's image folder (cache and tmp on group scratch) and prints the conf lines to paste; it never
  edits the conf (CR17).
- Promotion: a digest goes into `images.lock` only after `./crboost test -m cluster` passed with it on our cluster
  (CR13).
- Every published image: `/opt/licenses` with the upstream licence texts and a `SOURCE-COMMIT` per GPL tool;
  `THIRD_PARTY_LICENSES.md` beside the defs; each release links or attaches the GPL sources at those commits (CR11).
- PyMOL out (D7): `services/templating/pdb_service.py`'s three scripts (extent and CA count; coordinates rotated onto
  an alignment and saved; centred and saved as CIF for cisTEM's `simulate`) become in-process biopython, checked
  against PyMOL's output on one PDB before the `pymol` tool key, its def, image, template entry and probe go (C08,
  ONB-13).
- The images, in this order (each checked with `./crboost test -m cluster`, and one bridge run for miss_alignment):
  tilt_filter and napari as they are; imod without its unused conda env (after S6's Miniforge and TLS); pytom, isonet,
  cryocare and easymode off the NVIDIA devel base onto Ubuntu with runtime-only conda or pip CUDA; miss_alignment on
  `cuda-runtime`; warp and relion two-stage (AreTomo2 and RELION compiled in a devel stage and copied into a runtime
  stage, RELION `make install`ed into a prefix, source and build tree left behind) (C07, C08, CR3). Whatever is not
  done by the tag ships as its def plus `build.sh`, and goes into `v2027.1.0`.
- Weights: a fetch step per artefact that checks a committed sha256 (easymode's `models.sha256` committed; the tilt
  filter's on Zenodo with a release-asset copy once roadmap 39 D7 is answered) (C11, CR18).

### S8 — Versions, releases, the site layout, updates

Needs D1, D2, D11.

- `VERSION`; `code_version()` returns `2026.12.0 (<branch>@<sha8>)` in a checkout and the version alone outside one,
  so every existing stamp (saves, driver headers, log, API) names the release (REL-1, CI-13).
- `CHANGELOG.md` in Keep a Changelog form with a mandatory **Upgrading** block per release (conf keys, images to pull
  or rebuild, lock changed, qsub changed); `v2026.12.0`'s written from HOTFIXES and the roadmaps finished since
  September (REL-2).
- `.github/workflows/release.yml` on a pushed `v*` tag: the tag equals `VERSION`, the CHANGELOG has its section, the CI
  gates pass; it drafts the release, appends computed upgrade notes (what changed in `container_defs/`,
  `config/*.template.*`, `requirements.txt`, `images.lock`, `SCHEMA_VERSION`, `API_VERSION` since the previous tag),
  attaches `SHA256SUMS` and the GPL source links, then publishes (immutable releases lock assets at publish). A tag
  ruleset forbids moving or deleting `v*`. The maintainer tags by hand; nothing else pushes tags.
- The site layout of D2 in `docs/installation.md`. `install.sh` takes a shared uv home for the cache and CPython (today
  fixed at `install.sh:44`); a release folder's bytecode is compiled at install, since users cannot write there.
- `./crboost updates`, one command for everything a site does with releases (D2). It finds `<root>` from the
  install's resolved path and says so when the install is a plain checkout instead:
  - `./crboost updates`: which release `current` points at; the releases in `releases/` with their date, size,
    environment, and whether a server on this host runs from them; the newest release in `repo.git` and when it was
    last fetched. Read-only, any user.
  - `./crboost updates fetch`: fetches tags into `repo.git` and prints the notes and Upgrading block of every newer
    release. The newest is picked with `packaging.version` (D1).
  - `./crboost updates install [tag]` (the newest by default): clones it beside the others, builds its environment,
    compiles its bytecode, runs its preflight against `site/`, and prints the conf keys new or changed since the
    current release, the code-owned keys the site conf should drop (D9) and the images to pull or rebuild.
  - `./crboost updates use <tag>`: moves `current` after a y/N, saying that running servers keep their release until
    restarted. Going back is the same command.
  - `./crboost updates clean`: lists the releases that are neither current nor the one before it and that no server
    on this host runs, with the environments no remaining release uses; removes them after a y/N. It cannot see
    other users' queued jobs on every cluster, so it never touches the previous release, and says so.
- The update banner (roadmap 23 stage 6, local half): the running version against `current`'s, "a newer release is
  installed, restart to use it"; and D11's notice from `repo.git`.

### S9 — The public face

Needs D3, D12.

- The notes out per D3: `git rm --cached` of `memory/`, `docs/roadmaps/`, `docs/reports/` and
  `scripts/file_roadmap_issues.py`, each ignored, the files left where they are on the maintainer's machine (other
  clones and worktrees no longer get them); history per D3's open question. Code comments lose their roadmap and
  report pointers; the runtime message at `drivers/driver_base.py:618` points at `docs/known_bugs.md` (HY1). The
  reference docs that stay
  (`docs/known_bugs.md`, `docs/miss-alignment.md`, `docs/particle-data-flow.md`, `CLAUDE.md`) lose lab sample names,
  group paths and pointers into the notes.
- README: what it is in three lines and a screenshot, what it needs, status and supported clusters, quick start for a
  new site, then using an existing install with `<install dir>`, Claude Code, cite, licence, the tools it wraps;
  badges for CI, release, licence, DOI (HY6).
- `CITATION.cff`, `CONTRIBUTING.md` (dev setup, `./crboost ci`, house idioms, `main` is dev), `SECURITY.md` (private
  reporting; threat model: per-user server, token, tunnel; shared projects make group members trusted),
  `CODE_OF_CONDUCT.md`, `NOTICE` and `THIRD_PARTY_NOTICES` (Mol*, the icons if they are Radix), issue forms (bug:
  version, preflight output, scheduler and Apptainer versions, the tool; site install), a PR template (HY5, HY7, HY8,
  QOL-1).
- The Mol* embed's source recovered from a backup; **(January)** if it is lost, rebuilt from a pinned molstar release
  in `web/molstar_embed/` with its lockfile (HY5).
- GitHub: description, topics, homepage; the five merged branches deleted and `pipeline_state_expansion` kept as an
  `archive/` tag; issue titles with names or worse renamed to roles; the roadmap-mirror issues (#114–#137, their
  bodies carry lab paths and link roadmaps that leave) closed; one milestone per release (HY13, PP9).

### S10 — The candidate and the release

Needs D12. No NOT RUN triage (D13).

- The user docs' stale parts fixed: installation.md's update section becomes `./crboost updates`, its image section
  the manifest's table (PP23).
- `v2026.12.0rc1` tagged by the maintainer; the lab install moves to the release layout on it.
- §5's install pass at Munich on rc1; what it finds fixed; `rc2` if needed, and Munich updates to it with
  `./crboost updates` (the update drill).
- `v2026.12.0`, its GitHub release and Zenodo DOI.

### The cut line and the calendar

December ships what lets Munich install and update without help and what a stranger sees first. The release is S1,
S2, S5, S6, S8, S9 and the Munich pass; S7 is best effort and never holds the tag (Munich's images come by Dropbox).
**(January)**: the curation-session rework, `site-survey`, the Mol* rebuild if its source is lost, the Claude Code
plugin, and any image S7 did not finish. If December runs short, the next to move are S3's NiceGUI (D10's fallback,
said in the notes) and S4's cookie and fonts.

| Week of | Stages |
|---|---|
| 23 Nov | §5's questions sent; D3's history answered. S1, S2 (D5's conversion included); S3's NiceGUI branch started |
| 30 Nov | S3, S4, S5 (D9 included), S6; `images.yml` running as soon as S6's defs are in (the builds are long) |
| 7 Dec | S7 as far as it gets, S8, S9; the history rewrite if chosen, then `rc1` at the end of the week |
| 14 Dec | S10: Munich's pass on rc1 (images by Dropbox), fixes, `v2026.12.0` (legacy CBE shuts down on the 18th) |
| January | `v2027.1.0`: everything marked (January) and the images S7 did not finish |

Cadence after that: `YYYY.M.0` monthly in the second week, after Dependabot's grouped lock PR and a week of lab use;
`YYYY.M.N` patches for HOTFIX entries that change results or block runs, and within a week for a high or critical
advisory in the web stack (REL-3).

## 5. The Munich install pass

**Ask now (week of 23 Nov), as a plain list:** GPU models and NVIDIA driver version (CUDA 12.9 images need a driver
new enough for it); host OS and glibc; Apptainer and SLURM versions; partition, GRES type and QOS names; whether MPS
may run on GPU nodes; SLURM accounting; whether `--fakeroot` builds work and the build host has outbound internet
(for images that are not published yet); outbound internet from the headnode; ssh from headnode to
nodes; CVMFS mounts (`unpacked.cern.ch` would let them skip pulls once images exist as OCI, CR8); whether login nodes
reap long processes; their microscope's Cs, amplitude contrast, both hands and EER grouping (HOTFIXES H2, H3, H4 are
blockers for them if these are not the lab's). Tell them the uv route is the supported one (their R1).

**The pass, on `rc1`.** Only their own `site/`, no local patches:
1. The D2 layout: `repo.git`, `releases/v2026.12.0rc1`, `current`; `./install.sh`.
2. `site/conf.yaml` from the template with their values; `./crboost preflight` until it is clean.
3. Images: the lab's current images by Dropbox (`./crboost images pull` for any already published); their conf points
   at them, and the identity check (D8) confirms each is the image the manifest names.
4. `./crboost test`, then `./crboost test -m cluster`.
5. The copia protocol on one EMPIAR-12580 tilt series, against stated expectations: each stage's status, wall time
   at CBE, the Class3D outcome (ONB-8).
6. The update drill: `rc1` to `rc2` with `./crboost updates`; a server left running on rc1 keeps working until it is
   restarted; a project saved by rc2 and opened in rc1 keeps its backup (and warns only if the schema moved).

**Done when** an unmodified release and their `site/` pass preflight and the cluster tier, copia runs, and they
updated once without asking. That closes roadmap 20 S5; the result is recorded here.

## 6. Not here

- A docs site: the README and `docs/*.md` render on GitHub. Later, Zensical on Pages (mkdocs-material reaches its end
  of life in May 2027) (DOC-1).
- pre-commit.ci (a second ruff pin), Renovate (rejects the lock header), Docker Hub as a home, self-hosted runners,
  GPU CI.
- Hashes in the lock (roadmap 39 D2 stands; reopening it is a separate decision).
- The conda/pip route for running the server (Munich's R1): unsupported, the own-Python start stays as documented.
- Polling load on SLURM (roadmap 46): the install doc states today's rate for the admins.
