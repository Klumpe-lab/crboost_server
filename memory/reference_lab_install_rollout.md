---
name: reference_lab_install_rollout
description: "The lab's shared crboost install at /groups/klumpe/software/crboost_server — a plain main checkout updated by `git pull origin main`; what a rollout must sync by hand (conf.yaml, weights); check the HEAD tree for partial commits before pushing"
metadata:
  node_type: memory
  type: reference
  originSessionId: 795022f0-bb92-430b-879e-3d4830758a5e
  modified: 2026-10-02T14:50:24.385Z
---

`/groups/klumpe/software/crboost_server` is a clone on `main` (origin = GitHub Klumpe-lab/crboost_server), updated by
the maintainer with `git pull origin main` (fast-forward). Files are chmod 777 but `core.filemode = false`, so the
mode bits never show as local changes. Its own `venv/` (torch 2.6, Pillow; no pytest) and a `.nicegui/` dir.

Readable from the sandbox (no git): `.git/logs/HEAD` gives the deployed commit; hash-compare the tree with
`tmpOut/gittools/readindex.pl` (see [[hpc-env-setup-for-running-python-ruff-in-this-project]]) to confirm a clean pull.

Git-ignored, so every rollout must check by hand:
- `config/conf.yaml` — diff against the maintainer's `~/dev/crboost_server/config/conf.yaml`; unread keys show as
  load warnings (landing strip + preflight). `tools.<name>` is required for every tool a job uses.
- `config/qsub.sh` — must contain QSUB_EXIT_MARKERS (services/computing/slurm_service.py) byte for byte; the
  CRBOOST_SERVER_DIR/CRBOOST_PYTHON exports are unused since roadmap 20 but harmless.
- `filterTilts/*.pth` — tilt-classifier weights, copied from the maintainer's checkout.

2026-10-02 rollout (lab at 60ea956 → main): prepared `tmpOut/group_conf.yaml` (drop crboost_root; add tilt_filter,
tiltFilter + missAlign profiles, tools.miss_alignment; cisTEM bin_path = the bin DIRECTORY; denoisetrain floor 8h → 2h).
qsub.sh needed nothing. The weights file was missing there.

2026-10-18 rollout check (lab at 543798f → aa1c9a1): HEAD was a PARTIAL commit — callers of
render_species_mark / 3-arg subtomo_particles_stars / matching_subtomo_instance(…, ce_job_dir) / vis_asset_url(thumb=)
were committed, their definitions were not (dev server runs the worktree, so it looked fine). Always check the
HEAD tree itself before a push: `tmpOut/gittools/lstree.pl` + `impcheck.pl` + ruff on the export. conf.yaml: drop
`use_afterok_orchestrator` (key removed). qsub.sh, venv, weights: nothing. Token gate first reaches the lab here.

2026-10-27 (roadmap 39 S2): uv venv proven in the liveness worktree (CPython 3.11.18, preflight + `pytest -m cluster`
green, fresh 8-TS chain ran). Lab rollout steps are in roadmap 39 "Rollout": shared uv Python/cache under
`/groups/klumpe/software/cache_and_artifacts/uv/{python,cache}` (umask 002, chmod a+rX), old venv kept as
`venv.modules`, qsub.sh module lines commented + `unset PYTHONPATH`. Before any push, run the import-names check
on the committed tree (scratch tool idea: every `from <repo module> import x` must resolve) — main itself shipped
workspace_page.py without its `_field_styles`/`svg_icon`/`styles` definitions (found 2026-10-27).

2026-10-28: lab install switched to uv at `44375a0` (preflight OK, `pytest -m cluster` 11 passed in 16:43, server up).
Old module venv parked OUTSIDE the checkout at `/groups/klumpe/software/crboost_venv.modules` (`.gitignore` has
`venv/` only); `config/qsub.sh.bak-modules` = pre-uv qsub.sh. Delete both once the lab has run on uv for a while.
Later `uv pip sync` on the lab: export `UV_CACHE_DIR=/groups/klumpe/software/cache_and_artifacts/uv/cache` first.
