---
name: project-python-env-roadmap39
description: "Roadmap 39 (2026-10-22) — uv-built locked env on its own Python, tilt-filter torch to a container, CI; venv likely imports numpy/pandas/scipy from the arrow module"
metadata:
  node_type: memory
  type: project
  originSessionId: b51a50b1-f0e6-4506-800e-3f8705f7f36f
  modified: 2026-10-22T13:57:27.643Z
---

Roadmap 39 (`docs/roadmaps/roadmap_39-python-environment-and-setup.md`), scoped 2026-10-22, nothing built.
Stages: S0 inventory (user runs one command), S1 `uv pip compile` lock, S2 uv-managed Python 3.11 (qsub drops
module lines), S3 tilt filter in its own torch 2.6.0 container (venv 5.6 GB → < 1 GB), S4 CI + pinned ruff 0.15.12,
S5 container labels + weights sha256. Decisions D1–D7 owed.

Key finding: `venv/` has no numpy/pandas/scipy/requests; `~/.profile` loads `arrow/16.1.0-gfbf-2023b`, which likely
supplies them via PYTHONPATH (unverified: /software not mounted in the sandbox).

**Why:** user asked whether uv beats pip and what else to improve in the setup.
**How to apply:** build only on explicit "build" ([[feedback-scope-design-talk-into-roadmap]]); start from S0's
printed versions when pinning.

**2026-10-28:** S0–S2 done and live. Lab install on the uv venv (record in roadmap 39 "Rollout"); a fresh copia
chain (`copia-tutorial-20261028-1036`, Import + External/job002–011) ran clean on it, no module lines in any
run_submit.script. `compile-bytecode = true` in uv.toml. The "venv lacks numpy" note above is history.

**2026-11-06:** B200 nodes: torch 2.6.0/cu124 has no sm_100 kernels, so the DL tilt filter died there; torch went to
2.8.0/cu128 (`d12f371`) and `install.sh` became self-contained (`f49ad87`). That evening S3 and S6 were built on the
working tree, NOT RUN on dev/lab/a project: the environment is `.uv/env/<python>-<12 hex of the lock's pins>`, found or
built by `./crboost` via `install.sh` (never modified, built under a temp name and renamed, no `--clear`, no
"stop every server first"); `venv` is only a convenience link; commands are `./crboost preflight|test|python|lock|env|clean`.
The tilt filter's torch moved to the `tilt_filter` image (`container_defs/tilt_filter_torch2.8.0_cuda12.8.def`,
`filterTilts/deepLearning/predict.py`): of the venv's 7.3 GB, 6.6 was CUDA torch; an environment is now 0.8 GB, a first
start 7 s. Maintainer chose a container over CPU torch (CPU: 116 tilts/s per forward pass on 4 threads, ~15 with the
checkpoint's 8-fold TTA). Pending by hand: delete `crboost_python:` from both conf.yaml, put the image in the lab's
`sifs/` and add `tools.tilt_filter`, first `./crboost` in dev, then the lab. Details and records: roadmap 39 S3 and S6.
