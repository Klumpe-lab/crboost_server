---
name: reference_dev_tree_live_drivers
description: "The user's projects (e.g. AGG412 pilot) export PYTHONPATH=/users/artem.kushner/dev/crboost_server in run_submit.script — any edit to services/ or drivers/ in the dev tree is live for every driver that starts afterwards, without a server restart"
metadata:
  node_type: memory
  type: reference
  originSessionId: cff85849-feff-49ba-824b-23bc16030660
  modified: 2026-10-05T11:02:08.708Z
---

Verified 2026-10-05 on `AGG412_20260204_412/External/job008/run_submit.script`: `PYTHONPATH=/users/artem.kushner/dev/crboost_server`.
Drivers re-resolve their inputs at drive time (`drivers/driver_base.get_driver_context` → `ProjectState.load` →
`PathResolutionService.resolve_all_paths`), so uncommitted edits to the resolver, ProjectState or a driver reach
queued SLURM jobs of a running chain the moment they start — the server process itself only changes on restart.

**How to apply:** before editing services/ or drivers/ while the user has a chain running, say so; after editing
code that drivers import, hand the user a read-only dry run on the live project (Python runs only in their shell,
see [[hpc-env-setup-for-running-python-ruff-in-this-project]]) or the stash command, before queued drivers start.
Related: [[reference_lab_install_rollout]] (the lab install is a separate clone).
