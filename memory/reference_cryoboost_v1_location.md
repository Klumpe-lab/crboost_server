---
name: reference-cryoboost-v1-location
description: Old cryoboost v1 source lives at /users/artem.kushner/dev/CryoBoost/ — useful for driver-level diffs against the crboost_server refactor.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 21982a9c-b83c-43ad-906d-a06e1aa36cdc
---

Old cryoboost v1 source is at `/users/artem.kushner/dev/CryoBoost/`. Layout:

- `bin/` — driver entry points (e.g. `crboost_warp_fs_motion_and_ctf.py`, `crboost_warp_ts_import.py`, etc.) — these get launched as `fn_exe` from RELION External jobs in cryoboost v1 projects.
- `src/warp/` — Python modules (e.g. `src.warp.fsMotionAndCtf` imported by the bin scripts).
- `config/`, `data/`, `docs/`, `tests/`.

Use case: when something in crboost_server diverges from established behavior, diff the corresponding bin script + src module against crboost_server's `drivers/` equivalent.

Driver naming mapping (cryoboost v1 → crboost_server):
- `crboost_warp_fs_motion_and_ctf.py` ↔ `drivers/fs_motion_and_ctf.py`
- `crboost_warp_ts_import.py` ↔ `drivers/ts_import.py`
- (etc. — confirm by file inspection)

The local launcher `/users/artem.kushner/dev/driver_crboost.sh` wraps cryoboost v1 in an Apptainer container (`/users/artem.kushner/crboost.sif`) and submits to the Klumpe CBE cluster.

Related: [[project-412-tomohand-discrepancy]].
