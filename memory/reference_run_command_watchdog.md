---
name: reference_run_command_watchdog
description: "run_command has ONE watchdog now: total wall-clock (SLURM walltime minus a margin); the 45-min idle watchdog was removed, so a hung tool holds its GPUs until walltime"
metadata:
  node_type: memory
  type: reference
  originSessionId: 5d9bc108-a26e-4583-a84b-7d29b152fae3
  modified: 2026-10-19T16:00:00.000Z
---

`drivers/driver_base.py::run_command` kills the process group only on **total wall-clock**
(`derive_watchdog_timeout`: SLURM walltime minus WALLTIME_MARGIN_SEC; message `TIMEOUT after Ns`). Its value is
that *we* kill the run and still write `.fail` + a reason instead of SLURM killing it mid-line. A trip is not the
bug — look at why the tool was slow (~20× slower than a known-good run of the same TS → suspect node rot).

The `NO OUTPUT for Ns` idle watchdog (added 2026-08-11) is **gone** (verified 2026-10-19): the docstring says a
tool's silence is never treated as a hang, since RELION/WarpTools/pytom print nothing for hours. Consequence: a
tool that hangs (e.g. missAlign's trainer after a patch-builder process died, AGG412 FCIsolation job016,
2026-10-19) sits on its GPUs until the walltime watchdog — days under `g_long`. Only a human Cancel stops it.

See [[reference_cbe_node_mount_rot]], [[reference_cbe_slurm_qos]],
[[project_denoise_train_dynamic_walltime]], [[project_task_status_state_machine]].
