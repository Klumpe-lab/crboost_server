---
name: feedback-no-cluster-lib-snooping
description: "Don't hunt the cluster's shared libs / force numpy to import in the sandbox to self-verify; py_compile+ruff is the ceiling, runtime is the user's job"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b828f5f3-259e-4212-850c-38c13da2b136
---

The user was visibly annoyed (2026-06-23) by Claude `find`-ing the cluster for `libflexiblas.so.3` and prepending it to `LD_LIBRARY_PATH` to get the cluster numpy/skimage to import in Claude's sandbox, purely to self-verify code.

**Why:** numpy/scipy/skimage come from the cluster's module-loaded scipy-bundle and need BLAS libs that aren't on the sandbox's path (see [[hpc-env-setup-for-running-python-ruff-in-this-project]]). Trying to make them import = poking around their cluster's library tree, which the user dislikes and considers a waste.

**How to apply:** verification ceiling in-sandbox is `venv/bin/python3 -m py_compile`, `venv/bin/ruff check`, `venv/bin/ruff format`. Do NOT chase LD_LIBRARY_PATH, hunt for cluster .so files, or otherwise try to import numpy-dependent modules to run them. Runtime verification (anything importing numpy/pandas/skimage, the full app) is the USER's job — they restart `python main.py` (no auto-reload) and check. If a runtime check would genuinely help, just ASK the user to run it and paste output, rather than engineering around the sandbox.

**Also (2026-10-02):** the same goes for project data. Asked a yes/no question ("is the backfill only for older projects?"), Claude started scanning every registry under /groups/klumpe/crboost_data to back the answer; the maintainer interrupted angrily ("don't scan anything … answer this one question"). Answer direct questions from what is already known; scan data only when asked to.
