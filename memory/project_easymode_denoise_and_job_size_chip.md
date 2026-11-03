---
name: project-easymode-denoise-and-job-size-chip
description: "2026-10-23 built (NOT RUN), committed on main by 2026-10-24 — Denoise predict method \"easymode\" (n2n/ddw/iso, no train job, registry 1.7) + finished-job size chip in the job page header"
metadata:
  node_type: memory
  type: project
  originSessionId: 44732dc1-e6a9-4518-b982-0081f27ad7f9
  modified: 2026-10-23T10:17:54.116Z
---

Built 2026-10-23, NOT RUN; committed on main by 2026-10-24.

- **Denoise predict "easymode"** (`DenoiseMethod.EASYMODE`, `EasymodeDenoiser` n2n/ddw/iso, `easymode_tta`, `easymode_iter`). `model_path` slot `when="uses_trained_model"`; `inherited_from_train` → (None, None) for easymode, `trained_method` is the raw lookup the Denoiser row uses. Driver `_run_easymode_denoise`: resample to 10 Å (`write_resampled`), `easymode denoise` with a per-task EASYMODE_CACHE_DIR + offline proxy, `map_back` onto the recon grid, weights `<cache>/models/<m>_direct.h5` stamped as registry `model_path`. Registry schema → (1, 7) ("easymode" literal): older servers skip TS that carry one. Denoise predict no longer depends on Denoise train in `spec.py`. Gallery label "easymode iso".
- **Denoiser row reworked same day** (user: the inference "locked" predict to IsoNet / cryoCARE): one choice = each denoisetrain job (wires `model_path` via source override, copies method + deconv), cryoCARE/IsoNet only for a manual model file, or easymode. `inherited_from_train` matches the resolved model_path only; the single-train fallback is gone. The "lock" the user saw was the old chip: the server on 24326 started 09:07, before the build.
- **Job size chip** `ui/pipeline_builder/disk_footprint_chip.py`: `DiskUsage.jobs` (job folder → bytes) from the existing project walk; stale vs the exit-marker mtime → one `request()` per page.
- Unverified: easymode denoise's output filename/shape (driver fails loudly if not exactly one MRC on the 10 Å grid).

**Why:** user wanted easymode-denoised missAlign tomos (job024, MyrGFP_Grid2) tracked in the gallery and reusable by several Segment runs, and per-job disk sizes visible.
**How to apply:** first run = Denoise predict easymode iso on tsReconstruct__2 → Segment on it, constraint g2|g4. Related [[project-segmentation-diagnosis-job039]], [[project_disk_footprint_roadmap36]], [[reference-cbe-gpu-memory]].
