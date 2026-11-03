---
name: reference_chimerax_vnc_perf
description: "ChimeraX+ArtiaX VNC lag = Mesa software GL; SOLVED 2026-06-06 with GPU node + VirtualGL EGL backend — keystone was baking the NVIDIA EGL vendor json (apptainer --nv omits it)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 6e4bb703-95a4-4a64-9500-94c5cb897a73
---

**★ SOLVED 2026-06-06 — GPU + VirtualGL works, "perfectly usable" on a P100 (not quite local-smooth,
fine for curation). The full working recipe is in the "GPU artifacts" block at the bottom; the KEYSTONE
was baking the NVIDIA EGL vendor json — apptainer `--nv` ships the NVIDIA EGL libs but NOT the GLVND
vendor config, so without it everything silently falls to Mesa→llvmpipe software. Everything above is
the original diagnosis, kept for context.**

The ChimeraX+ArtiaX curation appliance is **painfully laggy — 3-5 s to react to a single
camera move** (confirmed 2026-06-03, user loading a ribosome map). Root cause: it runs **Mesa
`llvmpipe` SOFTWARE GL on a CPU node (partition `c`)** — the 3-D scene is rasterized on the CPU.
It's the **render**, not the VNC transport (transport of a 1080p frame is sub-second; the
multi-second stall is llvmpipe producing each frame).

**What does NOT fix it (don't waste time here):**
- `lighting simple` + `graphics silhouettes false` — barely helped (confirmed). Shading isn't the
  cost; geometry rasterization at 1080p on llvmpipe is. So more ChimeraX quality knobs won't save it.
- Swapping the VNC **viewer** (RealVNC → TurboVNC) — viewer/encoding only affects the sub-second
  transport/decode, not the multi-second render. Marginal at best.

**The REAL fix = GPU + hardware GL:**
- Run the session on partition **`g`** (P100/V100/RTX/A100, infinite walltime) instead of `c`.
- apptainer **`--nv`** (inject host NVIDIA driver libs).
- **VirtualGL** `vglrun chimerax` — intercepts GLX, renders on the GPU, reads frames back to the
  VNC server. Ideally pair with the **TurboVNC server** (the actual "TurboVNC" win is server+VGL,
  not the client). CBE has TurboVNC/VirtualGL/Mesa as modules.
- This is hardware GL = fluid. It's literally the project's ORIGINAL plan (TurboVNC+VirtualGL,
  see [[project_artiax_bridge]] progress notes) before the software-GL appliance pivot for build
  portability.

**Watch-out:** with `--nv`, the container's bundled Mesa `libGL` must NOT shadow the driver's
`libGL`/`libGLX` on the GL path — VirtualGL needs the NVIDIA GL. Verify libGL resolution inside the
container when wiring this. (Separate from the [[reference_cbe_kernel_abi]] `.note.ABI-tag` trap,
which is about Qt6 loading, not GL.)

`CurationConfig.partition` (`services/configs/config_service.py`, `conf.yaml` `curation:`) is
currently `c`; the GPU path flips it to `g` + adds the `--nv`/`vglrun` to the worker
`containers/chimerax_artiax/curation_session.sh`.

**GPU artifacts — BUILT + WORKING (2026-06-06).** Recipe in `containers/chimerax_artiax/`:
- `chimerax_artiax_GL.def` (built → `chimerax_artiax_GL.sif`) — software-GL `.def` minus
  `LIBGL_ALWAYS_SOFTWARE`/`GALLIUM_DRIVER`; + VirtualGL **3.1.4** (`virtualgl_3.1.4_amd64.deb`),
  ABI-tag-stripped like the ChimeraX libs (libvglfaker is LD_PRELOAD'd → same el7 trap
  [[reference_cbe_kernel_abi]]); **+ the keystone:** bakes
  `/usr/share/glvnd/egl_vendor.d/10_nvidia.json` =
  `{"file_format_version":"1.0.0","ICD":{"library_path":"libEGL_nvidia.so.0"}}`.
- `curation_session.sh` — `CX_VGL` set ⇒ `apptainer exec --nv … /opt/VirtualGL/bin/vglrun -d egl chimerax`
  + pins `__EGL_VENDOR_LIBRARY_FILENAMES=…/10_nvidia.json` (guarded on the file). Does NOT touch
  `__GLX_VENDOR_LIBRARY_NAME` (2-D readback stays on the Xvnc software GLX). Unset ⇒ software path.
- `launch_curation_vnc.sh g` ⇒ `CX_VGL=1` + `--gres=gpu:1` + finite `--time` (`08:00:00` default,
  `CX_TIME` override): the **GPU QOS rejects `--time=0`** with `QOSMaxWallDurationPerJobLimit`.

**WHY IT WAS HARD (the keystone gotcha):** apptainer `--nv` injects the NVIDIA EGL/GLX *libraries*
(nvidia-smi works, libEGL_nvidia.so.0 present) but NOT the GLVND EGL *vendor config*. So libEGL saw only
`50_mesa.json` → probed cgroup-denied `/dev/dri` → llvmpipe software. The `libEGL warning: failed to open
/dev/dri/...: Permission denied` spam at startup IS that fallback; its DISAPPEARANCE after the json bake =
NVIDIA EGL in use = win. Proof: `vglrun -d egl glxinfo | grep -i "OpenGL renderer"` → `NVIDIA … Tesla
P100`. Quick driver check: `apptainer exec --nv …_GL.sif nvidia-smi -L`.

**RO writes fixed (2026-06-06):** worker adds `--writable-tmpfs` to the `apptainer exec` so ChimeraX's
`preregistration`/command-history writes to the baked-RO `/opt/cx` land in an ephemeral overlay (bundle
PATHS unchanged ⇒ ArtiaX still loads — safer than relocating XDG_DATA_HOME whose abs paths the toolshed
cache bakes). If a node rejects `--writable-tmpfs` (unprivileged overlay unsupported on el7), drop it (the
writes are non-fatal) and seed a writable XDG_DATA_HOME copy instead.

**Residual (cosmetic, non-fatal, both variants):** `ChimeraXHtmlView` ImportError kills the ChimeraX
**Log panel only** (QtWebEngine missing `libgbm1`/`libasound2`/`libxshmfence1`; ArtiaX unaffected — add
those libs in a rebuild to fix). **STILL UN-VALIDATED:** the actual ArtiaX *tomogram → place pick → save
`.coords`* loop on the GPU sif — only `open 4ug0` (a PDB model) has rendered = proves hardware GL, NOT
ArtiaX's tomogram/slice path. That loop is the real "ready for use" test.

**NOT yet wired into the crboost UI button.** Plug-in roadmap in `docs/roadmaps/completed/roadmap_artiax-bridge.md` → "## Plug the
GPU container into the manual-picking infrastructure": CurationConfig `gres`/`vgl` knobs → backend sbatch
(`--gres` + `export CX_VGL=1`) → `.cxc` preload (`CB_CXC` hook exists) → ingest. [[project_artiax_bridge]]
