# ChimeraX + ArtiaX curation appliance (CBE)

Run ChimeraX+ArtiaX **on the cluster** (where the tomograms live and where picks are saved) and
view it over a low-latency VNC session. Eliminates downloading hundred-GB datasets to a laptop and
the sshfs write problems — the tomogram is read in place; manual `.coords` land directly on the
cluster filesystem. Designed for blitzing 40–80 tomograms in one persistent session.

## Two variants (same worker)
- **`chimerax_artiax.def` — software OpenGL (Mesa/llvmpipe), partition `c`.** Slice picking is light
  GL, no GPU burned, infinite walltime. Free 3-D rotation is 3-5 s/frame though.
- **`chimerax_artiax_GL.def` — GPU + VirtualGL, partition `g`. VALIDATED 2026-06-06: fluid 3-D on a
  P100.** The keystone fix was that apptainer `--nv` ships the NVIDIA EGL *libs* but not the GLVND EGL
  *vendor config*, so the def bakes `/usr/share/glvnd/egl_vendor.d/10_nvidia.json`; the worker runs
  `vglrun -d egl chimerax` under `--nv`. Full diagnosis in memory `reference_chimerax_vnc_perf`.
- **One worker drives both** (`curation_session.sh`), selected by `CX_VGL` (set automatically by
  `./launch_curation_vnc.sh g`). Use GL when you want responsive 3-D; CPU when GPU nodes are scarce.

## Why this shape
- **VNC, not `ssh -X`.** Only compressed frames cross the wire (the multi-second software-GL lag was
  the *render*, not transport — a viewer swap won't fix it; the GPU variant does). The X session
  persists on the node until ChimeraX quits, so you can disconnect/reconnect without losing state.
- **Fits the existing pattern.** Same `apptainer exec` conventions and `cryoboost_containers/` home
  as `pymol.sif`, `relion5.0_tomo.sif`, etc.

## Build (once)
Fully self-contained — `%post` fetches ChimeraX from UCSF (license auto-accepted) and bakes in
ArtiaX. No binaries to download or scp. `--fakeroot` works on CBE (root-mapped namespace); the
build needs outbound HTTPS (same as the `docker://` base pull).
```
apptainer build --fakeroot chimerax_artiax.sif    chimerax_artiax.def      # CPU / software GL
apptainer build --fakeroot chimerax_artiax_GL.sif chimerax_artiax_GL.def   # GPU / VirtualGL
```
Then point crboost at one: set `curation.sif_path` in `config/conf.yaml` (or the `CX_SIF` env var,
which overrides). To change the ChimeraX version, edit `CHIMERAX_VERSION` in the def's `%post`; the
GL def also pins `VGL_VERSION` (bump if the VirtualGL `.deb` URL 404s).

## Run
**From the crboost UI (primary):** the sidebar "Launch ChimeraX + ArtiaX" button submits this
session as a SLURM job and shows you the exact `ssh -L` tunnel + viewer + password. See
`backend.launch_curation_session()` and `ui/curation_session_dialog.py`.

**By hand (debug):**
```bash
CX_SIF=/…/chimerax_artiax.sif    ./launch_curation_vnc.sh      # CPU node (partition c)
CX_SIF=/…/chimerax_artiax_GL.sif ./launch_curation_vnc.sh g    # GPU node (auto: --gres=gpu:1, --nv, vglrun)
```
Both paths run the same worker, `curation_session.sh`. It prints a `ssh -L …` line and a VNC
password (and writes `session.json` for crboost). On your Mac: run that tunnel, then point the
**TurboVNC Viewer** at `localhost:5901`. In ChimeraX, `open <recon>.mrc` + `open <picks>.coords`
(or use the bundle/`.cxc` crboost will generate).
