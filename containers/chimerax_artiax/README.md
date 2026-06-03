# ChimeraX + ArtiaX curation appliance (CBE)

Run ChimeraX+ArtiaX **on the cluster** (where the tomograms live and where picks are saved) and
view it over a low-latency VNC session. Eliminates downloading hundred-GB datasets to a laptop and
the sshfs write problems — the tomogram is read in place; manual `.coords` land directly on the
cluster filesystem. Designed for blitzing 40–80 tomograms in one persistent session.

## Why this shape
- **Software OpenGL (Mesa/llvmpipe), partition `c`.** ArtiaX picking is slice-based (light GL), so
  no GPU / VirtualGL needed. CPU nodes are plentiful + infinite walltime, and we don't burn a GPU
  for hours of human-paced clicking. (Want GPU speed? Launch on `g`, run apptainer with `--nv`, and
  drop the `LIBGL_ALWAYS_SOFTWARE`/`GALLIUM_DRIVER` lines from the def.)
- **VNC, not `ssh -X`.** Only compressed frames cross the wire. The X session persists on the node
  until ChimeraX quits, so you can disconnect/reconnect the viewer without losing state.
- **Fits the existing pattern.** Same `apptainer exec` conventions and `cryoboost_containers/` home
  as `pymol.sif`, `relion5.0_tomo.sif`, etc.

## Build (once)
Fully self-contained — `%post` fetches ChimeraX from UCSF (license auto-accepted) and bakes in
ArtiaX. No binaries to download or scp. `--fakeroot` works on CBE (root-mapped namespace); the
build needs outbound HTTPS (same as the `docker://` base pull).
```
apptainer build --fakeroot chimerax_artiax.sif chimerax_artiax.def
```
Then point crboost at it: set `curation.sif_path` in `config/conf.yaml` (or the `CX_SIF` env var,
which overrides). To change the ChimeraX version, edit `CHIMERAX_VERSION` in the def's `%post`.

## Run
**From the crboost UI (primary):** the sidebar "Launch ChimeraX + ArtiaX" button submits this
session as a SLURM job and shows you the exact `ssh -L` tunnel + viewer + password. See
`backend.launch_curation_session()` and `ui/curation_session_dialog.py`.

**By hand (debug):**
```bash
CX_SIF=/groups/klumpe/software/containers/sifs/chimerax_artiax.sif ./launch_curation_vnc.sh
```
Both paths run the same worker, `curation_session.sh`. It prints a `ssh -L …` line and a VNC
password (and writes `session.json` for crboost). On your Mac: run that tunnel, then point the
**TurboVNC Viewer** at `localhost:5901`. In ChimeraX, `open <recon>.mrc` + `open <picks>.coords`
(or use the bundle/`.cxc` crboost will generate).

## First-run checklist (the bits I couldn't test from the sandbox — verify once)
- **ChimeraX exe name**: the `.deb` usually installs `chimerax`. If it's `ChimeraX`, run with
  `CX_BIN=ChimeraX ./launch_curation_vnc.sh`.
- **`apptainer` on compute nodes**: if not on PATH, fix the `module load` line in `curation_session.sh`.
- **VNC reachability**: the tunnel forwards login→`$NODE:$VNC_PORT`, so the server binds with
  `-localhost no`. If an intra-cluster firewall blocks that rfb port, either `ssh` straight to the
  node (if allowed) or ask IT for the open port range.
- **`vncserver` flags**: written for TigerVNC (`-rfbport`, `-localhost`, `-SecurityTypes VncAuth`).
  Adjust if you swap in TurboVNC server.
- **ChimeraX fetch (build time)**: if the build errors with "could not parse a download URL" or a
  non-.deb download, the pinned `CHIMERAX_VERSION` likely doesn't ship an ubuntu22.04 build — bump
  it in the def's `%post`. ArtiaX is baked into `/opt/cx` (`XDG_DATA_HOME`); nothing installs at runtime.

## Roadmap (next, in crboost itself)
- **One-click launch** ✓ DONE — sidebar button → `backend.launch_curation_session()` sbatch's the
  worker and surfaces node/port/password + tunnel in the web UI (`ui/curation_session_dialog.py`).
  Still needs a real runtime test (submit → tunnel → confirm ChimeraX+ArtiaX come up over VNC).
- **Step-through**: crboost pre-writes per-tomogram `.cxc` + a tiny `crboost next/prev/save` command
  so each tomogram is "scrub → pick → Save & Next", then auto-ingests the saved `.coords`.
