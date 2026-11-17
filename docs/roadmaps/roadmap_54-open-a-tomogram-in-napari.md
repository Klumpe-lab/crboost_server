# Roadmap 54 — Open a tomogram in napari

**Status:** scoped 2026-11-17; D1–D3 taken and S0–S3 built the same day. Checked end to end on a CPU node with
software GL: the image, the launcher, the worker, the service, the VNC password, a second tomogram, and the
ChimeraX path through the changed worker (§6). **Owed:** the GPU run (R1, queued: every full B200 and RTX 6000 was
taken), the browser pass (R2), 3-D on a GPU (R3). This is the first slice of bringing a colleague's napari package
into crboost: it only opens a tomogram. What the package adds (§7) is scoped once we have read it.

**In one line.** Every tomogram preview that has a `3dmod` chip now also has a `napari` chip. A click opens a dialog
that starts napari on a GPU node showing that tomogram, or shows it in the napari already running, and hands over
the SSH tunnel, address and password for a VNC viewer. Under the hood this is the ChimeraX curation session's
SLURM + VNC + VirtualGL worker with napari as the app, our own image, and a small launcher of ours that the
colleague's plugins can later run alongside.

## 1. What existed

- **3dmod** was a copy chip in tomogram previews (`ui/components/copyable.py`): a click copied `3dmod <volume>` to
  paste into a terminal with X11. The pick viewer's 3dmod popover (`ui/particles/pick_viewer.py`) adds the `.mod`
  overlay.
- **ChimeraX + ArtiaX** runs as a desktop session: `services/curation/session_service.py` submits an sbatch job
  running `containers/chimerax_artiax/curation_session.sh` on `curation.partition` (g, `b200|rtx6000`, prefer b200,
  avoid MIG). That job starts Xvnc, runs ChimeraX under `vglrun -d egl` from `chimerax_artiax_GL.sif` with `--nv`,
  and writes `session.json` (node, port, password, tunnel command) for the control center.
- **napari on the cluster:** SBGrid's `/programs/x86_64-linux/napari/{0.4.18,0.6.4}` and EESSI's
  `napari/0.6.6-foss-2025b`, both unused by crboost and not used now either (D2).
- **Earlier napari plans:** picking_ui/10 S4, a gated napari picking pilot, never started; its file was lost in
  `bc2b41b`. Roadmap 41 chose ChimeraX for microtubules, and the DL tilt filter port left its author's napari
  viewer behind. Neither rules out a viewer that someone asks for.
- **Not in the browser.** `memory/feedback_no_browser_volume_rendering.md` still holds: tomograms are scrubbed in a
  real volumetric viewer, never rendered in the page.

## 2. Where napari runs (D1)

| | How | For | Against |
|---|---|---|---|
| A. Headnode over X11 | a copy chip like 3dmod's, `ssh -X` | no SLURM, no image | napari draws with OpenGL; over `ssh -X` that is indirect GLX, which XQuartz turns off by default and which is slow where it works; the volume is read on the shared headnode |
| **B. VNC desktop on a GPU node** | the ChimeraX session's worker with napari as the app | known to work on b200 and rtx6000 with VirtualGL; the volume is read on the node; the colleague's package goes into our image | a queue slot; a session waits when the cards are taken |
| C. In the ChimeraX desktop | napari on a live curation session's display | no second job | shares that job's 4 CPUs and 16 G |
| D. The cluster's on-demand portal | an interactive app on the portal that serves Jupyter | no tunnel or VNC client | an app on IT's portal, not in crboost: crboost cannot start a session, point it at a tomogram or see it |

**Decided (D1, 2026-11-17): B**, for speed. A is not built. D is the more complex route and is not taken; if the
portal could later serve a desktop that crboost submits itself, it could replace the tunnel for ChimeraX and napari
both, as a roadmap of its own. C stays possible (§6, S4) because the worker takes the app as a parameter.

## 3. The image (D2, as built)

**Decided (D2, 2026-11-17): our own image**, not SBGrid's or EESSI's napari.

`container_defs/napari_GL.def` → `/groups/klumpe/software/containers/sifs/napari_GL.sif` (445 MB, built
2026-11-17 by `/scratch/users/artem.kushner/container_verify/build.sh napari_GL /dev/shm/apptainer-build-napari`):

- Ubuntu 24.04, whose glibc 2.39 loads the EL10 host GL/EGL libraries `--nv` mounts (images with glibc < 2.36 fail
  on `GLIBC_ABI_DT_RELR`).
- TigerVNC 1.13, fluxbox, x11vnc (for `-storepasswd`), Mesa, VirtualGL 3.1.4 and the GLVND EGL vendor file for
  NVIDIA, as in `chimerax_artiax_GL.def`.
- napari 0.10.0 with PyQt6 (Qt 6.12), vispy 0.17.0, numpy 2.5.5, mrcfile 1.5.4, in a venv at `/opt/napari`
  (`/opt/napari/requirements.lock` holds the freeze). napari 0.10 needs Python ≥ 3.11 and defaults to PyQt6.

Two build findings, both recorded in the def and in `build.sh`:

- Ubuntu 24.04's apt fails when the build directory is on BeeGFS: renaming a downloaded `.deb` gives EBUSY. The
  build therefore runs in `/dev/shm`. `build.sh` now takes the temp dir as an optional second argument, and `/tmp`
  on the compute nodes is BeeGFS too, so it can't be used either.
- Packages that set group ownership (xterm's `libutempter0` to `utmp`, dbus to `messagebus`) cannot install under
  this cluster's fakeroot (no subuid range), so the image has neither; the session runs only the window manager and
  napari.

## 4. The launcher: `containers/napari/open_tomogram.py`

Our script, run with the image's Python from the server's checkout (the worker binds its directory), so it always
matches the server that started the session. It imports nothing from crboost.

- **Requests.** crboost writes `open.json` into the session dir (`{"seq", "title", "layers": [{"name",
  "path"}]}`), renamed into place. The launcher checks it every second and applies a request with a higher `seq`
  (a clock in ms, so a server restart cannot go backwards). A new request replaces the layers the previous one
  opened; layers the user made stay. The outcome goes to `opened.json` (`seq`, `ok`, `error`, each layer's shape
  and voxel size). This file exchange replaces the command server napari lacks.
- **Reading.** `mrcfile.mmap`, so a 2-D plane is one read from disk; 3-D reads the whole volume once. float16
  (Warp's mode 12) displays as it is.
- **Scale.** The header's voxel size, in Å (`units="angstrom"`, axis labels z/y/x, the scale bar on). A header
  without one opens at 1 per voxel, and the layer name, the window title, a warning and `opened.json` say the voxel
  size is unknown.
- **Display.** Contrast is set from the 0.5–99.5 percentiles of nine planes in the middle 60 % of Z. The view opens
  in 2-D on the middle plane. The first request waits for the event loop, so the view fits the maximized window.
- **By hand:** `containers/napari/launch_napari_vnc.sh` (srun on a GPU node, volumes as arguments).

The roadmap's earlier ideas of a display-binned copy for 3-D and a `--plugin` flag are dropped. 3-D reads the
full volume in about 7 s per GB (§6, S0), and a GPU holds the texture (R3 checks it). napari's own plugin system
loads whatever is installed in the image, so the colleague's package needs no hook of ours (§7).

## 5. crboost's side

- **Config** (`services/configs/config_service.py` `NapariConfig`, `config/conf.template.yaml` `napari:`):
  `sif_path`, `cpus`, `mem`, `time`, `geometry`. The placement fields (`partition`, `gres`, `constraint`, `prefer`,
  `avoid`) are `curation:`'s unless set. `vgl`, `login_host` and `passwordless_vnc` are always `curation:`'s.
- **Worker** (`containers/chimerax_artiax/curation_session.sh`): `CB_APP=chimerax|napari` picks the launch line.
  ChimeraX keeps its REST channel, `.cxc` and save dir. napari gets the launcher with `--session-dir`, the user's
  own `~/.config/napari` and `"rest_port": null`. Both now write **their own VNC password file**
  (`~/.vnc/passwd.crboost.XXXXXX`, passed with `-PasswordFile`, removed at exit). Before this, every session wrote
  `~/.vnc/passwd`, which Xvnc reads on each connection, so a second session would have changed the first one's
  password.
- **Service** (`CurationSessionService`): `launch_napari_session`, `open_in_napari_session`,
  `get_napari_session_info`, `find_active_napari_session`, `stop_napari_session`, all delegated by the backend.
  - The job is named `cb-napari`, its registry is `~/.crboost/napari/registry.jsonl` and its session dirs are in
    `~/.crboost/napari/`, so a napari session and a ChimeraX session can run side by side.
  - The sbatch writing moved into `_submit_vnc_job`; ChimeraX's `submit.sh` is unchanged line for line.
  - The registry search moved into `_find_live_registered`.
  - A queued session reports its SLURM reason (`slurm_reason`, which ChimeraX's poll now gets too) and SLURM's
    start estimate (`expected_start`, `SlurmService.expected_start`, asked at most once a minute).
- **UI:** `ui/components/viewer_chips.py` puts `napari` beside `3dmod` in every preview corner that had the 3dmod
  chip (tomogram gallery zoom, the per-tomogram dashboard; six call sites, `copy_3dmod_chip` gone; class
  `.cb-viewer-chip`). `ui/napari_session_dialog.py`:
  - **Header:** the status (no session / queued with its start estimate / starting / live on a node / ended).
  - **What the window shows:** the tomogram, its Å/px and its size, or what failed.
  - **Connection:** the tunnel, the address and the password, each with a copy button.
  - **Buttons:** Stop, Close, and one accent action: *Start napari* or *Show this tomogram*.
  - **Polling:** every 3 s while the dialog is open.
- **Self-test:** `tests/test_cluster.py` probes the napari image like the ChimeraX one.

## 6. Stages and checks

- **S0** (2026-11-17), on `AGG412_20251113_412_rerun` Position_1 (512 × 1024 × 1024 float16, 1 GB, 6.2 Å) on
  `/groups`: opening the map 4 ms, one plane 6–22 ms, the contrast sample 0.17 s, the whole volume 7.5 s. 2-D needs
  no binned copy.
- **S1** image and launcher, **S2** worker and service, **S3** chips and dialog, all built 2026-11-17.
- **Checked on a CPU node with software GL** (cbe-c3-12; test scripts in
  `/scratch/users/artem.kushner/container_verify/napari_test/`):
  - The launcher under Xvnc: the viewer is up in 2.8 s and the first request applied in 2 s; scale 6.2 Å, contrast
    from the sample, middle plane. 20 plane changes take 0.17 s.
  - The service end to end: `cb-napari` was submitted, the session was ready about 35 s later with the tomogram
    open, and `find_active_napari_session` finds it while the curation lookup does not. The VNC password works and
    a wrong one is refused. A second tomogram replaced the first in about 2 s, and a missing path is refused before
    anything is sent.
  - A screenshot of the live desktop shows napari maximized, the second tomogram filling the canvas and its name
    in the title.
  - ChimeraX through the changed worker: the session came up, its password works, and the REST channel answers
    `version`.
- **R1, the GPU run (owed).** Jobs 1751598 (`gpu:nvidia_b200:1`) and 1751600 (`gpu:quadro_rtx_6000:1`) run
  `napari_test/napari_gl.sh`. They were queued behind full cards, with SLURM estimating 15:55 and 04:25 the next
  day. Each log (`napari_test/napari_gl_<job>.log`) should name the NVIDIA renderer, not llvmpipe, and its
  `out_<job>/` should hold the 2-D, 3-D and second-request screenshots.
- **R2, the browser pass (owed).** The steps are:
  1. Click a tomogram's `napari` chip and *Start napari*. The status should go from Queued (with its estimate) to
     Starting to Live.
  2. Run the tunnel and connect a VNC viewer with the password.
  3. In another tomogram's dialog, click *Show this tomogram*.
  4. Click Stop.
- **R3, 3-D on a GPU (owed).** Software GL could not allocate the 2 GB 3-D texture (`GL_OUT_OF_MEMORY`) and napari
  crashed. A B200 holds it. An RTX 6000 (24 GB) should hold this one, but a bin-2 volume (16 GB as float32) may
  not. If R1 shows trouble, the fix is a coarser level for 3-D in the launcher.
- **S4 (optional), option C:** napari on a live ChimeraX session's display. Built only if asked for.

Known limits, as built:

- A session waits while the GPUs it may use are taken. The dialog shows the SLURM reason and its estimate.
- A volume outside `container_binds` (and outside `$HOME`) is not visible in the session; the launcher reports it
  as unreadable.
- One napari per user. *Show this tomogram* replaces what crboost opened there.
- Each chip opens its own volume: no denoised or segmentation layers beside the raw one yet, and no napari entry in
  the pick viewer's 3dmod popover, whose overlay of picks belongs with §7.

## 7. The colleague's package (next)

To be scoped when the maintainer shows it. Questions to answer against what exists now:

- What it adds on top of a plain viewer (picking, segmentation editing, tilt-series review, an annotation format),
  and whether it writes files crboost should pick up. If it does, those files follow the external-picker contract
  (Model B: a scoped dir, a `manifest.json`, a watcher that ingests), the same as ArtiaX; that contract was
  picking_ui/10, whose file needs restoring first.
- Its dependencies (torch? a napari version?): it installs into `napari_GL.sif`'s venv if they fit napari 0.10 and
  Python 3.12, else it gets its own image (`memory/feedback_container_separation.md`).
- Whether the server must drive it beyond opening volumes: `open.json` can carry more fields (picks to overlay,
  the layer to edit), read by the launcher.
- Pick overlays: crboost's centered-Å coordinates to napari's ZYX world coordinates in Å through
  `services/particles/coords.py`; no display-bin shift, since napari opens the full-resolution volume.

## 8. Decisions

- **D1 (taken 2026-11-17)**: B, a VNC desktop on a GPU node, for speed (§2).
- **D2 (taken 2026-11-17)**: our own image, `napari_GL.sif` (§3).
- **D3 (taken 2026-11-17)**: no Agent API verb for now; looking stays in the UI (roadmap 42).
