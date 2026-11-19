# Roadmap 61 — napari in the OnDemand desktop: the viewer as a side-car environment, no image of ours

Scoped 2026-11-19, nothing built. Follows 54 (napari from a tomogram's chip), 55 (the session through OnDemand's
relay, the colleague's viewer as the companion) and 56 (3-D speed). **Revised the same day.** The viewer is now
EisGreis, the lab's repository of the colleague's viewer (roadmap 65), which owns the environment and the command
that S1 and S2 had planned here. D1–D4 are answered (§3), and S0b and S0c are settled (§4): a uv environment runs in
IT's desktop, and a desktop on a full GPU draws through VirtualGL.

**Why.** The maintainer watched the colleague run his viewer inside the portal's Remote Desktop: it felt quicker
than our session and has no bar along the bottom. The colleague runs it as a plain environment beside IT's desktop,
with no image of his own. That route also takes the display stack (VNC server, window manager, GL, websockify) off
our hands. What stays ours is EisGreis (roadmap 65) and crboost's side of the link. The wider goal (every part of
crboost buildable and updatable from what the lab owns) waits; this roadmap reclaims napari first.

## 1. What there is (read 2026-11-19)

**Our route (54–56).** The napari chip submits `cb-napari` (`services/curation/session_service.py`
`launch_napari_session`), which starts `containers/chimerax_artiax/curation_session.sh` with `CB_APP=napari` in
`napari_GL_v2.sif` (recipe `/groups/klumpe/software/containers/defs/napari_GL_v2.def`, outside version control).
That brings:

- TigerVNC's Xvnc, and **fluxbox started with its defaults, whose toolbar is the bar along the bottom**;
- VirtualGL on a full GPU, or Mesa on a MIG slice (*Start now*);
- websockify, and the portal's noVNC through `/rnode`.

napari 0.10.0 and PyQt6 sit in `/opt/napari`. The launcher `containers/napari/open_tomogram.py` is a plain script
(`--session-dir`, `--companion`). With `napari.companion` set, it runs the colleague's viewer (`CompanionSession`).

**The link between server and viewer is files, and needs no change.**

- The server writes `open.json` into `~/.crboost/napari/<id>/` by rename (`_write_napari_request`), and also
  `bundle.json`, the picks and segmentations (`services/visualization/napari_bundle.py`).
- The launcher answers in `opened.json` (`_read_napari_state`).
- A session is found by its registry line (`~/.crboost/napari/registry.jsonl`) and its SLURM job, live by job
  **name** `cb-napari` (`find_active_napari_session` → `_find_live_registered`).

Roadmap 65 §4 writes this down as protocol version 1, and adds `desktop.json`.

**IT's Remote Desktop** (`/software/ood/atlas/desktop`, repo CLIP-HPC/ood-remote-desktop; image
`/software/ood/desktop/ood-desktop.sif` from CLIP-HPC/ood-desktop-image, Rocky 9.8):

- Xfce on TurboVNC through OnDemand's `vnc_container` template, websockify from the image. The SLURM job is named
  `ood-desktop` (`submit.yml.erb`), and the desktop starts in `$HOME`.
- The image's `ood-desktop` launcher (`/opt/ood-desktop/bin/ood-desktop`) runs `xfce4-session`, which starts what
  `~/.config/autostart` lists. The launcher itself only adds `Hidden=true` entries for services that make no sense in
  a batch job, and leaves the user's own entries alone.
- Partitions c (default), g, m. On g the form says "CUDA, no 3D acceleration yet", and its default GPU type is a B200
  MIG slice, which does CUDA only.
- A GPU session sets `LD_LIBRARY_PATH=<session>/glvnd:/.singularity.d/libs`. The image's own glvnd libraries come
  first, then the host's NVIDIA libraries that `--nv` brings in (`template/script.sh.erb`). Every terminal in the
  desktop inherits this.
- The image carries VirtualGL (`/opt/VirtualGL/bin`: `vglrun`, `glxinfo`, `eglinfo`), `libGL` and `libEGL`
  (glvnd), and Mesa's EGL vendor file only (`/usr/share/glvnd/egl_vendor.d/50_mesa.json`).
- Every session runs in the `interactive` QOS: at most 12 h, and per user 48 cores, 1000 GB, 2 GPUs and 5 sessions.
  The form's defaults are 4 cores, 16 GB and 8 h.
- `/users`, `/groups`, `/scratch` and `/software` are visible with no binds (`mount hostfs = yes`). The image has its
  own Slurm client but no Apptainer, so nothing of ours that needs a container can start inside it.
- The image has `libfontconfig`, `libdbus-1`, `libxkbcommon-x11` and most of libxcb. It lacks `libxcb-cursor`,
  `libxcb-icccm`, `libxcb-image`, `libxcb-keysyms` and `libxcb-render-util`, which pip's Qt needs to start on X11
  (S0b).

**The colleague's way.** A conda environment on `/groups/klumpe/software/Napari/env` from his `environment.yml`
(conda-forge: napari 0.6.6, PyQt5 5.15, napari-mrcfile-reader 0.2.0), started by `napari_viewer_hpc.sh` from a
terminal in the desktop. The script clears `PYTHONPATH`, `PYTHONHOME`, `LD_LIBRARY_PATH` and `QT_PLUGIN_PATH` first,
and his notes ask for a desktop with about 16 GB. conda-forge's Qt brings its own X libraries, which is why it runs
in IT's image. His viewer is now the lab's repository `Klumpe-lab/EisGreis` (roadmap 65).

**Not measured:** what makes the desktop feel quicker. Both draw in software. The candidates are TurboVNC against
TigerVNC (encoding), napari 0.6.6 with PyQt5 against 0.10 with PyQt6, and the window managers (S0a, now optional).

## 2. The shape

napari runs as a side-car environment inside the user's own OnDemand desktop. IT owns the desktop: the session, its
resources, VNC, the window manager, GL.

- **EisGreis owns the viewer** (roadmap 65): the environment (uv, built by its `install.sh` into its own install,
  never changed once built) and the command, `<install>/eisgreis-crboost`. The command starts the viewer and
  registers the desktop as a session crboost can find (`desktop.json`, roadmap 65 §4).
- **crboost owns its side of the link:** finding the desktop, writing `open.json` and `bundle.json` into it, the
  napari dialog's hints, and the autostart switch.

The flow:

1. Open a Remote Desktop in the portal: partition c, 32 GB and 8 cores; or partition g with a full GPU for 3-D (S5).
2. Run the command in its terminal, or let it start with the desktop (S4).
3. In crboost, the napari chip finds the desktop session and writes `open.json` into it.
4. The viewer shows the tomogram.

*Show this tomogram*, the bundle, and the saves into `<project>/Curation/napari/` work as today. Without a crboost
server, `<install>/eisgreis` is the standalone viewer, the way the colleague uses his.

## 3. Decisions

Answered by the maintainer 2026-11-19:

- **D1 — the viewer's home:** `Klumpe-lab/EisGreis`, the colleague's repository published as the lab's, developed
  with him (roadmap 65 D1–D3). `napari.eisgreis` names its install (roadmap 65 S3). Until then the deployed copy stays
  `/groups/klumpe/software/Napari/Napari_crboost`.
- **D2 — how the environment is built:** uv, with napari 0.10 and PyQt6 from PyPI (roadmap 65 D4). It runs in IT's
  image once five X libraries are supplied. EisGreis's install carries them, so nothing waits for IT (S0b, D5).
- **D3 — our job route:** kept until 3-D in a desktop is checked (R2). S0c shows a desktop on a full GPU draws
  through VirtualGL, so the route can retire (S6) once R2 passes, in either a software or a GPU desktop.
- **D4 — starting with the desktop:** an opt-in in User settings (S4). Off, the command is run by hand.

Still open, and blocking nothing:

- **D5 — IT.** Tell them what we rely on: their image's libraries, TurboVNC, the form and its limits. Worth asking
  for, though no stage waits on it:
  - the five X libraries in `ood-desktop-image`: `xcb-util-cursor`, `xcb-util-wm`, `xcb-util-image`,
    `xcb-util-keysyms`, `xcb-util-renderutil`;
  - 3-D acceleration in GPU desktops: `vglrun -d egl` works in their image on a full GPU today (S0c), so it is a
    matter of their desktop starting programs under it, or of documenting it;
  - whether the form can take its values from the URL (S0d).

## 4. Stages

**S0 — facts, no code.**

- a. *Optional now that the route is chosen.* Where the speed is: the same tomogram, on the same kind of node and
  cores, in a desktop and in our *Start now*. Measure the time to the first slice, the scroll, and a 3-D frame
  (roadmap 56's `bench3d.py`). Record the VNC servers, the encodings and the napari versions. The maintainer judges
  the feel (R0).
- b. **Done 2026-11-19.** Which environment starts napari 0.10 in IT's image. The probe is in
  `~/.crboost/eisgreis_probe/`: a uv environment, napari 0.10.0 and PyQt6 6.12.1, uv-managed CPython 3.12.13, started
  inside `ood-desktop.sif` on TurboVNC's Xvnc.
  - Without the five X libraries Qt aborts ("From 6.5.0, xcb-cursor0 or libxcb-cursor0 is needed to load the Qt xcb
    platform plugin").
  - With them (124 KB, conda-forge's xcb-util builds, on `LD_LIBRARY_PATH`), napari draws with llvmpipe.
  - Import takes 0.5–0.8 s, and the window plus a 64 × 512 × 512 layer 5–9 s, on the busy login node.
  - pip's PyQt6 needs `libxcb-cursor`, `-icccm`, `-image`, `-keysyms` and `-render-util`. The conda-forge Qt of the
    colleague's environment resolves the same libraries from its own `lib/`.
  - On conda-forge, napari 0.10.0 solves with PyQt5 5.15.11 or PySide6 6.12.0 (with `xcb-util-cursor`); there is no
    PyQt6 there.
  - A venv built on the host's `/usr/bin/python3.12` does not start in the image. EisGreis uses uv's own Python.
- c. **Done 2026-11-19** (job 1832128, a full B200 on cbe-g5-8, `s0c.sh` and its log in
  `~/.crboost/eisgreis_probe/`). IT's image under `--nv` with its GPU-session `LD_LIBRARY_PATH` (the image's glvnd,
  then `/.singularity.d/libs`), and TurboVNC's Xvnc:
  - `vglrun -d egl glxinfo -B` gives "NVIDIA B200/PCIe/SSE2", with or without an NVIDIA EGL vendor file;
  - napari from the uv environment under `vglrun -d egl` gives the same renderer (import 1.3 s, window and layer
    10.4 s on a cold node);
  - MIG slices remain software only ("No EGL devices found", roadmap 54).
- d. The portal's desktop form: its URL (the app's token), and whether it takes field values from the URL. Known: the
  job's name is `ood-desktop`. Still to check: whether `SLURM_JOB_ID` reaches a terminal in the desktop (apptainer
  passes the job's environment unless told not to).
- e. The link: a request written on the login node and read on the desktop's node through the home directory. The job
  route already relies on this; record the delay.

**S1, S2 — moved to roadmap 65.**

- The environment (planned here as a `napari/` folder and `./crboost napari-env`) is EisGreis's: its `uv.lock` and
  `install.sh` (roadmap 65 S2).
- The command (planned as `./crboost napari`) is EisGreis's `eisgreis-crboost` (roadmap 65 S1). As planned here, it
  writes `desktop.json`, refreshes a heartbeat every 10 s, removes both on exit, appends a registry line of kind
  `desktop`, and opens files given on the command line.
- The launcher leaves `containers/` (roadmap 65 S3).

**S3 — crboost finds the desktop.**

- `find_active_napari_session` also takes registry lines of kind `desktop`. They are live while their SLURM job runs
  (by id: the desktop's job is OnDemand's `ood-desktop`, not `cb-napari`) and the heartbeat is under 30 s old. The
  newest live session wins, as now.
- crboost reads the protocol versions in `desktop.json` and writes one the viewer reads. If they share none, the
  dialog says which side to update (roadmap 65 §4).
- The napari dialog (`ui/napari_session_dialog.py`), with a live desktop: *Show in my desktop* writes `open.json`
  there.
- With none, the dialog says how to start one:
  - the portal link;
  - the values: partition c, 32 GB and 8 cores (the companion keeps a tomogram's FFT in memory only from 24 GB); or,
    for 3-D, partition g with a full GPU, an RTX 6000 first (roadmap 56: 14 ms a 3-D frame against a B200's 62);
  - the one command, `<napari.eisgreis>/eisgreis-crboost`, with a copy button.
- *On a GPU node* (D3) stays beside it until S6. The chips and *Show this tomogram* do not change.

**S4 — with the desktop (D4).** The User settings switch writes `~/.config/autostart/crboost-napari.desktop`, whose
`Exec` is `<napari.eisgreis>/eisgreis-crboost --wait`. That registers the desktop and opens no window until the first
request. Turning the switch off removes the file. Xfce reads it in every desktop the user opens (S0's facts); the
login nodes run no Xfce.

**S5 — GPU drawing in the desktop** (S0c works). EisGreis's launchers (roadmap 65 §5) do this:

- On a full GPU (`nvidia-smi -L` names one and no MIG device) they run the viewer under `vglrun -d egl`; on a MIG
  slice or a CPU node, under Mesa. The window title says which.
- They keep the desktop's GL arrangement in `LD_LIBRARY_PATH` (its `glvnd` folder and `/.singularity.d/libs`) and
  drop every other entry. Clearing the variable outright, as the colleague's script does, would leave the NVIDIA
  libraries unfound.

**S6 — retire, after R1–R3 and D3.** napari's job route goes:

- the `CB_APP=napari` path and *Start now*;
- the `napari.*` placement keys;
- `napari_GL*.def` and the images, which by then hold only a desktop (roadmap 65 S3);
- napari's use of `ood.websockify_sif`.

ChimeraX keeps its job route and the curation worker. Roadmap 63's December image list loses napari.

## 5. Checks (the maintainer)

- **R0** — the feel: a desktop against *Start now*, on the same tomogram (S0a; optional).
- **R1** — open a desktop, run `eisgreis-crboost`, click the chip: the tomogram appears in the desktop. *Show this
  tomogram* switches it, and a second project's tomogram follows.
- **R2** — 3-D of a tomogram with segmentations in a desktop: software at 8 and 32 cores against roadmap 56's
  numbers, and a desktop on a full GPU under `vglrun`.
- **R3** — the companion's saves (comments CSV, STARs) land in `<project>/Curation/napari/`.
- **R4** — the colleague's standalone use, with files on the command line, works as before.

## 6. Not here

- MCP, the bare-repo origin, the wheel store, and container recipes in git: the wider self-containment, which waits.
- ChimeraX's route.
- The viewer's code, licence and structure: roadmap 65.
