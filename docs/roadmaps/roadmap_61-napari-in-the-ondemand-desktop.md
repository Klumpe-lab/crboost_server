# Roadmap 61 — napari in the OnDemand desktop: the viewer as a side-car environment, no image of ours

Scoped 2026-11-19. **S3 and S4 built the same night** with roadmap 65 S1–S3 (§7); uncommitted, R1–R3 owed. Follows
54 (napari from a tomogram's chip), 55 (the session through OnDemand's relay, the colleague's viewer as the companion)
and 56 (3-D speed). **Revised the same day.** The viewer is now
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

**S3 — crboost finds the desktop.** *Built 2026-11-19 (§7).*

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

**S4 — with the desktop (D4).** *Built 2026-11-19 (§7).* The User settings switch writes `~/.config/autostart/crboost-napari.desktop`, whose
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

## 7. Records

- **2026-11-19, late. S3 and S4 built** (crboost; EisGreis's side is roadmap 65 S1–S2, the same night).
  - **Finding the desktop** (`services/curation/session_service.py`, `services/curation/napari_link.py`).
    `find_active_napari_session` reads both kinds of registry line: a job of ours is live while its `cb-napari` job
    is; a `desktop` line while its SLURM job runs, whatever its name, and `alive` is under 30 s old. A desktop line
    without a job id (S0d is still open: whether `SLURM_JOB_ID` reaches a desktop's terminal) is live on the
    heartbeat alone. The newest live one wins, and the answer says `route`. A session folder without crboost's
    `job.json` is a desktop's: it is ready while its heartbeat is fresh, ended when not.
  - **Versions.** crboost writes `protocol` into `open.json`: the newest version the viewer's `desktop.json` reads,
    or, before a viewer runs, what the install's `eisgreis-crboost --version` says (kept five minutes). None shared:
    the dialog shows "EisGreis <version> at <install> reads protocol …, this crboost writes …: update <side>".
  - **The dialog.** With a live desktop viewer: its node (and "waiting for a tomogram" while a `--wait` viewer has no
    window), *Show in my desktop*, no Stop (the desktop is the user's). With none running: *In your desktop*, the
    portal link (`napari.desktop.form` under `ood.url` when set, else the portal), the site's hint
    (`napari.desktop.hint`; the dev conf says "partition c, 32 GB, 8 cores; for 3-D, partition g with a full GPU, an
    RTX 6000 first"), and the command with a copy button. *On a GPU node* stays beside it (D3). The open page shows a
    tomogram in a desktop viewer and says so, with no tab to move to; the rail's light reads "in your desktop".
  - **S4.** User settings, *Start EisGreis with my remote desktop*: writes or removes
    `~/.config/autostart/crboost-napari.desktop` (`Exec=<napari.eisgreis>/eisgreis-crboost --wait`). The file itself
    is the setting. A `--wait` viewer registers the desktop, opens its window on the first request, and after the
    window is closed waits for the next one.
  - **Checked** (roadmap 65 §10): crboost's service found a desktop viewer in IT's image and showed two projects'
    tomograms in it; the launcher drew on a full B200 through VirtualGL. Owed: R1–R3 with the portal's own desktop.
- **2026-11-20. S0d, from OnDemand's own records** (`~/ondemand/data/sys/dashboard/batch_connect/sys/desktop/`). The
  Remote Desktop app's token is `sys/desktop`, so its form is
  `/pun/sys/dashboard/batch_connect/sys/desktop/session_contexts/new`, now `napari.desktop.form` in both confs. The
  desktop starts with `apptainer exec` into an instance, without `--cleanenv`, so the job's `SLURM_JOB_ID` should
  reach its terminals; R1 shows it (the dialog's SLURM job row for a desktop viewer).
- **2026-11-20, later. The job route runs IT's desktop.** The maintainer still got the old look from the chip: fluxbox's
  bar along the bottom and a grainier picture, because *Start now* and *Start on a full GPU* ran `napari_GL_v2.sif`
  (TigerVNC, fluxbox). Now `containers/napari/desktop_session.sh` starts the portal's own desktop image the way the
  portal's Remote Desktop app does (an apptainer instance of `ood-desktop.sif`, TurboVNC with its password file,
  Xfce through `ood-desktop`, the image's glvnd libraries in front of `--nv`'s), then websockify and
  `eisgreis-crboost --session-dir`; it writes the same `session.json`, so the dialog, the open page and the browser
  link are unchanged. `napari.sif_path` names that image (`/software/ood/desktop/ood-desktop.sif`, IT's link to its
  current one), and preflight checks it has `ood-desktop`, `vncserver` and `vncpasswd`. `curation_session.sh` is
  ChimeraX's alone again; `napari_GL.def` is no longer run. The desktop sets `EISGREIS_NO_AUTOSTART`, so a user's
  autostart entry stands down there rather than start a second viewer. This is S6's image retirement, early; the job
  route itself stays (one click from the chip, no portal form).
  - The picture: the portal's noVNC link carries the card's compression and image-quality sliders (defaults 6 and 2,
    remembered per browser); crboost's link sets neither, so noVNC uses its own (quality 6). With TurboVNC on both
    sides now, the server is the same.
  - *Stop* now saves. A plain `scancel` signals every process of the job at once, so the VNC server died beside the
    viewer, which never got to save (as with the old worker). crboost now signals the worker's script alone
    (`scancel --batch --signal=TERM`); its cleanup, which a second signal no longer cuts short, sends SIGTERM to the
    viewer itself, found by its command line inside the instance (apptainer's wrapper passes no signal on), waits up
    to 20 s while the viewer closes its window and saves, then takes the desktop down. Meanwhile crboost counts the
    session as gone. One still running a minute later is shown again, and a Stop then cancels it outright, as it does
    a queued job (which refuses the signal: "Job can not be altered now").
  - Checked on nodes with test session folders (never the live registry): a CPU node (cbe-c3-9, job 1840030: IT's
    Xfce with the panel at the top, the viewer maximized in it, llvmpipe) and a full B200 (job 1840290: "NVIDIA
    B200/PCIe/SSE2" through VirtualGL); a stop through crboost's own `stop_napari_session` saved the comments CSV and
    ended the job (1840283); two signals 0.3 s apart (1840317) saved, cleaned up and ended the job in 7 s.
- **2026-11-20, later still. The picture, measured; no stalls on a busy node; the time limit saves.** The maintainer
  still found the picture a bit grainy. A small RFB client asking what noVNC 1.3 asks (Tight, a JPEG quality level, a
  compression level) read the JPEG TurboVNC 3.3 sends: noVNC's default level 6 is JPEG quality 79 (4:4:4), the portal
  card's default level 2 is 41 (4:2:0), 7 is 86, 8 is 92, 9 is 100. On a real tomogram (the AGG412 rerun's
  Position_11_3, 6.2 Å/px, on a 1600x1000 desktop), against a lossless capture, in the tomogram's area:

  | level | full screen | PSNR | mean / max error (grey levels) |
  |---|---|---|---|
  | 2 (the portal card) | 0.27 MB | 26.8 dB | 8.2 / 169 |
  | 6 (noVNC's, crboost's link) | 0.46 MB | 32.7 dB | 4.1 / 73 |
  | 7 | 0.55 MB | 35.5 dB | 3.0 / 64 |
  | 8 | 0.67 MB | 39.6 dB | 1.9 / 42 |
  | 9 | 1.27 MB | 54.9 dB | 0.2 / 5 |
  | lossless | 0.98 MB | exact | |

  - The worker starts TurboVNC with `-alr 0.5`: half a second after the picture stops changing, what went out as
    JPEG goes again losslessly (Tight's copy and palette filters, both of which noVNC 1.3 decodes; it has no gradient
    filter). Checked (job 1840647): 3 s after connecting at level 6, and half a second after a two-click zoom, the
    client's screen matched a lossless capture pixel for pixel; each refresh was ~1 MB. Motion still goes as JPEG at
    the browser's level (noVNC's settings panel changes it, per browser). What grain remains is the tomogram's own
    noise, for EisGreis's low-pass.
  - `pgrep -f` over the whole node took 42 s on a busy one (it reads every process's command line, other users' too,
    and one can stall); inside the instance it took 0.04 s. The window-manager wait at start and Stop's search for
    the viewer now look inside the instance. A Stop ends the job in 2 s: the viewer is signalled 49 ms after the
    script's TERM and has closed and saved 46 ms later.
  - At the time limit SLURM killed every process at once, which lost unsaved work as Stop used to. The napari job
    now carries `--signal=B:TERM@120`, so the worker gets the Stop treatment first: a 4-minute session launched
    through `launch_napari_session` (test registry, CPU node, job 1840703) got the signal at 1:43, saved and ended
    1 s later.
