# Roadmap 61 — napari in the OnDemand desktop: the viewer as a side-car environment, no image of ours

Scoped 2026-11-19, nothing built. Follows 54 (napari from a tomogram's chip), 55 (the session through OnDemand's
relay, the colleague's viewer as the companion) and 56 (3-D speed).

**Why.** The maintainer watched the colleague run his viewer inside the portal's Remote Desktop: it felt quicker
than our session and has no bar along the bottom. The colleague runs it as a plain environment beside IT's desktop,
with no image of his own. That route also takes the display stack (VNC server, window manager, GL, websockify) off
our hands: what stays ours is the napari environment, the companion and the link from crboost. The wider goal (every
part of crboost buildable and updatable from what the lab owns) waits; this roadmap reclaims napari first.

## 1. What there is (read 2026-11-19)

**Our route (54–56).** The napari chip submits `cb-napari` (`services/curation/session_service.py`
`launch_napari_session`), which starts `containers/chimerax_artiax/curation_session.sh` with `CB_APP=napari` in
`napari_GL_v2.sif` (recipe `/groups/klumpe/software/containers/defs/napari_GL_v2.def`, outside version control):
TigerVNC's Xvnc, **fluxbox started with its defaults, whose toolbar is the bar along the bottom**, VirtualGL on a
full GPU or Mesa on a MIG slice (*Start now*), websockify, and the portal's noVNC through `/rnode`. napari 0.10.0 and
PyQt6 sit in `/opt/napari`. The launcher `containers/napari/open_tomogram.py` is a plain script (`--session-dir`,
`--companion`); with `napari.companion` set it runs the colleague's viewer (`CompanionSession`).

**The link between server and viewer is files, and needs no change.** The server writes `open.json` (and
`bundle.json`, the picks and segmentations, `services/visualization/napari_bundle.py`) into
`~/.crboost/napari/<id>/` by rename (`_write_napari_request`); the launcher answers in `opened.json`
(`_read_napari_state`). A session is found by its registry line (`~/.crboost/napari/registry.jsonl`) and its SLURM
job, live by job **name** `cb-napari` (`find_active_napari_session` → `_find_live_registered`).

**IT's Remote Desktop** (`/software/ood/atlas/desktop`, repo CLIP-HPC/ood-remote-desktop; image
`/software/ood/desktop/ood-desktop.sif` from CLIP-HPC/ood-desktop-image, Rocky 9.8):
- Xfce on TurboVNC through OnDemand's `vnc_container` template, websockify from the image.
- Partitions c (default), g, m. On g: "CUDA, no 3D acceleration yet"; MIG slices are CUDA only. So a desktop draws
  in software today, and the colleague's quicker-feeling session was software GL too.
- Every session runs in the `interactive` QOS: at most 12 h, and per user 48 cores, 1000 GB, 2 GPUs, 5 sessions.
  The form's defaults are 4 cores, 16 GB, 8 h.
- `/users`, `/groups`, `/scratch` and `/software` are visible with no binds (`mount hostfs = yes`); the image has its
  own Slurm client; it has no Apptainer, so nothing of ours that needs a container can start inside it.
- The image carries `/opt/VirtualGL` (`vglrun`), `libGL`, `libEGL`, `libfontconfig`, `libdbus-1`,
  `libxkbcommon-x11`, and **not `libxcb-cursor`**, which PyQt6 (Qt 6.5 and later) needs to start on X11.

**The colleague's way.** A conda environment on `/groups/klumpe/software/Napari/env` from his `environment.yml`
(conda-forge: napari 0.6.6, PyQt5 5.15, napari-mrcfile-reader 0.2.0), started by `napari_viewer_hpc.sh` from a
terminal in the desktop (it clears `PYTHONPATH`, `PYTHONHOME`, `LD_LIBRARY_PATH`, `QT_PLUGIN_PATH` first); his
notes ask for a desktop with about 16 GB. conda-forge's Qt brings its own X libraries, which is why it runs in IT's
image.

**Not measured yet:** what makes the desktop feel quicker. Both draw in software; the candidates are TurboVNC
against TigerVNC (encoding), napari 0.6.6 with PyQt5 against 0.10 with PyQt6, and the window managers (S0a).

## 2. The shape

napari runs as a side-car environment inside the user's own OnDemand desktop. IT owns the desktop: the session, its
resources, VNC, the window manager, GL. crboost owns three things, all in the repo and on `/groups`:

1. **The environment:** locked in the repo, built by our launcher into the install, never changed once built.
2. **The command:** `<install>/crboost napari [files …]`, run in the desktop's terminal. It starts the viewer and
   registers the desktop as a napari session crboost can find.
3. **The link:** the session folder and its files, as today.

The flow: open a Remote Desktop in the portal (partition c, 32 GB, 8 cores) → run the command in its terminal (or
let it start with the desktop, S4) → in crboost the napari chip finds the desktop session and writes `open.json`
into it → the viewer shows the tomogram. *Show this tomogram*, the bundle and the companion's saves into
`<project>/Curation/napari/` work as today. Without a crboost server the same command is the standalone viewer, the
way the colleague uses his.

## 3. Decisions

- **D1 — the viewer's home (open, the maintainer with the colleague):** his repository opened, or copied whole
  into a home the lab owns (inside crboost, or a lab repository). Nothing here depends on the answer:
  `napari.companion` names a checkout either way. Until then the deployed copy stays
  `/groups/klumpe/software/Napari/Napari_crboost`.
- **D2 — how the environment is built:** uv with PyPI wheels (PyQt5, or PyQt6 with `libxcb-cursor` supplied), or a
  locked conda-forge environment (pixi). S0b decides by what starts in IT's image. napari 0.10 either way: the
  companion's 3-D view and bundle hooks are written against it. Recommended: whichever starts; uv on a tie, since
  the install already has it and one lock format serves both environments.
- **D3 — our job route:** kept as *On a GPU node* while the desktop has no GPU drawing, since 3-D of segmentations is
  where software rendering is slowest (roadmap 56). Retired (S6) once the desktop's 3-D is good enough, by S0c or
  the maintainer's R2.
- **D4 — starting with the desktop:** an opt-in in User settings (S4); off, the command is run by hand.
- **D5 — IT:** tell them what we rely on (their image's libraries, TurboVNC, the form and its limits), and ask about
  3-D acceleration (`vglrun` is in their image).

## 4. Stages

**S0 — facts, no code.**
- a. Where the speed is. Open the same tomogram on the same kind of node and cores in a desktop (the colleague's
  environment) and in our *Start now*. Measure the time to the first slice, the scroll and a 3-D frame (roadmap 56's
  `bench3d.py`), and record the VNC servers and encodings and the napari versions. The maintainer judges the feel
  (R0).
- b. Which environment starts napari 0.10 and the companion in IT's image. Try uv with PyQt5, uv with PyQt6, and
  conda-forge. Start each headless inside `ood-desktop.sif` in a job with its own TurboVNC, as the desktop does, and
  record a screenshot, the GL renderer string and the import time.
- c. GL in a desktop on a full GPU: whether `vglrun -d egl` under `--nv` gives the NVIDIA renderer.
- d. The portal's desktop form: its URL (the app's token), whether it takes field values from the URL, and the
  desktop job's SLURM name and whether `SLURM_JOB_ID` reaches its terminal.
- e. The link: a request written on the login node and read on the desktop's node through the home directory. The
  job route already relies on this; record the delay.

**S1 — the environment.** A `napari/` folder in the repo holds its manifest and lock (D2): napari 0.10, the Qt
binding, napari-mrcfile-reader 0.2.0, mrcfile, numpy, scipy, and whatever the companion imports. `./crboost
napari-env` (and `install.sh`) builds it into `<install>/.napari/env-<hash of the lock>`, never modified afterwards,
as roadmap 39 does for the server's (S3, S6 there). `./crboost env` and `./crboost clean` list it. The self-test
checks it imports napari and starts Qt offscreen.

**S2 — the command.** `./crboost napari [files …] [--wait]` runs the launcher natively in that environment. It
clears `PYTHONPATH`, `PYTHONHOME`, `LD_LIBRARY_PATH` and `QT_PLUGIN_PATH` as the colleague's script does. The
launcher moves out of `containers/` (it no longer belongs to an image). With `napari.companion` set, the window is
the companion viewer. The command:
- makes `~/.crboost/napari/<id>/` and writes `desktop.json`: the desktop's SLURM job id, node, `DISPLAY`, pid,
  napari and environment versions, start time;
- refreshes a heartbeat file every 10 s, and removes both on exit;
- appends a registry line of kind `desktop`;
- with files given, opens them as the standalone viewer does.

**S3 — crboost finds the desktop.**
- `find_active_napari_session` also takes registry lines of kind `desktop`. They are live while their SLURM job runs
  (by id: the desktop's job has OnDemand's name, not `cb-napari`) and the heartbeat is under 30 s old. The newest live
  session wins, as now.
- The napari dialog (`ui/napari_session_dialog.py`), with a live desktop: *Show in my desktop* writes `open.json`
  there.
- With none: how to start one. The portal link, the values (partition c, 32 GB, 8 cores: the companion keeps a
  tomogram's FFT in memory only from 24 GB), and the one command with a copy button.
- *On a GPU node* (D3) stays beside it. The chips and *Show this tomogram* do not change.

**S4 — with the desktop (D4).** The User settings switch writes `~/.config/autostart/crboost-napari.desktop`,
whose `Exec` is the command with `--wait`: it registers the desktop and opens no window until the first request.
Turning the switch off removes the file. Xfce reads it in every desktop the user opens; the login nodes run no Xfce.

**S5 — GPU drawing in the desktop, only if S0c works.** On a full GPU the command runs napari under
`vglrun -d egl`, otherwise under Mesa; the window title says which.

**S6 — retire, after R1–R3 and D3.** napari's job route goes: the `CB_APP=napari` path, *Start now*, `napari.*`
placement keys, `napari_GL*.def` and the images, napari's use of `ood.websockify_sif`. ChimeraX keeps its job route
and the curation worker.

## 5. Checks (the maintainer)

- **R0** — the feel: a desktop against *Start now*, on the same tomogram (S0a).
- **R1** — open a desktop, run the command, click the chip: the tomogram appears in the desktop. *Show this tomogram*
  switches it, and a second project's tomogram follows.
- **R2** — 3-D of a tomogram with segmentations in the desktop, at 8 and 32 cores, against roadmap 56's numbers.
- **R3** — the companion's saves (comments CSV, STARs) land in `<project>/Curation/napari/`.
- **R4** — the colleague's standalone use, with files on the command line, works as before.

## 6. Not here

- MCP, the bare-repo origin, the wheel store and container recipes in git: the wider self-containment, which waits.
- ChimeraX's route.
- The viewer's code and licence (D1), which the maintainer settles with the colleague.
