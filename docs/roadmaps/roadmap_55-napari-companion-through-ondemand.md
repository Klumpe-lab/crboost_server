# Roadmap 55 — A napari companion, opened through OnDemand

**Status:** scoped 2026-11-17; S0 (the browser route, by hand) done the same day: the maintainer saw a tomogram in
napari in a browser tab, through the cluster's Open OnDemand portal, with no SSH key, tunnel or VNC client.
S1 built the same evening, with the one-click chip (§4 S1): checked on the node and in the server's code, not yet
in a browser (R1 owed).
Follows roadmap 54, which built the session (crboost starts napari on a GPU node and points it at tomograms)
but handed it over as an SSH tunnel, which failed in practice and which most lab members cannot use: CBE.next
SSH needs a key or certificate that many of them do not have.

**In one line.** Clicking `napari` on a tomogram opens a browser tab with napari showing that tomogram, plus a
crboost panel. In that panel you add the tomogram's denoised and segmentation layers and its picks, edit picks or
segmentations, and save them back into the project. The colleague's package plugs into the same napari. The tab
goes through OnDemand's login and relay; the session, the image and everything in it are ours.

## 1. What exists

- **The session (roadmap 54).** `services/curation/session_service.py` submits `cb-napari`, which runs the
  shared VNC worker `containers/chimerax_artiax/curation_session.sh` with `CB_APP=napari` on `napari_GL.sif`.
  The worker starts TigerVNC's Xvnc (`-localhost no -rfbport $VNC_PORT`, its own password file) and the launcher
  `containers/napari/open_tomogram.py`, and writes `session.json` (node, port, password, tunnel command).
  crboost sends requests through `open.json` in the session dir; the launcher answers in `opened.json`.
  *Start now* runs on an interactive B200 MIG slice with software rendering and starts in seconds; *Start on a
  full GPU* uses VirtualGL.
- **The dialog** (`ui/napari_session_dialog.py`) shows the status and three copy rows: tunnel, address, password.
- **The ArtiaX write-back.** `services/curation/watcher.py` ingests saves under
  `<project>/Curation/<species>/<tomo>/` as pick lists, using the dir's `manifest.json` for attribution and
  never guessing when it is missing. This is the external-picker contract (Model B: a scoped dir, a manifest, a
  watcher). Its roadmap, picking_ui/10, is listed in the README but the file is gone and needs restoring.
- **Coordinates.** `services/particles/coords.py` converts between crboost's centered-Å picks and voxels
  (`centered_angst_to_voxel`, `voxel_to_centered_angst`).

## 2. The browser route (verified 2026-11-17)

**How the portal's Remote Desktop works.** IT's app is `/software/ood/atlas/desktop`, repo
`CLIP-HPC/ood-remote-desktop`:
- OnDemand's `vnc_container` template runs TurboVNC in `/software/ood/desktop/ood-desktop.sif` with
  `-localhost -securitytypes VNC`.
- `/usr/bin/websockify` from that image turns the VNC port into a websocket.
- The browser loads the dashboard's noVNC, which connects through the portal's relay
  `/rnode/<short node name>/<websocket port>/websockify`. The relay expects the short host name.
- Logging in to the portal (Microsoft sign-in) is the only gate.
- The desktop image has no Apptainer, so our image cannot be started from inside an OnDemand desktop. That is
  not needed: the relay reaches any port of any compute node.

**The by-hand proof (S0).**
- Into the running napari session (job 1753999 on `cbe-g5-5`, VNC on 5950), websockify was started inside
  the job: `srun --jobid=1753999 --overlap -n1 apptainer exec /software/ood/desktop/ood-desktop.sif
  /usr/bin/websockify 25950 localhost:5950`.
- The link was
  `https://ood.cbenext.vbc.ac.at/pun/sys/dashboard/noVNC-1.3.0/vnc.html?autoconnect=true&resize=remote&path=rnode/cbe-g5-5/25950/websockify&password=<the session's VNC password>`.
- The portal (172.24.208.189) connected at 13:56, and the maintainer saw the tomogram in the tab at once.

**What this relies on.** The `/rnode` relay and the dashboard's noVNC path (`noVNC-1.3.0`) are how OnDemand's
own apps work, but nothing documents them for jobs that OnDemand did not start. An OnDemand upgrade can move the
noVNC path. Both are therefore config (S1), the install self-test checks the noVNC URL answers (S1), and IT gets
told what we use (D5).

**Who can reach a session.** Through the relay, any user logged in to the portal can reach any node port, so the
VNC password is the only lock, as it is for OnDemand's own desktops. Today Xvnc also listens on every interface
(`-localhost no`) for the SSH tunnel. With websockify on the node, Xvnc can listen on localhost only, as IT's
desktop does.

## 3. What the companion is

Four parts. Only the last one depends on the colleague's package.

1. **Browser access** to the session, through the portal (S1).
2. **The companion panel.** A crboost dock widget inside napari: the tomogram crboost opened, its siblings, its
   picks, and Save. It is a Python package in this repo. The launcher imports it from the server's checkout, as
   it already runs `open_tomogram.py` from there, so a change needs a session restart, not an image rebuild, and
   it always matches the server that started the session. It imports nothing from crboost: the server resolves
   paths and converts coordinates, and the panel reads what the server wrote. The widget is added with
   `viewer.window.add_dock_widget`, so it needs no napari plugin registration. Packaging it as an npe2 plugin, so
   it also works outside crboost, can come later (D2).
3. **The exchange.** Server → napari: `open.json`, extended with layers that have roles and with points files.
   napari → server: saves into the project's `Curation/<species>/<tomo>/` dirs under the manifest contract,
   ingested by the watcher. No HTTP between node and server. Files are what both sides already use, and the
   server's in-memory state stays the truth.
4. **The colleague's package.** It installs into `napari_GL.sif` if its dependencies fit napari 0.10 and Python
   3.12, else it gets its own image (`memory/feedback_container_separation.md`). While it is being developed it
   loads from a checkout path named in the config, like the panel. Roadmap 54 §7 lists the questions to answer
   when we read it.

**Not here.**
- Rendering volumes in crboost's own page: `memory/feedback_no_browser_volume_rendering.md` holds. The tab shows
  a picture of napari streamed by VNC, not a volume drawn in our page.
- An iframe inside crboost: the portal's login and cookies would not work in a frame on another site, so the
  session opens in its own tab.
- An Agent API verb (roadmap 54 D3 stands).
- A napari app in OnDemand's own menu. It is possible later (IT installs sandbox apps from a git repo), but
  crboost could then neither start the session nor point it at a tomogram.

## 4. Stages

**S0 — the route by hand.** Done 2026-11-17 (§2).

**S1 — Open in browser** (napari and ChimeraX together, since they share the worker). **Built 2026-11-17**, as
below, with these differences:
- *One click.* The maintainer asked for one click from any tomogram to napari in a tab. The `napari` chip is now
  a link (`<a target=_blank>`) to `/napari/open?volume=<path>` (`ui/napari_open_page.py`). That page shows the
  tomogram in the user's napari session, starting one with *Start now* when none runs (D6). It moves the tab on to
  the portal link as soon as the desktop is up. The tab has to open from the click itself, since a tab opened
  later from the server is blocked as a popup, so it opens at once and waits on that page. With a session
  already live, another chip click shows that tomogram in it and opens one more tab onto the same desktop (noVNC
  connects shared). The session dialog moved behind the chip's `▾` (full GPU, Stop, status). A session started
  before the portal was configured has no websocket: the page and both dialogs say so, and the page offers
  *Stop and start again*.
- *websockify comes from IT's image for now* (`ood.websockify_sif: /software/ood/desktop/ood-desktop.sif` in the
  lab conf; blank means the session's own image). Our images are not rebuilt yet. Adding `websockify` to
  `napari_GL.def` and `chimerax_artiax_GL.def` and blanking the key is owed.
- *The fragment check is answered:* noVNC 1.3 reads every setting through `getConfigVar`, which reads the
  fragment first (`app/webutil.js`, v1.3.0). So the link puts everything after `#`, and the password never
  reaches the portal.
- *The self-test* gets 302 from the portal for a browser's `Accept` and 401 otherwise, for *any* path, a made-up
  noVNC version included, because sign-in stands in front of everything. So `test_ood_portal_answers` shows the
  portal is up and reachable, not that `novnc_path` still exists. Only R1 shows that.
- *Checked on the node* (job 1763560, cbe-g5-5, through the service's own submit path):
  - `session.json` carries `ws_port`, and the link is built from it.
  - Xvnc refuses connections from the headnode.
  - websockify answers the websocket upgrade with `RFB 003.008`.
  - napari showed the first tomogram, then a second one sent through the page's code against the live session.
  - A headless-browser run of the page was not possible here.

The plan as scoped:
- Image: `websockify` in `napari_GL.def` (pip, into `/opt/napari`) and in `chimerax_artiax_GL.def`, so the
  session does not depend on a path inside IT's image.
- Worker: picks a free port, starts websockify next to Xvnc, stops it at exit, writes `ws_port` into
  `session.json`. Xvnc gets `-localhost yes` when the portal is configured (D1).
- Config: `ood:` with `url` (the lab sets `https://ood.cbenext.vbc.ac.at`) and `novnc_path` (the lab sets
  `/pun/sys/dashboard/noVNC-1.3.0/vnc.html`). The template leaves both blank, so a site without OnDemand keeps
  the tunnel.
- Service: builds the link from the current config and `session.json` (node short name, `ws_port`, password).
  It does not copy a string written when the session started, which is how the stale tunnel host got through on
  2026-11-17.
- Dialog: one accent *Open in browser*, a real link with `target=_blank`, shown once the session is live
  (a tab opened from a background poll would be blocked as a popup). The tunnel, address and password rows show
  only without `ood.url`. Same change in the ChimeraX control center.
- Self-test: `./crboost test` checks that `ood.url` + `novnc_path` answers (302 to the sign-in counts as
  present, 404 does not).
- Check (one line): does noVNC 1.3 take `password` from the fragment (`#password=…`)? A fragment is never sent
  to the portal, so the password stays out of its logs. If not, keep the query string, as OnDemand's own
  Launch button does.

**S2 — the panel skeleton.**
- Package `containers/napari/crboost_companion/`, imported by `open_tomogram.py`, which keeps the request loop.
- The dock shows the project, the job and the tomogram crboost opened, its Å/px, and the last request's outcome
  (what `opened.json` says today, now also inside napari).
- Every layer crboost opened carries `metadata["crboost"]` (role, source job, path), so the panel can tell
  crboost's layers from the user's own.

**S3 — a tomogram's layers.**
- The server resolves a tomogram's siblings: the reconstruction, denoised volumes (Denoise Predict jobs) and
  segmentations (Segment jobs), named by job. It writes them into `open.json` with a role (`image` or `labels`).
- The panel lists them with a checkbox each; ticking one adds the layer, and labels open as a napari Labels layer
  on the same scale.
- One click from the chip still opens only the volume clicked; the rest are one tick away.
- A sibling at another pixel size gets its own scale from its header. A missing header is stated, as the launcher
  does today.

**S4 — picks, to look at.**
- The server writes each pick list of that tomogram as a points file in napari's ZYX Å, converted with
  `services/particles/coords.py`. No display-bin shift, since napari opens the full-resolution volume.
- `open.json` names the files with species, list name and colour. The panel lists them; ticking one adds a
  Points layer, read-only, with the score as a property for colour and filtering.

**S5 — save back.**
- First restore picking_ui/10 (the external-picker contract) from git history, so napari and ArtiaX follow one
  written contract.
- Edited or new picks: *Save picks* writes into `<project>/Curation/<species>/<tomo>/` under the manifest the
  server wrote when it opened the session. The watcher ingests it as a pick list (D3). Napari's Z Y X in Å go back
  through `voxel_to_centered_angst` on the server side, never in the panel.
- Edited segmentations: saved as a new MRC next to the picks with a manifest entry; what crboost does with them
  (a Segment-like output a later job can take) is decided at that stage, not before.
- The panel only writes into the dir its manifest names. It never touches job outputs
  (a job, or a viewer, never changes another job's outputs).

**S6 — the colleague's package.** Built 2026-11-17 evening, ahead of S2–S5, because the maintainer asked for it.

*What the package is.* A private repo, cloned at `~/dev/<checkout>`; the lab install, which its author keeps up
to date with `git pull`, is under `/groups/klumpe/software/Napari/` (the conf names it). `napari_mrc_lowpass_fast.py` is a
`LowPassController` with four docks:
- a low-pass filter in Å (hard, gaussian, butterworth or cosine), with a cached FFT
- one comment per tomogram, saved to `tomogram_comments.csv`
- particle annotation: a points layer per particle name, each saved as `particle_stars/<tomo stem>__<name>.star`
  with ZYX in Å from the volume corner (voxel × pixel size) and `_rlnBoxSize` in Å
- measurements in nm, and PNG export of a slice

It writes into the folder it starts in. It saves on *Save*, *Save all* and window close, not on a tomogram switch,
whatever its README says. *Save all* writes every tomogram it holds into the current folder, an empty STAR
included for a tomogram that has the layer but no points. It loads the whole volume as float32. Its author pins
napari 0.6.6 and PyQt5. A tilt-series excluder sits beside it, not used here.

*The decisions, taken by the maintainer:*
- **Environment: our image.** `napari_GL_v2.sif` is `napari_GL.def` plus `napari-mrcfile-reader` 0.2.0 (the
  one package it needs beyond napari 0.10) and websockify 0.13 (S1). It was built on a CPU node in `/dev/shm`
  (`/scratch/.../container_verify/build.sh`). `napari.sif_path` points at v2, and v1 stays for running sessions.
- **Outputs: per project.** `<project>/Curation/napari/`: the project is the nearest folder above the volume with
  `project_params.json`; the server creates the folder and sends it as `outputs` in `open.json`.

*How it plugs in.* `napari.companion` names the checkout. The worker passes it to the launcher
(`--companion`), binds it, and caps the BLAS/FFT threads at the job's CPUs. The launcher's `CompanionSession`
builds the colleague's controller in place of plain napari, using his HPC helpers for threads and the FFT cache.
It holds one project at a time:
- Before another tomogram opens, what it holds is saved.
- Moving to another project saves into the old folder and forgets the old project's tomograms, so nothing leaks
  across.
- The project's comments CSV and STARs are then read back, so a tomogram reopened later shows its picks.
- A repeat request for the tomogram on screen does nothing.
- A volume outside every project saves only when asked, into the home folder, and says so.

`opened.json` carries `viewer: companion` and `outputs`, and the session dialog names the folder. Sessions get
32 GB (`napari.mem`), so his FFT cache is on.

*Checked.*
- napari 0.10 with real OpenGL (Xvnc + llvmpipe): a tomogram loads in 13.6 s; a 40 Å gaussian takes 14.3 s on
  4 threads without the cache; a STAR round trip gives voxel (10, 20, 30) → 62, 124, 186 Å.
- The switching rules end to end, headless on three AGG412 tomograms: two picks and a comment survived a project
  switch and came back, and the second project's folder held only its own tomogram.
- A real session on cbe-g5-2 (job 1764453) on v2 through the service: the AGG412 rerun tomogram opened, with
  outputs in that project's `Curation/napari/`.
- *Show this tomogram* looked broken at first (the maintainer, 2026-11-17). The switch did happen, but:
  - napari 0.10 left his viewer at slice 41 of 511, near the top, where tomograms look alike: his viewer sets the
    pixel size after adding the layer.
  - It took ~20 s on 4 CPUs with the FFT cache on, in a frozen window.
  The launcher now centres the slice and fits the view after each load, and puts "Opening …" in the title first.
  `napari.software.cpus` is 8. The switch takes 5–9 s, checked by screenshot on job 1765969.
- 3-D in software rendering failed on a 1024×1024×512 volume (a 2 GB texture), and crboost's picks and
  segmentations did not reach his viewer. Both built 2026-11-18 in a clone of his repo, for him to take
  (a clone in the maintainer's home, uncommitted; conf `napari.companion` names it until he has them):
  - `view3d.py`: while the canvas is in 3-D, every visible image layer over 64 M voxels draws as a binned copy
    (density by mean, at most 64 M voxels; segmentations by max, at most 16 M), binned in parallel and cached.
    Density draws as an attenuated MIP in inverted grey. Switching to 3-D took 5.2 s on a software session
    (1.0 s cached), with no GL error.
  - `crboost_bundle.py`: a bundle (JSON) of a tomogram's picks and segmentations, drawn with that tomogram and
    never saved into his STARs. Picks draw in 2-D as sections of spheres of the species' diameter
    (`projection_mode="rescale_spherical"`, with the Z slice thickness widened to the largest box).
    `out_of_slice_display` does nothing on napari 0.10 at a 1-voxel thickness, his own layers included.
  - crboost writes the bundle per request (`services/visualization/napari_bundle.py`): every species'
    candidate-extract candidates, its workbench pick lists, and the registry's segmentations on the volume's
    reconstruction, in the opened volume's voxels. What it leaves out is listed in `notes`. The launcher hands
    it over afresh on every *Show this tomogram*.
  - Checked by screenshot (jobs 1775702, 1775726, MyrGFP Position_10 and Position_1): the 412 candidates fall on
    the vesicles and the ribosome candidates in the cytoplasm, and the switch swaps the bundle.
  - Not in the bundle yet: kept/filtered subsets, kitchen sets, `.coords`-only lists.

*Owed.*
- R5: pick in a browser tab, Save, reopen from the chip.
- ChimeraX's image without websockify, which keeps `ood.websockify_sif` on IT's image.
- The STARs into crboost pick lists: S5, with D3 below.

**S7 — idle sessions.** A closed tab leaves the session holding its slice until its 8 h end. TigerVNC's
`-MaxDisconnectionTime <s>` ends Xvnc when no client has been connected for that long, and the job ends with it.
The value is D4's.

## 5. Decisions

- **D1 — the SSH tunnel where the portal is configured.** Taken 2026-11-17: not shown, and Xvnc on localhost only.
  The tunnel stays for sites without OnDemand (`ood.url` blank). The alternative is a collapsed "SSH tunnel"
  section for the few with keys, which keeps Xvnc on every interface.
- **D2 — where the panel lives.** Recommend: in this repo, loaded from the server's checkout, so it is versioned
  with the server. Its own repo and an npe2 package only when someone wants it outside crboost.
- **D3 — the save-back format for picks.** Settled by S6: the colleague's viewer writes its own STAR (ZYX Å from
  the volume corner, one file per tomogram and particle name), so the watcher learns that format, converting
  through `services/particles/coords.py` with the tomogram's shape and pixel size, and skips empty files. No
  `.coords` from napari.
- **D4 — idle stop.** How long after the last tab closes a session ends, or never. The maintainer's number;
  blank means off.
- **D5 — telling IT.** We rely on the `/rnode` relay and the dashboard's noVNC for jobs OnDemand did not start.
  Recommend one message to the CBE.next Teams channel once S1 is in use, so an upgrade does not break it
  unannounced.
- **D6 — what one click starts.** Taken 2026-11-17: *Start now* (software GL on an interactive MIG slice, up in
  seconds). The full-GPU start stays in the session dialog.

## 6. Checks owed

- **R1 (S1):** one click on the chip → a tab that waits, then napari showing the tomogram (the first time through
  the portal's sign-in). *Show this tomogram* from another
  tomogram updates the same tab. Stop ends the job, and the tab shows the disconnect.
- **R2 (S1):** the same on a full GPU (VirtualGL), and a ChimeraX session through its control center.
- **R3 (S1):** a second user's session is refused without the password: open the link with the password
  removed.
- **R5 (S6):** one click on a project's tomogram opens the colleague's viewer in a tab. A pick and a comment, then
  another tomogram's chip: the first one's STAR and comment are in `<project>/Curation/napari/`, and reopening it
  shows them.
- **R4 (S3–S5):** the layers, picks and a saved pick list end to end on one AGG412 rerun tomogram, the saved list
  showing up in the Picks & curation tab.
- Roadmap 54's R2 (the tunnel pass) is replaced by R1 here.
