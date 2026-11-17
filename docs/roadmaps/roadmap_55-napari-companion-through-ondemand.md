# Roadmap 55 — A napari companion, opened through OnDemand

**Status:** scoped 2026-11-17; S0 (the browser route, by hand) done the same day: the maintainer saw a tomogram in
napari in a browser tab, through the cluster's Open OnDemand portal, with no SSH key, tunnel or VNC client.
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

**S1 — Open in browser** (napari and ChimeraX together, since they share the worker).
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

**S6 — the colleague's package.** Scoped when the maintainer shows it (roadmap 54 §7). The panel and the
exchange above are what it builds on.

**S7 — idle sessions.** A closed tab leaves the session holding its slice until its 8 h end. TigerVNC's
`-MaxDisconnectionTime <s>` ends Xvnc when no client has been connected for that long, and the job ends with it.
The value is D4's.

## 5. Decisions

- **D1 — the SSH tunnel where the portal is configured.** Recommend: not shown, and Xvnc on localhost only.
  The tunnel stays for sites without OnDemand (`ood.url` blank). The alternative is a collapsed "SSH tunnel"
  section for the few with keys, which keeps Xvnc on every interface.
- **D2 — where the panel lives.** Recommend: in this repo, loaded from the server's checkout, so it is versioned
  with the server. Its own repo and an npe2 package only when someone wants it outside crboost.
- **D3 — the save-back format for picks.** Today's watcher ingests ArtiaX's `.coords`. Either napari saves a
  `.coords` too (the watcher stays as it is), or the watcher learns a napari points CSV. Recommend `.coords`,
  unless the colleague's package already writes a format of its own.
- **D4 — idle stop.** How long after the last tab closes a session ends, or never. The maintainer's number;
  blank means off.
- **D5 — telling IT.** We rely on the `/rnode` relay and the dashboard's noVNC for jobs OnDemand did not start.
  Recommend one message to the CBE.next Teams channel once S1 is in use, so an upgrade does not break it
  unannounced.

## 6. Checks owed

- **R1 (S1):** chip → *Start now* → *Open in browser* → tomogram in the tab. *Show this tomogram* from another
  tomogram updates the same tab. Stop ends the job, and the tab shows the disconnect.
- **R2 (S1):** the same on a full GPU (VirtualGL), and a ChimeraX session through its control center.
- **R3 (S1):** a second user's session is refused without the password: open the link with the password
  removed.
- **R4 (S3–S5):** the layers, picks and a saved pick list end to end on one AGG412 rerun tomogram, the saved list
  showing up in the Picks & curation tab.
- Roadmap 54's R2 (the tunnel pass) is replaced by R1 here.
