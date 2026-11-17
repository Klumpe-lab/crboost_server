# Roadmap 54 — Open a tomogram in napari

**Status:** scoped 2026-11-17, nothing built or run. First slice of bringing Lorenz's napari package into crboost:
this roadmap only opens a tomogram. What his package adds (§7) is scoped once we have read it.

**In one line.** Wherever crboost offers `3dmod` for a tomogram, it also offers napari: one click opens that
reconstruction (or a binned display copy) in a napari window at the right voxel size, on a GPU node over the same
SLURM + VNC desktop the ChimeraX curation session already uses. The launcher is a small script of ours, and
Lorenz's plugins and widgets will be loaded through it later.

## 1. What exists today

- **3dmod** is a copy chip (`ui/components/copyable.py:44`, `copy_3dmod_chip`). It copies `3dmod <volume>` for the
  user to paste into a terminal on the headnode with X11. The pick viewer's 3dmod popover
  (`ui/particles/pick_viewer.py:2563`) adds the `.mod` overlay. The volume comes from `resolve_volume_for_3dmod`
  (`services/dashboard_data.py:127`): the row's `rlnTomoReconstructedTomogram`, or None when it is not on disk.
- **ChimeraX + ArtiaX** runs as a desktop session: `services/curation/session_service.py` submits an sbatch job
  that runs `containers/chimerax_artiax/curation_session.sh` on `curation.partition` (g, `b200|rtx6000`, prefer
  b200, avoid MIG). The job starts Xvnc, runs ChimeraX under `vglrun -d egl` from `chimerax_artiax_GL.sif`
  with `--nv`, and writes `session.json` (node, port, password, tunnel command). The control center
  (`ui/curation_session_dialog.py`) shows the tunnel and the password. There is one session per user, registered
  across projects. `curation.display_bin` (default 2) opens a block-binned copy cached beside the reconstruction.
- **napari on the cluster:** SBGrid has `/programs/x86_64-linux/napari/{0.4.18,0.6.4}` and
  `napari-tomoslice/0.1.1`, plus `napari_boxmanager` and `napari-segselect`. EESSI has the module
  `napari/0.6.6-foss-2025b`. Nothing in crboost uses any of them.
- **Earlier napari plans:** picking_ui/10 S4 ("napari pilot, PyQt5 container, same contract as ArtiaX") was gated
  and never started. Its file was lost in `bc2b41b` (README "restore pending"). Two later decisions went against
  napari: roadmap 41 chose to stay on ChimeraX for microtubules, and the DL tilt filter port did not bring over its
  author's napari viewer. Neither decision rules out a viewer that someone asks for, and this roadmap is that viewer.
- **Not in the browser.** `memory/feedback_no_browser_volume_rendering.md` still applies: tomograms are scrubbed in
  a real volumetric viewer, never projected or rendered in the page.

## 2. Where napari runs (D1)

| | How | For | Against |
|---|---|---|---|
| **A. Headnode over X11** | a `napari` copy chip beside `3dmod`; SBGrid napari on the headnode, `ssh -X` | no SLURM, no container, a day's work | napari draws with OpenGL (vispy). Over `ssh -X` that is indirect GLX: XQuartz turned it off by default in 2.8, and where it works it is slow. The 1–2 GB volume is read on the shared headnode. |
| **B. A VNC desktop on a GPU node** (recommended) | the ChimeraX session's machinery with napari as the app: an sbatch job, Xvnc, `vglrun -d egl napari`, the same tunnel | known to work on b200 and rtx6000 with VirtualGL (ChimeraX does it every day); fluid 3D rendering; the volume is read on the node; Lorenz's package goes into the image | one more image; a session takes a queue slot and starts in tens of seconds |
| **C. In the ChimeraX desktop** | when a curation session is live, start napari on its display (`DISPLAY=:N` on that node) | no second job, and ChimeraX and napari sit side by side | both apps share the session's 4 CPUs and 16 G; napari has to come from an image that includes VirtualGL |

Recommendation: **B, built so that C costs little afterwards.** The worker takes the app as a parameter, which
gives C a single code path. A stays as a fallback only if S0 shows that X11 is good enough on the lab's Macs, and we
do not build it otherwise.

## 3. The image (D2)

`napari_GL.sif`, defined in `/groups/klumpe/software/containers/defs/napari_GL.def` beside the other images:

- **Base:** the same base as `chimerax_artiax_GL.def`, with TigerVNC, VirtualGL, x11vnc and a window manager, so
  the worker script needs no special case for napari.
- **Python packages:** napari 0.6.x (pinned, `[pyqt5]`) and `mrcfile`. Lorenz's package comes later, either baked
  into the image or installed into a bind-mounted environment, decided in §7.
- **Alternatives:** SBGrid's 0.6.4 or the EESSI 0.6.6 module, run under the GL image's `vglrun` with `/programs`
  bound in. That saves building an image but mixes two glibc and Qt stacks. S0 tries it, and we adopt it only if it
  works cleanly.

## 4. The launcher: `containers/napari/open_tomogram.py`

This is a script of ours, not napari's command line, because napari's built-in readers do not open MRC files the way
we need:

1. It opens the volume with `mrcfile.mmap` (read-only) and reads ZYX directly, so scrubbing in 2D reads one slice
   at a time from BeeGFS rather than the whole file.
2. It sets the layer scale from the header voxel size (Å). **If the header has no voxel size, the window title
   says so and the scale is left at 1 with a "voxel size unknown" label**; it never defaults to 1 Å silently
   (CLAUDE.md, "never invent defaults").
3. It sets contrast limits from robust percentiles of a sample of slices, not from the whole volume.
4. It opens in 2D slicing at the middle Z. 3D rendering is one toggle away and works on the display-binned copy
   (§5).
5. It takes several volumes (`--layer name=path` repeated), so that raw, denoised and segmentation can sit as
   layers of one tomogram. It also accepts a `--plugin` hook, which is where Lorenz's package enters (§7).

## 5. What crboost opens

- **Which file:** the same volume `resolve_volume_for_3dmod` returns. If it returns None, the chip is greyed out
  with "no reconstruction on disk", as the 3dmod chip already does.
- **3D rendering:** the binned copy that `curation.display_bin` already writes and caches (one code path, one
  cache). 2D slicing always uses the full-resolution volume. The `(N-1)/2·px` corner shift does not matter for
  viewing alone, but it matters as soon as picks are overlaid (§7). The layer's `translate` records the shift then.
- **Denoised and segmentation layers:** offered when the project has a Denoise Predict or Segment output for the
  same tomogram. They are layers in the same window, not separate windows.

## 6. Stages

- **S0 — hand checks, no product code.** (1) Run SBGrid napari 0.6.4 over `ssh -X` from a lab Mac on one
  bin-4 reconstruction, and note whether it opens and how slices scroll (this decides whether A is dropped).
  (2) In an `srun` on a b200 and on an rtx6000, run `vglrun -d egl napari` inside the ChimeraX GL image with
  `/programs` bound in, and check whether it starts, whether the GPU renders (`glxinfo -B` inside the session) and
  how fast 3D is on a bin-2 copy. (3) Time `mrcfile.mmap` slicing of a 1–2 GB float32 reconstruction on BeeGFS.
  These results decide D1 and D2.
- **S1 — the image and the launcher.** Write `napari_GL.def` (or record why SBGrid is enough), add
  `containers/napari/open_tomogram.py`, and add a hand path `launch_napari_vnc.sh` alongside
  `launch_curation_vnc.sh`. Done when a hand launch opens a tomogram at the right voxel size.
- **S2 — the worker takes an app.** Generalise `curation_session.sh` so that `CB_APP=chimerax|napari` picks the
  launch line, and keep everything else as it is (display choice, VNC auth, `session.json`, the tunnel). Add a
  `napari:` block to `conf.yaml` and the template (`sif_path`, `napari_bin`, and its own `cpus`/`mem`/`time`), and
  inherit placement (`partition`, `gres`, `constraint`, `prefer`, `avoid`, `vgl`) from `curation` unless it is set.
  Add `CurationSessionService.launch_viewer_session(app=…)` and make `launch_curation_session` call it. Napari
  sessions go in the same per-user registry with `app` recorded, so a user can have one of each.
- **S3 — the UI entry.** Add a `napari` chip next to every 3dmod chip (tomogram previews, the pick viewer's
  popover). A click either starts a napari session scoped to that tomogram or, when one is live, says so and
  offers to switch it. The session dialog is the curation control center with its ChimeraX-only parts (REST
  switch, ArtiaX save notes) hidden for napari. Switching tomograms in a running napari session needs a command
  channel, which napari lacks. S3 restarts the session instead, and §7 may bring a better way.
- **S4 — option C (optional).** When a ChimeraX session is live, offer "open in this desktop" so napari starts on
  the same display. Built only if the user asks for it after S3.

S0 is a few hours. S1–S3 follow existing code closely; the risk is in the image (S1), not in crboost.

## 7. Lorenz's package (next)

To be scoped when the maintainer shows it. Questions to answer against this roadmap:

- What it adds on top of a plain viewer (picking, segmentation editing, tilt-series review, an annotation format),
  and whether it writes files crboost should pick up. If it does, those files follow the external-picker contract
  (Model B: a scoped dir, a `manifest.json`, a watcher that ingests), the same as ArtiaX. The contract was
  picking_ui/10, and its file needs restoring first.
- Its dependencies (torch? a napari version?), which decide between baking it into `napari_GL.sif` and installing
  it into a bind-mounted environment.
- Whether it needs the server to talk to a running session (switching tomograms, pushing picks). napari has no REST
  server like ChimeraX's `remotecontrol`; a small socket inside the launcher would be ours to write.
- Pick overlays: crboost's centered-Å coordinates converted to napari's ZYX voxel points through
  `services/particles/coords.py`, with the display-bin corner shift undone exactly as `artiax_bridge` does.

## 8. Decisions owed

- **D1** — where napari runs (§2): B recommended; S0 decides whether A is dropped.
- **D2** — a built image or SBGrid/EESSI napari under the GL image's VirtualGL (§3), decided by S0 (2).
- **D3** — whether the Agent API gets a verb for this. Looking stays in the UI (roadmap 42), so the default is no,
  but the standing rule is that an outside API mirrors every UI action. The maintainer decides.
