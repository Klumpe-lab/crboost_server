# Roadmap 65 — EisGreis: the lab's tomogram viewer, with crboost's link as a module of its own

Scoped 2026-11-19; the maintainer answered D1–D8 the same day, and the colleague his open points that evening (§2).
S1–S3 built the same night, with roadmap 61 S3–S4 (§10); uncommitted, the checks of §8 owed. Follows 54 (napari from a
tomogram's chip), 55 (the colleague's viewer as crboost's companion) and 56 (3-D speed). Goes with 61 (the viewer in
the user's own OnDemand desktop), which takes its environment and its command from here.

## Pickup (next session)

S1–S3 and roadmap 61 S3–S4 are built (§10, 2026-11-19, late); nothing is committed. Next:

1. The maintainer commits EisGreis in `~/dev/EisGreis` onto `crboost-bundle-and-3d` (three commits: the renames, the
   package, the install) and pushes that branch; then crboost's commit. The commands were handed over.
2. The lab install, from the lab checkout: `./crboost eisgreis-install /groups/klumpe/software/EisGreis --ref
   crboost-bundle-and-3d`. The lab conf's `napari.companion` becomes `napari.eisgreis: /groups/klumpe/software/EisGreis`,
   with `napari.desktop.hint` (the dev conf has both).
3. The checks: §8 R1–R3 and roadmap 61 R1–R3. GitHub Actions runs on the push.
4. Then the PR to the colleague, `crboost-bundle-and-3d` into `main`, merged with a merge commit: a squash would drop
   the renames that keep his files' history.
5. Nothing goes to EisGreis's `main` from us (D5). Every commit and push is handed as line-by-line commands.

**Why.** The lab looks through tomograms, keeps notes and marks particles in the colleague's napari viewer, and crboost
already drives it (roadmap 55). He has published it as the lab's repository, `Klumpe-lab/EisGreis`, and agreed that we
develop it with him. Two aims, worked on in parallel:

- EisGreis becomes a tomogram viewer that installs and runs on its own: his Mac, a lab PC, an OnDemand desktop.
- crboost gets a link into it that crboost can change without waiting for viewer releases, and that viewer changes
  cannot break unnoticed.

The second aim decides the shape. The link is a module of EisGreis that the viewer never imports, and crboost reaches
it only through versioned files.

## 1. What there is (read 2026-11-19)

**The repository.** `Klumpe-lab/EisGreis` is public and has no licence file. `main` is a072b3f, the colleague's history
from 2026-03-13 (eight commits). It is a copy of his private repository with all its branches:
`feature/fast-viewer-pc-hpc`, and `crboost-bundle-and-3d` (825242d, our bundle and binned 3-D of 2026-11-18).

**The code** is flat scripts, about 4,900 lines:

| File | Lines | What it is |
|---|---|---|
| `napari_mrc_lowpass_fast.py` | 1,365 | `LowPassController`, one class that owns four docks: filter (file list, cutoff in Å, filter type, pixel and box size), comments (per tomogram, CSV), particles (named points layers per tomogram, a STAR each) and export (PNG with a burned-in scale bar). It also holds the measurement layer, the Å scale bar, `load_mrc`, `swap_volume`, `apply_lowpass` and `run_viewer()` |
| `napari_mrc_lowpass.py` | 1,165 | the first, slow viewer, kept unchanged beside the fast one |
| `napari_mrc_lowpass_hpc.py`, `_pc.py` | 127, 50 | launch profiles: CPUs from SLURM or the affinity mask, the FFT cache only with 24 GB or more, the GL renderer with a warning when it is software |
| `shared_filters_fast.py`, `shared_filters.py` | 163, 143 | `LowPassFilter`: one FFT per tomogram, the spectrum cached, hard, gaussian, butterworth and cosine masks, threaded; a bandpass |
| `shared_mrc.py`, `shared_mrc_fast.py`, `shared_star.py`, `shared_mdoc.py` | 72, 114, 114 | MRC by mmap with the header's voxel size, points to and from STAR (Z, Y, X), SerialEM mdoc |
| `napari_tilt_series_excluder.py` | 357 | a tilt-series browser: mdoc order, keep or exclude each tilt |
| `napari_cryolo_bandpass.py` | 252 | a crYOLO-style bandpass widget |
| `crboost_bundle.py`, `view3d.py` | 343, 380 | ours: crboost's picks, segmentations and volumes as reference layers, and 3-D on any renderer (binned copies, segmentations as meshes) |
| `napari_viewer_hpc.sh`, `environment.yml` | | the desktop launcher (it clears `PYTHONPATH`, `PYTHONHOME`, `LD_LIBRARY_PATH` and `QT_PLUGIN_PATH`); conda-forge napari 0.6.6, PyQt 5.15, napari-mrcfile-reader 0.2.0 |

The controller reaches into napari's private modules in four places:

- `napari._vispy.visuals.text`, for a DPI patch;
- `napari_builtins._measure_shapes`;
- `window._qt_viewer` and `window._qt_window`;
- the scale bar's vispy visual, with two patches for its labels.

These are what break between napari versions. MRC files are read with mrcfile directly. napari-mrcfile-reader serves
only napari's own File > Open.

**Our open work on it.** There are 331 lines beyond 825242d, uncommitted. They are identical in the dev clone of his
repository and in the lab copy `/groups/klumpe/software/Napari/Napari_crboost`, which the lab conf's
`napari.companion` names. They hold three things:

- the bundle's volume switch: `swap_volume` puts a denoised volume in the tomogram's place;
- 3-D segmentations as smooth meshes, with merged labels as a dock switch (roadmap 56 S1b);
- two napari 0.10 fixes in the controller. The file and particle lists become callables, which napari re-reads on
  every layer change. A points selection is cleared before a data swap, which otherwise raises IndexError.

**crboost's side.** `containers/napari/open_tomogram.py` (470 lines) runs in our image on a session's VNC desktop. It
has two classes:

- `Session`, plain napari;
- `CompanionSession`, which drives `LowPassController` through its private members (`load_mrc`, `register_path`,
  `loaded_paths`, `comments`, `particle_coordinates`, `default_star_dir`, `annotation_controls.star_dir`, and more).
  It holds one project at a time, saves before a switch, puts outputs into `<project>/Curation/napari/`, shows the
  bundle, and answers in `opened.json`.

The server side:

- the napari section of `services/curation/session_service.py`: `_write_napari_request` (~L768),
  `_read_napari_state`, `launch_napari_session`, `find_active_napari_session` (~L965);
- the bundle builder, `services/visualization/napari_bundle.py`;
- `ui/napari_open_page.py`, `ui/napari_session_dialog.py`, `ui/napari_status.py`, `ui/components/viewer_chips.py`;
- `NapariConfig` (`services/configs/config_service.py:230`, `companion` at :255);
- the napari check in `tests/test_cluster.py`.

**The environment, measured 2026-11-19** (`~/.crboost/eisgreis_probe/`). A uv environment with napari 0.10.0 and
PyQt6 6.12.1 (uv-managed CPython 3.12.13; 715 MB; about 2 min to build cold) starts inside IT's desktop image
(`ood-desktop.sif`: Rocky 9.8, TurboVNC, Mesa llvmpipe). It needs five X libraries on its library path, which the
image lacks:

- `libxcb-cursor.so.0`
- `libxcb-icccm.so.4`
- `libxcb-image.so.0`
- `libxcb-keysyms.so.1`
- `libxcb-render-util.so.0`

Together they are 124 KB (conda-forge's xcb-util builds). Without them Qt aborts: "From 6.5.0, xcb-cursor0 or
libxcb-cursor0 is needed to load the Qt xcb platform plugin". With them, the import takes 0.5–0.8 s. The window plus
a 64 × 512 × 512 layer took 5–9 s on the busy login node.

The environment must use uv's own Python. One built on the host's `/usr/bin/python3.12` does not start in the image,
which has no such file. conda-forge would also work, since its Qt carries these libraries, which is why the
colleague's environment runs there; D4 is uv.

On a full GPU the same environment draws on the card inside IT's image. The image runs under `--nv` with its
GPU-session `LD_LIBRARY_PATH`, and napari runs under the image's own `vglrun -d egl`. The renderer reported "NVIDIA
B200/PCIe/SSE2" (job 1832128, roadmap 61 S0c).

## 2. Decisions

Answered by the maintainer 2026-11-19:

| # | Decision | Answer |
|---|---|---|
| D1 | The viewer's home and name | `Klumpe-lab/EisGreis`, public, his history kept. Distribution and import name `eisgreis`, command `eisgreis` |
| D2 | Licence | BSD-3-Clause, napari's own. The colleague agreed |
| D3 | Where crboost's link lives | In EisGreis, as its own import package `eisgreis_crboost`. `eisgreis` never imports it. crboost's server reaches it only through the session files (§4) |
| D4 | Stack | napari 0.10 + PyQt6, built with uv (`uv.lock`, uv-managed Python), no conda. The install carries the five X libraries, so nothing waits for IT. If IT adds them to its desktop image (roadmap 61 D5), the launcher stops using the carried copies |
| D5 | Turning his scripts into a package | On a branch of EisGreis, never on his `main`: `crboost-bundle-and-3d`, which is ours already, unless the maintainer names another. The restructure is that branch's commits, with `git mv` keeping every file's history. It reaches `main` by one PR the colleague merges. His commits to `main` in the meantime are merged into the branch and ported into the new layout before the PR |
| D6 | How EisGreis gets onto a cluster | Its own install (`/groups/klumpe/software/EisGreis`), which crboost's conf names. crboost's optional step only fetches a release and runs EisGreis's installer, so a viewer fix needs no crboost change |
| D7 | crboost's GPU job route | Kept until 3-D in a desktop is checked (roadmap 61 D3) |
| D8 | Starting with the desktop | Opt-in in User settings (roadmap 61 D4) |

Answered by the colleague 2026-11-19, through the maintainer:

- BSD-3-Clause is fine.
- The napari 0.6.6 pin was arbitrary, so napari 0.10 stands.
- His history is his credit: he is the first commit. No credit line beyond it.
- The restructure needs no day agreed: it lives on a branch until he merges it (D5).

## 3. The shape

```
Klumpe-lab/EisGreis
├── pyproject.toml        eisgreis: napari 0.10, PyQt6, mrcfile, numpy, scipy, magicgui, qtpy
│                         scripts: eisgreis, eisgreis-tilts, eisgreis-crboost
├── uv.lock               one lock for his Mac, a Linux PC and the cluster
├── LICENSE               BSD-3-Clause
├── install.sh            the cluster install (§5)
├── src/eisgreis/         the viewer; imports nothing from eisgreis_crboost
│   ├── api.py            what a driver may use (below); everything else is internal
│   ├── io/               mrc.py, star.py, mdoc.py
│   ├── filters.py        LowPassFilter, bandpass
│   ├── runtime.py        CPUs, memory, the FFT cache, the GL renderer
│   ├── session.py        files, the tomogram on screen, pixel size, comments, particles, output folder (no Qt)
│   ├── docks/            filter.py, comments.py, particles.py, export.py, view3d.py, reference.py
│   ├── reference.py      reference layers: picks, segmentations, other volumes on the tomogram's grid
│   ├── view3d.py         3-D on any renderer
│   ├── scalebar.py       the scale bar, PNG export
│   ├── _napari.py        every use of napari's private modules, in one place
│   ├── tilts/            the tilt-series excluder
│   └── app.py            builds the viewer and its docks; `eisgreis [files…]`
├── src/eisgreis_crboost/ the link
│   ├── protocol.py       the session files: names, versions, reading and writing (no Qt)
│   ├── agent.py          open.json → eisgreis.api → opened.json
│   ├── bundle.py         bundle.json → eisgreis's reference layers
│   ├── desktop.py        desktop.json, the heartbeat, the registry line
│   └── __main__.py       `eisgreis-crboost [--wait] [--session-dir DIR]`
├── tests/                headless (QT_QPA_PLATFORM=offscreen); fixtures/crboost/ holds requests and bundles crboost wrote
└── docs/                 crboost-protocol.md, install.md
```

Three rules keep the two halves apart:

- **`eisgreis` imports nothing from `eisgreis_crboost`.** A test walks the imports, as crboost's `check_boundaries.py`
  does.
- **`eisgreis_crboost` uses `eisgreis.api` only.** A refactor inside the viewer changes the link only when the API
  changes. When it does, the same PR updates the link, and the link's tests show it.
- **The session files are the only contract with crboost_server**, and each carries a version (§4). crboost never
  imports EisGreis, and EisGreis never imports crboost.

`eisgreis` itself stays generic:

- Reference layers take picks, segmentations and volumes from any source: a STAR from RELION or Warp as well as
  crboost's bundle.
- 3-D treats a layer as a segmentation by its metadata, not by who made it.

So `crboost_bundle.py` splits. Its crboost part (the bundle format, its notes) goes to `eisgreis_crboost.bundle`; the
rest becomes `eisgreis.reference`.

**`eisgreis.api`, a first cut.** It makes public what `CompanionSession` reaches today:

- `build(profile=None) -> TomogramViewer` and `run(paths, profile=None, on_ready=None)`.
- `TomogramViewer.open(path, title=None)` (centred, the whole volume in view), `.current` and `.forget(path)`.
- `.set_outputs(folder)`: the comments CSV, the STAR folder and the PNG folder. What it holds is saved into the old
  folders first. Also `.save()`, and `.reload_annotations()`, which reads comments and STARs back from the folder.
- `.reference.show_picks(name, coords_zyx, box_angstrom, color)`, `.show_segmentation(path, level, color,
  voxel_size)`, `.add_volume(name, path, kind)` and `.clear()`.
- `.view3d.set_seg_mode("surfaces" | "merged")`, `.renderer`, and `.napari`, the `napari.Viewer`.

**Where each file goes** (the restructure, S1):

| Today | Becomes |
|---|---|
| `shared_mrc.py`, `shared_mrc_fast.py` | `eisgreis/io/mrc.py` |
| `shared_star.py`, `shared_mdoc.py` | `eisgreis/io/star.py`, `eisgreis/io/mdoc.py` |
| `shared_filters_fast.py`, with the bandpass of `shared_filters.py` | `eisgreis/filters.py` |
| `napari_mrc_lowpass_fast.py` | `session.py` (state), `docks/*` (one per dock), `scalebar.py`, `_napari.py`, `app.py`, `api.py` |
| `napari_mrc_lowpass_hpc.py`, `napari_mrc_lowpass_pc.py` | `runtime.py`, and `eisgreis --profile hpc\|pc` (hpc when SLURM is present) |
| `napari_mrc_lowpass.py` (the slow viewer) | dropped; it stays in the history |
| `view3d.py` | `eisgreis/view3d.py` and `docks/view3d.py` |
| `crboost_bundle.py` | `eisgreis/reference.py` and `docks/reference.py`; the format goes to `eisgreis_crboost/bundle.py` |
| `napari_tilt_series_excluder.py`, `napari_cryolo_bandpass.py` | `eisgreis/tilts/`, and a dock in `docks/` |
| `napari_viewer_hpc.sh` | the install's launchers (§5) |
| `CompanionSession` in crboost's `containers/napari/open_tomogram.py` | `eisgreis_crboost/agent.py`; the plain `Session` goes |

## 4. The crboost protocol, version 1

The session folder is `~/.crboost/napari/<id>/`. The home directory is the one place the headnode and every desktop
share. Every file is written beside its final name and renamed into place.

- **`open.json`**, crboost → viewer. Today's fields as `_write_napari_request` writes them, plus `protocol`:
  - `seq`: a clock in ms; a higher one replaces the request before it;
  - `title`;
  - `layers`: `[{name, path}]`, of which the first is opened;
  - `outputs`: the project's `Curation/napari/`, or null;
  - `bundle`: the path of `bundle.json`, or null.
- **`bundle.json`**, crboost → viewer. `format: crboost-napari-bundle`, `version: 1`, as the docstring of
  `crboost_bundle.py` describes it. Built by `services/visualization/napari_bundle.py`.
- **`opened.json`**, viewer → crboost: `seq`, `ok`, `error`, `layers` (`[{name, path, shape, voxel_size,
  shape_3d}]`), `renderer`, `viewer`, `outputs`, and `bundle` (what was drawn and what was left out).
- **`desktop.json`**, viewer → crboost, new:
  - `protocol: [1]` and `bundle_versions: [1]`;
  - `slurm_job_id`, `node`, `display`, `pid`, `started`;
  - the `eisgreis` and `napari` versions, and `renderer`.

  The viewer touches `alive` every 10 s, and both files go when it exits. It also appends a line to
  `~/.crboost/napari/registry.jsonl`: `{kind: "desktop", slurm_job_id, session_dir, started}`.
- **No project.** A request without `outputs` (a volume outside every project) saves into the session folder's
  `outputs/`, never the working directory (roadmap 63's H32).

**Versions.** In `desktop.json` the viewer says which protocol and bundle versions it reads, and crboost writes the
newest one both know. A side that meets a version it does not know says so in the UI and never falls back silently.
In crboost's dialog that reads: "EisGreis <version> at <install> reads protocol 1, this crboost writes 2: update
EisGreis". EisGreis's `docs/crboost-protocol.md` is the reference, and crboost's writers cite it. A format change in
crboost comes with a PR to EisGreis that adds the new fixture to `tests/fixtures/crboost/` and the version the link
reads.

## 5. The environment and the install

- **Development.** `uv sync` and `uv run eisgreis file.mrc`, the same on his Mac and on Linux. `uv.lock` is committed.
- **The cluster install** (`install.sh`, D6) goes into `/groups/klumpe/software/EisGreis/`, on the pattern of crboost's
  roadmap 39 S6:
  - a pinned uv fetched into the install;
  - uv-managed Python, never the host's;
  - the environment built into `.uv/env/<python>-<hash of uv.lock>` and never modified afterwards, so a running
    viewer keeps its environment through an update;
  - the five X libraries in `xlibs/`, fetched by pinned URL and sha256, with no conda.

  Updating is `git pull` plus `install.sh`, as he does today. When crboost's release folders arrive (roadmap 63 D2),
  EisGreis can take the same layout.
- **The launchers**, `eisgreis` and `eisgreis-crboost`, sit at the install's top. They are sh scripts that:
  - clear `PYTHONPATH`, `PYTHONHOME` and `QT_PLUGIN_PATH`, as his `napari_viewer_hpc.sh` does;
  - filter `LD_LIBRARY_PATH` rather than clear it. They keep a GPU desktop's GL arrangement (its `glvnd` folder and
    `/.singularity.d/libs`) and drop the rest, such as a module's paths. Cleared outright, it would leave the NVIDIA
    libraries unfound (roadmap 61 S0c);
  - put `xlibs/` on `LD_LIBRARY_PATH` only when the system lacks one of the five (`ldconfig -p`);
  - exec the environment's script;
  - on a full GPU (no MIG device), run it under `vglrun -d egl` (roadmap 61 S5).
- **crboost's optional step.** `./crboost eisgreis-install [dir]` clones a release tag into the folder, runs its
  `install.sh`, and prints the conf line (`napari.eisgreis: <dir>`). That is all crboost knows about installing
  EisGreis.

## 6. Toward a capable tomogram viewer

Parity comes first: S1 changes no behaviour except the napari 0.10 port. After it, in the order the colleague and the
maintainer choose (owners open):

- **The window never freezes on open.** Opening a tomogram reads the whole volume and its FFT on the Qt thread (~20 s
  on 4 CPUs, roadmap 55). Instead, show the memory-mapped slice at once, low-pass in a worker thread (napari's
  `thread_worker`), and swap the result in when it is ready.
- **Slabs.** A slab thickness in Å beside the low-pass, using napari 0.10's `projection_mode="mean"` with
  `dims.thickness`.
- **Picks in more conventions.** Today's STAR is Z, Y, X in pixels. Add RELION 5's tomography columns
  (`rlnCenteredCoordinate{X,Y,Z}Angst`, `rlnTomoName`) for reading and writing, and open any STAR of picks as a
  reference layer.
- **A napari plugin.** An npe2 manifest puts the docks in any napari's Plugins menu, with the session found per
  viewer. It also gives a reader for `.mrc`, `.rec` and `.st` that takes the header's voxel size, so
  napari-mrcfile-reader can go.
- **Tilt series in the same window**, with the excluder as a dock.
- **3-D:** a plane or slab of the volume in 3-D (napari's `depiction="plane"`), at the binned level.
- **Releases:** CalVer tags as crboost's (roadmap 63 D1), and a changelog. PyPI once it is stable.

## 7. Stages

**S0 — before any code (the maintainer).**

- a. **Done 2026-11-19.** The colleague's answers (§2).
- b. Put our open work onto EisGreis. The 331 lines are committed on `crboost-bundle-and-3d` and pushed to that
  branch, not to `main` (commands in the hand-over).
- c. *Optional, not blocking.* Ask IT to add the five X libraries to `ood-desktop-image` (roadmap 61 D5).

**S1 — the restructure** (D5), on the work branch, in a clone at `~/dev/EisGreis`. *Built 2026-11-19 (§10).*

- `git mv` into the layout of §3, one module at a time as the table says, then the splits. The controller becomes
  `session.py` (state, no Qt) and one module per dock, behind `api.py`.
- The napari 0.10 port:
  - particle layers drawn out of the slice by `projection_mode` and slice thickness, since `out_of_slice_display`
    no longer draws at thickness 1;
  - centring on open;
  - the file and particle lists as callables;
  - every use of napari's private modules moved into `_napari.py`, then checked against what napari 0.10 does
    itself (its scale bar takes units). Each one napari covers is dropped.
- `pyproject.toml`, `uv.lock`, `LICENSE`, and a README with the uv install and run, crediting him as he wants.
- `eisgreis_crboost`:
  - `CompanionSession` moved and rewritten on `eisgreis.api`;
  - `desktop.json`, the heartbeat and the registry line;
  - the no-project fallback of §4;
  - `docs/crboost-protocol.md`.
- Tests:
  - io: MRC voxel size, the STAR round trip, mdoc;
  - filters: the masks, the cache;
  - session: the comments CSV, per-tomogram particles, the output folder;
  - the link, on fixtures: a request opens the tomogram; a project switch saves into the old folder first; an
    unknown protocol version is refused with a message;
  - the import boundary.
- GitHub Actions on the free runners: `uv sync`, ruff, pytest offscreen.
- Not in S1: new features.

**S2 — the install** (§5): `install.sh`, the launchers and `xlibs/`, and the lab install at
`/groups/klumpe/software/EisGreis`. The old copies under `/groups/klumpe/software/Napari/` stay until R4. *Built
2026-11-19 (§10); the lab install waits for the push (Pickup 2).*

**S3 — crboost points at EisGreis** (crboost repo). *Built 2026-11-19 (§10).*

- conf `napari.eisgreis: <install>` replaces `napari.companion`. `NapariConfig` changes, and preflight checks that the
  install's launchers exist, that `eisgreis-crboost --version` answers, and which protocol it reads. A conf that
  still names `napari.companion` gets a warning naming the new key.
- The GPU job route (kept, D7) runs `<install>/eisgreis-crboost --session-dir …` in our VNC image instead of the
  image's napari (`curation_session.sh`, `CB_APP=napari`). `containers/napari/open_tomogram.py` and
  `launch_napari_vnc.sh` go. The image keeps only the desktop: Xvnc, the window manager, VirtualGL and websockify.
- `open.json` gains `protocol`, and `_write_napari_request` writes the version `desktop.json` asks for. The job
  route's viewer writes `desktop.json` too, so both routes agree on a version the same way.
- `./crboost eisgreis-install`, and `tests/test_cluster.py` checks the install's environment.
- Roadmap 61 S3–S4 (crboost finds the desktop, autostart) build on this.

## 8. Checks (the maintainer)

- **R1** — `eisgreis` from the lab install in an OnDemand desktop, on a tomogram: low-pass, comments, particles, PNG
  and 3-D, as before the restructure.
- **R2** — the colleague's own use on his Mac, with `uv run`.
- **R3** — crboost's chip with EisGreis on the GPU job route: the tomogram, the bundle, and a project switch that
  saves into the old project's `Curation/napari/`.
- **R4** — the lab's old copies under `/groups/klumpe/software/Napari/` retired once R1–R3 pass.

## 9. Not here

- The OnDemand serving: finding the desktop, autostart, GPU drawing in a desktop, retiring the job route (roadmap 61).
- Viewer features beyond §6's list, and their order.
- PyPI and napari hub.
- ChimeraX.
- Roadmap 63 publishes the napari image in December "as it is". After S3 that image needs no napari, and roadmap 61
  S6 may retire it altogether. That roadmap settles it.

## 10. Records

- **2026-11-19.** Scoped. The facts of §1 come from reading EisGreis, the dev clone and the lab copy, and from the
  probe in `~/.crboost/eisgreis_probe/`:
  - the uv environment and the five X libraries;
  - napari started in IT's image on TurboVNC, with a screenshot (`shot_libs2.png`);
  - the same on a full B200 under `vglrun` (`s0c.sh`, job 1832128, `shot_gpu.png`).

  No EisGreis or crboost code changed.
- **2026-11-19, evening.** The colleague's answers came in: BSD-3 is fine, the 0.6.6 pin was arbitrary, and his
  history is his credit. D5 became branch-only: nothing of ours goes to his `main` until he merges a PR. The X
  libraries are carried by the install, so IT's part is optional. Pickup section added.

- **2026-11-19, late. S1–S3 built**, with roadmap 61 S3–S4. The branch head was 7504c51 (our 331 lines on 825242d;
  `main` still a072b3f, so nothing of the colleague's to merge). Clone at `~/dev/EisGreis`; nothing committed.
  - **S1.** The layout of §3: `src/eisgreis/` (`app.py` the controller, `session.py` its state without Qt, `docks/`
    one module per dock, `io/`, `filters.py`, `runtime.py`, `reference.py`, `view3d.py`, `scalebar.py`, `_napari.py`,
    `tilts/`, `api.py`) and `src/eisgreis_crboost/` (`protocol.py`, `agent.py`, `bundle.py`, `desktop.py`,
    `__main__.py`). `pyproject.toml` (uv_build, both packages in one distribution), `uv.lock` (124 packages:
    napari 0.10.0, PyQt6 6.12.1, as the probe ran), `.python-version` 3.12.13, `LICENSE` (BSD-3, "the EisGreis
    contributors"), README without a credit line, the colleague's `.codex` skill moved to the new layout.
  - **The napari 0.10 port found two faults** the 331 lines still had on 0.10: the scale bar never showed (napari
    renamed `_overlay_to_visual`, so the patch never found its visual), and a tomogram opened at slice 2 of 32 (the
    slider keeps its world position when the scale changes). Both fixed; the slice is centred on every open.
  - **What napari 0.10 covers, and what it does not.** Its text visual has the DPI fallback, so that patch went. Its
    scale bar takes the layers' unit but prints Å compacted ("1.2 kÅ"), so the nm label stays: one patch of the
    overlay's `_calculate_best_length`, per viewer, in `_napari.py`. `out_of_slice_display` now means
    `rescale_linear`, which shrinks a point by the slab's margin, not its own size: particle layers use
    `rescale_spherical` like the reference picks, and the Z slab is at least the largest box. Shapes ignore the slab,
    so measurements stay in their plane. Docks take no title from the widget any more ("Dock widget 1"): names are
    passed.
  - **Where S1 differs from §3.** `eisgreis-bandpass` is a fourth command (the crYOLO-style widget was its own
    script). The tilt excluder's low-pass is `LowPassFilter` (same masks, the full-grid filter dropped).
    `napari-mrcfile-reader` stays a dependency for napari's own File > Open; the viewer reads with mrcfile. The
    crboost dock is the generic *Reference* dock. The API grew what the agent needed: `register`, `tomograms`,
    `outputs`, `shape`, `pixel_size`, `maximize`, and reference calls take `tomogram=` and `pixel_size=` (a
    reference set is kept per tomogram, drawn while it is on screen). `desktop.json` carries `window` (false while a
    `--wait` viewer has no window). `eisgreis-crboost --version` prints JSON without starting Qt; `--check` loads
    napari and Qt offscreen.
  - **Tests**: 77, run in IT's image on a private TurboVNC display (offscreen Qt, OpenGL through it): io, filters
    against the full-grid filter, session, the import boundary, protocol and `desktop.json`, the viewer on a napari
    window (centring, nm scale bar, particles per tomogram, recalibration, volume swap, reference layers, 3-D
    surfaces, dock names), and the link on fixtures crboost's own writers made (`scripts/eisgreis_fixtures.py` in
    crboost): a request opens its tomogram, a project switch saves into the old folder first and reads back, a bundle
    shows, outside every project saves into the session folder, protocol 2 and a request without one are refused.
    GitHub Actions: uv sync, ruff, pytest under xvfb.
  - **The rename commit.** The files were moved in the working tree, so the renames-only commit is built in the index
    from HEAD's blob ids (`git update-index --cacheinfo`), then the package commit on top. The controller's file
    becomes `app.py`, which holds its successor (`Controller`), so `git log --follow src/eisgreis/app.py` walks back
    into `napari_mrc_lowpass_fast.py`; `napari_viewer_hpc.sh` becomes the launcher `eisgreis`.
  - **S2.** `install.sh` (uv 0.13.7 fetched into `.uv/bin`, uv's Python, `.uv/env/3.12.13-a4edcf912300`, built in
    about a minute; EisGreis installed editable), `launch.sh` sourced by `eisgreis` and `eisgreis-crboost`. The
    X libraries: conda-forge's `xcb-util-wm-0.4.2-hb711507_0`, `-image-0.4.0-hb83e432_3`, `-keysyms-0.4.1-hb711507_0`
    and `-renderutil-0.3.10-hb83e432_1` give the probe's copies byte for byte; `xcb-util-cursor-0.1.6-hb03c661_0`
    is the probe's version and compiler (its copy differs in 527 bytes). Pinned by URL and sha256.
  - **S3** (crboost). `napari.eisgreis` replaces `napari.companion` (a conf naming the old key is told the new one),
    `napari.desktop.form` and `.hint` for 61 S3. The job route runs `<install>/eisgreis-crboost --session-dir` in the
    image's desktop (`CB_EISGREIS`, the install bound), its launcher deciding VirtualGL; `containers/napari/` is gone.
    `open.json` carries `protocol`, the newest the viewer's `desktop.json` reads, or before it runs the install's
    `--version` (`services/curation/napari_link.py`); no shared version is an error naming the side to update.
    Preflight checks the install and the protocol; `./crboost eisgreis-install`; the cluster test runs `--check` in
    the session image. The dev conf names `~/dev/EisGreis`, installed in place.
  - **Checked end to end** (`~/.crboost/eisgreis_test/e2e/`, a test registry, never the live one): EisGreis by its
    launcher in IT's desktop image (carried X libraries, llvmpipe), found by crboost's service as a desktop session,
    a tomogram with crboost's picks and segmentation shown in 21 s from start, a second project in 2 s with the
    first's CSV saved before the switch, SIGTERM removing `desktop.json` and `alive`. The same install in
    `napari_GL_v2.sif` with `--session-dir` (the job route; the image's own X libraries, `/.singularity.d/libs`
    kept). On a full B200 under `--nv` in IT's image with a GPU desktop's library path (job 1833014): the launcher
    dropped a stray module path, kept `glvnd` and `/.singularity.d/libs`, ran `vglrun -d egl`, and the viewer drew on
    "NVIDIA B200/PCIe/SSE2". Not done here: the job route through crboost's sbatch (R3), a user's own desktop through
    the portal (61 R1), the colleague's Mac (R2).
