# ArtiaX manual-picking bridge — design plan

Status: **END-TO-END WORKING on GPU (2026-06-07): one-click launch → `.cxc` preloads tomo + auto picks in
ArtiaX → manual pick → save `.coords` → crboost reads it back in-register (5 real human picks confirmed).**
The unlock was a scipy/el7 fix, not UX (see "## SESSION 2026-06-07 — END TO END WORKING" below). Curation
dialogs consolidated into one status-first control center; container relocated to
`/groups/klumpe/software/containers/{defs,sifs}`. Remaining: ingest UI/registry + auto-watch; the multi-list
pick workbench; the Log-panel (gray) + VNC-fidelity peeves. This doc is the contract.

## NEXT SESSION — start here (prioritized)

The feature WORKS end to end; everything below builds OUT from a working base. In priority order:

1. **Ingest UI + registry — closes the round-trip in the UI.** The converter is done
   (`artiax_bridge.import_coords_to_centered_star`); wire the UI: an "Import" button that ingests ANY saved
   `.coords` in the session/bundle dir (NOT just `<tomo>__manual.coords` — the user saved `particles.coords`),
   runs the converter → `ManualPicks/<species>/<tomo>.star`, and registers a `manual` `PickList`. Spec:
   "## Curation workbench — the multi-list model → Build order" slice 4 + "## Post-launch curation workflow
   roadmap → Ingest pipeline".
2. **The multi-list pick workbench — the big remaining feature.** Slices 3b→6: wire the recon-cutout atlas as
   the gallery fallback for un-extracted lists → per-list contact sheet → "Open in ArtiaX" loads a CHOSEN list →
   filter (CC/top-N) → merge selected → `_combined` sibling + the `resolve_canonical_optset` tier (zero driver
   changes downstream). Slices 1+2+3-enabler already landed. Spec: "## Curation workbench — the multi-list model
   → Build order / Progress".
3. **Curation UX polish — overhaul items 2-3.** ONE co-located `Curation/<species>/<tomo>/` dir (auto/manual/
   imported `.coords` + `.cxc`) replacing the scattered `.curation_sessions/bundles/` + ManualPicks split; a
   per-(species,tomo) file overview; promote the "Curate in ArtiaX" button to a primary action with an inline
   live-session status dot. Spec: "## Curation UX overhaul" items 2-3 (item 1 = the control center = DONE).
4. **Optional / when motivated:** REST one-click load (paste-in → POST to the running ChimeraX,
   "## Dispatch commands to ChimeraX from crboost (REST)"); VNC fidelity (TurboVNC server swap in the def);
   Log panel (QtWebEngine on el7 — deprioritized, `slurm.log` covers debugging).

Env + locations: Claude's venv has NO numpy/mrcfile/starfile/pandas → UI/star-IO/cutout work is py_compile+ruff
only, runtime-test in the user's module-loaded app. The container def is now at
`/groups/klumpe/software/containers/defs/chimerax_artiax_GL.def` (edit THERE, not the repo); the GL sif at
`/groups/klumpe/software/containers/sifs/chimerax_artiax_GL.sif` (conf.yaml points at it); worker scripts stay
in the repo (`containers/chimerax_artiax/curation_session.sh`, launched by `backend` by path).

## SESSION 2026-06-07 — END TO END WORKING (the breakthrough + learnings)

The whole feature works on a live GPU session. The build order that got us here + what we learned:

1. **GPU one-click** — `CurationConfig` gained `gres`/`vgl`; `backend.launch_curation_session` emits
   `#SBATCH --gres=` + `export CX_VGL=1`; conf.yaml → partition g / gres gpu:1 / vgl true / the `_GL.sif`.
   The session lands on a P100 and renders fluidly.

2. **THE UNLOCK — scipy on el7 (not a UX bug).** `artiax` commands silently did nothing (while `open 4ug0`
   worked) because ChimeraX 1.9's bundled scipy fails to load on CBE's el7 kernel:
   `ImportError: libscipy_openblas-*.so: ELF load command address/offset not page-aligned`. ArtiaX imports
   scipy lazily on first command → ImportError → vanishes into the broken Log panel → "nothing happens." This
   is a SIBLING of the ABI-tag trap but a different ELF check (segment alignment), so the objcopy strip never
   covered it. **Fix = a different wheel, not ELF surgery:** manylinux2014 `scipy==1.11.4` (numpy 1.26.4
   unchanged), pinned in the def AFTER the objcopy sweep (fresh el7 wheels must NOT be stripped), with a
   build-time `import scipy.linalg` assertion. Proven headlessly before the rebuild. See
   `reference_cbe_kernel_abi`. **General rule: prefer manylinux2014 wheels on this cluster.**

3. **UX consolidated.** Three dialogs → one status-first `open_curation_control_center(backend, project_path,
   *, bundle=None)` (replaces open_curation_session / commands / chooser). Live → the FULL connect block (ssh
   tunnel + VNC address + password, all copyable — the piece users never saved) + framed "load this tomogram"
   steps + Stop. Off → Start (preloads the tomo). Stop ≠ relaunch and Start/Stop are busy-guarded → the old
   double-submit (two GPU sessions) is gone. Both the gallery button and sidebar route here.

4. **PROVEN loop.** Live 412 session: preload → place manual picks → save `particles.coords` → crboost reads
   5 well-formed corner-Å picks. The coordinate contract holds with real human picks, in-register.

5. **Container relocated** to `/groups/klumpe/software/containers/{defs,sifs}` (def + GL sif); CPU def retired;
   conf.yaml points at the /groups GL sif. **Edit the def THERE now**, not the repo (the worker scripts stay in
   the repo — they're crboost code crboost launches by path).

**Open peeves (non-blocking):**
- **Log panel still gray** — `libgbm1 libasound2 libxshmfence1` were NOT enough for QtWebEngine on el7; it
  likely also needs `--no-sandbox` / more libs / a real `/dev/shm`. Non-essential (command errors still land
  in the session `slurm.log` — that's how we found the scipy bug). Deprioritized.
- **VNC fidelity ("like 480p video"), worst on the tomogram** — the Xvnc is `-depth 24` (correct), so this is
  CLIENT-side: use a real VNC viewer (TigerVNC/TurboVNC) at **full colour + max/lossless quality + 1:1**, NOT
  macOS Screen Sharing (adaptive-lossy + scales the desktop). Over the localhost ssh tunnel lossless is free.
  Durable server-side fix = swap TigerVNC → **TurboVNC** in the container (the canonical VirtualGL pairing).
- **Ingest UI not wired** — the converter exists (`artiax_bridge.import_coords_to_centered_star`); needs an
  "Import" button + the `PickList` registry + a watch that ingests ANY saved `.coords` (the user named theirs
  `particles.coords`, not `<tomo>__manual.coords`). This is the next build, then the multi-list workbench.

## GPU/VirtualGL acceleration — LANDED (2026-06-06)

The software-GL appliance was unusable for 3-D inspection (3-5 s per camera move = Mesa `llvmpipe`
rasterizing on a CPU node). A GPU + VirtualGL variant now renders **fluidly on a P100** ("perfectly
usable"). Full hard-won diagnosis + recipe in memory `reference_chimerax_vnc_perf`. What shipped, all
in `containers/chimerax_artiax/` (one worker drives both the CPU and GPU sifs):

- **`chimerax_artiax_GL.def`** (built → `chimerax_artiax_GL.sif`) — the CPU def minus the software-GL
  forcing, + VirtualGL 3.1.4 (ABI-tag-stripped like the ChimeraX libs — `libvglfaker` is LD_PRELOAD'd,
  same el7 trap), + **the keystone:** bakes `/usr/share/glvnd/egl_vendor.d/10_nvidia.json`. apptainer
  `--nv` injects the NVIDIA EGL *libs* but NOT the GLVND vendor config, so without it libEGL sees only
  Mesa → probes cgroup-denied `/dev/dri` → falls to llvmpipe. That `/dev/dri ... Permission denied`
  spam at startup is the tell; its absence = NVIDIA EGL in use.
- **`curation_session.sh`** — `CX_VGL` set ⇒ `apptainer exec --nv --writable-tmpfs … vglrun -d egl
  chimerax`, pins `__EGL_VENDOR_LIBRARY_FILENAMES` at the json. `--writable-tmpfs` lets ChimeraX write
  `preregistration`/history (the baked `/opt/cx` is RO) without erroring. Unset ⇒ software path, unchanged.
- **`launch_curation_vnc.sh g`** ⇒ `CX_VGL=1` + `--gres=gpu:1` + finite `--time` (GPU QOS rejects
  `--time=0` with `QOSMaxWallDurationPerJobLimit`).
- **Residuals (cosmetic, non-fatal, both variants):** the `Log`/`ChimeraXHtmlView` error (QtWebEngine
  missing `libgbm1`/`libasound2`/`libxshmfence1`; Log panel only, ArtiaX fine) — add those libs in a
  rebuild if the Log panel is wanted. **Still un-validated:** the actual ArtiaX *tomogram → pick → save
  `.coords`* loop on the GPU sif (only `open 4ug0`, a PDB model, has been rendered so far).

## Plug the GPU container into the manual-picking infrastructure

Goal: the gains above + the existing curation infra (config-driven launch, the connect dialog, the
`CB_CXC` preload hook) converge so the crboost one-click button opens a **GPU** session **preloaded**
with the right tomogram + picks, the user curates, and the picks flow back in.

**✓ Items 1-2 LANDED (session 2026-06-07).** `CurationConfig` gained `gres: Optional[str]=None` + `vgl: bool=False`
(`config_service.py`); `backend.launch_curation_session` emits `#SBATCH --gres={cur.gres}` when set + `export CX_VGL=1`
when `vgl` (spliced as precomputed `gres_line`/`vgl_export` so the literal chain stays intact; mirrors
`launch_curation_vnc.sh g`; `get/stop_curation_session`, `find_active_curation_session`, and the connect dialog are
unchanged — liveness still keys off `-J cb-curation`). conf.yaml `curation:` now defaults to the GL appliance
(`partition: g`, `gres: gpu:1`, `vgl: true`, finite `time`). **Caveat:** `sif_path` points at the **repo** copy
`containers/chimerax_artiax/chimerax_artiax_GL.sif` (on /users, GPU node mounts it) — the GL sif is NOT yet in
`/groups/klumpe/software/containers/sifs/`; `cp` it there + repoint for the canonical shared home (`CX_SIF` still
overrides). Verified off-cluster: py_compile + ruff clean, `Config` parses conf.yaml + template, the rendered GPU
sbatch carries `-p g`/`--gres=gpu:1`/`CX_VGL=1` while the CPU template carries neither. **GATE (unchanged):** the
actual GPU tomo→pick→save `.coords` loop is still un-run — only `open 4ug0` has rendered on the GL sif.

Build order:

1. **GPU knobs in `CurationConfig`** (`services/configs/config_service.py:108`) — add `gres: Optional[str]
   = None` and `vgl: bool = False` (partition + `time` + `sif_path` already exist; `time` already
   defaults to the finite `08:00:00` the GPU QOS needs). conf.yaml `curation:` points `sif_path` at the
   `_GL.sif`, sets `partition: g`, `gres: "gpu:1"`, `vgl: true`.
2. **Wire them through `backend.launch_curation_session`** (`backend.py:175`) — the sbatch wrapper adds
   `#SBATCH --gres={cur.gres}` when set, and `export CX_VGL=1` when `cur.vgl`. (The manual
   `launch_curation_vnc.sh g` already does exactly this; this just mirrors it on the sbatch path.) No
   change to `get_curation_session_info`/`stop_curation_session`. The connect dialog is unchanged.
3. **Preload — kill the blank session** (the #1 usability gap). **✓ MECHANISM LANDED (session 2026-06-07).**
   `.cxc` generator + bundle prep in `artiax_bridge.py` (`build_session_cxc` / `session_chimerax_commands` /
   `prepare_curation_bundle`); `backend.launch_curation_session(project_path, cxc_path=…)` passes `CB_CXC`;
   `backend.prepare_curation_bundle(...)` thin wrapper; per-(species,tomo) **"Curate in ArtiaX" button** in the
   gallery (`_render_species_tab_header` → `_handle_curate_in_artiax`). For a live session it shows the **real**
   `open …` commands (Tier-1, `open_curation_commands_dialog`); for none it launches preloaded. **The remaining
   work is the UX overhaul + REST auto-dispatch below — see "## Curation UX overhaul".** GATE: NOT validated in
   ChimeraX yet (do the auto picks land un-shifted? if not, add a px cmd to `session_chimerax_commands`).
4. **Single-session reuse** — `ensure-session`. **✓ LANDED (session 2026-06-07):** `find_active_curation_session`
   (squeue-derived liveness) + persisted `job.json`; sidebar button is two-state (Reconnect / Stop & relaunch).
   **Remaining:** enqueue-next-tomo into a live session's manifest (the blitz) — not built.
5. **Ingest + the multi-list workbench** — saved `.coords` → `PickList` (manual/imported) + registry →
   the per-tomo card (slices 4-6 of "## Curation workbench"). **Converter ✓ LANDED (session 2026-06-07):**
   `artiax_bridge.import_coords_to_centered_star` (+ `import` CLI). **Remaining:** the UI "Import" button, the
   `PickList` registry wiring, and the manual-save watch path. This is where the GPU session and the workbench
   meet: curate in the fast ArtiaX session, the result lands as an authoritative list downstream.

Dependencies: 1→2 are small and unlock GPU one-click; 3's mechanism is done — its **UX + REST dispatch** (below)
is now the highest-value win; 5 is the existing workbench plan. None require a container rebuild (all
crboost-side) except the optional QtWebEngine libs.

## Curation UX overhaul — the "Curate in ArtiaX" control center (NEXT SESSION, user-driven 2026-06-07)

The per-(species,tomo) **"Curate in ArtiaX"** button now works end to end (export picks → `.cxc`/bundle → live
session shows paste-in `open` commands, or none launches preloaded). **But the connective tissue is raw**
(direct user feedback). Concrete gaps + the fixes:

**✓ Item 1 + the connect-info fix + bug-fix LANDED (session 2026-06-07, user-driven).** `ui/curation_session_dialog.py`
rewritten to ONE status-first **`open_curation_control_center(backend, project_path, *, bundle=None)`** that replaces
all three old dialogs (`open_curation_session_dialog` / `open_curation_commands_dialog` / `open_curation_chooser_dialog`).
Both entry points now route to it — the sidebar (`pipeline_builder_panel.launch_curation_session`, no bundle) and the
gallery (`tomo_dashboard._handle_curate_in_artiax`, bundle=this tomo). It always opens showing **session status**, and:
**live** → the FULL connection block (ssh tunnel + VNC address + one-time password, all copyable — *the missing piece:
the per-tomo live path used to show only paste-in commands, so a user who never reconnected via the sidebar had no
tunnel/password*) **plus** the framed "load this tomogram" steps (① viewer · ② click cmd line · ③ paste) + the
`.coords` save target; **off** → a **Start** button (preloads the tomo) that polls → transitions into the live view,
with the load commands available in an expansion even before launch. **Double-submit bug fixed:** the old
chooser's *Stop & relaunch* had no guard (the logs showed it scancel twice + sbatch two GPU sessions); Stop now just
scancels → returns to the off state (Start is a separate click), and both Start/Stop are re-entry-guarded
(`state["busy"]`, set synchronously before the first await). py_compile + ruff clean (no F821); the dialog's runtime
render/poll/guard behavior is the usual live-session gate. **Still open from this section:** item 2 (per-(species,tomo)
file overview + ONE co-located `Curation/<species>/<tomo>/` dir — bundle still lands in `.curation_sessions/bundles/<species>/`)
and item 3 (promote the button to a primary action with inline live-status). The REST dispatch below would turn the
paste-in "load" step into a true one-click.

Concrete gaps + the fixes:

1. **Make the popup a session-aware control center, not a command dump.** Today the dialog assumes a session is
   already live and just lists commands. It should ALWAYS open showing, top-to-bottom:
   - **Session status** (from `find_active_curation_session`): live (node N + a **Reconnect** affordance) /
     starting / **off**.
   - If **off** → a **"Start session"** button right in this dialog (launch preloaded with THIS tomo's `.cxc`),
     so the user never has to go hunt the sidebar button first. (Wire to the same launch path; pass `cxc_path`.)
   - If **live** → the load action. Until REST dispatch lands (next section) this stays paste-in, but **framed
     as steps**, because the "these go into ChimeraX" context is currently missing entirely: "① switch to your
     VNC viewer · ② click the ChimeraX command line at the bottom of its window · ③ paste:" + the copy button.
   - Always → the **file overview** (item 2) + the manual-save target (already shown).
2. **Per-(species,tomo) file overview + ONE co-located curation dir.** The user shouldn't reason about scattered
   paths. Show the files for THIS (species, tomo) — PyTOM auto picks (`candidates.star` / exported `auto.coords`),
   the recon, any manual/imported lists, the manual-save target — each copyable, each with on-disk status
   (exists · size · mtime). **Co-locate them:** today the bundle lands in `.curation_sessions/bundles/<species>/`
   while ManualPicks land elsewhere — converge on ONE dir per (species, tomo) (e.g. `Curation/<species>/<tomo>/`
   holding `auto.coords`, `manual.coords`, imported lists, the `.cxc`) so "browse what's here" is a single
   folder. This doubles as the input set for the multi-list workbench (one file ⇒ one `PickList`).
3. **Button discoverability.** The button is lost in the tab-header essentials row among gray stats + the
   "sizes & 3dmod" expansion + the render/IMOD admin icons. Promote it to a clearly-separated **primary action**
   in the species tab (its own row/area), ideally with the live-session status inline (e.g. "Curate in ArtiaX
   ● live on c2-11" vs "○ no session").
4. **Gallery panel UX (DEFERRED — user-flagged for a later dedicated pass).** The gallery already carries a lot
   of interaction (per-list toggles, dot↔cutout brushing, keep/drop). The user explicitly wants this left for
   later; placeholder here so it's not lost.

## Dispatch commands to ChimeraX from crboost (REST `remotecontrol`) — INVESTIGATE, biggest UX unlock

The paste-in step exists ONLY because crboost can't talk to the running ChimeraX. It very likely CAN, and if so
"Load this tomogram" becomes a real one-click: crboost POSTs `artiax open tomo <recon>; open <auto.coords>` and
the tomo appears in the user's open viewer — **no paste-in at all.** ChimeraX ships
`remotecontrol rest start port <N> json true`: an HTTP endpoint inside ChimeraX that runs arbitrary ChimeraX
commands (`…/run?command=open+<path>`).

Feasibility — the reachability is the crux and it's promising:
- **The same network path that makes VNC work makes REST work.** VNC works via `ssh -L rfbport:<node>:rfbport
  user@login`, i.e. the login/headnode can already reach the compute node's ports; crboost runs there and
  `session.json` already records `node`. So crboost should hit `http://<node>:<rest_port>/` like VNC does.
- **Caveat:** ChimeraX `remotecontrol rest` historically binds **127.0.0.1** on the node for safety, so a direct
  headnode→node:port GET may be refused. Two robust options: **(a)** run the curl ON the node —
  `ssh <node> "curl -s 'localhost:<rest_port>/run?command=…'"` (the headnode can ssh to a node where the user has
  a running job); **(b)** check whether this ChimeraX build can bind the REST server to a chosen address/all
  interfaces, then reach it directly like VNC. Spike both; whichever connects decides the design.
- **No native auth** on the REST server → use a high random port + the localhost-bind + ssh-hop (a) so it's
  unreachable off-node; record `rest_port` in `session.json` next to the VNC port.
- **Wiring:** worker bakes `remotecontrol rest start port <rand> json true` into startup / the `.cxc`;
  `session.json` gains `rest_port`; `backend.send_chimerax_commands(session, cmds)` POSTs (direct or via
  `ssh <node> curl`); the per-tomo **"Load in running session"** button calls it; **paste-in stays as the
  graceful fallback** when the POST fails (session died / unreachable).
- **Unlocks beyond load:** the same channel drives the Tier-3 blitz (crboost sends `next/prev` to walk a
  manifest) and can read state back (`info models` JSON) to confirm a save. Cleaner than the baked `crboost`
  ChimeraX bundle for *gallery-driven* control; the bundle is still better for hands-in-viewer keyboard nav —
  they compose.
- **Recommendation:** spike it FIRST next session — start ChimeraX with `remotecontrol rest`, from the headnode
  try direct `http://<node>:<port>` and `ssh <node> curl localhost:<port>`, POST a trivial `open 4ug0`. If it
  connects, the paste-in popup collapses into a one-click "Load," the single biggest UX win left.

## Switch the GL container to DEFAULT (it is now the main appliance)

`chimerax_artiax_GL.sif` renders fluidly (LANDED 2026-06-06) and should become the default the one-click button
uses; the software-GL CPU sif drops to a fallback. Concretely (this IS "## Plug the GPU container" items 1-2):
- conf.yaml `curation:` → `sif_path: …/chimerax_artiax_GL.sif`, `partition: g`, `gres: "gpu:1"`, `vgl: true`,
  `time: "08:00:00"`.
- `CurationConfig` gains `gres`/`vgl`; `backend.launch_curation_session` adds `#SBATCH --gres=` + `export
  CX_VGL=1` when set (mirror what `launch_curation_vnc.sh g` already does on the manual path).
- Keep the CPU sif reachable via env/config override (no-GPU nodes / QtWebEngine-free path).
- **Validate during the spike:** the GPU sif's actual tomogram→pick→save `.coords` loop is still un-confirmed
  (only `open 4ug0`, a PDB model, has rendered) — this is the same runtime gate as the preload px check.

## Session log & next-session handoff (2026-06-03)

### Landed (session 2 — UI one-click launch, Option A)
Decision (with user): **Option A** — crboost only hands over connection details (one `ssh -L`
tunnel + viewer + password); NO in-crboost VNC proxying/noVNC (rejected as a Rube-Goldberg proxy
chain on top of the existing browser→login→crboost forward). The button's only job is to start the
SLURM job and surface the tunnel.
- **Config (not hardcoded):** `CurationConfig` in `services/configs/config_service.py`
  (`config_service.curation`) + a `curation:` block in `conf.yaml`/`conf.template.yaml`. Fields:
  `sif_path` (CX_SIF env var overrides), `partition` (default `c`), `cpus`, `mem`, `time` (`"0"`
  = infinite on `c`), `geometry`, `chimerax_bin`, `login_host` (blank ⇒ headnode FQDN). conf.yaml
  points at `/groups/klumpe/software/containers/sifs/chimerax_artiax.sif`.
- **One worker script** `containers/chimerax_artiax/curation_session.sh` (consolidated — deleted
  `_session_inside.sh`; `launch_curation_vnc.sh` now srun-`--pty`s the same worker). Runs on the
  compute node, starts Xvnc+fluxbox+ChimeraX, and writes a machine-readable `session.json`
  (`node/port/password/login_host/tunnel_cmd`) into `$CB_SESSION_DIR` for crboost to read; also
  prints the human banner for the manual path. `bash -n` clean.
- **Backend** (`backend.py`): `launch_curation_session(project_path?)` writes a per-session sbatch
  wrapper under `<project>/.curation_sessions/<id>/` (or `~/.crboost/curation/<id>/`), submits with
  a SLURM-env-stripped `sbatch`, returns `{slurm_job_id, session_dir, session_id}`.
  `get_curation_session_info(session_dir, job_id)` polls `session.json` then squeue
  (ready/pending/starting/ended). `stop_curation_session(job_id)` → scancel.
- **UI**: sidebar button (`view_in_ar` icon) in `RosterWidget._build_curation_btn`
  (`ui/pipeline_builder/pipeline_roster.py`) → `PipelineBuilderPanel.launch_curation_session`
  (SingleFlight-guarded) → `ui/curation_session_dialog.py` submits, polls every 3 s, then shows the
  3-step connect card (install viewer → copy `ssh -L` → viewer+password) + Stop button.
- All 5 changed files: `py_compile` + `ruff` clean (3 F401s flagged are pre-existing unused imports
  in untouched import blocks — left per surgical-changes rule). **NOT yet runtime-tested** (needs a
  real submit + tunnel + eyeball — same gate as the appliance).

### Landed (session 1)
- **`services/visualization/coords.py`** — canonical centered-Å ↔ voxel math + `TomoFrame`;
  gallery `imod_vis.py:46-92` deduped onto it. ruff + parity-independent round-trip verified.
- **`services/visualization/artiax_bridge.py`** — `.coords` read/write + centered-Å↔ArtiaX
  converters + CLI (`export`, `selftest`). Run in the user's module-loaded env (needs
  pandas/starfile/mrcfile; Claude's venv lacks them).
- **ArtiaX `.coords` format DECODED + round-trip VALIDATED**: plain `X Y Z`, physical Å from the
  volume **corner** (= `voxel × pixel_size`), no header, positions only; pixel size from the MRC
  header (ChimeraX reads 6.2 correctly). Mapping: `centered_Å = artiax_Å − (N/2)×pixel_size`.
  Exported our PyTOM picks → opened in ArtiaX → **they land on the density, NO flip/axis swap.**
  Confirmed cmds: `open <f>.coords` loads a particle list; `artiax open tomo <path>`; UI mouse
  mode "mark point" places markers; save a list as `.coords`.
- **`containers/chimerax_artiax/`** — self-contained Apptainer curation appliance, **BUILT OK**
  (SIF at `/groups/klumpe/software/containers/sifs/chimerax_artiax.sif`). `chimerax_artiax.def`
  fetches ChimeraX 1.9 from UCSF (non-commercial license auto-accepted via the download CGI;
  anchor the redirect on `/chimerax/` to skip the `2;url=` meta-refresh prefix), bakes ArtiaX
  (`toolshed reload all; toolshed install ArtiaX` into `/opt/cx` via `XDG_DATA_HOME`; build
  hard-fails if ArtiaX isn't present), Mesa **software GL** + TigerVNC + fluxbox. ChimeraX exe =
  lowercase `chimerax`. `launch_curation_vnc.sh` + `_session_inside.sh` srun a partition-`c` CPU
  node, start VNC, print the exact `ssh -L` tunnel + one-time password.
- **`MOUNT_CBE_FOR_CHIMERAX.md`** — data-access/mount guide + ChimeraX/ArtiaX install + manual
  picking walkthrough (now secondary to the remote-GUI route).

### Cluster facts (CBE/CLIP @ VBC, `clip-login-1.cbe.vbc.ac.at`)
- Apptainer 1.1.9, `--fakeroot` works (root-mapped namespace). Lab container area:
  `/groups/klumpe/software/containers/{defs,sifs}`. RESOLVED (session 2): the SIF location is NOT
  hardcoded — it's `conf.yaml` `curation.sif_path` (CX_SIF env overrides), so it can live anywhere.
  conf.yaml currently points at `…/containers/sifs/chimerax_artiax.sif`.
- Partitions: `c` = 125 CPU nodes, **infinite** walltime (use for curation + software GL); `g` =
  P100/V100/RTX/A100, infinite. No Open OnDemand. TurboVNC/VirtualGL/Mesa available as modules.

### NEXT SESSION — do these in order
1. **Runtime-test the launch path** (worker + UI button both built, never launched). Two ways in,
   same worker (`curation_session.sh`): (a) **UI** — open a project, click the sidebar "Launch
   ChimeraX + ArtiaX" button, follow the connect card; (b) **manual** —
   `CX_SIF=…/sifs/chimerax_artiax.sif containers/chimerax_artiax/launch_curation_vnc.sh`. Then
   tunnel → TurboVNC viewer → confirm ChimeraX+ArtiaX come up on software GL and can `open
   <recon>.mrc` + `open <auto.coords>`. **Critically verify ArtiaX loads at runtime from the
   read-only `/opt/cx`** (the XDG bake) when run as the cluster user, not root. Watch-outs:
   `session.json` visibility across NFS (poll is 3 s); rfb-port firewall login↔compute; vncserver
   flags; display `:1` collisions if two sessions land on one node (MVP is one-at-a-time). First-run
   checklist in `containers/chimerax_artiax/README.md`.
2. **`.cxc` generator** (add to `artiax_bridge.py`) — per tomogram: open recon as tomogram + load
   our `auto.coords` + create an empty manual list to pick into.
3. **Import path** — manual `.coords` → `ManualPicks/<species>/<tomo>.star` via
   `artiax_bridge.artiax_to_centered_angst`; "Import ArtiaX picks" button in
   `ui/tomo_dashboard_dialog.py`.
4. **Combine + `_combined` sibling** — per-(species,tomo) policy (union/manual-only/auto-only/
   curated+manual), dedup within ~particle_diameter/2 (Å); materialize
   `optimisation_set_combined.star` + `particles_combined.star`; add resolver tier
   `_combined > _filtered > original` (same `prefer_if_exists` as
   `services/jobs/subtomo_extraction.py:65-77`). → zero driver changes downstream.
5. **Stage C/D** — one-click VNC launch ✓ DONE (`backend.launch_curation_session` +
   `ui/curation_session_dialog.py`). Remaining: per-tomo `.cxc` session manifest + a
   `crboost next/prev/save` ChimeraX command for the 40–80-tomogram blitz + auto-ingest of saved
   `.coords`. Also: launch button is currently project-global (opens a bare session) — wire it to
   pass the active tomogram's recon + `.cxc` once step 2 lands.

### Gotchas
- Claude's venv has NO numpy/pandas — `venv/bin/ruff` + `python3 -m py_compile` only; run the
  bridge CLI / full app in the user's module-loaded env.
- ArtiaX's RELION-5 *writer* is buggy (#40) → we exchange via `.coords` (positions only), owning
  both conversions. Do NOT switch to a RELION-5-star round-trip.
- macOS sshfs *writes* to the mount fail / spawn `._` AppleDouble files — another reason the
  remote-GUI route wins (saved picks land on the cluster FS directly).

## Curation workbench — the multi-list model (CURRENT DESIGN, 2026-06-03, grounded in code)

This is the current curation data-model + UI contract. It **supersedes** the "auto read-only reference
+ manual-additions list + combine-policy" framing in the roadmap below; the `_combined` sibling +
resolver tier still stand. Per (species, tomo) there are **N named pick lists**, each a
visibility-toggleable layer (shape+color) over the recon slab canvas, each with its cutouts in a linked
contact sheet. Exactly **one list is authoritative** — the one materialized at the canonical optset
location the existing resolver already forwards downstream + to cross-project aggregation. The workbench
manages many lists; merge/commit collapses a chosen subset into that one authoritative list.

**This is largely a generalization of the existing per-species overlay** (`ui/tomo_dashboard_dialog.py`),
not new machinery: per-species dot layers, the visibility toggle, linked dot↔cutout brushing, and
keep/drop curation already exist — we widen "per species" → "per list." Two genuinely-new pieces: shape
encoding, and on-the-fly cutouts from the binned recon.

### List model
A `PickList` per (species, tomo): `{slug, label, type, path, species_id, tomo_name, count, created_at,
created_by, color, shape, parent_slugs, visible}`. Types + glyphs:
- `auto` (circle) — PyTOM `candidates.star` (External job). Always present, read-only; registered virtually.
- `filtered` (circle, dimmed) — CC/top-N derived from a parent. The existing `particles_filtered.star`
  keep/drop curation is just this type.
- `manual` (diamond) — saved in ArtiaX → ingested `.coords` → centered-Å star (`ManualPicks/<species>/<tomo>.star`).
- `imported` (square) — user-supplied `.coords`/star, ingested like manual.
- `merged` (triangle) — 2+ lists combined w/ radius dedup; the commit candidate → authoritative `_combined`.

Registry: a `pick_lists` collection on `ProjectState`, mirroring `AggregationMerge`/`AggregationSource`
(`services/project_state.py:432-511`) — same `mark_dirty()`/`save_if_dirty()` persistence. `created_by`
is stamped by crboost (OS/login user) at create/import (ArtiaX files carry no author).

### The authoritative list (downstream, unchanged)
Merge/commit writes `optimisation_set_combined.star` + `particles_combined.star` in the subtomo job dir.
Extend the single canonical resolver `picks_filter.resolve_canonical_optset` (`picks_filter.py:269-277`)
to tier `_combined > _filtered > original` — both the IO-slot resolver (`path_resolution_service.py:576-592`,
`prefer_if_exists`) and cross-project aggregation route through it, so downstream + aggregation pick the
merged set up with **zero driver changes**. One-line tier add; reuse `subtomo_link._coord_key`
(`subtomo_link.py:41`, 0.1 Å) for radius dedup on merge.

### Rendering seams (all in `ui/tomo_dashboard_dialog.py`)
- **Dots:** `_render_pick_layer()` (3976) already emits one toggleable `.cb-pick-layer` per set with a
  `--sp-color` var; shape is a uniform 3px circle = the seam. Add a per-list **shape class**
  (circle/diamond/square/triangle via border-radius / rotate / clip-path) + per-list color. Layer id
  widens `…-{species_idx}-…` → `…-{list_slug}-…`.
- **Toggle:** the per-species checkbox→`classList` hide (4391-4406) becomes **per-list**, governing dots
  + that list's cutouts together (your "visibility toggles both dots and cutouts").
- **Cutouts for any list:** today the atlas is built by subtomo extraction (pre-extracted `.mrc`, `vis/`
  dir). Manual/imported/merged lists have no extraction → generate the cutout atlas **on the fly from the
  binned recon MRC** at each pick voxel (reuse `render_pick_cutouts_atlas()` sourced from the recon,
  central-slab box-crop). Feeds the same `_render_gallery_body()` (4898) tile grid + linked brushing
  (5554-5681) unchanged.
- **Linked brushing** (dot↔tile hover/click, 5554-5681) already exists, scoped per layer id — extend the
  wired-id set to per-list.
- **Drop stats:** remove the z-pct / nn-dist / CC-histogram panels (`write_picks_data` extras); keep CC
  only as a per-dot hover value + the filter handle.

### Interactions
Each list chip: shape+color swatch · label · count · **eye** (visibility) · type icon · created/by
tooltip · actions [Open in ArtiaX, Filter→derive, Delete]. Chips are **multi-selectable** (distinct from
the eye toggle) → **Merge selected** → a new `merged` list. **Set authoritative** on any list
materializes the `_combined` sibling. "Open in ArtiaX" = the two-state ensure-session button loading that
list; editing in ArtiaX is **non-destructive** → saving yields a new `manual`-type list, source untouched.

### Build order
1. **Data model** — `PickList` + `ProjectState.pick_lists` registry (mirror `AggregationMerge`); enumerate
   existing auto/filtered lists into it. [foundation]
2. **Per-list overlay refactor** — widen `_render_pick_layer` + toggle from per-species to per-list; add
   shape encoding; per-list eye governs dots. [the visible core]
3. **On-the-fly recon cutouts** — atlas from the binned recon at arbitrary coords; visibility governs
   cutouts; per-list linked brushing. [the "meat" — dots↔cutouts for any list]
4. **Ingest/import** — `.coords`→manual/imported star + registry; "Open in ArtiaX" loads a chosen list
   (ensure-session). [create lists]
5. **Filter** — CC/top-N derive → new `filtered` list. [primitive filter]
6. **Merge + authoritative** — select 2+ → dedup merge → `merged`; "Set authoritative" → `_combined`
   sibling + the `resolve_canonical_optset` tier. [closes the loop downstream]

**Progress (2026-06-03):**
- **Slice 1 ✓** (landed, verified to ceiling) — `PickListType` (`services/models_base.py`), `PickList` +
  `ProjectState.pick_lists` + `get/add/remove_pick_list` accessors (`services/project_state.py`). Only
  workbench-authored lists persist; auto/filtered stay disk-synthesized. Additive, no schema bump.
- **Slice 2 ✓** (compile+ruff clean; backward-compatible, needs runtime eyeball) — per-LIST overlay in
  `ui/tomo_dashboard_dialog.py`: `_collect_pick_lists_for_species` (slice-2 returns just `auto`),
  `_render_pick_layer(..., shape)` + `.cb-shape-*` glyph CSS + `.cb-swatch-*`, `_render_particles_canvas`
  toggle row + layer loops widened per-list (layer id `cb-pl-{nonce}-{idx}-{slug}-{axis}`; per-list
  `_lid_xy/_lid_xz` stored for cutout linking; gallery bridge still targets the `auto` layer). With one
  auto list per species it renders identically to before — zero visual regression by design.
- **Slice 3 (enabler) ✓** (compile+ruff clean) — `services/visualization/recon_cutouts.py`:
  `render_recon_cutouts_atlas()` builds the SAME atlas PNG+index schema as
  `preview_render.render_pick_cutouts_atlas` but from the binned recon (central-Z-slab box crop per pick,
  tomogram-wide norm), so ANY list (manual/imported/un-extracted auto) gets cutouts. NOT yet wired.
- **Blank-session fix — `.cxc` preload MECHANISM ✓** (the reprioritized #1; compile/ruff/bash-n clean,
  string-gen + worker-composition verified by exec, but NOT ChimeraX-tested). The launch path can now open
  preloaded instead of blank:
  - `services/visualization/artiax_bridge.py`: `build_session_cxc()` (pure-stdlib `.cxc` text — header with
    tomo/species/px/size, `set bgColor black`, optional `windowsize`, then the CONFIRMED load backbone, then
    a manual-list/save-target comment), `session_chimerax_commands()` (the single source of the load
    backbone: `artiax start` / `artiax open tomo <recon>` / `open <auto.coords>` / `lighting simple` — reused
    so the `.cxc` and the future Tier-1 copyable-commands UI never drift), `prepare_curation_bundle()`
    (exports our picks → `<tomo>__auto.coords` + writes `open_<tomo>.cxc` + returns resolved paths/commands;
    hard-errors if the recon MRC is absent), and a `bundle` CLI subcommand. Uncertain ArtiaX subcommands
    (forcing a particle-list px, creating the empty manual list) are emitted as **comments**, not commands,
    so a wrong guess can't halt the script — the user confirms exact syntax on the first runtime test.
  - `containers/chimerax_artiax/curation_session.sh`: new `CB_CXC` env → appends the `.cxc` (via `printf %q`)
    to `CX_LAUNCH`, composing with the existing software-GL **and** `vglrun` paths; missing/blank ⇒ warns +
    falls back to a blank session (never fails to launch).
  - `backend.launch_curation_session(project_path, cxc_path=None)` exports `CB_CXC` into the sbatch wrapper.
  - `launch_curation_vnc.sh` usage documents `CB_CXC` (propagates via `srun --export=ALL`).
  - **Runtime-test path (no UI yet):** `python -m services.visualization.artiax_bridge bundle
    --candidates <ce>/candidates.star --tomograms <ce>/tomograms.star --tomo <T> --out-dir <d> --species <s>`
    (user's module env) → `CX_SIF=… CB_CXC=<d>/open_<T>.cxc launch_curation_vnc.sh`. This proves preload
    before any UI wiring (the handoff's "runtime-test then build UI" order).
- **Session-reuse substrate ✓ (landed this session, unit-verified)** — the squeue-derived liveness pieces
  the ensure-session button needs, all in `backend.py`:
  - `launch_curation_session` now persists `<session_dir>/job.json` (`slurm_job_id`/`session_id`/`cxc`) at
    submit, so a fresh render or another tab can recover a session it didn't launch.
  - `find_active_curation_session(project_path) -> dict|None` scans `.curation_sessions/*/job.json`, makes ONE
    `get_user_jobs()` call, and returns the first session whose job is a live (`RUNNING/PENDING/CONFIGURING/
    SCHEDULED/COMPLETING`) `-J cb-curation` job — merged with `session.json` connection fields. Liveness is
    squeue-derived, never a stored boolean (dodges the stuck-yellow stale-flag trap). Unit-tested off-cluster
    against fake squeue states: live-found, dead→None, name-filter-rejects, PENDING-live, array-id match,
    empty-dir/no-json no-crash. **Does NOT yet de-dupe at submit** — `launch_curation_session` still always
    `sbatch`es; the ensure-session button must call `find_active_curation_session` first and branch.
  - `prepare_curation_bundle(project_path, candidates_star, tomograms_star, tomo_name, species_label)` — thin
    async wrapper (`asyncio.to_thread`) over the bridge; writes the bundle under
    `<project>/.curation_sessions/bundles/<species_slug>/` and returns `{cxc_path, commands, …}`. This is the
    method the per-tomo button calls (the UI already has `job_dir` → candidates/tomograms star, so no
    species→job-dir resolver is needed in the backend).
- **Per-tomo "Curate in ArtiaX" button + Tier-1/Tier-2 routing ✓ (landed this session; compile/ruff clean,
    SingleFlight logic + backend wired, but NOT runtime-tested — no UI eyeball off-cluster).** The blank
    session is now reachable preloaded from the gallery:
  - `backend.get_backend()` — process-wide singleton (set in `CryoBoostBackend.__init__`, same idiom as
    `get_config_service`), so the tomo dashboard (a function module with no threaded backend) can reach it.
  - `ui/tomo_dashboard_dialog.py`: a `view_in_ar` button in `_render_species_tab_header` (gated on
    `row.get("vol_path")` = recon present), → `_handle_curate_in_artiax(sp, project_path)`
    (module-level `_curation_flight = SingleFlight()`, keyed `species_id:tomo`). It calls
    `backend.prepare_curation_bundle(project_path, job_dir/candidates.star, job_dir/tomograms.star, tomo,
    label)` then `find_active_curation_session`: live → `open_curation_commands_dialog(commands, info,
    manual_coords)` (Tier-1 paste-in, no 2nd sbatch); none → `open_curation_session_dialog(..., cxc_path=…)`
    (Tier-2 preload).
  - `ui/curation_session_dialog.py`: `open_curation_session_dialog` gained `cxc_path` (→
    `launch_curation_session(..., cxc_path=…)`) and a preload-aware "once it opens" step; new
    `open_curation_commands_dialog(commands, session_info, manual_coords)` for Tier-1.
  - **Gate runtime check:** if auto picks land shifted in ArtiaX, add a pixel-size cmd to
    `session_chimerax_commands` (one place) — the buttons need no change.
- **Ensure-session sidebar button ✓ (landed this session; compile/ruff clean, NOT UI-eyeballed).** The sidebar
  "Launch ChimeraX + ArtiaX" is no longer one-shot: `PipelineBuilderPanel.launch_curation_session` now calls
  `find_active_curation_session` first (SingleFlight-guarded). Live → `open_curation_chooser_dialog` ("running
  on node N · SLURM J" + **Reconnect** / **Stop & relaunch**); none → launch as before. **Reconnect** opens
  `open_curation_session_dialog(..., existing=active)` — new `existing` param drives a `_reconnect()` path that
  seeds `job_id`/`session_dir` and polls `session.json` with **NO second sbatch**. **Stop & relaunch** =
  `stop_curation_session(old)` → fresh launch (escape hatch for a wedged session). This is the click-time
  chooser the plan's "concrete code shape" specifies (keeps the squeue call OUT of the timer-driven roster
  render). Idempotent by construction — at most one session. **Still open:** bundle always re-exports (no mtime
  staleness); the chooser is sidebar-only (the per-tomo button still goes straight to Tier-1 commands /
  Tier-2 launch, which is correct — it always has a specific tomo to load).
- **Ingest CONVERTER ✓ (landed this session; compile/ruff clean, math already covered by `selftest`).**
  `artiax_bridge.import_coords_to_centered_star(coords_path, tomograms_star, tomo, out_star, project_root)` —
  the inverse of `export_tomo_picks_to_coords`, using the SAME `TomoFrame` so the `N/2` cancels (round trip
  parity-exact; the existing `selftest` already verifies the `artiax_to_centered_angst` leg to <1e-6). Writes a
  minimal RELION-5 star (`rlnTomoName` + the three `rlnCenteredCoordinate*Angst`) at e.g.
  `ManualPicks/<species>/<tomo>.star`. New `import` CLI subcommand. **Full round-trip is now CLI-testable:**
  `… export --candidates … --out a.coords` → `… import --coords a.coords --tomograms … --out m.star` → compare.
  **Still converter-only** — NOT wired to a UI "Import" button, the `PickList` registry, or the manual-save
  watch path (those are the next step, gated on the runtime confirmation that picks save where expected).
- **NEXT (slice 3b/4 + polish):**
  - **Ingest UI/registry** — "Import ArtiaX picks" button + watch the session's `manual/` dir →
    `import_coords_to_centered_star` → register a `manual` `PickList`; raw import kept at
    `ManualPicks/<species>/imports/<tomo>__<stamp>.coords` for provenance.
  - wire the recon atlas as the gallery fallback when no subtomo atlas exists
  (`_render_species_tab_body` ~4682 — mind `_render_gallery_body`'s `subtomo_job_dir` curation-save
  coupling: degrade to read-only cutouts when None); then per-list contact sheet; then "Open in ArtiaX"
  loading a chosen list; then filter + merge→`_combined`. All need the module-loaded env (numpy/
  mrcfile/starfile/pandas absent in Claude's venv) — verify in the running app.

## Post-launch curation workflow roadmap (2026-06-03)

The appliance now **renders** — ChimeraX+ArtiaX opens over VNC and is interactive. Everything
below is the *workflow* on top of that: getting the right data in front of the user with zero
path-typing, and getting their manual picks back into the pipeline.

### Appliance final working recipe (baked into `containers/chimerax_artiax/chimerax_artiax.def`)
The chain of non-obvious fixes it took to get a working GUI on this cluster — DO NOT regress these:
1. **No `vncpasswd` + no python in the image** → install `x11vnc` purely for `x11vnc -storepasswd`
   to mint the VncAuth file. (TigerVNC 1.12 ships `Xtigervnc`/`vncserver` but no passwd tool.)
2. **Read-only config** → the baked `/opt/cx` is read-only; do NOT set `XDG_CONFIG_HOME` there.
   Leave it unset (→ `~/.config`); the worker overrides config+cache to unique `/tmp` dirs per run.
   `XDG_DATA_HOME=/opt/cx/share` stays (ArtiaX loads from there).
3. **Bundled Qt6 not on loader path** → register `…/PyQt6/Qt6/lib` via `/etc/ld.so.conf.d` + `ldconfig`.
4. **GUI runtime libs** → install the Qt6 `xcb`/X11/GL set (`libxcb-cursor0`, `libxkbcommon-x11-0`,
   `libegl1`, … — see def).
5. **THE BIG ONE — CentOS 7 kernel 3.10 < Qt6's `NT_GNU_ABI_TAG` (Linux 4.11).** glibc's loader
   refuses any lib tagged for a newer kernel than the running one, surfacing as `libQt6Core.so.6:
   cannot open shared object file` even though the file is present AND in the ld cache (the cache
   line literally prints `OS ABI: Linux 4.11.0`). Fix: `objcopy --remove-section=.note.ABI-tag` on
   every `.so` under `/usr/lib/ucsf-chimerax`, then `ldconfig`. This is a CLUSTER-WIDE fact — any
   modern-Ubuntu/manylinux GUI container has this collision on CBE's el7 nodes.

Worker `curation_session.sh`: `vncserver` (TigerVNC) + `fluxbox` + `chimerax`; writes `session.json`
(node/port/password/tunnel) that the crboost dialog polls. Connect = one `ssh -L` + any VNC viewer.

### Polish items
- **Viewer-agnostic dialog copy** — DONE (any VNC viewer / macOS Screen Sharing; not TurboVNC-specific).
- **ChimeraX opens at ~⅓ of the VNC desktop, not maximized.** The viewer shows the full 1920×1080
  fluxbox desktop; ChimeraX's default window is small within it. Fix options (pick one, verify once):
  - (a) **fluxbox auto-maximize (no extra package):** worker writes `~/.fluxbox/apps` before launch
    with a rule matching ChimeraX (`[app] (name=ChimeraX) [Maximized] {yes}`). Confirm the X window
    name/class with `xprop` in a live session first.
  - (b) **`wmctrl`** baked into the SIF: after the window maps, `wmctrl -r ChimeraX -b
    add,maximized_vert,maximized_horz` from a background poll in the worker.
  - (c) ChimeraX startup `.cxc` `windowsize <W> <H>` to match the VNC geometry (sets the graphics
    area; combine with a WM maximize for true fullscreen).
  - Recommended: (a) first; fall back to (b). Also let `CX_GEOMETRY` track the user's screen.

### The seamless curation experience (reduce mental burden)
**Principle:** crboost knows everything (project, species, per-tomo recon + picks, pixel size, N);
ChimeraX knows nothing. The bridge carries crboost's knowledge into the session so the user never
types a path or remembers a species↔project↔tomo mapping.

**What we CAN dispatch to ChimeraX (we're less limited than it feels):**
- A **startup `.cxc`** (ChimeraX command script) — full control: open the recon as a tomogram,
  `artiax start` / `artiax open tomo`, load auto/curated picks as a particle list, create an empty
  manual list, set contrast/slab/window size.
- **Launch `chimerax <session>.cxc`** from the worker (add a `CX_OPEN`/`CB_CXC` env the worker
  passes as `chimerax <file.cxc>`), so the session opens pre-loaded.
- **A tiny baked-in `crboost` ChimeraX bundle** (installed like ArtiaX) registering commands
  (`crboost next/prev/save/status`) that read a session-manifest JSON and drive a multi-tomogram
  blitz. This is the elaborate-but-elegant endgame.

**Session model — DECIDED (2026-06-03): one live session per project, reused; never one-per-tomogram.**
The earlier fork (Tier-2 launches a fresh session per tomogram vs. reconnect-and-swap) collapses.
A fresh session per tomogram is a non-starter for the 40–80-tomo blitz: every SLURM job lands on a
new node/port/password, so the user would re-tunnel + re-open the VNC viewer for *every* tomogram —
exactly the friction the feature exists to remove. The Option-A connection (one `ssh -L` + viewer +
password) is only cheap when amortized across the whole run. So the rule across all tiers: **crboost
tracks at most one live curation session per project and routes all curation through it.**
- *Liveness = ground truth, not a cached flag.* Derive "is a session live?" by scanning the project's
  `.curation_sessions/*/` dirs and cross-referencing `squeue` (curation jobs are `-J cb-curation`); live
  iff its SLURM job is RUNNING/PENDING. The dir + `session.json` supply connection details. Persist the
  `slurm_job_id` into the dir at submit (today it lives ONLY in the dialog's local state in
  `curation_session_dialog.py`, so a fresh render can't recover a session it didn't launch). Do NOT keep
  a separate "is-running" boolean in `ProjectState` — that's the stale-flag trap behind the stuck-yellow
  bug (`feedback_inmemory_state_authoritative` / `project_candidate_preview_subtomo_cache_race`); squeue
  is the source of truth, the dir is the durable record.
- *The launch button becomes `ensure-session`, not `launch-session`.* If a session is live, reuse it
  (no second `sbatch` — which also sidesteps the display-`:1` collision the appliance MVP warned about).
  If none, submit one. Idempotent by construction — at most one session exists.
- *Tier-2 "Curate this tomo" = enqueue into the live session.* If live: write the (tomo, recon,
  auto.coords, manual-save-path) into the session manifest and tell the user "switch to your open
  viewer" (no new tunnel). If none: launch, with this tomo as manifest entry #1. This **merges Tier-2
  and Tier-3** — the manifest + baked `crboost next/prev/save` bundle is not a separate endgame tier,
  it's the in-session mechanism that makes "work out of the one open session" real. Build the manifest
  as soon as Tier-2 needs to load a *second* tomo into a live session.

**Launch-button states — the two-state `ensure-session` affordance (DECIDED 2026-06-03).** Don't
silently reuse; *show* the live session and let the user choose. The button (sidebar today; the
per-tomo Tier-2 button later) renders from the liveness check above into one of two states:
- *No live session* → **"Launch ChimeraX + ArtiaX"** — current behavior (submit + connect dialog).
- *A live session for this project* → a compact **"Session running on node N ▸ [Reconnect] · [Stop &
  relaunch]"**. **Reconnect** opens the existing connect dialog in *reconnect mode* — seed it with the
  recovered `session_dir`+`slurm_job_id` and go straight to polling `session.json` / `render_ready`
  (the tunnel/viewer/password are already in `session.json`); **NO second `sbatch`**. **Stop &
  relaunch** = `stop_curation_session(old)` then submit fresh — the escape hatch for a wedged session.
  (For the Tier-2 per-tomo button, *Reconnect* first enqueues this tomo into the live session's
  manifest, i.e. "I loaded it — switch to your open viewer.")

Concrete code shape (build + verify together with the launch-path runtime test — none of it is
eyeball-able without a real session):
- `backend.launch_curation_session`: after a successful `sbatch`, write `{"slurm_job_id": …}` into the
  session dir (e.g. `job.json`) so the id outlives the dialog.
- `backend.find_active_curation_session(project_path) -> dict|None`: scan `.curation_sessions/*/`, read
  each `job.json`, one `get_user_jobs()` call to filter to live `cb-curation` jobs, return the live
  one's `{session_dir, slurm_job_id, status, + session.json fields if present}` (or `None`).
- `open_curation_session_dialog(backend, project_path, existing=None)`: when `existing` is passed, set
  `state["job_id"]/["session_dir"]` from it and call `_poll()` + start the timer instead of `_start()`.
- `PipelineBuilderPanel.launch_curation_session` (SingleFlight): call `find_active_curation_session`
  first; if live, render the two-action chooser, else open the dialog as today.

Remaining sub-decisions (smaller, not blocking):
- *Who drives the swap.* Default: hands-stay-in-ChimeraX — the baked bundle's `next/prev/save`
  (toolbar/keybinding) walks the manifest; the gallery button only seeds/extends the queue.
  Alternative: drive from the crboost gallery via ChimeraX's `remotecontrol rest` endpoint (POST
  `open …` to the running session) — lets a gallery click swap the tomo, but adds a second port/tunnel
  + couples crboost to the live session over HTTP. Recommendation: manifest+bundle (already designed,
  keeps the user in the viewer); keep REST as the escape hatch if gallery-driven swapping is wanted.
- *Session scope: per-project.* Recon paths are absolute so one session *could* curate any project,
  but ManualPicks + ingest + registry are project-scoped → one session per project, tracked in that
  project's `ProjectState`. Two projects in two tabs ⇒ two independent sessions; the "is one live?"
  check is project-scoped.

**Tier 1 — per-tomo copyable info panel (cheap, immediate, independently useful).**
In the gallery (`ui/tomo_dashboard_dialog.py`), each tomo preview gets a compact panel with:
`rlnTomoName`, species, recon path (binned MRC), auto/curated picks path, pixel size, binned N,
`ManualPicks/<species>/<tomo>.star` target — each copyable, plus a "copy ChimeraX open commands"
button emitting the exact `open <recon>` / `open <picks>.coords` lines. Transparency + manual fallback.

**Tier 2 — one-click "Curate this tomogram in ArtiaX" (the real seamless path).**
From a tomo preview (species-scoped): button → crboost generates a `.cxc` for (species, tomo) that
opens the recon as a tomogram, loads `<tomo>__auto.coords` as a read-only reference list, creates an
empty `<tomo>__manual` list to pick into, sets pixel size + a sensible slab/contrast; the worker
launches `chimerax open_<tomo>.cxc`. User types nothing. (Keep a handedness/axis hook tied to
`rlnTomoHand`; our PyTOM picks needed NO flip, but keep the seam.)

**Tier 3 — session manifest + blitz (40–80 tomograms).**
crboost writes a session manifest (JSON: ordered (species, tomo, recon, auto.coords, manual-save
path)); the baked `crboost` bundle's `next/prev/save` loads tomo *i*, and `save` writes
`<tomo>__manual.coords` to the manifest path + advances. The "scrub → pick → Save & Next" loop the
feature exists for. Bottom-toolbar buttons or key bindings.

### Ingest + merge infrastructure
**Where manual picks land:** the session runs on the cluster, so saved `.coords` write straight to
the cluster FS — no download. Convention: save to `<session_dir>/manual/<tomo>__manual.coords` (the
`.cxc`/bundle names the list + target; Tier-3 `save` does it automatically).

**Ingest pipeline (per saved `.coords`):**
1. crboost watches the session's `manual/` dir (poll, or on session end, or a gallery "Import" button).
2. Read `.coords` (physical Å from the volume corner = `voxel × pixel_size`).
3. Convert → centered-Å via `artiax_bridge.artiax_to_centered_angst` with the tomo's binned-MRC N +
   pixel_size — the SAME `N/2` as export, so the round trip is parity-exact.
4. Write `ManualPicks/<species>/<tomo>.star` (RELION-5 centered-Å); keep the raw import for
   provenance at `ManualPicks/<species>/imports/<tomo>__<stamp>.coords`.
5. Update a `ProjectState` registry (mirrors `aggregation_merges`): per (species, tomo) → manual
   count, source file, `imported_at`, pixel_size+N (refuse a mismatched re-import), `combine_policy`.

**Merge / combine (ZERO driver changes downstream):**
- Policy per (species, tomo): `union | manual_only | auto_only | curated+manual` (default
  `curated+manual`). On `union`, dedup manual vs auto within ≈`particle_diameter/2` (Å) — use Å
  distance, not the 0.1-Å exact `_coord_key` (`subtomo_link.py:41`).
- Materialize a `_combined` optimisation_set sibling (`optimisation_set_combined.star` +
  `particles_combined.star`) in the subtomo job dir, exactly like the `_filtered` siblings.
- Add resolver tier `_combined > _filtered > original` via the same `prefer_if_exists`
  (`services/jobs/subtomo_extraction.py:65-77` + `services/path_resolution_service.py`). → subtomo
  extraction, refinement, and the `MergedSources` aggregation all pick it up unchanged.
- Staleness: key the `_combined` sibling on source mtimes (auto + manual) so re-curating auto
  rebuilds it (same lesson as `project_candidate_preview_subtomo_cache_race`).

### Suggested build order (next sessions)
1. **Polish** — fullscreen (fluxbox apps) + viewer-agnostic copy (done). [small]
2. **Tier-1 info panels** in the gallery (copyable paths/cmds). [small, independently useful]
3. **`.cxc` generator** in `artiax_bridge.py` + **session liveness registry in `ProjectState`** +
   **`ensure-session` Tier-2** one-click for a specific tomo: reuse the live session (enqueue the tomo
   into its manifest) else launch one (this tomo = manifest entry #1). The worker takes a `.cxc`/manifest
   to open. Button is **two-state** (Reconnect / Stop & relaunch) off a squeue-derived liveness check —
   first code step is persisting `slurm_job_id` to the session dir + `find_active_curation_session`. [the
   seamless core — see "Session model — DECIDED" + "Launch-button states"]
4. **Ingest** (`.coords` → `ManualPicks` star + registry) + gallery "Import" button. [closes the loop]
5. **`_combined` sibling** + resolver tier + staleness. [downstream, zero driver changes]
6. **Tier-3** baked `crboost` ChimeraX bundle (`next/prev/save` toolbar/keybindings) over the manifest
   from step 3, for the hands-in-viewer blitz UX. [elaborate endgame — manifest already exists by here]
7. **Aggregation tie-in** — combined sets as curated sources cross-project.

## Goal

Add a bidirectional manual-picking workflow at the PyTOM gallery stage so a user can:

1. **Export** our pipeline picks (auto / curated) for one tomogram into a bundle that opens
   directly in ChimeraX + ArtiaX on their Mac, with positions landing on the real density.
2. **Manually pick** in ArtiaX and **import** those picks back, per `(species, tomogram)`.
3. **Browse, merge, and commit** an eventual combined manual+automatic pick set per tomogram
   that all downstream steps (subtomo extraction, refinement, cross-project aggregation)
   consume transparently.

Success = a manual pick placed in ArtiaX shows up, in-register, in the same downstream
`optimisation_set.star` the refinement reads — with zero driver changes downstream.

## Decisions locked (2026-06-02)

- **Positions only.** Manual picks carry no orientation; angles are derived later by
  refinement. This lets us own all coordinate math and use a coords-only exchange.
- **Coords-only exchange (`.coords`).** We do NOT use ArtiaX's RELION-5 writer — it has an
  open, unresolved shift bug (FrangakisLab/ArtiaX#40: "Saving a Relion5 particle list shifts
  all particles"; reporter confirms `.em`/`.coords` save correctly, RELION-5 does not). We
  convert `.coords` ↔ our centered-Å star ourselves.
- **Data path: rsync/scp bundles** (primary). SMB mount is a conditional secondary (see
  §Data access).
- **ArtiaX is a display + placement surface only.** No live IPC, no ChimeraX-as-a-service.
  We are NOT building a browser volume picker (see memory `feedback_no_browser_volume_rendering`).

## The coordinate contract (the crux)

Our picks are RELION-5 **centered Ångström**: `rlnCenteredCoordinate{X,Y,Z}Angst`, origin at
the tomogram center. The existing forward transform lives in
`services/visualization/imod_vis.py:46-112`:

```
pixel_size = rlnTomoTiltSeriesPixelSize × rlnTomoTomogramBinning      # e.g. 1.55 × 4 = 6.20 Å
N         = binned tomogram dims, read from the MRC header (NOT rlnTomoSize*, which is unbinned)
voxel     = centered_Å / pixel_size + N/2                              # forward (Å → voxel)
```

Import inverts it with the SAME `N` and `pixel_size`:

```
centered_Å = (voxel − N/2) × pixel_size                               # inverse (voxel → Å)
```

**Why coords-only is parity-robust.** The half-voxel ambiguity that bites RELION-5
(`N/2` float vs `int(N/2)`, see 3dem/relion#1280) and the ArtiaX#40 shift both live in the
*centered-Å ↔ pixel* conversion. By exchanging raw `.coords` and applying the SAME `N/2` on
export and import, that term cancels in the round trip — internal self-consistency is
guaranteed regardless of which parity convention is "correct." The only external dependencies
are `N` (binned MRC header dims — deterministic) and `pixel_size` (deterministic).

**What still needs empirical calibration (Phase 0):** the *display* correctness — does ArtiaX
place a given `.coords` row on the actual density, or is there an axis flip / handedness? The
NextPYP ArtiaX guide needed `volume flip axis z` + `volume permuteAxes xzy` for some recons,
and we already carry `rlnTomoHand = -1` with a temporary override in
`drivers/template_match_pytom.py:154`.

**`.coords` format CONFIRMED (2026-06-02)** from a real ArtiaX 0.6 save: plain `X Y Z` whitespace
text, no header, positions only, in **physical Å from the volume corner** (= `voxel × pixel_size`)
at the MRC-header pixel size (ChimeraX read 6.2 correctly — no manual pixel-size entry needed). So
`centered_Å = artiax_Å − (N/2)×pixel_size`. Converter implemented + lint/round-trip-verified in
`services/visualization/artiax_bridge.py`. The ONLY remaining Phase-0 unknown is **display landing /
axis flip** — checked by exporting our picks and confirming they sit on density.

**Single source of truth:** factor the centered-Å ↔ voxel math out of `imod_vis.py` into one
shared helper (`services/visualization/coords.py`) used by viz, export, and import. One
definition, no drift (same discipline as `picks_filter.resolve_canonical_optset`).

## ArtiaX setup + the `.cxc` auto-config

- Install on the Mac: **ChimeraX** (native Apple Silicon/Intel), then `toolshed install ArtiaX`.
  Nothing runs on the cluster.
- ArtiaX does **not** read tomogram size / pixel size from the particle file — it makes the
  user type them in (and wants *two* pixel sizes: a binned one on the tomogram and an unbinned
  "Pixelsize Factors" origin). This manual entry is the #1 way users shift every particle.
- ArtiaX exposes a scripting surface (`artiax start`, `artiax open tomo`, `artiax particles`,
  `artiax tomo`, `artiax attach`, …). **We emit a `.cxc` per tomogram that bakes in the recon
  path, pixel size, and tomo size**, so the user never types a number. Exact flag syntax is
  confirmed in Phase 0 via ChimeraX `usage artiax particles` / reading the bundle's cmd module.

Bundle layout (one per `(species, tomogram)`):

```
<bundle>/
  <tomo>_<apx>Apx.mrc          # the binned recon (or a relative ref if mounted)
  <tomo>__auto.coords          # our auto/curated picks, positions, for visual reference
  open_<tomo>.cxc              # opens recon + auto.coords with pixel size/size baked in
  README.txt                   # "create a new particle list for manual picks, save as
                               #  <tomo>__manual.coords, drop it back in this folder"
```

## Data access on this cluster (CLIP / CBE @ VBC)

Findings (`clip-login-1.cbe.vbc.ac.at`):
- Project + binned recons live on `/users` = NFS `clip-cbe.imp.ac.at:/clip_homes`.
- Group/scratch volumes (`/groups`, `/resources`, `/scratch`) = NFS `storage.vbc.ac.at:/ifs/…`
  — `/ifs` ⇒ Dell/EMC **Isilon (OneFS)**, which *can* serve SMB.

Recommendation:
- **Primary — rsync/scp bundles.** A session touches a bounded set of tomograms; a binned
  recon is tens–hundreds of MB (bin4/bin8), not the multi-GB full-res. Pull bundles to local
  disk, pick locally, push the tiny `.coords` back. No mount infra, lowest latency, robust,
  fits the existing CLI-around-GUI style.
- **Secondary — SMB mount (worth one email to IT).** Direct NFS mount to a Mac is not feasible
  (internal appliances, IT-controlled exports, WAN-blocked). SMB *might* work for the Isilon
  `/groups` if VBC enabled the SMB protocol + VPN + account access. Current data is on `/users`
  (a different, non-Isilon server) so this would mean relocating curation data to `/groups`.
  Ask IT: *"Is SMB enabled on `storage.vbc.ac.at` for `/groups`, mountable from a Mac on the VPN
  via `smb://storage.vbc.ac.at/groups`?"* If yes, the bundle generator just writes into a
  `/groups` path and the round trip is a folder on a Finder volume.

The integration is identical either way — only *where the bundle folder lives* differs.

## Storage & housekeeping model

- **Manual picks** stored per species/tomo as `ManualPicks/<species>/<tomo_name>.star` —
  RELION-5 centered-Å (our convention), one file per tomogram. Browsable, diffable, the
  durable "auxiliary manual picks file" the workflow centers on. Original imports kept too
  (`ManualPicks/<species>/imports/<tomo>__<stamp>.coords`) for provenance.
- **Registry in `ProjectState`** (mirrors the existing `aggregation_merges` / `AggregationSource`
  patterns): per `(species, tomo)` → manual pick count, source filename, `imported_at`,
  `combine_policy`. Reuses the dirty-tracking + JSON persistence already in `project_state.py`.
- **Combine policy** per `(species, tomo)`: `union | manual_only | auto_only | curated+manual`
  (default `curated+manual` = curated-or-original auto ∪ manual). On `union`, dedup manual
  picks that coincide with an auto pick within a radius (≈ particle_diameter/2, in Å) so a
  manual pick re-placed on an existing auto pick doesn't double-count.

## Downstream wiring (zero driver changes)

Materialize a **`_combined` optimisation_set sibling** in the subtomo job dir
(`optimisation_set_combined.star` + `particles_combined.star`), exactly like the existing
`_filtered` siblings. Add a resolver tier so preference is `_combined > _filtered > original`
via the same `prefer_if_exists` mechanism already in `services/jobs/subtomo_extraction.py:65-77`
and `services/path_resolution_service.py`. Then subtomo extraction, refinement, and the
existing `MergedSources` aggregation (`drivers/subtomo_merge.py`, `ui/aggregation_merge_card.py`)
all pick up the combined set with no code change — same trick `_filtered` uses today.

Cross-project: a combined per-tomo set is just a curated source; it flows into the existing
aggregation machinery unchanged. The edge cases in `PICKS_FILTER_AGGREGATION_ROADMAP.md`
(tomo-name collisions, optics renumber, handedness drift) apply identically.

## UI changes (gallery)

In `ui/tomo_dashboard_dialog.py`, per tomogram:
- **"Export to ArtiaX"** → writes the bundle, surfaces a copy-paste `rsync` line (or the
  `/groups` path if mounted).
- **"Import ArtiaX picks"** → file picker for the saved `<tomo>__manual.coords`; converts →
  `ManualPicks/<species>/<tomo>.star`; updates the registry.
- A **manual lane / count badge** per tomo + a **combine-policy toggle** + **"commit combined"**.
- Follow the reactive rules (memory `feedback_reactive_ui_patterns`): `FingerprintedView` for
  the manual-lane counts, `SingleFlight` on the export/import handlers (they open dialogs).

## Edge cases to handle (call them out now)

1. **Tomo-name mapping.** The `.coords` filename / ArtiaX list must map to our `rlnTomoName`
   (`try2_after_pixShift_Position_11_2`). Bake the name into the bundle filename; verify on import.
2. **Axis flip / handedness.** Resolved by Phase 0; the `.cxc` includes any needed
   `volume flip`/`permuteAxes`, OR we apply a z-mirror in the converter. Tied to `rlnTomoHand`.
3. **Dedup radius on union** (see above). Use Å distance, not the 0.1-Å exact `_coord_key`
   (`subtomo_link.py:41`) — manual picks won't hit exact auto coords.
4. **Staleness.** If auto picks are re-curated after a manual import, the combined set must
   rebuild. Key the combined sibling on source mtimes (same lesson as
   `project_candidate_preview_subtomo_cache_race`).
5. **Multi-species.** Import is species-scoped by the gallery context (the user picks within a
   species). A manual `.coords` carries no species — we attach it from the active species.
6. **Missing recon.** Export requires the binned recon MRC (same hard dep as the gallery). If
   absent, surface the error, don't guess dims.
7. **Pixel-size / binning match.** The recon in the bundle must match the binning the picks were
   produced at; record `pixel_size` + `N` in the registry and refuse a mismatched re-import.

## Phased implementation plan

- **Phase 0 — calibration + shared coords helper (de-risk first).**
  ✓ `coords.py` (canonical transform + `TomoFrame`) extracted and gallery deduped onto it,
  verified 2026-06-02. **Remaining (gated on ArtiaX install):** confirm ArtiaX's actual
  `.coords` flavor (units/origin/delimiter) + the `artiax` command syntax; build a `.coords`
  exporter + a CLI that emits one bundle for a known tomogram. Open in ArtiaX (locally),
  confirm an auto-pick lands on density, save back, re-import, diff < 0.5 px. Output: a
  *verified* convention (units, origin, flip, exact `.cxc` syntax). The exporter is deliberately
  NOT written until ArtiaX confirms the format — no guessing. **Nothing else proceeds until this passes.**
- **Phase 1 — export path.** Bundle generator + `.cxc` + gallery "Export to ArtiaX" button +
  rsync helper line. User can see pipeline picks in ArtiaX on their Mac.
- **Phase 2 — import + storage.** `.coords` → centered-Å star converter; `ManualPicks/` layout;
  registry in `ProjectState`; gallery "Import" button + manual lane.
- **Phase 3 — combine + downstream.** Combine policy + dedup; `_combined` sibling; resolver
  tier; staleness rebuild. Downstream consumes transparently.
- **Phase 4 (optional) — aggregation tie-in.** Surface combined sets as curated sources in the
  cross-project merge; reconcile with the aggregation edge cases.

## Key code touch-points

- Coords math (factor out): `services/visualization/imod_vis.py:46-112`
- Pick star I/O + samples: `drivers/subtomo_merge.py:188-258`; `projects/try2_after_pixShift/External/job007/candidates.star`
- Curation siblings + canonical resolver: `services/visualization/picks_filter.py:146-269`
- IO-slot tiering (`prefer_if_exists`): `services/jobs/subtomo_extraction.py:65-77`; `services/path_resolution_service.py`
- Gallery UI: `ui/tomo_dashboard_dialog.py`
- Aggregation reuse: `drivers/subtomo_merge.py:348`; `ui/aggregation_merge_card.py`; `services/aggregation_discovery.py`
- Handedness watch-out: `drivers/template_match_pytom.py:154`; `rlnTomoHand` in `tomograms.star`

## References

- ArtiaX: github.com/FrangakisLab/ArtiaX — formats incl. RELION5 `.star`, `.coords`, `.em`;
  install via ChimeraX toolshed.
- ArtiaX#40 (RELION-5 save shift, unresolved) — the reason we use `.coords`.
- 3dem/relion#1280 (float vs int tomogram centering, 0.5-px) — the parity hazard we cancel.
- RELION-5 coords convention: relion.readthedocs.io → STA/Datatypes/particle_set,
  STA_tutorial/ImportCoords.
- ArtiaX papers: PMC9667824 (2022); ScienceDirect S1047847725000504 (2024, geomodels/tools).
- NextPYP ArtiaX guide: nextpyp.app/files/pyp/latest/docs/guide/chimerax_artiax.html
