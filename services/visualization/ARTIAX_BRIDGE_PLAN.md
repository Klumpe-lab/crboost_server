# ArtiaX manual-picking bridge — design plan

Status: **Coordinate bridge VALIDATED; self-contained appliance + crboost one-click launch
WORKING — ChimeraX+ArtiaX renders over VNC end-to-end (2026-06-03).** Remaining: the curation
*workflow* (pre-load tomograms, ingest manual picks, merge) — see "## Post-launch curation
workflow roadmap" below. This doc is the contract.

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
3. **`.cxc` generator** in `artiax_bridge.py` + **Tier-2** one-click launch for a specific tomo
   (worker takes a `.cxc` to open). [the seamless core]
4. **Ingest** (`.coords` → `ManualPicks` star + registry) + gallery "Import" button. [closes the loop]
5. **`_combined` sibling** + resolver tier + staleness. [downstream, zero driver changes]
6. **Tier-3** `crboost` ChimeraX bundle + session manifest for the blitz. [elaborate endgame]
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
