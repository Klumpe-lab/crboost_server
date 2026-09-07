# 10 — The external picker: what ChimeraX can actually be told, and the contract that survives it

**Status:** S1–S3 CODE-COMPLETE 2026-08-22 (see §6), runtime pass owed (§5). S4 remains a gated
pilot — nothing built. Re-scoped in part 2026-09-04 by 13 (`13-seeded-list-and-session-switch.md`):
see the "[13 note]" paragraphs below — history is appended to, not rewritten.
Scoped 2026-08-21 from `q_important_ui_fixes.md:38-40` (+ the two session peeves at `:1-2`).
**Risk:** medium (S2 changes ingest semantics). **Depends on:** 09 for where the controls live.

**Directive (maintainer, 2026-08-21):** *"We somewhat started walking down the path of adding little
controls to crboost like 'save current picks in the control session' or 'load another tomogram into
current control session'… this is absolutely the wrong route and we should not try to bridge this
super brittle interface. If chimerax doesn't provide enough flexibility we can come up with some
'staging' mechanism where we deal with what chimerax provides and the user assigns a manually-picked
list artiax just produced to a particular species so there is absolutely no ambiguity. But frankly
I'm also starting to lean towards picking up napari."*

## 1. Assessment — the honest answer to "can we tell ChimeraX 'load tomo X, picking species Y'?"

**Load tomo X: yes.** The channel exists and works: ChimeraX runs `remotecontrol rest` in the
container (`containers/chimerax_artiax/curation_session.sh:78-79`), crboost drives it with
ssh-hopped `curl -G` (`services/curation/session_service.py:432-473`), and the swap command chain
(`close session ; artiax start ; artiax open tomo … ; open …coords ; cd <curation_dir>`,
`services/visualization/artiax_bridge.py:210-237`) is implemented and running today.

**"What we are currently picking is species Y": no — not inside ChimeraX.** ArtiaX has no species
concept. The session's scope lives entirely on OUR side, and it is held wrong in five compounding
ways (all current code):

1. A `.coords` file carries zero identity — three floats per line (`artiax_bridge.py:38-59`).
   Species + tomogram are inferred purely from the directory it lands in.
2. That directory is chosen by the **last REST swap**, from an in-memory dict
   (`_curation_loaded`, `session_service.py:681-687`) that is **empty after a crboost restart** and
   never populated by a `.cxc`-preloaded start (`:713-714`). ArtiaX keeps holding a tomogram the
   backend no longer knows about.
3. Pick a *second* species in the same session → its list is saved into the FIRST species' folder
   and registered as that species' manual list. Nothing checks, nothing asks
   (`session_service.py:558-575`).
4. Only the **newest** `.coords` in a folder is ingested — of N saved lists, N−1 are silently
   ignored (`artiax_bridge.py:186-192`, `watcher.py:200-209`).
5. Every ingest collapses to one slug `"manual"` per (species, tomo) — a re-import replaces it
   (`services/particles/ingest.py:19-36`); which ArtiaX list a file was is not recorded.

Plus the operational facts: a swap is ~20 s, compute-bound on the full-resolution recon
(`ARTIAX_BRIDGE_PLAN.md:412-440`, the proposed display-binned recon was never built), and the
round-trip edit of an existing list (W1) was never built either. **Conclusion: the REST bridge can
push commands fine; it cannot make the session's *meaning* trustworthy.** The maintainer's directive
is therefore adopted as the decision of record: no more outbound driving after launch.

## 2. The three models

| | A — status quo | **B — scoped launch + staging (adopt)** | C — napari picker (pilot) |
|---|---|---|---|
| Session | one per user, swapped via REST | one per user; **scope set only at launch** via `.cxc`; changing tomogram = relaunch or ArtiaX's own file dialog | launched per (species, tomo) task |
| Identity | inferred from last swap (fragile ×5) | **declared at launch, stamped in a manifest; anything else lands in the inbox and is explicitly assigned** | baked into the tool — it renders the scope banner and writes the manifest itself |
| Outbound cmds after launch | swap, save-partlist | **none** | none needed |
| Clunk | low (when it works) | medium — user saves by hand in ArtiaX; assignment dialog for strays | lowest possible — but must be built |
| Risk | silent misattribution | none silent — worst case is "please assign this" | Qt/GL on el7 nodes; a new container |

> **[13 note, 2026-09-04]** B's "outbound cmds after launch: none" is relaxed by exactly ONE command:
> 13-S2 reintroduces the confirmed in-session scope switch (`close session ; artiax start ; artiax
> open tomo ; open auto ; open seed ; lighting simple`, then `cd` as its own call) over the same
> quarantined `send_chimerax_command` channel. It is confirm-first, never auto-saves, and closes
> fragility #2 by rewriting `scope.json` + the target dir's manifest in the same call — so the
> session's scope on disk is the switch's scope, and a crboost restart still names it. *Restart on
> this tomogram* stays as the fallback and is the only path when `curation.rest_enabled` is off.

**Recommendation:** build B now (S1–S3, small, picker-agnostic — nothing in it is wasted if C wins),
pilot C behind the same contract (S4). Keep ChimeraX/ArtiaX as the shipping picker until the pilot
clears its criteria.

## 3. Stages

**S1 — tear out the bridge, keep the launcher.** Delete from the control center: "Load into running
session" (`ui/curation_session_dialog.py:259-265`, handler `:452-522`) and "Save picks now"
(`:294-299`, handler `:524-567`); delete `list_actions.load_tomo_into_session` and the two ⚡ call
sites 09 hasn't already removed; quarantine (not delete) the backend REST send path —
`send_chimerax_command` stays for launch-time health checks only, with a comment naming this
decision. The control center becomes: session status, VNC/tunnel info, Stop, and the save contract.
Fix the two session peeves while in the file: the dialog closes on Esc (it is page-layout-parented,
so wire the key handler explicitly), and `passwordless_vnc: false` becomes a `CurationConfig` option
(`SecurityTypes None`) — default OFF, tooltip states the risk on a shared cluster.

> **[13 note, 2026-09-04]** "quarantine the REST send path" holds, but the quarantine now admits two
> callers: launch-time health checks and 13-S2's `switch_session_scope`. Any further caller must
> record scope on disk in the same call, or it reopens fragility #2.

**S2 — the staging contract.** Launch is the one moment identity is declared: the curate action
(09's single affordance) writes `manifest.json` (`species_id`, `tomo_name`, `launched_at`, list of
reference exports) into `Curation/<species>/<tomo>/` beside the generated `.cxc`. Ingest hardening:
the watcher imports **every** new `.coords` in a scoped dir (not newest-only), one PickList per file
stem — `ingest.py` drops the one-`"manual"`-per-(species,tomo) collapse in favor of
`slug = "manual__<stem>"`, with a load-time migration for existing `"manual"` lists. A save that
lands OUTSIDE any scoped dir goes to the unattributed inbox as today (`watcher.py:213-237`), but the
inbox rows gain the explicit **assignment** action: pick species (+ tomogram if not inferable) → the
file is moved into the right scoped dir and ingested. That assignment step IS the maintainer's
staging mechanism — no ambiguity survives it. W1 (re-open an existing list for editing, re-ingest
under the same slug) rides on the same per-stem slugs and closes here.

> **[13 note, 2026-09-04]** "every `.coords` becomes its own list" stays the *mechanism* — the
> watcher still ingests every settled file, one PickList per stem. The *advertised* contract is
> narrower: one pre-seeded list per (species, tomogram), `<species_id>__<tomo>__picks.coords`, created
> 0-byte and registered as a 0-pick `picks` row at the *Curate picks* click, opened LAST by the
> `.cxc` so it is the selected list when the user arrives. Extra lists still work; the UI stops
> advertising them and the naming/placing burden for them is the user's.

**S3 — make launch fast enough to not miss the swap.** The display-binned recon from
`ARTIAX_BRIDGE_PLAN.md:412-440`: generate (lazily, cached beside the recon) a bin×2/×4 copy for the
`.cxc` preload; full-res stays one ArtiaX click away. Cuts the 1 GB/20 s load to ~1–3 s and makes
"relaunch to change tomogram" tolerable, which is what lets B live without the swap.

**S4 — napari pilot (gated: maintainer greenlights before any build).** Scope: one apptainer image
(napari + PyQt5 — PyQt5, not Qt6, to dodge the el7 kernel-3.10 ABI-tag trap; reuse the VirtualGL /
partition-g scaffolding proven for the ChimeraX sif) and one bespoke dock widget: scope banner
("picking <species> on <tomo>"), ortho-slice viewer of the display-binned recon, one Points layer
per species with the registry color, Save button that writes `.coords` + manifest into the same
scoped dir — same watcher, same contract, zero new ingest code. Success criteria, measured on a g
node over VNC: launch ≤ current session launch; slice scroll usable (<150 ms perceived); the
save→ingest loop produces a correctly-attributed list with no manual assignment; delete/move of a
pick is two clicks or less. Explicitly NOT a volume renderer — slices only (house rule: no heroic
3D in constrained environments).

## 4. Open questions (for the discussion the maintainer asked for)

- Does "relaunch per tomogram" feel acceptable once S3 lands, or does B need a sanctioned
  "open another tomogram" path *inside* ArtiaX (user does it by hand; the scoped-dir contract
  catches the saves via the inbox)?
- Session-per-(species,tomo) vs per-user-with-declared-scope: S2 assumes the latter (cheaper on
  SLURM). If misattribution still bites, the former is the stricter fallback.
- Is the napari pilot worth doing before or after 11's viewer rework? They are independent; the
  viewer rework does not wait.

## 5. Runtime checklist (maintainer)

1. Control center: no Load/Save buttons; **Esc closes it**; Stop still sweeps the session.
2. Launch curate on (species, tomo): `Curation/<sp>/<tomo>/manifest.json` exists and names both,
   and gains `launched_at` + `slurm_job_id` once Start goes through. Save two differently-named
   lists in ArtiaX → BOTH appear as separate lists on the Picks & curation tab, labelled by file.
3. Save the SAME name a second time (having moved a pick) → the same list updates; no second row.
4. Save a `.coords` into the wrong folder deliberately → it shows under UNATTRIBUTED SAVES with the
   reason and the file names; `assign ↗` → pick species + tomogram → file relocated, list imported,
   the row disappears on the next full sweep (~30 s).
5. Restart crboost mid-session → nothing misattributes, and the control center still names the
   running session's scope (that is `scope.json` doing its job, not an in-memory dict).
6. S3: first curate on a fresh tomogram generates `<recon>_cbdisp2.mrc` (say so — it is the slow
   part); second launch reuses it; ArtiaX load feels seconds, not tens of seconds. **Coordinate
   check, the one that matters:** place a pick on a recognisable feature in the display copy, let
   it ingest, and confirm on the full-res volume (3dmod / a re-export) that it sits on the same
   feature — the `(N-1)/2·px` term is applied, so a systematic half-block offset would mean it is
   being applied twice or not at all.
7. With a session live on tomogram A, press `curate` on tomogram B → the panel warns that the
   session's scope differs and offers **Restart on this tomogram**; confirming lands B.
8. Optional, per site: set `curation.passwordless_vnc: true` → the desktop comes up with no
   password and the Connect section says why that is a risk.
9. A project with pre-10 `manual` lists: on load, the server log shows `Migrated pick list …:
   manual -> manual__<stem>` and the tab still shows the list, its authoritative radio, and its
   extraction state.
10. `ruff check .` + `python check_boundaries.py` + `python -c "import main"`.

## 6. Log

- 2026-08-21 — assessed and scoped. Decision of record: adopt Model B (scoped launch + staging
  inbox + explicit assignment; no outbound REST after launch), napari as a gated pilot behind the
  identical contract. The five-way identity fragility documented in §1 is the evidence base.

- 2026-08-22 — **S1 + S2 + S3 CODE-COMPLETE** (S4 untouched: still gated on a greenlight).
  `ruff check .` green; `check_boundaries.py` and `import main` still owed (no runnable python in
  the agent sandbox). What landed, by stage:

  **S1 — bridge out, launcher kept.** Deleted `CurationSessionService.load_into_session` /
  `save_curation_picks` / `save_session_particle_lists` / `get_curation_loaded` /
  `loaded_curation_dirs` and their four `backend.py` delegations, `artiax_bridge.
  swap_chimerax_commands`, `list_actions.load_tomo_into_session`, and the control center's Load /
  Save buttons with their handlers. `picks_tab.curate` lost its liveness branch — one path, always
  `curate_in_artiax`; the `unknown`-liveness hazard goes with it. `send_chimerax_command` stays,
  docstring-quarantined to launch-time health checks, and `curation.rest_enabled` was repointed to
  gate only that. `ui/tomo_dashboard_dialog._handle_open_list_in_artiax` + `_artiax_inputs` were
  kept until now as "the W1 foundation" and are deleted: S2 closes W1 a different way.
  Both session peeves: the panel is `no-backdrop-dismiss` instead of `persistent`, so **Esc closes
  it** (Quasar does the dismiss; a `on_value_change` hook runs the one teardown, so the poll timer
  and the per-client registry entry go with it) — *deviation from the stage text, which called for
  an explicit key handler; this is the smaller change and keeps stray backdrop clicks harmless*.
  `curation.passwordless_vnc` (default OFF) → `CX_VNC_NOPASS` → the worker starts Xvnc with
  `-SecurityTypes None` and mints no password file; the Connect section states the exposure inline.

  **S2 — the staging contract.** `manifest.json` per `Curation/<species>/<tomo>/`, written by
  `artiax_bridge.prepare_curation_bundle` (scope declared) and stamped with `launched_at` +
  `slurm_job_id` by `launch_curation_session` (scope committed); `scope.json` beside the session,
  merged into what both `find_active_curation_session*` return, so the control center can name the
  running session's scope **after a crboost restart** — the thing `_curation_loaded` could not do.
  Ingest: the watcher now takes EVERY settled `.coords` per dir, not the newest; `ingest.
  manual_slug_for` mints `manual__<stem>` and `import_curation_picks` writes `<slug>.star`, so N
  saved lists are N lists and re-saving a name UPDATES that list (**that is W1, closed**).
  Attribution reads the manifest first and only falls back to directory-slug matching for pre-10
  dirs; a manifest naming an unregistered species is an error to report, never a reason to guess.
  The watcher's hot set is derived from manifest `launched_at` rather than from an in-memory record
  of what a session was told to load. `migrate_legacy_manual_slugs` runs in `ProjectState.load`
  (before `pipeline_order` is derived) and re-keys four things: the list, the authoritative choice,
  the per-list extraction instance, and any `source_overrides` value naming the old synthetic
  producer — files are deliberately not renamed. `list_admin.pick_list_files` now deletes only the
  `.coords` whose stem matches the list, and takes the extraction dir from the recorded
  `extracted_path`. The unattributed inbox carries its files and gained **assign** →
  `assign_unattributed_coords` moves them into the chosen scope and writes its manifest; neither
  dropdown is pre-selected.

  **S3 — display-binned recon.** `curation.display_bin` (default 2) → `ensure_display_recon`
  block-means the recon once, cached as `<stem>_cbdisp<N>.mrc` beside it (streamed per output
  Z-slab, unique temp + `os.replace`, far-edge crop). **The coordinate consequence is handled
  exactly, not approximated:** block-binning puts binned voxel `v` at full-res `v*N + (N-1)/2`, so
  `corner_full = corner_display + (N-1)/2·px`. That single constant is subtracted on export, added
  on import, and recorded per dir as `corner_offset_angst`; at N=1 it is 0.0 and every path is what
  it was. A failure to build the copy is never silent — `display_note` reaches a toast on `curate`
  and a line in the control center, and the session opens the full-res volume.

  **Added beyond the stage text, and why:** a **"Restart on this tomogram"** button, shown only when
  a live session's scope differs from the panel's. Without the swap, a live session on tomogram A
  left `curate` on tomogram B with no way forward but Stop-then-Start, and the panel never said so.
  It confirms first, because stopping the job drops whatever is unsaved and crboost can no longer
  save it for the user. This is the concrete answer to open question 1 in §4.

  **Known residual (not a silent one):** a `.coords` imported by explicit PATH from outside any
  curation dir is read as full-resolution coordinates, because nothing declares which volume it was
  picked on. If it was in fact a session save moved out of its folder, that is a `(N-1)/2·px` shift
  (~3.1 Å at N=2 / 6.2 Å per px). The import dialog states the assumption and points at the two
  routes that keep the frame exact (save into the folder, or assign from the inbox).

- 2026-09-04 — **re-scoped in part by 13.** Model B's "no outbound driving after launch" is relaxed
  by one confirmed command (the scope switch, 13-S2), the N-lists mechanism is kept but no longer
  advertised (one pre-seeded list per (species, tomo), 13-S1), and the control-center doors 09/10
  spread across the workspace collapse to the per-tomogram *Curate picks* button (13-S3). The
  "[13 note]" paragraphs above mark where this doc now lies; nothing here was rewritten.
