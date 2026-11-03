---
name: reference_chimerax_save_dialog_last_dir
description: "ArtiaX 'Save particle list' = ChimeraX MainSaveDialog (QFileDialog) → opens in Qt's LAST-VISITED dir; cwd/`cd` steers only the first dialog of the process. After a scope switch the dialog lists the PREVIOUS seed → overwrite. Verified 2026-09-06 from sources + a real incident."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 8dffe8d2-4b7c-4e9b-9985-c10c9a0ba16f
  modified: 2026-09-06T12:38:23.715Z
---

**Fact (ChimeraX + ArtiaX sources, 2026-09-06):** ArtiaX's *Save particle list* button calls its
`show_save_file_dialog(session)` → `ArtiaXSaveDialog` (subclass of ChimeraX `MainSaveDialog`, a
QFileDialog) with NO `initial_directory`. Qt then opens in the process-global *last visited*
directory; only the very first dialog of the process falls back to `os.getcwd()`. The dialog's
list chooser (`SaveArgsWidget`) defaults to `session.ArtiaX.selected_partlist` — the list opened
last — so after a switch the NEW picks get written into whatever file the dialog shows, which is
the OLD scope's seed. `info models` prints `#id, <basename>, shown` per model; ArtiaX names a
`.coords` list after its file basename. The ArtiaX save command is `save <path> partlist #id`.

**Incident:** `denovo_picking_testdrive`, 2026-09-06 13:55 — switch to `newspecies` recorded, save
24 s later landed in `somespecies_denovo`'s seed; watcher re-ingested it as somespecies_denovo.
Both versions survive in `imports/` (every ingest archives the raw `.coords`).

**The lever (2026-09-06, later):** `QFileDialog.setDirectory()` on a hidden dialog SETS Qt's
process-wide last-visited dir. crboost writes `<curation_dir>/set_save_dir.py`
(`artiax_bridge.write_save_dir_script`), the `.cxc` ends with `runscript <it>`, and the switch
sends the same `runscript` as its own best-effort call (`save_dir_error` → warning toast).
RUNTIME-UNVERIFIED as of 2026-09-06 (13 §4 item 13 is the check; ChimeraX log line
`crboost: save dialog now opens in …`).

**How to apply:** never claim `cd` fixes the save folder. Reliable save targets: (a) the runscript
above, (b) the full path pasted into the dialog's name field, (c) `save <seed> partlist #id` run by
the USER in ChimeraX's command line (crboost shows it after a verified switch; never sends it).
The watcher flags an `off-scope` save (newer than the hot dir's `launched_at`, in another dir) in
red; `mark_hot` moves the hot dir at launch/switch (~7 s to a row update, was ~32 s) and the Picks
tab's Refresh = `watcher.sweep_now`. A hard guard (refuse ingest + move-to-scoped-seed +
restore-from-imports) is designed but NOT built — maintainer decision pending. See
[[project_curation_seed_arc]], [[project_artiax_bridge]].
