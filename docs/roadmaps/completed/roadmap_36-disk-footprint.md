# Roadmap 36 — disk footprint: see and clear the trash, and stop writing what nothing reads

**Status:** S1 (trash view) and S2 (no `.staging/` duplicates + one-off cleanup script) built 2026-10-18, NOT RUN.
S3–S5 proposed, not built; decisions owed in §4.
Measurements from the projects sizer's caches (`<project>/.disk_usage.json`, roadmap 25) and `du` on
AGG412 job directories, 2026-10-18.

## 1. Where the bytes go (the eight AGG412 projects, 17.7 TB)

| Bucket | Size | Of which |
|---|---|---|
| Motion & CTF | 6.70 TB | ~50 % `.staging/` duplicates (Grid2 job002: 191 of 381 GB); of the rest, 2/3 even/odd half-averages (32 MB per tilt each) |
| Template matching | 5.76 TB | score + angle maps, ~4 GB per tomogram per species |
| Reconstruct | 2.71 TB | 2/3 even/odd half-tomograms (`halfmap_frames` 1: 1 GB each) |
| Trash | 1.71 TB | removed jobs (SK 1.17 TB: its six particle jobs, removed 2026-10-18; Grid2 442 GB) |
| Alignment | 0.55 TB | ~50 % `.staging/` duplicates (Grid2 job007: 16 of 32 GB); AreTomo's `_aligned.mrc` (170 MB per TS) is read by nothing |
| Warp Export, subtomos, candidates, tsCtf, other | 0.3 TB | |

Removing a job never frees space: `pipeline_deletion_service._move_job_dir_to_trash` renames its directory
into `<project>/Trash/<Top>_<jobNNN>`, and nothing ever deleted it.

## 2. Stages

### S1 — Trash view on the projects roster (built 2026-10-18, NOT RUN)
- `services/project_trash.py`: `trash_summary` (entry count + Trash/ mtime, no sizing), `list_trash` (each
  entry sized as the sizer sizes it, its instance id from `run_submit.script`, largest first), `clear_trash`
  (deletes named direct entries only; a symlink is unlinked, never followed; carries on past what it
  cannot delete and names it).
- `backend.py`: the project scan adds `trash_count` / `trash_mtime`, and the trash's mtime counts as
  activity for the size re-measure; `list_project_trash`, `clear_project_trash` (off the loop). Emptying a
  trash drops it from the cached size at once (`DiskUsageService.trash_cleared`).
- `ui/projects_overview.py`: a trash chip on every row whose Trash/ holds something (full view: after the
  size; compact view: its own column) — the trash's size when the last measurement is newer than the
  trash, else the number of removed jobs. A click opens a dialog (page-layout parented, SingleFlight):
  the removed jobs with type, instance id, Trash/ directory, last-written date and size, measured afresh,
  and one danger button, "Clear trash · <size>". Inert in deletion mode.
- Not touched: anything a driver imports, so queued chains are unaffected.
- *Runtime check:* restart the server; the landing roster shows a chip on SK (1.2 TB), Grid2 (442 GB),
  FCIsolation, unmilled2 and the pilot; SK's dialog lists six jobs (Template Match ×2, Pick candidates ×2,
  Subtomo Extraction ×2) plus three Oct-4 preprocessing jobs; Clear trash empties `SK/Trash/`, the row's
  size drops by the trash at once and the chip goes.
- *Follow-up, built 2026-10-18, NOT RUN: deleting from the roster no longer blocks.* A confirmed project
  deletion takes the row out of the list at once and runs as a background task with its card in the tray
  (bottom-right). `services/project_delete.py` first renames the project to `.<name>.deleting-<hex>`, which
  every project scan skips, then deletes it job directory by job directory, project_params.json last; a
  deletion that fails, or is cancelled from its card, puts what is left back under the project's name. The
  trash dialog's Clear button no longer waits for the sizes: clearing closes the dialog, hides the chip and
  runs in the tray the same way. Tasks of no project show in every page's tray (`snapshot_for_project`).
  *Runtime check:* delete two projects back to back: both rows go at once, two cards count job
  directories, neither row comes back. Open a trash and clear it at once: the dialog closes, the chip goes,
  the card ends "N jobs deleted". Cancel a large deletion from its card: the project returns to the list
  with what was left.

### S2 — No `.staging/` duplicates (built 2026-10-18, NOT RUN)
`drivers/fs_motion_and_ctf.py::collect_fs_outputs` copies each task's XMLs, averages and half-averages
out of `.staging/task_<ts>/` (`shutil.copy2`), and `drivers/ts_alignment.py` copies the tilt-stack dir
(`shutil.copytree`); the staged copies stay. Move them instead (same filesystem: a rename), or delete the
task's staged outputs after a successful collect. ~20 % of every project (3.6 TB here), and the collect gets
faster. Check first: nothing reads `.staging/` after its task (grep: only the drivers themselves); a failed
task re-stages from scratch either way.
- Built: both collects now move (`Path.replace`, a rename) instead of copying; the rest of
  `.staging/task_<ts>/` (frame symlinks, settings, tomostar, uncollected logs) stays.
- Existing projects: `scripts/clear_staging_duplicates.py [--delete] PATH ...` (stdlib only; PATH a job,
  a project or a directory of projects; dry run by default). Deletes a staged file only when the job is
  fsMotionAndCtf / aligntiltsWarp, the TS has `.task_status/<ts>.ok`, and the job dir holds a regular
  file at the same place (AreTomo alias mapped back) with the same size; prunes the dirs it emptied.
- *Runtime check:* dry run on Grid2 reports ~191 GB for job002 and ~16 GB for job007; after `--delete`, a
  downstream job (tsReconstruct) still runs on that project; a fresh fs_motion + alignment run leaves
  no files under `.staging/task_*/warp_*` and the job dirs complete.

### S3 — Half-sets only when something uses them (proposed; decision §4.1)
Only denoising reads the even/odd halves (cryoCARE, IsoNet; tsReconstruct `require_even_odd_halves`).
- tsReconstruct `halfmap_frames`: default 0 unless the pipeline holds a denoise job. 2 GB per TS (~11 %
  of a 412 project); adding denoising later costs a tsReconstruct re-run (~5 min per TS).
- Motion & CTF `out_average_halves`: same rule would save 64 MB per tilt (~2.5 GB per TS, ~14 %), but
  adding denoising later then means re-running motion correction on the raw EERs. Proposed: keep it on.

### S4 — Template-matching maps (proposed; decision §4.2)
- *Free maps* on a TM job: delete `*_scores.mrc` / `*_angles.mrc` once its extraction is settled (keeps
  `job.json` and the candidates). ~4 GB per tomogram per species; re-extraction afterwards needs a TM re-run
  (~15 min per tomogram on an A100). Roadmap 29 §8 item 6 is the same question for AGG412.
- Search the lamella slab only (PyTOM's search-region options; the slab from
  `services/analysis/lamella_slab.py`): Grid2's fitted slabs were 1190–2480 Å of the 3174 Å box, so ~40 %
  less disk and GPU time. Unverified: whether PyTOM then writes maps of the search region or of the whole
  tomogram.

### S5 — Delete for good when removing a job (proposed)
A "Delete the files now (skip Trash/)" checkbox in the remove-job dialogs, for jobs one knows are dead.

## 3. Not proposed
- A smaller tomogram Z extent: thick or tilted lamellae would be cut.
- float16 TM maps: PyTOM's extraction on mode-12 MRCs is untested; the angle map holds indices above
  float16's exact-integer range.
- AreTomo's `_aligned.mrc`: unused but ~1 % of the alignment job; not worth a change on its own.

## 4. Decisions owed
1. S3: tsReconstruct halves off unless denoising (proposed); motion-correction halves stay on (proposed).
2. S4: a per-job *Free maps* action (proposed) and/or slab-restricted TM (after the PyTOM check).
3. S5: go/no-go.
