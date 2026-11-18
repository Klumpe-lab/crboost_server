# Roadmap 57 — readable job folders

A project's `External/` holds `job002 … job055` and nothing else. To find the alignment, or the template match
for one species, you open job folders one by one until a `job.star` says what ran there. This roadmap adds a
symlink beside each job folder, named after what the job is, so `ls -l External` reads like the pipeline:

```
$ ls -l External
002_motion_ctf          -> job002
003_ts_import           -> job003
004_tilt_filter         -> job004
005_alignment_aretomo   -> job005
008_reconstruct         -> job008
009_denoise_train_cryocare -> job009
023_tm_ribosome         -> job023
024_pick_ribosome       -> job024
job002/
job003/
…
```

## 1. Today (checked 2026-11-18)

- The number is the only name. `_submit_chain` takes each fresh job's folder from `state.job_dir_counter`, which
  only goes up (`services/scheduling_and_orchestration/pipeline_orchestrator_service.py:649`,
  `rel = f"{category.value}/job{job_num:03d}"`); the Import job gets `Import/job001` the same way.
- What a folder holds is written down in two places only: the `--instance_id` in its `job.star` / `run_submit.script`
  (Trash already reads it from there: `services/project_trash.py:31`), and `ProjectState.jobs[*].relion_job_name`.
- The UI says "Alignment (job004)" (`instance_display_name`, `services/jobs/spec.py:372`), so going from the page
  to the folder is easy. Going from a folder to the job is not.
- Folders nothing points at any more: a FAILED job whose inputs changed starts over in a new folder
  (`pipeline_orchestrator_service.py:606`, the `start_over` loop), so its old folder stays, unnamed. The disk page counts these as
  "unlisted job dirs" (`services/project_disk_usage.py:193`).
- `default_pipeline.star` is not written under the afterok chain (`services/path_resolution_service.py:1777`), so
  RELION's own alias column is not something we can lean on. The link is RELION's alias idea done on disk only.
- `display_label` exists on every job model (`services/jobs/_base.py:87`) and nothing sets it.

## 2. Decided (2026-11-18)

- **Links in `External/` (and `Import/`), the real folders stay `jobNNN`.** Every path written into a star file,
  every `jobNNN` parser (`project_disk_usage._JOB_DIR_RE`, `analytics_store.py:223`, `sta_kitchen/service.py:1102`,
  `subtomo_link.py:96`, `scripts/clear_staging_duplicates.py:140`), Trash naming and the AreTomo name-length limit
  (memory `reference_aretomo_long_ts_name`) stay as they are. Renaming the folders themselves was rejected for
  exactly those reasons; a separate top-level `Jobs/` folder was rejected as one more place to look.
- **The name is generated: `NNN_<type>[_<qualifier>]`.** The folder number first, so `ls` sorts in run order and two
  jobs of the same kind never collide; then a short type slug; then the method or species when the job has one.
- **Only a folder a job in the pipeline owns gets a link.** A superseded attempt or a deleted job's folder has none.

## 3. The name

`NNN` is `relion_job_number`, three digits as in the folder. The type slug is a new `dir_slug` field on `JobSpec`
(`services/jobs/spec.py:56`), one per row, so the table lives next to `display_name`:

| JobType | slug | qualifier |
|---|---|---|
| importmovies | `import` | — |
| fsMotionAndCtf | `motion_ctf` | — |
| tsImport | `ts_import` | — |
| tiltFilter | `tilt_filter` | — |
| aligntiltsWarp | `alignment` | `alignment_method`: `aretomo` / `imod` / `relion` |
| missAlign | `missalign` | — |
| tsCtf | `ctf` | — |
| tsReconstruct | `reconstruct` | — |
| denoisetrain | `denoise_train` | `denoise_method`: `cryocare` / `isonet` / … |
| denoisepredict | `denoise_predict` | `denoise_method` |
| segment | `segment` | `method`: `easymode` / `ais_model` |
| subset | `subset` | — |
| templatematching | `tm` | species id |
| tmextractcand | `pick` | species id |
| filamentPick | `filament_pick` | species id |
| subtomoExtraction | `extract` | species id |
| warpExport | `warp_export` | species id when set |
| extractPickList | `pick_list` | species id |
| reconstructParticle | `recon_particle` | species id |
| class3d | `class3d` | species id |
| refine3d | `refine3d` | species id |
| maskCreate | `mask` | species id |
| postprocess | `postprocess` | species id |

Rules:
- One qualifier at most: the species id if the job has one, else the method field named above, else none.
  Species ids are already slugs (`copia_particle`, `mysdonut`).
- The whole name is lowercased, anything outside `[a-z0-9_-]` becomes `_`, and it is cut at 48 characters.
- A clone (`templatematching__2`) needs nothing extra: it has its own folder number.
- A method enum gaining a value needs no table edit: the slug is the lowercased value.
- If the user changes a job's method or species and it reruns in the same folder (a FAILED retry in place), the
  link is renamed on the next reconcile. Harmless: nothing reads the link.

## 4. How the links are kept

One module, `services/job_links.py`, with one public function:

`sync_job_links(project_dir, state) -> None` makes the links in `External/` and `Import/` match `ProjectState`:

1. Wanted: for every job with a `relion_job_name` whose folder exists, `<Cat>/<name> -> <jobNNN>` (relative target,
   so the project can move or be opened through another mount).
2. Ours: a symlink in `External/` or `Import/` whose name matches `^\d{3}_[a-z0-9_-]+$` and whose target is a bare
   `job\d+`. Nothing else is touched. Real folders are never touched, and a link a user made by hand with another
   name is left alone.
3. Ours but not wanted (the job was deleted, started over in a new folder, or renamed): unlinked.
4. Wanted but missing or pointing elsewhere: written as `.<name>.tmp` then `os.replace`, so a reader never sees a
   half-made link.

It runs off the loop (`asyncio.to_thread`): a `scandir` of two folders and a few `symlink` calls. A failure to write
a link (another user's folder without write permission, a read-only mount) is caught as `OSError`, logged once per
project at WARNING, and the run goes on: a missing link breaks nothing. No `ProjectState` field, no schema bump: the
links are derived and can be rebuilt any time.

Where it is called (each is where `relion_job_name` changes or a folder goes away):
- `_submit_chain`, after pass 3 commits the counter (the folders now exist), including the refused-midway case.
- `PipelineDeletionService._move_job_dir_to_trash` (`pipeline_deletion_service.py:154`): after the move, so the old
  link does not dangle.
- `pipeline_edit.remove_job` / `delete_job` (`services/pipeline_edit.py:312`, `:340`).
- Species rename, if species ids can change (check in S1; today they look fixed at creation).

## 5. Where links could bite

- **A path chosen through a link.** Someone browsing in the file picker or a terminal picks
  `External/023_tm_ribosome/…/x.mrc` as a template or an import. Stored like that, the `jobNNN` parsers above would
  not find the job in it. S2 makes such paths canonical where they come in; it must not call `Path.resolve()` on
  the whole path, since `frames/` and imports are symlinks to raw data on purpose. Only the one link component
  is swapped for its target.
- **Walkers of `External/`.** The disk sizer stats with `follow_symlinks=False`, so a link costs its own few bytes
  and lands in the `External` bucket, not twice. `project_delete.delete_path` lists only real folders
  (`is_dir(follow_symlinks=False)`, `project_delete.py:67`), so deleting a project never walks a job twice.
  `clear_staging_duplicates.py` globs `job*`, which a link name never matches. S1 greps for any other
  `iterdir`/`glob` over a category folder that would now see the links.
- **RELION opening the project.** RELION lists `External/` by `default_pipeline.star`, not by `ls`; extra entries
  are invisible to it.
- **Drivers.** They get real paths from `job_model.paths`; nothing on a compute node sees a link unless a user
  pasted one (S2).

## 6. Stages

**S1 — the links.** `dir_slug` on every `JobSpec` row; `services/job_links.py` (`link_name(job_model)` +
`sync_job_links`); the calls in §4. Check: make a fresh copia project from the protocol, Run through Reconstruct and
one TM, and read `ls -l External Import`: one link per job, run order, the alignment and TM qualified. Then delete a
job (it goes to Trash) and see its link gone; make a FAILED alignment start over in a new folder by resetting its
inputs and see the link move to the new number. The disk page's numbers for the project are unchanged.

**S2 — paths through a link come back canonical.** `canonical_job_path(project_dir, path)` in `job_links.py`: if a
component right under `External/` or `Import/` is one of our links, replace it with its target; nothing else
changes. Called where a user-chosen path enters project state: the local file picker's result
(`ui/local_file_picker.py`), template and mask registration, tomogram and segmentation imports, kitchen sources,
and the agent API's path arguments (`add_template`, `add_mask`, `import_tomograms`, `create_project` sources).
Check: pick a template through `External/0NN_…/` in the picker; the species page and `project_params.json` show
`jobNNN`.

**S3 — the UI uses the same name.** Wherever a job page shows or copies its folder, it shows the link path
(`External/005_alignment_aretomo`), so a path copied into a terminal reads the same as `ls`. The disk page names
"unlisted job dirs" by what ran there (the `--instance_id` read Trash already does), e.g. "earlier attempt of
Alignment (job010)". Check: copy a job's folder from its page, `cd` into it in a terminal; the disk page of a
project with a superseded attempt names it.

## 7. Open

- **D1 — existing projects.** As built in S1, a project gets its links at its next Run or delete. Running
  `sync_job_links` when a project is opened would give every old project links at once, but a read would then
  write into other users' projects, and the API reads any project. Default: no, links come with the next Run
  (memory `feedback_fresh_projects_not_backfills`). Say if open should do it for projects the user can write.
- **D2 — user labels.** If the UI ever lets a user name a job (`display_label`), the link could follow it
  (`012_imod-binned-retry`). Not in scope; the slot is the qualifier.
- **D3 — superseded attempts.** Decided: no link. If S3's naming on the disk page is not enough, they could get
  `NNN_<slug>_superseded` links instead.

## 8. Not here

- Renaming the real folders, or any change to what is written into star files.
- Writing `default_pipeline.star` again for the afterok chain (RELION's alias column would come with it).
- Folders outside `External/` and `Import/` (`TiltFilter/`, `MergedSources/`, `Curation/`): they are already named.

## 9. Pickup

Start with S1 on its own branch. Read §3 and §4, then `pipeline_orchestrator_service.py:563` (`_submit_chain`, the
pass 3 commit is where the call goes) and `pipeline_deletion_service.py:154`. S1 is one new module, one field per
`JobSpec` row and four call sites; S2 and S3 are independent of each other and can follow in either order.
