---
name: project_per_ts_deletion
description: "Per-TS (lane) deletion from Journey + gallery zoom — NOT BUILT as of 2026-10-05; inventory done, design drafted, awaiting go"
metadata:
  node_type: memory
  type: project
  originSessionId: 75e22313-469d-437b-8d21-38cfe78659d8
  modified: 2026-10-05T16:54:16.844Z
---

Asked 2026-10-05 (the PI's UI peeves; the 3dmod chip, gallery zoom ←/→ and the projects deletion mode
were built that day, ruff-clean, NOT RUN). Per-TS deletion was investigated but not coded.

Design drafted: `services/ts_deletion.py` plan + execute (UI-free), dialog in `ui/ts_delete_dialog.py`,
trigger icon beside the Journey strip's exclude toggle and a Delete button in the gallery zoom header.
- Keep the TS in the registry but excluded (reason "deleted"): removing it makes `preflight_registry`
  fail any re-run whose input still lists it (fsMotion enumerates from the Import star). Leave Import,
  `mdoc/`, `frames/` alone ("up to fsMotion"). Clear its registry outputs per trashed job.
- Per job: trash `.staging/task_<ts>/`, `task_<idx>.out/.err` (idx = manifest position), `tilt_series/<ts>.star`,
  exact per-job names (fsMotion per `Frame.id`: xml, average/{,even/,odd/}.mrc, average/_motion.json,
  powerspectrum, logs; align/missAlign xml + tiltstack/<ts>/; recon `{ts}_[\d.]+Apx` fullmatch incl.
  even/odd/ctf/deconv/png/_cbdisp; TM/CE tmResults/<ts>{,_scores,_angles,_job,_particles}; CE vis/preview/<ts>/,
  imod coords_<ts>; subtomo Subtomograms/<ts>/), flip `.task_status/<ts>.*` → `.skip`, drop rlnTomoName rows
  from top-level stars (originals copied to Trash first), drop the key from vis/preview/manifest.json and
  curation_reviewed.json. Pick lists of the tomo + Curation/<sp>/<tomo>/ trashed; MERGED lists kept.
- Everything to `Trash/tilt_series/<ts>_<stamp>/<rel path>`; symlinks moved as links; refuse any path whose
  parent's real path leaves the project. Block while any project job is queued/running, and for the last TS.
- Kept as mixed products: denoisetrain model, missAlign model.ckpt, reconstructParticle, class3d.
- Prefix hazard: `..._Position_1` vs `..._Position_1_2` coexist — never glob `<ts>_*`. Per-job templates
  belong in a `JobSpec` field (check_boundaries R4 forbids JobType-keyed tables elsewhere).
Open for the user: Trash vs hard delete; deleted lane stays visible muted vs hidden.
