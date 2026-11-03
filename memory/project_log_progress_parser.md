---
name: project_log_progress_parser
description: "services/log_progress.py — standalone run.out progress parser (missAlign, Refine3D/Class3D, reconstructParticle, denoise train, generic tqdm/keras/RELION bar); shown in projects roster Running column + job page header chip; BUILT 2026-10-20, NOT RUN"
metadata:
  type: project
---

Built 2026-10-20 on user request ("parse a status out of the tool stdouts … don't make things depend on it").
`services/log_progress.read(job_type, job_dir) -> LogProgress | None`: incremental per-file tail reader
(offset + facts cached, first read = last 2 MB, truncated/replaced run.out restarts), per-job-type parsers
keyed by JobType value, any other type → its last tqdm/keras/RELION bar. Parser bugs log once and return None.

Hooks (the only coupling): `backend._live_progress` sets `lp["log"]` for a running job WITHOUT per-TS counts
(array jobs keep ok/total); `ui/projects_overview._render_live_progress` shows `log.short` (amber when run.out
quiet > 10 min) + hover lines; `ui/pipeline_builder/log_progress_chip.py` in the job header, 15 s job-scope poll.

Formats were checked against real logs (Grid2 job022/036/032, copia-1015 job011, vreteno job012 cryoCARE,
LORENZ job022 IsoNet) with grep -P only — no Python run in the sandbox.

Same day, user: "too detailed" → only the WHOLE-job estimate (per-unit time × units left; Refine 3D and generic bars get none); cell = overall %; roster hover = `.cb-params-tt` key/value card with an overall bar (`_live_progress_card`).

**How to apply:** runtime check = landing Running column + a running job's header while missAlign/Refine3D run.
Array jobs' per-task logs are not parsed (by design: counts exist). Related: [[project_landing_overview_rework_2026-10-19]].
