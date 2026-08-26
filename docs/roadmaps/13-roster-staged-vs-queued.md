# Roadmap 13 — Roster: staged vs queued

**Status:** scoped 2026-08-23 with the maintainer, **not implemented**. Comes from the roster/queue
walkthrough slot that `particles-ux-peeves.md` has been holding open since 2026-08-16 ("still
pending — the 2026-08-18 walkthrough covered Particles / Species / job tabs, not the roster").

Not derived from the 2026-08-10 audit. It closes a **semantic** gap: the roster tells the user
something that is not true, and the value it reads has no way of being truer.

## Before → After

**Before:** the moment you add a job in the pipeline builder its roster row lights the amber
"Scheduled" dot. That is the same dot, the same colour and the same word a job gets when it is
genuinely sitting in the SLURM afterok DAG waiting for nodes. Nothing on the row distinguishes
"I have staged this, it will run when I press Run" from "this is queued on the cluster right now".
The header reads `0/8` for a project where nothing has been submitted at all.

**After:** a staged job reads as staged — its own dot, and a header that says how many jobs are
waiting for the user rather than waiting for the cluster. Amber "Scheduled" means the cluster has
it. **Gain:** the roster stops claiming work is in flight when it is not, and "why has nothing
started" stops being a question the UI provokes.

---

## 1. The conflation (with evidence)

`JobStatus` has no value meaning *staged*:

| where | what it writes | what it means there |
|---|---|---|
| `services/jobs/_base.py:106` | `execution_status: JobStatus = Field(default=JobStatus.SCHEDULED)` | a brand-new job model — **staged** |
| `services/scheduling_and_orchestration/pipeline_orchestrator_service.py:234` | `job_model.execution_status = JobStatus.SCHEDULED` | just deployed to the schemer — **queued** |
| `services/scheduling_and_orchestration/pipeline_runner.py:276` | `new_status = JobStatus.SCHEDULED` for RELION `Pending` | in the pipeline star, not started — **queued** |
| `services/scheduling_and_orchestration/pipeline_runner.py:328/341` | reset of a job that left the pipeline star | back to **staged**, correctly |

The roster renders that one value directly: `RosterWidget._status_widget`
(`ui/pipeline_builder/pipeline_roster.py:167`) binds `execution_status` through `_dot_html`, and
`_DOT_COLORS[JobStatus.SCHEDULED]` is `#fbbf24` (`ui/status_indicator.py:8`). There is no branch
where a staged job could paint differently, because nothing tells the row it is staged.

## 2. It is derivable — no model change needed

The discriminator already exists in `ProjectState`:

- **`pipeline_active`** (`services/project_state.py:747`) — is a run live at all.
- **`pipeline_order`** (`services/project_state.py:729`) — the participating set, persisted at deploy
  (`pipeline_orchestrator_service.py:88`) and restored on load (`project_state.py:1339`).

so:

```
queued  ==  state.pipeline_active and instance_id in state.pipeline_order
staged  ==  execution_status is SCHEDULED and not queued
```

**Use `pipeline_order`, not the per-job fields.** The obvious discriminator —
`relion_job_name` / `slurm_job_id` — has a hole: `deploy_and_run_scheme` explicitly *nulls*
`relion_job_name` at `pipeline_orchestrator_service.py:234-236` and `sync_all_jobs` only refills it
on the next 3 s tick. For that window a genuinely-submitted schemer job carries SCHEDULED with no
job name and would paint as staged. `pipeline_order` is written before the branch (line 88) and has
no such window.

**What makes the whole thing safe:** staged and queued cannot coexist in a project.
`add_instance_to_pipeline` and `remove_instance_from_pipeline` both early-`return` on
`ui_mgr.is_running` (`ui/pipeline_builder/pipeline_builder_panel.py:270, 415`), and `is_running`
tracks `pipeline_active` within one poll tick (`status_poller.py:59-84`). So the split is
"before Run" vs "after Run" at the project level, never a per-job race.

**Legacy projects are fine.** `pipeline_order` is backfilled from `jobs.keys()` on load for projects
that predate P1.0 (`project_state.py:1339`). That over-populates it — but only matters when
`pipeline_active` is True, and a project that never ran has it False, so everything reads staged.
Correct.

**After a run finishes** `pipeline_active` goes False and any job still SCHEDULED reads staged again.
That is the honest answer: the schemer stopped before reaching it, or it was added afterwards, and
either way it is waiting for the user to press Run.

## 3. Why NOT a `JobStatus.STAGED` member

The obvious-looking route is the expensive one. ~20 sites read `SCHEDULED`, and two are load-bearing
rather than cosmetic:

- **`services/jobs/_base.py:423`** gates USER_PARAMS writes on `status in (SCHEDULED, FAILED)`. Miss
  it and every staged job's parameters silently become read-only — the same silent-drop class as the
  int-param corruption (`reference_int_job_param_corruption`), and just as hard to trace.
- **`pipeline_monitor._maybe_resume_deferred`** (`pipeline_monitor.py:268-300`) and the
  restart-recovery scan (`:152-165`) sweep every SCHEDULED job into a re-deploy set.

plus, cosmetically but wrongly: `path_resolution_service.py:521` (`source_in_flight` →
`expect_file_later`), `_LIVE_JOB_STATUSES` (`ui/particles/list_actions.py:59`),
`species_overview.is_live` (`services/particles/species_overview.py:64`), `dashboard_data.py:383,
426, 602`, `io_config_component.py:46`, `ui/species/picks_tab.py:115`.

And every existing `project_params.json` stores `"Scheduled"` for never-run jobs, so a new member
needs a load-time reclassification — which is §2's predicate anyway, just written into the model
instead of read at render. **Decision: derive it.** Revisit only if a consumer needs staged-ness
without a `ProjectState` in hand.

## 4. Stage 0 — gather first

Confirm at runtime before writing code (none of this is checkable from the sandbox):

1. Open a project with staged-but-never-run jobs. Confirm every roster row shows the amber dot and
   the header reads `0/N`.
2. Press Run. Confirm the same rows keep the same dot (i.e. that the current UI genuinely shows no
   transition) — this is the before-picture the stage is judged against.
3. After the run settles, add one more job. Confirm it paints identically to the finished ones'
   siblings.
4. Check `project_params.json` for `pipeline_order` on a legacy project (one created before P1.0) —
   confirm the backfill produced the full job list, as `project_state.py:1339` intends.

## 5. Stages

| stage | what | files |
|---|---|---|
| **S1** | `ProjectState.is_staged(instance_id)` implementing §2, with the `pipeline_order`-not-`relion_job_name` reasoning in its docstring. Interactive jobs (`IS_INTERACTIVE`) return False — they are never dispatched, and "staged" would misdescribe a TiltFilter awaiting user commit. | `services/project_state.py` |
| **S2** | Roster paints it: a distinct staged dot (hollow slate ring, not a filled amber disc) and the amber reserved for genuinely-in-flight. `_status_widget` currently binds `execution_status` alone via `bind_content_from`, so this stage is where the row stops being a pure one-field binding — keep the binding for the live statuses and gate the staged case above it. | `ui/status_indicator.py`, `ui/pipeline_builder/pipeline_roster.py` |
| **S3** | `RosterWidget.signature()` gains `pipeline_active` and the `pipeline_order` tuple. **Without this the roster will not repaint on the staged→queued transition** — the signature reads neither today (`pipeline_roster.py:186-247`). | `ui/pipeline_builder/pipeline_roster.py` |
| **S4** | `update_status_label` (`pipeline_roster.py:1382`) stops folding staged jobs into `done/total`; header reads `3/8 · 5 staged`. Decide there whether `total` means "selected" or "submitted". | `ui/pipeline_builder/pipeline_roster.py` |
| **S5** (optional) | One vocabulary: the same predicate behind `io_config_component.py:46` and `ui/species/picks_tab.py:115` so a staged producer does not read "scheduled" in the IO tab either. | those two |

S1+S3 commit together (the predicate is useless unpainted, the paint is broken unsignatured).
S4 and S5 are independent.

## 6. Adjacent findings — decide, don't drift

Surfaced while scoping; **not** in scope unless the maintainer says so:

- **The two early-`return`s are silent.** Clicking `+` or the row's remove icon while a pipeline runs
  does nothing at all — no toast, no disabled state (`pipeline_builder_panel.py:270, 415`). Users
  will read that as a dead button. A one-line `_safe_notify` each, or a disabled affordance in the
  roster, closes it.
- **`pipeline_order` is written but never read by any UI.** Nothing shows run membership. Once S1
  exists, "this job was not part of the last run" is one predicate away — worth a chip on rows that
  were added after a completed run.

## 7. Modern-Python weave-in

`is_staged` is a plain predicate — resist a `StrEnum` here, that is exactly the §3 route. If S5
happens and three surfaces need the same three-way answer, a `Literal["staged", "queued", "live"]`
returned by one function beats three call sites each re-deriving it.

## 8. Runtime checklist

1. Fresh project, add 4 jobs, do not run → all four show the staged dot; header `0/4 · 4 staged`.
2. Press Run → all four flip to amber/purple/blue within one 3 s tick **without a manual refresh**
   (this is what S3 buys; if they only change after a click, the signature is wrong).
3. Let it finish → succeeded rows green; any job the schemer never reached is staged, not amber.
4. Add a fifth job after the run → staged; the four settled ones unchanged.
5. Two browser tabs on the same project: press Run in one, confirm the other's roster transitions on
   its own poll.
6. Legacy project (pre-P1.0 `project_params.json`) → opens without every row claiming queued.

## 9. Open questions for the implementer

- Does `total` in the header mean the selected set or the submitted set? S4 has to pick one; the
  fallback path at `pipeline_roster.py:1391` currently counts selected.
- Should a FAILED job in a finished run read as staged-for-retry? Today it reads FAILED and
  `deploy_and_run_scheme` will re-submit it — arguably correct as-is, but the header's
  `done = completed + failed` then counts it as done while it is about to re-run.
