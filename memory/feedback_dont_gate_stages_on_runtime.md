---
name: feedback_dont_gate_stages_on_runtime
description: "Don't hold a roadmap stage back waiting for runtime verification, and don't ask for per-stage checklists — build the whole arc, verify once at the end (user, 2026-08-17)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 4b717cb8-b8a6-4820-96d2-9161a86c1ed9
  modified: 2026-08-17T11:04:40.983Z
---

Keep executing roadmap stages back-to-back on code-review confidence. Do NOT stop at a
"destructive stage" boundary, do NOT ask the user to boot the app between stages, and do
NOT append per-stage runtime checklists as blocking asks. The user runs one verification
pass after **all** the relevant roadmaps are done, then several dedicated UI passes.

Verbatim (2026-08-17, after I held roadmap 11-S3 back because the replacing tabs had never
rendered): *"No worries about unverified things and you can definitely remodel harder
inside the denovo picking roadmaps if things indeed are scoped to change. I will take a
double look after we are done completely with all the relevant roadmaps and we will make a
few more passes over the UI."*

**Why:** the sandbox cannot run the app (see [[reference_hpc_env]] — `venv/bin/python3` is a
dangling symlink into unmounted `/software`, only `./venv/bin/ruff` works), so a
verification gate does not buy verification — it just stalls the arc and costs a session
per stage. The user is the only one who can run it, and he would rather do it once, with
the whole surface finished, than boot and click after every commit.

**How to apply:**
- Finish the queue. Ship each stage with its stage-log entry + `notes/<roadmap>-stage-snap/<stage>/`
  snapshot + `./venv/bin/ruff check .`; keep the runtime checklist in the doc as a record,
  not as a blocking ask in the reply.
- Deletion stages (e.g. 11-S3 stripping Journey affordances) are fair game — grep for zero
  callers, then delete.
- **Re-scope the doc, don't deviate silently.** He explicitly greenlit "remodel harder
  inside the denovo picking roadmaps if things indeed are scoped to change": when the plan
  no longer matches reality, rewrite that roadmap's section and say so in the stage log.
- Still true regardless: never commit for him ([[feedback_never_commit]]), never invent
  defaults ([[feedback_never_silent_defaults]]), write roadmaps directly with no plan
  ceremony ([[feedback_write_roadmaps_directly]]).
