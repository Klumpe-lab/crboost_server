---
name: project_munich_handoff_2026-09-15
description: "First public share prep (Munich) — docs sorted (roadmaps/ completed/ reports/), munich_handoff_note.md, qsub.template.sh = production script, preflight.py rewritten minimal, requirements completed, 4 dead modules deleted, CLAUDE.md tracked; UNCOMMITTED, preflight never run"
metadata: 
  node_type: memory
  type: project
  originSessionId: 07578eac-4c63-45f0-92fc-11954ec4cd3d
  modified: 2026-09-15T09:09:43.381Z
---

2026-09-15 (branch k3_implementation, all uncommitted; backups in that session's scratchpad):
- docs: `docs/*.md` = reference; `docs/roadmaps/roadmap_*` open, `completed/` done+superseded, `docs/reports/<topic>/`
  (412-capsid, copia, journey-dashboard, orchestrator, architecture-audit, v1-parity incl. scripts). CTF outlier
  roadmap renumbered 18→19. Commit `bc2b41b` had deleted roadmaps/05 + picking_ui/ 00,07–13 — links point at
  their would-be homes; restore commands handed over, user undecided.
- `munich_handoff_note.md` (repo root, un-ignored in .gitignore alongside CLAUDE.md): venv at `<repo>/venv`
  (ours leans on module python 3.11.5 + arrow module for numpy/pandas/scipy/requests), qsub.sh edits, conf.yaml
  fields incl. silent `g`/`g2|g3|g4` fallbacks, hard-coded container binds (`container_service.py:201`),
  protocol asset paths/specs (11.80 Å/px), missing DL tilt-filter weights, pointer to the v1 audit.
- `config/qsub.template.sh` = our qsub.sh with modules/--exclude commented; `conf.template.yaml`
  `use_afterok_orchestrator: true`, cisTEM path genericized.
- `preflight.py` rewritten (~230 lines): create configs from templates, imports, Config load, tool paths,
  sbatch/apptainer on PATH, partitions vs sinfo, qsub marker block/exit. NOT RUN (no interpreter in sandbox).
- Deleted: services/container_service.py, services/parameters_service.py, ui/components/task_utils.py,
  ui/dashboard/data.py (repo ruff errors 58 → 5, the 5 pre-existing E501 in services/jobs/fs_motion_ctf.py).
- User deferred: 0.0.0.0 bind, duplicate-server recovery, DL weights, driver divergences
  ([[project_v1_driver_parity_audit_2026-09-15]]); user will delete knowledgebase_demo/ themself.
See [[feedback_public_repo_no_names_no_swearing]].
