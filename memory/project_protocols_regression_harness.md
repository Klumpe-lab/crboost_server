---
name: project-protocols-regression-harness
description: "Protocols (roadmap 16, re-scoped 2026-09-04): a bundle = pipeline shape + params; create a regular project from it, run it the regular way; workspace Protocols view = protocol | current params. The regression harness of roadmaps 14/16 was DELETED 2026-09-04."
metadata:
  type: project
  originSessionId: ebb39767-a336-47cc-94ac-b964e5dccf33
  modified: 2026-09-04T14:00:00.000Z
---

Document of record: `docs/roadmaps/completed/roadmap_16-protocols-workbench.md` (rewritten 2026-09-04). Roadmap 14 is
superseded (its harness deleted; its protocol core kept). See [[feedback-protocols-scope-no-harness]].

**What exists:** `services/protocols/{schema,discovery,export,apply}.py`; `crboost_protocol.py list |
validate | export | apply`; `config/protocols/copia-empiar12580/{protocol.yaml, assets/}` (11 stages,
importmovies → class3d, the CryoBoost v1 tutorial and nothing else — no denoising); `ProjectState.protocol_origin` {name, version,
bundle_dir, applied_at} + frozen `<project>/protocol/protocol.yaml`; `ui/protocols_dialog.py` (landing:
list · detail · **Create project** → `apply_protocol` → `open_project_in_workspace`, roster seeded with
the stages, normal Run); `ui/protocols_view.py` (switcher header · this project's params protocol |
current with "edited" · protocol as declared · save-as-protocol); foot-of-rail light dim / lit /
breathing-while-running.

**Deleted 2026-09-04:** `services/regress/`, `crboost_regress.py`, `scheme_export.py`, `test/` bundles,
`expects`/`schema_fingerprint`, `PipelineMonitor.is_running`, `LocalDataConfig`, every verdict /
record / bless / evaluate surface and word.

**Data:** the EMPIAR-12580 copia frames are on disk at
`/groups/klumpe/crboost_data/local_data/input/copia-empiar12580/` (29 files) — use them as the
movies/mdocs globs when creating a project from the copia protocol.

**Found on the first real run (2026-09-04):** `drivers/denoise_predict.py` wrote its `tomograms.star`
with an unnamed `data_` block (single-block `starfile.read` without `always_dict=True` drops the block
name) → template matching "No 'global' block". Fixed: read with `always_dict=True`, always write
`{"global": df}`.

**Runtime owed (16 §6):** static gates → landing Create project from copia → 13 staged rows, lit light →
Run → chain through class3d → Protocols view parameter tables; resolution in the class3d job tab.
Old run projects carry a frozen copy with `expects:` → "frozen copy unreadable" chip (delete
`<project>/protocol/` or re-create).

Related: [[project-orchestrator-replacement]], [[project-job-dir-ownership-and-ui-latency]].
