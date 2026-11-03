---
name: folder-split-import
description: "2026-10-27 multi-folder SerialEM import — split question (project per folder / one project with folder-named series), 44-char creation gate removed, import_sources.json provenance; NOT RUN"
metadata:
  node_type: memory
  type: project
  originSessionId: c49f2cec-5b44-48d7-93cc-40b4939fa86b
  modified: 2026-10-27T07:55:53.202Z
---

Built 2026-10-27, NOT RUN. Trigger: /groups/klumpe/001_Data/20260821/frames holds two grid folders
(B1G4 63 mdocs, B2G2 146) sharing 53 `Position_N` names → Create blocked on "Unique tilt-series names".

- `ui/dataset_split_panel.py`: when `DatasetOverview.folder_name_collisions()` is non-empty, the landing
  form shows an explainer + segmented choice + one overview per folder (`folder_overviews()`, shared
  TiltSeriesInfo objects). Create blocked until the user picks; per-folder mode has a name field per folder
  (prefilled with the folder name) and creates N projects, then switches to the roster.
- One-project mode: `ts_names_by_folder=True` → series `<project>_<folder_tag>_Position_N`, frames prefixed
  the same (`import_ts_name` / `folder_tag` in mdoc_service).
- 44-char creation gate (`describe_long_ts_names`) deleted at user's request; relies on the ts_alignment
  alias (`staged_ts_name`), whose first real run is this project. known_bugs #3 updated.
- `<project>/import_sources.json`: per series the absolute source mdoc, layer, stack, and frame-link →
  source file. Registry schema NOT changed (extra="forbid" cross-branch risk, see [[p-bad-extra-forbidden]]).
- Selection cache now keyed by mdoc path (filename fallback).

**Why:** user wanted to stop "saving characters" and to be offered the split explicitly.
**How to apply:** runtime check owed — both modes on the 20260821 data; check alignment logs `runs as '<alias>'`.
