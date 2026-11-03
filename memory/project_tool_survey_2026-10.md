---
name: project-tool-survey-2026-10
description: 2026-10-10 decision — stay with PyTOM for picking, add easymode segmentation later; survey of M/TomoTwin/DeepFinder/Kaggle pickers in docs/reports/tool-survey/
metadata:
  type: project
---

Survey at `docs/reports/tool-survey/picking-segmentation-refinement-2026-10.md` (M, TomoTwin, DeepFinder, CZII
Kaggle pickers, easymode, MemBrain-seg). Maintainer decision 2026-10-10: keep PyTOM as the picker; add easymode
later; no roadmap for any of it. The lab will soon target many particle types plus membranes and organelles.

**How to apply:** don't propose switching pickers; easymode integration is the next tool addition when asked.
M is a candidate future refiner via `JobSpec.refines` after the Warp export lands. Related
[[project-particle-analytics-roadmap30]].
