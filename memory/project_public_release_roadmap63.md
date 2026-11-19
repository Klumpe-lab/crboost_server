---
name: project-public-release-roadmap63
description: "Public release / deployment plan for December 2026 (CI, CalVer tags, release folders, containers on GHCR, Munich pass); roadmap 63 WRITTEN 2026-11-19, D1-D15 owed, nothing built"
metadata:
  type: project
---

2026-11-19: the maintainer asked for a plan to make crboost release-ready by December 2026 (free GitHub CI/CD,
release + tag schedule, repo cleanup, QoL, and above all building and serving the tool containers; uv). An 8-lens
read-only recon ran over two sessions; all 8 are in `docs/reports/public-release/recon-2026-11-19.md` (rebuilt by
`dump_recon.py` from the first session's workflow journal plus three re-run lens JSONs, scrubbed of home paths).
`docs/roadmaps/roadmap_63-public-release.md` is written: §1 what there is, §2 now (outside stages), §3 decisions
D1-D15 with recommendations, §4 stages S1-S10 + a calendar (rc1 week of 7 Dec, v2026.12.0 week of 14 Dec),
§5 the Munich install pass; checked once adversarially (20 corrections folded in: NiceGUI target 3.19.x, no-nvcc as the publish rule, rc tag sorting, tags via `<root>/repo.git`, a December/January cut line). Recon ids: hygiene's are HY1-HY17 (renamed from H*, which clashed with HOTFIXES). It folds in roadmap 39 S4/S5, roadmap 20 §1.7/S5, roadmap 23 stage 6 (pointers added).

Key facts that shaped it: GHCR caps a layer at 10 GB and an ORAS-pushed SIF is one layer (relion/warp/isonet too
big until two-stage); the CUDA EULA keeps nvcc internal (publish runtime/base final stages); ChimeraX never
published; every SIF embeds its def (identity check without rebuilds); the lab install was found mode 777 under a
777 group dir (flagged to the maintainer, not changed).

**Why:** public rollout + the Munich site installing and updating on its own.
**How to apply:** next session, take the maintainer's answers to D1-D6 (they gate S2, S6-S8), then build from S1
in order; build nothing before told. Related [[project_python_env_roadmap39]],
[[project_release_prep_roadmaps_2026-09-29]], [[feedback_write_roadmaps_directly]].
