---
name: project-public-release-roadmap63
description: "Public release plan for December 2026 (roadmap 63, decided 11-19): S1 and S2 done 11-20 (pushed 2dee937, settings on, first run read, fixes handed); #139 + security PRs wait for S3; next S3"
metadata:
  node_type: memory
  type: project
  originSessionId: b14df66b-8aa0-471d-9a7f-b4b20c23ddbf
  modified: 2026-11-20T11:10:00.432Z
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

Decisions 2026-11-19 (maintainer): D2 release folders + a `./crboost updates` CLI (status/fetch/install/use/clean); D3 the notes (memory/, docs/roadmaps/, docs/reports/) become local untracked files on the maintainer's machine, the maintainer's call not the PI's; erased from history by one filter-repo just before rc1, no GitHub Support request; D5 convert the 11 ui files, no --update; D6 .def + two-stage, CI builds every def (6 h ok, a few times a year), Munich gets images by Dropbox so S7 never holds the tag; D7 PyMOL removed (biopython in-process); D9 split in December; D11 local only, no outbound check; D12 'CryoBoost contributors'; D13 no NOT RUN triage, shipping some untested work is accepted; D15 the lab's GL ChimeraX image (ChimeraX may go later). After S9 lands, roadmaps and memory edits are no longer committed: leave them out of hand-off git commands.

Built 2026-11-20. S1 committed (`3dbf48f`, H32 `18b31c0`). The first three commits had swept in the EisGreis
session's napari switch through shared files; rebuilt from patches before the push
([[feedback_shared_checkout_commits_from_patches]]). `./crboost ci` (`1cf2493`); README without the agent API
(D16, decided 11-20, `9e0b9bf`); D4 `302f738` + `.git-blame-ignore-revs` `177810b`. D5, `.github/` (ci, weekly,
dependabot), `.gitleaks.toml`, install.sh's sha256-checked uv and the S2 record were handed as patches. The
branch_map worktree is superseded (main rebuilt the map; clean, never pushed). The repo is public already
(Klumpe-lab/crboost_server, an org: gitleaks runs as a binary, its action needs a licence there).
Pushed 11-20 at `2dee937` (H32's record fixed in it); every S2 repo setting on the same day, read back from the API.
First run read 11-20 (S2 record, "First run on GitHub"): ci failed only shellcheck (SC2154, the napari
`desktop_session.sh` ERR trap; the directive `curation_session.sh` has went in with the napari session's `7e150ba`,
ci green on `87a2a33`); serve smoke, install.sh under dash
and on AlmaLinux 9/10, gitleaks passed; lychee got 429 from deepwiki.com, now `--accept`ed; pip-audit fails until
S3 (issue #138 stays open). CodeQL: 17 alerts, for S4 to triage. Dependabot: #139 (NiceGUI 3.19, starlette 1.10
which drops `add_event_handler`, ruff 0.17.5's RUF071/RUF036) is S3's bump, left open; its lock passed. Dependabot
resolves on its newest Python (3.15), never reads uv.toml, takes only `--python-version` from the lock header;
security updates run as `pip` jobs outside dependabot.yml and fail until S3 drops the numpy/pandas cluster pins.
The lychee and dependabot.yml fixes and the record were handed 11-20 as two patches; check `git log` for whether
they landed. Next stage: S3 on its own worktree (NiceGUI 3.19 + starlette lifespan, ruff,
pins out; then see whether security PRs resolve).

**Why:** public rollout + the Munich site installing and updating on its own.
**How to apply:** read roadmap 63 §4.0 (build beside the lab: main stays lab-safe, test servers never on the lab's base /groups/klumpe/crboost_data, images staged outside sifs/, first trial pytom) and §7's stage records; hand commits as checked patches; build nothing before told. Related [[project_python_env_roadmap39]],
[[project_release_prep_roadmaps_2026-09-29]], [[feedback_write_roadmaps_directly]].
