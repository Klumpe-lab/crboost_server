# Roadmap 63 — Public release: CI, releases, containers served, a second site installs from the README

Scoped 2026-11-19, NOT WRITTEN YET: recon only, nothing built. Target: pitch-perfect by December 2026, the Munich
site installing and updating without help.

**The ask (maintainer, 2026-11-19).** Of everything a real deployed software has, what is missing: free CI/CD on
GitHub, a release and tag schedule, what leaves the repo, quality-of-life, and above all how the tool containers are
built and served, and where uv stands. Deployment, not scientific correctness.

## Pickup (next session)

1. Raw recon: [docs/reports/public-release/recon-2026-11-19.md](../reports/public-release/recon-2026-11-19.md).
   4 of 8 lenses were in when the session hit its limit: `ci_feasibility`, `ci_release_research`, `onboarding`
   (a real fresh-clone `./install.sh` + preflight + static test), `containers_inventory`. Still running then:
   `hygiene`, `containers_research` (GHCR/ORAS limits, licences of redistributing CUDA/ChimeraX/PyMOL images),
   `prior_plans`, `ops_security`.
2. Collect the rest. If that session's workflow finished, re-run the dump (it reads the journal):
   `python3 docs/reports/public-release/dump_recon.py <journal> docs/reports/public-release/recon-2026-11-19.md`,
   journal = `~/.claude/projects/-users-artem-kushner-dev-crboost-server/a7285195-8201-418c-b704-c52a0c612678/subagents/workflows/wf_a6b8ee2c-6e2/journal.jsonl`.
   If not, resume it (finished lenses come from cache): Workflow `scriptPath`
   `~/.claude/projects/-users-artem-kushner-dev-crboost-server/a7285195-8201-418c-b704-c52a0c612678/workflows/scripts/public-release-recon-wf_a6b8ee2c-6e2.js`,
   `resumeFromRunId: wf_a6b8ee2c-6e2`, same args (`today` 2026-11-19, `scratch` = that session's scratchpad `/recon`).
   Resume works only in the same Claude Code session; in a new one, re-run just the four missing lenses.
3. Write this roadmap from the recon: §1 what there is, §2 decisions owed (with recommendations), §3 stages
   (committable chunks, December order), §4 the Munich install pass. Then one adversarial pass over its claims
   (file anchors, the web facts: GHCR layer limits, licences) before handing it over.
4. Before committing the report: it came from agents told to use roles, not names; grep it once for names and
   home paths anyway (public repo rule).

## What was known before the recon (checked 2026-11-19)

- GitHub repo public, Apache-2.0, 913 commits, pack ~29 MB; 0 releases, 0 tags, 0 Actions workflows, 0 packages.
- uv: done as roadmap 39 S0–S3, S6 (locked `requirements.txt`, env `.uv/env/<python>-<hash>`, uv 0.13.7 pinned in
  `uv.toml`). Roadmap 39 S4 (CI + pinned ruff; ruff is pinned now) and S5 (container `%labels`, weights sha256) are
  NOT built and fold into this roadmap or stay there (decide when writing).
- Containers: 15 `.def` in `container_defs/`, built by hand; images 0.5–13.4 GB on the lab filesystem
  (relion 13.4, warp_aretomo 12.7, isonet2 12.4, pytom 7.9, pymol 8.9, miss_alignment 7.6, cryocare 6.8,
  easymode 5.8 with NO def in the repo, tilt_filter 4.1, imod 1.2, chimerax 1.0, napari 0.5). cisTEM is a native
  binary dir. Several defs are legacy (pytom 0.10, relion5.0_tomo, isonet2.def, warp dev41 + AreTomo1).
- `docs/installation.md` "Which tool is which" points a new site at the LEGACY defs (`relion5.0_tomo`, pytom 0.10,
  `isonet2.def`, warp dev41 + AreTomo1), while the live `conf.yaml` runs the `*_sm100` builds. Fix in the docs stage.
- DL tilt-filter weights (`filterTilts/*.pth`, 45 MB) git-ignored, copied by hand; publishing them (Hugging Face
  likely) and the model author's licence still open (roadmap 39 D7).
- Settled, not to relitigate: one container per tool when pinnings differ; a newer tool version upgrades its
  container in place; `requirements.txt` lock rather than pyproject; per-user servers with tokens (no central VM);
  no automation around cluster-infra failures.
