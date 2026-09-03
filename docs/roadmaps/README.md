# Refactor roadmaps

Derived from `docs/architecture-assessment-2026-08-10.md` (full audit with file:line citations) and
the maintainer's feedback on it, plus the UX walkthroughs since. Each roadmap is independently
executable and every stage is a self-contained commit.

**Completed roadmaps move to [archive/](archive/README.md)** — 00–04, 07, 08–12, `denovo_picking/`
and `picking_ui/01–06` all live there now, with their stage logs intact. This file lists only what is
still open.

## Live

| # | Roadmap | Theme | Risk | Status |
|---|---|---|---|---|
| 05 | [per-ts-top-up](05-per-ts-top-up.md) | Re-run one failed/skipped tilt-series without recomputing the job or its downstream chain | medium (capability gap; index-shift hazard gates the cascade stage) | stage 0 partial 2026-08-13 (aggregation contract + `is_excluded` round-trip verified); no code |
| 06 | [dl-tilt-filter](06-dl-tilt-filter.md) | DL tilt filter as always-interactive first-class method; hard approval barrier | medium (orchestration barrier + resume) | scoped 2026-08-11, revised same day; not started |
| 13 | [roster-staged-vs-queued](13-roster-staged-vs-queued.md) | The roster paints a just-added job with the same amber "Scheduled" dot as one queued on the cluster; derive the split from `pipeline_active` + `pipeline_order` rather than adding a `JobStatus` member | low (render-only; no model or migration) | scoped 2026-08-23; not started |
| 14 | [protocols-regression-harness](14-protocols-regression-harness.md) | Protocols (portable declarative workflows: schema · export · apply · RELION-scheme derivation) + the copia EMPIAR-12580 regression harness (Tier B end-to-end runner with PASS/DRIFT/FAIL/INFRA verdicts, record→bless bands, landing-page button; Tier A materialized-scheme snapshots) | medium (new services + CLIs; one behaviour change: dead `config/Schemes/` path deleted) | **S0–S7 CODE-COMPLETE 2026-08-27**, PENDING RUNTIME; the runner inversion + workspace introspection continue in 16 |
| 15 | [job-dir-ownership](15-job-dir-ownership.md) | An `External/jobNNN/` dir has no owner: every liveness guard is in one server process's RAM, so a retry can 'sbatch' a second supervisor into a live job dir and 'rmtree' its working set mid-run (reproduced 2026-08-28, 62/114 TS lost to a bogus "producer drift"). On-disk ownership lock + atomic input rebuilds + markers that only the owner may write | medium (touches the supervisor entry path and the retry launcher; no model or migration) | scoped 2026-08-28 from a production failure; not started |
| 16 | [protocols-workbench](16-protocols-workbench.md) | A protocol run becomes a PROJECT (`protocol_origin` + `<project>/protocol/`); the harness splits into `launch_run` / `evaluate_run` / CLI-only `wait_for_settle` so the server's PipelineMonitor is the single reconciler; a foot-of-rail status light opens a full-width Protocols view (stages with dispatch params, live status, verdicts, metrics, checks, failure signatures, logs, files, "edited" chips, run history, launch / stop / evaluate / promote-to-baseline / save-as-protocol) | medium (runner restructure; schema 3.7 additive; CLI run dirs lose one level) | S0 2026-09-03; S1–S5 CODE-COMPLETE the same day, PENDING RUNTIME (§9) |
| — | [picking_ui/](picking_ui/00-overview.md) | index + the **guideline of record** for species-derived fields in job tabs; 07 = the deferred cross-project particle registry (scoping only, P-25) | — | 01–06 built and archived; **wave 2 built 2026-08-21/22**: 08 (control vocabulary + workbench chrome), 09 (Picks & curation one surface), 10-S1…S3 (external-picker contract, Model B adopted; S4 napari gated) and 11-S1…S6 (pick viewer, full page + slim) are all CODE-COMPLETE and owe ONE runtime pass together. 12 (aggregate candidates) stays a stub — it is blocked on a maintainer decision about which job the flow spawns |
| — | [particles-ux-peeves](particles-ux-peeves.md) | living triage sheet for the maintainer's Journey/Species/roster walkthrough (bug / structural / cosmetic → home stage) | — | seeded 2026-08-16 (P-01…P-08); walkthrough intake 2026-08-18 (P-10…P-25, homed in picking_ui/) |
| — | [ARC-RUNTIME-CHECKLIST.md](ARC-RUNTIME-CHECKLIST.md) | the one consolidated runtime script for roadmaps 07, 08–12 and de-novo S5/S6 | — | §0 static gates GREEN 2026-08-18; sections 1–5 (~40 steps) OWED |

## The open debt

**Everything archived is code-complete and unverified at runtime.** The standing decision (maintainer,
2026-08-17) is to build back-to-back and verify ONCE at the end rather than gating each stage, so the
whole species-registry arc plus `picking_ui/01–06` is sitting on code-review confidence.
`ruff check .` is clean; `python -c "import main"` and `python check_boundaries.py` were green on
2026-08-18 and should be re-run after each batch (they cannot be run from the assistant sandbox — no
interpreter on the mounted path).

**Not yet collected into any roadmap:** the Jobs tab peeves and the
"don't touch this yet" items in `q_important_ui_fixes.md:60-66` (beam-induced-motion connected graph,
tomo-recon gallery with Journey links, landing-page data-identification jank, pre-populated per-TS
task rows, logs-tab width + copy button). They land in `particles-ux-peeves.md` first. The Picks /
Curation peeves and the de-novo-picking-feel intake arrived 2026-08-21 and are homed in
`picking_ui/08–12` (rows P-32…P-48).

**05 is not from the audit.** 00–04 derive from `docs/architecture-assessment-2026-08-10.md`; 05 comes
from a reproduced production failure in `/groups/klumpe/crboost_data/deadcode_test` (2026-08-11) and
fixes a capability gap rather than a structural one. It shares an enum and a state-machine phase with
`docs/task-status-state-machine-roadmap.md` — read that one first.

Shared rules for all roadmaps:

- **Gather before changing.** Every roadmap has a Stage 0 that collects the facts the later stages
  need. If a stage reveals a surprise, stop and record it in the roadmap file before proceeding.
- **Committable stages.** Every stage is a self-contained commit (or short series) that leaves the app
  runnable. No stage mixes a move with a behavior change.
- **Behavior changes are quarantined.** When a refactor would also *fix* a divergence (e.g. a driver
  that enumerates TS differently), the fix goes in its own commit, explicitly flagged, never folded
  into a mechanical move.
- **Runtime verification is the user's step.** Claude's sandbox ceiling is `ruff` + grep + reading;
  each roadmap ends with a short runtime checklist (which project to open, what to click, what to
  expect) for verification after `python main.py` restart.
- **Modern-Python weave-in.** Each roadmap has a section recommending which Python 3.11 facilities
  (Protocol, StrEnum, abstractmethod, discriminated unions, match, dataclasses) fit *that* refactor,
  so the new idioms arrive area-by-area instead of as a global style campaign.
