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
| — | [picking_ui/](picking_ui/00-overview.md) | index + the **guideline of record** for species-derived fields in job tabs; 07 = the deferred cross-project particle registry (scoping only, P-25) | — | 01–06 built and archived; 00 + 07 stay here as the live index and the deferred scope |
| — | [particles-ux-peeves](particles-ux-peeves.md) | living triage sheet for the maintainer's Journey/Species/roster walkthrough (bug / structural / cosmetic → home stage) | — | seeded 2026-08-16 (P-01…P-08); walkthrough intake 2026-08-18 (P-10…P-25, homed in picking_ui/) |
| — | [ARC-RUNTIME-CHECKLIST.md](ARC-RUNTIME-CHECKLIST.md) | the one consolidated runtime script for roadmaps 07, 08–12 and de-novo S5/S6 | — | §0 static gates GREEN 2026-08-18; sections 1–5 (~40 steps) OWED |

## The open debt

**Everything archived is code-complete and unverified at runtime.** The standing decision (maintainer,
2026-08-17) is to build back-to-back and verify ONCE at the end rather than gating each stage, so the
whole species-registry arc plus `picking_ui/01–06` is sitting on code-review confidence.
`ruff check .` is clean; `python -c "import main"` and `python check_boundaries.py` were green on
2026-08-18 and should be re-run after each batch (they cannot be run from the assistant sandbox — no
interpreter on the mounted path).

**Not yet collected into any roadmap:** the roster / queue peeves, the Picks / Curation / Jobs tab
peeves, how de-novo picking actually feels once the jobs have been run, and the three items in
`q_important_ui_fixes.md` (beam-induced-motion connected graph, bring back the tomo-recon gallery with
Journey links, landing-page data-identification jank). They land in `particles-ux-peeves.md` first.

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
