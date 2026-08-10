# Refactor roadmaps

Derived from `docs/architecture-assessment-2026-08-10.md` (full audit with file:line citations) and
the maintainer's feedback on it. Each roadmap is independently executable; suggested order below, but
only 00 blocks anything else.

| # | Roadmap | Theme | Risk |
|---|---|---|---|
| 00 | [deletions-and-lint](00-deletions-and-lint.md) | Delete dead code, tighten ruff, fix 3 tiny bugs | none (pure deletion, verified) |
| 01 | [service-boundary](01-service-boundary.md) | Facade becomes real; QC/visualization processing gets its own home; undo the services→ui inversion | low (mechanical moves) |
| 02 | [tiltseries-single-source](02-tiltseries-single-source.md) | Consolidate on the TiltSeriesRegistry; kill regex re-derivation; typed InstanceId | medium (parity-gated) |
| 03 | [errors-and-results](03-errors-and-results.md) | One lightweight result/error idiom + lucid logging | low |
| 04 | [driver-consolidation](04-driver-consolidation.md) | ArrayDriver template, ToolCommand, one status protocol | medium (behavior-preserving by construction, per-driver rollout) |

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
