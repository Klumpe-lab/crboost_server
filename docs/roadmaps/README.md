# Refactor roadmaps

Derived from `docs/architecture-assessment-2026-08-10.md` (full audit with file:line citations) and
the maintainer's feedback on it. Each roadmap is independently executable; suggested order below, but
only 00 blocks anything else.

| # | Roadmap | Theme | Risk | Status |
|---|---|---|---|---|
| 00 | [deletions-and-lint](00-deletions-and-lint.md) | Delete dead code, tighten ruff, fix 3 tiny bugs | none (pure deletion, verified) | **complete** — all 5 stages landed + committed 2026-08-11 |
| 01 | [service-boundary](01-service-boundary.md) | Facade becomes real; QC/visualization processing gets its own home; undo the services→ui inversion | low (mechanical moves) | **complete** — all 8 stages done 2026-08-11 (`check_boundaries.py` enforces; runtime verification of stages 5–7 owed) |
| 02 | [tiltseries-single-source](02-tiltseries-single-source.md) | Consolidate on the TiltSeriesRegistry; kill regex re-derivation; typed InstanceId | medium (parity-gated) | **stages 0–6 done** — 0–5 committed + runtime-confirmed 2026-08-12, stage 6 (preimport fold + TomogramGeometry rename) code-complete same day; fresh-run parity harness still owed |
| 03 | [errors-and-results](03-errors-and-results.md) | One lightweight result/error idiom + lucid logging | low | **code-complete** — all 5 stages done 2026-08-12 (`ok()`/`err()` everywhere, census in `03-stage0-census.md`, swallow triage, `load_warnings`); runtime failure-path checks owed |
| 04 | [driver-consolidation](04-driver-consolidation.md) | ArrayDriver template, ToolCommand, one status protocol | medium (behavior-preserving by construction, per-driver rollout) | **stages 0–2 done + 4–5 done** 2026-08-13 — census + `TaskStatusStore` + all 14 `ToolCommand`/`run_tool` migrations, the 5 transcript-covered types byte-verified on a live run ([design](04-stage2-toolcommand-design.md), [transcripts](04-stage2-transcripts.md)); `BaseIngestAdapter` + `subtomo_merge`→`services/` + single-sourced driver invocation. Stage 3: `ArrayDriver` + `DriverContext` + ts_reconstruct + ts_ctf done (ledger #8 closed), 6 drivers left to migrate opportunistically. **Two open ASKs**: denoise/tilt_filter registry-stamp failure semantics (stage 4), `extract_pick_list` job identity (stage 5, blocked on census #68) |
| 05 | [per-ts-top-up](05-per-ts-top-up.md) | Re-run one failed/skipped tilt-series without recomputing the job or its downstream chain | medium (capability gap; index-shift hazard gates the cascade stage) | stage 0 partial 2026-08-13 (aggregation contract + `is_excluded` round-trip verified); no code |
| 06 | [dl-tilt-filter](06-dl-tilt-filter.md) | DL tilt filter as always-interactive first-class method; hard approval barrier | medium (orchestration barrier + resume) | scoped 2026-08-11, revised same day; not started |
| — | [denovo_picking/](denovo_picking/00-overview.md) | De-novo species + manual picking; tomo-import hardening; project-type dissolution; aggregation factor-out | medium (dashboard inversion, parity-gated) | **spine S1→S2→S3 code-complete** 2026-08-13 (species creation + registry hygiene; `TomoGeometry` provider + Particles-panel inversion + ArtiaX decoupling; candidate-free extraction + resolver injection + the `384/1.0/224` default killed) — **runtime owed on all three**, no code left on the spine. S4/S5/S6 trailers not started. D-8 amended by S3 (producer id is species+tomo+slug scoped); supersedes `notes/PARTICLE_PROJECT_ROADMAP.md` |

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
