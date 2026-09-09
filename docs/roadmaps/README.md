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
| 14 | [protocols-regression-harness](14-protocols-regression-harness.md) | Protocols (portable declarative pipeline shapes: schema · export · apply) — the copia EMPIAR-12580 bundle; the regression harness this roadmap also built was DELETED 2026-09-04 (see 16) | medium | S0–S7 2026-08-27; **superseded by 16 on 2026-09-04** — protocol core kept, harness deleted |
| 15 | [job-dir-ownership](15-job-dir-ownership.md) | An `External/jobNNN/` dir has no owner: every liveness guard is in one server process's RAM, so a retry can 'sbatch' a second supervisor into a live job dir and 'rmtree' its working set mid-run (reproduced 2026-08-28, 62/114 TS lost to a bogus "producer drift"). On-disk ownership lock + atomic input rebuilds + markers that only the owner may write | medium (touches the supervisor entry path and the retry launcher; no model or migration) | scoped 2026-08-28 from a production failure; not started |
| 16 | [protocols-workbench](16-protocols-workbench.md) | A protocol is the shape of a pipeline with its parameters: create a regular project from one (landing dialog) and run it through the regular pipeline; the workspace Protocols view shows the protocol beside the project's parameters ("edited"), the bundle as declared, and save-as-protocol; `ProjectState.protocol_origin` + a frozen `<project>/protocol/protocol.yaml` | small (services/protocols + two UI files; schema 3.7 additive) | re-scoped and cut back 2026-09-04; code complete, runtime §6 owed |
| 17 | [url-routing](17-url-routing.md) | Addressable URLs — `/p/<project>/<view>/<target>` for every view (job tab + subsection, journey on a TS, species tab, pick viewer, gallery, protocols): paste a link and land there, click around and the address bar tracks. Project identity = the directory name (+ `?base=` to disambiguate); History API only, never `navigate.to` | low-medium (no model, no migration — the deep-link callbacks all exist already; what is missing is the address bar) | scoped 2026-09-07; **S0–S5 code-complete 2026-09-08**, runtime §7 (15 steps) OWED |
| — | [picking_ui/](picking_ui/00-overview.md) | index + the **guideline of record** for species-derived fields in job tabs; 07 = the deferred cross-project particle registry (scoping only, P-25) | — | 01–06 built and archived; **wave 2 built 2026-08-21/22**: 08 (control vocabulary + workbench chrome), 09 (Picks & curation one surface), 10-S1…S3 (external-picker contract, Model B adopted; S4 napari gated) and 11-S1…S6 (pick viewer, full page + slim) are all CODE-COMPLETE and owe ONE runtime pass together. 12 (aggregate candidates) stays a stub — it is blocked on a maintainer decision about which job the flow spawns. **13 scoped 2026-09-04** — one pre-seeded ArtiaX list per (species, tomo), confirmed in-session switch over REST (reverses 10's Model-B "no outbound" clause), one door (Curate picks), spinner fixes; **S0–S4 CODE-COMPLETE 2026-09-04**, ONE runtime pass owed (13 §4) |
| 18 | [k3-stack-ingest](../../notes/k3_ingest/00-plan.md) | SerialEM/K3 frame-aligned MRC stacks as a data source: split utility upfront + SerialEM mdoc-dialect fixes in the scan; one project shape, no format switch | small (scan + one utility; `ts_reconstruct.py` switch fix touches every project) | dry run GREEN end to end 2026-09-09; waiting on dose from the acquirer; no code |
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
