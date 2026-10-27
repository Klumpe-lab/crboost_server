# Roadmaps

Forward-looking plans. Every file here is named `roadmap_*`; reference documentation lives in `docs/`
and investigations / handoffs / audits live in `docs/reports/`.

**This folder lists only what is still open** (not started, or partially built). Roadmaps whose stages
are all code-complete — and plans that were superseded — live in [completed/](completed/README.md) with
their stage logs intact.

## Pending — wanted, in this order

**Pending** marks a roadmap the maintainer has decided to build: it carries a `**Pending (maintainer, <date>):**`
line at the top of its file saying why and in what order. Everything under *Open* is scoped but not scheduled.

| # | Roadmap | Theme | Status |
|---|---|---|---|
| 32 | [staging-inputs-and-subsets](roadmap_32-staging-inputs-and-subsets.md) | Branching: inputs fixed at submit, staging while running, the Subset job, Fork…; **job liveness** (was 33, merged 2026-10-26 as §14: one supervisor per job dir, live = QUEUED or RUNNING); **the UI is the hard part** (§15: a branched, interactive roster and an operational TS × job view) | Pending 2026-10-26, own worktree. S1–S3 + pipeline table on main (NOT RUN; S2 shipped before its liveness prerequisite); next §14 L4 + L1, then the §15 UI design, then S4 Fork; §9.5 decided (a Subset goes to the same project) |
| 23 | [per-user-servers](roadmap_23-per-user-servers.md) | One server per person with Jupyter-style tokens: token gate, then a single-writer lease, read-only viewing, a launcher | Stage 1 (token gate) on main 2026-10-15; the Munich slice (file modes, version-skew warning, `saved_by`) built 2026-10-26, NOT RUN; stages 2–7 = multi-user CBE work, not pending |
| 39 | [python-environment-and-setup](roadmap_39-python-environment-and-setup.md) | A locked environment built with uv on its own Python, the tilt filter's torch in a container, CI; §6 the start-up cost of every array task, queued jobs and code version | Pending 2026-10-26; nothing built; S0 inventory command owed; decisions owed §2 |
| 27 | [ts-axis-pipeline](roadmap_27-ts-axis-pipeline.md) | The pipeline as jobs × tilt series: failure isolation (1), catch-up without recompute (2), per-TS execution / streaming (3, now holding 32's old §7 assessment and a §12 vision) | Pending 2026-10-26, **after 32**. Stage 1 landed 2026-10-04; 2a (restored TS never re-run) is a standalone fix any time; decisions owed §10 |

## Open — scoped, not scheduled

| # | Roadmap | Theme | Status |
|---|---|---|---|
| 05 | [per-ts-top-up](roadmap_05-per-ts-top-up.md) | Re-run one failed/skipped tilt-series without recomputing the job or its downstream chain | stage 0 partial 2026-08-13; no code. **File deleted in `bc2b41b` — restore pending**. Scope absorbed by 27 (its stage 2, catch-up = per-TS top-up), 2026-10-04 |
| 19 | [ctf-fit-outlier-check](roadmap_19-ctf-fit-outlier-check.md) | Per-tilt CTF-fit outlier check: fail loudly when Warp's defocus fit diverges (was numbered 18, which collided with k3-stack-ingest) | scoped 2026-09-09; not started |
| 21 | [miss-alignment-and-warp-export](roadmap_21-miss-alignment-and-warp-export.md) | Miss-alignment as a declared refinement stage the resolver follows (no other job changes); drop-in output; Warp particle export only if its local-deformation grids prove useful | stages 1–3 code-complete 2026-09-29 (first run done); stage 4 landed with 24; stages 5–6 continue in 24; stage 7 (§7, the Journey view of what a run changed): 7a Refinement section + warp strip and 7b blink BUILT 2026-10-27, NOT RUN; 7c open |
| 30 | [particle-analytics-and-set-tools](roadmap_30-particle-analytics-and-set-tools.md) | The particle stage's at-a-glance view: scores in σ, TM health, σ ladders with null and decoy curves, spatial QC, Class3D readouts, statistical controls (decoy template, random-position decoys, σ-bin reconstructions, recall), and set tools (rule filters, class subsets, splits, algebra, lineage) | scoped 2026-10-10; stages 1–5 built the same day, NOT RUN; stage 6 services built, its UI owed; absorbs 29's C3/C4 |
| 35 | [resizable-views-and-quiet-journey](roadmap_35-resizable-views-and-quiet-journey.md) | Drag the boundary between workspace views; the close cross without its full-height gutter; the Journey in the Tilts tab's language (one font pair, house controls only, one chart tile size, grey markers), in three passes behind a temporary `style` dropdown | scoped 2026-10-17; §3 decided (main tree, `localStorage`, species colours per pass, app-wide monospace out); S1–S4 built 2026-10-17, NOT RUN; S5 waits for the pick between the three passes |
| — | [orchestrator-replacement](roadmap_orchestrator-replacement.md) | `relion_schemer` out → ProjectState + SLURM afterok DAG + thin reconciler | the schemer is gone (2026-10-15, NOT RUN; roadmap 32 §13): R1 afterok unconditional, R4 schemer code deleted, D4 and C5 done; left: R2 the RELION export (C1–C4, C6, C7) and the rest of R3 (D1, D3, D5) |
| — | [registry-consolidation](roadmap_registry-consolidation.md) | TiltSeriesRegistry becomes THE store | partial: phases 0–2 built; phase 4 (de-star the drivers) not done |
| — | [tilt-filter-tomostar-relion-split](roadmap_tilt-filter-tomostar-relion-split.md) | Tilt-filter verdicts propagate through the tomostar / RELION star split | partial: stage 1, 2A + emit_star fix runtime-verified; 2B, 3, §5 open |
| — | [task-status-state-machine](roadmap_task-status-state-machine.md) | `.ok/.fail/.skip` markers → one JSON task record | partial: containment landed (`is_superseded_task`); §5 JSON record not built |
| — | [preprocessing-metrics](roadmap_preprocessing-metrics.md) | Warp/AreTomo readouts surfaced in the UI (catalogue: `docs/preprocessing-metrics-inventory.md`) | partial: ①–④ + ⑥ code-complete, pending runtime; ⑤ exposure-curation scatter not built |
| — | [picking_ui/07](picking_ui/roadmap_07-scoping-particle-registry-edges.md) | Deferred cross-project particle registry (scoping only, P-25) | scoping only. **File deleted in `bc2b41b` — restore pending** |
| — | [picking_ui/10](picking_ui/roadmap_10-external-picker-contract.md) | External-picker contract (Model B) | S1–S3 code-complete; S4 napari pilot gated. **File deleted in `bc2b41b` — restore pending** |
| — | [particles-ux-peeves](roadmap_particles-ux-peeves.md) | Living triage sheet for the maintainer's Journey/Species/roster walkthrough | partial: most rows homed and built; P-25, P-49/50 open |
| — | [arc-runtime-checklist](roadmap_arc-runtime-checklist.md) | The one consolidated runtime script for roadmaps 07, 08–12 and de-novo S5/S6 | §0 static gates green 2026-08-18; sections 1–5 (~40 steps) owed |

The picking-UI series index and guideline of record is
[completed/picking_ui/roadmap_00-overview.md](completed/picking_ui/roadmap_00-overview.md) (restore pending).

## Release-prep arc (2026-09-29)

Five roadmaps from the pre-release list, all approved. Each opens with its settled decisions and starts at
stage 0. Suggested order, by dependency:

1. **20 site-portability** — archived 2026-10-26 (completed/); S5, the Munich re-install, is the maintainer's step.
2. **23 per-user-servers** — the security item for release.
3. **06 dl-tilt-filter** — closed 2026-10-15 (works well enough; moved to completed/ with 26).
4. **21 miss-alignment** — plumbing and drop-in output first; real runs once defaults are picked from the paper.
5. **22 blank-tilt-rays** — archived 2026-10-26 (completed/), fix not built.

Found along the way: candidate scoring counted a species match only for succeeded candidates, so a fresh
multi-species run cross-bound (21 §6) — reproduced 2026-10-05 and fixed by
[28](completed/roadmap_28-species-aware-io-wiring.md) (built, not run). (Deploy's missing re-entrancy lock, 06 defect 4, is fixed by 06 chunk 9.)

## The open debt

**Everything in completed/ is code-complete and unverified at runtime.** The standing decision (maintainer,
2026-08-17) is to build back-to-back and verify ONCE at the end rather than gating each stage, so the
whole species-registry arc plus `picking_ui/01–06` is sitting on code-review confidence.
`ruff check .` is clean; `python -c "import main"` and `python check_boundaries.py` were green on
2026-08-18 and should be re-run after each batch (they cannot be run from the assistant sandbox — no
interpreter on the mounted path).

**Not yet collected into any roadmap:** the Jobs tab peeves and the
"don't touch this yet" items in the maintainer's notes (beam-induced-motion connected graph,
tomo-recon gallery with Journey links, landing-page data-identification jank, pre-populated per-TS
task rows, logs-tab width + copy button). They land in `roadmap_particles-ux-peeves.md` first. The Picks /
Curation peeves and the de-novo-picking-feel intake arrived 2026-08-21 and are homed in
`picking_ui/08–12` (rows P-32…P-48).

**05 is not from the audit.** 00–04 derive from `docs/reports/architecture-audit/architecture-assessment-2026-08-10.md`; 05 comes
from a reproduced production failure in `/groups/klumpe/crboost_data/deadcode_test` (2026-08-11) and
fixes a capability gap rather than a structural one. It shares an enum and a state-machine phase with
`docs/roadmaps/roadmap_task-status-state-machine.md` — read that one first.

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
