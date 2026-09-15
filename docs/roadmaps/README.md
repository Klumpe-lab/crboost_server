# Roadmaps

Forward-looking plans. Every file here is named `roadmap_*`; reference documentation lives in `docs/`
and investigations / handoffs / audits live in `docs/reports/`.

**This folder lists only what is still open** (not started, or partially built). Roadmaps whose stages
are all code-complete — and plans that were superseded — live in [completed/](completed/README.md) with
their stage logs intact.

## Live

| # | Roadmap | Theme | Status |
|---|---|---|---|
| 05 | [per-ts-top-up](roadmap_05-per-ts-top-up.md) | Re-run one failed/skipped tilt-series without recomputing the job or its downstream chain | stage 0 partial 2026-08-13; no code. **File deleted in `bc2b41b` — restore pending** |
| 06 | [dl-tilt-filter](roadmap_06-dl-tilt-filter.md) | DL tilt filter as always-interactive first-class method; hard approval barrier | scoped 2026-08-11; not started |
| 13 | [roster-staged-vs-queued](roadmap_13-roster-staged-vs-queued.md) | The roster paints a just-added job with the same amber "Scheduled" dot as one queued on the cluster; derive the split from `pipeline_active` + `pipeline_order` | scoped 2026-08-23; not started |
| 15 | [job-dir-ownership](roadmap_15-job-dir-ownership.md) | On-disk ownership lock for `External/jobNNN/` so a retry cannot sbatch a second supervisor into a live job dir (reproduced 2026-08-28, 62/114 TS lost) | scoped 2026-08-28; not started |
| 19 | [ctf-fit-outlier-check](roadmap_19-ctf-fit-outlier-check.md) | Per-tilt CTF-fit outlier check: fail loudly when Warp's defocus fit diverges (was numbered 18, which collided with k3-stack-ingest) | scoped 2026-09-09; not started |
| — | [orchestrator-replacement](roadmap_orchestrator-replacement.md) | `relion_schemer` out → ProjectState + SLURM afterok DAG + thin reconciler | partial: afterok DAG is the main path (`_submit_chain`, `reconcile_afterok`); P1.C and the schemer decommission not started |
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
