# Roadmaps

Forward-looking plans. Every file here is named `roadmap_*`; reference documentation lives in `docs/`
and investigations / handoffs / audits live in `docs/reports/`.

**This folder lists only what is still open** (not started, or partially built). Roadmaps whose stages
are all code-complete — and plans that were superseded — live in [completed/](completed/README.md) with
their stage logs intact.

## Live

| # | Roadmap | Theme | Status |
|---|---|---|---|
| 05 | [per-ts-top-up](roadmap_05-per-ts-top-up.md) | Re-run one failed/skipped tilt-series without recomputing the job or its downstream chain | stage 0 partial 2026-08-13; no code. **File deleted in `bc2b41b` — restore pending**. Scope absorbed by 27 (its stage 2, catch-up = per-TS top-up), 2026-10-04 |
| 19 | [ctf-fit-outlier-check](roadmap_19-ctf-fit-outlier-check.md) | Per-tilt CTF-fit outlier check: fail loudly when Warp's defocus fit diverges (was numbered 18, which collided with k3-stack-ingest) | scoped 2026-09-09; not started |
| 20 | [site-portability](roadmap_20-site-portability.md) | The Munich install review + one container wrapper: site values only in config, interpreter from `sys.executable`, tool caches in `$TMPDIR`, self-test | R6 + S1–S4 committed 2026-09-29 (not run); next: the CBE runtime pass, then S5 (Munich; handoff checklist written 2026-10-15) — owed list at the top of the file |
| 21 | [miss-alignment-and-warp-export](roadmap_21-miss-alignment-and-warp-export.md) | Miss-alignment as a declared refinement stage the resolver follows (no other job changes); drop-in output; Warp particle export only if its local-deformation grids prove useful | stages 1–3 code-complete 2026-09-29 (first run done); stage 4 landed with 24; stages 5–6 continue in 24 |
| 22 | [blank-tilt-rays](roadmap_22-blank-tilt-rays.md) | The "aliasing" artifact is beam-blocked tilts surviving Warp's import coin flip; deterministic blank-exposure exclusion | investigated 2026-09-29; maintainer confirming on a separate branch |
| 23 | [per-user-servers](roadmap_23-per-user-servers.md) | One server per person with Jupyter-style tokens: the token gate first (what Munich needs), then a single-writer lease, read-only viewing of others' projects, a launcher | approved 2026-09-29; rev 2 2026-10-15 (re-checked against main, token gate moved to stage 1, the `/api/file` read-anything hole added); stage 1 (token gate) done 2026-10-15, on main (curl probes pass; browser checks not exercised) |
| 24 | [missalign-regimes-and-evaluation](roadmap_24-missalign-regimes-and-evaluation.md) | Paper-grade miss-alignment regimes (schedules in Å, PAPER preset, fitted Z box, rebuilt stacks, multi-GPU, QOS) and two verdicts: tilt-split FSC and ribosome STA | stages 1 + 3 code-complete 2026-09-30, NOT RUN; stage 4 (Warp export) built 2026-10-11, NOT RUN |
| 27 | [ts-axis-pipeline](roadmap_27-ts-axis-pipeline.md) | The pipeline as jobs × tilt series: one failed TS is dropped with a reason instead of stopping the dataset (1), can be retried or restored without recomputing anyone else (2), and eventually no TS waits for the slowest at each stage (3); supersedes 05 | stage 1 (failure isolation, 7 drivers) + the AreTomo name-length fix landed 2026-10-04, NOT RUN — runtime checklist §11 owed on the AGG412 projects; stages 2 (catch-up) and 3 (streaming) scoped 2026-10-04, not started; decisions owed §10 |
| 29 | [agg412-sta](roadmap_29-agg412-sta.md) | AGG412 (8 projects, 867 tomograms): finish the picking (19 failed jobs, none needing recompute), filter, average the ribosome (hand check, data yardstick) and the 412 VLP (curation → classes → local capsomer averaging), and a miss-alignment A/B on the pilot | scoped 2026-10-08; stage 0 done; miss-alignment A/B on MyrGFP_Grid2 2026-10-17 (+20 % picks, +8 % top-50 LCC); rollout to the other seven projects §6.2 (2026-10-18: parameters, decisions MA1–MA18 with MA10–MA18 proposed, run log); C1–C4 not built |
| 30 | [particle-analytics-and-set-tools](roadmap_30-particle-analytics-and-set-tools.md) | The particle stage's at-a-glance view: scores in σ, TM health, σ ladders with null and decoy curves, spatial QC, Class3D readouts, statistical controls (decoy template, random-position decoys, σ-bin reconstructions, recall), and set tools (rule filters, class subsets, splits, algebra, lineage) | scoped 2026-10-10; nothing built; absorbs 29's C3/C4; decisions owed §10 |
| 31 | [refine3d-job](roadmap_31-refine3d-job.md) | Gold-standard Refine 3D as a pipeline job (`mpirun -n 3 relion_refine_mpi --auto_refine`), the verdict roadmap 24 needs | stage 1 built 2026-10-12, NOT RUN |
| 32 | [staging-inputs-and-subsets](roadmap_32-staging-inputs-and-subsets.md) | Add and start jobs while others run; a submitted job's inputs never move; a Subset job and Fork… (in this project or a new one), with Import tomograms and aggregation sources unified as imported outputs | scoped 2026-10-14 from `docs/reports/pipeline-graph/`; stage 0 answered, stage 1 + §8 pipeline table + stage 2 (no S2.3) + stage 3 (Subset job) built 2026-10-15, NOT RUN; the schemer path deleted on the way (§13, NOT RUN); §9.2–9.4 decided, §9.5–9.7 owed |
| 33 | [job-liveness](roadmap_33-job-liveness.md) | One supervisor per job dir (an owner file checked against SLURM, atomic rebuilds, markers that name their writer, a submit-side pre-flight) and one liveness vocabulary (live = QUEUED or RUNNING: staged jobs never count as live, live jobs cannot be edited, deleted or removed); the registry fails loudly. Merges 13 and 15 | scoped 2026-10-15 (re-checked against main); nothing built; decisions owed §3; prerequisite of 27 stage 2 and 32 stage 2 |
| 34 | [ui-latency-and-workers](roadmap_34-ui-latency-and-workers.md) | Nothing heavy on the event loop: a stall watchdog, hidden views that do nothing, disk reads in threads and shared between tabs, cheap saves, no gzip of volumes, no work per keystroke, and CPU work (previews, renders, analytics, thumbnails) in a spawn pool of worker processes | decisions taken 2026-10-17 (§3); S0–S6 built and first run 2026-10-17; open: §8 job and sub-tab switches still 1–2 s, S7, §7 leftovers |
| 35 | [resizable-views-and-quiet-journey](roadmap_35-resizable-views-and-quiet-journey.md) | Drag the boundary between workspace views; the close cross without its full-height gutter; the Journey in the Tilts tab's language (one font pair, house controls only, one chart tile size, grey markers), in three passes behind a temporary `style` dropdown | scoped 2026-10-17; §3 decided (main tree, `localStorage`, species colours per pass, app-wide monospace out); S1–S4 built 2026-10-17, NOT RUN; S5 waits for the pick between the three passes |
| 36 | [disk-footprint](roadmap_36-disk-footprint.md) | Removed jobs keep their space in Trash/: see and clear it per project from the roster; then stop writing what nothing reads (`.staging/` duplicates, unused half-sets, TM maps after extraction) | S1 (trash view) built 2026-10-18, NOT RUN; S2–S5 proposed, decisions owed §4 |
| 37 | [segmentations-v1](roadmap_37-segmentations-v1.md) | Segment job (easymode + Ais models), import of outside segmentations, features tied to tomogram and reconstruction, transparent-fill overlays (gallery first, then Journey and pick viewer), ChimeraX surfaces, template matching restricted to a feature region; tutorials for the tools outside crboost | S0 done; S1–S7 built 2026-10-20, NOT COMMITTED; the Segment job's first run 2026-10-20 (MyrGFP_Grid2 job038) led to 38 |
| 38 | [segmentation-quality](roadmap_38-segmentation-quality.md) | Segment at easymode's 10 Å (spectrum crop, results mapped back onto the tomogram's grid), TTA 8 (Ais's limit), Z smoothing of plane-by-plane networks; the first run's findings on pixel size, normalization and banding | scoped and built 2026-10-20, NOT RUN |
| 39 | [python-environment-and-setup](roadmap_39-python-environment-and-setup.md) | A locked environment built with uv on its own Python (no module loads on the nodes), the tilt filter's torch in a container (venv 5.6 GB → < 1 GB), CI with a pinned ruff, labelled containers, checksummed weights | scoped 2026-10-22; nothing built; S0 inventory command owed (numpy/pandas/scipy likely come from the `arrow` module, not the venv); decisions owed §2 |
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

1. **20 site-portability** — the container wrapper and config cleanup the others lean on (miss-alignment's
   cluster-install failure is this roadmap's silent binary fallback).
2. **23 per-user-servers** — the security item for release.
3. **06 dl-tilt-filter** — closed 2026-10-15 (works well enough; moved to completed/ with 26).
4. **21 miss-alignment** — plumbing and drop-in output first; real runs once defaults are picked from the paper.
5. **22 blank-tilt-rays** — independent; being confirmed on its own branch.

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
