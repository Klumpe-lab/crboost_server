---
name: project_release_prep_roadmaps_2026-09-29
description: "Pre-release arc — roadmaps 06 rev3, 20, 21 rev2, 22, 23 consolidated with the maintainer's answers on branch predep_roadmaps; no code; implementation happens in NEW sessions"
metadata:
  node_type: memory
  type: project
  originSessionId: 3f09196c-4d8f-4ce7-86fc-913cc823e796
  modified: 2026-10-15T08:05:37.941Z
---

2026-09-29, branch `predep_roadmaps`. Consolidated after the maintainer answered each roadmap. NO code yet;
the maintainer said implementation starts in new sessions, one roadmap at a time.

- **06 rev 4** DL filter — three modes on the job ROW: Manual & DL review PARK the pipeline until Approve (DL review dispatches the DL run on request); DL auto runs in the chain (synthetic edge to alignment) and commits in-job. Built assuming weights work; maintainer is asking the model author for upgraded weights; liveness = banner in review, FAIL in auto.
  our labels; review `always|never`; barrier = segment submission; K3 out of scope; no author dependency.
- **20** site portability — APPROVED in full (Munich review R1–R7 + one container wrapper + selftest).
  PROGRESS 2026-09-29 on branch `bindmounts_and_auth` (committed by the maintainer, NOT RUN): R6 toast, S1
  wrapper (container_runtime/container_binds, cache env, per-job binds, raise on unconfigured tool),
  S2a (driver interpreter = crboost_python or sys.executable; REPO_ROOT from __file__), S2b (driver
  sys.path inserts gone), S2c (one sbatch renderer in slurm_service; qsub trailer = the ONLY exit-marker
  writer; SlurmConfig validator unquotes constraints because every project file stores them quoted) and
  S2d (tool names = conf keys, aliases/containers:/tsreconstruct_supervisor_slurm gone) handed as git lines
  2026-09-29, S2e (empty site defaults, ConfigService.load_warnings for unread keys → landing strip +
  preflight), S3 (ChimeraX worker binds via CX_BINDS; CX_SIF override gone) and S4 (pytest.ini + tests/:
  static tier = preflight as a test; `-m cluster` = one sbatch --wait per tool via tests/cluster_probe.py →
  run_tool ON the node; probes UNVERIFIED) handed too. Shared-install conf edits are the maintainer's
  (classifier blocks me; they did the safe-now three 2026-09-29). RUNTIME 2026-09-29: pytest static 3/3 +
  cluster 11/11 green; copia chain import→class3d all Succeeded (copia-tutorial-20260929-1821, 1 TS),
  binds/--array/markers verified. Still unrun: curation launch (S3), cisTEM template sim, old-project
  unquote, cancel path. NEXT: those, then merge + S5; the
  handoff block at the top of the roadmap file lists exact sites. CUDA_VISIBLE_DEVICES deliberately NOT
  forwarded (pytom -g uses host ids).
- **21 rev 2 (APPROVED)** miss-alignment — declared refinement (`JobSpec.refines`) followed by the resolver; no other job
  changes; drop-in star + tilt_series/ from refined XMLs (aligntiltsWarp XML/star pair = fixture); Warp export
  = an ADDITIONAL producer, only useful once GridMovement (local deformation) grids are non-trivial; maintainer
  reads the paper and picks defaults.
- **22** blank-tilt rays — maintainer verifying stages 0–1 on a separate branch ([[project_blank_tilt_rays]]).
- **23** per-user servers with Jupyter-style tokens — APPROVED. Gotchas recorded: cookies scoped by host not port
  (per-port token + NiceGUI session cookie names), side channels (VNC, ChimeraX REST), viewers never reconcile.
  REV 2 WRITTEN 2026-10-15 (anchors re-checked on main): stage 1 = token gate + raw-file-route guards (the Munich
  slice; `/api/file`, `ui/template_workbench.py:102-107`, serves ANY file the owner can read); then facts, lease,
  read-only UX, launcher, shared install, retire port. The maintainer builds it in a SEPARATE WORKTREE. STAGE 1 BUILT 2026-10-15 (worktree ~/dev/crboost_server_auth, branch `token_auth`; curl runtime pass on 8090 ACCEPTED 2026-10-15, browser halves of checks 2–6 not exercised; Munich handoff docs written same day: README "Upgrading an existing install", roadmap 20 Stage 5 checklist, conf.template afterok comment fixed): services/server_auth.py TokenGate, ~/.crboost/{token,secret,nicegui}, per-port cookies; /api/file = loaded project OR registered template/mask (imported masks live outside projects); stage log in the roadmap. Nothing
  calls back into the server over HTTP (curation REST is server→node), so the gate breaks no job flow.
- **06 CLOSED 2026-10-15** (works well enough) — moved to completed/ with 26 and 25.
- **24 deleted** — VM/IT/front door dropped ("not going there anytime soon"); a parked paragraph lives in 23.

**Why:** release prep after the PIs' positive feedback. **How to apply:** in a new session, open the roadmap,
start at its stage 0; don't re-litigate the decisions above.

Side finding (unverified, outside all roadmaps): candidate scoring counts species match only for SUCCEEDED
candidates (path_resolution_service.py:944) → a fresh multi-species afterok run may cross-bind
tmextractcand__A to templatematching__B. Also: deploy may double-submit on a double Run (06 defect 4).

**All five approved 2026-09-29.** docs/roadmaps/README.md has a "Release-prep arc" section with the suggested
order (20 → 23 → 06 → 21; 22 on its own branch). Each roadmap's status line says "Next session: start at stage 0".

**2026-10-16:** `token_auth` and `graphical_pipeline` are both merged into main (main `b1278b7`, merged in a
throwaway detached worktree, then fast-forwarded). All extra worktrees are gone; only `~/dev/crboost_server`
remains. The branch refs `token_auth`/`graphical_pipeline` still exist (deleting them is the user's call).
Still owed: token rotation after it was pasted in chat; the shared install `/groups/klumpe/software/crboost_server`
must pull main (old code drops Subset jobs on save) and lose its `use_afterok_orchestrator:` line.
