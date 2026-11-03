---
name: reference_registry_schema_cross_branch
description: "Pydantic `frames.N.p_bad … extra_forbidden` spam = a dl_filter-branch project (registry schema 1.5) opened from a main-branch server (1.4); every TS sidecar is skipped"
metadata:
  node_type: memory
  type: reference
  originSessionId: 84ca8c42-2d04-4356-a155-d4a03fe9614b
  modified: 2026-10-01T11:37:20.564Z
---

`frames.N.p_bad  Extra inputs are not permitted [type=extra_forbidden]` in the server log means a project whose
registry was written by the `dl_filter` branch (REGISTRY_SCHEMA_VERSION (1, 5), additive `Frame.p_bad`) was opened
by a server running main (1, 4). Registry models are `extra="forbid"`, so `TiltSeriesRegistry.load()` logs a warning
per sidecar and skips that TS. The project then looks empty on that server. If anything saves the registry with a
dirty index, the index is rewritten without the skipped TS (the sidecars stay on disk).

Projects at 1.5 (checked 2026-10-01): agg_20251113_412, agg_20260311_412_Grid3, bullseye_artifact_two,
bullseye_artifact_two_ctfband. Open them only from the dl_filter clone (~/dev/crboost_server_dl_filter) until the
branch merges. No load-side guard exists; the user was offered one (refuse saves when the on-disk schema is newer).
Related: [[project_dl_tilt_filter]], [[reference_worktree_per_session]].

**2026-10-01:** dl_filter merged into main (13111a1), so main reads 1.5 now. The warning only comes back from a server
running an older checkout or branch.
