---
name: feedback_public_repo_no_names_no_swearing
description: "Repo is public (Munich share 2026-09-15) — docs/roadmaps/code must never name colleagues or quote the maintainer's swearing; use roles (\"the PI\", \"a colleague\", \"a user\"); q_*.md = the user's questions-to-Claude, disposable"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 07578eac-4c63-45f0-92fc-11954ec4cd3d
  modified: 2026-09-15T09:09:31.462Z
---

2026-09-15, before the first public share: the user had me scrub every colleague first name (PI, lab members,
DL-model author, a user whose run was cited) and every quoted curse from docs/, and delete their dated todo
lists and all `q_*.md` files ("my convention for 'question to claude'").

**Why:** CLAUDE.md is now tracked too, and docs ship. Roadmap peeves had been recorded as verbatim maintainer
quotes including profanity; reports named the PI and colleagues throughout.

**How to apply:** when writing roadmaps/reports/comments, paraphrase peeves without the swearing and refer
to people by role — the PI, a colleague, a user, the model author. Personal dirs in paths →
`/groups/<group>/user/<pi>/…`, `<user>_…`. Lab group paths (`/groups/klumpe/…`) in dated reports were left;
config templates must use `/path/to/…` with a comment like `/groups/<name_of_group>/…`. The container_defs
`Author` lines (credit) were left in place, flagged to the user. `q_*.md` files are inputs, not records —
don't cite them from docs. See [[feedback_write_roadmaps_directly]].

**This memory folder is in the repo too (2026-11-03):** `memory/` in the checkout is the real folder and
`~/.claude/projects/<key>/memory` links to it, so every memory file ships. Write them by the same rule:
roles, no names (also not in paths or file names: `<pi>`, `<colleague>`), curses paraphrased, never quoted.
The unscrubbed originals were kept outside the repo in `~/.claude/projects/<key>/memory.unscrubbed-2026-11-03`.
