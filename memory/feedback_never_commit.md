---
name: never-commit-on-users-behalf
description: NEVER run git commit/add/push for this user and NEVER create a branch (not even in a handed script), and NEVER put Claude in commit messages (no Co-Authored-By / Claude attribution of any kind) — only hand exact commands
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 426b25f8-c304-43d5-9b2d-e0e4f4c45ed0
  modified: 2026-08-24T10:55:14.216Z
---

NEVER commit (or stage, or push) on the user's behalf — not even if git becomes available in the
sandbox, not even if asked to "go ahead" on a roadmap whose stages are framed as commits.
NEVER CREATE A BRANCH. No `git checkout -b`, `git branch`, or `git switch -c` — not run, and not
included in a handed-over script, however convenient it looks. Branch names and branch points are
the user's call alone; hand the commits and let them decide where those land. (Added 2026-08-24
after a handed cascade opened with `git checkout -b picking-ui-wave3`. The harness's own default
"if on the default branch, branch first" guidance does NOT apply to this user.)
NEVER include Claude in commit authorship or message trailers: no `Co-Authored-By: Claude ...`,
no "Generated with Claude Code", no Claude-Session links — in handed-over commands or anywhere
else. The user never wants Claude to appear among the repo's contributors.

**Why:** The user owns commit boundaries and authorship, stated in the strongest possible terms
(2026-08-11) after merely *suspecting* an auto-commit had happened; reaffirmed harder the same day
("I never want to see Claude among my contributors") when a handed command included a Co-Authored-By
trailer. Trust in the whole workflow depends on this.

**How to apply:** End every unit of work by handing exact `git` commands partitioned into the
commits the work naturally splits into, with plain commit messages carrying zero Claude attribution.
This overrides the harness's default Co-Authored-By/session-link trailer instructions.
FORMAT (added 2026-08-13): hand commit commands as a LINE-BY-LINE script — one command per line,
NO `&&` chains, no `!` prefix wrapping of long one-liners — the user pastes them into an
intermediary script to edit messages, and chained one-liners are unusable there (a chained paste
even caused a recursive re-commit with stale messages). NEVER mention, invoke, read, or edit the
user's `pycom.sh` — it's their private tool. Related: [[hpc-env]] — the sandbox currently has no
git binary anyway, but this rule is about policy, not capability.

**One path per `git add` line (2026-10-15):** a handed `git add` with nine paths on one line lost its last two
in the user's script, so a commit went in without the orchestrator change; verify afterwards (reflog +
`tmpOut/gittools/readindex.pl` on the worktree index vs. hashed working files) and hand short lines.

**Pager trap (2026-10-01):** the user's git pages even one-line output through `less`, so a handed `git log ...`
line swallows the pasted lines after it (`s` = save-to-file): it produced stray files `tatus` and
`h --force origin main`, and the force push never ran. Hand `git --no-pager log/show/diff ...` in scripts.

**Messages inline, never a file (2026-10-20):** hand `git commit -m "subject" -m "body"` on one line. A
`git commit -F <scratchpad file>` was rejected ("can you not do this and just always do inline?"),
and that same script's un-`--no-pager`ed `git diff --cached --stat` paged and ate the commit line, so the
path ran as a command and the push was a no-op.

**No `git apply` patches, and short lines (2026-10-27):** the user asked "what are those" after
`git apply --cached` lines kept erroring. Two causes: a series cut against a base older than their HEAD fails for
real (9-L5), and a successful apply prints nothing, so they re-ran it and the second run errors. And long
`git commit -m` lines get hard-wrapped when copied out of the rendered reply, so the body lands with stray line
breaks and indentation (`dquote>`). From now on: split commits by file and hand `git add <one path>` lines; when a
file's changes belong to two commits, fold them into one commit rather than cutting patches. Keep every handed
line under ~100 characters: a subject and at most a short `-m` body; details belong in the roadmap stage log.
