---
name: reference_worktree_per_session
description: "Parallel Claude sessions must each run in their own git worktree; per-worktree setup this repo needs (untracked conf/qsub/venv, port)"
metadata:
  node_type: memory
  type: reference
  originSessionId: 33314afa-1d1c-4c5b-a360-89306916d7ef
  modified: 2026-09-29T15:23:29.924Z
---

2026-09-29: two sessions (roadmap 06 on `dl_filter`, roadmap 20 on `bindmounts_and_auth`) were started in the
SAME directory after `git checkout -b` in each — one working tree, one HEAD, so both edited the same files and
both "branches" were really the last one checked out. The user then moved the DL session to its own worktree.

A worktree in this repo does not get the gitignored files it needs:
- `config/conf.yaml` and `config/qsub.sh` — copy from the main checkout (qsub.sh carries absolute
  CRBOOST_SERVER_DIR/CRBOOST_PYTHON paths; the driver launch prepends the server's own dir to PYTHONPATH, so
  the worktree's code still wins).
- `venv/` — symlink the main one; without it `driver_launch_prefix` silently falls back to bare `python3`
  (until roadmap 20 stage 2 derives the interpreter from `sys.executable`).
- Start `main.py` from inside the worktree (`CryoBoostBackend(Path.cwd())`, repo root found from cwd), on
  its own port; don't point two servers at the same test project.

**The system git has no `worktree`** (pre-2.5: "'worktree' is not a git command"). What worked: a local
clone + checkout of the existing branch (`git clone ~/dev/crboost_server ../crboost_server_dl_filter`,
`git checkout dl_filter`; origin = the main repo, `git push origin dl_filter` brings commits back). The Lmod
module `git/2.36.0-gcccore-11.3.0-nodocs` would allow a real worktree, but then that git must be used for
every command inside it.

**A handed worktree setup block must start with that `module load`.** 2026-10-15 (roadmap 23, `token_auth`):
the block handed from main had none, the `git worktree add -b …` line left no trace (no branch, no
`.git/worktrees/` entry), the conf/qsub/.pth/venv lines failed for lack of the folder, and
`cp -r …/.claude …/crboost_server_auth/` then CREATED the folder as a copy of `.claude` (a lone top-level
`settings.local.json`), which blocks a retry of `git worktree add` (non-empty target; an empty folder is fine).
If a session was already started in the folder, its harness-made `memory/` dir must be `rmdir`ed before the
memory `ln -s`, or the link lands inside it.

**Memory is keyed by the session's start directory**: a session started in the clone does not load this
memory unless `~/.claude/projects/-users-artem-kushner-dev-crboost-server-dl-filter/memory` is a symlink to
this one (or the session is told to read MEMORY.md here).

**How to apply:** if files change on disk mid-session that I didn't touch, suspect a second session in the
same tree — check `.git/HEAD` / recent mtimes and tell the user before handing commit scripts. Also check for
UNCOMMITTED docs that other branches lack: the 2026-09-29 release-prep roadmaps (06 rev 4, 20–23, README arc)
were never committed and existed only in the main tree's working copy.

2026-10-02: happened again (roadmap 26 F build + a roadmap 25 session in the same tree; shared backend.py,
ui/projects_overview.py, ui/dashboard/css.py, README). What kept the commits apart without git in the sandbox:
snapshot each file before my first edit (`tmpOut/f_patches/snap.sh`), cut per-chunk unified diffs after
(`mkpatch.sh`), drop foreign hunks by replaying my own edits on a reconstructed base (`applyhunks.pl`, perl,
reverse/forward), verify the chain round-trips, and hand `git apply --cached F<n>.patch` + commit lines,
committed BEFORE the other session's whole-file `git add`s.
