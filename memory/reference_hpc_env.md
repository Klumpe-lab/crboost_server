---
name: hpc-env-setup-for-running-python-ruff-in-this-project
description: Lmod cluster — LD_LIBRARY_PATH is set in .claude/settings.local.json so venv/bin/python3 and venv/bin/ruff work without env prefixes; do NOT re-add PYROOT/SSLROOT inline
metadata: 
  node_type: memory
  type: reference
  originSessionId: f7c6f9fc-f919-4efc-b1d0-199e184750c4
  modified: 2026-10-02T14:50:13.015Z
---

**2026-10-28: the venv's python WORKS in the sandbox again** (uv: `venv/bin/python3` → `~/.local/share/uv/python/
cpython-3.11…`). numpy/pandas/nicegui/mrcfile import; full-app imports (`import ui.particles.pick_viewer`) and
`check_boundaries.py` run; `run_in_worker` spawns real worker processes (script needs an `if __name__ == "__main__"`
guard). NiceGUI user simulation works without pytest-asyncio: `prepare_simulation(); ui.run(storage_secret=…)`, then
`core.app.router.lifespan_context` + `httpx.ASGITransport` → `User(client)` (nicegui.testing). No git, still.
The sections below about a dangling symlink are outdated.

**⚠️ SANDBOXED CONTAINERIZED ENVIRONMENT — NO `git` BINARY (user, 2026-08-10).** Claude's bash sandbox has no git and no access to cluster binaries beyond what's listed below. Do NOT hunt for git (or other tools) in /software, /usr, /opt — it's not there and the user finds the search noisy. When git info is needed (log, diff, branch state), give the user the exact command(s) and let them run it — suggest the `! <command>` prefix so output lands in the conversation. `.git/` files (HEAD, refs) are readable directly if only the branch name is needed. Commits and history: `.git/logs/HEAD` (reflog, plain text). Loose objects (commits made after the clone) decode without git or zlib: `(printf "\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03"; tail -c +3 .git/objects/ab/cdef…) | gzip -dc 2>/dev/null` (gzip header + raw deflate; run with `LC_ALL=C` to silence perl locale noise when parsing trees). Simplest (2026-10-27): `/usr/local/bin/node` exists, and `zlib.inflateSync` reads loose objects directly (the 8-byte gzip header fails; it must be the 10-byte one above). Packed objects: `node tmpOut/gittools/packread.js <repo> <path>...` (2026-10-29; git-ignored; the 2026-10-02 perl tools are gone) prints same/DIFF of HEAD's blob vs the worktree, resolves OFS/REF deltas, re-hashes to verify; `OUT=<dir>` writes HEAD's copies there, `COUNT=list` lists HEAD's files. Compare against HEAD's tree, not only the index: 2026-10-29 HEAD was found to lack half of a rename the worktree had. With those, "what's uncommitted" = hash every index entry's worktree file vs its index sha; HEAD==index via the index TREE extension's root sha vs the commit's tree.

**⚠️ CORRECTION (2026-08-17, verified twice): `venv/bin/python3` is a DANGLING SYMLINK in the sandbox.**
It points at `/software/f2022/software/python/3.11.5-gcccore-13.2.0/bin/python3`, and `/software` has been
unmounted for the last ~6 sessions → `bash: ./venv/bin/python3: No such file or directory` (note: bash still
reports `rc=0` for that, so check the message, not the exit code). So **`py_compile` / `compileall` /
`check_boundaries.py` are NOT available to Claude** — hand them to the user. `./venv/bin/ruff` DOES work
(0.15.12, both `check` and `format`) because it is a real binary in the venv, not a symlink out. Verification
ceiling is therefore ruff + reading. Don't burn turns retrying python; if `/software` is remounted the section
below applies again unchanged.

**Use `venv/bin/ruff` directly (and `venv/bin/python3` when /software is mounted). No env prefix needed.**

The cluster's Lmod modules are NOT available in Claude's bash sandbox (no `module load`), and `venv/bin/python3` links against `libpython3.11.so.1.0` from the EasyBuild python install — so historically every bash call had to prefix `PYROOT=… SSLROOT=… LD_LIBRARY_PATH=…`. As of 2026-05-18 that is solved at the config layer:

```jsonc
// .claude/settings.local.json
{
  "env": {
    "LD_LIBRARY_PATH": "/software/f2022/software/python/3.11.5-gcccore-13.2.0/lib:/software/f2022/software/openssl/1.1/lib"
  },
  "permissions": { ... }
}
```

Every Bash invocation in this session inherits LD_LIBRARY_PATH. Verified working with `venv/bin/python3 --version`, `venv/bin/python3 -c "import ssl"`, and `venv/bin/ruff --version` — no prefix.

**Do not** re-introduce the prefix dance. If `venv/bin/python3` fails with "cannot open shared object file: libpython3.11.so.1.0", the env block has been removed or the cluster paths changed. Check `.claude/settings.local.json` first.

**⚠️ THE SERVER DOES NOT AUTO-RELOAD (2026-06-09 — cost ~3 sessions of "my fix didn't work").** `main.py` runs `uvicorn.run(app, ...)` with NO `reload=True` (and passes an app *object*, which uvicorn can't reload anyway); NiceGUI uses `ui.run_with`, not `ui.run(reload=True)`. **Every Python AND CSS edit requires the USER to restart `python main.py` + hard-reload the browser** (CSS is injected from Python via `ensure_assets_loaded`, so a browser refresh alone is not enough). Until then, edits appear to do *absolutely nothing* — identical behavior across multiple independent fixes is the tell. When a user reports "no change," CONFIRM the code is live first (e.g. a fresh `logger.info` line in the terminal, or a changed computed style in devtools) before assuming the fix is wrong and re-guessing.

**venv state (unchanged from before):**
- venv at `/users/artem.kushner/dev/crboost_server/venv` is configured `include-system-site-packages = false`.
- Has: nicegui, fastapi, mrcfile, biopython, starfile, scikit-image, ruff.
- Does NOT have: pandas, numpy, scipy, pytz, dateutil. These come from the cluster's scipy-bundle via the user's interactive `module load arrow/...`, NOT available to Claude. So full-app imports (anything that pulls services/project_state.py → services/job_models.py → pandas) still need the user to run preflight/main.py themselves.

**What works without the user:**
- `venv/bin/python3 -m py_compile <file>` (syntax check).
- `venv/bin/ruff check <files>` / `venv/bin/ruff format <files>`.
- Imports of pandas-free modules (io_slots, configs, path_resolution_service, anything in services/visualization that doesn't touch ProjectState).

**What requires the user's interactive env:**
- `python preflight.py`, `python main.py`, anything that imports ProjectState/JobModels.

User said (2026-05-10): "u are free to install missing packages [in the venv]" — for tooling like ruff this is the right move. For runtime deps (pandas etc.) leave the cluster scipy-bundle as-is.

**Dry-running a merge without git (2026-10-01):** objects since the Jan-2026 pack are all LOOSE in both
`~/dev/crboost_server/.git` and the clone (local clone = hardlinks), so a perl tree-walker over the gzip-trick
decoder lists base/tip trees; `/usr/bin/diff3 -m ours base theirs` then simulates the 3-way merge per file. Blobs of
files unchanged since January are PACKED (decoder returns empty) — check for empty output before trusting a compare.

**The git module breaks the server's shell (2026-10-16).** `module load git/2.36.0-gcccore-11.3.0-nodocs` puts
gcccore 11.3.0's older libstdc++ in front, so `python main.py` dies importing scipy (`GLIBCXX_3.4.30' not found`
from `pypocketfft`). Hand git commands for a separate terminal, or tell the user to `module purge` and reload
`build-env/f2022`, `python/3.11.5-gcccore-13.2.0`, `gcccore/13.2.0`, `arrow/16.1.0-gfbf-2023b` before restarting.
