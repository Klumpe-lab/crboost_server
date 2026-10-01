# Roadmap 23 — One server per person with Jupyter-style tokens

**Status:** approved 2026-09-29, **not implemented**. Next session: start at stage 0. Release item: "token-based authentication — two
users on the same port interfere; per-member ports are too brittle; keep being able to look at, debug
and dig through each other's projects." The Munich install review asked for the same thing (roadmap 20,
R7): anyone on the network can open the GUI — "secure, token like Jupyter".

**Decision (maintainer, 2026-09-29):** Jupyter-style tokens. Every person runs **their own** server,
started or reused by one command that prints a URL carrying a secret token. Identity is the Unix user
running the process. Looking at someone else's project means opening it **read-only in your own server**.
No VM, no hub, no SSO — not now.

## 1. How the connections work

**Today** — one process, no auth, runs as whoever started it:
```
  your browser ──────┐
  colleague's ───────┼──► headnode:8081 ──► python main.py (uid = whoever started it)
  anyone on VBC net ─┘        no auth              │ sbatch as that person
                                                   ▼
                                     SLURM jobs + files under /groups as that person
```

**After** — one process per person, each behind its own token:
```
                         ┌───────────────────── headnode ─────────────────────┐
  your browser ─────────►│ :21234  crboost (uid you)   ── sbatch ──► jobs as you  │
    cookie tok_21234     │    │ holds lease on project A                        │
                         │    ▼ reads/writes /groups/…/A                        │
  colleague's browser ──►│ :23456  crboost (uid them)  ── sbatch ──► jobs as them │
    cookie tok_23456     │    │ holds lease on project B                        │
                         │    └─ opens A read-only, re-reads it when it changes │
  stranger ─────────────►│ :21234 without the token ──► 401                      │
                         └────────────────────────────────────────────────────┘
```

**Starting, and the first request:**
```
  $ crboost
    ~/.crboost/run/<install>.json says a server is alive and /healthz answers?
        yes → print its URL
        no  → start main.py detached on your stable port, wait for /healthz, print the URL
  CryoBoost is running:
    http://<headnode>:21234/?token=Xq3…                     (on the VBC network / VPN)
    ssh -N -L 21234:localhost:21234 <you>@<headnode>        (from outside, then
    http://localhost:21234/?token=Xq3…                       open this locally)

  browser ── GET /?token=Xq3…  ───────────────► token middleware: token matches
          ◄─ 302 → /   Set-Cookie: crboost_token_21234=Xq3…; HttpOnly; SameSite=Lax
          ── GET /     (cookie) ───────────────► page
          ── WS /_nicegui_ws/socket.io (cookie) ► live UI
          ── GET /api/vis-asset?… (cookie) ─────► images
```

**One writer per project — the lease** (`<project>/.crboost/lease.json`):
```
  your server:   writes {user, host, pid, port, version, heartbeat} every 60 s → may save
  their server:  lease fresh (< 3 min) → project opens read-only, "being edited in <you>'s CryoBoost"
                 lease stale (your server died/stopped) → read-only + "Take over editing" (confirm)
```

## 2. What it fixes, and what to be careful with

**Fixes:**
1. **Strangers driving your server.** Today anyone who reaches :8081 can submit jobs as its owner and
   delete any project (`shutil.rmtree`, `data_import_panel.py:474-481`).
2. **Interference.** Prefs, the `~/.crboost/conf.yaml` override, the task tray, the curation viewer and
   the dialog guards leak between browsers in one process; one person's restart, crash or slow render hits
   everyone. With one person per process none of that crosses people.
3. **Wrong identity.** Jobs, files, fairshare and quota belong to the person who submitted them.
4. **Port juggling.** Each person gets a stable port derived from their uid and a stable token, so the
   bookmark keeps working; nobody assigns ports.
5. **The start/stop dance.** `crboost` is idempotent; a restart only affects you; pipelines keep running
   in SLURM either way (afterok DAG).

**Be careful with:**
1. **The token is a password in a URL.** It sits in browser history and in anything pasted to chat or a
   screenshot; whoever has it controls your server as you. The redirect strips it from the address bar;
   `crboost rotate-token` revokes it. Never put a tokened URL in a bug report.
2. **Plain HTTP.** On the internal network the token and cookie travel unencrypted — the norm for Jupyter
   on HPC, fine inside VBC, not fine on an untrusted network. The SSH tunnel variant is encrypted.
3. **Localhost is shared on a headnode.** Every user on the machine can connect to your port — the token
   is needed even if the server bound to 127.0.0.1 only.
4. **Cookies are scoped by host, not port.** Two CryoBoost servers on the same host — or *every* server
   you reach through a tunnel, since they all look like `localhost` — share one cookie jar. So the token
   cookie is named per port (`crboost_token_<port>`), and so is NiceGUI's session cookie: NiceGUI 3.0.3
   installs Starlette's `SessionMiddleware` with the default cookie name `session`, and skips that if one
   is already registered (`nicegui/storage.py:38-46`) — register our own with
   `session_cookie=f"crboost_session_{port}"` before `ui.run_with`. Without this, switching between your
   dev and release servers in one browser resets each other's sessions.
5. **Two servers of the same user** (dev checkout + shared install) are both "you": the run file is keyed
   by install path, and the lease distinguishes them by pid/port, so they never both write a project.
6. **Side channels the token does not cover.** The ChimeraX curation desktop's VNC (password by default,
   `curation.passwordless_vnc` opt-in removes it) and its REST command port (127.0.0.1 on the compute node,
   no auth — reachable by other users' jobs on the same node). Keep VNC passworded; the REST port is a
   separate, small fix.
7. **Viewers must never reconcile.** With SLURM `PrivateData` they cannot see the owner's jobs and would
   mark live jobs FAILED after 90 s (`pipeline_runner.py:43`).
8. **Version skew.** An older server saving a newer project silently drops what it cannot parse — the
   schema guard (§3.6) makes such a project read-only instead.
9. **Idle servers.** A forgotten server keeps polling SLURM for its projects; idle shutdown after 12 h
   without a client. Nothing is lost — the next `crboost` catches up from disk and SLURM.

## Why not the alternatives

| | One shared server + per-user tokens | Static port per member | **Per-person server + token** |
|---|---|---|---|
| Who jobs and files run as | the server's owner, for everyone | each person | **each person** |
| Blast radius of restart/crash/slow render | everyone | one person | **one person** |
| Stops strangers | yes | **no** | **yes** |
| Viewing others' projects | trivial | nothing built | read-only in your own server |
| Code | per-request identity through ~a dozen per-process stores; `sbatch` still runs as the owner unless we add sudo/slurmrestd | a port table | lease, save guard, read-only UI, token middleware, launcher |

---

## 3. Design

### 3.1 The launcher — `bin/crboost` (stdlib only, starts instantly)
- `crboost` / `crboost start`: read `~/.crboost/run/<install-hash>.json` (mode 600: pid, host, port,
  version, started). Alive and `/healthz` answers with our token → print the URL. Otherwise start
  `main.py` detached (`setsid`, output to `~/.crboost/logs/`), wait for `/healthz`, print the URL.
- Prints both the direct URL and the tunnel variant (the tunnel hint exists today: `main.py:55-57`,
  `ui/landing_status_strip.py:358-362`).
- `crboost stop | restart | status | logs | url | rotate-token | open <project-path-or-/p/route>`.
- **Stable URL:** port = `20000 + uid % 10000` (next free port on collision; offset per install so dev
  and release differ); token in `~/.crboost/token` (600), reused across restarts.
- `umask 002` for the server; SLURM propagates the submitter's umask to jobs.
- Idle shutdown after 12 h without a connected client (configurable).

### 3.2 Token check — one pure-ASGI middleware
- `?token=` → set `crboost_token_<port>` (HttpOnly, SameSite=Lax) and redirect without the token; or the
  cookie. Wraps the outermost app handed to `uvicorn.run` (`main.py:205`) and checks **http and
  websocket** scopes, so NiceGUI's socket.io mount (`nicegui/nicegui.py:53`, inside the app we pass) is
  covered. Everything else → 401 page "open the link printed by `crboost`". `/healthz` included.
- Per-port NiceGUI session cookie (§2 item 4). Per-user `storage_secret` in `~/.crboost/secret` (today
  `"crboost-change-me"`, `main.py:170`); NiceGUI storage at `~/.crboost/nicegui` (`NICEGUI_STORAGE_PATH`;
  today `./.nicegui` in the working directory, which a shared install would share).
- `/api/tilt-thumb` restricted to project roots (today: any readable `.png`, `main.py:101-106`).
- Repo-relative paths from `__file__`, not `Path.cwd()` (`main.py:137,150`, `project_service.py:336`,
  `config_service.py:30`) — done by roadmap 20 S2a (`config_service.REPO_ROOT`).

### 3.3 Single writer per project — the lease
- `<project>/.crboost/lease.json`, atomic write. Taken when a server opens a project for writing (open,
  create, recovery). Free = absent, stale (heartbeat > 3 min) or ours. Heartbeat every 60 s from the
  existing monitor tick.
- Held and fresh → read-only. Stale → read-only with **Take over editing** (confirmation; written to the
  project's event log). Released on close/shutdown.
- `ProjectState.save()` raises `ReadOnlyProject` without the lease — never a silent no-op — including the
  direct `state.save()` calls in `ui/aggregation/merge_card.py:151,156,382`. Backend mutators (submit,
  delete, transfer, create, approve) return `err(code=ErrorCode.READ_ONLY)`.
- Derived caches a viewer writes into a foreign project are best-effort: a `PermissionError` there is
  caught narrowly and viewing continues without the cache.

### 3.4 Read-only viewing
- Header chip: "Read-only — being edited in <user>'s CryoBoost (20 s ago)" / "… nobody is editing — Take over".
- One `callbacks["can_write"]` flag drives the mutating controls; the save guard backs up anything missed.
- A read-only state re-reads `project_params.json` when its mtime moves (signature-gated, 15 s). The
  "in-memory state is authoritative" rule applies to writers only. Viewers never reconcile (§2 item 7).

### 3.5 Recovery and reconciliation are scoped
- `_discover_and_recover` (`pipeline_monitor.py:80-137`) adopts a project only if its lease is free or
  ours **and** the owner is us or `@lab` (legacy owner-less projects: `created_by`). Today it adopts every
  active project in the default base, so N servers would mean N reconcilers per project.
- Every chain `sbatch` gets `--kill-on-invalid-dep=yes`, so SLURM cancels dependents of a failed job
  while the owner's server is down.

### 3.6 Schema guard
- File schema newer than `SCHEMA_VERSION` (`project_state.py:51`) → read-only regardless of the lease,
  banner "written by a newer CryoBoost (x.y) — update to edit" (today: INFO log, then data loss on save,
  `project_state.py:1090, 1296-1305`).

### 3.7 File modes
- `ProjectState.save()` and the registry sidecars (`registry.py:440-444`): `os.fchmod(fd, 0o666 & ~umask)`
  after `mkstemp` (today every save leaves mode 0600 — hidden by Isilon's ACLs, fatal on a POSIX filesystem).

### 3.8 One shared install
- `/groups/klumpe/software/crboost_server` is the release install (tagged checkout + venv, refreshed per
  release; stale since 2026-07-24). `crboost` reaches `PATH` through one line in `~/.bashrc`.
- The server compares its version with the install on disk every 15 min → "vX.Y installed — Restart to
  update" banner; the button runs `crboost restart` detached; same URL afterwards.

## 4. Stages

### Stage 0 — Facts (user-run, ~30 min; item 5 in the sandbox)
1. `scontrol show config | grep -iE 'PrivateData|SchedulerParameters'`.
2. RSS of a running server (`ps -o rss= -p <pid>`) with one project open.
3. From a desktop on the VBC network: does `http://<headnode>:<high port>` load, or does everyone tunnel?
4. With a colleague: can they create and delete a file inside your project dir on `/groups` (Isilon ACLs)?
5. List every file the server writes while *viewing* a project (cache writers) → the §3.3 list.
- *Success:* answers recorded here.

### Stage 1 — Lease, save guard, scoped recovery, schema guard, file modes
- §3.3 minus UI, §3.5, §3.6, §3.7.
- *Success:* two servers open one project → the second is read-only and its saves raise; a restarted
  server adopts only its own active projects; a newer-schema project opens read-only in older code; a
  fresh save is 0644/0664.

### Stage 2 — Read-only UX
- §3.4.
- *Success:* a colleague watches your pipeline advance live in their own server and cannot queue
  anything; after you stop yours they take over with one confirmation.

### Stage 3 — Token, launcher, per-user storage
- §3.1, §3.2.
- *Success:* `crboost` twice prints the same URL; no token → 401 on pages, `/api/*`, static assets and
  the websocket; dev and release servers open side by side in one browser without logging each other out;
  `kill -9` + `crboost` → the bookmark still works.

### Stage 4 — Shared install + update banner
- §3.8.
- *Success:* after the install is updated, running servers show the banner within 15 min; Restart brings
  up the new version on the same URL.

### Stage 5 — Retire the shared port
- README "Running" → `crboost` (tunnel variant kept as the from-outside route); announce to the lab.

## 5. Not doing
- Per-principal refactor of prefs/config/tasks/curation — moot with one person per process.
- User database, login page, roles — Unix identity and group permissions already exist.
- TLS — tunnel users get SSH encryption; the internal network carries plain HTTP as with Jupyter.

**Parked, not now:** a local `crboost connect` helper that does the SSH tunnel behind the scenes;
a front door (Open OnDemand app or a JupyterHub-style hub) with institute login. Both would spawn exactly
these per-user servers, so nothing here needs redoing; the only prerequisite would be prefix-safe URLs
(`ui/routing.py:92-104`, ~9 hard-coded `/api`/`/static` URLs, ~10 `navigate.to("/…")` calls).

## 6. Runtime checklist (two people)
1. Both run `crboost` → two URLs; each without its token → 401.
2. A opens B's running project → read-only chip, live progress, Run disabled.
3. B stops their server; A takes over; B restarts and sees read-only "being edited by A".
4. `sacct -u <A>` shows A's jobs under A.

## 7. Risks
1. If the login node reaps long-running processes, `crboost` restarts in seconds; pipelines are unaffected.
2. With `PrivateData`, viewers show the owner's last-written state, not live SLURM state — show its age.
3. Clock skew between login nodes vs the 3 min staleness window — fine with NTP; revisit if takeovers misfire.

**Modern-Python weave-in:** the lease as a frozen `dataclass` with `acquire/heartbeat/release` returning
`ok()/err()`; `ReadOnlyProject(Exception)`; `ErrorCode.READ_ONLY` added to the existing enum.
