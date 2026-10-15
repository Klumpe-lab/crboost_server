# Roadmap 23 — One server per person with Jupyter-style tokens

**Status:** rev 2, 2026-10-15: every anchor re-checked against main, and the stages re-ordered so the token gate
comes first. It is what the Munich install needs, and it does not depend on the lease. Approved 2026-09-29;
**nothing built**. Being built in a separate worktree. **Pickup: start at stage 1.** Munich ships after stage 1;
stages 2–7 are the multi-user CBE work.

Release item: "token-based authentication — two users on the same port interfere; per-member ports are too
brittle; keep being able to look at, debug and dig through each other's projects." The Munich install review
asked for the same thing (roadmap 20, R7): anyone on the network can open the GUI — "secure, token like Jupyter".

**Decision (maintainer, 2026-09-29):** Jupyter-style tokens. Every person runs **their own** server,
started or reused by one command that prints a URL carrying a secret token. Identity is the Unix user
running the process. Looking at someone else's project means opening it **read-only in your own server**.
No VM, no hub, no SSO — not now.

**Order (rev 2):** stage 1 gates the servers that exist today: whoever opens the port needs the token. Stages 3–7
then turn one shared port into one server per person, and stage 3 (the lease) has to land before a second server
may write a project.

## 1. How the connections work

**Today** — one process, no auth, runs as whoever started it:
```
  your browser ──────┐
  colleague's ───────┼──► headnode:8081 ──► python main.py (uid = whoever started it)
  anyone on VBC net ─┘        no auth              │ sbatch as that person
                                                   ▼
                                     SLURM jobs + files under /groups as that person
```

**After stage 1** — the same process, behind a token:
```
  your browser  ── GET /?token=Xq3… ─► token gate: match ─► 302 → /   Set-Cookie: crboost_token_8081=Xq3…
                ── GET /  (cookie) ───► page
                ── WS /_nicegui_ws/socket.io (cookie) ─► live UI
                ── GET /api/vis-asset?… (cookie) ──────► images
  anyone else   ── any path, no cookie ─► 401 (http) / close 1008 (websocket)
```

**After stage 7** — one process per person, each behind its own token:
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

**Starting (stage 5, the launcher):**
```
  $ crboost
    ~/.crboost/run/<install>.json says a server is alive and /healthz answers?
        yes → print its URL
        no  → start main.py detached on your stable port, wait for /healthz, print the URL
  CryoBoost is running:
    http://<headnode>:21234/?token=Xq3…                     (on the VBC network / VPN)
    ssh -N -L 21234:localhost:21234 <you>@<headnode>        (from outside, then
    http://localhost:21234/?token=Xq3…                       open this locally)
```

**One writer per project — the lease (stage 3)** (`<project>/.crboost/lease.json`):
```
  your server:   writes {user, host, pid, port, version, heartbeat} every 60 s → may save
  their server:  lease fresh (< 3 min) → project opens read-only, "being edited in <you>'s CryoBoost"
                 lease stale (your server died/stopped) → read-only + "Take over editing" (confirm)
```

## 2. What it fixes, and what to be careful with

**Fixes:**
1. **Strangers driving your server.** Today anyone who reaches the port can submit jobs as its owner, delete any
   project the owner may delete (`backend.delete_project`, `backend.py:1872-1880`, behind the Projects Overview
   deletion mode), and **read any file the owner can read**: `/api/file?path=` (`ui/template_workbench.py:102-107`)
   returns any existing file, `~/.ssh/id_rsa` included, and `/api/tilt-thumb` (`main.py:129-136`) any `.png`.
   rev 1 listed only the second.
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
   screenshot; whoever has it controls your server as you. The redirect strips it from the address bar; deleting
   `~/.crboost/token` and restarting revokes it (`crboost rotate-token` once the launcher exists). Never put a
   tokened URL in a bug report, and never render the token in the UI.
2. **Plain HTTP.** On the internal network the token and cookie travel unencrypted — the norm for Jupyter
   on HPC, fine inside VBC, not fine on an untrusted network. The SSH tunnel variant is encrypted.
3. **Localhost is shared on a headnode.** Every user on the machine can connect to your port — the token
   is needed even if the server bound to 127.0.0.1 only.
4. **Cookies are scoped by host, not port.** Two CryoBoost servers on the same host — or *every* server
   you reach through a tunnel, since they all look like `localhost` — share one cookie jar. So the token
   cookie is named per port (`crboost_token_<port>`), and so is NiceGUI's session cookie: NiceGUI 3.0.3
   installs Starlette's `SessionMiddleware` with the default cookie name `session`, and skips that if one is
   already registered on its own app (`nicegui/storage.py:38-46`). Without this, switching between your dev and
   release servers in one browser resets each other's sessions (and `app.storage.user` with them).
5. **Two servers of the same user** (dev checkout + shared install) are both "you": the run file is keyed
   by install path, and the lease distinguishes them by pid/port, so they never both write a project.
6. **Side channels the token does not cover.** The ChimeraX curation desktop's VNC (password by default;
   `curation.passwordless_vnc`, `config_service.py:180`, opt-in removes it) and its REST command port
   (127.0.0.1 on the compute node, no auth — reachable by other users' jobs on the same node). Keep VNC
   passworded; the REST port is a separate, small fix.
7. **Viewers must never reconcile.** The monitor tick reconciles every registered project with
   `pipeline_active` (`pipeline_monitor.py:220-250`). With SLURM `PrivateData` a viewer cannot see the owner's
   jobs and would conclude them FAILED after `_AFTEROK_ABSENT_GRACE_SEC` = 90 s (`pipeline_runner.py:55`).
8. **Version skew.** An older server saving a newer project silently drops what it cannot parse — the
   schema guard (§3.5) makes such a project read-only instead.
9. **Idle servers.** A forgotten server keeps polling SLURM for its projects; idle shutdown after 12 h
   without a client. Nothing is lost — the next `crboost` catches up from disk and SLURM.
10. **Copied links.** A `/p/<project>/…` link pasted to a colleague opens only in a browser holding that server's
    cookie. Theirs gets a 401; from stage 4 on they open it in their own server (`crboost open <route>`).

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

### 3.1 The token gate (stage 1)
One new module (e.g. `services/server_auth.py`, stdlib only) and edits to `main.py`, `ui/template_workbench.py`,
`ui/landing_status_strip.py` and `README.md`.

- **One pure-ASGI middleware around the outermost app:** `uvicorn.run(app, …)` (`main.py:249`) becomes
  `uvicorn.run(TokenGate(app, token, port), …)`. It checks **http and websocket** scopes, so NiceGUI's socket.io
  mount (`nicegui/nicegui.py:53`, inside the app we pass), its long-polling fallback, `/api/*`, `/static`
  (`main.py:187`) and NiceGUI's own assets are all behind it. The `lifespan` scope passes through untouched (the
  startup/shutdown handlers at `main.py:217-218` ride on it).
- **`?token=`** → compare with `hmac.compare_digest`; on a match set `crboost_token_<port>` (HttpOnly,
  SameSite=Lax, Path=/) and 302 to the same path with the rest of the query kept, so a deep link
  `/p/<project>/<view>?base=…&token=…` lands on its route. A matching cookie passes. Anything else: http → 401
  page "open the link printed when the server started"; websocket → close 1008 before accept.
- **The token:** `~/.crboost/token`, made once with `secrets.token_urlsafe(32)`, created mode 600
  (`os.open(…, O_CREAT|O_EXCL, 0o600)`), reused across restarts so bookmarks and open tabs survive. No opt-out
  flag: a dev server on a shared headnode needs it as much as the release one.
- **Where the link is shown:** the startup banner (`banner_lines`, `main.py:59-64`) prints the direct URL with
  `?token=` and the tunnel command plus `http://localhost:<port>/?token=…`. The landing strip's host popover
  (`ui/landing_status_strip.py:367-377`) tells people to "open http://localhost:{port}", which now 401s in a fresh
  browser: point it at the startup link instead, without showing the token.
- **Session cookie per port:** `nicegui.app.add_middleware(SessionMiddleware, secret_key=…,
  session_cookie=f"crboost_session_{port}")` before `ui.run_with` (`main.py:227`). It must go on NiceGUI's app
  (`from nicegui import app`), not our FastAPI app: that is the list `set_storage_secret` inspects
  (`nicegui/storage.py:40`), after which NiceGUI only appends its request tracking.
- **Storage secret:** `~/.crboost/secret` (600, made once) unless `CRBOOST_STORAGE_SECRET` is set; today the
  fallback is the literal `"crboost-change-me"` (`main.py:220`).
- **NiceGUI storage dir:** `~/.crboost/nicegui`. Today it is `.nicegui` in the working directory, so the shared
  install's `.nicegui/` holds every user's browser storage. `Storage.path` is read at call time
  (`nicegui/storage.py:53, 75`), so assign the class attribute before `ui.run_with`; the `NICEGUI_STORAGE_PATH`
  env var only works if set before the first `nicegui` import.
- **Raw file routes (defence in depth).** The gate already stops strangers; these guards stop a leaked link from
  reading the owner's home directory:
  - `/api/file` (`ui/template_workbench.py:102-107`; only caller `:823`, which loads template and mask volumes
    into Mol*): the `/api/vis-asset` guard (`main.py:138-164`: the resolved path under a loaded project root)
    plus volume suffixes only. Templates live under `<project>/templates/<species_id>/`
    (`services/species_admin.py:79`); check that species taken from the catalog (`species_catalog_root`) are
    copied there before relying on the root rule.
  - `/api/tilt-thumb` (`main.py:129-136`; callers `ui/tilt_previews.py:827`, `services/dashboard_data.py:292`):
    under a loaded project root.
  - `/api/project-preview` (`main.py:166-185`) stays as is: reconstruction PNGs inside a project tree, by design
    for projects nobody has loaded.
- Repo-relative paths (rev 1's last item) are done: roadmap 20 S2a, `config_service.REPO_ROOT`; no `Path.cwd()`
  is left.
- `README.md` "Running": the server prints a link with `?token=`; open that one.

### 3.2 Single writer per project — the lease (stage 3)
- `<project>/.crboost/lease.json`, atomic write. Taken when a server opens a project for writing (open,
  create, recovery). Free = absent, stale (heartbeat > 3 min) or ours. Heartbeat every 60 s from the
  monitor tick (`TICK_INTERVAL_SEC` = 3 s, `pipeline_monitor.py:52`). Same pattern as the job-dir owner file in
  roadmap 33 S1: atomic create, a liveness check, steal only when dead.
- Held and fresh → read-only. Stale → read-only with **Take over editing** (confirmation; written to the
  project's event log). Released on close/shutdown.
- `ProjectState.save()` (`project_state.py:1100`) raises `ReadOnlyProject` without the lease — never a silent
  no-op — including the direct `state.save()` calls in `ui/aggregation/merge_card.py:151,156`. Backend mutators
  return `err(code=ErrorCode.READ_ONLY)`: `start_pipeline` (`backend.py:247`), `delete_job` (`:258`),
  `submit_tilt_filter_predict` (`:297`), `approve_tilt_filter` (`:490`), the particle-set creators and
  `delete_particle_set` (`:1382-1532`), `create_project_and_scheme` (`:1846`), `delete_project` (`:1872`),
  `transfer_project_ownership` (`:1882`). `ErrorCode` (`services/result.py:23`) gains the member.
- Derived caches a viewer writes into a foreign project are best-effort: a `PermissionError` there is
  caught narrowly and viewing continues without the cache.

### 3.3 Read-only viewing (stage 4)
- Header chip: "Read-only — being edited in <user>'s CryoBoost (20 s ago)" / "… nobody is editing — Take over".
- One `callbacks["can_write"]` flag drives the mutating controls; the save guard backs up anything missed.
- A read-only state re-reads `project_params.json` when its mtime moves (signature-gated, 15 s). The
  "in-memory state is authoritative" rule applies to writers only. Viewers never reconcile (§2 item 7): the tick
  (`pipeline_monitor.py:220-250`) skips read-only states.

### 3.4 Recovery and reconciliation are scoped (stage 3)
- `_discover_and_recover` (`pipeline_monitor.py:88-129`) loads every project in the default base whose file
  says `pipeline_active`. For afterok projects that load is the adoption (`_recover_one`, `:132-145`): from
  then on the tick reconciles it, so N servers mean N reconcilers per project. Adopt only if the lease is free or
  ours **and** the owner is us or `@lab` (legacy owner-less projects: `created_by`).
- Every chain `sbatch` gets `--kill-on-invalid-dep=yes` next to `--dependency=afterok` (`pipeline_runner.py:1119-1120`).
  Today the reconciler cancels DependencyNeverSatisfied dependents itself (`pipeline_runner.py:480-483`), which
  happens only while the owner's server runs.

### 3.5 Schema guard (stage 3)
- File schema newer than `SCHEMA_VERSION` (3, 9) (`project_state.py:52`) → read-only regardless of the lease,
  banner "written by a newer CryoBoost (x.y) — update to edit". Today a newer minor is logged at INFO as "Will be
  upgraded on next save" (`project_state.py:1166-1190`) and the save stamps the code's version (`:1108`): that
  is the data loss.

### 3.6 File modes (stage 3)
- `ProjectState.save()` (`mkstemp`, `project_state.py:1112`) and the registry sidecars (`registry.py:438`):
  `os.fchmod(fd, 0o666 & ~umask)` after `mkstemp`. Today every save leaves mode 0600 — hidden by Isilon's ACLs,
  fatal on a POSIX filesystem.

### 3.7 The launcher — `bin/crboost` (stage 5; stdlib only, starts instantly)
- `crboost` / `crboost start`: read `~/.crboost/run/<install-hash>.json` (mode 600: pid, host, port,
  version, started). Alive and `/healthz` answers with our token → print the URL. Otherwise start
  `main.py` detached (`setsid`, output to `~/.crboost/logs/`), wait for `/healthz`, print the URL.
  `/healthz` sits behind the gate like everything else; the launcher has the token.
- `crboost stop | restart | status | logs | url | rotate-token | open <project-path-or-/p/route>`.
- **Stable URL:** port = `20000 + uid % 10000` (next free port on collision; offset per install so dev
  and release differ); the token from stage 1.
- `umask 002` for the server; SLURM propagates the submitter's umask to jobs.
- Idle shutdown after 12 h without a connected client (configurable).

### 3.8 One shared install (stage 6)
- `/groups/klumpe/software/crboost_server` is the release install: a `main` clone updated with
  `git pull origin main` (last 2026-10-14), its own venv and `.nicegui/`. `crboost` reaches `PATH` through one
  line in `~/.bashrc`.
- There is no version string in the code. The server records the commit it started from (the install's
  `.git/HEAD`) and compares it with the install every 15 min → "update installed — Restart to update" banner; the
  button runs `crboost restart` detached; same URL afterwards.

## 4. Stages

### Stage 1 — Token gate (Munich ships after this)
- §3.1.
- *Success:*
  1. A fresh browser without the token gets 401 on `/`, `/p/<project>`, `/api/vis-asset?…`, `/static/main.css`
     and NiceGUI's assets, and the socket.io handshake is refused.
  2. The printed link opens the app and the address bar shows no token afterwards; a deep link carrying
     `?token=` lands on its route.
  3. `kill -9` + restart: the same link and an already-open tab keep working.
  4. Dev and release servers open side by side in one browser through one tunnel without logging each other
     out or sharing prefs.
  5. With the token: `/api/file?path=$HOME/.ssh/id_rsa` is refused; a template still loads in the Species
     page's viewer; tilt thumbnails still render in the Tilts tab and the Journey.
  6. The landing strip's host popover no longer points at a URL that 401s.

### Stage 2 — Facts (user-run, ~30 min; item 5 in the sandbox)
1. `scontrol show config | grep -iE 'PrivateData|SchedulerParameters'`.
2. RSS of a running server (`ps -o rss= -p <pid>`) with one project open.
3. From a desktop on the VBC network: does `http://<headnode>:<high port>` load, or does everyone tunnel?
4. With a colleague: can they create and delete a file inside your project dir on `/groups` (Isilon ACLs)?
5. List every file the server writes while *viewing* a project (cache writers) → the §3.2 list.
- *Success:* answers recorded here.

### Stage 3 — Lease, save guard, scoped recovery, schema guard, file modes
- §3.2 minus UI, §3.4, §3.5, §3.6.
- *Success:* two servers open one project → the second is read-only and its saves raise; a restarted
  server adopts only its own active projects; a newer-schema project opens read-only in older code; a
  fresh save is 0644/0664.

### Stage 4 — Read-only UX
- §3.3.
- *Success:* a colleague watches your pipeline advance live in their own server and cannot queue
  anything; after you stop yours they take over with one confirmation.

### Stage 5 — Launcher
- §3.7.
- *Success:* `crboost` twice prints the same URL; `kill -9` + `crboost` → the bookmark still works; dev and
  release installs get different ports.

### Stage 6 — Shared install + update banner
- §3.8.
- *Success:* after the install is updated, running servers show the banner within 15 min; Restart brings
  up the new version on the same URL.

### Stage 7 — Retire the shared port
- README "Running" → `crboost` (tunnel variant kept as the from-outside route); announce to the lab.

## 5. Not doing
- Per-principal refactor of prefs/config/tasks/curation — moot with one person per process.
- User database, login page, roles — Unix identity and group permissions already exist.
- TLS — tunnel users get SSH encryption; the internal network carries plain HTTP as with Jupyter.

**Parked, not now:** a local `crboost connect` helper that does the SSH tunnel behind the scenes;
a front door (Open OnDemand app or a JupyterHub-style hub) with institute login. Both would spawn exactly
these per-user servers, so nothing here needs redoing; the only prerequisite would be prefix-safe URLs
(`route_to_path`, `ui/routing.py:92-104`; 7 hard-coded `/api` / `/static` / `/molstar-workbench` URLs; 15
`navigate.to(…)` calls).

## 6. Runtime checklist
Stage 1 (one person, then Munich): the six success checks above.

Stages 3–7 (two people):
1. Both run `crboost` → two URLs; each without its token → 401.
2. A opens B's running project → read-only chip, live progress, Run disabled.
3. B stops their server; A takes over; B restarts and sees read-only "being edited by A".
4. `sacct -u <A>` shows A's jobs under A.

## 7. Risks
1. If the login node reaps long-running processes, `crboost` restarts in seconds; pipelines are unaffected.
2. With `PrivateData`, viewers show the owner's last-written state, not live SLURM state — show its age.
3. Clock skew between login nodes vs the 3 min staleness window — fine with NTP; revisit if takeovers misfire.

**Modern-Python weave-in:** `TokenGate` as a plain ASGI class (`__call__(scope, receive, send)`), no framework
middleware base: it must see websocket scopes. The lease as a frozen `dataclass` with
`acquire/heartbeat/release` returning `ok()/err()`; `ReadOnlyProject(Exception)`; `ErrorCode.READ_ONLY` added to
the existing enum.
