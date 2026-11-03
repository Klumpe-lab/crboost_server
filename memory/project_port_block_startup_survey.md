---
name: project_port_block_startup_survey
description: "Server picks its own port from 24300–24331, surveys /proc for leftovers (self-tunnels, hung servers, orphan workers) on every start, hardened tunnel command; BUILT 2026-10-21, NOT RUN"
metadata:
  type: project
---

Built 2026-10-21 on user request ("foolproof, 5-year-old usable" startup). NOT RUN (no python in the sandbox; ruff clean).
main.py is now thin (stdlib + services/port_survey.py + services/startup_spinner.py); the app moved to
server_app.py (setup_app, serve on a pre-bound socket). Record: roadmap 23 stage log, last entry.

**Why:** "misconnections / hangs forever" on the headnode. Found 2026-10-21 on clip-login-1: ten of the user's
`ssh -f -N -L X:localhost:X user@clip-login-1` tunnels running ON the headnode (banner line pasted in the wrong
terminal), holding [::1]:X while crboost bound only IPv4 0.0.0.0 → localhost tunnels landed in the self-loop;
another user sat on 8081 (README default); the old main.py printed links before uvicorn bound.

**How to apply:** a second server of the same checkout needs `--port` (the survey otherwise prints the running
one's links and exits). Leftovers are only reported (`kill -9 …`), never killed — user said "suggest the command".
Related: [[project_ui_latency_roadmap34]] (workers: PDEATHSIG added; spawn now imports the thin main.py).

**2026-10-21 later:** first real start took 16.3 s cold ("Server starting" → monitor up; survey not in it), 7.7 s warm
(--port 8042 rerun). Old main.py logged "Server starting" AFTER its imports and then needed 0.3–1.4 s (5 s on a slow-
SLURM day) → module imports are the bulk (~6 s warm), not measured per module (needs `-X importtime`). Built (NOT
RUN): stage spinner (ASCII dose-symmetric tilt sweep, "ok <stage> <s>" lines), "Started in …" log line + summary
`startup` row, SLURM/recent-projects summary moved to a background thread alongside app build + server start.
User rejected braille/npm-style spinners.
