# Talking to your data from Claude Code

Your CryoBoost server answers an API (`POST /api/v1/<verb>`, behind its access token) with one verb for each
thing the UI lets you do to compose, set up and run a project. Claude Code reaches it through a small stdio MCP
server in this checkout, `agent/crboost_mcp.py`, which finds your server, checks that its port is yours, and
forwards each tool call with your token. Nothing runs a model inside the server.

## Setup

On the login node where your server runs (its startup banner names the node):

```bash
claude mcp add --scope user crboost -- <checkout>/crboost mcp
mkdir -p ~/.claude/skills && ln -s <checkout>/agent/skills/crboost ~/.claude/skills/crboost
```

`crboost mcp` runs the forwarder with the checkout's own uv, which fetches its two dependencies (`mcp`, `httpx`)
on first start. A user-scope entry belongs to one Claude Code login: with `CLAUDE_CONFIG_DIR` set, add it with that
same setting. `claude mcp list` then shows `crboost` connected. Started with no server up, the forwarder offers
only `get_server_info`, which says how to start one; once the server answers, the next call brings the full tool
list without `/mcp`.

The forwarder finds the server from `CRBOOST_URL` (localhost only), else the port this checkout last ran on
(`~/.crboost/ports.json`), else the one CryoBoost server running as you on this node. It reads your token from
`~/.crboost/token` and sends it only to a port whose listener runs under your uid; it never reaches another
login node.

## Permissions

In `~/.claude/settings.json`:

```json
{
  "permissions": {
    "allow": ["mcp__crboost__get_*", "mcp__crboost__list_*", "mcp__crboost__inspect_*", "mcp__crboost__plan_*"],
    "deny": ["Edit(//**/project_params.json)"]
  }
}
```

The reads then run without a prompt; every other verb prompts. `create_project`, `run`, `stop`, `cancel_job`,
`cancel_leftovers` and the two tilt-prediction verbs ask on every call whatever the rules say: you see every
value before a project exists, and every submission or cancellation of SLURM work. The deny rule keeps Claude
Code's own file tools off the project files; it does not stop a script run through Bash.

## What it does and does not do

- It makes projects from any data folder the landing page can scan, and only new ones. Every acquisition value
  comes from you: it shows what the mdocs say and asks for the rest. It also makes a project from other
  projects' tomograms (their Reconstruct or Denoise Predict jobs), ready for template matching, whose values
  come with them.
- It makes templates and masks as the template workbench does, and asks for a pixel size the project's
  tomograms do not give, or a mask's threshold.
- It adds any combination of jobs the roster allows, sets any parameter, SLURM setting or input the job pages
  allow, plans a run and runs it, under the UI's rules: a refusal there is a refusal here.
- It changes only the projects it made. Any project can be read; for one made by hand it gives you the link.
- Looking and judging stay with you in the UI: the tilt review and Approve, curating picks, judging particles,
  the STA kitchen. Every answer carries the link to what it touched.
- It reports a failure with its log lines and runs again only when you ask.
- Every write is a line in the project's `Logs/agent_api.jsonl` and an event on the server's console.

## From a script or curl

```bash
curl -s -X POST -H "Authorization: Bearer $(cat ~/.crboost/token)" localhost:<port>/api/v1/get_server_info
curl -s -H "Authorization: Bearer $(cat ~/.crboost/token)" localhost:<port>/api/v1/tools
```

Write verbs take the token only as the Bearer header, never from the browser cookie. FastAPI's schema is at
`/openapi.json`.
