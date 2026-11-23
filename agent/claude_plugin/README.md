# crboost-plan: change plans as tables in Claude Code

When an agent asks to apply a CryoBoost change plan (`apply_changes_now`), this Claude Code plugin opens the plan in
a pane instead of the MCP form: one table per project, a row for each job the plan touches and a column for each
setting it changes, the value after the plan in each cell. You move through it, edit it and decide there:

| Key | Does |
| --- | --- |
| ↑ ↓ ← → | move between cells; the columns scroll sideways with you |
| Enter | edit the cell (a number or text is typed, a choice picked, an on/off switched) |
| 1–9 | show that project; `s` the plan's steps in words |
| a / d | apply the plan / decline it |
| Esc | back to the prompt (the pane stays; ctrl+x tab or a click returns to it) |

The line under the table describes the cell you are on: the setting's help, what the job has now and what it gets,
its type and limits. Deletions can be kept (`Keep` drops that step). Every edit goes to your CryoBoost server, which
rewrites the plan and checks it again, as an edit on the plan's page does; the page and the pane show the same plan,
and an edit or approval on the page reaches the pane.

The agent's call waits until you decide, and then reads what happened: applied (with what you edited), declined,
or closed without deciding. The plan, its edits and the decision are the server's (`api/plans.py`: `get_plan_table`,
`edit_plan`, `decide_plan`, `get_plan_change`); those verbs are never listed as tools, so the model cannot edit or
decide a plan itself. The pane reaches your server as the crboost MCP forwarder does: the forwarder names the port
(`get_server_info`, after checking the port is yours), and the pane sends your token from `~/.crboost/token`.

Where the pane cannot be shown (no server, a plan that deletes something for good, a terminal too narrow to seat a
pane), the call goes on to the server's terminal dialog as before.

## Install

The crboost MCP server must be set up first (`docs/agent.md`). Then, on the node where you run Claude Code:

```bash
claude plugin marketplace add <checkout>/agent
claude plugin install crboost-plan@crboost
```

The plugin is read from the checkout itself, so a `git pull` and `/reload-plugins` bring its changes. `/crboost-plan`
brings the pane back if you closed it while a plan waits.

## Developing

`claude --plugin-dir <checkout>/agent/claude_plugin` loads it from disk and reloads it on save.
`claude plugin validate <checkout>/agent/claude_plugin` type-checks it; `claude plugin test <checkout>/agent/claude_plugin`
runs `tests/` (the table drawn from a sample plan; Apply, Decline and an edit against a stand-in server).
