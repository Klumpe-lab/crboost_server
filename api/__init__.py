"""The agent API: one verb for each thing the UI lets you do to compose, set up and run a project.

Each verb goes through the same service function as its button, under the same rules (the doors in
services/). `verbs.VERBS` is the one table; `router` serves it as `POST /api/v1/<verb>` and as the MCP tool
list at `GET /api/v1/tools`. Any client with the token can call it; Claude Code does through the stdio
forwarder in agent/. Nothing here imports the UI.
"""
