#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp>=2.3,<3", "httpx>=0.28,<1"]
# ///
"""The crboost MCP server for Claude Code: a thin stdio forwarder to the user's own CryoBoost server.

It holds no CryoBoost logic. It finds the user's server on this node, lists the server's verbs as tools
(`GET /api/v1/tools`) and forwards each tool call as `POST /api/v1/<verb>` with the access token as a Bearer
header. It imports only the standard library, `mcp` and `httpx`, so it starts well within Claude Code's MCP
start-up timeout, and keeps `mcp` out of the server's environment.

Where the server is: `CRBOOST_URL` (localhost only); else the port this checkout last ran on
(~/.crboost/ports.json); else the one CryoBoost server running as this user on this node. Several, and it
names them. The token (~/.crboost/token) goes only to a port whose listener runs under this user's uid, read
from /proc/net/tcp before every call: every user of a login node shares localhost, and a port can change hands
between restarts. It never reaches across login nodes, where it could not check the owner.

With no server up, it offers `get_server_info`, which says how to start one, and tries again on every call;
once the server answers, it announces the full tool list (`notifications/tools/list_changed`).

    claude mcp add --scope user crboost -- uv run --script <checkout>/agent/crboost_mcp.py
"""

from __future__ import annotations

import getpass
import json
import os
import socket
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import anyio
import httpx
import mcp_types as types
from mcp.server.lowlevel.server import NotificationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.server.subscriptions import InMemorySubscriptionBus, ListenHandler, ToolsListChanged

CRBOOST_HOME = Path.home() / ".crboost"
TOKEN_FILE = CRBOOST_HOME / "token"
PORTS_FILE = CRBOOST_HOME / "ports.json"
CHECKOUT = Path(__file__).resolve().parents[1]
INSTRUCTIONS_FILE = CHECKOUT / "api" / "instructions.md"
# The block CryoBoost servers take their ports from (services/port_survey.py PORT_RANGE).
PORT_RANGE = range(24300, 24332)
# A CryoBoost server names its user in this header of its 401 (services/server_auth.py OWNER_HEADER).
OWNER_HEADER = "x-cryoboost-user"
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_CALL_TIMEOUT_S = 600.0
_PROBE_TIMEOUT_S = 2.0

USER = getpass.getuser()
HOST = socket.gethostname()


# ── Finding the user's server on this node ───────────────────────────────────


def listener_uids() -> dict[int, int]:
    """Every listening TCP port on this node with the uid that owns its socket."""
    out: dict[int, int] = {}
    for name in ("tcp", "tcp6"):
        try:
            lines = Path(f"/proc/net/{name}").read_text().splitlines()[1:]
        except FileNotFoundError:
            continue  # no IPv6 on this node
        for line in lines:
            f = line.split()
            if len(f) >= 8 and f[3] == "0A":  # 0A = LISTEN
                out[int(f[1].rsplit(":", 1)[1], 16)] = int(f[7])
    return out


def cryoboost_owner(port: int) -> str | None:
    """The user a CryoBoost server on this port names in its 401, or None for anything else."""
    try:
        r = httpx.get(f"http://127.0.0.1:{port}/api/v1/tools", timeout=_PROBE_TIMEOUT_S)
    except httpx.HTTPError:
        return None
    return r.headers.get(OWNER_HEADER)


def find_server() -> tuple[int | None, str]:
    """(port, "") of this user's CryoBoost server on this node, or (None, why not)."""
    mine = {p for p, uid in listener_uids().items() if uid == os.getuid()}
    url = os.environ.get("CRBOOST_URL")
    if url:
        parts = urlsplit(url)
        if parts.hostname not in _LOCAL_HOSTS or not parts.port:
            return None, f"CRBOOST_URL={url}: give http://localhost:<port> of a server on this node ({HOST})."
        if parts.port not in mine:
            return None, f"Port {parts.port} on {HOST} is not held by a server of {USER}'s; nothing is sent there."
        return parts.port, ""
    try:
        remembered = json.loads(PORTS_FILE.read_text()).get(str(CHECKOUT))
    except (OSError, ValueError):
        remembered = None
    if remembered in mine and cryoboost_owner(remembered) == USER:
        return remembered, ""
    running = [p for p in sorted(mine) if p in PORT_RANGE and cryoboost_owner(p) == USER]
    if len(running) == 1:
        return running[0], ""
    if running:
        return None, (
            f"Several CryoBoost servers of {USER}'s run on {HOST} (ports {', '.join(map(str, running))}): "
            "name one with CRBOOST_URL=http://localhost:<port>."
        )
    return None, (
        f"No CryoBoost server of {USER}'s answers on this login node ({HOST}). Start one from {CHECKOUT} with "
        "`venv/bin/python3 crboost`; its startup banner names the node it runs on, and this forwarder must run "
        "on that same node."
    )


class Forwarder:
    """The connection to the server: the tool list it serves, and each call forwarded to it."""

    def __init__(self) -> None:
        self.port: int | None = None
        self.why = ""
        self.tools: list[types.Tool] = []
        self.signature: tuple = ()

    def _token(self) -> str:
        return TOKEN_FILE.read_text().strip()

    def _owned(self) -> bool:
        """The port is still held by a socket of this user's: checked before the token goes anywhere."""
        return self.port is not None and listener_uids().get(self.port) == os.getuid()

    def connect(self) -> bool:
        """Find the server and read its tools; True when the tool list changed."""
        if not self._owned():
            self.port, self.why = find_server()
        if self.port is None:
            changed = bool(self.tools)
            self.tools, self.signature = [], ()
            return changed
        try:
            r = httpx.get(
                f"http://127.0.0.1:{self.port}/api/v1/tools",
                headers={"Authorization": f"Bearer {self._token()}"},
                timeout=_PROBE_TIMEOUT_S * 5,
            )
            r.raise_for_status()
            served = r.json()["tools"]
        except (httpx.HTTPError, OSError, ValueError, KeyError) as e:
            self.port, self.why = None, f"The CryoBoost server did not list its tools: {e}"
            changed = bool(self.tools)
            self.tools, self.signature = [], ()
            return changed
        signature = tuple((t["name"], json.dumps(t["inputSchema"], sort_keys=True)) for t in served)
        if signature == self.signature:
            return False
        self.signature = signature
        self.tools = [
            types.Tool(
                name=t["name"],
                description=t["description"],
                input_schema=t["inputSchema"],
                annotations=types.ToolAnnotations(read_only_hint=t["annotations"].get("readOnlyHint", False)),
                meta=t.get("_meta"),
            )
            for t in served
        ]
        return True

    def listed(self) -> list[types.Tool]:
        if self.tools:
            return self.tools
        return [
            types.Tool(
                name="get_server_info",
                description="This user's CryoBoost server, or how to start one when none answers.",
                input_schema={"type": "object", "properties": {}},
                annotations=types.ToolAnnotations(read_only_hint=True),
            )
        ]

    async def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.port is None or not self._owned():
            return {"success": False, "error": self.why or "No CryoBoost server found."}
        async with httpx.AsyncClient(timeout=_CALL_TIMEOUT_S) as client:
            r = await client.post(
                f"http://127.0.0.1:{self.port}/api/v1/{name}",
                json=arguments,
                headers={"Authorization": f"Bearer {self._token()}"},
            )
        try:
            return r.json()
        except ValueError:
            return {"success": False, "error": f"HTTP {r.status_code}: {r.text[:500]}"}


def main() -> None:
    forwarder = Forwarder()
    bus = InMemorySubscriptionBus()

    async def on_list_tools(ctx, params) -> types.ListToolsResult:
        await anyio.to_thread.run_sync(forwarder.connect)
        return types.ListToolsResult(tools=forwarder.listed())

    async def on_call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
        changed = await anyio.to_thread.run_sync(forwarder.connect)
        if changed:
            # The handshake era takes a plain notification; the 2026-07-28 wire, the listen stream.
            await bus.publish(ToolsListChanged())
            try:
                await ctx.session.send_tool_list_changed()
            except (RuntimeError, AttributeError) as e:
                print(f"crboost: tool list change not sent on this session: {e}", file=sys.stderr)
        try:
            result = await forwarder.call(params.name, dict(params.arguments or {}))
        except httpx.HTTPError as e:
            result = {"success": False, "error": f"The CryoBoost server did not answer: {e}"}
        text = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=text)], is_error=not result.get("success", False)
        )

    server = Server(
        "crboost",
        version="1",
        instructions=INSTRUCTIONS_FILE.read_text().strip() if INSTRUCTIONS_FILE.is_file() else None,
        on_list_tools=on_list_tools,
        on_call_tool=on_call_tool,
        on_subscriptions_listen=ListenHandler(bus),
    )

    async def serve() -> None:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream, write_stream, server.create_initialization_options(NotificationOptions(tools_changed=True))
            )

    anyio.run(serve)


if __name__ == "__main__":
    main()
