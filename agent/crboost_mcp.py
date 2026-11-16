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

A call the user must confirm (apply_changes_now) comes back with `confirm`: a dialog's text and a one-time token.
The forwarder shows the text in Claude Code's own dialog (MCP form elicitation: asked mid-call on the handshake
wire, answered "input required" and resolved on Claude Code's retry on the 2026-07-28 wire), and only on the
user's apply calls again with the token. The token never reaches the agent, nor Claude Code.

    claude mcp add --scope user crboost -- <checkout>/crboost mcp

The launcher runs it with the checkout's own uv (`.uv/bin/uv`, the one `uv.toml` pins), wherever Claude Code
was started.
"""

from __future__ import annotations

import getpass
import json
import os
import secrets
import socket
import sys
import time
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
        f"No CryoBoost server of {USER}'s answers on this login node ({HOST}). Start one with "
        f"`{CHECKOUT}/crboost`; its startup banner names the node it runs on, and this forwarder must run on that "
        "same node."
    )


class Forwarder:
    """The connection to the server: the tool list it serves, and each call forwarded to it."""

    def __init__(self) -> None:
        self.port: int | None = None
        self.why = ""
        self.tools: list[types.Tool] = []
        self.signature: tuple = ()
        # Tools the user confirms in this forwarder's dialog (`_meta["crboost/dialog"]`).
        self.dialogs: set[str] = set()

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
        self.dialogs = {t["name"] for t in served if (t.get("_meta") or {}).get("crboost/dialog")}
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


CONFIRM_KEY = "confirm"
# How long an open dialog (2026-07-28 wire) waits for the user's answer before its token is forgotten.
DIALOG_TTL_S = 3600.0
NOT_APPLIED = (
    "The user declined the plan in the terminal dialog: nothing changed. Ask them what to change, or leave it on "
    "its page."
)
# Keys only this forwarder sets on a dialog verb's call; one the agent sends is dropped.
FORWARDER_KEYS = ("confirmation", "edits", "dialog")


def accepted(answer: Any) -> dict[str, Any] | None:
    """The form's fields when the user pressed Accept; None for a decline, a cancel, or anything malformed."""
    if isinstance(answer, dict):
        action, content = answer.get("action"), answer.get("content")
    else:
        action, content = getattr(answer, "action", None), getattr(answer, "content", None)
    if action != "accept":
        return None
    return dict(content) if isinstance(content, dict) else {}


def can_ask(ctx) -> bool:
    caps = ctx.session.client_capabilities
    return caps is not None and caps.elicitation is not None


class Dialogs:
    """Dialogs open on the 2026-07-28 wire, where the server cannot ask mid-call: the tool answers "input
    required" with the dialog, and Claude Code calls again with the user's answer and the opaque state. The state
    is a random key into this table; the server's token stays here and never travels."""

    def __init__(self) -> None:
        self._open: dict[str, tuple[float, str, dict[str, Any], dict[str, Any], dict[str, Any]]] = {}

    def open(self, name: str, arguments: dict[str, Any], confirm: dict[str, Any], first: dict[str, Any]):
        now = time.monotonic()
        self._open = {k: v for k, v in self._open.items() if now - v[0] < DIALOG_TTL_S}
        state = secrets.token_urlsafe(18)
        self._open[state] = (now, name, arguments, confirm, first)
        request = types.ElicitRequest(
            params=types.ElicitRequestFormParams(message=confirm["message"], requested_schema=confirm["schema"])
        )
        return types.InputRequiredResult(input_requests={CONFIRM_KEY: request}, request_state=state)

    def take(self, state: str, name: str, arguments: dict[str, Any]):
        """The dialog this retry answers, once; None when it is unknown, expired, or for another call."""
        held = self._open.pop(state, None)
        if held is None or time.monotonic() - held[0] >= DIALOG_TTL_S or held[1] != name or held[2] != arguments:
            return None
        return held


async def ask_now(ctx, confirm: dict[str, Any]) -> dict[str, Any] | None | str:
    """The handshake-era wire: ask in the middle of the call. The fields on Accept, None otherwise, or why it
    could not be asked."""
    try:
        answer = await ctx.session.elicit_form(confirm["message"], confirm["schema"], related_request_id=ctx.request_id)
    except Exception as e:  # any failure to ask is reported to the agent as "not asked", never as an Accept
        print(f"crboost: the confirmation dialog failed: {e!r}", file=sys.stderr)
        return f"The confirmation dialog could not be shown: {e}"
    return accepted(answer)


async def settle(forwarder: Forwarder, name: str, arguments: dict[str, Any], confirm, first, answer):
    """The call again with the server's one-time token and the form's fields when the user pressed Accept; else
    what happened, for the agent, without the token."""
    if isinstance(answer, dict):
        return await forwarder.call(name, {**arguments, "confirmation": confirm["token"], "edits": answer})
    if answer is None:
        return {"success": False, "error": NOT_APPLIED, "declined": True, "ui_url": first.get("ui_url")}
    return {**first, "error": f"{answer} {first.get('error', '')}".strip()}


def main() -> None:
    forwarder = Forwarder()
    dialogs = Dialogs()
    bus = InMemorySubscriptionBus()

    async def on_list_tools(ctx, params) -> types.ListToolsResult:
        await anyio.to_thread.run_sync(forwarder.connect)
        return types.ListToolsResult(tools=forwarder.listed())

    async def on_call_tool(ctx, params: types.CallToolRequestParams):
        changed = await anyio.to_thread.run_sync(forwarder.connect)
        if changed:
            # The handshake era takes a plain notification; the 2026-07-28 wire, the listen stream.
            await bus.publish(ToolsListChanged())
            try:
                await ctx.session.send_tool_list_changed()
            except (RuntimeError, AttributeError) as e:
                print(f"crboost: tool list change not sent on this session: {e}", file=sys.stderr)
        arguments = dict(params.arguments or {})
        if params.name in forwarder.dialogs:
            # Only this forwarder says a dialog can be shown or was answered; never the agent.
            for key in FORWARDER_KEYS:
                arguments.pop(key, None)
            arguments["dialog"] = True
        try:
            if params.name in forwarder.dialogs and params.request_state is not None:
                held = dialogs.take(params.request_state, params.name, arguments)
                if held is None:
                    result = {"success": False, "error": "That dialog is no longer open: nothing changed. Call again."}
                else:
                    _, _, _, confirm, first = held
                    answer = (params.input_responses or {}).get(CONFIRM_KEY)
                    result = await settle(forwarder, params.name, arguments, confirm, first, accepted(answer))
            else:
                result = await forwarder.call(params.name, arguments)
                confirm = result.pop("confirm", None)
                if confirm:
                    if not can_ask(ctx):
                        answer = "This Claude Code does not show a server's dialogs (no MCP elicitation)."
                    elif ctx.session.can_send_request:
                        answer = await ask_now(ctx, confirm)
                    else:
                        return dialogs.open(params.name, arguments, confirm, result)
                    result = await settle(forwarder, params.name, arguments, confirm, result, answer)
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
