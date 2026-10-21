"""Jupyter-style access token for the CryoBoost server.

Whoever reaches the port controls the server as the user running it, so every HTTP request and
websocket must carry the token: once as `?token=…` (the link printed at startup), after which a
per-port cookie carries it. The token and the session secret live in ~/.crboost with mode 600 and
are reused across restarts, so bookmarks and open tabs keep working. Deleting ~/.crboost/token and
restarting issues a new one.
"""

from __future__ import annotations

import getpass
import hmac
import html
import os
import secrets
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

CRBOOST_HOME = Path.home() / ".crboost"
TOKEN_FILE = CRBOOST_HOME / "token"
STORAGE_SECRET_FILE = CRBOOST_HOME / "secret"
NICEGUI_STORAGE_DIR = CRBOOST_HOME / "nicegui"

# Sent with the 401, so whoever reaches the wrong port learns whose server it is (and services/port_survey.py
# can tell a CryoBoost server from another program).
OWNER_HEADER = "x-cryoboost-user"


def _unauthorized_page(owner: str, port: int) -> bytes:
    owner = html.escape(owner)
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>CryoBoost: token required</title></head>"
        "<body style='font-family: sans-serif; margin: 3em;'>"
        f"<h3>This is {owner}'s CryoBoost server (port {port}). It needs its access token.</h3>"
        "<p>Open the link the server printed when it started (it ends in <code>?token=&hellip;</code>). "
        f"The token is also in <code>~/.crboost/token</code> of {owner}.</p>"
        f"<p>Not {owner}? Then this is not your server: yours printed its own port and link when it started.</p>"
        "</body></html>"
    ).encode()


def read_or_create_secret(path: Path) -> str:
    """The secret stored in `path`; on first use a new random one, created with mode 600."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        value = path.read_text().strip()
        if not value:
            raise RuntimeError(f"{path} is empty: delete it and restart to create a new one") from None
        return value
    value = secrets.token_urlsafe(32)
    with os.fdopen(fd, "w") as f:
        f.write(value + "\n")
    return value


def session_cookie_name(port: int) -> str:
    """Cookies are scoped by host, not port: servers sharing a host (or a tunnel's localhost) need
    their own cookie names or they reset each other's sessions."""
    return f"crboost_session_{port}"


class TokenGate:
    """ASGI middleware around the outermost app. It sees http and websocket scopes, so NiceGUI's
    socket.io mount (websocket and long-polling), /api/*, /static and NiceGUI's assets are all behind
    it; lifespan passes through.

    `?token=` matching → set the per-port cookie and redirect to the same URL without the token.
    A matching cookie → pass. Anything else → 401 page, or the websocket handshake is refused."""

    def __init__(self, app, token: str, port: int) -> None:
        self.app = app
        self._token = token.encode()
        self._cookie = f"crboost_token_{port}"
        self._owner = getpass.getuser()
        self._page = _unauthorized_page(self._owner, port)

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        query = parse_qsl(scope.get("query_string", b"").decode("latin-1"), keep_blank_values=True)
        offered = next((v for k, v in reversed(query) if k == "token"), None)
        if offered is not None and self._matches(offered):
            if scope["type"] == "http" and scope["method"] in ("GET", "HEAD"):
                await self._redirect_without_token(scope, query, send)
            else:
                await self.app(scope, receive, send)
            return
        if self._matches(self._cookie_value(scope)):
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            # Closing before accept makes the server answer the handshake with 403.
            await receive()
            await send({"type": "websocket.close", "code": 1008})
            return
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"text/html; charset=utf-8"),
                    (b"content-length", str(len(self._page)).encode()),
                    (b"cache-control", b"no-store"),
                    (OWNER_HEADER.encode(), self._owner.encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": self._page})

    def _matches(self, value: str | None) -> bool:
        return value is not None and hmac.compare_digest(value.encode(), self._token)

    def _cookie_value(self, scope) -> str | None:
        for name, value in scope.get("headers", []):
            if name != b"cookie":
                continue
            for part in value.decode("latin-1").split(";"):
                key, sep, val = part.strip().partition("=")
                if sep and key == self._cookie:
                    return val
        return None

    async def _redirect_without_token(self, scope, query: list[tuple[str, str]], send) -> None:
        # The rest of the query is kept, so a deep link /p/<project>/<view>?base=…&token=… lands on its route.
        path = "/" + (scope.get("raw_path") or scope["path"].encode()).decode("latin-1").lstrip("/")
        rest = urlencode([(k, v) for k, v in query if k != "token"])
        location = path + (f"?{rest}" if rest else "")
        cookie = f"{self._cookie}={self._token.decode()}; HttpOnly; SameSite=Lax; Path=/"
        await send(
            {
                "type": "http.response.start",
                "status": 302,
                "headers": [
                    (b"location", location.encode("latin-1")),
                    (b"set-cookie", cookie.encode()),
                    (b"cache-control", b"no-store"),
                    (b"content-length", b"0"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": b""})
