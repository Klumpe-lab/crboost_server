"""Which port the server takes, what holds the others, and what of the user's own is left over on this host.

Stdlib only: crboost runs it before the heavy imports, so a port problem is reported within a second of
starting. Linux only: listening sockets come from /proc/net/tcp{,6}, which give every socket's owner uid; the
process behind a socket is found through /proc/<pid>/fd, which is readable for the user's own processes only.

What counts as left over (`Survey.leftovers`), all the user's own and on this host:
- an `ssh -L` tunnel to this same host: the printed tunnel command run here instead of on the user's computer.
  It forwards to itself, holds the port, and a browser tunnel that lands on it hangs;
- a CryoBoost server that is stopped (Ctrl-Z), does not answer, or holds no port long after starting;
- a CryoBoost worker process whose server is gone, and the workers of the servers above.
"""

from __future__ import annotations

import errno
import getpass
import json
import os
import pwd
import socket
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from services.server_auth import CRBOOST_HOME, OWNER_HEADER

# The block CryoBoost servers take their ports from. 32 ports for the handful of people who run one at a
# time: below Linux's ephemeral range (32768-60999, where outgoing connections get their ports), clear of X11
# forwarding (6010+), VNC (59xx), the 8xxx that Jupyter and every dev server default to, and of the ports
# browsers refuse to open. Each server gets the same port every time it is free (`_preferred_port`).
PORT_RANGE = range(24300, 24332)
PORTS_FILE = CRBOOST_HOME / "ports.json"

_PROBE_TIMEOUT_S = 1.0
# A server this old without a listening port is not starting any more: it is stuck.
_PORTLESS_GRACE_S = 120
# ssh options that take a value (`-L 8080:host:80`, `-p 22`, `-o Opt=x`), so the destination can be found.
_SSH_VALUE_FLAGS = frozenset("BbcDEeFIiJLlmOoPpQRSWw")
_STATES = {
    "R": "running",
    "S": "idle",
    "D": "waiting on disk",
    "T": "stopped (Ctrl-Z)",
    "t": "stopped (debugger)",
    "Z": "defunct",
}

SERVER, WORKER, TUNNEL = "server", "worker", "tunnel"


class PortTaken(Exception):
    """No port for the server; `lines` says why and what to do."""

    def __init__(self, lines: list[str]):
        super().__init__("\n".join(lines))
        self.lines = lines


@dataclass(frozen=True)
class Listener:
    port: int
    uid: int
    inode: int
    ipv6: bool


@dataclass(frozen=True)
class Answer:
    """What a port said to a plain HTTP GET: nothing in time, another program, or a CryoBoost server (`owner`
    is "" for an older one that does not name its user)."""

    answered: bool
    cryoboost: bool = False
    owner: str = ""


@dataclass
class Proc:
    """One of the user's own processes on this host."""

    pid: int
    ppid: int
    state: str
    args: list[str]
    cwd: str
    started: float
    ports: list[int]
    crboost_env: bool
    kind: str = ""
    install: Path | None = None
    answer: Answer | None = None
    # Why it is left over; "" when it is not.
    leftover: str = ""

    @property
    def port(self) -> int | None:
        in_range = [p for p in self.ports if p in PORT_RANGE]
        return min(in_range or self.ports, default=None)


@dataclass
class Survey:
    listeners: list[Listener]
    procs: list[Proc]
    # Ports in PORT_RANGE held by other users: port -> (user, what answered there).
    others: dict[int, tuple[str, Answer]] = field(default_factory=dict)

    @property
    def leftovers(self) -> list[Proc]:
        return [p for p in self.procs if p.leftover]

    def running(self) -> list[Proc]:
        """The user's CryoBoost servers that answer."""
        return [p for p in self.procs if p.kind == SERVER and not p.leftover and p.answer and p.answer.answered]

    def running_here(self, install: Path) -> Proc | None:
        """A CryoBoost server of this same checkout that answers: starting a second one would only give two
        servers reconciling the same projects."""
        return next((p for p in self.running() if p.install == install and p.port is not None), None)


# ── Reading /proc ────────────────────────────────────────────────────────────


def listeners() -> list[Listener]:
    """Every listening TCP socket on this host, whoever owns it."""
    out = []
    for name in ("tcp", "tcp6"):
        try:
            lines = Path(f"/proc/net/{name}").read_text().splitlines()[1:]
        except FileNotFoundError:
            # No IPv6 on this host.
            continue
        for line in lines:
            f = line.split()
            if len(f) < 10 or f[3] != "0A":  # 0A = LISTEN
                continue
            out.append(Listener(int(f[1].rsplit(":", 1)[1], 16), int(f[7]), int(f[9]), name == "tcp6"))
    return out


def _boot_time() -> float:
    for line in Path("/proc/stat").read_text().splitlines():
        if line.startswith("btime "):
            return float(line.split()[1])
    return 0.0


def own_processes(socket_ports: dict[int, int]) -> list[Proc]:
    """The user's processes on this host, with the listening ports each holds (`socket_ports`: inode -> port)."""
    uid, me = os.getuid(), os.getpid()
    boot, ticks = _boot_time(), os.sysconf("SC_CLK_TCK")
    out = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdigit() or int(entry.name) == me:
            continue
        base = f"/proc/{entry.name}"
        try:
            if entry.stat().st_uid != uid:
                continue
            stat = Path(f"{base}/stat").read_text()
            args = [a.decode(errors="replace") for a in Path(f"{base}/cmdline").read_bytes().split(b"\0") if a]
            cwd = os.readlink(f"{base}/cwd")
            environ = Path(f"{base}/environ").read_bytes()
            ports = []
            for fd in os.listdir(f"{base}/fd"):
                try:
                    target = os.readlink(f"{base}/fd/{fd}")
                except OSError:
                    # The fd closed between the listing and the read.
                    continue
                if target.startswith("socket:[") and int(target[8:-1]) in socket_ports:
                    ports.append(socket_ports[int(target[8:-1])])
        except OSError:
            # The process ended mid-read, or it is not ours to read after all.
            continue
        # The fields after the command name, which may itself hold spaces and parentheses.
        fields = stat.rsplit(")", 1)[1].split()
        out.append(
            Proc(
                pid=int(entry.name),
                ppid=int(fields[1]),
                state=fields[0],
                args=args,
                cwd=cwd,
                started=boot + int(fields[19]) / ticks,
                ports=sorted(set(ports)),
                crboost_env=b"CRBOOST_PORT=" in environ,
            )
        )
    return out


# ── What each process is ─────────────────────────────────────────────────────


def _install_of(proc: Proc) -> Path | None:
    """The checkout a CryoBoost server runs from: the crboost launcher it was started with, next to a backend.py.
    main.py is the launcher's old name, still running from checkouts that predate the rename."""
    if any("multiprocessing" in a for a in proc.args):
        return None
    for arg in proc.args[1:4]:
        if Path(arg).name in ("crboost", "main.py"):
            path = Path(proc.cwd, arg)
            if (path.parent / "backend.py").is_file():
                return path.parent.resolve()
    return None


def _ssh_forwards(args: list[str]) -> tuple[str | None, list[str]]:
    """(the host an ssh command connects to, the target host of each of its -L forwards)."""
    targets: list[str] = []
    i = 1
    while i < len(args):
        arg = args[i]
        if not arg.startswith("-") or arg == "-":
            return arg.rsplit("@", 1)[-1], targets
        # Clustered flags (`-fNL 8080:…`): the first one that takes a value ends the cluster.
        for j, flag in enumerate(arg[1:], start=1):
            if flag in _SSH_VALUE_FLAGS:
                value = arg[j + 1 :]
                if not value:
                    i += 1
                    value = args[i] if i < len(args) else ""
                # [bind_address:]port:host:hostport
                parts = value.split(":")
                if flag == "L" and len(parts) >= 3:
                    targets.append(parts[-2].strip("[]"))
                break
        i += 1
    return None, targets


def _this_host_names() -> set[str]:
    name = socket.gethostname()
    return {name, name.split(".")[0], "localhost", "127.0.0.1", "::1"}


def probe(port: int, ipv6: bool) -> Answer:
    """GET / on the port, at the address it listens on: a CryoBoost server answers with its user in a header
    (an older one with a page that names CryoBoost); nothing within a second is a hung or stopped program."""
    host = "::1" if ipv6 else "127.0.0.1"
    data = b""
    try:
        with socket.create_connection((host, port), timeout=_PROBE_TIMEOUT_S) as conn:
            conn.sendall(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            while len(data) < 8192 and (chunk := conn.recv(4096)):
                data += chunk
    except TimeoutError:
        if not data:
            return Answer(answered=False)
    except OSError:
        # Refused or reset: nothing that serves pages on this port.
        if not data:
            return Answer(answered=False)
    head = data.split(b"\r\n\r\n", 1)[0].decode("latin-1").lower()
    for line in head.splitlines():
        key, _, value = line.partition(":")
        if key.strip() == OWNER_HEADER:
            return Answer(answered=True, cryoboost=True, owner=value.strip())
    return Answer(answered=True, cryoboost=b"CryoBoost" in data)


def _user(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        # A uid the passwd database does not know (LDAP miss): the number is what there is.
        return str(uid)


def survey(install: Path) -> Survey:
    """Every listener on this host, the user's processes sorted into servers, workers and tunnels to this host,
    which of them are left over, and who holds the ports of PORT_RANGE."""
    found = listeners()
    uid = os.getuid()
    procs = own_processes({lst.inode: lst.port for lst in found if lst.uid == uid})
    here = _this_host_names()
    for p in procs:
        p.install = _install_of(p)
        if p.install is not None:
            p.kind = SERVER
        elif p.crboost_env and any("multiprocessing" in a for a in p.args):
            p.kind = WORKER
        elif p.args and os.path.basename(p.args[0]) == "ssh":
            dest, targets = _ssh_forwards(p.args)
            if dest in here and any(t in here for t in targets):
                p.kind, p.leftover = TUNNEL, "ssh tunnel to this same host"

    servers = [p for p in procs if p.kind == SERVER]
    v6_only = {lst.port for lst in found if lst.ipv6} - {lst.port for lst in found if not lst.ipv6}
    others = sorted({lst.port: lst.uid for lst in found if lst.port in PORT_RANGE and lst.uid != uid}.items())
    with ThreadPoolExecutor(max_workers=16) as pool:
        server_answers = {
            p.pid: pool.submit(probe, p.port, p.port in v6_only) for p in servers if p.port and p.state not in "Tt"
        }
        other_answers = {port: (owner, pool.submit(probe, port, port in v6_only)) for port, owner in others}
        for p in servers:
            p.answer = server_answers[p.pid].result() if p.pid in server_answers else Answer(answered=False)
        result = Survey(found, procs, {port: (_user(o), f.result()) for port, (o, f) in other_answers.items()})

    now = time.time()
    for p in servers:
        if p.state in "Tt":
            p.leftover = "CryoBoost server, stopped (Ctrl-Z)"
        elif p.port is None and now - p.started > _PORTLESS_GRACE_S:
            p.leftover = "CryoBoost server that holds no port"
        elif p.port is not None and not p.answer.answered:
            p.leftover = "CryoBoost server that does not answer"
    by_pid = {p.pid: p for p in procs}
    for p in procs:
        if p.kind != WORKER:
            continue
        parent = by_pid.get(p.ppid)
        if parent is None or parent.kind != SERVER:
            p.leftover = "CryoBoost worker whose server is gone"
        elif parent.leftover:
            p.leftover = f"CryoBoost worker of {parent.pid}"
    return result


# ── Taking a port ────────────────────────────────────────────────────────────


def bind_listen(port: int, host: str | None) -> socket.socket:
    """A listening socket on `port`. Without `host`: every interface, IPv4 and IPv6 in one socket, so a program
    holding the port on either family (an ssh tunnel on ::1 included) makes this fail instead of splitting
    `localhost` between two programs."""
    if host is None:
        try:
            return _listen(socket.AF_INET6, ("::", port), dual_stack=True)
        except OSError as e:
            if e.errno == errno.EADDRINUSE:
                raise
            # No IPv6 on this host: IPv4 is all there is.
        return _listen(socket.AF_INET, ("0.0.0.0", port))
    family, _, _, _, address = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)[0]
    return _listen(family, address)


def _listen(family: int, address: tuple, *, dual_stack: bool = False) -> socket.socket:
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        if dual_stack:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        # A restart may bind at once although the last run's connections still linger in TIME_WAIT.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.listen(2048)
    except OSError:
        sock.close()
        raise
    return sock


def _preferred_port(install: Path) -> int:
    """The port this checkout got last time, else one derived from the user and the checkout: stable across
    restarts, and a dev checkout and the shared install of one user do not start out on the same port."""
    try:
        remembered = json.loads(PORTS_FILE.read_text()).get(str(install))
    except (OSError, ValueError):
        # First start, or an unreadable file: fall back to the derived port.
        remembered = None
    if isinstance(remembered, int) and remembered in PORT_RANGE:
        return remembered
    return PORT_RANGE[zlib.crc32(f"{getpass.getuser()}:{install}".encode()) % len(PORT_RANGE)]


def _remember_port(install: Path, port: int) -> None:
    try:
        ports = json.loads(PORTS_FILE.read_text())
    except (OSError, ValueError):
        ports = {}
    ports[str(install)] = port
    tmp = PORTS_FILE.with_suffix(".tmp")
    try:
        PORTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(ports, indent=1))
        os.replace(tmp, PORTS_FILE)
    except OSError:
        # Only the next start's first guess depends on it; it then derives the port again.
        pass


def claim(found: Survey, install: Path, port: int | None, host: str | None) -> tuple[socket.socket, int]:
    """Bind `port`, or without one the first free port of PORT_RANGE from this checkout's preferred one.
    Raises PortTaken saying who holds the port or the range."""
    if port is not None:
        try:
            return bind_listen(port, host), port
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            raise PortTaken([holder_line(found, port), "Leave out --port and CryoBoost picks a free one."]) from None
    taken = {lst.port for lst in found.listeners}
    start = PORT_RANGE.index(_preferred_port(install))
    for i in range(len(PORT_RANGE)):
        candidate = PORT_RANGE[(start + i) % len(PORT_RANGE)]
        if candidate in taken:
            continue
        try:
            sock = bind_listen(candidate, host)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            # Taken since the survey read the list.
            continue
        _remember_port(install, candidate)
        return sock, candidate
    holders = [holder_line(found, p) for p in PORT_RANGE]
    raise PortTaken(
        [f"Every port of {PORT_RANGE[0]}–{PORT_RANGE[-1]} is taken:", *holders, "Start with --port <a free port>."]
    )


# ── Saying it ────────────────────────────────────────────────────────────────


def _home(path: Path | str) -> str:
    return str(path).replace(str(Path.home()), "~", 1)


def holder_line(found: Survey, port: int) -> str:
    """Who holds `port`, in one line, with what to do about it when it is the user's."""
    mine = next((p for p in found.procs if port in p.ports), None)
    if mine is not None:
        if mine.leftover:
            return f"Port {port}: your {mine.leftover} (pid {mine.pid}). End it with: kill -9 {mine.pid}"
        what = f"CryoBoost from {_home(mine.install)}" if mine.kind == SERVER else " ".join(mine.args)[:60]
        return f"Port {port}: your {what} (pid {mine.pid}), running. Stop it with: kill {mine.pid}"
    uids = {lst.uid for lst in found.listeners if lst.port == port}
    if not uids:
        return f"Port {port}: free now"
    if os.getuid() in uids:
        return f"Port {port}: one of your programs that this check cannot see"
    user, answer = found.others.get(port, (", ".join(_user(u) for u in sorted(uids)), Answer(answered=False)))
    return f"Port {port}: {user}'s {'CryoBoost' if answer.cryoboost else 'program'}"


def since(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%b %d %H:%M")


def leftover_lines(found: Survey) -> list[str]:
    """The user's leftovers as a small table and the one command that ends them; nothing when there are none."""
    rows = found.leftovers
    if not rows:
        return []
    table = [("PID", "WHAT", "PORT", "STATE", "SINCE")]
    for p in rows:
        what = p.leftover + (f" ({_home(p.install)})" if p.install else "")
        ports = ", ".join(map(str, p.ports)) or "—"
        table.append((str(p.pid), what, ports, _STATES.get(p.state, p.state), since(p.started)))
    widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
    host = socket.gethostname().split(".")[0]
    lines = [f"Left over on {host} from earlier sessions (yours; they do nothing useful):"]
    lines += ["  " + "  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)) for row in table]
    lines.append(f"End them with:  kill -9 {' '.join(str(p.pid) for p in rows)}")
    return lines


def port_line(found: Survey, port: int) -> str:
    """This server's port and who else uses the block."""
    others = [f"{p} {user}" + (" (CryoBoost)" if a.cryoboost else "") for p, (user, a) in sorted(found.others.items())]
    mine = [f"{p.port} you ({_home(p.install)})" for p in found.running() if p.port is not None and p.port != port]
    used = others + mine
    block = f"{PORT_RANGE[0]}–{PORT_RANGE[-1]}"
    return f"{port}  (block {block}" + (f"; also in use: {' · '.join(used)})" if used else ")")
