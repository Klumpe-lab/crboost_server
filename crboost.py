"""The server. The `crboost` launcher runs this with the checkout's own Python environment; start it no other way."""
import pycache_setup  # noqa: F401  (first: it decides where every later import is compiled to)
import argparse
import getpass
import logging
import os
import socket
import sys
from pathlib import Path

from services import port_survey
from services.code_version import code_version
from services.log_setup import setup_logging
from services.server_auth import TOKEN_FILE, read_or_create_secret
from services.startup_spinner import Spinner

# Everything above is light. The app itself (server_app: NiceGUI, the backend) is imported only once the port
# is settled, so a port problem is reported at once, and the spinner turns while the rest loads.

logger = logging.getLogger(__name__)

INSTALL = Path(__file__).resolve().parent

_BOLD, _CYAN, _DIM, _RESET = "\x1b[1m", "\x1b[1;36m", "\x1b[2m", "\x1b[0m"
# Blank lines between the two commands and the troubleshooting under them.
_GAP = 3


def banner_lines(port: int, hostname: str, token: str, via: str | None = None) -> list[str]:
    # Each command and link alone on its line, so a whole-line yank copies nothing else.
    # `via` (local.SshLoginHost) is a login alias that may land on another login node than this one: the tunnel
    # then ends there and goes on to this headnode by name, which resolves inside the cluster.
    # ExitOnForwardFailure: a tunnel that cannot take its port says so and ends, instead of going to the
    # background without a port (pasted on the headnode, where the server holds it, or over an old tunnel).
    # ServerAliveInterval: a tunnel whose connection died (laptop asleep, VPN switched) ends instead of
    # hanging every page. 127.0.0.1, not localhost: localhost may mean IPv6 ::1, where another program may listen.
    return [
        f"{_BOLD}On your own computer (not here), open the tunnel:{_RESET}",
        f"{_CYAN}ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 "
        + (f"-L {port}:{hostname}:{port} {via}" if via else f"-L {port}:127.0.0.1:{port} $USER@{hostname}")
        + _RESET,
        f"{_BOLD}Then open in your browser:{_RESET}",
        f"{_CYAN}http://localhost:{port}/?token={token}{_RESET}",
    ]


def install_sticky_banner(lines: list[str]) -> None:
    """Pin `lines` to the top of the terminal (DECSTBM scroll region) so log
    output scrolls underneath. Plain print when stdout isn't a TTY (redirected
    to a file, systemd, ...) or CRBOOST_PLAIN_LOG is set."""
    import atexit
    import re
    import shutil

    if not sys.stdout.isatty() or os.environ.get("CRBOOST_PLAIN_LOG"):
        print("\n".join(lines) + "\n")
        return
    cols, rows = shutil.get_terminal_size()
    # A line longer than the terminal wraps onto more rows; the scroll region must start below all of them,
    # or log output overwrites the wrapped tail of the banner.
    visible = [re.sub(r"\x1b\[[0-9;]*m", "", line) for line in lines]
    top = sum(max(1, -(-len(v) // cols)) for v in visible) + 1
    out = sys.stdout
    out.write("\x1b[2J\x1b[H")  # clear screen, cursor home
    out.write("\n".join(lines) + "\n")
    out.write(f"\x1b[{top};{rows}r\x1b[{top};1H")  # scroll region below the banner, cursor into it
    out.flush()

    def _restore():
        out.write(f"\x1b[r\x1b[{rows};1H\n")
        out.flush()

    atexit.register(_restore)


def _ssh_login_host(bind_host: str | None) -> str | None:
    """local.SshLoginHost, unless this server listens on loopback only, where no other login node reaches it."""
    if bind_host in ("127.0.0.1", "::1", "localhost"):
        return None
    try:
        from services.configs.config_service import get_config_service

        return get_config_service().config.local.SshLoginHost or None
    except Exception:  # a broken conf.yaml is reported where it is used; the banner falls back to the headnode
        return None


def trouble_lines(port: int, found: port_survey.Survey, *notes: str) -> list[str]:
    """What to do when the commands do not work: in gray, a few blank lines under them."""
    tunnel = (
        "ssh says 'Address already in use'? An old tunnel holds the port on your computer; end it with: "
        f"pkill -f 'ssh.*-L {port}:'"
    )
    return [""] * _GAP + _gray([tunnel, *notes, *_leftovers(found)])


def _leftovers(found: port_survey.Survey) -> list[str]:
    lines = port_survey.leftover_lines(found)
    return ["", *lines] if lines else []


def _gray(lines: list[str]) -> list[str]:
    return [f"{_DIM}{line}{_RESET}" if line else "" for line in lines]


def _startup_line(timings: list[tuple[str, float]]) -> str:
    """How long the start took and where it went."""
    total = sum(secs for _, secs in timings)
    stages = " · ".join(f"{label} {secs:.1f}" for label, secs in timings)
    return f"{total:.1f} s  ({stages})"


def main() -> None:
    parser = argparse.ArgumentParser(description="CryoBoost Server")
    block = f"{port_survey.PORT_RANGE[0]}–{port_survey.PORT_RANGE[-1]}"
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Port to listen on (default: a free one in {block}, the same one each time it is free)",
    )
    parser.add_argument(
        "--host", type=str, default=None, help="Address to bind to (default: every interface, IPv4 and IPv6)"
    )
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG-level logging")
    args = parser.parse_args()
    hostname = socket.gethostname()

    spinner = Spinner().start()
    try:
        spinner.stage("ports", "checking the ports")
        found = port_survey.survey(INSTALL)
        running = found.running_here(INSTALL) if args.port is None else None
        if running is not None:
            # A second server of this checkout would reconcile the same projects twice; point at the first.
            spinner.stop()
            token = read_or_create_secret(TOKEN_FILE)
            print(
                f"CryoBoost from this checkout is already running (pid {running.pid}, port {running.port}, "
                f"since {port_survey.since(running.started)}). To use it:\n"
            )
            print("\n".join(banner_lines(running.port, hostname, token, _ssh_login_host(None))))
            restart = f"To restart it instead: kill {running.pid}, then start again."
            print("\n".join(trouble_lines(running.port, found, restart)))
            return
        try:
            sock, port = port_survey.claim(found, INSTALL, args.port, args.host)
        except port_survey.PortTaken as e:
            spinner.stop()
            print("\n".join([*e.lines, *_gray(_leftovers(found))]), file=sys.stderr)
            sys.exit(1)

        # The landing page's status strip shows it; worker processes inherit it, which is how a later start
        # recognises them (services/port_survey.py).
        os.environ["CRBOOST_PORT"] = str(port)
        log_file = setup_logging(debug=args.debug, port=port)
        logger.info(
            "Server starting: port %d, pid %d, user %s, %s, code %s",
            port,
            os.getpid(),
            getpass.getuser(),
            INSTALL,
            code_version() or "unknown (not a git checkout)",
        )
        token = read_or_create_secret(TOKEN_FILE)

        spinner.stage("modules", "loading the app")
        import server_app

        spinner.stage("app", "building the app")
        app = server_app.setup_app(port)

        def _ready() -> None:
            # Added after setup_app's startup handlers, so it runs once the monitors are up; the port has been
            # listening since claim(), so a click on these links is answered from now on.
            spinner.stop()
            logger.info("Started in %s", _startup_line(spinner.timings))
            install_sticky_banner(banner_lines(port, hostname, token, _ssh_login_host(args.host)))
            log = str(log_file).replace(str(Path.home()), "~", 1) if log_file else "none, the console only"
            # Into the scrolling region under the pinned commands, where log output scrolls it away.
            print("\n".join(trouble_lines(port, found, f"Server log: {log}")) + "\n", flush=True)

        app.add_event_handler("startup", _ready)
        spinner.stage("server", "bringing up the server")
        server_app.serve(app, token, port, sock)
    finally:
        spinner.stop()


# Not "__mp_main__": the worker processes (services/workers.py) are spawned, and a spawned child
# imports this module under that name; it must not start a second server.
if __name__ == "__main__":
    main()
