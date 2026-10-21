#!/usr/bin/env python3
import pycache_setup  # noqa: F401  (first: it decides where every later import is compiled to)
import argparse
import getpass
import logging
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from services import port_survey
from services.log_setup import setup_logging
from services.server_auth import TOKEN_FILE, read_or_create_secret
from services.startup_spinner import Spinner

# Everything above is light. The app itself (server_app: NiceGUI, the backend) is imported only once the port
# is settled, so a port problem is reported at once, and the spinner turns while the rest loads.

logger = logging.getLogger(__name__)

INSTALL = Path(__file__).resolve().parent

_BOLD, _CYAN, _DIM, _YELLOW, _RESET = "\x1b[1m", "\x1b[1;36m", "\x1b[2m", "\x1b[33m", "\x1b[0m"


def banner_lines(port: int, hostname: str, token: str) -> list[str]:
    # Each command and link alone on its line, so a whole-line yank copies nothing else.
    # ExitOnForwardFailure: a tunnel that cannot take its port says so and ends, instead of going to the
    # background without a port (pasted on the headnode, where the server holds it, or over an old tunnel).
    # ServerAliveInterval: a tunnel whose connection died (laptop asleep, VPN switched) ends instead of
    # hanging every page. 127.0.0.1, not localhost: localhost may mean IPv6 ::1, where another program may listen.
    return [
        f"{_BOLD}On your own computer (not here), open the tunnel:{_RESET}",
        f"{_CYAN}ssh -f -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 "
        f"-L {port}:127.0.0.1:{port} $USER@{hostname}{_RESET}",
        f"{_DIM}ssh says 'Address already in use'? An old tunnel holds the port on your computer; end it with: "
        f"pkill -f 'ssh.*-L {port}:'{_RESET}",
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


def _dim_rows(rows: list[tuple[str, str]]) -> list[str]:
    """Rows in the startup summary's style (startup_summary.summary_lines)."""
    return [f"{_DIM}{label:<10} {value}{_RESET}" for label, value in rows]


def _startup_line(timings: list[tuple[str, float]], cluster_s: float) -> str:
    """How long the start took and where it went. The cluster check runs alongside the app's stages; a
    "cluster" stage is only the wait for it at the end."""
    total = sum(secs for _, secs in timings)
    stages = " · ".join(f"{label} {secs:.1f}" for label, secs in timings)
    return f"{total:.1f} s  ({stages}; cluster check {cluster_s:.1f} s alongside)"


def _leftover_block(found: port_survey.Survey) -> list[str]:
    lines = port_survey.leftover_lines(found)
    return ["", *(f"{_YELLOW}{line}{_RESET}" for line in lines)] if lines else []


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
        spinner.stage("ports", "looking at what holds the ports on this host")
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
            print("\n".join(banner_lines(running.port, hostname, token)))
            print(f"\nTo restart it instead: kill {running.pid}, then start again.")
            print("\n".join(_leftover_block(found)))
            return
        try:
            sock, port = port_survey.claim(found, INSTALL, args.port, args.host)
        except port_survey.PortTaken as e:
            spinner.stop()
            print("\n".join([*e.lines, *_leftover_block(found)]), file=sys.stderr)
            sys.exit(1)
        n_left = len(found.leftovers)
        leftovers = f" · {n_left} leftover{'s' if n_left != 1 else ''} of yours, listed below" if n_left else ""
        spinner.note(f"{port}{leftovers}")

        # The landing page's status strip shows it; worker processes inherit it, which is how a later start
        # recognises them (services/port_survey.py).
        os.environ["CRBOOST_PORT"] = str(port)
        log_file = setup_logging(debug=args.debug, port=port)
        logger.info("Server starting: port %d, pid %d, user %s, %s", port, os.getpid(), getpass.getuser(), INSTALL)
        token = read_or_create_secret(TOKEN_FILE)

        spinner.stage("modules", "loading the app")
        import server_app
        from startup_summary import summary_lines

        def _check_cluster() -> tuple[list[str], float]:
            t0 = time.monotonic()
            return summary_lines(port, hostname, getpass.getuser(), token, log_file), time.monotonic() - t0

        # The snapshot of SLURM and the recent projects waits on sinfo/squeue and the shared filesystem, so it
        # runs alongside the app's own stages; its SLURM probe has an event loop of its own in that thread.
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="startup-cluster")
        cluster = pool.submit(_check_cluster)
        # No wait here: _ready collects the result.
        pool.shutdown(wait=False)

        spinner.stage("app", "building the app: backend, routes, pages")
        app = server_app.setup_app(port)

        def _ready() -> None:
            # Added after setup_app's startup handlers, so it runs once the monitors are up; the port has been
            # listening since claim(), so a click on these links is answered from now on.
            if not cluster.done():
                spinner.stage("cluster", "waiting for SLURM and the project directories")
            summary, cluster_s = cluster.result()
            spinner.stop()
            took = _startup_line(spinner.timings, cluster_s)
            logger.info("Started in %s", took)
            summary += _dim_rows([("port", port_survey.port_line(found, port)), ("startup", took)])
            summary += _leftover_block(found)
            install_sticky_banner(banner_lines(port, hostname, token))
            # Printed into the scrolling region under the pinned links: a startup snapshot that log output may
            # scroll away.
            print("\n".join(summary) + "\n", flush=True)

        app.add_event_handler("startup", _ready)
        spinner.stage("server", "starting the server: workers, pipeline monitor, recovery")
        server_app.serve(app, token, port, sock)
    finally:
        spinner.stop()


# Not "__mp_main__": the worker processes (services/workers.py) are spawned, and a spawned child
# imports this module under that name; it must not start a second server.
if __name__ == "__main__":
    main()
