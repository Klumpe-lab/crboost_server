"""Every check CI runs, as one command: `./crboost ci`. The GitHub workflow runs the same command.

Each step is a subprocess with its own time. A failed step does not stop the others; the table at the end names
what failed, and the exit code is 1 when anything did. The Python checks read the tracked files (`git ls-files`,
so a new file counts once it is added): a stray file in a checkout neither fails a check nor hides from one. Steps
that import crboost read config/conf.template.yaml under an empty HOME, never a site's conf.yaml or a user's
~/.crboost.

    ./crboost ci            # every step; the lock and the MCP smoke reach PyPI when uv's cache lacks a package
    ./crboost ci --fast     # without those two
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CONF_TEMPLATE = REPO / "config" / "conf.template.yaml"
FORWARDER = "agent/crboost_mcp.py"
# Python the import check leaves alone: the forwarder (its own dependencies), what runs inside a tool's image
# (containers/, and the tilt filter's network, which needs torch), the tests (pytest imports them), the
# maintainer's scripts and the notes.
NOT_IMPORTED = ("agent/", "containers/", "docs/", "memory/", "scripts/", "tests/", "filterTilts/deepLearning/")
# Shell scripts that are part of the notes, not the product.
NOT_SHELL_CHECKED = ("docs/", "memory/")
STEP_TIMEOUT_S = 900


@dataclass
class Outcome:
    ok: bool | None  # None: skipped
    summary: str = ""
    detail: str = ""


@dataclass
class Context:
    env: dict[str, str]
    tmp: Path
    py_files: list[str]
    shell_scripts: list[str]
    uv: list[str] = field(default_factory=list)
    uv_env: dict[str, str] = field(default_factory=dict)


def _proc(
    cmd: list[str], env: dict[str, str], *, stdin: str | None = None, cwd: Path = REPO
) -> subprocess.CompletedProcess | Outcome:
    """The finished process, or the failed Outcome when it could not start or did not end in time."""
    try:
        return subprocess.run(
            cmd, cwd=cwd, env=env, input=stdin, capture_output=True, text=True, timeout=STEP_TIMEOUT_S
        )
    except subprocess.TimeoutExpired:
        return Outcome(False, f"no result within {STEP_TIMEOUT_S} s")
    except OSError as e:
        return Outcome(False, f"{cmd[0]}: {e}")


def _run(cmd: list[str], env: dict[str, str], *, stdin: str | None = None, cwd: Path = REPO) -> Outcome:
    """Passes on exit 0; the summary is the output's last line, the whole output kept when it failed."""
    run = _proc(cmd, env, stdin=stdin, cwd=cwd)
    if isinstance(run, Outcome):
        return run
    text = (run.stdout + run.stderr).strip()
    said = run.stdout.strip() or run.stderr.strip()  # a tool's stderr notes do not stand in for its result
    last = said.splitlines()[-1] if said else ""
    return Outcome(run.returncode == 0, last, "" if run.returncode == 0 else text)


def _tracked(*patterns: str) -> list[str]:
    run = subprocess.run(["git", "ls-files", "-z", "--", *patterns], cwd=REPO, capture_output=True, text=True)
    if run.returncode != 0:
        sys.exit(f"ci: needs a git checkout (git ls-files: {run.stderr.strip()})")
    return [f for f in run.stdout.split("\0") if f and (REPO / f).is_file()]


def _is_posix_sh(script: str) -> bool:
    with open(REPO / script, "rb") as f:
        return f.readline().strip() == b"#!/bin/sh"


# ── Steps ────────────────────────────────────────────────────────────────────


def ruff_check(ctx: Context) -> Outcome:
    return _run([sys.executable, "-m", "ruff", "check", "--force-exclude", *ctx.py_files], ctx.env)


def ruff_format(ctx: Context) -> Outcome:
    return _run([sys.executable, "-m", "ruff", "format", "--check", "--force-exclude", *ctx.py_files], ctx.env)


def boundaries(ctx: Context) -> Outcome:
    return _run([sys.executable, "check_boundaries.py"], ctx.env)


def look_literals(ctx: Context) -> Outcome:
    return _run([sys.executable, "check_look_literals.py"], ctx.env)


_COMPILE = r"""
import sys
import warnings

warnings.simplefilter("error")  # a SyntaxWarning (an invalid escape, say) fails too
bad = []
for path in sys.argv[1:]:
    try:
        with open(path, "rb") as f:
            compile(f.read(), path, "exec", dont_inherit=True)
    except (SyntaxError, ValueError, Warning) as e:
        bad.append(f"{path}: {type(e).__name__}: {e}")
print("\n".join(bad))
print(f"{len(sys.argv) - 1} files compile" if not bad else f"{len(bad)} of {len(sys.argv) - 1} files do not compile")
sys.exit(1 if bad else 0)
"""


def compile_files(ctx: Context) -> Outcome:
    return _run([sys.executable, "-c", _COMPILE, *ctx.py_files], ctx.env)


_IMPORTS = r"""
import importlib
import sys
from pathlib import Path

from services.configs import config_service

config_service.DEFAULT_CONFIG_PATH = Path(sys.argv[1])
failed = []
for name in sys.argv[2:]:
    try:
        importlib.import_module(name)
    except (Exception, SystemExit) as e:
        failed.append(f"{name}: {type(e).__name__}: {e}")
print("\n".join(failed))
n = len(sys.argv) - 2
print(f"{n} modules import" if not failed else f"{len(failed)} of {n} modules do not import")
sys.exit(1 if failed else 0)
"""


def imports(ctx: Context) -> Outcome:
    modules = []
    for f in ctx.py_files:
        if f.startswith(NOT_IMPORTED):
            continue
        parts = Path(f).with_suffix("").parts
        modules.append(".".join(parts[:-1] if parts[-1] == "__init__" else parts))
    return _run([sys.executable, "-c", _IMPORTS, str(CONF_TEMPLATE), *sorted(modules)], ctx.env)


def repo_tests(ctx: Context) -> Outcome:
    return _run(
        [sys.executable, "-m", "pytest", "-m", "repo", "-q", "-p", "no:cacheprovider", "tests/test_repo.py"], ctx.env
    )


def _pins(path: Path) -> list[str]:
    """The lock's pin lines, as install.sh names an environment by them: comments and blank lines out, sorted."""
    lines = (re.sub(r"\s*#.*", "", line).strip() for line in path.read_text().splitlines())
    return sorted(line for line in lines if line)


def lock(ctx: Context) -> Outcome:
    """requirements.txt is what requirements.in compiles to. Compiled into a copy, so uv keeps the lock's pins
    where they still fit, exactly as `./crboost lock` does, and the checkout's file is never written."""
    copy = ctx.tmp / "requirements.txt"
    shutil.copyfile(REPO / "requirements.txt", copy)
    run = _run([*ctx.uv, "pip", "compile", "--quiet", "requirements.in", "-o", str(copy)], ctx.uv_env)
    if not run.ok:
        return run
    have, want = _pins(REPO / "requirements.txt"), _pins(copy)
    if have == want:
        return Outcome(True, f"requirements.txt is requirements.in compiled ({len(have)} pins)")
    diff = difflib.unified_diff(have, want, "requirements.txt", "requirements.in, compiled", lineterm="")
    return Outcome(False, "requirements.txt is not requirements.in compiled: run ./crboost lock", "\n".join(diff))


def mcp_lock(ctx: Context) -> Outcome:
    return _run([*ctx.uv, "lock", "--script", FORWARDER, "--check"], ctx.uv_env)


_INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "crboost-ci", "version": "0"},
    },
}


def mcp_smoke(ctx: Context) -> Outcome:
    """The forwarder as Claude Code starts it (`./crboost mcp`, from another folder): its locked packages install
    and import, and it answers MCP's initialize, then ends on EOF. It looks for no server: that happens on the
    first tool listing, which this does not ask for."""
    run = _proc([str(REPO / "crboost"), "mcp"], ctx.env, stdin=json.dumps(_INITIALIZE) + "\n", cwd=ctx.tmp)
    if isinstance(run, Outcome):
        return run
    output = (run.stdout + run.stderr).strip()
    if run.returncode != 0:
        return Outcome(False, f"./crboost mcp ended with exit code {run.returncode}", output)
    answer = next((json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")), {})
    info = answer.get("result", {}).get("serverInfo", {})
    if info.get("name") != "crboost":
        return Outcome(False, "the forwarder did not answer initialize as crboost", output)
    return Outcome(True, f"initialize answered by {info['name']}, protocol {answer['result']['protocolVersion']}")


def shell_syntax(ctx: Context) -> Outcome:
    """`bash -n` on every script, and `sh -n` on the #!/bin/sh ones: on Ubuntu /bin/sh is dash, which knows no
    bashisms."""
    sh = os.path.realpath(shutil.which("sh") or "sh")
    problems, posix = [], 0
    for script in ctx.shell_scripts:
        shells = ["bash", "sh"] if _is_posix_sh(script) else ["bash"]
        posix += len(shells) == 2
        for shell in shells:
            run = _run([shell, "-n", script], ctx.env)
            if not run.ok:
                problems.append(f"{shell} -n {script}:\n{run.detail}")
    if problems:
        return Outcome(False, f"{len(problems)} syntax errors", "\n".join(problems))
    return Outcome(True, f"{len(ctx.shell_scripts)} scripts under bash, {posix} also under sh ({Path(sh).name})")


def shellcheck(ctx: Context) -> Outcome:
    exe = shutil.which("shellcheck")
    if exe is None:
        if os.environ.get("CI"):
            return Outcome(False, "shellcheck is not on PATH; the runner image ships it")
        return Outcome(None, "not installed here")
    run = _run([exe, "--severity=warning", *ctx.shell_scripts], ctx.env)
    return run if not run.ok else Outcome(True, f"{len(ctx.shell_scripts)} scripts, nothing at warning or above")


@dataclass(frozen=True)
class Step:
    name: str
    run: Callable[[Context], Outcome]
    network: bool = False


STEPS = (
    Step("ruff check", ruff_check),
    Step("ruff format", ruff_format),
    Step("boundaries", boundaries),
    Step("look literals", look_literals),
    Step("compile", compile_files),
    Step("imports", imports),
    Step("repo tests", repo_tests),
    Step("lock", lock, network=True),
    Step("mcp lock", mcp_lock),
    Step("mcp smoke", mcp_smoke, network=True),
    Step("sh -n", shell_syntax),
    Step("shellcheck", shellcheck),
)


# ── Running them ─────────────────────────────────────────────────────────────


def _context(tmp: Path) -> Context:
    home = tmp / "home"
    home.mkdir()
    env = {**os.environ, "HOME": str(home)}
    uv = os.environ.get("CRBOOST_UV") or str(REPO / ".uv" / "bin" / "uv")
    uv_env = {
        **env,
        "UV_NO_CONFIG": "1",
        "UV_CACHE_DIR": str(REPO / ".uv" / "cache"),
        "UV_PYTHON_INSTALL_DIR": str(REPO / ".uv" / "python"),
    }
    shell = [f for f in _tracked("*.sh", "crboost") if not f.startswith(NOT_SHELL_CHECKED)]
    return Context(
        env=env,
        tmp=tmp,
        py_files=_tracked("*.py"),
        shell_scripts=sorted(shell),
        uv=[uv, "--config-file", str(REPO / "uv.toml")],
        uv_env=uv_env,
    )


def _github(results: list[tuple[Step, Outcome, float]]) -> None:
    """On GitHub Actions: an error annotation per failed step, and the table on the run's summary page."""
    if not os.environ.get("GITHUB_ACTIONS"):
        return
    for step, outcome, _ in results:
        if outcome.ok is False:
            print(f"::error title=./crboost ci: {step.name}::{outcome.summary}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        rows = ["| step | result | time | |", "|---|---|---|---|"]
        for step, outcome, secs in results:
            result = {True: "ok", False: "**failed**", None: "skipped"}[outcome.ok]
            rows.append(f"| {step.name} | {result} | {secs:.1f} s | {outcome.summary.replace('|', '/')} |")
        with open(summary, "a") as f:
            f.write("### ./crboost ci\n\n" + "\n".join(rows) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="./crboost ci", description=__doc__.split("\n\n")[0])
    parser.add_argument("--fast", action="store_true", help="skip the steps that may reach PyPI")
    args = parser.parse_args(argv)

    results: list[tuple[Step, Outcome, float]] = []
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="crboost-ci-") as tmp:
        ctx = _context(Path(tmp))
        print(f"./crboost ci: {len(ctx.py_files)} Python files, {len(ctx.shell_scripts)} shell scripts", flush=True)
        for step in STEPS:
            t0 = time.monotonic()
            outcome = Outcome(None, "--fast") if args.fast and step.network else step.run(ctx)
            secs = time.monotonic() - t0
            results.append((step, outcome, secs))
            result = {True: "ok", False: "FAILED", None: "skipped"}[outcome.ok]
            print(f"  {step.name:<14} {result:<8} {secs:6.1f} s   {outcome.summary}", flush=True)
            if outcome.ok is False and outcome.detail:
                print("\n".join("      " + line for line in outcome.detail.splitlines()[-60:]), flush=True)

    failed = [s.name for s, o, _ in results if o.ok is False]
    skipped = sum(o.ok is None for _, o, _ in results)
    print()
    print(f"  {'step':<14} {'result':<8} {'time':>8}")
    for step, outcome, secs in results:
        result = {True: "ok", False: "FAILED", None: "skipped"}[outcome.ok]
        print(f"  {step.name:<14} {result:<8} {secs:6.1f} s")
    passed = len(results) - len(failed) - skipped
    which = f" ({', '.join(failed)})" if failed else ""
    print(
        f"\n{len(results)} steps: {passed} passed, {len(failed)} failed{which}, {skipped} skipped; "
        f"{time.monotonic() - started:.0f} s"
    )
    _github(results)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
