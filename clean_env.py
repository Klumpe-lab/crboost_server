"""Leave out what a cluster module put on PYTHONPATH; imported first by crboost and preflight.py.

A venv that has its own packages (the uv quick start) can only be shadowed by a module's site-packages on
PYTHONPATH, and sbatch hands the server's environment to every job it submits. When this interpreter's own
site-packages holds numpy, the process starts itself again without PYTHONPATH, so nobody has to `module purge`
first. A venv built on a module's Python, whose packages come from PYTHONPATH, is left as it is.
"""

import os
import sys
import sysconfig
from pathlib import Path

# Started with some python3 that is not this checkout's venv (the system's 3.6, or a module's newer one that
# `#!/usr/bin/env python3` finds first): the venv's interpreter runs it instead, since only it has the packages.
# sys.prefix is the venv directory only inside the venv. Kept to syntax an old interpreter can parse, since it
# runs before the version is known.
_venv_dir = Path(__file__).resolve().parent / "venv"
_venv_python = _venv_dir / "bin" / "python3"
if _venv_python.exists() and Path(sys.prefix).resolve() != _venv_dir.resolve():
    print(f"[crboost] {sys.executable} is Python {sys.version.split()[0]}; starting {_venv_python}")
    os.execv(str(_venv_python), [str(_venv_python), *sys.argv])
if sys.version_info < (3, 11):  # noqa: UP036 — the point is to catch a wrong interpreter
    sys.exit(f"crboost needs Python 3.11 or newer; {sys.executable} is {sys.version.split()[0]}")

if os.environ.get("PYTHONPATH") and (Path(sysconfig.get_paths()["purelib"]) / "numpy").is_dir():
    print(f"[crboost] not using PYTHONPATH from this shell (the venv has its own packages): {os.environ['PYTHONPATH']}")
    os.execve(
        sys.executable, [sys.executable, *sys.orig_argv[1:]], {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    )
