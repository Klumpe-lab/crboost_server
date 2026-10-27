"""Leave out what a cluster module put on PYTHONPATH; imported first by main.py and preflight.py.

A venv that has its own packages (the uv quick start) can only be shadowed by a module's site-packages on
PYTHONPATH, and sbatch hands the server's environment to every job it submits. When this interpreter's own
site-packages holds numpy, the process starts itself again without PYTHONPATH, so nobody has to `module purge`
first. A venv built on a module's Python, whose packages come from PYTHONPATH, is left as it is.
"""

import os
import sys
import sysconfig
from pathlib import Path

if os.environ.get("PYTHONPATH") and (Path(sysconfig.get_paths()["purelib"]) / "numpy").is_dir():
    print(f"[crboost] not using PYTHONPATH from this shell (the venv has its own packages): {os.environ['PYTHONPATH']}")
    os.execve(
        sys.executable, [sys.executable, *sys.orig_argv[1:]], {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    )
