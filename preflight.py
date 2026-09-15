#!/usr/bin/env python3
"""CryoBoost Server preflight.

Creates config/conf.yaml and config/qsub.sh from their templates when they are missing, then checks
the things that break a fresh install. Run it with the interpreter that runs the server and drivers:

    venv/bin/python3 preflight.py

Never edits an existing config file. Exits 1 when any check fails.
"""

import importlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"
CONF_FILE = CONFIG_DIR / "conf.yaml"
QSUB_FILE = CONFIG_DIR / "qsub.sh"
VENV_DIR = ROOT / "venv"
VENV_PYTHON = VENV_DIR / "bin" / "python3"

# Import names of the packages requirements.txt installs that the server or the drivers import.
REQUIRED_MODULES = [
    "nicegui",
    "fastapi",
    "uvicorn",
    "pydantic",
    "yaml",
    "requests",
    "numpy",
    "pandas",
    "scipy",
    "Bio",
    "skimage",
    "PIL",
    "starfile",
    "mrcfile",
    "torch",
    "torchvision",
]

# drivers/array_job_base.py finds this block verbatim in qsub.sh and strips it from array child tasks.
MARKER_BLOCK = (
    "if [ $EXIT_CODE -eq 0 ]; then\n"
    '    echo "Creating RELION_JOB_EXIT_SUCCESS"\n'
    '    touch "./RELION_JOB_EXIT_SUCCESS"\n'
    "else\n"
    '    echo "Creating RELION_JOB_EXIT_FAILURE"\n'
    '    touch "./RELION_JOB_EXIT_FAILURE"\n'
    "fi"
)

failures: list[str] = []


def ok(msg: str) -> None:
    print(f"  [OK]   {msg}")


def warn(msg: str) -> None:
    print(f"  [WARN] {msg}")


def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")
    failures.append(msg)


def create_from_templates() -> bool:
    """Copy missing config files from their templates. True when anything was created."""
    created = False
    if not CONF_FILE.exists():
        text = (CONFIG_DIR / "conf.template.yaml").read_text()
        CONF_FILE.write_text(text.replace("/path/to/crboost_server", str(ROOT)))
        print("  created config/conf.yaml from the template")
        created = True
    if not QSUB_FILE.exists():
        text = (CONFIG_DIR / "qsub.template.sh").read_text()
        text = text.replace("XXXcrboost_rootXXX", str(ROOT)).replace("XXXcrboost_pythonXXX", str(VENV_PYTHON))
        QSUB_FILE.write_text(text)
        print("  created config/qsub.sh from the template")
        created = True
    return created


def check_python() -> None:
    print("\nPython")
    version = sys.version.split()[0]
    if sys.version_info < (3, 11):  # noqa: UP036 — the point is to catch a wrong interpreter
        fail(f"Python {version}; 3.11+ required")
    else:
        ok(f"Python {version} ({sys.executable})")

    if not VENV_PYTHON.exists():
        fail(f"{VENV_PYTHON} not found; drivers on compute nodes run exactly this interpreter")
    elif Path(sys.prefix).resolve() != VENV_DIR.resolve():
        warn(f"not running inside {VENV_DIR}; the imports below were checked in a different environment")

    missing = []
    for name in REQUIRED_MODULES:
        try:
            importlib.import_module(name)
        except ImportError as e:
            missing.append(f"{name} ({e})")
    if missing:
        fail("cannot import: " + "; ".join(missing))
    else:
        ok("imports: " + ", ".join(REQUIRED_MODULES))


def check_config():
    print("\nConfig (config/conf.yaml, overlaid by ~/.crboost/conf.yaml if present)")
    sys.path.insert(0, str(ROOT))
    try:
        from services.configs.config_service import get_config_service

        cfg = get_config_service().config
    except Exception as e:  # any load or validation error is the finding; report it and skip dependent checks
        fail(f"config does not load: {e}")
        return None
    ok("config loads and validates")

    base = cfg.local.DefaultProjectBase
    if not base:
        fail("local.DefaultProjectBase is not set")
    elif not Path(base).is_dir():
        fail(f"local.DefaultProjectBase does not exist: {base}")
    elif not os.access(base, os.W_OK):
        fail(f"local.DefaultProjectBase is not writable: {base}")
    else:
        ok(f"local.DefaultProjectBase: {base}")

    for name, tool in cfg.tools.items():
        is_container = tool.exec_mode == "container"
        path = tool.container_path if is_container else tool.bin_path
        if not path:
            warn(f"tools.{name}: no {'container_path' if is_container else 'bin_path'} set; jobs using it will fail")
        elif is_container and not Path(path).is_file():
            fail(f"tools.{name}: container not found: {path}")
        elif not is_container and not (Path(path).is_file() or shutil.which(path)):
            fail(f"tools.{name}: binary not found: {path}")
        else:
            ok(f"tools.{name}: {path}")
    return cfg


def check_slurm(cfg) -> None:
    print("\nSLURM and Apptainer (this headnode)")
    for exe in ("sbatch", "squeue", "sacct", "sinfo"):
        if shutil.which(exe) is None:
            fail(f"{exe} not on PATH")
    runtime = shutil.which("apptainer") or shutil.which("singularity")
    if runtime is None:
        fail("neither apptainer nor singularity on PATH (compute nodes need it too: load it in qsub.sh)")
    else:
        ok(f"container runtime: {runtime}")

    if cfg is None or shutil.which("sinfo") is None:
        return
    result = subprocess.run(["sinfo", "-h", "-o", "%P"], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        fail(f"sinfo failed: {result.stderr.strip()}")
        return
    available = {p.rstrip("*") for p in result.stdout.split()}

    # (config key, partition value, required?) — curation (ChimeraX/ArtiaX sessions) is optional.
    wanted = [
        ("slurm_defaults.partition", cfg.slurm_defaults.partition, True),
        ("supervisor_slurm.partition", cfg.supervisor_slurm.partition, True),
        ("curation.partition", cfg.curation.partition, False),
    ]
    for job_type, profile in cfg.job_resource_profiles.items():
        if profile.partition:
            wanted.append((f"job_resource_profiles.{job_type}.partition", profile.partition, True))
    for key, value, required in wanted:
        unknown = [p for p in value.split(",") if p not in available]
        if not unknown:
            ok(f"{key} = {value}")
        else:
            msg = f"{key} = {value!r}: no such partition (sinfo lists {', '.join(sorted(available))})"
            if required:
                fail(msg)
            else:
                warn(msg)


def check_qsub() -> None:
    print("\nconfig/qsub.sh")
    text = QSUB_FILE.read_text()
    before = len(failures)
    for placeholder in ("XXXcommandXXX", "XXXoutfileXXX", "XXXerrfileXXX", "XXXextra1XXX"):
        if placeholder not in text:
            fail(f"{placeholder} is gone; crboost fills it per job")
    for leftover in ("XXXcrboost_rootXXX", "XXXcrboost_pythonXXX"):
        if leftover in text:
            fail(f"{leftover} was never filled in")
    if MARKER_BLOCK not in text:
        fail("exit-marker block differs from qsub.template.sh; array child tasks would write RELION_JOB_EXIT_*")
    if "exit $EXIT_CODE" not in text:
        fail("no `exit $EXIT_CODE`; failed jobs would exit 0 and their afterok dependents would still run")
    if len(failures) == before:
        ok("placeholders, exit-marker block and exit code intact")


def main() -> int:
    print(f"CryoBoost preflight: {ROOT}\n\nConfig files")
    if create_from_templates():
        print("\nEdit config/conf.yaml (paths, partitions, tools) and the SLURM HEADER of config/qsub.sh,")
        print("then re-run preflight.")
        return 1
    ok("config/conf.yaml and config/qsub.sh exist")

    check_python()
    cfg = check_config()
    check_slurm(cfg)
    check_qsub()

    print()
    if failures:
        print(f"{len(failures)} check(s) failed.")
        return 1
    print("All checks passed. Start the server with: venv/bin/python3 main.py --port 8081")
    return 0


if __name__ == "__main__":
    sys.exit(main())
