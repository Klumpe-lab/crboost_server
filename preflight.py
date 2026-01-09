#!/usr/bin/env python3
"""
CryoBoost Server Setup Script

Creates conf.yaml and qsub.sh from templates, validates the environment.
Run with: python preflight.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR / "config"

# Template files (what ships with the repo)
CONF_TEMPLATE = CONFIG_DIR / "conf.template.yaml"
QSUB_TEMPLATE = CONFIG_DIR / "qsub.template.sh"

# Generated files (created by this script, in .gitignore)
CONF_FILE = CONFIG_DIR / "conf.yaml"
QSUB_FILE = CONFIG_DIR / "qsub.sh"


class C:
    """ANSI colors"""

    G = "\033[92m"  # green
    Y = "\033[93m"  # yellow
    R = "\033[91m"  # red
    B = "\033[94m"  # blue
    BOLD = "\033[1m"
    DIM = "\033[2m"
    E = "\033[0m"  # end


def ok(text: str):
    print(f"  {C.G}[OK]{C.E} {text}")


def warn(text: str):
    print(f"  {C.Y}[WARN]{C.E} {text}")


def fail(text: str):
    print(f"  {C.R}[FAIL]{C.E} {text}")


def info(text: str):
    print(f"  {C.B}[INFO]{C.E} {text}")


def header(text: str):
    print(f"\n{C.BOLD}{text}{C.E}")


def prompt(question: str, default: str = "") -> str:
    if default:
        user_input = input(f"  {question} [{default}]: ").strip()
        return user_input if user_input else default
    return input(f"  {question}: ").strip()


def prompt_yn(question: str, default: bool = True) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    response = input(f"  {question} {suffix}: ").strip().lower()
    if not response:
        return default
    return response in ("y", "yes")


def run_cmd(cmd: str, timeout: int = 30) -> tuple[bool, str, str]:
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)


def load_yaml(path: Path) -> dict:
    import yaml

    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: dict):
    import yaml

    # Custom representer to handle None values nicely
    def represent_none(dumper, _):
        return dumper.represent_scalar("tag:yaml.org,2002:null", "")

    yaml.add_representer(type(None), represent_none)

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def step_config_file() -> tuple[dict, list]:
    """Create or load conf.yaml from template. Returns (config, list of unset fields)."""
    header(f"1. Configuration: {CONF_FILE}")

    unset_fields = []

    # Case 1: conf.yaml already exists
    if CONF_FILE.exists():
        ok("conf.yaml exists")
        if prompt_yn("Load existing and continue?", default=True):
            config = load_yaml(CONF_FILE)
            return config, unset_fields
        if not prompt_yn("Overwrite with fresh config?", default=False):
            return load_yaml(CONF_FILE), unset_fields

    # Case 2: Need to create conf.yaml from template
    if not CONF_TEMPLATE.exists():
        fail(f"Template not found: {CONF_TEMPLATE}")
        info("Your repository seems incomplete. Re-clone or restore conf.template.yaml")
        sys.exit(1)

    info("Creating conf.yaml from template...")
    print()

    # Start with template defaults
    config = load_yaml(CONF_TEMPLATE)

    # Interactive prompts for essential values
    config["crboost_root"] = prompt("crboost_root (server installation directory)", default=str(SCRIPT_DIR))

    # Python executable
    default_python = str(Path(config["crboost_root"]) / "venv" / "bin" / "python3")
    info(f"Suggested python paths:")
    print(f"       venv:  {default_python}")
    print(f"       conda: ~/miniconda3/envs/crboost/bin/python")
    config["crboost_python"] = prompt("crboost_python", default=default_python)


    # Local settings
    if "local" not in config:
        config["local"] = {}

    config["local"]["DefaultProjectBase"] = prompt(
        "DefaultProjectBase (where projects are created)", default=config.get("local", {}).get("DefaultProjectBase", "")
    )

    # Optional: data globs (can leave empty)
    print()
    info("Default data paths are optional - you can set these later or per-project")
    movies = prompt("DefaultMoviesGlob (optional, press Enter to skip)", default="")
    mdocs = prompt("DefaultMdocsGlob (optional, press Enter to skip)", default="")
    config["local"]["DefaultMoviesGlob"] = movies if movies else None
    config["local"]["DefaultMdocsGlob"] = mdocs if mdocs else None

    # SLURM defaults - try to auto-detect partition
    print()
    header("  SLURM defaults")
    partitions = detect_slurm_partitions()

    if "slurm_defaults" not in config:
        config["slurm_defaults"] = {}

    if partitions:
        info(f"Detected partitions: {', '.join(partitions[:5])}")
        suggested = partitions[0]
    else:
        suggested = config.get("slurm_defaults", {}).get("partition", "gpu")
        info("Could not detect SLURM partitions (maybe not on login node)")

    config["slurm_defaults"]["partition"] = prompt("partition", default=suggested)
    config["slurm_defaults"]["constraint"] = prompt(
        "constraint (optional)", default=config.get("slurm_defaults", {}).get("constraint", "")
    )

    # Containers - just prompt for the base directory
    print()
    header("  Container paths")
    info("Containers must be pre-built .sif files")

    if "containers" not in config:
        config["containers"] = {}

    container_dir = prompt("Container directory (where your .sif files are)", default="/path/to/containers")

    # Set container paths based on directory, or leave as placeholders
    container_names = {
        "warp_aretomo": "warp*.sif",
        "cryocare": "cryocare*.sif",
        "pytom": "pytom*.sif",
        "relion": "relion*.sif",
        "imod": "imod*.sif",
    }

    if container_dir and container_dir != "/path/to/containers":
        container_path = Path(container_dir)
        for name, pattern in container_names.items():
            matches = list(container_path.glob(pattern))
            if matches:
                config["containers"][name] = str(matches[0])
                ok(f"Found {name}: {matches[0].name}")
            else:
                config["containers"][name] = f"{container_dir}/{name}.sif"
                unset_fields.append(f"containers.{name}")
                warn(f"{name}: not found, using placeholder")
    else:
        for name in container_names:
            config["containers"][name] = f"/path/to/{name}.sif"
            unset_fields.append(f"containers.{name}")

    # Save config
    save_yaml(CONF_FILE, config)
    ok(f"Created {CONF_FILE}")

    return config, unset_fields


def detect_slurm_partitions() -> list:
    """Try to detect available SLURM partitions."""
    success, stdout, _ = run_cmd("sinfo -h -o '%P' 2>/dev/null")
    if not success:
        return []

    partitions = []
    for line in stdout.strip().split("\n"):
        pname = line.strip().rstrip("*")
        if pname and pname not in partitions:
            partitions.append(pname)
    return partitions


def step_validate_python(config: dict) -> bool:
    """Check Python executable specified in conf.yaml -> python_executable"""
    
    header("2. Python (conf.yaml -> crboost_python)")

    py_path = config.get("crboost_python", "")

    if not py_path:
        fail("crboost_python not set in conf.yaml")
        return False

    if py_path.startswith("/path/to"):
        fail(f"crboost_python is placeholder: {py_path}")
        return False

    py = Path(py_path)

    if not py.exists():
        fail(f"Not found: {py}")
        info("Create venv: python3 -m venv /path/to/venv")
        info("Or set crboost_python to your conda python path")
        return False

    ok(f"{py}")

    success, stdout, _ = run_cmd(f"{py} --version")
    if success:
        ok(f"Version: {stdout.strip()}")
    else:
        fail("Could not run python --version")
        return False

    # Check critical imports
    critical = ["pydantic", "yaml", "nicegui"]
    missing = []
    for mod in critical:
        success, _, _ = run_cmd(f"{py} -c 'import {mod}'")
        if not success:
            missing.append(mod)

    if missing:
        fail(f"Missing: {', '.join(missing)}")
        info(f"Run: {py} -m pip install -r requirements.txt")
        return False

    ok(f"Modules: {', '.join(critical)}")
    return True


def step_validate_containers(config: dict) -> dict:
    """Check containers specified in conf.yaml -> containers"""
    header("3. Containers (conf.yaml -> containers)")

    containers = config.get("containers", {})

    if not containers:
        warn("No containers configured")
        return {}

    test_commands = {
        "relion"      : "relion --version 2>&1 | head -1",
        "warp_aretomo": "WarpTools --version 2>&1 | head -1",
        "cryocare"    : "python -c 'import cryocare' && echo 'cryocare OK'",
        "pytom"       : "echo 'pytom ok'",
        "imod"        : "imodinfo 2>&1 | head -1",
    }

    results = {}

    for name, path in containers.items():
        if not path or path.startswith("/path/to"):
            warn(f"{name}: needs configuration")
            results[name] = {"exists": False, "works": False}
            continue

        container_path = Path(path)

        if not container_path.exists():
            fail(f"{name}: not found at {path}")
            results[name] = {"exists": False, "works": False}
            continue

        size_gb = container_path.stat().st_size / 1e9
        results[name] = {"exists": True, "works": False}

        test_cmd = test_commands.get(name, "echo 'accessible'")
        full_cmd = f'apptainer exec {path} bash -c "{test_cmd}"'

        success, stdout, stderr = run_cmd(full_cmd, timeout=60)
        output = (stdout.strip() or stderr.strip())[:50]

        if success and "not found" not in output.lower() and "error" not in output.lower():
            ok(f"{name}: {size_gb:.1f}GB - {output}")
            results[name]["works"] = True
        else:
            warn(f"{name}: {size_gb:.1f}GB - test failed")

    return results


def step_validate_slurm(config: dict) -> list:
    """Check SLURM availability and partitions"""
    header("4. SLURM (conf.yaml -> slurm_defaults)")

    success, stdout, _ = run_cmd("sinfo --version")
    if not success:
        warn("sinfo not available - run setup on a login node to validate SLURM")
        return []

    ok(f"SLURM: {stdout.strip()}")

    success, stdout, _ = run_cmd("sinfo -h -o '%P %a %D %G'")
    if not success:
        fail("Could not query partitions")
        return []

    partitions = []
    info("Partitions:")
    for line in stdout.strip().split("\n"):
        if line.strip():
            parts = line.split()
            if parts:
                pname = parts[0].rstrip("*")
                if pname not in partitions:
                    partitions.append(pname)
                print(f"       {line}")

    configured = config.get("slurm_defaults", {}).get("partition", "")
    if configured:
        if configured in partitions:
            ok(f"Configured partition '{configured}' exists")
        else:
            warn(f"Configured partition '{configured}' not in cluster")

    return partitions


def step_setup_qsub(config: dict) -> list:
    """Generate qsub.sh from template. Returns list of placeholders still present."""
    header(f"5. SLURM job script: {QSUB_FILE}")
    
    placeholders_remaining = []
    
    if QSUB_FILE.exists():
        ok("qsub.sh exists")
        return placeholders_remaining
    
    if not QSUB_TEMPLATE.exists():
        fail(f"Template not found: {QSUB_TEMPLATE}")
        return ["qsub.sh (missing template)"]
    
    content = QSUB_TEMPLATE.read_text()
    
    # Substitute known values
    crboost_root = config.get("crboost_root", "")
    python_exec = config.get("crboost_python", "")
    
    if crboost_root and not crboost_root.startswith("/path/to"):
        content = content.replace("XXXcrboost_rootXXX", crboost_root)
    else:
        placeholders_remaining.append("CRBOOST_SERVER_DIR in qsub.sh")
    
    if python_exec and not python_exec.startswith("/path/to"):
        content = content.replace("XXXcrboost_pythonXXX", python_exec)
    else:
        placeholders_remaining.append("CRBOOST_PYTHON in qsub.sh")
    
    QSUB_FILE.write_text(content)
    ok(f"Created {QSUB_FILE}")
    info("Edit qsub.sh to add your cluster's module loads")
    
    return placeholders_remaining


def step_validate_directories(config: dict) -> list:
    """Check project directories. Returns list of issues."""
    header("6. Directories")

    issues = []

    project_base = config.get("local", {}).get("DefaultProjectBase", "")

    if not project_base:
        warn("local.DefaultProjectBase not set")
        issues.append("DefaultProjectBase")
    elif project_base.startswith("/path/to"):
        warn(f"DefaultProjectBase is placeholder")
        issues.append("DefaultProjectBase")
    else:
        ppath = Path(project_base)
        if ppath.exists():
            test_file = ppath / ".crboost_write_test"
            try:
                test_file.touch()
                test_file.unlink()
                ok(f"DefaultProjectBase: {project_base}")
            except PermissionError:
                fail(f"DefaultProjectBase not writable: {project_base}")
                issues.append("DefaultProjectBase (permissions)")
        else:
            warn(f"DefaultProjectBase does not exist: {project_base}")
            if prompt_yn("Create it?"):
                try:
                    ppath.mkdir(parents=True)
                    ok(f"Created {project_base}")
                except Exception as e:
                    fail(f"Could not create: {e}")
                    issues.append("DefaultProjectBase")

    schemes = SCRIPT_DIR / "config" / "Schemes" / "warp_tomo_prep"
    if schemes.exists():
        ok(f"Schemes: {schemes}")
    else:
        warn(f"Schemes missing: {schemes}")
        issues.append("Schemes directory")

    return issues


def print_summary(config: dict, container_results: dict, unset_fields: list, qsub_todos: list, dir_issues: list):
    header("Summary")

    all_issues = list(unset_fields) + list(qsub_todos) + list(dir_issues)

    crboost_root = config.get("crboost_root", "")
    python_exec = config.get("crboost_python", "")
    project_base = config.get("local", {}).get("DefaultProjectBase", "")

    def fmt_val(val):
        if not val or val.startswith("/path/to"):
            return f"{C.R}NOT SET{C.E}"
        return val

    print(f"  crboost_root: {fmt_val(crboost_root)}")
    print(f"  crboost_python: {fmt_val(python_exec)}")
    print(f"  DefaultProjectBase: {fmt_val(project_base)}")

    print(f"\n  Containers:")
    for name, r in container_results.items():
        if r.get("works"):
            print(f"    {C.G}{name}: OK{C.E}")
        elif r.get("exists"):
            print(f"    {C.Y}{name}: exists but test failed{C.E}")
        else:
            print(f"    {C.R}{name}: missing{C.E}")
            if f"containers.{name}" not in all_issues:
                all_issues.append(f"containers.{name}")

    print()
    if not all_issues:
        print(f"  {C.G}{C.BOLD}Ready to go!{C.E}")
        py = python_exec if python_exec and not python_exec.startswith("/path/to") else "python3"
        print(f"  Start server: {py} main.py")
    else:
        print(f"  {C.Y}{C.BOLD}TODO before running:{C.E}")
        for issue in all_issues:
            print(f"    - {issue}")
        print()
        print(f"  Edit {C.BOLD}config/conf.yaml{C.E} and {C.BOLD}config/qsub.sh{C.E}, then re-run setup.py")


def main():
    print(f"{C.BOLD}CryoBoost Setup{C.E} - {SCRIPT_DIR}\n")

    try:
        import yaml
    except ImportError:
        fail("PyYAML not installed")
        info("Run: pip install pyyaml (or use your venv/conda)")
        sys.exit(1)

    config, unset_fields = step_config_file()
    step_validate_python(config)
    container_results = step_validate_containers(config)
    step_validate_slurm(config)
    qsub_todos = step_setup_qsub(config)
    dir_issues = step_validate_directories(config)
    print_summary(config, container_results, unset_fields, qsub_todos, dir_issues)


if __name__ == "__main__":
    main()
