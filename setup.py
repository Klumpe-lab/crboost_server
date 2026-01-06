#!/usr/bin/env python3
"""
CryoBoost Server Setup Script

Creates conf.yaml and qsub.sh from templates, validates the environment.
Run with: python setup.py
"""

import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR / "config"
CONF_TEMPLATE = CONFIG_DIR / "conf.yaml.template"
CONF_FILE = CONFIG_DIR / "conf.yaml"
QSUB_TEMPLATE = CONFIG_DIR  / "qsub.template.sh"
QSUB_FILE = CONFIG_DIR  / "qsub.sh"


class C:
    """ANSI colors"""
    G = "\033[92m"   # green
    Y = "\033[93m"   # yellow
    R = "\033[91m"   # red
    B = "\033[94m"   # blue
    BOLD = "\033[1m"
    E = "\033[0m"    # end


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
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def step_config_file() -> dict:
    """Create or load conf.yaml from template"""
    header(f"1. Configuration file: {CONF_FILE}")
    
    if CONF_FILE.exists():
        ok("conf.yaml exists")
        if prompt_yn("Load existing and continue?", default=True):
            return load_yaml(CONF_FILE)
        if prompt_yn("Overwrite with fresh template?", default=False):
            shutil.copy(CONF_TEMPLATE, CONF_FILE)
            info("Created fresh conf.yaml from template")
        else:
            return load_yaml(CONF_FILE)
    else:
        if not CONF_TEMPLATE.exists():
            fail(f"Template not found: {CONF_TEMPLATE}")
            sys.exit(1)
        shutil.copy(CONF_TEMPLATE, CONF_FILE)
        ok("Created conf.yaml from template")
    
    config = load_yaml(CONF_FILE)
    
    info("Configure essential paths (edit conf.yaml later to change):\n")
    
    config["crboost_root"] = prompt("crboost_root", default=str(SCRIPT_DIR))
    
    default_venv = str(Path(config["crboost_root"]) / "venv")
    config["venv_path"] = prompt("venv_path", default=default_venv)
    
    if "local" not in config:
        config["local"] = {}
    config["local"]["DefaultProjectBase"] = prompt(
        "local.DefaultProjectBase (where projects are created)",
        default=config.get("local", {}).get("DefaultProjectBase", "")
    )
    
    save_yaml(CONF_FILE, config)
    ok("Saved conf.yaml")
    return config

def step_validate_python(config: dict) -> bool:
    """Check Python executable specified in conf.yaml -> python_executable"""
    header("2. Python (conf.yaml -> python_executable)")
    
    py_path = config.get("python_executable", "")
    
    if not py_path:
        fail("python_executable not set in conf.yaml")
        return False
    
    if py_path.startswith("/path/to"):
        fail(f"python_executable is placeholder: {py_path}")
        return False
    
    py = Path(py_path)
    
    if not py.exists():
        fail(f"Not found: {py}")
        info("Set python_executable to your venv/conda python path")
        return False
    
    ok(f"python_executable: {py}")
    
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
    
    ok(f"Modules OK: {', '.join(critical)}")
    return True

# def step_validate_venv(config: dict) -> bool:
#     """Check Python venv specified in conf.yaml -> venv_path"""
#     header("2. Python environment (conf.yaml -> venv_path)")
    
#     venv_path_str = config.get("venv_path", "")
    
#     if not venv_path_str:
#         fail("venv_path not set in conf.yaml")
#         info("Set 'venv_path: /path/to/your/venv' in conf.yaml")
#         return False
    
#     if venv_path_str.startswith("/path/to"):
#         fail(f"venv_path is still placeholder: {venv_path_str}")
#         info("Edit conf.yaml and set venv_path to your actual venv location")
#         return False
    
#     venv_path = Path(venv_path_str)
    
#     if not venv_path.is_absolute():
#         fail(f"venv_path should be absolute, got: {venv_path}")
#         return False
    
#     if not venv_path.exists():
#         fail(f"venv directory not found: {venv_path}")
#         info("Create with: python3 -m venv {venv_path}")
#         return False
    
#     venv_python = venv_path / "bin" / "python3"
#     if not venv_python.exists():
#         fail(f"python3 not found at: {venv_python}")
#         return False
    
#     ok(f"venv_path: {venv_path}")
    
#     success, stdout, _ = run_cmd(f"{venv_python} --version")
#     if success:
#         ok(f"Python: {stdout.strip()}")
    
#     # Check critical imports
#     critical = ["pydantic", "yaml", "nicegui"]
#     missing = []
#     for mod in critical:
#         success, _, _ = run_cmd(f"{venv_python} -c 'import {mod}'")
#         if not success:
#             missing.append(mod)
    
#     if missing:
#         fail(f"Missing modules: {', '.join(missing)}")
#         info(f"Run: {venv_python} -m pip install -r requirements.txt")
#         return False
    
#     ok(f"Required modules present: {', '.join(critical)}")
#     return True


def step_validate_containers(config: dict) -> dict:
    """Check containers specified in conf.yaml -> containers"""
    header("3. Containers (conf.yaml -> containers)")
    
    containers = config.get("containers", {})
    
    if not containers:
        warn("No containers configured")
        info("Add container paths under 'containers:' in conf.yaml")
        return {}
    
    # Test commands that work reliably in each container
    test_commands = {
        "relion": "relion --version 2>&1 | head -1",
        "warp_aretomo": "WarpTools --version 2>&1 | head -1",
        "cryocare": "python -c 'import cryocare' && echo 'cryocare import OK'",
        "pytom": "python -c 'from pytom_tm.entry_points import match_template; print(\"pytom OK\")'",
        "imod": "imodinfo 2>&1 | head -1",
    }
    
    results = {}
    
    for name, path in containers.items():
        if not path or path.startswith("/path/to"):
            warn(f"{name}: placeholder path, needs configuration")
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
        # Use proper quoting to avoid bash interpretation issues
        full_cmd = f'apptainer exec {path} bash -c "{test_cmd}"'
        
        success, stdout, stderr = run_cmd(full_cmd, timeout=60)
        output = (stdout.strip() or stderr.strip())[:50]
        
        # Check for actual failures in output even if exit code is 0
        if success and "not found" not in output.lower() and "error" not in output.lower():
            ok(f"{name}: {size_gb:.1f}GB - {output}")
            results[name]["works"] = True
        else:
            warn(f"{name}: {size_gb:.1f}GB - test failed: {output}")
    
    return results


def step_validate_slurm(config: dict) -> list:
    """Check SLURM availability and partitions"""
    header("4. SLURM cluster (conf.yaml -> slurm_defaults.partition)")
    
    success, stdout, _ = run_cmd("sinfo --version")
    if not success:
        fail("sinfo not available - are you on a login node with SLURM?")
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
                partitions.append(pname)
                print(f"       {line}")
    
    # Dedupe partition names (same partition can appear multiple times with different node configs)
    unique_partitions = list(dict.fromkeys(partitions))
    
    configured = config.get("slurm_defaults", {}).get("partition", "")
    if configured:
        if configured in unique_partitions:
            ok(f"Configured partition '{configured}' exists")
        else:
            warn(f"Configured partition '{configured}' not found")
            info(f"Available: {', '.join(unique_partitions)}")
    else:
        info(f"No partition configured. Set slurm_defaults.partition in conf.yaml")
        info(f"Available: {', '.join(unique_partitions)}")
    
    return unique_partitions


def step_setup_qsub(config: dict):
    """Generate qsub.sh from template"""
    header(f"5. SLURM job script: {QSUB_FILE}")
    
    if QSUB_FILE.exists():
        ok("qsub.sh exists")
        if not prompt_yn("Regenerate from template?", default=False):
            info("Keeping existing qsub.sh")
            return
    
    if not QSUB_TEMPLATE.exists():
        fail(f"Template not found: {QSUB_TEMPLATE}")
        return
    
    content = QSUB_TEMPLATE.read_text()
    
    crboost_root = config.get("crboost_root", str(SCRIPT_DIR))
    venv_path = config.get("venv_path", "")
    venv_python = str(Path(venv_path) / "bin" / "python3") if venv_path and not venv_path.startswith("/path/to") else "VENV_NOT_SET"
    
    content = content.replace("XXXcrboost_rootXXX", crboost_root)
    content = content.replace("XXXvenv_pythonXXX", venv_python)
    
    QSUB_FILE.parent.mkdir(parents=True, exist_ok=True)
    QSUB_FILE.write_text(content)
    
    ok(f"Generated qsub.sh")
    warn("Edit qsub.sh to add your cluster's module loads!")


def step_validate_directories(config: dict):
    """Check project directories"""
    header("6. Directories")
    
    project_base = config.get("local", {}).get("DefaultProjectBase", "")
    
    if not project_base:
        warn("local.DefaultProjectBase not set in conf.yaml")
    elif project_base.startswith("/path/to"):
        warn(f"DefaultProjectBase is placeholder: {project_base}")
    else:
        ppath = Path(project_base)
        if ppath.exists():
            test_file = ppath / ".crboost_write_test"
            try:
                test_file.touch()
                test_file.unlink()
                ok(f"DefaultProjectBase: {project_base} (writable)")
            except PermissionError:
                fail(f"DefaultProjectBase: {project_base} (not writable)")
        else:
            warn(f"DefaultProjectBase does not exist: {project_base}")
            if prompt_yn("Create it?"):
                try:
                    ppath.mkdir(parents=True)
                    ok(f"Created {project_base}")
                except Exception as e:
                    fail(f"Could not create: {e}")
    
    schemes = SCRIPT_DIR / "config" / "Schemes" / "warp_tomo_prep"
    if schemes.exists():
        ok(f"Schemes dir: {schemes}")
    else:
        warn(f"Schemes dir missing: {schemes}")


def print_summary(config: dict, container_results: dict, partitions: list):
    header("Summary")
    
    issues = []
    
    crboost_root = config.get("crboost_root", "")
    venv_path = config.get("venv_path", "")
    project_base = config.get("local", {}).get("DefaultProjectBase", "")
    
    def check_val(name, val):
        if not val or val.startswith("/path/to"):
            issues.append(name)
            return f"{C.R}NOT SET{C.E}"
        return val
    
    print(f"  crboost_root: {check_val('crboost_root', crboost_root)}")
    print(f"  venv_path: {check_val('venv_path', venv_path)}")
    print(f"  DefaultProjectBase: {check_val('DefaultProjectBase', project_base)}")
    
    print(f"\n  Containers:")
    for name, r in container_results.items():
        if r.get("works"):
            print(f"    {C.G}{name}: OK{C.E}")
        elif r.get("exists"):
            print(f"    {C.Y}{name}: exists but test failed{C.E}")
            issues.append(f"container:{name}")
        else:
            print(f"    {C.R}{name}: missing{C.E}")
            issues.append(f"container:{name}")
    
    if partitions:
        print(f"\n  SLURM partitions: {', '.join(dict.fromkeys(partitions))}")
    else:
        print(f"\n  {C.Y}SLURM: not available{C.E}")
    
    print()
    if not issues:
        print(f"  {C.G}{C.BOLD}Ready to go!{C.E}")
        venv_py = Path(venv_path) / "bin" / "python3" if venv_path else "python3"
        print(f"  Start server: {venv_py} main.py")
    else:
        print(f"  {C.Y}Issues to fix:{C.E} {', '.join(issues)}")
        print(f"  Edit conf.yaml and re-run setup.py")


def main():
    print(f"{C.BOLD}CryoBoost Setup{C.E} - {SCRIPT_DIR}\n")
    
    try:
        import yaml
    except ImportError:
        fail("PyYAML not installed. Run: pip install pyyaml")
        sys.exit(1)
    
    config = step_config_file()
    step_validate_python(config)
    # step_validate_venv(config)
    container_results = step_validate_containers(config)
    partitions = step_validate_slurm(config)
    step_setup_qsub(config)
    step_validate_directories(config)
    print_summary(config, container_results, partitions)


if __name__ == "__main__":
    main()