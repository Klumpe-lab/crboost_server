#!/usr/bin/env python3
"""
CryoBoost Server Setup Script

This script helps configure a fresh CryoBoost installation by:
1. Creating conf.yaml from template
2. Validating container paths and testing them
3. Checking SLURM connectivity and partition availability  
4. Setting up the qsub template
5. Validating Python environment

Run with: python setup.py
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Derive paths from script location
SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR / "config"
CONF_TEMPLATE = CONFIG_DIR / "conf.yaml.template"
CONF_FILE = CONFIG_DIR / "conf.yaml"
QSUB_TEMPLATE = CONFIG_DIR / "qsub" / "qsub.template.sh"
QSUB_FILE = CONFIG_DIR / "qsub" / "qsub.sh"


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_header(text: str):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.END}\n")


def print_ok(text: str):
    print(f"  {Colors.GREEN}[OK]{Colors.END} {text}")


def print_warn(text: str):
    print(f"  {Colors.YELLOW}[WARN]{Colors.END} {text}")


def print_fail(text: str):
    print(f"  {Colors.RED}[FAIL]{Colors.END} {text}")


def print_info(text: str):
    print(f"  {Colors.BLUE}[INFO]{Colors.END} {text}")


def prompt(question: str, default: str = "") -> str:
    """Prompt user for input with optional default"""
    if default:
        user_input = input(f"  {question} [{default}]: ").strip()
        return user_input if user_input else default
    else:
        return input(f"  {question}: ").strip()


def prompt_yn(question: str, default: bool = True) -> bool:
    """Yes/no prompt"""
    suffix = "[Y/n]" if default else "[y/N]"
    response = input(f"  {question} {suffix}: ").strip().lower()
    if not response:
        return default
    return response in ("y", "yes")


def run_command(cmd: str, timeout: int = 30) -> tuple[bool, str, str]:
    """Run a shell command and return (success, stdout, stderr)"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)


def load_yaml(path: Path) -> dict:
    """Load YAML file"""
    import yaml
    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: dict):
    """Save YAML file preserving some formatting"""
    import yaml
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


# ==============================================================================
# SETUP STEPS
# ==============================================================================

def step_config_file() -> dict:
    """Step 1: Create or load conf.yaml"""
    print_header("Step 1: Configuration File")
    
    if CONF_FILE.exists():
        print_ok(f"conf.yaml exists at {CONF_FILE}")
        if prompt_yn("Load existing config and continue?", default=True):
            config = load_yaml(CONF_FILE)
            return config
        else:
            if prompt_yn("Overwrite with fresh template?", default=False):
                shutil.copy(CONF_TEMPLATE, CONF_FILE)
                print_info(f"Copied template to {CONF_FILE}")
            else:
                print_info("Keeping existing config")
                return load_yaml(CONF_FILE)
    else:
        if not CONF_TEMPLATE.exists():
            print_fail(f"Template not found at {CONF_TEMPLATE}")
            print_info("Please ensure conf.yaml.template exists in config/")
            sys.exit(1)
        
        shutil.copy(CONF_TEMPLATE, CONF_FILE)
        print_ok(f"Created {CONF_FILE} from template")
    
    config = load_yaml(CONF_FILE)
    
    # Prompt for essential values
    print_info("Let's configure the essential paths:\n")
    
    config["crboost_root"] = prompt(
        "CryoBoost server root directory", 
        default=str(SCRIPT_DIR)
    )
    
    default_venv = str(Path(config["crboost_root"]) / "venv")
    config["venv_path"] = prompt(
        "Virtual environment path",
        default=default_venv
    )
    
    if "local" not in config:
        config["local"] = {}
    
    config["local"]["DefaultProjectBase"] = prompt(
        "Default project base directory",
        default=config.get("local", {}).get("DefaultProjectBase", "")
    )
    
    save_yaml(CONF_FILE, config)
    print_ok("Configuration saved")
    
    return config


def step_validate_venv(config: dict) -> bool:
    """Step 2: Validate Python virtual environment"""
    print_header("Step 2: Python Environment")
    
    venv_path = Path(config.get("venv_path", ""))
    
    if not venv_path or not venv_path.exists():
        print_warn(f"Virtual environment not found at: {venv_path}")
        print_info("Create one with: python3 -m venv /path/to/venv")
        print_info("Then install requirements: pip install -r requirements.txt")
        return False
    
    venv_python = venv_path / "bin" / "python3"
    if not venv_python.exists():
        print_fail(f"Python not found in venv: {venv_python}")
        return False
    
    print_ok(f"Found venv Python at {venv_python}")
    
    # Check Python version
    success, stdout, _ = run_command(f"{venv_python} --version")
    if success:
        print_ok(f"Python version: {stdout.strip()}")
    
    # Check critical imports
    critical_modules = ["pydantic", "yaml", "nicegui", "asyncio"]
    missing = []
    
    for module in critical_modules:
        success, _, _ = run_command(f"{venv_python} -c 'import {module}'")
        if success:
            print_ok(f"Module '{module}' available")
        else:
            print_fail(f"Module '{module}' not found")
            missing.append(module)
    
    if missing:
        print_warn(f"Missing modules. Run: {venv_python} -m pip install {' '.join(missing)}")
        return False
    
    return True


def step_validate_containers(config: dict) -> dict:
    """Step 3: Validate container paths and test them"""
    print_header("Step 3: Container Validation")
    
    containers = config.get("containers", {})
    
    if not containers:
        print_warn("No containers configured in conf.yaml")
        print_info("Add container paths under the 'containers' section")
        return {}
    
    # Test commands for each container type
    test_commands = {
        "relion": "relion --version 2>&1 | head -1",
        "warp_aretomo": "WarpTools --version 2>&1 | head -1",
        "cryocare": "python -c 'import cryocare; print(\"cryocare OK\")'",
        "pytom": "pytom_match_pick.py --help 2>&1 | head -1",
        "imod": "imodinfo 2>&1 | head -1",
    }
    
    results = {}
    
    for name, path in containers.items():
        print(f"\n  Checking {name}...")
        
        if not path or path.startswith("/path/to"):
            print_warn(f"  {name}: placeholder path, needs configuration")
            results[name] = {"exists": False, "works": False}
            continue
        
        container_path = Path(path)
        
        if not container_path.exists():
            print_fail(f"  {name}: file not found at {path}")
            results[name] = {"exists": False, "works": False}
            continue
        
        print_ok(f"  {name}: file exists ({container_path.stat().st_size / 1e9:.1f} GB)")
        results[name] = {"exists": True, "works": False}
        
        # Try to run test command
        test_cmd = test_commands.get(name, "echo 'container accessible'")
        full_cmd = f"apptainer exec {path} bash -c \"{test_cmd}\""
        
        success, stdout, stderr = run_command(full_cmd, timeout=60)
        
        if success:
            output = stdout.strip() or stderr.strip()
            print_ok(f"  {name}: test passed - {output[:60]}")
            results[name]["works"] = True
        else:
            print_warn(f"  {name}: test command failed")
            if stderr:
                print_info(f"    Error: {stderr[:100]}")
    
    return results


def step_validate_slurm(config: dict) -> list:
    """Step 4: Check SLURM connectivity and partitions"""
    print_header("Step 4: SLURM Cluster")
    
    # Check if sinfo is available
    success, stdout, stderr = run_command("sinfo --version")
    if not success:
        print_fail("sinfo command not found - is SLURM installed/loaded?")
        print_info("You may need to load a SLURM module or run from a login node")
        return []
    
    print_ok(f"SLURM available: {stdout.strip()}")
    
    # Get partition info
    success, stdout, stderr = run_command("sinfo -h -o '%P %a %D %c %m %G'")
    if not success:
        print_fail("Could not query SLURM partitions")
        return []
    
    print_info("Available partitions:")
    
    partitions = []
    for line in stdout.strip().split("\n"):
        if line.strip():
            parts = line.split()
            if parts:
                partition_name = parts[0].rstrip("*")  # Remove default marker
                partitions.append(partition_name)
                print(f"    {line}")
    
    # Compare with configured partition
    configured_partition = config.get("slurm_defaults", {}).get("partition", "")
    
    if configured_partition:
        if configured_partition in partitions:
            print_ok(f"Configured partition '{configured_partition}' exists")
        else:
            print_warn(f"Configured partition '{configured_partition}' not found in cluster")
            print_info(f"Available: {', '.join(partitions)}")
    else:
        print_info(f"No partition configured. Available: {', '.join(partitions)}")
        if partitions and prompt_yn(f"Use '{partitions[0]}' as default?"):
            if "slurm_defaults" not in config:
                config["slurm_defaults"] = {}
            config["slurm_defaults"]["partition"] = partitions[0]
            save_yaml(CONF_FILE, config)
            print_ok(f"Set default partition to '{partitions[0]}'")
    
    return partitions


def step_setup_qsub(config: dict):
    """Step 5: Set up qsub template"""
    print_header("Step 5: SLURM Job Script (qsub.sh)")
    
    if QSUB_FILE.exists():
        print_ok(f"qsub.sh exists at {QSUB_FILE}")
        if not prompt_yn("Regenerate from template?", default=False):
            print_info("Keeping existing qsub.sh")
            return
    
    if not QSUB_TEMPLATE.exists():
        print_fail(f"Template not found at {QSUB_TEMPLATE}")
        return
    
    # Read template
    template_content = QSUB_TEMPLATE.read_text()
    
    # Substitute values
    crboost_root = config.get("crboost_root", str(SCRIPT_DIR))
    venv_path = config.get("venv_path", "")
    venv_python = str(Path(venv_path) / "bin" / "python3") if venv_path else ""
    
    content = template_content.replace("XXXcraboroot_rootXXX", crboost_root)
    content = content.replace("XXXvenv_pythonXXX", venv_python)
    
    # Write output
    QSUB_FILE.parent.mkdir(parents=True, exist_ok=True)
    QSUB_FILE.write_text(content)
    
    print_ok(f"Generated {QSUB_FILE}")
    print_warn("Remember to add cluster-specific module loads to qsub.sh!")


def step_validate_directories(config: dict):
    """Step 6: Check directory permissions"""
    print_header("Step 6: Directory Validation")
    
    # Check project base
    project_base = config.get("local", {}).get("DefaultProjectBase", "")
    
    if project_base:
        project_path = Path(project_base)
        if project_path.exists():
            print_ok(f"Project base exists: {project_base}")
            # Test writability
            test_file = project_path / ".crboost_write_test"
            try:
                test_file.touch()
                test_file.unlink()
                print_ok("Project base is writable")
            except PermissionError:
                print_fail("Project base is not writable")
        else:
            print_warn(f"Project base does not exist: {project_base}")
            if prompt_yn("Create it?"):
                try:
                    project_path.mkdir(parents=True)
                    print_ok(f"Created {project_base}")
                except Exception as e:
                    print_fail(f"Could not create directory: {e}")
    else:
        print_warn("No DefaultProjectBase configured")
    
    # Check Schemes directory
    schemes_dir = SCRIPT_DIR / "config" / "Schemes" / "warp_tomo_prep"
    if schemes_dir.exists():
        print_ok(f"Schemes directory exists: {schemes_dir}")
    else:
        print_warn(f"Schemes directory not found: {schemes_dir}")


def print_summary(config: dict, container_results: dict, partitions: list):
    """Print final summary"""
    print_header("Setup Summary")
    
    all_ok = True
    
    # Config
    print(f"  Configuration: {CONF_FILE}")
    print(f"  CryoBoost Root: {config.get('crboost_root', 'NOT SET')}")
    print(f"  Venv Path: {config.get('venv_path', 'NOT SET')}")
    print(f"  Project Base: {config.get('local', {}).get('DefaultProjectBase', 'NOT SET')}")
    print()
    
    # Containers
    print("  Containers:")
    for name, result in container_results.items():
        status = "OK" if result.get("works") else ("EXISTS" if result.get("exists") else "MISSING")
        color = Colors.GREEN if result.get("works") else (Colors.YELLOW if result.get("exists") else Colors.RED)
        print(f"    {color}{name}: {status}{Colors.END}")
        if not result.get("works"):
            all_ok = False
    print()
    
    # SLURM
    if partitions:
        print(f"  SLURM Partitions: {', '.join(partitions[:5])}{'...' if len(partitions) > 5 else ''}")
    else:
        print(f"  {Colors.YELLOW}SLURM: Not available or not configured{Colors.END}")
    
    print()
    if all_ok:
        print(f"  {Colors.GREEN}{Colors.BOLD}Setup looks good! You can start the server with:{Colors.END}")
        print(f"    cd {SCRIPT_DIR}")
        venv_python = Path(config.get('venv_path', '')) / 'bin' / 'python3'
        print(f"    {venv_python} main.py")
    else:
        print(f"  {Colors.YELLOW}Some issues need attention - see warnings above.{Colors.END}")


def main():
    print(f"\n{Colors.BOLD}CryoBoost Server Setup{Colors.END}")
    print(f"Repository: {SCRIPT_DIR}\n")
    
    # Check for yaml module early
    try:
        import yaml
    except ImportError:
        print_fail("PyYAML not installed. Run: pip install pyyaml")
        sys.exit(1)
    
    # Run setup steps
    config = step_config_file()
    step_validate_venv(config)
    container_results = step_validate_containers(config)
    partitions = step_validate_slurm(config)
    step_setup_qsub(config)
    step_validate_directories(config)
    
    print_summary(config, container_results, partitions)


if __name__ == "__main__":
    main()