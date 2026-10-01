"""Cluster self-test, minutes (mostly queue time): `pytest -m cluster`.

One short SLURM job per configured tool, rendered from config/qsub.sh by the production sbatch
renderer and run the way a driver runs a tool (tests/cluster_probe.py -> run_tool). So the site's
qsub.sh header, the driver interpreter, the container wrapper's binds and cache directories, and
the GPU are exercised along with the tool. All jobs are submitted before any test waits, so the
queue wait is paid once. Jobs use slurm_defaults' resources and run in ~/.crboost/selftest/<name>/,
which the compute nodes must see; a failure names the command and the job's logs.
"""

import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from services.computing.slurm_service import SlurmConfig, write_sbatch_script
from services.configs.config_service import REPO_ROOT, get_config_service
from services.jobs.spec import driver_launch_prefix

# The quickest call that proves each tool starts in its container on a compute node, the way the
# pipeline calls it; the GPU tools also check that they see the GPU.
PROBES = {
    "warp_aretomo": "nvidia-smi && WarpTools --help",
    "relion": "relion_refine --version",
    "pytom": "pytom_match_template.py --help && python -c 'import cupy; assert int(cupy.arange(8).sum()) == 28'",
    "cryocare": "python -c 'import cryocare, tensorflow as tf; assert tf.config.list_physical_devices(\"GPU\")'",
    "isonet": "isonet.py --help",
    "miss_alignment": "miss-alignment --help && python -c 'import torch; assert torch.cuda.is_available()'",
    "imod": (
        "printf '10\\t10\\t10\\n' > probe.txt && point2model probe.txt probe.mod -sphere 3 -scat && test -s probe.mod"
    ),
    "pymol": "python -c 'import pymol'",
    "cistem": "command -v simulate",
}
# ChimeraX (curation.sif_path) is no `tools:` entry: the curation worker starts it with a bare
# `<runtime> exec`, and so does its probe.
CHIMERAX = "chimerax"
SELFTEST_ROOT = Path.home() / ".crboost" / "selftest"


def _job_command(name: str) -> str | None:
    """The command a probe job runs, or None when `name` is not configured at this site."""
    config = get_config_service().config
    if name == CHIMERAX:
        cur = config.curation
        if not cur.sif_path:
            return None
        return f"{config.container_runtime} exec {shlex.quote(cur.sif_path)} {shlex.quote(cur.chimerax_bin)} --version"
    if name not in config.tools:
        return None
    prefix = driver_launch_prefix(server_dir=REPO_ROOT, driver_script=REPO_ROOT / "tests" / "cluster_probe.py")
    return f"{prefix} {shlex.quote(name)} {shlex.quote(PROBES[name])}"


@pytest.fixture(scope="session")
def jobs():
    """Every configured probe, submitted with `sbatch --wait` before any test waits on one."""
    cfg = SlurmConfig.from_config_defaults().model_copy(update={"time": "0:15:00"})
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SLURM_", "SBATCH_"))}
    submitted = {}
    for name in [*PROBES, CHIMERAX]:
        command = _job_command(name)
        if command is None:
            continue
        job_dir = SELFTEST_ROOT / name
        shutil.rmtree(job_dir, ignore_errors=True)
        job_dir.mkdir(parents=True)
        script = write_sbatch_script(job_dir / "run.sh", cfg, command)
        proc = subprocess.Popen(
            ["sbatch", "--wait", str(script)],
            cwd=job_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        submitted[name] = (proc, command, job_dir)
    return submitted


def _tail(path: Path, lines: int = 25) -> str:
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:]) if path.exists() else "(no file)"


@pytest.mark.cluster
def test_every_tool_has_a_probe():
    missing = sorted(set(get_config_service().config.tools) - set(PROBES))
    assert not missing, f"no self-test probe for tools: {', '.join(missing)} (add one to PROBES in {__file__})"


@pytest.mark.cluster
@pytest.mark.parametrize("name", [*PROBES, CHIMERAX])
def test_runs_on_a_compute_node(jobs, name):
    if name not in jobs:
        pytest.skip(f"{name} is not configured")
    proc, command, job_dir = jobs[name]
    sbatch_output, _ = proc.communicate()
    assert proc.returncode == 0, (
        f"{name}: the job exited {proc.returncode}\n"
        f"command: {command}\n"
        f"logs:    {job_dir / 'run.out'}\n"
        f"         {job_dir / 'run.err'}\n"
        f"sbatch:  {sbatch_output.strip()}\n"
        f"--- run.err, last lines ---\n{_tail(job_dir / 'run.err')}"
    )
