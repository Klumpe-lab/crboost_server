"""One tool call from inside a self-test SLURM job (tests/test_cluster.py), started the way a driver
is: the server's interpreter and PYTHONPATH, then `run_tool`, so the container command is built on
the compute node exactly as in the pipeline.

    cluster_probe.py <tool> <command>
"""

import sys
from pathlib import Path

from drivers.driver_base import run_tool

run_tool(sys.argv[2], tool_name=sys.argv[1], cwd=Path.cwd())
