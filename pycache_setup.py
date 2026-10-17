"""Where the server's compiled bytecode goes; imported first by main.py, before any other module.

Bytecode goes to a per-user cache instead of next to the sources (which stay as they are), so the
server does not compile every module from source on each start and again on the first click that
imports one: hundreds of ms on the event loop per view. A `-B` on the command line is overridden
for the same reason.
"""

import sys
from pathlib import Path

sys.dont_write_bytecode = False
sys.pycache_prefix = str(Path.home() / ".cache" / "crboost" / "pycache")
