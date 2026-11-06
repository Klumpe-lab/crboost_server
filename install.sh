#!/bin/sh
# Builds this checkout's own Python environment: uv, CPython 3.11 and venv/ all live inside the checkout
# (.uv/, venv/), so the install does not depend on ~/.local, a cluster module or anyone's shell.
# Safe to run again: it rebuilds venv/ from requirements.txt. Stop the server first.
set -eu
cd "$(dirname "$0")"
here=$(pwd -P)
export UV_PYTHON_INSTALL_DIR="$here/.uv/python" UV_CACHE_DIR="$here/.uv/cache"
export UV_PYTHON_PREFERENCE=only-managed UV_LINK_MODE=copy UV_NO_CONFIG=1
unset PYTHONPATH PYTHONHOME VIRTUAL_ENV CONDA_PREFIX

uv="$here/.uv/bin/uv"
if [ ! -x "$uv" ]; then
    echo "install: fetching uv into $here/.uv/bin"
    curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$here/.uv/bin" sh
fi

"$uv" python install 3.11
"$uv" venv --clear --python 3.11 venv
"$uv" pip sync --python venv/bin/python3 --compile-bytecode requirements.txt
venv/bin/python3 -c "import torch; print('torch', torch.__version__, torch._C._cuda_getArchFlags())"
echo "install: done. Next: venv/bin/python3 preflight.py, then ./crboost"
