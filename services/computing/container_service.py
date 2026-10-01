# services/container_service.py
"""
Renders every external-tool call from conf.yaml.

A container tool runs as ``<runtime> exec --nv --cleanenv -B … <image> bash -c '<env>; <cmd>'``;
a binary tool runs with its ``bin_path`` first on PATH. The only site paths are
``container_binds`` and the tool entries; nothing is guessed, so a tool without a conf entry
(or without its path) raises instead of running something else.
"""

import getpass
import logging
import os
import shlex
from collections.abc import Iterable
from pathlib import Path

from services.configs.config_service import get_config_service

logger = logging.getLogger(__name__)

# JIT/kernel caches every tool may write. Under --cleanenv each would land wherever its
# default points (often a read-only image path or a shared $HOME), so all of them go to one
# node-local, per-user directory.
_CACHE_VARS = {
    "XDG_CACHE_HOME": "",
    "CUPY_CACHE_DIR": "cupy",
    "CUDA_CACHE_PATH": "nv",
    "MPLCONFIGDIR": "matplotlib",
    "TORCHINDUCTOR_CACHE_DIR": "torchinductor",
    "TRITON_CACHE_DIR": "triton",
    "NUMBA_CACHE_DIR": "numba",
}


def _cache_root() -> Path:
    return Path(os.environ.get("TMPDIR") or "/tmp") / f"crboost-{getpass.getuser()}"


def _container_env(cache_root: Path) -> dict[str, str]:
    env = {var: str(cache_root / sub) if sub else str(cache_root) for var, sub in _CACHE_VARS.items()}
    # Progress bars are carriage-return noise in a job log.
    env["TQDM_DISABLE"] = "1"
    return env


def _bind_list(paths: Iterable[str]) -> list[str]:
    """Existing paths, deduplicated, dropping any path already inside another bound one
    (every bind is at the same path, so an ancestor bind exposes it already)."""
    kept: list[str] = []
    for p in sorted({str(p) for p in paths if p and Path(p).exists()}):
        if not any(p == k or p.startswith(k.rstrip("/") + "/") for k in kept):
            kept.append(p)
    return kept


class ContainerService:
    def wrap_command_for_tool(
        self, command: str, cwd: Path, tool_name: str, additional_binds: list[str] | None = None
    ) -> str:
        """The shell command that runs `command` with `tool_name`, from `cwd`.

        `additional_binds` are extra host paths the call reads or writes; the wrapper
        resolves them, drops the ones that do not exist and dedups them, so callers do not."""
        cfg = get_config_service()
        tool = cfg.get_tool_config(tool_name)

        if tool.exec_mode == "binary":
            if not tool.bin_path:
                raise ValueError(f"Tool '{tool_name}' runs as a binary but has no bin_path in config/conf.yaml")
            if not Path(tool.bin_path).is_dir():
                raise ValueError(
                    f"Tool '{tool_name}': bin_path must be the directory holding its executables, got {tool.bin_path}"
                )
            return f"export PATH={shlex.quote(tool.bin_path)}:$PATH; {command}"

        if not tool.container_path:
            raise ValueError(f"Tool '{tool_name}' runs in a container but has no container_path in config/conf.yaml")

        cache_root = _cache_root()
        cache_root.mkdir(parents=True, exist_ok=True)
        binds = _bind_list(
            [
                "/tmp",
                str(cache_root),
                str(Path(cwd).resolve()),
                *(str(Path(b).resolve()) for b in additional_binds or []),
                *cfg.config.container_binds,
            ]
        )
        env = "; ".join(f"export {k}={shlex.quote(v)}" for k, v in _container_env(cache_root).items())
        parts = [
            cfg.config.container_runtime.value,
            "exec",
            "--nv",
            "--cleanenv",
            *(f"-B {shlex.quote(b)}" for b in binds),
            shlex.quote(tool.container_path),
            "bash",
            "-c",
            shlex.quote(f"{env}; {command}"),
        ]
        return " ".join(parts)


_container_service: ContainerService | None = None


def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
