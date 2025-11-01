# services/container_service.py

from pathlib import Path
import re
import shlex
from typing import List, Optional, Tuple
from services.config_service import get_config_service

# --- All color constants removed ---

class Colors:
    # Kept helper methods as class methods
    @classmethod
    def _parse_container_command(cls, command: str) -> Tuple[str, List[str], str, str]:
        """Parse the containerized command into components."""
        env_match = re.match(r"(.*?)(apptainer|singularity)", command, re.DOTALL)
        env_cleanup = env_match.group(1).strip().rstrip(";").strip() if env_match else ""

        bind_pattern = r"-B\s+([^\s]+)"
        bind_paths = re.findall(bind_pattern, command)

        container_match = re.search(r"([^\s]+\.sif)", command)
        container_path = container_match.group(1) if container_match else "unknown"
        inner_match = re.search(r"bash -c '(.+)'$", command)
        inner_command = inner_match.group(1) if inner_match else "unknown"

        return env_cleanup, bind_paths, container_path, inner_command

    @classmethod
    def _format_inner_command(cls, command: str, indent: int = 3) -> List[str]:
        """
        Format inner command with intelligent line breaking.
        Breaks at logical points: &&, |, ;, and long argument lists.
        """
        lines = []
        spaces = " " * indent
        max_width = 100

        # First, split by logical operators
        logical_splits = re.split(r"(\s+&&\s+|\s+\|\|\s+|\s*;\s*)", command)

        for part_idx, part in enumerate(logical_splits):
            part = part.strip()
            if not part:
                continue

            # Check if this is an operator
            if part in ["&&", "||", ";"]:
                if lines:
                    lines[-1] += f" {part} \\"
                continue

            # Split into command and arguments
            try:
                tokens = shlex.split(part)
            except ValueError:
                tokens = part.split() # Fallback for unquoted strings
            
            if not tokens:
                continue

            current_line = spaces
            line_length = indent

            for token_idx, token in enumerate(tokens):
                token_len = len(token) + 1  # +1 for space

                # Start new line if:
                # 1. Would exceed max width
                # 2. Token is a flag (starts with --)
                should_break = False
                if line_length + token_len > max_width and token_idx > 0:
                    should_break = True
                elif token.startswith("--") and token_idx > 0 and not tokens[token_idx-1].startswith("--"):
                     should_break = True

                if should_break:
                    lines.append(current_line.rstrip() + " \\")
                    current_line = spaces
                    line_length = indent

                current_line += token + " "
                line_length += token_len

            # Add the completed line
            if current_line.strip():
                lines.append(current_line.rstrip())

        return lines

    @classmethod
    def format_command_log(cls, tool_name: str, command: str, cwd: Path, container_path: Optional[str] = None) -> str:
        """
        Format a command execution log with simple, clean formatting.
        """
        env_cleanup, bind_paths, parsed_container, inner_command = cls._parse_container_command(command)
        display_container = container_path or parsed_container

        def shorten_path(p: str, max_len: int = 70) -> str:
            if len(p) <= max_len:
                return p
            try:
                parts = Path(p).parts
                if len(parts) > 4:
                    return f"{parts[0]}/{parts[1]}/.../{parts[-2]}/{parts[-1]}"
            except Exception:
                pass # Handle non-path strings
            return p

        lines = [
            f"--- [ CONTAINER EXECUTION ] ---",
            f"  Tool:      {tool_name}",
            f"  CWD:       {cwd}",
            f"  Image:     {shorten_path(display_container)}",
        ]

        if env_cleanup:
            lines.append("")
            lines.append("  Environment:")
            env_vars = env_cleanup.replace("unset", "").strip().split()
            if env_vars:
                lines.append("    unset \\")
                # Group vars for readability, e.g., 5 per line
                for i in range(0, len(env_vars), 5):
                    line_vars = env_vars[i:i+5]
                    line = "        " + " ".join(line_vars)
                    if i + 5 < len(env_vars):
                        line += " \\"
                    lines.append(line)

        lines.append("")
        lines.append("  Command:")
        lines.append("    apptainer run --nv --cleanenv \\")
        
        if bind_paths:
             for bind in bind_paths:
                # The path already contains ":ro" if it's read-only
                lines.append(f"        -B {bind} \\")

        lines.append(f"        {shorten_path(parsed_container)} \\")
        lines.append(f"        bash -c '")
        
        if inner_command and inner_command != "unknown":
            # Indent inner command
            formatted_lines = cls._format_inner_command(inner_command, indent=12)
            for line in formatted_lines:
                lines.append(f"{line}")
        
        lines.append(f"        '")
        lines.append("-" * 70)

        return "\n".join(lines)


class ContainerService:
    def __init__(self):
        self.config = get_config_service()
        self.gui_containers = {"relion"}
        self.cli_containers = {"warp_aretomo", "cryocare", "pytom"}

    def get_container_path(self, tool_name: str) -> Optional[str]:
        return self.config.get_container_for_tool(tool_name)

    def wrap_command_for_tool(self, command: str, cwd: Path, tool_name: str, additional_binds: List[str] = None) -> str:
        # The driver will only call this, so we just call the single command wrapper
        return self._wrap_single_command(command, cwd, tool_name, additional_binds)

    def _wrap_single_command(self, command: str, cwd: Path, tool_name: str, additional_binds: List[str] = None) -> str:
        """Wrap a single command in container"""
        container_path = self.get_container_path(tool_name)
        if not container_path:
            print(f"[CONTAINER WARN] No container found for tool '{tool_name}', running natively")
            return command

        binds = set()
        essential_paths = ["/tmp", "/scratch", str(Path.home()), str(cwd.resolve())]
        for p in essential_paths:
            if Path(p).exists():
                binds.add(str(Path(p).resolve()))

        if additional_binds:
            for p in additional_binds:
                path = Path(p).resolve()
                if path.exists():
                    binds.add(str(path))

        hpc_paths = [
            "/usr/bin",
            "/usr/lib64/slurm",
            "/run/munge",
            "/etc/passwd",
            "/etc/group",
            "/groups",
            "/programs",
            "/software",
        ]
        for p_str in hpc_paths:
            path = Path(p_str)
            if path.exists():
                if "passwd" in p_str or "group" in p_str:
                    binds.add(f"{p_str}:{p_str}:ro")
                else:
                    binds.add(p_str)

        bind_args = []
        for path in sorted(binds):
            bind_args.extend(["-B", path])

        inner_command_quoted = shlex.quote(command)

        apptainer_cmd_parts = [
            "apptainer",
            "run",
            "--nv",
            "--cleanenv",
            *bind_args,
            container_path,
            "bash",
            "-c",
            inner_command_quoted,
        ]

        apptainer_cmd = " ".join(apptainer_cmd_parts)

        clean_env_vars = [
            "SINGULARITY_BIND",
            "APPTAINER_BIND",
            "SINGULARITY_BINDPATH",
            "APPTAINER_BINDPATH",
            "SINGULARITY_NAME",
            "APPTAINER_NAME",
            "SINGULARITY_CONTAINER",
            "APPTAINER_CONTAINER",
            "LD_PRELOAD",
            "XDG_RUNTIME_DIR",
            "CONDA_PREFIX",
            "CONDA_DEFAULT_ENV",
            "CONDA_PROMPT_MODIFIER",
        ]
        if "relion" in tool_name.lower():
            clean_env_vars.extend(["DISPLAY", "XAUTHORITY"])

        clean_env_cmd = "unset " + " ".join(clean_env_vars)
        final_command = f"{clean_env_cmd}; {apptainer_cmd}"

        # This log will be printed by the driver script into run.out
        # It is now plain-text and formatted.
        print(Colors.format_command_log(tool_name, final_command, cwd, container_path))

        return final_command


_container_service = None


def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
