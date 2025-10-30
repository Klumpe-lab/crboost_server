# services/container_service.py

from pathlib import Path
import re
import shlex
from typing import List, Optional, Tuple
from services.config_service import get_config_service

class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    @classmethod
    def _parse_container_command(cls, command: str) -> Tuple[str, List[str], str, str]:
        """Parse the containerized command into components."""
        env_match = re.match(r"(.*?)(apptainer|singularity)", command, re.DOTALL)
        env_cleanup = (
            env_match.group(1).strip().rstrip(";").strip() if env_match else ""
        )

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
            tokens = part.split()
            if not tokens:
                continue

            current_line = spaces
            line_length = indent

            for token_idx, token in enumerate(tokens):
                token_len = len(token) + 1  # +1 for space

                # Start new line if:
                # 1. Would exceed max width
                # 2. Token is a flag (starts with --)
                # 3. Previous token was a long value
                should_break = False
                if line_length + token_len > max_width and token_idx > 0:
                    should_break = True
                elif token.startswith("--") and token_idx > 0:
                    should_break = True

                if should_break:
                    lines.append(current_line.rstrip() + " \\")
                    current_line = spaces
                    line_length = indent

                current_line += token + " "
                line_length += token_len

            # Add the completed line
            if current_line.strip():
                # Add continuation if not last part
                if part_idx < len(logical_splits) - 1:
                    lines.append(current_line.rstrip())
                else:
                    lines.append(current_line.rstrip())

        return lines

    @classmethod
    def format_command_log(
        cls,
        tool_name: str,
        command: str,
        cwd: Path,
        container_path: Optional[str] = None,
    ) -> str:
        """
        Format a command execution log with colors and proper structure.
        This is the SINGLE place where commands are logged.
        """
        env_cleanup, bind_paths, parsed_container, inner_command = (
            cls._parse_container_command(command)
        )
        display_container = container_path or parsed_container
        def shorten_path(p: str, max_len: int = 50) -> str:
            if len(p) <= max_len:
                return p
            parts = p.split("/")
            if len(parts) > 3:
                return f"{'/'.join(parts[:2])}/.../{parts[-1]}"
            return p
        lines = [
            f"{cls.BOLD}{cls.CYAN}╭─ CONTAINER EXECUTION{cls.RESET}",
            f"{cls.CYAN}│{cls.RESET} {cls.BOLD}Tool:{cls.RESET}      {cls.GREEN}{tool_name}{cls.RESET}",
            f"{cls.CYAN}│{cls.RESET} {cls.BOLD}Container:{cls.RESET} {cls.DIM}{shorten_path(display_container)}{cls.RESET}",
            f"{cls.CYAN}│{cls.RESET} {cls.BOLD}CWD:{cls.RESET}       {cls.YELLOW}{cwd}{cls.RESET}",
        ]
        if env_cleanup:
            lines.append(f"{cls.CYAN}│{cls.RESET}")
            lines.append(
                f"{cls.CYAN}│{cls.RESET} {cls.BOLD}Environment cleanup:{cls.RESET}"
            )
            if env_cleanup.startswith("unset"):
                vars_to_unset = env_cleanup.replace("unset", "").strip().split()
                for i, var in enumerate(vars_to_unset):
                    prefix = "    unset " if i == 0 else "          "
                    suffix = " \\" if i < len(vars_to_unset) - 1 else ""
                    lines.append(
                        f"{cls.CYAN}│{cls.RESET} {cls.DIM}{prefix}{var}{suffix}{cls.RESET}"
                    )
        lines.append(f"{cls.CYAN}│{cls.RESET}")
        lines.append(
            f"{cls.CYAN}│{cls.RESET} {cls.BOLD}Container invocation:{cls.RESET}"
        )
        lines.append(
            f"{cls.CYAN}│{cls.RESET}   {cls.BLUE}apptainer run --nv --cleanenv \\{cls.RESET}"
        )
        if bind_paths:
            for i, bind in enumerate(bind_paths):
                suffix = " \\" if i < len(bind_paths) - 1 else " \\"
                if ":ro" in bind:
                    base_bind = bind.replace(":ro", "")
                    display_bind = f"{base_bind} {cls.DIM}(read-only){cls.RESET}"
                else:
                    display_bind = bind
                lines.append(
                    f"{cls.CYAN}│{cls.RESET}     {cls.BLUE}-B{cls.RESET} {cls.YELLOW}{display_bind}{cls.RESET}{cls.BLUE}{suffix}{cls.RESET}"
                )
        lines.append(
            f"{cls.CYAN}│{cls.RESET}     {cls.DIM}{shorten_path(parsed_container)} \\{cls.RESET}"
        )
        if inner_command and inner_command != "unknown":
            lines.append(f"{cls.CYAN}│{cls.RESET}")
            lines.append(f"{cls.CYAN}│{cls.RESET} {cls.BOLD}Inner command:{cls.RESET}")
            formatted_lines = cls._format_inner_command(inner_command, indent=3)
            for line in formatted_lines:
                lines.append(f"{cls.CYAN}│{cls.RESET} {cls.GREEN}{line}{cls.RESET}")
        lines.append(f"{cls.CYAN}╰{'─' * 70}{cls.RESET}")

        return "\n".join(lines)

class ContainerService:
    def __init__(self):
        self.config = get_config_service()
        self.gui_containers = {"relion"}
        self.cli_containers = {"warp_aretomo", "cryocare", "pytom"}

    def get_container_path(self, tool_name: str) -> Optional[str]:
        return self.config.get_container_for_tool(tool_name)

    def wrap_command_for_tool(
        self,
        command: str,
        cwd: Path,
        tool_name: str,
        additional_binds: List[str] = None,
    ) -> str:
        """
        Wrap a command for containerized execution.
        If command contains ';', only the first part is containerized,
        allowing for native post-processing commands.
        """
        # Check if command has native post-processing (split on first ';' only)
        if ';' in command:
            container_part, native_part = command.split(';', 1)
            
            # Wrap only the container part using existing logic
            wrapped_container = self._wrap_single_command(
                container_part.strip(), cwd, tool_name, additional_binds
            )
            
            # Return: containerized_command ; native_command
            # The native part runs AFTER the container exits
            return f"{wrapped_container} ; {native_part.strip()}"
        else:
            # Normal single command - wrap everything
            return self._wrap_single_command(command, cwd, tool_name, additional_binds)

    def _wrap_single_command(
        self,
        command: str,
        cwd: Path,
        tool_name: str,
        additional_binds: List[str] = None,
    ) -> str:
        """Wrap a single command in container (existing logic unchanged)"""
        container_path = self.get_container_path(tool_name)
        if not container_path:
            print(
                f"[CONTAINER WARN] No container found for tool '{tool_name}', running natively"
            )
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

        # Add HPC paths
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

        # CRITICAL: shlex.quote handles all escaping - DO NOT CHANGE THIS
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

        # Check if relion tool (better than substring check)
        if "relion" in tool_name.lower():
            clean_env_vars.extend(["DISPLAY", "XAUTHORITY"])

        clean_env_cmd = "unset " + " ".join(clean_env_vars)
        final_command = f"{clean_env_cmd}; {apptainer_cmd}"

        print(Colors.format_command_log(tool_name, final_command, cwd, container_path))

        return final_command


_container_service = None


def get_container_service() -> ContainerService:
    global _container_service
    if _container_service is None:
        _container_service = ContainerService()
    return _container_service
