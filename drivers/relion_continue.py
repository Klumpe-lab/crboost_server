"""Continuing a relion_refine run that stopped early (a time limit, a lost node) from the last iteration it finished.

relion_refine writes a checkpoint at the end of every iteration: run_itNNN_optimiser.star and the files it names.
`relion_refine --continue <that star> --o run_ct<N>` goes on from there and names what it writes run_ct<N>_it<M>_*.
On --continue it takes every option from the checkpoint except the computation ones in CONTINUE_OPTIONS, so a re-run
with other settings is refused rather than continued under the old ones.
"""

from __future__ import annotations

import re
from pathlib import Path

from drivers.driver_base import ToolCommand

# On --continue relion_refine reads every option back from the checkpoint except these computation ones.
CONTINUE_OPTIONS = ("--j", "--pool", "--gpu", "--preread_images", "--dont_combine_weights_via_disc")
# A finished iteration's optimiser star; a continuation names its files run_ct<N>_it<M>_*.
OPTIMISER_RE = re.compile(r"run(?:_ct\d+)?_it(\d{3})_optimiser\.star")
PROGRAMS = ("relion_refine_mpi", "relion_refine")


def program_index(parts: list[str]) -> int:
    """Where the relion_refine program sits in a command's parts: first, or after an mpirun prefix."""
    for i, part in enumerate(parts):
        if part in PROGRAMS:
            return i
    raise ValueError(f"no relion_refine program in: {' '.join(parts)}")


def latest_checkpoint(job_dir: Path, iteration_files: tuple[str, ...]) -> tuple[int, Path] | None:
    """The last iteration (>= 1) an earlier attempt finished writing in job_dir, and its optimiser star. An iteration
    counts only when every one of iteration_files (suffixes after run[_ctN]_itNNN) is on disk beside that star."""
    done = []
    for optimiser in job_dir.glob("run*_optimiser.star"):
        m = OPTIMISER_RE.fullmatch(optimiser.name)
        prefix = optimiser.name.removesuffix("_optimiser.star")
        if m and int(m.group(1)) >= 1 and all((job_dir / f"{prefix}{s}").is_file() for s in iteration_files):
            done.append((int(m.group(1)), optimiser))
    return max(done, default=None)


def options_of(argv: list[str]) -> dict[str, tuple[str, ...]]:
    """relion_refine arguments by option: '--sym C1 --ctf' -> {'--sym': ('C1',), '--ctf': ()}."""
    options: dict[str, tuple[str, ...]] = {}
    key = None
    for token in argv:
        if token.startswith("--"):
            key = token
            options[key] = ()
        elif key is not None:
            options[key] += (token,)
    return options


def changed_since_start(job_dir: Path, argv: list[str]) -> list[str]:
    """The options in argv (relion_refine's own arguments) that differ from those the run in job_dir was started
    with, as its run_it000_optimiser.star records them. A continued run reads them from its checkpoint, so it would
    not apply these."""
    first = job_dir / "run_it000_optimiser.star"
    recorded = next((line[1:].split() for line in first.read_text().splitlines() if line.startswith("# --")), None)
    if recorded is None:
        raise ValueError(f"{first} does not record the command its run was started with")
    skip = (*CONTINUE_OPTIONS, "--o")
    then = {k: v for k, v in options_of(recorded).items() if k not in skip}
    now = {k: v for k, v in options_of(argv).items() if k not in skip}

    def shown(values: tuple[str, ...] | None) -> str:
        return "unset" if values is None else " ".join(values) or "on"

    return [f"{k} {shown(then.get(k))} -> {shown(now.get(k))}" for k in sorted(then | now) if then.get(k) != now.get(k)]


def continue_command(fresh: ToolCommand, optimiser: Path, output_root: Path) -> ToolCommand:
    """fresh as a continuation from optimiser: the same mpirun prefix (if any), program and computation options;
    every other option comes from the checkpoint."""
    i = program_index(fresh.parts) + 1
    cmd = ToolCommand(" ".join(fresh.parts[:i]))
    cmd.opt_path("--continue", optimiser, quote=False).opt_path("--o", output_root, quote=False)
    for option, values in options_of(fresh.parts[i:]).items():
        if option in CONTINUE_OPTIONS:
            cmd.raw(" ".join((option, *values)))
    return cmd
