"""Which checkout this process runs, read from its `.git` directory without a git binary.

Several installs (the lab install, personal checkouts, other sites) open the same projects; a
project saved by one and opened by another is where schema skew loses fields. The version is
stamped into every project save and named in the warning when they disagree.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _git_dirs(root: Path) -> tuple[Path, Path] | None:
    """(the dir holding HEAD, the dir holding refs): the same for a clone, different for a worktree,
    whose `.git` is a file naming its own gitdir, which names the common dir in `commondir`."""
    dot_git = root / ".git"
    if dot_git.is_dir():
        return dot_git, dot_git
    if not dot_git.is_file():
        return None
    text = dot_git.read_text().strip()
    if not text.startswith("gitdir:"):
        return None
    head_dir = (root / text.split(":", 1)[1].strip()).resolve()
    common = head_dir / "commondir"
    refs_dir = (head_dir / common.read_text().strip()).resolve() if common.is_file() else head_dir
    return head_dir, refs_dir


def _resolve_ref(refs_dir: Path, ref: str) -> str | None:
    loose = refs_dir / ref
    if loose.is_file():
        return loose.read_text().strip()
    packed = refs_dir / "packed-refs"
    if packed.is_file():
        for line in packed.read_text().splitlines():
            sha, _, name = line.partition(" ")
            if name == ref:
                return sha
    return None


@cache
def code_version() -> str | None:
    """`<branch>@<short sha>` (or `@<short sha>` when detached) of the checkout this code runs from;
    None outside a git checkout, e.g. an unpacked release archive."""
    try:
        dirs = _git_dirs(REPO_ROOT)
        if dirs is None:
            return None
        head_dir, refs_dir = dirs
        head = (head_dir / "HEAD").read_text().strip()
        if not head.startswith("ref:"):
            return f"@{head[:8]}"
        ref = head.split(":", 1)[1].strip()
        sha = _resolve_ref(refs_dir, ref)
        branch = ref.removeprefix("refs/heads/")
        return f"{branch}@{sha[:8]}" if sha else branch
    except OSError:
        # Unreadable .git (permissions, a half-copied checkout): the version is unknown, which the
        # callers already say in words.
        return None
