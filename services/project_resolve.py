"""Turn a project *directory name* into an absolute path (roadmap 17 S1).

The URL scheme addresses a project by its directory name — ``/p/HIV_Tomo_Batch1`` — so
that deep links need no new id, no schema change and no index file. This module is the
one place that turns that name back into a path, by searching the bases the user
actually works out of.

Search order (§3 of the roadmap):

1. an explicit ``base`` — the ``?base=`` query param. Authoritative: if the project is
   not there, that is the answer, nothing else is searched.
2. ``current`` — the project this tab already has open, when its directory name matches.
   A no-op re-entry can never be ambiguous, so it short-circuits.
3. ``prefs.project_base_path`` — the base the landing page is pointed at.
4. every ``prefs.recent_project_roots`` entry.
5. the config's ``DefaultProjectBase``.

Bases 3-5 are searched *together*, not first-wins: two distinct directories holding a
project of the same name is a real ambiguity and gets reported (``PROJECT_AMBIGUOUS``
plus the candidate paths) rather than guessed at — CLAUDE.md, *Surfacing uncertainty*.
"""

from __future__ import annotations

import logging
from pathlib import Path

from services.configs.config_service import get_config_service
from services.configs.user_prefs_service import get_prefs_service
from services.result import ErrorCode, err, ok

logger = logging.getLogger(__name__)

PARAMS_FILE = "project_params.json"


def _is_project(path: Path) -> bool:
    try:
        return (path / PARAMS_FILE).is_file()
    except OSError:  # unreadable / unmounted base — not a project we can offer
        return False


def _resolved(path: Path) -> str:
    """Identity key for de-duplicating candidates that differ only by symlink/`..`."""
    try:
        return str(path.resolve())
    except OSError:
        return str(path)


def _search_bases() -> list[Path]:
    """Bases 3-5, in order, de-duplicated. Order matters only for reporting — a hit in
    any of them counts the same when checking for ambiguity."""
    prefs = get_prefs_service().prefs
    raw: list[str] = [prefs.project_base_path or ""]
    raw += [r.path for r in prefs.recent_project_roots]
    raw.append(get_config_service().default_project_base or "")

    out: list[Path] = []
    seen: set[str] = set()
    for entry in raw:
        entry = (entry or "").strip()
        if not entry:
            continue
        p = Path(entry).expanduser()
        key = _resolved(p)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def resolve_project(name: str, *, base: str | None = None, current: Path | str | None = None) -> dict:
    """``ok(path=Path, base=Path)`` for a unique hit.

    Failures carry a :class:`ErrorCode` because the caller genuinely branches on the
    cause: ``PROJECT_AMBIGUOUS`` renders a chooser over ``candidates``, anything else
    sends the user back to the landing page quoting ``searched``.
    """
    name = (name or "").strip()
    if not name:
        return err("No project name in the URL.", code=ErrorCode.PROJECT_NOT_FOUND, searched=[])

    if base:
        base_path = Path(base).expanduser()
        target = base_path / name
        if _is_project(target):
            return ok(path=target, base=base_path)
        return err(
            f"No project '{name}' under {base_path}.", code=ErrorCode.PROJECT_NOT_FOUND, searched=[str(base_path)]
        )

    if current:
        cur = Path(current).expanduser()
        if cur.name == name and _is_project(cur):
            return ok(path=cur, base=cur.parent)

    bases = _search_bases()
    hits: list[Path] = []
    seen: set[str] = set()
    for b in bases:
        target = b / name
        if not _is_project(target):
            continue
        key = _resolved(target)
        if key in seen:
            continue
        seen.add(key)
        hits.append(target)

    if not hits:
        return err(
            f"No project named '{name}' under any known projects directory.",
            code=ErrorCode.PROJECT_NOT_FOUND,
            searched=[str(b) for b in bases],
        )
    if len(hits) > 1:
        logger.info("Ambiguous project name %r: %s", name, [str(h) for h in hits])
        return err(
            f"'{name}' exists in {len(hits)} places — pick one.",
            code=ErrorCode.PROJECT_AMBIGUOUS,
            candidates=[str(h) for h in hits],
        )
    return ok(path=hits[0], base=hits[0].parent)
