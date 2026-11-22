"""Which projects the agent API may change.

Reads reach every project. A write reaches the projects the API made and, while the user has turned on
"Allow agents to access and edit my projects" in User settings, the projects this user owns. A lab-shared
project and another person's project stay out of reach: one person's switch does not open what belongs to
others. "This user" is the account the server runs as, the one the roster's "Only mine" means.
"""

from __future__ import annotations

import asyncio
import getpass
import pwd
from pathlib import Path

from services.configs.user_settings import load_user_settings

SETTING_NAME = "Allow agents to access and edit my projects"
SETTING_WHERE = "User settings (the person icon on the start page's toolbar)"


def _file_owner(project_path: Path) -> str | None:
    """The account that owns the project's params file: who a project without a recorded creator belongs to,
    as the roster has it."""
    try:
        return pwd.getpwuid((project_path / "project_params.json").stat().st_uid).pw_name
    except (OSError, KeyError):
        # Unreadable, or a uid the passwd database does not know: the owner stays unknown.
        return None


def refusal_for(name: str, *, made_by_api: bool, shared: bool, owner: str | None, allowed: bool) -> str | None:
    """The rule itself, on what a project records: None when the API may change it, else why not."""
    if made_by_api:
        return None
    if shared:
        return f"{name} is shared with the lab: agents change only the projects they made and their user's own."
    me = getpass.getuser()
    if owner != me:
        who = owner or "nobody this server can name"
        return f"{name} belongs to {who}: agents change only the projects they made and their user's own."
    if not allowed:
        return (
            f"{name} was made by hand. Agents change it only while '{SETTING_NAME}' is on in {SETTING_WHERE}; "
            "the user decides that, or makes the change in the UI."
        )
    return None


def _refusal(state, project_path: Path) -> str | None:
    # Newer code wrote it: this server would drop what it cannot read (HOTFIXES H38).
    if reason := getattr(state, "read_only_reason", None):
        return reason
    return refusal_for(
        project_path.name,
        made_by_api=state.made_by_api,
        shared=state.is_shared,
        owner=state.effective_owner or _file_owner(project_path),
        allowed=load_user_settings().agents_may_edit_my_projects,
    )


async def write_refusal(state, project_path: Path) -> str | None:
    """None when the API may change this project now, else why not, in words for the user. Read afresh on every
    call, so switching the setting off stops the next write."""
    return await asyncio.to_thread(_refusal, state, project_path)


async def agents_allowed() -> bool:
    """The User settings switch, as saved now."""
    return (await asyncio.to_thread(load_user_settings)).agents_may_edit_my_projects
