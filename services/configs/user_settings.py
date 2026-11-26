"""User settings: switches that belong to the person running this server, set in the toolbar's User settings.

They live in `~/.crboost/user_settings.json`, not in the browser-synced preferences
(`user_prefs_service.py`): those are reloaded from whichever browser opens a page last, and a switch that
grants agents write access must not flip back because an old browser tab reconnected. Read from the file
whenever it changed, so the server answers with what the dialog last saved, whichever process saved it.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

SETTINGS_FILE = Path.home() / ".crboost" / "user_settings.json"


class UserSettings(BaseModel):
    # Agents (the agent API, e.g. Claude Code) may change and delete in the projects this user owns, not only
    # in those the API made. Reading is open either way. Off until the user turns it on.
    agents_may_edit_my_projects: bool = False
    # The look the pages open in (ui/dashboard/theme.py THEMES): light, dark, or one of the papers.
    theme: str = "light"


_cached: tuple[float, UserSettings] | None = None


def load_user_settings() -> UserSettings:
    """The saved settings; the defaults when nothing was saved. Blocking (a stat, and a read when the file
    changed): call it from a thread on the event loop's behalf."""
    global _cached
    try:
        st = SETTINGS_FILE.stat()
    except FileNotFoundError:
        _cached = None
        return UserSettings()
    if st.st_uid != os.getuid():
        logger.warning("%s is not owned by this user; using the defaults (everything off)", SETTINGS_FILE)
        return UserSettings()
    mtime = st.st_mtime
    if _cached is not None and _cached[0] == mtime:
        return _cached[1]
    try:
        settings = UserSettings.model_validate_json(SETTINGS_FILE.read_text())
    except (OSError, ValidationError):
        # A file that cannot be read grants nothing: every switch is off until the dialog saves it again.
        logger.exception("Could not read %s; using the defaults (everything off)", SETTINGS_FILE)
        settings = UserSettings()
    _cached = (mtime, settings)
    return settings


def save_user_settings(settings: UserSettings) -> None:
    """Write the settings atomically, readable and writable by this user only (mkstemp's 0600): nobody else
    may switch what this user's agents are allowed. Blocking."""
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(SETTINGS_FILE.parent), prefix=".user_settings_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(settings.model_dump_json(indent=2))
        os.replace(tmp, SETTINGS_FILE)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
