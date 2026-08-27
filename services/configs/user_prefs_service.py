# services/user_prefs_service.py
"""
User preferences persistence - wraps NiceGUI's app.storage.user
and optionally syncs to ~/.crboost/ for server-side access.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

MAX_RECENT_ROOTS = 20


class RecentPath(BaseModel):
    """A recently used directory path."""

    path: str
    last_used: datetime = Field(default_factory=datetime.now)
    label: str | None = None


# Keep old name as alias for backward compat with stored prefs
RecentProjectRoot = RecentPath


class UserPreferences(BaseModel):
    """Persisted user preferences"""

    project_base_path: str = ""
    movies_glob: str = ""
    mdocs_glob: str = ""
    show_only_mine: bool = True
    recent_project_roots: list[RecentPath] = Field(default_factory=list)
    recent_data_paths: list[RecentPath] = Field(default_factory=list)

    # Journey dashboard prefs — user-level, persist across projects + TS.
    # `dashboard_panel` is the ONE section the Journey shows ("all" = every section);
    # a single-select replaced the per-panel hidden set (2026-08-27). Dataset section
    # starts collapsed (header + metrics only).
    dashboard_panel: str = "all"
    dashboard_dataset_collapsed: bool = True

    # --- shared helpers for both MRU lists ---

    @staticmethod
    def _add_to_mru(mru: list[RecentPath], path: str, label: str | None = None, require_exists: bool = True) -> bool:
        if not path or not path.strip():
            return False
        try:
            resolved = str(Path(path).resolve())
        except Exception:
            return False
        if require_exists:
            p = Path(resolved)
            if not p.is_absolute() or not p.exists() or not p.is_dir():
                return False
        mru[:] = [r for r in mru if r.path != resolved]
        mru.insert(0, RecentPath(path=resolved, last_used=datetime.now(), label=label))
        del mru[MAX_RECENT_ROOTS:]
        return True

    @staticmethod
    def _remove_from_mru(mru: list[RecentPath], path: str):
        mru[:] = [r for r in mru if r.path != path]

    @staticmethod
    def _prune_mru(mru: list[RecentPath]) -> int:
        before = len(mru)
        mru[:] = [r for r in mru if Path(r.path).exists() and Path(r.path).is_dir()]
        return before - len(mru)

    # --- project roots ---

    def add_recent_root(self, path: str, label: str | None = None) -> bool:
        return self._add_to_mru(self.recent_project_roots, path, label)

    def remove_recent_root(self, path: str):
        self._remove_from_mru(self.recent_project_roots, path)

    def clear_recent_roots(self):
        self.recent_project_roots.clear()

    def prune_invalid_roots(self) -> int:
        return self._prune_mru(self.recent_project_roots)

    # --- data paths (raw frames / mdocs directories) ---

    def add_recent_data_path(self, path: str, label: str | None = None) -> bool:
        return self._add_to_mru(self.recent_data_paths, path, label)

    def remove_recent_data_path(self, path: str):
        self._remove_from_mru(self.recent_data_paths, path)

    def clear_recent_data_paths(self):
        self.recent_data_paths.clear()

    def prune_invalid_data_paths(self) -> int:
        return self._prune_mru(self.recent_data_paths)


class UserPrefsService:
    """
    Manages user preferences with dual storage:
    - Primary: NiceGUI's app.storage.user (browser-based)
    - Secondary: ~/.crboost/prefs.json (server-side)
    """

    STORAGE_KEY = "crboost_user_prefs"

    def __init__(self):
        self._prefs: UserPreferences | None = None
        self._file_path = Path.home() / ".crboost" / "prefs.json"

    def _ensure_crboost_dir(self):
        self._file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_from_app_storage(self, storage: dict[str, Any]) -> UserPreferences:
        """Load preferences from NiceGUI's app.storage.user"""
        raw = storage.get(self.STORAGE_KEY)
        if raw:
            try:
                self._prefs = UserPreferences(**raw)
                # Auto-prune invalid paths on load
                pruned = self._prefs.prune_invalid_roots()
                pruned += self._prefs.prune_invalid_data_paths()
                if pruned > 0:
                    logger.info("Pruned %d invalid recent paths", pruned)
                return self._prefs
            except Exception as e:
                logger.error("Failed to parse stored prefs: %s", e)

        # Fallback: try loading from file
        self._prefs = self._load_from_file() or UserPreferences()
        if self._prefs:
            self._prefs.prune_invalid_roots()
        return self._prefs

    def save_to_app_storage(self, storage: dict[str, Any]):
        """Save preferences to NiceGUI's app.storage.user"""
        if self._prefs:
            storage[self.STORAGE_KEY] = self._prefs.model_dump(mode="json")
            self._save_to_file()

    def clear_all(self, storage: dict[str, Any]):
        """Nuclear option: clear all prefs from both storages"""
        self._prefs = UserPreferences()
        storage[self.STORAGE_KEY] = {}
        if self._file_path.exists():
            self._file_path.unlink()
        logger.info("All preferences cleared")

    def _load_from_file(self) -> UserPreferences | None:
        """Load from ~/.crboost/prefs.json"""
        if not self._file_path.exists():
            return None
        try:
            with open(self._file_path) as f:
                data = json.load(f)
            return UserPreferences(**data)
        except Exception as e:
            logger.error("Failed to load from file: %s", e)
            return None

    def _save_to_file(self):
        """Save to ~/.crboost/prefs.json"""
        if not self._prefs:
            return
        try:
            self._ensure_crboost_dir()
            with open(self._file_path, "w") as f:
                json.dump(self._prefs.model_dump(mode="json"), f, indent=2, default=str)
        except Exception as e:
            logger.error("Failed to save to file: %s", e)

    @property
    def prefs(self) -> UserPreferences:
        if self._prefs is None:
            self._prefs = UserPreferences()
        return self._prefs

    def update_fields(
        self,
        project_base_path: str | None = None,
        movies_glob: str | None = None,
        mdocs_glob: str | None = None,
    ):
        """Update basic preference fields (NOT recent_roots)"""
        if project_base_path is not None:
            self._prefs.project_base_path = project_base_path
        if movies_glob is not None:
            self._prefs.movies_glob = movies_glob
        if mdocs_glob is not None:
            self._prefs.mdocs_glob = mdocs_glob


_prefs_service: UserPrefsService | None = None


def get_prefs_service() -> UserPrefsService:
    global _prefs_service
    if _prefs_service is None:
        _prefs_service = UserPrefsService()
    return _prefs_service
