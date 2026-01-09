# services/user_prefs_service.py
"""
User preferences persistence - wraps NiceGUI's app.storage.user
and optionally syncs to ~/.crboost/ for server-side access.
"""

from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

MAX_RECENT_ROOTS = 20


class RecentProjectRoot(BaseModel):
    """A recently used project root directory"""
    path: str
    last_used: datetime = Field(default_factory=datetime.now)
    label: Optional[str] = None


class UserPreferences(BaseModel):
    """Persisted user preferences"""
    project_base_path: str = ""
    movies_glob: str = ""
    mdocs_glob: str = ""
    recent_project_roots: List[RecentProjectRoot] = Field(default_factory=list)
    
    def add_recent_root(self, path: str, label: Optional[str] = None) -> bool:
        """
        Add a project root to MRU list.
        Returns True if added, False if rejected.
        ONLY call this after validation (path exists, is dir, contains projects).
        """
        if not path or not path.strip():
            return False
        
        try:
            resolved = str(Path(path).resolve())
        except Exception:
            return False
        
        p = Path(resolved)
        if not p.is_absolute() or not p.exists() or not p.is_dir():
            return False
        
        # Remove existing entry if present
        self.recent_project_roots = [
            r for r in self.recent_project_roots if r.path != resolved
        ]
        
        # Add to front
        self.recent_project_roots.insert(0, RecentProjectRoot(
            path=resolved,
            last_used=datetime.now(),
            label=label
        ))
        
        # Trim to max
        self.recent_project_roots = self.recent_project_roots[:MAX_RECENT_ROOTS]
        return True
    
    def remove_recent_root(self, path: str):
        """Remove a specific path from recent roots"""
        self.recent_project_roots = [
            r for r in self.recent_project_roots if r.path != path
        ]
    
    def clear_recent_roots(self):
        """Clear all recent roots"""
        self.recent_project_roots = []
    
    def prune_invalid_roots(self) -> int:
        """Remove roots that no longer exist. Returns count of pruned."""
        before = len(self.recent_project_roots)
        self.recent_project_roots = [
            r for r in self.recent_project_roots 
            if Path(r.path).exists() and Path(r.path).is_dir()
        ]
        return before - len(self.recent_project_roots)


class UserPrefsService:
    """
    Manages user preferences with dual storage:
    - Primary: NiceGUI's app.storage.user (browser-based)
    - Secondary: ~/.crboost/prefs.json (server-side)
    """
    
    STORAGE_KEY = "crboost_user_prefs"
    
    def __init__(self):
        self._prefs: Optional[UserPreferences] = None
        self._file_path = Path.home() / ".crboost" / "prefs.json"
    
    def _ensure_crboost_dir(self):
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_from_app_storage(self, storage: Dict[str, Any]) -> UserPreferences:
        """Load preferences from NiceGUI's app.storage.user"""
        raw = storage.get(self.STORAGE_KEY)
        if raw:
            try:
                self._prefs = UserPreferences(**raw)
                # Auto-prune invalid paths on load
                pruned = self._prefs.prune_invalid_roots()
                if pruned > 0:
                    print(f"[PREFS] Pruned {pruned} invalid recent roots")
                return self._prefs
            except Exception as e:
                print(f"[PREFS] Failed to parse stored prefs: {e}")
        
        # Fallback: try loading from file
        self._prefs = self._load_from_file() or UserPreferences()
        if self._prefs:
            self._prefs.prune_invalid_roots()
        return self._prefs
    
    def save_to_app_storage(self, storage: Dict[str, Any]):
        """Save preferences to NiceGUI's app.storage.user"""
        if self._prefs:
            storage[self.STORAGE_KEY] = self._prefs.model_dump(mode="json")
            self._save_to_file()
    
    def clear_all(self, storage: Dict[str, Any]):
        """Nuclear option: clear all prefs from both storages"""
        self._prefs = UserPreferences()
        storage[self.STORAGE_KEY] = {}
        if self._file_path.exists():
            self._file_path.unlink()
        print("[PREFS] All preferences cleared")
    
    def _load_from_file(self) -> Optional[UserPreferences]:
        """Load from ~/.crboost/prefs.json"""
        if not self._file_path.exists():
            return None
        try:
            with open(self._file_path) as f:
                data = json.load(f)
            return UserPreferences(**data)
        except Exception as e:
            print(f"[PREFS] Failed to load from file: {e}")
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
            print(f"[PREFS] Failed to save to file: {e}")
    
    @property
    def prefs(self) -> UserPreferences:
        if self._prefs is None:
            self._prefs = UserPreferences()
        return self._prefs
    
    def update_fields(
        self,
        project_base_path: Optional[str] = None,
        movies_glob: Optional[str] = None,
        mdocs_glob: Optional[str] = None,
    ):
        """Update basic preference fields (NOT recent_roots)"""
        if project_base_path is not None:
            self._prefs.project_base_path = project_base_path
        if movies_glob is not None:
            self._prefs.movies_glob = movies_glob
        if mdocs_glob is not None:
            self._prefs.mdocs_glob = mdocs_glob


_prefs_service: Optional[UserPrefsService] = None


def get_prefs_service() -> UserPrefsService:
    global _prefs_service
    if _prefs_service is None:
        _prefs_service = UserPrefsService()
    return _prefs_service