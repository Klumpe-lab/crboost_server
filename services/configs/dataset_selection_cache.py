# services/configs/dataset_selection_cache.py
"""
Persists per-dataset tilt-series selections to ~/.crboost/dataset_selections.json
so that selections survive page reloads, failed project creations, and server restarts.

Keyed by mdocs glob pattern (which uniquely identifies a dataset).
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from services.dataset_models import DatasetOverview

logger = logging.getLogger(__name__)

_CACHE_PATH = Path.home() / ".crboost" / "dataset_selections.json"
MAX_CACHED_DATASETS = 50


def _load_cache() -> Dict:
    if not _CACHE_PATH.exists():
        return {}
    try:
        with open(_CACHE_PATH) as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not load dataset selection cache: %s", e)
        return {}


def _save_cache(cache: Dict):
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        logger.warning("Could not save dataset selection cache: %s", e)


def save_selections(mdocs_glob: str, overview: DatasetOverview):
    """Persist the current selected/deselected state for every tilt-series in the dataset."""
    if not mdocs_glob or not overview.positions:
        return
    selections = {}
    for pos in overview.positions:
        for ts in pos.tilt_series:
            selections[ts.mdoc_filename] = ts.selected

    cache = _load_cache()
    cache[mdocs_glob] = {
        "saved_at": datetime.now().isoformat(),
        "source_directory": overview.source_directory,
        "selections": selections,
    }
    # Prune old entries
    if len(cache) > MAX_CACHED_DATASETS:
        entries = sorted(cache.items(), key=lambda kv: kv[1].get("saved_at", ""), reverse=True)
        cache = dict(entries[:MAX_CACHED_DATASETS])
    _save_cache(cache)
    logger.info("Saved selection for %s (%d tilt-series)", mdocs_glob, len(selections))


def apply_selections(mdocs_glob: str, overview: DatasetOverview) -> bool:
    """Apply previously saved selections to a freshly parsed DatasetOverview.

    Returns True if a saved selection was found and applied.
    """
    if not mdocs_glob:
        return False
    cache = _load_cache()
    entry = cache.get(mdocs_glob)
    if not entry:
        return False
    selections = entry.get("selections", {})
    if not selections:
        return False

    applied = 0
    for pos in overview.positions:
        any_selected = False
        for ts in pos.tilt_series:
            if ts.mdoc_filename in selections:
                ts.selected = selections[ts.mdoc_filename]
                applied += 1
            if ts.selected:
                any_selected = True
        pos.selected = any_selected

    if applied > 0:
        logger.info("Restored %d tilt-series selections for %s", applied, mdocs_glob)
        return True
    return False


def get_saved_timestamp(mdocs_glob: str) -> Optional[str]:
    """Return the ISO timestamp of the last saved selection for this dataset, or None."""
    if not mdocs_glob:
        return None
    cache = _load_cache()
    entry = cache.get(mdocs_glob)
    if entry:
        return entry.get("saved_at")
    return None
