"""Pick-list ingest: register what the curation / import machinery wrote on disk as
``PickList`` entries on ``ProjectState`` (roadmap 09-S2).

Pure state mutation, UI-free — shared by the species page's explicit "Import picks
from path…" click and the server-side curation watcher. Callers persist: the UI through
``backend.save_project(project_path, force=True)``, server loops through
``StateService.save_project(project_path=...)`` (no client context there).
"""

from __future__ import annotations

from pathlib import Path

from services.models_base import PickListType, PickSourceKind
from services.project_state import PickList, ProjectState


def register_manual_pick_list(state: ProjectState, result: dict, species_id: str, tomo_name: str) -> PickList:
    """Upsert the ``manual`` PickList for (species, tomo) from an
    ``import_curation_picks`` result (``{count, out_star, coords_source, created_by,
    ...}``). One ``manual`` list per (species, tomo) — a re-import replaces it (the raw
    .coords stay archived per import for provenance). The label is the .coords stem
    the user chose in ArtiaX (P5), not a fixed "Manual (ArtiaX)".

    Provenance: ``source_kind`` is ``artiax`` when the .coords sits in the tomogram's
    own curation dir (a session save — found by the watcher, the explicit click or a
    path pick), else ``import`` (an external file); ``source_ref`` is the stem resp.
    the imported path. ``add_pick_list`` marks dirty + bumps ``registry_rev``; the
    caller persists.
    """
    src = Path(result.get("coords_source") or "")
    out_star = Path(result["out_star"])
    session_save = bool(src.name) and src.resolve().parent == out_star.resolve().parent
    kind = PickSourceKind.ARTIAX if session_save else PickSourceKind.IMPORT
    pick_list = PickList(
        slug="manual",
        label=src.stem or "Manual (ArtiaX)",
        list_type=PickListType.MANUAL,
        species_id=species_id,
        tomo_name=tomo_name,
        path=str(out_star),
        count=int(result.get("count", 0)),
        created_by=result.get("created_by", ""),
        source_kind=kind.value,
        source_ref=src.stem if session_save else (str(src) if src.name else ""),
    )
    state.add_pick_list(pick_list)
    return pick_list
