"""Pick-list ingest: register what the curation / import machinery wrote on disk as
``PickList`` entries on ``ProjectState``.

Pure state mutation, UI-free — shared by the species page's explicit "Import picks
from path…" click and the server-side curation watcher. Callers persist: the UI through
``backend.save_project(project_path, force=True)``, server loops through
``StateService.save_project(project_path=...)`` (no client context there).

One list per source file. A hand-picked list's slug is derived from its ``.coords`` file
stem, so the file is the identity: a new name is a new list, and re-saving under the same
name updates that list in place. Older projects carry lists with the literal slug
``"manual"`` (one per species and tomogram); ``load_project_state`` migrates them.

The seeded default. The advertised contract is one list per (species, tomogram): *Curate
picks* pre-creates ``<species_id>__<tomo>__picks.coords`` empty and registers it here as a
0-pick row — "pick into it in ArtiaX". Its slug is ``manual__<that stem>`` like any other
save, so the watcher's re-ingest of the user's save into the same file is the ordinary
same-slug upsert. It is labelled by its file stem like every other manual list, because
that is the name the user has to find in ArtiaX's save dialog; ``relabel_seed_rows`` fixes
older rows labelled ``picks``.
"""

from __future__ import annotations

from pathlib import Path

from services.models_base import PickListType, PickSourceKind
from services.particles.list_ref import fs_slug
from services.project_state import PickList, ProjectState
from services.visualization.artiax_bridge import default_seed_name

MANUAL_PREFIX = "manual__"
LEGACY_MANUAL_SLUG = "manual"  # slug of every hand-picked list in older projects
_ALIASED_SEED_LABEL = "picks"  # label of seeded rows in older projects; relabelled at load


def manual_slug_for(coords_path: Path | str) -> str:
    """``manual__<stem>`` for a ``.coords`` (or any source file). Prefixed rather than
    bare so the slug namespace stays readable next to ``auto`` / ``merged__<name>``, and
    filesystem-safe because the slug names files (``<slug>.star``, the per-list extraction
    out dir) and is interpolated into the extraction instance id."""
    return MANUAL_PREFIX + fs_slug(Path(coords_path).stem)


def default_slug_for(species_id: str, tomo_name: str) -> str:
    """The slug of the seeded default list for one (species, tomogram) — the same
    ``manual__<stem>`` any save of that file would get (``fs_slug`` is the identity on an
    already ``_safe_slug``-ed ASCII stem), so seed and save are one list."""
    return manual_slug_for(default_seed_name(species_id, tomo_name))


def is_default_slug(slug: str, species_id: str, tomo_name: str) -> bool:
    return slug == default_slug_for(species_id, tomo_name)


def register_manual_pick_list(state: ProjectState, result: dict, species_id: str, tomo_name: str) -> PickList:
    """Upsert the pick list for ONE ``.coords`` from an ``import_curation_picks`` result
    (``{count, slug, out_star, coords_source, created_by, ...}``). The slug is that file's
    (``manual__<stem>``), so two lists saved in one session land as two lists and a
    re-save of the same file replaces only itself. The label is the file stem — the
    seeded default included — not a fixed "Manual (ArtiaX)".

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
    slug = result.get("slug") or manual_slug_for(src or out_star)
    pick_list = PickList(
        slug=slug,
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


def migrate_legacy_manual_slugs(state: ProjectState) -> list[tuple[str, str, str, str]]:
    """Rename legacy ``"manual"`` lists to ``manual__<stem>``, re-keying everything
    that referenced them by slug. Returns the ``(species, tomo, old, new)`` it changed
    (empty on an already-migrated project), so the loader can log what moved.

    Called from ``load_project_state`` — before anything reads the registry, so no
    consumer ever sees the two schemes at once. Three things key on the slug and all three
    are rewritten here: the list itself, the per-list extraction job instance, and any
    downstream job whose input is overridden onto this list's synthetic producer
    (``pick_list__<species>__<tomo>__<slug>``) — miss that last one and the override
    silently reads as "the pick list behind this no longer exists".
    Files are not renamed: ``PickList.path`` and ``extracted_path`` are
    absolute and stay valid, and renaming a star out from under a recorded extraction
    output would break the very link that proves it current.

    The stem comes from what ingest already recorded — ``source_ref`` (the .coords stem
    for a session save, the imported path for an external file), else ``label``. A list
    carrying neither becomes ``manual__legacy``: no invented filename, and the user sees
    a name that says what it is.
    """
    from services.particles.list_ref import extract_pick_list_instance_id
    from services.path_resolution_service import PICK_LIST_PRODUCER_PREFIX

    def _producer(species_id: str, tomo: str, slug: str) -> str:
        return f"{PICK_LIST_PRODUCER_PREFIX}{species_id}__{tomo}__{slug}"

    changed: list[tuple[str, str, str, str]] = []
    for pl in state.pick_lists:
        if pl.slug != LEGACY_MANUAL_SLUG:
            continue
        ref = pl.source_ref or ""
        stem = Path(ref).stem if ("/" in ref or ref.endswith(".coords")) else ref
        new = manual_slug_for(stem or pl.label or "legacy")
        old_iid = extract_pick_list_instance_id(pl.species_id, pl.tomo_name, LEGACY_MANUAL_SLUG)
        job = state.jobs.pop(old_iid, None)
        if job is not None:
            state.jobs[extract_pick_list_instance_id(pl.species_id, pl.tomo_name, new)] = job
        old_producer = _producer(pl.species_id, pl.tomo_name, LEGACY_MANUAL_SLUG)
        new_producer = _producer(pl.species_id, pl.tomo_name, new)
        for jm in (state.jobs or {}).values():
            overrides = getattr(jm, "source_overrides", None) or {}
            for slot, value in list(overrides.items()):
                if old_producer in str(value):
                    overrides[slot] = str(value).replace(old_producer, new_producer)
        pl.slug = new
        changed.append((pl.species_id, pl.tomo_name, LEGACY_MANUAL_SLUG, new))
    return changed


def relabel_seed_rows(state: ProjectState) -> list[tuple[str, str, str]]:
    """Older rows where the seeded default list is labelled ``picks`` get their file stem
    back as the label — what every other manual list
    shows, and the name the user must find in ArtiaX's save dialog. Called from
    ``load_project_state`` beside ``migrate_legacy_manual_slugs``; returns the
    ``(species, tomo, stem)`` it changed so the loader can log and mark dirty. A session
    save re-ingests under the stem anyway; this only closes the gap for rows that never
    got one."""
    changed: list[tuple[str, str, str]] = []
    for pl in state.pick_lists:
        stem = pl.source_ref or ""
        if pl.list_type == PickListType.MANUAL and pl.label == _ALIASED_SEED_LABEL and stem and "/" not in stem:
            pl.label = stem
            changed.append((pl.species_id, pl.tomo_name, stem))
    return changed
