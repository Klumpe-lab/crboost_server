"""Pick-list administration (roadmap 11-S2): delete ONE workbench list, headless.

The counterpart of ``species_admin.delete_species`` at list granularity, for the Species
page's Picks tab. ``pick_list_files`` names what a delete removes so the confirm dialog
can list it; ``delete_pick_list`` removes the files, unregisters the ``PickList``
(dirty + rev) and persists by explicit path. Failures are collected into the result, never
fatal — the registry entry goes even when a file is stuck.

A ``manual`` list is re-derived by the ``CurationWatcher`` from the user's ``.coords`` save
in the tomogram's curation dir (after a restart the ``_seen`` set is empty), so deleting it
means deleting that save too — it is listed, and the archived ``imports/`` copies are kept
for provenance. Since roadmap 10-S2 the match is by file STEM, so deleting one hand-picked
list leaves the other lists saved in the same session alone. The authoritative choice is NOT
rewritten: a dangling choice surfaces through the gate ("dangling choice") and the Picks
tab's radio, instead of silently falling back to ``auto``.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from services.models_base import PickListType
from services.particles import picks_filter
from services.particles.ingest import manual_slug_for
from services.particles.list_ref import extract_pick_list_instance_id
from services.project_state import MERGED_DIR_NAME, PickList, get_project_state_for, get_state_service
from services.result import err, ok
from services.visualization import artiax_bridge

logger = logging.getLogger(__name__)


def pick_list_files(pl: PickList) -> dict[str, list[Path]]:
    """What deleting ``pl`` removes, by kind: ``stars`` (its star + ``<stem>_filtered.star``
    when present), ``dirs`` (the per-list extraction output) and, for a ``manual`` list,
    ``coords`` (the ONE user save the watcher would re-register this list from). Only paths
    that exist are listed.

    Both narrowed by roadmap 10-S2, which made a manual list per saved ``.coords``:
    ``coords`` matches this list's own source file by stem — deleting one list must not
    take its neighbours' saves with it — and the extraction dir is taken from the RECORDED
    ``extracted_path`` when there is one, since a list migrated off the old shared
    ``manual`` slug has an output dir that no longer matches its slug.

    An AGGREGATE list is its whole ``MergedSources/<name>/`` directory, not its
    ``particles.star``: the merge wrote four more files beside it (tomograms, optimisation
    set, provenance, summary), and an ``optimisation_set.star`` left behind still reads as a
    live merged source to everything that probes that path."""
    out: dict[str, list[Path]] = {"stars": [], "dirs": [], "coords": []}
    if not pl.path:
        return out
    star = Path(pl.path)
    for p in (star, picks_filter.filtered_list_path(star)):
        if p.exists():
            out["stars"].append(p)
    out_dir = Path(pl.extracted_path).parent if pl.extracted_path else star.parent / pl.slug
    if out_dir.is_dir():
        out["dirs"].append(out_dir)
    if pl.list_type == PickListType.MERGED and star.parent.parent.name == MERGED_DIR_NAME and star.parent.is_dir():
        out["dirs"].append(star.parent)
    if pl.list_type == PickListType.MANUAL:
        out["coords"] = [c for c in artiax_bridge.user_coords_saves(star.parent) if manual_slug_for(c) == pl.slug]
    return out


async def delete_pick_list(project_path: Path, species_id: str, tomo_name: str, slug: str) -> dict:
    """Remove the list's files (``pick_list_files``), unregister it and persist.
    ``ok(deleted_files=n, errors=[text, ...])`` (errors non-empty = partial, the registry
    entry IS gone); ``err(...)`` only when no such list is registered."""
    project_path = Path(project_path)
    state = get_project_state_for(project_path)
    pl = state.get_pick_list(slug, species_id, tomo_name)
    if pl is None:
        return err(f"no registered pick list '{slug}' for {species_id}/{tomo_name}")

    files = pick_list_files(pl)
    errors: list[str] = []
    deleted = 0
    for p in files["stars"] + files["coords"]:
        try:
            p.unlink()
            deleted += 1
        except OSError as e:
            logger.warning("Could not remove %s: %s", p, e)
            errors.append(f"Could not remove {p.name}: {e}")
    for d in files["dirs"]:
        try:
            shutil.rmtree(d)
            deleted += 1
        except OSError as e:
            logger.warning("Could not remove %s: %s", d, e)
            errors.append(f"Could not remove {d.name}/: {e}")

    # The per-list extraction instance describes THIS list (roadmap 07) and its out dir has
    # just gone with `files["dirs"]`. It has to go too: a `manual` list is re-minted under the
    # same slug by the curation watcher on the next save, and would otherwise inherit the
    # deleted list's status and failure text until something resubmits it.
    # PRECONDITION the caller owns: any in-flight extraction of this list is already cancelled
    # (`backend.cancel_pick_list_extraction`). Popping the instance discards the only record of
    # its SLURM id, and the job would re-create the directory just deleted — so a delete that
    # skips the cancel leaves an orphan nothing in the project can stop or explain. Kept out of
    # here rather than done here: this module is UI-free and holds no SlurmService.
    state.jobs.pop(extract_pick_list_instance_id(species_id, tomo_name, slug), None)
    state.remove_pick_list(slug, species_id, tomo_name)
    # Awaited, not fire-and-forget: a create_task here could be GC'd before it runs and
    # leave the deleted list back on disk after a reload.
    await get_state_service().save_project(project_path=project_path, force=True)
    return ok(deleted_files=deleted, errors=errors)
