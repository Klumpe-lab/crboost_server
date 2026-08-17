"""Pick-list ACTIONS shared by the Journey and the Species page (roadmap 11-S1).

Carved out of ``ui/tomo_dashboard_dialog.py`` so the Species page (manage & act) and the
Journey (look & curate) drive the same code for extract / merge / dedup / ⚡ load /
import: each helper takes the ``backend`` and a ``ListRef`` (``services/particles/list_ref``)
instead of the Journey's ``sp`` / ``lst`` render dicts, resolves ``ProjectState`` by the
ref's EXPLICIT path (never the tab accessor — the extraction wait runs in a BackgroundTask
with no client context, W2), and reports back through ``on_done`` (the Journey passes its
``request_refresh``; the Species page passes a no-op — the registry rev drives its views).
One module-level ``SingleFlight`` guards every handler: the buttons that fire them live in
poll-refreshed containers, so several clicks can land before one does.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path

from nicegui import context, ui

from services.aggregation_authoritative import extraction_params_for_species
from services.models_base import ListExtractionState, PickListType, PickSourceKind
from services.particles import picks_filter
from services.particles.ingest import register_manual_pick_list
from services.particles.list_ref import ListRef, fs_slug
from services.project_state import ExtractionParams, PickList, get_project_state_for
from services.visualization.tomo_geometry import geometry_for_ts
from ui.background_task import BackgroundTask
from ui.components.reactive import SingleFlight
from ui.curation_session_dialog import open_curation_control_center

logger = logging.getLogger(__name__)

OnDone = Callable[[], None]

_flight = SingleFlight()

# Extraction-state badge shown on each workbench list row (Slice A surfaces it; the
# per-list Extract action that flips it is Slice C). Auto lists show none.
_EXTRACTION_BADGE = {
    ListExtractionState.EXTRACTED: ("✓ extracted", "cb-badge-ok"),
    ListExtractionState.NOT_EXTRACTED: ("○ not extracted", "cb-badge-todo"),
    ListExtractionState.STALE: ("⚠ stale · re-extract", "cb-badge-stale"),
}


def extraction_badge(state: ListExtractionState) -> tuple[str, str]:
    """(text, css class) of the derived per-list extraction badge; ("", "") for none."""
    return _EXTRACTION_BADGE.get(state, ("", ""))


def _no_backend() -> None:
    ui.notify("Backend unavailable.", type="negative")


# ── Extraction ────────────────────────────────────────────────────────────────


async def extract_list(backend, ref: ListRef, *, on_done: OnDone) -> None:
    """Submit + track a per-list subtomo extraction (Slice C). Resolves three things —
    the schema source (the species' candidate optset, else the tomogram's tomograms.star
    for a de-novo species), this list's curated star, and the extraction geometry — then
    fires ``backend.extract_pick_list_and_wait`` (submit + await the out dir + record
    ``PickList.mark_extracted`` + persist). A species with no committed geometry gets the
    required dialog instead of a guessed box size (D-3). SingleFlight-guarded; the wait
    runs in a BackgroundTask (the backend persists by explicit ``project_path``, W2)."""
    async with _flight(f"extract:{ref.species_id}:{ref.tomo_name}:{ref.slug}") as acquired:
        if not acquired:
            return
        if backend is None:
            _no_backend()
            return
        star = ref.star_path
        if not star:
            ui.notify(f"'{ref.label}' has no backing star to extract.", type="warning")
            return
        # Prefer the curated subset so extraction consumes the KEPT picks, not all of them.
        filtered = picks_filter.filtered_list_path(Path(star))
        list_star = str(filtered) if filtered.exists() else str(star)

        # Schema source: mirror the species' candidates.star when it has a
        # candidate-extract job; otherwise synthesize from the tomogram's own star
        # (a de-novo species never had a TM/CE job to mirror).
        state = get_project_state_for(ref.project_path)
        candidate_optset = None
        tomograms_star = None
        if ref.ce_job_dir is not None and (ref.ce_job_dir / "optimisation_set.star").exists():
            candidate_optset = ref.ce_job_dir / "optimisation_set.star"
        else:
            geom = geometry_for_ts(state, ref.project_path, ref.tomo_name)
            if geom is None:
                ui.notify(
                    "No candidate optimisation set and no tomograms.star for this tomogram — "
                    "nothing to build an extraction input from.",
                    type="negative",
                    timeout=6000,
                )
                return
            tomograms_star = Path(geom.tomograms_star)

        subtomo_jm = state.jobs.get(ref.subtomo_iid) if ref.subtomo_iid else None
        params = extraction_params_for_species(state, ref.species_id, subtomo_jm)
        if params is None:
            # D-3: no committed geometry anywhere. ASK — never fall back to the old
            # silent 384/1.0/224, which cut wrong-but-plausible subtomograms.
            prompt_extraction_geometry(backend, ref, candidate_optset, tomograms_star, list_star, on_done=on_done)
            return

        _submit_list_extraction(backend, ref, candidate_optset, tomograms_star, list_star, params, on_done)


def _submit_list_extraction(
    backend,
    ref: ListRef,
    candidate_optset: Path | None,
    tomograms_star: Path | None,
    list_star: str,
    params: dict,
    on_done: OnDone,
) -> None:
    """Fire the per-list extraction as a tracked BackgroundTask. Split out of
    ``extract_list`` so the geometry dialog can submit the same way once the user
    commits box/bin/crop."""

    async def _run(progress_cb):
        progress_cb(0, 0, "extracting subtomograms…")
        return await backend.extract_pick_list_and_wait(
            ref.project_path,
            candidate_optset,
            Path(list_star),
            ref.tomo_name,
            ref.species_id,
            ref.slug,
            tomograms_star=tomograms_star,
            **params,
        )

    BackgroundTask(
        title=f"Extract · {ref.label}",
        subtitle=ref.tomo_name,
        project_path=str(ref.project_path),
        dedup_key=f"extract:{ref.species_id}:{ref.tomo_name}:{ref.slug}",
    ).submit(_run, on_complete=lambda _t: on_done(), show_start_toast=True)
    ui.notify(f"Extraction submitted for '{ref.label}' — tracking in the task tray.", type="info")


def prompt_extraction_geometry(
    backend,
    ref: ListRef,
    candidate_optset: Path | None,
    tomograms_star: Path | None,
    list_star: str,
    *,
    on_done: OnDone,
) -> None:
    """Ask for box / binning / crop before a first extraction, and persist the answer on
    the species (D-3).

    Reached only when NOTHING has committed a geometry: no SUBTOMO_EXTRACTION job model
    and no ``species.extraction_params``. The fields open EMPTY on purpose — prefilling
    them with the old 384/1.0/224 would just relabel a silent default as a confirmed one.
    """
    with ui.dialog() as dialog, ui.card().classes("w-[26rem] max-w-full gap-2"):
        ui.label("Extraction geometry").classes("text-base font-bold")
        ui.label(
            f"'{ref.species_label or ref.species_id}' has no subtomo-extraction job to inherit box/binning/crop "
            "from. Set them once — they are saved on the species and reused for every later extraction."
        ).classes("text-xs text-gray-600")
        box_in = ui.number("box size (px, unbinned)", min=16, step=2).props("dense outlined").classes("w-full")
        bin_in = ui.number("binning", min=0.1, step=0.5).props("dense outlined").classes("w-full")
        crop_in = ui.number("crop size (px)", min=16, step=2).props("dense outlined").classes("w-full")

        async def _commit() -> None:
            box, binning, crop = box_in.value, bin_in.value, crop_in.value
            if not box or not binning or not crop:
                ui.notify("Box size, binning and crop are all required.", type="warning")
                return
            if backend is None:
                _no_backend()
                return
            state = get_project_state_for(ref.project_path)
            geometry = ExtractionParams(box_size=int(box), binning=float(binning), crop_size=int(crop))

            def _apply(species) -> None:
                species.extraction_params = geometry

            if not state.mutate_species(ref.species_id, _apply):
                ui.notify("Species not found — reload the project.", type="negative")
                return
            # Await the write: the extraction below runs in a BackgroundTask with no
            # client context, and a fire-and-forget save can lose the geometry the
            # user just committed.
            await backend.save_project(ref.project_path, force=True)
            dialog.close()
            subtomo_jm = state.jobs.get(ref.subtomo_iid) if ref.subtomo_iid else None
            params = extraction_params_for_species(state, ref.species_id, subtomo_jm)
            _submit_list_extraction(backend, ref, candidate_optset, tomograms_star, list_star, params, on_done)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button("Save & extract", icon="science", color="indigo", on_click=_commit).props("no-caps")
    dialog.open()


# ── Merge / dedup ─────────────────────────────────────────────────────────────


def merge_source_for(ref: ListRef) -> dict | None:
    """The star ``ref`` contributes to a merge — its KEPT subset (the user's keep/drop must
    not bleed dropped picks into it); see ``picks_filter.merge_source_for``."""
    return picks_filter.merge_source_for(
        ref.slug, [ref.merge_source_dict()], ce_job_dir=ref.ce_job_dir, subtomo_job_dir=ref.subtomo_job_dir
    )


async def merge_lists(backend, refs: list[ListRef], name: str) -> str | None:
    """Union 2+ lists of ONE (species, tomo) into a named ``merged`` list: writes the star
    (``backend.merge_pick_lists``), registers the ``PickList`` (dirty + rev) and persists by
    explicit path. Returns the new list's slug, or None when nothing was merged (the caller
    refreshes / selects). NAME → slug: re-using a name replaces that merge (upsert); a new
    name makes a distinct merged list (own slug → own star, chip, curation), so no clobber."""
    chosen = [s for s in (merge_source_for(r) for r in refs) if s]
    if len(chosen) < 2:
        ui.notify("Tick at least 2 lists to merge.", type="warning")
        return None
    if backend is None:
        _no_backend()
        return None
    ref = refs[0]
    raw_name = (name or "").strip() or "Merged"
    slug = f"merged__{fs_slug(raw_name)}"
    res = await backend.merge_pick_lists(
        ref.project_path,
        ref.species_id,
        ref.species_label,
        ref.tomo_name,
        [{"path": c["path"], "type": c["type"]} for c in chosen],
        out_slug=slug,
    )
    if not res.get("success"):
        ui.notify(f"Merge failed: {res.get('error')}", type="negative")
        return None
    get_project_state_for(ref.project_path).add_pick_list(
        PickList(
            slug=slug,
            label=raw_name,
            list_type=PickListType.MERGED,
            species_id=ref.species_id,
            tomo_name=ref.tomo_name,
            path=res["out_star"],
            count=int(res.get("count", 0)),
            parent_slugs=[c["slug"] for c in chosen],
            source_kind=PickSourceKind.MERGE.value,
            source_ref="+".join(c["slug"] for c in chosen),
            created_by=getattr(backend, "username", ""),
        )
    )
    # Persist by explicit project_path (not the client-context default) so the
    # merged list survives a restart even if this runs without a resolvable
    # client state — the same contract the manual-list persist proved out (P4).
    await backend.save_project(ref.project_path, force=True)
    ui.notify(f"Created '{raw_name}' — {res.get('count', 0)} picks from {len(chosen)} lists", type="positive")
    return slug


async def dedup_list(backend, ref: ListRef, radius_ang: float, *, on_done: OnDone) -> None:
    """Greedy radius-dedup of a merged list in place (manual kept over auto). The backend
    rewrites the star, updates the ``PickList`` count, persists and bumps the rev; the
    list reads STALE afterwards → re-extract."""
    if backend is None:
        _no_backend()
        return
    res = await backend.deduplicate_pick_list(ref.project_path, ref.species_id, ref.tomo_name, ref.slug, radius_ang)
    if not res.get("success"):
        ui.notify(f"Deduplicate failed: {res.get('error')}", type="negative")
        return
    ui.notify(f"Removed {res.get('n_removed', 0)} overlapping picks · {res.get('n_after', 0)} kept", type="positive")
    on_done()


# ── ArtiaX ────────────────────────────────────────────────────────────────────


async def curate_in_artiax(backend, ref: ListRef) -> None:
    """Per-tomo 'Curate in ArtiaX': export this (species, tomo)'s picks to a
    `.coords` + `.cxc`, then open the curation control center bound to this
    tomogram. The control center is status-first — it shows the live session's
    connection info + the load commands, or a Start button that preloads this
    tomogram. SingleFlight-guarded so repeated clicks prep only one bundle."""
    async with _flight(f"{ref.species_id}:{ref.tomo_name}") as acquired:
        if not acquired:
            return
        if backend is None:
            _no_backend()
            return
        if ref.tomograms_star is None:
            ui.notify(f"No tomograms.star resolved for {ref.tomo_name} — nothing to open in ArtiaX.", type="warning")
            return
        candidates_star, tomograms_star = ref.candidates_star, ref.tomograms_star
        ui.notify(f"Preparing ArtiaX bundle for {ref.tomo_name}…", type="info")
        bundle = await backend.prepare_curation_bundle(
            ref.project_path,
            candidates_star,
            tomograms_star,
            ref.tomo_name,
            ref.species_label or ref.species_id,
            species_id=ref.species_id,
        )
        if not bundle.get("success"):
            ui.notify(f"Could not prepare picks for {ref.tomo_name}: {bundle.get('error')}", type="negative")
            return
        bundle["tomo_name"] = ref.tomo_name
        bundle["candidates_star"] = str(candidates_star) if candidates_star else ""
        bundle["tomograms_star"] = str(tomograms_star)
        bundle["species_id"] = ref.species_id
        bundle["species_label"] = ref.species_label or ref.species_id
        await open_curation_control_center(backend, ref.project_path, bundle=bundle)


async def load_tomo_into_session(backend, ref: ListRef) -> None:
    """Per-tomo ⚡ 'Load into running session': swap the user's ALREADY-running
    ChimeraX/ArtiaX to THIS (species, tomo) over the REST channel — the reuse path
    that avoids relaunching a viewer per tomogram (the session is per-user, found
    across all projects). No live session → point the user at 'Curate in ArtiaX'."""
    async with _flight(f"loadinto:{ref.species_id}:{ref.tomo_name}") as acquired:
        if not acquired:
            return
        # This handler awaits a ~20 s load; during it the dashboard's periodic
        # main_area.clear() deletes the slot this coroutine was entered under, so a later
        # bare ui.notify dies with "parent element ... has been deleted". Capture the page
        # LAYOUT slot (never cleared) up front and route every notify through it; swallow
        # the residual race so a stale toast never surfaces a traceback.
        try:
            host = context.client.layout.default_slot
        except Exception:
            host = nullcontext()

        def _notify(msg: str, **kw) -> None:
            try:
                with host:
                    ui.notify(msg, **kw)
            except Exception:
                logger.info("load-into-session: dropped notify (slot gone): %s", msg)

        if backend is None:
            _notify("Backend unavailable.", type="negative")
            return
        if ref.tomograms_star is None:
            _notify(f"No tomograms.star resolved for {ref.tomo_name} — nothing to load.", type="warning")
            return
        active = await backend.find_active_curation_session_any()
        if not active:
            active = await backend.find_active_curation_session(ref.project_path)
        if not active or not active.get("rest_port"):
            _notify(
                "No running ChimeraX session yet — click ‘Curate in ArtiaX’ to start one, then load tomograms into it.",
                type="warning",
                timeout=6000,
            )
            return

        # Confirm — `close session` wipes unsaved manual picks. Layout-parented so
        # the 4 s dashboard refresh can't clear the dialog mid-interaction.
        with host:
            with ui.dialog().props("persistent") as confirm, ui.card().classes("w-[26rem] max-w-full gap-2"):
                ui.label("Load into running session?").classes("text-sm font-bold")
                ui.label(
                    f"Swap the running ArtiaX (on {active.get('node') or '?'}) to {ref.tomo_name} + its picks, "
                    "clearing what's open now. Any manual picks you haven't saved for the current tomogram would "
                    "be lost."
                ).classes("text-[12px] text-gray-600")
                save_cb = ui.checkbox("Save my current picks first", value=True).props("dense").classes("text-[12px]")
                ui.label("crboost saves your open lists to the current tomogram's folder before switching.").classes(
                    "text-[10px] text-gray-400"
                )
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                    ui.button("Load", color="indigo", on_click=lambda: confirm.submit(True)).props("dense no-caps")
        go = await confirm
        do_save = bool(save_cb.value) if go else False
        try:
            confirm.delete()
        except Exception:
            pass
        if not go:
            return

        candidates_star, tomograms_star = ref.candidates_star, ref.tomograms_star
        _notify(f"Loading {ref.tomo_name} into the running session…", type="info")
        res = await backend.load_into_session(
            active,
            ref.project_path,
            candidates_star,
            tomograms_star,
            ref.tomo_name,
            ref.species_label or ref.species_id,
            species_id=ref.species_id,
            save_first=do_save,
        )
        if res.get("success"):
            n = res.get("auto_count")
            _notify(
                f"Loaded {ref.tomo_name}{f' ({n} picks)' if n is not None else ''} into the running session.",
                type="positive",
            )
        else:
            _notify(f"Load failed: {res.get('error') or 'unknown error'}", type="negative", timeout=7000)


# ── Import ────────────────────────────────────────────────────────────────────


async def register_imported_picks(backend, ref: ListRef, result: dict, *, on_done: OnDone) -> None:
    """Explicit-import click path: upsert the ``manual`` PickList for this (species, tomo)
    from a ``backend.import_curation_picks`` result (``services.particles.ingest``, shared
    with the server-side watcher), persist AWAITED with force=True so the registry actually
    lands on disk (a fire-and-forget ``create_task(save_project())`` was getting GC'd before
    it ran, leaving ``pick_lists: []`` in project_params.json), toast, and ``on_done`` so
    the new diamond layer appears."""
    pl = register_manual_pick_list(get_project_state_for(ref.project_path), result, ref.species_id, ref.tomo_name)
    await backend.save_project(ref.project_path, force=True)
    src = Path(result.get("coords_source", "")).name
    ui.notify(
        f"Imported {pl.count} manual picks for {ref.tomo_name}" + (f" (from {src})" if src else ""),
        type="positive",
        timeout=3000,
    )
    on_done()


def import_picks_from_path(backend, ref: ListRef, *, on_done: OnDone) -> None:
    """Import a ``.coords`` by explicit path (ArtiaX's save dialog may default anywhere,
    and an external file has no curation dir at all): paste the full path → the same
    backend import → register the ``manual`` list. Also the fallback of the Journey's
    auto-discover import when no saved .coords was found."""
    if ref.tomograms_star is None:
        ui.notify(f"No tomograms.star resolved for {ref.tomo_name} — cannot map .coords into it.", type="warning")
        return
    tomograms_star = ref.tomograms_star
    with ui.dialog() as dialog, ui.card().classes("w-[34rem] max-w-full gap-2"):
        ui.label(f"Import ArtiaX picks — {ref.tomo_name}").classes("text-base font-bold")
        ui.label(
            "No saved .coords was found in this project's curation dirs. Paste the full path to the "
            ".coords you saved from ArtiaX (any filename)."
        ).classes("text-xs text-gray-600")
        path_in = ui.input("path to .coords").props("dense outlined").classes("w-full font-mono text-xs")

        async def _do_import():
            p = (path_in.value or "").strip()
            if not p:
                ui.notify("Enter a path", type="warning")
                return
            if backend is None:
                _no_backend()
                return
            result = await backend.import_curation_picks(
                ref.project_path,
                tomograms_star,
                ref.tomo_name,
                ref.species_label or ref.species_id,
                ref.species_id,
                coords_path=Path(p),
            )
            if not result.get("success"):
                ui.notify(f"Import failed: {result.get('error')}", type="negative", timeout=4000)
                return
            dialog.close()
            await register_imported_picks(backend, ref, result, on_done=on_done)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button("Import", icon="download", color="indigo", on_click=_do_import).props("no-caps")
    dialog.open()
