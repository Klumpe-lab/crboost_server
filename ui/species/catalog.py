"""Species-catalog dialogs (roadmap 12-S3) — the only UI the lab catalog has.

Two actions, both snapshots and both explicit: **import** instantiates a catalog species into
this project (rail → "From catalog"), **publish** copies this project's species up as a new
catalog version (Overview → "Publish to catalog"). Neither renders at all when
`species_catalog_root` is unset — `catalog.is_enabled()` is the single check, and "off" is a
normal state, never an error banner.

Disk work (the catalog lives on a shared filesystem, so every read can be a Lustre round trip)
runs through `run.io_bound`; both handlers are SingleFlight-guarded because they open dialogs
from buttons that a rev-gated refresh can replace mid-click.
"""

from __future__ import annotations

import logging
from pathlib import Path

from nicegui import run, ui

from services.particles import catalog
from services.project_state import get_project_state_for
from ui.components.reactive import SingleFlight
from ui.particles.list_actions import dialog_host

logger = logging.getLogger(__name__)

_flight = SingleFlight()
_HINT = "text-[10px] text-gray-400"
_MONO = "font-family: ui-monospace, monospace; font-size: 10px;"


def _entry_line(e: catalog.CatalogEntry) -> str:
    bits = [f"v{e.latest_version}", f"{e.n_templates} tpl", f"{e.n_masks} mask"]
    if e.diameter_ang:
        bits.append(f"Ø {e.diameter_ang:g} Å")
    if e.symmetry and e.symmetry != "C1":
        bits.append(e.symmetry)
    if e.published_by:
        bits.append(f"by {e.published_by}")
    return " · ".join(bits)


async def import_from_catalog(backend, project_path: Path, *, on_done=None) -> None:
    """Pick a catalog species and instantiate it here. Returns after the import is
    persisted; `on_done(species_id)` lets the caller select the new species."""
    async with _flight("catalog_import") as acquired:
        if not acquired:
            return
        try:
            entries = await run.io_bound(catalog.list_catalog)
        except OSError as e:
            # A shared FS that is down is worth saying out loud — an empty picker would
            # read as "the catalog is empty", which is a different and wrong story.
            ui.notify(f"Could not read the species catalog: {e}", type="negative", timeout=8000)
            return
        if not entries:
            ui.notify(
                f"The species catalog at {catalog.catalog_root()} has nothing published yet — "
                "publish a species from its Overview tab first.",
                type="info",
                timeout=7000,
            )
            return

        chosen: dict[str, str] = {}
        with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-[34rem] max-w-full gap-2"):
            ui.label("Import a species from the lab catalog").classes("text-sm font-bold")
            ui.label(
                "The definition and its template / mask files are COPIED into this project. "
                "Nothing syncs afterwards — a later catalog version does not change what you import now."
            ).classes(_HINT)
            with ui.element("div").classes("w-full").style("max-height: 20rem; overflow-y: auto;"):
                for e in entries:
                    row = ui.element("div").classes(
                        "w-full flex items-center gap-2 px-2 py-1 cursor-pointer hover:bg-slate-50"
                    )
                    row.on("click", lambda _e, cid=e.catalog_id: (chosen.update(id=cid), dlg.submit(True)))
                    with row:
                        ui.label(e.name).classes("text-[12px] font-medium text-slate-700")
                        ui.space()
                        ui.label(_entry_line(e)).style(_MONO).classes("text-slate-500").tooltip(
                            f"catalog id: {e.catalog_id}"
                        )
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=lambda: dlg.submit(None)).props("flat dense no-caps")
        go = await dlg
        dlg.delete()
        if not go or not chosen.get("id"):
            return

        state = get_project_state_for(project_path)
        try:
            species = await run.io_bound(catalog.import_species, state, project_path, chosen["id"])
        except (OSError, ValueError) as e:
            logger.exception("catalog import of %s failed", chosen.get("id"))
            ui.notify(f"Import failed: {e}", type="negative", timeout=8000)
            return
        await backend.save_project(project_path, force=True)
        ui.notify(f"Imported '{species.name}' from the catalog", type="positive")
        if on_done:
            on_done(species.id)


async def publish_to_catalog(backend, project_path: Path, species_id: str, *, on_done=None) -> None:
    """Publish this project's species as a NEW catalog version, after showing exactly what
    will be copied and which version number it becomes."""
    async with _flight(f"catalog_publish:{species_id}") as acquired:
        if not acquired:
            return
        state = get_project_state_for(project_path)
        sp = state.get_species(species_id)
        if sp is None:
            ui.notify("That species is no longer registered.", type="warning")
            return
        missing = [
            p
            for p in [t.template_path for t in sp.templates] + [m.mask_path for m in sp.masks]
            if not Path(p).is_file()
        ]

        with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[32rem] max-w-full gap-2"):
            next_v = (sp.catalog_version + 1) if sp.catalog_version else 1
            ui.label(f"Publish '{sp.name}' to the lab catalog as v{next_v}?").classes("text-sm font-bold")
            ui.label(
                "Copied: name, colour, diameter, symmetry, notes, and every registered template "
                "and mask file. NOT copied: picks, keep/drop filters, merges, extractions — those "
                "stay in this project."
            ).classes(_HINT)
            ui.label(f"{len(sp.templates)} template(s) · {len(sp.masks)} mask(s)").classes("text-[11px] text-slate-600")
            if sp.catalog_id:
                ui.label(
                    f"This species already came from / went to catalog '{sp.catalog_id}'. Publishing adds "
                    f"v{next_v} beside the existing versions — nothing published before is touched."
                ).classes("text-[10px] text-slate-500")
            if missing:
                ui.label(
                    f"{len(missing)} registered file(s) are missing on disk — publishing is refused until "
                    "they are back, because the catalog entry could not be imported anywhere:"
                ).classes("text-[10px] text-red-700")
                for p in missing[:6]:
                    ui.label(f"• {p}").style(_MONO).classes("text-red-700")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
                btn = ui.button("Publish", icon="publish", on_click=lambda: confirm.submit(True)).props(
                    "dense no-caps unelevated color=indigo"
                )
                if missing:
                    btn.disable()
        go = await confirm
        confirm.delete()
        if not go:
            return

        try:
            record = await run.io_bound(catalog.publish_species, state, project_path, species_id)
        except (OSError, ValueError) as e:
            logger.exception("catalog publish of %s failed", species_id)
            ui.notify(f"Publish failed: {e}", type="negative", timeout=8000)
            return
        state.mark_dirty()
        await backend.save_project(project_path, force=True)
        ui.notify(f"Published '{record.name}' as {record.catalog_id} v{record.version}", type="positive")
        if on_done:
            on_done()
