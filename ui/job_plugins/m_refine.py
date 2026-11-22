"""M refine plugin: the default parameters, and above them a chip when the round's CTF switches will refine nothing.

M refines the CTF only of a species finer than 'CTF from' (--ctf_minresolution) and prints nothing when it skips it.
The resolution a round starts at is known before it runs: the earlier round's (its m_round.json) for a next round,
the Refine 3D's own (run_model.star beside its half maps) for a first round, which M's own measure, in its mask,
comes within a shell or so of. Read in a thread after the tab is built; drawn again after an edit.
"""

import asyncio
import json
from pathlib import Path

import starfile
from nicegui import ui

from services.models_base import JobType
from ui.components.reactive import Debounced, owned_timer
from ui.job_plugins import register_params_renderer
from ui.job_plugins.default_renderer import render_default_params_card

_CHIP_STYLE = "border-radius: var(--cb-r-md, 4px); padding: 3px 8px; margin-top: 4px; flex-wrap: nowrap;"


@register_params_renderer(JobType.M_REFINE)
def render_m_refine_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, **ctx):
    holder: dict = {}

    def note():
        holder["slot"] = ui.column().classes("w-full gap-0")
        _fill_note(holder["slot"], job_model)

    refresh = Debounced(lambda: _fill_note(holder["slot"], job_model) if "slot" in holder else None)

    def on_save():
        save_handler()
        refresh()

    return render_default_params_card(job_type, job_model, is_frozen, on_save, ui_mgr=ui_mgr, before=note, **ctx)


def start_resolution(job_model) -> tuple[float | None, str]:
    """The resolution the round's species starts at, and whose it is; (None, "") when not known yet. Disk only, for a
    thread."""
    paths = job_model.paths or {}
    if job_model.continue_from_m:
        raw = paths.get("input_m")
        if not raw or not Path(raw).is_file():
            return None, ""
        record = json.loads(Path(raw).read_text())
        value = record.get("resolution_after_ang")
        return (float(value), f"M's after round {record.get('round')}") if value else (None, "")
    raw = paths.get("input_half_map")
    model = Path(raw).parent / "run_model.star" if raw else None
    if model is None or not model.is_file():
        return None, ""
    general = starfile.read(model, always_dict=True).get("model_general")
    value = general.get("rlnCurrentResolution") if isinstance(general, dict) else None
    return (float(value), "the Refine 3D's") if value else (None, "")


def _fill_note(slot, job_model) -> None:
    async def _fill() -> None:
        resolution, whose = await asyncio.to_thread(start_resolution, job_model)
        if slot.is_deleted:
            return
        slot.clear()
        switches = job_model.ctf_switches()
        if resolution is None or not switches or resolution <= job_model.ctf_minresolution:
            return
        tooltip = (
            f"M refines the CTF (defocus, Cs, phase, Zernike terms) only of a species finer than 'CTF from' "
            f"({job_model.ctf_minresolution:g} Å), and says nothing when it skips it. This round starts at about "
            f"{resolution:.2f} Å ({whose}), so its CTF switches would do nothing. Raise 'CTF from' above that to "
            "refine the CTF."
        )
        with slot:
            with (
                ui.row()
                .classes("w-full items-center gap-2")
                .style(
                    "background: var(--cb-warn-bg, #fffbeb); border: 1px solid var(--cb-warn-line, #fde68a); "
                    f"{_CHIP_STYLE}"
                )
                .tooltip(tooltip)
            ):
                ui.icon("warning", size="14px").classes("text-amber-600")
                ui.label("CTF not refined").classes("text-[11px] font-semibold text-amber-800")
                ui.label(
                    f"the species starts at {resolution:.2f} Å, coarser than 'CTF from' "
                    f"{job_model.ctf_minresolution:g} Å"
                ).classes("text-[11px] text-amber-800")

    owned_timer(0.01, _fill, slot, once=True)
