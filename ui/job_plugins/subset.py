"""Subset plugin: which tilt series of the output it cuts go on, picked in a table with the
readouts the Journey has per tilt series, and the jobs that can follow it.

The table is one `ui.html`: a click on a row toggles that tilt series and repaints the table in
place. The cut star and the registry are read off the event loop, once per render. Once the
Subset has run it is read-only like any finished job; another choice is a Clone of it.
"""

from __future__ import annotations

import html
import logging
from dataclasses import dataclass
from pathlib import Path

from nicegui import run, ui

from services.io_slots import JobFileType
from services.jobs.spec import display_name
from services.models_base import JobStatus, JobType
from services.path_resolution_service import PathResolutionService, subset_cut
from services.subsets import cut_names, jobs_after
from ui.components.buttons import house_button
from ui.components.reactive import owned_timer
from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import CLR_HEADER, HELPER_STYLE, MONO, SANS
from ui.ui_state import get_instance_display_name

logger = logging.getLogger(__name__)

_RED = "#dc2626"
_AMBER = "#d97706"
_LINE_STYLE = f"{SANS} font-size: 10px; color: {CLR_HEADER};"
_TH = (
    f"{SANS} font-size: 9px; font-weight: 600; color: #94a3b8; text-align: left; padding: 2px 10px 3px 0; "
    "position: sticky; top: 0; background: #fff; white-space: nowrap;"
)
_TD = f"{MONO} font-size: 10px; color: #334155; padding: 1px 10px 1px 0; white-space: nowrap;"
# A click anywhere on a row sends its tilt series.
_ROW_CLICK_JS = "(event) => { const tr = event.target.closest('tr[data-ts]'); if (tr) emit({ts: tr.dataset.ts}); }"


@dataclass(frozen=True)
class _Row:
    name: str
    position: str
    n_used: int | None
    ctf_resolution: float | None
    ctf_from: str | None
    n_blank: int | None
    n_dim: int | None
    excluded: bool
    exclusion_reason: str | None


def _load_rows(star: Path, project_path: Path) -> list[_Row]:
    """The tilt series of the cut star, in its order, with their readouts from the registry."""
    from services.particles.analytics_store import tomogram_quality
    from services.tilt_series import get_registry_for

    quality = tomogram_quality(project_path)
    registry = get_registry_for(project_path)
    rows: list[_Row] = []
    for name in cut_names(star):
        q = quality.get(name, {})
        ts = registry.get_tilt_series(name) if registry.has_tilt_series(name) else None
        rows.append(
            _Row(
                name=name,
                position=f"{ts.stage_position}·{ts.beam_position}" if ts is not None else "—",
                n_used=q.get("n_used"),
                ctf_resolution=q.get("ctf_resolution"),
                ctf_from=q.get("ctf_from"),
                n_blank=q.get("n_blank"),
                n_dim=q.get("n_dim"),
                excluded=bool(ts is not None and ts.is_excluded),
                exclusion_reason=ts.exclusion_reason if ts is not None else None,
            )
        )
    return rows


def _table_html(rows: list[_Row], chosen: set[str], frozen: bool) -> str:
    esc = html.escape
    head = "".join(
        f'<th style="{_TH}" title="{esc(tip)}">{esc(label)}</th>'
        for label, tip in (
            ("", "Kept by this Subset"),
            ("Tilt series", ""),
            ("Position", "Stage position · beam position"),
            ("Tilts", "Tilts in the reconstruction"),
            ("CTF fit", "Warp's CTF fit resolution of the series; the job it comes from is in each cell's hover"),
            ("Blank · dim", "Blank and dim exposures among those tilts"),
            ("", "Excluded from processing in the registry: the jobs after this Subset skip it too"),
        )
    )
    body: list[str] = []
    for r in rows:
        kept = r.name in chosen
        row_style = ("" if frozen else "cursor: pointer;") + ("" if kept else "opacity: 0.45;")
        ctf = f"{r.ctf_resolution:.1f} Å" if r.ctf_resolution else "—"
        ctf_tip = f"CTF fit of {r.ctf_from}" if r.ctf_from else "No CTF fit in the registry"
        blank_dim = f"{r.n_blank} · {r.n_dim}" if r.n_blank is not None and r.n_dim is not None else "—"
        excluded = (
            f'<span style="color: {_AMBER};" title="{esc(r.exclusion_reason or "excluded from processing")}">'
            "excluded</span>"
            if r.excluded
            else ""
        )
        cells = (
            ("●" if kept else "○", ""),
            (esc(r.name), ""),
            (esc(r.position), ""),
            ("—" if r.n_used is None else str(r.n_used), ""),
            (esc(ctf), esc(ctf_tip)),
            (esc(blank_dim), ""),
            (excluded, ""),
        )
        tds = "".join(f'<td style="{_TD}" title="{tip}">{text}</td>' for text, tip in cells)
        body.append(f'<tr data-ts="{esc(r.name, quote=True)}" style="{row_style}">{tds}</tr>')
    return (
        f'<table style="border-collapse: collapse;"><thead><tr>{head}</tr></thead>'
        f"<tbody>{''.join(body)}</tbody></table>"
    )


@register_params_renderer(JobType.SUBSET)
def render_subset_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **ctx):
    state = job_model._project_state
    instance_id = ctx.get("instance_id") or next(iid for iid, jm in state.jobs.items() if jm is job_model)
    callbacks = ctx.get("callbacks") or {}

    ui.label(
        "Run writes this Subset at once, no SLURM job: the rows of the star it cuts for the tilt series kept "
        "here. The jobs added after it process only those."
    ).style(HELPER_STYLE + " margin-bottom: 4px;")

    cut = subset_cut(state, job_model)
    check = PathResolutionService(state).validate_input_slot(
        JobType.SUBSET, job_model, "input_star", check_filesystem=False, consumer_instance_id=instance_id
    )
    if cut is None or not check.resolved_path:
        ui.label(check.error_message or "It cuts nothing.").style(f"{SANS} font-size: 10px; color: {_RED};")
        return
    source_id, slot = cut
    source = state.jobs[source_id]
    star = Path(check.resolved_path)
    ui.label(f"Cuts {get_instance_display_name(source_id, source)} · {star.name}").style(_LINE_STYLE).tooltip(str(star))
    if slot.produces == JobFileType.ALIGNED_TILT_SERIES_STAR:
        ui.label(
            "A CTF job after this Subset with defocus_hand = auto decides the hand from these tilt series only."
        ).style(HELPER_STYLE)
    if source.execution_status != JobStatus.SUCCEEDED:
        ui.label(
            f"{source_id} is {source.execution_status.value.lower()}: its tilt series show once it has finished, "
            "and Run refuses this Subset until then."
        ).style(f"{SANS} font-size: 10px; color: {_AMBER};")
        return

    body = ui.column().classes("w-full").style("gap: 4px; margin-top: 6px;")
    with body:
        ui.label("Reading the tilt series…").style(HELPER_STYLE)

    async def load() -> None:
        try:
            rows = await run.io_bound(_load_rows, star, Path(state.project_path))
        except Exception as e:
            logger.exception("Subset %s: cannot read %s", instance_id, star)
            if not body.is_deleted:
                body.clear()
                with body:
                    ui.label(f"Cannot read the tilt series of {star}: {e}").style(
                        f"{SANS} font-size: 10px; color: {_RED};"
                    )
            return
        if not body.is_deleted:
            body.clear()
            with body:
                _render_body(job_model, rows, is_frozen, save_handler)

    owned_timer(0.01, load, body, once=True)

    followers = jobs_after(slot.produces)
    add = callbacks.get("prompt_species_and_add")
    if followers and add is not None:
        with ui.row().classes("items-center gap-2").style("margin-top: 8px;"):
            for jt in followers:
                house_button(
                    f"Add {display_name(jt)}",
                    lambda jt=jt: add(jt, after=instance_id),
                    tooltip=f"A new {display_name(jt)} that reads this Subset's tilt series, and the rest of "
                    f"its inputs from {source_id} where {source_id} has them, else from where {source_id} "
                    "read them.",
                )
    return None


def _render_body(job_model, rows: list[_Row], frozen: bool, save_handler) -> None:
    order = [r.name for r in rows]
    listed = set(order)

    count = ui.label("").style(_LINE_STYLE)
    stale = ui.label("").style(f"{SANS} font-size: 9px; color: {_AMBER};")

    def paint() -> None:
        chosen = set(job_model.ts_names)
        count.set_text(f"{len(chosen & listed)} of {len(order)} chosen")
        absent = [n for n in job_model.ts_names if n not in listed]
        stale.set_text(
            f"{len(absent)} chosen tilt series are not in the star it cuts ({', '.join(absent[:5])}"
            f"{' …' if len(absent) > 5 else ''}); Run refuses this Subset until they are dropped (None)."
            if absent
            else ""
        )
        stale.set_visibility(bool(absent))
        table.set_content(_table_html(rows, chosen, frozen))

    def choose(names: list[str]) -> None:
        if frozen:
            return
        job_model.ts_names = names
        save_handler()
        paint()

    def on_click(e) -> None:
        name = (e.args or {}).get("ts") if isinstance(e.args, dict) else None
        if frozen or name not in listed:
            return
        chosen = set(job_model.ts_names)
        chosen ^= {name}
        # Kept in the star's order; names it does not list stay until None drops them.
        choose([n for n in order if n in chosen] + [n for n in job_model.ts_names if n not in listed])

    if not frozen:
        with ui.row().classes("items-center gap-2"):
            house_button("All", lambda: choose(list(order)))
            house_button("None", lambda: choose([]))
            ui.label("click a row to keep or leave out its tilt series").style(HELPER_STYLE)

    with ui.element("div").style("max-height: 420px; overflow-y: auto; width: 100%;"):
        table = ui.html("", sanitize=False)
        table.on("click", handler=on_click, js_handler=_ROW_CLICK_JS)
    paint()
