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

import asyncio
import logging
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path

from nicegui import context, ui

from services.aggregation.authoritative import extraction_params_for_species
from services.background_tasks import get_background_task_registry
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles import picks_filter
from services.particles.ingest import register_manual_pick_list
from services.particles.list_ref import AUTO_SLUG, ListRef, extract_pick_list_instance_id, fs_slug
from services.particles.species_overview import ExtractJob
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


_LIVE_JOB_STATUSES = (JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.RUNNING)


def _no_backend() -> None:
    ui.notify("Backend unavailable.", type="negative")


def dialog_host():
    """The slot a dialog must be parented at: the page LAYOUT slot, never the element that
    opened it. NiceGUI runs an event handler "within the context of the parent slot of the
    sender" (``events.handle_event``), and both callers put these buttons inside containers
    a refresh clears — the Journey's rail rebuild, the Species page's rev-gated Picks table
    — so a dialog parented there dies mid-interaction ("parent element ... has been
    deleted"). ``nullcontext`` when there is no client layout (a background task, a
    non-page context): the caller's current slot is then the only option."""
    try:
        return context.client.layout.default_slot
    except (RuntimeError, AttributeError):
        return nullcontext()


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

        state = get_project_state_for(ref.project_path)
        live = state.jobs.get(extract_pick_list_instance_id(ref.species_id, ref.tomo_name, ref.slug))
        replacing = live is not None and live.execution_status in _LIVE_JOB_STATUSES
        if replacing and not await _confirm_reextract(ref, live):
            return

        # Schema source: mirror the species' candidates.star when it has a
        # candidate-extract job; otherwise synthesize from the tomogram's own star
        # (a de-novo species never had a TM/CE job to mirror).
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

        _submit_list_extraction(
            backend, ref, candidate_optset, tomograms_star, list_star, params, on_done, replacing=replacing
        )


async def _confirm_reextract(ref: ListRef, job) -> bool:
    """Ask before re-extracting a list whose own extraction is still in flight.

    A question, not a block: submitting again deletes the out dir the queued/running job is
    writing into, so the user has to mean it — but a watcher lost to a server restart leaves
    the instance reading Running with nothing left to move it (the accepted residual of
    roadmap 07-S3), and disabling the action there would strand the list for good."""
    with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[28rem] max-w-full gap-2"):
        ui.label(f"An extraction for '{ref.label}' is already {job.execution_status.value.lower()}").classes(
            "text-sm font-bold"
        )
        ui.label(
            f"SLURM {job.slurm_job_id or '—'} · {ref.tomo_name}. Submitting again deletes this run's output "
            "directory and re-cuts from scratch, so the job already in the queue would be writing into a "
            "directory pulled out from under it."
        ).classes("text-[12px] text-gray-600")
        ui.label(
            "If the server was restarted while an extraction was in flight, the status stays here with nothing "
            "left to move it — re-extracting is then exactly the right thing to do."
        ).classes("text-[10px] text-gray-400")
        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=lambda: confirm.submit(None)).props("flat dense no-caps")
            ui.button("Re-extract anyway", on_click=lambda: confirm.submit(True)).props("dense no-caps color=orange-7")
    go = await confirm
    confirm.delete()
    return bool(go)


def _submit_list_extraction(
    backend,
    ref: ListRef,
    candidate_optset: Path | None,
    tomograms_star: Path | None,
    list_star: str,
    params: dict,
    on_done: OnDone,
    *,
    replacing: bool = False,
) -> None:
    """Fire the per-list extraction as a tracked BackgroundTask. Split out of
    ``extract_list`` so the geometry dialog can submit the same way once the user
    commits box/bin/crop.

    ``replacing`` = the caller already confirmed a re-extract over a live one, which the
    task registry would otherwise silently swallow: it dedupes on ``dedup_key`` by handing
    back the running task id WITHOUT calling the coroutine, so the user's consent would
    produce no submit at all."""

    async def _run(progress_cb):
        progress_cb(0, 0, "extracting subtomograms…")
        res = await backend.extract_pick_list_and_wait(
            ref.project_path,
            candidate_optset,
            Path(list_star),
            ref.tomo_name,
            ref.species_id,
            ref.slug,
            tomograms_star=tomograms_star,
            **params,
        )
        if not res.get("success"):
            # RAISE, don't return: the task registry marks a task succeeded on any
            # non-exception return (services/background_tasks.runner), so handing back an
            # err() dict painted a failed extraction as a green, message-less success in the
            # tray — one of the invisible failures roadmap 07 exists to end. The instance's
            # own status/last_error carry it too (07-S3); this is the transient surface.
            raise RuntimeError(res.get("error") or "extraction failed")
        return f"{res.get('count', 0)} particles extracted"

    dedup_key = f"extract:{ref.species_id}:{ref.tomo_name}:{ref.slug}"
    in_flight = BackgroundTask.existing(dedup_key)
    if in_flight is not None and not replacing:
        # The registry DEDUPES by returning the existing task id WITHOUT calling the
        # coroutine (services/background_tasks.BackgroundTaskRegistry.submit), so submitting
        # here would do nothing at all while the toast below claimed otherwise. Say what is
        # actually true. Reachable in the narrow window where the awaiter has already written
        # a terminal status (so `extract_list` asks nothing) but its task is still settling.
        ui.notify(f"An extraction for '{ref.label}' is still in flight — see the task tray.", type="info")
        return
    if in_flight is not None:
        # The user confirmed a re-extract over a live one. Cancel the old awaiter — it is
        # watching the out dir this submit is about to wipe — and submit WITHOUT the dedup
        # key: `registry.cancel` only delivers the CancelledError on the next loop turn, so
        # the old record still reads `is_running` right here and would swallow the new submit.
        # Double-submit protection on this path is the SingleFlight guard plus the confirm
        # dialog itself; the instance's own status is what the next click reads anyway.
        get_background_task_registry().cancel(in_flight.id)
        dedup_key = None

    BackgroundTask(
        title=f"Extract · {ref.label}", subtitle=ref.tomo_name, project_path=str(ref.project_path), dedup_key=dedup_key
    ).submit(_run, on_complete=lambda _t: on_done(), show_start_toast=True)
    ui.notify(
        f"{'Re-extraction' if replacing else 'Extraction'} submitted for '{ref.label}' — tracking in the task tray.",
        type="info",
    )


# ── Extraction logs + geometry record (roadmap 07-S4) ─────────────────────────

_MONO = "font-family: ui-monospace, SFMono-Regular, Menlo, monospace;"
_LOG_MAX_LINES = 400  # lines KEPT from each file (the tail); the marker below says what went
# The widget holds more than we ever push, on purpose: `ui.log(max_lines=N)` drops from the
# FRONT, so capping it at the truncation threshold would evict the "[… truncated …]" marker —
# the one line that says the view is partial. Same split as `ui/pipeline_builder/logs_tab.py`.
_LOG_WIDGET_LINES = _LOG_MAX_LINES * 2


def extraction_geometry_text(job: ExtractJob) -> str:
    """The geometry this instance last cut with, or a plain statement that no submit has
    written one yet. Before roadmap 07 this existed ONLY in the launch command line, so
    nothing could say after the fact what box a list had been cut with."""
    if not job.box_size:
        return "geometry not recorded — this instance has never been submitted"
    return f"box {job.box_size} px · bin {job.binning:g} · crop {job.crop_size or 'none'}"


async def open_extraction_logs(backend, project_path: Path, job: ExtractJob, *, title: str) -> None:
    """The per-list extraction job's logs, as a dialog.

    The pipeline's log viewer (``ui/pipeline_builder/logs_tab.py``) cannot serve this job: it
    keys off ``relion_job_name`` and per-tab widget refs, and a per-list extraction is not a
    scheme job so it has neither. ``backend.get_job_logs`` needs only a directory — and
    ``config/qsub.sh`` already writes ``run.out``/``run.err`` into the list's out dir — so the
    instance's recorded ``job_dir`` is the whole address. An instance whose submit never
    recorded one says exactly that instead of showing empty logs.

    ``job`` is a snapshot taken when the row was rendered, so Reload re-reads the instance's
    live status and failure text as well as the two files — a Reload that refreshed only half
    the dialog would be its own small lie."""
    if backend is None:
        _no_backend()
        return
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[46rem] max-w-full gap-2"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("science", size="16px").classes("text-indigo-500")
            ui.label(f"Extraction — {title}").classes("text-sm font-bold")
            ui.space()
            status_lbl = ui.label(job.status).classes("text-[11px] font-bold text-slate-500")
        meta = [extraction_geometry_text(job)]
        if job.slurm_job_id:
            meta.append(f"SLURM {job.slurm_job_id}")
        ui.label(" · ".join(meta)).classes("cb-detail-meta")
        err_lbl = ui.label(job.error).classes("text-[11px] text-red-700 whitespace-pre-wrap")
        err_lbl.set_visibility(bool(job.error))

        def _refresh_status() -> None:
            jm = get_project_state_for(project_path).jobs.get(job.instance_id)
            if jm is None:
                # Reachable: another tab deleted the list, and `delete_pick_list` pops this
                # instance with it. Returning silently would leave the header asserting the
                # snapshot's status next to a log pane reading "job directory not found".
                status_lbl.set_text("gone")
                status_lbl.classes(replace="text-[11px] font-bold text-orange-700")
                err_lbl.set_text("This extraction instance is no longer registered — its pick list was deleted.")
                err_lbl.set_visibility(True)
                return
            status_lbl.set_text(jm.execution_status.value)
            err_lbl.set_text(jm.last_error)
            err_lbl.set_visibility(bool(jm.last_error))

        if not job.job_dir:
            ui.label(
                "This instance recorded no job directory, so there is nothing to read — it was never submitted "
                "(or was submitted by a build that predates roadmap 07)."
            ).classes("text-[11px] text-orange-700")
        else:
            ui.label(job.job_dir).classes("text-[10px] text-gray-400").style(_MONO)
            ui.label("run.out").classes("text-[10px] font-bold text-gray-500 uppercase tracking-wider")
            out_log = (
                ui.log(max_lines=_LOG_WIDGET_LINES)
                .classes("w-full p-2")
                .style(
                    f"height: 12rem; overflow-y: auto; {_MONO} font-size: 10px; line-height: 1.4; background: #fafafa;"
                )
            )
            ui.label("run.err").classes("text-[10px] font-bold text-gray-500 uppercase tracking-wider")
            err_log = (
                ui.log(max_lines=_LOG_WIDGET_LINES)
                .classes("w-full p-2")
                .style(
                    f"height: 7rem; overflow-y: auto; {_MONO} font-size: 10px; line-height: 1.4; "
                    "color: #b91c1c; background: #fefafa;"
                )
            )

            async def _load() -> None:
                _refresh_status()
                logs = await backend.get_job_logs(str(project_path), job.job_dir)
                for widget, key in ((out_log, "stdout"), (err_log, "stderr")):
                    text = logs.get(key) or "(empty)"
                    lines = text.split("\n")
                    if len(lines) > _LOG_MAX_LINES:
                        text = f"[… truncated {len(lines) - _LOG_MAX_LINES} lines …]\n" + "\n".join(
                            lines[-_LOG_MAX_LINES:]
                        )
                    widget.clear()
                    widget.push(text)

        with ui.row().classes("w-full justify-end gap-2"):
            if job.job_dir:
                ui.button("Reload", icon="refresh", on_click=_load).props("flat dense no-caps size=sm")
            ui.button("Close", on_click=lambda: dialog.submit(None)).props("flat dense no-caps size=sm")
    dialog.open()
    if job.job_dir:
        await _load()
    # Await + delete, never bare close(): this dialog is parented at the layout slot (which
    # nothing ever clears) and every log line is an element, so closing alone retains the whole
    # tree for the life of the page and re-sends it on a websocket reconnect. Awaiting also
    # makes the caller's SingleFlight cover the dialog's lifetime, which is what its docstring
    # already claims — a second click while it is open is a no-op instead of a second dialog.
    # The `value` guard is not defensive noise: `Dialog.__await__` OPENS the dialog, so a user
    # who dismissed it while `_load()` was reading run.out off Lustre would see it pop back.
    if dialog.value:
        await dialog
    dialog.delete()


def geometry_inputs() -> tuple[ui.number, ui.number, ui.number]:
    """The three extraction-geometry fields (box / binning / crop), EMPTY on purpose —
    prefilling them with the old 384/1.0/224 would just relabel a silent default as a
    confirmed one. Shared by the per-list prompt and the Picks tab's extract-all pre-flight."""
    box_in = ui.number("box size (px, unbinned)", min=16, step=2).props("dense outlined").classes("w-full")
    bin_in = ui.number("binning", min=0.1, step=0.5).props("dense outlined").classes("w-full")
    crop_in = ui.number("crop size (px)", min=16, step=2).props("dense outlined").classes("w-full")
    return box_in, bin_in, crop_in


async def commit_extraction_geometry(backend, project_path: Path, species_id: str, box, binning, crop) -> bool:
    """Validate the three fields and persist ``species.extraction_params`` (D-3) through
    ``mutate_species`` (dirty + rev) and an AWAITED forced save — the extraction that follows
    runs in a BackgroundTask with no client context, and a fire-and-forget save can lose the
    geometry the user just committed. False = not committed (the reason was toasted)."""
    if not box or not binning or not crop:
        ui.notify("Box size, binning and crop are all required.", type="warning")
        return False
    if backend is None:
        _no_backend()
        return False
    geometry = ExtractionParams(box_size=int(box), binning=float(binning), crop_size=int(crop))

    def _apply(species) -> None:
        species.extraction_params = geometry

    if not get_project_state_for(project_path).mutate_species(species_id, _apply):
        ui.notify("Species not found — reload the project.", type="negative")
        return False
    await backend.save_project(project_path, force=True)
    return True


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
    and no ``species.extraction_params``.
    """
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[26rem] max-w-full gap-2"):
        ui.label("Extraction geometry").classes("text-base font-bold")
        ui.label(
            f"'{ref.species_label or ref.species_id}' has no subtomo-extraction job to inherit box/binning/crop "
            "from. Set them once — they are saved on the species and reused for every later extraction."
        ).classes("text-xs text-gray-600")
        ui.label("Also editable any time in the Particles registry → Overview → Extraction geometry.").classes(
            "text-[10px] text-gray-400"
        )
        box_in, bin_in, crop_in = geometry_inputs()

        async def _commit() -> None:
            if not await commit_extraction_geometry(
                backend, ref.project_path, ref.species_id, box_in.value, bin_in.value, crop_in.value
            ):
                return
            dialog.close()
            state = get_project_state_for(ref.project_path)
            subtomo_jm = state.jobs.get(ref.subtomo_iid) if ref.subtomo_iid else None
            params = extraction_params_for_species(state, ref.species_id, subtomo_jm)
            _submit_list_extraction(backend, ref, candidate_optset, tomograms_star, list_star, params, on_done)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button("Save & extract", icon="science", color="indigo", on_click=_commit).props("no-caps")
    dialog.open()


# ── Merge / dedup ─────────────────────────────────────────────────────────────


def can_merge_source(ref: ListRef) -> bool:
    """Whether ``merge_source_for`` can produce a star for this ref. The Picks table gates
    its merge tick on this: an ``auto`` row with neither a committed filter nor a
    candidate-extract job is drawn disabled with the reason instead of raising out of the
    merge handler (``picks_filter.merge_source_for`` answers that case by raising)."""
    if ref.slug != AUTO_SLUG:
        return bool(ref.star_path)
    return picks_filter.auto_source_for(ref.ce_job_dir, ref.subtomo_job_dir) is not None


def merge_source_for(ref: ListRef) -> dict | None:
    """The star ``ref`` contributes to a merge — its KEPT subset (the user's keep/drop must
    not bleed dropped picks into it); see ``picks_filter.merge_source_for``. None, with the
    reason named, when the ref has nothing to contribute: the tick that selected it is drawn
    disabled, so this is the stale-render race (the filter star went away between render and
    click), and it must not surface as a traceback."""
    if not can_merge_source(ref):
        ui.notify(
            f"{ref.label} on {ref.tomo_name} has no star to merge from — "
            "an auto list needs a committed particles_filtered.star or a candidate-extract job.",
            type="warning",
        )
        return None
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


def open_dedup_dialog(backend, ref: ListRef, *, default_radius_ang: float, on_done: OnDone) -> None:
    """Overlap overview + 'Deduplicate' for a merged list, as a dialog (the Journey's former
    inline clash panel, now the Picks tab's). At the CHOSEN radius it shows how many picks clash;
    the user varies the radius and clicks Deduplicate to remove them (manual kept over
    auto). Nothing dedups automatically."""
    if not ref.star_path:
        ui.notify(f"'{ref.label}' has no backing star.", type="warning")
        return
    star = ref.star_path
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[26rem] max-w-full gap-2"):
        with ui.row().classes("items-center gap-2"):
            ui.icon("join_inner", size="16px").classes("text-orange-700")
            ui.label(f"Overlap — {ref.label} · {ref.tomo_name}").classes("text-sm font-bold")
        radius_in = (
            ui.number("overlap radius", value=default_radius_ang, step=1, min=0)
            .props("dense outlined suffix=Å debounce=600")
            .classes("w-full text-xs")
            .tooltip("Two picks closer than this (Å) are treated as the same particle")
        )
        note = ui.label("checking overlaps…").classes("text-[11px] text-gray-600")

        async def _recompute(_e=None):
            if backend is None:
                return
            r = float(radius_in.value or 0)
            stats = await backend.list_clash_stats(star, ref.tomo_name, r)
            if not stats.get("success"):
                note.set_text(f"overlap check unavailable — {stats.get('error') or 'unknown error'}")
                return
            nt, nc, nr, na = stats["n_total"], stats["n_clashing"], stats["n_removed"], stats["n_after"]
            if nr <= 0:
                note.set_text(f"no overlaps at {r:g} Å · {nt} picks")
                note.classes(replace="text-[11px] text-emerald-700")
                dedup_btn.props("disable")
            else:
                note.set_text(f"{nc} of {nt} clash at {r:g} Å → dedup keeps {na}")
                note.classes(replace="text-[11px] text-orange-800 font-medium")
                dedup_btn.props(remove="disable")

        async def _do_dedup():
            dialog.close()
            await dedup_list(backend, ref, float(radius_in.value or 0), on_done=on_done)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat dense no-caps")
            dedup_btn = (
                ui.button("Deduplicate", icon="cleaning_services", on_click=_do_dedup)
                .props("dense no-caps color=orange-7")
                .tooltip(
                    "Remove every pick within the radius of a higher-priority pick (manual kept over auto). "
                    "Rewrites this merged list — re-extract after."
                )
            )
        radius_in.on_value_change(_recompute)
        asyncio.create_task(_recompute())
    dialog.open()


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
    across all projects). No live session → point the user at the Curation tab (the
    Journey's ⚡ pre-empts this by routing to ``curate_in_artiax`` itself, 11-S3)."""
    async with _flight(f"loadinto:{ref.species_id}:{ref.tomo_name}") as acquired:
        if not acquired:
            return
        # This handler awaits a ~20 s load; during it the dashboard's periodic
        # main_area.clear() deletes the slot this coroutine was entered under, so a later
        # bare ui.notify dies with "parent element ... has been deleted". Capture the page
        # LAYOUT slot (never cleared) up front and route every notify through it; swallow
        # the residual race so a stale toast never surfaces a traceback.
        host = dialog_host()

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
            # Three callers, three surfaces: the Curation tab (a 'curate' button sits next
            # to ⚡), the Picks tab's per-tomogram ⚡, and the Journey — where 11-S3 removed
            # the Curate button, and ⚡ reaches here only when the ~16 s liveness cache says
            # a session is up, i.e. just after one died. So name WHERE the action is, not a
            # button that exists on only one of them.
            _notify(
                "No running ChimeraX session — start one with 'curate' on the species page's Curation tab "
                "(it opens the control center for that tomogram).",
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


def import_picks_from_path(
    backend, ref: ListRef, *, on_done: OnDone, tomo_options: dict[str, ListRef] | None = None, intro: str | None = None
) -> None:
    """Import a ``.coords`` by explicit path (ArtiaX's save dialog may default anywhere,
    and an external file has no curation dir at all): paste the full path → the same
    backend import → register the ``manual`` list of the target tomogram. Also the
    fallback of the Journey's auto-discover import when no saved .coords was found.

    ``tomo_options`` (``{tomo_name: ref}``, the Species page's species-level entry) adds a
    tomogram picker — the .coords maps into ONE tomogram's frame, and the tomogram may have
    no picks yet, so the caller supplies the universe; ``ref`` is the initial choice."""
    if tomo_options is None and ref.tomograms_star is None:
        ui.notify(f"No tomograms.star resolved for {ref.tomo_name} — cannot map .coords into it.", type="warning")
        return
    target = {"ref": ref}
    with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[34rem] max-w-full gap-2"):
        title = "Import ArtiaX picks" if tomo_options else f"Import ArtiaX picks — {ref.tomo_name}"
        ui.label(title).classes("text-base font-bold")
        ui.label(
            intro
            or "No saved .coords was found in this project's curation dirs. Paste the full path to the "
            ".coords you saved from ArtiaX (any filename)."
        ).classes("text-xs text-gray-600")
        if tomo_options:
            names = list(tomo_options)
            tomo_sel = (
                ui.select(names, value=ref.tomo_name if ref.tomo_name in tomo_options else names[0], label="tomogram")
                .props("dense outlined options-dense")
                .classes("w-full text-xs")
            )
            target["ref"] = tomo_options[tomo_sel.value]

            def _pick(e):
                target["ref"] = tomo_options[e.value]

            tomo_sel.on_value_change(_pick)
        path_in = ui.input("path to .coords").props("dense outlined").classes("w-full font-mono text-xs")

        async def _do_import():
            p = (path_in.value or "").strip()
            if not p:
                ui.notify("Enter a path", type="warning")
                return
            if backend is None:
                _no_backend()
                return
            r = target["ref"]
            if r.tomograms_star is None:
                ui.notify(f"No tomograms.star resolved for {r.tomo_name} — cannot map .coords into it.", type="warning")
                return
            result = await backend.import_curation_picks(
                r.project_path,
                r.tomograms_star,
                r.tomo_name,
                r.species_label or r.species_id,
                r.species_id,
                coords_path=Path(p),
            )
            if not result.get("success"):
                ui.notify(f"Import failed: {result.get('error')}", type="negative", timeout=4000)
                return
            dialog.close()
            await register_imported_picks(backend, r, result, on_done=on_done)

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat")
            ui.button("Import", icon="download", color="indigo", on_click=_do_import).props("no-caps")
    dialog.open()
