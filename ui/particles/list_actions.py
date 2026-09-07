"""Pick-list ACTIONS shared by the Journey and the Species page (roadmap 11-S1).

Carved out of ``ui/tomo_dashboard_dialog.py`` so the Species page (manage & act) and the
Journey (look & curate) drive the same code for extract / merge / dedup / curate /
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
from pathlib import Path

from nicegui import ui

from services.aggregation.extraction import extraction_params_for_species
from services.background_tasks import get_background_task_registry
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles import picks_filter
from services.particles.ingest import register_manual_pick_list
from services.particles.list_admin import delete_pick_list, pick_list_files
from services.particles.list_ref import AUTO_SLUG, ListRef, extract_pick_list_instance_id, fs_slug, known_tomograms
from services.particles.species_overview import ExtractJob, extract_job_for
from services.project_state import ExtractionParams, PickList, get_project_state_for
from services.visualization.tomo_geometry import geometry_for_ts
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button
from ui.components.fields import house_number, house_select, house_text
from ui.components.dialogs import dialog_host
from ui.components.reactive import SingleFlight
from ui.curation_session_dialog import open_curation_control_center

logger = logging.getLogger(__name__)

OnDone = Callable[[], None]

_flight = SingleFlight()

_HINT_CLS = "text-[10px] text-gray-400"

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
            house_button("Cancel", lambda: confirm.submit(None))
            house_button("Re-extract anyway", lambda: confirm.submit(True), kind="accent")
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
                house_button("Reload", _load)
            house_button("Close", lambda: dialog.submit(None))
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
    box_in = house_number("Box size", min=16, step=2, width="w-28", hint="px, unbinned")
    bin_in = house_number("Binning", min=0.1, step=0.5, width="w-28")
    crop_in = house_number("Crop size", min=16, step=2, width="w-28", hint="px")
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
            house_button("Cancel", dialog.close)
            house_button("Save & extract", _commit, kind="accent")
    dialog.open()


# ── Merge / dedup ─────────────────────────────────────────────────────────────


def can_merge_source(ref: ListRef) -> bool:
    """Whether ``merge_source_for`` can produce a star for this ref: an ``auto`` row with
    neither a committed filter nor a candidate-extract job has nothing to contribute, and
    ``picks_filter.merge_source_for`` answers that case by raising. The gate is here so a
    merge caller can say so instead. (Its old caller, the Picks table's merge tick, went
    with picking-UI 09-S3 — creating merges is the Aggregate-candidates flow's job.)"""
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


# ── Delete ────────────────────────────────────────────────────────────────────


async def delete_list(
    backend, project_path: Path, species_id: str, tomo_name: str, slug: str, label: str, *, on_done: OnDone
) -> None:
    """Confirm (listing exactly what goes) → ``list_admin.delete_pick_list``.

    Takes the identity rather than a ``ListRef`` so the Journey / viewer rail — which
    renders from ``sp`` / ``lst`` dicts and has no anchors resolved — can offer the same
    delete as the Species page's Picks table without a disk pass to build one.

    An in-flight extraction is cancelled FIRST: its output directory is what this removes,
    and its instance goes with the list, so a job left running would re-create the directory
    with nothing left in the project able to stop it.
    """
    async with _flight(f"delete:{project_path}:{species_id}:{tomo_name}:{slug}") as acquired:
        if not acquired:
            return
        state = get_project_state_for(project_path)
        pl = state.get_pick_list(slug, species_id, tomo_name)
        if pl is None:
            ui.notify(f"'{label}' is not a registered list — nothing to delete.", type="warning")
            return
        files = await asyncio.to_thread(pick_list_files, pl)
        job = extract_job_for(state, species_id, tomo_name, slug)
        live_job = job is not None and job.is_live
        with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[30rem] max-w-full gap-2"):
            ui.label(f"Delete '{label}' on {tomo_name}?").classes("text-sm font-bold")
            for kind, kind_label in (("stars", "star file"), ("dirs", "extraction output"), ("coords", "ArtiaX save")):
                for path in files[kind]:
                    ui.label(f"• {kind_label}: {path}").classes("text-[10px] font-mono text-gray-600")
            if not any(files.values()):
                ui.label("• nothing on disk — only the registry entry").classes("text-[11px] text-gray-500")
            if files["coords"]:
                ui.label(
                    "The .coords saves go too — otherwise the curation watcher re-registers this list on the "
                    "next save scan. Archived copies under imports/ are kept."
                ).classes(_HINT_CLS)
            if live_job:
                ui.label(
                    f"An extraction for this list is {job.status.lower()} (SLURM {job.slurm_job_id or '—'}) — "
                    "it is cancelled first. Left running it would re-create the output directory this delete "
                    "removes, and its instance goes with the list, so nothing would be left to stop it with."
                ).classes("text-[10px] text-orange-700")
            ui.label("This cannot be undone.").classes(_HINT_CLS + " text-red-600")
            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", lambda: confirm.submit(None))
                house_button("Delete list", lambda: confirm.submit(True), kind="danger")
        go = await confirm
        confirm.delete()
        if not go:
            return
        if live_job and backend is None:
            ui.notify(
                "Backend unavailable — the in-flight extraction was NOT cancelled and may re-create the "
                "directory this delete removes.",
                type="warning",
                timeout=6000,
            )
        elif live_job:
            cancelled = await backend.cancel_pick_list_extraction(project_path, species_id, tomo_name, slug)
            if not cancelled.get("success"):
                ui.notify(cancelled["error"], type="warning", timeout=6000)
        result = await delete_pick_list(project_path, species_id, tomo_name, slug)
        if not result.get("success"):
            ui.notify(result["error"], type="negative")
            return
        for problem in result.get("errors") or []:
            ui.notify(problem, type="warning", timeout=5000)
        ui.notify(f"Deleted '{label}' ({result.get('deleted_files', 0)} file(s))", type="positive")
        on_done()


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
        radius_in = house_number(
            "Overlap radius",
            value=default_radius_ang,
            step=1,
            min=0,
            width="w-28",
            hint="Two picks closer than this (Å) are treated as the same particle",
        )
        radius_in.props("suffix=Å debounce=600")
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
            house_button("Cancel", dialog.close)
            dedup_btn = house_button("Deduplicate", _do_dedup, kind="accent").tooltip(
                "Remove every pick within the radius of a higher-priority pick (manual kept over auto). "
                "Rewrites this merged list — re-extract after."
            )
        radius_in.on_value_change(_recompute)
        asyncio.create_task(_recompute())
    dialog.open()


# ── ArtiaX ────────────────────────────────────────────────────────────────────


async def curate_in_artiax(backend, ref: ListRef) -> None:
    """Per-tomo 'Curate in ArtiaX' — THE launch/scope affordance (roadmap 09-S2, 10-S1).

    Declares the scope: exports this (species, tomo)'s picks to a `.coords`, writes the
    `.cxc` that preloads them and the `manifest.json` that says which species and which
    tomogram this directory is for, pre-seeds the default list (13-S1: an empty
    `<species>__<tomo>__picks.coords`, opened last by the `.cxc`, registered as a 0-pick
    `picks` row — never overwritten on a repeat click), then opens the control center
    bound to it. Starting the session from there stamps that scope onto the session too. A
    different tomogram means coming back here: the control center then offers the confirmed
    in-session switch (13-S2) or a restart.

    The bundle prep is the slow part on a tomogram whose display copy does not exist yet —
    hence the toast BEFORE the await, which names it. SingleFlight-guarded so repeated
    clicks prep only one bundle."""
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
        ui.notify(
            f"Preparing ArtiaX bundle for {ref.tomo_name} — the first time on a tomogram this also builds "
            "its downscaled display copy, which can take a while.",
            type="info",
            timeout=4000,
        )
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
        if bundle.get("display_note"):
            # The display copy could not be built, so this session opens the full-res
            # volume and takes ~20 s. Say it once, here — never silently slow.
            ui.notify(f"No downscaled copy for {ref.tomo_name}: {bundle['display_note']}", type="warning", timeout=7000)
        if bundle.get("seed_error"):
            # The seed file exists and the .cxc opens it; only its 0-pick row is missing.
            # The watcher registers it on the first save — say so rather than hide it.
            ui.notify(
                f"{bundle['seed_error']} — the seed still opens in ArtiaX; its row appears on the first save.",
                type="warning",
                timeout=7000,
            )
        await open_curation_control_center(backend, ref.project_path, bundle=bundle)


# ── Import ────────────────────────────────────────────────────────────────────


async def register_imported_picks(backend, ref: ListRef, result: dict, *, on_done: OnDone) -> None:
    """Explicit-import click path: upsert the ``manual__<stem>`` PickList for this
    (species, tomo) from a ``backend.import_curation_picks`` result
    (``services.particles.ingest``, shared with the server-side watcher), persist AWAITED
    with force=True so the registry actually lands on disk (a fire-and-forget
    ``create_task(save_project())`` was getting GC'd before it ran, leaving
    ``pick_lists: []`` in project_params.json), toast, and ``on_done`` so the new diamond
    layer appears. Importing a file whose name matches an existing list REPLACES that
    list — same name, same list (10-S2) — so the toast names it."""
    pl = register_manual_pick_list(get_project_state_for(ref.project_path), result, ref.species_id, ref.tomo_name)
    await backend.save_project(ref.project_path, force=True)
    src = Path(result.get("coords_source", "")).name
    ui.notify(
        f"Imported {pl.count} picks into '{pl.label}' on {ref.tomo_name}" + (f" (from {src})" if src else ""),
        type="positive",
        timeout=3000,
    )
    on_done()


def import_picks_from_path(
    backend,
    ref: ListRef,
    *,
    on_done: OnDone,
    tomo_options: dict[str, ListRef] | None = None,
    intro: str | None = None,
    initial_path: str | None = None,
) -> None:
    """Import a ``.coords`` by explicit path (ArtiaX's save dialog may default anywhere,
    and an external file has no curation dir at all): paste the full path → the same
    backend import → register the ``manual`` list of the target tomogram. Also the
    fallback of the Journey's auto-discover import when no saved .coords was found.

    ``tomo_options`` (``{tomo_name: ref}``, the Species page's species-level entry) adds a
    tomogram picker — the .coords maps into ONE tomogram's frame, and the tomogram may have
    no picks yet, so the caller supplies the universe; ``ref`` is the initial choice.

    ``initial_path`` pre-fills the field for a caller that already browsed to the file (the
    Picks & curation import row). The dialog still opens rather than importing straight off
    the pick: the frame a by-path file is read in is stated here and nowhere else."""
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
        # The frame a by-path file is read in. A save made INSIDE a curation dir carries
        # that session's display-binning offset (10-S3) via its manifest; a file from
        # anywhere else has no manifest, so it is read as full-resolution corner-Å. That is
        # the right default for an external file and wrong by (N-1)/2·px for a session save
        # the user moved out — so say which, rather than let a silent few-Å shift through.
        ui.label(
            "Read as full-resolution coordinates. If this file came out of a crboost curation session, "
            "put it back in that tomogram's folder (or assign it from UNATTRIBUTED SAVES) instead — that "
            "route keeps the exact frame the session displayed."
        ).classes("text-[11px] text-gray-500")
        if tomo_options:
            names = list(tomo_options)
            tomo_sel = house_select(
                "Tomogram", names, value=ref.tomo_name if ref.tomo_name in tomo_options else names[0], width="w-64"
            )
            tomo_sel.props("options-dense")
            target["ref"] = tomo_options[tomo_sel.value]

            def _pick(e):
                target["ref"] = tomo_options[e.value]

            tomo_sel.on_value_change(_pick)
        path_in = house_text("Path to .coords", width="w-full", value=initial_path or "")

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
            house_button("Cancel", dialog.close)
            house_button("Import", _do_import, kind="accent")
    dialog.open()


# ── Staging: assign an unattributed save (roadmap 10-S2) ──────────────────────

_CHOOSE = "— choose —"  # no pre-selection: a wrong default here files picks under the wrong species


async def assign_unattributed(backend, project_path: Path, entry: dict, *, on_done: OnDone) -> None:
    """Assign `.coords` files the watcher could not attribute to an explicit
    (species, tomogram) — the maintainer's staging mechanism, and the reason nothing in
    the ingest path ever guesses.

    ``entry`` is one row of ``CurationWatcher.unattributed()`` (``dir`` · ``reason`` ·
    ``files``). Confirming MOVES those files into ``Curation/<species>/<tomo>/`` and
    writes the manifest that declares the identity; the watcher ingests them within a
    tick. Neither dropdown is pre-selected — filing someone's hand-picked coordinates
    under a plausible-looking species is exactly the silent misattribution Model B exists
    to make impossible.
    """
    async with _flight(f"assign:{entry.get('dir')}") as acquired:
        if not acquired:
            return
        if backend is None:
            _no_backend()
            return
        state = get_project_state_for(project_path)
        species = {str(getattr(sp, "name", "") or sp.id): sp.id for sp in state.species_registry}
        if not species:
            ui.notify("No species registered yet — create one first, then assign these picks to it.", type="warning")
            return
        tomos = await asyncio.to_thread(known_tomograms, state, project_path)
        if not tomos:
            ui.notify(
                "No tomogram is described by any tomograms.star yet — nothing to assign these picks into.",
                type="warning",
            )
            return
        files = [Path(f) for f in (entry.get("files") or [])]

        with dialog_host(), ui.dialog() as dialog, ui.card().classes("w-[36rem] max-w-full gap-2"):
            ui.label("Assign these picks").classes("text-base font-bold")
            ui.label(entry.get("dir", "")).classes("text-[11px] font-mono text-gray-500 break-all")
            ui.label(entry.get("reason", "")).classes("text-[11px] text-orange-700")
            with ui.column().classes("w-full gap-0 pt-1"):
                for f in files:
                    ui.label(f.name).classes("text-[11px] font-mono text-slate-700").tooltip(str(f))
            sp_sel = house_select("Species", [_CHOOSE, *species], value=_CHOOSE, width="w-64")
            tomo_sel = house_select("Tomogram", [_CHOOSE, *tomos], value=_CHOOSE, width="w-64")
            tomo_sel.props("options-dense")
            ui.label(
                f"{len(files)} file(s) will be MOVED into that tomogram's curation folder and imported as "
                "one pick list each, named after the file."
            ).classes("text-[11px] text-gray-500")

            async def _do_assign() -> None:
                sp_name, tomo = sp_sel.value, tomo_sel.value
                if sp_name == _CHOOSE or tomo == _CHOOSE:
                    ui.notify("Choose both a species and a tomogram.", type="warning")
                    return
                res = await backend.assign_unattributed_coords(
                    project_path, Path(entry["dir"]), species[sp_name], sp_name, tomo
                )
                if not res.get("success"):
                    ui.notify(f"Assign failed: {res.get('error')}", type="negative", timeout=6000)
                    return
                dialog.close()
                skipped = res.get("skipped") or []
                ui.notify(
                    f"Assigned {res.get('count')} file(s) to {sp_name} · {tomo} — importing."
                    + (f" {len(skipped)} could not be moved." if skipped else ""),
                    type="positive" if not skipped else "warning",
                    timeout=5000,
                )
                on_done()

            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", dialog.close)
                house_button("Assign", _do_assign, kind="accent")
        dialog.open()
