"""Per-list subtomo-extraction inputs + which lists still need cutting.

Everything ``backend.extract_pick_list`` needs to cut ONE workbench pick list — the schema
source, the star to consume, the geometry — resolved from an explicit ``ProjectState``, plus
``pending_extractions``: the lists of a species whose subtomograms are missing or stale, and
the reason for the ones that extraction alone cannot fix. Also holds ONE mutator,
``apply_aggregation_overrides``, which points consumer jobs' input_optimisation slots at the
active merged optset.

This module used to be ``authoritative.py`` and its centre of gravity was the
authoritative-list model: one pick list per (species, tomogram) nominated as THE one
downstream consumes, resolved to an optimisation_set handle and rolled up into a
ready/pending/blocked ``GateReport``. That model is gone. What reaches a refinement is now
whatever the user selects as a source in the Aggregate-candidates flow — chosen explicitly,
in front of the merge it feeds — so nothing needs a stored per-tomogram nomination, and a
list is simply extracted or not.

PURE / HEADLESS by design:
  - Takes an EXPLICIT ``ProjectState`` (never the tab-context accessor), so it is correct
    inside a background task / CLI with no NiceGUI client context (the W2 lesson).
  - Resolves job dirs from the passed state's ``relion_job_name`` / ``job_path_mapping``
    via a local copy of ``services.dashboard_data.job_dir_for`` (kept from when that
    helper still read the client-context global; both are headless now).
  - Imports NO UI module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from services.io_slots import JobFileType
from services.models_base import JobType, ListExtractionState, resolve_species
from services.project_state import MERGED_DIR_NAME
from services.particles import picks_filter

logger = logging.getLogger(__name__)


# ── headless job-dir / species resolution (no UI, no client-context global) ──────────────


def _job_dir(state, instance_id: str, job_model, project_path: Path) -> Path | None:
    """Resolve a job's dir from the EXPLICIT state (``relion_job_name``, then
    ``job_path_mapping``). Local copy of ``services.dashboard_data.job_dir_for`` from when
    that helper still read the client-context tab accessor (both are headless now)."""
    rjn = getattr(job_model, "relion_job_name", None)
    if rjn:
        d = project_path / str(rjn).rstrip("/")
        if d.is_dir():
            return d
    mapped = (getattr(state, "job_path_mapping", None) or {}).get(instance_id)
    if mapped:
        d = project_path / str(mapped).rstrip("/")
        if d.is_dir():
            return d
    return None


def _instance_for_species(state, species_id: str, job_type) -> tuple | None:
    """The (instance_id, job_model) of the first job of ``job_type`` attached to this
    species, or None."""
    for iid, jm in state.jobs.items():
        if getattr(jm, "job_type", None) != job_type:
            continue
        if resolve_species(state, jm, iid)[1] == species_id:
            return iid, jm
    return None


def _candidate_instance_for_species(state, species_id: str) -> tuple | None:
    """The TEMPLATE_EXTRACT_PYTOM (candidate-extract) instance for this species — its job
    dir holds the ``optimisation_set.star`` whose ``candidates.star`` schema a per-list
    extraction mirrors."""
    return _instance_for_species(state, species_id, JobType.TEMPLATE_EXTRACT_PYTOM)


def _subtomo_instance_for_species(state, species_id: str) -> tuple | None:
    """The SUBTOMO_EXTRACTION instance for this species — its job model carries the
    box/bin/crop a per-list extraction must match; its dir holds the 'auto' optset."""
    return _instance_for_species(state, species_id, JobType.SUBTOMO_EXTRACTION)


def subtomo_job_dir_for_species(state, species_id: str, project_path: Path) -> Path | None:
    """The ``SUBTOMO_EXTRACTION`` job dir attached to this species — the source of the
    'auto' ``optimisation_set`` and the per-tomo curation accounting."""
    inst = _subtomo_instance_for_species(state, species_id)
    return _job_dir(state, inst[0], inst[1], project_path) if inst else None


# ── extraction inputs ────────────────────────────────────────────────────────────────────


def extraction_params_for_species(state, species_id: str, subtomo_jm=None) -> dict | None:
    """The ``backend.extract_pick_list`` geometry kwargs for one species, or **None** when
    the species has no committed extraction geometry.

    Precedence: the species' SUBTOMO_EXTRACTION job model (whatever the auto set was cut
    with, so a manual list stays mixable with it) → ``species.extraction_params``
    (committed through the extract dialog for a de-novo species). There is deliberately NO
    third branch: the old ``384/1.0/224`` fallback silently cut wrong-but-plausible
    subtomograms for every species that never ran a subtomo job, and a box size is not
    ours to guess (denovo roadmap D-3). None means "ask the user", never "use a default".

    ``max_dose=-1`` / ``min_frames=1`` ARE defaults, legitimately: they are the tool's own
    "no limit" sentinels (the driver omits the flags entirely at those values), and
    stack2d/float16 are output-representation choices, not sample geometry.
    """
    jm = subtomo_jm
    if jm is None:
        sub = _subtomo_instance_for_species(state, species_id)
        jm = sub[1] if sub else None

    box = binning = crop = None
    if jm is not None:
        box, binning, crop = getattr(jm, "box_size", None), getattr(jm, "binning", None), getattr(jm, "crop_size", None)
    if not (box and binning and crop):
        species = state.get_species(species_id) if hasattr(state, "get_species") else None
        ep = getattr(species, "extraction_params", None) if species is not None else None
        if ep is None:
            return None
        box, binning, crop = ep.box_size, ep.binning, ep.crop_size

    return dict(
        box_size=int(box),
        binning=float(binning),
        crop_size=int(crop),
        max_dose=float(getattr(jm, "max_dose", -1.0)),
        min_frames=int(getattr(jm, "min_frames", 1) or 1),
        do_stack2d=bool(getattr(jm, "do_stack2d", True)),
        do_float16=bool(getattr(jm, "do_float16", True)),
    )


def extract_inputs_for_list(state, project_path: Path, species_id: str, pl) -> dict | None:
    """Resolve everything ``backend.extract_pick_list`` needs for ONE workbench pick list:
    the schema source, the consumed list star (prefer ``<stem>_filtered.star``), and the
    extraction geometry. Returns ``{candidate_optset | tomograms_star, list_star, params}``,
    or None when something required is missing (so the caller reports the list BLOCKED rather
    than submitting a doomed job) — see ``extract_inputs_blocked_reason`` for which.

    Schema source: the species' candidate ``optimisation_set.star``
    (TEMPLATE_EXTRACT_PYTOM — its ``candidates.star`` schema is mirrored) when it exists;
    otherwise the tomogram's ``tomograms.star``, which the candidate-free builder
    synthesizes schema + optics from (a de-novo species has no candidate-extract job)."""
    project_path = Path(project_path)
    star = getattr(pl, "path", "")
    if not star:
        return None
    filtered = picks_filter.filtered_list_path(Path(star))
    list_star = str(filtered) if filtered.exists() else str(star)

    params = extraction_params_for_species(state, species_id)
    if params is None:
        return None

    source = _schema_source_for_list(state, project_path, species_id, pl)
    if source is None:
        return None
    return {**source, "list_star": list_star, "params": params}


def _schema_source_for_list(state, project_path: Path, species_id: str, pl) -> dict | None:
    """``{"candidate_optset": ...}`` or ``{"tomograms_star": ...}`` — exactly one, matching
    ``backend.extract_pick_list``'s two schema sources. None when neither is on disk."""
    cand = _candidate_instance_for_species(state, species_id)
    if cand is not None:
        cand_dir = _job_dir(state, cand[0], cand[1], project_path)
        if cand_dir is not None and (cand_dir / "optimisation_set.star").exists():
            return {"candidate_optset": str(cand_dir / "optimisation_set.star")}

    # No candidate-extract job (or it never produced an optset): fall back to the
    # tomogram's own star, which carries the optics the synthesized block needs.
    from services.visualization.tomo_geometry import geometry_for_ts

    geom = geometry_for_ts(state, project_path, getattr(pl, "tomo_name", ""))
    if geom is None:
        return None
    return {"tomograms_star": str(geom.tomograms_star)}


def extract_inputs_blocked_reason(state, project_path: Path, species_id: str, pl) -> str:
    """Why ``extract_inputs_for_list`` returned None, in the user's terms. Kept next to it
    so a blocked entry names the actual gap instead of one catch-all string."""
    if not getattr(pl, "path", ""):
        return "the list has no backing star file"
    if extraction_params_for_species(state, species_id) is None:
        return "no extraction geometry — set box/binning/crop on the species before extracting"
    if _schema_source_for_list(state, Path(project_path), species_id, pl) is None:
        return "no candidate optimisation_set and no tomograms.star for this tomogram"
    return "unknown"


# ── what still needs cutting ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PendingExtraction:
    """One pick list of a species whose subtomograms are missing or out of date.

    ``blocked_reason`` empty ⇒ extraction alone fixes it (``backend.extract_pending_lists``
    submits exactly these). Non-empty ⇒ something else has to happen first, and the string
    says what — never dropped silently, and never submitted as a doomed job.
    """

    species_id: str
    tomo_name: str
    slug: str
    label: str
    extraction_state: str  # ListExtractionState value: NOT_EXTRACTED | STALE
    # The optimisation_set a PREVIOUS extraction of this list produced, "" when there is
    # none. Present precisely when the list is STALE, which is what lets a merge tell
    # "a source you selected is behind its picks" from "a list that was never cut".
    extracted_path: str
    blocked_reason: str


def pending_extractions(state, project_path: Path, species_id: str) -> list[PendingExtraction]:
    """Every workbench pick list of ``species_id`` that is NOT_EXTRACTED or STALE, in
    (tomogram, slug) order, each carrying its blocked reason or "".

    The ``auto`` candidate set is deliberately absent: it is not a ``PickList`` and is never
    cut per list — re-running the SUBTOMO_EXTRACTION job is what refreshes it.

    Touches disk (a ``filtered_count`` sync per list plus the input probes), so callers run
    it off the event loop.
    """
    project_path = Path(project_path)
    out: list[PendingExtraction] = []
    for pl in sorted(
        (p for p in (getattr(state, "pick_lists", None) or []) if p.species_id == species_id),
        key=lambda p: (p.tomo_name, p.slug),
    ):
        # Headless correctness: sync from disk so extraction_state() doesn't read STALE off a
        # None cache — that cache otherwise only self-heals on a render path.
        picks_filter.sync_filtered_count(pl)
        est = pl.extraction_state()
        if est == ListExtractionState.EXTRACTED:
            continue
        blocked = ""
        if extract_inputs_for_list(state, project_path, species_id, pl) is None:
            blocked = extract_inputs_blocked_reason(state, project_path, species_id, pl)
        out.append(
            PendingExtraction(
                species_id=species_id,
                tomo_name=pl.tomo_name,
                slug=pl.slug,
                label=pl.label or pl.slug,
                extraction_state=est.value,
                extracted_path=str(pl.extracted_path or ""),
                blocked_reason=blocked,
            )
        )
    return out


# ── consumer wiring — the module's one mutator ───────────────────────────────────────────


def apply_aggregation_overrides(state) -> int:
    """For aggregation projects with a completed merge, point every consumer
    job's input_optimisation slot at the active merged optimisation_set.star.
    Idempotent. Returns count of jobs updated.

    Wires via the synthetic merged-sources producer's `source_key` (not a bare
    `manual:` path) so the IO-config dropdown shows "Merged sources — <name>"
    selected and the merged optset resolves like any normal producer.

    Writes `source_overrides[slot]` (the resolver key — the single source of
    truth: the driver's path is re-resolved from it at deploy). Also pre-populates
    `paths[slot]` for pre-deploy UI display only; that value is discarded and
    rebuilt by resolve_all_paths at deploy, so it never reaches the driver.

    Also clears stale `is_orphaned` / `missing_inputs` markers since they
    were written before the override existed.

    INVOCATION-scoped, deliberately (de-novo S6). Callers:
      - a new RP/Class3D/Refine3D is added while a merge is active;
      - a merge finished (retro-wires already-added consumers);
      - the merge card set a different merge active.

    It used to be render-scoped as well ("self-heal on every workspace render") and gated on
    `state.is_aggregation`. That flag is gone, so a render-scoped mutator would now run for
    EVERY project on every render — new, riskier behaviour rather than the same behaviour
    with one fewer flag. A project with an active merge opted into this wiring by creating
    the merge; nothing needs to re-decide it on a render. The early return below is now the
    only guard: no active merged optset ⇒ nothing to wire.
    """
    optset = state.active_merged_optset()
    if optset is None or not optset.exists():
        logger.debug("apply_aggregation_overrides: no active merged optset")
        return 0

    optset_str = str(optset)
    # source_key of the synthetic merged-sources candidate (path_resolution_service.
    # _add_merged_sources_candidates). instance_path is shared via ProjectState so the
    # two always agree; the `or` mirrors the candidate's same fallback defensively.
    instance_path = state.active_merged_optset_instance_path() or MERGED_DIR_NAME
    override_value = f"{JobType.MERGED_SOURCES.value}:{instance_path}"
    updated = 0
    for instance_id, job_model in state.jobs.items():
        schema = getattr(type(job_model), "INPUT_SCHEMA", None) or []
        slot_keys = [s.key for s in schema if s.accepts and JobFileType.OPTIMISATION_SET_STAR in s.accepts]
        if not slot_keys:
            continue
        if getattr(job_model, "source_overrides", None) is None:
            job_model.source_overrides = {}
        if getattr(job_model, "paths", None) is None:
            job_model.paths = {}
        changed = False
        for k in slot_keys:
            if job_model.source_overrides.get(k) != override_value:
                job_model.source_overrides[k] = override_value
                changed = True
            # Pre-populate paths so the driver finds the optset even if path
            # resolution at deploy time somehow loses the override.
            if job_model.paths.get(k) != optset_str:
                job_model.paths[k] = optset_str
                changed = True
        # Clear stale orphan markers — they were written before the override
        # existed and would otherwise stick around forever.
        if changed:
            if getattr(job_model, "is_orphaned", False):
                job_model.is_orphaned = False
            if getattr(job_model, "missing_inputs", None):
                job_model.missing_inputs = []
            updated += 1
            logger.info("apply_aggregation_overrides: wired %s slots %s -> %s", instance_id, slot_keys, optset_str)
    return updated
