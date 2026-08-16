"""Authoritative-list → optimisation_set resolution + per-species enumeration.

Read-only foundation (plus ONE mutator, ``apply_aggregation_overrides``, which
wires consumer jobs' input_optimisation slots at the merged optset — it lives
here because it is the write-side twin of this module's resolution logic) for
seamless cross-tomo/cross-project aggregation
(``docs/LIST_EXTRACTION_AND_AGGREGATION.md`` §8.1–8.2): for each
``(species, tomo)`` it resolves the ONE authoritative pick list to a concrete
``optimisation_set`` handle plus its DERIVED extraction state, so an aggregator can see —
without rendering the dashboard — exactly which authoritative lists are extracted, stale,
or missing before rolling them up into a species-level optimisation set.

PURE / HEADLESS by design:
  - Takes an EXPLICIT ``ProjectState`` (never the tab-context accessor), so it is correct
    inside a background task / CLI with no NiceGUI client context (the W2 lesson).
  - Resolves job dirs from the passed state's ``relion_job_name`` / ``job_path_mapping``
    via a local copy of ``services.dashboard_data.job_dir_for`` (kept from when that
    helper still read the client-context global; both are headless now).
  - Imports NO UI module.

This is steps 1–2 of the build order; the gate / auto-extract / roll-up (§8.3+) build on it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from services.io_slots import JobFileType
from services.models_base import JobType, ListExtractionState, resolve_species
from services.project_state import MERGED_DIR_NAME
from services.particles import picks_filter

logger = logging.getLogger(__name__)


@dataclass
class AuthoritativeHandle:
    """The authoritative list for one ``(species, tomo)`` resolved to a forwarding handle.

    ``optset_path`` is the on-disk ``optimisation_set.star`` to forward (present even when
    STALE, so a table can show *where* it is); whether it is safe to forward is decided by
    ``extraction_state`` (EXTRACTED = ready; STALE = re-extract first; NOT_EXTRACTED = extract).
    """

    species_id: str
    tomo_name: str
    slug: str  # the authoritative slug ('auto' default, or a workbench list slug)
    kind: str  # 'auto' | 'filtered' | 'workbench' | 'dangling' (slug with no PickList)
    extraction_state: ListExtractionState
    optset_path: str | None  # optimisation_set to forward (may be stale), or None if unresolved
    kept: int | None  # kept / extracted count for this tomo
    total: int | None  # total picks for this tomo (for 'kept/total')
    notes: list[str] = field(default_factory=list)


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
    'auto' authoritative ``optimisation_set`` and the per-tomo curation accounting."""
    inst = _subtomo_instance_for_species(state, species_id)
    return _job_dir(state, inst[0], inst[1], project_path) if inst else None


# ── resolution ───────────────────────────────────────────────────────────────────────────


def resolve_authoritative_optset(
    state,
    species_id: str,
    tomo_name: str,
    slug: str,
    *,
    subtomo_job_dir: Path | None = None,
    auto_curation: dict | None = None,
) -> AuthoritativeHandle:
    """Resolve one ``(species, tomo)``'s authoritative list to a forwarding handle.

    - ``'auto'`` / ``'filtered'`` — NOT a per-list PickList; the handle is the species'
      SUBTOMO_EXTRACTION ``optimisation_set(_filtered).star`` (filtered-if-curated, via
      ``picks_filter.resolve_canonical_optset``). It is EXTRACTED iff that optset exists
      (the pipeline produced it); slicing to this ``rlnTomoName`` happens later at merge.
    - workbench slug (``manual`` / ``imported`` / ``merged``) — the per-list extraction:
      ``PickList.extraction_state()`` after a headless ``filtered_count`` sync, with the
      ``out/optimisation_set.star`` recorded in ``extracted_path``.

    ``auto_curation`` is an optional ``{kept, total, reviewed}`` for this tomo (from
    ``load_tomo_curation``) so a batch enumerator doesn't re-read the subtomo job per tomo.
    """
    notes: list[str] = []

    if slug in ("auto", "filtered"):
        kind = slug
        if subtomo_job_dir is None:
            notes.append("no SUBTOMO_EXTRACTION job for this species — 'auto' optset unresolved")
            return AuthoritativeHandle(
                species_id, tomo_name, slug, kind, ListExtractionState.NOT_EXTRACTED, None, None, None, notes
            )
        optset = picks_filter.resolve_canonical_optset(Path(subtomo_job_dir))
        kept = total = None
        if auto_curation is not None:
            kept = auto_curation.get("kept")
            total = auto_curation.get("total")
        if not optset.exists():
            notes.append(f"subtomo optimisation_set missing under {subtomo_job_dir}")
            return AuthoritativeHandle(
                species_id, tomo_name, slug, kind, ListExtractionState.NOT_EXTRACTED, None, kept, total, notes
            )
        if slug == "filtered":
            notes.append(
                "'filtered' as authoritative is provisional — treated as the auto job's curated subset (doc §9.1)"
            )
        # The 'auto'/'filtered' set is the pipeline extraction; it has no per-list staleness
        # (re-running the subtomo job is an upstream concern), so existence ⇒ EXTRACTED.
        return AuthoritativeHandle(
            species_id, tomo_name, slug, kind, ListExtractionState.EXTRACTED, str(optset), kept, total, notes
        )

    # Workbench list (manual / imported / merged) — a per-list extraction handle.
    pl = state.get_pick_list(slug, species_id, tomo_name)
    if pl is None:
        # The authoritative dict points at a slug with no PickList (a deleted/renamed list):
        # a dangling choice the gate must surface, NOT silently treat as extractable.
        notes.append(f"authoritative slug '{slug}' has no PickList for ({species_id}, {tomo_name}) — dangling choice")
        return AuthoritativeHandle(
            species_id, tomo_name, slug, "dangling", ListExtractionState.NOT_EXTRACTED, None, None, None, notes
        )
    # Headless correctness: sync filtered_count from disk so extraction_state() doesn't read
    # STALE off a None cache (the render-path-only sync — doc §8.2 note / the badge bug).
    picks_filter.sync_filtered_count(pl)
    est = pl.extraction_state()
    optset = None
    if pl.extracted_path and Path(pl.extracted_path).exists():
        optset = pl.extracted_path  # present even when STALE; extraction_state conveys freshness
    return AuthoritativeHandle(
        species_id, tomo_name, slug, "workbench", est, optset, pl.extracted_count or None, pl.count or None, notes
    )


def enumerate_authoritative(
    state, project_path: Path, species_id: str, *, curation_by_tomo: dict | None = None
) -> list[AuthoritativeHandle]:
    """Every ``(species, tomo)``'s authoritative list resolved to a handle + extraction
    state — the read-only basis for the aggregation gate (doc §8.1). The tomo universe is
    the subtomo job's tomograms (``load_tomo_curation``) ∪ every tomo with a persisted
    workbench list for this species, so manual-only tomos are not missed.

    ``curation_by_tomo`` (``{ts_name: {kept, total, reviewed}}`` for the subtomo job) lets a
    caller that already ran ``load_tomo_curation`` pass it in instead of re-reading
    ``particles.star`` (``services.particles.species_overview``)."""
    project_path = Path(project_path)
    subtomo_dir = subtomo_job_dir_for_species(state, species_id, project_path)

    if curation_by_tomo is None:
        curation_by_tomo = {}
        if subtomo_dir is not None:
            from services.aggregation_discovery import load_tomo_curation

            for tc in load_tomo_curation(str(subtomo_dir)):
                curation_by_tomo[tc.ts_name] = {"kept": tc.kept, "total": tc.total, "reviewed": tc.reviewed}
    tomo_names: set = set(curation_by_tomo)
    for pl in getattr(state, "pick_lists", []):
        if pl.species_id == species_id:
            tomo_names.add(pl.tomo_name)

    out: list[AuthoritativeHandle] = []
    for tomo_name in sorted(tomo_names):
        slug = state.get_authoritative_slug(species_id, tomo_name)
        out.append(
            resolve_authoritative_optset(
                state,
                species_id,
                tomo_name,
                slug,
                subtomo_job_dir=subtomo_dir,
                auto_curation=curation_by_tomo.get(tomo_name),
            )
        )
    return out


# ── extraction inputs + the §8.3 gate ────────────────────────────────────────────────────


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
    or None when something required is missing (so the gate reports the list BLOCKED rather
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
    so the gate's blocked entry names the actual gap instead of one catch-all string."""
    if not getattr(pl, "path", ""):
        return "the list has no backing star file"
    if extraction_params_for_species(state, species_id) is None:
        return "no extraction geometry — set box/binning/crop on the species before extracting"
    if _schema_source_for_list(state, Path(project_path), species_id, pl) is None:
        return "no candidate optimisation_set and no tomograms.star for this tomogram"
    return "unknown"


def handle_to_dict(h: AuthoritativeHandle) -> dict:
    """JSON-safe dict for a handle (enum → its value), for backend / UI consumption."""
    return {
        "species_id": h.species_id,
        "tomo_name": h.tomo_name,
        "slug": h.slug,
        "kind": h.kind,
        "extraction_state": h.extraction_state.value,
        "optset_path": h.optset_path,
        "kept": h.kept,
        "total": h.total,
        "notes": list(h.notes),
    }


@dataclass
class GateReport:
    """§8.3 classification of a species' authoritative lists into the three states that
    decide whether a roll-up can proceed:

    - ``ready`` — EXTRACTED and current (auto/filtered/workbench): forwardable as-is.
    - ``pending`` — workbench lists that are NOT_EXTRACTED / STALE: EXTRACTABLE here
      (``backend.extract_authoritative_pending``).
    - ``blocked`` — not fixable by per-list extraction: an 'auto'/'filtered' tomo whose
      subtomo-job optset is missing (the subtomo job must run), or a 'dangling' slug with no
      PickList (the user must re-choose). The gate must NOT silently proceed past these.
    """

    ready: list[AuthoritativeHandle] = field(default_factory=list)
    pending: list[AuthoritativeHandle] = field(default_factory=list)
    blocked: list[AuthoritativeHandle] = field(default_factory=list)

    @property
    def can_proceed(self) -> bool:
        """True iff every authoritative list is EXTRACTED-and-current (nothing pending,
        nothing blocked) — the precondition for a species roll-up."""
        return not self.pending and not self.blocked

    def to_dict(self) -> dict:
        return {
            "can_proceed": self.can_proceed,
            "counts": {"ready": len(self.ready), "pending": len(self.pending), "blocked": len(self.blocked)},
            "ready": [handle_to_dict(h) for h in self.ready],
            "pending": [handle_to_dict(h) for h in self.pending],
            "blocked": [handle_to_dict(h) for h in self.blocked],
        }


def compute_gate_report(handles: list[AuthoritativeHandle]) -> GateReport:
    """Classify enumerated handles into ready / pending / blocked (see ``GateReport``).
    Pure — no disk, no submission."""
    report = GateReport()
    for h in handles:
        if h.extraction_state == ListExtractionState.EXTRACTED:
            report.ready.append(h)
        elif h.kind == "workbench":  # NOT_EXTRACTED | STALE, with a real PickList → extractable
            report.pending.append(h)
        else:  # auto/filtered with no optset, or a dangling slug → needs other action
            report.blocked.append(h)
    return report


# ── CLI (verification) ─────────────────────────────────────────────────────────────────
# Read-only. Run in the module env (needs pandas/starfile, which Claude's venv lacks):
#   python -m services.aggregation_authoritative --project <proj-root> [--species <id>]


def _main() -> None:
    import argparse

    from services.project_state import get_state_service

    ap = argparse.ArgumentParser(description="Print per-species authoritative-list extraction status (read-only).")
    ap.add_argument("--project", required=True, help="project root (dir containing project_params.json)")
    ap.add_argument("--species", help="species_id; default = every species in the project")
    args = ap.parse_args()

    project_path = Path(args.project)
    state = get_state_service().state_for(project_path)
    species_ids = [args.species] if args.species else sorted({s.id for s in getattr(state, "species_registry", [])})
    if not species_ids:
        print("No species in project.")
        return

    for sid in species_ids:
        print(f"\n=== species {sid} ===")
        handles = enumerate_authoritative(state, project_path, sid)
        if not handles:
            print("  (no tomograms / authoritative lists)")
            continue
        for h in handles:
            kt = f"{h.kept if h.kept is not None else '-'}/{h.total if h.total is not None else '-'}"
            extra = f"  [{'; '.join(h.notes)}]" if h.notes else ""
            print(
                f"  {h.tomo_name:42s} auth={h.slug:18s} {h.kind:9s} {h.extraction_state.value:14s} "
                f"kept/total={kt:9s} {h.optset_path or ''}{extra}"
            )
        report = compute_gate_report(handles)
        print(
            f"  -- {len(handles)} tomo(s): {len(report.ready)} ready, {len(report.pending)} pending-extract, "
            f"{len(report.blocked)} blocked → roll-up {'READY' if report.can_proceed else 'BLOCKED'}"
        )
        for h in report.pending:
            print(f"     · PENDING  {h.tomo_name} / {h.slug} ({h.extraction_state.value})")
        for h in report.blocked:
            reason = "; ".join(h.notes) or h.extraction_state.value
            print(f"     · BLOCKED  {h.tomo_name} / {h.slug} ({h.kind}: {reason})")


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

    Called from three sites:
      - When a new RP/Class3D/Refine3D is added in an aggregation project.
      - After a successful merge (retro-wires already-added consumers).
      - On every workspace render (idempotent self-heal for jobs that pre-date
        either of the above hooks).
    """
    if not getattr(state, "is_aggregation", False):
        return 0
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


if __name__ == "__main__":
    _main()
