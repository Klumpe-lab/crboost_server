"""Authoritative-list → optimisation_set resolution + per-species enumeration.

Read-only foundation for seamless cross-tomo/cross-project aggregation
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

from dataclasses import dataclass, field
from pathlib import Path

from services.models_base import JobType, ListExtractionState
from services.visualization import picks_filter


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


def _species_id_for_job(state, instance_id: str, job_model) -> str | None:
    """species_id a per-particle job attaches to — headless re-implementation of
    ``services.dashboard_data.resolve_species``'s id resolution: ``instance_id`` '__suffix',
    else ``job_model.species_id``, else the single-species fallback."""
    suffix = instance_id.split("__", 1)
    if len(suffix) > 1 and suffix[1]:
        return suffix[1]
    sid = getattr(job_model, "species_id", None)
    if sid:
        return sid
    reg = getattr(state, "species_registry", None) or []
    if len(reg) == 1:
        return reg[0].id
    return None


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
        if _species_id_for_job(state, iid, jm) == species_id:
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


def enumerate_authoritative(state, project_path: Path, species_id: str) -> list[AuthoritativeHandle]:
    """Every ``(species, tomo)``'s authoritative list resolved to a handle + extraction
    state — the read-only basis for the aggregation gate (doc §8.1). The tomo universe is
    the subtomo job's tomograms (``load_tomo_curation``) ∪ every tomo with a persisted
    workbench list for this species, so manual-only tomos are not missed."""
    project_path = Path(project_path)
    subtomo_dir = subtomo_job_dir_for_species(state, species_id, project_path)

    curation_by_tomo: dict = {}
    tomo_names: set = set()
    if subtomo_dir is not None:
        from services.aggregation_discovery import load_tomo_curation

        for tc in load_tomo_curation(str(subtomo_dir)):
            curation_by_tomo[tc.ts_name] = {"kept": tc.kept, "total": tc.total, "reviewed": tc.reviewed}
            tomo_names.add(tc.ts_name)
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


def extract_inputs_for_list(state, project_path: Path, species_id: str, pl) -> dict | None:
    """Resolve everything ``backend.extract_pick_list`` needs for ONE workbench pick list:
    the species candidate ``optimisation_set.star`` (TEMPLATE_EXTRACT_PYTOM job — its
    ``candidates.star`` schema is mirrored), the consumed list star (prefer
    ``<stem>_filtered.star``), and box/bin/crop from the SUBTOMO_EXTRACTION job model
    (defaulting like the dashboard's ``_handle_extract_list`` when there's no subtomo job).
    Returns ``{candidate_optset, list_star, params}``, or None when a required job/file is
    missing (so the gate reports the list BLOCKED rather than submitting a doomed job)."""
    project_path = Path(project_path)
    cand = _candidate_instance_for_species(state, species_id)
    if cand is None:
        return None
    cand_dir = _job_dir(state, cand[0], cand[1], project_path)
    if cand_dir is None:
        return None
    candidate_optset = cand_dir / "optimisation_set.star"
    if not candidate_optset.exists():
        return None
    star = getattr(pl, "path", "")
    if not star:
        return None
    filtered = picks_filter.filtered_list_path(Path(star))
    list_star = str(filtered) if filtered.exists() else str(star)

    sub = _subtomo_instance_for_species(state, species_id)
    jm = sub[1] if sub else None
    params = dict(
        box_size=int(getattr(jm, "box_size", 384) or 384),
        binning=float(getattr(jm, "binning", 1.0) or 1.0),
        crop_size=int(getattr(jm, "crop_size", 224) or 224),
        max_dose=float(getattr(jm, "max_dose", -1.0)),
        min_frames=int(getattr(jm, "min_frames", 1) or 1),
        do_stack2d=bool(getattr(jm, "do_stack2d", True)),
        do_float16=bool(getattr(jm, "do_float16", True)),
    )
    return {"candidate_optset": str(candidate_optset), "list_star": list_star, "params": params}


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


if __name__ == "__main__":
    _main()
