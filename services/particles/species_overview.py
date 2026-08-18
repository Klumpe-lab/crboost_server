"""Headless per-species pick / extraction overview (roadmap 09-S3).

One read model for "where does this species stand": every pick list on every tomogram
as a ``ListRow`` (count · kept · authoritative · extraction state · provenance) plus the
species roll-up (``SpeciesOverview``). The Species page (roadmap 10 status block, 11 Picks
tab) and the CLI below render it; nothing here draws.

Composition only — the facts come from the readers that already own the disk work:
``aggregation.discovery.load_tomo_curation`` / ``counts_by_tomo`` (per-tomo auto
accounting), ``aggregation.authoritative.enumerate_authoritative`` + ``compute_gate_report``
(the authoritative choice per tomo and the roll-up gate), ``dashboard_data`` (bound
candidate-extract / subtomo instances), ``PickList.extraction_state()`` after a headless
``picks_filter.sync_filtered_count``. Takes an EXPLICIT ``ProjectState`` (never the
tab-context accessor); callers run it off the event loop.

Since roadmap 07 each workbench row also carries its ``ExtractJob``: the per-list
extraction instance's live status, failure text and geometry. That is the transient half —
``extraction_state`` stays the authority on whether the list IS extracted — and it is an
in-memory ``state.jobs`` read, so it costs nothing next to the star reads above.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.aggregation.authoritative import compute_gate_report, enumerate_authoritative, subtomo_job_dir_for_species
from services.aggregation.discovery import counts_by_tomo, load_tomo_curation
from services.dashboard_data import ce_instance_for_species, job_dir_for, matching_subtomo_instance
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles import picks_filter
from services.particles.list_ref import extract_pick_list_instance_id

AUTO_SLUG = "auto"
NOT_APPLICABLE = "n/a"  # auto list of a species with no subtomo job: nothing to be extracted or stale


@dataclass(frozen=True, slots=True)
class ExtractJob:
    """The per-list extraction JOB behind one row (roadmap 07) — the LIVE half of the story
    ``extraction_state`` tells durably: what the last submit is doing right now, why it
    failed, and with which geometry it cut.

    ``None`` on a row that has no instance, which is the normal state twice over: the
    ``auto`` list is never extracted per list, and a workbench list that has never been
    submitted has nothing to report. Read from ``state.jobs`` only — no disk — so callers
    may put it on a render/signature path."""

    instance_id: str
    status: str  # JobStatus value: Queued → Running → Succeeded | Failed
    error: str  # the driver's own failure text (result.json), "" when none
    job_dir: str  # where qsub writes run.out / run.err; "" when the submit never recorded it
    slurm_job_id: str
    box_size: int  # the geometry this instance last cut with — 0 until a submit writes it
    binning: float
    crop_size: int

    @property
    def is_live(self) -> bool:
        """Submitted and not finished (Scheduled / Queued / Running). Re-extracting the list
        wipes this run's out dir from under it, so an action that would resubmit has to say so
        first — and must never be blocked outright, because a watcher lost to a server restart
        leaves the instance reading Running with nothing left to move it."""
        return self.status in (JobStatus.SCHEDULED.value, JobStatus.QUEUED.value, JobStatus.RUNNING.value)


def extract_job_for(state, species_id: str, tomo_name: str, slug: str) -> ExtractJob | None:
    """The per-list extraction instance for ONE list, or None when it was never submitted.

    Public because the row builder is not the only caller: anything about to destroy a list's
    output directory (delete, re-extract) has to know whether a job is currently writing into
    it, and this is the one read that answers that."""
    iid = extract_pick_list_instance_id(species_id, tomo_name, slug)
    jm = state.jobs.get(iid)
    if jm is None:
        return None
    return ExtractJob(
        instance_id=iid,
        status=jm.execution_status.value,
        error=jm.last_error,
        job_dir=jm.paths.get("job_dir", ""),
        slurm_job_id=jm.slurm_job_id or "",
        box_size=jm.box_size,
        binning=jm.binning,
        crop_size=jm.crop_size,
    )


@dataclass(frozen=True, slots=True)
class ListRow:
    """One pick list on one tomogram."""

    species_id: str
    tomo_name: str
    slug: str
    list_type: str  # PickListType value
    label: str
    count: int
    kept: int | None  # None = no keep/drop committed (all `count` kept)
    is_authoritative: bool
    extraction_state: str  # ListExtractionState value, or NOT_APPLICABLE
    star_path: str | None  # the list's coordinates star (auto: the CE job's candidates.star)
    extracted_path: str | None  # the optimisation_set produced from this list, when one exists
    source_kind: str  # PickSourceKind value ("" on lists that pre-date the field)
    source_ref: str
    extract_job: ExtractJob | None  # the live per-list extraction job (roadmap 07); None = never submitted


@dataclass(frozen=True, slots=True)
class SpeciesOverview:
    species_id: str
    n_tomos_with_picks: int
    n_picks: int  # sum of `count` over rows
    n_kept: int  # sum of kept (falls back to count where no filter is committed)
    n_extracted_lists: int  # rows in EXTRACTED state
    gate: str  # READY | PENDING | BLOCKED — compute_gate_report roll-up (READY is vacuous with no rows)
    ce_iid: str | None
    subtomo_iid: str | None
    rows: tuple[ListRow, ...]


def species_overview(state, project_path: Path, species_id: str) -> SpeciesOverview:
    """Rows for every (tomogram, list) of ``species_id`` + the roll-up. Tomo universe =
    the subtomo job's tomograms (else the CE job's candidate tomograms) ∪ every tomogram
    with a persisted workbench list. The auto row's count/kept follow the subtomo job's
    accounting when that job exists (what aggregation forwards), else the CE job's
    ``candidates.star`` with ``extraction_state == "n/a"``."""
    project_path = Path(project_path)
    ce = ce_instance_for_species(state, species_id)
    sub = matching_subtomo_instance(state, species_id)
    ce_dir = job_dir_for(state, ce[0], ce[1], project_path) if ce else None
    subtomo_dir = subtomo_job_dir_for_species(state, species_id, project_path)

    # Per-tomo auto accounting: {tomo: (total, kept)}; kept None = no filter committed.
    auto_counts: dict[str, tuple[int, int | None]] = {}
    curation_by_tomo: dict[str, dict] = {}
    if subtomo_dir is not None:
        for tc in load_tomo_curation(str(subtomo_dir)):
            curation_by_tomo[tc.ts_name] = {"kept": tc.kept, "total": tc.total, "reviewed": tc.reviewed}
            auto_counts[tc.ts_name] = (tc.total, tc.kept)
    elif ce_dir is not None:
        auto_counts = {t: (n, None) for t, n in counts_by_tomo(ce_dir / "candidates.star").items()}
    auto_optset: Path | None = None
    if subtomo_dir is not None:
        canonical = picks_filter.resolve_canonical_optset(subtomo_dir)
        auto_optset = canonical if canonical.exists() else None

    handles = enumerate_authoritative(state, project_path, species_id, curation_by_tomo=curation_by_tomo)
    report = compute_gate_report(handles)
    gate = "BLOCKED" if report.blocked else ("PENDING" if report.pending else "READY")

    tomo_names = set(auto_counts) | {pl.tomo_name for pl in state.pick_lists if pl.species_id == species_id}
    rows: list[ListRow] = []
    for tomo in sorted(tomo_names):
        auth_slug = state.get_authoritative_slug(species_id, tomo)
        total, kept = auto_counts.get(tomo, (0, None))
        if total > 0:
            if subtomo_dir is None:
                auto_state = NOT_APPLICABLE
            else:
                auto_state = (ListExtractionState.EXTRACTED if auto_optset else ListExtractionState.NOT_EXTRACTED).value
            rows.append(
                ListRow(
                    species_id=species_id,
                    tomo_name=tomo,
                    slug=AUTO_SLUG,
                    list_type=PickListType.AUTO.value,
                    label="candidates",
                    count=total,
                    kept=kept,
                    is_authoritative=auth_slug in (AUTO_SLUG, PickListType.FILTERED.value),
                    extraction_state=auto_state,
                    star_path=str(ce_dir / "candidates.star") if ce_dir else None,
                    extracted_path=str(auto_optset) if auto_optset else None,
                    source_kind=PickSourceKind.TM.value,
                    source_ref=ce[0] if ce else "",
                    extract_job=None,  # the auto list follows its subtomo job; it is never cut per list
                )
            )
        for pl in state.get_pick_lists(species_id, tomo):
            picks_filter.sync_filtered_count(pl)  # headless: the cache only self-heals when the Journey renders
            rows.append(
                ListRow(
                    species_id=species_id,
                    tomo_name=tomo,
                    slug=pl.slug,
                    list_type=pl.list_type.value,
                    label=pl.label or pl.slug,
                    count=int(pl.count or 0),
                    kept=pl.filtered_count,
                    is_authoritative=auth_slug == pl.slug,
                    extraction_state=pl.extraction_state().value,
                    star_path=pl.path or None,
                    extracted_path=pl.extracted_path or None,
                    source_kind=pl.source_kind,
                    source_ref=pl.source_ref,
                    extract_job=extract_job_for(state, species_id, tomo, pl.slug),
                )
            )

    return SpeciesOverview(
        species_id=species_id,
        n_tomos_with_picks=len({r.tomo_name for r in rows if r.count > 0}),
        n_picks=sum(r.count for r in rows),
        n_kept=sum(r.kept if r.kept is not None else r.count for r in rows),
        n_extracted_lists=sum(1 for r in rows if r.extraction_state == ListExtractionState.EXTRACTED.value),
        gate=gate,
        ce_iid=ce[0] if ce else None,
        subtomo_iid=sub[0] if sub else None,
        rows=tuple(rows),
    )


def all_species_overview(state, project_path: Path) -> list[SpeciesOverview]:
    """One overview per registered species, in registry order."""
    return [species_overview(state, project_path, sp.id) for sp in state.species_registry]


# ── CLI (verification) ─────────────────────────────────────────────────────────────────
# Read-only. Run in the module env (needs pandas/starfile):
#   python -m services.particles.species_overview --project <proj-root> [--species <id>]


def _main() -> None:
    import argparse

    from services.project_state import get_state_service

    ap = argparse.ArgumentParser(description="Print every pick list of a species per tomogram + roll-up (read-only).")
    ap.add_argument("--project", required=True, help="project root (dir containing project_params.json)")
    ap.add_argument("--species", help="species_id; default = every species in the project")
    args = ap.parse_args()

    project_path = Path(args.project)
    state = get_state_service().state_for(project_path)
    overviews = (
        [species_overview(state, project_path, args.species)]
        if args.species
        else all_species_overview(state, project_path)
    )
    if not overviews:
        print("No species in project.")
        return
    for ov in overviews:
        print(f"\n=== species {ov.species_id}  ce={ov.ce_iid or '-'}  subtomo={ov.subtomo_iid or '-'} ===")
        for r in ov.rows:
            kt = f"{r.kept if r.kept is not None else '-'}/{r.count}"
            auth = "auth" if r.is_authoritative else "    "
            job = r.extract_job.status if r.extract_job is not None else "-"
            print(
                f"  {r.tomo_name:42s} {r.slug:18s} {r.list_type:9s} {auth} {r.extraction_state:14s} "
                f"job={job:10s} kept/total={kt:9s} {r.source_kind:7s} {r.source_ref}"
            )
        print(
            f"  -- {ov.n_tomos_with_picks} tomo(s) with picks · {ov.n_picks} picks · {ov.n_kept} kept · "
            f"{ov.n_extracted_lists} extracted list(s) → gate {ov.gate}"
        )


if __name__ == "__main__":
    _main()
