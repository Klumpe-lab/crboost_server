"""Headless per-species pick / extraction overview (roadmap 09-S3).

One read model for "where does this species stand": every pick list on every tomogram
as a ``ListRow`` (count · kept · extraction state · provenance) plus the species roll-up
(``SpeciesOverview``). The Species page (roadmap 10 status block, 11 Picks tab) and the CLI
below render it; nothing here draws.

Composition only — the facts come from the readers that already own the disk work:
``aggregation.discovery.load_tomo_curation`` / ``counts_by_tomo`` (per-tomo auto
accounting), ``aggregation.extraction.subtomo_job_dir_for_species``, ``dashboard_data``
(bound candidate-extract / subtomo instances), ``PickList.extraction_state()`` after a
headless ``picks_filter.sync_filtered_count``. Takes an EXPLICIT ``ProjectState`` (never the
tab-context accessor); callers run it off the event loop.

Since roadmap 07 each workbench row also carries its ``ExtractJob``: the per-list
extraction instance's live status, failure text and geometry. That is the transient half —
``extraction_state`` stays the authority on whether the list IS extracted — and it is an
in-memory ``state.jobs`` read, so it costs nothing next to the star reads above.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.aggregation.extraction import subtomo_job_dir_for_species
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


def tm_origin_for_ce(state, ce_iid: str | None) -> tuple[str, str, str]:
    """``(template_path, mask_path, note)`` — the template + mask that produced ONE
    candidate-extract instance's picks (roadmap 09-S4).

    A SNAPSHOT chain, not a guess: the candidate-extract instance recorded the ``tmResults``
    directory it actually consumed in ``paths["input_tm_job"]``, and that directory's parent
    IS the template-match job dir. So the answer stays right for a species that has since
    acquired a second template-match instance, and it is an in-memory ``state.jobs`` read —
    no disk, safe on a render / signature path.

    All three empty = no template was involved at all, which is the de-novo answer, not a
    gap: with no candidate-extract instance the auto row's counts came out of the subtomo
    job's own curation records. Otherwise a missing link leaves both paths empty and puts a
    SHORT reason in ``note``, which the row prints in place of a name (the cell is narrow;
    its tooltip carries the sentence). Nothing here falls back to "the species' template".
    """
    if not ce_iid:
        return "", "", ""
    ce_jm = state.jobs.get(ce_iid)
    if ce_jm is None:
        return "", "", "job gone"
    tm_results = str((getattr(ce_jm, "paths", None) or {}).get("input_tm_job", "") or "")
    if not tm_results:
        return "", "", "no TM input recorded"
    tm_dir = str(Path(tm_results).parent)
    for jm in state.jobs.values():
        if str((getattr(jm, "paths", None) or {}).get("job_dir", "")) == tm_dir:
            return str(getattr(jm, "template_path", "") or ""), str(getattr(jm, "mask_path", "") or ""), ""
    return "", "", "TM job gone"


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
    extraction_state: str  # ListExtractionState value, or NOT_APPLICABLE
    star_path: str | None  # the list's coordinates star (auto: the CE job's candidates.star)
    extracted_path: str | None  # the optimisation_set produced from this list, when one exists
    source_kind: str  # PickSourceKind value ("" on lists that pre-date the field)
    source_ref: str
    extract_job: ExtractJob | None  # the live per-list extraction job (roadmap 07); None = never submitted
    # ORIGIN (picking-UI roadmap 09-S4) — which template and mask actually produced these
    # coordinates. Full paths, so the row can name the file and hover the whole path; "" on a
    # list no template produced (manual / imported / merged). ``origin_note`` carries the reason
    # the chain could NOT be resolved for a list that should have one, and is shown INSTEAD of a
    # name — never as a fallback guess (CLAUDE.md "Surfacing uncertainty").
    template_path: str
    mask_path: str
    origin_note: str


@dataclass(frozen=True, slots=True)
class SpeciesOverview:
    species_id: str
    n_tomos_with_picks: int
    n_picks: int  # sum of `count` over rows
    n_kept: int  # sum of kept (falls back to count where no filter is committed)
    n_extracted_lists: int  # rows in EXTRACTED state
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

    # One resolution per species: every auto row of this species came out of the same
    # candidate-extract instance, so the template+mask behind them is the same too.
    tmpl_path, tmpl_mask, tmpl_note = tm_origin_for_ce(state, ce[0] if ce else None)
    tomo_names = set(auto_counts) | {pl.tomo_name for pl in state.pick_lists if pl.species_id == species_id}
    rows: list[ListRow] = []
    for tomo in sorted(tomo_names):
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
                    extraction_state=auto_state,
                    star_path=str(ce_dir / "candidates.star") if ce_dir else None,
                    extracted_path=str(auto_optset) if auto_optset else None,
                    source_kind=PickSourceKind.TM.value,
                    source_ref=ce[0] if ce else "",
                    extract_job=None,  # the auto list follows its subtomo job; it is never cut per list
                    template_path=tmpl_path,
                    mask_path=tmpl_mask,
                    origin_note=tmpl_note,
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
                    extraction_state=pl.extraction_state().value,
                    star_path=pl.path or None,
                    extracted_path=pl.extracted_path or None,
                    source_kind=pl.source_kind,
                    source_ref=pl.source_ref,
                    extract_job=extract_job_for(state, species_id, tomo, pl.slug),
                    # Hand-placed / imported / merged coordinates: no template produced them,
                    # and saying so is a fact, not a gap to flag.
                    template_path="",
                    mask_path="",
                    origin_note="",
                )
            )

    return SpeciesOverview(
        species_id=species_id,
        n_tomos_with_picks=len({r.tomo_name for r in rows if r.count > 0}),
        n_picks=sum(r.count for r in rows),
        n_kept=sum(r.kept if r.kept is not None else r.count for r in rows),
        n_extracted_lists=sum(1 for r in rows if r.extraction_state == ListExtractionState.EXTRACTED.value),
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
            job = r.extract_job.status if r.extract_job is not None else "-"
            print(
                f"  {r.tomo_name:42s} {r.slug:18s} {r.list_type:9s} {r.extraction_state:14s} "
                f"job={job:10s} kept/total={kt:9s} {r.source_kind:7s} {r.source_ref}"
            )
        origin = next((r for r in ov.rows if r.template_path or r.origin_note), None)
        if origin is not None:
            tm, mk = Path(origin.template_path).name or "-", Path(origin.mask_path).name or "-"
            note = f"  [{origin.origin_note}]" if origin.origin_note else ""
            print(f"  -- origin: template={tm} mask={mk}{note}")
        print(
            f"  -- {ov.n_tomos_with_picks} tomo(s) with picks · {ov.n_picks} picks · {ov.n_kept} kept · "
            f"{ov.n_extracted_lists} extracted list(s)"
        )


if __name__ == "__main__":
    _main()
