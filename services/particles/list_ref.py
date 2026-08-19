"""``ListRef`` — the identity of ONE pick list on ONE tomogram, as the shared pick-list
actions consume it (roadmap 11-S1).

The Journey and the Species page describe the same list from different materials (the
Journey from its ``sp`` / ``lst`` render dicts, the Species page from a ``PickList`` plus
the species' bound candidate-extract / subtomo instances); ``ui/particles/list_actions``
takes neither — it takes this. UI-free, frozen, hashable: everything an action needs to
resolve state (``project_path``, ``species_id``, ``tomo_name``, ``slug``), the list's own
star, and the species' job anchors (the CE job dir for ``candidates.star`` /
``optimisation_set.star``, the subtomo job dir for the ``auto`` list's kept subset, the
subtomo instance for the extraction geometry) — plus the tomogram's ``tomograms.star``,
which the ArtiaX round trip needs even for a species with no jobs at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.dashboard_data import ce_instance_for_species, job_dir_for, matching_subtomo_instance
from services.models_base import InstanceId, JobType, PickListType
from services.visualization.tomo_geometry import geometry_for_ts, read_tomo_table, tomogram_star_sources

AUTO_SLUG = "auto"


def fs_slug(name: str) -> str:
    """Filesystem-safe slug (no regex dep): merged-list slugs (``merged__<name>``) and the
    Journey's cutout-cache file names."""
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in str(name)).strip("_") or "x"


def extract_pick_list_instance_id(species_id: str, tomo_name: str, slug: str) -> str:
    """Instance id of the per-list extraction job for ONE pick list (roadmap 07).

    Keyed on the full ``(species, tomo, slug)`` triple for the same reason
    ``path_resolution_service.pick_list_producer_id`` is: ``PickList.slug`` is unique only
    WITHIN a (species, tomogram), and every hand-picked list is minted with the literal
    ``slug="manual"`` (``services/particles/ingest.py``), so a slug-only id would collapse
    every manual list in the project onto ONE instance — the second submit silently
    overwriting the first's geometry, status and error.

    Not string-equal to the producer id and not derivable from it: the tomogram name is
    slugged here because this id is interpolated unquoted into the driver launch command
    (``services.jobs.spec.driver_invocation``), whereas the producer id embeds the raw
    name. Both are built from the triple; neither is parsed out of the other.
    """
    return str(InstanceId(JobType.EXTRACT_PICK_LIST, f"{species_id}__{fs_slug(tomo_name)}__{slug}"))


@dataclass(frozen=True, slots=True)
class ListRef:
    project_path: Path
    species_id: str
    species_label: str
    tomo_name: str
    slug: str
    label: str
    list_type: PickListType
    star_path: str | None  # this list's coordinates star (auto: the CE job's candidates.star); None = nothing on disk
    ce_job_dir: Path | None  # the species' candidate-extract job dir (None for a de-novo species)
    subtomo_job_dir: Path | None  # the species' subtomo-extraction job dir (None when it never ran)
    subtomo_iid: str | None  # that job's instance id — its model carries the extraction geometry
    tomograms_star: Path | None  # the tomogram's tomograms.star (the CE job's copy when it has one)

    @property
    def is_auto(self) -> bool:
        return self.slug == AUTO_SLUG

    @property
    def candidates_star(self) -> Path | None:
        """The species' reference pick list for an ArtiaX session — None for a species
        with no candidate-extract job (the session opens with an empty pick set)."""
        return self.ce_job_dir / "candidates.star" if self.ce_job_dir else None

    def merge_source_dict(self) -> dict:
        """The render-dict shape ``picks_filter.merge_source_for`` looks a list up by."""
        return {"slug": self.slug, "path": self.star_path, "list_type": self.list_type}


# ── Building refs from ProjectState (the Species page path) ───────────────────────────


@dataclass(frozen=True, slots=True)
class SpeciesAnchors:
    """The job anchors every ref of one species shares: resolved once per compute."""

    species_label: str
    ce_job_dir: Path | None
    subtomo_job_dir: Path | None
    subtomo_iid: str | None


def species_anchors(state, project_path: Path, species_id: str) -> SpeciesAnchors:
    """Resolve the species' candidate-extract / subtomo instances to (label, dirs, iid) the
    same way the Journey's collectors do (``ce_instance_for_species`` /
    ``matching_subtomo_instance`` + ``job_dir_for``)."""
    project_path = Path(project_path)
    species = state.get_species(species_id)
    ce = ce_instance_for_species(state, species_id)
    sub = matching_subtomo_instance(state, species_id)
    return SpeciesAnchors(
        species_label=str(getattr(species, "name", "") or species_id),
        ce_job_dir=job_dir_for(state, ce[0], ce[1], project_path) if ce else None,
        subtomo_job_dir=job_dir_for(state, sub[0], sub[1], project_path) if sub else None,
        subtomo_iid=sub[0] if sub else None,
    )


def _star_tomograms(star_path: Path) -> set[str]:
    """Every ``rlnTomoName`` a ``tomograms.star`` carries (empty when it is absent or has
    no such column). Memoized by mtime inside ``read_tomo_table``, so asking per tomogram
    costs one parse per file."""
    df = read_tomo_table(star_path)
    if df is None or "rlnTomoName" not in df.columns:
        return set()
    return set(df["rlnTomoName"].astype(str))


def tomograms_star_for(state, project_path: Path, species_id: str, tomo_name: str) -> Path | None:
    """The ``tomograms.star`` an ArtiaX round trip / .coords import maps this tomogram
    through: the candidate-extract job's own copy when that star actually lists the tomogram
    (a denoised chain repoints the volume there — it must stay authoritative, as in the
    Journey), else the geometry provider's star; None when nothing describes the tomo. A CE
    star that does not carry this row is NOT a fallback: claiming it would make callers read
    `has_geometry` as true and fail later inside ``prepare_curation_bundle``. Reads disk — run
    off the event loop."""
    project_path = Path(project_path)
    ce = ce_instance_for_species(state, species_id)
    if ce is not None:
        jd = job_dir_for(state, ce[0], ce[1], project_path)
        if jd is not None and tomo_name in _star_tomograms(jd / "tomograms.star"):
            return jd / "tomograms.star"
    geom = geometry_for_ts(state, project_path, tomo_name)
    return Path(geom.tomograms_star) if geom is not None else None


def list_ref_for(
    project_path: Path,
    anchors: SpeciesAnchors,
    species_id: str,
    tomo_name: str,
    slug: str,
    *,
    label: str,
    list_type: PickListType | str,
    star_path: str | None,
    tomograms_star: Path | None,
) -> ListRef:
    """A ref from ``species_overview`` row facts (``ListRow``) + the species anchors."""
    lt = list_type if isinstance(list_type, PickListType) else PickListType(list_type)
    return ListRef(
        project_path=Path(project_path),
        species_id=species_id,
        species_label=anchors.species_label,
        tomo_name=tomo_name,
        slug=slug,
        label=label,
        list_type=lt,
        star_path=star_path,
        ce_job_dir=anchors.ce_job_dir,
        subtomo_job_dir=anchors.subtomo_job_dir,
        subtomo_iid=anchors.subtomo_iid,
        tomograms_star=tomograms_star,
    )


def _species_tomograms(state, species_id: str, anchors: SpeciesAnchors) -> set[str]:
    """The tomograms this SPECIES knows about, which the project-level stars need not
    mention: every persisted pick list's, plus every row of its candidate-extract job's own
    ``tomograms.star``. ``known_tomograms`` reads only the reconstruct and imported stars
    (``tomogram_star_sources``), so a species whose volumes arrived through a merge — or
    only through a CE job's own copy — would otherwise draw rows in the Journey and none
    on the Species page."""
    out = {pl.tomo_name for pl in state.pick_lists if pl.species_id == species_id}
    if anchors.ce_job_dir is not None:
        out |= _star_tomograms(anchors.ce_job_dir / "tomograms.star")
    return out


def species_tomo_map(
    state, project_path: Path, species_id: str, extra_tomos: tuple[str, ...] = ()
) -> tuple[SpeciesAnchors, dict[str, Path | None]]:
    """``(anchors, {tomo: tomograms.star | None})`` for every tomogram this species could
    hold picks on: ``extra_tomos`` (the caller's own rows) ∪ what the species itself knows
    (``_species_tomograms``) ∪ every tomogram the project describes. ONE disk pass that both
    Species-page tabs build their refs from — the Picks table needs a star per row's
    tomogram, the Curation tab one per tomogram it lists, and a tomogram with no picks yet
    is a legitimate import target for both. Reads disk — run off the event loop."""
    anchors = species_anchors(state, project_path, species_id)
    tomos = sorted(
        set(extra_tomos) | _species_tomograms(state, species_id, anchors) | set(known_tomograms(state, project_path))
    )
    return anchors, {t: tomograms_star_for(state, project_path, species_id, t) for t in tomos}


def auto_ref(
    project_path: Path, anchors: SpeciesAnchors, species_id: str, tomo_name: str, star: Path | None
) -> ListRef:
    """The tomogram's REFERENCE slot as a ref: the species' ``auto`` list (its
    candidates.star when a candidate-extract job exists, nothing for a de-novo species).
    What the per-tomo actions take — ⚡ load into session, Curate in ArtiaX, import a
    ``.coords`` — none of which need a pick list to exist yet."""
    return list_ref_for(
        project_path,
        anchors,
        species_id,
        tomo_name,
        AUTO_SLUG,
        label="candidates",
        list_type=PickListType.AUTO,
        star_path=str(anchors.ce_job_dir / "candidates.star") if anchors.ce_job_dir else None,
        tomograms_star=star,
    )


def known_tomograms(state, project_path: Path) -> list[str]:
    """Every tomogram name a ``tomograms.star`` of this project describes (reconstruct job
    ∪ imported), in source order — the picker universe for "import picks into <tomo>".
    Reads disk — run off the event loop."""
    out: list[str] = []
    seen: set[str] = set()
    for _source, star_path in tomogram_star_sources(state, Path(project_path)):
        df = read_tomo_table(star_path)
        if df is None or "rlnTomoName" not in df.columns:
            continue
        for name in df["rlnTomoName"].astype(str):
            if name not in seen:
                seen.add(name)
                out.append(name)
    return out
