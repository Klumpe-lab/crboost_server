"""One row per tomogram of a species: its pick lists joined with the template match's per-tomogram scores.

`score_rows` reads the score side: the analytics source a species' numbers describe (the chosen one, else the
first succeeded non-control one) and its per-tomogram rows and markers (`services.particles.analytics`). The
Particles registry's Scores tab and its Tomograms table both read it.

`join_tomograms` is the join, pure: the species' list rows (`species_overview`), the tomogram universe (every
tomogram a `tomograms.star` describes, with or without picks) and the score rows, into `TomoRow`s, each carrying its
lists as `TomoList`s. `match_coords` pairs `.coords` files with tomograms by name for an import, and says why a file
it cannot pair stays unpaired.

Headless: no UI, no event loop. `score_rows` reads records (disk), so callers run it off the loop.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from services.array_tasks import ts_display_name, ts_position_sort_key
from services.models_base import ListExtractionState, PickListType
from services.particles import analytics
from services.particles.analytics_store import (
    AnalyticsSource,
    candidates_record,
    job_dir_of,
    sources_for_species,
    subtomo_instances_for_ce,
    subtomo_record,
    table_record,
    tm_health_record,
)
from services.particles.records import Record
from services.particles.set_ops import read_table
from services.particles.sigma import LADDER_SIGMAS
from services.particles.species_overview import NOT_APPLICABLE, ExtractJob, ListRow

logger = logging.getLogger(__name__)


# ── The score side ─────────────────────────────────────────────────────────────


@dataclass
class SubtomoFacts:
    """The newest subtomo extraction made from the source's candidates."""

    instance_id: str
    record: Record | None
    n_curated: int | None  # None: no curated subset exists

    @property
    def n_all(self) -> int | None:
        return None if self.record is None else self.record.data.get("n_out")


@dataclass
class ScoreRows:
    sources: tuple[AnalyticsSource, ...] = ()
    source: AnalyticsSource | None = None
    tm: Record | None = None
    cand: Record | None = None
    subtomo: SubtomoFacts | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)  # `analytics.tomogram_rows`
    markers: list[analytics.Marker] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)  # expected absences ("not run yet")
    errors: list[str] = field(default_factory=list)  # failures, traceback in the server log


def _count_rows(path: Path) -> int | None:
    try:
        return len(read_table(path).df)
    except (OSError, ValueError) as e:
        logger.warning("Could not count rows of %s: %s", path, e)
        return None


def score_rows(state, project_path: Path, species_id: str, wanted: str | None = None) -> ScoreRows:
    """The per-tomogram scores of the source `wanted` names, else the first succeeded non-control source, else the
    first succeeded one, else the first. Reads records (disk): run off the event loop."""
    sources = tuple(sources_for_species(state, species_id, project_path))
    src = (
        next((s for s in sources if s.key == wanted), None)
        or next((s for s in sources if s.succeeded and not s.control), None)
        or next((s for s in sources if s.succeeded), None)
        or (sources[0] if sources else None)
    )
    out = ScoreRows(sources=sources, source=src)
    if src is not None and src.kind != "jobs":
        try:
            out.cand = table_record(project_path, src)
        except FileNotFoundError as e:
            out.notes.append(str(e))
        except Exception as e:  # reported in the tab, traceback to the log
            logger.exception("Analytics record for %s failed", src.key)
            out.errors.append(f"{src.label}: {type(e).__name__}: {e}")
        out.rows = analytics.tomogram_rows(None, out.cand.data if out.cand else None)
    elif src is not None:
        for attr, instance, fetch in (
            ("tm", src.tm_instance, tm_health_record),
            ("cand", src.ce_instance, candidates_record),
        ):
            if not instance:
                continue
            try:
                setattr(out, attr, fetch(project_path, state, instance))
            except FileNotFoundError as e:
                out.notes.append(str(e))  # not run yet: said, not an error
            except Exception as e:  # reported in the tab, traceback to the log
                logger.exception("Analytics record for %s failed", instance)
                out.errors.append(f"{instance}: {type(e).__name__}: {e}")
        if src.ce_instance:
            subtomos = subtomo_instances_for_ce(state, src.ce_instance, project_path)
            sub_dir = job_dir_of(state, project_path, subtomos[0]) if subtomos else None
            if sub_dir is not None and (sub_dir / "particles.star").exists():
                curated = sub_dir / "particles_filtered.star"
                try:
                    sub_rec = subtomo_record(project_path, state, subtomos[0])
                except (OSError, ValueError) as e:
                    logger.exception("Subtomo record for %s failed", subtomos[0])
                    out.errors.append(f"{subtomos[0]}: {type(e).__name__}: {e}")
                    sub_rec = None
                out.subtomo = SubtomoFacts(subtomos[0], sub_rec, _count_rows(curated) if curated.exists() else None)
        out.rows = analytics.tomogram_rows(
            out.tm.data if out.tm else None,
            out.cand.data if out.cand else None,
            out.subtomo.record.data if out.subtomo and out.subtomo.record else None,
        )
        out.markers = analytics.tomogram_markers(out.rows)
    return out


# ── The join ───────────────────────────────────────────────────────────────────

# How the registry names each marker (`analytics.tomogram_markers`); the rule itself is the marker's text.
MARKER_LABELS = {
    "sigma_outlier": "σ outlier",
    "no_peak": "no peak",
    "runtime_outlier": "slow",
    "top_at_edge": "edge max",
    "cap_bound": "cap",
}

_I10 = LADDER_SIGMAS.index(10.0) if 10.0 in LADDER_SIGMAS else None


@dataclass(frozen=True, slots=True)
class TomoInfo:
    """A tomogram of the species' universe, as the caller resolved it."""

    name: str
    has_geometry: bool  # a tomograms.star describes it: without one nothing loads or imports
    n_saves: int  # user .coords in its curation folder
    curation_dir: str
    volume: str  # the reconstruction on disk the picks are on; "" when there is none


@dataclass(frozen=True, slots=True)
class TomoList:
    """One pick list of a tomogram (a sub-row)."""

    slug: str
    label: str
    list_type: str  # PickListType value
    source_kind: str
    source_ref: str
    count: int
    kept: int | None  # None: no keep/drop filter committed
    extraction_state: str  # ListExtractionState value, or NOT_APPLICABLE
    star_path: str
    template_path: str
    mask_path: str
    origin_note: str
    extract_job: ExtractJob | None


@dataclass(frozen=True, slots=True)
class TomoRow:
    name: str
    short: str  # the position name, the one part that differs from row to row
    lists: tuple[TomoList, ...]
    picks: int
    kept: int
    top: float | None  # the highest template-match peak, σ
    ge10: int | None  # picks at or above 10σ
    extracted: str  # "n_out/n_in" of the subtomo extraction, "✓" every list cut and current, "stale", "" none
    markers: tuple[tuple[str, str], ...]  # (label, rule)
    has_geometry: bool
    n_saves: int
    curation_dir: str
    volume: str
    details: str  # the hover of the name

    @property
    def has_actions(self) -> bool:
        """Whether a list of it has actions of its own (an `auto` list follows its subtomo job and has none)."""
        return any(pl.list_type != PickListType.AUTO.value for pl in self.lists)


def _list(r: ListRow) -> TomoList:
    return TomoList(
        slug=r.slug,
        label=r.label,
        list_type=r.list_type,
        source_kind=r.source_kind,
        source_ref=r.source_ref,
        count=r.count,
        kept=r.kept,
        extraction_state=r.extraction_state,
        star_path=r.star_path or "",
        template_path=r.template_path,
        mask_path=r.mask_path,
        origin_note=r.origin_note,
        extract_job=r.extract_job,
    )


def _extracted(lists: Sequence[TomoList], sub: dict[str, Any]) -> str:
    if sub:
        return f"{sub['n_out']}/{sub['n_in']}" if sub.get("n_in") is not None else str(sub.get("n_out", ""))
    states = [pl.extraction_state for pl in lists if pl.extraction_state != NOT_APPLICABLE]
    if not states:
        return ""
    if ListExtractionState.STALE.value in states:
        return "stale"
    if all(s == ListExtractionState.EXTRACTED.value for s in states):
        return "✓"
    return ""


def join_tomograms(
    list_rows: Iterable[ListRow],
    tomos: Iterable[TomoInfo],
    scores: Iterable[dict[str, Any]],
    markers: Iterable[analytics.Marker],
) -> tuple[TomoRow, ...]:
    """One `TomoRow` per tomogram of `tomos` ∪ the tomograms the lists name, in position order."""
    by_tomo: dict[str, list[TomoList]] = {}
    for r in list_rows:
        by_tomo.setdefault(r.tomo_name, []).append(_list(r))
    infos = {t.name: t for t in tomos}
    rows = {r["tomo"]: r for r in scores}
    marks: dict[str, list[tuple[str, str]]] = {}
    for m in markers:
        marks.setdefault(m.tomo, []).append((MARKER_LABELS.get(m.key, m.key), m.text))
    out = []
    for name in sorted(set(infos) | set(by_tomo), key=ts_position_sort_key):
        lists = tuple(by_tomo.get(name, ()))
        a = rows.get(name) or {}
        c = a.get("cand") or {}
        ladder = c.get("ladder") or []
        details = [name]
        if a.get("n_tilts"):
            details.append(f"{a['n_tilts']} tilts")
        if a.get("runtime_min"):
            details.append(f"matched in {a['runtime_min']:.0f} min on {(a.get('node') or '').split('.', 1)[0]}")
        info = infos.get(name)
        out.append(
            TomoRow(
                name=name,
                short=ts_display_name(name),
                lists=lists,
                picks=sum(pl.count for pl in lists),
                kept=sum(pl.kept if pl.kept is not None else pl.count for pl in lists),
                top=a.get("top_sigma"),
                ge10=int(ladder[_I10]) if c and _I10 is not None and _I10 < len(ladder) else None,
                extracted=_extracted(lists, a.get("sub") or {}),
                markers=tuple(marks.get(name, ())),
                has_geometry=bool(info.has_geometry) if info else False,
                n_saves=info.n_saves if info else 0,
                curation_dir=info.curation_dir if info else "",
                volume=info.volume if info else "",
                details=" · ".join(details),
            )
        )
    return tuple(out)


# ── Importing .coords by name ──────────────────────────────────────────────────


def _names_in(stem: str, tomo: str) -> bool:
    """Whether `tomo` appears in `stem` as a whole name: not followed or preceded by a letter or digit, so
    Position_1 is not found in Position_10_2."""
    return re.search(rf"(?<![A-Za-z0-9]){re.escape(tomo)}(?![A-Za-z0-9])", stem) is not None


def match_coords(
    files: Iterable[str], tomo_names: Iterable[str]
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """(file, tomogram) for each `.coords` whose name names exactly one tomogram (or names several of which one
    contains all the others: Position_1_2 over Position_1), and (file, why) for the rest. Nothing is guessed."""
    names = list(tomo_names)
    matched: list[tuple[str, str]] = []
    unmatched: list[tuple[str, str]] = []
    for f in files:
        stem = Path(f).stem
        hits = [t for t in names if _names_in(stem, t)]
        widest = max(hits, key=len) if hits else None
        if widest is None:
            unmatched.append((f, "no tomogram's name is in the file name"))
        elif all(h in widest for h in hits):
            matched.append((f, widest))
        else:
            unmatched.append((f, "the name fits several tomograms: " + ", ".join(sorted(hits))))
    return matched, unmatched
