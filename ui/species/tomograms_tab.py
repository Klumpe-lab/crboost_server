"""Tomograms — one table, one row per tomogram of the species.

A pick set is addressed by *which tomogram* and *which species*; everything else it has — its
lists, their counts, the template match's scores on the tomogram, extraction — is a column
of the tomogram's row. The universe is every tomogram the project describes, picks or not: a
tomogram with nothing on it is exactly where de-novo picking starts.

- **The head**: the counts as its meta, its title carrying the where-to-save contract on hover
  (derived from the watcher's own cadence), then `Import picks…` (files or a folder of
  `.coords`, each paired with its tomogram by name, the unpaired listed, never guessed),
  `Extract all pending` and Refresh.
- **One line** saying, when every tomogram holds one list, what that list is; else where the
  scores come from.
- **The watcher**, folded to one line: what the server did with this species' saves, and the
  folders holding a save it could not attribute.
- **The table** (`services/particles/tomogram_table.join_tomograms`): tomogram, picks, kept,
  top σ, ≥ 10σ, extracted, markers, "open in [picks] [napari] [3dmod]", "curate in [ArtiaX]
  [napari]". ArtiaX is the door to curation, with a green dot when the live session has the
  tomogram open; napari's curation is in development, so its button is there and disabled.
  A tomogram with several lists, or with a list of its own (hand-picked, imported, merged),
  opens (▶) to them as sub-rows, each with its actions: extract or re-extract, inspect
  overlaps (merged), the extraction job's logs, copy the `.coords` path, delete. A click on a
  column head sorts by it, in the browser.

The table is one HTML element (`_table_html`) with one click handler: rows, sorting and
opening rebuild nothing on the server. Its data comes from one off-loop `_compute` (star
reads, globs, the analytics records) when the in-memory key (registry rev + job statuses +
watcher log) moves, on show, and at most every `_DISK_REFRESH_S` while shown. The session
poll is the shared, throttled `session_status` cache. Every action is
`ui/particles/list_actions`, so the Journey and this tab drive one implementation; dialogs
are parented at the page layout (`dialog_host`), never inside the table a refresh replaces.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

from nicegui import ui

from services.aggregation.extraction import extraction_params_for_species
from services.array_tasks import ts_position_sort_key
from services.configs.config_service import get_config_service
from services.curation.watcher import FULL_SWEEP_EVERY, SETTLE_SEC, TICK_SEC
from services.dashboard_data import read_tomograms_table, resolve_volume_for_3dmod
from services.models_base import JobStatus, ListExtractionState, PickListType, PickSourceKind
from services.particles.ingest import is_default_slug, register_manual_pick_list
from services.particles.list_ref import ListRef, auto_ref, list_ref_for, species_tomo_map
from services.particles.species_overview import NOT_APPLICABLE, species_overview
from services.particles.tomogram_table import TomoInfo, TomoList, TomoRow, join_tomograms, match_coords, score_rows
from services.project_state import get_project_state_for
from services.visualization import artiax_bridge
from ui.background_task import BackgroundTask
from ui.components.buttons import house_button, icon_button
from ui.components.copyable import _write_clipboard, imod_command
from ui.components.dialogs import dialog_host
from ui.components.reactive import FingerprintedView, SingleFlight
from ui.job_plugins._field_styles import MONO, SANS, TH_VOICE, keep_case, section_head
from ui.local_file_picker import local_file_picker
from ui.napari_open_page import open_url
from ui.napari_session_dialog import open_napari_dialog
from ui.particles import list_actions, session_status
from ui.species.tab import TabContext

logger = logging.getLogger(__name__)

_DISK_REFRESH_S = 15.0  # rows / refs / curation-dir recompute cadence while shown (star reads + globs)
_HINT_CLS = "text-[10px] text-gray-400"
_LINK_CLS = "text-[10px] text-slate-700 cursor-pointer underline decoration-dotted"
_SOFT = "var(--cb-ink-3, #5b7089)"
_FAINT = "var(--cb-ink-4, #94a3b8)"
_BODY = "var(--cb-ink-2, #1e293b)"
_WARN_INK = "var(--cb-warn-ink, #8a4b06)"
_CAPTION = f"{SANS} font-size: 9.5px; color: {_SOFT}; line-height: 1.45;"

# What the save contract promises, derived from the watcher's own cadence: a full sweep of
# Curation/*/*/ every FULL_SWEEP_EVERY ticks, plus the settle window that keeps a half-written
# file until the next tick. A tomogram LOADED in the live session is in the watcher's hot set
# and is rescanned every tick instead.
_DETECT_S = FULL_SWEEP_EVERY * TICK_SEC + SETTLE_SEC
_HOT_DETECT_S = TICK_SEC + SETTLE_SEC

_EVENT_CLS = {
    "ingested": "cb-badge-ok",
    "error": "text-red-600",
    "no-geometry": "cb-badge-stale",
    "unattributed": "cb-badge-stale",
    "off-scope": "text-red-600",  # a save landed in a folder the live session was NOT scoped to
}

# The extraction job's mark per status on a sub-row: (glyph, words for the hover).
_JOB_MARK = {
    JobStatus.SCHEDULED: ("◌", "submitting"),
    JobStatus.QUEUED: ("◌", "queued in SLURM"),
    JobStatus.RUNNING: ("◐", "running"),
    JobStatus.SUCCEEDED: ("✓", "last run succeeded"),
    JobStatus.FAILED: ("✕", "last run FAILED"),
    JobStatus.UNKNOWN: ("?", "status unknown"),
}
_LIST_WORDS = {"auto": "candidates", "manual": "hand-picked", "imported": "imported", "merged": "merged"}

# (sort key, head, width, hover); a key "" is not sortable.
_COLS: tuple[tuple[str, str, str, str], ...] = (
    ("position", "tomogram", "", "Sorted by position by default. A click on a name copies the tomogram's folder."),
    ("picks", "picks", "58px", "Picks of this species on the tomogram, every list together."),
    ("kept", "kept", "52px", "Picks kept by a committed keep/drop filter; the total where none is committed."),
    ("top", "top σ", "52px", "The highest template-match peak, in σ."),
    ("ge10", "≥10σ", "46px", "Picks at or above 10σ."),
    (
        "extracted",
        "extracted",
        "66px",
        "Particles the subtomo extraction made / the candidates it was given; ✓ when every list is cut and current.",
    ),
    (
        "markers",
        "markers",
        "",
        "Rules that point a human at a tomogram; nothing is dropped by them. Hover for the rule. No geometry: no "
        "tomograms.star describes it, so a .coords cannot be mapped into it and ArtiaX has no volume to open. "
        "Saved: user .coords in its curation folder, each registered by the watcher as a list named after the file.",
    ),
    ("", "open in", "136px", "The pick viewer, and the viewer chips every tomogram preview carries."),
    (
        "",
        "curate in",
        "118px",
        "ArtiaX prepares the tomogram's reference picks, the startup script and the manifest that makes every save "
        "attributable, pre-seeds an empty list to pick into (never overwriting one) and opens the control center to "
        "start a session. Curating in napari is in development: its saved picks are not read back into crboost yet.",
    ),
)
SORTS = tuple(k for k, *_ in _COLS if k)
_NUMERIC = ("picks", "kept", "top", "ge10", "extracted")

_TABLE_JS = (
    "(e) => { const t = e.target.closest('[data-act]'); if (!t) return; const act = t.dataset.act; "
    "if (act === 'toggle') { const b = t.closest('tbody'); b.classList.toggle('open'); "
    "emit({act: act, tomo: b.dataset.tomo, open: b.classList.contains('open')}); return; } "
    "if (act === 'sort') { const table = t.closest('table'); const k = t.dataset.key; "
    "const bodies = Array.from(table.tBodies); "
    "bodies.sort((a, b) => Number(a.dataset['r' + k]) - Number(b.dataset['r' + k])); "
    "bodies.forEach((b) => table.appendChild(b)); "
    "table.querySelectorAll('th[data-key]').forEach((h) => h.classList.toggle('on', h.dataset.key === k)); "
    "emit({act: act, key: k}); return; } "
    "if (t.disabled) return; "
    "const b = t.closest('tbody'); emit({act: act, tomo: b ? b.dataset.tomo : '', slug: t.dataset.slug || ''}); }"
)


def _e(s: object) -> str:
    return escape(str(s), quote=True)


def _num(v: float | int | None, fmt: str = "{:,}") -> str:
    return "—" if v is None else fmt.format(v)


def _save_contract(species_slug: str) -> str:
    """The where-to-save contract, as the title's tooltip. It is the one thing a user must
    get right by hand, but it is read once, so it hangs off the title rather than the page."""
    return (
        f"ArtiaX pre-seeds ONE empty list per tomogram — Curation/{species_slug}/<tomogram>/"
        f"{species_slug}__<tomogram>__picks.coords — and opens it last. Pick into it and save it back to "
        f"that same file (positions-only .coords, not RELION star) · picked up within ~{_HOT_DETECT_S:.0f} s in the "
        f"session's tomogram, ~{_DETECT_S:.0f} s elsewhere — nothing to press here. ArtiaX's save dialog opens in "
        f"the folder you LAST saved to, so after a switch paste the list's path (copy on its row) into the dialog. "
        f"A save under another name in that folder still becomes its own list, named after the file; a save "
        f"outside these folders shows in the watcher's unattributed saves, where you assign it."
    )


def _coords_path(pl: TomoList) -> str:
    """The ``.coords`` file behind a hand-picked list — a session save sits beside its star
    under the list's recorded stem, an external import IS its recorded path. "" for auto /
    merged lists and for rows that pre-date provenance (nothing is guessed)."""
    if pl.list_type != PickListType.MANUAL.value or not pl.source_ref:
        return ""
    if pl.source_kind == PickSourceKind.ARTIAX.value and pl.star_path:
        return str(Path(pl.star_path).parent / f"{pl.source_ref}.coords")
    if pl.source_kind == PickSourceKind.IMPORT.value and "/" in pl.source_ref:
        return pl.source_ref
    return ""


def _dedup_default_radius(state, species_id: str) -> float:
    """Default overlap radius (Å) ≈ Ø/2 from the species; 100 Å when Ø is unset. Not a silent
    default — the dedup dialog shows the live clash count at the radius and removes nothing
    until the user commits."""
    sp = state.get_species(species_id)
    d = float(getattr(sp, "diameter_ang", 0.0) or 0.0)
    return round(d / 2.0, 1) if d > 0 else 100.0


# ── Off-loop compute ──────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class _Computed:
    """Everything the render reads, in comparable (signature-able) form."""

    rows: tuple[TomoRow, ...]
    refs: tuple[tuple[str, str, ListRef], ...]  # (tomo, slug, ref) — one per list
    tomo_refs: tuple[tuple[str, ListRef], ...]  # (tomo, its reference slot) — what curate and import act on
    species_slug: str  # the Curation/<slug>/ directory the watcher scans
    source_label: str  # where the scores come from
    notes: tuple[str, ...]
    error: str | None

    @property
    def n_picks(self) -> int:
        return sum(r.picks for r in self.rows)


def _volume(star: Path | None, tomo: str, project_path: Path, tables: dict) -> str:
    """The reconstruction a tomograms.star row names, when it is on disk; "" otherwise."""
    if star is None:
        return ""
    if star not in tables:
        tables[star] = read_tomograms_table(star)
    df = tables[star]
    if df is None:
        return ""
    match = df[df["rlnTomoName"].astype(str) == tomo]
    if match.empty:
        return ""
    p = resolve_volume_for_3dmod(match.iloc[0], project_path)
    return str(p) if p is not None else ""


def _compute(state, project_path: Path, species_id: str) -> _Computed:
    """Thread body: `species_overview`, the per-tomogram `tomograms.star` resolution, one glob
    per curation folder and the analytics records."""
    ov = species_overview(state, project_path, species_id)
    sp = state.get_species(species_id)
    label = str(getattr(sp, "name", "") or species_id)
    anchors, stars = species_tomo_map(state, project_path, species_id, tuple(r.tomo_name for r in ov.rows))
    # The species half of the save path, taken from `curation_dir` itself rather than
    # re-slugged here: the contract must name the directory the watcher scans, and only
    # that helper owns the slug rule.
    species_slug = artiax_bridge.curation_dir(project_path, "x", species_id=species_id, species_label=label).parent.name
    refs = tuple(
        (
            r.tomo_name,
            r.slug,
            list_ref_for(
                project_path,
                anchors,
                species_id,
                r.tomo_name,
                r.slug,
                label=r.label,
                list_type=r.list_type,
                star_path=r.star_path,
                tomograms_star=stars.get(r.tomo_name),
            ),
        )
        for r in ov.rows
    )
    tables: dict = {}
    infos, tomo_refs = [], []
    for tomo, star in stars.items():
        cdir = artiax_bridge.curation_dir(project_path, tomo, species_id=species_id, species_label=label)
        infos.append(
            TomoInfo(
                name=tomo,
                has_geometry=star is not None,
                n_saves=_count_user_saves(cdir, species_id, tomo),
                curation_dir=str(cdir),
                volume=_volume(star, tomo, project_path, tables),
            )
        )
        tomo_refs.append((tomo, auto_ref(project_path, anchors, species_id, tomo, star)))
    scores = score_rows(state, project_path, species_id)
    rows = join_tomograms(ov.rows, infos, scores.rows, scores.markers)
    notes = tuple(scores.notes) + tuple(f"Scores unavailable — {e}" for e in scores.errors)
    return _Computed(
        rows, refs, tuple(tomo_refs), species_slug, scores.source.label if scores.source else "", notes, None
    )


def _count_user_saves(cdir: Path, species_id: str, tomo: str) -> int:
    """User `.coords` in the folder, excluding an untouched seed: the 0-byte file crboost
    created is not something the user saved."""
    seed = artiax_bridge.seed_coords_path(cdir, species_id, tomo)
    n = 0
    for c in artiax_bridge.user_coords_saves(cdir):
        try:
            if c == seed and c.stat().st_size == 0:
                continue
        except OSError:
            continue  # vanished between glob and stat — the next recompute sees what is there
        n += 1
    return n


# ── The table ─────────────────────────────────────────────────────────────────


def _ranks(rows: tuple[TomoRow, ...]) -> dict[str, dict[str, int]]:
    """{sort key: {tomogram: its place}} — what the browser sorts the rows by."""
    keys = {
        "position": lambda t: ts_position_sort_key(t.name),
        "picks": lambda t: (-t.picks, ts_position_sort_key(t.name)),
        "kept": lambda t: (-t.kept, ts_position_sort_key(t.name)),
        "top": lambda t: (-(t.top or 0.0), ts_position_sort_key(t.name)),
        "ge10": lambda t: (-(t.ge10 or 0), ts_position_sort_key(t.name)),
        "extracted": lambda t: (-_extracted_n(t.extracted), ts_position_sort_key(t.name)),
        "markers": lambda t: (-len(t.markers), ts_position_sort_key(t.name)),
    }
    return {k: {t.name: i for i, t in enumerate(sorted(rows, key=fn))} for k, fn in keys.items()}


def _extracted_n(text: str) -> float:
    head = text.split("/", 1)[0]
    if head.isdigit():
        return float(head)
    return 0.5 if text == "✓" else 0.0


def _expandable(t: TomoRow) -> bool:
    """A tomogram opens to its lists when it has several, or when one of them has actions of its own."""
    return len(t.lists) > 1 or t.has_actions


def _table_html(c: _Computed, species_id: str, *, sort: str, opened: set[str], live_tomo: str, ood: bool) -> str:
    ranks = _ranks(c.rows)
    cols = "".join(f'<col style="width: {w};">' if w else "<col>" for _k, _h, w, _t in _COLS)
    heads = []
    for k, h, _w, tip in _COLS:
        cls = ["n"] if k in _NUMERIC else []
        if k == sort:
            cls.append("on")
        if not k:
            cls.append("x")
        sortable = f' data-act="sort" data-key="{k}"' if k else ""
        heads.append(
            f'<th class="{" ".join(cls)}" title="{_e(tip)}"{sortable}>{_e(h).replace("σ", keep_case("σ"))}</th>'
        )
    order = sorted(c.rows, key=lambda t: ranks[sort][t.name])
    body = "".join(_tomo_html(t, species_id, ranks, opened, live_tomo, ood) for t in order)
    return f'<table class="cb-tt"><colgroup>{cols}</colgroup><thead><tr>{"".join(heads)}</tr></thead>{body}</table>'


def _cell(v, fmt: str = "{:,}") -> str:
    return f'<td class="n{"" if v else " dim"}">{_num(v, fmt)}</td>'


def _tomo_html(t: TomoRow, species_id: str, ranks: dict, opened: set[str], live_tomo: str, ood: bool) -> str:
    many = _expandable(t)
    data_ranks = "".join(f' data-r{k}="{ranks[k][t.name]}"' for k in ranks)
    tomo = _e(t.name)
    car = '<span class="cb-tt-car" data-act="toggle">▶</span>' if many else '<span class="cb-tt-car"></span>'
    nlists = f'<span class="cb-tt-nlists" data-act="toggle">{len(t.lists)} lists</span>' if len(t.lists) > 1 else ""
    marks = "".join(f'<span class="cb-tt-mk" title="{_e(text)}">{_e(label)}</span>' for label, text in t.markers)
    if not t.has_geometry:
        marks += '<span class="cb-tt-mk" title="No tomograms.star describes it">no geometry</span>'
    if t.n_saves:
        marks += (
            f'<span class="cb-tt-mk" title="{t.n_saves} user .coords in its curation folder">{t.n_saves} saved</span>'
        )
    extracted = {"stale": "⚠ stale"}.get(t.extracted, t.extracted) or "–"
    # open in: the pick viewer, then napari and 3dmod on the reconstruction
    opens = '<span class="cb-tt-vc" data-act="viewer" title="Open the pick viewer">picks</span>'
    if not t.volume:
        no_vol = "No reconstruction of it on disk"
        opens += "".join(f'<span class="cb-tt-vc off" title="{no_vol}">{w}</span>' for w in ("napari", "3dmod"))
    else:
        if ood:
            opens += (
                f'<a class="cb-tt-vc" href="{_e(open_url(t.volume))}" target="_blank" rel="noopener" '
                'title="Open in napari, in a new browser tab (through the cluster\'s OnDemand portal)">napari</a>'
                f'<span class="cb-tt-vc" data-act="napari" title="Your napari session">▾</span>'
            )
        else:
            opens += '<span class="cb-tt-vc" data-act="napari" title="Open in napari">napari</span>'
        opens += '<span class="cb-tt-vc" data-act="3dmod" title="Copy the 3dmod command">3dmod</span>'
    live = (
        '<span class="cb-tt-live" title="the live ChimeraX + ArtiaX session has it open"></span>'
        if t.name == live_tomo
        else ""
    )
    if t.has_geometry:
        artiax = f'<button class="cb-tt-cur" data-act="curate" title="{_e(_curate_tip(t))}">ArtiaX</button>'
    else:
        artiax = '<button class="cb-tt-cur off" disabled title="No geometry: nothing to open">ArtiaX</button>'
    napari_cur = '<button class="cb-tt-cur off" disabled title="napari curation: in development">napari</button>'
    row = (
        '<tr class="r">'
        f'<td class="nm" title="{_e(t.details)}">{car}'
        f'<span data-act="copy">{_e(t.short)}</span>{nlists}</td>'
        f"{_cell(t.picks)}{_cell(t.kept)}"
        f'<td class="n{"" if t.top is not None else " dim"}">{_num(t.top, "{:.1f}")}</td>'
        f"{_cell(t.ge10)}"
        f'<td class="n{"" if t.extracted else " dim"}">{_e(extracted)}</td>'
        f'<td class="w">{marks}</td>'
        f"<td>{opens}</td>"
        f'<td style="text-align: right; white-space: nowrap;">{live}{artiax}{napari_cur}</td>'
        "</tr>"
    )
    subs = "".join(_list_html(t, pl, species_id) for pl in t.lists) if many else ""
    cls = ' class="open"' if many and t.name in opened else ""
    return f'<tbody data-tomo="{tomo}"{data_ranks}{cls}>{row}{subs}</tbody>'


def _curate_tip(t: TomoRow) -> str:
    tip = f"Open {t.short} in ArtiaX with this species' picks"
    return tip + (f"; {t.n_saves} saved list(s) wait in its folder" if t.n_saves else "")


def _list_html(t: TomoRow, pl: TomoList, species_id: str) -> str:
    slug = _e(pl.slug)
    kind = _LIST_WORDS.get(pl.list_type, pl.list_type)
    origin = pl.origin_note or (Path(pl.template_path).name if pl.template_path else "")
    src = " · ".join(x for x in (kind, pl.source_kind, origin) if x)
    src_tip = src
    if pl.template_path:
        src_tip += f" — template {pl.template_path} · mask {pl.mask_path or '—'}"
    if pl.origin_note:
        src_tip += " — cannot say which template and mask produced these picks; crboost will not guess the rest."
    if pl.extraction_state == NOT_APPLICABLE:
        state, state_tip = "", "auto list of a species with no subtomo-extraction job — nothing to extract here"
    else:
        text, _cls = list_actions.extraction_badge(ListExtractionState(pl.extraction_state))
        glyph, _, words = text.partition(" ")
        state = "" if pl.extraction_state == ListExtractionState.NOT_EXTRACTED.value else glyph
        state_tip = words or text
    job = ""
    if pl.extract_job is not None:
        status = JobStatus(pl.extract_job.status)
        glyph, words = _JOB_MARK.get(status, ("?", status.value))
        tip = _job_tooltip(pl.extract_job, words)
        job = f'<span class="cb-tt-ib" data-act="logs" data-slug="{slug}" title="{_e(tip)}">{glyph}</span>'
    if is_default_slug(pl.slug, species_id, t.name):
        name_tip = (
            f"the pre-seeded ArtiaX list — pick into it and save it back to its file · {pl.star_path or 'no star'}"
        )
    else:
        name_tip = pl.star_path or "no backing star on disk"
    acts = ""
    if pl.list_type != PickListType.AUTO.value:
        extracted = pl.extraction_state == ListExtractionState.EXTRACTED.value
        acts += (
            f'<span class="cb-tt-ib" data-act="extract" data-slug="{slug}" '
            f'title="{"Re-extract" if extracted else "Extract"} this list\'s subtomograms">'
            f"{'re-extract' if extracted else 'extract'}</span>"
        )
        if pl.list_type == PickListType.MERGED.value:
            acts += (
                f'<span class="cb-tt-ib" data-act="dedup" data-slug="{slug}" '
                'title="Overlapping picks — inspect and deduplicate">overlaps</span>'
            )
        coords = _coords_path(pl)
        if coords:
            acts += (
                f'<span class="cb-tt-ib" data-act="copy-coords" data-slug="{slug}" '
                f'title="Copy its .coords path — {_e(coords)}">copy</span>'
            )
        acts += (
            f'<span class="cb-tt-ib x" data-act="delete" data-slug="{slug}" '
            'title="Delete this list (its star, curated subset, extraction output and saves)">×</span>'
        )
    kept = _num(pl.kept) if pl.kept is not None else "–"
    return (
        '<tr class="sub">'
        f'<td class="nm" style="padding-left: 22px; color: {_BODY};" title="{_e(name_tip)}">└ {_e(pl.label)}</td>'
        f'<td class="n">{_num(pl.count)}</td>'
        f'<td class="n{"" if pl.kept is not None else " dim"}" title="kept after a keep/drop filter">{kept}</td>'
        "<td></td><td></td>"
        f'<td class="n" title="{_e(state_tip)}">{_e(state)}{job}</td>'
        f'<td class="w" title="{_e(src_tip)}">{_e(src)}</td>'
        f'<td colspan="2" style="text-align: right;">{acts}</td>'
        "</tr>"
    )


def _job_tooltip(job, words: str) -> str:
    """One line for the extraction job's mark: what it is doing, the SLURM id, the geometry it
    cut with, the failure text when there is one."""
    bits = [f"extraction {words}"]
    if job.slurm_job_id:
        bits.append(f"SLURM {job.slurm_job_id}")
    bits.append(list_actions.extraction_geometry_text(job))
    if job.error:
        bits.append(job.error if len(job.error) <= 240 else job.error[:240] + " …")
    bits.append("click for logs" if job.job_dir else "no log dir recorded for this submit")
    return " — ".join(bits)


# ── View ──────────────────────────────────────────────────────────────────────


class _TomogramsView(FingerprintedView):
    def __init__(self, container: ui.element, tab: TomogramsTab) -> None:
        super().__init__(container)
        self._tab = tab

    def signature(self) -> Any:
        c = self._tab.computed
        return (
            self._tab.ctx.species_id,
            self._tab.live_tomo(),
            None if c is None else (c.rows, c.species_slug, c.source_label, c.notes, c.error),
            self._tab.watcher_signature(),
        )

    def render(self) -> None:
        tab = self._tab
        c = tab.computed
        contract = _save_contract(c.species_slug) if c is not None and not c.error else "computing…"
        meta = ""
        if c is not None and not c.error:
            n_lists = sum(len(t.lists) for t in c.rows)
            n_ext = sum(
                1 for t in c.rows for pl in t.lists if pl.extraction_state == ListExtractionState.EXTRACTED.value
            )
            meta = f"{len(c.rows)} tomograms · {c.n_picks:,} picks · {n_ext} of {n_lists} lists extracted"
        with section_head("Tomograms", right=meta, tooltip=contract):
            if c is not None and not c.error:
                house_button(
                    "Import picks…",
                    tab.import_picks,
                    tooltip="Choose a .coords or a folder of them: each is paired with its tomogram by name, the "
                    "unpaired are listed, never guessed",
                )
                house_button(
                    "Extract all pending",
                    tab.extract_all_pending,
                    tooltip="Subtomo-extract every list of this species that is not extracted / stale — shows "
                    "what it would submit before anything is sent",
                )
                icon_button(
                    "refresh",
                    tab.refresh_now,
                    tooltip="Scan every curation folder for saves now and recompute — the watcher otherwise sees "
                    "the session's own folder within ~7 s and the others within ~30 s",
                )
        if c is None:
            ui.label("computing…").style(_CAPTION)
            return
        if c.error:
            ui.label(f"Tomograms unavailable — {c.error}").classes("text-[11px] text-red-600").tooltip(
                "species_overview / ref resolution / the curation-folder scan raised; full traceback in the log"
            )
            return
        if not c.rows:
            ui.label(
                "No tomogram is described by any tomograms.star yet — reconstruct or import tomograms first."
            ).style(_CAPTION)
            return
        self._render_lead(c)
        self._render_watcher()
        for n in c.notes:
            ui.label(n).style(_CAPTION)
        html = ui.html(
            _table_html(
                c,
                tab.ctx.species_id,
                sort=tab.sort,
                opened=tab.opened,
                live_tomo=tab.live_tomo(),
                ood=get_config_service().ood.enabled,
            ),
            sanitize=False,
        ).style("width: 100%;")
        html.on("click", tab.on_table_click, js_handler=_TABLE_JS)

    def _render_lead(self, c: _Computed) -> None:
        """One line: what the one list of every tomogram is, else where the scores come from."""
        lists = [pl for t in c.rows for pl in t.lists]
        if not lists:
            ui.label(
                "No picks yet — ArtiaX on a tomogram below to pick into it, Import picks… for .coords made "
                "elsewhere, or add Pick candidates with the roster's PARTICLES +."
            ).style(_CAPTION)
            return
        if all(len(t.lists) <= 1 for t in c.rows) and len({(pl.slug, pl.list_type) for pl in lists}) == 1:
            one = lists[0]
            origin = f", template {Path(one.template_path).name}" if one.template_path else ""
            words = _LIST_WORDS.get(one.list_type, one.list_type)
            text = f"Every tomogram with picks holds one list: {one.label} ({words}{origin})"
            ui.label(text + (f"; scores from {c.source_label}." if c.source_label else ".")).style(_CAPTION)
        elif c.source_label:
            ui.label(f"Scores from {c.source_label}.").style(_CAPTION)

    def _render_watcher(self) -> None:
        tab = self._tab
        events, unattributed = tab.watcher_view()
        n_read = sum(1 for e in events if e["kind"] == "ingested")
        words = f"{n_read} save{'s' if n_read != 1 else ''} read"
        if unattributed:
            words += f" · {len(unattributed)} not attributed"
        line = ui.element("div").style(
            "display: flex; align-items: baseline; gap: 6px; cursor: pointer; user-select: none;"
        )
        with line:
            caret = ui.label("▾" if tab.watcher_open else "▸").style(f"font-size: 8px; color: {_FAINT};")
            ui.label("watcher").style(TH_VOICE)
            ui.label(words).style(f"{SANS} font-size: 9.5px; color: {_WARN_INK if unattributed else _SOFT};")
        line.tooltip("What the server did with saves for this species, and the folders it could not attribute")
        body = ui.column().classes("w-full gap-0").style("padding-left: 14px;")
        body.set_visibility(tab.watcher_open)

        def _toggle() -> None:
            tab.watcher_open = not tab.watcher_open
            body.set_visibility(tab.watcher_open)
            caret.set_text("▾" if tab.watcher_open else "▸")

        line.on("click", _toggle)
        with body:
            if not events:
                ui.label("Nothing ingested yet for this species.").classes(_HINT_CLS)
            for e in events:
                with ui.row().classes("w-full items-center gap-2").style("min-height: 20px; flex-wrap: nowrap;"):
                    ui.label(e["ts"].replace("T", " ")).classes("text-[10px] text-gray-400").style(MONO)
                    ui.label(e["kind"]).classes(f"text-[10px] font-bold {_EVENT_CLS.get(e['kind'], 'text-gray-500')}")
                    ui.label(e["tomo_name"]).classes("text-[10px] text-slate-600 truncate").style(MONO)
                    ui.label(Path(e["coords"]).name).classes("text-[10px] text-gray-500 truncate").tooltip(e["coords"])
                    if e.get("count") is not None:
                        ui.label(f"{e['count']} picks").classes("text-[10px] text-slate-600")
                    if e.get("message"):
                        ui.label(e["message"]).classes("text-[10px] text-orange-700 truncate").tooltip(e["message"])
            if unattributed:
                ui.label("Unattributed saves").style(f"{TH_VOICE} margin-top: 6px;").tooltip(
                    "Project-wide: a save here reached no species, and was never guessed at."
                )
                for u in unattributed:
                    with ui.column().classes("w-full gap-0"):
                        with ui.row().classes("w-full items-center gap-2").style("flex-wrap: nowrap;"):
                            ui.label(u["dir"]).classes("text-[10px] text-slate-700 truncate").style(MONO).tooltip(
                                u["dir"]
                            )
                            n = len(u.get("files") or [])
                            if n:
                                ui.label(f"{n} file{'s' if n != 1 else ''}").classes(_HINT_CLS).tooltip(
                                    "\n".join(Path(f).name for f in u["files"])
                                )
                            ui.space()
                            # The only way these picks acquire a species, and it is the
                            # user's call — never an inference.
                            ui.label("assign ↗").classes(_LINK_CLS).on(
                                "click", lambda _e, entry=u: tab.assign_unattributed(entry)
                            ).tooltip("Move these .coords into a species + tomogram's curation folder and import them")
                        ui.label(u["reason"]).classes("text-[10px] text-orange-700")


def _noop() -> None:
    """`on_done` for the shared actions: this page repaints off the registry rev they
    bump, so there is nothing to call back into."""
    return None


# ── Tab ───────────────────────────────────────────────────────────────────────


class TomogramsTab:
    def __init__(self, ctx: TabContext) -> None:
        self.ctx = ctx
        self.computed: _Computed | None = None
        self._last_key: Any = None
        self._last_at: float = 0.0
        self._flight = SingleFlight()
        self._view: _TomogramsView | None = None
        # What the browser shows of the table, kept for its next render: the sort and the opened tomograms.
        self.sort = "position"
        self.opened: set[str] = set()
        self.watcher_open = False

    @property
    def backend(self):
        return self.ctx.backend

    # ── Tab protocol ──────────────────────────────────────────────────────────

    def build(self, container: ui.element) -> None:
        with container:
            slot = ui.column().classes("w-full").style("gap: 8px; padding: 12px 14px 18px;")
        self._view = _TomogramsView(slot, self)
        # Paint "computing…" only; the first compute is kicked by the page's
        # `_refresh_visible_tab` right after this.
        self._view.refresh()

    async def refresh_now(self) -> None:
        """Refresh: sweep every curation folder of the project NOW (the watcher ingests what it
        finds and bumps the registry rev), then recompute without waiting for the 15 s disk
        cadence. A save younger than the watcher's settle window is picked up on the next tick —
        ArtiaX writes are not atomic."""
        async with self._flight("refresh_now") as acquired:
            if not acquired:
                return
            watcher = self._watcher()
            if watcher is not None:
                await watcher.sweep_now(self.ctx.project_path)
            self._last_at = 0.0
            self.refresh()

    def refresh(self) -> None:
        """Page tick / on show. Two cadences: the session poll (shared, ~16 s, squeue) and the
        disk recompute (15 s) — neither runs while the page is hidden, because the page only
        ticks the VISIBLE tab."""
        if session_status.is_stale():
            asyncio.create_task(self._poll_session())
        key = self._memory_key()
        stale = time.monotonic() - self._last_at >= _DISK_REFRESH_S
        if self.computed is None or key != self._last_key or stale:
            asyncio.create_task(self._recompute(key))
        elif self._view is not None:
            self._view.refresh()

    def _memory_key(self) -> tuple:
        state = get_project_state_for(self.ctx.project_path)
        return (
            state.registry_rev,
            tuple(sorted((iid, str(getattr(jm, "execution_status", ""))) for iid, jm in state.jobs.items())),
            self.watcher_signature(),
        )

    async def _poll_session(self) -> None:
        async with self._flight("session") as acquired:
            if not acquired:
                return
            await session_status.poll(self.backend)
            if self._view is not None:
                self._view.refresh()

    async def _recompute(self, key: tuple) -> None:
        async with self._flight("compute") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            if self.backend is not None:
                # Settle any extraction instance whose awaiter is gone before reading the rows
                # off it: otherwise a sub-row's job mark asserts Queued/Running for a job that left
                # the SLURM queue long ago. No-op — and no squeue call — when nothing is
                # non-terminal, which is the normal case.
                await self.backend.reconcile_pick_list_extractions(ctx.project_path, ctx.species_id)
            state = get_project_state_for(ctx.project_path)
            try:
                computed = await asyncio.to_thread(_compute, state, ctx.project_path, ctx.species_id)
            except Exception as e:
                # Reported, not swallowed: traceback to the log, the cause into the tab.
                logger.exception("Tomograms compute failed for %s", ctx.species_id)
                computed = _Computed((), (), (), "", "", (), f"{type(e).__name__}: {e}")
            self.computed = computed
            self._last_key = key
            self._last_at = time.monotonic()
            if self._view is not None:
                self._view.refresh()

    # ── Lookups ───────────────────────────────────────────────────────────────

    def _ref(self, tomo: str, slug: str) -> ListRef | None:
        c = self.computed
        return None if c is None else next((r for t, s, r in c.refs if t == tomo and s == slug), None)

    def _tomo_ref(self, tomo: str) -> ListRef | None:
        c = self.computed
        return None if c is None else next((r for t, r in c.tomo_refs if t == tomo), None)

    def _row(self, tomo: str) -> TomoRow | None:
        c = self.computed
        return None if c is None else next((t for t in c.rows if t.name == tomo), None)

    def _list(self, tomo: str, slug: str) -> TomoList | None:
        t = self._row(tomo)
        return None if t is None else next((pl for pl in t.lists if pl.slug == slug), None)

    # ── Watcher (in-memory; safe on the event loop and in a signature) ────────

    def _watcher(self):
        return getattr(self.backend, "curation_watcher", None)

    def watcher_view(self) -> tuple[list[dict], list[dict]]:
        """(this species' events newest-first, capped; project-wide unattributed folders)."""
        watcher = self._watcher()
        if watcher is None:
            return [], []
        events = [e for e in watcher.events(self.ctx.project_path) if e["species_id"] == self.ctx.species_id]
        return list(reversed(events))[:12], watcher.unattributed(self.ctx.project_path)

    def watcher_signature(self) -> tuple:
        """Cheap fingerprint of what the watcher line draws — the watcher appends in place, so
        counts + the newest timestamp are enough to notice."""
        events, unattributed = self.watcher_view()
        return (len(events), events[0]["ts"] if events else "", tuple(u["dir"] for u in unattributed))

    def live_tomo(self) -> str:
        """The tomogram the live session has open for this species, "" when none. No I/O: the
        shared `session_status` cache this tab polls."""
        if not session_status.is_live():
            return ""
        sc = session_status.scope()
        return str(sc.get("tomo_name") or "") if sc.get("species_id") == self.ctx.species_id else ""

    # ── The table's clicks ────────────────────────────────────────────────────

    async def on_table_click(self, e) -> None:
        a = e.args or {}
        act, tomo, slug = a.get("act", ""), a.get("tomo", ""), a.get("slug", "")
        if act == "sort":
            key = a.get("key") or "position"
            if key in SORTS:
                self.sort = key
            return
        if act == "toggle":
            (self.opened.add if a.get("open") else self.opened.discard)(tomo)
            return
        t = self._row(tomo)
        if t is None:
            return
        with dialog_host():
            if act == "copy":
                self._copy(t.curation_dir, "The save folder — paste it into ArtiaX's save dialog")
            elif act == "viewer":
                await self._open_viewer(tomo)
            elif act == "napari" and t.volume:
                await open_napari_dialog(self.backend, t.volume)
            elif act == "3dmod" and t.volume:
                await _write_clipboard(imod_command(t.volume))
            elif act == "curate":
                ref = self._tomo_ref(tomo)
                if ref is not None:
                    await list_actions.curate_in_artiax(self.backend, ref)
            else:
                await self._list_act(act, t, slug)

    async def _list_act(self, act: str, t: TomoRow, slug: str) -> None:
        pl, ref = self._list(t.name, slug), self._ref(t.name, slug)
        if pl is None or ref is None:
            return
        if act == "extract":
            await list_actions.extract_list(self.backend, ref, on_done=_noop)
        elif act == "dedup":
            state = get_project_state_for(self.ctx.project_path)
            list_actions.open_dedup_dialog(
                self.backend,
                ref,
                default_radius_ang=_dedup_default_radius(state, self.ctx.species_id),
                on_done=self.refresh,
            )
        elif act == "logs" and pl.extract_job is not None:
            async with self._flight(f"logs:{pl.extract_job.instance_id}") as acquired:
                if acquired:
                    await list_actions.open_extraction_logs(
                        self.backend, self.ctx.project_path, pl.extract_job, title=f"{pl.label} · {t.name}"
                    )
        elif act == "copy-coords":
            coords = _coords_path(pl)
            if coords:
                self._copy(
                    coords, "Copied the .coords path — paste it into the file-name field of ArtiaX's save dialog"
                )
        elif act == "delete":
            await list_actions.delete_list(
                self.backend, ref.project_path, ref.species_id, ref.tomo_name, ref.slug, ref.label, on_done=self.refresh
            )

    @staticmethod
    def _copy(text: str, words: str) -> None:
        ui.clipboard.write(text)
        ui.notify(words, type="positive", classes="cb-toast-copied", timeout=1800)

    async def _open_viewer(self, tomo: str) -> None:
        """The full-page pick viewer on this species + tomogram; the workspace builds it on first use."""
        async with self._flight("viewer") as acquired:
            if not acquired:
                return
            open_viewer = self.ctx.callbacks.get("open_pick_viewer")
            if open_viewer is None:
                ui.notify("Pick viewer not available in this view", type="warning")
                return
            await open_viewer(self.ctx.species_id, tomo)

    # ── Import picks… ─────────────────────────────────────────────────────────

    async def import_picks(self) -> None:
        """A .coords or a folder of them; each paired with its tomogram by name, the pairs and the unpaired shown
        before anything is read. SingleFlight: the button sits in a view a refresh can replace mid-click."""
        async with self._flight("import") as acquired:
            if not acquired:
                return
            c = self.computed
            if c is None or c.error:
                return
            with dialog_host():
                picked = await local_file_picker(str(self.ctx.project_path), mode="any", glob="*.coords")
            if not picked:
                return
            files = await asyncio.to_thread(_coords_files, picked[0])
            if not files:
                ui.notify(f"No .coords in {picked[0]}", type="warning")
                return
            mappable = [t.name for t in c.rows if t.has_geometry]
            matched, unmatched = match_coords(files, mappable)
            if await self._confirm_import(matched, unmatched):
                await self._run_import(matched)

    async def _confirm_import(self, matched: list[tuple[str, str]], unmatched: list[tuple[str, str]]) -> bool:
        with dialog_host(), ui.dialog() as dlg, ui.card().classes("w-[40rem] max-w-full gap-2"):
            ui.label(f"Import picks · {len(matched)} of {len(matched) + len(unmatched)} paired").classes(
                "text-sm font-bold"
            )
            ui.label(
                "Read as full-resolution coordinates. A file that came out of a crboost curation session belongs "
                "back in that tomogram's folder (or assigned from the watcher's unattributed saves) instead — that "
                "route keeps the exact frame the session displayed. A file named like an existing list replaces it."
            ).classes("text-[11px] text-gray-500")
            with ui.column().classes("w-full gap-0").style("max-height: 50vh; overflow-y: auto;"):
                for f, tomo in matched:
                    with ui.row().classes("w-full items-center gap-2 no-wrap"):
                        ui.label(Path(f).name).classes("text-[10px] font-mono text-gray-700 truncate").tooltip(f)
                        ui.label("→").classes(_HINT_CLS)
                        ui.label(tomo).classes("text-[10px] font-mono text-gray-700 truncate")
                for f, why in unmatched:
                    with ui.row().classes("w-full items-center gap-2 no-wrap"):
                        ui.label(Path(f).name).classes("text-[10px] font-mono text-gray-500 truncate").tooltip(f)
                        ui.label(f"not imported: {why}").classes("text-[10px] text-orange-700 truncate").tooltip(why)
            with ui.row().classes("w-full justify-end gap-2"):
                house_button("Cancel", lambda: dlg.submit(False))
                go = house_button(f"Import {len(matched)}", lambda: dlg.submit(True), kind="accent")
                if not matched:
                    go.disable()
        ok = bool(await dlg)
        dlg.delete()
        return ok

    async def _run_import(self, matched: list[tuple[str, str]]) -> None:
        backend, ctx = self.backend, self.ctx
        state = get_project_state_for(ctx.project_path)
        done, failed = 0, []
        for f, tomo in matched:
            ref = self._tomo_ref(tomo)
            if ref is None or ref.tomograms_star is None:
                failed.append(f"{Path(f).name}: no tomograms.star for {tomo}")
                continue
            result = await backend.import_curation_picks(
                ref.project_path,
                ref.tomograms_star,
                tomo,
                ref.species_label or ref.species_id,
                ref.species_id,
                coords_path=Path(f),
            )
            if not result.get("success"):
                failed.append(f"{Path(f).name}: {result.get('error')}")
                continue
            register_manual_pick_list(state, result, ref.species_id, tomo)
            done += 1
        if done:
            await backend.save_project(ctx.project_path, force=True)
        if failed:
            ui.notify(f"Imported {done}; failed {len(failed)}: " + "; ".join(failed[:3]), type="warning", timeout=8000)
        else:
            ui.notify(f"Imported {done} list{'s' if done != 1 else ''}", type="positive", timeout=3000)
        self.refresh()

    async def assign_unattributed(self, entry: dict) -> None:
        """Give an orphaned save an explicit (species, tomogram). Its files are moved into
        the right curation folder and the watcher ingests them; nothing here infers anything."""
        with dialog_host():
            await list_actions.assign_unattributed(self.backend, self.ctx.project_path, entry, on_done=self.refresh)

    # ── Extract all pending ───────────────────────────────────────────────────

    async def extract_all_pending(self) -> None:
        """Pre-flight first (the pending report is the preview), then submit. Never a blind
        fan-out: `extract_pending_lists` starts one SLURM job per pending list, so the user
        sees the count, the tomograms and the blocked reasons before confirming — and commits
        the extraction geometry here if the species has none."""
        async with self._flight("extract_all") as acquired:
            if not acquired:
                return
            ctx = self.ctx
            rows = await self.backend.get_pending_extractions(ctx.project_path, ctx.species_id)
            pending = [r for r in rows if not r["blocked_reason"]]
            blocked = [r for r in rows if r["blocked_reason"]]
            if not pending:
                msg = "Nothing pending — every list is extracted and current with its picks."
                if blocked:
                    msg = f"Nothing extractable: {len(blocked)} list(s) blocked (see the table)."
                ui.notify(msg, type="warning" if blocked else "info", timeout=5000)
                return

            state = get_project_state_for(ctx.project_path)
            need_geometry = extraction_params_for_species(state, ctx.species_id) is None
            with dialog_host(), ui.dialog() as confirm, ui.card().classes("w-[32rem] max-w-full gap-2"):
                ui.label(f"Extract {len(pending)} pending list(s)?").classes("text-sm font-bold")
                ui.label("One subtomo-extraction job per list, submitted now and tracked in the task tray.").classes(
                    _HINT_CLS
                )
                for h in pending:
                    ui.label(f"• {h['tomo_name']} · {h['slug']} ({h['extraction_state']})").classes(
                        "text-[10px] font-mono text-gray-600"
                    )
                for h in blocked:
                    ui.label(f"⚠ blocked — {h['tomo_name']} · {h['slug']}: {h['blocked_reason']}").classes(
                        "text-[10px] text-orange-700"
                    )
                box_in = bin_in = crop_in = None
                if need_geometry:
                    ui.label(
                        "This species has no committed extraction geometry (no subtomo job to inherit it from). "
                        "Set it once — it is saved on the species and reused for every later extraction."
                    ).classes("text-xs text-gray-600")
                    box_in, bin_in, crop_in = list_actions.geometry_inputs()
                with ui.row().classes("w-full justify-end gap-2"):
                    house_button("Cancel", lambda: confirm.submit(None))
                    house_button("Extract", lambda: confirm.submit(True), kind="accent")
            go = await confirm
            geometry = (box_in.value, bin_in.value, crop_in.value) if need_geometry and go else None
            confirm.delete()
            if not go:
                return
            if need_geometry and not await list_actions.commit_extraction_geometry(
                self.backend, ctx.project_path, ctx.species_id, *geometry
            ):
                return
            self._submit_extract_all(len(pending))

    def _submit_extract_all(self, n_pending: int) -> None:
        ctx = self.ctx
        backend = self.backend

        async def _run(progress_cb):
            progress_cb(0, n_pending, "extracting pending lists…")
            res = await backend.extract_pending_lists(ctx.project_path, ctx.species_id)
            return (
                f"{len(res.get('succeeded') or [])} extracted · {len(res.get('failed') or [])} failed · "
                f"{len(res.get('blocked') or [])} blocked · {len(res.get('still_running') or [])} still running"
            )

        BackgroundTask(
            title=f"Extract pending · {ctx.species_id}",
            subtitle=f"{n_pending} list(s)",
            project_path=str(ctx.project_path),
            dedup_key=f"extract_pending:{ctx.species_id}",
        ).submit(_run, on_complete=lambda _t: self.refresh(), show_start_toast=True)


def _coords_files(picked: str) -> list[str]:
    """The `.coords` the user chose: the file, or every `.coords` directly in the folder."""
    p = Path(picked)
    if p.is_dir():
        return sorted(str(f) for f in p.glob("*.coords") if f.is_file())
    return [str(p)] if p.suffix == ".coords" and p.is_file() else []
