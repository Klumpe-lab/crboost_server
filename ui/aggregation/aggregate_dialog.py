"""The "Aggregate candidates" dialog — unite one species' picks at any granularity
(roadmap `picking_ui/12-S4/S5/S6`).

Opened from the roster's PARTICLES-header icon. Two grades behind one door
(`docs/particle-data-flow.md` §3):

  COORDINATE grade   picks not yet extracted — manual lists, imported `.coords`, a
                     candidate-extract `candidates.star`. Owned here.
  EXTRACTED grade    particles that already have pixels. Delegated verbatim to
                     `merge_card.open_aggregation_merge_dialog`, which has done this
                     correctly for a while and is not worth reimplementing.

Why the coordinate half is a separate module rather than a mode inside `merge_card`: that
dialog is built around `AggregationSource`, one entry per optimisation set, and its whole
selection model is optset-shaped. A coordinate source is a LIST — several per tomogram,
most of them with no optset anywhere. Bolting a second identity model into the same tree
would make both harder to read than either is now.

**One species per aggregate, always** (roadmap 12, D2). The species picker is the first
control and everything below re-derives from it. This is what removes the geometry
ambiguity — whose box, whose Ø — that made this feature hard to scope.

The terminal action follows the payload GRADE, never a user choice (roadmap 12, D1): a
coordinate union is what a Pick-candidates job WOULD have produced, so the offer is to
skip that job and extract. ② is a peak-finder over score volumes; handing it coordinates
would leave it nothing to do.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from nicegui import run, ui

from backend import get_backend
from services.particles.coord_merge import CoordSource, clash_report, merge_coordinate_sources, plan_merge
from services.particles.list_ref import fs_slug
from services.models_base import PickListType, PickSourceKind
from services.project_state import MERGED_DIR_NAME, PickList, get_project_state_for
from ui.components.buttons import house_button
from ui.components.reactive import SingleFlight

log = logging.getLogger(__name__)

SLATE = "#475569"
SLATE_MUTED = "#94a3b8"
AMBER = "#b45309"

# Fallback clash radius when the species states no diameter. NOT a guessed particle size:
# the field is disabled and labelled "no Ø on the species" so the number on screen is
# visibly ours, and the species page is where it gets fixed.
_NO_DIAMETER_RADIUS = 0.0


@dataclass
class _Row:
    """One selectable pick list in the tree."""

    project_path: Path
    project_name: str
    species_id: str
    species_label: str
    tomo_name: str
    slug: str
    label: str
    list_type: str
    star_path: str
    tomograms_star: str
    count: int

    @property
    def key(self) -> str:
        return f"{self.project_path}\x1f{self.species_id}\x1f{self.tomo_name}\x1f{self.slug}"

    def to_source(self) -> CoordSource:
        return CoordSource(
            star_path=Path(self.star_path),
            tomograms_star=Path(self.tomograms_star),
            tomo_name=self.tomo_name,
            project_path=self.project_path,
            list_type=self.list_type,
            label=f"{self.project_name}:{self.tomo_name}:{self.label or self.slug}",
        )


# ---------------------------------------------------------------------------
# Enumeration (disk; runs off the event loop)
# ---------------------------------------------------------------------------


def _rows_for_project(project_path: Path) -> list[_Row]:
    """Every coordinate-grade list in one project, as selectable rows.

    Reads the project's own ProjectState — including a FOREIGN one, which is exactly how
    cross-project aggregation reaches lists that have never been extracted and therefore
    have no optimisation set for `discover_subtomo_optimisation_sets` to find.

    Rows whose star is missing on disk are dropped rather than offered: selecting one
    would fail at merge time with a path error instead of at selection time with an
    explanation.
    """
    from services.particles.list_ref import species_tomo_map

    try:
        state = get_project_state_for(project_path)
    except Exception:
        log.warning("Aggregate: cannot read project state at %s", project_path, exc_info=True)
        return []

    species = {sp.id: sp for sp in (state.species_registry or [])}
    rows: list[_Row] = []
    for species_id, sp in species.items():
        try:
            anchors, tomo_stars = species_tomo_map(state, project_path, species_id)
        except Exception:
            log.warning("Aggregate: cannot map tomograms for %s in %s", species_id, project_path, exc_info=True)
            continue

        # The species' auto list: one candidates.star spanning every tomogram the CE job
        # processed. Offered once per tomogram, because selection is per tomogram.
        auto_star = anchors.ce_job_dir / "candidates.star" if anchors.ce_job_dir else None
        for tomo_name, tstar in tomo_stars.items():
            if tstar is None:
                continue
            if auto_star is not None and auto_star.exists():
                rows.append(
                    _Row(
                        project_path=project_path,
                        project_name=project_path.name,
                        species_id=species_id,
                        species_label=sp.name,
                        tomo_name=tomo_name,
                        slug="auto",
                        label="candidates",
                        list_type="auto",
                        star_path=str(auto_star),
                        tomograms_star=str(tstar),
                        count=0,
                    )
                )
        for pl in state.pick_lists:
            if pl.species_id != species_id or not pl.path:
                continue
            tstar = tomo_stars.get(pl.tomo_name)
            if tstar is None or not Path(pl.path).exists():
                continue
            rows.append(
                _Row(
                    project_path=project_path,
                    project_name=project_path.name,
                    species_id=species_id,
                    species_label=sp.name,
                    tomo_name=pl.tomo_name,
                    slug=pl.slug,
                    label=pl.label or pl.slug,
                    list_type=str(getattr(pl.list_type, "value", pl.list_type)),
                    star_path=pl.path,
                    tomograms_star=str(tstar),
                    count=int(pl.count or 0),
                )
            )
    return rows


def _enumerate(project_path: Path) -> list[_Row]:
    """This project's rows first, then every other discoverable project's."""
    from services.aggregation.discovery import discover_pick_list_projects
    from services.configs.user_prefs_service import get_prefs_service

    here = Path(project_path).resolve()
    rows = _rows_for_project(here)
    prefs = get_prefs_service().prefs
    base_paths = [r.path for r in prefs.recent_project_roots if r.path]
    for other in discover_pick_list_projects(base_paths):
        if other == here:
            continue
        rows.extend(_rows_for_project(other))
    return rows


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------


class _AggregateDialog:
    """State of ONE open aggregate dialog. Per-tab, never a module global — the same
    reason `merge_card._MergeDialog` exists rather than the `_DIALOG_REFS` it replaced."""

    def __init__(self, project_path: Path) -> None:
        self.project_path = Path(project_path)
        self.state = get_project_state_for(self.project_path)
        self.flight = SingleFlight()
        self.rows: list[_Row] = []
        self.selected: set[str] = set()
        self.species_id: str = ""
        self.radius: float = _NO_DIAMETER_RADIUS
        self.name: str = ""
        self.spawn: bool = True
        self.tree: ui.element | None = None
        self.summary_host: ui.element | None = None

    # ---- derived ----

    def species_options(self) -> dict[str, str]:
        """Species present in ANY discovered project, keyed by id. Two projects can label
        one species differently; the id is what the pick lists agree on."""
        out: dict[str, str] = {}
        for r in self.rows:
            out.setdefault(r.species_id, r.species_label or r.species_id)
        return dict(sorted(out.items(), key=lambda kv: kv[1].lower()))

    def visible_rows(self) -> list[_Row]:
        return [r for r in self.rows if r.species_id == self.species_id]

    def chosen(self) -> list[_Row]:
        return [r for r in self.visible_rows() if r.key in self.selected]

    def default_radius(self) -> tuple[float, str]:
        """(radius, why). The species' Ø is the honest default; absent, it stays 0 and the
        clash report says it cannot be computed rather than inventing a particle size."""
        for sp in self.state.species_registry or []:
            if sp.id == self.species_id and sp.diameter_ang:
                return float(sp.diameter_ang), f"species Ø {sp.diameter_ang:g} Å"
        return _NO_DIAMETER_RADIUS, "no Ø on the species — set one on the Particles registry"

    # ---- rendering ----

    def on_species(self, species_id: str) -> None:
        self.species_id = species_id
        self.selected.clear()
        self.radius, _ = self.default_radius()
        self.render_tree()
        self.render_summary()

    def toggle(self, key: str, on: bool) -> None:
        if on:
            self.selected.add(key)
        else:
            self.selected.discard(key)
        self.render_summary()

    def toggle_tomo(self, tomo_name: str, on: bool) -> None:
        for r in self.visible_rows():
            if r.tomo_name == tomo_name:
                self.toggle(r.key, on)
        self.render_tree()

    def render_tree(self) -> None:
        if self.tree is None:
            return
        self.tree.clear()
        rows = self.visible_rows()
        with self.tree:
            if not rows:
                ui.label("No coordinate lists for this species in any discoverable project.").classes(
                    "text-[11px] py-4"
                ).style(f"color: {SLATE_MUTED};")
                return
            by_project: dict[Path, list[_Row]] = {}
            for r in rows:
                by_project.setdefault(r.project_path, []).append(r)
            for project_path, prows in by_project.items():
                is_here = project_path == self.project_path.resolve()
                with ui.row().classes("w-full items-center gap-2 pt-2"):
                    ui.label(prows[0].project_name).classes("text-[11px] font-bold").style(f"color: {SLATE};")
                    if not is_here:
                        ui.label("other project").classes("text-[9px] px-1 rounded").style(
                            f"color: {AMBER}; border: 1px solid {AMBER};"
                        )
                by_tomo: dict[str, list[_Row]] = {}
                for r in prows:
                    by_tomo.setdefault(r.tomo_name, []).append(r)
                for tomo_name, trows in sorted(by_tomo.items()):
                    all_on = all(r.key in self.selected for r in trows)
                    with ui.row().classes("w-full items-center gap-2 pl-3"):
                        ui.checkbox(
                            value=all_on, on_change=lambda e, t=tomo_name: self.toggle_tomo(t, bool(e.value))
                        ).props("dense size=xs")
                        ui.label(tomo_name).classes("text-[10px]").style(f"color: {SLATE};")
                    for r in sorted(trows, key=lambda x: (x.list_type, x.slug)):
                        with ui.row().classes("w-full items-center gap-2 pl-9"):
                            ui.checkbox(
                                value=r.key in self.selected, on_change=lambda e, k=r.key: self.toggle(k, bool(e.value))
                            ).props("dense size=xs")
                            ui.label(r.label).classes("text-[10px]").style(f"color: {SLATE};")
                            ui.label(r.list_type).classes("text-[9px]").style(f"color: {SLATE_MUTED};")
                            if r.count:
                                ui.label(f"{r.count}").classes("text-[9px]").style(f"color: {SLATE_MUTED};")

    def render_summary(self) -> None:
        """Counts, the plan's renames and unverified lines. Computed from state already in
        memory — the clash report needs the merged star and so waits for the merge."""
        if self.summary_host is None:
            return
        self.summary_host.clear()
        chosen = self.chosen()
        with self.summary_host:
            if len(chosen) < 2:
                ui.label("Select at least 2 lists.").classes("text-[10px]").style(f"color: {SLATE_MUTED};")
                return
            tomos = {r.tomo_name for r in chosen}
            projects = {r.project_path for r in chosen}
            ui.label(
                f"{len(chosen)} lists · {len(tomos)} tomogram{'s' if len(tomos) != 1 else ''} · "
                f"{len(projects)} project{'s' if len(projects) != 1 else ''}"
            ).classes("text-[10px]").style(f"color: {SLATE};")
            _, why = self.default_radius()
            ui.label(f"clash radius: {self.radius:g} Å ({why})").classes("text-[9px]").style(f"color: {SLATE_MUTED};")

    # ---- action ----

    async def run(self, dlg) -> None:
        async with self.flight("aggregate") as acquired:
            if not acquired:
                ui.notify("An aggregate is already running.", type="info", timeout=2000)
                return
            chosen = self.chosen()
            if len(chosen) < 2:
                ui.notify("Select at least 2 lists.", type="warning", timeout=2500)
                return
            root = self.state.project_path
            if root is None:
                ui.notify("No project loaded.", type="negative", timeout=4000)
                return

            sources = [r.to_source() for r in chosen]
            try:
                plan = await run.io_bound(plan_merge, sources)
            except Exception as e:
                log.exception("Aggregate: planning failed")
                ui.notify(f"Cannot aggregate: {e}", type="negative", timeout=9000)
                return
            if not plan.ok:
                await _show_blockers(plan.blocking)
                return
            if plan.unverified and not await _confirm_unverified(plan.unverified, plan.renamed):
                return

            label = (self.name or "").strip() or f"{self.species_id}-aggregate"
            slug = fs_slug(label)
            out_dir = Path(root) / MERGED_DIR_NAME / slug
            ui.notify("Aggregating…", type="info", timeout=2500)
            try:
                summary = await run.io_bound(merge_coordinate_sources, sources, out_dir=out_dir, name=label)
            except Exception as e:
                log.exception("Aggregate into %s failed", out_dir)
                ui.notify(f"Aggregate failed: {e}", type="negative", timeout=9000)
                return

            report = None
            if self.radius > 0:
                report = await run.io_bound(clash_report, Path(summary["outputs"]["particles_star"]), self.radius)

            single_tomo = _single_tomo(chosen)
            if single_tomo is not None:
                await self._register_pick_list(chosen, single_tomo, slug, label, summary)

            dlg.close()
            await _show_result(self, summary, report, single_tomo, out_dir)

    async def _register_pick_list(self, chosen, tomo_name: str, slug: str, label: str, summary: dict) -> None:
        """L1 case: an aggregate confined to ONE tomogram of THIS project is a merged pick
        list, so register it and it gets a chip on the picks surface like any other. This
        is the replacement 09-S3 promised when it deleted the merge bar — until now nothing
        could create a merged list at all (roadmap 12-S6)."""
        if any(r.project_path != self.project_path.resolve() for r in chosen):
            return  # a cross-project union is not addressable by (species, tomo, slug)
        bk = get_backend()
        self.state.add_pick_list(
            PickList(
                slug=f"merged__{slug}",
                label=label,
                list_type=PickListType.MERGED,
                species_id=self.species_id,
                tomo_name=tomo_name,
                path=summary["outputs"]["particles_star"],
                count=int(summary["n_picks"]),
                parent_slugs=[r.slug for r in chosen],
                source_kind=PickSourceKind.MERGE.value,
                source_ref="+".join(r.slug for r in chosen),
                created_by=getattr(bk, "username", "") if bk else "",
            )
        )
        if bk is not None:
            await bk.save_project(self.project_path, force=True)


def _single_tomo(rows: list[_Row]) -> str | None:
    names = {r.tomo_name for r in rows}
    return next(iter(names)) if len(names) == 1 else None


# ---------------------------------------------------------------------------
# Secondary dialogs
# ---------------------------------------------------------------------------


async def _show_blockers(lines: list[str]) -> None:
    """A wall, not a question. Every line here is a STATED disagreement between two
    reconstructions of the same acquisition — pooling their coordinates would put picks in
    the wrong physical place, which no amount of user intent makes correct."""
    with ui.dialog() as dlg, ui.card().classes("w-[38rem] max-w-full gap-2"):
        ui.label("These lists cannot be aggregated").classes("text-sm font-bold")
        ui.label(
            "The selected tomograms are the same acquisition but were reconstructed differently, "
            "so their centred-Å coordinates do not describe the same places."
        ).classes("text-[11px] text-gray-600")
        for line in lines[:20]:
            ui.label(f"• {line}").classes("text-[10px] font-mono text-red-700")
        with ui.row().classes("w-full justify-end"):
            house_button("Close", lambda: dlg.submit(None))
    await dlg
    dlg.delete()


async def _confirm_unverified(lines: list[str], renamed: dict[str, str]) -> bool:
    """A question, not a wall. These are facts nothing on disk states (handedness is the
    usual one) plus any tomogram name we had to disambiguate. Neither is proof of a
    problem, and silently assuming either way is what the never-invent-defaults policy
    forbids — so the user is shown them and decides."""
    with ui.dialog() as dlg, ui.card().classes("w-[38rem] max-w-full gap-2"):
        ui.label("Aggregate with unverified facts?").classes("text-sm font-bold")
        for line in lines[:20]:
            ui.label(f"• {line}").classes("text-[10px] font-mono text-orange-700")
        if renamed:
            ui.label("Tomogram names disambiguated:").classes("text-[11px] text-gray-600 pt-1")
            for old, new in list(renamed.items())[:10]:
                ui.label(f"• {old} → {new}").classes("text-[10px] font-mono text-gray-600")
        with ui.row().classes("w-full justify-end gap-2"):
            house_button("Cancel", lambda: dlg.submit(None))
            house_button("Aggregate anyway", lambda: dlg.submit(True), kind="accent")
    go = await dlg
    dlg.delete()
    return bool(go)


async def _show_result(d: _AggregateDialog, summary: dict, report, single_tomo: str | None, out_dir: Path) -> None:
    """What was produced, what clashes, and the ONE thing to do next.

    The verb follows the payload grade, never a dropdown (roadmap 12, D1): a coordinate
    union is what a Pick-candidates job would have produced, so the offer is to extract.
    """
    dropped = summary["columns"]["sidecar_only"]
    with ui.dialog() as dlg, ui.card().classes("w-[40rem] max-w-full gap-2"):
        ui.label(f"Aggregated “{summary['name']}”").classes("text-sm font-bold")
        ui.label(
            f"{summary['n_picks']} picks · {summary['n_tomograms']} tomograms · {summary['n_sources']} lists"
        ).classes("text-[11px] text-gray-700")
        ui.label(str(out_dir)).classes("text-[9px] font-mono text-gray-500")

        if report is not None:
            t = report["total"]
            if t["n_removed"]:
                ui.label(
                    f"⚠ {t['n_clashing']} picks clash within {report['radius_angst']:g} Å; "
                    f"deduplicating would drop {t['n_removed']}, leaving {t['n_after']}."
                ).classes("text-[11px] text-orange-700")
                ui.label(
                    "Nothing was deduplicated. The union keeps clashers on purpose — hand placements "
                    "sort before machine picks, so a dedup keeps the human one."
                ).classes("text-[10px] text-gray-600")
            else:
                ui.label(f"No clashes within {report['radius_angst']:g} Å.").classes("text-[11px] text-gray-600")
        else:
            ui.label(
                "Clash report not computed — the species states no diameter. Set Ø on the "
                "Particles registry to get one."
            ).classes("text-[10px] text-orange-700")

        if dropped:
            ui.label(
                f"Columns only some lists carried are in provenance.star, not particles.star: {', '.join(dropped)}."
            ).classes("text-[10px] text-gray-600")

        with ui.row().classes("w-full justify-end gap-2 pt-1"):
            house_button("Done", lambda: dlg.submit(None))
            if single_tomo is not None:
                house_button("Extract this list", lambda: dlg.submit("extract"), kind="accent")
            else:
                house_button("Set up extraction", lambda: dlg.submit("subtomo"), kind="accent")
    choice = await dlg
    dlg.delete()
    if choice == "extract":
        await _extract_merged_list(d, summary, single_tomo, out_dir)
    elif choice == "subtomo":
        _prepopulate_subtomo(d, summary)


async def _extract_merged_list(d: _AggregateDialog, summary: dict, tomo_name: str, out_dir: Path) -> None:
    """L1 terminal action: subtomo-extract the merged list through the per-list extraction
    roadmap 07 already built.

    Geometry comes from the species' COMMITTED `extraction_params` or nowhere. There is no
    fallback box here on purpose: `ExtractionParams` carries no defaults precisely because a
    guessed box produces wrong-but-plausible subtomograms, and this is not the surface that
    gets to decide one. Absent, the user is sent to the panel that owns the decision.
    """
    species = next((sp for sp in (d.state.species_registry or []) if sp.id == d.species_id), None)
    geom = getattr(species, "extraction_params", None) if species else None
    if geom is None:
        ui.notify(
            f"‘{d.species_id}’ has no committed extraction geometry. Set box / crop / binning on the "
            "Particles registry → Overview, then Extract the new merged list from Picks & curation.",
            type="warning",
            timeout=10000,
        )
        return

    from services.particles.list_ref import species_anchors

    anchors = await run.io_bound(species_anchors, d.state, d.project_path, d.species_id)
    ce_optset = anchors.ce_job_dir / "optimisation_set.star" if anchors.ce_job_dir else None
    bk = get_backend()
    if bk is None:
        ui.notify("No backend — cannot submit.", type="negative")
        return
    result = await bk.extract_pick_list(
        d.project_path,
        ce_optset if (ce_optset and ce_optset.exists()) else None,
        Path(summary["outputs"]["particles_star"]),
        tomo_name,
        d.species_id,
        f"merged__{Path(out_dir).name}",
        box_size=int(geom.box_size),
        binning=float(geom.binning),
        crop_size=int(geom.crop_size),
        tomograms_star=Path(summary["outputs"]["tomograms_star"]),
    )
    if not result.get("success"):
        ui.notify(f"Extraction failed to submit: {result.get('error')}", type="negative", timeout=9000)
        return
    ui.notify(
        f"Extraction submitted (job {result.get('slurm_job_id', '?')}). Watch it on Picks & curation.",
        type="positive",
        timeout=6000,
    )


def _prepopulate_subtomo(d: _AggregateDialog, summary: dict) -> None:
    """Point the project's subtomo-extraction job at the aggregate.

    Deliberately does NOT submit. Extraction geometry (box / crop / binning) is a decision
    the species owns and the job tab states; silently launching one with whatever numbers
    happened to be on the instance is exactly the invented-default this codebase refuses.
    The user lands on a job whose input is already correct and presses run themselves.
    """
    from services.models_base import JobType

    optset = summary["outputs"]["optimisation_set"]
    targets = [iid for iid, j in d.state.jobs.items() if j.job_type == JobType.SUBTOMO_EXTRACTION]
    if not targets:
        ui.notify(
            f"No Subtomo extraction job in this project yet — add one from the roster, then set its input to {optset}",
            type="warning",
            timeout=9000,
        )
        return
    iid = targets[0]
    d.state.jobs[iid].paths["input_optimisation"] = optset
    d.state.mark_dirty()
    bk = get_backend()
    if bk is not None:
        asyncio.create_task(bk.save_project(d.project_path, force=True))
    ui.notify(f"‘{iid}’ input set to the aggregate. Open its tab, check the box size, then run.", type="positive")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def open_aggregate_dialog(project_path) -> None:
    """Open "Aggregate candidates" for ``project_path``."""
    d = _AggregateDialog(project_path)

    with (
        ui.dialog() as dlg,
        ui.card()
        .classes("w-[880px] max-w-[96vw] max-h-[92vh] overflow-hidden border border-slate-200 bg-white p-0")
        .style("color: #1e293b;"),
    ):
        with ui.row().classes("w-full items-center gap-2 px-4 py-2 border-b border-slate-200 bg-slate-50"):
            ui.icon("merge_type", size="18px").style(f"color: {SLATE};")
            ui.label("Aggregate candidates").classes("text-sm font-bold text-slate-700")
            ui.space()
            ui.button(icon="close", on_click=dlg.close).props("flat dense round size=sm").classes("text-slate-500")

        controls = ui.row().classes("w-full items-center gap-3 px-4 py-2 border-b border-slate-100 bg-slate-50")
        tree = ui.column().classes("w-full px-3 py-1 gap-0 overflow-auto bg-white").style("max-height: 52vh;")
        summary_host = ui.column().classes("w-full px-4 pt-2 gap-0")
        footer = ui.row().classes("w-full items-center gap-2 px-4 py-2 border-t border-slate-200")

        d.tree = tree
        d.summary_host = summary_host

        with controls:
            species_select = (
                ui.select({}, label="Species", on_change=lambda e: d.on_species(str(e.value or "")))
                .props("dense outlined")
                .classes("text-xs")
                .style("min-width: 200px;")
            )
            ui.input(placeholder="Name this aggregate", on_change=lambda e: setattr(d, "name", e.value or "")).props(
                "dense outlined"
            ).classes("text-xs").style("width: 220px;")
            ui.space()
            ui.label("extracted particles instead ↗").classes("text-[10px] cursor-pointer underline").style(
                f"color: {SLATE_MUTED};"
            ).on("click", lambda: _switch_to_extracted(dlg, d.project_path)).tooltip(
                "Merge particles that already have subtomograms — the other payload grade"
            )

        with footer:
            ui.label("Nothing is deduplicated: the union keeps clashers so you can see them.").classes(
                "text-[10px]"
            ).style(f"color: {SLATE_MUTED};")
            ui.space()
            house_button("Cancel", dlg.close)
            house_button("Aggregate", lambda: asyncio.create_task(d.run(dlg)), kind="accent")

        with tree:
            ui.label("Scanning your projects…").classes("text-[11px] py-4").style(f"color: {SLATE_MUTED};")

    dlg.open()

    async def _load() -> None:
        d.rows = await run.io_bound(_enumerate, d.project_path)
        options = d.species_options()
        species_select.set_options(options)
        if options:
            first = next(iter(options))
            species_select.set_value(first)
            d.on_species(first)
        else:
            d.render_tree()

    asyncio.create_task(_load())


def _switch_to_extracted(dlg, project_path: Path) -> None:
    from ui.aggregation.merge_card import open_aggregation_merge_dialog

    dlg.close()
    open_aggregation_merge_dialog(project_path)
