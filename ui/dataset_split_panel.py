# ui/dataset_split_panel.py
"""
The suggestion shown in place of the dataset overview when the scanned mdocs come from
several folders that reuse the same series names — SerialEM restarts `Position_N` for every
session, so two sessions under one data folder both hold `Position_1.mdoc`. It says why, offers
the two ways to import them (a project per folder, or one project whose series are named after
their folder), and shows one dataset overview per folder. Which way is the user's call: until
they pick, Create stays blocked.
"""

from collections.abc import Callable
from pathlib import Path

from nicegui import ui

from services.configs.mdoc_service import ARETOMO_TS_NAME_MAX, folder_tag
from services.tilt_series.preimport import DatasetOverview
from ui.components.fields import house_text
from ui.components.segmented import render_segmented
from ui.dataset_overview_panel import build_dataset_overview_panel, render_parse_warnings
from ui.job_plugins._field_styles import PANEL_HEAD_STYLE, PANEL_STYLE, PANEL_TITLE_STYLE
from ui.styles import MONO, SANS as FONT

CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_WARN = "#d97706"

SPLIT_PER_FOLDER = "per_folder"
SPLIT_ONE_PROJECT = "one_project"
_CHOICES = ((SPLIT_PER_FOLDER, "A project per folder"), (SPLIT_ONE_PROJECT, "One project, folder-named series"))


class FolderSplit:
    """The user's answer for one scanned overview. `choice` is None until they pick;
    `names` holds the project name typed for each folder (keyed by its path), prefilled
    with the folder's name."""

    def __init__(self, overview: DatasetOverview):
        self.overview = overview
        self.folders = overview.folder_overviews()
        self.choice: str | None = None
        self.names = {f.source_directory: Path(f.source_directory).name for f in self.folders}

    def folders_to_create(self) -> list[DatasetOverview]:
        """The folders with at least one selected series: one project each, per folder."""
        return [f for f in self.folders if f.selected_tilt_series]


class FolderSplitPanel:
    """Renders a `FolderSplit` into the current slot; `refresh()` re-reads the project name
    and the choice without rebuilding (the per-folder tables keep their expanded rows)."""

    def __init__(self, split: FolderSplit, *, project_name: Callable[[], str], on_change: Callable[[], None]) -> None:
        self._split = split
        self._project_name = project_name
        self._on_change = on_change
        self._name_rows: list[ui.element] = []
        self._series_lines: list[tuple[ui.label, str]] = []
        self._build()

    def _build(self) -> None:
        split = self._split
        collisions = split.overview.folder_name_collisions()
        examples = ", ".join(sorted(collisions)[:3]) + (", …" if len(collisions) > 3 else "")
        body = f"{FONT} font-size: 10px; color: {CLR_LABEL};"
        with ui.column().classes("w-full").style(f"gap: 6px; {PANEL_STYLE}"):
            ui.label(f"{len(split.folders)} acquisition folders reuse the same series names").style(
                f"{PANEL_TITLE_STYLE} {PANEL_HEAD_STYLE}"
            )
            ui.label(
                f"{len(collisions)} series names ({examples}) occur in more than one folder. SerialEM restarts "
                "Position_N for every session, so these look like separate sessions, and one project holds each "
                "series name once."
            ).style(body)
            with ui.column().classes("w-full gap-0"):
                for f in split.folders:
                    with ui.row().classes("items-baseline gap-2 no-wrap"):
                        ui.label(f.source_directory).style(f"{MONO} font-size: 9px; color: {CLR_LABEL};")
                        ui.label(f"{f.total_tilt_series} mdocs").style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL};")
            with ui.row().classes("items-center gap-2 no-wrap"):
                self._seg = render_segmented(_CHOICES, split.choice or "", self._pick, classes="cb-seg-sm")
                self._hint = ui.label("").style(f"{FONT} font-size: 9px;")
            render_parse_warnings(split.overview.parse_warnings)

        for f in split.folders:
            folder = f.source_directory
            with ui.column().classes("w-full").style(f"gap: 6px; {PANEL_STYLE}"):
                build_dataset_overview_panel(
                    f, on_change=self._on_change, title=Path(folder).name, title_tooltip=folder
                )
                with ui.row().classes("w-full items-center gap-2") as name_row:
                    house_text(
                        "Project name",
                        value=split.names[folder],
                        width="w-64",
                        hint="The project this folder's series import into",
                        on_change=lambda e, k=folder: self._set_name(k, e.value),
                    )
                self._name_rows.append(name_row)
                line = ui.label("").style(f"{MONO} font-size: 9px; color: {CLR_SUBLABEL};")
                self._series_lines.append((line, folder))
        self.refresh()

    def _pick(self, key: str) -> None:
        self._split.choice = key
        self._seg.set_active(key)
        self.refresh()
        self._on_change()

    def _set_name(self, folder: str, value: str | None) -> None:
        self._split.names[folder] = (value or "").strip()
        self.refresh()
        self._on_change()

    def refresh(self) -> None:
        split = self._split
        choice = split.choice
        if choice is None:
            self._hint.set_text("choose one — Create waits for it")
            self._hint.style(f"color: {CLR_WARN};")
        elif choice == SPLIT_PER_FOLDER:
            n = len(split.folders_to_create())
            self._hint.set_text(f"{n} projects, one per folder, each keeping its series names — named below")
            self._hint.style(f"color: {CLR_SUBLABEL};")
        else:
            self._hint.set_text(
                "one project, named in Project Name above; every series leads with its folder's name. "
                f"AreTomo runs a name over {ARETOMO_TS_NAME_MAX} characters under a short alias."
            )
            self._hint.style(f"color: {CLR_SUBLABEL};")
        for row in self._name_rows:
            row.set_visibility(choice == SPLIT_PER_FOLDER)
        for line, folder in self._series_lines:
            first = next((ts.ts_name for p in self._folder(folder).positions for ts in p.tilt_series), "")
            if choice == SPLIT_PER_FOLDER:
                name = split.names.get(folder) or "<project>"
                line.set_text(f"series named {name}_{first}, …")
            elif choice == SPLIT_ONE_PROJECT:
                name = (self._project_name() or "").strip() or "<project>"
                line.set_text(f"series named {name}_{folder_tag(folder)}_{first}, …")
            line.set_visibility(choice is not None)

    def _folder(self, folder: str) -> DatasetOverview:
        return next(f for f in self._split.folders if f.source_directory == folder)
