import asyncio
import fnmatch
from pathlib import Path
from typing import List, Optional

from nicegui import ui

from ui.styles import MONO as _MONO, SANS as _SANS


_FOLDER_SVG = (
    '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">'
    '<path d="M1.5 4C1.5 3.448 1.948 3 2.5 3H6.379L7.72 4.2C7.893 4.388 8.138 4.5 8.394 4.5H13.5'
    "C14.052 4.5 14.5 4.948 14.5 5.5V12C14.5 12.552 14.052 13 13.5 13H2.5C1.948 13 1.5 12.552 1.5 "
    '12V4Z" fill="#fbbf24" stroke="#d97706" stroke-width="0.75"/>'
    "</svg>"
)

_FILE_SVG = (
    '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">'
    '<path d="M3.5 1.5H9.5L13 5V14.5H3.5V1.5Z" fill="#f9fafb" stroke="#d1d5db" stroke-width="0.75"/>'
    '<path d="M9.5 1.5V5H13" stroke="#d1d5db" stroke-width="0.75" fill="none"/>'
    '<line x1="5.5" y1="7.5" x2="11" y2="7.5" stroke="#e5e7eb" stroke-width="0.75"/>'
    '<line x1="5.5" y1="9.5" x2="11" y2="9.5" stroke="#e5e7eb" stroke-width="0.75"/>'
    '<line x1="5.5" y1="11.5" x2="9" y2="11.5" stroke="#e5e7eb" stroke-width="0.75"/>'
    "</svg>"
)


class local_file_picker(ui.dialog):
    def __init__(
        self,
        directory: str,
        *,
        upper_limit: Optional[str] = ...,
        mode: str = "directory",
        multiple: bool = False,
        glob: Optional[str] = None,
    ) -> None:
        """A navigable file/directory picker.

        ``multiple`` (file mode only) renders a checkbox per file and OK returns every
        ticked path (selection persists across navigation). ``glob`` (e.g. ``"*.mrc"``)
        filters which *files* are shown — directories are always listed so you can
        navigate. Directory scans run off the event loop (Lustre-friendly)."""
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        self.multiple = bool(multiple)
        self.glob = glob
        self.selected_path: Optional[Path] = None
        self.selected_paths: set[str] = set()  # multi-select: absolute path strings
        self._row_checkboxes: dict[str, ui.checkbox] = {}  # current view's file checkboxes
        self.count_label = None  # select-all bar counter (multi mode)

        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = Path(directory if upper_limit == ... else upper_limit).expanduser().resolve()

        with self, ui.card().classes("p-0").style("width: 70vw; max-width: 900px;"):
            # Header: up button + editable path bar
            with ui.row().classes("w-full items-center px-3 py-2 bg-gray-50 border-b gap-2"):
                self.up_button = (
                    ui.button(icon="arrow_upward", on_click=self._go_up)
                    .props("flat round dense size=sm")
                    .classes("text-gray-500 shrink-0")
                )
                self.path_input = (
                    ui.input(value=str(self.path))
                    .props("dense borderless hide-bottom-space")
                    .style(f"{_MONO} font-size: 12px; color: #374151; flex: 1;")
                )
                self.path_input.on("keyup.enter", lambda e: self._navigate_to_typed())

            # File list
            self.list_container = ui.column().classes("w-full p-0 overflow-y-auto").style("height: 50vh;")

            # Footer
            with ui.row().classes("w-full justify-between items-center px-4 py-3 bg-gray-50 border-t"):
                if self.mode == "directory":
                    hint = "Double-click folder to enter. OK selects current directory."
                elif self.multiple:
                    hint = "Tick files to select. OK confirms the ticked set."
                else:
                    hint = "Click to select, double-click to confirm."
                ui.label(hint).style(f"{_SANS} font-size: 11px; color: #9ca3af;")
                with ui.row().classes("gap-2"):
                    ui.button("Cancel", on_click=self.close).props("flat no-caps").style(
                        f"{_SANS} font-size: 12px; color: #6b7280;"
                    )
                    ui.button("OK", on_click=self._handle_ok).props("no-caps unelevated").style(
                        f"{_SANS} font-size: 12px; background: #2563eb; color: white; "
                        "border-radius: 6px; padding: 3px 16px;"
                    )

        with self.list_container:
            ui.label("Loading…").classes("text-gray-400 text-sm p-4")
        ui.timer(0.01, self._refresh_list, once=True)

    # ── Navigation ────────────────────────────────────────────────────────────

    async def _navigate_to_typed(self) -> None:
        typed = (self.path_input.value or "").strip()
        if not typed:
            return
        p = Path(typed).expanduser().resolve()
        if not p.exists() or not p.is_dir():
            ui.notify(f"Not a valid directory: {typed}", type="warning", timeout=2500)
            self.path_input.value = str(self.path)
            return
        if self.upper_limit is not None:
            if p != self.upper_limit and self.upper_limit not in p.parents:
                ui.notify("Cannot navigate above the root limit", type="warning", timeout=2500)
                self.path_input.value = str(self.path)
                return
        self.path = p
        self.selected_path = None
        await self._refresh_list()

    async def _go_up(self) -> None:
        parent = self.path.parent
        if parent != self.path:
            self.path = parent
            self.selected_path = None
            await self._refresh_list()

    # ── List ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _scan_dir(path: Path, glob: Optional[str]) -> List[tuple]:
        """Off-loop directory scan: returns sorted (Path, is_dir, size|None) tuples with
        all stat/iterdir cost paid here (never on the event loop or in the render path).
        Files are filtered by ``glob`` (fnmatch); directories are always kept."""
        try:
            raw = [p for p in path.iterdir() if not p.name.startswith(".")]
        except (PermissionError, FileNotFoundError, OSError):
            return []
        out: List[tuple] = []
        for p in raw:
            try:
                is_dir = p.is_dir()
            except OSError:
                continue
            if not is_dir and glob and not fnmatch.fnmatch(p.name, glob):
                continue
            size = None
            if not is_dir:
                try:
                    size = p.stat().st_size
                except OSError:
                    size = None
            out.append((p, is_dir, size))
        out.sort(key=lambda t: (not t[1], t[0].name.lower()))
        return out

    async def _refresh_list(self) -> None:
        self.path_input.value = str(self.path)
        self._update_up_button()
        entries = await asyncio.to_thread(self._scan_dir, self.path, self.glob)
        self.list_container.clear()
        self._row_checkboxes.clear()
        with self.list_container:
            if not entries:
                ui.label("Empty directory").classes("text-gray-400 text-sm p-4")
                return
            if self.multiple and any(not is_dir for _, is_dir, _ in entries):
                self._render_select_all_bar([p for p, is_dir, _ in entries if not is_dir])
            for item, is_dir, size in entries:
                self._create_row(item, is_dir, size)

    def _render_select_all_bar(self, files: List[Path]) -> None:
        with ui.row().classes("w-full items-center px-4 py-1 gap-2 bg-gray-50 border-b border-gray-100"):
            ui.button("Select all", on_click=lambda: self._set_paths(files, True)).props(
                "flat dense no-caps size=sm"
            ).style(f"{_SANS} font-size: 11px; color: #4f46e5;")
            ui.button("Clear", on_click=lambda: self._set_paths(None, False)).props("flat dense no-caps size=sm").style(
                f"{_SANS} font-size: 11px; color: #6b7280;"
            )
            self.count_label = ui.label(self._count_text()).style(
                f"{_MONO} font-size: 10px; color: #9ca3af; margin-left: auto;"
            )

    def _count_text(self) -> str:
        return f"{len(self.selected_paths)} selected"

    def _update_count(self) -> None:
        if self.count_label is not None:
            self.count_label.text = self._count_text()

    def _toggle_path(self, path: str, value: bool) -> None:
        if value:
            self.selected_paths.add(path)
        else:
            self.selected_paths.discard(path)
        self._update_count()

    def _set_paths(self, files: Optional[List[Path]], value: bool) -> None:
        if value and files:
            for f in files:
                self.selected_paths.add(str(f))
        elif not value:
            self.selected_paths.clear()
        for sp, cb in self._row_checkboxes.items():
            cb.value = sp in self.selected_paths
        self._update_count()

    def _create_row(self, item: Path, is_dir: bool, size: Optional[int]) -> None:
        row = ui.row().classes("w-full items-center px-4 py-1 cursor-pointer hover:bg-blue-50 border-b border-gray-100")
        row.path = item
        row.is_dir = is_dir
        multi_file = self.multiple and not is_dir

        with row:
            if multi_file:
                cb = ui.checkbox(
                    value=str(item) in self.selected_paths,
                    on_change=lambda e, p=str(item): self._toggle_path(p, bool(e.value)),
                ).props("dense")
                self._row_checkboxes[str(item)] = cb
            ui.html(_FOLDER_SVG if is_dir else _FILE_SVG, sanitize=False).classes("shrink-0")
            ui.label(item.name).style(f"{_SANS} font-size: 13px; color: #374151;").classes("ml-2 truncate flex-1")
            if not is_dir and size is not None:
                size_str = f"{size / 1024:.1f} KB" if size > 1024 else f"{size} B"
                ui.label(size_str).style(f"{_MONO} font-size: 10px; color: #9ca3af;")

        row.on("dblclick", lambda e, r=row: self._double_click_row(r))
        if not multi_file:
            row.on("click", lambda e, r=row: self._select_row(r))

    def _select_row(self, row) -> None:
        for child in self.list_container:
            if hasattr(child, "path"):
                child.classes(remove="bg-blue-100")
        row.classes("bg-blue-100")
        self.selected_path = row.path

    async def _double_click_row(self, row) -> None:
        if row.is_dir:
            self.path = row.path
            self.selected_path = None
            await self._refresh_list()
        elif self.multiple:
            p = str(row.path)
            now = p not in self.selected_paths
            self._toggle_path(p, now)
            cb = self._row_checkboxes.get(p)
            if cb is not None:
                cb.value = now
        elif self.mode == "file":
            self.submit([str(row.path)])

    # ── OK ────────────────────────────────────────────────────────────────────

    async def _handle_ok(self):
        if self.multiple:
            if self.selected_paths:
                self.submit(sorted(self.selected_paths))
            else:
                ui.notify("Tick at least one file", type="warning")
            return
        if self.selected_path:
            if self.mode == "directory" and self.selected_path.is_dir():
                self.submit([str(self.selected_path)])
            elif self.mode == "file" and self.selected_path.is_file():
                self.submit([str(self.selected_path)])
            elif self.mode == "directory":
                ui.notify("Please select a folder", type="warning")
            else:
                ui.notify("Invalid selection", type="warning")
        else:
            if self.mode == "directory":
                self.submit([str(self.path)])
            else:
                ui.notify("Please select a file", type="warning")

    # ── Up button ─────────────────────────────────────────────────────────────

    def _update_up_button(self) -> None:
        at_root = self.path == self.path.parent
        if self.upper_limit is None:
            self.up_button.props(f"disable={at_root}")
        else:
            at_limit = self.path == self.upper_limit or self.upper_limit in self.path.parents
            self.up_button.props(f"disable={at_root or at_limit}")
