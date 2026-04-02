from pathlib import Path
from typing import Optional

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
    ) -> None:
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        self.selected_path: Optional[Path] = None

        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = (
                Path(directory if upper_limit == ... else upper_limit).expanduser().resolve()
            )

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
            self.list_container = (
                ui.column().classes("w-full p-0 overflow-y-auto").style("height: 50vh;")
            )

            # Footer
            with ui.row().classes("w-full justify-between items-center px-4 py-3 bg-gray-50 border-t"):
                if self.mode == "directory":
                    ui.label("Double-click folder to enter. OK selects current directory.").style(
                        f"{_SANS} font-size: 11px; color: #9ca3af;"
                    )
                else:
                    ui.label("Click to select, double-click to confirm.").style(
                        f"{_SANS} font-size: 11px; color: #9ca3af;"
                    )
                with ui.row().classes("gap-2"):
                    ui.button("Cancel", on_click=self.close).props("flat no-caps").style(
                        f"{_SANS} font-size: 12px; color: #6b7280;"
                    )
                    ui.button("OK", on_click=self._handle_ok).props("no-caps unelevated").style(
                        f"{_SANS} font-size: 12px; background: #2563eb; color: white; "
                        "border-radius: 6px; padding: 3px 16px;"
                    )

        self._refresh_list()

    # ── Navigation ────────────────────────────────────────────────────────────

    def _navigate_to_typed(self) -> None:
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
        self._refresh_list()

    def _go_up(self) -> None:
        parent = self.path.parent
        if parent != self.path:
            self.path = parent
            self.selected_path = None
            self._refresh_list()

    # ── List ─────────────────────────────────────────────────────────────────

    def _refresh_list(self) -> None:
        self.list_container.clear()
        self.path_input.value = str(self.path)
        self._update_up_button()

        try:
            items = sorted(
                [p for p in self.path.iterdir() if not p.name.startswith(".")],
                key=lambda p: (not p.is_dir(), p.name.lower()),
            )
        except (PermissionError, FileNotFoundError):
            items = []

        with self.list_container:
            if not items:
                ui.label("Empty directory").classes("text-gray-400 text-sm p-4")
                return
            for item in items:
                self._create_row(item)

    def _create_row(self, item: Path) -> None:
        is_dir = item.is_dir()
        row = ui.row().classes(
            "w-full items-center px-4 py-1 cursor-pointer hover:bg-blue-50 border-b border-gray-100"
        )
        row.path = item
        row.is_dir = is_dir

        with row:
            ui.html(_FOLDER_SVG if is_dir else _FILE_SVG, sanitize=False).classes("shrink-0")
            ui.label(item.name).style(
                f"{_SANS} font-size: 13px; color: #374151;"
            ).classes("ml-2 truncate flex-1")
            if not is_dir:
                try:
                    size = item.stat().st_size
                    size_str = f"{size / 1024:.1f} KB" if size > 1024 else f"{size} B"
                    ui.label(size_str).style(
                        f"{_MONO} font-size: 10px; color: #9ca3af;"
                    )
                except Exception:
                    pass

        row.on("click", lambda e, r=row: self._select_row(r))
        row.on("dblclick", lambda e, r=row: self._double_click_row(r))

    def _select_row(self, row) -> None:
        for child in self.list_container:
            if hasattr(child, "path"):
                child.classes(remove="bg-blue-100")
        row.classes("bg-blue-100")
        self.selected_path = row.path

    def _double_click_row(self, row) -> None:
        if row.is_dir:
            self.path = row.path
            self.selected_path = None
            self._refresh_list()
        elif self.mode == "file":
            self.submit([str(row.path)])

    # ── OK ────────────────────────────────────────────────────────────────────

    async def _handle_ok(self):
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
