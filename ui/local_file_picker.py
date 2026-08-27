import asyncio
import fnmatch
import os
from pathlib import Path

from nicegui import ui

from ui.components.buttons import house_button
from ui.styles import MONO as _MONO, SANS as _SANS


_MAX_FILE_ROWS = 400  # file rows shipped per listing; the rest is a count

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
        upper_limit: str | None = ...,
        mode: str = "directory",
        multiple: bool = False,
        glob: str | None = None,
    ) -> None:
        """A navigable file/directory picker.

        ``mode``: ``"directory"`` returns a folder, ``"file"`` returns a file, ``"any"``
        returns WHICHEVER the user highlighted. ``"any"`` exists for pickers whose caller
        can read both — the tomogram importer takes a folder of volumes, one volume, or a
        ``tomograms.star``, and asking the user to first say which of those they have is
        the question the file browser already answers.

        ``multiple`` (file mode only) renders a checkbox per file and OK returns every
        ticked path (selection persists across navigation). ``glob`` filters which *files*
        are shown — one pattern (``"*.mrc"``) or several, comma-separated
        (``"*.mrc,*.rec"``), since fnmatch has no alternation and a file type with two
        conventional suffixes is the common case. Directories are always listed so you can
        navigate. Directory scans run off the event loop (Lustre-friendly)."""
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        self.multiple = bool(multiple)
        self.glob = glob
        self.selected_path: Path | None = None
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
                elif self.mode == "any":
                    hint = "Click a folder or a file to pick it. Double-click a folder to enter it."
                elif self.multiple:
                    hint = "Tick files to select. OK confirms the ticked set."
                else:
                    hint = "Click to select, double-click to confirm."
                ui.label(hint).style(f"{_SANS} font-size: 11px; color: #9ca3af;")
                with ui.row().classes("gap-2"):
                    house_button("Cancel", self.close)
                    house_button("OK", self._handle_ok, kind="accent")

        with self.list_container:
            ui.label("Loading…").classes("text-gray-400 text-sm p-4")
        ui.timer(0.01, self._refresh_list, once=True)

    # ── Navigation ────────────────────────────────────────────────────────────

    async def _navigate_to_typed(self) -> None:
        """Enter in the path bar. A typed path to an existing FILE is a SELECTION in single
        file mode — pasting a known absolute path is the fastest way to pick one, and
        "not a valid directory" would be a lie about a path that exists. Anything else
        navigates, under the same root limit."""
        typed = (self.path_input.value or "").strip()
        if not typed:
            return
        p = Path(typed).expanduser().resolve()
        pick_file = self.mode in ("file", "any") and not self.multiple and p.is_file()
        if not pick_file and not p.is_dir():
            ui.notify(f"Not a valid directory: {typed}", type="warning", timeout=2500)
            self.path_input.value = str(self.path)
            return
        if self.upper_limit is not None:
            under = p.parent if pick_file else p
            if under != self.upper_limit and self.upper_limit not in under.parents:
                ui.notify("Cannot navigate above the root limit", type="warning", timeout=2500)
                self.path_input.value = str(self.path)
                return
        if pick_file:
            self.submit([str(p)])
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
    def _scan_dir(path: Path, glob: str | None, dirs_only: bool) -> tuple[list[tuple], int]:
        """Off-loop directory scan via os.scandir (d_type — no per-entry stat where the
        filesystem reports it). Returns (sorted (Path, is_dir) tuples, hidden_file_count).
        ``dirs_only`` (directory pickers) lists no files at all: a 5000-frame tomogram
        directory is a number, not 5000 rows shipped over the socket. Files are otherwise
        filtered by ``glob`` (fnmatch) and capped at _MAX_FILE_ROWS."""
        out: list[tuple] = []
        hidden = 0
        n_files = 0
        try:
            with os.scandir(path) as it:
                for e in it:
                    if e.name.startswith("."):
                        continue
                    try:
                        is_dir = e.is_dir()
                    except OSError:
                        continue
                    if is_dir:
                        out.append((Path(e.path), True))
                        continue
                    if glob and not any(fnmatch.fnmatch(e.name, g) for g in glob.split(",")):
                        continue
                    n_files += 1
                    if dirs_only or n_files > _MAX_FILE_ROWS:
                        hidden += 1
                        continue
                    out.append((Path(e.path), False))
        except OSError:
            return [], 0
        out.sort(key=lambda t: (not t[1], t[0].name.lower()))
        return out, hidden

    async def _refresh_list(self) -> None:
        self.path_input.value = str(self.path)
        self._update_up_button()
        # Feedback first: the old listing must not linger while a slow NFS readdir runs,
        # or the user reads stale entries as "the folder switch did not happen".
        self.list_container.clear()
        self._row_checkboxes.clear()
        with self.list_container:
            ui.label("Loading…").classes("text-gray-400 text-sm p-4")
        target = self.path
        entries, hidden = await asyncio.to_thread(self._scan_dir, target, self.glob, self.mode == "directory")
        if target != self.path:
            return  # navigated again meanwhile; that refresh owns the list
        self.list_container.clear()
        with self.list_container:
            if not entries and not hidden:
                ui.label("Empty directory").classes("text-gray-400 text-sm p-4")
                return
            if self.multiple and any(not is_dir for _, is_dir in entries):
                self._render_select_all_bar([p for p, is_dir in entries if not is_dir])
            for item, is_dir in entries:
                self._create_row(item, is_dir)
            if hidden:
                note = f"{hidden} files" if self.mode == "directory" else f"+{hidden} more files (narrow with the glob)"
                ui.label(note).style(f"{_MONO} font-size: 10px; color: #9ca3af; padding: 6px 16px;")

    def _render_select_all_bar(self, files: list[Path]) -> None:
        with ui.row().classes("w-full items-center px-4 py-1 gap-2 bg-gray-50 border-b border-gray-100"):
            house_button("Select all", lambda: self._set_paths(files, True))
            house_button("Clear", lambda: self._set_paths(None, False))
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

    def _set_paths(self, files: list[Path] | None, value: bool) -> None:
        if value and files:
            for f in files:
                self.selected_paths.add(str(f))
        elif not value:
            self.selected_paths.clear()
        for sp, cb in self._row_checkboxes.items():
            cb.value = sp in self.selected_paths
        self._update_count()

    def _create_row(self, item: Path, is_dir: bool) -> None:
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
        elif self.mode in ("file", "any"):
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
            if self.mode == "any":
                self.submit([str(self.selected_path)])
            elif self.mode == "directory" and self.selected_path.is_dir():
                self.submit([str(self.selected_path)])
            elif self.mode == "file" and self.selected_path.is_file():
                self.submit([str(self.selected_path)])
            elif self.mode == "directory":
                ui.notify("Please select a folder", type="warning")
            else:
                ui.notify("Invalid selection", type="warning")
        else:
            # Nothing highlighted: "directory" and "any" both mean "the folder I am
            # standing in" — for "any" that is the common case (open the recon folder,
            # press OK, take everything in it).
            if self.mode in ("directory", "any"):
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
