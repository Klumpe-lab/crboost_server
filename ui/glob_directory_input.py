from pathlib import Path
from typing import Callable, Optional

from nicegui import ui

from ui.styles import MONO as _MONO
_CLR_GHOST = "#d1d5db"


class GlobDirectoryInput:
    """
    Directory-path input with filesystem autocomplete that assembles a full glob.
    The user edits a bare directory path; the component owns the extension
    (e.g. "*.eer") and produces dir/extension as .value.
    """

    def __init__(
        self,
        extension: str,
        initial_glob: str = "",
        on_change: Optional[Callable[[str], None]] = None,
        placeholder: str = "",
    ) -> None:
        self.extension = extension
        self._on_change = on_change
        self._dir_value = self._strip_glob(initial_glob)
        self._dropdown_visible = False

        with ui.element("div").classes("flex-1 min-w-0").style("position: relative;") as self._root:
            self.input_el = (
                ui.input(value=self._dir_value, placeholder=placeholder, on_change=self._handle_change)
                .props("dense borderless hide-bottom-space")
                .style(
                    f"{_MONO} font-size: 11px; width: 100%; border-bottom: 1px solid {_CLR_GHOST}; padding: 1px 2px;"
                )
            )
            self.input_el.on("keyup.escape", lambda e: self._hide_dropdown())
            self.input_el.on("blur", lambda e: ui.timer(0.15, self._hide_dropdown, once=True))

            # Dropdown: absolutely positioned below the input, initially hidden
            self._dropdown = ui.element("div").style(
                "position: absolute; top: 100%; left: 0; right: 0; z-index: 9999; "
                "background: white; border: 1px solid #e5e7eb; border-radius: 4px; "
                "box-shadow: 0 4px 12px rgba(0,0,0,0.10); display: none; "
                "max-height: 220px; overflow-y: auto;"
            )
            self._suggestion_col = ui.column().classes("w-full p-0 gap-0")

        # suggestions need to live inside the dropdown div
        with self._dropdown:
            self._suggestion_col = ui.column().classes("w-full p-0 gap-0")

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def value(self) -> str:
        return self._build_glob(self._dir_value)

    def set_directory(self, dir_path: str) -> None:
        self._dir_value = dir_path
        self.input_el.value = dir_path
        self._hide_dropdown()
        if self._on_change:
            self._on_change(self._build_glob(dir_path))

    def set_from_glob(self, glob_str: str) -> None:
        self.set_directory(self._strip_glob(glob_str))

    def set_extension(self, extension: str) -> None:
        self.extension = extension
        if self._dir_value and self._on_change:
            self._on_change(self._build_glob(self._dir_value))

    def enable(self) -> None:
        self.input_el.enable()

    def disable(self) -> None:
        self.input_el.disable()

    def props(self, props_str: str = "", *, remove: str = "") -> "GlobDirectoryInput":
        if props_str:
            self.input_el.props(props_str)
        if remove:
            self.input_el.props(remove=remove)
        return self

    # ── Dropdown show/hide ────────────────────────────────────────────────────

    def _show_dropdown(self) -> None:
        self._dropdown.style(remove="display: none;")
        self._dropdown.style("display: block;")
        self._dropdown_visible = True

    def _hide_dropdown(self) -> None:
        self._dropdown.style(remove="display: block;")
        self._dropdown.style("display: none;")
        self._dropdown_visible = False

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _strip_glob(self, glob_str: str) -> str:
        if not glob_str:
            return ""
        p = Path(glob_str)
        if "*" in p.name or "?" in p.name:
            parent = p.parent
            return "" if str(parent) == "." else str(parent)
        return glob_str

    def _build_glob(self, dir_path: str) -> str:
        if not dir_path or not dir_path.strip():
            return ""
        return str(Path(dir_path.strip()) / self.extension)

    def _handle_change(self, e) -> None:
        value = e.value if hasattr(e, "value") else ""
        self._dir_value = value or ""
        self._refresh_suggestions(self._dir_value)
        if self._on_change:
            self._on_change(self._build_glob(self._dir_value))

    def _get_completions(self, typed: str) -> list[str]:
        if not typed or not typed.strip():
            return []
        typed = typed.strip()
        p = Path(typed)

        # Path ends with slash: user confirmed a directory, show all children
        if typed.endswith("/"):
            parent = p
            prefix = ""
        elif p.is_dir():
            parent = p
            prefix = ""
        else:
            parent = p.parent
            prefix = p.name.lower()

        if not parent.is_dir():
            return []
        try:
            return sorted(
                [
                    str(d)
                    for d in parent.iterdir()
                    if d.is_dir()
                    and not d.name.startswith(".")
                    and (not prefix or d.name.lower().startswith(prefix))
                ],
                key=lambda x: Path(x).name.lower(),
            )[:10]
        except (PermissionError, FileNotFoundError):
            return []

    def _refresh_suggestions(self, typed: str) -> None:
        completions = self._get_completions(typed)
        self._suggestion_col.clear()
        if not completions:
            self._hide_dropdown()
            return
        with self._suggestion_col:
            for path_str in completions:
                name = Path(path_str).name
                (
                    ui.button(name, on_click=lambda p=path_str: self.set_directory(p))
                    .props("flat no-caps align=left")
                    .classes("w-full rounded-none hover:bg-blue-50")
                    .style(
                        f"{_MONO} font-size: 11px; color: #374151; "
                        "padding: 5px 12px; text-align: left; border-bottom: 1px solid #f3f4f6;"
                    )
                )
        self._show_dropdown()
