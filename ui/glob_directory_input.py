import asyncio
import os
from pathlib import Path
from collections.abc import Callable

from nicegui import ui

from ui.styles import MONO as _MONO

_CLR_GHOST = "#d1d5db"
_MAX_COMPLETIONS = 12
_DEBOUNCE_SEC = 0.25


# parent dir -> (mtime, sorted subdir names). One readdir per directory per
# change, not one per keystroke: on NFS/Lustre a 5000-entry tomogram directory
# with no d_type support costs a stat per entry, and the user types 20 keys.
_SUBDIR_CACHE: dict[str, tuple[float, list[str]]] = {}


def _subdirs_of(parent: str) -> list[str]:
    try:
        mtime = os.stat(parent).st_mtime
    except OSError:  # missing / unreadable parent: nothing to suggest
        return []
    cached = _SUBDIR_CACHE.get(parent)
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        with os.scandir(parent) as it:
            names = [e.name for e in it if not e.name.startswith(".") and e.is_dir()]
    except OSError:
        return []
    names.sort(key=str.lower)
    _SUBDIR_CACHE[parent] = (mtime, names)
    return names


def _list_subdirs(typed: str) -> list[str]:
    """Filesystem read for the dropdown. Runs in a worker thread — never list a
    directory on the event loop."""
    typed = typed.strip()
    if not typed:
        return []
    if typed.endswith("/"):
        parent, prefix = typed, ""
    else:
        parent, prefix = os.path.dirname(typed) or "/", os.path.basename(typed).lower()
    return [os.path.join(parent, n) for n in _subdirs_of(parent) if not prefix or n.lower().startswith(prefix)]


class GlobDirectoryInput:
    """
    Directory-path input with filesystem autocomplete that assembles a full glob.
    The user edits a bare directory path; the component owns the extension
    (e.g. "*.eer") and produces dir/extension as .value. With an empty
    extension .value is the bare directory.

    Two callbacks: `on_change` fires on every edit (keep it cheap); `on_commit`
    fires when the user commits to a directory — Enter, or picking a recent
    entry — and is where expensive work (indexing, mdoc parsing) belongs.
    `recent_provider` supplies MRU paths shown at the top of the dropdown.
    """

    def __init__(
        self,
        extension: str,
        initial_glob: str = "",
        on_change: Callable[[str], None] | None = None,
        on_commit: Callable[[str], None] | None = None,
        recent_provider: Callable[[], list[str]] | None = None,
        on_forget_recent: Callable[[str], None] | None = None,
        placeholder: str = "",
    ) -> None:
        self.extension = extension
        self._on_change = on_change
        self._on_commit = on_commit
        self._recent_provider = recent_provider
        self._on_forget_recent = on_forget_recent
        self._dir_value = self._strip_glob(initial_glob)
        self._completions: list[str] = []
        self._lookup_task: asyncio.Task | None = None
        self._lookup_gen = 0

        with ui.element("div").classes("flex-1 min-w-0").style("position: relative;"):
            self.input_el = (
                ui.input(value=self._dir_value, placeholder=placeholder, on_change=self._handle_change)
                .props("dense borderless hide-bottom-space")
                .style(
                    f"{_MONO} font-size: 11px; width: 100%; border-bottom: 1px solid {_CLR_GHOST}; padding: 1px 2px;"
                )
            )
            self.input_el.on("keyup.escape", lambda e: self._hide_dropdown())
            self.input_el.on("keydown.enter", lambda e: self._commit())
            self.input_el.on("keydown.tab.prevent", lambda e: self._tab_complete())
            self.input_el.on("focus", lambda e: self._show_recents_if_idle())
            self.input_el.on("blur", lambda e: ui.timer(0.15, self._hide_dropdown, once=True))

            self._dropdown = ui.element("div").style(
                "position: absolute; top: 100%; left: 0; right: 0; z-index: 9999; "
                "background: white; border: 1px solid #e5e7eb; border-radius: 4px; "
                "box-shadow: 0 4px 12px rgba(0,0,0,0.10); display: none; "
                "max-height: 260px; overflow-y: auto;"
            )
            with self._dropdown:
                self._suggestion_col = ui.column().classes("w-full p-0 gap-0")

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def value(self) -> str:
        return self._build_glob(self._dir_value)

    @value.setter
    def value(self, glob_or_dir: str) -> None:
        """Silent set: updates the field without firing on_change (callers that
        assign .value have already updated state themselves)."""
        self._dir_value = self._strip_glob(glob_or_dir or "")
        self.input_el.value = self._dir_value

    def set_directory(self, dir_path: str, *, commit: bool = False) -> None:
        self._dir_value = dir_path
        self.input_el.value = dir_path
        self._hide_dropdown()
        if self._on_change:
            self._on_change(self._build_glob(dir_path))
        if commit:
            self._commit()

    def set_from_glob(self, glob_str: str, *, commit: bool = False) -> None:
        self.set_directory(self._strip_glob(glob_str), commit=commit)

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

    def _hide_dropdown(self) -> None:
        self._dropdown.style(remove="display: block;")
        self._dropdown.style("display: none;")

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
        if not self.extension:
            return dir_path.strip()
        return str(Path(dir_path.strip()) / self.extension)

    def _commit(self) -> None:
        self._hide_dropdown()
        if self._on_commit and self._dir_value.strip():
            self._on_commit(self._build_glob(self._dir_value))

    def _handle_change(self, e) -> None:
        value = e.value if hasattr(e, "value") else ""
        self._dir_value = value or ""
        self._schedule_lookup(self._dir_value)
        if self._on_change:
            self._on_change(self._build_glob(self._dir_value))

    def _tab_complete(self) -> None:
        """Tab: accept the single match, else extend to the common prefix."""
        if not self._completions:
            return
        if len(self._completions) == 1:
            self.set_directory(self._completions[0] + "/")
            return
        common = os.path.commonprefix(self._completions)
        if len(common) > len(self._dir_value):
            self.set_directory(common)

    def _recents(self, typed: str) -> list[str]:
        if not self._recent_provider:
            return []
        typed = typed.strip()
        return [p for p in self._recent_provider() if p != typed and (not typed or p.startswith(typed))]

    def _forget(self, path_str: str, subdirs: list[str]) -> None:
        self._on_forget_recent(path_str)
        self._render(subdirs, self._recents(self._dir_value))

    def _show_recents_if_idle(self) -> None:
        if not self._dir_value.strip():
            self._render([], self._recents(""))

    def _schedule_lookup(self, typed: str) -> None:
        """Debounced, threaded directory listing; a newer keystroke supersedes
        an in-flight lookup so a slow NFS readdir can never repaint stale results."""
        self._lookup_gen += 1
        gen = self._lookup_gen
        if self._lookup_task and not self._lookup_task.done():
            self._lookup_task.cancel()

        async def _run():
            await asyncio.sleep(_DEBOUNCE_SEC)
            subdirs = await asyncio.to_thread(_list_subdirs, typed)
            if gen != self._lookup_gen:
                return
            self._completions = subdirs
            self._render(subdirs, self._recents(typed))

        self._lookup_task = asyncio.create_task(_run())

    def _render(self, subdirs: list[str], recents: list[str]) -> None:
        self._suggestion_col.clear()
        if not subdirs and not recents:
            self._hide_dropdown()
            return
        row_style = (
            f"{_MONO} font-size: 11px; color: #374151; padding: 5px 12px; text-align: left; "
            "border-bottom: 1px solid #f3f4f6;"
        )
        with self._suggestion_col:
            for path_str in recents:
                # Recent entries: sky tint (vs. the plain blue-50 hover of live
                # completions) + a tiny orange tag so they read as "history".
                with (
                    ui.row()
                    .classes("w-full items-center no-wrap")
                    .style("gap: 0; background: #f0f9ff; border-left: 2px solid #f59e0b;")
                ):
                    ui.button(path_str, on_click=lambda p=path_str: self.set_directory(p, commit=True)).props(
                        "flat no-caps align=left"
                    ).classes("flex-1 min-w-0 rounded-none hover:bg-sky-100").style(row_style + " color: #0369a1;")
                    ui.label("recently viewed").style(
                        "font-family: system-ui, sans-serif; font-size: 8px; color: #f59e0b; "
                        "letter-spacing: 0.04em; white-space: nowrap; padding: 0 6px;"
                    )
                    if self._on_forget_recent:
                        ui.button(icon="close", on_click=lambda p=path_str: self._forget(p, subdirs)).props(
                            "flat dense round size=xs"
                        ).classes("text-slate-300 hover:text-red-400 mr-1")
            shown = subdirs[:_MAX_COMPLETIONS]
            for path_str in shown:
                ui.button(Path(path_str).name + "/", on_click=lambda p=path_str: self.set_directory(p)).props(
                    "flat no-caps align=left"
                ).classes("w-full rounded-none hover:bg-blue-50").style(row_style)
            if len(subdirs) > len(shown):
                ui.label(f"+{len(subdirs) - len(shown)} more — keep typing").style(
                    f"{_MONO} font-size: 9px; color: #94a3b8; padding: 4px 12px;"
                )
        self._show_dropdown()
