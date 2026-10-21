import asyncio
import fnmatch
import html
import os

from nicegui import ui

from ui.components.buttons import house_button
from ui.components.reactive import Debounced
from ui.dashboard.css import ensure_assets_loaded
from ui.styles import MONO as _MONO, SANS as _SANS


_MAX_ROWS = 500  # rows shipped per paint; the rest is a count ("type to narrow")
_TYPE_SETTLE_S = 0.12
_WILDCARDS = ("*", "?", "[")

# Where the previous pick landed. A picker opened with no directory starts here, so the second
# template of a session does not start the walk from / again. One server per user, so one value.
_last_dir: str | None = None

_FOLDER_SVG = (
    '<svg width="12" height="12" viewBox="0 0 16 16" style="flex-shrink:0">'
    '<path d="M1.5 4C1.5 3.45 1.95 3 2.5 3H6.4L7.9 4.5H13.5C14.05 4.5 14.5 4.95 14.5 5.5V12'
    'C14.5 12.55 14.05 13 13.5 13H2.5C1.95 13 1.5 12.55 1.5 12V4Z" fill="#bfdbfe" stroke="#60a5fa"/></svg>'
)
_FILE_SVG = (
    '<svg width="12" height="12" viewBox="0 0 16 16" style="flex-shrink:0">'
    '<path d="M3.5 1.5H9.5L13 5V14.5H3.5V1.5Z" fill="#fff" stroke="#94a3b8"/>'
    '<path d="M9.5 1.5V5H13" fill="none" stroke="#94a3b8"/></svg>'
)

_ROW_STYLE = (
    f"display: flex; align-items: center; gap: 6px; height: 19px; padding: 0 10px; {_SANS} "
    "font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
)
_NOTE_STYLE = f"{_SANS} font-size: 10px; color: #94a3b8; padding: 6px 10px;"

# One listener per list, not per row: the rows are an HTML string, so a 500-entry folder is one
# element on the socket. The click highlights at once in the browser and tells the server which.
_CLICK_JS = (
    "(e) => { const r = e.target.closest('[data-i]'); if (!r) return; "
    "r.parentElement.querySelectorAll('.cb-fp-sel').forEach(x => x.classList.remove('cb-fp-sel')); "
    "r.classList.add('cb-fp-sel'); emit({i: Number(r.dataset.i)}); }"
)
_OPEN_JS = "(e) => { const r = e.target.closest('[data-i]'); if (r) emit({i: Number(r.dataset.i)}); }"


def _arrow_js(list_id: int) -> str:
    """↑/↓ in the path bar move the highlight through the list, in the browser."""
    return (
        "(e) => { if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return; e.preventDefault(); "
        f"const rows = getHtmlElement({list_id}).querySelectorAll('[data-i]'); if (!rows.length) return; "
        "let k = -1; rows.forEach((r, j) => { if (r.classList.contains('cb-fp-sel')) k = j; }); "
        "k = e.key === 'ArrowDown' ? Math.min(k + 1, rows.length - 1) : Math.max(k - 1, 0); "
        "rows.forEach(r => r.classList.remove('cb-fp-sel')); rows[k].classList.add('cb-fp-sel'); "
        "rows[k].scrollIntoView({block: 'nearest'}); emit({i: Number(rows[k].dataset.i)}); }"
    )


def _scan(folder: str) -> list[tuple[str, bool]] | None:
    """(name, is_dir) of a folder, folders first; None when it cannot be read. os.scandir uses
    d_type, so no per-entry stat where the filesystem reports it. Runs in a thread."""
    out: list[tuple[str, bool]] = []
    try:
        with os.scandir(folder) as it:
            for e in it:
                if e.name.startswith("."):
                    continue
                try:
                    out.append((e.name, e.is_dir()))
                except OSError:  # entry vanished or is unreadable mid-scan: not listable
                    continue
    except OSError:  # missing / unreadable folder: the caller says so
        return None
    out.sort(key=lambda t: (not t[1], t[0].lower()))
    return out


def _kind(path: str) -> str | None:
    if os.path.isdir(path):
        return "dir"
    if os.path.isfile(path):
        return "file"
    return None


def _nearest_dir(path: str) -> str:
    while not os.path.isdir(path):
        parent = os.path.dirname(path)
        if parent == path:
            return os.path.expanduser("~")
        path = parent
    return path


def _matches(name: str, typed: str) -> bool:
    if not typed:
        return True
    if any(c in typed for c in _WILDCARDS):
        return fnmatch.fnmatch(name.lower(), typed.lower())
    return typed.lower() in name.lower()


class local_file_picker(ui.dialog):
    def __init__(self, directory: str | None = None, *, mode: str = "directory", glob: str | None = None) -> None:
        """The file/folder browser every picker in the app opens.

        ``mode``: ``"directory"`` returns a folder, ``"file"`` returns a file, ``"any"``
        returns WHICHEVER the user highlighted. ``"any"`` exists for pickers whose caller
        can read both — the tomogram importer takes a folder of volumes, one volume, or a
        ``tomograms.star``, and asking the user to first say which of those they have is
        the question the file browser already answers.

        The path bar is the autocomplete: what is typed after the last ``/`` filters the
        listing of the folder before it (substring, or a wildcard pattern), so the list
        under the bar IS the suggestion list and typing costs no disk read once the folder
        is listed. Tab completes, ↑/↓ choose, Enter opens. A pasted path goes straight
        there; a path that does not exist lands on its nearest existing folder and says so.

        ``glob`` prefills the file filter — one pattern (``"*.mrc"``) or several,
        comma-separated (``"*.mrc,*.rec"``); the user can edit or clear it. Folders are
        always listed. ``directory=None`` starts where the previous pick landed."""
        super().__init__()
        ensure_assets_loaded()

        self.mode = mode
        self._start = directory
        self._dir = ""  # the folder listed
        self._entries: list[tuple[str, bool]] = []
        self._rows: list[tuple[str, bool]] = []  # selectable rows by data-i: (absolute path, is_dir)
        self._sel: int | None = None
        self._scans: dict[str, list[tuple[str, bool]] | None] = {}  # this dialog's listings
        self._gen = 0
        self._echo: str | None = None  # a path-bar value this class wrote itself
        self._typed_task: asyncio.Task | None = None
        self._typed_settled = Debounced(self._kick_typed, _TYPE_SETTLE_S)

        with self, ui.card().style("width: 680px; max-width: 92vw; padding: 0; gap: 0; border-radius: 6px;"):
            with (
                ui.row()
                .classes("w-full items-center no-wrap")
                .style("gap: 2px; padding: 5px 8px; background: #f8fafc; border-bottom: 1px solid #e2e8f0;")
            ):
                self._up = self._icon("arrow_upward", self._go_up, "Parent folder")
                self._icon("home", self._go_home, "Home folder")
                self.path_input = (
                    ui.input(on_change=self._on_typed)
                    .props("dense hide-bottom-space autofocus autocomplete=off spellcheck=false")
                    .classes("cb-field cb-field-mono flex-1 min-w-0")
                    .style("margin-left: 4px;")
                    .tooltip("Type or paste a path · Tab completes · ↑↓ choose · Enter opens")
                )
                self.path_input.on("keydown.enter", self._on_enter)
                self.path_input.on("keydown.tab.prevent", self._on_tab)
                self._glob_input = None
                if mode != "directory":
                    self._glob_input = (
                        ui.input(value=glob or "", placeholder="all files", on_change=self._on_glob)
                        .props("dense hide-bottom-space autocomplete=off spellcheck=false")
                        .classes("cb-field cb-field-mono")
                        .style("width: 110px; margin-left: 6px;")
                        .tooltip("Files shown: one pattern or several, comma-separated (*.mrc,*.rec). Empty = all")
                    )

            with ui.element("div").style("height: 46vh; overflow-y: auto; padding: 2px 0;"):
                self._list = ui.html("", sanitize=False)
            self._list.on("click", self._on_click, js_handler=_CLICK_JS)
            self._list.on("dblclick", self._on_open, js_handler=_OPEN_JS)
            self.path_input.on("keydown", self._on_click, js_handler=_arrow_js(self._list.id))

            with (
                ui.row()
                .classes("w-full items-center no-wrap")
                .style("gap: 6px; padding: 5px 8px; background: #f8fafc; border-top: 1px solid #e2e8f0;")
            ):
                self._status = ui.label("").style(
                    f"{_MONO} font-size: 10px; color: #94a3b8; flex: 1; min-width: 0; "
                    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
                )
                house_button("Cancel", self.close)
                house_button("Select", self._on_ok, kind="accent")

            ui.timer(0.01, self._open_start, once=True)

    @staticmethod
    def _icon(icon: str, handler, tip: str) -> ui.button:
        return (
            ui.button(icon=icon, on_click=handler)
            .props("flat dense round size=xs")
            .classes("text-slate-400 shrink-0")
            .tooltip(tip)
        )

    # ── Navigation ────────────────────────────────────────────────────────────

    def _abs(self, typed: str) -> str:
        """An absolute path for what is in the bar; relative text is relative to the listed
        folder. Symlinks are kept as typed — /groups/… stays /groups/…, not its mount."""
        t = os.path.expanduser(typed.strip())
        return os.path.abspath(os.path.join(self._dir or os.path.expanduser("~"), t))

    def _split_typed(self) -> tuple[str, str]:
        """(folder to list, name filter) for the path bar."""
        typed = (self.path_input.value or "").strip()
        if not typed:
            return self._dir, ""
        p = self._abs(typed)
        if typed.endswith("/"):
            return p, ""
        return os.path.dirname(p), os.path.basename(p)

    def _set_bar(self, value: str) -> None:
        self._echo = value
        self.path_input.value = value

    async def _open_start(self) -> None:
        wanted = os.path.abspath(os.path.expanduser(self._start or _last_dir or "~"))
        start = await asyncio.to_thread(_nearest_dir, wanted)
        await self._enter(start)
        if start != wanted:
            self._note(f"{wanted} does not exist · nearest folder shown", bad=True)

    async def _enter(self, folder: str) -> None:
        """List `folder` (fresh read) and put it in the bar, ready for the next name."""
        self._scans.pop(folder, None)
        self._set_bar(folder.rstrip("/") + "/")
        self._sel = None
        if await self._load(folder):
            self._paint()

    async def _load(self, folder: str) -> bool:
        """Make `folder` the listed one. False (listing unchanged) when it cannot be read or a
        newer navigation overtook this one."""
        self._gen += 1
        gen = self._gen
        if folder not in self._scans:
            self._list.style("opacity: 0.45;")
            self._note(f"reading {folder} …")
            self._scans[folder] = await asyncio.to_thread(_scan, folder)
            self._list.style("opacity: 1;")
        if gen != self._gen:
            return False
        entries = self._scans[folder]
        if entries is None:
            self._scans.pop(folder, None)
            self._note(f"cannot read {folder}", bad=True)
            return False
        self._dir = folder
        self._entries = entries
        self._up.set_enabled(folder != os.path.dirname(folder))
        return True

    async def _go_up(self) -> None:
        if self._dir and self._dir != os.path.dirname(self._dir):
            await self._enter(os.path.dirname(self._dir))

    async def _go_home(self) -> None:
        await self._enter(os.path.expanduser("~"))

    # ── Path bar ──────────────────────────────────────────────────────────────

    def _on_typed(self, e) -> None:
        if e.value == self._echo:
            self._echo = None
            return
        self._echo = None
        self._sel = None
        self._typed_settled()

    def _kick_typed(self) -> None:
        self._typed_task = asyncio.create_task(self._apply_typed())

    async def _apply_typed(self) -> None:
        folder, _ = self._split_typed()
        if folder and folder != self._dir:
            if await asyncio.to_thread(_kind, folder) != "dir":
                self._note(f"no folder {folder}", bad=True)
                return
            if not await self._load(folder):
                return
        self._paint()

    async def _on_tab(self) -> None:
        self._typed_settled.cancel()
        await self._apply_typed()
        folder, typed = self._split_typed()
        if folder != self._dir or not typed:
            return
        names = [(n, d) for n, d in self._entries if n.lower().startswith(typed.lower())]
        if len(names) == 1:
            name, is_dir = names[0]
            if is_dir:
                await self._enter(os.path.join(folder, name))
            else:
                self._set_bar(os.path.join(folder, name))
            return
        common = os.path.commonprefix([n for n, _ in names])
        if len(common) > len(typed):
            self._set_bar(os.path.join(folder, common))
            self._paint_for(common)

    async def _on_enter(self) -> None:
        if self._sel is not None:
            await self._open_row(self._sel)
            return
        typed = (self.path_input.value or "").strip()
        if not typed:
            return
        p = self._abs(typed)
        kind = await asyncio.to_thread(_kind, p)
        if kind == "dir":
            await self._enter(p)
        elif kind == "file":
            if self.mode == "directory":
                self._note("that is a file · this asks for a folder", bad=True)
            else:
                self._pick(p, False)
        elif any(c in os.path.basename(p) for c in _WILDCARDS):
            # A pasted glob (…/frames/*.eer): its folder, with the pattern as the file filter.
            parent = await asyncio.to_thread(_nearest_dir, os.path.dirname(p))
            if self._glob_input is not None:
                self._glob_input.value = os.path.basename(p)
            await self._enter(parent)
        else:
            nearest = await asyncio.to_thread(_nearest_dir, p)
            await self._enter(nearest)
            self._note(f"{p} does not exist · nearest folder shown", bad=True)

    # ── List ──────────────────────────────────────────────────────────────────

    def _note(self, text: str, *, bad: bool = False) -> None:
        self._status.text = text
        self._status.style(f"color: {'#b91c1c' if bad else '#94a3b8'};")

    def _on_glob(self) -> None:
        self._sel = None
        self._paint()

    def _globs(self) -> list[str]:
        if self._glob_input is None:
            return []
        return [g.strip() for g in (self._glob_input.value or "").split(",") if g.strip()]

    def _paint(self) -> None:
        folder, typed = self._split_typed()
        self._paint_for(typed if folder == self._dir else "")

    def _paint_for(self, typed: str) -> None:
        globs = self._globs()
        dirs = [n for n, d in self._entries if d and _matches(n, typed)]
        all_files = [n for n, d in self._entries if not d]
        globbed = [n for n in all_files if not globs or any(fnmatch.fnmatch(n, g) for g in globs)]
        files = [n for n in globbed if _matches(n, typed)]
        if typed and not any(c in typed for c in _WILDCARDS):
            # Names that START with what was typed come first: that is the completion.
            low = typed.lower()
            dirs.sort(key=lambda n: not n.lower().startswith(low))
            files.sort(key=lambda n: not n.lower().startswith(low))

        listed = [(n, True) for n in dirs] + [(n, False) for n in files]
        shown = listed[:_MAX_ROWS]
        self._rows = []
        parts: list[str] = []
        for name, is_dir in shown:
            label = html.escape(name)
            if is_dir or self.mode != "directory":
                i = len(self._rows)
                self._rows.append((os.path.join(self._dir, name), is_dir))
                sel = " cb-fp-sel" if i == self._sel else ""
                parts.append(
                    f'<div class="cb-fp-row{sel}" data-i="{i}" style="{_ROW_STYLE} color: #334155; cursor: pointer;">'
                    f"{_FOLDER_SVG if is_dir else _FILE_SVG}<span>{label}</span></div>"
                )
            else:
                # A folder picker shows the files too, greyed: they say whether this is the folder.
                parts.append(f'<div style="{_ROW_STYLE} color: #cbd5e1;">{_FILE_SVG}<span>{label}</span></div>')
        if not listed:
            empty = f"nothing matches “{html.escape(typed)}”" if typed else "empty folder"
            if not typed and globbed != all_files:
                empty = f"no files match {html.escape(', '.join(globs))}"
            parts.append(f'<div style="{_NOTE_STYLE}">{empty}</div>')
        elif len(listed) > len(shown):
            parts.append(f'<div style="{_NOTE_STYLE}">+{len(listed) - len(shown)} more · type to narrow</div>')
        self._list.content = "".join(parts)

        status = f"{len(dirs)} folders · {len(files)} files"
        if len(globbed) != len(all_files):
            status += f" ({len(all_files) - len(globbed)} hidden by filter)"
        self._note(status)

    def _on_click(self, e) -> None:
        i = e.args.get("i") if isinstance(e.args, dict) else None
        if isinstance(i, int) and 0 <= i < len(self._rows):
            self._sel = i

    async def _on_open(self, e) -> None:
        i = e.args.get("i") if isinstance(e.args, dict) else None
        if isinstance(i, int) and 0 <= i < len(self._rows):
            await self._open_row(i)

    async def _open_row(self, i: int) -> None:
        path, is_dir = self._rows[i]
        if is_dir:
            await self._enter(path)
        else:
            self._pick(path, False)

    # ── Select ────────────────────────────────────────────────────────────────

    def _pick(self, path: str, is_dir: bool) -> None:
        global _last_dir
        _last_dir = path if is_dir else os.path.dirname(path)
        self.submit([path])

    async def _on_ok(self) -> None:
        if self._sel is not None:
            path, is_dir = self._rows[self._sel]
            if is_dir and self.mode == "file":
                await self._enter(path)
            else:
                self._pick(path, is_dir)
            return
        typed = (self.path_input.value or "").strip()
        p = self._abs(typed) if typed else self._dir
        kind = await asyncio.to_thread(_kind, p)
        if kind == "file" and self.mode != "directory":
            self._pick(p, False)
        elif kind == "dir" and self.mode == "file":
            await self._enter(p)
        elif kind == "dir":
            self._pick(p, True)
        elif self.mode != "file" and self._dir:
            # Nothing chosen: "the folder I am standing in" — for "any" that is the common case
            # (open the recon folder, Select, take everything in it).
            self._pick(self._dir, True)
        else:
            self._note("pick a file", bad=True)
