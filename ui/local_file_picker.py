# ui/local_file_picker.py
from pathlib import Path
from typing import Optional, List
from nicegui import ui


class local_file_picker(ui.dialog):

    def __init__(self, directory: str, *,
                 upper_limit: Optional[str] = ...,
                 mode: str = 'directory', 
                 glob_pattern_annotation: str = None) -> None:
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        self.selected_path: Optional[Path] = None
        
        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = Path(directory if upper_limit == ... else upper_limit).expanduser().resolve()

        with self, ui.card().classes('p-0').style('width: 70vw; max-width: 900px;'):
            
            # Header
            with ui.row().classes('w-full items-center px-4 py-2 bg-gray-100 border-b'):
                self.up_button = ui.button(icon='arrow_upward', on_click=self._go_up).props('flat round dense')
                self.path_label = ui.label(str(self.path)).classes('ml-2 text-sm font-mono text-gray-700')

            # File list container - scrollable
            self.list_container = ui.column().classes('w-full p-0 overflow-y-auto').style('height: 50vh;')
            
            # Footer
            with ui.row().classes('w-full justify-between items-center px-4 py-3 bg-gray-100 border-t'):
                if self.mode == 'directory':
                    ui.label('Double-click folder to enter. OK selects current directory.').classes('text-xs text-gray-500')
                else:
                    ui.label('Click to select, double-click to confirm.').classes('text-xs text-gray-500')
                
                with ui.row().classes('gap-2'):
                    ui.button('Cancel', on_click=self.close).props('flat no-caps')
                    ui.button('OK', on_click=self._handle_ok).props('no-caps').classes('bg-blue-600 text-white')

        self._refresh_list()

    def _refresh_list(self) -> None:
        """Rebuild the file list."""
        self.list_container.clear()
        self.path_label.set_text(str(self.path))
        self._update_up_button()
        
        try:
            items = sorted(
                [p for p in self.path.iterdir() if not p.name.startswith('.')],
                key=lambda p: (not p.is_dir(), p.name.lower())
            )
        except (PermissionError, FileNotFoundError):
            items = []

        with self.list_container:
            if not items:
                ui.label('Empty directory').classes('text-gray-400 text-sm p-4')
                return
            
            for item in items:
                self._create_row(item)

    def _create_row(self, item: Path) -> None:
        """Create a single file/folder row."""
        is_dir = item.is_dir()
        icon = 'folder' if is_dir else 'description'
        icon_color = 'text-yellow-600' if is_dir else 'text-gray-400'
        
        row = ui.row().classes(
            'w-full items-center px-4 py-1 cursor-pointer hover:bg-blue-50 border-b border-gray-100'
        )
        
        # Store path on the row element
        row.path = item
        row.is_dir = is_dir
        
        with row:
            ui.icon(icon, size='20px').classes(icon_color)
            ui.label(item.name).classes('ml-2 text-sm truncate flex-1')
            if not is_dir:
                # Show file size
                try:
                    size = item.stat().st_size
                    size_str = f'{size / 1024:.1f} KB' if size > 1024 else f'{size} B'
                    ui.label(size_str).classes('text-xs text-gray-400')
                except:
                    pass
        
        # Click to select
        row.on('click', lambda e, r=row: self._select_row(r))
        # Double-click to enter/select
        row.on('dblclick', lambda e, r=row: self._double_click_row(r))

    def _select_row(self, row) -> None:
        """Handle single click - select the row."""
        # Deselect all
        for child in self.list_container:
            if hasattr(child, 'path'):
                child.classes(remove='bg-blue-100')
        
        # Select this one
        row.classes('bg-blue-100')
        self.selected_path = row.path

    def _double_click_row(self, row) -> None:
        """Handle double click - enter folder or select file."""
        if row.is_dir:
            self.path = row.path
            self.selected_path = None
            self._refresh_list()
        elif self.mode == 'file':
            self.submit([str(row.path)])

    async def _handle_ok(self):
        if self.selected_path:
            if self.mode == 'directory' and self.selected_path.is_dir():
                self.submit([str(self.selected_path)])
            elif self.mode == 'file' and self.selected_path.is_file():
                self.submit([str(self.selected_path)])
            elif self.mode == 'directory' and self.selected_path.is_file():
                ui.notify('Please select a folder', type='warning')
            else:
                ui.notify('Invalid selection', type='warning')
        else:
            # No selection - in directory mode, select current folder
            if self.mode == 'directory':
                self.submit([str(self.path)])
            else:
                ui.notify('Please select a file', type='warning')

    def _go_up(self) -> None:
        parent = self.path.parent
        if parent != self.path:
            self.path = parent
            self.selected_path = None
            self._refresh_list()

    def _update_up_button(self) -> None:
        at_root = self.path == self.path.parent
        if self.upper_limit is None:
            self.up_button.props(f'disable={at_root}')
        else:
            at_limit = self.path == self.upper_limit or self.upper_limit in self.path.parents
            self.up_button.props(f'disable={at_root or at_limit}')
