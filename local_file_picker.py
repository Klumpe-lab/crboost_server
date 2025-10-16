import platform
from pathlib import Path
from typing import Optional

from nicegui import events, ui

class local_file_picker(ui.dialog):

    def __init__(self, directory: str, *,
                 upper_limit: Optional[str] = ...,
                 mode: str = 'directory', 
                 glob_pattern_annotation: str = None) -> None:
        """Enhanced Local File Picker

        :param directory: The directory to start in.
        :param upper_limit: The directory to stop at.
        :param mode: 'directory' to select a directory, 'file' to select a file.
        :param glob_pattern_annotation: A string to display as a helpful hint (e.g., '*.eer').
        """
        super().__init__()

        self.path = Path(directory).expanduser().resolve()
        self.mode = mode
        if upper_limit is None:
            self.upper_limit = None
        else:
            self.upper_limit = Path(directory if upper_limit == ... else upper_limit).expanduser().resolve()

        with self, ui.card().classes('w-[60rem] max-w-full'):
            ui.add_head_html('<style>.ag-selection-checkbox { display: none; }</style>')

            with ui.row().classes('w-full items-center px-4 pb-2'):
                self.up_button = ui.button(icon='arrow_upward', on_click=self._go_up).props('flat round dense')
                self.path_label = ui.label(str(self.path)).classes('ml-2 text-mono')

            self.grid = ui.aggrid({
                'columnDefs': [{'field': 'name', 'headerName': 'File'}],
                'rowSelection': 'single',
            }, html_columns=[0]).classes('w-full').on('cellDoubleClicked', self.handle_double_click)

            with ui.row().classes('w-full justify-end items-center px-4 pt-2'):
                with ui.row().classes('mr-auto items-center'):
                    # NEW: Instructions change based on the mode
                    if self.mode == 'directory':
                        ui.label('Select a folder and click Ok, or navigate into a folder and click Ok.').classes('text-xs text-gray-500')
                    else:
                        ui.label('Select a file and click Ok, or double-click a file.').classes('text-xs text-gray-500')
                    
                    if glob_pattern_annotation:
                        ui.label(f'Expected: {glob_pattern_annotation}').classes('text-xs text-gray-500 ml-4 p-1 bg-gray-100 rounded')

                ui.button('Cancel', on_click=self.close).props('outline')
                ui.button('Ok', on_click=self._handle_ok)

        self.update_grid()

    def update_grid(self) -> None:
        try:
            paths = sorted(
                [p for p in self.path.glob('*') if p.name != '.DS_Store'],
                key=lambda p: (not p.is_dir(), p.name.lower())
            )
        except FileNotFoundError:
            self.path = Path('/').expanduser().resolve()
            paths = []

        self.grid.options['rowData'] = [
            {
                'name': f'📁 <strong>{p.name}</strong>' if p.is_dir() else f'📄 {p.name}',
                'path': str(p),
                'is_dir': p.is_dir(),
            }
            for p in paths
        ]
        
        self.path_label.set_text(str(self.path))
        self._update_up_button()
        self.grid.update()

    def handle_double_click(self, e: events.GenericEventArguments) -> None:
        data = e.args['data']
        path = Path(data['path'])
        
        if data['is_dir']:
            self.path = path
            self.update_grid()
        elif self.mode == 'file':  # Only submit on double-click if in file mode
            self.submit([str(path)])
        else: # In directory mode, notify that you can't double-click a file
            ui.notify('Please select a directory and click "Ok".', type='info')

    async def _handle_ok(self):
        rows = await self.grid.get_selected_rows()
        # NEW: If nothing is selected, select the current directory (in directory mode)
        if not rows:
            if self.mode == 'directory':
                self.submit([str(self.path)])
            else:
                ui.notify('Please select a file.', type='warning')
            return

        # NEW: Validate selection based on mode
        selected_path = Path(rows[0]['path'])
        is_dir = rows[0]['is_dir']

        if self.mode == 'directory':
            if is_dir:
                self.submit([str(selected_path)])
            else:
                ui.notify('You selected a file. Please select a directory.', type='negative')
        elif self.mode == 'file':
            if not is_dir:
                self.submit([str(selected_path)])
            else:
                ui.notify('You selected a directory. Please select a file.', type='negative')

    def _go_up(self) -> None:
        self.path = self.path.parent
        self.update_grid()

    def _update_up_button(self) -> None:
        if self.upper_limit is None:
            self.up_button.props(f'disable={self.path == self.path.parent}')
        else:
            self.up_button.props(f'disable={self.path == self.upper_limit or self.upper_limit in self.path.parents}')
