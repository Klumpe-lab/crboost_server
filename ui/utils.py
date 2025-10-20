import asyncio
from pathlib import Path
from nicegui import ui
from local_file_picker import local_file_picker


def create_path_input_with_picker(label: str, mode: str, glob_pattern: str = '', default_value: str = '') -> ui.input:
    """A factory for creating a text input with a file/folder picker button."""

    async def _pick_path():
        start_dir = Path(path_input.value).parent if path_input.value and Path(path_input.value).exists() else '~'
        result = await local_file_picker(
            start_dir,
            mode=mode,
            glob_pattern_annotation=glob_pattern or None
        )
        if result:
            selected_path = Path(result[0])
            if mode == 'directory' and glob_pattern:
                path_input.set_value(str(selected_path / glob_pattern))
            else:
                path_input.set_value(str(selected_path))

    with ui.row().classes('w-full items-center no-wrap'):
        hint = f"Provide a path to a {mode}"
        if glob_pattern:
            hint = f"Provide a glob pattern, e.g., /path/to/files/{glob_pattern}"

        path_input = ui.input(label=label, value=default_value) \
            .classes('flex-grow') \
            .props(f'dense outlined hint="{hint}"')

        ui.button(icon='folder', on_click=_pick_path).props('flat dense')

    return path_input