
# prompt_builder.py
import os
import sys
import socket
import argparse
from pathlib import Path
from nicegui import ui
from fastapi import FastAPI
import uvicorn

PROJECT_DIR = None
selected_files = {}  # filepath -> True (just tracking selection)
prompt_display = None

def get_directory_structure(path, parent_path=""):
    """Get files and directories - only immediate children"""
    items = {'dirs': [], 'files': []}
    
    try:
        for item in sorted(os.listdir(path)):
            if item.startswith('.'):
                continue
            
            item_path = os.path.join(path, item)
            rel_path = os.path.join(parent_path, item) if parent_path else item
            
            if os.path.isdir(item_path):
                items['dirs'].append({'name': item, 'path': rel_path, 'full_path': item_path})
            else:
                items['files'].append({'name': item, 'path': rel_path, 'full_path': item_path})
    except PermissionError:
        pass
    
    return items

def read_file(filepath):
    """Read file content"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {e}"

def update_prompt():
    """Update the prompt preview"""
    prompt = ""
    for filepath in selected_files.keys():
        # Re-read file content every time to get latest changes
        content = read_file(os.path.join(PROJECT_DIR, filepath))
        ext = filepath.split('.')[-1] if '.' in filepath else ''
        prompt += f"{filepath}\n```{ext}\n{content}\n```\n\n"
    
    prompt_display.set_content(prompt)

def reset_all():
    """Reset all selections"""
    selected_files.clear()
    prompt_display.set_content('')
    ui.notify('Reset all selections', type='info')

def render_directory_contents(container, path, parent_path=""):
    """Render the contents of a directory"""
    items = get_directory_structure(path, parent_path)
    
    # Render directories first
    for dir_item in items['dirs']:
        with container:
            expansion = ui.expansion(dir_item['name']).classes('w-full').props('dense header-class="text-weight-bold"').style('margin: 0; padding: 0; font-size: 13px')
            child_container = ui.column().style('margin-left: 12px; padding: 0; gap: 0; margin-top: 0')
            
            # Lazy load: render children when expanded, clear when collapsed
            def toggle_handler(e, path=dir_item['full_path'], ppath=dir_item['path'], cont=child_container):
                if e.args:  # Expanded
                    render_directory_contents(cont, path, ppath)
                else:  # Collapsed
                    cont.clear()
            
            expansion.on('update:model-value', toggle_handler)
            
            with expansion:
                child_container
    
    # Then files
    for file_item in items['files']:
        with container:
            def make_handler(filepath, full_path):
                def handler(e):
                    if e.value:  # Checked
                        selected_files[filepath] = True  # Just track selection, not content
                    else:  # Unchecked
                        selected_files.pop(filepath, None)
                    
                    update_prompt()
                return handler
            
            checkbox = ui.checkbox(file_item['name']).classes('w-full').props('dense size=xs').style('margin: 0; padding: 0; font-size: 13px')
            checkbox.on_value_change(make_handler(file_item['path'], file_item['full_path']))

async def copy_to_clipboard():
    """Copy prompt to clipboard"""
    prompt = ""
    for filepath in selected_files.keys():
        # Re-read file content to get latest changes
        content = read_file(os.path.join(PROJECT_DIR, filepath))
        ext = filepath.split('.')[-1] if '.' in filepath else ''
        prompt += f"{filepath}\n```{ext}\n{content}\n```\n\n"
    
    ui.run_javascript(f'''
        navigator.clipboard.writeText({repr(prompt)});
    ''')
    ui.notify('Copied to clipboard!', type='positive')

def create_ui():
    """Create the UI routes"""
    
    @ui.page('/')
    def main_page():
        global prompt_display

        ui.add_head_html('''
            <style>
                .q-expansion-item__container { 
                    padding: 0 !important; 
                    margin: 0 !important;
                }
                .q-expansion-item__content {
                    padding: 0 !important;
                    margin: 0 !important;
                    min-height: 0 !important;
                    display: none !important;
                }
                .q-expansion-item__content > * {
                    display: block !important;
                }
                .q-item { 
                    min-height: 18px !important; 
                    padding: 2px 8px !important; 
                    margin: 0 !important;
                }
                .q-checkbox { 
                    margin: 0 !important; 
                    padding: 0 !important; 
                }
                .q-expansion-item {
                    margin: 0 !important;
                    padding: 0 !important;
                }
                /* Hide scrollbar but keep functionality */
                ::-webkit-scrollbar {
                    display: none;
                }
                * {
                    -ms-overflow-style: none;
                    scrollbar-width: none;
                }
            </style>
        ''')
        
        with ui.row().classes('w-full gap-0').style('height: 100vh; margin: 0; padding: 0'):
            # Left sidebar - file browser
            with ui.column().classes('border-r overflow-y-auto bg-gray-50').style('width: 500px; height: 100vh; overflow-x: hidden; padding: 12px; gap: 0'):
                tree_container = ui.column().classes('w-full').style('padding: 0; margin: 0; gap: 0')
                # Only load root level initially
                render_directory_contents(tree_container, PROJECT_DIR)
            
            # Right side - prompt preview
            with ui.column().classes('flex-1 p-4').style('height: 100vh; overflow: hidden'):
                with ui.row().classes('w-full items-center mb-2').style('gap: 8px'):
                    ui.button('Copy', on_click=copy_to_clipboard, icon='content_copy').props('flat dense')
                    ui.button('Reset', on_click=reset_all, icon='refresh').props('flat dense')
                    ui.space()
                
                # Markdown viewer with code highlighting
                with ui.scroll_area().classes('w-full').style('height: calc(100vh - 80px)'):
                    prompt_display = ui.markdown().classes('w-full')

def setup_app():
    """Configures and returns the FastAPI app."""
    app = FastAPI()
    create_ui()
    ui.run_with(app, title="Prompt Builder")
    return app

def get_local_ip():
    """Get the local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"

if __name__ in {"__main__", "__mp_main__"}:
    parser = argparse.ArgumentParser(description='Prompt Builder')
    parser.add_argument('project_dir', type=str, help='Project directory to browse')
    parser.add_argument('--port', type=int, default=8082, help='Port to run server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()
    
    PROJECT_DIR = os.path.abspath(args.project_dir)
    
    if not os.path.isdir(PROJECT_DIR):
        print(f"Error: {PROJECT_DIR} is not a valid directory")
        sys.exit(1)
    
    app = setup_app()
    
    local_ip = get_local_ip()
    hostname = socket.gethostname()
    
    print("Prompt Builder Starting")
    print(f"Project: {PROJECT_DIR}")
    print(f"Access URLs:")
    print(f"  Local:    http://localhost:{args.port}")
    print(f"  Network:  http://{local_ip}:{args.port}")
    print("\nTo access from another machine, use an SSH tunnel:")
    print(f"  ssh -L {args.port}:{hostname}:{args.port} [YOUR_USERNAME]@{hostname}")
    print("-" * 30)
    
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
    )
