# ui/state_inspector_tab.py
"""
State Inspector Tab - Live view of application state.
Shows the hierarchical structure and current values of all state.
"""

import json
from nicegui import ui
from typing import Dict, Any
from datetime import datetime


def _format_state_for_display(state_obj) -> Dict[str, Any]:
    """
    Convert the PipelineState object into a display-friendly dict.
    Handles datetime serialization and nested Pydantic models.
    """
    try:
        state_dict = state_obj.dict()
        if 'created_at' in state_dict:
            state_dict['created_at'] = state_dict['created_at'].isoformat() if isinstance(state_dict['created_at'], datetime) else str(state_dict['created_at'])
        if 'modified_at' in state_dict:
            state_dict['modified_at'] = state_dict['modified_at'].isoformat() if isinstance(state_dict['modified_at'], datetime) else str(state_dict['modified_at'])
        
        return state_dict
    except Exception as e:
        return {"error": f"Failed to serialize state: {e}"}


def _create_tree_from_dict(data: Dict[str, Any], parent_key: str = "root") -> str:
    """
    Create a tree-like string representation of nested dicts.
    Returns formatted text suitable for display in a log or code block.
    """
    lines = []
    
    def recurse(d, indent=0):
        if isinstance(d, dict):
            for key, value in d.items():
                if isinstance(value, dict):
                    lines.append("  " * indent + f"├─ {key}:")
                    recurse(value, indent + 1)
                elif isinstance(value, list):
                    lines.append("  " * indent + f"├─ {key}: [{len(value)} items]")
                    if value and indent < 3:
                        for i, item in enumerate(value[:3]):
                            if isinstance(item, dict):
                                lines.append("  " * (indent + 1) + f"  [{i}]:")
                                recurse(item, indent + 2)
                            else:
                                lines.append("  " * (indent + 1) + f"  [{i}]: {item}")
                        if len(value) > 3:
                            lines.append("  " * (indent + 1) + f"  ... +{len(value) - 3} more")
                else:
                    if isinstance(value, str) and len(value) > 60:
                        value = value[:57] + "..."
                    lines.append("  " * indent + f"├─ {key}: {value}")
        else:
            lines.append("  " * indent + f"└─ {d}")
    
    recurse(data)
    return "\n".join(lines)


def build_state_inspector_tab():
    """
    Build the State Inspector tab UI.
    Shows live application state in both JSON and tree formats.
    Returns an async function to load initial data.
    """
    from app_state import state as app_state
    
    # State for the tab
    tab_state = {
        'auto_refresh': False,
        'refresh_interval': 2.0,
    }
    
    def get_state_json() -> str:
        """Get current state as pretty JSON"""
        try:
            state_dict = _format_state_for_display(app_state)
            return json.dumps(state_dict, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)}, indent=2)
    
    def get_state_tree() -> str:
        """Get current state as tree view"""
        try:
            state_dict = _format_state_for_display(app_state)
            return _create_tree_from_dict(state_dict)
        except Exception as e:
            return f"Error creating tree view: {e}"
    
    def get_state_summary() -> Dict[str, Any]:
        """Get high-level state summary"""
        try:
            return {
                "modified_at": app_state.modified_at.isoformat() if hasattr(app_state.modified_at, 'isoformat') else str(app_state.modified_at),
                "jobs_loaded": len(app_state.jobs),
                "job_names"  : list(app_state.jobs.keys()),
                "microscope" : app_state.microscope.microscope_type.value if hasattr(app_state.microscope.microscope_type, 'value') else str(app_state.microscope.microscope_type),
                "partition"  : app_state.computing.partition.value if hasattr(app_state.computing.partition, 'value') else str(app_state.computing.partition),
            }
        except Exception as e:
            return {"error": str(e)}
    
    def refresh_displays():
        """Update all display elements with current state"""
        try:
            # Update summary
            summary = get_state_summary()
            summary_text = "\n".join([f"{k}: {v}" for k, v in summary.items()])
            summary_display.set_text(summary_text)
            
            # Update tree view
            tree_view.clear()
            tree_view.push(get_state_tree())
            
            # Update JSON view
            json_display.set_content(get_state_json())
            
            # Update timestamp
            last_refresh_label.set_text(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            ui.notify(f"Error refreshing state: {e}", type='negative')
    
    async def auto_refresh_loop():
        """Background task for auto-refresh"""
        import asyncio
        while True:
            await asyncio.sleep(tab_state['refresh_interval'])
            if tab_state['auto_refresh']:
                refresh_displays()
    
    def toggle_auto_refresh(e):
        """Toggle auto-refresh on/off"""
        tab_state['auto_refresh'] = e.value
        if e.value:
            ui.notify('Auto-refresh enabled', type='info')
            import asyncio
            asyncio.create_task(auto_refresh_loop())
        else:
            ui.notify('Auto-refresh disabled', type='info')
    
    def copy_state_to_clipboard():
        """Copy current state JSON to clipboard"""
        try:
            state_json = get_state_json()
            ui.run_javascript(f'''
                navigator.clipboard.writeText({json.dumps(state_json)}).then(() => {{
                    console.log("State copied to clipboard");
                }});
            ''')
            ui.notify('State copied to clipboard', type='positive')
        except Exception as e:
            ui.notify(f'Failed to copy: {e}', type='negative')
    
    # Build the UI
    with ui.column().classes('w-full gap-2'):
        
        # Header with controls
        with ui.card().classes('w-full p-2 mb-2'):
            with ui.row().classes('w-full items-center gap-2'):
                ui.label('STATE INSPECTOR').classes('text-xs font-semibold text-gray-700')
                ui.button('Refresh', on_click=lambda: refresh_displays(), icon='refresh').props('dense size=sm flat')
                ui.checkbox('Auto-refresh', on_change=toggle_auto_refresh).props('dense').classes('text-xs')
                ui.button('Copy JSON', on_click=copy_state_to_clipboard, icon='content_copy').props('dense size=sm flat')
                ui.space()
                last_refresh_label = ui.label('Last refresh: Never').classes('text-xs text-gray-500')
        
        # Instructions banner
        with ui.row().classes('w-full p-2 bg-blue-50 rounded mb-2 items-center gap-2'):
            ui.icon('info', size='sm').classes('text-blue-600')
            ui.label(
                'Live view of application state. Changes made in Projects tab will be reflected here. '
                'Use auto-refresh to monitor in real-time.'
            ).classes('text-xs text-blue-800')
        
        # Summary card
        with ui.card().classes('w-full p-2 mb-2'):
            ui.label('Summary').classes('text-xs font-semibold mb-1 text-gray-700')
            summary_display = ui.label('Loading...').classes('text-xs font-mono whitespace-pre text-gray-600')
        
        # Tabbed views for different formats
        with ui.tabs().classes('w-full text-xs') as view_tabs:
            tree_tab = ui.tab('Tree View')
            json_tab = ui.tab('JSON')
            microscope_tab = ui.tab('Microscope')
            acquisition_tab = ui.tab('Acquisition')
            computing_tab = ui.tab('Computing')
            jobs_tab = ui.tab('Jobs')
        
        with ui.tab_panels(view_tabs, value=tree_tab).classes('w-full'):
            
            # Tree view panel
            with ui.tab_panel(tree_tab):
                ui.label('Hierarchical tree view of all state').classes('text-xs text-gray-600 mb-2')
                tree_view = ui.log(max_lines=1000).classes('w-full h-96 border rounded bg-gray-50 p-2 text-xs font-mono')
            
            # JSON panel
            with ui.tab_panel(json_tab):
                ui.label('Raw JSON representation').classes('text-xs text-gray-600 mb-2')
                json_display = ui.code(language='json').classes('w-full h-96 overflow-auto text-xs')
            
            # Microscope panel
            with ui.tab_panel(microscope_tab):
                ui.label('Microscope Parameters').classes('text-xs font-semibold mb-2 text-gray-700')
                with ui.card().classes('w-full p-3'):
                    with ui.column().classes('gap-2'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('science', size='sm').classes('text-purple-600')
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.microscope,
                                'microscope_type',
                                backward=lambda v: f"Type: {v.value if hasattr(v, 'value') else v}"
                            )
                        
                        ui.separator()
                        
                        with ui.grid(columns=2).classes('gap-2 w-full'):
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.microscope,
                                'pixel_size_angstrom',
                                backward=lambda v: f"Pixel Size: {v} Å"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.microscope,
                                'acceleration_voltage_kv',
                                backward=lambda v: f"Voltage: {v} kV"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.microscope,
                                'spherical_aberration_mm',
                                backward=lambda v: f"Cs: {v} mm"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.microscope,
                                'amplitude_contrast',
                                backward=lambda v: f"Amplitude: {v}"
                            )
            
            # Acquisition panel
            with ui.tab_panel(acquisition_tab):
                ui.label('Acquisition Parameters').classes('text-xs font-semibold mb-2 text-gray-700')
                with ui.card().classes('w-full p-3'):
                    with ui.column().classes('gap-2'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('camera_alt', size='sm').classes('text-blue-600')
                            ui.label('Data Acquisition Settings').classes('text-xs font-medium text-gray-600')
                        
                        ui.separator()
                        
                        with ui.grid(columns=2).classes('gap-2 w-full'):
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'dose_per_tilt',
                                backward=lambda v: f"Dose/Tilt: {v} e⁻/Å²"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'detector_dimensions',
                                backward=lambda v: f"Detector: {v[0]}x{v[1]}"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'tilt_axis_degrees',
                                backward=lambda v: f"Tilt Axis: {v}°"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'eer_fractions_per_frame',
                                backward=lambda v: f"EER Fractions: {v if v else 'N/A'}"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'sample_thickness_nm',
                                backward=lambda v: f"Thickness: {v} nm"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.acquisition,
                                'gain_reference_path',
                                backward=lambda v: f"Gain Ref: {v if v else 'None'}"
                            )
            
            # Computing panel
            with ui.tab_panel(computing_tab):
                ui.label('Computing Resources').classes('text-xs font-semibold mb-2 text-gray-700')
                with ui.card().classes('w-full p-3'):
                    with ui.column().classes('gap-2'):
                        with ui.row().classes('items-center gap-2'):
                            ui.icon('computer', size='sm').classes('text-green-600')
                            ui.label('Cluster Configuration').classes('text-xs font-medium text-gray-600')
                        
                        ui.separator()
                        
                        with ui.grid(columns=2).classes('gap-2 w-full'):
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.computing,
                                'partition',
                                backward=lambda v: f"Partition: {v.value if hasattr(v, 'value') else v}"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.computing,
                                'gpu_count',
                                backward=lambda v: f"GPUs: {v}"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.computing,
                                'memory_gb',
                                backward=lambda v: f"Memory: {v} GB"
                            )
                            ui.label().classes('text-xs font-mono').bind_text_from(
                                app_state.computing,
                                'threads',
                                backward=lambda v: f"Threads: {v}"
                            )
            
            # Jobs panel
            with ui.tab_panel(jobs_tab):
                ui.label('Loaded Jobs').classes('text-xs font-semibold mb-2 text-gray-700')
                
                jobs_container = ui.column().classes('gap-2 w-full')
                
                def refresh_jobs_display():
                    """Refresh the jobs display"""
                    jobs_container.clear()
                    
                    if not app_state.jobs:
                        with jobs_container:
                            ui.label('No jobs loaded yet').classes('text-xs text-gray-500 italic')
                        return
                    
                    with jobs_container:
                        for job_name, job_model in app_state.jobs.items():
                            with ui.expansion(job_name, icon='work').classes('w-full').props('dense'):
                                job_dict = job_model.dict()
                                
                                with ui.column().classes('gap-1 p-2'):
                                    for param_name, value in job_dict.items():
                                        # Format the value nicely
                                        if isinstance(value, (dict, list)):
                                            value_str = json.dumps(value, indent=2)
                                            with ui.card().classes('p-2 bg-gray-50'):
                                                ui.label(param_name).classes('text-xs font-semibold mb-1')
                                                ui.code(value_str, language='json').classes('text-xs')
                                        else:
                                            with ui.row().classes('items-center gap-2'):
                                                ui.label(f"{param_name}:").classes('text-xs font-medium text-gray-600')
                                                ui.label(str(value)).classes('text-xs font-mono text-gray-800')
                
                refresh_jobs_display()
                
                ui.button('Refresh Jobs', on_click=lambda: refresh_jobs_display(), icon='refresh').props('dense size=sm flat').classes('mt-2')
    
    async def load_state_inspector():
        """Async function to load initial state data"""
        try:
            refresh_displays()
            print("--- [DEBUG] State inspector loaded ---")
        except Exception as e:
            print(f"--- [DEBUG] ERROR loading state inspector: {e} ---")
            ui.notify(f"Error loading state: {e}", type='negative')
    
    return load_state_inspector