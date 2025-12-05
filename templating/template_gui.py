from nicegui import ui, app
import os
import templating.backend   # Importing the logic file created above

# Configuration
PORT = 8085
OUTPUT_DEFAULT = "tmpOut/templates"

def main():
    
    with ui.column().classes('w-full max-w-4xl mx-auto p-4'):
        ui.label('Create Matching Template').classes('text-2xl font-bold mb-4')
        with ui.card().classes('w-full mb-4'):
            ui.label('Configuration').classes('text-lg font-bold')
            with ui.grid(columns=3).classes('w-full gap-4'):
                template_apix = ui.number('Template Pixelsize (Å)', value=1.5, format='%.2f')
                out_folder = ui.input('Output Folder', value=OUTPUT_DEFAULT)
                def update_box_suggestions():
                    pass 

        # --- Column 1: PDB Handling ---
        with ui.row().classes('w-full gap-4'):
            
            with ui.card().classes('w-1/2'):
                ui.label('1. PDB Input').classes('text-lg font-bold text-primary')
                
                pdb_path = ui.input('PDB File Path').classes('w-full')
                
                with ui.row().classes('w-full items-center'):
                    pdb_code = ui.input('PDB Code (e.g. 4v6x)').classes('flex-grow')
                    async def fetch_pdb_click():
                        if not pdb_code.value: return
                        ui.notify(f"Fetching {pdb_code.value}...")
                        success, res = await  ui.run_javascript(f'return "{pdb_code.value}"') # dummy await
                        success, res = templating.backend.fetch_pdb(pdb_code.value, out_folder.value)
                        if success:
                            pdb_path.value = os.path.abspath(res)
                            ui.notify(f"Fetched: {res}", type='positive')
                        else:
                            ui.notify(f"Error: {res}", type='negative')
                    
                    ui.button('Fetch', on_click=fetch_pdb_click).classes('ml-2')
                
                with ui.row().classes('mt-2'):
                    ui.button('Align to Axis', on_click=lambda: ui.notify("Alignment functionality mock")).props('outline')
                    ui.button('View PDB', on_click=lambda: ui.notify("Use external viewer")).props('outline')

                ui.separator().classes('my-4')
                
                ui.label('Simulation Parameters').classes('font-bold')
                sim_apix = ui.number('Sim Pixel Size', value=1.5, format='%.2f').bind_value(template_apix)
                sim_box = ui.number('Sim Box Size (Vox)', value=128)
                sim_res = ui.number('Resolution (Å)', value=20.0)
                sim_bfactor = ui.number('B-Factor', value=0)
                
                async def run_sim_click():
                    if not pdb_path.value: 
                        ui.notify("No PDB selected", type='warning')
                        return
                    
                    name = os.path.basename(pdb_path.value).split('.')[0]
                    sim_out_name = f"{name}_sim_res{sim_res.value}.mrc"
                    sim_out_path = os.path.join(out_folder.value, sim_out_name)
                    
                    ui.notify("Simulating Map...")
                    # Run backend
                    success, msg = templating.backend.simulate_map_from_pdb(
                        pdb_path.value, sim_out_path, 
                        sim_apix.value, int(sim_box.value), 
                        sim_res.value, sim_bfactor.value
                    )
                    
                    if success:
                        ui.notify("Simulation Complete", type='positive')
                        # Auto-fill the Map input
                        map_path.value = os.path.abspath(sim_out_path)
                    else:
                        ui.notify(f"Sim Error: {msg}", type='negative')

                ui.button('Simulate Map from PDB', on_click=run_sim_click).classes('w-full mt-2 bg-primary')

            # --- Column 2: Volume/Map Handling ---
            with ui.card().classes('w-1/2'):
                ui.label('2. Map Processing').classes('text-lg font-bold text-secondary')
                
                map_path = ui.input('Map File Path').classes('w-full')
                
                with ui.row().classes('w-full items-center'):
                    emdb_id = ui.input('EMDB ID (e.g. 1001)').classes('flex-grow')
                    async def fetch_emdb_click():
                        if not emdb_id.value: return
                        ui.notify(f"Fetching EMDB-{emdb_id.value}...")
                        success, res = backend.download_emdb(emdb_id.value, out_folder.value)
                        if success:
                            map_path.value = os.path.abspath(res)
                            ui.notify("Download complete", type='positive')
                        else:
                            ui.notify(res, type='negative')
                    ui.button('Fetch', on_click=fetch_emdb_click).classes('ml-2')

                ui.separator().classes('my-4')
                ui.label('Generate Template (Process Volume)').classes('font-bold')
                
                # Template gen parameters (replicating generateTemplate.py logic)
                with ui.grid(columns=2).classes('w-full gap-2'):
                    tm_box = ui.number('Final Box Size', value=96)
                    tm_res = ui.number('Lowpass Res (Å)', value=30.0)
                
                async def generate_template_click():
                    if not map_path.value:
                        ui.notify("No Input Map", type='warning'); return
                    
                    base_name = os.path.splitext(os.path.basename(map_path.value))[0]
                    out_white = os.path.join(out_folder.value, f"{base_name}_white.mrc")
                    out_black = os.path.join(out_folder.value, f"{base_name}_black.mrc")
                    
                    ui.notify("Processing Volume...")
                    
                    # 1. Generate White (Density positive)
                    success, _ = backend.process_volume_numpy(
                        map_path.value, out_white, 
                        template_apix.value, int(tm_box.value), 
                        invert=False, lowpass_res=tm_res.value
                    )
                    
                    # 2. Generate Black (Density negative/inverted)
                    success_b, _ = backend.process_volume_numpy(
                        map_path.value, out_black, 
                        template_apix.value, int(tm_box.value), 
                        invert=True, lowpass_res=tm_res.value
                    )
                    
                    if success and success_b:
                        ui.notify(f"Generated {os.path.basename(out_black)}", type='positive')
                    else:
                        ui.notify("Error processing volume", type='negative')

                ui.button('Process / Generate Template', on_click=generate_template_click).classes('w-full mt-2 bg-secondary')
                
                ui.separator().classes('my-2')
                
                # Basic Shape Logic
                with ui.expansion('Basic Shapes (Ellipsoids)'):
                    shape_dims = ui.input('Diameter x:y:z (Å)', value='100:100:100')
                    async def shape_click():
                        ui.notify("Generating Shape...")
                        success, path = backend.create_ellipsoid(
                            shape_dims.value, template_apix.value, out_folder.value
                        )
                        if success:
                            map_path.value = os.path.abspath(path)
                            ui.notify("Shape Generated", type='positive')
                        else:
                            ui.notify(path, type='negative')
                    ui.button('Create Shape', on_click=shape_click).classes('w-full')

    # Footer
    with ui.footer().classes('bg-grey-200 text-black p-2'):
        ui.label('CryoBoost Port | Uvicorn: 8085')

# Run uvicorn
if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=PORT, title="CryoBoost Template Gen", show=False)