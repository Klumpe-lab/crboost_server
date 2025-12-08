import os
import asyncio
import numpy as np
import tempfile
import subprocess
from typing import Dict, Any, Optional

# Try importing pymol, handle if missing
try:
    import pymol2
    PYMOL_AVAILABLE = True
except ImportError:
    PYMOL_AVAILABLE = False
    print("Warning: pymol2 module not found. PDB features will fail.")

from Bio.PDB import MMCIFParser, MMCIFIO
from services.config_service import get_config_service

class PDBService:
    def __init__(self, backend):
        self.backend = backend
        self.config = get_config_service()
        self.pymol = None
        if PYMOL_AVAILABLE:
            self.pymol = pymol2.PyMOL()
            self.pymol.start()

    def _ensure_pymol(self):
        if not self.pymol:
            raise RuntimeError("PyMOL2 is not available in the server environment.")

    def fetch_pdb(self, pdb_code: str, output_folder: str) -> Dict[str, Any]:
        """Fetches PDB/CIF from remote."""
        self._ensure_pymol()
        try:
            os.makedirs(output_folder, exist_ok=True)
            self.pymol.cmd.set('fetch_path', output_folder)
            
            # PyMOL fetch returns the object name on success
            res = self.pymol.cmd.fetch(pdb_code)
            
            if res != -1:
                # Determine where it downloaded (PyMOL behavior varies, usually adds extension)
                # We force save it to ensure we know the path
                cif_path = os.path.join(output_folder, f"{pdb_code}.cif")
                self.pymol.cmd.save(cif_path, pdb_code, format="cif")
                
                # Cleanup internal memory
                self.pymol.cmd.delete(pdb_code)
                return {"success": True, "path": cif_path}
            else:
                return {"success": False, "error": f"PyMOL failed to fetch {pdb_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def simulate_map_from_pdb(
        self, 
        pdb_path: str, 
        pixel_size: float, 
        box_size: int, 
        output_folder: str
    ) -> Dict[str, Any]:
        """
        Ports simulateMapFromPDB from old pdb.py
        """
        self._ensure_pymol()
        try:
            if not os.path.exists(pdb_path):
                return {"success": False, "error": f"PDB file not found: {pdb_path}"}
            
            # 1. Prepare Paths
            pdb_name = os.path.basename(pdb_path)
            name_no_ext = os.path.splitext(pdb_name)[0]
            
            # Standard naming convention
            final_name_base = f"{name_no_ext}_apix{pixel_size}_box{box_size}"
            output_mrc_path = os.path.join(output_folder, f"{final_name_base}_white.mrc")
            os.makedirs(output_folder, exist_ok=True)

            # 2. Center PDB in Box (using PyMOL logic from old code)
            self.pymol.cmd.load(pdb_path, "current_model")
            
            # Offset calculation from old code: off=(outBox/2)*outPix
            offset = (box_size / 2.0) * pixel_size
            
            # Translate to center of box (assuming box origin is 0,0,0)
            # The old code translates by [off, off, off]
            self.pymol.cmd.translate([offset, offset, offset], "current_model")
            
            # Save temporary centered PDB for simulation
            temp_pdb_name = f"temp_sim_{name_no_ext}.cif"
            temp_pdb_path = os.path.join(output_folder, temp_pdb_name)
            self.pymol.cmd.save(temp_pdb_path, "current_model", format="cif")
            self.pymol.cmd.delete("current_model")

            # 3. Create Parameter File for 'simulate' executable
            # Based on old pdb.py dictionary construction
            param_file_path = os.path.join(output_folder, f"{final_name_base}.inp")
            
            params = {
                'outFile': os.path.basename(output_mrc_path),
                'scPotential': 'Yes',
                'boxSize': int(box_size),
                'threads': 8, # Reduced from 25 to be safe on shared nodes
                'inputPDBPath': temp_pdb_name,
                'addPart': 'No',
                'outputPix': pixel_size,
                'perAtomScaleBfact': 1,
                'perAtomBfact': 0,
                'oversample': 2,
                'numOfFrames': 7,
                'expert': 'No',
                'EOF': 'EOF',
                'exit': 'exit 0'
            }

            with open(param_file_path, 'w') as f:
                for val in params.values():
                    f.write(f"{val}\n")

            # 4. Run Simulation Binary
            # We assume 'simulate' is in the PATH or environment provided by config
            # Old code: call=envL+";cd " + outFold + ";simulate < " + os.path.basename(paramFileName)
            
            # We construct a command that changes dir first to ensure relative paths in .inp work
            cmd = f"cd {output_folder} && simulate < {os.path.basename(param_file_path)}"
            
            # We run this synchronously in the thread (via asyncio.to_thread in service caller)
            # or use subprocess directly here.
            print(f"[PDBService] Running: {cmd}")
            
            # Attempt to run. Note: 'simulate' must be in the system PATH of the server user!
            process = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True,
                executable="/bin/bash" 
            )

            if process.returncode != 0:
                return {"success": False, "error": f"Simulate failed: {process.stderr}"}

            if not os.path.exists(output_mrc_path):
                 return {"success": False, "error": "Simulation finished but output file missing."}

            return {"success": True, "path": output_mrc_path}

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}