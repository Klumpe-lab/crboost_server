# services/pdb_service.py
import os
import numpy as np
import gemmi
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

from services.container_service import get_container_service


class PDBService:
    def __init__(self, backend):
        self.backend = backend
        self.container_service = get_container_service()
        
        # Use configured path or auto-find
        self.cistem_binary = "/groups/klumpe/software/cisTEM/bin/simulate"
        # if configured_path and os.path.exists(configured_path):
        #     self.cistem_binary = os.path.abspath(configured_path)
        # else:
        #     self.cistem_binary = self._find_cistem_binary()

    def _find_cistem_binary(self) -> Optional[str]:
        """Locate the CISTEM simulate binary."""
        # Check common locations
        possible_paths = [
            "cisTEM/bin/simulate",
            "./cisTEM/bin/simulate",
            "../cisTEM/bin/simulate",
            "bin/simulate",
            "./bin/simulate",
        ]

        # Also check PATH
        try:
            result = subprocess.run(["which", "simulate"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass

        # Check relative paths
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return os.path.abspath(path)

        return None

    # =========================================================================
    # STRUCTURE METADATA & ALIGNMENT (unchanged)
    # =========================================================================

    async def get_structure_metadata(self, pdb_path: str) -> Dict[str, Any]:
        """Extracts metadata using standard loops."""
        return await asyncio.to_thread(self._get_meta_sync, pdb_path)

    def _get_meta_sync(self, path: str) -> Dict[str, Any]:
        try:
            st = gemmi.read_structure(path)
            res_count = sum(len(model) for model in st)

            coords = []
            for model in st:
                for chain in model:
                    for residue in chain:
                        for atom in residue:
                            coords.append(atom.pos.tolist())

            coords = np.array(coords)
            if len(coords) == 0:
                return {"error": "No atoms found"}

            mins = np.min(coords, axis=0)
            maxs = np.max(coords, axis=0)
            dims = maxs - mins

            sym = "C1"
            if hasattr(st, "info") and st.info and "spacegroup_name" in st.info:
                sym = st.info["spacegroup_name"]
            elif hasattr(st, "spacegroup_name"):
                sym = st.spacegroup_name

            return {
                "success": True,
                "residues": res_count,
                "bbox": [round(float(x), 2) for x in dims],
                "max_dim": round(float(np.max(dims)), 2),
                "symmetry": sym,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def align_to_principal_axes(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """Aligns structure using SVD."""
        return await asyncio.to_thread(self._align_svd_sync, input_path, output_path)

    def _align_svd_sync(self, input_path: str, output_path: str) -> Dict[str, Any]:
        try:
            st = gemmi.read_structure(input_path)
            all_atoms = []
            for model in st:
                for chain in model:
                    for residue in chain:
                        for atom in residue:
                            all_atoms.append(atom)

            coords = np.array([a.pos.tolist() for a in all_atoms])
            center = np.mean(coords, axis=0)
            centered_coords = coords - center

            _, _, vh = np.linalg.svd(centered_coords)
            aligned_coords = centered_coords @ vh.T

            for i, atom in enumerate(all_atoms):
                atom.pos = gemmi.Position(*aligned_coords[i])

            # FIX: Use correct method for gemmi 0.7.4
            if output_path.endswith(".cif"):
                doc = st.make_mmcif_document()
                doc.write_file(output_path)
            else:
                st.write_pdb(output_path)
            return {"success": True, "path": output_path}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================================
    # CISTEM SIMULATE - MAIN SIMULATION
    # =========================================================================

    async def simulate_map_from_pdb(
        self,
        pdb_path: str,
        output_folder: str,
        target_apix: float,
        target_box: int,
        resolution: float = 10.0,
        sim_apix: Optional[float] = None,
        sim_box: Optional[int] = None,
        mod_scale_bf: float = 1.0,
        mod_bf: float = 0.0,
        oversample: int = 4,
        num_frames: int = 7,
        num_threads: int = 4,
    ) -> Dict[str, Any]:
        """
        Simulates density map from PDB using CISTEM's simulate binary.

        Args:
            pdb_path: Input PDB/CIF file
            output_folder: Where to save results
            target_apix: Final template pixel size (Å/pixel)
            target_box: Final template box size (pixels)
            resolution: Target resolution for lowpass (Å)
            sim_apix: Simulation pixel size (default: min(4.0, target_apix))
            sim_box: Simulation box size (default: auto-calculated)
            mod_scale_bf: Linear scaling of per-atom B-factor (default: 1.0)
            mod_bf: Per-atom B-factor offset (default: 0.0)
            oversample: Oversampling factor (default: 4)
            num_frames: Number of frames for movie (default: 7)
            num_threads: Number of threads (default: 4)
        """
        try:
            # Check if CISTEM is available
            if not self.cistem_binary:
                return {
                    "success": False,
                    "error": "CISTEM simulate binary not found. Please install or configure path.",
                }

            os.makedirs(output_folder, exist_ok=True)
            base_name = Path(pdb_path).stem

            # Auto-calculate simulation parameters
            if sim_apix is None:
                sim_apix = min(4.0, target_apix)

            if sim_box is None:
                meta = await self.get_structure_metadata(pdb_path)
                if meta.get("success"):
                    max_dim = meta["max_dim"]
                    sim_box = self._calculate_optimal_box(max_dim, sim_apix)
                else:
                    sim_box = max(256, target_box * 2)

            # Run CISTEM simulation
            sim_result = await self._run_cistem_simulate(
                pdb_path=pdb_path,
                output_folder=output_folder,
                base_name=base_name,
                sim_apix=sim_apix,
                sim_box=sim_box,
                mod_scale_bf=mod_scale_bf,
                mod_bf=mod_bf,
                oversample=oversample,
                num_frames=num_frames,
                num_threads=num_threads,
            )

            if not sim_result["success"]:
                return sim_result

            sim_map_path = sim_result["sim_path"]

            # Process with Relion to create final template pair
            final_result = await self._process_simulated_map(
                sim_map_path=sim_map_path,
                output_folder=output_folder,
                base_name=base_name,
                sim_apix=sim_apix,
                target_apix=target_apix,
                target_box=target_box,
                resolution=resolution,
            )

            # Cleanup intermediate simulation file
            if os.path.exists(sim_map_path):
                try:
                    os.remove(sim_map_path)
                except:
                    pass

            return final_result

        except Exception as e:
            return {"success": False, "error": f"Simulation error: {str(e)}"}

    async def _run_cistem_simulate(
        self,
        pdb_path: str,
        output_folder: str,
        base_name: str,
        sim_apix: float,
        sim_box: int,
        mod_scale_bf: float,
        mod_bf: float,
        oversample: int,
        num_frames: int,
        num_threads: int,
    ) -> Dict[str, Any]:
        """
        Run CISTEM simulate binary directly via stdin.
        """
        try:
            # Prepare structure file - simulate prefers CIF
            struct_file = os.path.join(output_folder, f"{base_name}_for_sim.cif")

            # Load structure and translate to positive quadrant
            st = gemmi.read_structure(pdb_path)

            # CISTEM requirement: translate to positive quadrant
            offset = (sim_box / 2) * sim_apix
            for model in st:
                for chain in model:
                    for residue in chain:
                        for atom in residue:
                            pos = atom.pos
                            atom.pos = gemmi.Position(pos.x + offset, pos.y + offset, pos.z + offset)

            # FIX: Use correct method for gemmi 0.7.4
            doc = st.make_mmcif_document()
            doc.write_file(struct_file)
            
            struct_basename = os.path.basename(struct_file)


            # Output path
            sim_output_name = f"{base_name}_sim_raw.mrc"
            sim_output_path = os.path.join(output_folder, sim_output_name)

            # Create stdin input for simulate
            # Matches old CryoBoost parameter order
            stdin_input = "\n".join(
                [
                    sim_output_name,  # Output file name
                    "Yes",  # Use scattering potential
                    str(sim_box),  # Box size
                    str(num_threads),  # Number of threads
                    struct_basename,  # Input structure (relative path)
                    "No",  # Add particles?
                    str(sim_apix),  # Output pixel size
                    str(mod_scale_bf),  # Per-atom scale B-factor
                    str(mod_bf),  # Per-atom B-factor offset
                    str(oversample),  # Oversample factor
                    str(num_frames),  # Number of frames
                    "No",  # Expert mode?
                    "",  # Empty line to finish
                ]
            )

            # Run simulate with stdin
            result = await asyncio.to_thread(self._run_simulate_sync, self.cistem_binary, output_folder, stdin_input)

            if not result["success"]:
                # Cleanup on failure
                for f in [struct_file]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                        except:
                            pass
                return result

            # Verify output was created
            if not os.path.exists(sim_output_path):
                return {"success": False, "error": f"CISTEM completed but output not found: {sim_output_name}"}

            # Cleanup structure file
            if os.path.exists(struct_file):
                try:
                    os.remove(struct_file)
                except:
                    pass

            return {"success": True, "sim_path": sim_output_path}

        except Exception as e:
            return {"success": False, "error": f"CISTEM execution error: {str(e)}"}

    def _run_simulate_sync(self, binary_path: str, cwd: str, stdin_input: str) -> Dict[str, Any]:
        """
        Synchronous execution of simulate binary.
        Run in thread pool to avoid blocking.
        """
        try:
            process = subprocess.Popen(
                [binary_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=cwd,
                text=True,
                env=os.environ.copy(),
            )

            stdout, stderr = process.communicate(input=stdin_input, timeout=300)  # 5 min timeout

            if process.returncode != 0:
                return {
                    "success": False,
                    "error": f"CISTEM failed with code {process.returncode}. STDERR: {stderr[:1000]}",
                }

            return {"success": True, "stdout": stdout, "stderr": stderr}

        except subprocess.TimeoutExpired:
            process.kill()
            return {"success": False, "error": "CISTEM simulation timeout (>5 min)"}
        except Exception as e:
            return {"success": False, "error": f"Subprocess error: {str(e)}"}

    async def _process_simulated_map(
        self,
        sim_map_path: str,
        output_folder: str,
        base_name: str,
        sim_apix: float,
        target_apix: float,
        target_box: int,
        resolution: float,
    ) -> Dict[str, Any]:
        """
        Process CISTEM output with Relion.
        Creates white (positive) and black (negative) contrast templates.
        """
        try:
            # Generate output filenames
            name_core = f"{base_name}_apix{target_apix:.2f}_box{target_box}"
            if resolution:
                name_core += f"_lp{int(resolution)}"

            path_white = os.path.join(output_folder, f"{name_core}_white.mrc")
            path_black = os.path.join(output_folder, f"{name_core}_black.mrc")

            # Step 1: Create white template (positive contrast)
            cmd_white = (
                f"relion_image_handler "
                f"--i {sim_map_path} "
                f"--o {path_white} "
                f"--angpix {sim_apix:.4f} "
                f"--rescale_angpix {target_apix:.4f} "
                f"--new_box {target_box} "
            )

            if resolution and resolution > 0:
                cmd_white += f"--lowpass {resolution} --filter_edge_width 6 "

            cmd_white += f"--force_header_angpix {target_apix:.4f}"

            result_white = await self.backend.run_shell_command(
                cmd_white, tool_name="relion", additional_binds=[os.path.dirname(sim_map_path), output_folder]
            )

            if not result_white.get("success"):
                return {"success": False, "error": f"Relion processing (white) failed: {result_white.get('error')}"}

            # Step 2: Create black template (inverted contrast)
            cmd_black = f"relion_image_handler --i {path_white} --o {path_black} --multiply_constant -1"

            result_black = await self.backend.run_shell_command(
                cmd_black, tool_name="relion", additional_binds=[output_folder]
            )

            if not result_black.get("success"):
                return {"success": False, "error": f"Relion processing (black) failed: {result_black.get('error')}"}

            return {
                "success": True,
                "path": path_black,  # Default to black for template matching
                "path_white": path_white,
                "path_black": path_black,
            }

        except Exception as e:
            return {"success": False, "error": f"Template processing error: {str(e)}"}

    def _calculate_optimal_box(
        self, max_dim_angstrom: float, pixel_size: float, min_box: int = 96, alignment: int = 32
    ) -> int:
        """
        Calculate optimal box size for simulation.
        Adds 30% padding and aligns to 32-pixel boundary.
        """
        box_needed = (max_dim_angstrom / pixel_size) * 1.3
        box_size = int(((box_needed + alignment - 1) // alignment) * alignment)
        return max(box_size, min_box)
