import os
import json
import asyncio
import subprocess
from pathlib import Path
import textwrap
from typing import Dict, Any, Optional
from Bio.PDB import MMCIFParser, MMCIFIO

from services.container_service import get_container_service


class PDBService:
    def __init__(self, backend):
        self.backend = backend
        self.container_service = get_container_service()

    # =========================================================================
    # BOX SIZE CALCULATION - Restore original logic
    # =========================================================================

    def _calculate_optimal_box(
        self, max_dim_angstrom: float, pixel_size: float, min_box: int = 128, alignment: int = 32
    ) -> int:
        """
        Calculate optimal box size matching original libpdb.py logic.
        Aligns to 'alignment' boundary (default 32).
        """
        size_in_pixels = max_dim_angstrom / pixel_size
        num_blocks = int((size_in_pixels + alignment - 1) // alignment)
        box_size = num_blocks * alignment

        if box_size < min_box:
            box_size = min_box

        return int(box_size)

    # =========================================================================
    # PYMOL WRAPPER - Fixed for container file access
    # =========================================================================

    async def _run_pymol_script(
        self, script_content: str, output_dir: Path, additional_binds: list = None
    ) -> Dict[str, Any]:
        output_dir = Path(output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        script_file = output_dir / "pymol_script.py"
        script_file.write_text(script_content)

        cmd = f"python3 {script_file.name}"
        binds = [str(output_dir)] + (additional_binds or [])
        binds = list(set(str(Path(b).resolve()) for b in binds if Path(b).exists()))

        result = await self.backend.run_shell_command(cmd, cwd=output_dir, tool_name="pymol", additional_binds=binds)

        # Only delete on SUCCESS
        if result.get("success") and script_file.exists():
            script_file.unlink()
        else:
            print(f"[PDBService] Script kept for debugging: {script_file}")

        return result

    # =========================================================================
    # CIF POST-PROCESSING - Critical for CISTEM compatibility
    # =========================================================================

    def _reparse_cif(self, cif_path: Path) -> bool:
        """
        Re-parse and re-save CIF using BioPython.
        PyMOL's CIF output may have formatting issues.
        """
        try:
            from Bio.PDB import MMCIFParser, MMCIFIO

            parser = MMCIFParser(QUIET=True)
            structure = parser.get_structure("structure_id", str(cif_path))
            io = MMCIFIO()
            io.set_structure(structure)
            io.save(str(cif_path))
            return True
        except Exception as e:
            print(f"[PDBService] CIF re-parsing failed: {e}")
            return False

    # =========================================================================
    # STRUCTURE METADATA
    # =========================================================================

    async def get_structure_metadata(self, pdb_path: str) -> Dict[str, Any]:
        """Extract metadata using PyMOL."""
        pdb_path = str(Path(pdb_path).resolve())

        script = textwrap.dedent(f"""
import pymol2
import json
import sys

try:
    pymol = pymol2.PyMOL()
    pymol.start()
    
    pymol.cmd.load("{pdb_path}", "structure")
    
    min_xyz, max_xyz = pymol.cmd.get_extent("structure")
    dims = [max_xyz[i] - min_xyz[i] for i in range(3)]
    max_dim = max(dims)
    res_count = pymol.cmd.count_atoms("structure and name CA")
    
    result = {{
        "success": True,
        "residues": int(res_count),
        "bbox": [round(float(x), 2) for x in dims],
        "max_dim": round(float(max_dim), 2),
        "symmetry": "C1"
    }}
    
    print("PYMOL_RESULT:" + json.dumps(result))
    pymol.stop()
    
except Exception as e:
    print("PYMOL_ERROR:" + str(e), file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
""").strip()

        output_dir = Path(pdb_path).parent
        result = await self._run_pymol_script(script, output_dir, [str(output_dir)])

        if not result.get("success"):
            return {"success": False, "error": result.get("error", "Unknown error")}

        stdout = result.get("output", "")
        for line in stdout.split("\n"):
            if line.startswith("PYMOL_RESULT:"):
                return json.loads(line.replace("PYMOL_RESULT:", ""))

        return {"success": False, "error": "No result found in PyMOL output"}

    # =========================================================================
    # ALIGNMENT
    # =========================================================================

    async def align_to_principal_axes(self, input_path: str, output_path: str) -> Dict[str, Any]:
        """Align structure using PyMOL."""
        input_path = str(Path(input_path).resolve())
        output_path = str(Path(output_path).resolve())

        script = textwrap.dedent(f"""
import pymol2
import numpy as np
import json
import sys

try:
    pymol = pymol2.PyMOL()
    pymol.start()
    
    pymol.cmd.load("{input_path}", "structure")
    
    coords = pymol.cmd.get_coords("structure")
    center = np.mean(coords, axis=0)
    centered_coords = coords - center
    
    covariance_matrix = np.cov(centered_coords.T)
    eigenvalues, eigenvectors = np.linalg.eigh(covariance_matrix)
    
    idx = eigenvalues.argsort()[::-1]
    eigenvalues = eigenvalues[idx]
    eigenvectors = eigenvectors[:, idx]
    
    aligned_coords = np.dot(centered_coords, eigenvectors)
    pymol.cmd.load_coords(aligned_coords.tolist(), "structure")
    
    fmt = "cif" if "{output_path}".endswith(".cif") else "pdb"
    pymol.cmd.save("{output_path}", "structure", format=fmt)
    
    pymol.stop()
    
    print("PYMOL_RESULT:" + json.dumps({{"success": True, "path": "{output_path}"}}))
    
except Exception as e:
    print("PYMOL_ERROR:" + str(e), file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
""").strip()

        output_dir = Path(output_path).parent
        input_dir = Path(input_path).parent

        result = await self._run_pymol_script(script, output_dir, [str(input_dir), str(output_dir)])

        if not result.get("success"):
            return {"success": False, "error": result.get("error", "Unknown error")}

        # CRITICAL: Re-parse CIF on host
        if output_path.endswith(".cif") and Path(output_path).exists():
            if not self._reparse_cif(Path(output_path)):
                return {"success": False, "error": "CIF post-processing failed"}

        stdout = result.get("output", "")
        for line in stdout.split("\n"):
            if line.startswith("PYMOL_RESULT:"):
                return json.loads(line.replace("PYMOL_RESULT:", ""))

        return {"success": True, "path": output_path}

    # =========================================================================
    # CISTEM SIMULATION - Fixed to match original exactly
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
        oversample: int = 2,
        num_frames: int = 7,
        num_threads: int = 25,
    ) -> Dict[str, Any]:
        """Simulate density map from PDB using CISTEM."""
        try:
            # CHECK: Ensure cistem is configured (binary or container)
            cistem_config = self.container_service.config.get_tool_config("cistem")
            if not cistem_config:
                return {"success": False, "error": "Tool 'cistem' not configured in conf.yaml"}

            pdb_path = str(Path(pdb_path).resolve())
            output_folder = str(Path(output_folder).resolve())
            os.makedirs(output_folder, exist_ok=True)

            base_name = Path(pdb_path).stem

            # Auto-calculate sim parameters
            if sim_apix is None:
                sim_apix = min(4.0, target_apix)

            if sim_box is None:
                meta = await self.get_structure_metadata(pdb_path)
                if not meta.get("success"):
                    return {"success": False, "error": "Could not determine structure dimensions"}
                max_dim = meta["max_dim"]
                sim_box = self._calculate_optimal_box(max_dim, sim_apix)

            print(f"\n{'=' * 70}")
            print(f"[PDBService] Starting PDB → Template Simulation")
            print(f"{'=' * 70}")
            print(f"  Input PDB:     {pdb_path}")
            print(f"  Output folder: {output_folder}")
            print(f"  Simulation:    {sim_apix}Å/px, box {sim_box}")
            print(f"  Target:        {target_apix}Å/px, box {target_box}")
            print(f"  Resolution:    {resolution}Å")
            print(f"{'=' * 70}\n")

            # Run CISTEM with PyMOL preprocessing
            sim_result = await self._run_cistem_with_pymol(
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

            # Process with Relion
            final_result = await self._process_simulated_map(
                sim_map_path=sim_result["sim_path"],
                output_folder=output_folder,
                base_name=base_name,
                sim_apix=sim_apix,
                target_apix=target_apix,
                target_box=target_box,
                resolution=resolution,
            )

            # Cleanup intermediate
            if os.path.exists(sim_result["sim_path"]):
                try:
                    os.remove(sim_result["sim_path"])
                except Exception as e:
                    print(f"[PDBService] Warning: Could not delete {sim_result['sim_path']}: {e}")

            if final_result.get("success"):
                print(f"\n{'=' * 70}")
                print(f"[PDBService] ✓ Template Generation Complete")
                print(f"  White: {Path(final_result['path_white']).name}")
                print(f"  Black: {Path(final_result['path_black']).name}")
                print(f"{'=' * 70}\n")

            return final_result

        except Exception as e:
            import traceback

            error_detail = f"Simulation error: {str(e)}\n{traceback.format_exc()}"
            print(f"[PDBService] ✗ {error_detail}")
            return {"success": False, "error": error_detail}

    async def _run_cistem_with_pymol(
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
        Prepare structure with PyMOL then run CISTEM.
        Uses absolute paths throughout - cisTEM supports this natively.
        Refactored to support both Container and Binary modes via ContainerService.
        """

        import os
        import asyncio
        import textwrap
        from pathlib import Path

        output_folder = Path(output_folder).resolve()
        output_folder.mkdir(parents=True, exist_ok=True)

        # Output files (all absolute paths)
        struct_file = output_folder / f"{base_name}_for_sim.cif"
        sim_output_path = output_folder / f"{base_name}_sim_raw.mrc"

        # Calculate offset for PyMOL transformation
        offset = (sim_box / 2.0) * sim_apix

        print(f"[PDBService] Step 1/3: PyMOL Structure Preparation")
        print(f"  Input:     {Path(pdb_path).name}")
        print(f"  Output:    {struct_file.name}")
        print(f"  Transform: Center COM → Translate [{offset:.1f}, {offset:.1f}, {offset:.1f}]")

        # === PYMOL PREPARATION ===
        pymol_script = textwrap.dedent(
            f"""
            import pymol2
            import sys
            import os

            try:
                pymol = pymol2.PyMOL()
                pymol.start()

                pymol.cmd.load("{pdb_path}", "structure")

                # Center by center-of-mass
                com = pymol.cmd.get_position("structure")
                pymol.cmd.translate([-com[0], -com[1], -com[2]], "structure")

                # Translate to positive quadrant
                pymol.cmd.translate([{offset}, {offset}, {offset}], "structure")

                # Save as CIF
                pymol.cmd.save("{struct_file}", "structure", format="cif")

                # Verify file was created
                if not os.path.exists("{struct_file}"):
                    raise Exception("PyMOL did not create output file")

                file_size = os.path.getsize("{struct_file}")
                if file_size == 0:
                    raise Exception("PyMOL created empty file")

                pymol.stop()

                print("PYMOL_SUCCESS")

            except Exception as e:
                print("PYMOL_ERROR:" + str(e), file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
                sys.exit(1)
            """
        ).strip()

        binds_needed = list(set([str(Path(pdb_path).parent), str(output_folder)]))

        pymol_result = await self._run_pymol_script(pymol_script, output_folder, binds_needed)

        if not pymol_result.get("success"):
            return {"success": False, "error": f"PyMOL failed: {pymol_result.get('error')}"}

        # CRITICAL: Verify file exists on HOST
        if not struct_file.exists():
            return {"success": False, "error": f"PyMOL did not create {struct_file.name}"}

        # CRITICAL: Re-parse CIF using BioPython (matches original libpdb.py lines 69-74)
        print(f"[PDBService] Re-parsing CIF with BioPython...")
        if not self._reparse_cif(struct_file):
            return {"success": False, "error": "CIF post-processing failed"}

        print(f"[PDBService] ✓ Structure prepared: {struct_file.name} ({struct_file.stat().st_size:,} bytes)")

        # === RUN CISTEM ===
        # 1. Resolve Tool Path and Mode
        tool_config = self.container_service.config.get_tool_config("cistem")
        if tool_config.exec_mode == "container":
            inner_cmd = "simulate"  # Binary inside container
        else:
            inner_cmd = tool_config.bin_path  # Absolute path to binary on host

        print(f"\n[PDBService] Step 2/3: cisTEM Density Simulation")
        print(f"  Mode:      {tool_config.exec_mode}")
        print(f"  Command:   {inner_cmd}")
        print(f"  Input:     {struct_file}")
        print(f"  Output:    {sim_output_path}")
        print(f"  Box:       {sim_box}")
        print(f"  Apix:      {sim_apix}")
        print(f"  Threads:   {num_threads}")

        # Prepare stdin parameters (using absolute paths!)
        # cisTEM reads these line-by-line
        stdin_input = "\n".join(
            [
                str(sim_output_path),      # outFile - ABSOLUTE PATH
                "Yes",                     # scPotential
                str(int(sim_box)),         # boxSize
                str(num_threads),          # threads
                str(struct_file),          # inputPDBPath - ABSOLUTE PATH
                "No",                      # addPart
                str(sim_apix),             # outputPix
                str(mod_scale_bf),         # perAtomScaleBfact
                str(mod_bf),               # perAtomBfact
                str(int(oversample)),      # oversample
                str(num_frames),           # numOfFrames
                "No",                      # expert
                "",                        # EOF
            ]
        )

        print(f"[PDBService]   cisTEM stdin parameters:")
        for i, line in enumerate(stdin_input.split("\n"), 1):
            if line:
                print(f"[PDBService]      {i:2d}. {line}")

        try:
            # 2. Wrap command (handles containerization or native pass-through)
            # We must bind the output folder so container can read/write files
            full_command = self.container_service.wrap_command_for_tool(
                command=inner_cmd,
                cwd=output_folder,
                tool_name="cistem",
                additional_binds=[str(output_folder)],
            )

            # 3. Environment setup (cisTEM needs OMP_NUM_THREADS)
            env = os.environ.copy()
            env["OMP_NUM_THREADS"] = str(num_threads)

            # 4. Execute WITHOUT blocking the event loop
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(output_folder),
                env=env,
            )

            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    process.communicate(input=stdin_input.encode("utf-8")),
                    timeout=600,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                return {"success": False, "error": "cisTEM timeout (>600s)"}

            stdout = (stdout_b or b"").decode("utf-8", errors="replace")
            stderr = (stderr_b or b"").decode("utf-8", errors="replace")

            print(f"[PDBService] ✓ cisTEM completed (exit code: {process.returncode})")

            if stdout:
                lines = [l for l in stdout.strip().split("\n") if l.strip()]
                if len(lines) > 30:
                    print(f"[PDBService]   Output (last 15 lines):")
                    for line in lines[-15:]:
                        print(f"[PDBService]      {line}")
                else:
                    print(f"[PDBService]   Output:")
                    for line in lines:
                        print(f"[PDBService]      {line}")

            if stderr and stderr.strip():
                print(f"[PDBService] ⚠ cisTEM stderr:")
                for line in stderr.strip().split("\n")[:10]:
                    print(f"[PDBService]      {line}")

            if process.returncode != 0:
                error = f"cisTEM failed with exit code {process.returncode}"
                if stderr:
                    error += f"\nStderr: {stderr[:500]}"
                return {"success": False, "error": error}

            # Verify output exists
            if not sim_output_path.exists():
                error = f"cisTEM did not create output file: {sim_output_path}\nDirectory contents:\n"
                for f in output_folder.iterdir():
                    error += f"  {f.name}\n"
                return {"success": False, "error": error}

            # Verify MRC is valid
            try:
                import mrcfile

                with mrcfile.open(sim_output_path, "r", permissive=True) as mrc:
                    shape = mrc.data.shape
                    voxel_size = float(mrc.voxel_size.x)
                    print(f"[PDBService] ✓ Simulation complete: {sim_output_path.name}")
                    print(f"[PDBService]      Shape: {shape}")
                    print(f"[PDBService]      Voxel size: {voxel_size:.3f} Å")
            except Exception as e:
                return {"success": False, "error": f"Invalid MRC file created: {e}"}

            # Cleanup structure file (optional - keep for debugging if needed)
            try:
                struct_file.unlink()
                print(f"[PDBService] ✓ Cleaned up: {struct_file.name}")
            except Exception as e:
                print(f"[PDBService] ⚠ Could not delete {struct_file.name}: {e}")

            return {"success": True, "sim_path": str(sim_output_path)}

        except Exception as e:
            import traceback

            error = f"cisTEM execution error: {str(e)}\n{traceback.format_exc()}"
            return {"success": False, "error": error}


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
        """Process CISTEM output with Relion."""
        try:
            print(f"\n[PDBService] Step 3/3: Template Finalization (Relion)")

            name_core = f"{base_name}_apix{target_apix:.2f}"
            if resolution and resolution > 0:
                name_core += f"_ares{int(resolution)}"
            name_core += f"_box{target_box}"

            path_white = os.path.join(output_folder, f"{name_core}_white.mrc")
            path_black = os.path.join(output_folder, f"{name_core}_black.mrc")

            # WHITE template
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
                return {"success": False, "error": f"Relion (white) failed: {result_white.get('error')}"}

            # BLACK template
            cmd_black = f"relion_image_handler --i {path_white} --o {path_black} --multiply_constant -1"

            result_black = await self.backend.run_shell_command(
                cmd_black, tool_name="relion", additional_binds=[output_folder]
            )

            if not result_black.get("success"):
                return {"success": False, "error": f"Relion (black) failed: {result_black.get('error')}"}

            return {"success": True, "path": path_black, "path_white": path_white, "path_black": path_black}

        except Exception as e:
            import traceback

            return {"success": False, "error": f"Processing error: {str(e)}\n{traceback.format_exc()}"}
