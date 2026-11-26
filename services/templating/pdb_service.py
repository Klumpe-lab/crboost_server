import os
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
from services.computing.container_service import get_container_service
from services.computing.processes import run_process
from services.configs.config_service import get_config_service
from services.result import err, ok
from services.templating.template_service import normalize_white_and_negate_to_black
from services.workers import WorkKind, run_in_worker

logger = logging.getLogger(__name__)


def _first_model_atoms(path: str) -> tuple[Any, list]:
    """The first model of a PDB or mmCIF file and its atoms, every alternate location included: what PyMOL
    loaded, whose place these functions took (roadmap 63 D7)."""
    from Bio.PDB import MMCIFParser, PDBParser

    parser = MMCIFParser(QUIET=True) if path.lower().endswith((".cif", ".mmcif")) else PDBParser(QUIET=True)
    model = next(iter(parser.get_structure("structure", path)))
    atoms = [atom for residue in model.get_residues() for atom in residue.get_unpacked_list()]
    if not atoms:
        raise ValueError(f"{os.path.basename(path)} holds no atoms")
    return model, atoms


def structure_extent(path: str) -> dict[str, Any]:
    """The model's bounding box in Å, its longest side, and how many of its atoms are named CA."""
    _model, atoms = _first_model_atoms(path)
    xyz = np.array([atom.coord for atom in atoms], dtype=np.float64)
    dims = xyz.max(axis=0) - xyz.min(axis=0)
    return {
        "residues": sum(atom.get_id() == "CA" for atom in atoms),
        "bbox": [round(float(d), 2) for d in dims],
        "max_dim": round(float(dims.max()), 2),
        "symmetry": "C1",
    }


def write_centered_cif(path: str, out_path: str, offset: float) -> int:
    """Write the model's first model as mmCIF with the centroid of its atoms (each atom weighing the same, as
    PyMOL's centre did) moved to (offset, offset, offset). Returns how many atoms it wrote."""
    from Bio.PDB import MMCIFIO

    model, atoms = _first_model_atoms(path)
    xyz = np.array([atom.coord for atom in atoms], dtype=np.float64)
    shift = offset - xyz.mean(axis=0)
    for atom, coord in zip(atoms, xyz + shift, strict=True):
        atom.set_coord(coord.astype(np.float32))
    io = MMCIFIO()
    io.set_structure(model)
    io.save(out_path)
    if not os.path.getsize(out_path):
        raise OSError(f"{out_path} was written empty")
    return len(atoms)


class PDBService:
    def __init__(self, backend):
        self.backend = backend
        self.container_service = get_container_service()

    # =========================================================================
    # BOX SIZE CALCULATION
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
    # STRUCTURE METADATA
    # =========================================================================

    async def get_structure_metadata(self, pdb_path: str) -> dict[str, Any]:
        """The model's bounding box and its longest side, read in a worker: a ribosome is 200,000 atoms of
        Python objects."""
        path = str(Path(pdb_path).resolve())
        try:
            meta = await run_in_worker(WorkKind.TEMPLATE, structure_extent, path)
        except Exception as e:
            logger.exception("Reading %s failed", path)
            return err(f"Could not read {Path(path).name}: {e}")
        return ok(**meta)

    # =========================================================================
    # CISTEM SIMULATION
    # =========================================================================

    async def simulate_map_from_pdb(
        self,
        pdb_path: str,
        output_folder: str,
        target_apix: float,
        target_box: int,
        resolution: float = 10.0,
        sim_apix: float | None = None,
        sim_box: int | None = None,
        mod_scale_bf: float = 1.0,
        mod_bf: float = 0.0,
        oversample: int = 2,
        num_frames: int = 7,
        num_threads: int = 25,
    ) -> dict[str, Any]:
        """Simulate density map from PDB using CISTEM."""
        try:
            # Ensure cistem is configured (binary or container)
            if not get_config_service().is_tool_configured("cistem"):
                return err("Tool 'cistem' not configured in conf.yaml")

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
                    return err(f"Could not determine structure dimensions: {meta.get('error') or 'unknown error'}")
                max_dim = meta["max_dim"]
                sim_box = self._calculate_optimal_box(max_dim, sim_apix)

            logger.info(
                "Starting PDB → Template Simulation: input=%s, output=%s, "
                "sim=%sÅ/px box %s, target=%sÅ/px box %s, resolution=%sÅ",
                pdb_path,
                output_folder,
                sim_apix,
                sim_box,
                target_apix,
                target_box,
                resolution,
            )

            sim_result = await self._run_cistem(
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
                    logger.warning("Could not delete %s: %s", sim_result["sim_path"], e)

            if final_result.get("success"):
                logger.info(
                    "✓ Template Generation Complete: White=%s, Black=%s",
                    Path(final_result["path_white"]).name,
                    Path(final_result["path_black"]).name,
                )

            return final_result

        except Exception as e:
            import traceback

            error_detail = f"Simulation error: {e!s}\n{traceback.format_exc()}"
            logger.error("✗ %s", error_detail)
            return err(error_detail)

    async def _run_cistem(
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
    ) -> dict[str, Any]:
        """Put the model in the middle of the simulation box, then simulate its density with cisTEM, as a
        container or a binary (ContainerService). Absolute paths throughout; cisTEM takes them."""
        output_folder = Path(output_folder).resolve()
        output_folder.mkdir(parents=True, exist_ok=True)

        struct_file = output_folder / f"{base_name}_for_sim.cif"
        sim_output_path = output_folder / f"{base_name}_sim_raw.mrc"

        # cisTEM's box has its origin at a corner: the model's centroid goes to the box's middle.
        offset = (sim_box / 2.0) * sim_apix
        logger.info(
            "Step 1/3: structure preparation: input=%s, output=%s, centroid to [%.1f, %.1f, %.1f]",
            Path(pdb_path).name,
            struct_file.name,
            offset,
            offset,
            offset,
        )
        try:
            n_atoms = await run_in_worker(WorkKind.TEMPLATE, write_centered_cif, pdb_path, str(struct_file), offset)
        except Exception as e:
            logger.exception("Preparing %s failed", pdb_path)
            return err(f"Could not prepare {Path(pdb_path).name} for the simulation: {e}")
        logger.info("%d atoms written", n_atoms)

        logger.info("✓ Structure prepared: %s (%s bytes)", struct_file.name, f"{struct_file.stat().st_size:,}")

        # === RUN CISTEM ===
        # 1. Resolve Tool Path and Mode. The wrapper puts `simulate` on PATH in both modes;
        # OMP_NUM_THREADS rides on the command because --cleanenv drops the caller's env.
        tool_config = get_config_service().get_tool_config("cistem")
        inner_cmd = f"OMP_NUM_THREADS={num_threads} simulate"

        logger.info(
            "Step 2/3: cisTEM Density Simulation: mode=%s, cmd=%s, input=%s, output=%s, box=%s, apix=%s, threads=%s",
            tool_config.exec_mode,
            inner_cmd,
            struct_file,
            sim_output_path,
            sim_box,
            sim_apix,
            num_threads,
        )

        # Prepare stdin parameters (absolute paths)
        # cisTEM reads these line-by-line
        stdin_input = "\n".join(
            [
                str(sim_output_path),  # outFile - absolute path
                "Yes",  # scPotential
                str(int(sim_box)),  # boxSize
                str(num_threads),  # threads
                str(struct_file),  # inputPDBPath - absolute path
                "No",  # addPart
                str(sim_apix),  # outputPix
                str(mod_scale_bf),  # perAtomScaleBfact
                str(mod_bf),  # perAtomBfact
                str(int(oversample)),  # oversample
                str(num_frames),  # numOfFrames
                "No",  # expert
                "",  # EOF
            ]
        )

        logger.info("cisTEM stdin parameters:")
        for i, line in enumerate(stdin_input.split("\n"), 1):
            if line:
                logger.info("  %2d. %s", i, line)

        try:
            # 2. Wrap command (handles containerization or native pass-through)
            # We must bind the output folder so container can read/write files
            full_command = self.container_service.wrap_command_for_tool(
                command=inner_cmd, cwd=output_folder, tool_name="cistem", additional_binds=[str(output_folder)]
            )

            # 3. Execute without blocking the event loop
            try:
                process = await run_process(
                    full_command, shell=True, cwd=output_folder, input=stdin_input.encode("utf-8"), timeout=600
                )
            except subprocess.TimeoutExpired:
                return err("cisTEM timeout (>600s)")
            stdout_b, stderr_b = process.stdout, process.stderr

            stdout = (stdout_b or b"").decode("utf-8", errors="replace")
            stderr = (stderr_b or b"").decode("utf-8", errors="replace")

            logger.info("✓ cisTEM completed (exit code: %s)", process.returncode)

            if stdout:
                lines = [ln for ln in stdout.strip().split("\n") if ln.strip()]
                if len(lines) > 30:
                    logger.info("Output (last 15 lines):")
                    for line in lines[-15:]:
                        logger.info("  %s", line)
                else:
                    logger.info("Output:")
                    for line in lines:
                        logger.info("  %s", line)

            if stderr and stderr.strip():
                logger.warning("⚠ cisTEM stderr:")
                for line in stderr.strip().split("\n")[:10]:
                    logger.warning("  %s", line)

            if process.returncode != 0:
                error = f"cisTEM failed with exit code {process.returncode}"
                if stderr:
                    error += f"\nStderr: {stderr[:500]}"
                return err(error)

            # Verify output exists
            if not sim_output_path.exists():
                error = f"cisTEM did not create output file: {sim_output_path}\nDirectory contents:\n"
                for f in output_folder.iterdir():
                    error += f"  {f.name}\n"
                return err(error)

            # Verify MRC is valid
            try:
                import mrcfile

                with mrcfile.open(sim_output_path, "r", permissive=True) as mrc:
                    shape = mrc.data.shape
                    voxel_size = float(mrc.voxel_size.x)
                    logger.info(
                        "✓ Simulation complete: %s, shape=%s, voxel_size=%.3f Å",
                        sim_output_path.name,
                        shape,
                        voxel_size,
                    )
            except Exception as e:
                return err(f"Invalid MRC file created: {e}")

            # Cleanup structure file (optional - keep for debugging if needed)
            try:
                struct_file.unlink()
                logger.info("✓ Cleaned up: %s", struct_file.name)
            except Exception as e:
                logger.warning("⚠ Could not delete %s: %s", struct_file.name, e)

            return ok(sim_path=str(sim_output_path))

        except Exception as e:
            import traceback

            error = f"cisTEM execution error: {e!s}\n{traceback.format_exc()}"
            return err(error)

    async def _process_simulated_map(
        self,
        sim_map_path: str,
        output_folder: str,
        base_name: str,
        sim_apix: float,
        target_apix: float,
        target_box: int,
        resolution: float,
    ) -> dict[str, Any]:
        """Process CISTEM output with Relion."""
        try:
            logger.info("Step 3/3: Template Finalization (Relion)")

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
                return err(f"Relion (white) failed: {result_white.get('error') or 'unknown error'}")

            # σ-normalize the resampled+lowpassed white volume and emit
            # black as its negation. Shared with process_volume_async to
            # keep all template outputs at mean=0, std=1 regardless of
            # source (RELION class / EMDB / PDB simulate / ellipsoid).
            norm_err = await asyncio.to_thread(normalize_white_and_negate_to_black, path_white, path_black)
            if norm_err:
                return err(f"Normalization failed: {norm_err}")

            return ok(path=path_black, path_white=path_white, path_black=path_black)

        except Exception as e:
            import traceback

            return err(f"Processing error: {e!s}\n{traceback.format_exc()}")
