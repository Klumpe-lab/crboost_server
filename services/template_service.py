import os
import asyncio
import gzip
import shutil
import requests
import numpy as np
import mrcfile
from scipy import fftpack
from skimage import filters
from pathlib import Path
from typing import Optional, Dict, Any, List

from services.container_service import get_container_service


class TemplateService:
    def __init__(self, backend):
        self.backend = backend
        self.container_service = get_container_service()

    # =========================================================
    # ASYNC WRAPPERS
    # =========================================================

    async def fetch_emdb_map_async(self, emdb_id: str, output_folder: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._fetch_emdb_map_sync, emdb_id, output_folder)

    async def fetch_pdb_async(self, pdb_id: str, output_folder: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._fetch_pdb_sync, pdb_id, output_folder)

    async def list_template_files_async(self, folder: str) -> List[str]:
        return await asyncio.to_thread(self._list_files_sync, folder)

    async def calculate_thresholds_async(self, input_path: str, lowpass: float = None) -> Dict[str, float]:
        return await asyncio.to_thread(self._calculate_thresholds_sync, input_path, lowpass)

    async def delete_file_async(self, file_path: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._delete_file_sync, file_path)

    # =========================================================
    # CORE RELION VOLUME PROCESSING
    # =========================================================

    async def process_volume_async(
        self,
        input_path: str,
        output_folder: str,
        target_apix: float,
        target_box: int,
        resolution: float = None,
        tag: str = "",
    ) -> Dict[str, Any]:
        """
        Processes volume using relion_image_handler.
        Replaces legacy scipy zoom with Fourier-space resampling.
        Creates paired white/black contrast templates.
        """
        try:
            if not os.path.exists(input_path):
                return {"success": False, "error": "Input file not found"}

            os.makedirs(output_folder, exist_ok=True)
            base = tag if tag else Path(input_path).stem
            # Sanitize name to prevent relion command issues
            base = base.replace("_white", "").replace("_black", "")

            name_core = f"{base}_apix{target_apix:.2f}_box{target_box}"
            if resolution:
                name_core += f"_lp{int(resolution)}"

            path_w = os.path.join(output_folder, f"{name_core}_white.mrc")
            path_b = os.path.join(output_folder, f"{name_core}_black.mrc")

            # 1. Generate White (Positive) Template
            # We use --rescale_angpix for Fourier downsampling
            cmd_w = (
                f"relion_image_handler --i {input_path} --o {path_w} "
                f"--rescale_angpix {target_apix} --new_box {target_box} "
            )
            if resolution:
                cmd_w += f"--lowpass {resolution} --filter_edge_width 6 "

            res_w = await self.backend.run_shell_command(
                cmd_w, tool_name="relion", additional_binds=[os.path.dirname(input_path), output_folder]
            )

            if not res_w["success"]:
                return res_w

            # 2. Generate Black (Inverted) Template
            # Uses the newly created white template as input to save computation
            cmd_b = f"relion_image_handler --i {path_w} --o {path_b} --multiply_constant -1"
            res_b = await self.backend.run_shell_command(cmd_b, tool_name="relion", additional_binds=[output_folder])

            if not res_b["success"]:
                return res_b

            return {"success": True, "path_white": path_w, "path_black": path_b}

        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================
    # SHAPE GENERATION (REFACTORED)
    # =========================================================

    async def generate_basic_shape_async(
        self, shape_def: str, pixel_size: float, output_folder: str, min_box_size: int = 96, lowpass_res: float = 45.0
    ) -> Dict[str, Any]:
        """Generate ellipsoid with numpy and refine dimensions with RELION."""
        try:
            dims_ang = np.array([float(x) for x in shape_def.split(":")])
            max_dim_ang = float(np.max(dims_ang))

            # Initial box size for generation (padded)
            box_size = int(((max_dim_ang / pixel_size) * 1.3 + 31) // 32) * 32
            box_size = max(box_size, min_box_size)

            radii_pix = (dims_ang / pixel_size) / 2.0

            temp_name = f"temp_shape_{shape_def.replace(':', '_')}.mrc"
            temp_path = os.path.join(output_folder, temp_name)

            # Create mask data
            nz, ny, nx = (box_size, box_size, box_size)
            z, y, x = np.ogrid[-nz / 2 : nz / 2, -ny / 2 : ny / 2, -nx / 2 : nx / 2]
            mask_data = ((x / radii_pix[0]) ** 2 + (y / radii_pix[1]) ** 2 + (z / radii_pix[2]) ** 2) <= 1.0
            mask_data = mask_data.astype(np.float32)

            # Save temporary raw mask
            with mrcfile.new(temp_path, overwrite=True) as mrc:
                mrc.set_data(mask_data)
                mrc.voxel_size = pixel_size

            # Use RELION to apply lowpass and finalize box/contrasts
            # This ensures symmetry and normalization match pdb-derived templates
            res = await self.process_volume_async(
                temp_path,
                output_folder,
                pixel_size,
                box_size,
                resolution=lowpass_res,
                tag=f"ellipsoid_{shape_def.replace(':', '_')}",
            )

            if os.path.exists(temp_path):
                os.remove(temp_path)

            return res

        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================
    # RELION MASKING
    # =========================================================

    async def create_mask_relion(self, input_vol, output_mask, threshold, extend, soft, lowpass):
        """Standard RELION mask creation."""
        try:
            abs_in = os.path.abspath(input_vol)
            abs_out = os.path.abspath(output_mask)
            os.makedirs(os.path.dirname(abs_out), exist_ok=True)

            cmd = (
                f"relion_mask_create --i {abs_in} --o {abs_out} --ini_threshold {threshold} "
                f"--extend_inimask {extend} --width_soft_edge {soft} --lowpass {lowpass} --j 4"
            )

            binds = list(set([os.path.dirname(abs_in), os.path.dirname(abs_out)]))
            res = await self.backend.run_shell_command(
                cmd, cwd=Path(os.path.dirname(abs_out)), tool_name="relion", additional_binds=binds
            )

            if res["success"]:
                return {"success": True, "path": abs_out}
            return {"success": False, "error": res.get("error", "Unknown error")}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================
    # INTERNAL UTILS
    # =========================================================

    def _calculate_thresholds_sync(self, input_path: str, lowpass: float = None) -> Dict[str, float]:
        """Calculate multiple threshold methods using skimage filters."""
        try:
            with mrcfile.open(input_path) as mrc:
                vol = mrc.data.copy()
                voxel_size = float(mrc.voxel_size.x)

            if lowpass is not None and lowpass > 0:
                # Use internal numpy lowpass for threshold estimation
                vol = self._gaussian_lowpass(vol, lowpass, voxel_size)

            return {
                "otsu": float(filters.threshold_otsu(vol)),
                "isodata": float(filters.threshold_isodata(vol)),
                "li": float(filters.threshold_li(vol)),
                "yen": float(filters.threshold_yen(vol)),
                "flexible_bounds": float(np.mean(vol) + 1.85 * np.std(vol)),
            }
        except Exception as e:
            print(f"[TemplateService] Threshold calculation error: {e}")
            return {"flexible_bounds": 0.001}

    def _gaussian_lowpass(self, volume: np.ndarray, cutoff_angstrom: float, voxel_size: float) -> np.ndarray:
        """Apply Gaussian low-pass filter in Fourier space (numpy version)."""
        nx, ny, nz = volume.shape
        kx = fftpack.fftfreq(nx, d=voxel_size)
        ky = fftpack.fftfreq(ny, d=voxel_size)
        kz = fftpack.fftfreq(nz, d=voxel_size)

        kx_grid, ky_grid, kz_grid = np.meshgrid(kx, ky, kz, indexing="ij")
        k_squared = kx_grid**2 + ky_grid**2 + kz_grid**2

        sigma = cutoff_angstrom / (2 * np.pi)
        gaussian_filter = np.exp(-k_squared * (2 * np.pi**2) * sigma**2)

        vol_fft = fftpack.fftn(volume)
        filtered_vol = np.real(fftpack.ifftn(vol_fft * gaussian_filter))
        return filtered_vol.astype(np.float32)

    def _list_files_sync(self, folder: str) -> List[str]:
        path = Path(folder)
        if not path.exists():
            return []
        extensions = {".pdb", ".cif", ".mrc", ".map", ".rec", ".ccp4", ".ent"}
        return sorted([str(f) for f in path.iterdir() if f.suffix.lower() in extensions and "_preview" not in f.name])

    def _delete_file_sync(self, file_path: str) -> Dict[str, Any]:
        try:
            p = Path(file_path)
            if p.exists() and p.is_file():
                os.remove(p)
                return {"success": True}
            return {"success": False, "error": "File not found"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _fetch_pdb_sync(self, pdb_id: str, output_folder: str) -> Dict[str, Any]:
        try:
            pdb_id = pdb_id.lower().strip()
            out_path = Path(output_folder) / f"{pdb_id}.cif"
            if out_path.exists():
                return {"success": True, "path": str(out_path)}

            url = f"https://files.rcsb.org/download/{pdb_id}.cif"
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                out_path.write_bytes(r.content)
            return {"success": True, "path": str(out_path)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _fetch_emdb_map_sync(self, emdb_id: str, output_folder: str) -> Dict[str, Any]:
        try:
            emdb_id = emdb_id.upper().strip().replace("EMD-", "").replace("EMD", "")
            url = f"https://ftp.ebi.ac.uk/pub/databases/emdb/structures/EMD-{emdb_id}/map/emd_{emdb_id}.map.gz"
            gz_path = os.path.join(output_folder, f"emd_{emdb_id}.map.gz")
            map_path = os.path.join(output_folder, f"emd_{emdb_id}.map")

            if os.path.exists(map_path):
                return {"success": True, "path": map_path}

            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(gz_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

            with gzip.open(gz_path, "rb") as f_in:
                with open(map_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            if os.path.exists(gz_path):
                os.remove(gz_path)
            return {"success": True, "path": map_path}
        except Exception as e:
            return {"success": False, "error": str(e)}
