# services/template_service.py
import os
import asyncio
import gzip
import shutil
import requests
import numpy as np
import mrcfile
import scipy.ndimage
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

    async def process_volume_async(
        self,
        input_path: str,
        output_folder: str,
        target_apix: float,
        target_box: int,
        resolution: float = None,
        tag: str = "",
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._process_volume_sync, input_path, output_folder, target_apix, target_box, resolution, tag
        )

    async def generate_basic_shape_async(
        self, shape_def: str, pixel_size: float, output_folder: str, min_box_size: int = 96, lowpass_res: float = 45.0
    ) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._generate_basic_shape_sync, shape_def, pixel_size, output_folder, min_box_size, lowpass_res
        )

    async def calculate_thresholds_async(self, input_path: str, lowpass: float = None) -> Dict[str, float]:
        return await asyncio.to_thread(self._calculate_thresholds_sync, input_path, lowpass)

    async def delete_file_async(self, file_path: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self._delete_file_sync, file_path)

    async def get_optimal_box_size_async(self, dimension_ang: float, pixel_size: float, min_box: int = 96) -> int:
        return await asyncio.to_thread(self._get_optimal_box_size_sync, dimension_ang, pixel_size, min_box)

    # =========================================================
    # BOX SIZE CALCULATION
    # =========================================================

    def _get_optimal_box_size_sync(self, dimension_ang: float, pixel_size: float, min_box: int = 96) -> int:
        """
        Calculate optimal box size based on particle dimension.
        Uses 32-voxel alignment with ~20% padding.
        """
        if pixel_size <= 0:
            return min_box
        dim_pix = dimension_ang / pixel_size
        padded = dim_pix * 1.2
        offset = 32
        box_size = int((padded + offset - 1) // offset) * offset
        return max(box_size, min_box)

    def _adapt_box_for_pixel_size(self, box: int, pix_template: float, pix_target: float, min_box: int = 96) -> int:
        """Recalculate box size when pixel sizes differ."""
        calc_box = box * (pix_template / pix_target)
        offset = 32
        new_box = int((calc_box + offset - 1) // offset) * offset
        return max(new_box, min_box)

    # =========================================================
    # THRESHOLD CALCULATION
    # =========================================================

    def _calculate_thresholds_sync(self, input_path: str, lowpass: float = None) -> Dict[str, float]:
        """Calculate multiple threshold methods."""
        try:
            with mrcfile.open(input_path) as mrc:
                vol = mrc.data.copy()
                voxel_size = float(mrc.voxel_size.x)

            if lowpass is not None and lowpass > 0:
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

    # =========================================================
    # LOW-PASS FILTERING
    # =========================================================

    def _gaussian_lowpass(
        self, volume: np.ndarray, cutoff_angstrom: float, voxel_size: float, invert: bool = False
    ) -> np.ndarray:
        """Apply Gaussian low-pass filter in Fourier space."""
        nx, ny, nz = volume.shape
        kx = fftpack.fftfreq(nx, d=voxel_size)
        ky = fftpack.fftfreq(ny, d=voxel_size)
        kz = fftpack.fftfreq(nz, d=voxel_size)

        kx_grid, ky_grid, kz_grid = np.meshgrid(kx, ky, kz, indexing="ij")
        k_squared = kx_grid**2 + ky_grid**2 + kz_grid**2

        sigma = cutoff_angstrom / (2 * np.pi)
        gaussian_filter = np.exp(-k_squared * (2 * np.pi**2) * sigma**2)

        vol_fft = fftpack.fftn(volume)
        filtered_fft = vol_fft * gaussian_filter
        filtered_vol = np.real(fftpack.ifftn(filtered_fft))

        filtered_vol = filtered_vol - np.mean(filtered_vol)
        std = np.std(filtered_vol)
        if std > 0:
            filtered_vol = filtered_vol / std

        if invert:
            filtered_vol = -filtered_vol

        return filtered_vol.astype(np.float32)

    # =========================================================
    # SHAPE GENERATION
    # =========================================================

    def _generate_basic_shape_sync(
        self, shape_def: str, pixel_size: float, output_folder: str, min_box_size: int = 96, lowpass_res: float = 45.0
    ) -> Dict[str, Any]:
        """Generate ellipsoid with automatic box calculation and lowpass."""
        print(f"[TemplateService] Generating shape: {shape_def} @ {pixel_size}Å/px, LP={lowpass_res}Å")

        try:
            try:
                dims_ang = np.array([float(x) for x in shape_def.split(":")])
            except ValueError:
                return {"success": False, "error": "Invalid format. Use x:y:z"}

            if len(dims_ang) != 3:
                return {"success": False, "error": "3 dimensions required (x:y:z)"}

            radii_pix = (dims_ang / pixel_size) / 2.0
            max_dim_ang = float(np.max(dims_ang))

            box_size = self._get_optimal_box_size_sync(max_dim_ang, pixel_size, min_box_size)
            print(f"  Box size: {box_size} px, File size: ~{(box_size**3 * 4) / (1024 * 1024):.1f} MB")

            shape_str = shape_def.replace(":", "_")
            base_name = f"ellipsoid_{shape_str}_apix{pixel_size}"
            os.makedirs(output_folder, exist_ok=True)

            path_black = os.path.join(output_folder, f"{base_name}_black.mrc")
            path_white = os.path.join(output_folder, f"{base_name}_white.mrc")

            mask_data = self._create_ellipsoid_array((box_size, box_size, box_size), radii_pix)

            filtered = self._gaussian_lowpass(mask_data, lowpass_res, pixel_size, invert=False)
            filtered_inv = self._gaussian_lowpass(mask_data, lowpass_res, pixel_size, invert=True)

            self._save_mrc(filtered_inv, path_black, pixel_size)
            self._save_mrc(filtered, path_white, pixel_size)

            return {"success": True, "path_black": path_black, "path_white": path_white, "box_size": box_size}

        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _create_ellipsoid_array(self, shape, radii):
        nz, ny, nx = shape
        z, y, x = np.ogrid[-nz / 2 : nz / 2, -ny / 2 : ny / 2, -nx / 2 : nx / 2]
        mask = ((x / radii[0]) ** 2 + (y / radii[1]) ** 2 + (z / radii[2]) ** 2) <= 1.0
        return mask.astype(np.float32)

    # =========================================================
    # VOLUME PROCESSING
    # =========================================================

    def _process_volume_sync(
        self,
        input_path: str,
        output_folder: str,
        target_apix: float,
        target_box: int,
        resolution: float = None,
        tag: str = "",
    ) -> Dict[str, Any]:
        """Process volume: resample, crop, optional lowpass, create black/white versions."""
        try:
            if not os.path.exists(input_path):
                return {"success": False, "error": "File not found"}

            with mrcfile.open(input_path) as mrc:
                data = mrc.data.copy()
                orig_apix = float(mrc.voxel_size.x)

            zoom = orig_apix / target_apix
            if abs(zoom - 1.0) > 0.01:
                data = scipy.ndimage.zoom(data, zoom, order=1)

            data = self._crop_or_pad(data, target_box)

            if resolution is not None and resolution > 0:
                data = self._gaussian_lowpass(data, resolution, target_apix, invert=False)

            base = tag if tag else os.path.splitext(os.path.basename(input_path))[0]
            name = f"{base}_apix{target_apix}_box{target_box}"
            if resolution:
                name += f"_lp{int(resolution)}"

            path_w = os.path.join(output_folder, f"{name}_white.mrc")
            path_b = os.path.join(output_folder, f"{name}_black.mrc")

            os.makedirs(output_folder, exist_ok=True)
            self._save_mrc(data, path_w, target_apix)
            self._save_mrc(data * -1.0, path_b, target_apix)

            return {"success": True, "path_black": path_b, "path_white": path_w}
        except Exception as e:
            import traceback

            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _crop_or_pad(self, data, size):
        curr = np.array(data.shape)
        target = np.array([size, size, size])

        if np.array_equal(curr, target):
            return data

        new_data = np.zeros(target, dtype=np.float32)
        c_curr = curr // 2
        c_targ = target // 2

        start_curr = np.maximum(0, c_curr - c_targ)
        end_curr = np.minimum(curr, c_curr + c_targ + (target % 2))
        len_copy = end_curr - start_curr

        start_targ = np.maximum(0, c_targ - c_curr)
        end_targ = start_targ + len_copy

        new_data[start_targ[0] : end_targ[0], start_targ[1] : end_targ[1], start_targ[2] : end_targ[2]] = data[
            start_curr[0] : end_curr[0], start_curr[1] : end_curr[1], start_curr[2] : end_curr[2]
        ]
        return new_data

    def _save_mrc(self, data, path, pixs):
        with mrcfile.new(path, overwrite=True) as mrc:
            mrc.set_data(data.astype(np.float32))
            mrc.voxel_size = pixs

    # =========================================================
    # FILE OPERATIONS
    # =========================================================

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
                preview = p.parent / (p.stem + "_preview" + p.suffix)
                if preview.exists():
                    os.remove(preview)
                return {"success": True}
            return {"success": False, "error": "File not found"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _fetch_pdb_sync(self, pdb_id: str, output_folder: str) -> Dict[str, Any]:
        try:
            pdb_id = pdb_id.lower().strip()
            os.makedirs(output_folder, exist_ok=True)
            out_path = Path(output_folder) / f"{pdb_id}.cif"

            if out_path.exists():
                return {"success": True, "path": str(out_path)}

            url = f"https://files.rcsb.org/download/{pdb_id}.cif"
            print(f"[TemplateService] Downloading {url}...")

            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                out_path.write_bytes(r.content)

            return {"success": True, "path": str(out_path)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _fetch_emdb_map_sync(self, emdb_id: str, output_folder: str) -> Dict[str, Any]:
        try:
            emdb_id = emdb_id.upper().strip().replace("EMD-", "").replace("EMD", "")
            if not emdb_id:
                return {"success": False, "error": "Invalid ID"}

            url = f"https://ftp.ebi.ac.uk/pub/databases/emdb/structures/EMD-{emdb_id}/map/emd_{emdb_id}.map.gz"
            os.makedirs(output_folder, exist_ok=True)
            gz_path = os.path.join(output_folder, f"emd_{emdb_id}.map.gz")
            map_path = os.path.join(output_folder, f"emd_{emdb_id}.map")

            if os.path.exists(map_path):
                return {"success": True, "path": map_path}

            print(f"[TemplateService] Downloading {url}...")
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(gz_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

            print(f"[TemplateService] Extracting...")
            with gzip.open(gz_path, "rb") as f_in:
                with open(map_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            if os.path.exists(gz_path):
                os.remove(gz_path)

            return {"success": True, "path": map_path}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================================================
    # RELION MASKING
    # =========================================================

    async def create_mask_relion(self, input_vol, output_mask, threshold, extend, soft, lowpass):
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
