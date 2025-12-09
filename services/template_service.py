import os
import asyncio
import gzip
import shutil
import requests
import numpy as np
import mrcfile
import scipy.ndimage
from pathlib import Path
from typing import Optional, Dict, Any, List

from services.container_service import get_container_service


class TemplateService:
    def __init__(self, backend):
        self.backend = backend
        self.container_service = get_container_service()

    # --- Async Wrappers ---

    async def fetch_emdb_map_async(self, emdb_id: str, output_folder: str) -> Dict[str, Any]:
        """Non-blocking wrapper for EMDB fetch."""
        return await asyncio.to_thread(self._fetch_emdb_map_sync, emdb_id, output_folder)

    async def fetch_pdb_async(self, pdb_id: str, output_folder: str) -> Dict[str, Any]:
        """Non-blocking wrapper for PDB fetch."""
        return await asyncio.to_thread(self._fetch_pdb_sync, pdb_id, output_folder)

    async def list_template_files_async(self, folder: str) -> List[str]:
        """List relevant files in the template directory."""
        return await asyncio.to_thread(self._list_files_sync, folder)

    async def process_volume_async(self, *args, **kwargs) -> Dict[str, Any]:
        """Non-blocking wrapper for volume processing."""
        return await asyncio.to_thread(self._process_volume_sync, *args, **kwargs)

    async def generate_basic_shape_async(self, *args, **kwargs) -> Dict[str, Any]:
        """Non-blocking wrapper for shape generation."""
        return await asyncio.get_running_loop().run_in_executor(
            None, lambda: self._generate_basic_shape_sync(*args, **kwargs)
        )

    async def calculate_auto_threshold_async(self, input_path: str) -> float:
        return await asyncio.to_thread(self._calculate_auto_threshold_sync, input_path)


    async def delete_file_async(self, file_path: str) -> Dict[str, Any]:
            """Deletes a specific file."""
            return await asyncio.to_thread(self._delete_file_sync, file_path)

    def _delete_file_sync(self, file_path: str) -> Dict[str, Any]:
        try:
            p = Path(file_path)
            if p.exists() and p.is_file():
                os.remove(p)
                # Optional: try to clean up associated preview if it exists
                preview = p.parent / (p.stem + "_preview" + p.suffix)
                if preview.exists():
                    os.remove(preview)
                return {"success": True}
            return {"success": False, "error": "File not found"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    # --- Synchronous Implementations ---

    def _list_files_sync(self, folder: str) -> List[str]:
        path = Path(folder)
        if not path.exists():
            return []
        extensions = {".pdb", ".cif", ".mrc", ".map", ".rec", ".ccp4", ".ent"}
        # Return full paths as strings
        return sorted([str(f) for f in path.iterdir() if f.suffix.lower() in extensions])

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

            # Check if map already exists
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

    def _generate_basic_shape_sync(
        self, shape_def: str, pixel_size: float, output_folder: str, min_box_size: int = 96
    ) -> Dict[str, Any]:
        print(f"--- [ INTERNAL PROCESSING ] ---")
        print(f"  Action:       Generate Basic Shape")
        print(f"  Definition:   {shape_def} (Angstroms)")
        print(f"  Pixel Size:   {pixel_size} Å")

        try:
            # 1. Parse Dimensions
            try:
                dims_ang = np.array([float(x) for x in shape_def.split(":")])
            except ValueError:
                return {"success": False, "error": "Invalid format. Use x:y:z"}

            if len(dims_ang) != 3:
                return {"success": False, "error": "3 dimensions required (x:y:z)"}

            # 2. Calculate Geometry
            radii_pix = (dims_ang / pixel_size) / 2.0

            offset = 32
            max_dim_pix = np.max(radii_pix * 2)
            calc_box = max_dim_pix * 1.2  # 20% padding
            box_size = int((calc_box + offset - 1) // offset) * offset
            box_size = max(box_size, min_box_size)

            print(f"  Calculated Box: {box_size} px")

            # 3. File Paths
            shape_str = shape_def.replace(":", "_")
            base_name = f"ellipsoid_{shape_str}_apix{pixel_size}"
            os.makedirs(output_folder, exist_ok=True)

            path_black = os.path.join(output_folder, f"{base_name}_black.mrc")
            path_white = os.path.join(output_folder, f"{base_name}_white.mrc")

            # 4. Generate Data (CPU Intensive)
            print(f"  Generating ellipsoid array...")
            mask_data = self._create_ellipsoid_array((box_size, box_size, box_size), radii_pix)

            # 5. Lowpass Filter
            lowpass_res = 45.0  # Fixed soft edge
            sigma = (lowpass_res / pixel_size) / 3.0
            print(f"  Applying Gaussian Filter (sigma={sigma:.2f})...")
            soft_mask = scipy.ndimage.gaussian_filter(mask_data, sigma=sigma)

            # 6. Save Files
            print(f"  Saving to: {path_black}")
            self._save_mrc(soft_mask * -1.0, path_black, pixel_size)
            self._save_mrc(soft_mask, path_white, pixel_size)

            print("-" * 30)
            return {"success": True, "path_black": path_black, "path_white": path_white, "box_size": box_size}

        except Exception as e:
            import traceback

            print(f"  [ERROR] {str(e)}")
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _create_ellipsoid_array(self, shape, radii):
        """Numpy implementation of ellipsoid mask."""
        nz, ny, nx = shape
        z, y, x = np.ogrid[-nz / 2 : nz / 2, -ny / 2 : ny / 2, -nx / 2 : nx / 2]

        # Ellipsoid equation: (x/rx)^2 + (y/ry)^2 + (z/rz)^2 <= 1
        mask = ((x / radii[0]) ** 2 + (y / radii[1]) ** 2 + (z / radii[2]) ** 2) <= 1.0
        return mask.astype(np.float32)

    def _process_volume_sync(
        self, input_path: str, output_folder: str, target_apix: float, target_box: int, tag: str = ""
    ) -> Dict[str, Any]:
        """Resample, crop, invert contrast."""
        try:
            if not os.path.exists(input_path):
                return {"success": False, "error": "File not found"}

            with mrcfile.open(input_path) as mrc:
                data = mrc.data.copy()
                orig_apix = mrc.voxel_size.x

            # Resample
            zoom = orig_apix / target_apix
            if abs(zoom - 1.0) > 0.01:
                data = scipy.ndimage.zoom(data, zoom, order=1)

            # Crop/Pad
            data = self._crop_or_pad(data, target_box)

            # Save
            base = os.path.splitext(os.path.basename(input_path))[0]
            if tag:
                base = tag

            name = f"{base}_apix{target_apix}_box{target_box}"
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

        # Calculate centers
        c_curr = curr // 2
        c_targ = target // 2

        # Calculate overlap ranges
        start_curr = np.maximum(0, c_curr - c_targ)
        end_curr = np.minimum(curr, c_curr + c_targ + (target % 2))  # handle odd/even

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

    def _calculate_auto_threshold_sync(self, path):
        try:
            with mrcfile.open(path) as mrc:
                d = mrc.data
                return float(np.mean(d) + 1.85 * np.std(d))
        except:
            return 0.001

    # --- RELION Masking (Already Async via backend shell runner) ---

    async def create_mask_relion(self, input_vol, output_mask, threshold, extend, soft, lowpass):
        """Calls relion_mask_create via container service."""
        try:
            abs_in = os.path.abspath(input_vol)
            abs_out = os.path.abspath(output_mask)
            os.makedirs(os.path.dirname(abs_out), exist_ok=True)

            cmd = (
                f"relion_mask_create --i {abs_in} --o {abs_out} --ini_threshold {threshold} "
                f"--extend_inimask {extend} --width_soft_edge {soft} --lowpass {lowpass} --j 4"
            )

            # Bind directories
            binds = list(set([os.path.dirname(abs_in), os.path.dirname(abs_out)]))

            res = await self.backend.run_shell_command(
                cmd, cwd=Path(os.path.dirname(abs_out)), tool_name="relion", additional_binds=binds
            )

            if res["success"]:
                return {"success": True, "path": abs_out}
            return {"success": False, "error": res.get("error", "Unknown error")}
        except Exception as e:
            return {"success": False, "error": str(e)}
