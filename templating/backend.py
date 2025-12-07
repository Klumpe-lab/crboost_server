import os
import gzip
import shutil
import requests
import numpy as np
import mrcfile
from scipy.ndimage import zoom, gaussian_filter
from Bio.PDB import PDBParser, MMCIFParser
import warnings

# --- File Utilities ---


def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


def download_file(url, target_path):
    try:
        # NOTE: We use iter_content to ensure we get decoded (decompressed) content
        # if the server sends gzip-encoded text but we want to save it as text.
        # However, keeping it robust in get_coords_from_file is safer.
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(target_path, "wb") as f:
                shutil.copyfileobj(response.raw, f)
            return True, f"Downloaded to {target_path}"
        return False, f"HTTP {response.status_code}"
    except Exception as e:
        return False, str(e)


# --- PDB Handling ---


def fetch_pdb(pdb_code, out_folder):
    """Downloads PDB/CIF from RCSB."""
    ensure_folder(out_folder)
    pdb_code = pdb_code.lower()
    # Try CIF first
    url = f"https://files.rcsb.org/download/{pdb_code}.cif"
    target = os.path.join(out_folder, f"{pdb_code}.cif")
    success, msg = download_file(url, target)
    if success:
        return True, target

    # Try PDB
    url = f"https://files.rcsb.org/download/{pdb_code}.pdb"
    target = os.path.join(out_folder, f"{pdb_code}.pdb")
    return download_file(url, target)


def get_coords_from_file(file_path):
    """
    Parses PDB/CIF and returns numpy array of coordinates.
    Robustly handles GZIP compressed files automatically.
    """
    # 1. Check if file is GZIP compressed (Magic number 1f 8b)
    is_gzipped = False
    try:
        with open(file_path, "rb") as f:
            header = f.read(2)
            if header == b"\x1f\x8b":
                is_gzipped = True
    except Exception:
        pass  # Handle empty files or permission errors gracefully later

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Select Parser
        if ".cif" in file_path.lower():
            parser = MMCIFParser()
        else:
            parser = PDBParser()

        # Open file correctly (Text mode is required for BioPython)
        if is_gzipped:
            handle = gzip.open(file_path, "rt")
        else:
            handle = open(file_path, "r")

        try:
            structure = parser.get_structure("struct", handle)
        except Exception as e:
            handle.close()
            raise e

        handle.close()

    coords = []
    for atom in structure.get_atoms():
        coords.append(atom.get_coord())
    return np.array(coords)


def align_pdb_to_principal_axis(file_path, output_path):
    """Aligns structure to principal axes using PCA."""
    try:
        coords = get_coords_from_file(file_path)
    except Exception as e:
        return False, str(e)

    if len(coords) == 0:
        return False, "No atoms found"

    # Center
    center = np.mean(coords, axis=0)
    centered = coords - center

    # PCA
    cov = np.cov(centered.T)
    evals, evecs = np.linalg.eigh(cov)

    # Sort by eigenvalue (largest to smallest)
    idx = evals.argsort()[::-1]
    evecs = evecs[:, idx]

    # Rotate
    aligned_coords = np.dot(centered, evecs)

    return True, aligned_coords


def simulate_map_from_pdb(file_path, output_mrc, apix, box_size, resolution, bfactor=0):
    """Generates a density map from atoms."""
    try:
        # 1. Get Coords
        coords = get_coords_from_file(file_path)

        if len(coords) == 0:
            return False, "PDB file contains no atoms"

        # 2. Center Coords in Box
        center_mass = np.mean(coords, axis=0)
        coords_centered = coords - center_mass

        # Shift to grid center
        box_center_ang = (box_size * apix) / 2.0
        coords_grid_ang = coords_centered + box_center_ang

        # Convert to pixels
        coords_px = coords_grid_ang / apix

        # 3. Create Grid
        grid = np.zeros((box_size, box_size, box_size), dtype=np.float32)

        # 4. Simple Density Projection
        # Filter atoms outside box
        mask = (
            (coords_px[:, 0] >= 0)
            & (coords_px[:, 0] < box_size)
            & (coords_px[:, 1] >= 0)
            & (coords_px[:, 1] < box_size)
            & (coords_px[:, 2] >= 0)
            & (coords_px[:, 2] < box_size)
        )
        valid_coords = coords_px[mask].astype(int)

        if len(valid_coords) == 0:
            return False, "All atoms are outside the simulation box. Increase box size."

        # Add density
        np.add.at(grid, (valid_coords[:, 0], valid_coords[:, 1], valid_coords[:, 2]), 1.0)

        # 5. Apply Lowpass
        sigma = resolution / (3.0 * apix)
        if bfactor > 0:
            sigma = np.sqrt(sigma**2 + (bfactor / (8 * np.pi**2 * apix**2)))

        grid = gaussian_filter(grid, sigma=sigma)

        # 6. Save
        with mrcfile.new(output_mrc, overwrite=True) as mrc:
            mrc.set_data(grid)
            mrc.voxel_size = apix

        return True, "Simulation complete"
    except Exception as e:
        return False, str(e)


# --- Volume Processing ---


def download_emdb(emdb_id, out_folder):
    """Downloads map from EMDB."""
    ensure_folder(out_folder)
    clean_id = emdb_id.lower().replace("emd_", "").replace("emd-", "")
    url = f"https://ftp.ebi.ac.uk/pub/databases/emdb/structures/EMD-{clean_id}/map/emd_{clean_id}.map.gz"
    gz_target = os.path.join(out_folder, f"emd_{clean_id}.map.gz")
    map_target = os.path.join(out_folder, f"emd_{clean_id}.map")

    success, msg = download_file(url, gz_target)
    if not success:
        return False, msg

    try:
        with gzip.open(gz_target, "rb") as f_in:
            with open(map_target, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_target)
        return True, map_target
    except Exception as e:
        return False, str(e)


def process_volume_numpy(input_path, output_path, target_apix, target_box, invert=False, lowpass_res=None):
    """Rescaling, Resizing (Crop/Pad), Inverting, Filtering."""
    try:
        with mrcfile.open(input_path) as mrc:
            data = mrc.data.copy()
            src_apix = mrc.voxel_size.x

        # 1. Rescale
        if target_apix and abs(src_apix - target_apix) > 0.01:
            scale = src_apix / target_apix
            data = zoom(data, scale, order=1)

        # 2. Resize
        if target_box:
            curr_z, curr_y, curr_x = data.shape
            new_data = np.zeros((target_box, target_box, target_box), dtype=np.float32)

            cz, cy, cx = curr_z // 2, curr_y // 2, curr_x // 2
            nz, ny, nx = target_box // 2, target_box // 2, target_box // 2

            z_start_src = max(0, cz - nz)
            z_end_src = min(curr_z, cz + nz)
            y_start_src = max(0, cy - ny)
            y_end_src = min(curr_y, cy + ny)
            x_start_src = max(0, cx - nx)
            x_end_src = min(curr_x, cx + nx)

            z_start_dst = max(0, nz - cz)
            y_start_dst = max(0, ny - cy)
            x_start_dst = max(0, nx - cx)
            lz = z_end_src - z_start_src
            ly = y_end_src - y_start_src
            lx = x_end_src - x_start_src

            new_data[z_start_dst : z_start_dst + lz, y_start_dst : y_start_dst + ly, x_start_dst : x_start_dst + lx] = (
                data[z_start_src : z_start_src + lz, y_start_src : y_start_src + ly, x_start_src : x_start_src + lx]
            )
            data = new_data

        # 3. Lowpass
        if lowpass_res:
            sigma = lowpass_res / (3.0 * target_apix)
            data = gaussian_filter(data, sigma)

        # 4. Invert
        if invert:
            data = data * -1.0

        # Write
        with mrcfile.new(output_path, overwrite=True) as mrc:
            mrc.set_data(data)
            mrc.voxel_size = target_apix

        return True, output_path

    except Exception as e:
        return False, str(e)


def create_ellipsoid(dims_str, apix, out_folder):
    """Creates geometric ellipsoid mask."""
    ensure_folder(out_folder)
    try:
        dims = [float(x) for x in dims_str.split(":")]
        box_dim = int(max(dims) * 1.5 / apix)
        box_dim = ((box_dim + 31) // 32) * 32

        c = box_dim / 2.0
        z, y, x = np.ogrid[:box_dim, :box_dim, :box_dim]

        rx, ry, rz = [d / (2 * apix) for d in dims]
        mask = ((x - c) ** 2 / rx**2 + (y - c) ** 2 / ry**2 + (z - c) ** 2 / rz**2) <= 1.0

        data = gaussian_filter(mask.astype(np.float32), sigma=1.0)

        base_name = f"ellipsoid_{dims_str.replace(':', '_')}_apix{apix}"
        path_white = os.path.join(out_folder, f"{base_name}_white.mrc")
        path_black = os.path.join(out_folder, f"{base_name}_black.mrc")

        with mrcfile.new(path_white, overwrite=True) as mrc:
            mrc.set_data(data)
            mrc.voxel_size = apix

        with mrcfile.new(path_black, overwrite=True) as mrc:
            mrc.set_data(data * -1.0)
            mrc.voxel_size = apix

        return True, path_black
    except Exception as e:
        return False, str(e)
