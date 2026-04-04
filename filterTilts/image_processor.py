import numpy as np
import mrcfile
from pathlib import Path
from PIL import Image
from concurrent.futures import ProcessPoolExecutor
from scipy.fft import fft2, ifft2, fftshift, ifftshift
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


class ImageProcessor:
    """Handles MRC to PIL image conversion and preprocessing."""
    
    def __init__(self, target_size=384, ignore_non_square=False, max_workers=20):
        """
        Initialize image processor.
        
        Parameters:
        - target_size: Target size for resized images
        - ignore_non_square: Whether to skip non-square images (eg from K3 camera)
        - max_workers: Number of parallel workers
        """
        self.target_size = target_size
        self.ignore_non_square = ignore_non_square
        self.max_workers = max_workers
        
    def fourier_crop(self, image_array, new_shape):
        """
        Resize image using Fourier cropping to preserve high-frequency information.
        
        Parameters:
        - image_array: 2D numpy array
        - new_shape: Tuple (height, width) for output size
        
        Returns:
        - Resized numpy array
        """
        f_transform = fft2(image_array)
        f_transform_shifted = fftshift(f_transform)
        current_shape = f_transform_shifted.shape
        
        resized_f_transform_shifted = np.zeros(new_shape, dtype=f_transform_shifted.dtype)
        
        center_current = [dim // 2 for dim in current_shape]
        center_new = [dim // 2 for dim in new_shape]
        
        slices_current = [slice(center - min(center, new_center), 
                               center + min(center, new_center)) 
                         for center, new_center in zip(center_current, center_new)]
        slices_new = [slice(new_center - min(center, new_center), 
                           new_center + min(center, new_center)) 
                     for center, new_center in zip(center_current, center_new)]
        
        resized_f_transform_shifted[tuple(slices_new)] = f_transform_shifted[tuple(slices_current)]
        resized_f_transform = ifftshift(resized_f_transform_shifted)
        resized_image_array = ifft2(resized_f_transform).real
        
        return resized_image_array
    
    def mrc_to_pil(self, mrc_path, save_png_path=None):
        """
        Convert single MRC file to PIL Image.
        
        Parameters:
        - mrc_path: Path to MRC file
        - save_png_path: Optional path to save PNG
        
        Returns:
        - PIL Image object or None if skipped
        """
        try:
            with mrcfile.open(mrc_path, permissive=True) as mrc:
                data = mrc.data
                
                if self.ignore_non_square and data.shape[0] != data.shape[1]:
                    return None
                
                # Resize using Fourier cropping
                if data.shape != (self.target_size, self.target_size):
                    data = self.fourier_crop(data, (self.target_size, self.target_size))
                
                # Normalize to 0-255
                data = data - np.min(data)
                if np.max(data) > 0:
                    data = data / np.max(data) * 255
                data = data.astype(np.uint8)
                
                pil_image = Image.fromarray(data, mode='L')
                
                # Save PNG if path provided
                if save_png_path:
                    save_png_path = Path(save_png_path)
                    save_png_path.parent.mkdir(parents=True, exist_ok=True)
                    pil_image.save(save_png_path)
                
                return pil_image
                
        except Exception as e:
            print(f"Error processing {mrc_path}: {e}")
            return None
    
    def _convert_single_parallel(self, args):
        """Helper function for parallel processing.
        Packs arguments into a single tuple for ProcessPoolExecutor."""
        mrc_path, png_output_folder = args
        
        if png_output_folder:
            mrc_path_obj = Path(mrc_path)
            png_path = Path(png_output_folder) / f"{mrc_path_obj.stem}.png"
        else:
            png_path = None
            
        return self.mrc_to_pil(mrc_path, png_path)

    def batch_convert(self, mrc_paths, nr_images, png_output_folder=None, show_progress=True):
        """
        Convert multiple MRC files to PIL images in parallel.
        
        Parameters:
        - mrc_paths: List of paths to MRC files
        - png_output_folder: Optional folder to save PNGs
        - show_progress: Whether to show progress bar
        
        Returns:
        - List of PIL Image objects
        """
        if png_output_folder:
            Path(png_output_folder).mkdir(parents=True, exist_ok=True)
        
        import time
        start_time = time.time()

        print(f"Total images: {nr_images}")
        print(f"Workers: {self.max_workers}")
        print(f"Base load: {nr_images // self.max_workers} images per worker")
        if nr_images % self.max_workers > 0:
            print(f"Extra images: {nr_images % self.max_workers} workers get 1 additional image")
        args_list = [(path, png_output_folder) for path in mrc_paths]
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            if show_progress and tqdm is not None:
                pil_images = list(tqdm(
                    executor.map(self._convert_single_parallel, args_list),
                    total=len(args_list),
                    desc="Converting MRC to PIL"
                ))
            else:
                pil_images = list(executor.map(self._convert_single_parallel, args_list))
        
        # Filter out None values (skipped images)
        pil_images = [img for img in pil_images if img is not None]
        
        end_time = time.time()
        print(f"Batch conversion completed in {end_time - start_time:.2f} seconds")

        return pil_images