# services/setup_service.py
import yaml
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass

@dataclass
class MicroscopeParams:
    pixel_size: float
    voltage: float
    spherical_aberration: float = 2.7
    amplitude_contrast: float = 0.1
    dose_per_tilt: float = 3.0

@dataclass
class TiltSeriesParams:
    tilt_axis_angle: float
    image_size: str
    eer_grouping: int = 5
    invert_handedness: bool = False

@dataclass
class ReconstructionParams:
    reconstruction_pixel_size: float
    tomogram_size: str
    sample_thickness: float
    alignment_method: str = "AreTomo"
    patch_size: int = 800

class SetupService:
    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.setup_templates_dir = server_dir / "config" / "setup_templates"
        self.setup_templates_dir.mkdir(exist_ok=True)
    
    def load_microscope_presets(self) -> Dict[str, MicroscopeParams]:
        """Load predefined microscope configurations"""
        presets = {
            "Krios_G3": MicroscopeParams(
                pixel_size=1.35,
                voltage=300,
                spherical_aberration=2.7,
                amplitude_contrast=0.1
            ),
            "TFS_Glacios": MicroscopeParams(
                pixel_size=1.6,
                voltage=200, 
                spherical_aberration=2.7,
                amplitude_contrast=0.1
            ),
            "Custom": MicroscopeParams(
                pixel_size=1.35,
                voltage=300,
                spherical_aberration=2.7,
                amplitude_contrast=0.1
            )
        }
        return presets
    
    def calculate_binning_factor(self, acquisition_pixel_size: float, target_pixel_size: float) -> float:
        """Calculate binning factor for reconstruction"""
        return target_pixel_size / acquisition_pixel_size
    
    def suggest_reconstruction_params(self, acquisition_params: MicroscopeParams) -> ReconstructionParams:
        """Suggest reasonable reconstruction parameters based on acquisition"""
        binning_factor = 4.0  # Default 4x binning
        target_pixel_size = acquisition_params.pixel_size * binning_factor
        
        # Suggest tomogram size based on common dimensions
        # This would be more sophisticated in practice
        tomogram_size = "1024x1024x512"
        
        return ReconstructionParams(
            reconstruction_pixel_size=target_pixel_size,
            tomogram_size=tomogram_size,
            sample_thickness=300,  # nm, typical value
            alignment_method="AreTomo",
            patch_size=800
        )
    
    def save_setup_config(self, project_path: Path, config: Dict[str, Any]) -> bool:
        """Save setup configuration to project"""
        try:
            config_path = project_path / "setup_config.yaml"
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            return True
        except Exception as e:
            print(f"Error saving setup config: {e}")
            return False
    
    def load_setup_config(self, project_path: Path) -> Dict[str, Any]:
        """Load setup configuration from project"""
        try:
            config_path = project_path / "setup_config.yaml"
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
            return {}
        except Exception as e:
            print(f"Error loading setup config: {e}")
            return {}