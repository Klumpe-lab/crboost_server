# services/parameter_manager_v2.py
import glob
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from services.parameter_models import (
    PipelineState, ComputingParams, MicroscopeParams, 
    AcquisitionParams, ImportMoviesParams, FsMotionCtfParams,
    TsAlignmentParams
)

class ParameterManagerV2:
    """Clean parameter manager with clear responsibilities"""
    
    def __init__(self, config_path: Path = None):
        if config_path is None:
            config_path = Path("config/conf.yaml")
        
        # Initialize with computing params from config
        self.state = PipelineState(
            computing=ComputingParams.from_conf_yaml(config_path)
        )
        self.config_path = config_path
        print(f"[PARAMS-V2] Initialized with computing: {self.state.computing.dict()}")
    
    def update_from_mdoc(self, mdocs_glob: str):
        """Parse first mdoc and update relevant params"""
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            print(f"[WARN] No mdoc files found at: {mdocs_glob}")
            return
        
        try:
            mdoc_path = Path(mdoc_files[0])
            print(f"[PARAMS-V2] Parsing mdoc: {mdoc_path}")
            mdoc_data = self._parse_mdoc(mdoc_path)
            
            # Update microscope params
            if 'pixel_spacing' in mdoc_data:
                self.state.microscope.pixel_size_angstrom = mdoc_data['pixel_spacing']
            if 'voltage' in mdoc_data:
                self.state.microscope.acceleration_voltage_kv = mdoc_data['voltage']
            
            # Update acquisition params  
            if 'exposure_dose' in mdoc_data:
                # Scale up by 1.5x as per original logic
                dose = mdoc_data['exposure_dose'] * 1.5
                # Clamp to valid range
                dose = max(0.1, min(9.0, dose))
                self.state.acquisition.dose_per_tilt = dose
                
            if 'tilt_axis_angle' in mdoc_data:
                self.state.acquisition.tilt_axis_degrees = mdoc_data['tilt_axis_angle']
            
            # Parse detector dimensions
            if 'image_size' in mdoc_data:
                dims = mdoc_data['image_size'].split('x')
                if len(dims) == 2:
                    self.state.acquisition.detector_dimensions = (int(dims[0]), int(dims[1]))
                    
                    # Detect K3/EER based on dimensions
                    if "5760" in mdoc_data['image_size'] or "11520" in mdoc_data['image_size']:
                        self.state.acquisition.eer_fractions_per_frame = 32
                        print(f"[PARAMS-V2] Detected K3/EER camera, set fractions to 32")
            
            self.state.update_modified()
            print(f"[PARAMS-V2] Updated from mdoc: {len(mdoc_files)} files found")
                
        except Exception as e:
            print(f"[ERROR] Failed to parse mdoc {mdoc_files[0]}: {e}")
            import traceback
            traceback.print_exc()
    
    def update_parameter(self, param_path: str, value: Any):
        """Update parameter using dot notation (e.g., 'microscope.pixel_size_angstrom')"""
        parts = param_path.split('.')
        
        # Special handling for detector dimensions
        if param_path == 'acquisition.detector_dimensions' and isinstance(value, str):
            dims = value.split('x')
            if len(dims) == 2:
                value = (int(dims[0]), int(dims[1]))
        
        try:
            # Navigate to the parameter
            obj = self.state
            for part in parts[:-1]:
                if hasattr(obj, part):
                    obj = getattr(obj, part)
                elif hasattr(obj, 'jobs') and part in obj.jobs:
                    obj = obj.jobs[part]
                else:
                    raise ValueError(f"Invalid parameter path: {param_path} (couldn't find {part})")
            
            # Set the value
            if hasattr(obj, parts[-1]):
                setattr(obj, parts[-1], value)
                self.state.update_modified()
                print(f"[PARAMS-V2] Updated {param_path} = {value}")
            else:
                raise ValueError(f"Parameter {parts[-1]} not found in {param_path}")
                
        except Exception as e:
            print(f"[ERROR] Failed to update parameter {param_path}: {e}")
            raise
    
    def prepare_job_params(self, job_name: str, job_star_path: Optional[Path] = None) -> BaseModel:
        """Get params for a specific job, creating if needed"""
        if job_name not in self.state.jobs:
            self.state.populate_job(job_name, job_star_path)
        return self.state.jobs[job_name]
    
    def export_for_project(self, 
                          project_name: str, 
                          movies_glob: str,
                          mdocs_glob: str,
                          selected_jobs: List[str]) -> Dict[str, Any]:
        """Export clean configuration for project"""
        
        # Ensure all selected jobs have params
        for job in selected_jobs:
            if job not in self.state.jobs:
                # Try to load from template job.star if available
                template_path = Path("config/Schemes/warp_tomo_prep") / job / "job.star"
                self.state.populate_job(job, template_path if template_path.exists() else None)
        
        # Get containers from config if available
        containers = {}
        try:
            import yaml
            with open(self.config_path) as f:
                conf = yaml.safe_load(f)
                containers = conf.get('containers', {})
        except:
            pass
        
        export = {
            "metadata": {
                "config_version": "2.0",
                "created_by": "CryoBoost Parameter Manager V2",
                "created_at": datetime.now().isoformat(),
                "project_name": project_name
            },
            "data_sources": {
                "frames_glob": movies_glob,
                "mdocs_glob": mdocs_glob,
                "gain_reference": self.state.acquisition.gain_reference_path
            },
            "containers": containers,
            "microscope": self.state.microscope.dict(),
            "acquisition": self.state.acquisition.dict(),
            "computing": self.state.computing.dict(),
            "jobs": {
                job: self.state.jobs[job].dict() 
                for job in selected_jobs 
                if job in self.state.jobs
            }
        }
        
        return export
    
    def save_to_file(self, path: Path):
        """Save current state to JSON file"""
        try:
            state_dict = {
                "microscope": self.state.microscope.dict(),
                "acquisition": self.state.acquisition.dict(),
                "computing": self.state.computing.dict(),
                "jobs": {name: params.dict() for name, params in self.state.jobs.items()},
                "metadata": {
                    "created_at": self.state.created_at.isoformat(),
                    "modified_at": self.state.modified_at.isoformat()
                }
            }
            
            with open(path, 'w') as f:
                json.dump(state_dict, f, indent=2)
            
            print(f"[PARAMS-V2] Saved state to {path}")
            
        except Exception as e:
            print(f"[ERROR] Failed to save state to {path}: {e}")
    
    def load_from_file(self, path: Path):
        """Load state from JSON file"""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            # Update state
            if 'microscope' in data:
                self.state.microscope = MicroscopeParams(**data['microscope'])
            if 'acquisition' in data:
                self.state.acquisition = AcquisitionParams(**data['acquisition'])
            if 'computing' in data:
                self.state.computing = ComputingParams(**data['computing'])
            
            # Load jobs
            if 'jobs' in data:
                for job_name, job_data in data['jobs'].items():
                    if job_name == 'importmovies':
                        self.state.jobs[job_name] = ImportMoviesParams(**job_data)
                    elif job_name == 'fsMotionAndCtf':
                        self.state.jobs[job_name] = FsMotionCtfParams(**job_data)
                    elif job_name == 'tsAlignment':
                        self.state.jobs[job_name] = TsAlignmentParams(**job_data)
            
            print(f"[PARAMS-V2] Loaded state from {path}")
            
        except Exception as e:
            print(f"[ERROR] Failed to load state from {path}: {e}")
    
    def get_ui_state(self) -> Dict[str, Any]:
        """Get current state for UI display"""
        # Format for UI consumption
        ui_state = {
            # Flat parameters for backward compatibility with current UI
            'pixel_size_angstrom': {'value': self.state.microscope.pixel_size_angstrom, 'source': 'user'},
            'acceleration_voltage_kv': {'value': self.state.microscope.acceleration_voltage_kv, 'source': 'user'},
            'spherical_aberration_mm': {'value': self.state.microscope.spherical_aberration_mm, 'source': 'user'},
            'amplitude_contrast': {'value': self.state.microscope.amplitude_contrast, 'source': 'user'},
            'dose_per_tilt': {'value': self.state.acquisition.dose_per_tilt, 'source': 'user'},
            'detector_dimensions': {'value': self.state.acquisition.detector_dimensions, 'source': 'user'},
            'tilt_axis_degrees': {'value': self.state.acquisition.tilt_axis_degrees, 'source': 'user'},
            'eer_fractions_per_frame': {'value': self.state.acquisition.eer_fractions_per_frame, 'source': 'user'} if self.state.acquisition.eer_fractions_per_frame else None,
            
            # Also include hierarchical for new UI
            'microscope': self.state.microscope.dict(),
            'acquisition': self.state.acquisition.dict(),
            'computing': self.state.computing.dict(),
            'jobs': {name: params.dict() for name, params in self.state.jobs.items()}
        }
        
        # Remove None values
        return {k: v for k, v in ui_state.items() if v is not None}
    
    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
        """Parse mdoc file for key metadata"""
        result = {}
        header_data = {}
        first_section = {}
        in_zvalue_section = False
        
        with open(mdoc_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('[ZValue'):
                    in_zvalue_section = True
                elif in_zvalue_section and '=' in line:
                    key, value = [x.strip() for x in line.split('=', 1)]
                    first_section[key] = value
                elif not in_zvalue_section and '=' in line:
                    key, value = [x.strip() for x in line.split('=', 1)]
                    header_data[key] = value
        
        # Extract values, preferring header, falling back to first section
        if 'PixelSpacing' in header_data:
            result['pixel_spacing'] = float(header_data['PixelSpacing'])
        elif 'PixelSpacing' in first_section:
            result['pixel_spacing'] = float(first_section['PixelSpacing'])
        
        if 'Voltage' in header_data:
            result['voltage'] = float(header_data['Voltage'])
        elif 'Voltage' in first_section:
            result['voltage'] = float(first_section['Voltage'])
        
        if 'ImageSize' in header_data:
            result['image_size'] = header_data['ImageSize'].replace(' ', 'x')
        elif 'ImageSize' in first_section:
            result['image_size'] = first_section['ImageSize'].replace(' ', 'x')
        
        # ExposureDose often in sections
        if 'ExposureDose' in first_section:
            result['exposure_dose'] = float(first_section['ExposureDose'])
        elif 'ExposureDose' in header_data:
            result['exposure_dose'] = float(header_data['ExposureDose'])
        
        # TiltAxisAngle
        if 'TiltAxisAngle' in first_section:
            result['tilt_axis_angle'] = float(first_section['TiltAxisAngle'])
        elif 'Tilt axis angle' in header_data:
            result['tilt_axis_angle'] = float(header_data['Tilt axis angle'])
        
        return result
    
    # ===== LEGACY COMPATIBILITY =====
    def get_legacy_user_params_dict(self) -> Dict[str, Any]:
        """
        Adapter for backward compatibility with old pipeline orchestrator.
        This can be removed once pipeline orchestrator is updated.
        """
        
        # Ensure EER fractions has a valid value
        eer_fractions_value = 32
        if self.state.acquisition.eer_fractions_per_frame:
            eer_fractions_value = self.state.acquisition.eer_fractions_per_frame
            if eer_fractions_value <= 0:
                eer_fractions_value = 32
        
        return {
            # For _build_import_movies_command
            "nominal_tilt_axis_angle": str(self.state.acquisition.tilt_axis_degrees),
            "nominal_pixel_size": str(self.state.microscope.pixel_size_angstrom),
            "voltage": str(self.state.microscope.acceleration_voltage_kv),
            "spherical_aberration": str(self.state.microscope.spherical_aberration_mm),
            "amplitude_contrast": str(self.state.microscope.amplitude_contrast),
            "dose_per_tilt_image": str(self.state.acquisition.dose_per_tilt),
            
            # For _build_warp_fs_motion_ctf_command
            "angpix": str(self.state.microscope.pixel_size_angstrom),
            "cs": str(self.state.microscope.spherical_aberration_mm),
            "amplitude": str(self.state.microscope.amplitude_contrast),
            "eer_fractions": str(eer_fractions_value),
        }