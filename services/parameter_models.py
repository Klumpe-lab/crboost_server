# services/parameter_models.py
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Tuple
from enum import Enum
from pathlib import Path
import pandas as pd
import yaml
import starfile
from datetime import datetime

# ============= ENUMS =============
class Partition(str, Enum):
    CPU = "c"
    GPU = "g"
    GPU_V100 = "g-v100"
    GPU_A100 = "g-a100"
    MEMORY = "m"

class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS = "Glacios"
    TALOS = "Talos"
    CUSTOM = "Custom"

class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD = "IMOD"
    RELION = "Relion"

# ============= CORE PARAMETER GROUPS =============
class MicroscopeParams(BaseModel):
    """Microscope-specific parameters"""
    microscope_type: MicroscopeType = MicroscopeType.CUSTOM
    pixel_size_angstrom: float = Field(default=1.35, ge=0.5, le=10.0)
    acceleration_voltage_kv: float = Field(default=300.0)
    spherical_aberration_mm: float = Field(default=2.7, ge=0.0, le=10.0)
    amplitude_contrast: float = Field(default=0.1, ge=0.0, le=1.0)
    
    @validator('acceleration_voltage_kv')
    def validate_voltage(cls, v):
        allowed = [200.0, 300.0]
        if v not in allowed:
            # Allow but warn
            print(f"[WARN] Voltage {v} not in standard values {allowed}")
        return v
    
class AcquisitionParams(BaseModel):
    """Data acquisition parameters"""
    dose_per_tilt: float = Field(default=3.0, ge=0.1, le=9.0)
    detector_dimensions: Tuple[int, int] = (4096, 4096)
    tilt_axis_degrees: float = Field(default=-95.0, ge=-180.0, le=180.0)
    eer_fractions_per_frame: Optional[int] = Field(default=None, ge=1, le=100)
    sample_thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    gain_reference_path: Optional[str] = None
    invert_tilt_angles: bool = False
    invert_defocus_hand: bool = False
    
class ComputingParams(BaseModel):
    """Computing resource parameters"""
    partition: Partition = Partition.GPU
    gpu_count: int = Field(default=1, ge=0, le=8)
    memory_gb: int = Field(default=32, ge=4, le=512)
    threads: int = Field(default=8, ge=1, le=128)
    
    @classmethod
    def from_conf_yaml(cls, config_path: Path) -> 'ComputingParams':
        """Extract computing params from conf.yaml"""
        if not config_path.exists():
            print(f"[WARN] Config not found at {config_path}, using defaults")
            return cls()
            
        try:
            with open(config_path) as f:
                conf = yaml.safe_load(f)
            
            computing = conf.get('computing', {})
            
            # Try different partition keys
            for partition_key in ['g', 'g-v100', 'g-a100', 'g-p100']:
                # Try both underscore and hyphen variants
                clean_key = partition_key.replace('-', '_')
                if clean_key in computing or partition_key in computing:
                    part_data = computing.get(clean_key) or computing.get(partition_key)
                    
                    # Extract values with defaults
                    gpu_count = part_data.get('NrGPU', 1)
                    ram_str = str(part_data.get('RAM', '32G'))
                    memory_gb = int(ram_str.replace('G', '').replace('g', ''))
                    threads = part_data.get('NrCPU', 8)
                    
                    return cls(
                        partition=Partition(partition_key),
                        gpu_count=gpu_count,
                        memory_gb=memory_gb,
                        threads=threads
                    )
            
            # Fallback to defaults
            return cls()
            
        except Exception as e:
            print(f"[ERROR] Failed to parse computing config: {e}")
            return cls()

# ============= JOB-SPECIFIC PARAMETER MODELS =============
class ImportMoviesParams(BaseModel):
    """Parameters for import movies job"""
    # From microscope
    pixel_size: float = Field(ge=0.5, le=10.0)
    voltage: float = Field(ge=50.0)
    spherical_aberration: float = Field(ge=0.0)
    amplitude_contrast: float = Field(ge=0.0, le=1.0)
    
    # From acquisition  
    dose_per_tilt_image: float = Field(ge=0.1)
    tilt_axis_angle: float = Field(ge=-180.0, le=180.0)
    
    # Job-specific
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)
    invert_defocus_hand: bool = False
    
    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional['ImportMoviesParams']:
        """Load defaults from job.star template if it exists"""
        if not star_path.exists():
            return None
        
        try:
            data = starfile.read(star_path, always_dict=True)
            
            # Look for job parameters block
            job_params = {}
            if 'job' in data:
                if isinstance(data['job'], dict):
                    job_params = data['job']
                else:
                    # Convert dataframe to dict if needed
                    job_params = data['job'].to_dict('records')[0] if len(data['job']) > 0 else {}
            
            # Parse with defaults
            return cls(
                pixel_size=float(job_params.get('nominal_pixel_size', 1.35)),
                voltage=float(job_params.get('voltage', 300)),
                spherical_aberration=float(job_params.get('spherical_aberration', 2.7)),
                amplitude_contrast=float(job_params.get('amplitude_contrast', 0.1)),
                dose_per_tilt_image=float(job_params.get('dose_per_tilt_image', 3.0)),
                tilt_axis_angle=float(job_params.get('nominal_tilt_axis_angle', -95.0)),
                optics_group_name=job_params.get('optics_group_name', 'opticsGroup1'),
                invert_defocus_hand=bool(job_params.get('invert_defocus_hand', False))
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

# services/parameter_models.py

# services/parameter_models.py

class FsMotionCtfParams(BaseModel):
    """Parameters for WarpTools motion correction and CTF"""
    # From microscope
    pixel_size: float = Field(ge=0.5, le=10.0)
    voltage: float = Field(ge=50.0)
    cs: float = Field(ge=0.0)
    amplitude: float = Field(ge=0.0, le=1.0)
    
    # EER specific
    eer_ngroups: int = Field(default=32, ge=1)
    
    # Motion correction parameters (from job.star)
    m_range_min_max: str = "500:10"  # Format: "min:max"
    m_bfac: int = Field(default=-500)
    m_grid: str = "1x1x3"
    
    # CTF parameters  
    c_range_min_max: str = "30:6.0"  # Format: "min:max"
    c_defocus_min_max: str = "1.1:8"  # Format: "min:max" (in microns!)
    c_grid: str = "2x2x1"
    c_window: int = Field(default=512, ge=128)
    
    # Processing control
    perdevice: int = Field(default=1, ge=0, le=8)
    
    # Output control
    do_at_most: int = Field(default=-1)
    
    # Optional gain reference
    gain_path: Optional[str] = None
    gain_operations: Optional[str] = None
    
    # Helper properties to parse range strings
    @property
    def m_range_min(self) -> int:
        return int(self.m_range_min_max.split(':')[0])
    
    @property
    def m_range_max(self) -> int:
        return int(self.m_range_min_max.split(':')[1])
    
    @property
    def c_range_min(self) -> float:
        return float(self.c_range_min_max.split(':')[0])
    
    @property
    def c_range_max(self) -> float:
        return float(self.c_range_min_max.split(':')[1])
    
    @property
    def defocus_min_microns(self) -> float:
        return float(self.c_defocus_min_max.split(':')[0])
    
    @property
    def defocus_max_microns(self) -> float:
        return float(self.c_defocus_min_max.split(':')[1])
    
    # Convert microns to Angstroms for WarpTools
    @property
    def defocus_min_angstroms(self) -> float:
        return self.defocus_min_microns * 10000.0
    
    @property
    def defocus_max_angstroms(self) -> float:
        return self.defocus_max_microns * 10000.0
    
    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional['FsMotionCtfParams']:
        """Load from job.star template"""
        if not star_path.exists():
            return None
            
        try:
            data = starfile.read(star_path, always_dict=True)
            job_params = {}
            if 'joboptions_values' in data:
                # Extract parameters from the joboptions_values table
                df = data['joboptions_values']
                param_dict = pd.Series(
                    df.rlnJobOptionValue.values, 
                    index=df.rlnJobOptionVariable
                ).to_dict()
                
                # Return an INSTANCE of FsMotionCtfParams, not a dict
                return cls(
                    # These will be overridden by global state, but provide defaults
                    pixel_size=1.35,
                    voltage=300.0,  
                    cs=2.7,
                    amplitude=0.1,
                    eer_ngroups=int(param_dict.get('param1_value', 32)),
                    gain_path=param_dict.get('param2_value', None),
                    gain_operations=param_dict.get('param3_value', None),
                    m_range_min_max=param_dict.get('param4_value', '500:10'),
                    m_bfac=int(param_dict.get('param5_value', -500)),
                    m_grid=param_dict.get('param6_value', '1x1x3'),
                    c_range_min_max=param_dict.get('param7_value', '30:6.0'),
                    c_defocus_min_max=param_dict.get('param8_value', '1.1:8'),
                    c_grid=param_dict.get('param9_value', '2x2x1'),
                    perdevice=int(param_dict.get('param10_value', 1)),
                    c_window=512
                )
                
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

class TsAlignmentParams(BaseModel):
    """Parameters for tilt series alignment"""
    alignment_method: AlignmentMethod = AlignmentMethod.ARETOMO
    binning: int = Field(default=4, ge=1, le=16)
    thickness_nm: float = Field(default=300.0, ge=50.0, le=2000.0)
    do_at_most: int = Field(default=-1)
    
    # AreTomo specific
    tilt_cor: int = Field(default=1)  # 0=no correction, 1=correct
    out_imod: int = Field(default=0)  # 0=no IMOD files, 1=generate
    patch_x: int = Field(default=5, ge=1)
    patch_y: int = Field(default=5, ge=1)
    
    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional['TsAlignmentParams']:
        """Load from job.star template"""
        if not star_path.exists():
            return None
            
        try:
            data = starfile.read(star_path, always_dict=True)
            job_params = {}
            if 'job' in data:
                if isinstance(data['job'], dict):
                    job_params = data['job']
                else:
                    job_params = data['job'].to_dict('records')[0] if len(data['job']) > 0 else {}
                    
            method_str = job_params.get('alignment_method', 'AreTomo')
            # Convert string to enum safely
            try:
                method = AlignmentMethod(method_str)
            except ValueError:
                method = AlignmentMethod.ARETOMO
            
            return cls(
                alignment_method=method,
                binning=int(job_params.get('binning', 4)),
                thickness_nm=float(job_params.get('thickness', 300.0)),
                tilt_cor=int(job_params.get('tilt_cor', 1)),
                out_imod=int(job_params.get('out_imod', 0)),
                patch_x=int(job_params.get('patch_x', 5)),
                patch_y=int(job_params.get('patch_y', 5))
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None

# ============= MAIN PIPELINE STATE =============
class PipelineState(BaseModel):
    """Central state with hierarchical organization"""
    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams = Field(default_factory=AcquisitionParams)
    computing: ComputingParams = Field(default_factory=ComputingParams)
    jobs: Dict[str, BaseModel] = Field(default_factory=dict)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    modified_at: datetime = Field(default_factory=datetime.now)
    
    def populate_job(self, job_name: str, job_star_path: Optional[Path] = None):
        """Create job params from current state and optional job.star"""
        print(f"[PIPELINE STATE DEBUG] Populating job {job_name} with pixel_size={self.microscope.pixel_size_angstrom}")

        # First try to load from job.star if provided
        job_params = None
        if job_star_path and job_star_path.exists():
            if job_name == 'importmovies':
                job_params = ImportMoviesParams.from_job_star(job_star_path)
            elif job_name == 'fsMotionAndCtf':
                job_params = FsMotionCtfParams.from_job_star(job_star_path)
            elif job_name == 'tsAlignment':
                job_params = TsAlignmentParams.from_job_star(job_star_path)
        
        # If no job.star or failed to load, create from current state
        if job_params is None:

            if job_name == 'importmovies':
                job_params = ImportMoviesParams(
                    pixel_size=self.microscope.pixel_size_angstrom,
                    voltage=self.microscope.acceleration_voltage_kv,
                    spherical_aberration=self.microscope.spherical_aberration_mm,
                    amplitude_contrast=self.microscope.amplitude_contrast,
                    dose_per_tilt_image=self.acquisition.dose_per_tilt,
                    tilt_axis_angle=self.acquisition.tilt_axis_degrees,
                    invert_defocus_hand=self.acquisition.invert_defocus_hand
                )
            elif job_name == 'fsMotionAndCtf':
                job_params = None
                if job_star_path and job_star_path.exists():
                    job_params = FsMotionCtfParams.from_job_star(job_star_path)
                    print(f"[PIPELINE STATE DEBUG] Loaded fsMotionAndCtf from job.star")
                
                # If no job.star or failed to load, create new with current global values
                if job_params is None:
                    job_params = FsMotionCtfParams(
                        pixel_size=self.microscope.pixel_size_angstrom,
                        voltage=self.microscope.acceleration_voltage_kv,
                        cs=self.microscope.spherical_aberration_mm,
                        amplitude=self.microscope.amplitude_contrast,
                        eer_ngroups=self.acquisition.eer_fractions_per_frame or 32
                    )
                    print(f"[PIPELINE STATE DEBUG] Created new fsMotionAndCtf with current global state")
                else:
                    # Update the loaded params with current global values
                    job_params.pixel_size = self.microscope.pixel_size_angstrom
                    job_params.voltage = self.microscope.acceleration_voltage_kv
                    job_params.cs = self.microscope.spherical_aberration_mm
                    job_params.amplitude = self.microscope.amplitude_contrast
                    job_params.eer_ngroups = self.acquisition.eer_fractions_per_frame or 32
                    print(f"[PIPELINE STATE DEBUG] Updated fsMotionAndCtf with current global state")
                
                self.jobs[job_name] = job_params
                    
        
            elif job_name == 'tsAlignment':
                job_params = TsAlignmentParams(
                    thickness_nm=self.acquisition.sample_thickness_nm
                )
            else:
                print(f"[WARN] Unknown job type: {job_name}")
                return
        
        self.jobs[job_name] = job_params
        self.modified_at = datetime.now()
    
    def update_modified(self):
        """Update the modified timestamp"""
        self.modified_at = datetime.now()