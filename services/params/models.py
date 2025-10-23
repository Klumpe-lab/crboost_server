# models/parameter_models.py

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, Literal, Union, Tuple
from enum import Enum
from pathlib import Path

class ParameterSource(str, Enum):
    """Track where each parameter originated"""
    MDOC = "mdoc"
    CONFIG = "config"
    USER = "user"
    DEFAULT = "default"
    DERIVED = "derived"

class SourcedValue(BaseModel):
    """A value with its source tracked"""
    value: Any
    source: ParameterSource = ParameterSource.DEFAULT
    
    class Config:
        arbitrary_types_allowed = True

# ===== LAYER 1: RAW DATA SOURCES =====

class RawMdocData(BaseModel):
    """Exactly what we read from mdoc files, no interpretation"""
    pixel_spacing: float  # Angstroms
    voltage: float  # kV
    exposure_dose: float  # e-/A^2
    image_size_str: str  # e.g., "4096x4096"
    tilt_axis_angle: float  # degrees
    is_serialem: bool
    num_mdoc_files: int
    
    @property
    def image_dimensions(self) -> Tuple[int, int]:
        """Parse image size string to tuple"""
        parts = self.image_size_str.split('x')
        return (int(parts[0]), int(parts[1]))

# ===== LAYER 2: PIPELINE STATE =====

class PipelineState(BaseModel):
    """
    Canonical parameter state for entire pipeline.
    This is the single source of truth.
    """
    
    # Microscope parameters (from mdoc)
    pixel_size_angstrom: SourcedValue
    acceleration_voltage_kv: SourcedValue
    spherical_aberration_mm: SourcedValue
    amplitude_contrast: SourcedValue
    
    # Acquisition parameters (from mdoc)
    dose_per_tilt: SourcedValue  # e-/A^2 total per tilt
    detector_dimensions: SourcedValue  # (width, height)
    tilt_axis_degrees: SourcedValue
    
    # Processing parameters (user-configured with defaults)
    reconstruction_binning: SourcedValue
    sample_thickness_nm: SourcedValue
    eer_fractions_per_frame: Optional[SourcedValue] = None
    gain_reference_path: Optional[SourcedValue] = None
    invert_tilt_angles: SourcedValue
    invert_defocus_hand: SourcedValue
    
    # Computing defaults (from config or user)
    default_partition: SourcedValue
    default_gpu_count: SourcedValue
    default_memory_gb: SourcedValue
    default_threads: SourcedValue
    
    class Config:
        arbitrary_types_allowed = True
    
    def get_derived_value(self, key: str) -> Any:
        """Compute derived values on-demand instead of storing them"""
        if key == "reconstruction_pixel_size":
            return self.pixel_size_angstrom.value * self.reconstruction_binning.value
        elif key == "dose_rate":  # For RELION (dose per frame, not per tilt)
            # Assuming ~40 frames per tilt for typical collection
            return self.dose_per_tilt.value / 40.0
        elif key == "tomogram_dimensions":
            width, height = self.detector_dimensions.value
            return f"{width}x{height}x2048"  # Standard Z dimension
        else:
            raise KeyError(f"Unknown derived value: {key}")
    
    def update_value(self, key: str, value: Any, source: ParameterSource = ParameterSource.USER):
        """Update a parameter value and track its source"""
        if hasattr(self, key):
            field = getattr(self, key)
            if isinstance(field, SourcedValue):
                field.value = value
                field.source = source
            else:
                raise ValueError(f"Field {key} is not a SourcedValue")
        else:
            raise KeyError(f"Unknown parameter: {key}")

# ===== LAYER 3: JOB PARAMETERS =====

class JobParams(BaseModel):
    """Base class for job-specific parameter extraction"""
    job_type: str
    
    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        """Extract parameters in job-specific format"""
        raise NotImplementedError
    
    def extract_computing_params(self, state: PipelineState) -> Dict[str, str]:
        """Common computing parameter extraction"""
        return {
            "qsub_extra1": "1",  # nodes (always 1 for now)
            "qsub_extra2": "",   # mpi_per_node (let RELION handle)
            "qsub_extra3": state.default_partition.value,
            "qsub_extra4": str(state.default_gpu_count.value),
            "qsub_extra5": f"{state.default_memory_gb.value}G",
            "nr_threads": str(state.default_threads.value),
        }

class ImportMoviesParams(JobParams):
    """Parameters for RELION importmovies job"""
    job_type: Literal["importmovies"] = "importmovies"
    
    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        """Extract RELION-native import parameters"""
        params = {
            # Microscope parameters
            "angpix": str(state.pixel_size_angstrom.value),
            "kV": str(state.acceleration_voltage_kv.value),
            "Cs": str(state.spherical_aberration_mm.value),
            "Q0": str(state.amplitude_contrast.value),
            
            # Acquisition parameters
            "dose_rate": str(state.get_derived_value("dose_rate")),
            "tilt_axis_angle": str(state.tilt_axis_degrees.value),
            
            # Processing flags
            "flip_tiltseries_hand": "Yes" if state.invert_tilt_angles.value else "No",
            
            # File patterns (these would be set by the pipeline orchestrator)
            "fn_in_raw": "frames/*.eer",  # Will be updated by orchestrator
            "fn_in_motioncorr": "MotionCorr/job002/frames/*.mrc",  # Placeholder
        }
        
        # Add computing params (import doesn't need GPU)
        computing = self.extract_computing_params(state)
        computing["qsub_extra3"] = "c"  # CPU partition
        computing["qsub_extra4"] = "0"  # No GPU
        computing["qsub_extra5"] = "16G"  # Less memory needed
        computing["nr_threads"] = "4"
        
        params.update(computing)
        return params

class FsMotionAndCtfParams(JobParams):
    """Parameters for Warp's fsMotionAndCtf external job"""
    job_type: Literal["fsMotionAndCtf"] = "fsMotionAndCtf"
    
    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        """Extract Warp parameters in paramX_label/value format"""
        # Calculate EER fractions if needed
        eer_fractions = 32  # Default
        if state.eer_fractions_per_frame and state.eer_fractions_per_frame.value:
            eer_fractions = state.eer_fractions_per_frame.value
        
        # Build external parameters
        external_params = [
            ("eer_fractions", str(eer_fractions)),
            ("bin_factor", "2"),  # Standard binning for CTF estimation
            ("angpix", str(state.pixel_size_angstrom.value)),
            ("voltage", str(state.acceleration_voltage_kv.value)),
            ("Cs", str(state.spherical_aberration_mm.value)),
            ("amplitude", str(state.amplitude_contrast.value)),
            ("defocus_min", "5000"),  # Reasonable defaults for tomography
            ("defocus_max", "50000"),
        ]
        
        # Handle defocus handedness inversion
        if state.invert_defocus_hand.value:
            external_params.append(("flip_phases", "set_flip"))
        else:
            external_params.append(("flip_phases", "set_noflip"))
        
        # Convert to paramX format
        params = {}
        for i, (label, value) in enumerate(external_params, 1):
            params[f"param{i}_label"] = label
            params[f"param{i}_value"] = value
        
        # Add computing params (Warp needs GPU)
        computing = self.extract_computing_params(state)
        params.update(computing)
        
        return params

# ===== PARAMETER MANAGER SERVICE =====

class ParameterManager:
    """
    Orchestrates parameter flow through the pipeline.
    This replaces the monolithic ParameterService.
    """
    
    def __init__(self):
        self.state: Optional[PipelineState] = None
        self.job_params: Dict[str, JobParams] = {}
        self._register_job_params()
    
    def _register_job_params(self):
        """Register all job parameter extractors"""
        self.job_params = {
            "importmovies": ImportMoviesParams(),
            "fsMotionAndCtf": FsMotionAndCtfParams(),
        }
    
    def initialize_from_mdoc(self, mdoc_data: RawMdocData) -> PipelineState:
        """
        Create pipeline state from parsed mdoc data.
        This is the main initialization entry point.
        """
        # Calculate dose per tilt (mdoc gives per frame, we need total)
        # SerialEM typically uses ExposureDose * 1.5 as a heuristic
        dose_per_tilt = mdoc_data.exposure_dose * 1.5
        
        # Validate and clamp dose to reasonable range
        if dose_per_tilt < 0.1 or dose_per_tilt > 9.0:
            print(f"Warning: Dose per tilt {dose_per_tilt} out of range, defaulting to 3.0")
            dose_per_tilt = 3.0
        
        self.state = PipelineState(
            # Microscope parameters
            pixel_size_angstrom=SourcedValue(
                value=mdoc_data.pixel_spacing,
                source=ParameterSource.MDOC
            ),
            acceleration_voltage_kv=SourcedValue(
                value=mdoc_data.voltage,
                source=ParameterSource.MDOC
            ),
            spherical_aberration_mm=SourcedValue(
                value=2.7,  # Standard default
                source=ParameterSource.DEFAULT
            ),
            amplitude_contrast=SourcedValue(
                value=0.1,  # Standard default
                source=ParameterSource.DEFAULT
            ),
            
            # Acquisition parameters
            dose_per_tilt=SourcedValue(
                value=dose_per_tilt,
                source=ParameterSource.MDOC
            ),
            detector_dimensions=SourcedValue(
                value=mdoc_data.image_dimensions,
                source=ParameterSource.MDOC
            ),
            tilt_axis_degrees=SourcedValue(
                value=mdoc_data.tilt_axis_angle,
                source=ParameterSource.MDOC
            ),
            
            # Processing defaults
            reconstruction_binning=SourcedValue(
                value=4,  # Standard 4x binning
                source=ParameterSource.DEFAULT
            ),
            sample_thickness_nm=SourcedValue(
                value=300.0,
                source=ParameterSource.DEFAULT
            ),
            invert_tilt_angles=SourcedValue(
                value=False,
                source=ParameterSource.DEFAULT
            ),
            invert_defocus_hand=SourcedValue(
                value=False,
                source=ParameterSource.DEFAULT
            ),
            
            # Computing defaults
            default_partition=SourcedValue(
                value="g",  # GPU partition
                source=ParameterSource.DEFAULT
            ),
            default_gpu_count=SourcedValue(
                value=1,
                source=ParameterSource.DEFAULT
            ),
            default_memory_gb=SourcedValue(
                value=32,
                source=ParameterSource.DEFAULT
            ),
            default_threads=SourcedValue(
                value=8,
                source=ParameterSource.DEFAULT
            ),
        )
        
        # Set EER fractions if we detect EER data
        if mdoc_data.image_size_str and "K3" in str(mdoc_data.image_dimensions):
            self.state.eer_fractions_per_frame = SourcedValue(
                value=32,  # Standard for K3
                source=ParameterSource.DEFAULT
            )
        
        return self.state
    
    def get_job_parameters(self, job_type: str) -> Dict[str, Any]:
        """
        Get parameters formatted for a specific job.
        This is what gets written to job.star files.
        """
        if not self.state:
            raise ValueError("Pipeline state not initialized")
        
        if job_type not in self.job_params:
            raise ValueError(f"No parameter extractor for job type: {job_type}")
        
        extractor = self.job_params[job_type]
        return extractor.extract_params(self.state)
    
    def update_parameter(self, param_name: str, value: Any, source: ParameterSource = ParameterSource.USER):
        """
        Update a single parameter.
        No propagation needed - derived values are computed on-demand!
        """
        if not self.state:
            raise ValueError("Pipeline state not initialized")
        
        self.state.update_value(param_name, value, source)
    
    def batch_update_parameters(self, updates: Dict[str, Any], source: ParameterSource = ParameterSource.USER):
        """Update multiple parameters at once"""
        for param_name, value in updates.items():
            self.update_parameter(param_name, value, source)
    
    def get_parameter_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get a summary of all parameters with their sources"""
        if not self.state:
            return {}
        
        summary = {}
        for field_name, field_value in self.state.dict().items():
            if isinstance(field_value, dict) and 'value' in field_value:
                summary[field_name] = {
                    'value': field_value['value'],
                    'source': field_value['source'],
                }
        
        return summary

# ===== MDOC PARSER (separated from service) =====

class MdocParser:
    """Handles mdoc file parsing into raw data"""
    
    @staticmethod
    def parse_mdoc_files(mdoc_glob: str) -> RawMdocData:
        """Parse mdoc files and extract raw data"""
        import glob
        
        mdoc_files = glob.glob(mdoc_glob)
        if not mdoc_files:
            raise FileNotFoundError(f"No mdoc files found: {mdoc_glob}")
        
        # Parse first mdoc for metadata
        with open(mdoc_files[0], 'r') as f:
            content = f.read()
        
        # Extract values exactly as they appear
        pixel_spacing = float(content.split('PixelSpacing = ')[1].split('\n')[0])
        voltage = float(content.split('Voltage = ')[1].split('\n')[0]) if 'Voltage = ' in content else 300.0
        
        # Get exposure dose (per frame)
        exposure_dose = 1.0  # Default
        if 'ExposureDose = ' in content:
            exposure_dose = float(content.split('ExposureDose = ')[1].split('\n')[0])
        
        # Image size
        image_size = "4096x4096"  # Default
        if 'ImageSize = ' in content:
            image_size = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
        
        # Determine software and tilt axis
        is_serialem = 'SerialEM:' in content
        if is_serialem:
            tilt_axis = float(content.split('Tilt axis angle = ')[1].split(',')[0]) if 'Tilt axis angle = ' in content else -95.0
        else:
            # Tomo5 - parse from RotationAngle
            tilt_axis = 0.0
            for line in content.split('\n'):
                if 'RotationAngle = ' in line:
                    tilt_axis = abs(float(line.split('RotationAngle = ')[1]))
                    break
        
        return RawMdocData(
            pixel_spacing=pixel_spacing,
            voltage=voltage,
            exposure_dose=exposure_dose,
            image_size_str=image_size,
            tilt_axis_angle=tilt_axis,
            is_serialem=is_serialem,
            num_mdoc_files=len(mdoc_files)
        )