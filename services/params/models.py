# services/params/models.py

from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional, Dict, Any, Literal, Union, Tuple, TypeVar, Generic, List
from enum import Enum
from pathlib import Path

T = TypeVar('T')

class Parameter(BaseModel, Generic[T]):
    """
    A strongly-typed parameter with validation constraints.
    This is our basic building block.
    """
    value: T
    min_value: Optional[T] = None
    max_value: Optional[T] = None
    choices: Optional[List[T]] = None
    description: Optional[str] = None
    source: Optional[str] = None  
    
    @validator('value')
    def validate_constraints(cls, v, values):
        """Validate value against constraints"""
        if 'min_value' in values and values['min_value'] is not None:
            if v < values['min_value']:
                raise ValueError(f"Value {v} below minimum {values['min_value']}")
        
        if 'max_value' in values and values['max_value'] is not None:
            if v > values['max_value']:
                raise ValueError(f"Value {v} above maximum {values['max_value']}")
        
        # Check choices
        if 'choices' in values and values['choices'] is not None:
            if v not in values['choices']:
                raise ValueError(f"Value {v} not in allowed choices: {values['choices']}")
        
        return v
    
    class Config:
        arbitrary_types_allowed = True

FloatParam = Parameter[float]
IntParam = Parameter[int]
StrParam = Parameter[str]
BoolParam = Parameter[bool]
PathParam = Parameter[Optional[Path]]

class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    KRIOS_G4 = "Krios_G4"
    GLACIOS = "Glacios"
    TALOS = "Talos"
    CUSTOM = "Custom"

class Partition(str, Enum):
    CPU = "c"
    GPU = "g"
    GPU_V100 = "g-v100"
    GPU_A100 = "g-a100"
    MEMORY = "m"

class AlignmentMethod(str, Enum):
    ARETOMO = "AreTomo"
    IMOD = "IMOD"
    RELION = "Relion"

class RawMdocData(BaseModel):
    """Exactly what we read from mdoc files"""
    pixel_spacing: float
    voltage: float
    exposure_dose: float
    image_size_str: str
    tilt_axis_angle: float
    is_serialem: bool
    num_mdoc_files: int
    
    @property
    def image_dimensions(self) -> Tuple[int, int]:
        parts = self.image_size_str.split('x')
        return (int(parts[0]), int(parts[1]))

class PipelineState(BaseModel):
    """
    Central parameter state with strongly-typed, validated parameters.
    """
    
    # ===== Microscope Parameters =====
    microscope_type: Parameter[MicroscopeType] = Field(
        default_factory=lambda: Parameter[MicroscopeType](
            value=MicroscopeType.CUSTOM,
            choices=list(MicroscopeType),
            description="Type of microscope"
        )
    )
    
    pixel_size_angstrom: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=1.35,
            min_value=0.5,
            max_value=10.0,
            description="Pixel size in Angstroms"
        )
    )
    
    acceleration_voltage_kv: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=300.0,
            choices=[200.0, 300.0],
            description="Acceleration voltage in kV"
        )
    )
    
    spherical_aberration_mm: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=2.7,
            min_value=0.0,
            max_value=10.0,
            description="Spherical aberration Cs in mm"
        )
    )
    
    amplitude_contrast: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=0.1,
            min_value=0.0,
            max_value=1.0,
            description="Amplitude contrast ratio"
        )
    )
    
    # ===== Acquisition Parameters =====
    dose_per_tilt: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=3.0,
            min_value=0.1,
            max_value=9.0,
            description="Total dose per tilt in e-/A^2"
        )
    )
    
    detector_dimensions: Parameter[Tuple[int, int]] = Field(
        default_factory=lambda: Parameter[Tuple[int, int]](
            value=(4096, 4096),
            description="Detector dimensions (width, height)"
        )
    )
    
    tilt_axis_degrees: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=-95.0,
            min_value=-180.0,
            max_value=180.0,
            description="Tilt axis angle in degrees"
        )
    )
    
    # ===== Processing Parameters =====
    reconstruction_binning: IntParam = Field(
        default_factory=lambda: IntParam(
            value=4,
            min_value=1,
            max_value=16,
            description="Binning factor for reconstruction"
        )
    )
    
    sample_thickness_nm: FloatParam = Field(
        default_factory=lambda: FloatParam(
            value=300.0,
            min_value=50.0,
            max_value=2000.0,
            description="Sample thickness in nanometers"
        )
    )
    
    eer_fractions_per_frame: Optional[IntParam] = Field(
        default=None,
        description="EER fractions per rendered frame (if applicable)"
    )
    
    gain_reference_path: Optional[PathParam] = Field(
        default=None,
        description="Path to gain reference file"
    )
    
    invert_tilt_angles: BoolParam = Field(
        default_factory=lambda: BoolParam(
            value=False,
            description="Invert tilt series handedness"
        )
    )
    
    invert_defocus_hand: BoolParam = Field(
        default_factory=lambda: BoolParam(
            value=False,
            description="Invert defocus handedness"
        )
    )
    
    alignment_method: Parameter[AlignmentMethod] = Field(
        default_factory=lambda: Parameter[AlignmentMethod](
            value=AlignmentMethod.ARETOMO,
            choices=list(AlignmentMethod),
            description="Tilt series alignment method"
        )
    )
    
    # ===== Computing Resources =====
    default_partition: Parameter[Partition] = Field(
        default_factory=lambda: Parameter[Partition](
            value=Partition.GPU,
            choices=list(Partition),
            description="Default compute partition"
        )
    )
    
    default_gpu_count: IntParam = Field(
        default_factory=lambda: IntParam(
            value=1,
            min_value=0,
            max_value=8,
            description="Number of GPUs"
        )
    )
    
    default_memory_gb: IntParam = Field(
        default_factory=lambda: IntParam(
            value=32,
            min_value=4,
            max_value=512,
            description="Memory allocation in GB"
        )
    )
    
    default_threads: IntParam = Field(
        default_factory=lambda: IntParam(
            value=8,
            min_value=1,
            max_value=128,
            description="Number of CPU threads"
        )
    )
    
    # ===== Derived Value Computation =====
    def get_derived_value(self, key: str) -> Any:
        """Compute derived values on-demand"""
        if key == "reconstruction_pixel_size":
            return self.pixel_size_angstrom.value * self.reconstruction_binning.value
        elif key == "dose_rate":
            # For RELION: dose per frame (assuming 40 frames/tilt)
            return self.dose_per_tilt.value / 40.0
        elif key == "tomogram_dimensions":
            width, height = self.detector_dimensions.value
            return f"{width}x{height}x2048"
        else:
            raise KeyError(f"Unknown derived value: {key}")
    
    def update_parameter(self, param_name: str, value: Any):
        """Update a parameter value with validation"""
        if hasattr(self, param_name):
            param = getattr(self, param_name)
            if isinstance(param, Parameter):
                # Create new parameter with same constraints but new value
                param_class = type(param)
                updated_param = param_class(
                    value=value,
                    min_value=param.min_value,
                    max_value=param.max_value,
                    choices=param.choices,
                    description=param.description
                )
                setattr(self, param_name, updated_param)
            else:
                raise ValueError(f"{param_name} is not a Parameter")
        else:
            raise KeyError(f"Unknown parameter: {param_name}")
    
    def validate_all(self) -> Dict[str, Any]:
        """Validate all parameters and return any issues"""
        issues = {"errors": [], "warnings": []}
        
        # Check for unusual pixel size
        if self.pixel_size_angstrom.value < 0.8 or self.pixel_size_angstrom.value > 5.0:
            issues["warnings"].append(
                f"Pixel size {self.pixel_size_angstrom.value} Å is unusual for cryo-EM"
            )
        
        # Check dose
        if self.dose_per_tilt.value > 6.0:
            issues["warnings"].append(
                f"Dose per tilt {self.dose_per_tilt.value} e-/Å² is quite high"
            )
        
        # Check binning vs pixel size
        final_pixel = self.get_derived_value("reconstruction_pixel_size")
        if final_pixel > 20.0:
            issues["warnings"].append(
                f"Reconstruction pixel size {final_pixel} Å is very large"
            )
        
        return issues

class JobParams(BaseModel):
    """Base class for job-specific parameter extraction"""
    job_type: str
    
    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        raise NotImplementedError
    
    def extract_computing_params(self, state: PipelineState) -> Dict[str, str]:
        """Extract computing parameters with type safety"""
        return {
            "qsub_extra1": "1",  # nodes
            "qsub_extra2": "",   # mpi_per_node
            "qsub_extra3": state.default_partition.value.value,  # Partition enum value
            "qsub_extra4": str(state.default_gpu_count.value),
            "qsub_extra5": f"{state.default_memory_gb.value}G",
            "nr_threads": str(state.default_threads.value),
        }

class ImportMoviesParams(JobParams):
    job_type: Literal["importmovies"] = "importmovies"
    
    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        params = {
            "angpix": str(state.pixel_size_angstrom.value),
            "kV": str(state.acceleration_voltage_kv.value),
            "Cs": str(state.spherical_aberration_mm.value),
            "Q0": str(state.amplitude_contrast.value),
            "dose_rate": str(state.get_derived_value("dose_rate")),
            "tilt_axis_angle": str(state.tilt_axis_degrees.value),
            "flip_tiltseries_hand": "Yes" if state.invert_tilt_angles.value else "No",
            "fn_in_raw": "frames/*.eer",
        }
        
        computing = self.extract_computing_params(state)
        computing["qsub_extra3"] = Partition.CPU.value  # Override to CPU
        computing["qsub_extra4"] = "0"  # No GPU
        computing["qsub_extra5"] = "16G"
        computing["nr_threads"] = "4"
        
        params.update(computing)
        return params

class FsMotionAndCtfParams(JobParams):
    job_type: Literal["fsMotionAndCtf"] = "fsMotionAndCtf"
    

    def extract_params(self, state: PipelineState) -> Dict[str, Any]:
        eer_fractions = 32  # Default
        if state.eer_fractions_per_frame and state.eer_fractions_per_frame.value:
            eer_fractions = state.eer_fractions_per_frame.value
        
        external_params = [
            ("eer_fractions", str(eer_fractions)),
            ("bin_factor", "2"),
            ("angpix", str(state.pixel_size_angstrom.value)),
            ("voltage", str(state.acceleration_voltage_kv.value)),
            ("Cs", str(state.spherical_aberration_mm.value)),
            ("amplitude", str(state.amplitude_contrast.value)),
            ("defocus_min", "5000"),
            ("defocus_max", "50000"),
            ("flip_phases", "set_flip" if state.invert_defocus_hand.value else "set_noflip"),
        ]
        
        params = {}
        for i, (label, value) in enumerate(external_params, 1):
            params[f"param{i}_label"] = label
            params[f"param{i}_value"] = value
        
        params.update(self.extract_computing_params(state))
        return params

#TODO:... other jobs


# ===== PARAMETER MANAGER =====

class ParameterManager:
    """Manages pipeline parameters with type safety and validation"""
    
    def __init__(self):
        self.state: Optional[PipelineState] = None
        self.job_params: Dict[str, JobParams] = {
            "importmovies": ImportMoviesParams(),
            "fsMotionAndCtf": FsMotionAndCtfParams(),
        }
    
    def initialize_from_mdoc(self, mdoc_data: RawMdocData) -> PipelineState:
        """Initialize with validated, typed parameters from mdoc"""
        
        # Calculate and validate dose per tilt
        dose_per_tilt = mdoc_data.exposure_dose * 1.5
        dose_per_tilt = max(0.1, min(9.0, dose_per_tilt))  # Clamp to valid range
        
        self.state = PipelineState()
        
        # Update with mdoc values (with validation)
        self.state.pixel_size_angstrom.value = mdoc_data.pixel_spacing
        self.state.pixel_size_angstrom.source = "mdoc"
        
        self.state.acceleration_voltage_kv.value = mdoc_data.voltage
        self.state.acceleration_voltage_kv.source = "mdoc"
        
        self.state.dose_per_tilt.value = dose_per_tilt
        self.state.dose_per_tilt.source = "mdoc"
        
        self.state.detector_dimensions.value = mdoc_data.image_dimensions
        self.state.detector_dimensions.source = "mdoc"
        
        self.state.tilt_axis_degrees.value = mdoc_data.tilt_axis_angle
        self.state.tilt_axis_degrees.source = "mdoc"
        
        # Set EER if K3 detected
        if "5760" in mdoc_data.image_size_str or "11520" in mdoc_data.image_size_str:
            self.state.eer_fractions_per_frame = IntParam(
                value=32,
                min_value=1,
                max_value=100,
                description="EER fractions for K3"
            )
        
        # Validate everything
        issues = self.state.validate_all()
        if issues["errors"]:
            print(f"Parameter errors: {issues['errors']}")
        if issues["warnings"]:
            print(f"Parameter warnings: {issues['warnings']}")
        
        return self.state
    
    def get_job_parameters(self, job_type: str) -> Dict[str, Any]:
        """Get validated parameters for a specific job"""
        if not self.state:
            raise ValueError("Pipeline state not initialized")
        
        if job_type not in self.job_params:
            raise ValueError(f"Unknown job type: {job_type}")
        
        return self.job_params[job_type].extract_params(self.state)
    
    def update_parameter(self, param_name: str, value: Any):
        """Update parameter with automatic validation"""
        if not self.state:
            raise ValueError("Pipeline state not initialized")
        
        self.state.update_parameter(param_name, value)