# services/parameters_service.py

import glob
import json
from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional, Dict, Any, Literal, Union, Tuple, TypeVar, Generic, List
from enum import Enum
from pathlib import Path
from functools import lru_cache

# --- Service Imports ---
from services.config_service import get_config_service, Config

# Note: We are NOT using JobParams/ImportMoviesParams for now
# to respect the constraint of not changing the pipeline_orchestrator's
# command builders. We will use `get_legacy_user_params_dict` instead.


T = TypeVar('T')

# (Parameter, FloatParam, IntParam, etc. classes remain IDENTICAL to your draft)
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

# (Enums: MicroscopeType, Partition, AlignmentMethod remain IDENTICAL)
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

# (RawMdocData class remains IDENTICAL)
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

# (PipelineState class remains IDENTICAL)
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
                
                # Handle path conversion
                if param_class == PathParam and value is not None:
                    value = Path(value)
                
                updated_param = param_class(
                    value=value,
                    min_value=param.min_value,
                    max_value=param.max_value,
                    choices=param.choices,
                    description=param.description
                )
                setattr(self, param_name, updated_param)
            # Handle optional parameters being set to None
            elif param is None and value is not None:
                # This is for setting an Optional[IntParam] like eer_fractions
                # We need to find its type hint
                field_type = self.__annotations__.get(param_name)
                if field_type and hasattr(field_type, '__args__'):
                    # Assumes Optional[ParamType]
                    param_type = field_type.__args__[0]
                    if issubclass(param_type, Parameter):
                         setattr(self, param_name, param_type(value=value))
            
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

# (JobParams, ImportMoviesParams, FsMotionAndCtfParams classes are removed for now)
# (We will add them back when we refactor the pipeline_orchestrator)


# ===== PARAMETER MANAGER (NEW / HEAVILY MODIFIED) =====

class ParameterManager:
    """Manages pipeline parameters with type safety and validation"""
    
    def __init__(self):
        self.state: Optional[PipelineState] = None
        self.config_service = get_config_service()
        self._initialize_state_from_config(self.config_service.get_config())

    def _initialize_state_from_config(self, config: Config):
        """Initialize the default PipelineState from conf.yaml"""
        self.state = PipelineState()
        
        # Apply computing defaults
        try:
            # Try to find the first defined GPU partition for defaults
            gpu_part_name = next(p for p in ['g', 'g_v100', 'g_a100'] if getattr(config.computing, p, None))
            gpu_part = getattr(config.computing, gpu_part_name)
            
            self.state.update_parameter("default_partition", Partition(gpu_part_name.replace('_', '-')))
            self.state.update_parameter("default_gpu_count", gpu_part.NrGPU)
            self.state.update_parameter("default_memory_gb", int(gpu_part.RAM.replace('G', '')))
            self.state.update_parameter("default_threads", gpu_part.NrCPU)
        except (StopIteration, AttributeError, ValueError) as e:
            print(f"[WARN] Could not parse default computing config from conf.yaml: {e}. Using model defaults.")

        # Apply microscope defaults (e.g., from 'Krios_G3')
        try:
            # We'll just load the defaults from the model, but this is where
            # you could load a specific microscope preset from config.microscopes
            pass 
        except Exception as e:
            print(f"[WARN] Could not parse microscope defaults from conf.yaml: {e}")

    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
        """
        Parses an .mdoc file into a header string and a list of data dictionaries.
        (Logic lifted from data_import_service.py)
        """
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False
        header_data = {}

        with open(mdoc_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('[ZValue'):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {'ZValue': line.split('=')[1].strip().strip(']')}
                    in_zvalue_section = True
                elif in_zvalue_section and '=' in line:
                    key, value = [x.strip() for x in line.split('=', 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)
                    if '=' in line:
                        key, value = [x.strip() for x in line.split('=', 1)]
                        header_data[key] = value

        if current_section:
            data_sections.append(current_section)

        return {'header': "\n".join(header_lines), 'header_data': header_data, 'data': data_sections}

    def autodetect_from_mdoc(self, mdocs_glob: str) -> Optional[PipelineState]:
        """
        Finds the first mdoc file, parses it, and updates the state.
        Returns the updated state.
        """
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            print(f"[WARN] No mdoc files found at: {mdocs_glob}")
            return self.state

        try:
            first_mdoc_path = Path(mdoc_files[0])
            parsed_mdoc = self._parse_mdoc(first_mdoc_path)
            
            header = parsed_mdoc['header_data']
            first_section = parsed_mdoc['data'][0] if parsed_mdoc['data'] else {}

            # Extract values, preferring header, falling back to first section
            pixel_spacing = float(header.get('PixelSpacing', first_section.get('PixelSpacing', 1.0)))
            voltage = float(header.get('Voltage', first_section.get('Voltage', 300)))
            image_size_str = header.get('ImageSize', first_section.get('ImageSize', '4096x4096')).replace(' ', 'x')
            
            # ExposureDose and TiltAxisAngle are often in the sections
            exposure_dose = float(first_section.get('ExposureDose', header.get('ExposureDose', 3.0)))
            tilt_axis_angle = float(first_section.get('TiltAxisAngle', header.get('Tilt axis angle', -95.0)))

            raw_data = RawMdocData(
                pixel_spacing=pixel_spacing,
                voltage=voltage,
                exposure_dose=exposure_dose,
                image_size_str=image_size_str,
                tilt_axis_angle=tilt_axis_angle,
                is_serialem="SerialEM" in parsed_mdoc['header'],
                num_mdoc_files=len(mdoc_files)
            )

            # Use the existing initializer from your draft
            self.initialize_from_mdoc(raw_data)
            return self.state

        except Exception as e:
            print(f"[ERROR] Failed to parse mdoc {mdoc_files[0]}: {e}")
            return self.state

    def initialize_from_mdoc(self, mdoc_data: RawMdocData) -> PipelineState:
        """Initialize with validated, typed parameters from mdoc"""
        
        # Calculate and validate dose per tilt
        # (This logic was in your draft and is good)
        dose_per_tilt = mdoc_data.exposure_dose * 1.5
        dose_per_tilt = max(0.1, min(9.0, dose_per_tilt))  # Clamp to valid range
        
        if not self.state:
            self.state = PipelineState()
        
        # Update with mdoc values (with validation)
        self.state.update_parameter("pixel_size_angstrom", mdoc_data.pixel_spacing)
        self.state.pixel_size_angstrom.source = "mdoc"
        
        self.state.update_parameter("acceleration_voltage_kv", mdoc_data.voltage)
        self.state.acceleration_voltage_kv.source = "mdoc"
        
        self.state.update_parameter("dose_per_tilt", dose_per_tilt)
        self.state.dose_per_tilt.source = "mdoc"
        
        self.state.update_parameter("detector_dimensions", mdoc_data.image_dimensions)
        self.state.detector_dimensions.source = "mdoc"
        
        self.state.update_parameter("tilt_axis_degrees", mdoc_data.tilt_axis_angle)
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

    def get_state_as_dict(self) -> Dict[str, Any]:
        """Return the current state as a serializable dict"""
        if not self.state:
            self._initialize_state_from_config(self.config_service.get_config())
        return json.loads(self.state.json()) # Use json to handle complex types

    def update_parameter_from_ui(self, param_name: str, value: Any):
        """Update a single parameter from the UI, with validation"""
        if not self.state:
            self.state = PipelineState()
        
        try:
            self.state.update_parameter(param_name, value)
        except Exception as e:
            print(f"[ERROR] Failed to update parameter {param_name} with value {value}: {e}")
            # Optionally re-raise or return an error status

    def save_state_to_json(self, path: Path):
        """Save the current PipelineState to a JSON file"""
        if self.state:
            try:
                path.write_text(self.state.json(indent=2))
                print(f"[PARAMS] Saved state to {path}")
            except Exception as e:
                print(f"[ERROR] Failed to save state to {path}: {e}")
        else:
            print("[WARN] No state to save.")
            
    def get_legacy_user_params_dict(self) -> Dict[str, Any]:
        """
        *** ADAPTER METHOD ***
        This is the Layer 3 adapter that translates the new PipelineState
        into the old `user_params` dict that pipeline_orchestrator expects.
        This respects the constraint of not changing the command builders.
        """
        if not self.state:
            self.state = PipelineState()
            
        s = self.state
        
        return {
            # For _build_import_movies_command
            "nominal_tilt_axis_angle": str(s.tilt_axis_degrees.value),
            "nominal_pixel_size": str(s.pixel_size_angstrom.value),
            "voltage": str(s.acceleration_voltage_kv.value),
            "spherical_aberration": str(s.spherical_aberration_mm.value),
            "amplitude_contrast": str(s.amplitude_contrast.value),
            "dose_per_tilt_image": str(s.dose_per_tilt.value),
            
            # For _build_warp_fs_motion_ctf_command
            "angpix": str(s.pixel_size_angstrom.value),
            "cs": str(s.spherical_aberration_mm.value),
            "amplitude": str(s.amplitude_contrast.value),
            
            # Add other common params
            "eer_fractions": str(s.eer_fractions_per_frame.value) if s.eer_fractions_per_frame else "32",
        }

@lru_cache()
def get_parameter_manager() -> ParameterManager:
    return ParameterManager()