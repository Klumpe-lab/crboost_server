# services/parameters_service.py


from datetime import datetime
import glob
import json
from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional, Dict, Any, Literal, Union, Tuple, TypeVar, Generic, List
from enum import Enum
from pathlib import Path
from functools import lru_cache


T = TypeVar('T')

class Parameter(BaseModel, Generic[T]):
    """
    A strongly-typed parameter with validation constraints.
    """
    value: T
    min_value: Optional[T] = None
    max_value: Optional[T] = None
    choices: Optional[List[T]] = None
    description: Optional[str] = None
    source: Optional[str] = None

    def copy(self, **kwargs) -> 'Parameter[T]':
        """Create a copy of the parameter with optional updates"""
        return self.__class__(**{**self.dict(), **kwargs})

    def dict(self, **kwargs) -> Dict[str, Any]:
        """Convert to dictionary, compatible with Pydantic"""
        return super().dict(**kwargs)

    @validator('value')
    def validate_constraints(cls, v, values):
        """Validate value against constraints"""
        if 'min_value' in values and values['min_value'] is not None:
            if v < values['min_value']:
                raise ValueError(f"Value {v} below minimum {values['min_value']}")
        
        if 'max_value' in values and values['max_value'] is not None:
            if v > values['max_value']:
                raise ValueError(f"Value {v} above maximum {values['max_value']}")
        
        if 'choices' in values and values['choices'] is not None:
            if v not in values['choices']:
                raise ValueError(f"Value {v} not in allowed choices: {values['choices']}")
        
        return v
    
    class Config:
        arbitrary_types_allowed = True

# Create explicit type aliases for better IDE support
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
    
    # Use explicit Parameter[Tuple[int, int]] instead of generic
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
    
class ParameterManager:
    """Manages pipeline parameters with type safety and validation"""
    
    def __init__(self):
        # Initialize state immediately - never None
        from services.config_service import get_config_service
        self.config_service = get_config_service()
        self.state = PipelineState()  # Always initialized, not Optional
        self._initialize_state_from_config()

    def _initialize_state_from_config(self):
            """Initialize the default PipelineState from conf.yaml"""
            try:
                computing_defaults = self.config_service.get_default_computing_params()
                partition_map = {
                    'g': Partition.GPU,
                    'g-v100': Partition.GPU_V100,
                    'g-a100': Partition.GPU_A100,
                    'c': Partition.CPU,
                    'm': Partition.MEMORY
                }
                
                partition_enum = partition_map.get(computing_defaults['partition'], Partition.GPU)
                
                # Update parameters directly
                self.state.default_partition.value = partition_enum
                self.state.default_partition.source = "conf.yaml"
                
                self.state.default_gpu_count.value = computing_defaults['gpu_count']
                self.state.default_gpu_count.source = "conf.yaml"
                
                self.state.default_memory_gb.value = computing_defaults['memory_gb']
                self.state.default_memory_gb.source = "conf.yaml"
                
                self.state.default_threads.value = computing_defaults['cpu_count']
                self.state.default_threads.source = "conf.yaml"
                
                print(f"[PARAMS] Initialized computing defaults from config: {computing_defaults}")
            except Exception as e:
                print(f"[WARN] Could not parse computing defaults from conf.yaml: {e}. Using model defaults.")

    def update_parameter_from_ui(self, param_name: str, value: Any, mark_as_user_input: bool = True):
        """Update a parameter from the UI, automatically marking source as 'user_input'"""
        try:
            if not hasattr(self.state, param_name):
                raise AttributeError(f"Parameter '{param_name}' not found")
            
            param = getattr(self.state, param_name)
            if not isinstance(param, Parameter):
                raise TypeError(f"'{param_name}' is not a Parameter instance")
            
            param.value = value
            
            if mark_as_user_input:
                param.source = "user_input"
            
            print(f"[PARAMS] Updated {param_name} = {value} (source: {'user_input' if mark_as_user_input else 'unchanged'})")
        except Exception as e:
            print(f"[ERROR] Failed to update parameter {param_name}: {e}")
            raise

    def initialize_from_mdoc(self, mdoc_data: RawMdocData) -> PipelineState:
        """Initialize with validated, typed parameters from mdoc"""
        
        # Calculate and validate dose per tilt
        dose_per_tilt = mdoc_data.exposure_dose * 1.5
        dose_per_tilt = max(0.1, min(9.0, dose_per_tilt))  # Clamp to valid range
        
        # Update parameters directly
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
                description="EER fractions for K3",
                source="mdoc"
            )
        
        # Validate everything
        issues = self.state.validate_all()
        if issues["errors"]:
            print(f"Parameter errors: {issues['errors']}")
        if issues["warnings"]:
            print(f"Parameter warnings: {issues['warnings']}")
        
        return self.state


    def export_for_project(self, 
                          project_name: str,
                          movies_glob: str,
                          mdocs_glob: str,
                          selected_jobs: List[str]) -> Dict[str, Any]:

        def clean_param(param: Parameter, override_source: str = None) -> Dict[str, Any]:
            return {
                "value": param.value,
                "description": param.description,
                "source": override_source or param.source or "default"
            }
        
        containers = self.config_service.get_config().containers or {}
        
        export = {
            "metadata": {
                "config_version": "1.0",
                "created_by": "CryoBoost Parameter Manager",
                "created_at": datetime.now().isoformat(),
                "project_name": project_name
            },
            
            "data_sources": {
                "frames_glob": movies_glob,
                "mdocs_glob": mdocs_glob,
                "gain_reference": str(self.state.gain_reference_path.value) if self.state.gain_reference_path and self.state.gain_reference_path.value else None
            },
            
            "containers": {
                name: path for name, path in containers.items()
            },
            
            "microscope": {
                "type": clean_param(self.state.microscope_type),
                "pixel_size_angstrom": clean_param(self.state.pixel_size_angstrom),
                "acceleration_voltage_kv": clean_param(self.state.acceleration_voltage_kv),
                "spherical_aberration_mm": clean_param(self.state.spherical_aberration_mm),
                "amplitude_contrast": clean_param(self.state.amplitude_contrast),
            },
            
            "acquisition": {
                "dose_per_tilt": clean_param(self.state.dose_per_tilt),
                "detector_dimensions": {
                    "value": self.state.detector_dimensions.value,
                    "description": self.state.detector_dimensions.description,
                    "source": self.state.detector_dimensions.source or "default"
                },
                "tilt_axis_degrees": clean_param(self.state.tilt_axis_degrees),
                "eer_fractions_per_frame": clean_param(self.state.eer_fractions_per_frame) if self.state.eer_fractions_per_frame else None,
            },
            
            "computing": {
                "default_partition": clean_param(self.state.default_partition, override_source="conf.yaml"),
                "default_gpu_count": clean_param(self.state.default_gpu_count, override_source="conf.yaml"),
                "default_memory_gb": clean_param(self.state.default_memory_gb, override_source="conf.yaml"),
                "default_threads": clean_param(self.state.default_threads, override_source="conf.yaml"),
            },
            
            "jobs": self._export_job_parameters(selected_jobs)
        }
        
        return export
        

    def _export_job_parameters(self, selected_jobs: List[str]) -> Dict[str, Dict[str, Any]]:
        job_params = {}
        
        for job_name in selected_jobs:
            if job_name == 'importmovies':
                job_params[job_name] = {
                    "nominal_tilt_axis_angle": {
                        "value": self.state.tilt_axis_degrees.value,
                        "description": "Tilt axis angle for import",
                        "source": self.state.tilt_axis_degrees.source or "default"
                    },
                    "nominal_pixel_size": {
                        "value": self.state.pixel_size_angstrom.value,
                        "description": "Pixel size for import",
                        "source": self.state.pixel_size_angstrom.source or "default"
                    },
                    "voltage": {
                        "value": self.state.acceleration_voltage_kv.value,
                        "description": "Acceleration voltage",
                        "source": self.state.acceleration_voltage_kv.source or "default"
                    },
                    "spherical_aberration": {
                        "value": self.state.spherical_aberration_mm.value,
                        "description": "Spherical aberration",
                        "source": self.state.spherical_aberration_mm.source or "default"
                    },
                    "amplitude_contrast": {
                        "value": self.state.amplitude_contrast.value,
                        "description": "Amplitude contrast",
                        "source": self.state.amplitude_contrast.source or "default"
                    },
                    "dose_per_tilt_image": {
                        "value": self.state.dose_per_tilt.value,
                        "description": "Dose per tilt image",
                        "source": self.state.dose_per_tilt.source or "default"
                    }
                }
            
            elif job_name == 'fsMotionAndCtf':
                job_params[job_name] = {
                    "angpix": {
                        "value": self.state.pixel_size_angstrom.value,
                        "description": "Pixel size for motion correction",
                        "source": self.state.pixel_size_angstrom.source or "default"
                    },
                    "eer_ngroups": {
                        "value": self.state.eer_fractions_per_frame.value if self.state.eer_fractions_per_frame else 32,
                        "description": "EER fractions grouping",
                        "source": self.state.eer_fractions_per_frame.source if self.state.eer_fractions_per_frame else "default"
                    },
                    "voltage": {
                        "value": self.state.acceleration_voltage_kv.value,
                        "description": "Acceleration voltage",
                        "source": self.state.acceleration_voltage_kv.source or "default"
                    },
                    "cs": {
                        "value": self.state.spherical_aberration_mm.value,
                        "description": "Spherical aberration",
                        "source": self.state.spherical_aberration_mm.source or "default"
                    },
                    "amplitude": {
                        "value": self.state.amplitude_contrast.value,
                        "description": "Amplitude contrast",
                        "source": self.state.amplitude_contrast.source or "default"
                    }
                }
            
            elif job_name == 'tsAlignment':
                job_params[job_name] = {
                    "binning": {
                        "value": self.state.reconstruction_binning.value,
                        "description": "Alignment binning",
                        "source": self.state.reconstruction_binning.source or "default"
                    },
                    "alignment_method": {
                        "value": self.state.alignment_method.value,
                        "description": "Alignment algorithm",
                        "source": self.state.alignment_method.source or "default"
                    }
                }
            
            # Add more jobs as you implement them...
        
        return job_params



    def get_unified_config_dict(self) -> Dict[str, Any]:
        """Export EVERYTHING as one unified configuration dict"""
        unified = json.loads(self.state.json())
        
        unified['_metadata'] = {
            'config_version': '1.0',
            'created_by': 'CryoBoost Parameter Manager',
            'parameter_sources': self._get_parameter_sources()
        }
        
        unified['computing_config'] = {
            'default_partition': self.state.default_partition.value.value,
            'default_gpu_count': self.state.default_gpu_count.value,
            'default_memory_gb': self.state.default_memory_gb.value,
            'default_threads': self.state.default_threads.value,
        }
        
        return unified
    

    
    def load_unified_config(self, path: Path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                
                # Remove metadata before loading into state
                data.pop('_metadata', None)
                data.pop('computing_config', None)  # This is derived from other params
                
                self.state = PipelineState(**data)
                print(f"[PARAMS] Loaded unified config from {path}")
            except Exception as e:
                print(f"[ERROR] Failed to load unified config from {path}: {e}")
 
    def get_state_as_dict(self) -> Dict[str, Any]:
        return json.loads(self.state.json())

    def save_unified_config(self, path: Path):
        """Save the complete unified configuration to JSON"""
        try:
            unified_config = self.get_unified_config_dict()
            path.write_text(json.dumps(unified_config, indent=2))
            print(f"[PARAMS] Saved unified config to {path}")
        except Exception as e:
            print(f"[ERROR] Failed to save unified config to {path}: {e}")

    def _get_parameter_sources(self) -> Dict[str, str]:
        """Track where each parameter value came from"""
        sources = {}
        if self.state:
            for field_name in self.state.__fields__.keys():
                param = getattr(self.state, field_name)
                if isinstance(param, Parameter) and param.source:
                    sources[field_name] = param.source
        return sources

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

            self.initialize_from_mdoc(raw_data)
            return self.state

        except Exception as e:
            print(f"[ERROR] Failed to parse mdoc {mdoc_files[0]}: {e}")
            return self.state

            
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
        eer_fractions_value = s.eer_fractions_per_frame.value if s.eer_fractions_per_frame else 32
        # FORCE POSITIVE VALUE
        if eer_fractions_value <= 0:
            eer_fractions_value = 32
            
            
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