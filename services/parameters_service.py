# services/parameter_service.py

import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Type, TypeVar
from pydantic import BaseModel, Field, validator
from enum import Enum
from functools import lru_cache
import glob
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from .starfile_service import StarfileService
from .config_service import get_config_service, Config

# ===== CORE PARAMETER MODELS =====

class MicroscopeType(str, Enum):
    KRIOS_G3 = "Krios_G3"
    GLACIOS = "TFS_Glacios"
    CUSTOM = "Custom"

class AcquisitionSoftware(str, Enum):
    SERIALEM = "SerialEM"
    TOMO5 = "Tomo5"

class JobType(str, Enum):
    IMPORT_MOVIES = "importmovies"
    MOTION_CORR = "motioncorr"
    FS_MOTION_CTF = "fsMotionAndCtf"
    CTF_FIND = "ctffind"
    ALIGN_TILTS = "aligntilts"
    RECONSTRUCTION = "reconstruction"
    TEMPLATE_MATCHING = "templatematching"
    TS_ALIGNMENT = "tsAlignment"
    TS_RECONSTRUCT = "tsReconstruct"
    TS_CTF = "tsCtf"

class MicroscopeParams(BaseModel):
    """Centralized microscope parameters"""
    microscope_type: MicroscopeType = Field(default=MicroscopeType.CUSTOM)
    voltage: float = Field(300.0, gt=0, description="Acceleration voltage (kV)")
    spherical_aberration: float = Field(2.7, description="Cs (mm)")
    amplitude_contrast: float = Field(0.1, description="Amplitude contrast ratio")
    pixel_size: float = Field(1.35, gt=0, description="Pixel size (Å)")
    acquisition_software: AcquisitionSoftware = Field(default=AcquisitionSoftware.SERIALEM)
    tilt_axis_angle: float = Field(-95.0, description="Nominal tilt axis angle")
    
    class Config:
        use_enum_values = True

class TomogramSetupParams(BaseModel):
    """Tomogram-specific setup parameters"""
    dose_per_tilt: float = Field(3.0, gt=0, description="Total dose per tilt (e⁻/Å²)")
    image_size: str = Field("4096x4096", description="Image dimensions")
    eer_grouping: Optional[int] = Field(32, description="EER fractions per rendered frame")
    gain_reference_path: Optional[Path] = None
    invert_handedness: bool = Field(False, description="Flip tilt series hand")
    reconstruction_pixel_size: float = Field(11.8, gt=0)
    tomogram_size: str = Field("4096x4096x2048", description="Reconstruction dimensions")
    sample_thickness: float = Field(300.0, gt=0, description="Sample thickness (nm)")
    alignment_method: str = Field("AreTomo", description="Alignment method")
    patch_size: int = Field(800, description="Patch size for alignment")
    
    @validator('image_size', 'tomogram_size')
    def validate_dimensions(cls, v):
        if 'x' not in v:
            raise ValueError('Size must be in format "WxH" or "WxHxD"')
        return v

class ComputingParams(BaseModel):
    """Computing resource parameters"""
    nodes: int = Field(1, ge=1)
    partition: str = Field("g", description="SLURM partition")
    gpus: int = Field(1, ge=0)
    memory: str = Field("32G", description="Memory allocation")
    threads: int = Field(8, ge=1)
    mpi_per_node: Optional[int] = None

class RelionJobParams(BaseModel):
    """Parameters for RELION native jobs"""
    job_type: JobType
    computing: ComputingParams = Field(default_factory=ComputingParams)
    
    # RELION-specific parameters
    angpix: Optional[float] = None
    kV: Optional[float] = None
    dose_rate: Optional[float] = None
    eer_grouping: Optional[int] = None
    binned_angpix: Optional[float] = None
    flip_tiltseries_hand: Optional[str] = None
    
    class Config:
        use_enum_values = True

class ExternalJobParams(BaseModel):
    """Parameters for external jobs using paramX_label/value system"""
    job_type: JobType
    computing: ComputingParams = Field(default_factory=ComputingParams)
    external_params: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
    
    def to_param_pairs(self) -> List[Dict[str, str]]:
        """Convert to paramX_label/value pairs for job.star"""
        return [
            {"label": k, "value": str(v)} 
            for i, (k, v) in enumerate(self.external_params.items(), 1)
        ]

class PipelineParameters(BaseModel):
    """Central container for all pipeline parameters"""
    microscope: MicroscopeParams = Field(default_factory=MicroscopeParams)
    tomogram_setup: TomogramSetupParams = Field(default_factory=TomogramSetupParams)
    job_parameters: Dict[JobType, Union[RelionJobParams, ExternalJobParams]] = Field(default_factory=dict)
    
    # Project metadata
    project_name: str = "default_project"
    scheme_name: str = "default_scheme"
    
    def get_job_params(self, job_type: JobType) -> Optional[Union[RelionJobParams, ExternalJobParams]]:
        return self.job_parameters.get(job_type)
    
    def update_job_params(self, job_type: JobType, params: Union[RelionJobParams, ExternalJobParams]):
        self.job_parameters[job_type] = params

# ===== MDOC PARSING =====

@dataclass
class MdocMetadata:
    """Parsed metadata from mdoc files"""
    pixel_size: float
    voltage: float
    dose_per_tilt: float
    image_size: str
    tilt_axis_angle: float
    acquisition_software: AcquisitionSoftware
    num_mdoc_files: int = 0

class MdocParser:
    """Handles mdoc file parsing"""
    
    @staticmethod
    def parse_mdoc_directory(mdoc_glob: str) -> MdocMetadata:
        """Parse mdoc files and extract metadata"""
        mdoc_files = glob.glob(mdoc_glob)
        if not mdoc_files:
            raise FileNotFoundError(f"No mdoc files found with pattern: {mdoc_glob}")
        
        # Parse first mdoc file for basic metadata
        first_mdoc = mdoc_files[0]
        return MdocParser._parse_single_mdoc(first_mdoc, len(mdoc_files))
    
    @staticmethod
    def _parse_single_mdoc(mdoc_path: Path, total_files: int = 1) -> MdocMetadata:
        """Parse a single mdoc file"""
        with open(mdoc_path, 'r') as f:
            content = f.read()
        
        metadata = {}
        
        # Extract basic metadata
        if 'PixelSpacing = ' in content:
            metadata['pixel_size'] = float(content.split('PixelSpacing = ')[1].split('\n')[0])
        if 'Voltage = ' in content:
            metadata['voltage'] = float(content.split('Voltage = ')[1].split('\n')[0])
        if 'ExposureDose = ' in content:
            metadata['dose_per_tilt'] = float(content.split('ExposureDose = ')[1].split('\n')[0])
        if 'ImageSize = ' in content:
            metadata['image_size'] = content.split('ImageSize = ')[1].split('\n')[0].replace(' ', 'x')
        
        # Determine acquisition software and tilt axis
        if 'SerialEM:' in content:
            metadata['acquisition_software'] = AcquisitionSoftware.SERIALEM
            if 'Tilt axis angle = ' in content:
                metadata['tilt_axis_angle'] = float(content.split('Tilt axis angle = ')[1].split(',')[0])
        else:
            metadata['acquisition_software'] = AcquisitionSoftware.TOMO5
            # Parse RotationAngle from ZValue sections for Tomo5
            lines = content.split('\n')
            for line in lines:
                if 'RotationAngle = ' in line:
                    metadata['tilt_axis_angle'] = abs(float(line.split('RotationAngle = ')[1]))
                    break
        
        metadata['num_mdoc_files'] = total_files
        
        return MdocMetadata(**metadata)

# ===== XML PARSING =====

class XMLParser:
    """Handles XML metadata parsing (Warp, etc.)"""
    
    @staticmethod
    def parse_warp_xml(xml_path: Path) -> Dict[str, Any]:
        """Parse Warp XML files for CTF parameters"""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            ctf_data = {}
            ctf = root.find(".//CTF")
            if ctf is not None:
                ctf_data['defocus'] = float(ctf.find(".//Param[@Name='Defocus']").get('Value'))
                ctf_data['defocus_angle'] = float(ctf.find(".//Param[@Name='DefocusAngle']").get('Value'))
                ctf_data['defocus_delta'] = float(ctf.find(".//Param[@Name='DefocusDelta']").get('Value'))
            
            return ctf_data
        except Exception as e:
            print(f"Warning: Could not parse XML file {xml_path}: {e}")
            return {}

# ===== MAIN PARAMETER SERVICE =====

class ParameterService:
    """
    Centralized service for managing all parameters across CryoBoost.
    Replaces SetupService, SimpleComputingService parameter logic, and centralizes
    parameter propagation from multiple scattered services.
    """
    
    def __init__(self, config_service: Config = None):
        self.config_service = config_service or get_config_service()
        self.star_handler = StarfileService()
        self._current_params: Optional[PipelineParameters] = None
        self.mdoc_parser = MdocParser()
        self.xml_parser = XMLParser()
        
        # Job type mappings
        self.job_tools = {
            JobType.IMPORT_MOVIES: 'relion_import',
            JobType.FS_MOTION_CTF: 'warptools',
            JobType.TS_ALIGNMENT: 'aretomo',
            JobType.MOTION_CORR: 'relion',
            JobType.CTF_FIND: 'relion',
            JobType.ALIGN_TILTS: 'relion',
            JobType.RECONSTRUCTION: 'relion',
            JobType.TEMPLATE_MATCHING: 'pytom',
        }
    
    # ===== INITIALIZATION METHODS =====
    
    def initialize_from_mdoc(self, mdoc_glob: str, project_name: str = "default") -> PipelineParameters:
        """Initialize parameters from mdoc files - main entry point"""
        mdoc_meta = self.mdoc_parser.parse_mdoc_directory(mdoc_glob)
        
        microscope_params = MicroscopeParams(
            microscope_type=MicroscopeType.CUSTOM,
            voltage=mdoc_meta.voltage,
            pixel_size=mdoc_meta.pixel_size,
            acquisition_software=mdoc_meta.acquisition_software,
            tilt_axis_angle=mdoc_meta.tilt_axis_angle
        )
        
        tomogram_params = TomogramSetupParams(
            dose_per_tilt=mdoc_meta.dose_per_tilt,
            image_size=mdoc_meta.image_size,
            reconstruction_pixel_size=mdoc_meta.pixel_size * 4,  # default 4x binning
            tomogram_size=f"{mdoc_meta.image_size.split('x')[0]}x2048"
        )
        
        self._current_params = PipelineParameters(
            microscope=microscope_params,
            tomogram_setup=tomogram_params,
            project_name=project_name,
            scheme_name=f"scheme_{project_name}"
        )
        
        return self._current_params
    
    def initialize_from_preset(self, preset_name: str, project_name: str = "default") -> PipelineParameters:
        """Initialize from microscope presets"""
        presets = self._load_microscope_presets()
        if preset_name not in presets:
            raise ValueError(f"Unknown preset: {preset_name}. Available: {list(presets.keys())}")
        
        preset = presets[preset_name]
        self._current_params = PipelineParameters(
            microscope=preset['microscope'],
            tomogram_setup=preset['tomogram_setup'],
            project_name=project_name,
            scheme_name=f"scheme_{project_name}"
        )
        
        return self._current_params
    
    # ===== JOB PARAMETER MANAGEMENT =====
    
    def create_job_parameters(self, job_types: List[JobType]) -> None:
        """Create default parameters for specified job types"""
        if not self._current_params:
            raise ValueError("Pipeline parameters not initialized")
            
        for job_type in job_types:
            if job_type in [JobType.IMPORT_MOVIES, JobType.MOTION_CORR, JobType.CTF_FIND, 
                           JobType.ALIGN_TILTS, JobType.RECONSTRUCTION]:
                self._create_relion_job_params(job_type)
            else:
                self._create_external_job_params(job_type)
    
    def _create_relion_job_params(self, job_type: JobType):
        """Create RELION native job parameters"""
        computing_params = self._get_computing_params(job_type)
        
        base_params = {
            'job_type': job_type,
            'computing': computing_params,
            'angpix': self._current_params.microscope.pixel_size,
            'kV': self._current_params.microscope.voltage,
        }
        
        if job_type == JobType.IMPORT_MOVIES:
            params = RelionJobParams(
                **base_params,
                dose_rate=self._current_params.tomogram_setup.dose_per_tilt,
                flip_tiltseries_hand="Yes" if self._current_params.tomogram_setup.invert_handedness else "No"
            )
        elif job_type == JobType.MOTION_CORR:
            params = RelionJobParams(
                **base_params,
                eer_grouping=self._current_params.tomogram_setup.eer_grouping
            )
        elif job_type in [JobType.RECONSTRUCTION, JobType.ALIGN_TILTS]:
            params = RelionJobParams(
                **base_params,
                binned_angpix=self._current_params.tomogram_setup.reconstruction_pixel_size
            )
        else:
            params = RelionJobParams(**base_params)
        
        self._current_params.update_job_params(job_type, params)
    
    def _create_external_job_params(self, job_type: JobType):
        """Create external job parameters"""
        computing_params = self._get_computing_params(job_type)
        
        if job_type == JobType.FS_MOTION_CTF:
            params = ExternalJobParams(
                job_type=job_type,
                computing=computing_params,
                external_params={
                    "eer_fractions": self._current_params.tomogram_setup.eer_grouping or 32,
                    "bin_factor": 2,
                    "angpix": self._current_params.microscope.pixel_size,
                    "voltage": self._current_params.microscope.voltage,
                    "cs": self._current_params.microscope.spherical_aberration,
                }
            )
        elif job_type == JobType.TS_RECONSTRUCT:
            params = ExternalJobParams(
                job_type=job_type,
                computing=computing_params,
                external_params={
                    "voxel_size": self._current_params.tomogram_setup.reconstruction_pixel_size,
                    "tomogram_size": self._current_params.tomogram_setup.tomogram_size,
                }
            )
        else:
            params = ExternalJobParams(
                job_type=job_type,
                computing=computing_params,
                external_params={}
            )
        
        self._current_params.update_job_params(job_type, params)
    
    def _get_computing_params(self, job_type: JobType) -> ComputingParams:
        """Get computing parameters for a job type"""
        # Use simple computing service logic
        simple_params = self._get_simple_computing_params(job_type)
        return ComputingParams(**simple_params)
    
    def _get_simple_computing_params(self, job_type: JobType) -> Dict[str, Any]:
        """Simple computing parameters (replaces SimpleComputingService)"""
        default_params = {
            "nodes": 1,
            "partition": "g",
            "gpus": 1,
            "memory": "32G",
            "threads": 8
        }
        
        job_specific = {
            JobType.IMPORT_MOVIES: {"partition": "c", "gpus": 0, "memory": "16G", "threads": 4},
            JobType.FS_MOTION_CTF: {"partition": "g", "gpus": 1, "memory": "32G", "threads": 8},
            JobType.MOTION_CORR: {"partition": "g", "gpus": 1, "memory": "32G", "threads": 8},
        }
        
        return {**default_params, **job_specific.get(job_type, {})}
    
    # ===== PARAMETER PROPAGATION =====
    
    def propagate_parameter_change(self, param_path: str, new_value: Any) -> None:
        """When a parameter changes, propagate to dependent jobs"""
        if not self._current_params:
            return
            
        # Example: if pixel_size changes
        if param_path == "microscope.pixel_size":
            self._propagate_pixel_size_change(new_value)
        elif param_path == "tomogram_setup.dose_per_tilt":
            self._propagate_dose_change(new_value)
        elif param_path == "tomogram_setup.reconstruction_pixel_size":
            self._propagate_reconstruction_pixel_size_change(new_value)
    
    def _propagate_pixel_size_change(self, new_pixel_size: float):
        """Propagate pixel size change to relevant jobs"""
        for job_type, job_params in self._current_params.job_parameters.items():
            if isinstance(job_params, RelionJobParams) and job_params.angpix is not None:
                job_params.angpix = new_pixel_size
            elif isinstance(job_params, ExternalJobParams) and "angpix" in job_params.external_params:
                job_params.external_params["angpix"] = new_pixel_size
    
    def _propagate_dose_change(self, new_dose: float):
        """Propagate dose change to relevant jobs"""
        for job_type, job_params in self._current_params.job_parameters.items():
            if isinstance(job_params, RelionJobParams) and job_params.dose_rate is not None:
                job_params.dose_rate = new_dose
    
    def _propagate_reconstruction_pixel_size_change(self, new_pixel_size: float):
        """Propagate reconstruction pixel size change"""
        for job_type, job_params in self._current_params.job_parameters.items():
            if isinstance(job_params, RelionJobParams) and job_params.binned_angpix is not None:
                job_params.binned_angpix = new_pixel_size
            elif isinstance(job_params, ExternalJobParams) and "voxel_size" in job_params.external_params:
                job_params.external_params["voxel_size"] = new_pixel_size
    
    # ===== JOB.STAR CONVERSION =====
    
    def to_job_star_dict(self, job_type: JobType) -> Dict[str, Any]:
        """Convert parameters to job.star compatible dictionary"""
        job_params = self._current_params.job_parameters.get(job_type)
        if not job_params:
            return {}
        
        star_dict = {}
        
        # Add computing parameters (qsub extras)
        if job_params.computing:
            star_dict.update(self._computing_to_qsub_params(job_params.computing))
        
        # Add job-specific parameters
        if isinstance(job_params, RelionJobParams):
            star_dict.update(self._relion_params_to_star(job_params))
        elif isinstance(job_params, ExternalJobParams):
            star_dict.update(self._external_params_to_star(job_params))
        
        return star_dict
    
    def _computing_to_qsub_params(self, computing: ComputingParams) -> Dict[str, Any]:
        """Convert computing params to qsub format"""
        return {
            "qsub_extra1": str(computing.nodes),
            "qsub_extra3": computing.partition,
            "qsub_extra4": str(computing.gpus),
            "qsub_extra5": computing.memory,
            "nr_threads": str(computing.threads)
        }
    
    def _relion_params_to_star(self, params: RelionJobParams) -> Dict[str, Any]:
        """Convert RELION params to job.star format"""
        star_dict = {}
        for field, value in params.dict(exclude_none=True).items():
            if field not in ['job_type', 'computing'] and value is not None:
                star_dict[field] = str(value)
        return star_dict
    
    def _external_params_to_star(self, params: ExternalJobParams) -> Dict[str, Any]:
        """Convert external params to paramX_label/value format"""
        star_dict = {}
        for i, (key, value) in enumerate(params.external_params.items(), 1):
            star_dict[f"param{i}_label"] = key
            star_dict[f"param{i}_value"] = str(value)
        return star_dict
    
    # ===== PRESET MANAGEMENT =====
    
    def _load_microscope_presets(self) -> Dict[str, Dict]:
        """Load predefined microscope configurations"""
        return {
            "Krios_G3": {
                'microscope': MicroscopeParams(
                    microscope_type=MicroscopeType.KRIOS_G3,
                    pixel_size=1.35,
                    voltage=300,
                    spherical_aberration=2.7,
                    amplitude_contrast=0.1
                ),
                'tomogram_setup': TomogramSetupParams(
                    dose_per_tilt=3.0,
                    image_size="4096x4096",
                    reconstruction_pixel_size=5.4,
                    tomogram_size="4096x4096x2048"
                )
            },
            "TFS_Glacios": {
                'microscope': MicroscopeParams(
                    microscope_type=MicroscopeType.GLACIOS,
                    pixel_size=1.6,
                    voltage=200,
                    spherical_aberration=2.7,
                    amplitude_contrast=0.1
                ),
                'tomogram_setup': TomogramSetupParams(
                    dose_per_tilt=3.0,
                    image_size="4096x4096",
                    reconstruction_pixel_size=6.4,
                    tomogram_size="4096x4096x2048"
                )
            }
        }
    
    # ===== PERSISTENCE =====
    
    def save_to_project(self, project_path: Path) -> None:
        """Save parameters to project directory"""
        if self._current_params:
            param_file = project_path / "pipeline_parameters.yaml"
            with open(param_file, 'w') as f:
                yaml.dump(self._current_params.dict(), f)
    
    def load_from_project(self, project_path: Path) -> Optional[PipelineParameters]:
        """Load parameters from project directory"""
        param_file = project_path / "pipeline_parameters.yaml"
        if param_file.exists():
            with open(param_file, 'r') as f:
                data = yaml.safe_load(f)
            self._current_params = PipelineParameters(**data)
            return self._current_params
        return None
    
    # ===== VALIDATION =====
    
    def validate_parameters(self) -> Dict[str, Any]:
        """Validate current parameters"""
        if not self._current_params:
            return {"valid": False, "errors": ["No parameters loaded"]}
        
        errors = []
        warnings = []
        
        # Validate microscope parameters
        micro = self._current_params.microscope
        if micro.pixel_size < 0.5 or micro.pixel_size > 10:
            warnings.append(f"Pixel size {micro.pixel_size} seems unusual")
        if micro.voltage not in [200, 300]:
            warnings.append(f"Voltage {micro.voltage} kV is non-standard")
        
        # Validate tomogram setup
        tomo = self._current_params.tomogram_setup
        if tomo.dose_per_tilt < 0.1 or tomo.dose_per_tilt > 9:
            errors.append(f"Dose per tilt {tomo.dose_per_tilt} is out of reasonable range")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }

# ===== SERVICE INSTANTIATION =====

@lru_cache()
def get_parameter_service() -> ParameterService:
    return ParameterService()