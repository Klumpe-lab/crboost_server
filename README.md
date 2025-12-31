[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Klumpe-lab/crboost_server)





# CryoBoost Server

To see the UI running on the cluster's subnet from anywhere on the general institution network -- open ssh tunnel with a port forwarding:

Start the server on the headnode of the cluster:
```
(venv) ᢹ CBE-login [dev/crboost_server] python3 main.py
Warning: pymol2 module not found. PDB features will fail.
CryoBoost Server Starting
Access URLs:
  Local:    http://localhost:8081
  Network: http://172.24.96.17:8081

To access from another machine, use an SSH tunnel:
ssh -L 8081:[your cluster node]:8081 [YOUR_USERNAME]@[your cluster node]
------------------------------
INFO:     Started server process [37299]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8081 (Press CTRL+C to quit)
```
On your local machine (where the browser is), open a porward forwarding tunnel:

```
ssh -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# or in the background
ssh -f -N -L ${LOCAL_PORT}:localhost:${HEADNODE_PORT} ${USERNAME}@${HEADNODE_URL}
# (kill when done:  `pkill -f "ssh.*${LOCAL_PORT}:localhost:${HEADNODE_PORT}"`)
```

- `LOCAL_PORT` is any free port of choosing on your computer
- `HEADNODE_PORT` is the port on which this software (crboost_server) is running on the headnode
- `USERNAME` and `HEADNODE_URL` are the credentials for your local cluster setup

This, of course, assumes that `USERNAME` has previously added their public key to the cluster's ssh folder (usually done for you by Slurm's admins).

You may also want to save this configuration to your local sshconfig (example):

```
Host cryoboost-tunnel
    HostName [YOUR CLUSTR NODE]
    User [YOUR USERNAME]
    LocalForward 8080 localhost:1717
    LocalForward 8081 localhost:1718
    LocalForward 8082 localhost:1719
```

Then, `ssh cryoboost-tunnel` suffices on local.


```
Your Laptop          SSH Tunnel               Head Node
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Browser    │────►│ Port 8080       │────►│ Port 1717    │
│ localhost:  │     │       ↓         │     │ CryoBoost    │
│   8080      │     │ SSH Connection  │     │ Server       │ 
└─────────────┘     └─────────────────┘     └──────────────┘
```


# CryoBoost Server Documentation

## Purpose and Overview

CryoBoost Server is a cryo-EM job orchestrator that provides a web-based interface for constructing and executing processing pipelines on SLURM clusters. It leverages containerized tools from the cryo-EM ecosystem and unifies processing under RELION's pipeline system while eliminating the need for SSH connections through a browser-based UI accessible via port forwarding [1](#0-0) .

## Dispatch Mechanism

The system implements a "Just-In-Time" (JIT) orchestration model where pipeline schemes are transient execution artifacts rather than persistent state files. When a user clicks "Run Pipeline", the system:

1. **State Snapshot**: Freezes UI state into `project_params.json` and filters jobs by status (ignoring `SUCCEEDED` jobs) [2](#0-1) 
2. **Historical Resolution**: Queries `default_pipeline.star` to find output paths from the last successful upstream job, creating a "bridge" between static data and dynamic runs [3](#0-2) 
3. **Ephemeral Generation**: Creates a unique scheme directory with only the jobs in the execution queue, patched with specific upstream paths [4](#0-3) 
4. **Execution**: Invokes `relion_schemer` with the temporary scheme, which executes jobs sequentially and appends results to the main project's `default_pipeline.star` [5](#0-4) 

## Architecture Modules

### Core Backend
- **CryoBoostBackend**: Main FastAPI backend class that coordinates all services and handles project lifecycle [6](#0-5) 
- **PipelineOrchestratorService**: Orchestrates scheme creation, job command building, and container wrapping [7](#0-6) 
- **ProjectService**: Handles project structure creation and data import [8](#0-7) 

### Parameter Management
- **PipelineState**: Central state container with hierarchical parameter organization [9](#0-8) 
- **Job Parameter Models**: Pydantic models for each job type (ImportMoviesParams, FsMotionCtfParams, TsAlignmentParams) with validation and synchronization [10](#0-9) 
- **JobType Enum**: Defines available pipeline jobs with strict execution order [11](#0-10) 

### Execution Layer
- **Container Service**: Wraps commands for containerized execution with proper bind paths and environment isolation [12](#0-11) 
- **SLURM Integration**: Uses qsub templates with placeholder substitution for cluster job submission [13](#0-12) 
- **Command Builders**: Tool-specific command construction (ImportMoviesCommandBuilder, FsMotionCtfCommandBuilder, TsAlignmentCommandBuilder) [14](#0-13) 

### State Synchronization
- **Global State Management**: Hierarchical parameter system with global microscope/acquisition parameters as authoritative source [15](#0-14) 
- **Job Sync Protocol**: All job models implement `sync_from_pipeline_state()` for bidirectional synchronization [16](#0-15) 

### Citations

**File:** services/pipeline_orchestrator_service.py (L28-45)
```python
class PipelineOrchestratorService:
    """
    Orchestrates the creation and execution of Relion pipelines.
    """
    
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.star_handler = StarfileService()
        self.config_service = get_config_service()
        self.container_service = get_container_service()
        
        # Map job names to their corresponding builder class
        self.job_builders: Dict[str, BaseCommandBuilder] = {
            'importmovies'  : ImportMoviesCommandBuilder(),
            'fsMotionAndCtf': FsMotionCtfCommandBuilder(),
            'tsAlignment'   : TsAlignmentCommandBuilder(),
        }

```

**File:** services/pipeline_orchestrator_service.py (L66-110)
```python
    def _get_job_paths(
        self,
        job_name: str,
        job_index: int,
        selected_jobs: List[str],
        acquisition_params: AcquisitionParams,
        project_dir: Path,
        job_dir: Path
    ) -> Dict[str, Path]:
        """Construct the relative input/output paths for a job"""
        paths = {}
        
        def get_job_dir_by_name(name: str) -> Optional[str]:
            try:
                idx = selected_jobs.index(name)
                if name == 'importmovies':
                    return f"Import/job{idx+1:03d}"
                else:
                    return f"External/job{idx+1:03d}"
            except ValueError:
                return None

        if job_name == 'importmovies':
            paths['input_dir'] = Path(f"../../{project_dir.name}/mdoc")
            paths['output_dir'] = Path(".")
            paths['pipeline_control'] = Path(".")
            
        elif job_name == 'fsMotionAndCtf':
            import_dir = get_job_dir_by_name('importmovies')
            if import_dir:
                paths['input_star'] = Path(f"../{import_dir}/movies.star")
            
            paths['output_star'] = Path("movies_mic.star")
            if acquisition_params.gain_reference_path:
                paths['gain_reference'] = Path(acquisition_params.gain_reference_path)

        elif job_name == 'tsAlignment':
            motion_dir = get_job_dir_by_name('fsMotionAndCtf')
            if motion_dir:
                paths['input_star'] = Path(f"../{motion_dir}/movies_mic.star")

            paths['output_dir'] = Path(".")
            paths['output_star'] = Path("aligned.star")

        return paths
```

**File:** services/pipeline_orchestrator_service.py (L137-148)
```python
    async def create_custom_scheme(self, 
        project_dir: Path, 
        new_scheme_name: str, 
        base_template_path: Path, 
        selected_jobs: List[str], 
        additional_bind_paths: List[str]
    ):
        try:
            new_scheme_dir = project_dir / "Schemes" / new_scheme_name
            new_scheme_dir.mkdir(parents=True, exist_ok=True)
            base_scheme_name = base_template_path.name

```

**File:** services/pipeline_orchestrator_service.py (L149-190)
```python
            for job_index, job_name in enumerate(selected_jobs):
                job_number_str = f"job{job_index+1:03d}"
                
                source_job_dir = base_template_path / job_name
                dest_job_dir = new_scheme_dir / job_name
                if dest_job_dir.exists():
                    shutil.rmtree(dest_job_dir)
                shutil.copytree(source_job_dir, dest_job_dir)

                job_star_path = dest_job_dir / "job.star"
                if not job_star_path.exists():
                    continue

                job_data = self.star_handler.read(job_star_path)
                params_df = job_data.get('joboptions_values')
                if params_df is None:
                    continue

                # Get the job model from global state
                job_model = self.backend.app_state.jobs.get(job_name)
                if not job_model:
                    print(f"[PIPELINE WARNING] Job {job_name} not in state, skipping")
                    continue
                
                # Get the tool name
                tool_name = self._get_job_tool(job_name, job_model)
                if not tool_name:
                    print(f"[PIPELINE WARNING] No tool mapping for job {job_name}, skipping containerization")
                    continue

                # Get the paths
                job_dir_name = "Import" if job_name == 'importmovies' else "External"
                job_run_dir = project_dir / job_dir_name / job_number_str
                
                paths = self._get_job_paths(
                    job_name,
                    job_index,
                    selected_jobs,
                    self.backend.app_state.acquisition,
                    project_dir.parent / project_dir.name,
                    job_run_dir
                )
```

**File:** services/pipeline_orchestrator_service.py (L195-201)
```python
                # Wrap command with container
                final_containerized_command = self.container_service.wrap_command_for_tool(
                    command=raw_command,
                    cwd=project_dir,
                    tool_name=tool_name,
                    additional_binds=additional_bind_paths
                )
```

**File:** backend.py (L42-53)
```python
class CryoBoostBackend:

    def __init__(self, server_dir: Path):
        self.server_dir = server_dir
        self.project_service = ProjectService(self)
        self.pipeline_orchestrator = PipelineOrchestratorService(self)
        self.container_service = get_container_service()
        
        # Store a reference to global state (for services that need it)
        self.app_state = app_state
        print(f"[BACKEND] Initialized with state reference")

```

**File:** backend.py (L94-96)
```python
            structure_result = await self.project_service.create_project_structure(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )
```

**File:** backend.py (L255-278)
```python
    async def _run_relion_schemer(self, project_dir: Path, scheme_name: str, additional_bind_paths: List[str]):
        """Run relion_schemer to execute the pipeline scheme"""
        try:
            run_command = f"unset DISPLAY && relion_schemer --scheme {scheme_name} --run --verb 2"
            
            # This call prints the formatted log, so no need for manual print
            full_run_command = self.container_service.wrap_command_for_tool(
                command=run_command,
                cwd=project_dir,
                tool_name="relion_schemer",
                additional_binds=additional_bind_paths
            )
            
            process = await asyncio.create_subprocess_shell(
                full_run_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_dir
            )
            
            self.active_schemer_process = process
            asyncio.create_task(self._monitor_schemer(process, project_dir))
            
            return {"success": True, "message": f"Workflow started (PID: {process.pid})", "pid": process.pid}
```

**File:** services/parameter_models.py (L151-229)
```python
class ImportMoviesParams(BaseModel):
    """Parameters for import movies job - implements JobParamsProtocol"""
    model_config = ConfigDict(validate_assignment=True)
    
    # From microscope
    pixel_size          : float = Field(ge=0.5, le=10.0)
    voltage             : float = Field(ge=50.0)
    spherical_aberration: float = Field(ge=0.0)
    amplitude_contrast  : float = Field(ge=0.0, le=1.0)

    # From acquisition
    dose_per_tilt_image: float = Field(ge=0.1)
    tilt_axis_angle    : float = Field(ge=-180.0, le=180.0)

    # Job-specific
    optics_group_name  : str  = "opticsGroup1"
    do_at_most         : int  = Field(default=-1)
    invert_defocus_hand: bool = False

    @classmethod
    def from_job_star(cls, star_path: Path) -> Optional[Self]:
        """Load defaults from job.star template"""
        if not star_path or not star_path.exists():
            return None

        try:
            data: Dict[str, Union[pd.DataFrame, Dict[str, Any]]] = starfile.read(
                star_path, always_dict=True
            )
            
            job_data = data.get('job')
            if job_data is None:
                return None
                
            # Convert DataFrame to dict if needed
            if isinstance(job_data, pd.DataFrame):
                if len(job_data) == 0:
                    return None
                job_params: Dict[str, Any] = job_data.to_dict('records')[0]
            else:
                job_params: Dict[str, Any] = job_data

            return cls(
                pixel_size           = float(job_params.get("nominal_pixel_size", 1.35)),
                voltage              = float(job_params.get("voltage", 300)),
                spherical_aberration = float(job_params.get("spherical_aberration", 2.7)),
                amplitude_contrast   = float(job_params.get("amplitude_contrast", 0.1)),
                dose_per_tilt_image  = float(job_params.get("dose_per_tilt_image", 3.0)),
                tilt_axis_angle      = float(job_params.get("nominal_tilt_axis_angle", -95.0)),
                optics_group_name    = job_params.get("optics_group_name", "opticsGroup1"),
                invert_defocus_hand  = bool(job_params.get("invert_defocus_hand", False)),
            )
        except Exception as e:
            print(f"[WARN] Could not parse job.star at {star_path}: {e}")
            return None
    
    @classmethod
    def from_pipeline_state(cls, state: 'PipelineState') -> Self:
        """Create from global pipeline state"""
        return cls(
            pixel_size           = state.microscope.pixel_size_angstrom,
            voltage              = state.microscope.acceleration_voltage_kv,
            spherical_aberration = state.microscope.spherical_aberration_mm,
            amplitude_contrast   = state.microscope.amplitude_contrast,
            dose_per_tilt_image  = state.acquisition.dose_per_tilt,
            tilt_axis_angle      = state.acquisition.tilt_axis_degrees,
            invert_defocus_hand  = state.acquisition.invert_defocus_hand,
        )
    
    def sync_from_pipeline_state(self, state: 'PipelineState') -> Self:
        """Update microscope/acquisition params from global state IN-PLACE"""
        self.pixel_size           = state.microscope.pixel_size_angstrom
        self.voltage              = state.microscope.acceleration_voltage_kv
        self.spherical_aberration = state.microscope.spherical_aberration_mm
        self.amplitude_contrast   = state.microscope.amplitude_contrast
        self.dose_per_tilt_image  = state.acquisition.dose_per_tilt
        self.tilt_axis_angle      = state.acquisition.tilt_axis_degrees
        self.invert_defocus_hand  = state.acquisition.invert_defocus_hand
        return self
```

**File:** services/parameter_models.py (L434-441)
```python
class PipelineState(BaseModel):
    """Central state with hierarchical organization"""
    model_config = ConfigDict(validate_assignment=True)
    
    microscope : MicroscopeParams     = Field(default_factory=MicroscopeParams)
    acquisition: AcquisitionParams    = Field(default_factory=AcquisitionParams)
    computing  : ComputingParams      = Field(default_factory=ComputingParams)
    jobs       : Dict[str, BaseModel] = Field(default_factory=dict)
```

**File:** services/job_types.py (L8-27)
```python
class JobType(str, Enum):
    """Enumeration of all pipeline job types"""
    IMPORT_MOVIES = "importmovies"
    FS_MOTION_CTF = "fsMotionAndCtf"
    TS_ALIGNMENT = "tsAlignment"
    
    @classmethod
    def from_string(cls, value: str) -> 'JobType':
        """Safe conversion from string with better error message"""
        try:
            return cls(value)
        except ValueError:
            valid = [e.value for e in cls]
            raise ValueError(f"Unknown job type '{value}'. Valid types: {valid}")
    
    @property
    def display_name(self) -> str:
        """Human-readable name"""
        return self.value.replace('_', ' ').title()

```

**File:** config/qsub/qsub_cbe_warp.sh (L1-38)
```shellscript
#!/bin/bash
#SBATCH --job-name=CryoBoost-Warp
#SBATCH --constraint="g2|g3|g4"
#SBATCH --partition=XXXextra3XXX
#SBATCH --nodes=XXXextra1XXX
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=XXXthreadsXXX
#SBATCH --gres=gpu=XXXextra4XXX
#SBATCH --mem=XXXextra5XXX
#SBATCH --time=5:00:00
#SBATCH --output=XXXoutfileXXX
#SBATCH --error=XXXerrfileXXX

echo "--- SLURM JOB START ---"
echo "Node: $(hostname)"

JOB_DIR=$(dirname "XXXoutfileXXX")

echo "Original CWD: $(pwd)"
echo "Target Job Directory: ${JOB_DIR}"

cd "${JOB_DIR}"
echo "New CWD: $(pwd)"

XXXcommandXXX

EXIT_CODE=$?
echo "--- SLURM JOB END (Exit Code: $EXIT_CODE) ---"

if [ $EXIT_CODE -eq 0 ]; then
    echo "Creating RELION_JOB_EXIT_SUCCESS"
    touch "./RELION_JOB_EXIT_SUCCESS"
else
    echo "Creating RELION_JOB_EXIT_FAILURE"
    touch "./RELION_JOB_EXIT_FAILURE"
fi

```

**File:** app_state.py (L56-112)
```python
def update_from_mdoc(mdocs_glob: str):
    """
    Parse first mdoc file and update microscope/acquisition params.
    This mutates state.microscope and state.acquisition.
    """
    mdoc_files = glob.glob(mdocs_glob)
    if not mdoc_files:
        print(f"[WARN] No mdoc files found at: {mdocs_glob}")
        return

    try:
        mdoc_path = Path(mdoc_files[0])
        print(f"[STATE] Parsing mdoc: {mdoc_path}")
        mdoc_data = _parse_mdoc(mdoc_path)

        # Update microscope params
        if "pixel_spacing" in mdoc_data:
            state.microscope.pixel_size_angstrom = mdoc_data["pixel_spacing"]
        if "voltage" in mdoc_data:
            state.microscope.acceleration_voltage_kv = mdoc_data["voltage"]

        # Update acquisition params
        if "exposure_dose" in mdoc_data:
            dose = mdoc_data["exposure_dose"] * 1.5  # Scale as per original logic
            dose = max(0.1, min(9.0, dose))  # Clamp
            state.acquisition.dose_per_tilt = dose

        if "tilt_axis_angle" in mdoc_data:
            state.acquisition.tilt_axis_degrees = mdoc_data["tilt_axis_angle"]

        # Parse detector dimensions
        if "image_size" in mdoc_data:
            dims = mdoc_data["image_size"].split("x")
            if len(dims) == 2:
                state.acquisition.detector_dimensions = (int(dims[0]), int(dims[1]))

                # Detect K3/EER based on dimensions
                if (
                    "5760" in mdoc_data["image_size"]
                    or "11520" in mdoc_data["image_size"]
                ):
                    state.acquisition.eer_fractions_per_frame = 32
                    print("[STATE] Detected K3/EER camera, set fractions to 32")

        state.update_modified()

        # Update any existing jobs with new global values
        for job_name in list(state.jobs.keys()):
            _sync_job_with_global_params(job_name)

        print(f"[STATE] Updated from mdoc: {len(mdoc_files)} files found")

    except Exception as e:
        print(f"[ERROR] Failed to parse mdoc {mdoc_files[0]}: {e}")
        import traceback

        traceback.print_exc()
```
