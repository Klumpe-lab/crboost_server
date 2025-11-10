# services/param_generator.py
"""
Synchronous script to generate job_params.json for continuation jobs.
Called by drivers when params are missing.

This script loads the master project_params.json, re-hydrates the
necessary models, re-calculates paths for the *new* job number,
and prints the resulting job_params JSON to stdout.
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, List
import os

SERVER_ROOT = Path(__file__).parent.parent.resolve()
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

# These imports must be synchronous
try:
    from services.project_service import ProjectService
    from services.parameter_models import JobType, jobtype_paramclass
    # We need a dummy Backend instance to initialize ProjectService
    from backend import CryoBoostBackend 
except ImportError as e:
    print(f"Error: Failed to import CryoBoost services.", file=sys.stderr)
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}", file=sys.stderr)
    print(f"sys.path: {sys.path}", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    sys.exit(1)

def load_project_config(project_path: Path) -> Dict[str, Any]:
    """Load the project's master project_params.json file."""
    config_file = project_path / "project_params.json"
    if not config_file.exists():
        raise FileNotFoundError(f"Project config not found: {config_file}")
    with open(config_file, 'r') as f:
        return json.load(f)

def generate_params(job_type_str: str, project_path: Path, job_number: int) -> Dict[str, Any]:
    try:
        project_config = load_project_config(project_path)
    except Exception as e:
        print(f"Error loading project config: {e}", file=sys.stderr)
        raise

    # 2. Get necessary info from config
    selected_jobs: List[str] = list(project_config.get("jobs", {}).keys())
    if not selected_jobs:
        raise ValueError("No 'jobs' found in project_params.json")

    # 3. Re-hydrate the Pydantic model for *this* job
    try:
        job_models_data = project_config.get("jobs", {})
        if job_type_str not in job_models_data:
            raise ValueError(f"Job type '{job_type_str}' not found in project_params.json")
        
        job_model_data = job_models_data[job_type_str]
        
        job_type_enum = JobType.from_string(job_type_str)
        param_class = jobtype_paramclass().get(job_type_enum)
        if not param_class:
            raise ValueError(f"No param class found for {job_type_enum}")
            
        job_model = param_class(**job_model_data)
        
    except Exception as e:
        print(f"Error hydrating job model '{job_type_str}': {e}", file=sys.stderr)
        raise

    # 4. Instantiate ProjectService to use its path resolution logic
    # We pass a dummy backend instance, as we only need its methods.
    dummy_backend = CryoBoostBackend(SERVER_ROOT) 
    project_service = ProjectService(dummy_backend)
    project_service.set_project_root(project_path)

    # 5. Resolve all paths for this *new* job number
    try:
        paths = project_service.resolve_job_paths(
            job_name=job_type_str,
            job_number=job_number,
            selected_jobs=selected_jobs
        )
    except Exception as e:
        print(f"Error in resolve_job_paths: {e}", file=sys.stderr)
        raise

    # 6. Re-create binds (for completeness, though driver may not use all)
    data_sources = project_config.get("data_sources", {})
    all_binds = [
        str(project_path.parent.resolve()),
        str(SERVER_ROOT)
    ]
    if data_sources.get("frames_glob"):
        all_binds.append(str(Path(data_sources["frames_glob"]).parent.resolve()))
    if data_sources.get("mdocs_glob"):
        all_binds.append(str(Path(data_sources["mdocs_glob"]).parent.resolve()))

    # 7. Build the final serializable dict
    data_to_serialize = {
        "job_type": job_type_str,
        "job_model": job_model.model_dump(),
        "paths": {k: str(v) for k, v in paths.items()},
        "additional_binds": list(set(all_binds)),
    }
    
    return data_to_serialize

def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--job_type", type=str, required=True, help="Job type string")
    parser.add_argument("--project_path", type=Path, required=True, help="Project directory") 
    parser.add_argument("--job_number", type=int, required=True, help="Job number")
    parser.add_argument("--output_file", type=Path, required=True, help="Output path for job_params.json")  # ADD THIS
    
    args = parser.parse_args()

    print(f"[PARAM_GEN] Generating params for {args.job_type} job {args.job_number}", file=sys.stderr)

    try:
        final_params = generate_params(args.job_type, args.project_path.resolve(), args.job_number)
        
        # Write directly to the specified file
        with open(args.output_file, 'w') as f:
            json.dump(final_params, f, indent=2)
        
        print(f"[PARAM_GEN] Successfully wrote {args.output_file}", file=sys.stderr)
        sys.exit(0)
        
    except Exception as e:
        print(f"[PARAM_GEN] FATAL: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()