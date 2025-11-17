# test_continuation.py (GENERALIZED VERSION)

"""
Test script for continuation services.

This script will:
1. Read the project's default_pipeline.star to find the LAST job.
2. Ask for confirmation to delete and reset that job.
3. If confirmed, it backs up files, performs the operation, and verifies the result.
4. Asks to restore from backup or keep the changes.

SAFETY: Creates backups before modifying any files.
"""

import sys
from pathlib import Path
import shutil
import json
import pandas as pd
import re

sys.path.insert(0, str(Path(__file__).parent))

from backend import CryoBoostBackend
from services.starfile_service import StarfileService

def print_separator(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def backup_project_files(project_path: Path, scheme_name: str):
    """Create backups of critical files before testing"""
    print_separator("CREATING BACKUPS")
    
    files_to_backup = [
        "default_pipeline.star",
        f"Schemes/{scheme_name}/scheme.star"
    ]
    
    for file_rel in files_to_backup:
        file_path = project_path / file_rel
        backup_path = project_path / f"{file_rel}.backup"
        
        if file_path.exists():
            shutil.copy2(file_path, backup_path)
            print(f"✓ Backed up: {file_rel} -> {file_rel}.backup")
        else:
            print(f"✗ File not found: {file_file_rel}")


def restore_from_backup(project_path: Path, scheme_name: str):
    """Restore files from backup"""
    print_separator("RESTORING FROM BACKUP")
    
    files_to_restore = [
        "default_pipeline.star",
        f"Schemes/{scheme_name}/scheme.star"
    ]
    
    for file_rel in files_to_restore:
        backup_path = project_path / f"{file_rel}.backup"
        file_path = project_path / file_rel
        
        if backup_path.exists():
            shutil.copy2(backup_path, file_path)
            print(f"✓ Restored: {file_rel}")
        else:
            print(f"✗ Backup not found: {file_rel}.backup")


def get_value_from_table(table_data, row_idx, col_name):
    """Get value from table data that might be dict or DataFrame"""
    if isinstance(table_data, pd.DataFrame):
        if not table_data.empty:
            return table_data.at[row_idx, col_name]
    elif isinstance(table_data, dict):
        return table_data.get(col_name)
    return None


def show_pipeline_state(project_path: Path, title: str):
    """Display current state of pipeline"""
    print_separator(title)
    
    star_handler = StarfileService()
    pipeline_path = project_path / "default_pipeline.star"
    
    if not pipeline_path.exists():
        print("✗ Pipeline file not found!")
        return
    
    pipeline_data = star_handler.read(pipeline_path)
    
    # Show general info
    general_df = pipeline_data.get("pipeline_general")
    if general_df is not None:
        counter = get_value_from_table(general_df, 0, "rlnPipeLineJobCounter")
        print(f"\nJob Counter: {counter}")
    
    # Show processes
    print("\nProcesses:")
    processes_df = pipeline_data.get("pipeline_processes")
    if processes_df is not None:
        if isinstance(processes_df, pd.DataFrame) and not processes_df.empty:
            for _, row in processes_df.iterrows():
                name = row["rlnPipeLineProcessName"]
                status = row["rlnPipeLineProcessStatusLabel"]
                print(f"  {name:<30} {status}")
        elif isinstance(processes_df, dict):
            name = processes_df.get("rlnPipeLineProcessName", "")
            status = processes_df.get("rlnPipeLineProcessStatusLabel", "")
            print(f"  {name:<30} {status}")
    
    # Show input edges
    print("\nInput Edges:")
    input_edges_df = pipeline_data.get("pipeline_input_edges")
    if input_edges_df is not None:
        if isinstance(input_edges_df, pd.DataFrame) and not input_edges_df.empty:
            for _, row in input_edges_df.iterrows():
                from_node = row["rlnPipeLineEdgeFromNode"]
                to_process = row["rlnPipeLineEdgeProcess"]
                print(f"  {from_node} -> {to_process}")
        elif isinstance(input_edges_df, dict):
            from_node = input_edges_df.get("rlnPipeLineEdgeFromNode", "")
            to_process = input_edges_df.get("rlnPipeLineEdgeProcess", "")
            print(f"  {from_node} -> {to_process}")
    
    # Show output edges
    print("\nOutput Edges:")
    output_edges_df = pipeline_data.get("pipeline_output_edges")
    if output_edges_df is not None:
        if isinstance(output_edges_df, pd.DataFrame) and not output_edges_df.empty:
            for _, row in output_edges_df.iterrows():
                from_process = row["rlnPipeLineEdgeProcess"]
                to_node = row["rlnPipeLineEdgeToNode"]
                print(f"  {from_process} -> {to_node}")
        elif isinstance(output_edges_df, dict):
            from_process = output_edges_df.get("rlnPipeLineEdgeProcess", "")
            to_node = output_edges_df.get("rlnPipeLineEdgeToNode", "")
            print(f"  {from_process} -> {to_node}")


def show_scheme_state(project_path: Path, scheme_name: str, title: str):
    """Display current state of scheme"""
    print_separator(title)
    
    star_handler = StarfileService()
    scheme_path = project_path / "Schemes" / scheme_name / "scheme.star"
    
    if not scheme_path.exists():
        print("✗ Scheme file not found!")
        return
    
    scheme_data = star_handler.read(scheme_path)
    
    # Show general info
    general_df = scheme_data.get("scheme_general")
    if general_df is not None:
        current_node = get_value_from_table(general_df, 0, "rlnSchemeCurrentNodeName")
        print(f"\nCurrent Node: {current_node}")
    
    # Show jobs
    print("\nJobs:")
    jobs_df = scheme_data.get("scheme_jobs")
    if jobs_df is not None:
        if isinstance(jobs_df, pd.DataFrame) and not jobs_df.empty:
            for _, row in jobs_df.iterrows():
                original = row["rlnSchemeJobNameOriginal"]
                current = row["rlnSchemeJobName"]
                has_started = row["rlnSchemeJobHasStarted"]
                mode = row["rlnSchemeJobMode"]
                print(f"  {original:<20} -> {current:<30} HasStarted={has_started} Mode={mode}")
        elif isinstance(jobs_df, dict):
            original = jobs_df.get("rlnSchemeJobNameOriginal", "")
            current = jobs_df.get("rlnSchemeJobName", "")
            has_started = jobs_df.get("rlnSchemeJobHasStarted", "")
            mode = jobs_df.get("rlnSchemeJobMode", "")
            print(f"  {original:<20} -> {current:<30} HasStarted={has_started} Mode={mode}")


def check_job_directory(project_path: Path, job_name_full: str):
    """Check if job exists in original location or Trash"""
    job_dir_name = job_name_full.rstrip("/") # e.g., "External/job005"
    
    job_dir = project_path / job_dir_name
    trash_dir = project_path / "Trash" / job_dir_name
    
    print(f"\nJob directory location check for {job_dir.name}:")
    print(f"  Original: {job_dir.exists()} ({job_dir})")
    print(f"  Trash:    {trash_dir.exists()} ({trash_dir})")
    return job_dir, trash_dir


def _get_job_number_from_name(job_name_full: str) -> int:
    """Extracts job number (e.g., 5) from "External/job005/" """
    try:
        # Match 'job' followed by 3+ digits
        match = re.search(r'job(\d{3,})', job_name_full)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    raise ValueError(f"Could not parse job number from: {job_name_full}")


def test_continuation_services():
    """Main test function"""
    
    # --- Configuration ---
    # !!! UPDATE THESE TWO VALUES !!!
    PROJECT_PATH = Path("/users/artem.kushner/dev/crboost_server/projects/0_statuses_7")
    SCHEME_NAME = "scheme_0_statuses_7"
    # ---------------------
    
    print_separator("CONTINUATION SERVICES TEST (GENERALIZED)")
    print(f"Project: {PROJECT_PATH}")
    print(f"Scheme:  {SCHEME_NAME}")
    
    if not PROJECT_PATH.exists():
        print(f"\n✗ ERROR: Project path does not exist: {PROJECT_PATH}")
        return
    
    # Initialize backend
    print("\nInitializing backend...")
    backend = CryoBoostBackend(Path.cwd())
    star_handler = StarfileService()
    
    # --- Find Last Job ---
    print_separator("FINDING LAST JOB")
    pipeline_path = PROJECT_PATH / "default_pipeline.star"
    if not pipeline_path.exists():
        print(f"✗ ERROR: default_pipeline.star not found in {PROJECT_PATH}")
        return

    pipeline_data = star_handler.read(pipeline_path)
    processes_df = pipeline_data.get("pipeline_processes")
    
    if processes_df is None or processes_df.empty:
        print("✗ ERROR: No processes found in default_pipeline.star. Cannot continue.")
        return

    last_job_row = processes_df.iloc[-1]
    last_job_name_full = last_job_row["rlnPipeLineProcessName"]
    last_job_status = last_job_row["rlnPipeLineProcessStatusLabel"]
    
    try:
        last_job_number = _get_job_number_from_name(last_job_name_full)
    except ValueError as e:
        print(f"✗ ERROR: {e}")
        return
        
    job_info = backend.pipeline_manipulation.get_job_info_by_number(
        PROJECT_PATH, 
        last_job_number,
        SCHEME_NAME  # <-- PASS THE SCHEME NAME HERE
    )
    if not job_info:
        print(f"✗ ERROR: Could not get info for job {last_job_number} from PipelineManipulationService")
        return
        
    last_job_type = job_info.get("job_type")
    
    print(f"✓ Found last job:   {last_job_name_full} (Job {last_job_number})")
    print(f"✓ Job Type:         {last_job_type}")
    print(f"✓ Current Status:   {last_job_status}")
    
    if last_job_type is None:
        print(f"✗ ERROR: Could not determine job_type for {last_job_name_full}.")
        print("  (Is job_params.json missing or malformed in that directory?)")
        return

    # Show initial state
    show_pipeline_state(PROJECT_PATH, "INITIAL PIPELINE STATE")
    show_scheme_state(PROJECT_PATH, SCHEME_NAME, "INITIAL SCHEME STATE")
    check_job_directory(PROJECT_PATH, last_job_name_full)
    
    # --- Ask for Confirmation ---
    print_separator("CONFIRM DELETION")
    print("The following actions will be performed:")
    print(f"  1. DELETE '{last_job_name_full}' from default_pipeline.star tables.")
    print(f"  2. MOVE directory '{PROJECT_PATH / last_job_name_full.rstrip('/')}' to 'Trash/'.")
    print(f"  3. RESET job '{last_job_type}' in 'scheme.star' (set HasStarted=0).")
    print(f"  4. SET 'rlnSchemeCurrentNodeName' in 'scheme.star' to the job *before* {last_job_type}.")
    
    response = input(f"\nDelete and reset job {last_job_number} ({last_job_type})? (yes/no) [no]: ").strip().lower()
    
    if response not in ["yes", "y"]:
        print("Operation cancelled by user.")
        sys.exit(0)

    # --- Perform the deletion and reset ---
    
    # Create backups
    backup_project_files(PROJECT_PATH, SCHEME_NAME)
    
    print_separator("EXECUTING DELETE AND RESET")
    
    result = backend.continuation.delete_and_reset_job(
        project_path=str(PROJECT_PATH),
        job_number=last_job_number,
        scheme_name=SCHEME_NAME
    )
    
    print("\nResult:")
    print(json.dumps(result, indent=2))
    
    if not result["success"]:
        print("\n✗ Operation failed!")
        print("Restoring from backup...")
        restore_from_backup(PROJECT_PATH, SCHEME_NAME)
        return
    
    # Show final state
    show_pipeline_state(PROJECT_PATH, "FINAL PIPELINE STATE")
    show_scheme_state(PROJECT_PATH, SCHEME_NAME, "FINAL SCHEME STATE")
    original_dir, trash_dir = check_job_directory(PROJECT_PATH, last_job_name_full)
    
    # --- Verify expected changes ---
    print_separator("VERIFICATION")
    
    # Check pipeline
    pipeline_data = star_handler.read(PROJECT_PATH / "default_pipeline.star")
    processes_df = pipeline_data.get("pipeline_processes")
    
    job_in_pipeline = False
    if processes_df is not None:
        if isinstance(processes_df, pd.DataFrame):
            job_in_pipeline = any(last_job_name_full in name for name in processes_df["rlnPipeLineProcessName"])
        elif isinstance(processes_df, dict):
            job_in_pipeline = last_job_name_full in processes_df.get("rlnPipeLineProcessName", "")
    
    print(f"✓ Job {last_job_number} removed from pipeline: {not job_in_pipeline}")
    
    # Check scheme
    scheme_data = star_handler.read(PROJECT_PATH / "Schemes" / SCHEME_NAME / "scheme.star")
    jobs_df = scheme_data.get("scheme_jobs")
    
    job_reset_in_scheme = False
    if jobs_df is not None:
        if isinstance(jobs_df, pd.DataFrame):
            job_row = jobs_df[jobs_df["rlnSchemeJobNameOriginal"] == last_job_type]
            if not job_row.empty:
                has_started = job_row.iloc[0]["rlnSchemeJobHasStarted"]
                job_name = job_row.iloc[0]["rlnSchemeJobName"]
                job_reset_in_scheme = (has_started == 0 and job_name == last_job_type)
        elif isinstance(jobs_df, dict):
            if jobs_df.get("rlnSchemeJobNameOriginal") == last_job_type:
                has_started = jobs_df.get("rlnSchemeJobHasStarted")
                job_name = jobs_df.get("rlnSchemeJobName")
                job_reset_in_scheme = (has_started == 0 and job_name == last_job_type)
    
    print(f"✓ Job type '{last_job_type}' reset in scheme: {job_reset_in_scheme}")
    
    # Check directory moved
    job_moved = trash_dir.exists() and not original_dir.exists()
    print(f"✓ Job {last_job_number} directory moved to trash: {job_moved}")
    
    # --- Summary ---
    print_separator("TEST SUMMARY")
    
    all_passed = (not job_in_pipeline) and job_reset_in_scheme and job_moved
    
    if all_passed:
        print("\n✓✓✓ ALL CHECKS PASSED ✓✓✓")
        print("\nThe pipeline is now ready for continuation!")
        print(f"Next run will create job{result['next_job_number']:03d}/")
    else:
        print("\n✗✗✗ SOME CHECKS FAILED ✗✗✗")
        print("\nRestoring from backup...")
        restore_from_backup(PROJECT_PATH, SCHEME_NAME)
    
    # Ask user if they want to keep changes or restore
    print("\n" + "=" * 80)
    
    if not all_passed:
        print("Forcing restore due to failed checks.")
        response = "no"
    else:
        response = input("\nKeep changes? (yes/no) [no]: ").strip().lower()
    
    if response not in ["yes", "y"]:
        print("\nRestoring from backup...")
        restore_from_backup(PROJECT_PATH, SCHEME_NAME)
        print("✓ Restored to original state")
    else:
        print("\n✓ Changes kept. You can now restart the pipeline.")
        print(f"  Run: relion_schemer --scheme {SCHEME_NAME} --run")


if __name__ == "__main__":
    try:
        test_continuation_services()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
