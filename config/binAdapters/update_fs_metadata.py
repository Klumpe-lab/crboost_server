# config/binAdapters/update_fs_metadata.py
#!/usr/bin/env python3
"""
Helper script to update fsMotionAndCtf metadata.
Called after WarpTools completes.
"""
import sys
from pathlib import Path

# Add server directory to path so we can import services
import os
sys.path.insert(0, os.environ.get('CRBOOST_SERVER_DIR', '/users/artem.kushner/dev/crboost_server'))

from services.metadata_service import update_fs_motion_ctf_metadata

def main():
    # Paths are deterministic
    job_dir = Path.cwd()  # We're running from External/job002
    input_star = job_dir / "../../Import/job001/tilt_series.star"
    output_star = job_dir / "fs_motion_and_ctf.star"
    
    print(f"[METADATA HELPER] Job dir: {job_dir}")
    print(f"[METADATA HELPER] Input star: {input_star}")
    print(f"[METADATA HELPER] Output star: {output_star}")
    
    result = update_fs_motion_ctf_metadata(
        job_dir=job_dir,
        input_star=input_star,
        output_star=output_star
    )
    
    print(f"[METADATA HELPER] Result: {result}")
    
    if result["success"]:
        print(f"[METADATA HELPER] ✓ {result['message']}")
        sys.exit(0)
    else:
        print(f"[METADATA HELPER] ✗ {result.get('error', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main()