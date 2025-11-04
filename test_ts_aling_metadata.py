#!/usr/bin/env python3
"""
Standalone script to test ts_alignment metadata processing
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from services.metadata_service import MetadataTranslator
from services.parameter_models import AlignmentMethod

def test_ts_alignment_metadata():
    """Test the ts_alignment metadata processing in isolation"""
    
    # Paths from your failed project
    job_dir = Path("/users/artem.kushner/dev/crboost_server/projects/0_tilts_noimod/External/job003")
    input_star = Path("/users/artem.kushner/dev/crboost_server/projects/0_tilts_noimod/External/job002/fs_motion_and_ctf.star")
    output_star = job_dir / "aligned_tilt_series.star"
    tomo_dimensions = "4096x4096x2048"
    alignment_method = AlignmentMethod.ARETOMO.value  # "AreTomo"
    
    print("=== Testing ts_alignment metadata processing ===")
    print(f"Job directory: {job_dir}")
    print(f"Input STAR: {input_star}")
    print(f"Output STAR: {output_star}")
    print(f"Tomogram dimensions: {tomo_dimensions}")
    print(f"Alignment method: {alignment_method}")
    print()
    
    # Check if required files exist
    required_files = [
        job_dir / "tomostar/0_tilts_noimod_Position_1.tomostar",
        job_dir / "warp_tiltseries/tiltstack/0_tilts_noimod_Position_1/0_tilts_noimod_Position_1.st.aln"
    ]
    
    for file_path in required_files:
        if file_path.exists():
            print(f"✓ Found: {file_path}")
        else:
            print(f"✗ Missing: {file_path}")
            return False
    
    # Initialize metadata service
    metadata_service = MetadataTranslator()
    
    print("\n=== Starting metadata processing ===")
    result = metadata_service.update_ts_alignment_metadata(
        job_dir=job_dir,
        input_star_path=input_star,
        output_star_path=output_star,
        tomo_dimensions=tomo_dimensions,
        alignment_method=alignment_method
    )
    
    print("\n=== Result ===")
    if result["success"]:
        print("✓ SUCCESS!")
        print(f"Message: {result['message']}")
        print(f"Output: {result.get('output_path', 'N/A')}")
        
        # Check if output files were created
        output_files = [
            output_star,
            job_dir / "tilt_series/0_tilts_noimod_Position_1.star"
        ]
        
        print("\n=== Output files ===")
        for file_path in output_files:
            if file_path.exists():
                print(f"✓ Created: {file_path}")
            else:
                print(f"✗ Missing: {file_path}")
                
    else:
        print("✗ FAILED!")
        print(f"Error: {result['error']}")
    
    return result["success"]

if __name__ == "__main__":
    success = test_ts_alignment_metadata()
    sys.exit(0 if success else 1)