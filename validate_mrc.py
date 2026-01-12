#!/usr/bin/env python3
"""
MRC file validation script
Usage: python validate_mrc.py <path_to_mrc_file>
"""

import sys
import os
from pathlib import Path
import mrcfile
import numpy as np


def validate_mrc(filepath):
    """Validate an MRC file and print detailed information"""
    
    print(f"\n{'='*60}")
    print(f"Validating: {filepath}")
    print(f"{'='*60}\n")
    
    # Check if file exists
    if not os.path.exists(filepath):
        print(f"ERROR: File does not exist: {filepath}")
        return False
    
    # Check file size
    file_size = os.path.getsize(filepath)
    print(f"File size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
    
    try:
        # Validate using mrcfile
        print("\n1. Running mrcfile.validate()...")
        is_valid = mrcfile.validate(filepath)
        
        if is_valid:
            print("   ✓ File passed validation")
        else:
            print("   ✗ File FAILED validation")
        
        # Open and inspect
        print("\n2. Opening and inspecting file...")
        with mrcfile.open(filepath, mode='r', permissive=True) as mrc:
            print(f"   Mode: {mrc.header.mode}")
            print(f"   Data shape: {mrc.data.shape}")
            print(f"   Data dtype: {mrc.data.dtype}")
            
            # Header info
            print("\n3. Header Information:")
            print(f"   NX, NY, NZ: {mrc.header.nx}, {mrc.header.ny}, {mrc.header.nz}")
            print(f"   Cell dimensions (Å): {mrc.header.cella.x:.3f}, {mrc.header.cella.y:.3f}, {mrc.header.cella.z:.3f}")
            
            # Voxel size
            voxel_x = mrc.voxel_size.x
            voxel_y = mrc.voxel_size.y
            voxel_z = mrc.voxel_size.z
            print(f"   Voxel size (Å): {voxel_x:.4f}, {voxel_y:.4f}, {voxel_z:.4f}")
            
            # Origin
            print(f"   Origin: {mrc.header.origin.x:.3f}, {mrc.header.origin.y:.3f}, {mrc.header.origin.z:.3f}")
            
            # Map statistics
            print("\n4. Map Statistics:")
            data = mrc.data
            print(f"   Min: {np.min(data):.6f}")
            print(f"   Max: {np.max(data):.6f}")
            print(f"   Mean: {np.mean(data):.6f}")
            print(f"   Std Dev: {np.std(data):.6f}")
            
            # Check for NaN or Inf
            nan_count = np.isnan(data).sum()
            inf_count = np.isinf(data).sum()
            print(f"   NaN values: {nan_count}")
            print(f"   Inf values: {inf_count}")
            
            if nan_count > 0 or inf_count > 0:
                print("   ⚠ WARNING: File contains NaN or Inf values!")
            
            # Extended header
            print("\n5. Extended Header:")
            if mrc.extended_header.size > 0:
                print(f"   Size: {mrc.extended_header.size} bytes")
                print(f"   Type: {mrc.header.exttyp.decode('ascii').strip()}")
            else:
                print("   No extended header")
            
            # Check if it's a valid CCP4 map
            print("\n6. Format Check:")
            map_str = bytes(mrc.header.map).decode('ascii', errors='ignore').strip()
            print(f"   MAP string: '{map_str}'")
            
            if map_str == 'MAP':
                print("   ✓ Valid CCP4 format")
            else:
                print(f"   ⚠ WARNING: MAP string is '{map_str}', expected 'MAP'")
            
            # Machine stamp
            machst = mrc.header.machst
            print(f"   Machine stamp: {[hex(b) for b in machst[:4]]}")
            
            # Check byte order
            if machst[0] == 0x44:
                print("   Byte order: Little endian")
            elif machst[0] == 0x11:
                print("   Byte order: Big endian")
            else:
                print(f"   ⚠ WARNING: Unknown byte order: {hex(machst[0])}")
        
        print(f"\n{'='*60}")
        print("Validation Summary:")
        print(f"{'='*60}")
        print(f"Overall: {'✓ VALID' if is_valid else '✗ INVALID'}")
        
        return is_valid
        
    except Exception as e:
        print(f"\nERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_mrc.py <path_to_mrc_file>")
        print("\nExample:")
        print("  python validate_mrc.py /path/to/file.mrc")
        sys.exit(1)
    
    filepath = sys.argv[1]
    is_valid = validate_mrc(filepath)
    
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()
