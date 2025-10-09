#!/usr/bin/env python3
import asyncio
from pathlib import Path
from backend import CryoBoostBackend

async def test_fixed_container():
    backend = CryoBoostBackend(Path.cwd())
    
    # Test the environment
    print("=== Testing Container Environment ===")
    test_dir = Path.cwd() / "test_container_fixed"
    test_dir.mkdir(exist_ok=True)
    
    await backend.debug_container_environment(test_dir)
    
    # Test Python imports specifically
    print("\n=== Testing Python Imports ===")
    test_imports = [
        "python -c \"import numpy; print('numpy OK:', numpy.__version__)\"",
        "python -c \"import pandas; print('pandas OK:', pandas.__version__)\"", 
        "python -c \"import starfile; print('starfile OK')\"",
        "python -c \"import mrcfile; print('mrcfile OK')\"",
        "python -c \"import tomography_python_programs; print('tomography_python_programs OK')\""
    ]
    
    for cmd in test_imports:
        print(f"\nTesting: {cmd}")
        result = await backend.run_shell_command(cmd, cwd=test_dir, use_container=True)
        if result['success']:
            print(f"SUCCESS: {result['output']}")
        else:
            print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    asyncio.run(test_fixed_container())