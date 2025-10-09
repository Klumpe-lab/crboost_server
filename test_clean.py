#!/usr/bin/env python3
import asyncio
from pathlib import Path
from backend import CryoBoostBackend

async def test_clean_container():
    backend = CryoBoostBackend(Path.cwd())
    
    print("=== Testing Container with Clean Environment ===")
    test_dir = Path.cwd() / "test_clean_container"
    test_dir.mkdir(exist_ok=True)
    
    test_commands = [
        "python -c \"import sys; print('Python executable:', sys.executable)\"",
        "python -c \"import numpy; print('Numpy path:', numpy.__file__)\"",
        "python -c \"import mrcfile; print('mrcfile OK')\"",
        "python -c \"import starfile; print('starfile OK')\"",
        "relion --version"
    ]
    
    for cmd in test_commands:
        print(f"\n--- Testing: {cmd} ---")
        # FORCE container usage for all these commands
        result = await backend.run_shell_command(cmd, cwd=test_dir, use_container=True)
        if result['success']:
            print(f"SUCCESS: {result['output']}")
        else:
            print(f"FAILED: {result['error']}")

if __name__ == "__main__":
    asyncio.run(test_clean_container())
