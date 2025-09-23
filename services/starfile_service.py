# services/starfile_service.py

import starfile
import pandas as pd
from pathlib import Path
from typing import Dict, Union

class StarfileService:
    """
    A dedicated service to handle all STAR file reading and writing.
    This isolates the 'starfile' library dependency to a single location.
    """
    def read(self, path: Union[str, Path]) -> Dict[str, pd.DataFrame]:
        """
        Reads a STAR file, always returning a dictionary of data blocks.
        This provides a consistent return type.
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"STAR file not found: {path}")
        return starfile.read(path, always_dict=True)

    def write(self, data: Union[Dict[str, pd.DataFrame], pd.DataFrame], path: Union[str, Path]):
        """
        Writes data to a STAR file, overwriting if it exists.
        """
        starfile.write(data, path, overwrite=True)