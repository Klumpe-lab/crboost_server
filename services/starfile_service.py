# services/starfile_service.py

import starfile
import pandas as pd
from pathlib import Path
from typing import Dict, Union, Any

class StarfileService:
    def read(self, path: Union[str, Path]) -> Dict[str, Any]:
        if not Path(path).exists():
            raise FileNotFoundError(f"STAR file not found: {path}")
        return starfile.read(path, always_dict=True)

    def write(self, data: Union[Dict[str, Any], pd.DataFrame], path: Union[str, Path]):
        try:
            if isinstance(data, dict):
                data = self._escape_star_data(data)
            elif isinstance(data, pd.DataFrame):
                data = self._escape_dataframe(data)
            
            starfile.write(data, path, overwrite=True)
        except Exception as e:
            print(f"[STARFILE ERROR] Failed to write {path}: {e}")
            starfile.write(data, path, overwrite=True)

    def _escape_star_data(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        escaped_dict = {}
        for key, value in data_dict.items():
            if isinstance(value, pd.DataFrame):
                escaped_dict[key] = self._escape_dataframe(value)
            elif isinstance(value, dict):
                escaped_dict[key] = self._escape_star_data(value)
            else:
                escaped_dict[key] = value
        return escaped_dict

    def _escape_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Escape special characters in dataframe string columns"""
        if df.empty:
            return df
        df_escaped = df.copy()
        for col in df_escaped.select_dtypes(include=['object']):
            df_escaped[col] = df_escaped[col].apply(
                lambda x: self._escape_string(x) if isinstance(x, str) else x
            )
        
        return df_escaped

    def _escape_string(self, s: str) -> str:
        """Escape problematic characters in strings for STAR file format"""
        if not isinstance(s, str):
            return s
        replacements = {
            '\n': ' ',      # Replace newlines with spaces
            '\t': ' ',      # Replace tabs with spaces
            '"': "'",       # Replace double quotes with single quotes
            '\\': '/',      # Replace backslashes with forward slashes
        }
        for find, replace in replacements.items():
            s = s.replace(find, replace)
        return s