# services/data_import_service.py

import os
import glob
import shutil
from pathlib import Path
from typing import Dict, List, Any

class DataImportService:
    """
    Handles the core logic of preparing raw data for a CryoBoost project.
    This includes parsing mdocs, creating symlinks, and rewriting mdocs with prefixes.
    """

    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
        """
        Parses an .mdoc file into a header string and a list of data dictionaries.
        Lifts logic from `mdocMeta.readMdoc`.
        """
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False

        with open(mdoc_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('[ZValue'):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {'ZValue': line.split('=')[1].strip().strip(']')}
                    in_zvalue_section = True
                elif in_zvalue_section and '=' in line:
                    key, value = [x.strip() for x in line.split('=', 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)

        if current_section:
            data_sections.append(current_section)

        return {'header': "\n".join(header_lines), 'data': data_sections}

    def _write_mdoc(self, mdoc_data: Dict[str, Any], output_path: Path):
        """
        Writes a parsed mdoc data structure back to a file.
        Lifts logic from `mdocMeta.writeMdoc`.
        """
        with open(output_path, 'w') as f:
            f.write(mdoc_data['header'] + '\n')
            for section in mdoc_data['data']:
                z_value = section.pop('ZValue', None)
                if z_value is not None:
                    f.write(f"[ZValue = {z_value}]\n")
                for key, value in section.items():
                    f.write(f"{key} = {value}\n")
                f.write("\n") # Add a newline for separation

    async def setup_project_data(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str
    ) -> Dict[str, Any]:
        """
        Orchestrates the data import process: creates dirs, symlinks movies,
        and rewrites mdocs with the specified prefix.
        """
        try:
            frames_dir = project_dir / 'frames'
            mdoc_dir = project_dir / 'mdoc'
            frames_dir.mkdir(exist_ok=True, parents=True)
            mdoc_dir.mkdir(exist_ok=True, parents=True)

            source_movie_dir = Path(movies_glob).parent
            mdoc_files = glob.glob(mdocs_glob)
            
            if not mdoc_files:
                return {"success": False, "error": f"No .mdoc files found with pattern: {mdocs_glob}"}

            for mdoc_path_str in mdoc_files:
                mdoc_path = Path(mdoc_path_str)
                parsed_mdoc = self._parse_mdoc(mdoc_path)

                for section in parsed_mdoc['data']:
                    if 'SubFramePath' not in section:
                        continue
                    
                    original_movie_name = Path(section['SubFramePath'].replace('\\', '/')).name
                    prefixed_movie_name = f"{import_prefix}{original_movie_name}"
                    
                    # Update the SubFramePath in the parsed data
                    section['SubFramePath'] = prefixed_movie_name

                    # Create symlink to the raw movie file
                    source_movie_path = source_movie_dir / original_movie_name
                    link_path = frames_dir / prefixed_movie_name
                    
                    if not source_movie_path.exists():
                        # Log or handle missing movie files
                        print(f"Warning: Source movie not found: {source_movie_path}")
                        continue

                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)

                # Write the new, modified mdoc file
                new_mdoc_path = mdoc_dir / f"{import_prefix}{mdoc_path.name}"
                self._write_mdoc(parsed_mdoc, new_mdoc_path)
            
            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}