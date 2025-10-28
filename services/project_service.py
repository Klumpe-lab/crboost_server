# services/project_service.py
import shutil
from pathlib import Path
from typing import Dict, Any
from services.starfile_service import StarfileService
import os
import glob



class DataImportService:
    """
    Handles the core logic of preparing raw data for a CryoBoost project.
    This includes parsing mdocs, creating symlinks, and rewriting mdocs with prefixes.
    """

    def _parse_mdoc(self, mdoc_path: Path) -> Dict[str, Any]:
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
                f.write("\n")

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
                    
                    section['SubFramePath'] = prefixed_movie_name

                    source_movie_path = source_movie_dir / original_movie_name
                    link_path = frames_dir / prefixed_movie_name
                    
                    if not source_movie_path.exists():
                        print(f"Warning: Source movie not found: {source_movie_path}")
                        continue

                    if not link_path.exists():
                        os.symlink(source_movie_path.resolve(), link_path)

                new_mdoc_path = mdoc_dir / f"{import_prefix}{mdoc_path.name}"
                self._write_mdoc(parsed_mdoc, new_mdoc_path)
            
            return {"success": True, "message": f"Imported {len(mdoc_files)} tilt-series."}
        except Exception as e:
            return {"success": False, "error": str(e)}


class ProjectService:
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()

    async def create_project_structure(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str
    ) -> Dict[str, Any]:
        """
        Creates the project directory structure and imports the raw data.
        Now with PRE-POPULATED qsub scripts!
        """
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "Schemes").mkdir(exist_ok=True)
            (project_dir / "Logs").mkdir(exist_ok=True)

            # Copy AND PRE-POPULATE qsub scripts
            await self._setup_qsub_templates(project_dir)
            
            import_result = await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )
            if not import_result["success"]:
                return import_result
            
            return {"success": True, "message": "Project directory structure created and data imported."}
        except Exception as e:
            return {"success": False, "error": f"Failed during directory setup: {str(e)}"}

    async def _setup_qsub_templates(self, project_dir: Path):
        """Copy qsub templates and replace placeholders with sensible defaults"""
        qsub_template_path = Path.cwd() / "config" / "qsub"
        project_qsub_path = project_dir / "qsub"
        
        if qsub_template_path.is_dir():
            # Copy all templates
            shutil.copytree(qsub_template_path, project_qsub_path, dirs_exist_ok=True)
            
            # Pre-populate the main qsub script we use
            main_qsub_script = project_qsub_path / "qsub_cbe_warp.sh"
            if main_qsub_script.exists():
                await self._prepopulate_qsub_script(main_qsub_script)
                
            print(f"[PROJECT] Pre-populated qsub scripts in {project_qsub_path}")

    async def _prepopulate_qsub_script(self, qsub_script_path: Path):
        """Replace XXXextraXXXX placeholders with sensible defaults"""
        with open(qsub_script_path, 'r') as f:
            content = f.read()
        
        # Replace all the extra placeholders with sensible defaults
        replacements = {
            "XXXextra1XXX": "1",      # nodes
            "XXXextra2XXX": "",       # mpi_per_node (empty = let relion handle it)
            "XXXextra3XXX": "g",      # partition (GPU)
            "XXXextra4XXX": "1",      # gpus  
            "XXXextra5XXX": "16G",    # memory
            "XXXthreadsXXX": "8",     # threads
        }
        
        for placeholder, value in replacements.items():
            content = content.replace(placeholder, value)
        
        # Write back
        with open(qsub_script_path, 'w') as f:
            f.write(content)
        
        print(f"[QSUB] Pre-populated {qsub_script_path} with defaults")
