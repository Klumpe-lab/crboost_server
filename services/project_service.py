# services/project_service.py

import shutil
from pathlib import Path
from typing import Dict, Any

from services.data_import_service import DataImportService
from services.starfile_service import StarfileService

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
