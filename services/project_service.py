import shutil
from pathlib import Path
from typing import Dict, Any

from .data_import_service import DataImportService
from .starfile_service import StarfileService
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

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
        Now also creates Logs and copies qsub scripts to match old CryoBoost.
        """
        try:
            project_dir.mkdir(parents=True, exist_ok=True)
            (project_dir / "Schemes").mkdir(exist_ok=True)
            
            # --- NEW: Add these two steps to match the old libpipe.py ---
            (project_dir / "Logs").mkdir(exist_ok=True)
            
            qsub_template_path = Path.cwd() / "config" / "qsub"
            if qsub_template_path.is_dir():
                shutil.copytree(qsub_template_path, project_dir / "qsub", dirs_exist_ok=True)
            # -----------------------------------------------------------

            import_result = await self.data_importer.setup_project_data(
                project_dir, movies_glob, mdocs_glob, import_prefix
            )
            if not import_result["success"]:
                return import_result
            
            return {"success": True, "message": "Project directory structure created and data imported."}
        except Exception as e:
            return {"success": False, "error": f"Failed during directory setup: {str(e)}"}
