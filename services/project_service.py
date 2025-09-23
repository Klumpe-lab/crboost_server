# services/project_service.py (Corrected)

import shutil
from pathlib import Path
from typing import Dict, Any

from .data_import_service import DataImportService
from .starfile_service import StarfileService
# A forward reference to avoid circular imports. The actual instance is passed in __init__.
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from backend import CryoBoostBackend

class ProjectService:
    def __init__(self, backend_instance: 'CryoBoostBackend'):
        self.backend = backend_instance
        self.data_importer = DataImportService()
        self.star_handler = StarfileService()

    async def create_new_project(
        self,
        project_dir: Path,
        movies_glob: str,
        mdocs_glob: str,
        import_prefix: str
    ) -> Dict[str, Any]:
        """
        Creates the project directory, imports data, and initializes the Relion pipeline.
        """
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "Schemes").mkdir(exist_ok=True)

        import_result = await self.data_importer.setup_project_data(
            project_dir, movies_glob, mdocs_glob, import_prefix
        )
        if not import_result["success"]:
            return import_result

        # FIX: Changed 'relion_pipeliner --do_projdir' to the correct command.
        # The '.' tells Relion to create the project in the current working directory.
        command = "relion --tomo --do_projdir . --dont_open_gui"
        
        
        result = await self.backend.run_shell_command(command, cwd=project_dir)
        if not result["success"]:
            # Provide a more detailed error message to the user
            detailed_error = f"Failed to initialize Relion project: {result['error']}\n{result['output']}"
            return {"success": False, "error": detailed_error}
        
        return {"success": True, "message": "Project created and initialized."}

    async def apply_scheme_template(
        self,
        project_dir: Path,
        template_path: Path,
        user_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Copies a scheme template into the project and applies user parameters to job.star files.
        """
        if not template_path.is_dir():
            return {"success": False, "error": f"Scheme template path not found or not a directory: {template_path}"}
            
        scheme_name = template_path.name
        project_scheme_dir = project_dir / "Schemes" / scheme_name
        
        try:
            shutil.copytree(template_path, project_scheme_dir, dirs_exist_ok=True)
            
            for job_dir in project_scheme_dir.iterdir():
                job_star_path = job_dir / "job.star"
                if job_dir.is_dir() and job_star_path.exists():
                    job_data = self.star_handler.read(job_star_path)
                    
                    if 'joboptions_values' in job_data:
                        params_df = job_data['joboptions_values']
                        for key, value in user_params.items():
                            if key in params_df['rlnJobOptionVariable'].values:
                                params_df.loc[params_df['rlnJobOptionVariable'] == key, 'rlnJobOptionValue'] = str(value)
                        job_data['joboptions_values'] = params_df
                        self.star_handler.write(job_data, job_star_path)

            return {"success": True, "message": f"Scheme '{scheme_name}' applied."}
        except Exception as e:
            return {"success": False, "error": str(e)}