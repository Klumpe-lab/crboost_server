### **Analysis of the Old CryoBoost & Key Takeaways**

Looking at the old project, especially `libpipe.py` and `librw.py`, we can see a clear pattern:

1.  **Configuration-Driven:** A central `conf.yaml` defines system paths, job types, and parameter aliases. This is a good concept we should keep.

2.  **Command Generation:** The `pipe` class is a massive command-builder. It pieces together `ssh`, `relion_schemer`, and `relion_pipeliner` commands using string replacement (`XXXJOBIDXXX`). This can be refactored and be made more well-strucutred via data models.

3.  **Data Parsing:** The `librw.py` file is full of essential logic for reading `.star` files, `.mdoc` files, and YAML configs. This code is valuable and can be adapted.


---
## The Roadmap: A Phased Approach

### **Phase 1: Foundation - Data Modeling with Pydantic**

We will define what we are working _with_ before adapting any logic. This is the most crucial step to avoid the  numerous "untyped string" methods you mentioned. 

1.  **Define the `JobDefinition`:**
    * Parse the `job.star` templates from the `config/Schemes` directory into a Pydantic model.
    * This model will capture the job type (e.g., `fsMotionAndCtf`), its name, and a list of its parameters (also Pydantic models).
    * **Example `Parameter` model:** `name: str`, `label: str`, `value: Union[str, int, float, bool]`, `description: Optional[str] = None`.
    * This gives us a type-safe list of all available "job templates."

2.  **Define the `Project` and `JobInstance`:**
    * A `Project` model will contain project-level information: a unique ID, a name, the filesystem path, and a list of `JobInstance` objects.
    * A `JobInstance` represents a specific job *within* a project. It will have its own unique ID, link back to its `JobDefinition`, store the user-configured parameter values, and track its status (`PENDING`, `RUNNING`, `COMPLETED`), SLURM ID, and output paths. It will also define its dependencies (e.g., `depends_on: Optional[UUID] = None`).

3.  **Define Configuration Models:**
    * Create Pydantic models that mirror the structure of `conf.yaml`. This allows us to load the config into a validated, auto-completable Python object instead of a raw dictionary.

**Outcome of Phase 1:** We have a new `models.py` file that is the bedrock of the entire application. All communication between the UI, backend, and execution logic will use these models. This eliminates a huge class of bugs.

---
### **Phase 2: Abstracting the Current Logic - The "Services" Layer**

Now we refactor the useful parts of the old codebase into clean, independent "services" that operate on our new Pydantic models. This isolates external interactions.

1.  **Create a `StarfileService`:**
    * Move the STAR file parsing logic from `librw.py` into a new `starfile_service.py`.
    * Create functions like `parse_job_definition(path: Path) -> JobDefinition` and `write_job_star(job_instance: JobInstance) -> Path`.
    * This service knows how to read and write `.star` files, but it doesn't know anything about SLURM or `relion_pipeliner`.

2.  **Create a `PipelinerService` (The Command-Builder, Reborn):**
    * This would the structured replacement for the string-concatenation based methods in `libpipe.py`.
    * It will have high-level functions like `schedule_job(project: Project, job: JobInstance)`.
    * Inside this function, it will use the validated data from the Pydantic models to safely construct the `relion_pipeliner` command using `subprocess`, without any messy string replacement. It will know how to format arguments like `--addJobOptions 'rlnPixelSize == 1.23'`.

**Outcome of Phase 2:** We have a clean separation of concerns. The backend orchestrates, but dedicated services handle the "dirty work" of file I/O and command execution in a structured, testable way.

---
### **Phase 3: Implementing Project Lifecycle Management**

With the foundation in place, we build the core logic for managing scientific projects.

1.  **Backend Project State:**
    * The `CryoBoostBackend` will maintain a dictionary of active projects: `self.projects: Dict[UUID, Project]`.
    * Implement methods to **persist and load** this state to/from a file (e.g., a `projects.json` on disk). This ensures the server can be restarted without losing track of all ongoing work.

2.  **"Hydrating a Project":**
    * Create a backend function `load_project_from_disk(path: Path) -> Project`.
    * This function will scan a Relion project directory, parse the `default_pipeline.star` to identify completed/running jobs, and construct our Pydantic `Project` and `JobInstance` models from that information. This is how you "resume" work.

3.  **API Endpoints:**
    * Create new FastAPI endpoints in `main.py` for project management:
        * `POST /api/projects` (Create a new project)
        * `GET /api/projects/{project_id}` (Get project status)
        * `POST /api/projects/{project_id}/jobs` (Add a new job to the workflow)
        * `POST /api/projects/{project_id}/run` (Start the pipeline)

**Outcome of Phase 3:** The server is no longer just a job dispatcher; it's a stateful project manager that understands the structure and dependencies of a full cryo-ET workflow.

---
### **Phase 4: UI**

Make the UI more dynamic and accommodate live tracking of jobs, user authentication and "resume project" functionality.



