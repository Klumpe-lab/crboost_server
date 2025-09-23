from pydantic import BaseModel, Field
from pathlib import Path
import uuid

class User(BaseModel):
    """Represents an authenticated user."""
    username: str

class Job(BaseModel):
    """Represents a single SLURM job tracked by the server."""
    owner: str  # The username of the user who submitted the job
    internal_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    slurm_id: int
    status: str = "PENDING"
    log_file: Path
    log_content: str = ""

    class Config:
        # Allow Path objects in the model
        arbitrary_types_allowed = True