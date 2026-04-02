from __future__ import annotations
from typing import ClassVar, List, Set, Tuple
from pydantic import Field

from services.jobs._base import AbstractJobParams
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class ImportMoviesParams(AbstractJobParams):
    job_type: JobType = Field(default=JobType.IMPORT_MOVIES)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.IMPORT
    RELION_JOB_TYPE: ClassVar[str] = "relion.importtomo"
    IS_CONTINUE: ClassVar[bool] = True

    USER_PARAMS: ClassVar[Set[str]] = {"optics_group_name", "do_at_most"}

    INPUT_SCHEMA: ClassVar[List[InputSlot]] = []
    OUTPUT_SCHEMA: ClassVar[List[OutputSlot]] = [
        OutputSlot(key="output_star", produces=JobFileType.TILT_SERIES_STAR, path_template="tilt_series.star")
    ]

    # Job-specific parameters
    optics_group_name: str = "opticsGroup1"
    do_at_most: int = Field(default=-1)

    def _get_job_specific_options(self) -> List[Tuple[str, str]]:
        """Import uses relative paths - RELION runs from project root."""
        frames_dir = self.frames_dir
        if frames_dir.exists():
            eer_files = list(frames_dir.glob("*.eer"))
            mrc_files = list(frames_dir.glob("*.mrc"))
            tiff_files = list(frames_dir.glob("*.tiff")) + list(frames_dir.glob("*.tif"))

            if eer_files:
                frame_ext = "*.eer"
            elif mrc_files:
                frame_ext = "*.mrc"
            elif tiff_files:
                frame_ext = "*.tiff"
            else:
                frame_ext = "*.eer"
        else:
            frame_ext = "*.eer"

        frames_pattern = f"./frames/{frame_ext}"
        mdoc_pattern = "./mdoc/*.mdoc"

        return [
            ("movie_files", frames_pattern),
            ("images_are_motion_corrected", "No"),
            ("mdoc_files", mdoc_pattern),
            ("optics_group_name", self.optics_group_name),
            ("prefix", ""),
            ("angpix", str(self.pixel_size)),
            ("kV", str(int(self.voltage))),
            ("Cs", str(self.spherical_aberration)),
            ("Q0", str(self.amplitude_contrast)),
            ("dose_rate", str(self.dose_per_tilt)),
            ("dose_is_per_movie_frame", "No"),
            ("tilt_axis_angle", str(self.tilt_axis_angle)),
            ("mtf_file", ""),
            ("flip_tiltseries_hand", "Yes" if self.acquisition.invert_defocus_hand else "No"),
        ]

    def _get_queue_options(self) -> List[Tuple[str, str]]:
        """Import jobs defaults to local run, but includes correct keys for consistency."""
        slurm_config = self.get_effective_slurm_config()

        options = [
            ("do_queue", "No"),
            ("queuename", slurm_config.partition),
            ("qsub", "sbatch"),
            ("qsubscript", "qsub.sh"),
            ("min_dedicated", "1"),
            ("other_args", ""),
        ]

        options.extend(list(slurm_config.to_qsub_extra_dict().items()))
        return options

    def get_tool_name(self) -> str:
        return "relion_import"
