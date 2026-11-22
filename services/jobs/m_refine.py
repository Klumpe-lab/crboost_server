from __future__ import annotations

import re
from typing import ClassVar

from pydantic import Field

from services.io_slots import InputSlot, JobFileType, OutputSlot
from services.jobs._base import L, M, S, SPECIES_GROUP, AbstractJobParams, form
from services.models_base import JobCategory, JobType

IMAGEWARP_RE = re.compile(r"^\d+x\d+$")
VOLUMEWARP_RE = re.compile(r"^\d+x\d+x\d+x\d+$")

# MCore's CTF switches, field -> flag. M refines none of them for a species coarser than ctf_minresolution.
CTF_FLAGS: dict[str, str] = {
    "ctf_defocus": "--ctf_defocus",
    "ctf_defocusexhaustive": "--ctf_defocusexhaustive",
    "ctf_cs": "--ctf_cs",
    "ctf_zernike3": "--ctf_zernike3",
    "ctf_zernike5": "--ctf_zernike5",
    "ctf_zernike2": "--ctf_zernike2",
    "ctf_zernike4": "--ctf_zernike4",
    "ctf_phase": "--ctf_phase",
}
GEOMETRY_FLAGS: dict[str, str] = {
    "refine_particles": "--refine_particles",
    "refine_stageangles": "--refine_stageangles",
    "refine_mag": "--refine_mag",
}


class MRefineParams(AbstractJobParams):
    """
    One round of Warp's M on a particle set: MTools sets the population up, MCore refines the tilt series' Warp model,
    the particles' poses and the map together.

    A first round reads a Refine 3D (its optimisation set and its unfiltered half maps) and a Mask job. M's sources
    are copies, in this job's folder, of the Warp files of the tilt series the particles sit on; M writes its
    refinements into them. Its species is the Refine 3D's halves at their own pixel size, the Mask job's surface made
    binary again (M grows and smooths it with the resolution), the symmetry and diameter from the species, one
    temporal sample. A follow-up round reads an earlier M refine and copies its population, sources and species.
    Either way the job runs one MCore round with its own switches. M's versioning is off: the job folder is the
    round's state, and another round from the same state is another follow-up of the same job.

    Out: M's half maps in the Refine 3D's box, for PostProcess in the masks the Refine 3D's went through; M's filtered
    map in that box; M's particles as a RELION 5 set without images; what M changed per tilt series
    (`m_changes.json`). The defaults are M's own: a round with no switch on refines nothing, which is the check of the
    import (M's map from the particles as they came, with Warp's own CTF).
    """

    job_type: JobType = Field(default=JobType.M_REFINE)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    CONFIG_PREAMBLE: ClassVar[str] = (
        "One M round a job. The first reads a Refine 3D and a Mask job and builds M's population in this job's "
        "folder (copies of the tilt series' Warp files, the species from the Refine 3D's halves); a follow-up "
        "(*Continue an M job*) copies an earlier round's and runs one more round with its own switches. Each "
        "round's half maps come out in the Refine 3D's box, so a PostProcess reads them in the masks the trunk "
        "used. A round of a 600 Å capsid at 1.55 Å took about an hour on one B200, most of it M rebuilding the "
        "map on one CPU core; a species at 774 px writes about 18 GB a round."
    )
    USER_PARAMS: ClassVar[set[str]] = {
        "continue_from_m",
        "symmetry",
        "diameter_ang",
        "refine_imagewarp",
        "refine_particles",
        "refine_stageangles",
        "refine_mag",
        "refine_volumewarp",
        "ctf_minresolution",
        "ctf_defocus",
        "ctf_defocusexhaustive",
        "ctf_cs",
        "ctf_zernike3",
        "ctf_zernike5",
        "ctf_zernike2",
        "ctf_zernike4",
        "ctf_phase",
        "iterations",
        "first_iteration_fraction",
        "min_particles",
        "weight_threshold",
        "use_denoiser",
        "perdevice",
        "cpu_memory",
        "other_args",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = ("Round", SPECIES_GROUP, "Geometry", "CTF", "Run", "Compute")
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation",
            accepts=[JobFileType.OPTIMISATION_SET_STAR],
            preferred_source="refine3d",
            when="first_round",
            description="The particles and their tomograms (a Refine 3D's set)",
        ),
        InputSlot(
            key="input_half_map",
            accepts=[JobFileType.HALF_MAP],
            preferred_source="refine3d",
            when="first_round",
            description="The unfiltered half maps M's species starts from",
        ),
        InputSlot(
            key="input_mask",
            accepts=[JobFileType.MASK],
            preferred_source="maskCreate",
            when="first_round",
            description="A Mask job: M's mask is its surface, binary",
        ),
        InputSlot(
            key="input_m",
            accepts=[JobFileType.M_ROUND],
            preferred_source="mRefine",
            when="continue_from_m",
            description="The M round this one continues",
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        # M's halves cropped (or padded) to the Refine 3D's box; PostProcess reads half1 and finds half2 beside it.
        OutputSlot(key="output_half_map", produces=JobFileType.HALF_MAP, path_template="half1.mrc"),
        OutputSlot(key="output_map", produces=JobFileType.REFERENCE_MAP, path_template="m_filtered.mrc"),
        # Positions and poses without images: the stacks were cut with another model.
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="optimisation_set.star",
            coordinates_only=True,
        ),
        OutputSlot(key="output_m", produces=JobFileType.M_ROUND, path_template="m_round.json"),
    ]

    continue_from_m: bool = Field(
        default=False,
        description="Off: a first round, from a Refine 3D and a Mask job. On: one more round of an earlier M refine, "
        "from a copy of its population, sources and species",
        title="Continue an M job",
        json_schema_extra=form("Round"),
    )

    # From the species (first round only: a follow-up keeps its parent's species)
    symmetry: str = Field(
        default="C1",
        description="Point symmetry of M's species (MTools create_species -s)",
        title="Symmetry",
        json_schema_extra=form(SPECIES_GROUP, M),
    )
    diameter_ang: float = Field(
        default=0.0,
        ge=0,
        description="The particle's diameter (create_species -d). M sizes its species' box from it, twice the "
        "diameter in pixels, and crops nothing: the capsid's 600 Å at 1.55 Å is a 774 px box",
        title="Diameter",
        json_schema_extra=form(SPECIES_GROUP, S, "Å"),
    )

    # Geometry
    refine_imagewarp: str = Field(
        default="",
        description="Refine the image warp on a grid of X×Y nodes per tilt (--refine_imagewarp, e.g. 3x3); empty = "
        "don't. An existing grid (miss-alignment's) is resized to it, not reset",
        title="Image warp grid",
        json_schema_extra=form("Geometry", M),
    )
    refine_particles: bool = Field(
        default=False,
        description="Refine the particles' poses (--refine_particles)",
        title="Particle poses",
        json_schema_extra=form("Geometry"),
    )
    refine_stageangles: bool = Field(
        default=False,
        description="Refine each tilt's stage angles (--refine_stageangles). RELION's tilt-series star cannot hold "
        "them: Warp Export and RELION's tilt-image programs refuse tilt series refined so (no RELION route)",
        title="Stage angles",
        json_schema_extra=form("Geometry"),
    )
    refine_mag: bool = Field(
        default=False,
        description="Refine the anisotropic magnification per tilt series (--refine_mag). RELION's tilt-series "
        "star holds none: Warp applies it where it back-projects, RELION reconstructs without it",
        title="Magnification",
        json_schema_extra=form("Geometry"),
    )
    refine_volumewarp: str = Field(
        default="",
        description="Refine the volume warp on a grid of X×Y×Z×T nodes (--refine_volumewarp, e.g. 1x1x1x20); "
        "empty = don't",
        title="Volume warp grid",
        json_schema_extra=form("Geometry", M),
    )

    # CTF
    ctf_minresolution: float = Field(
        default=8.0,
        gt=0,
        description="M refines the CTF only of a species finer than this (--ctf_minresolution), and says nothing "
        "when it skips it. At the default 8 Å a species at 8.2 Å gets no CTF refinement",
        title="CTF from",
        json_schema_extra=form("CTF", S, "Å"),
    )
    ctf_defocus: bool = Field(
        default=False,
        description="Refine each tilt's defocus by a local search (--ctf_defocus)",
        title="Defocus",
        json_schema_extra=form("CTF"),
    )
    ctf_defocusexhaustive: bool = Field(
        default=False,
        description="Search the defocus on a coarser, wider grid in the first sub-iteration (--ctf_defocusexhaustive)",
        title="Exhaustive first",
        json_schema_extra=form("CTF"),
    )
    ctf_cs: bool = Field(
        default=False,
        description="Refine the spherical aberration per tilt series (--ctf_cs; M: also a proxy for pixel size)",
        title="Cs",
        json_schema_extra=form("CTF"),
    )
    ctf_zernike3: bool = Field(
        default=False,
        description="Zernike polynomials of 3rd order: beam tilt, trefoil (--ctf_zernike3, fast)",
        title="Zernike 3rd",
        json_schema_extra=form("CTF"),
    )
    ctf_zernike5: bool = Field(
        default=False,
        description="Zernike polynomials of 5th order (--ctf_zernike5, fast)",
        title="Zernike 5th",
        json_schema_extra=form("CTF"),
    )
    ctf_zernike2: bool = Field(
        default=False,
        description="Zernike polynomials of 2nd order (--ctf_zernike2, slow)",
        title="Zernike 2nd",
        json_schema_extra=form("CTF"),
    )
    ctf_zernike4: bool = Field(
        default=False,
        description="Zernike polynomials of 4th order (--ctf_zernike4, slow)",
        title="Zernike 4th",
        json_schema_extra=form("CTF"),
    )
    ctf_phase: bool = Field(
        default=False,
        description="Refine the phase shift (--ctf_phase; phase-plate data only)",
        title="Phase shift",
        json_schema_extra=form("CTF"),
    )

    # Run
    iterations: int = Field(
        default=3,
        ge=0,
        description="Sub-iterations of the round (--iter). 0 refines nothing: M's map from the particles as they "
        "came, the check of the import",
        title="Sub-iterations",
        json_schema_extra=form("Run", S),
    )
    first_iteration_fraction: float = Field(
        default=1.0,
        gt=0,
        le=1,
        description="The fraction of the resolution used for alignment in the first sub-iteration, rising to 1 by "
        "the last (--first_iteration_fraction)",
        title="First fraction",
        json_schema_extra=form("Run", S),
    )
    min_particles: int = Field(
        default=1,
        ge=1,
        description="Refine only tilt series with at least this many particles in view (--min_particles)",
        title="Fewest particles",
        json_schema_extra=form("Run", S),
    )
    weight_threshold: float = Field(
        default=0.05,
        gt=0,
        description="Refine each tilt up to the resolution where its exposure weight falls to this "
        "(--weight_threshold)",
        title="Weight threshold",
        json_schema_extra=form("Run", S),
    )
    use_denoiser: bool = Field(
        default=True,
        description="Regularise the map with M's denoiser (M's default); off: a low-pass instead "
        "(create_species --dont_use_denoiser). Set when the species is made; a follow-up keeps its parent's",
        title="Denoiser",
        json_schema_extra=form("Run"),
    )

    # Compute
    perdevice: int = Field(
        default=1,
        ge=1,
        description="M's worker processes per GPU (--perdevice); one kept a B200 45–60 % busy in 11 GB",
        title="Workers per GPU",
        json_schema_extra=form("Compute", S),
    )
    cpu_memory: bool = Field(
        default=False,
        description="Keep the particle images in host memory instead of the GPU's (--cpu_memory)",
        title="Images in RAM",
        json_schema_extra=form("Compute"),
    )
    other_args: str = Field(
        default="",
        description="Extra MCore flags, appended verbatim",
        title="Extra arguments",
        json_schema_extra=form("Compute", L),
    )

    FIRST_ROUND_ONLY: ClassVar[frozenset[str]] = frozenset({"symmetry", "diameter_ang", "use_denoiser"})

    @property
    def first_round(self) -> bool:
        """A round that builds M's population from a Refine 3D (the inputs it has while this is True)."""
        return not self.continue_from_m

    def ctf_switches(self) -> list[str]:
        """The CTF switches that are on, by field name."""
        return [name for name in CTF_FLAGS if getattr(self, name)]

    def mcore_switches(self) -> list[str]:
        """MCore's arguments for what the round refines and how, in the order of its help text."""
        args = ["--iter", str(self.iterations)]
        if self.first_iteration_fraction != 1.0:
            args += ["--first_iteration_fraction", f"{self.first_iteration_fraction:g}"]
        if self.min_particles != 1:
            args += ["--min_particles", str(self.min_particles)]
        if self.cpu_memory:
            args.append("--cpu_memory")
        if self.weight_threshold != 0.05:
            args += ["--weight_threshold", f"{self.weight_threshold:g}"]
        if self.refine_imagewarp.strip():
            args += ["--refine_imagewarp", self.refine_imagewarp.strip()]
        args += [flag for name, flag in GEOMETRY_FLAGS.items() if getattr(self, name)]
        if self.refine_volumewarp.strip():
            args += ["--refine_volumewarp", self.refine_volumewarp.strip()]
        args += ["--ctf_minresolution", f"{self.ctf_minresolution:g}"]
        args += [CTF_FLAGS[name] for name in self.ctf_switches()]
        return args

    def run_problems(self) -> list[str]:
        # What drivers/m_refine.py refuses before M runs, said before the submit.
        problems = []
        if self.first_round:
            if self.diameter_ang <= 0:
                problems.append("no diameter: M sizes its species' box from it (the species sets it when it has one)")
            if not self.symmetry.strip():
                problems.append("no symmetry: C1 for none")
        if self.refine_imagewarp.strip() and not IMAGEWARP_RE.match(self.refine_imagewarp.strip()):
            problems.append(f"image warp grid {self.refine_imagewarp!r} is not XxY (e.g. 3x3)")
        if self.refine_volumewarp.strip() and not VOLUMEWARP_RE.match(self.refine_volumewarp.strip()):
            problems.append(f"volume warp grid {self.refine_volumewarp!r} is not XxYxZxT (e.g. 1x1x1x20)")
        if self.ctf_defocusexhaustive and not self.ctf_defocus:
            problems.append("the exhaustive defocus search works only with Defocus on")
        return problems

    def shows_param(self, name: str) -> bool:
        if name in self.FIRST_ROUND_ONLY:
            return self.first_round
        if name == "ctf_defocusexhaustive":
            return self.ctf_defocus
        return True

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        return [
            ("in_optimisation", str(self.paths.get("input_optimisation", ""))),
            ("fn_half1", str(self.paths.get("input_half_map", ""))),
            ("fn_mask", str(self.paths.get("input_mask", ""))),
            ("in_m", str(self.paths.get("input_m", ""))),
        ]

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "warp_aretomo"
