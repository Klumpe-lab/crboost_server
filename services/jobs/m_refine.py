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
# Last in MCore's help text, after the volume warp.
TILTMOVIES_FLAG = "--refine_tiltmovies"


class MRefineParams(AbstractJobParams):
    """
    One round of Warp's M on a particle set: MTools sets the population up, MCore refines the tilt series' Warp model,
    the particles' poses and the maps of every species in it together.

    A first round reads a Refine 3D (its optimisation set and its unfiltered half maps) and a Mask job. M's sources
    are copies, in this job's folder, of the Warp files of the tilt series the particles sit on; M writes its
    refinements into them. Its species is the Refine 3D's halves at their own pixel size, the Mask job's surface made
    binary again (M grows and smooths it with the resolution), the symmetry and diameter from the species. A
    follow-up round reads an earlier M refine and copies its population, sources and every species. It may add this
    job's species, from a Refine 3D and a Mask job as a first round does: M then fits each tilt series' model from
    all species at once, so an abundant species steadies a sparse one in the same tilt series (roadmap 73). Before
    its round a job can change its own species: expand its symmetry into copies, resample its particles' trajectories
    in time, give it a new mask. *Hand back only* runs no round: it hands back one species as the round it continues
    left it. M's versioning is off: the job folder is the round's state, and another round from the same state is
    another follow-up of the same job.

    Out, for the job's own species: M's half maps in the Refine 3D's box, for PostProcess in the masks the Refine 3D's
    went through; M's filtered map in that box; M's particles as a RELION 5 set without images. Every other species of
    the population gets the same in `species/<name>/`. Then what M changed per tilt series (`m_changes.json`). The
    defaults are M's own: a round with no switch on refines nothing, which is the check of the import (M's map from
    the particles as they came, with Warp's own CTF).
    """

    job_type: JobType = Field(default=JobType.M_REFINE)
    JOB_CATEGORY: ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str] = "relion.external"
    CONFIG_PREAMBLE: ClassVar[str] = (
        "One M round a job. The first reads a Refine 3D and a Mask job and builds M's population in this job's "
        "folder (copies of the tilt series' Warp files, the species from the Refine 3D's halves); a follow-up "
        "(*Continue an M job*) copies an earlier round's and runs one more round with its own switches, and can "
        "add this job's species to the population, so the tilt series are fitted from every species in them. Each "
        "round's half maps come out in the Refine 3D's box, so a PostProcess reads them in the masks the trunk "
        "used. A round of a 600 Å capsid at 1.55 Å took about an hour on one B200, most of it M rebuilding the "
        "map on one CPU core; a species at 774 px writes about 18 GB a round."
    )
    USER_PARAMS: ClassVar[set[str]] = {
        "continue_from_m",
        "add_species",
        "population_series_only",
        "hand_back_only",
        "symmetry",
        "diameter_ang",
        "temporal_samples",
        "expand_symmetry",
        "expand_from",
        "expand_to",
        "helical_units",
        "helical_twist",
        "helical_rise",
        "update_mask",
        "mask_dilate",
        "mask_center",
        "refine_imagewarp",
        "refine_particles",
        "refine_stageangles",
        "refine_mag",
        "refine_volumewarp",
        "refine_tiltmovies",
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
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = (
        "Round",
        SPECIES_GROUP,
        "Before the round",
        "Geometry",
        "CTF",
        "Run",
        "Compute",
    )
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation",
            accepts=[JobFileType.OPTIMISATION_SET_STAR],
            preferred_source="refine3d",
            when="makes_species",
            description="The particles and their tomograms (a Refine 3D's set)",
        ),
        InputSlot(
            key="input_half_map",
            accepts=[JobFileType.HALF_MAP],
            preferred_source="refine3d",
            when="makes_species",
            description="The unfiltered half maps M's species starts from",
        ),
        InputSlot(
            key="input_mask",
            accepts=[JobFileType.MASK],
            preferred_source="maskCreate",
            when="reads_mask",
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
    add_species: bool = Field(
        default=False,
        description="Bring this job's species into the population, from a Refine 3D and a Mask job as a first round "
        "makes one, and run the round on every species. M fits each tilt series' warps, defocus and the rest from "
        "all of them at once: an abundant species (the ribosome, hundreds a tilt series) steadies a sparse one in "
        "the same tilt series (a capsid, one or two). The species must not be in the population yet, and its half "
        "maps must be at the population's pixel size",
        title="Add this job's species",
        json_schema_extra=form("Round"),
    )
    population_series_only: bool = Field(
        default=True,
        description="On: of the new species' particles, only those on tilt series the population already has. Off: "
        "its other tilt series join the population as sources too, one more source per Warp settings file (untried: "
        "M's handling of a second source on a settings file another source has is not known yet, roadmap 73 §1)",
        title="Only the population's tilt series",
        json_schema_extra=form("Round"),
    )
    hand_back_only: bool = Field(
        default=False,
        description="Run no round: hand back this job's species as the round it continues left it (its half maps "
        "in the Refine 3D's box, its filtered map, its set), as the job's own outputs. For a species another job's "
        "round refined beside its own; takes seconds",
        title="Hand back only",
        json_schema_extra=form("Round"),
    )

    # From the species (a job that makes M's species: a follow-up keeps its parent's)
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
        "diameter in pixels, and crops nothing: a 600 Å particle at 1.55 Å is a 774 px box",
        title="Diameter",
        json_schema_extra=form(SPECIES_GROUP, S, "Å"),
    )
    temporal_samples: int = Field(
        default=0,
        ge=0,
        description="Samples of each particle's pose along the tilt series' exposure: with more than one, a particle "
        "moves and turns as the dose accumulates (M: 1 for small particles, up to 3 for very large ones). For a "
        "species the job makes, create_species -t; for its species in a follow-up, MTools resample_trajectories when "
        "the number changes. 0: as the species has it (1 for a new one). RELION's set holds one pose a particle: the "
        "set handed back carries the mean of the samples",
        title="Temporal samples",
        json_schema_extra=form("Before the round", S),
    )

    # Before the round: changes to the job's own species, in this job's copy
    expand_symmetry: bool = Field(
        default=False,
        description="Before the round, make each particle of this job's species one copy per symmetry operator "
        "(MTools expand_symmetry); M then refines every copy on its own, with the symmetry that is left (Keep). For "
        "the sub-units of an assembly in M's own model of the tilt series: a capsid's capsomers, a filament's units. "
        "The set handed back holds every copy, each naming its parent (cbParentUid, cbSubIndex)",
        title="Expand symmetry",
        json_schema_extra=form("Before the round"),
    )
    expand_from: str = Field(
        default="",
        description="The symmetry to expand, when it is part of the species' (--expand_from, e.g. C5 of an I1 "
        "capsid); empty: the species' own",
        title="Expand",
        json_schema_extra=form("Before the round", S),
    )
    expand_to: str = Field(
        default="",
        description="The symmetry the species keeps after the expansion (--expand_to, e.g. C1); empty: M's choice, "
        "which the log and the round's record state",
        title="Keep",
        json_schema_extra=form("Before the round", S),
    )
    helical_units: int = Field(
        default=1,
        ge=1,
        description="Units of a helical symmetry to expand (--helical_units); 1: a point group's expansion, more: "
        "a helix's, with its twist and rise",
        title="Helical units",
        json_schema_extra=form("Before the round", S),
    )
    helical_twist: float = Field(
        default=0.0,
        description="Twist of the helical symmetry to expand (--helical_twist)",
        title="Twist",
        json_schema_extra=form("Before the round", S, "°"),
    )
    helical_rise: float = Field(
        default=0.0,
        description="Rise of the helical symmetry to expand (--helical_rise)",
        title="Rise",
        json_schema_extra=form("Before the round", S, "Å"),
    )
    update_mask: bool = Field(
        default=False,
        description="Before the round, give this job's species a new mask: a Mask job's surface made binary, as a "
        "first round makes it (MTools update_mask pads it to the species' box). The mask input appears",
        title="New mask",
        json_schema_extra=form("Before the round"),
    )
    mask_dilate: int = Field(
        default=0,
        ge=0,
        description="Grow the new mask by this many voxels (update_mask --dilate)",
        title="Dilate",
        json_schema_extra=form("Before the round", S, "px"),
    )
    mask_center: bool = Field(
        default=False,
        description="Centre the species on the new mask's centre of mass (update_mask --center): its map and its "
        "particles' positions move with it, so the half maps handed back are centred on the masked part",
        title="Centre on the mask",
        json_schema_extra=form("Before the round"),
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
    refine_tiltmovies: bool = Field(
        default=False,
        description="Align each tilt's movie frames again, on the particles (--refine_tiltmovies). M reads every "
        "frame of every tilt: the raw movies (checked before M starts) and the gain the Warp settings name. Much "
        "slower than a round on the tilt averages; untried here",
        title="Tilt movies",
        json_schema_extra=form("Geometry"),
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

    # Set when a species is made (a first round, or a species added); a follow-up keeps its species' own.
    SPECIES_MAKING: ClassVar[frozenset[str]] = frozenset({"symmetry", "diameter_ang", "use_denoiser"})
    EXPANSION: ClassVar[frozenset[str]] = frozenset({"expand_from", "expand_to", "helical_units"})
    HELICAL: ClassVar[frozenset[str]] = frozenset({"helical_twist", "helical_rise"})
    NEW_MASK: ClassVar[frozenset[str]] = frozenset({"mask_dilate", "mask_center"})
    # What a round reads; a hand-back runs none.
    ROUND_ONLY: ClassVar[frozenset[str]] = frozenset(
        {
            "temporal_samples",
            "expand_symmetry",
            *EXPANSION,
            *HELICAL,
            *GEOMETRY_FLAGS,
            "refine_imagewarp",
            "refine_volumewarp",
            "refine_tiltmovies",
            "ctf_minresolution",
            *CTF_FLAGS,
            "iterations",
            "first_iteration_fraction",
            "min_particles",
            "weight_threshold",
            "perdevice",
            "cpu_memory",
            "other_args",
        }
    )

    @property
    def first_round(self) -> bool:
        """A round that builds M's population from a Refine 3D."""
        return not self.continue_from_m

    @property
    def adds_species(self) -> bool:
        """A follow-up that brings this job's species into the population it continues."""
        return self.continue_from_m and self.add_species and not self.hand_back_only

    @property
    def makes_species(self) -> bool:
        """A job that makes an M species from a Refine 3D (the inputs it has while this is True)."""
        return self.first_round or self.adds_species

    @property
    def hand_back(self) -> bool:
        """A follow-up that runs no round and hands its species back as the round it continues left it."""
        return self.continue_from_m and self.hand_back_only and not self.add_species

    @property
    def changes_mask(self) -> bool:
        """A follow-up that gives its species, already in the population, a new mask before its round."""
        return self.continue_from_m and not self.add_species and not self.hand_back_only and self.update_mask

    @property
    def reads_mask(self) -> bool:
        """The job reads a Mask job: for a species it makes, or for the new mask of its own."""
        return self.makes_species or self.changes_mask

    @property
    def helical_expansion(self) -> bool:
        return self.helical_units > 1

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
        if self.refine_tiltmovies:
            args.append(TILTMOVIES_FLAG)
        args += ["--ctf_minresolution", f"{self.ctf_minresolution:g}"]
        args += [CTF_FLAGS[name] for name in self.ctf_switches()]
        return args

    def expansion_args(self) -> list[str]:
        """MTools expand_symmetry's arguments besides the population and the species."""
        args = []
        if self.expand_from.strip():
            args += ["--expand_from", self.expand_from.strip()]
        if self.expand_to.strip():
            args += ["--expand_to", self.expand_to.strip()]
        if self.helical_expansion:
            args += ["--helical_units", str(self.helical_units)]
            args += ["--helical_twist", f"{self.helical_twist:g}", "--helical_rise", f"{self.helical_rise:g}"]
        return args

    def run_problems(self) -> list[str]:
        # What drivers/m_refine.py refuses before M runs, said before the submit.
        problems = []
        if self.continue_from_m and self.add_species and self.hand_back_only:
            problems.append(
                "'Add this job's species' and 'Hand back only' exclude each other: a hand-back runs no round"
            )
        if self.hand_back:
            return problems
        if self.makes_species:
            if self.diameter_ang <= 0:
                problems.append("no diameter: M sizes its species' box from it (the species sets it when it has one)")
            if not self.symmetry.strip():
                problems.append("no symmetry: C1 for none")
        if self.expand_symmetry:
            if self.helical_expansion and self.helical_twist == 0 and self.helical_rise == 0:
                problems.append("a helical expansion needs the helix's twist and rise")
            own = self.symmetry.strip().upper() if self.makes_species else ""
            if not self.helical_expansion and not self.expand_from.strip() and own == "C1":
                problems.append("expanding C1 makes no copies: name the symmetry to expand (Expand) or a helix")
        if self.refine_imagewarp.strip() and not IMAGEWARP_RE.match(self.refine_imagewarp.strip()):
            problems.append(f"image warp grid {self.refine_imagewarp!r} is not XxY (e.g. 3x3)")
        if self.refine_volumewarp.strip() and not VOLUMEWARP_RE.match(self.refine_volumewarp.strip()):
            problems.append(f"volume warp grid {self.refine_volumewarp!r} is not XxYxZxT (e.g. 1x1x1x20)")
        if self.ctf_defocusexhaustive and not self.ctf_defocus:
            problems.append("the exhaustive defocus search works only with Defocus on")
        return problems

    def shows_param(self, name: str) -> bool:
        if name == "add_species":
            return self.continue_from_m and not self.hand_back_only
        if name == "population_series_only":
            return self.adds_species
        if name == "hand_back_only":
            return self.continue_from_m and not self.add_species
        if name == "update_mask":
            return self.continue_from_m and not self.add_species and not self.hand_back_only
        if name in self.NEW_MASK:
            return self.changes_mask
        if name in self.SPECIES_MAKING:
            return self.makes_species
        if self.hand_back and name in self.ROUND_ONLY:
            return False
        if name in self.EXPANSION:
            return self.expand_symmetry
        if name in self.HELICAL:
            return self.expand_symmetry and self.helical_expansion
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
