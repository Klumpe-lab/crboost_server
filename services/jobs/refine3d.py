from __future__ import annotations
from typing import ClassVar
from pydantic import Field

from services.jobs._base import L, M, S, AbstractJobParams, SymmetryGroup, form
from services.models_base import JobType, JobCategory
from services.io_slots import InputSlot, OutputSlot, JobFileType


class Refine3DParams(AbstractJobParams):
    """
    Gold-standard 3D refinement: relion_refine_mpi --auto_refine --split_random_halves.
    Runs until RELION's own convergence criterion; the final map has no iteration tag.
    """

    job_type       : JobType               = Field(default=JobType.REFINE3D)
    JOB_CATEGORY   : ClassVar[JobCategory] = JobCategory.EXTERNAL
    RELION_JOB_TYPE: ClassVar[str]         = "relion.external"
    USER_PARAMS    : ClassVar[set[str]]    = {
        "healpix_order",
        "auto_local_healpix_order",
        "offset_range",
        "offset_step",
        "sigma_ang",
        "helix",
        "helical_outer_diameter",
        "sigma_tilt",
        "sigma_psi",
        "sigma_rot",
        "keep_tilt_prior_fixed",
        "ignore_helical_symmetry",
        "helical_twist",
        "helical_rise",
        "helical_nr_asu",
        "helical_z_percentage",
        "symmetry",
        "ini_high",
        "low_resol_join_halves",
        "particle_diameter",
        "reference_from_template",
        "reference_path",
        "solvent_mask_path",
        "mask_from_job",
        "solvent_correct_fsc",
        "flatten_solvent",
        "zero_mask",
        "firstiter_cc",
        "do_ctf",
        "do_norm",
        "do_scale",
        "dont_combine_weights_via_disc",
        "pad",
        "use_gpu",
        "preread_images",
        "mpi_ranks",
        "threads",
        "pool",
        "random_seed",
    }
    PARAM_GROUPS: ClassVar[tuple[str, ...]] = (
        "Refinement", "Reference & mask", "Angles & offsets", "Helical", "CTF & scale", "Compute",
    )
    # What only a filament refinement reads (helix on), and what only one that imposes its symmetry reads.
    HELICAL_PARAMS: ClassVar[frozenset[str]] = frozenset(
        {
            "helical_outer_diameter",
            "sigma_tilt",
            "sigma_psi",
            "sigma_rot",
            "keep_tilt_prior_fixed",
            "ignore_helical_symmetry",
        }
    )
    HELICAL_SYMMETRY_PARAMS: ClassVar[frozenset[str]] = frozenset(
        {"helical_twist", "helical_rise", "helical_nr_asu", "helical_z_percentage"}
    )
    INPUT_SCHEMA: ClassVar[list[InputSlot]] = [
        InputSlot(
            key="input_optimisation",
            accepts=[JobFileType.OPTIMISATION_SET_STAR],
            preferred_source="subtomoExtraction",
            needs_extracted=True,
        ),
        # A job's map, unless the species' template is the reference (reference_from_template).
        InputSlot(
            key="input_reference",
            accepts=[JobFileType.REFERENCE_MAP],
            preferred_source="reconstructParticle",
            when="reference_from_job",
        ),
        # A Mask job's mask, read only while mask_from_job is set; solvent_mask_path stays for a file made elsewhere.
        InputSlot(
            key="input_mask",
            accepts=[JobFileType.MASK],
            preferred_source="maskCreate",
            when="mask_from_job",
            beside=True,
        ),
    ]
    OUTPUT_SCHEMA: ClassVar[list[OutputSlot]] = [
        OutputSlot(
            key="output_optimisation",
            produces=JobFileType.OPTIMISATION_SET_STAR,
            path_template="run_optimisation_set.star",
        ),
        OutputSlot(key="output_map", produces=JobFileType.REFERENCE_MAP, path_template="run_class001.mrc"),
        # The unfiltered half maps of the converged run; PostProcess reads half1 and finds half2 beside it.
        OutputSlot(key="output_half_map", produces=JobFileType.HALF_MAP, path_template="run_half1_class001_unfil.mrc"),
    ]

    # Angular / translational search
    healpix_order           : int = Field(
        default=3, ge=1, le=6, description="Initial angular sampling (2=15deg, 3=7.5deg, 4=3.75deg)",
        title="Healpix order", json_schema_extra=form("Angles & offsets", S),
    )
    auto_local_healpix_order: int = Field(
        default=4, ge=1, le=8, description="Sampling from which auto-refine searches locally (4=3.75deg, 5=1.8deg)",
        title="Local from order", json_schema_extra=form("Angles & offsets", S),
    )
    offset_range            : int = Field(
        default=5, ge=0, description="Translational search range (pixels)", title="Offset range",
        json_schema_extra=form("Angles & offsets", S, "px"),
    )
    offset_step             : int = Field(
        default=1, ge=1, description="Translational search step (pixels)", title="Offset step",
        json_schema_extra=form("Angles & offsets", S, "px"),
    )
    # Template matching already found each particle's orientation; a global search repeats that work over every
    # orientation at once (days per iteration on tens of thousands of tilt-series particles).
    sigma_ang               : float = Field(
        default=4.0,
        description="Local angular search around the input angles: σ in degrees, searched ±3σ (4 = ±12°, one 12° "
        "template-matching step). -1 = global search, for particles without angles", title="Sigma angle",
        json_schema_extra=form("Angles & offsets", S, "°"),
    )

    # Filaments (relion_refine --helix): tilt and psi are searched around each segment's priors, which filament
    # picks write from the traced direction. Polarity is the particles' own: a segment whose rlnAnglePsiFlipRatio
    # is above 0.01 is tried both ways along its filament in the global stages, whatever --bimodal_psi says
    # (RELION reads that flag for non-helical data only).
    helix: bool = Field(
        default=False,
        description="Refine filament segments (relion_refine --helix). Tilt and psi are searched around each "
        "segment's priors, the filament's traced direction; the angle around the filament (rot) as Rot sigma says. "
        "The particles need rlnAngleTiltPrior, rlnAnglePsiPrior and rlnHelicalTubeID, as filament picks carry them. "
        "A segment is tried in both directions along its filament while its rlnAnglePsiFlipRatio says the polarity "
        "is unknown (0.5).",
        title="Helical",
        json_schema_extra=form("Helical"),
    )
    helical_outer_diameter: float = Field(
        default=300.0,
        gt=0,
        description="Outer diameter of the filament's mask, on the reference and the particles "
        "(a microtubule is about 250 Å across)",
        title="Tube diameter",
        json_schema_extra=form("Helical", S, "Å"),
    )
    sigma_tilt: float = Field(
        default=5.0,
        description="σ of the search around the tilt prior, searched ±3σ. -1 = no prior on tilt",
        title="Tilt sigma",
        json_schema_extra=form("Helical", S, "°"),
    )
    sigma_psi: float = Field(
        default=5.0,
        description="σ of the search around the psi prior, searched ±3σ. -1 = no prior on psi",
        title="Psi sigma",
        json_schema_extra=form("Helical", S, "°"),
    )
    sigma_rot: float = Field(
        default=-1.0,
        description="σ of the search of the angle around the filament. -1 = every angle, for segments "
        "whose rot is unknown (filament picks draw it at random). A value needs a tilt sigma too",
        title="Rot sigma",
        json_schema_extra=form("Helical", S, "°"),
    )
    keep_tilt_prior_fixed: bool = Field(
        default=True,
        description="The tilt prior stays the traced direction through the run; off, each iteration "
        "takes the tilt the one before found as its prior",
        title="Keep tilt prior",
        json_schema_extra=form("Helical"),
    )
    ignore_helical_symmetry: bool = Field(
        default=True,
        description="Impose no helical symmetry: the priors alone carry the filament's geometry (a "
        "microtubule's seam breaks its symmetry). Off, the twist and rise below are imposed",
        title="No helical symmetry",
        json_schema_extra=form("Helical"),
    )
    helical_twist: float = Field(
        default=0.0,
        description="Helical twist per subunit, positive for a right-handed helix (actin: -166.7)",
        title="Twist",
        json_schema_extra=form("Helical", S, "°"),
    )
    helical_rise: float = Field(
        default=0.0,
        ge=0,
        description="Helical rise per subunit (actin: 27.5)",
        title="Rise",
        json_schema_extra=form("Helical", S, "Å"),
    )
    helical_nr_asu: int = Field(
        default=1,
        ge=1,
        description="Asymmetric units per box that the symmetry is imposed on",
        title="Units per box",
        json_schema_extra=form("Helical", S),
    )
    helical_z_percentage: float = Field(
        default=30.0,
        gt=0,
        le=100,
        description="The central part of the box along the filament that the symmetry is read from and imposed with",
        title="Z percentage",
        json_schema_extra=form("Helical", S, "%"),
    )

    # Symmetry / filtering
    symmetry             : SymmetryGroup = Field(
        default=SymmetryGroup.C1, description="Point group symmetry", title="Symmetry",
        json_schema_extra=form("Refinement", M),
    )
    ini_high             : float         = Field(
        default=40.0, ge=0, description="Initial low-pass filter (Angstroms)", title="Low-pass reference",
        json_schema_extra=form("Refinement", S, "Å"),
    )
    low_resol_join_halves: float         = Field(
        default=40.0, ge=0, description="Half-maps are averaged below this resolution (Angstroms)",
        title="Join halves below", json_schema_extra=form("Refinement", S, "Å"),
    )
    particle_diameter    : float         = Field(
        default=-1.0, description="Mask diameter (Angstroms, -1 = auto)", title="Mask diameter",
        json_schema_extra=form("Reference & mask", S, "Å"),
    )

    # A template in place of a job's map: for particles no job has averaged with features yet (a filament traced
    # from a segmentation averages to a smooth tube, which has no protofilaments to lock the rot onto).
    reference_from_template: bool = Field(
        default=False,
        description="Start from the template below instead of a job's map (the reference input): for particles "
        "whose own average has no features to align to yet. The template must be white, at the particles' pixel "
        "size and in their box; it is filtered to Low-pass reference before the first iteration.",
        title="Template as reference", json_schema_extra=form("Reference & mask"),
    )
    reference_path     : str  = Field(
        default="", description="The template to start from: the species' selected template when the job was made",
        title="Reference template", json_schema_extra=form("Reference & mask", L),
    )

    # Optional mask
    solvent_mask_path  : str  = Field(
        default="", description="Path to soft mask for references (optional)", title="Solvent mask",
        json_schema_extra=form("Reference & mask", L),
    )
    mask_from_job      : bool = Field(
        default=False, description="Take the solvent mask from a Mask job (the mask input) instead of a file path",
        title="Mask from a job", json_schema_extra=form("Reference & mask"),
    )
    solvent_correct_fsc: bool = Field(
        default=True, description="With a solvent mask: report the mask-corrected FSC", title="Mask-corrected FSC",
        json_schema_extra=form("Reference & mask"),
    )

    # Reference handling
    flatten_solvent: bool = Field(
        default=True, description="Apply mask to references during refinement", title="Flatten solvent",
        json_schema_extra=form("Reference & mask"),
    )
    zero_mask      : bool = Field(
        default=True, description="Set outside-mask voxels to zero during refinement", title="Zero mask",
        json_schema_extra=form("Reference & mask"),
    )
    firstiter_cc   : bool = Field(
        default=True, description="Use CC in first iteration (reference greyscale not on the particles' scale)",
        title="First iteration CC", json_schema_extra=form("Reference & mask"),
    )

    # CTF / normalisation
    do_ctf                       : bool = Field(
        default=True, description="Apply CTF correction", title="CTF correction", json_schema_extra=form("CTF & scale"),
    )
    do_norm                      : bool = Field(
        default=True, description="Normalise particle images", title="Normalise", json_schema_extra=form("CTF & scale"),
    )
    do_scale                     : bool = Field(
        default=True, description="Correct for intensity scale differences", title="Scale",
        json_schema_extra=form("CTF & scale"),
    )
    dont_combine_weights_via_disc: bool = Field(
        default=True, description="Keep combination of weights in memory (faster, needs more RAM)",
        title="Weights in memory", json_schema_extra=form("Compute"),
    )
    pad                          : int  = Field(
        default=2, ge=1, description="Fourier padding factor (2 = standard)", title="Pad",
        json_schema_extra=form("Compute", S),
    )

    # Computation
    use_gpu       : bool = Field(
        default=True, description="Use GPU acceleration", title="Use GPU", json_schema_extra=form("Compute"),
    )
    preread_images: bool = Field(
        default=True, description="Pre-read particles into RAM (every MPI follower)", title="Pre-read images",
        json_schema_extra=form("Compute"),
    )
    mpi_ranks     : int  = Field(
        default=3, ge=3, description="MPI processes: leader + one per half-set (min 3)", title="MPI ranks",
        json_schema_extra=form("Compute", S),
    )
    threads       : int  = Field(
        default=3, ge=1, description="Threads per MPI process (--j)", title="Threads",
        json_schema_extra=form("Compute", S),
    )
    pool          : int  = Field(
        default=30, ge=1, description="Number of particles to pool per thread", title="Pool",
        json_schema_extra=form("Compute", S),
    )
    random_seed   : int  = Field(
        default=-1, ge=-1, description="relion_refine --random_seed (-1 = time-based)", title="Random seed",
        json_schema_extra=form("Refinement", S),
    )

    def _get_job_specific_options(self) -> list[tuple[str, str]]:
        input_opt = self.paths.get("input_optimisation", "")
        input_ref = self.paths.get("input_reference", "")
        return [("in_optimisation", str(input_opt)), ("in_3dref", str(input_ref))]

    @property
    def reference_from_job(self) -> bool:
        """Whether the reference is a job's map (the reference input), as against the template."""
        return not self.reference_from_template

    @property
    def imposes_helical_symmetry(self) -> bool:
        return self.helix and not self.ignore_helical_symmetry

    def shows_param(self, name: str) -> bool:
        if name in self.HELICAL_PARAMS:
            return self.helix
        if name in self.HELICAL_SYMMETRY_PARAMS:
            return self.imposes_helical_symmetry
        # A filament refinement searches around its priors by the three sigmas of its own.
        if name == "sigma_ang":
            return not self.helix
        if name == "reference_path":
            return self.reference_from_template
        return True

    def run_problems(self) -> list[str]:
        problems = list(super().run_problems())
        if self.reference_from_template and not self.reference_path.strip():
            problems.append(
                "Template as reference is on and no template is named: select one on the Species page for new jobs, "
                "or give its path here."
            )
        if not self.helix:
            return problems
        if self.sigma_rot > 0 and self.sigma_tilt <= 0:
            problems.append(
                "Helical: a rot sigma needs a tilt sigma too (RELION takes no prior on rot without one on tilt)."
            )
        if self.imposes_helical_symmetry and (self.helical_twist == 0 or self.helical_rise <= 0):
            problems.append(
                "Helical: imposing the symmetry needs its twist and rise; neither has a default. Set them, or turn "
                "No helical symmetry on."
            )
        return problems

    def is_driver_job(self) -> bool:
        return True

    def get_tool_name(self) -> str:
        return "relion"

    @staticmethod
    def get_input_requirements() -> dict[str, str]:
        return {"optimisation_set": "subtomoExtraction", "reference": "reconstructParticle"}
