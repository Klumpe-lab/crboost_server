"""What a filament's particles need of their box: RELION's rule for a helical tube, shared by the Refine 3D
driver (which checks the reference it is given) and Run (which checks an extraction before anything is cut)."""

from __future__ import annotations

from services.models_base import JobType, SpeciesKind

# relion_refine's soft mask edge for a filament when --maskedge is not given (ml_optimiser.cpp), in pixels.
HELICAL_MASK_EDGE_PX = 10


def helical_box_problem(
    box_px: int, pixel_ang: float, tube_diameter_ang: float, mask_diameter_ang: float
) -> str | None:
    """Why relion_refine would refuse this tube in this box (helix.cpp, checkParametersFor3DHelicalReconstruction),
    or None. The tube must lie within the mask, and the mask within the box. Without a mask diameter RELION takes
    90 % of the box, less the mask's two edges when the box is under 200 px."""
    box_ang = box_px * pixel_ang
    if mask_diameter_ang > 0:
        mask, how = mask_diameter_ang, "the Mask diameter set"
    else:
        inner = box_px - 2 * HELICAL_MASK_EDGE_PX if box_px * 0.05 < HELICAL_MASK_EDGE_PX else box_px
        mask = 0.9 * inner * pixel_ang
        how = f"what RELION takes for a box of {box_px} px at {pixel_ang:g} Å/px"
    if mask > box_ang:
        return f"The mask diameter of {mask:.0f} Å is wider than the box ({box_ang:.0f} Å)."
    if tube_diameter_ang < 4 * pixel_ang:
        return f"The tube diameter of {tube_diameter_ang:g} Å is under 4 px at {pixel_ang:g} Å/px."
    if tube_diameter_ang > mask:
        need = int(-(-tube_diameter_ang // (0.9 * pixel_ang))) + 2 * HELICAL_MASK_EDGE_PX
        need += need % 2
        return (
            f"The tube diameter of {tube_diameter_ang:g} Å is wider than the mask, {mask:.0f} Å ({how}). RELION "
            f"refuses that. Export the particles in a box of at least {need} px, or set a smaller tube diameter."
        )
    return None


def extraction_box_problem(state, job_model) -> str | None:
    """Why the particles an extraction job is set to cut could not be refined as a helical tube, or None: for a
    filament species only, RELION's rule on the job's box at its pixel size (the tilt series' × the binning) and
    the tube's diameter (the job's own for Warp Export, the species' for Subtomo Extraction)."""
    if job_model.job_type not in (JobType.WARP_EXPORT, JobType.SUBTOMO_EXTRACTION):
        return None
    species = state.get_species(job_model.species_id) if job_model.species_id else None
    if species is None or SpeciesKind(species.kind) != SpeciesKind.FILAMENT:
        return None
    pixel = float(state.microscope.pixel_size_angstrom) * float(job_model.binning)
    if job_model.job_type == JobType.WARP_EXPORT:
        box, tube = int(job_model.box_size), float(job_model.diameter_ang)
    else:
        crop = int(job_model.crop_size)
        box, tube = (crop if crop > 0 else int(job_model.box_size)), float(species.diameter_ang or 0.0)
    if tube <= 0:
        return None
    problem = helical_box_problem(box, pixel, tube, 0.0)
    return f"{species.name} is a filament. {problem}" if problem else None
