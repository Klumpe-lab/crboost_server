"""ImportTomograms plugin -- config form for the tomogram provider job.

Two modes (`source_mode`): ``reference`` copies an existing ``tomograms.star``;
``synthesize`` builds one from a glob of reconstructed ``.mrc`` files. The Mode
dropdown reactively shows only the active mode's fields (visibility bound to
``source_mode``). See PARTICLE_PROJECT_ROADMAP.md P1.
"""

from nicegui import ui

from ui.job_plugins import register_params_renderer
from ui.job_plugins._field_styles import field_grid, section_header, enum_field, numeric_field, path_row, text_field
from services.models_base import JobType
from services.jobs.import_tomograms import TomoImportMode


@register_params_renderer(JobType.IMPORT_TOMOGRAMS)
def render_import_tomograms_params(job_type, job_model, is_frozen, save_handler, *, ui_mgr=None, backend=None, **_ctx):
    common = dict(job_model=job_model, is_frozen=is_frozen, save_handler=save_handler)

    section_header("Tomogram source", first=True)
    with field_grid():
        enum_field(
            "Mode",
            attr="source_mode",
            enum_type=TomoImportMode,
            hint="reference: copy an existing tomograms.star · synthesize: build one from recon MRCs",
            **common,
        )

    ref_box = ui.element("div").style("width: 100%;")
    with ref_box:
        section_header("Reference")
        with field_grid():
            path_row(
                "tomograms.star",
                attr="reference_star",
                hint="Path to an existing tomograms.star (e.g. another project's tsReconstruct output)",
                **common,
            )
    ref_box.bind_visibility_from(job_model, "source_mode", value=TomoImportMode.REFERENCE.value)

    syn_box = ui.element("div").style("width: 100%;")
    with syn_box:
        section_header("Synthesize")
        with field_grid():
            path_row(
                "Recon MRC glob",
                attr="mrc_glob",
                hint="Glob of reconstructed tomogram .mrc files, e.g. /data/recons/*.mrc",
                **common,
            )
            numeric_field(
                "Pixel size",
                attr="pixel_size_angstrom",
                suffix="A",
                hint="Unbinned tilt-series pixel size. 0 = derive from the MRC voxel size / binning",
                **common,
            )
            numeric_field(
                "Binning",
                attr="tomogram_binning",
                hint="Tomogram binning vs the unbinned tilt series (1 = treat the recon as the working frame)",
                **common,
            )
            text_field("Optics group", attr="optics_group_name", hint="rlnOpticsGroupName", **common)
    syn_box.bind_visibility_from(job_model, "source_mode", value=TomoImportMode.SYNTHESIZE.value)
