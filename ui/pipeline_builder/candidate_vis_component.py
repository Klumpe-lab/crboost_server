"""
UI component for generating IMOD visualization from candidate extraction results.
"""

import traceback
from pathlib import Path

from nicegui import ui

from services.project_state import JobStatus


def _resolve_job_dir(job_model, ui_mgr) -> Path | None:
    project_path = ui_mgr.project_path
    if not project_path:
        return None
    job_name = job_model.relion_job_name
    if not job_name:
        return None
    job_dir = Path(project_path) / job_name
    if job_dir.is_dir():
        return job_dir
    return None


def _find_star_file(job_dir: Path, name: str) -> Path | None:
    candidate = job_dir / name
    if candidate.exists():
        return candidate
    return None


def _vis_already_exists(job_dir: Path) -> bool:
    imod_dir = job_dir / "vis" / "imodPartRad"
    return imod_dir.exists() and any(imod_dir.glob("*.mod"))


def _make_imod_command_runner():
    """
    Build a command runner that wraps commands through the IMOD container.
    """
    from services.computing.container_service import get_container_service

    container_service = get_container_service()

    def runner(cmd: str, cwd: Path) -> None:
        import subprocess

        wrapped = container_service.wrap_command_for_tool(
            cmd, cwd=cwd, tool_name="imod", additional_binds=[str(cwd)]
        )
        result = subprocess.run(wrapped, shell=True, capture_output=True, text=True, cwd=cwd)
        if result.returncode != 0:
            raise RuntimeError(
                f"Container command failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    return runner


def _handle_generate_vis(job_model, job_dir: Path, status_label, generate_btn):
    """Run visualization generation synchronously (fast, no executor needed)."""
    from services.visualization.imod_vis import generate_candidate_vis

    candidates_star = _find_star_file(job_dir, "candidates.star")
    tomograms_star = _find_star_file(job_dir, "tomograms.star")

    if not candidates_star:
        ui.notify("candidates.star not found in job directory", type="negative")
        return
    if not tomograms_star:
        ui.notify("tomograms.star not found in job directory", type="negative")
        return

    diameter = float(job_model.particle_diameter_ang)
    command_runner = _make_imod_command_runner()

    status_label.set_text("Generating...")
    status_label.classes(replace="text-xs text-blue-600 italic")
    generate_btn.props("loading")

    try:
        generate_candidate_vis(
            candidates_star=candidates_star,
            tomograms_star=tomograms_star,
            particle_diameter_ang=diameter,
            output_dir=job_dir,
            command_runner=command_runner,
        )
        status_label.set_text("Done -- models written to vis/")
        status_label.classes(replace="text-xs text-green-600")
        ui.notify("Visualization generated", type="positive", timeout=2000)
    except Exception as e:
        status_label.set_text(f"Failed: {e}")
        status_label.classes(replace="text-xs text-red-600")
        traceback.print_exc()
        ui.notify(f"Visualization failed: {e}", type="negative")
    finally:
        generate_btn.props(remove="loading")


def render_candidate_vis_panel(job_model, ui_mgr) -> None:
    job_dir = _resolve_job_dir(job_model, ui_mgr)
    job_succeeded = job_model.execution_status == JobStatus.SUCCEEDED

    with ui.card().classes("w-full border border-gray-200 shadow-sm overflow-hidden bg-white"):
        with ui.row().classes(
            "w-full items-center justify-between px-3 py-2 bg-gray-50 border-b border-gray-100"
        ):
            with ui.row().classes("items-center gap-2"):
                ui.icon("visibility", size="18px").classes("text-gray-500")
                ui.label("Candidate Visualization").classes("text-sm font-bold text-gray-800")

        with ui.column().classes("w-full p-3 gap-3"):

            if not job_succeeded:
                ui.label(
                    "Run candidate extraction first. Visualization can be generated after the job succeeds."
                ).classes("text-xs text-gray-500 italic")
                return

            if not job_dir:
                ui.label("Job directory not found.").classes("text-xs text-red-500")
                return

            has_vis = _vis_already_exists(job_dir)

            if has_vis:
                status_label = ui.label("IMOD models exist in vis/").classes("text-xs text-green-600")
            else:
                status_label = ui.label("No visualization generated yet.").classes(
                    "text-xs text-gray-500 italic"
                )

            with ui.row().classes("items-center gap-3"):
                generate_btn = ui.button(
                    "Regenerate IMOD Models" if has_vis else "Generate IMOD Models",
                    icon="scatter_plot",
                    on_click=lambda: _handle_generate_vis(
                        job_model, job_dir, status_label, generate_btn
                    ),
                ).props("dense no-caps unelevated").classes(
                    "bg-blue-50 text-blue-700 border border-blue-200 px-3"
                )

                ui.label(
                    f"Particle diameter: {job_model.particle_diameter_ang} A"
                ).classes("text-xs text-gray-500")

            if has_vis:
                with ui.column().classes("gap-1 mt-1"):
                    ui.label("Generated outputs:").classes(
                        "text-[10px] font-bold text-gray-400 uppercase"
                    )
                    for subdir, description in [
                        ("vis/imodPartRad", "Particle radius models (green spheres)"),
                        ("vis/imodCenter", "Center markers (red)"),
                        ("candidatesWarp", "Warp-compatible coordinates"),
                    ]:
                        full_path = job_dir / subdir
                        exists = full_path.exists()
                        icon = "check_circle" if exists else "radio_button_unchecked"
                        color = "text-green-500" if exists else "text-gray-300"
                        with ui.row().classes("items-center gap-2"):
                            ui.icon(icon, size="14px").classes(color)
                            ui.label(f"{subdir}/  --  {description}").classes(
                                "text-xs font-mono text-gray-600"
                            )

            ui.separator().classes("my-2")
            ui.label("View Volumes").classes(
                "text-[10px] font-bold text-gray-400 uppercase"
            )
            ui.label(
                "Interactive viewing requires X11 forwarding. "
                "Copy a command below and run it in a terminal session with IMOD loaded."
            ).classes("text-xs text-gray-500 italic")

            tomograms_star = _find_star_file(job_dir, "tomograms.star")
            if tomograms_star:
                try:
                    import starfile

                    tomo_data = starfile.read(tomograms_star, always_dict=True)
                    tomo_df = next(
                        (v for v in tomo_data.values() if "rlnTomoName" in v.columns),
                        None,
                    )
                    if tomo_df is not None and "rlnTomoReconstructedTomogram" in tomo_df.columns:
                        # Also check for IMOD models to offer combined viewing
                        has_models = _vis_already_exists(job_dir)

                        for _, row in tomo_df.iterrows():
                            vol_path = Path(row["rlnTomoReconstructedTomogram"])
                            tomo_name = row["rlnTomoName"]

                            # Prefer f32 version for IMOD4 compatibility
                            f32_path = vol_path.with_name(vol_path.stem + "_f32.mrc")
                            if f32_path.exists():
                                vol_path = f32_path

                            vol_exists = vol_path.exists()

                            # Build the 3dmod command with model overlay if available
                            model_path = job_dir / "vis" / "imodPartRad" / f"coords_{tomo_name}.mod"
                            if has_models and model_path.exists():
                                imod_cmd = f"3dmod {vol_path} {model_path}"
                            else:
                                imod_cmd = f"3dmod {vol_path}"

                            with ui.row().classes("items-center gap-2 w-full"):
                                ui.label(tomo_name).classes("text-xs font-mono text-gray-700 w-48 shrink-0")

                                ui.input(value=imod_cmd).props(
                                    "dense outlined readonly hide-bottom-space"
                                ).classes("text-xs font-mono flex-1").style(
                                    "min-width: 0;"
                                )

                                ui.button(
                                    icon="content_copy",
                                    on_click=lambda cmd=imod_cmd: (
                                        ui.clipboard.write(cmd),
                                        ui.notify("Copied", type="positive", timeout=800),
                                    ),
                                ).props("flat dense round size=sm").classes(
                                    "text-gray-500 hover:text-gray-800"
                                ).tooltip("Copy command")

                                if not vol_exists:
                                    ui.label("(volume not found)").classes(
                                        "text-[10px] text-red-400"
                                    )
                    else:
                        ui.label("No reconstructed tomograms listed.").classes(
                            "text-xs text-gray-400 italic"
                        )
                except Exception as e:
                    ui.label(f"Could not read tomograms.star: {e}").classes(
                        "text-xs text-red-400"
                    )
            else:
                ui.label("tomograms.star not found in job directory.").classes(
                    "text-xs text-gray-400 italic"
                )