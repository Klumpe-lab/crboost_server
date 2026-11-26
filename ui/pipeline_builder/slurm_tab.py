# ui/pipeline_builder/slurm_tab.py
"""
SLURM resources section, the job page's rail.

  • The head: "SLURM resources", the profile as its meta, "Reset to profile" on it while the job overrides it.
  • A two-column grid of label / box: "At once" (the array throttle) first for an array job, then the job's
    effective settings. Nodes and Tasks/node show only when not 1, or behind the "nodes & tasks" check.
  • An array job's supervisor as one folded line (partition · GRES · memory · time · QOS) that opens to its
    fields and a sentence on what it is.

Presets exist on the model and can be applied programmatically; the section has no preset UI.
"""

from collections.abc import Callable

from nicegui import ui

from services.job_params import SLURM_FIELDS, set_slurm
from ui.components.buttons import house_button
from ui.components.check import house_check
from ui.components.reactive import Debounced
from ui.job_plugins._field_styles import LABEL_VOICE, MONO, CLR_SUBLABEL, bind_numeric, section

# The grid: a 66 px label in the slot voice, the box to at most 150 px.
_LABEL_STYLE = (
    f"{LABEL_VOICE} width: 66px; flex-shrink: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;"
)
_CELL_STYLE = "display: flex; align-items: center; gap: 8px; min-height: 20px; min-width: 0;"
_GRID_STYLE = "display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 3px 16px; width: 100%;"
_BOX_STYLE = f"{MONO} font-size: 10.5px; flex: 1 1 0; min-width: 0; max-width: 150px;"
_INPUT_PROPS = (
    "dense borderless hide-bottom-space "
    "input-style=\"font-family: 'IBM Plex Mono', monospace; font-size: 10.5px; "
    'color: var(--cb-slate-800, #1e293b); padding: 1px 2px; min-height: 0;"'
)
# Settings that hold 1 nearly always: shown when not 1, or when asked for.
_QUIET = ("nodes", "ntasks_per_node")


def _is_array_job(job_model) -> bool:
    return "array_throttle" in getattr(job_model, "USER_PARAMS", set())


# Quiet time after a SLURM field edit before the panel rebuilds to show its effect (refresh_after_edit).
_REFRESH_AFTER_EDIT_S = 0.8


def render_slurm_tab(
    job_model, is_frozen: bool, save_handler: Callable, *, state, instance_id: str
) -> Callable[[], None]:
    """Render the panel; returns its refresh. The job tab reruns that after an edit in any section, since
    the effective settings can follow the job's parameters (missAlign's time limit and GRES), and on its
    poll, which shows a setting changed elsewhere (another tab, the API). Writes go through the setting
    door (`job_params.set_slurm`)."""
    shown = None
    # A field's edit reaches the rest of the panel once focus has stayed out of its fields for a
    # moment: rebuilding at once replaced the field the user had just clicked into. While one of
    # them has focus, a rebuild asked for by an edit elsewhere waits for its blur too.
    refresh_after_edit = Debounced(lambda: refresh(), _REFRESH_AFTER_EDIT_S)
    focused = {"on": False}
    # Nodes and Tasks/node shown although they are 1.
    more = {"on": False}

    def set_more(on: bool) -> None:
        more["on"] = on
        content.refresh()

    def hold_refresh(_e=None) -> None:
        focused["on"] = True
        refresh_after_edit.cancel()

    def release_refresh(_e=None) -> None:
        focused["on"] = False
        refresh_after_edit()

    def write(values: dict | None = None, *, reset: bool = False) -> None:
        res = set_slurm(state, instance_id, values, reset=reset)
        if not res["success"]:
            ui.notify(res["error"], type="warning")
        elif res["applied"] or reset:
            save_handler()

    @ui.refreshable
    def content():
        nonlocal shown
        shown = _signature(job_model)
        _render_slurm_content(
            job_model, is_frozen, write, save_handler, refresh, hold_refresh, release_refresh, more["on"], set_more
        )

    def refresh():
        # A rebuild replaces every input, so only rebuild when a value it shows has changed.
        if focused["on"]:
            return
        if _signature(job_model) != shown:
            content.refresh()

    content()
    return refresh


def _signature(job_model) -> tuple:
    """Everything the panel shows that can change while it is open."""
    return (
        job_model.get_effective_slurm_config().model_dump(),
        dict(job_model.slurm_overrides or {}),
        getattr(job_model, "array_throttle", None),
    )


def _render_slurm_content(
    job_model,
    is_frozen: bool,
    write: Callable,
    save_handler: Callable,
    refresh: Callable[[], None],
    hold: Callable,
    release: Callable,
    more: bool,
    set_more: Callable[[bool], None],
):
    effective_config = job_model.get_effective_slurm_config()
    overrides = job_model.slurm_overrides or {}
    is_array = _is_array_job(job_model)
    has_profile = job_model.has_resource_profile()
    profile = job_model.job_type.value if has_profile and job_model.job_type else ""

    with section("SLURM resources", right=f"profile {profile}" if profile else "") as sec:
        if has_profile and overrides and not is_frozen:

            def reset_to_profile():
                write(reset=True)
                refresh()

            with sec.head:
                house_button(
                    "Reset to profile",
                    reset_to_profile,
                    tooltip=f"Drop this job's own settings and take the {profile} profile's again",
                )
        quiet = [f for f in _QUIET if str(getattr(effective_config, f)) == "1"]
        with ui.element("div").style(_GRID_STYLE):
            if is_array:
                _render_throttle(job_model, is_frozen, save_handler, hold, release)
            for fname, label in SLURM_FIELDS:
                if fname in quiet and not more:
                    continue
                _render_resource_field(fname, label, getattr(effective_config, fname), is_frozen, write, hold, release)
        if quiet:
            house_check(
                "nodes & tasks",
                value=more,
                on_change=lambda e: set_more(e.value),
                tooltip="Nodes and Tasks/node are 1, as nearly every job has them; show them to change them",
            ).style("margin-top: 3px;")
        if is_array:
            _render_supervisor()
        _render_slurm_note(job_model)


def _render_throttle(job_model, is_frozen: bool, save_handler: Callable, hold: Callable, release: Callable) -> None:
    """The array throttle, "At once": how many of the array's tasks run at the same time."""
    with ui.element("div").style(_CELL_STYLE):
        ui.label("At once").style(_LABEL_STYLE).tooltip("SLURM --array throttle: the most tasks running at once")
        # precision=0 + numeric_forward: array_throttle is an `int` and the model does not validate on
        # assignment, so a fractional entry here would only surface as a load failure that drops the instance.
        inp = bind_numeric(
            ui.number(value=getattr(job_model, "array_throttle", 20), format="%d", precision=0),
            job_model,
            "array_throttle",
        )
        inp.props(_INPUT_PROPS)
        inp.style(_BOX_STYLE).classes("cb-select")
        if is_frozen:
            inp.props("readonly")
        else:

            def _on_throttle_blur(_e):
                save_handler()
                release()

            inp.on("focus", hold)
            inp.on("blur", _on_throttle_blur)


def _render_slurm_note(job_model):
    """The job class's SLURM_NOTE: one muted line, the detail in its tooltip."""
    note = getattr(job_model, "SLURM_NOTE", ())
    if not note:
        return
    line, detail = note
    with ui.row().classes("w-full items-center gap-1").style("margin-top: 6px; cursor: help;"):
        ui.icon("info_outline", size="11px").style(f"color: {CLR_SUBLABEL};")
        ui.label(line).style(f"font-size: 10px; color: {CLR_SUBLABEL};")
        ui.tooltip(detail).style("white-space: pre-line; max-width: 420px; font-size: 11px; line-height: 1.45;")


_FIELD_TOOLTIPS = {
    "prefer": "SLURM --prefer: node features tried first; when no such node is free the job starts on what "
    "Constraint allows instead of waiting",
    "avoid": "Node features to stay off (e.g. mig): the nodes that have one are sent as --exclude",
}


def _render_resource_field(fname: str, label: str, current, is_frozen, write, hold, release) -> None:
    """One SLURM setting: the job's effective value; it writes an override only when its text changed while it had
    focus, so tabbing through one, even one drawn before a change made elsewhere, writes nothing."""
    with ui.element("div").style(_CELL_STYLE):
        lbl = ui.label(label).style(_LABEL_STYLE)
        lbl.tooltip(_FIELD_TOOLTIPS.get(fname, f"SLURM --{fname.replace('_', '-')}"))
        inp = ui.input(value=str(current))
        inp.props(_INPUT_PROPS)
        inp.style(_BOX_STYLE).classes("cb-select")
        if is_frozen:
            inp.props("readonly")
        else:
            focus_handler, blur_handler = _field_handlers(fname, write, hold, release)
            inp.on("focus", focus_handler)
            inp.on("blur", blur_handler)


def _field_handlers(name: str, write: Callable, hold: Callable, release: Callable) -> tuple[Callable, Callable]:
    """One field's focus and blur: the text it had at focus, and on blur a write only if it changed since."""
    at_focus: dict = {"text": None}

    def on_focus(e) -> None:
        at_focus["text"] = e.sender.value
        hold()

    def on_blur(e) -> None:
        if at_focus["text"] is not None and e.sender.value != at_focus["text"]:
            write({name: e.sender.value})
        at_focus["text"] = None
        release()

    return on_focus, on_blur


def _render_supervisor() -> None:
    """An array job's supervisor, one folded line: partition · GRES · memory · time · QOS; a click opens its fields
    and what it is. Read-only: set in conf.yaml."""
    from services.configs.config_service import get_config_service

    try:
        sup = get_config_service().supervisor_slurm_defaults
    except FileNotFoundError:  # no conf.yaml (a checkout without preflight): nothing to show
        return
    fields = [
        ("Partition", sup.partition),
        ("GRES", sup.gres),
        ("Memory", sup.mem),
        ("Time limit", sup.time),
        ("QOS", sup.qos),
        ("Constraint", sup.constraint),
        ("Prefer", sup.prefer),
        ("CPUs/task", str(sup.cpus_per_task)),
    ]
    summary = " · ".join(str(v) for label, v in fields[:5] if v)
    with ui.element("div").style("width: 100%; margin-top: 4px;"):
        head = ui.element("div").style(f"{_CELL_STYLE} gap: 6px; cursor: pointer; user-select: none;")
        with head:
            ui.label("Supervisor").style(_LABEL_STYLE)
            ui.label(summary or "conf.yaml defaults").style(
                f"{MONO} font-size: 9.5px; color: var(--cb-x-5b7089, #5b7089); min-width: 0; overflow: hidden; "
                "text-overflow: ellipsis; "
                "white-space: nowrap;"
            )
            caret = ui.label("▸").style(f"font-size: 9px; color: {CLR_SUBLABEL}; flex-shrink: 0;")
        body = ui.element("div").style("width: 100%; margin-top: 3px;")
        with body:
            ui.label(
                "The supervisor dispatches the array's tasks and gathers their results. It is set in conf.yaml, the "
                "same for every array job."
            ).style(f"font-size: 9.5px; color: {CLR_SUBLABEL}; margin-bottom: 3px;")
            with ui.element("div").style(_GRID_STYLE):
                for label, v in fields:
                    with ui.element("div").style(_CELL_STYLE):
                        ui.label(label).style(_LABEL_STYLE)
                        ui.label(str(v) if v else "—").style(
                            f"{MONO} font-size: 10px; color: var(--cb-x-5b7089, #5b7089);"
                        )
        body.set_visibility(False)

        def flip(_e=None) -> None:
            body.set_visibility(not body.visible)
            caret.set_text("▾" if body.visible else "▸")

        head.on("click", flip)
