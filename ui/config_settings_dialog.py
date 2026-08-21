"""
Config settings dialog — a nicely-formatted, structured editor for conf.yaml.

Design notes
------------
* Edits never touch the shared server ``config/conf.yaml``. They are diffed
  against the server default and the delta is written to this user's
  ``~/.crboost/conf.yaml`` (see ConfigService.save_user_override). While that
  override exists it is preferred; "Revert to server defaults" deletes it.
* Every field that names a file/dir on disk carries a live existence dot
  (green = resolves, red = missing) that updates as you type — so a moved or
  mistyped container/venv path is obvious immediately.
* The editor is driven off ``effective_dict()`` (the raw merged yaml), not the
  pydantic Config, so keys the model ignores (e.g. ``crboost_python``) still
  round-trip faithfully.
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any
from collections.abc import Callable

from nicegui import ui

from services.configs.config_service import check_path_exists, get_config_service
from services.models_base import JobType
from ui.components.buttons import house_button
from ui.styles import MONO, SANS as FONT

logger = logging.getLogger(__name__)

CLR_HEADING = "#0f172a"
CLR_LABEL = "#475569"
CLR_SUBLABEL = "#94a3b8"
CLR_BORDER = "#e2e8f0"
CLR_ACCENT = "#2563eb"
CLR_OK = "#10b981"
CLR_BAD = "#ef4444"

_JOB_LABELS = {jt.value: jt.name.replace("_", " ").title() for jt in JobType}

# Which value-kind each path field is checked as. "glob" checks the parent
# directory exists (the pattern itself rarely matches at config time).
_TOOLS_HINT = "SIF image (container) or executable (binary). Green dot = the file resolves on disk."


def _dot_style(exists: bool, empty_ok: bool = False) -> str:
    if empty_ok:
        color = "#cbd5e1"
    else:
        color = CLR_OK if exists else CLR_BAD
    return (
        f"width: 8px; height: 8px; border-radius: 50%; background: {color}; "
        "flex-shrink: 0; box-shadow: 0 0 0 2px #ffffff;"
    )


def _glob_target_exists(value: str) -> bool:
    """A glob resolves if its containing directory exists (the pattern may not
    match yet at config time)."""
    if not value or not value.strip():
        return False
    p = Path(value)
    if "*" in value or "?" in value:
        return p.parent.exists()
    return p.exists()


def open_config_settings(on_saved: Callable[[], None] | None = None) -> None:
    """Open the settings dialog. `on_saved` is called after a successful
    save/revert so callers (the landing strip) can refresh their status."""
    cs = get_config_service()
    eff = cs.effective_dict()

    # Working copy the inputs mutate in place; saved as-is (diffed vs default).
    nv: dict[str, Any] = {
        "crboost_root": eff.get("crboost_root", "") or "",
        "crboost_python": eff.get("crboost_python", "") or "",
        "local": dict(eff.get("local") or {}),
        "slurm_defaults": dict(eff.get("slurm_defaults") or {}),
        "job_resource_profiles": copy.deepcopy(eff.get("job_resource_profiles") or {}),
        "tools": copy.deepcopy(eff.get("tools") or {}),
    }

    with ui.dialog() as dialog, ui.card().style(
        "width: 92vw; max-width: 1060px; max-height: 90vh; padding: 0; overflow: hidden; "
        "border-radius: 8px; box-shadow: 0 12px 32px rgba(15,23,42,0.18);"
    ):
        _build_header(dialog, cs, on_saved)
        with ui.scroll_area().classes("w-full").style("height: 66vh; padding: 0;"):
            body = ui.column().classes("w-full").style("gap: 0; padding: 0 0 10px;")
            with body:
                _section_environment(nv)
                _section_containers(nv)
                _section_slurm(nv)
                _section_local(nv)
        _build_footer(dialog, cs, nv, on_saved)

    dialog.open()


# ── chrome ────────────────────────────────────────────────────────────────


def _build_header(dialog, cs, on_saved) -> None:
    with ui.element("div").style(
        "display: flex; align-items: center; gap: 10px; padding: 12px 16px; "
        f"border-bottom: 1px solid {CLR_BORDER}; background: #f8fafc;"
    ):
        ui.icon("settings", size="18px").style(f"color: {CLR_ACCENT};")
        with ui.column().classes("gap-0").style("flex: 1; min-width: 0;"):
            ui.label("Server configuration").style(
                f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING}; letter-spacing: -0.01em;"
            )
            ui.label(str(cs.default_config_path)).style(
                f"{MONO} font-size: 9px; color: {CLR_SUBLABEL}; overflow: hidden; "
                "text-overflow: ellipsis; white-space: nowrap;"
            )
        _override_badge(cs)
        ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm").classes("text-slate-400")


def _override_badge(cs) -> None:
    if cs.has_user_override:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 5px; padding: 3px 9px; border-radius: 20px; "
            "background: #eff6ff; border: 1px solid #bfdbfe; flex-shrink: 0;"
        ).tooltip(f"Your personal override is active:\n{cs.user_override_path}"):
            ui.element("div").style(f"width: 6px; height: 6px; border-radius: 50%; background: {CLR_ACCENT};")
            ui.label("custom config").style(f"{FONT} font-size: 10px; font-weight: 600; color: #1e40af;")
    else:
        with ui.element("div").style(
            "display: flex; align-items: center; gap: 5px; padding: 3px 9px; border-radius: 20px; "
            "background: #f1f5f9; border: 1px solid #e2e8f0; flex-shrink: 0;"
        ).tooltip("Using the shared server defaults. Edits below are saved to your home folder only."):
            ui.element("div").style("width: 6px; height: 6px; border-radius: 50%; background: #94a3b8;")
            ui.label("server defaults").style(f"{FONT} font-size: 10px; font-weight: 600; color: {CLR_LABEL};")


def _build_footer(dialog, cs, nv, on_saved) -> None:
    async def _do_revert():
        ok = await _confirm_revert(cs)
        if not ok:
            return
        try:
            cs.revert_to_defaults()
            ui.notify("Reverted to server defaults", type="positive")
        except Exception as e:
            logger.info("Revert failed: %s", e)
            ui.notify(f"Revert failed: {e}", type="negative")
            return
        dialog.close()
        if on_saved:
            on_saved()

    def _do_save():
        try:
            cs.save_user_override(nv)
        except Exception as e:
            logger.info("Save config override failed: %s", e)
            ui.notify(f"Save failed: {e}", type="negative")
            return
        cs2 = get_config_service()
        if cs2.has_user_override:
            ui.notify("Saved to your personal config override", type="positive")
        else:
            ui.notify("No changes from defaults — override cleared", type="info")
        dialog.close()
        if on_saved:
            on_saved()

    with ui.element("div").style(
        "display: flex; align-items: center; gap: 8px; padding: 10px 16px; "
        f"border-top: 1px solid {CLR_BORDER}; background: #f8fafc;"
    ):
        house_button(
            "Revert to server defaults",
            _do_revert,
            kind="danger",
            tooltip="Delete your personal override — fall back to the shared server conf.yaml",
        )
        ui.element("div").style("flex: 1;")
        house_button("Cancel", dialog.close)
        house_button("Save to my config", _do_save, kind="accent")


async def _confirm_revert(cs) -> bool:
    if not cs.has_user_override:
        ui.notify("Already on server defaults — nothing to revert", type="info")
        return False
    with ui.dialog() as d, ui.card().classes("w-96"):
        ui.label("Revert to server defaults?").style(
            f"{FONT} font-size: 13px; font-weight: 600; color: {CLR_HEADING};"
        )
        ui.label("This deletes your personal override. The shared server configuration is untouched.").style(
            f"{FONT} font-size: 12px; color: {CLR_LABEL}; margin-top: 4px;"
        )
        ui.label(str(cs.user_override_path)).style(
            f"{MONO} font-size: 10px; color: {CLR_SUBLABEL}; margin-top: 6px; padding: 5px 7px; "
            "background: #f8fafc; border-radius: 4px; word-break: break-all;"
        )
        with ui.row().classes("w-full justify-end mt-3 gap-2"):
            house_button("Cancel", lambda: d.submit(False))
            house_button("Revert", lambda: d.submit(True), kind="danger")
    return bool(await d)


# ── section header + shared field builders ─────────────────────────────────


def _section_header(title: str, subtitle: str = "") -> None:
    with ui.element("div").style(
        "padding: 9px 16px 6px; background: #f8fafc; border-top: 1px solid #e2e8f0; "
        "border-bottom: 1px solid #e2e8f0;"
    ):
        ui.label(title).style(
            f"{FONT} font-size: 10px; font-weight: 700; color: #64748b; "
            "letter-spacing: 0.08em; text-transform: uppercase;"
        )
        if subtitle:
            ui.label(subtitle).style(f"{FONT} font-size: 10px; color: {CLR_SUBLABEL}; margin-top: 1px;")


def _path_field(label: str, initial: str, setter: Callable[[str], None], kind: str, note: str = "") -> None:
    """Row: label + live existence dot + path input. `kind` ∈ {tool, dir, glob}.
    `setter(value)` writes the new value into the working config dict."""

    def _exists(v: str) -> bool:
        if kind == "glob":
            return _glob_target_exists(v)
        if kind == "dir":
            return bool(v) and Path(v).is_dir()
        return check_path_exists(v)  # tool: file-or-PATH

    with ui.element("div").style("padding: 5px 16px;"):
        with ui.row().classes("w-full items-center").style("gap: 8px; flex-wrap: nowrap;"):
            ui.label(label).style(
                f"{FONT} font-size: 11px; color: {CLR_LABEL}; width: 128px; flex-shrink: 0; text-align: right;"
            )
            dot = ui.element("div").style(_dot_style(_exists(initial)))

            def _on_change(e, s=setter, d=dot):
                v = (e.value or "").strip()
                s(v)
                ok = _exists(v)
                d.style(_dot_style(ok))
                d.tooltip("Resolves on disk" if ok else "Not found on disk")

            inp = (
                ui.input(value=initial, on_change=_on_change)
                .props("dense")
                .classes("cb-field flex-1")
                .style("min-width: 0; width: 100%;")
            )
            inp.tooltip("Resolves on disk" if _exists(initial) else "Not found on disk")
        if note:
            with ui.row().classes("w-full").style("padding-left: 136px; margin-top: 1px;"):
                ui.label(note).style(f"{FONT} font-size: 9px; color: {CLR_SUBLABEL}; font-style: italic;")


# ── sections ────────────────────────────────────────────────────────────────


def _section_environment(nv: dict[str, Any]) -> None:
    _section_header("Environment", "Server install dir and the Python used inside SLURM jobs")

    def set_root(v):
        nv["crboost_root"] = v

    def set_py(v):
        nv["crboost_python"] = v

    _path_field("crboost root", nv["crboost_root"], set_root, kind="dir")
    _path_field(
        "job Python",
        nv["crboost_python"],
        set_py,
        kind="tool",
        note="Baked into config/qsub.sh at preflight — re-run preflight.py (or restart) to apply to new jobs.",
    )


def _section_containers(nv: dict[str, Any]) -> None:
    _section_header("Containers & tools", _TOOLS_HINT)
    tools: dict[str, Any] = nv["tools"]
    if not tools:
        with ui.element("div").style("padding: 8px 16px;"):
            ui.label("No tools configured.").style(
                f"{FONT} font-size: 11px; color: {CLR_SUBLABEL}; font-style: italic;"
            )
        return

    for name in sorted(tools):
        tc = tools[name]
        _tool_row(name, tc)


def _tool_row(name: str, tc: dict[str, Any]) -> None:
    """One tool: exec-mode select + a single active-path input with live dot.
    Switching mode repoints the input at container_path / bin_path."""
    tc.setdefault("exec_mode", "container")
    tc.setdefault("container_path", "")
    tc.setdefault("bin_path", "")

    def active_key() -> str:
        return "container_path" if tc.get("exec_mode") == "container" else "bin_path"

    with ui.element("div").style("padding: 5px 16px; border-bottom: 1px solid #f8fafc;"):
        with ui.row().classes("w-full items-center").style("gap: 8px; flex-wrap: nowrap;"):
            ui.label(name).style(
                f"{MONO} font-size: 10px; font-weight: 600; color: {CLR_HEADING}; "
                "width: 128px; flex-shrink: 0; text-align: right; overflow: hidden; text-overflow: ellipsis;"
            )
            dot = ui.element("div").style(_dot_style(check_path_exists(tc.get(active_key()))))

            def _refresh_dot(d=dot, t=tc):
                ok = check_path_exists(t.get("container_path" if t.get("exec_mode") == "container" else "bin_path"))
                d.style(_dot_style(ok))
                d.tooltip("Resolves on disk" if ok else "Not found on disk")

            mode_select = (
                ui.select(["container", "binary"], value=tc.get("exec_mode"))
                .props('dense options-dense borderless popup-content-class=cb-select-popup')
                .classes("cb-select")
                .style("width: 92px; flex-shrink: 0; font-size: 10px;")
            )

            def _on_path(e, t=tc):
                t[active_key()] = (e.value or "").strip()
                _refresh_dot()

            path_inp = (
                ui.input(value=tc.get(active_key()), on_change=_on_path)
                .props("dense")
                .classes("cb-field flex-1")
                .style("min-width: 0; width: 100%;")
            )
            path_inp.tooltip("Resolves on disk" if check_path_exists(tc.get(active_key())) else "Not found on disk")

            def _on_mode(e, t=tc, inp=path_inp):
                t["exec_mode"] = e.value
                inp.value = t.get(active_key()) or ""
                _refresh_dot()

            mode_select.on_value_change(_on_mode)


def _section_slurm(nv: dict[str, Any]) -> None:
    _section_header("SLURM defaults", "Applied when submitting jobs — takes effect on the next run")
    sd: dict[str, Any] = nv["slurm_defaults"]

    # Global defaults — a compact 2-per-row grid of the common fields.
    fields = [
        ("partition", "partition"),
        ("constraint", "constraint"),
        ("gres", "gres"),
        ("mem", "mem"),
        ("cpus_per_task", "cpus"),
        ("time", "time"),
        ("nodes", "nodes"),
        ("ntasks_per_node", "ntasks/node"),
    ]
    with ui.element("div").style(
        "display: grid; grid-template-columns: 1fr 1fr; gap: 4px 16px; padding: 8px 16px 4px;"
    ):
        for key, label in fields:
            _grid_text(sd, key, label)

    # Per-job overrides — only the fields actually present per profile.
    profiles: dict[str, Any] = nv["job_resource_profiles"]
    if profiles:
        with ui.element("div").style("padding: 4px 16px 2px;"):
            ui.label("Per-job overrides").style(
                f"{FONT} font-size: 9px; font-weight: 700; color: {CLR_SUBLABEL}; letter-spacing: 0.06em;"
            )
        for key in profiles:
            _profile_row(key, profiles[key])


def _grid_text(store: dict[str, Any], key: str, label: str) -> None:
    with ui.row().classes("items-center").style("gap: 6px; flex-wrap: nowrap;"):
        ui.label(label).style(
            f"{FONT} font-size: 10px; color: {CLR_SUBLABEL}; width: 74px; flex-shrink: 0; text-align: right;"
        )

        def _on_change(e, k=key):
            v = (e.value or "").strip()
            store[k] = _coerce_like(store.get(k), v)

        (
            ui.input(value=str(store.get(key, "")), on_change=_on_change)
            .props("dense")
            .classes("cb-field flex-1")
            .style("min-width: 0; width: 100%;")
        )


def _profile_row(job_key: str, profile: dict[str, Any]) -> None:
    display = _JOB_LABELS.get(job_key, job_key)
    with ui.element("div").style(
        "display: grid; grid-template-columns: 132px 1fr; gap: 8px; align-items: center; "
        "padding: 3px 16px; border-bottom: 1px solid #f8fafc;"
    ):
        ui.label(display).style(
            f"{MONO} font-size: 10px; color: #374151; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;"
        )
        with ui.row().classes("w-full items-center").style("gap: 6px; flex-wrap: wrap;"):
            for key in profile:
                _mini_field(profile, key)


def _mini_field(store: dict[str, Any], key: str) -> None:
    short = {"cpus_per_task": "cpu", "ntasks_per_node": "ntask", "constraint": "constr"}.get(key, key)
    with ui.row().classes("items-center").style("gap: 3px; flex-wrap: nowrap;"):
        ui.label(short).style(f"{FONT} font-size: 8px; color: {CLR_SUBLABEL}; flex-shrink: 0;")

        def _on_change(e, k=key):
            v = (e.value or "").strip()
            store[k] = _coerce_like(store.get(k), v)

        (
            ui.input(value=str(store.get(key, "")), on_change=_on_change)
            .props("dense")
            .classes("cb-field")
            .style("width: 76px;")
        )


def _section_local(nv: dict[str, Any]) -> None:
    _section_header("Project defaults", "Landing-page defaults for new projects")
    local: dict[str, Any] = nv["local"]
    local.setdefault("DefaultProjectBase", "")
    local.setdefault("DefaultMoviesGlob", "")
    local.setdefault("DefaultMdocsGlob", "")

    def mk(k):
        def _s(v):
            local[k] = v

        return _s

    _path_field("project base", local.get("DefaultProjectBase") or "", mk("DefaultProjectBase"), kind="dir")
    _path_field("movies glob", local.get("DefaultMoviesGlob") or "", mk("DefaultMoviesGlob"), kind="glob")
    _path_field("mdocs glob", local.get("DefaultMdocsGlob") or "", mk("DefaultMdocsGlob"), kind="glob")


def _coerce_like(old: Any, new: str) -> Any:
    """Keep the yaml type stable: if the default was an int, store an int so
    the diff-vs-default doesn't spuriously fire on '4' vs 4."""
    if isinstance(old, bool):
        return new.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(old, int) and not isinstance(old, bool):
        try:
            return int(new)
        except (TypeError, ValueError):
            return new
    return new
