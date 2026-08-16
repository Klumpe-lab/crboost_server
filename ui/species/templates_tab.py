"""Templates & masks tab (roadmap 10 S2) — the mounted Template Workbench.

`TemplateWorkbench` (`ui/template_workbench.py`: molstar bridge, ready gate, dedup,
polarity pairs) is constructed ONCE per species inside the tab's container, exactly as
the old `species_workbench_panel._ensure_species_rendered` did; the page keeps that
container and only flips its visibility on species switch. Never clear()+rebuild a
workbench: it reloads the molstar iframe and leaves the previous instance's window
`message` listener behind (double `emitEvent`).
"""

from __future__ import annotations

from nicegui import ui

from ui.species.tab import TabContext
from ui.template_workbench import TemplateWorkbench


class TemplatesTab:
    def __init__(self, ctx: TabContext) -> None:
        self._ctx = ctx
        self.workbench: TemplateWorkbench | None = None

    def build(self, container: ui.element) -> None:
        ctx = self._ctx
        with container:
            self.workbench = TemplateWorkbench(
                ctx.backend, str(ctx.project_path), species_id=ctx.species_id, on_species_deleted=ctx.on_species_deleted
            )

    def refresh(self) -> None:
        # The workbench observes its own species (rev-driven header edits, template /
        # mask appends); the page has nothing to push into it.
        return None
