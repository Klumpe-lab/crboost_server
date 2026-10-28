"""The kitchen's one custom element: ``kitchen.js`` draws the count line, the filter strip, the slab
panel and the gallery from one in-browser store, so no NiceGUI element exists per pick.

Props are small (URLs, the draft, view settings); verdicts and tags go through `apply_verdicts` /
`apply_tags`, so changing the tile size does not resend them.
"""

from __future__ import annotations

from typing import Any

from nicegui.element import Element


class KitchenView(Element, component="kitchen.js"):
    def __init__(self, *, species_color: str, tile_url: str, slab_url: str, view: dict[str, Any]) -> None:
        super().__init__()
        self._props.update(
            table_url="",
            tile_url=tile_url,
            slab_url=slab_url,
            draft={"filters": [], "post": {}},
            frame_choice={},
            species_color=species_color,
            tag_defs=[],
            **view,
        )

    def set_props(self, **props: Any) -> None:
        changed = {k: v for k, v in props.items() if self._props.get(k) != v}
        if changed:
            self._props.update(changed)
            self.update()

    def apply_verdicts(self, verdicts: dict[str, int]) -> None:
        self.run_method("apply_verdicts", verdicts)

    def apply_tags(self, tags: dict[str, int]) -> None:
        self.run_method("apply_tags", tags)

    def focus_frame(self, code: int) -> None:
        self.run_method("focus_frame", code)

    def show_uids(self, groups: list[dict]) -> None:
        """Show only these particles, group by group ({label, uids}), until the user goes back."""
        self.run_method("show_uids", groups)
