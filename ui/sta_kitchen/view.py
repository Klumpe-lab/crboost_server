"""The kitchen's one custom element: ``sta_kitchen.js`` draws the filter strip, the cutout gallery and the
by-tomogram stream from one in-browser store, so no NiceGUI element exists per pick.

Props are small (URLs, the draft, view settings); verdicts go through `apply_verdicts`, so changing the
tile size does not resend them.
"""

from __future__ import annotations

from typing import Any

from nicegui.element import Element


class KitchenView(Element, component="sta_kitchen.js"):
    def __init__(self, *, species_color: str, diameter_a: float | None, view: dict[str, Any]) -> None:
        super().__init__()
        self._props.update(
            table_url="",
            tiles_url="",
            slab_url="",
            draft={"filters": [], "post": {}},
            frame_choice={},
            source_choice={},
            subsets=[],
            groups=[],
            sets=[],
            subset_sets={},
            set_masks={},
            review=None,
            loupe_url="",
            loupe_png_url="",
            species_color=species_color,
            diameter_a=float(diameter_a) if diameter_a else None,
            **view,
        )

    def set_props(self, **props: Any) -> None:
        changed = {k: v for k, v in props.items() if self._props.get(k) != v}
        if changed:
            self._props.update(changed)
            self.update()

    def apply_verdicts(self, verdicts: dict[str, int]) -> None:
        self.run_method("apply_verdicts", verdicts)

    def apply_subsets(self, lists: dict[str, list[str]], full: bool) -> None:
        """Subsets' uids: every subset's (`full`), or only those named, the others kept."""
        self.run_method("apply_subsets", lists, full)

    def send_draft(self, draft: dict[str, Any]) -> None:
        """The draft, sent even when it equals the last one sent (the browser's own may have moved since)."""
        self._props["draft"] = draft
        self.update()
