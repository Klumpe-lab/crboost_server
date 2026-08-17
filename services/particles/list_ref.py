"""``ListRef`` — the identity of ONE pick list on ONE tomogram, as the shared pick-list
actions consume it (roadmap 11-S1).

The Journey and the Species page describe the same list from different materials (the
Journey from its ``sp`` / ``lst`` render dicts, the Species page from a ``PickList`` plus
the species' bound candidate-extract / subtomo instances); ``ui/particles/list_actions``
takes neither — it takes this. UI-free, frozen, hashable: everything an action needs to
resolve state (``project_path``, ``species_id``, ``tomo_name``, ``slug``), the list's own
star, and the species' job anchors (the CE job dir for ``candidates.star`` /
``optimisation_set.star``, the subtomo job dir for the ``auto`` list's kept subset, the
subtomo instance for the extraction geometry) — plus the tomogram's ``tomograms.star``,
which the ArtiaX round trip needs even for a species with no jobs at all.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from services.models_base import PickListType

AUTO_SLUG = "auto"


def fs_slug(name: str) -> str:
    """Filesystem-safe slug (no regex dep): merged-list slugs (``merged__<name>``) and the
    Journey's cutout-cache file names."""
    return "".join(c if (c.isalnum() or c in "._-") else "_" for c in str(name)).strip("_") or "x"


@dataclass(frozen=True, slots=True)
class ListRef:
    project_path: Path
    species_id: str
    species_label: str
    tomo_name: str
    slug: str
    label: str
    list_type: PickListType
    star_path: str | None  # this list's coordinates star (auto: the CE job's candidates.star); None = nothing on disk
    ce_job_dir: Path | None  # the species' candidate-extract job dir (None for a de-novo species)
    subtomo_job_dir: Path | None  # the species' subtomo-extraction job dir (None when it never ran)
    subtomo_iid: str | None  # that job's instance id — its model carries the extraction geometry
    tomograms_star: Path | None  # the tomogram's tomograms.star (the CE job's copy when it has one)

    @property
    def is_auto(self) -> bool:
        return self.slug == AUTO_SLUG

    @property
    def candidates_star(self) -> Path | None:
        """The species' reference pick list for an ArtiaX session — None for a species
        with no candidate-extract job (the session opens with an empty pick set)."""
        return self.ce_job_dir / "candidates.star" if self.ce_job_dir else None

    def merge_source_dict(self) -> dict:
        """The render-dict shape ``picks_filter.merge_source_for`` looks a list up by."""
        return {"slug": self.slug, "path": self.star_path, "list_type": self.list_type}
