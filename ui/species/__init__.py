"""The Species page (roadmap 10) — replaces the Template Workbench view.

`page.build_species_page` mounts ``[rail | detail]``: `rail.SpeciesRail` (one row per
registered species), a header (species pill + segmented tabs) and one lazily built,
cached container per (species, tab). Tabs implement `tab.SpeciesTab`; the Template
Workbench (`ui/template_workbench.py`) is MOUNTED by `templates_tab`, never rewritten.
`prompt.create_species` is the one create path (roster "+", page "+", Journey empty state).
"""
