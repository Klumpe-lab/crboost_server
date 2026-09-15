"""The Species page.

`page.build_species_page` mounts ``[rail | detail]``: `rail.SpeciesRail` (one row per
registered species), a header (species pill + segmented tabs) and one lazily built,
cached container per (species, tab). Tabs implement `tab.SpeciesTab`; `templates_tab`
mounts the Template Workbench (`ui/template_workbench.py`) as is.
`prompt.create_species` is the one create path (roster "+", page "+", Journey empty state).
"""
