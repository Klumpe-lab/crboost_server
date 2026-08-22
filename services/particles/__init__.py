"""Particle / pick-list services (roadmap 09).

The load-bearing half of the former ``services/visualization/`` package: coordinate
math (``coords``), pick-list merge (``pick_merge``), curated-subset filtering
(``picks_filter``), per-list extraction (``list_extraction``) and the pick → subtomo
linkage (``subtomo_link``); plus ``ingest`` (register on-disk lists on ``ProjectState``),
``species_overview`` (headless per-species read model), ``species_jobs`` (species ↔
particle-phase job attribution) and ``list_ref`` (the ``ListRef`` identity the shared
pick-list actions take, roadmap 11), ``tomo_identity`` (cross-project tomogram identity +
the coordinate-transferability gate) and ``coord_merge`` (the coordinate-grade union of
one species' lists across tomograms and projects, both roadmap 12). Rendering helpers stay in
``services/visualization/``.
"""
