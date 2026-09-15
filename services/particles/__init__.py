"""Particle / pick-list services.

Coordinate math (``coords``), pick-list merge (``pick_merge``), curated-subset filtering
(``picks_filter``), per-list extraction (``list_extraction``) and the pick → subtomo
linkage (``subtomo_link``); plus ``ingest`` (register on-disk lists on ``ProjectState``),
``species_overview`` (headless per-species read model), ``species_jobs`` (species ↔
particle-phase job attribution), ``list_ref`` (the ``ListRef`` identity the shared
pick-list actions take), ``tomo_identity`` (cross-project tomogram identity + the
coordinate-transferability gate) and ``coord_merge`` (the coordinate-grade union of one
species' lists across tomograms and projects). Rendering helpers live in
``services/visualization/``.
"""
