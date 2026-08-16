"""Particle / pick-list services (roadmap 09).

The load-bearing half of the former ``services/visualization/`` package: coordinate
math (``coords``), pick-list merge (``pick_merge``), curated-subset filtering
(``picks_filter``), per-list extraction (``list_extraction``) and the pick → subtomo
linkage (``subtomo_link``); plus ``ingest`` (register on-disk lists on ``ProjectState``),
``species_overview`` (headless per-species read model) and ``species_jobs`` (species ↔
particle-phase job attribution). Rendering helpers stay in ``services/visualization/``.
"""
