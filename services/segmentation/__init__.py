"""Segmentations: probability volumes of features (membrane, void, …) on a project's tomograms.

No UI here. `manifest` is the file every producer writes (the Segment job, an import of volumes
made elsewhere); the volumes sit in the job's directory, or where the import found them, and are
registered per tomogram in the tilt-series registry
(`services.tilt_series.models.SegmentationTomogramOutput`). `manifest`, `volumes`, `easymode` and
`search_region` also run on the compute nodes (the Segment and template-match drivers); `ingest`
registers volumes made elsewhere, `display` holds what the views show, `chimerax` what a ChimeraX
session loads.
"""
