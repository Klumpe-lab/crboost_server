"""Segmentations: probability volumes of features (membrane, void, …) on a project's tomograms.

Pure, no UI and no project state: the Segment job's driver imports this package on the compute
nodes. `manifest` is the file every producer writes; the volumes themselves sit in the producer's
directory and are registered per tomogram in the tilt-series registry
(`services.tilt_series.models.SegmentationTomogramOutput`).
"""
