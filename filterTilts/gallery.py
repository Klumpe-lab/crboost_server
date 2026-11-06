"""The one fact the host and the tilt-filter container share: the side of the gallery PNGs.

The host makes them (ImageProcessor: a Fourier crop to this size, min-max scaled to 8 bits, into TiltFilter/png) and
the container's model reads them (deepLearning.model_loader). Every model reads these; one trained at another size
gets them resized, as at training. The number lives here so that neither side imports the other's packages.
"""

GALLERY_PNG_SIZE = 384
