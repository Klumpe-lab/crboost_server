# ui/styles.py
# Shared CSS constants for the CryoBoost UI layer.

MONO = "font-family: 'IBM Plex Mono', monospace;"
SANS = "font-family: 'IBM Plex Sans', sans-serif;"

# Something on its way: a thin gray arc turning. SVG/SMIL, so the motion needs no stylesheet.
WAIT_SPINNER = (
    '<svg width="16" height="16" viewBox="0 0 16 16" style="display: block;">'
    '<circle cx="8" cy="8" r="6" fill="none" stroke="#94a3b8" stroke-width="1.5" stroke-linecap="round" '
    'stroke-dasharray="9 29"><animateTransform attributeName="transform" type="rotate" from="0 8 8" '
    'to="360 8 8" dur="0.9s" repeatCount="indefinite"/></circle></svg>'
)
