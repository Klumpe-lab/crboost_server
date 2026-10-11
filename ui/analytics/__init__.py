"""Particle analytics views: panels that render the records of `services/particles/records.py`.

Each panel takes records (and the user's current choices, such as a σ cut) and draws; none reads
a job directory or computes a fixed fact itself, so a panel can move between pages — the Species
page's Analytics tab today, a reworked Journey later — without re-wiring. Chart options come from
`ui/dashboard/figures.py`; controls are the house ones.
"""
