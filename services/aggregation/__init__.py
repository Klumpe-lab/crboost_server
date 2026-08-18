"""Cross-project aggregation services (de-novo roadmap S6).

Aggregation collects the SAME species' extracted particles from several projects into one
optimisation set for a joint refinement. Two headless halves live here:

- ``discovery`` — read-only enumeration: which projects hold subtomo optimisation sets, for
  which species, on which tomograms, plus the per-tomo curation counts the Species page and
  the Journey read as well. Pure; no ``ProjectState`` mutation.
- ``authoritative`` — the authoritative-handle model and its ``GateReport``: for one
  (species, tomogram), WHICH pick list is the one downstream consumes, whether its
  extraction is current, and what blocks a roll-up. Also owns
  ``apply_aggregation_overrides``, the one mutator, which is INVOCATION-scoped (the merge
  card's actions call it) and not render-scoped.

The merge itself runs on a compute node — ``services/subtomo_merge.py``, invoked by
``drivers/subtomo_merge.py``; it stays out of this package because it is driver-side code
with its own strict optics validation. The UI half is ``ui/aggregation/merge_card.py``.

Relocated from flat ``services/aggregation_*.py`` modules by S6; no import shims were left
behind, every call site was repointed.
"""
