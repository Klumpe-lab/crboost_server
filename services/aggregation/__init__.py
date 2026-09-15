"""Cross-project aggregation services.

Aggregation collects the same species' extracted particles from several projects into one
optimisation set for a joint refinement. Two headless halves live here:

- ``discovery`` — read-only enumeration: which projects hold subtomo optimisation sets, for
  which species, on which tomograms, plus the per-tomo curation counts the Species page and
  the Journey read as well. Pure; no ``ProjectState`` mutation.
- ``extraction`` — per-list subtomo-extraction inputs (schema source, list star, geometry)
  and ``pending_extractions``: which of a species' lists are not cut or have gone stale, and
  why the ones extraction can't fix are stuck. Also owns ``apply_aggregation_overrides``, the
  one mutator, which is invocation-scoped (the merge card's actions call it), not
  render-scoped. What feeds a refinement is the source set the user selects in the merge
  card, not a stored per-tomogram nomination.

The merge itself runs on a compute node — ``services/subtomo_merge.py``, invoked by
``drivers/subtomo_merge.py``; it stays out of this package because it is driver-side code
with its own strict optics validation. The UI half is ``ui/aggregation/merge_card.py``.
"""
